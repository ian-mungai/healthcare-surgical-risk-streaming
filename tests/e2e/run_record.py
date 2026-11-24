"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

import json
import re
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.aws_cli import AWS_TIMEOUT_SECONDS
from scripts.infra.host import RELEASE_DEADLINE_SECONDS
from scripts.infra.teardown import GONE_DEADLINE_SECONDS
from scripts.infra.terraform_wrapper import APPLY_TIMEOUT_SECONDS, INIT_TIMEOUT_SECONDS, PLAN_TIMEOUT_SECONDS
from scripts.process import CompletedProcess, TimeoutExpired, run_command, timeout_text

ROOT = Path(__file__).resolve().parents[2]
RUN_PAYLOAD_CLASS = 3
PASSED_END = "the result is verified and its work is pushed to hosted CI"
FAILED_END = "the failure is fixed and the fix is pushed to hosted CI"


class StopRun(Exception):
    """Raised inside the steps of a run to end them early. The run's own cleanup then runs, and only after it the record is closed, so no record is saved
    with its cleanup still pending."""


CREATE_STEP = "scripts.infra.state_bootstrap create"
CREATE_ATTEMPTED = "attempted"
CREATE_NONE = "none"
MARGIN_SECONDS = 10 * AWS_TIMEOUT_SECONDS
# The S3 requests of the teardown's data and state cleanup, each of which can take ``AWS_TIMEOUT_SECONDS`` (a drift check in the fast tests counts them again
# in ``scripts/infra/teardown.py``): the requests that do not depend on the bucket's contents (the listings, the lock, the owner checks, the bucket's
# deletion), and the most requests that one object version can cost (a state version is read and then deleted). The destroy itself writes the state version
# and Terraform's lock cycle (a lock version and its delete marker) into the bucket.
S3_FIXED_CALLS = 12
S3_CALLS_PER_VERSION = 2
DESTROY_WRITTEN_VERSIONS = 3
# The seconds that a command's own bounded steps can take in all, counted from the commands' Terraform calls (a drift check in the fast tests counts them
# again): the wrapper's plan is the init, the workspace check, the plan and two shows; its apply is two shows and the apply; the inventory is the init, the
# workspace check and a show; the teardown's plan is two state reads and a wrapper plan; its destroy is two reads, the apply, the Tailscale release and the
# waits for the bucket and for the removal checks, and the S3 requests that do not depend on the bucket's contents. The requests for each version in the
# bucket come on top (``command_deadline``).
STEP_SECONDS: dict[tuple[str, str], int] = {
    ("scripts.infra.terraform_wrapper", "plan"): INIT_TIMEOUT_SECONDS + 4 * PLAN_TIMEOUT_SECONDS,
    ("scripts.infra.terraform_wrapper", "apply"): 2 * PLAN_TIMEOUT_SECONDS + APPLY_TIMEOUT_SECONDS,
    ("scripts.infra.inventory", ""): INIT_TIMEOUT_SECONDS + 2 * PLAN_TIMEOUT_SECONDS,
    ("scripts.infra.teardown", "plan"): INIT_TIMEOUT_SECONDS + 6 * PLAN_TIMEOUT_SECONDS,
    ("scripts.infra.teardown", "destroy"): 2 * PLAN_TIMEOUT_SECONDS
    + APPLY_TIMEOUT_SECONDS
    + RELEASE_DEADLINE_SECONDS
    + 2 * GONE_DEADLINE_SECONDS
    + (S3_FIXED_CALLS + S3_CALLS_PER_VERSION * DESTROY_WRITTEN_VERSIONS) * AWS_TIMEOUT_SECONDS,
}


class StepTimedOut(Exception):
    """A step's outer deadline passed. It is not a ``RuntimeError``, so a handler that catches a failed step never takes it for one: a child of the step can
    still run, and what it is changing is not known. The runner records the failed step and closes the record. Whether the binding of its bucket is kept
    depends on the step: the create step's timeout keeps it with no removal tried (``settle_binding``), and each runner that stops its cleanup on any timeout
    says so itself. ``step`` names the step that timed out."""

    step: str = ""


def command_deadline(module: str, arguments: Sequence[str], default: float, versions: int = 0) -> float:
    """The outer deadline of a command that a runner starts: more than the sum of the command's own bounded steps (``STEP_SECONDS``) by ``MARGIN_SECONDS``,
    so the runner never stops a command that is still inside its own deadline. The teardown's destroy also counts the S3 requests of each of the
    ``versions`` object versions that its bucket holds. A command with no Terraform step keeps the runner's ``default``."""
    key = (module, arguments[0] if arguments else "")
    steps = STEP_SECONDS.get(key)
    if steps is None:
        return default
    return steps + MARGIN_SECONDS + (versions * S3_CALLS_PER_VERSION * AWS_TIMEOUT_SECONDS if key == ("scripts.infra.teardown", "destroy") else 0)


def timed_command(step: str, program: str, args: Sequence[str], **options: Any) -> CompletedProcess[str]:
    """``run_command`` whose timeout is a ``StepTimedOut`` that names ``step`` and the deadline and never the arguments, which can hold private names."""
    try:
        return run_command(program, args, **options)
    except TimeoutExpired:
        failure = StepTimedOut(timeout_text(step, options["timeout"]))
        failure.step = step
        raise failure from None


def read_binding(path: Path) -> dict[str, Any] | None:
    """The run binding that a runner's command wrote at ``path``: None only when there is no file, and an empty dict when something is there that cannot be
    read as a JSON object (a corrupt file, a failed read, a folder), so such a binding is never taken for an absent one."""
    try:
        loaded = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except OSError, ValueError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def named_bucket(binding: dict[str, Any] | None) -> str | None:
    """The bucket that a binding, read once with ``read_binding``, names. None when there is no binding: the commands write the binding before they create
    anything, so a finished command that left no file created no bucket. An empty string when there is a binding that names none that can be read: what it
    names is not known."""
    if binding is None:
        return None
    bucket = binding.get("bucket")
    return bucket if isinstance(bucket, str) else ""


def settle_binding(
    binding_path: Path, project_bucket: str, remove: Callable[[str], tuple[str, bool]], *, started: bool, timed_out: StepTimedOut | None = None
) -> tuple[str, bool]:
    """What a runner records as its cleanup when it closes, and whether its recovery binding at ``binding_path`` can be removed.

    The binding is read once, so the bucket and how it was created come from one answer: a second read that fails must never hide what the first one showed.
    The binding goes only when every resource that it names is confirmed gone or was never this run's: nothing was started, a finished create step left no
    binding (the commands write the binding before they create anything), the binding says that this run created nothing (``bucket_creation`` is ``none``:
    the create step refused a bucket that already existed, so no bucket and no object is removed, because none is this run's), or ``remove`` (the runner's
    removal of the bucket named by the binding, returning its text and whether S3 reported the bucket gone) confirmed it. Every other path keeps the binding
    and says that resources may remain: a create step that timed out, because a child of it may still be creating the bucket, so no removal is tried; a
    binding whose ``bucket_creation`` is ``attempted``, because the create call was sent and its outcome is not known (it may have timed out and still
    land), so a later answer that S3 has no such bucket proves nothing and no removal is tried; a binding that cannot be read for any reason or names a
    bucket that is no short-lived test bucket of this project; a removal that could not confirm. No path claims that no bucket was created unless it
    confirmed that.
    """
    if not started:
        return "no bucket was created", True
    name = binding_path.name
    binding = read_binding(binding_path)
    named = named_bucket(binding)
    if timed_out is not None and timed_out.step == CREATE_STEP:
        where = f"the binding {name} is kept" if named is not None else "no binding was written"
        return f"the create step timed out and a child of it may still run, so no removal was tried: the test bucket may remain ({where})", False
    if binding is None or named is None:
        return "no bucket was created: the create step finished and wrote no binding", True
    if binding.get("bucket_creation") == CREATE_ATTEMPTED:
        return (
            f"the bucket create was attempted and its outcome is not known, so no removal was tried: the test bucket may remain (the binding {name} is kept)",
            False,
        )
    if not named.startswith(f"{project_bucket}-test-"):
        return f"the binding {name} names no short-lived test bucket that can be read: nothing was removed and resources may remain; binding kept", False
    if binding.get("bucket_creation") == CREATE_NONE:
        return "no bucket was created by this run: the create step refused a bucket that already existed, so no bucket and no object was removed", True
    text, gone = remove(named)
    return (text, True) if gone else (f"{text}; resources may remain; the binding {name} is kept", False)


@dataclass
class Observation:
    case: str
    step: str
    expected: str
    observed: str
    passed: bool


@dataclass
class Record:
    feature: str
    slug: str
    rerun: str
    mode: str = "full"
    environment: dict[str, str] = field(default_factory=dict)
    revision: str = "no commit"
    tree_sha256: str = ""
    started_utc: str = field(default_factory=lambda: datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
    commands: list[str] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    outcome: str = "blocked"
    limits: list[str] = field(default_factory=list)
    cleanup: str = "pending"

    def expect(self, case: str, step: str, expected: str, observed: str, passed: bool) -> bool:
        self.observations.append(Observation(case, step, expected, observed, passed))
        return passed

    def close_after(self, cleanup: Callable[[], None], log: list[str], *, blocked: str = "") -> int:
        """Run the cleanup, then close the record, also when the cleanup fails or is interrupted.

        A failed cleanup is a failed assertion. An interrupt marks the run blocked, and the interrupt propagates after the record is written.
        """
        try:
            cleanup()
        except KeyboardInterrupt:
            blocked = blocked or "interrupted"
            self.cleanup = "cleanup interrupted"
            self.expect("cleanup", "stop", "the cleanup completes", self.cleanup, False)
            raise
        except Exception as error:
            self.cleanup = f"cleanup failed: {type(error).__name__}"
            self.expect("cleanup", "stop", "the cleanup completes", self.cleanup, False)
        finally:
            code = self.close(log, blocked=blocked)
        return code

    def close(self, log: list[str], *, blocked: str = "") -> int:
        """Set the outcome, write the record, its log and its retention label in a new folder, print each assertion and return the exit code."""
        self.outcome = f"blocked: {blocked}" if blocked else ("pass" if self.observations and all(item.passed for item in self.observations) else "fail")
        stamp = self.started_utc.replace(":", "").replace("-", "")
        runs = ROOT / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        mode = re.sub(r"[^a-z0-9]+", "_", self.mode.lower()).strip("_")
        folder = Path(tempfile.mkdtemp(prefix=f"{stamp}_{self.slug}_{mode}_", dir=runs))
        (folder / "record.json").write_text(json.dumps(asdict(self), indent=2) + "\n")
        (folder / "log.txt").write_text("\n".join(log) + "\n")
        label = {
            "class": RUN_PAYLOAD_CLASS,
            "purpose": self.feature,
            "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_condition": PASSED_END if self.outcome == "pass" else FAILED_END,
        }
        (folder / "retention.json").write_text(json.dumps(label, indent=2) + "\n")
        for item in self.observations:
            sys.stdout.write(f"{'PASS' if item.passed else 'FAIL'} {item.case} ({item.step}): {item.observed}\n")
        sys.stdout.write(f"{self.outcome}: record in {folder.relative_to(ROOT)}\n")
        return 0 if self.outcome == "pass" else 1
