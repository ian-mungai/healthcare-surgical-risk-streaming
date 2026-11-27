"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

import contextlib
import errno
import json
import os
import re
import shutil
import socket
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, overload

from scripts.aws_cli import AWS_TIMEOUT_SECONDS
from scripts.infra.host import RELEASE_DEADLINE_SECONDS
from scripts.infra.teardown import GONE_DEADLINE_SECONDS
from scripts.infra.terraform_wrapper import APPLY_TIMEOUT_SECONDS, INIT_TIMEOUT_SECONDS, PLAN_TIMEOUT_SECONDS
from scripts.process import CompletedProcess, TimeoutExpired, run_command, timeout_text

ROOT = Path(__file__).resolve().parents[2]
RUN_PAYLOAD_CLASS = 3
PASSED_END = "the result is verified and its work is pushed to hosted CI"
FAILED_END = "the failure is fixed and the fix is pushed to hosted CI"
BLOCKED_PREFIX = "blocked: "
VERDICT_FILES = ("record.json", "retention.json", "record.json.part", "retention.json.part")


def proven_absent(path: Path) -> bool:
    """True only when ``path`` is shown not to be there: its look-up says that the name or a part of its path does not exist. A look-up that fails for any other
    reason (a folder that cannot be searched, an input or output error) proves nothing, so the path is not called gone. A name that is there is not absent."""
    try:
        path.lstat()
    except FileNotFoundError, NotADirectoryError:
        return True
    except OSError:
        return False
    return False


def remove_verified(folder: Path, names: tuple[str, ...] = VERDICT_FILES) -> list[str]:
    """Remove the files ``names`` of ``folder`` and give back every name that is not proven absent afterwards (``proven_absent``): a removal is checked,
    never assumed, and a check that fails counts as a file that may still be there."""
    for name in names:
        with contextlib.suppress(OSError):
            (folder / name).unlink()
    return [name for name in names if not proven_absent(folder / name)]


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


class Unread:
    """A read that failed. It is never equal to anything, itself included, and it is false, so a failed read is no answer: two failed reads are not
    "unchanged", a failed listing is not "none left" and a failed scan is not "nothing found". ``reason`` holds the exit code or the kind of error and never
    any of the output."""

    __slots__ = ("reason",)

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __eq__(self, other: object) -> bool:
        return False

    def __hash__(self) -> int:
        return id(self)

    def __bool__(self) -> bool:
        return False

    def __str__(self) -> str:
        return f"unread ({self.reason})"

    __repr__ = __str__


@overload
def parsed(code: int, output: str) -> str | Unread: ...


@overload
def parsed[T](code: int, output: str, parse: Callable[[str], T]) -> T | Unread: ...


def parsed(code: int, output: str, parse: Callable[[str], Any] = str) -> Any:
    """The stripped standard output of a read, through ``parse``, when the command exited 0 and printed something that parses, else an ``Unread``."""
    if code != 0:
        return Unread(f"exit {code}")
    text = output.strip()
    if not text:
        return Unread("empty")
    try:
        return parse(text)
    except (ValueError, KeyError, TypeError, IndexError, StopIteration) as error:
        return Unread(type(error).__name__)


RFC3339 = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|[+-](\d{2}):(\d{2}))")
CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
NEVER_STARTED_YEAR = 1
OPEN, REFUSED, DROPPED = "open", "refused", "dropped"


def rfc3339_text(text: str) -> str:
    """``text`` itself when it is an RFC 3339 timestamp of a real moment, with up to nine fractional digits and a ``Z`` or an offset, else a ``ValueError``.

    The text is kept, not a ``datetime``, so every fractional digit of Docker's ``StartedAt`` takes part in a comparison. The zero time that Docker prints for
    a container that never started is no start time.
    """
    found = RFC3339.fullmatch(text)
    if found is None:
        raise ValueError("not an RFC 3339 timestamp")
    year, month, day, hour, minute, second = (int(found.group(index)) for index in range(1, 7))
    datetime(year, month, day, hour, minute, second)
    if found.group(9) is not None and (int(found.group(9)) > 23 or int(found.group(10)) > 59):
        raise ValueError("the offset is not a real one")
    if year == NEVER_STARTED_YEAR:
        raise ValueError("the container never started")
    return text


def container_id(text: str) -> str:
    """``text`` itself when it is a full Docker container ID (64 lower case hexadecimal digits), else a ``ValueError``."""
    if CONTAINER_ID.fullmatch(text) is None:
        raise ValueError("not a container ID")
    return text


def tcp_state(address: str, port: int, timeout: float) -> str | Unread:
    """What the TCP connect to ``address`` and ``port`` alone says, whatever a TLS or HTTP exchange would do after it.

    ``OPEN`` when the connect succeeds. ``REFUSED`` for a refusal or a reset (there was no data to reset). ``DROPPED`` when it times out. Any other error, such
    as an unreachable network or host, a name that does not resolve or a permission error, is an ``Unread`` that names the error: it says nothing about the
    port.
    """
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.settimeout(timeout)
            probe.connect((address, port))
    except ConnectionRefusedError, ConnectionResetError:
        return REFUSED
    except TimeoutError:
        return DROPPED
    except OSError as error:
        if error.errno == errno.ECONNREFUSED:
            return REFUSED
        return Unread(errno.errorcode.get(error.errno or 0, type(error).__name__))
    return OPEN


def state_counts(states: dict[str, str | Unread]) -> str:
    """How many of ``states`` are refused, dropped, open and unread, for an observation."""
    unread = sum(isinstance(state, Unread) for state in states.values())
    return ", ".join([*(f"{name}: {list(states.values()).count(name)}" for name in (REFUSED, DROPPED, OPEN)), f"unread: {unread}"])


def all_closed(states: dict[str, str | Unread]) -> bool:
    """True when there is at least one state and each is a refusal or a drop: no connect succeeded and none is unread."""
    return bool(states) and all(state in (REFUSED, DROPPED) for state in states.values())


def remove_scratch(record: Record, scratch: Path) -> bool:
    """Remove the run's scratch folder and record the result as an assertion. A link or a file left in its place counts as left (``lexists``)."""
    shutil.rmtree(scratch, ignore_errors=True)
    gone = not os.path.lexists(scratch)
    return record.expect("scratch folder removed", "control", "the scratch folder of the run is gone", f"removed: {gone}", gone)


def cleanup_step(record: Record, case: str, expected: str, label: str, action: Callable[[], bool]) -> bool:
    """Run one clean-up step and record it as an assertion named ``case``. A step that returns False, or raises anything but an interrupt (a command that
    timed out, a failed removal), fails the record with the error's type and never stops the steps after it."""
    try:
        done = action()
        observed = f"{label}: {done}"
    except Exception as error:
        done, observed = False, f"{type(error).__name__} stopped the step: {str(error)[:200]}"
    return record.expect(case, "control", expected, observed, done)


def clean_up(record: Record, steps: Sequence[tuple[str, str, str, Callable[[], bool]]], scratch: Path | None = None) -> str:
    """Run every clean-up step in order, then remove and verify the scratch folder in a ``finally`` path, also when a step is interrupted, and set and
    return the record's clean-up text.

    Each step is ``(case, expected, label, action)``. The text names each step and starts with ``cleanup failed`` and the failed steps when one did not work,
    so a record never reads as clean (or stays ``pending``) after a step that failed or timed out.
    """
    results: list[tuple[str, bool]] = []
    try:
        for case, expected, label, action in steps:
            results.append((case, cleanup_step(record, case, expected, label, action)))
    finally:
        if scratch is not None:
            gone = cleanup_step(record, "scratch folder removed", "the scratch folder of the run is gone", "removed", lambda: _scratch_gone(scratch))
            results.append(("scratch folder removed", gone))
        text = "; ".join(f"{case}: {done}" for case, done in results)
        failed = [case for case, done in results if not done]
        record.cleanup = f"cleanup failed ({', '.join(failed)}): {text}" if failed else text
    return record.cleanup


def _scratch_gone(scratch: Path) -> bool:
    """Remove the folder and check it: a link or a file left in its place counts as left (``lexists``)."""
    shutil.rmtree(scratch, ignore_errors=True)
    return not os.path.lexists(scratch)


def listed_words(code: int, output: str) -> list[str] | Unread:
    """The words of a listing that exited 0 (an empty list when it printed none), else an ``Unread``: a listing that failed is not an empty one."""
    return output.split() if code == 0 else Unread(f"exit {code}")


def readable(value: object) -> bool:
    """True when ``value`` is a read that succeeded: not a failure, not empty, and for a table or a collection of reads none of its items is one either."""
    if isinstance(value, dict):
        return bool(value) and all(readable(item) for item in value.values())
    if isinstance(value, (set, frozenset, list, tuple)):
        return bool(value) and all(readable(item) for item in value)
    return not isinstance(value, Unread) and value is not None and value != ""


def unchanged(before: object, after: object) -> bool:
    """True only when both reads succeeded and are equal. A read that failed, on either side or on both, is never "unchanged"."""
    return readable(before) and readable(after) and before == after


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
        self.observations.append(Observation(case, step, expected, str(observed), passed))
        return passed

    def close_after(self, cleanup: Callable[[], None], log: list[str], *, blocked: str | Callable[[], str] = "") -> int:
        """Run the cleanup, then close the record, also when the cleanup fails or is interrupted.

        A failed cleanup is a failed assertion. An interrupt marks the run blocked, and the interrupt propagates after the record is written. ``blocked`` may be
        a function, read after the cleanup, for a reason that the cleanup itself can set. A function is read again after the record is written: a reason that
        appeared during the write (an interrupt that arrived then) is written into the same folder before the result is printed.
        """
        interrupted = False
        try:
            cleanup()
        except KeyboardInterrupt:
            interrupted = True
            self.cleanup = "cleanup interrupted"
            self.expect("cleanup", "stop", "the cleanup completes", self.cleanup, False)
            raise
        except Exception as error:
            self.cleanup = f"cleanup failed: {type(error).__name__}"
            self.expect("cleanup", "stop", "the cleanup completes", self.cleanup, False)
        finally:

            def reason() -> str:
                return (blocked() if callable(blocked) else blocked) or ("interrupted" if interrupted else "")

            code = self.close(log, blocked=reason(), recheck=reason)
        return code

    def settle(self, blocked: str = "") -> None:
        """Set the outcome from the observations so far: blocked with its reason, else pass when every observation passed, else fail."""
        self.outcome = f"{BLOCKED_PREFIX}{blocked}" if blocked else ("pass" if self.observations and all(item.passed for item in self.observations) else "fail")

    def write(self, log: list[str], folder: Path | None = None) -> Path:
        """Write the record, its log and its retention label, in ``folder`` or in a new folder, and return the folder. Written again in the same folder, they
        hold the outcome and the observations as they are now."""
        if folder is None:
            stamp = self.started_utc.replace(":", "").replace("-", "")
            runs = ROOT / "runs"
            runs.mkdir(parents=True, exist_ok=True)
            mode = re.sub(r"[^a-z0-9]+", "_", self.mode.lower()).strip("_")
            folder = Path(tempfile.mkdtemp(prefix=f"{stamp}_{self.slug}_{mode}_", dir=runs))
        (folder / "log.txt").write_text("\n".join(log) + "\n")
        self.write_record(folder)
        return folder

    def invalidate(self, folder: Path) -> None:
        """Remove the earlier record, label and part files of ``folder`` and check that they are gone, so that no earlier verdict stays beside a rewrite that
        is stopped. An ``OSError`` is raised when one of them cannot be removed."""
        left = remove_verified(folder)
        if left:
            raise OSError(f"cannot remove {', '.join(left)}")

    def write_record(self, folder: Path) -> None:
        """Write the retention label and then the record in ``folder`` as they are now. The earlier verdict is removed first (``invalidate``), so that a stop
        between the two files leaves no earlier pass beside the label. Each file goes through a part file that replaces the file whole, and a part file is
        removed when its write fails or is interrupted. An ``OSError`` is raised at the first file that cannot be written."""
        self.invalidate(folder)
        label = {
            "class": RUN_PAYLOAD_CLASS,
            "purpose": self.feature,
            "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_condition": PASSED_END if self.outcome == "pass" else FAILED_END,
        }
        for name, document in (("retention.json", label), ("record.json", asdict(self))):
            staged = folder / f"{name}.part"
            try:
                staged.write_text(json.dumps(document, indent=2) + "\n")
                os.replace(staged, folder / name)
            except BaseException:
                with contextlib.suppress(OSError):
                    staged.unlink()
                raise

    def discard(self, folder: Path, earlier: str = "pass") -> bool:
        """Remove the record and the label of ``folder``, check that they are gone and say so on the error output; give back whether they are gone. A record
        that cannot be brought to its final verdict must not stay with an earlier pass: when it cannot be removed either, the message says that it stays."""
        left = remove_verified(folder)
        said = (
            f"{self.slug}: the record cannot be written again after a failed print, so there is no record\n"
            if not left
            else f"{self.slug}: the record cannot be written again after a failed print and cannot be removed or checked ({', '.join(left)}): "
            f"it may still say {earlier}\n"
        )
        with contextlib.suppress(OSError, ValueError, AttributeError):
            sys.stderr.write(said)
        return not left

    def report(self, folder: Path, rewrite: Callable[[Path], None] | None = None) -> int:
        """Print each assertion and the outcome, flushed, and return the exit code.

        A print or flush that fails (stdout closed, broken or None) is itself a failed assertion. One guarded path then saves the result: ``rewrite`` (this
        record's ``write_record`` when not given) writes the label and the record again in ``folder`` and says fail (or keeps the blocked reason), so a record
        never claims a pass that the run could not print. The earlier record and label are removed first and checked, so that no earlier pass stays beside a
        rewrite that is stopped. When the rewrite fails, the record and the label are removed again, and the removal is checked: when a file
        cannot be removed, the message on the error output says that it stays and may still say the earlier outcome.
        """
        try:
            if sys.stdout is None:
                raise OSError("standard output is closed")
            for item in self.observations:
                sys.stdout.write(f"{'PASS' if item.passed else 'FAIL'} {item.case} ({item.step}): {item.observed}\n")
            sys.stdout.write(f"{self.outcome}: record in {folder.relative_to(ROOT)}\n")
            sys.stdout.flush()
        except (OSError, ValueError) as error:
            earlier = self.outcome
            blocked = earlier.removeprefix(BLOCKED_PREFIX) if earlier.startswith(BLOCKED_PREFIX) else ""
            self.expect("run", "output", "the result is printed", type(error).__name__, False)
            self.settle(blocked)
            try:
                self.invalidate(folder)
                (rewrite or self.write_record)(folder)
            except OSError, ValueError:
                self.discard(folder, earlier)
            return 1
        return 0 if self.outcome == "pass" else 1

    def close(self, log: list[str], *, blocked: str = "", recheck: Callable[[], str] | None = None) -> int:
        """Set the outcome, write the record, its log and its retention label in a new folder, print each assertion and return the exit code (``report``
        saves a failed print as a failed record).

        ``recheck`` is read once after the write. A reason that it gives for a record that is not yet blocked is set and written again in the same folder,
        so the saved record never says pass for a run that an interrupt reached during its write.
        """
        self.settle(blocked)
        folder = self.write(log)
        late = recheck() if recheck is not None else ""
        if late and not self.outcome.startswith(BLOCKED_PREFIX):
            self.settle(late)
            self.write_record(folder)
        return self.report(folder)
