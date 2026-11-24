"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

import json
import re
import sys
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_PAYLOAD_CLASS = 3
PASSED_END = "the result is verified and its work is pushed to hosted CI"
FAILED_END = "the failure is fixed and the fix is pushed to hosted CI"
BLOCKED_PREFIX = "blocked: "


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

    def write_record(self, folder: Path) -> None:
        """Write the record and its retention label in ``folder`` as they are now."""
        (folder / "record.json").write_text(json.dumps(asdict(self), indent=2) + "\n")
        label = {
            "class": RUN_PAYLOAD_CLASS,
            "purpose": self.feature,
            "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_condition": PASSED_END if self.outcome == "pass" else FAILED_END,
        }
        (folder / "retention.json").write_text(json.dumps(label, indent=2) + "\n")

    def report(self, folder: Path) -> int:
        """Print each assertion and the outcome, flushed, and return the exit code.

        A print or flush that fails (stdout closed, broken or None) is itself a failed assertion: the record and its label in ``folder`` are written again and
        say fail (or keep their blocked reason), so a record never claims a pass that the run could not print.
        """
        try:
            if sys.stdout is None:
                raise OSError("standard output is closed")
            for item in self.observations:
                sys.stdout.write(f"{'PASS' if item.passed else 'FAIL'} {item.case} ({item.step}): {item.observed}\n")
            sys.stdout.write(f"{self.outcome}: record in {folder.relative_to(ROOT)}\n")
            sys.stdout.flush()
        except (OSError, ValueError) as error:
            blocked = self.outcome.removeprefix(BLOCKED_PREFIX) if self.outcome.startswith(BLOCKED_PREFIX) else ""
            self.expect("run", "output", "the result is printed", type(error).__name__, False)
            self.settle(blocked)
            self.write_record(folder)
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
