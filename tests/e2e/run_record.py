"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

import contextlib
import json
import os
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
VERDICT_FILES = ("record.json", "retention.json", "record.json.part", "retention.json.part")


def remove_verified(folder: Path, names: tuple[str, ...] = VERDICT_FILES) -> list[str]:
    """Remove the files ``names`` of ``folder`` and give back the names that are still there afterwards: a removal is checked, never assumed."""
    for name in names:
        with contextlib.suppress(OSError):
            (folder / name).unlink()
    return [name for name in names if os.path.lexists(folder / name)]


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
            else f"{self.slug}: the record cannot be written again after a failed print and cannot be removed ({', '.join(left)}): it may still say {earlier}\n"
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

    def close(self, log: list[str], *, blocked: str = "") -> int:
        """Set the outcome, write the record, its log and its retention label in a new folder, print each assertion and return the exit code (``report``
        saves a failed print as a failed record)."""
        self.settle(blocked)
        return self.report(self.write(log))
