"""The record that every end-to-end run writes under ``runs/``, passing or failing."""

from __future__ import annotations

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

ROOT = Path(__file__).resolve().parents[2]
RUN_PAYLOAD_CLASS = 3
PASSED_END = "the result is verified and its work is pushed to hosted CI"
FAILED_END = "the failure is fixed and the fix is pushed to hosted CI"
BLOCKED_PREFIX = "blocked: "


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
