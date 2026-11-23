"""End-to-end check that a project wait passes its deadline and fails visibly.

Run from the repository root: ``.venv/bin/python -m tests.e2e.deadline``. The cases run the host commands' own wait,
``scripts.infra.host.wait_for``, with a real program that hangs, started through the project's launcher. A control wait
that is ready in time must return its value. A wait on a program that never answers must raise ``HostError`` that names what
it waited for and its deadline, soon after the deadline, and no process of the hung program may be left. A program that answers
after the deadline must not count as ready. Every run, passing, failing or interrupted, writes one record in a new folder
under ``runs/`` and stops every process it finds with its own marker. A cleanup that fails is recorded as a failed assertion, and one that
is interrupted marks the run blocked; neither stops the record from being written.
"""

from __future__ import annotations

import os
import platform
import secrets
import signal
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from subprocess import CalledProcessError

from scripts.infra.host import HostError, wait_for
from scripts.process import ProcessGroupSurvived, TimeoutExpired, run_command
from tests.e2e.commit_guards import recorded_state
from tests.e2e.run_record import ROOT, Record

RERUN = ".venv/bin/python -m tests.e2e.deadline"
DEADLINE_SECONDS = 3
SLACK_SECONDS = 15
LATE_ANSWER_SECONDS = 5
COMMAND_TIMEOUT_SECONDS = 60
HUNG = "sleep {duration} & wait"


def marked_processes(marker: str) -> list[int]:
    """The ids of the processes whose command line holds ``marker``, read from the process table."""
    listing = run_command("ps", ["-axo", "pid=,command="], timeout=COMMAND_TIMEOUT_SECONDS, check=True).stdout
    return [int(line.split(None, 1)[0]) for line in listing.splitlines() if marker in line]


def hung_command(marker: str) -> list[str]:
    """A shell that starts a background sleep and waits for it. The sleep's duration is the marker, so no other program matches it."""
    return ["-c", HUNG.format(duration=marker)]


def timed_wait(what: str, probe_command: list[str], *, attempt_seconds: float | None = None) -> tuple[str | None, str, float]:
    """Run the host's wait on a program. Return its value, the error text and the seconds it took."""
    started = time.monotonic()

    def probe(remaining: float) -> str | None:
        result = run_command("sh", probe_command, timeout=remaining if attempt_seconds is None else attempt_seconds)
        return result.stdout if result.returncode == 0 and result.stdout.strip() else None

    try:
        value, error = wait_for(what, probe, DEADLINE_SECONDS), ""
    except HostError as raised:
        value, error = None, str(raised)
    return value, error, time.monotonic() - started


def run_cases(record: Record, log: list[str], marker: str) -> None:
    what = "the planted service to answer"
    value, error, seconds = timed_wait(what, ["-c", "echo ready"])
    log.append(f"== control: value {value!r}, error {error!r}, {seconds:.1f} s")
    ready = value == "ready\n" and not error and seconds < DEADLINE_SECONDS
    record.expect("wait that is ready in time", "control", "returns its value inside the deadline", f"value {value!r}, error {error!r}, {seconds:.1f} s", ready)

    value, error, seconds = timed_wait(what, hung_command(marker))
    log.append(f"== hung: value {value!r}, error {error!r}, {seconds:.1f} s")
    left = marked_processes(marker)
    message = f"deadline of {DEADLINE_SECONDS} seconds passed while waiting for {what}"
    expected = f"HostError '{message}' within {DEADLINE_SECONDS + SLACK_SECONDS} s and no process of the hung program left"
    observed = f"value {value!r}, error {error!r}, {seconds:.1f} s, processes left: {len(left)}"
    record.expect(
        "wait on a program that never answers",
        "fault",
        expected,
        observed,
        value is None and error == message and seconds < DEADLINE_SECONDS + SLACK_SECONDS and not left,
    )

    late = ["-c", f"sleep {LATE_ANSWER_SECONDS}; echo late"]
    value, error, seconds = timed_wait(what, late, attempt_seconds=COMMAND_TIMEOUT_SECONDS)
    log.append(f"== late: value {value!r}, error {error!r}, {seconds:.1f} s")
    expected = f"HostError '{message}' and no value, although the program answers after {LATE_ANSWER_SECONDS} s"
    record.expect(
        "wait on a program that answers after the deadline",
        "fault",
        expected,
        f"value {value!r}, error {error!r}, {seconds:.1f} s",
        value is None and error == message,
    )


def stop_marked(marker: str, record: Record) -> None:
    """Kill each process that still holds the marker and record the cleanup. A cleanup that cannot list or stop them is recorded as a failure."""
    try:
        left = marked_processes(marker)
        for pid in left:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue
    except (OSError, CalledProcessError, TimeoutExpired, ProcessGroupSurvived) as error:
        record.cleanup = f"cleanup failed: {type(error).__name__}: {error}"
        record.expect("cleanup", "stop", "the leftover processes are listed and stopped", record.cleanup, False)
        return
    record.cleanup = f"stopped {len(left)} leftover processes" if left else "no process was left"
    if left:
        record.expect("cleanup", "stop", "no process of the run is left", record.cleanup, False)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the deadline cases, write the run record and return 0 only when every assertion held."""
    if argv:
        sys.stderr.write(f"usage: python -m tests.e2e.deadline (no arguments, got {list(argv)!r})\n")
        return 2
    record = Record(feature="project wait deadline", slug="deadline_wait", rerun=RERUN)
    record.limits = ["the wait is the host commands' own function with a local program that hangs: no host, AWS or Tailscale call is made"]
    record.environment = {"platform": f"{platform.system()} {platform.machine()}", "python": platform.python_version()}
    record.commands = ["scripts.infra.host.wait_for through scripts.process.run_command: sh -c 'sleep <marker> & wait'"]
    marker = f"{int(datetime.now(UTC).timestamp()) % 1000 + 3600}.{secrets.randbelow(10**6):06d}"
    log: list[str] = []
    blocked = ""
    try:
        record.revision, record.tree_sha256 = recorded_state(ROOT)
        run_cases(record, log, marker)
    except KeyboardInterrupt:
        blocked = "interrupted"
        raise
    except Exception as error:
        log.append(f"{type(error).__name__}: {error}")
        record.expect("run", "protected execution", "every step completes", f"{type(error).__name__}; see log.txt", False)
    finally:
        code = record.close_after(lambda: stop_marked(marker, record), log, blocked=blocked)
    return code


if __name__ == "__main__":
    sys.exit(main())
