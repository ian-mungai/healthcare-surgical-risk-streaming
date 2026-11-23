"""The run of the deadline program and its record, when its cleanup fails.

``tests/e2e/deadline.py`` stops every process that holds its marker before it writes the run record. Failure modes covered here:

- The process listing is refused, so the cleanup raises and the run ends with no ``record.json``, no ``log.txt`` and no ``retention.json``.
- A leftover process cannot be stopped (permission refused) and the run ends the same way.
- A leftover process that the cleanup stopped is not a failure of the run, so a run that leaks passes.
- The cleanup failure is not in the record, so a passing record hides it.
- An interrupt and a cleanup failure together lose the record, or the interrupt.
- A real interrupt (SIGINT) during the final process listing, or an unexpected error in the cleanup, ends the run with no record.
- A case's own assertion is weakened (forced to pass, or reading too little of the wait's result), so a wait that misses its deadline, returns a late
  answer or leaves a process still passes. The tests above replace the cases; ``CaseAssertionTest`` runs the real case code against stand-in results of
  the wait, with no real waiting.
"""

from __future__ import annotations

import io
import json
import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.e2e import deadline, run_record

DENIED_LISTING = "#!/bin/sh\necho 'ps: operation not permitted' >&2\nexit 1\n"
INTERRUPTING_LISTING = '#!/bin/sh\nkill -INT "$PPID"\nsleep 5\n'


def passing_cases(record: run_record.Record, log: list[str], marker: str) -> None:
    record.expect("planted case", "control", "passes", "passed", True)


def interrupted_cases(record: run_record.Record, log: list[str], marker: str) -> None:
    record.expect("planted case", "control", "passes", "passed", True)
    raise KeyboardInterrupt


class CleanupFailureTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="deadline_run_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name) / "root"
        self.denied = Path(scratch.name) / "denied"
        self.denied.mkdir()
        (self.denied / "ps").write_text(DENIED_LISTING)
        (self.denied / "ps").chmod(0o755)
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.root),
            mock.patch.object(sys, "stdout", io.StringIO()),
            mock.patch.object(deadline, "run_cases", passing_cases),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def refuse_listing(self, listing: str = DENIED_LISTING) -> None:
        (self.denied / "ps").write_text(listing)
        patcher = mock.patch.dict(os.environ, {"PATH": f"{self.denied}{os.pathsep}{os.environ['PATH']}"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def folder(self) -> Path:
        folders = sorted((self.root / "runs").glob("*"))
        self.assertEqual(1, len(folders), "the run must write exactly one folder")
        for name in ("record.json", "log.txt", "retention.json"):
            self.assertTrue((folders[0] / name).is_file(), f"{name} is missing")
        return folders[0]

    def record(self) -> dict[str, object]:
        loaded: dict[str, object] = json.loads((self.folder() / "record.json").read_text())
        return loaded

    def failed_cleanup(self, record: dict[str, object]) -> list[dict[str, object]]:
        observations: list[dict[str, object]] = record["observations"]  # type: ignore[assignment]
        return [item for item in observations if item["case"] == "cleanup" and not item["passed"]]

    def test_a_run_whose_cleanup_finds_no_leftover_passes(self) -> None:
        self.assertEqual(0, deadline.main())
        record = self.record()
        self.assertEqual("pass", record["outcome"])
        self.assertEqual("no process was left", record["cleanup"])

    def test_a_leftover_process_that_the_cleanup_stopped_still_fails_the_run(self) -> None:
        with mock.patch.object(deadline, "marked_processes", return_value=[4242]), mock.patch.object(os, "kill") as kill:
            self.assertEqual(1, deadline.main())
        kill.assert_called_once_with(4242, signal.SIGKILL)
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertEqual("stopped 1 leftover processes", record["cleanup"])
        self.assertEqual(1, len(self.failed_cleanup(record)))

    def test_a_refused_process_listing_still_saves_a_failed_record_with_the_cleanup_failure(self) -> None:
        self.refuse_listing()
        self.assertEqual(1, deadline.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertTrue(str(record["cleanup"]).startswith("cleanup failed: CalledProcessError"), record["cleanup"])
        self.assertEqual(1, len(self.failed_cleanup(record)))
        self.assertEqual("passed", next(item["observed"] for item in record["observations"] if item["case"] == "planted case"))  # type: ignore[attr-defined]

    def test_a_leftover_process_that_cannot_be_stopped_still_saves_a_failed_record_with_the_cleanup_failure(self) -> None:
        with mock.patch.object(deadline, "marked_processes", return_value=[4242]), mock.patch.object(os, "kill", side_effect=PermissionError("not permitted")):
            self.assertEqual(1, deadline.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertTrue(str(record["cleanup"]).startswith("cleanup failed: PermissionError"), record["cleanup"])
        self.assertEqual(1, len(self.failed_cleanup(record)))

    def test_an_interrupt_and_a_refused_listing_save_a_blocked_record_and_the_interrupt_propagates(self) -> None:
        self.refuse_listing()
        with mock.patch.object(deadline, "run_cases", interrupted_cases), self.assertRaises(KeyboardInterrupt):
            deadline.main()
        record = self.record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual(1, len(self.failed_cleanup(record)))

    def test_a_real_interrupt_during_the_final_process_listing_saves_a_blocked_record_and_propagates(self) -> None:
        self.refuse_listing(INTERRUPTING_LISTING)
        with self.assertRaises(KeyboardInterrupt):
            deadline.main()
        record = self.record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual("cleanup interrupted", record["cleanup"])
        self.assertEqual(1, len(self.failed_cleanup(record)))
        self.assertEqual("passed", next(item["observed"] for item in record["observations"] if item["case"] == "planted case"))  # type: ignore[attr-defined]

    def test_an_unexpected_error_in_the_cleanup_still_saves_a_failed_record(self) -> None:
        with mock.patch.object(deadline, "marked_processes", side_effect=ValueError("planted: an unreadable process id")):
            self.assertEqual(1, deadline.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertTrue(str(record["cleanup"]).startswith("cleanup failed: ValueError"), record["cleanup"])
        self.assertEqual(1, len(self.failed_cleanup(record)))


MARKER = "3601.000001"
WHAT = "the planted service to answer"
DEADLINE_MESSAGE = f"deadline of {deadline.DEADLINE_SECONDS} seconds passed while waiting for {WHAT}"
CONTROL_CASE = "wait that is ready in time"
HUNG_CASE = "wait on a program that never answers"
LATE_CASE = "wait on a program that answers after the deadline"
# The wait's result as ``timed_wait`` returns it: the value, the error text and the seconds. The right result of each case, and the results that must fail it.
RIGHT = {
    CONTROL_CASE: ("ready\n", "", 0.1),
    HUNG_CASE: (None, DEADLINE_MESSAGE, deadline.DEADLINE_SECONDS + 0.2),
    LATE_CASE: (None, DEADLINE_MESSAGE, float(deadline.LATE_ANSWER_SECONDS)),
}
WRONG: dict[str, dict[str, tuple[str | None, str, float]]] = {
    CONTROL_CASE: {
        "no value": (None, "", 0.1),
        "another value": ("other\n", "", 0.1),
        "an error beside the value": ("ready\n", DEADLINE_MESSAGE, 0.1),
        "a value only at the deadline": ("ready\n", "", float(deadline.DEADLINE_SECONDS)),
    },
    HUNG_CASE: {
        "a value": ("ready\n", DEADLINE_MESSAGE, deadline.DEADLINE_SECONDS + 0.2),
        "no error": (None, "", deadline.DEADLINE_SECONDS + 0.2),
        "another error": (None, "the host is not ready", deadline.DEADLINE_SECONDS + 0.2),
        "an error that names no deadline": (None, f"deadline passed while waiting for {WHAT}", deadline.DEADLINE_SECONDS + 0.2),
        "the error only after the slack": (None, DEADLINE_MESSAGE, float(deadline.DEADLINE_SECONDS + deadline.SLACK_SECONDS)),
    },
    LATE_CASE: {
        "the late value": ("late\n", "", 5.0),
        "the late value beside the error": ("late\n", DEADLINE_MESSAGE, 5.0),
        "no error": (None, "", 5.0),
        "another error": (None, "the host is not ready", 5.0),
    },
}


class CaseAssertionTest(unittest.TestCase):
    """The case code of ``deadline.run_cases`` judges the wait's result: each case passes with the right result and fails with every wrong one.

    ``timed_wait`` and the process listing are replaced, so a case runs with no wait and no process, and a case assertion that is forced to pass,
    or that reads too little of the result, fails here instead of passing every fast test and failing only the real run.
    """

    def run_cases(self, results: dict[str, tuple[str | None, str, float]], leftovers: list[int] | None = None) -> dict[str, bool]:
        control = ["-c", "echo ready"]
        hung = deadline.hung_command(MARKER)

        def timed_wait(what: str, command: list[str], *, attempt_seconds: float | None = None) -> tuple[str | None, str, float]:
            self.assertEqual(WHAT, what)
            if command == control:
                return results[CONTROL_CASE]
            if command == hung:
                return results[HUNG_CASE]
            self.assertIsNotNone(attempt_seconds, "the late case must give its program a timeout beyond the deadline")
            self.assertEqual(["-c", f"sleep {deadline.LATE_ANSWER_SECONDS}; echo late"], command)
            return results[LATE_CASE]

        def marked_processes(marker: str) -> list[int]:
            self.assertEqual(MARKER, marker)
            return leftovers or []

        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        with mock.patch.object(deadline, "timed_wait", timed_wait), mock.patch.object(deadline, "marked_processes", marked_processes):
            deadline.run_cases(record, [], MARKER)
        judged = {item.case: item.passed for item in record.observations}
        self.assertEqual([CONTROL_CASE, HUNG_CASE, LATE_CASE], [item.case for item in record.observations], "each case is judged once")
        return judged

    def test_the_right_results_pass_every_case(self) -> None:
        self.assertEqual({CONTROL_CASE: True, HUNG_CASE: True, LATE_CASE: True}, self.run_cases(RIGHT))

    def test_each_wrong_result_fails_its_own_case_and_only_that_case(self) -> None:
        for case, wrong in WRONG.items():
            for label, result in wrong.items():
                with self.subTest(case=case, wrong=label):
                    judged = self.run_cases({**RIGHT, case: result})
                    self.assertEqual({name: name != case for name in RIGHT}, judged)

    def test_a_process_of_the_hung_program_that_is_left_fails_the_hung_case(self) -> None:
        self.assertEqual({CONTROL_CASE: True, HUNG_CASE: False, LATE_CASE: True}, self.run_cases(RIGHT, leftovers=[4242]))

    def test_every_case_fails_when_every_result_is_wrong(self) -> None:
        wrong = {case: next(iter(results.values())) for case, results in WRONG.items()}
        self.assertEqual({CONTROL_CASE: False, HUNG_CASE: False, LATE_CASE: False}, self.run_cases(wrong, leftovers=[4242]))


if __name__ == "__main__":
    unittest.main()
