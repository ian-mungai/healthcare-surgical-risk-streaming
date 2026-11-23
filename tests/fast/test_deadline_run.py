"""The run of the deadline program and its record, when its cleanup fails.

``tests/e2e/deadline.py`` stops every process that holds its marker before it writes the run record. Failure modes covered here:

- The process listing is refused, so the cleanup raises and the run ends with no ``record.json``, no ``log.txt`` and no ``retention.json``.
- A leftover process cannot be stopped (permission refused) and the run ends the same way.
- A leftover process that the cleanup stopped is not a failure of the run, so a run that leaks passes.
- The cleanup failure is not in the record, so a passing record hides it.
- An interrupt and a cleanup failure together lose the record, or the interrupt.
- A real interrupt (SIGINT) during the final process listing, or an unexpected error in the cleanup, ends the run with no record.
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


if __name__ == "__main__":
    unittest.main()
