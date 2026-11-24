"""The retention label that every run record folder carries: class, purpose, creation time and end condition.

Failure modes covered here:

- A run folder has no ``retention.json``, so nothing says why it is kept or when it goes.
- The label lacks a required field, or holds a class that is not an integer from 1 to 9, or a time that is not UTC ISO 8601.
- A failed or blocked run gets the end condition of a passing run, so its folder is swept before its failure is fixed.
- A record that is written again in its folder (after a scan that came later) keeps the earlier outcome and label, or makes a second folder, or prints the
  outcome before the final verdict.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import mock

from tests.e2e import run_record

REQUIRED = ("class", "purpose", "created", "end_condition")
PASSED = "the result is verified and its work is pushed to hosted CI"
FAILED = "the failure is fixed and the fix is pushed to hosted CI"


class RetentionLabelTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        for patcher in (mock.patch.object(run_record, "ROOT", self.root), mock.patch.object(sys, "stdout", io.StringIO())):
            patcher.start()
            self.addCleanup(patcher.stop)

    def close(self, *, passed: bool = True, blocked: str = "") -> dict[str, object]:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", passed)
        record.close([], blocked=blocked)
        (folder,) = (self.root / "runs").iterdir()
        label: dict[str, object] = json.loads((folder / "retention.json").read_text())
        self.assertTrue((folder / "record.json").is_file())
        return label

    def test_a_run_folder_holds_a_complete_label(self) -> None:
        label = self.close()
        self.assertEqual(sorted(REQUIRED), sorted(label))
        self.assertIs(int, type(label["class"]))
        self.assertEqual(3, label["class"])
        self.assertEqual("a planted feature", label["purpose"])
        created = str(label["created"])
        self.assertEqual(created, datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ"))
        self.assertEqual(PASSED, label["end_condition"])

    def test_a_failed_run_ends_when_its_fix_is_pushed(self) -> None:
        self.assertEqual(FAILED, self.close(passed=False)["end_condition"])

    def test_a_blocked_run_ends_when_its_fix_is_pushed(self) -> None:
        self.assertEqual(FAILED, self.close(blocked="interrupted")["end_condition"])


class WriteAgainTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.printed = io.StringIO()
        for patcher in (mock.patch.object(run_record, "ROOT", self.root), mock.patch.object(sys, "stdout", self.printed)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_a_record_written_again_in_its_folder_holds_the_final_verdict_in_the_record_and_the_label(self) -> None:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle("")
        folder = record.write(["one line"])
        self.assertEqual("pass", json.loads((folder / "record.json").read_text())["outcome"])
        self.assertEqual(PASSED, json.loads((folder / "retention.json").read_text())["end_condition"])
        record.expect("later case", "control", "expected", "observed", False)
        record.settle("")
        again = record.write(["one line"], folder)
        self.assertEqual(folder, again)
        self.assertEqual("fail", json.loads((folder / "record.json").read_text())["outcome"])
        self.assertEqual(2, len(json.loads((folder / "record.json").read_text())["observations"]))
        self.assertEqual(FAILED, json.loads((folder / "retention.json").read_text())["end_condition"])
        self.assertEqual(1, len(list((self.root / "runs").iterdir())))

    def test_a_late_interrupt_turns_a_passing_record_into_a_blocked_one(self) -> None:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle("")
        folder = record.write([])
        record.settle("interrupted")
        record.write([], folder)
        self.assertEqual("blocked: interrupted", json.loads((folder / "record.json").read_text())["outcome"])
        self.assertEqual(FAILED, json.loads((folder / "retention.json").read_text())["end_condition"])

    def test_the_report_prints_each_assertion_and_the_final_outcome_once_and_gives_the_exit_code(self) -> None:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle("")
        folder = record.write([])
        self.assertEqual("", self.printed.getvalue())
        self.assertEqual(0, record.report(folder))
        self.assertEqual(1, self.printed.getvalue().count("pass: record in"))
        record.settle("interrupted")
        self.assertEqual(1, record.report(folder))

    def test_close_still_does_all_three_steps_in_one_new_folder(self) -> None:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        self.assertEqual(0, record.close(["a"]))
        (folder,) = (self.root / "runs").iterdir()
        self.assertEqual("a\n", (folder / "log.txt").read_text())
        self.assertEqual("pass", json.loads((folder / "record.json").read_text())["outcome"])


class ClosedOutputTest(unittest.TestCase):
    """A record that is written while stdout is closed must say fail, so it never claims a pass that the run could not print."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        closed = io.StringIO()
        closed.close()
        for patcher in (mock.patch.object(run_record, "ROOT", self.root), mock.patch.object(sys, "stdout", closed)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_a_closed_stdout_gives_a_record_that_says_fail_and_exit_status_1(self) -> None:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        code = record.close([])
        (folder,) = (self.root / "runs").iterdir()
        written = json.loads((folder / "record.json").read_text())
        self.assertEqual(1, code)
        self.assertEqual("fail", written["outcome"])
        self.assertEqual(FAILED, json.loads((folder / "retention.json").read_text())["end_condition"])
        self.assertEqual(["output"], [item["step"] for item in written["observations"] if not item["passed"]])


class BrokenPipeTest(unittest.TestCase):
    """A write to a pipe whose reader is gone only fails when the buffer is flushed, so a flush that fails after the write rewrites the record to say fail."""

    def test_a_pipe_with_no_reader_gives_a_record_that_says_fail_and_exit_status_1(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name)
        read_end, write_end = os.pipe()
        os.close(read_end)
        stream = os.fdopen(write_end, "w", buffering=8192)
        with mock.patch.object(run_record, "ROOT", root), mock.patch.object(sys, "stdout", stream):
            record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
            record.expect("case", "step", "expected", "observed", True)
            code = record.close([])
        with contextlib.suppress(OSError):
            stream.close()
        (folder,) = (root / "runs").iterdir()
        written = json.loads((folder / "record.json").read_text())
        self.assertEqual(1, code)
        self.assertEqual("fail", written["outcome"])
        self.assertEqual(["output"], [item["step"] for item in written["observations"] if not item["passed"]])


class ReportAloneTest(unittest.TestCase):
    """A runner that writes its record and then calls ``report`` itself (Silver cases, Silver labels) must also save a failed print as a failed record."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        closed = io.StringIO()
        closed.close()
        for patcher in (mock.patch.object(run_record, "ROOT", self.root), mock.patch.object(sys, "stdout", closed)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def written(self, blocked: str) -> tuple[int, dict[str, Any], dict[str, Any]]:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle(blocked)
        folder = record.write(["a line"])
        code = record.report(folder)
        return code, json.loads((folder / "record.json").read_text()), json.loads((folder / "retention.json").read_text())

    def test_a_failed_print_after_write_gives_a_record_that_says_fail(self) -> None:
        code, written, label = self.written("")
        self.assertEqual((1, "fail", FAILED), (code, written["outcome"], label["end_condition"]))
        self.assertEqual(["output"], [item["step"] for item in written["observations"] if not item["passed"]])

    def test_a_stdout_that_python_set_to_none_gives_a_record_that_says_fail(self) -> None:
        """Recheck: a runner started with its standard output closed (``1>&-``) has ``sys.stdout`` set to ``None``."""
        with mock.patch.object(sys, "stdout", None):
            code, written, label = self.written("")
        self.assertEqual((1, "fail", FAILED), (code, written["outcome"], label["end_condition"]))

    def test_a_failed_print_keeps_the_blocked_reason(self) -> None:
        code, written, _ = self.written("interrupted")
        self.assertEqual((1, "blocked: interrupted"), (code, written["outcome"]))


if __name__ == "__main__":
    unittest.main()
