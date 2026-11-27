"""The retention label that every run record folder carries: class, purpose, creation time and end condition.

Failure modes covered here:

- A run folder has no ``retention.json``, so nothing says why it is kept or when it goes.
- The label lacks a required field, or holds a class that is not an integer from 1 to 9, or a time that is not UTC ISO 8601.
- A failed or blocked run gets the end condition of a passing run, so its folder is swept before its failure is fixed.
- A record that is written again in its folder (after a scan that came later) keeps the earlier outcome and label, or makes a second folder, or prints the
  outcome before the final verdict.
- A failed print is followed by a rewrite that fails: the original record (and its label) that says pass stays in the folder, or the rewrite raises.
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


class FailedRewriteTest(unittest.TestCase):
    """A print that fails is followed by a rewrite of the record and its label. When that rewrite fails, no file of the folder may say pass."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        closed = io.StringIO()
        closed.close()
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.root),
            mock.patch.object(sys, "stdout", closed),
            mock.patch.object(sys, "stderr", io.StringIO()),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def claims_pass(self, folder: Path) -> list[str]:
        """The files of the folder that say pass: a record whose outcome is pass, a label with the end condition of a passing run."""
        found = []
        if (folder / "record.json").exists() and json.loads((folder / "record.json").read_text())["outcome"] == "pass":
            found.append("record.json")
        if (folder / "retention.json").exists() and json.loads((folder / "retention.json").read_text())["end_condition"] == PASSED:
            found.append("retention.json")
        return found

    def written(self) -> tuple[run_record.Record, Path]:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle("")
        return record, record.write(["a line"])

    def report_with_failing(self, failing: tuple[str, ...]) -> tuple[int, Path]:
        record, folder = self.written()
        real = Path.write_text

        def broken(path: Path, data: str, *arguments: Any, **keywords: Any) -> int:
            if path.name.removesuffix(".part") in failing:
                raise OSError("planted write failure")
            return real(path, data, *arguments, **keywords)

        with mock.patch.object(Path, "write_text", broken):
            return record.report(folder), folder

    def test_a_rewrite_of_the_record_that_fails_leaves_no_file_that_says_pass_and_the_report_does_not_raise(self) -> None:
        code, folder = self.report_with_failing(("record.json",))
        self.assertEqual(1, code)
        self.assertEqual([], self.claims_pass(folder))

    def test_a_rewrite_of_the_label_that_fails_leaves_no_file_that_says_pass(self) -> None:
        code, folder = self.report_with_failing(("retention.json",))
        self.assertEqual(1, code)
        self.assertEqual([], self.claims_pass(folder))

    def test_both_rewrites_that_fail_leave_no_file_that_says_pass(self) -> None:
        code, folder = self.report_with_failing(("record.json", "retention.json"))
        self.assertEqual(1, code)
        self.assertEqual([], self.claims_pass(folder))

    def test_the_label_is_replaced_before_the_record_so_that_a_stop_between_them_leaves_the_label_of_a_failed_run(self) -> None:
        record, folder = self.written()
        replaced: list[str] = []
        real = os.replace

        def spy(source: Any, target: Any) -> None:
            replaced.append(Path(target).name)
            real(source, target)

        with mock.patch.object(os, "replace", spy):
            record.report(folder)
        self.assertEqual(["retention.json", "record.json"], replaced)

    def test_a_part_file_that_is_written_but_cannot_replace_its_file_is_removed(self) -> None:
        record, folder = self.written()
        with mock.patch.object(os, "replace", side_effect=OSError("planted replace failure")):
            self.assertEqual(1, record.report(folder))
        self.assertEqual([], sorted(path.name for path in folder.iterdir() if path.name.endswith(".part")))
        self.assertEqual([], self.claims_pass(folder))

    def test_a_failed_rewrite_leaves_no_part_file(self) -> None:
        for failing in (("record.json",), ("retention.json",)):
            with self.subTest(failing):
                _, folder = self.report_with_failing(failing)
                self.assertEqual([], sorted(path.name for path in folder.iterdir() if path.name.endswith(".part")))

    def test_a_rewrite_that_works_leaves_a_failing_record_and_a_failing_label_and_no_part_file(self) -> None:
        code, folder = self.report_with_failing(())
        self.assertEqual(1, code)
        self.assertEqual([], self.claims_pass(folder))
        self.assertEqual("fail", json.loads((folder / "record.json").read_text())["outcome"])
        self.assertEqual(FAILED, json.loads((folder / "retention.json").read_text())["end_condition"])
        self.assertEqual([], sorted(path.name for path in folder.iterdir() if path.name.endswith(".part")))

    def test_a_given_rewrite_replaces_the_default_one_and_its_failure_is_handled_the_same_way(self) -> None:
        record, folder = self.written()

        def rewrite(_: Path) -> None:
            raise OSError("planted failure of the caller's own rewrite")

        self.assertEqual(1, record.report(folder, rewrite))
        self.assertEqual([], self.claims_pass(folder))
        calls: list[Path] = []
        record, folder = self.written()
        self.assertEqual(1, record.report(folder, calls.append))
        self.assertEqual([folder], calls)

    def test_a_clean_print_never_calls_the_rewrite(self) -> None:
        record, folder = self.written()
        calls: list[Path] = []
        with mock.patch.object(sys, "stdout", io.StringIO()):
            self.assertEqual(0, record.report(folder, calls.append))
        self.assertEqual([], calls)


class VerifiedVerdictTest(unittest.TestCase):
    """A record that cannot be brought to its final verdict must not keep an earlier pass, and the reporter must say only what is true about the folder.

    A closed stdout and a folder that cannot be changed leave the earlier record in place: the message says so and does not claim that the record is gone. An
    interrupt in the middle of the rewrite leaves no earlier pass beside a label that says fail, and no part file.
    """

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="run_record_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        closed = io.StringIO()
        closed.close()
        self.errors = io.StringIO()
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.root),
            mock.patch.object(sys, "stdout", closed),
            mock.patch.object(sys, "stderr", self.errors),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def written(self) -> tuple[run_record.Record, Path]:
        record = run_record.Record(feature="a planted feature", slug="slug", rerun="rerun")
        record.expect("case", "step", "expected", "observed", True)
        record.settle("")
        return record, record.write(["a line"])

    def claims_pass(self, folder: Path) -> list[str]:
        found = []
        if (folder / "record.json").exists() and json.loads((folder / "record.json").read_text())["outcome"] == "pass":
            found.append("record.json")
        if (folder / "retention.json").exists() and json.loads((folder / "retention.json").read_text())["end_condition"] == PASSED:
            found.append("retention.json")
        return found

    def parts(self, folder: Path) -> list[str]:
        return sorted(path.name for path in folder.iterdir() if path.name.endswith(".part"))

    @unittest.skipIf(os.geteuid() == 0, "root can write into a folder that is made read only")
    def test_a_folder_that_cannot_be_changed_keeps_the_record_and_the_message_does_not_say_that_the_record_is_gone(self) -> None:
        record, folder = self.written()
        folder.chmod(0o555)
        self.addCleanup(folder.chmod, 0o755)
        code = record.report(folder)
        self.assertEqual(1, code)
        self.assertTrue((folder / "record.json").exists(), "the test needs the record to stay")
        message = self.errors.getvalue()
        self.assertNotIn("so there is no record", message)
        self.assertIn("cannot be removed", message)
        self.assertIn("pass", message)

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_a_record_behind_an_unsearchable_folder_is_not_reported_as_gone(self) -> None:
        record, folder = self.written()
        folder.chmod(0)
        self.addCleanup(folder.chmod, 0o755)
        self.assertEqual(1, record.report(folder))
        message = self.errors.getvalue()
        self.assertNotIn("so there is no record", message)
        self.assertIn("cannot be removed or checked", message)
        self.assertIn("pass", message)

    def test_a_removal_that_fails_is_reported_as_a_removal_that_failed(self) -> None:
        record, folder = self.written()
        with mock.patch.object(Path, "unlink", side_effect=OSError("planted unlink failure")):
            self.assertEqual(1, record.report(folder))
        self.assertTrue((folder / "record.json").exists())
        self.assertNotIn("so there is no record", self.errors.getvalue())
        self.assertIn("cannot be removed", self.errors.getvalue())

    def test_a_record_that_is_removed_is_reported_as_gone_after_the_files_are_checked(self) -> None:
        record, folder = self.written()
        with mock.patch.object(Path, "write_text", side_effect=OSError("planted write failure")):
            self.assertEqual(1, record.report(folder))
        self.assertEqual([], [path.name for path in folder.iterdir() if path.name in ("record.json", "retention.json")])
        self.assertIn("so there is no record", self.errors.getvalue())

    def test_an_interrupt_in_the_rewrite_leaves_no_file_that_says_pass_and_no_part_file_and_goes_on(self) -> None:
        real = os.replace
        for stopped in ("retention.json", "record.json"):
            with self.subTest(stopped):
                record, folder = self.written()

                def stop(source: Any, target: Any, stopped: str = stopped) -> None:
                    if Path(target).name == stopped:
                        raise KeyboardInterrupt
                    real(source, target)

                with mock.patch.object(os, "replace", stop), self.assertRaises(KeyboardInterrupt):
                    record.report(folder)
                self.assertEqual([], self.claims_pass(folder))
                self.assertEqual([], self.parts(folder))

    def test_an_interrupt_while_a_file_is_written_leaves_no_part_file(self) -> None:
        record, folder = self.written()
        record.settle("interrupted")
        real = Path.write_text

        def written_then_stopped(path: Path, data: str, *arguments: Any, **keywords: Any) -> int:
            real(path, data, *arguments, **keywords)
            raise KeyboardInterrupt

        with mock.patch.object(Path, "write_text", written_then_stopped), self.assertRaises(KeyboardInterrupt):
            record.write_record(folder)
        self.assertEqual([], self.parts(folder))

    def test_the_record_that_is_written_again_in_its_folder_never_leaves_the_earlier_pass_beside_a_label_that_says_fail(self) -> None:
        real = os.replace
        record, folder = self.written()
        record.expect("later case", "control", "expected", "observed", False)
        record.settle("")

        def stop(source: Any, target: Any) -> None:
            if Path(target).name == "record.json":
                raise KeyboardInterrupt
            real(source, target)

        with mock.patch.object(os, "replace", stop), self.assertRaises(KeyboardInterrupt):
            record.write([], folder)
        self.assertEqual([], self.claims_pass(folder))
        self.assertEqual([], self.parts(folder))

    def test_a_custom_rewrite_that_is_interrupted_is_handled_like_the_default_one(self) -> None:
        record, folder = self.written()

        def rewrite(_: Path) -> None:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            record.report(folder, rewrite)
        self.assertEqual([], self.claims_pass(folder))

    def test_the_earlier_verdict_is_removed_before_the_rewrite_starts(self) -> None:
        record, folder = self.written()
        seen: list[bool] = []
        record.report(folder, lambda again: seen.append((again / "record.json").exists() or (again / "retention.json").exists()))
        self.assertEqual([False], seen)

    def test_a_stray_part_file_of_an_earlier_run_is_removed_before_the_rewrite(self) -> None:
        record, folder = self.written()
        for name in ("record.json.part", "retention.json.part"):
            (folder / name).write_text("stray")
        seen: list[list[str]] = []
        record.report(folder, lambda again: seen.append(self.parts(again)))
        self.assertEqual([[]], seen)

    def test_an_interrupted_rewrite_goes_on_and_leaves_no_file_that_says_pass(self) -> None:
        record, folder = self.written()

        def rewrite(again: Path) -> None:
            (again / "retention.json").write_text(json.dumps({"end_condition": FAILED}))
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            record.report(folder, rewrite)
        self.assertEqual([], self.claims_pass(folder))


class ProvenAbsent(unittest.TestCase):
    """``proven_absent`` is True only when the path is shown not to be there: a check that fails is not a removal that worked."""

    def setUp(self) -> None:
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="proven_absent_")))

    def test_a_missing_name_is_proven_absent(self) -> None:
        self.assertIs(True, run_record.proven_absent(self.folder / "record.json"))

    def test_a_name_under_a_file_is_proven_absent(self) -> None:
        (self.folder / "file").write_text("x")
        self.assertIs(True, run_record.proven_absent(self.folder / "file" / "record.json"))

    def test_a_file_a_folder_and_a_broken_link_are_not_absent(self) -> None:
        (self.folder / "record.json").write_text("{}")
        (self.folder / "sub").mkdir()
        (self.folder / "link").symlink_to(self.folder / "nowhere")
        for name in ("record.json", "sub", "link"):
            with self.subTest(name):
                self.assertIs(False, run_record.proven_absent(self.folder / name))

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_a_name_behind_an_unsearchable_folder_is_not_proven_absent(self) -> None:
        (self.folder / "record.json").write_text("{}")
        self.folder.chmod(0)
        self.addCleanup(self.folder.chmod, 0o755)
        self.assertIs(False, run_record.proven_absent(self.folder / "record.json"))
        self.assertIs(False, run_record.proven_absent(self.folder / "other.json"))

    def test_any_other_error_is_not_proven_absent(self) -> None:
        for error in (PermissionError("denied"), OSError("i/o"), TimeoutError("slow")):
            with self.subTest(type(error).__name__), mock.patch.object(Path, "lstat", side_effect=error):
                self.assertIs(False, run_record.proven_absent(self.folder / "record.json"))

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_remove_verified_lists_every_name_when_the_folder_cannot_be_searched(self) -> None:
        (self.folder / "record.json").write_text('{"outcome": "pass"}')
        self.folder.chmod(0)
        self.addCleanup(self.folder.chmod, 0o755)
        self.assertEqual(list(run_record.VERDICT_FILES), run_record.remove_verified(self.folder))

    def test_remove_verified_gives_nothing_when_every_name_is_gone(self) -> None:
        (self.folder / "record.json").write_text("{}")
        self.assertEqual([], run_record.remove_verified(self.folder))
        self.assertFalse((self.folder / "record.json").exists())


if __name__ == "__main__":
    unittest.main()
