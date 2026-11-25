"""A Silver run saves every attachment first and writes ``record.json`` last, so that a failed save gives a failing record, never a passing one.

The shared helper is ``tests/fast/run_save.py``. The Silver cases and Silver signals runners (untracked, so their tests skip when they are not on this machine)
save through it, in the order that the Gold dimensions runner already has.

Failure modes covered here:

- A run wrote ``record.json`` with a passing verdict and then copied its script and its environment file into the run folder, so a failed copy left a folder
  whose record says pass although the run is incomplete.
- A failed write of the log, of an attachment or of the retention label gave a passing record, or no failing observation.
- A failed write of ``record.json`` left a folder without a record, or a record of another run, and the exit code was 0.
- The scan for a fixed-sample number beside a set name ran on the record only and not on the attachments, or its hit did not fail the record.
- A runner that has no scan (Silver hypotension and Silver reconcile) recorded "written and read back" without opening any saved file, so a script that was
  never written, or was cut short, passed.
- A retention label that could not be read back, or that held a hit, stayed with the end condition of a passing run beside a failing record.
- With a closed standard output, the reporter rewrote ``record.json`` before the label, and a failed rewrite left the original record that says pass.
- The Silver signals runner saved its script and its record with no scan for a fixed-sample number beside a set name.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.ingestion.notice import Code, Notice
from scripts.sample.candidates import CANDIDATES_FILE, CandidateError, read_candidates
from tests.e2e import run_record
from tests.e2e.run_record import Record
from tests.fast import run_save
from tests.fast.test_runner_gold_sealed import runner

SCRIPT = "print('a script')\n"


def attachments() -> dict[str, Callable[[], str]]:
    return {"run.json": lambda: json.dumps({"target": "local"}) + "\n", "run.py.txt": lambda: SCRIPT}


def trial() -> Record:
    record = Record(feature="a trial run", slug="trial", rerun="none")
    record.expect("a passing step", "control", "it holds", "it held", True)
    return record


def dropping(name: str) -> Callable[[Path, str], None]:
    """A write that reports success and writes nothing for ``name``, as a full disk or a lost folder can."""

    def dropped(path: Path, text: str) -> None:
        if path.name != name:
            path.write_text(text)

    return dropped


def cutting(name: str) -> Callable[[Path, str], None]:
    """A write that reports success and keeps only the first half of the text of ``name``."""

    def cut(path: Path, text: str) -> None:
        path.write_text(text[: len(text) // 2] if path.name == name else text)

    return cut


def breaking(name: str) -> Callable[[Path, str], None]:
    def broken(path: Path, text: str) -> None:
        if path.name.removesuffix(".part") == name:
            raise OSError("planted save failure")
        path.write_text(text)

    return broken


class SaveOrder(unittest.TestCase):
    def save(self, record: Record | None = None, **given: Any) -> tuple[int, Path, str]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="run_save_")))
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            code = run_save.save_run(record or trial(), ["a log line"], given.pop("attachments", attachments()), runs=folder, echo=False, **given)
        return code, folder, errors.getvalue()

    def stored(self, folder: Path) -> dict[str, Any]:
        found = sorted(folder.rglob("record.json"))
        self.assertEqual(1, len(found), [str(path) for path in folder.rglob("*")])
        return dict(json.loads(found[0].read_text()))

    def test_a_clean_save_writes_every_attachment_and_the_record_and_passes(self) -> None:
        code, folder, _ = self.save()
        self.assertEqual(0, code)
        stored = self.stored(folder)
        self.assertEqual("pass", stored["outcome"])
        names = {path.name for path in next(folder.iterdir()).iterdir()}
        self.assertEqual({"log.txt", "run.json", "run.py.txt", "retention.json", "record.json"}, names)
        self.assertEqual(SCRIPT, (next(folder.iterdir()) / "run.py.txt").read_text())

    def test_the_record_is_written_after_every_attachment_and_the_label(self) -> None:
        order: list[str] = []

        def spy(path: Path, text: str) -> None:
            order.append(path.name)
            path.write_text(text)

        self.save(write=spy)
        self.assertEqual("record.json.part", order[-1])
        self.assertEqual({"log.txt", "run.json", "run.py.txt", "retention.json"}, set(order[:-1]))

    def test_a_failed_attachment_the_log_or_the_label_gives_a_failing_record_and_a_nonzero_exit(self) -> None:
        for name in ("run.py.txt", "run.json", "log.txt", "retention.json"):
            with self.subTest(name):
                code, folder, _ = self.save(write=breaking(name))
                stored = self.stored(folder)
                self.assertNotEqual(0, code)
                self.assertEqual("fail", stored["outcome"])
                self.assertTrue(any(not item["passed"] for item in stored["observations"]))

    def test_a_script_that_cannot_be_read_gives_a_failing_record(self) -> None:
        def unreadable() -> str:
            raise OSError("planted read failure")

        code, folder, _ = self.save(attachments={"run.py.txt": unreadable})
        self.assertNotEqual(0, code)
        self.assertEqual("fail", self.stored(folder)["outcome"])

    def test_every_file_is_opened_and_compared_with_its_text_also_when_the_runner_has_no_scan(self) -> None:
        for name in ("log.txt", "run.json", "run.py.txt"):
            for label, write in (("dropped", dropping(name)), ("cut short", cutting(name))):
                with self.subTest(name=name, write=label):
                    code, folder, _ = self.save(write=write)
                    stored = self.stored(folder)
                    self.assertNotEqual(0, code)
                    self.assertEqual("fail", stored["outcome"])
                    saved = [item for item in stored["observations"] if item["case"] == "the run folder is saved and scanned"]
                    self.assertEqual([False], [item["passed"] for item in saved])

    def test_a_save_that_wrote_every_file_whole_passes_with_no_scan(self) -> None:
        code, folder, _ = self.save()
        self.assertEqual(0, code)
        self.assertEqual("pass", self.stored(folder)["outcome"])

    def test_a_record_that_cannot_be_written_leaves_no_record_and_a_nonzero_exit(self) -> None:
        code, folder, errors = self.save(write=breaking("record.json"))
        self.assertNotEqual(0, code)
        self.assertEqual([], sorted(folder.rglob("record.json")))
        self.assertEqual([], sorted(folder.rglob("record.json.part")))
        self.assertIn("record.json cannot be written", errors)

    def test_a_run_folder_that_cannot_be_made_gives_no_record_and_a_nonzero_exit(self) -> None:
        with tempfile.TemporaryDirectory(prefix="run_save_") as folder:
            blocker = Path(folder) / "runs"
            blocker.write_text("a file where the folder should be")
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                code = run_save.save_run(trial(), [], attachments(), runs=blocker, echo=False)
            self.assertNotEqual(0, code)
            self.assertIn("no run folder", errors.getvalue())

    def test_the_scan_reads_the_attachments_and_a_hit_fails_the_record(self) -> None:
        seen: list[str] = []

        def scan(texts: Any) -> int:
            found = list(texts)
            seen.extend(found)
            return int(any("planted number" in text for text in found))

        code, folder, _ = self.save(attachments={"run.py.txt": lambda: "a line with a planted number\n"}, scan=scan)
        self.assertNotEqual(0, code)
        stored = self.stored(folder)
        self.assertEqual("fail", stored["outcome"])
        self.assertTrue(any("planted number" in text for text in seen))
        self.assertIn("no identifier beside a set name", [item["case"] for item in stored["observations"]])

    def test_a_hit_in_the_retention_label_fails_the_record(self) -> None:
        def scan(texts: Any) -> int:
            return int(any("end_condition" in text for text in texts))

        code, folder, _ = self.save(scan=scan)
        self.assertNotEqual(0, code)
        stored = self.stored(folder)
        self.assertEqual("fail", stored["outcome"])
        self.assertEqual([False], [item["passed"] for item in stored["observations"] if item["case"] == "the retention label is written"])

    def test_a_clean_scan_passes_and_the_record_of_the_run_is_scanned_too(self) -> None:
        seen: list[str] = []

        def scan(texts: Any) -> int:
            seen.extend(texts)
            return 0

        code, folder, _ = self.save(scan=scan)
        self.assertEqual(0, code)
        self.assertEqual("pass", self.stored(folder)["outcome"])
        self.assertTrue(any("a trial run" in text for text in seen))

    def test_a_failing_run_stays_failing(self) -> None:
        record = trial()
        record.expect("a failing step", "control", "it holds", "it did not", False)
        code, folder, _ = self.save(record)
        self.assertNotEqual(0, code)
        self.assertEqual("fail", self.stored(folder)["outcome"])


class FinalFiles(unittest.TestCase):
    """The label and the record are written at the end by one guarded path: no file of the folder says pass when the record says fail."""

    def setUp(self) -> None:
        self.stderr = io.StringIO()
        self.enterContext(mock.patch.object(sys, "stderr", self.stderr))

    def save(self, *, scan: Any = None, write: Any = None, echo: bool = False, record: Record | None = None) -> tuple[int, Path]:
        folder = self.runs = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="run_save_")))
        self.enterContext(mock.patch.object(run_save, "ROOT", folder))
        self.enterContext(mock.patch.object(run_record, "ROOT", folder))
        code = run_save.save_run(record or trial(), ["a log line"], attachments(), scan=scan, runs=folder / "runs", write=write, echo=echo)
        return code, folder

    def files(self, folder: Path) -> dict[str, dict[str, Any]]:
        return {path.name: json.loads(path.read_text()) for path in folder.rglob("*.json") if path.name in ("record.json", "retention.json")}

    def claims_pass(self, folder: Path) -> list[str]:
        found = self.files(folder)
        return [name for name, document in found.items() if document.get("outcome") == "pass" or document.get("end_condition") == run_record.PASSED_END]

    def test_a_label_that_cannot_be_read_back_gives_a_failing_record_and_a_label_of_a_failed_run(self) -> None:
        real = Path.read_text

        def unreadable(path: Path, *arguments: Any, **keywords: Any) -> str:
            if path.name == "retention.json":
                raise OSError("planted read failure")
            return real(path, *arguments, **keywords)

        for label, scan in (("no scan", None), ("a scan", lambda texts: 0)):
            with self.subTest(label):
                with mock.patch.object(Path, "read_text", unreadable):
                    code, folder = self.save(scan=scan)
                found = self.files(folder)
                self.assertNotEqual(0, code)
                self.assertEqual("fail", found["record.json"]["outcome"])
                self.assertEqual(run_record.FAILED_END, found["retention.json"]["end_condition"])
                self.assertEqual([], self.claims_pass(folder))

    def test_a_label_that_holds_a_hit_stays_with_the_end_condition_of_a_failed_run(self) -> None:
        code, folder = self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)))
        self.assertNotEqual(0, code)
        self.assertEqual([], self.claims_pass(folder))
        self.assertEqual(run_record.FAILED_END, self.files(folder)["retention.json"]["end_condition"])

    def test_a_label_that_cannot_be_written_again_for_the_final_verdict_is_removed_and_the_record_says_fail(self) -> None:
        writes: list[str] = []

        def second_label_breaks(path: Path, text: str) -> None:
            writes.append(path.name.removesuffix(".part"))
            if writes[-1] == "retention.json" and writes.count("retention.json") > 1:
                raise OSError("planted write failure")
            path.write_text(text)

        code, folder = self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)), write=second_label_breaks)
        found = self.files(folder)
        self.assertNotEqual(0, code)
        self.assertEqual("fail", found["record.json"]["outcome"])
        self.assertNotIn("retention.json", found, "a label that cannot be made to say fail is removed")
        self.assertEqual([], self.claims_pass(folder))

    def test_a_label_part_file_that_cannot_replace_the_label_is_removed_with_the_label(self) -> None:
        real = os.replace

        def refuses_the_label(source: Any, target: Any) -> None:
            if Path(target).name == "retention.json" and str(source).endswith(".part"):
                raise OSError("planted replace failure")
            real(source, target)

        with mock.patch.object(os, "replace", refuses_the_label):
            code, folder = self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)))
        self.assertNotEqual(0, code)
        self.assertEqual([], sorted(path.name for path in folder.rglob("*.part")))
        self.assertEqual([], self.claims_pass(folder))
        self.assertNotIn("retention.json", self.files(folder))

    def test_a_closed_output_gives_a_failing_record_and_a_failing_label_and_exit_1(self) -> None:
        closed = io.StringIO()
        closed.close()
        with mock.patch.object(sys, "stdout", closed):
            code, folder = self.save(echo=True)
        found = self.files(folder)
        self.assertEqual(1, code)
        self.assertEqual("fail", found["record.json"]["outcome"])
        self.assertEqual(run_record.FAILED_END, found["retention.json"]["end_condition"])

    def test_a_closed_output_and_a_failed_rewrite_leave_no_file_that_says_pass_and_raise_nothing(self) -> None:
        closed = io.StringIO()
        closed.close()
        for failing in ("record.json", "retention.json"):
            with self.subTest(failing), mock.patch.object(sys, "stdout", closed):
                writes: list[str] = []

                def breaks(path: Path, text: str, failing: str = failing, writes: list[str] = writes) -> None:
                    writes.append(path.name.removesuffix(".part"))
                    if writes[-1] == failing and writes.count(failing) > 1:
                        raise OSError("planted write failure")
                    path.write_text(text)

                code, folder = self.save(echo=True, write=breaks)
                self.assertEqual(1, code)
                self.assertEqual([], self.claims_pass(folder))

    def test_the_label_is_written_before_the_record_in_the_rewrite_after_a_failed_print(self) -> None:
        closed = io.StringIO()
        closed.close()
        order: list[str] = []

        def spy(path: Path, text: str) -> None:
            order.append(path.name.removesuffix(".part"))
            path.write_text(text)

        with mock.patch.object(sys, "stdout", closed):
            self.save(echo=True, write=spy)
        self.assertEqual(["retention.json", "record.json"], order[-2:])
        self.assertEqual(2, order.count("record.json"), "the rewrite goes through the same write as the first save")

    def test_an_interrupt_in_the_label_rewrite_leaves_no_label_that_says_pass(self) -> None:
        real = os.replace
        replaced: list[str] = []

        def stop_at_the_label_rewrite(source: Any, target: Any) -> None:
            replaced.append(Path(target).name)
            if replaced.count("retention.json") > 0:
                raise KeyboardInterrupt
            real(source, target)

        with mock.patch.object(os, "replace", stop_at_the_label_rewrite), self.assertRaises(KeyboardInterrupt):
            self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)))
        folders = list((self.runs / "runs").iterdir())
        self.assertEqual(1, len(folders))
        self.assertEqual([], self.claims_pass(self.runs))
        self.assertEqual([], sorted(path.name for path in self.runs.rglob("*.part")))

    def test_an_interrupt_while_a_file_is_staged_leaves_no_part_file(self) -> None:
        def written_then_stopped(path: Path, text: str) -> None:
            path.write_text(text)
            if path.name == "record.json.part":
                raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.save(write=written_then_stopped)
        self.assertEqual([], sorted(path.name for path in self.runs.rglob("*.part")))
        self.assertEqual([], self.claims_pass(self.runs))

    def test_a_part_file_that_is_written_and_then_interrupted_is_removed_by_the_stage_itself(self) -> None:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="run_save_")))

        def written_then_stopped(path: Path, text: str) -> None:
            path.write_text(text)
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_save.stage(folder / "record.json", "{}", written_then_stopped)
        self.assertEqual([], sorted(path.name for path in folder.iterdir()))

    def test_a_run_folder_that_cannot_be_removed_is_not_reported_as_gone(self) -> None:
        with mock.patch.object(shutil, "rmtree"):
            code, folder = self.save(write=breaking("record.json"))
        self.assertNotEqual(0, code)
        self.assertTrue(any(folder.rglob("log.txt")), "the test needs the folder to stay")
        self.assertNotIn("so there is no record", self.stderr.getvalue())
        self.assertIn("cannot be removed", self.stderr.getvalue())

    def test_a_run_folder_that_is_removed_is_reported_as_gone_after_it_is_checked(self) -> None:
        code, folder = self.save(write=breaking("record.json"))
        self.assertNotEqual(0, code)
        self.assertEqual([], list(folder.rglob("log.txt")))
        self.assertIn("so there is no record", self.stderr.getvalue())

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_a_run_folder_behind_an_unsearchable_folder_is_not_reported_as_gone(self) -> None:
        runs = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="run_save_")))
        folder = runs / "run"
        folder.mkdir()
        (folder / "record.json").write_text('{"outcome": "pass"}')
        runs.chmod(0)
        self.addCleanup(runs.chmod, 0o755)
        self.assertEqual(1, run_save.remove_run(folder, trial(), "record.json cannot be written"))
        self.assertNotIn("so there is no record", self.stderr.getvalue())
        self.assertIn("cannot be removed or checked", self.stderr.getvalue())

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_a_label_that_cannot_be_checked_after_its_removal_fails_removes_the_run_and_says_what_is_true(self) -> None:
        stopped: list[Path] = []

        def put(path: Path, text: str) -> None:
            if path.name == "retention.json.part":
                path.parent.chmod(0)
                self.addCleanup(path.parent.chmod, 0o755)
                stopped.append(path.parent)
                raise OSError("planted failure of the label rewrite")
            path.write_text(text)

        code, _ = self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)), write=put)
        self.assertEqual(1, code)
        self.assertTrue(stopped, "the test needs the label to be written again")
        self.assertIn("the retention label cannot be made to say fail", self.stderr.getvalue())
        self.assertNotIn("so there is no record", self.stderr.getvalue())

    def test_a_closed_output_and_a_record_that_cannot_be_removed_keep_it_and_the_message_says_so(self) -> None:
        closed = io.StringIO()
        closed.close()
        with mock.patch.object(sys, "stdout", closed), mock.patch.object(Path, "unlink", side_effect=OSError("planted unlink failure")):
            code, folder = self.save(echo=True)
        self.assertEqual(1, code)
        self.assertTrue(any(folder.rglob("record.json")), "the test needs the record to stay")
        self.assertNotIn("so there is no record", self.stderr.getvalue())
        self.assertIn("cannot be removed", self.stderr.getvalue())

    def test_the_label_is_removed_before_it_is_written_again(self) -> None:
        seen: list[bool] = []
        real = run_save.stage

        def spy(path: Path, text: str, put: Any) -> None:
            if path.name == "retention.json" and path.exists():
                seen.append(True)
            real(path, text, put)

        with mock.patch.object(run_save, "stage", spy):
            self.save(scan=lambda texts: int(any("end_condition" in text for text in texts)))
        self.assertEqual([], seen, "a label that is written again is not there any more when it is staged")


class Leaks(unittest.TestCase):
    """``run_save.leaks`` counts the lines that hold a fixed-sample case or patient number (alone) and a set name: the scan that the Silver runners share."""

    def test_a_line_with_a_number_and_a_set_name_is_counted(self) -> None:
        self.assertEqual(1, run_save.leaks(["case 908 is in the test set\nno number here, test\n"], {"908"}))
        self.assertEqual(2, run_save.leaks(["908 training", "a validation line with 908"], {"908"}))

    def test_a_number_inside_a_longer_number_or_word_or_a_line_without_a_set_name_is_not_counted(self) -> None:
        self.assertEqual(0, run_save.leaks(["case 9081 is in the test set", "case 1908 is in the test set", "case x908 in training"], {"908"}))
        self.assertEqual(0, run_save.leaks(["vital_files/0908.vital is in the test set", "case 908 is a development case"], {"908"}))
        self.assertEqual(0, run_save.leaks(["case 908 testing", "contest 908"], {"908"}))

    def test_every_text_is_read_and_no_number_gives_no_hit(self) -> None:
        self.assertEqual(1, run_save.leaks(["clean", "908 validation"], {"908"}))
        self.assertEqual(0, run_save.leaks(["908 validation"], set()))


class ReadCandidatesOnce(unittest.TestCase):
    """``run_save.read_candidates_once`` reads the candidate list once for a runner and gives it, or a failing observation and None."""

    def test_a_good_read_gives_the_list_and_adds_no_observation(self) -> None:
        record = trial()
        items = [SimpleNamespace(case=1, patient=2), SimpleNamespace(case=3, patient=2)]
        self.assertEqual(items, run_save.read_candidates_once(record, lambda: items))
        self.assertEqual(1, len(record.observations), "a good read adds no observation")

    def test_a_read_is_made_once(self) -> None:
        calls: list[int] = []

        def read() -> list[Any]:
            calls.append(1)
            return []

        run_save.read_candidates_once(trial(), read)
        self.assertEqual([1], calls)

    def test_a_failed_read_gives_none_and_one_failing_observation_that_names_the_error_class_only(self) -> None:
        errors = (
            RuntimeError("path /secret/place"),
            ValueError("line 4"),
            OSError("disk"),
            CandidateError(Notice(Code.CANDIDATE_HEADER)),
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "secret reason"),
        )
        for error in errors:
            with self.subTest(type(error).__name__):
                record = trial()

                def failing(error: Exception = error) -> list[Any]:
                    raise error

                self.assertIsNone(run_save.read_candidates_once(record, failing))
                failed = [item for item in record.observations if not item.passed]
                self.assertEqual(["the fixed-sample candidate list is read"], [item.case for item in failed])
                self.assertIn(type(error).__name__, failed[0].observed)
                self.assertNotIn("secret", failed[0].observed)

    def test_an_error_that_is_not_an_i_o_or_value_error_is_not_hidden(self) -> None:
        def failing() -> list[Any]:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_save.read_candidates_once(trial(), failing)

    def test_the_numbers_are_the_case_and_patient_numbers_as_text(self) -> None:
        found = run_save.numbers_of([SimpleNamespace(case=1, patient=2), SimpleNamespace(case=3, patient=2)])
        self.assertEqual({"1", "2", "3"}, found)
        self.assertEqual(set(), run_save.numbers_of([]))


class SilverRunners(unittest.TestCase):
    """The main function of each Silver runner saves through the helper: a failed copy of its script gives a failing record."""

    CASES = (
        ("silver_cases", "silver_cases.py.txt", "silver_cases.json"),
        ("silver_signals", "silver_signals.py.txt", "silver_signals.json"),
        ("silver_patient_split", "silver_patient_split.py.txt", "silver_patient_split.json"),
        ("silver_hypotension", "silver_hypotension.py.txt", "silver_hypotension.json"),
        ("silver_reconcile", "silver_reconcile.py.txt", "silver_reconcile.json"),
    )
    # What each ``main`` reads before its run: the names that a test replaces, as ``create=True`` patches that a runner without the name ignores.
    READS: dict[str, dict[str, Any]] = {
        "silver_patient_split": {"read_candidates": lambda _: []},
        "silver_hypotension": {"folder_of": lambda *_: Path("."), "stand_in_defined": lambda _: True},
    }

    def main(
        self,
        name: str,
        write: Callable[[Path, str], None] | None,
        reads: Callable[[Path], Any] | None = None,
        ran: list[str] | None = None,
        *,
        dry: bool = False,
    ) -> tuple[int, Path]:
        """Run ``main`` of a runner with its container run stood in. ``ran`` gets the name when the run starts, ``self.given`` the arguments of the run, and
        ``self.planned`` the arguments of each dry plan; with ``dry`` the runner's own dry plan runs."""
        module = runner(name)
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_main_")))
        self.given: list[tuple[Any, ...]] = []
        self.planned: list[tuple[Any, ...]] = []

        def run_local(record: Record, *arguments: Any, **__: Any) -> None:
            self.given.append(arguments)
            if ran is not None:
                ran.append(name)
            record.expect("a passing step", "control", "it holds", "it held", True)

        def planned(*arguments: Any) -> list[str]:
            self.planned.append(arguments)
            return []

        patches: list[Any] = [
            mock.patch.object(module, "ROOT", folder),
            mock.patch.object(module, "recorded_state", lambda _: ("revision", "0" * 64), create=True),
            mock.patch.object(module, "checkout_state", lambda *_: ("revision", "0" * 64), create=True),
            mock.patch.object(module, "run_local", run_local),
            mock.patch.object(module, "stand_in_defined", lambda _: True, create=True),
            *([] if dry else [mock.patch.object(module, "planned_reference_names", planned, create=True)]),
            *(mock.patch.object(module, key, value, create=True) for key, value in self.READS.get(name, {}).items()),
            *([mock.patch.object(module, "read_candidates", reads)] if reads is not None else []),
        ]
        if write is not None:
            patches.append(mock.patch.object(run_save, "write_text", write))
        with contextlib.ExitStack() as stack, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            for patch in patches:
                stack.enter_context(patch)
            code = module.main()
        return code, folder

    def stored(self, folder: Path) -> dict[str, Any]:
        found = sorted(folder.rglob("record.json"))
        self.assertEqual(1, len(found), [str(path) for path in folder.rglob("*")])
        return dict(json.loads(found[0].read_text()))

    def test_a_clean_run_saves_its_environment_file_and_its_script_and_passes(self) -> None:
        for name, script, environment in self.CASES:
            with self.subTest(name):
                code, folder = self.main(name, None)
                self.assertEqual(0, code)
                self.assertEqual("pass", self.stored(folder)["outcome"])
                saved = {path.name for path in folder.rglob("*") if path.is_file()}
                self.assertLessEqual({script, environment, "log.txt", "retention.json", "record.json"}, saved)

    def test_the_runners_with_a_scan_scan_what_they_save_and_a_hit_fails_their_record(self) -> None:
        for name in ("silver_cases", "silver_signals", "silver_patient_split"):
            with self.subTest(name):
                module = runner(name)
                seen: list[str] = []
                with mock.patch.object(module, "leaks", lambda texts, numbers, into=seen: into.extend(texts) or 1):
                    code, folder = self.main(name, None)
                self.assertNotEqual(0, code)
                self.assertEqual("fail", self.stored(folder)["outcome"])
                self.assertTrue(any("def main" in text for text in seen), "the copy of the script is scanned")

    def test_a_candidate_list_that_cannot_be_read_gives_a_failing_record_and_no_raised_error_and_no_run(self) -> None:
        malformed = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_header_"))) / "candidates.tsv"
        malformed.write_text("not\tthe\theader\n1\t2\n")
        for label, path in (("missing", Path("/nonexistent/candidates.tsv")), ("malformed header", malformed)):
            for name in ("silver_cases", "silver_signals", "silver_patient_split"):
                with self.subTest(label, runner=name):
                    ran: list[str] = []

                    def unreadable(_: Path, path: Path = path) -> list[Any]:
                        return read_candidates(path)

                    scanned: list[object] = []
                    with mock.patch.object(runner(name), "leaks", lambda texts, numbers, into=scanned: into.append(numbers) or 0):
                        code, folder = self.main(name, None, unreadable, ran)
                    stored = self.stored(folder)
                    self.assertEqual([], scanned, "no scan over an empty set of numbers is recorded as passed: the scan is unavailable")
                    self.assertNotIn("no identifier beside a set name", [item["case"] for item in stored["observations"]])
                    self.assertEqual(1, code)
                    self.assertEqual("fail", stored["outcome"])
                    self.assertEqual([], ran, "a run that cannot scan for identifiers is not started")
                    self.assertEqual([], self.planned, "no dry plan is made from a list that was not read")
                    failed = [item for item in stored["observations"] if not item["passed"]]
                    self.assertEqual(["the fixed-sample candidate list is read"], [item["case"] for item in failed])
                    self.assertNotIn(str(path), json.dumps(stored), "the record names the error class, never a path")
                    self.assertIn("CandidateError" if label == "malformed header" else "Error", failed[0]["observed"])

    def test_a_malformed_header_with_the_real_dry_plan_gives_a_failing_record_and_no_error(self) -> None:
        malformed = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_header_"))) / "candidates.tsv"
        malformed.write_text("not\tthe\theader\n")
        code, folder = self.main("silver_cases", None, lambda _: read_candidates(malformed), dry=True)
        stored = self.stored(folder)
        self.assertEqual(1, code)
        self.assertEqual("fail", stored["outcome"])
        self.assertEqual(["the fixed-sample candidate list is read"], [item["case"] for item in stored["observations"] if not item["passed"]])

    def test_each_runner_calls_the_reader_once_on_a_full_dry_path_and_hands_the_list_on(self) -> None:
        for name in ("silver_cases", "silver_signals", "silver_patient_split"):
            with self.subTest(name):
                reads: list[Path] = []
                tracked = read_candidates(CANDIDATES_FILE)

                def reader(path: Path, reads: list[Path] = reads, tracked: list[Any] = tracked) -> list[Any]:
                    reads.append(path)
                    return tracked

                code, _ = self.main(name, None, reader, dry=True)
                self.assertEqual(0, code)
                self.assertEqual([CANDIDATES_FILE], reads, f"{name} reads the list once")
                (given,) = self.given
                self.assertTrue(any(isinstance(item, list) and item == tracked for item in given), "the run gets the list that main read")

    def test_the_dry_plan_gets_the_list_that_main_read(self) -> None:
        tracked = read_candidates(CANDIDATES_FILE)
        self.main("silver_cases", None, lambda _: tracked)
        (arguments,) = self.planned
        self.assertTrue(any(isinstance(item, list) and item == tracked for item in arguments))

    def test_the_candidate_list_is_read_by_one_call_in_main_and_by_no_other_function(self) -> None:
        for name in ("silver_cases", "silver_signals", "silver_patient_split"):
            with self.subTest(name):
                tree = ast.parse(Path(runner(name).__file__ or "").read_text())
                found: list[str] = []
                for function in (node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
                    for call in ast.walk(function):
                        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "read_candidates":
                            found.append(function.name)
                self.assertEqual(["main"], found)

    def test_a_run_object_takes_its_candidates_and_never_reads_the_list(self) -> None:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_run_")))
        (folder / "clinical.csv").write_text("caseid\n1\n")
        candidates = [SimpleNamespace(case=11, patient=22, slot="a_development")]
        for name, last in (("silver_cases", (Path("clinical"), Path("lab"))), ("silver_patient_split", folder / "clinical.csv")):
            with self.subTest(name):
                module = runner(name)
                with (
                    mock.patch.object(module, "read_candidates", side_effect=AssertionError("the list is read again")),
                    mock.patch.object(module, "expected_from_csv", lambda _: ({}, {}), create=True),
                ):
                    run = module.Run(SimpleNamespace(), SimpleNamespace(), trial(), last, candidates)
                self.assertIs(candidates, run.candidates)
                self.assertEqual({"11", "22"}, run.numbers)

    def test_the_candidate_list_is_read_once_by_main_and_the_identifiers_are_the_ones_that_the_scan_gets(self) -> None:
        for name in ("silver_cases", "silver_signals", "silver_patient_split"):
            with self.subTest(name):
                module = runner(name)
                reads: list[Path] = []
                scanned: list[set[str]] = []

                def reader(path: Path, reads: list[Path] = reads) -> list[Any]:
                    reads.append(path)
                    return [SimpleNamespace(case=111, patient=222), SimpleNamespace(case=333, patient=444)]

                def leaks_of(texts: Any, numbers: set[str], scanned: list[set[str]] = scanned) -> int:
                    scanned.append(set(numbers))
                    return 0

                with mock.patch.object(module, "leaks", leaks_of):
                    code, _ = self.main(name, None, reader)
                self.assertEqual(0, code)
                self.assertEqual(1, len(reads))
                self.assertTrue(scanned)
                self.assertEqual([{"111", "222", "333", "444"}] * len(scanned), scanned)

    def test_a_script_or_an_environment_file_that_is_not_written_gives_a_failing_record_in_every_runner(self) -> None:
        for name, script, environment in self.CASES:
            for failing in (script, environment):
                with self.subTest(name=name, failing=failing):
                    code, folder = self.main(name, dropping(failing))
                    self.assertNotEqual(0, code)
                    self.assertEqual("fail", self.stored(folder)["outcome"])

    def test_a_failed_copy_of_the_script_or_the_environment_file_gives_a_failing_record(self) -> None:
        for name, script, environment in self.CASES:
            for failing in (script, environment):
                with self.subTest(name=name, failing=failing):
                    code, folder = self.main(name, breaking(failing))
                    self.assertNotEqual(0, code)
                    self.assertEqual("fail", self.stored(folder)["outcome"])
