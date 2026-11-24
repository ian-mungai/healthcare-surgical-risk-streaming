"""A Silver run saves every attachment first and writes ``record.json`` last, so that a failed save gives a failing record, never a passing one.

The shared helper is ``tests/fast/run_save.py``. The Silver cases and Silver signals runners (untracked, so their tests skip when they are not on this machine)
save through it, in the order that the Gold dimensions runner already has.

Failure modes covered here:

- A run wrote ``record.json`` with a passing verdict and then copied its script and its environment file into the run folder, so a failed copy left a folder
  whose record says pass although the run is incomplete.
- A failed write of the log, of an attachment or of the retention label gave a passing record, or no failing observation.
- A failed write of ``record.json`` left a folder without a record, or a record of another run, and the exit code was 0.
- The scan for a fixed-sample number beside a set name ran on the record only and not on the attachments, or its hit did not fail the record.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

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

    def main(self, name: str, write: Callable[[Path, str], None] | None) -> tuple[int, Path]:
        module = runner(name)
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_main_")))

        def run_local(record: Record, *_: Any, **__: Any) -> None:
            record.expect("a passing step", "control", "it holds", "it held", True)

        patches: list[Any] = [
            mock.patch.object(module, "ROOT", folder),
            mock.patch.object(module, "recorded_state", lambda _: ("revision", "0" * 64)),
            mock.patch.object(module, "run_local", run_local),
            mock.patch.object(module, "stand_in_defined", lambda _: True, create=True),
            mock.patch.object(module, "planned_reference_names", lambda: [], create=True),
            *(mock.patch.object(module, key, value, create=True) for key, value in self.READS.get(name, {}).items()),
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

    def test_the_cases_runner_scans_what_it_saves_and_a_hit_fails_its_record(self) -> None:
        module = runner("silver_cases")
        seen: list[str] = []

        def leaks(texts: Any, numbers: set[str]) -> int:
            seen.extend(texts)
            return 1

        with mock.patch.object(module, "leaks", leaks):
            code, folder = self.main("silver_cases", None)
        self.assertNotEqual(0, code)
        self.assertEqual("fail", self.stored(folder)["outcome"])
        self.assertTrue(any("def main" in text for text in seen), "the copy of the script is scanned")

    def test_a_failed_copy_of_the_script_or_the_environment_file_gives_a_failing_record(self) -> None:
        for name, script, environment in self.CASES:
            for failing in (script, environment):
                with self.subTest(name=name, failing=failing):
                    code, folder = self.main(name, breaking(failing))
                    self.assertNotEqual(0, code)
                    self.assertEqual("fail", self.stored(folder)["outcome"])
