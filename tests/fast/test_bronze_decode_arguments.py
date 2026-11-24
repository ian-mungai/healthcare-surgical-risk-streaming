"""The arguments of the Bronze decoder are checked before the job creates anything: a read log or a manifest reference that is refused leaves no file.

Join round 1, findings 3 and 4. Failure modes covered here:

- A read log whose last name is one that the replay writes in its state folder (any letter case) is created, so a file that a service owns is made vacant.
- A ``--cases`` list that is not made of case numbers escapes as a traceback after the read log was created, so a retry with the same log path is refused.
- A manifest reference that cannot be read escapes as ``FileNotFoundError`` with the file's name instead of the manifest refusal.

The real job runs in ``tests/e2e/bronze_decode.py``; here the store and the log creation are replaced, so that a test shows whether the job reached them.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from replay.service.run_record import STATE_FOLDER_NAMES
from scripts.ingestion.manifest import ManifestError, ManifestRef
from scripts.ingestion.notice import Code, Notice, Subject, render
from scripts.ingestion.store import StoreError
from spark.bronze.inputs import InputError, parse_cases
from tests.fast.fake_vitaldb import installed

with installed():
    bronze_decode = importlib.import_module("spark.jobs.bronze_decode")

REFERENCE = ManifestRef("bucket", "manifests/manifest.json", "version", "a" * 64)
STOPPED = RuntimeError("the store was reached")


class ParseCasesTest(unittest.TestCase):
    def test_case_numbers_separated_by_commas_are_parsed_and_an_empty_list_means_every_case(self) -> None:
        self.assertIsNone(parse_cases(""))
        self.assertEqual([1, 22, 4000], parse_cases("1,22,4000"))
        self.assertEqual([1, 2], parse_cases("1,,2"), "an empty item between commas is skipped, as before")

    def test_anything_that_is_not_a_case_number_is_one_fixed_refusal_that_shows_no_part_of_the_input(self) -> None:
        for text in ("a", "1;2", "-1", " 1", "1.5", "0x10", "١٢", "9" * 40, "marker_value_x9"):
            with self.subTest(text=text), self.assertRaises(InputError) as raised:
                parse_cases(text)
            self.assertEqual(render(Notice(Code.USAGE)), str(raised.exception))
            self.assertNotIn("marker", str(raised.exception))


class DecodeArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="bronze_arguments_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.reference = self.folder / "reference.json"
        self.reference.write_text(REFERENCE.to_json())
        (self.folder / "landed").mkdir()

    def arguments(self, *extra: str, reference: Path | None = None) -> Any:
        return bronze_decode.parser().parse_args(
            ["write", "--branch", "b", "--manifest-reference", str(reference or self.reference), "--landed-folder", str(self.folder / "landed"), *extra]
        )

    def decode(self, *extra: str, reference: Path | None = None) -> list[Path]:
        """Run the job's first steps with the store replaced by a stop, and return the logs that it asked to create."""
        made: list[Path] = []
        with (
            mock.patch.object(bronze_decode, "create_read_log", side_effect=made.append),
            mock.patch.object(bronze_decode, "open_store", side_effect=STOPPED),
            self.assertRaises(RuntimeError) as stopped,
        ):
            bronze_decode.decode(self.arguments(*extra, reference=reference))
        self.assertIs(stopped.exception, STOPPED)
        return made

    def test_control_a_good_log_and_good_cases_reach_the_store_and_make_the_log_once(self) -> None:
        log = self.folder / "reads.log"
        self.assertEqual([log], self.decode("--read-log", str(log), "--cases", "1,2"))

    def test_a_read_log_with_the_name_of_a_state_file_is_refused_in_any_letter_case_and_no_file_is_made(self) -> None:
        for name in (*STATE_FOLDER_NAMES, "ScHeDuLe.JsOn", "SCHEDULE.JSON", ".."):
            log = self.folder / name
            with self.subTest(name=name), mock.patch.object(bronze_decode, "open_store", side_effect=STOPPED):
                with self.assertRaises(StoreError):
                    bronze_decode.decode(self.arguments("--read-log", str(log)))
                self.assertEqual(name == "..", log.exists(), "only the parent folder exists")
                self.assertEqual(["landed", "reference.json"], sorted(path.name for path in self.folder.iterdir()))

    def test_the_refused_name_is_refused_before_the_log_is_asked_for(self) -> None:
        made: list[Path] = []
        with mock.patch.object(bronze_decode, "create_read_log", side_effect=made.append), self.assertRaises(StoreError):
            bronze_decode.decode(self.arguments("--read-log", str(self.folder / "ScHeDuLe.JsOn")))
        self.assertEqual([], made)

    def test_a_malformed_cases_list_is_refused_before_the_log_exists_so_a_retry_with_the_same_path_works(self) -> None:
        log = self.folder / "reads.log"
        made: list[Path] = []
        with mock.patch.object(bronze_decode, "create_read_log", side_effect=made.append), self.assertRaises(InputError):
            bronze_decode.decode(self.arguments("--read-log", str(log), "--cases", "1,x"))
        self.assertEqual([], made)
        self.assertFalse(log.exists())
        self.assertEqual([log], self.decode("--read-log", str(log), "--cases", "1,2"), "the retry gets the same path")

    def test_a_malformed_cases_list_ends_the_command_with_the_fixed_refusal_and_no_traceback(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(bronze_decode, "start_deadline"),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = bronze_decode.main(
                ["write", "--branch", "b", "--manifest-reference", str(self.reference), "--landed-folder", str(self.folder), "--cases", "marker_value_x9"]
            )
        self.assertEqual((1, "", f"bronze_decode: refused: {render(Notice(Code.USAGE))}\n"), (code, out.getvalue(), err.getvalue()))

    def test_a_reference_that_cannot_be_read_is_the_manifest_refusal_and_shows_no_file_name(self) -> None:
        unreadable = self.folder / "marker_value_x9.json"
        undecodable = self.folder / "bytes.json"
        undecodable.write_bytes(b"\xff\xfe\x00marker_value_x9")
        for label, path in (("missing", unreadable), ("a folder", self.folder / "landed"), ("not text", undecodable)):
            with self.subTest(label=label):
                with self.assertRaises(ManifestError) as raised:
                    bronze_decode.decode(self.arguments(reference=path))
                self.assertEqual(render(Notice(Code.CANNOT_READ, subject=Subject.REFERENCE)), str(raised.exception))
                out, err = io.StringIO(), io.StringIO()
                with mock.patch.object(bronze_decode, "start_deadline"), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    code = bronze_decode.main(["write", "--branch", "b", "--manifest-reference", str(path)])
                self.assertEqual(1, code)
                self.assertEqual(
                    f"bronze_decode: the manifest cannot be trusted: {render(Notice(Code.CANNOT_READ, subject=Subject.REFERENCE))}\n", err.getvalue()
                )
                self.assertNotIn("marker_value_x9", err.getvalue() + out.getvalue())


if __name__ == "__main__":
    unittest.main()
