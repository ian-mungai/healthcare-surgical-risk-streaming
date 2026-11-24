"""The one file that a sealed job writes its result to, and the rules that keep it whole. No Spark runs here.

Failure modes covered here:

- A caller reads the free text of a job's two streams, so a line of them can carry a value or be taken for a result. The result is one file at a path on the
  command line, and nothing else reaches the caller.
- A path that is not absolute, a path with a NUL, a folder that is missing, a file or a part file that exists, or a second ``--result`` is taken, so that a
  file is replaced or a caller reads another one.
- A reader sees half a file: the document is written whole to ``<path>.part`` and renamed into place, and a failure leaves no part file.
- The file is not readable by the caller or is executable.
- An ending that the job maps to a code writes no file, or a failure writes the text of its error, a count or an argument into the file or a stream.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieError
from spark.result_file import FILE_MODE, Once, SealedError, add_result_option, result_path, run_sealed, usable, write_result
from spark.settings import SettingsError

DOCUMENT = {"job": "j", "passed": True, "diagnostics": []}


class Parsing(unittest.TestCase):
    def parser(self) -> argparse.ArgumentParser:
        command = argparse.ArgumentParser(exit_on_error=False)
        add_result_option(command)
        return command

    def test_an_absolute_path_is_taken_and_the_option_is_required(self) -> None:
        self.assertEqual("/a/b.json", self.parser().parse_args(["--result", "/a/b.json"]).result)
        with self.assertRaises(argparse.ArgumentError):
            self.parser().parse_args([])

    def test_a_path_that_is_not_absolute_or_holds_a_nul_or_is_long_or_ends_in_a_slash_is_refused(self) -> None:
        for text in ("", "result.json", "./x", "/a/\x00b", "/" + "a" * 5000, "/a/b/"):
            with self.subTest(text=text[:20]), self.assertRaises(argparse.ArgumentTypeError):
                result_path(text)

    def test_the_option_is_given_once(self) -> None:
        class Error(Exception):
            pass

        command = argparse.ArgumentParser()
        command.error = lambda message: (_ for _ in ()).throw(Error(message))  # type: ignore[method-assign]
        command.add_argument("--result", type=result_path, action=Once, required=True)
        with self.assertRaises(Error):
            command.parse_args(["--result", "/a", "--result", "/b"])
        self.assertIsNotNone(Once)


class Writing(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="result_file_"))
        self.path = f"{self.folder}/result.json"

    def test_a_path_is_usable_only_when_its_folder_is_there_and_neither_it_nor_its_part_file_is(self) -> None:
        self.assertTrue(usable(self.path))
        self.assertFalse(usable(f"{self.folder}/missing/result.json"))
        Path(f"{self.path}.part").write_text("x")
        self.assertFalse(usable(self.path))
        os.remove(f"{self.path}.part")
        Path(self.path).write_text("x")
        self.assertFalse(usable(self.path))

    def test_a_dangling_link_and_a_folder_are_not_usable(self) -> None:
        os.symlink(f"{self.folder}/nowhere", f"{self.folder}/link")
        os.mkdir(f"{self.folder}/dir")
        self.assertFalse(usable(f"{self.folder}/link"))
        self.assertFalse(usable(f"{self.folder}/dir"))

    def test_the_document_is_written_whole_with_sorted_keys_and_the_file_is_not_executable(self) -> None:
        write_result(self.path, DOCUMENT)
        self.assertEqual(DOCUMENT, json.loads(Path(self.path).read_text()))
        self.assertEqual(FILE_MODE, os.stat(self.path).st_mode & 0o777)
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_the_file_appears_whole_in_one_rename_from_the_part_file(self) -> None:
        seen: dict[str, object] = {}
        real = os.rename

        def rename(source: str, target: str) -> None:
            seen["target_before"] = os.path.lexists(target)
            seen["whole"] = json.loads(Path(source).read_text())
            seen["names"] = (os.path.basename(source), os.path.basename(target))
            real(source, target)

        with mock.patch("os.rename", rename):
            write_result(self.path, DOCUMENT)
        self.assertEqual((False, DOCUMENT, ("result.json.part", "result.json")), (seen["target_before"], seen["whole"], seen["names"]))

    def test_the_document_is_flushed_to_disk_before_the_rename(self) -> None:
        order: list[str] = []
        real_fsync, real_rename = os.fsync, os.rename

        def fsync(descriptor: int) -> None:
            order.append("fsync")
            real_fsync(descriptor)

        def rename(source: str, target: str) -> None:
            order.append("rename")
            real_rename(source, target)

        with (
            mock.patch("os.fsync", fsync),
            mock.patch("os.rename", rename),
        ):
            write_result(self.path, DOCUMENT)
        self.assertEqual(["fsync", "rename"], order)

    def test_a_failed_write_leaves_neither_a_file_nor_a_part_file(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("disk full")

        with mock.patch("os.rename", refuse), self.assertRaises(OSError):
            write_result(self.path, DOCUMENT)
        self.assertEqual([], os.listdir(self.folder))

    def test_a_part_file_that_exists_is_never_opened(self) -> None:
        Path(f"{self.path}.part").write_text("old")
        with self.assertRaises(FileExistsError):
            write_result(self.path, DOCUMENT)
        self.assertEqual("old", Path(f"{self.path}.part").read_text())

    def test_a_document_that_is_not_json_leaves_no_part_file(self) -> None:
        with self.assertRaises(TypeError):
            write_result(self.path, {"x": object()})
        self.assertEqual([], os.listdir(self.folder))


JOB = "toy_job"
MARKER = "MARKER_4417"


def toy_parser() -> argparse.ArgumentParser:
    class Quiet(argparse.ArgumentParser):
        def error(self, message: str) -> Any:
            sys.stderr.write(f"{JOB}: usage\n")
            raise SystemExit(2)

    command = Quiet(prog=JOB)
    modes = command.add_subparsers(dest="mode", required=True)
    mode = modes.add_parser("verify")
    mode.add_argument("--branch", required=True)
    add_result_option(mode)
    return command


def sealed(argv: Sequence[str], execute: Any) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run_sealed(toy_parser(), argv, execute, JOB)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def raising(error: Exception) -> Any:
    def execute(arguments: argparse.Namespace) -> dict[str, Any]:
        raise error

    return execute


class Sealed(unittest.TestCase):
    """``run_sealed``: every ending that the job maps to a code writes the one result file, and no text of an error reaches the file or a stream."""

    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="sealed_run_"))
        self.path = f"{self.folder}/result.json"
        self.argv = ["verify", "--branch", "b", "--result", self.path]

    def written(self) -> dict[str, Any]:
        document: dict[str, Any] = json.loads(Path(self.path).read_text())
        return document

    def test_a_clean_run_writes_its_facts_and_an_empty_list_of_diagnostics_and_prints_one_result_line(self) -> None:
        code, out, err = sealed(self.argv, lambda arguments: {"mode": arguments.mode, "branch": arguments.branch, "passed": {"t": True}})
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({"mode": "verify", "branch": "b", "passed": {"t": True}, "diagnostics": []}, self.written())
        self.assertEqual(self.written(), json.loads(out.removeprefix("RESULT ")))

    def test_a_failed_check_writes_each_cause_as_a_code_and_no_count_and_prints_the_lines(self) -> None:
        error = SealedError("two_sets: failed", "source_table: 2 tables", "pinned_counts: failed")
        code, out, err = sealed(self.argv, raising(error))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual(
            {"mode": "verify", "branch": "b", "job": JOB, "diagnostics": [{"code": "two_sets"}, {"code": "source_table"}, {"code": "pinned_counts"}]},
            self.written(),
        )
        self.assertEqual(f"{JOB}: two_sets: failed\n{JOB}: source_table: 2 tables\n{JOB}: pinned_counts: failed\n", err)

    def test_every_other_ending_is_one_code_in_the_file_and_the_text_of_the_error_is_in_no_output(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        endings = (
            (missing, 1, "branch_missing"),
            (other, 1, "catalog_error"),
            (NessieError(MARKER, status=404, error_code="OTHER"), 1, "catalog_error"),
            (SettingsError(MARKER), 2, "settings"),
            (RuntimeError(MARKER), 70, "internal_error"),
        )
        for error, status, expected in endings:
            with self.subTest(expected=expected):
                code, out, err = sealed(self.argv, raising(error))
                self.assertEqual((status, "", f"{JOB}: {expected}\n"), (code, out, err))
                self.assertEqual({"mode": "verify", "branch": "b", "job": JOB, "diagnostics": [{"code": expected}]}, self.written())
                self.assertNotIn(MARKER, Path(self.path).read_text())
                os.remove(self.path)

    def test_a_bad_call_is_usage_and_writes_nothing_and_does_not_run_the_job(self) -> None:
        ran: list[str] = []

        def execute(arguments: argparse.Namespace) -> dict[str, Any]:
            ran.append("run")
            return {}

        Path(f"{self.folder}/old.json").write_text("old")
        for argv in (
            ["verify", "--branch", "b"],
            ["verify", "--branch", "b", "--result", "relative.json"],
            ["verify", "--branch", "b", "--result", f"{self.folder}/old.json"],
        ):
            with self.subTest(argv=argv):
                self.assertEqual((2, "", f"{JOB}: usage\n"), sealed(argv, execute))
        self.assertEqual(([], ["old.json"]), (ran, os.listdir(self.folder)))

    def test_a_file_that_cannot_be_written_is_internal_error_with_no_file_and_no_result_line(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError(f"{MARKER} disk full")

        with mock.patch("os.rename", refuse):
            code, out, err = sealed(self.argv, lambda arguments: {"mode": "verify"})
        self.assertEqual((70, "", f"{JOB}: internal_error\n"), (code, out, err))
        self.assertEqual([], os.listdir(self.folder))

    def test_a_sealed_error_keeps_its_problems_in_order(self) -> None:
        self.assertEqual(["a: failed", "b: failed"], SealedError("a: failed", "b: failed").problems)


if __name__ == "__main__":
    unittest.main()
