"""The one file that a sealed job writes its result to, and the rules that keep it whole. No Spark runs here.

Failure modes covered here:

- A caller reads the free text of a job's two streams, so a line of them can carry a value or be taken for a result. The result is one file at a path on the
  command line, and nothing else reaches the caller.
- A path that is not absolute, a path with a NUL, a folder that is missing, a file or a part file that exists, or a second ``--result`` is taken, so that a
  file is replaced or a caller reads another one.
- A file that appears at the path while the job runs is replaced by the rename, so a result that a caller already holds is overwritten.
- A reader sees half a file: the document is written whole to ``<path>.part`` and renamed into place, and a failure leaves no part file.
- A path that passes the preflight but cannot hold its part file (a name of 255 bytes: ``<name>.part`` is too long) fails only after the job ran. The part
  file is reserved (created exclusively) before any work, and a reservation that fails is a bad call: ``usage``, exit 2, and nothing runs.
- A job that the watchdog stops leaves its reserved part file for a reader to take for a result.
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
from typing import Any, cast
from unittest import mock

from scripts.stack.nessie import NessieError
from spark import result_file
from spark.result_file import FILE_MODE, Once, SealedError, add_result_option, release_reserved, reserve, result_path, run_sealed, write_reserved
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

    def tearDown(self) -> None:
        release_reserved()

    def written(self, document: dict[str, Any] = DOCUMENT) -> None:
        descriptor = reserve(self.path)
        self.assertIsNotNone(descriptor)
        write_reserved(cast("int", descriptor), self.path, document)

    def test_a_path_is_reserved_only_when_its_folder_is_there_and_neither_it_nor_its_part_file_is(self) -> None:
        descriptor = reserve(self.path)
        self.assertIsNotNone(descriptor)
        self.assertEqual(["result.json.part"], os.listdir(self.folder))
        self.assertIsNone(reserve(self.path))
        release_reserved()
        self.assertEqual([], os.listdir(self.folder))
        self.assertIsNone(reserve(f"{self.folder}/missing/result.json"))
        Path(self.path).write_text("x")
        self.assertIsNone(reserve(self.path))
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_dangling_link_a_folder_and_a_part_file_that_exists_are_not_reserved(self) -> None:
        os.symlink(f"{self.folder}/nowhere", f"{self.folder}/link")
        os.mkdir(f"{self.folder}/dir")
        Path(f"{self.folder}/old.json.part").write_text("old")
        for name in ("link", "dir", "old.json"):
            with self.subTest(name=name):
                self.assertIsNone(reserve(f"{self.folder}/{name}"))
        self.assertEqual("old", Path(f"{self.folder}/old.json.part").read_text())

    def test_a_name_of_255_bytes_cannot_be_reserved_and_one_of_250_can(self) -> None:
        self.assertIsNone(reserve(f"{self.folder}/{'r' * 255}"))
        self.assertEqual([], os.listdir(self.folder))
        long_path = f"{self.folder}/{'r' * 250}"
        self.assertIsNotNone(reserve(long_path))
        release_reserved()
        self.assertEqual([], os.listdir(self.folder))

    def test_the_document_is_written_whole_with_sorted_keys_and_the_file_is_not_executable(self) -> None:
        self.written()
        self.assertEqual(DOCUMENT, json.loads(Path(self.path).read_text()))
        self.assertEqual(0o644, os.stat(self.path).st_mode & 0o777)
        self.assertEqual(0o644, FILE_MODE)
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
            self.written()
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
            self.written()
        self.assertEqual(["fsync", "rename"], order)

    def test_a_failed_write_leaves_neither_a_file_nor_a_part_file(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("disk full")

        with mock.patch("os.rename", refuse), self.assertRaises(OSError):
            self.written()
        self.assertEqual([], os.listdir(self.folder))

    def test_the_watchdog_release_removes_the_reserved_part_file_and_nothing_else(self) -> None:
        Path(f"{self.folder}/other.part").write_text("x")
        self.assertIsNotNone(reserve(self.path))
        release_reserved()
        release_reserved()
        self.assertEqual(["other.part"], os.listdir(self.folder))

    def test_a_file_that_appeared_after_the_reservation_is_never_written_over(self) -> None:
        descriptor = reserve(self.path)
        self.assertIsNotNone(descriptor)
        Path(self.path).write_text("other")
        with self.assertRaises(FileExistsError):
            write_reserved(cast("int", descriptor), self.path, DOCUMENT)
        self.assertEqual("other", Path(self.path).read_text())
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_document_that_is_not_json_leaves_no_part_file(self) -> None:
        with self.assertRaises(TypeError):
            self.written({"x": object()})
        self.assertEqual([], os.listdir(self.folder))

    def test_every_error_path_closes_the_descriptor_of_the_part_file(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("disk full")

        plants: dict[str, tuple[Any, dict[Any, Any]]] = {
            "a document that is not json": (contextlib.nullcontext(), {"x": object()}),
            "a document with keys that are not text": (contextlib.nullcontext(), {"x": float("nan"), (1, 2): 3}),
            "a rename that fails": (mock.patch("os.rename", refuse), DOCUMENT),
            "a file that appeared": (mock.patch("os.path.lexists", return_value=True), DOCUMENT),
        }
        for name, (patch, document) in plants.items():
            with self.subTest(name):
                descriptor = reserve(self.path)
                self.assertIsNotNone(descriptor)
                with patch, contextlib.suppress(Exception):
                    write_reserved(cast("int", descriptor), self.path, document)
                with self.assertRaises(OSError):
                    os.fstat(cast("int", descriptor))
                release_reserved()

    def test_a_descriptor_that_the_write_took_is_closed_once_after_a_good_write(self) -> None:
        descriptor = reserve(self.path)
        self.assertIsNotNone(descriptor)
        write_reserved(cast("int", descriptor), self.path, DOCUMENT)
        with self.assertRaises(OSError):
            os.fstat(cast("int", descriptor))


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

    def tearDown(self) -> None:
        release_reserved()

    def test_a_clean_run_writes_its_facts_and_an_empty_list_of_diagnostics_and_prints_nothing(self) -> None:
        code, out, err = sealed(self.argv, lambda arguments: {"mode": arguments.mode, "branch": arguments.branch, "passed": {"t": True}})
        self.assertEqual((0, "", ""), (code, out, err))
        self.assertEqual({"mode": "verify", "branch": "b", "passed": {"t": True}, "diagnostics": []}, self.written())
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_failed_check_writes_each_cause_as_a_code_and_no_count_and_prints_the_lines(self) -> None:
        error = SealedError("two_sets: failed", "source_table: failed", "pinned_counts: failed")
        code, out, err = sealed(self.argv, raising(error))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual(
            {"mode": "verify", "branch": "b", "job": JOB, "diagnostics": [{"code": "two_sets"}, {"code": "source_table"}, {"code": "pinned_counts"}]},
            self.written(),
        )
        self.assertEqual(f"{JOB}: two_sets: failed\n{JOB}: source_table: failed\n{JOB}: pinned_counts: failed\n", err)

    def test_a_problem_that_holds_more_than_its_code_is_printed_as_its_code_and_a_flag(self) -> None:
        code, out, err = sealed(self.argv, raising(SealedError("source_table: 2 tables", "bare_code", "x: 7 rows")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"{JOB}: source_table: failed\n{JOB}: bare_code: failed\n{JOB}: x: failed\n", err)

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
                self.assertEqual((status, "", f"{JOB}: {expected}: failed\n"), (code, out, err))
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

    def test_a_path_that_cannot_hold_its_part_file_is_a_bad_call_before_any_work(self) -> None:
        ran: list[str] = []

        def execute(arguments: argparse.Namespace) -> dict[str, Any]:
            ran.append("run")
            return {"mode": "verify", "branch": "b"}

        argv = ["verify", "--branch", "b", "--result", f"{self.folder}/{'r' * 255}"]
        self.assertEqual((2, "", f"{JOB}: usage\n"), sealed(argv, execute))
        self.assertEqual(([], []), (ran, os.listdir(self.folder)))

    def test_a_path_whose_part_file_fits_runs_and_leaves_only_the_result(self) -> None:
        name = "r" * 250
        argv = ["verify", "--branch", "b", "--result", f"{self.folder}/{name}"]
        self.assertEqual((0, "", ""), sealed(argv, lambda arguments: {"mode": "verify", "branch": "b"}))
        self.assertEqual([name], os.listdir(self.folder))

    def test_the_part_file_is_reserved_before_the_job_runs_and_gone_after_it(self) -> None:
        seen: list[list[str]] = []

        def execute(arguments: argparse.Namespace) -> dict[str, Any]:
            seen.append(os.listdir(self.folder))
            return {"mode": "verify", "branch": "b"}

        self.assertEqual(0, sealed(self.argv, execute)[0])
        self.assertEqual([["result.json.part"]], seen)
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_job_that_raises_past_the_mapping_leaves_no_part_file(self) -> None:
        def execute(arguments: argparse.Namespace) -> dict[str, Any]:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_sealed(toy_parser(), self.argv, execute, JOB)
        self.assertEqual([], os.listdir(self.folder))

    def test_a_file_that_cannot_be_written_is_internal_error_with_no_file_and_no_result_line(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError(f"{MARKER} disk full")

        with mock.patch("os.rename", refuse):
            code, out, err = sealed(self.argv, lambda arguments: {"mode": "verify"})
        self.assertEqual((70, "", f"{JOB}: internal_error: failed\n"), (code, out, err))
        self.assertIs(result_file.INTERNAL_EXIT_CODE, 70)
        self.assertEqual([], os.listdir(self.folder))

    def test_a_sealed_error_keeps_its_problems_in_order(self) -> None:
        self.assertEqual(["a: failed", "b: failed"], SealedError("a: failed", "b: failed").problems)


if __name__ == "__main__":
    unittest.main()
