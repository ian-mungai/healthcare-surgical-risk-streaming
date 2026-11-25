"""The one file that a sealed job writes its result to, and the rules that keep it whole. No Spark runs here.

Failure modes covered here:

- A caller reads the free text of a job's two streams, so a line of them can carry a value or be taken for a result. The result is one file at a path on the
  command line, and nothing else reaches the caller.
- A path that is not absolute, a path with a NUL, a folder that is missing, a file or a part file that exists, or a second ``--result`` is taken, so that a
  file is replaced or a caller reads another one.
- A file that appears at the path while the job runs is replaced, so a result that a caller already holds is overwritten. The document is put in place with
  ``publish_reserved`` (a hard link, which fails when the path exists), so nothing is replaced even by a late writer.
- A reader sees half a file: the document is written whole into the reserved part file and linked into place. A write or link that fails leaves no result
  file, and no part file when the part name can be removed. A part name that cannot be removed leaves the part file after a failed link as well as both
  names after the link (the result is whole). A removal that fails once and works at the end of the call leaves no part file, and the call still ends as
  ``internal_error``, exit 70.
- A path that passes the preflight but cannot hold its part file (a name of 255 bytes: ``<name>.part`` is too long) fails only after the job ran. The part
  file is reserved (created exclusively) before any work, and a reservation that fails is a bad call: ``usage``, exit 2, and nothing runs.
- A job that the watchdog stops leaves its reserved part file, or a second staging file that holds the whole result, for a reader to take for a result. The
  reserved part file is the only staging file (``test_result_watchdog.py`` stops a real process right before the link).
- The file is not readable by the caller or is executable.
- An ending that the job maps to a code writes no file, or a failure writes the text of its error, a count or an argument into the file or a stream.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
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

from scripts.ingestion.closed import ClosedInputError, QuietParser
from scripts.ingestion.output_file import write_new_file
from scripts.stack.nessie import NessieError
from spark import result_file
from spark.result_file import FILE_MODE, Once, SealedError, add_result_option, release_reserved, reserve, result_path, run_sealed, write_reserved
from spark.settings import SettingsError
from tests.fast.internal_line_support import ENTRY_FRAME, assert_internal, error_with_raising_loader

DOCUMENT = {"job": "j", "passed": True, "diagnostics": []}


class Parsing(unittest.TestCase):
    def parser(self) -> argparse.ArgumentParser:
        command = QuietParser()
        command.error = lambda message: (_ for _ in ()).throw(argparse.ArgumentError(None, message))  # type: ignore[method-assign]
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

        command = QuietParser()
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
        self.assertIsNotNone(reserve(self.path))
        write_reserved(self.path, document)

    def test_a_path_is_reserved_only_when_its_folder_is_there_and_neither_it_nor_its_part_file_is(self) -> None:
        self.assertEqual(f"{self.path}.part", reserve(self.path))
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

    def test_the_file_appears_whole_in_one_link_from_the_reserved_part_file_and_never_over_another(self) -> None:
        seen: dict[str, object] = {}
        real = os.link
        self.assertIsNotNone(reserve(self.path))

        def link(source: str, target: str) -> None:
            seen["target_before"] = os.path.lexists(target)
            seen["whole"] = json.loads(Path(source).read_text())
            seen["names"] = (os.path.basename(source), os.path.basename(target))
            seen["folder"] = os.listdir(self.folder)
            real(source, target)

        with mock.patch("os.link", link):
            write_reserved(self.path, DOCUMENT)
        self.assertEqual((False, DOCUMENT, ("result.json.part", "result.json")), (seen["target_before"], seen["whole"], seen["names"]))
        self.assertEqual(["result.json.part"], seen["folder"])
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_the_document_is_flushed_to_disk_before_the_file_is_put_in_place(self) -> None:
        order: list[str] = []
        real_fsync, real_link = os.fsync, os.link
        self.assertIsNotNone(reserve(self.path))

        def fsync(descriptor: int) -> None:
            order.append("fsync")
            real_fsync(descriptor)

        def link(source: str, target: str) -> None:
            order.append("link")
            real_link(source, target)

        with (
            mock.patch("os.fsync", fsync),
            mock.patch("os.link", link),
        ):
            write_reserved(self.path, DOCUMENT)
        self.assertEqual(["fsync", "link"], order)

    def test_a_failed_write_leaves_neither_a_file_nor_a_part_file(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("disk full")

        self.assertIsNotNone(reserve(self.path))
        with mock.patch("os.link", refuse), self.assertRaises(ClosedInputError):
            write_reserved(self.path, DOCUMENT)
        self.assertEqual([], os.listdir(self.folder))

    def test_the_watchdog_release_removes_the_reserved_part_file_and_nothing_else(self) -> None:
        Path(f"{self.folder}/other.part").write_text("x")
        self.assertIsNotNone(reserve(self.path))
        release_reserved()
        release_reserved()
        self.assertEqual(["other.part"], os.listdir(self.folder))

    def test_a_file_that_appeared_after_the_reservation_is_never_written_over(self) -> None:
        self.assertIsNotNone(reserve(self.path))
        Path(self.path).write_text("other")
        with self.assertRaises(ClosedInputError):
            write_reserved(self.path, DOCUMENT)
        self.assertEqual("other", Path(self.path).read_text())
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_document_that_is_not_json_leaves_no_part_file(self) -> None:
        with self.assertRaises(TypeError):
            self.written({"x": object()})
        self.assertEqual([], os.listdir(self.folder))

    def test_a_failed_write_leaves_no_result_file_and_no_part_file_when_the_part_name_can_be_removed(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError("disk full")

        plants: dict[str, tuple[Any, dict[Any, Any]]] = {
            "a document that is not json": (contextlib.nullcontext(), {"x": object()}),
            "a document with keys that are not text": (contextlib.nullcontext(), {"x": float("nan"), (1, 2): 3}),
            "a link that fails": (mock.patch("os.link", refuse), DOCUMENT),
        }
        for name, (patch, document) in plants.items():
            with self.subTest(name):
                self.assertIsNotNone(reserve(self.path))
                with patch, contextlib.suppress(Exception):
                    write_reserved(self.path, document)
                self.assertEqual([], os.listdir(self.folder))
                release_reserved()


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
                if expected == "internal_error":
                    self.assertEqual((status, ""), (code, out))
                    assert_internal(self, err, JOB, f"{JOB}: internal_error: failed", ("builtins.RuntimeError",), MARKER)
                else:
                    self.assertEqual((status, "", f"{JOB}: {expected}: failed\n"), (code, out, err))
                self.assertEqual({"mode": "verify", "branch": "b", "job": JOB, "diagnostics": [{"code": expected}]}, self.written())
                self.assertNotIn(MARKER, Path(self.path).read_text())
                self.assertNotIn("class=", Path(self.path).read_text())
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

    def test_an_error_whose_source_loader_raises_is_still_exit_70_with_the_result_written_and_the_line_of_its_frames(self) -> None:
        error = error_with_raising_loader()
        code, out, err = sealed(self.argv, raising(error))
        self.assertEqual((70, ""), (code, out))
        frames = assert_internal(self, err, JOB, f"{JOB}: internal_error: failed", ("builtins.KeyError",), "the loader failed")
        self.assertEqual(ENTRY_FRAME, frames[-1])
        self.assertEqual({"mode": "verify", "branch": "b", "job": JOB, "diagnostics": [{"code": "internal_error"}]}, self.written())

    def test_a_file_that_cannot_be_written_is_internal_error_with_no_file_and_no_result_line(self) -> None:
        link = os.link

        def refuse(source: str, target: str) -> None:
            if not str(target).endswith(".part"):
                raise OSError(f"{MARKER} disk full")
            link(source, target)

        with mock.patch("os.link", refuse):
            code, out, err = sealed(self.argv, lambda arguments: {"mode": "verify"})
        self.assertEqual((70, ""), (code, out))
        assert_internal(self, err, JOB, f"{JOB}: internal_error: failed", (), MARKER)
        self.assertIs(result_file.INTERNAL_EXIT_CODE, 70)
        self.assertEqual([], os.listdir(self.folder))

    def test_a_part_name_that_cannot_be_removed_after_the_link_is_internal_error_and_exit_70_not_a_clean_end(self) -> None:
        real = os.unlink

        def unlink(target: object, *arguments: Any, **keywords: Any) -> None:
            if str(target) == f"{self.path}.part":
                raise OSError(errno.EIO, f"{MARKER} input output error")
            real(target, *arguments, **keywords)  # type: ignore[arg-type]

        with mock.patch("os.unlink", unlink):
            code, out, err = sealed(self.argv, lambda arguments: {"mode": "verify"})
        self.assertEqual((70, ""), (code, out))
        assert_internal(self, err, JOB, f"{JOB}: internal_error: failed", (), MARKER)
        self.assertEqual(["result.json", "result.json.part"], sorted(os.listdir(self.folder)))

    def test_a_link_that_fails_and_a_part_name_that_cannot_be_removed_leave_the_part_file_in_both_writers(self) -> None:
        real_unlink = os.unlink

        def refuse_link(source: object, target: object) -> None:
            raise OSError(errno.ENOSPC, "disk full")

        def refuse_unlink(target: object, *arguments: Any, **keywords: Any) -> None:
            if str(target).endswith(".part"):
                raise OSError(errno.EIO, "input output error")
            real_unlink(target, *arguments, **keywords)  # type: ignore[arg-type]

        self.assertIsNotNone(reserve(self.path))
        with mock.patch("os.link", refuse_link), mock.patch("os.unlink", refuse_unlink), self.assertRaises(ClosedInputError):
            write_reserved(self.path, DOCUMENT)
        self.assertEqual(["result.json.part"], os.listdir(self.folder))
        os.unlink(f"{self.path}.part")
        with mock.patch("os.link", refuse_link), mock.patch("os.unlink", refuse_unlink), self.assertRaises(ClosedInputError):
            write_new_file(Path(self.path), "{}\n", mode=FILE_MODE)
        left = os.listdir(self.folder)
        self.assertEqual(1, len(left))
        self.assertTrue(left[0].endswith(".part") and left[0] != "result.json.part")

    def test_a_part_name_that_fails_to_be_removed_once_is_removed_at_the_end_and_the_call_still_ends_as_exit_70(self) -> None:
        real_unlink = os.unlink
        refused: list[str] = []

        def unlink_once(target: object, *arguments: Any, **keywords: Any) -> None:
            if str(target) == f"{self.path}.part" and not refused:
                refused.append(str(target))
                raise OSError(errno.EIO, "input output error")
            real_unlink(target, *arguments, **keywords)  # type: ignore[arg-type]

        real_link = os.link

        def refuse_link(source: Any, target: Any) -> None:
            if str(target) == self.path:
                raise OSError(errno.ENOSPC, "disk full")
            real_link(source, target)

        with mock.patch("os.link", refuse_link), mock.patch("os.unlink", unlink_once):
            code, out, err = sealed(self.argv, lambda arguments: {"mode": "verify"})
        self.assertEqual((70, ""), (code, out))
        assert_internal(self, err, JOB, f"{JOB}: internal_error: failed", (), MARKER)
        self.assertEqual(1, len(refused))
        self.assertEqual([], os.listdir(self.folder))

    def test_a_sealed_error_keeps_its_problems_in_order(self) -> None:
        self.assertEqual(["a: failed", "b: failed"], SealedError("a: failed", "b: failed").problems)


if __name__ == "__main__":
    unittest.main()
