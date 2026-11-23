"""The staging walk: the files under one shell pattern of absolute paths, counted as read, for the capacity sampler's child program.

The walk has no deadline of its own: the sampler that starts it kills it at the end of the reading. It never follows a link, skips a name that starts with a dot
unless the pattern's segment does, matches folders only for a pattern that ends with a slash, skips a file that is gone and lets any other failure, an
unreadable folder among them, end it, so that no unreadable folder reads as an empty one.
"""

from __future__ import annotations

import contextlib
import errno
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.staging_walk import EXIT_OS_ERROR, main, read_staging


def make_pods(folder: str) -> Path:
    pods = Path(folder) / "pods"
    for pod, size in (("a", 1000), ("b", 4000)):
        (pods / pod / "stage" / "deep").mkdir(parents=True)
        (pods / pod / "stage" / "deep" / "file.bin").write_bytes(b"x" * size)
        (pods / pod / "stage" / "empty.bin").write_bytes(b"")
    (pods / "a" / "other").mkdir()
    (pods / "a" / "other" / "not_staged.bin").write_bytes(b"y" * 777)
    return pods


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


class CountTest(unittest.TestCase):
    def test_every_file_under_every_match_is_counted_and_nothing_outside_the_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            row = read_staging(str(make_pods(folder) / "*" / "stage"))
        self.assertEqual({"matches": 2, "files": 4, "apparent_bytes": 5000}, {key: row[key] for key in ("matches", "files", "apparent_bytes")})
        self.assertGreaterEqual(row["allocated_bytes"], 5000)
        self.assertEqual(0, row["allocated_bytes"] % 512)

    def test_a_pattern_with_no_match_is_a_row_of_zeros(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            pods = make_pods(folder)
            for pattern in (str(pods / "*" / "absent"), str(pods / "absent"), str(pods / "nothing*")):
                with self.subTest(pattern=pattern):
                    self.assertEqual({"matches": 0, "files": 0, "apparent_bytes": 0, "allocated_bytes": 0}, read_staging(pattern))

    def test_a_link_is_counted_as_the_link_and_the_file_it_leads_to_is_not_followed(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage, outside = Path(folder) / "stage", Path(folder) / "outside.bin"
            stage.mkdir()
            outside.write_bytes(b"z" * 90000)
            (stage / "link").symlink_to(outside)
            (stage / "folder_link").symlink_to(Path(folder))
            row = read_staging(str(stage))
        self.assertEqual(2, row["files"])
        self.assertLess(row["apparent_bytes"], 90000)

    def test_a_file_that_vanishes_during_the_walk_is_not_a_failure(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage = Path(folder) / "stage"
            stage.mkdir()
            (stage / "stays.bin").write_bytes(b"x" * 2000)
            (stage / "goes.bin").write_bytes(b"x" * 3000)
            real = os.lstat

            def vanishing(path: Any, *args: Any, **keywords: Any) -> os.stat_result:
                if str(path).endswith("goes.bin"):
                    raise FileNotFoundError(errno.ENOENT, "gone")
                return real(path, *args, **keywords)

            with mock.patch("scripts.stack.staging_walk.os.lstat", vanishing):
                row = read_staging(str(stage))
        self.assertEqual((1, 2000), (row["files"], row["apparent_bytes"]))


class PatternTest(unittest.TestCase):
    def test_a_link_in_a_wildcard_position_is_not_followed_and_its_target_is_not_counted(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            pods = make_pods(folder)
            (Path(folder) / "outside" / "stage").mkdir(parents=True)
            (Path(folder) / "outside" / "stage" / "big.bin").write_bytes(b"z" * 90000)
            (pods / "linked").symlink_to(Path(folder) / "outside")
            row = read_staging(str(pods / "*" / "stage"))
        self.assertEqual((2, 5000), (row["matches"], row["apparent_bytes"]))

    def test_a_link_at_the_last_wildcard_position_is_one_match_counted_as_the_link(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage, outside = Path(folder) / "stage", Path(folder) / "outside.bin"
            stage.mkdir()
            outside.write_bytes(b"z" * 90000)
            (stage / "link").symlink_to(outside)
            row = read_staging(str(stage / "*"))
        self.assertEqual((1, 1), (row["matches"], row["files"]))
        self.assertLess(row["apparent_bytes"], 90000)

    def test_a_wildcard_does_not_match_a_name_that_starts_with_a_dot_unless_the_segment_does(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage = Path(folder) / "stage"
            stage.mkdir()
            (stage / ".partial").write_bytes(b"x" * 10)
            (stage / "done.bin").write_bytes(b"x" * 20)
            plain, dotted = read_staging(str(stage / "*")), read_staging(str(stage / ".*"))
        self.assertEqual((1, 20), (plain["matches"], plain["apparent_bytes"]))
        self.assertEqual((1, 10), (dotted["matches"], dotted["apparent_bytes"]))

    def test_a_pattern_that_ends_with_a_slash_matches_folders_only_and_not_a_file_or_a_link(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage = Path(folder) / "stage"
            (stage / "dir_a").mkdir(parents=True)
            (stage / "dir_a" / "part.bin").write_bytes(b"x" * 100)
            (stage / "file.bin").write_bytes(b"x" * 1000)
            (stage / "link").symlink_to(stage / "dir_a")
            wildcard, literal, plain = read_staging(str(stage) + "/*/"), read_staging(str(stage / "file.bin") + "/"), read_staging(str(stage / "*"))
        self.assertEqual((1, 100), (wildcard["matches"], wildcard["apparent_bytes"]))
        self.assertEqual(0, literal["matches"])
        self.assertEqual(3, plain["matches"], "the same pattern with no slash matches the file and the link as well")


@unittest.skipIf(os.geteuid() == 0, "a file system permission does not stop the owner of the system")
class UnreadableTest(unittest.TestCase):
    """A folder that may not be read ends the walk with the error. It is never an empty folder."""

    def test_an_unreadable_folder_inside_a_match(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage = Path(folder) / "stage"
            (stage / "locked").mkdir(parents=True)
            (stage / "locked" / "file.bin").write_bytes(b"x" * 100)
            (stage / "locked").chmod(0)
            try:
                with self.assertRaises(PermissionError):
                    read_staging(str(stage))
            finally:
                (stage / "locked").chmod(0o700)

    def test_an_unreadable_intermediate_folder_while_a_wildcard_expands(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            pods = make_pods(folder)
            (pods / "locked").mkdir()
            (pods / "locked").chmod(0)
            try:
                with self.assertRaises(PermissionError):
                    read_staging(str(pods / "*" / "stage"))
            finally:
                (pods / "locked").chmod(0o700)

    def test_the_unreadable_folder_that_a_wildcard_lists(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            pods = make_pods(folder)
            pods.chmod(0)
            try:
                with self.assertRaises(PermissionError):
                    read_staging(str(pods / "*" / "stage"))
            finally:
                pods.chmod(0o700)


class ProgramTest(unittest.TestCase):
    def test_the_program_prints_one_line_of_four_whole_numbers_and_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, out, err = run(str(make_pods(folder) / "*" / "stage"))
        row = json.loads(out)
        self.assertEqual((0, ""), (code, err))
        self.assertEqual(["matches", "files", "apparent_bytes", "allocated_bytes"], list(row))
        self.assertEqual(1, len(out.splitlines()))
        self.assertTrue(all(type(value) is int for value in row.values()))

    @unittest.skipIf(os.geteuid() == 0, "a file system permission does not stop the owner of the system")
    def test_an_unreadable_folder_is_the_fixed_exit_status_and_prints_no_word_of_its_path(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            stage = Path(folder) / "private-name-stage"
            (stage / "locked").mkdir(parents=True)
            (stage / "locked").chmod(0)
            try:
                code, out, err = run(str(stage))
            finally:
                (stage / "locked").chmod(0o700)
        self.assertEqual((EXIT_OS_ERROR, ""), (code, out))
        self.assertNotIn("private-name", err)

    def test_a_command_line_that_is_not_one_absolute_pattern_is_a_usage_refusal_that_repeats_nothing(self) -> None:
        for arguments in ((), ("/a", "/b"), ("relative/private-value",), ("",)):
            with self.subTest(arguments=arguments):
                code, out, err = run(*arguments)
                self.assertEqual((2, ""), (code, out))
                self.assertNotIn("private-value", err)


if __name__ == "__main__":
    unittest.main()
