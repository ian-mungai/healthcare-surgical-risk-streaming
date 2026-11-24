"""``replace_owned_file`` replaces the state that a long-running command owns, and nothing else.

The state folder is a folder that the command created or was given as its state folder. The path must lie directly inside it, the entry that is there (if any)
must be a regular file and not a link, and the write is a private temporary file in the state folder, then ``os.replace``. A path outside the folder, a link
and a folder are refused with fixed text and nothing is written. ``write_new_file`` names its temporary file with a short prefix, so a name of 255 bytes works.
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code, Notice, render
from scripts.ingestion.output_file import replace_owned_file, write_new_file

ORIGINAL = "original\n"
REFUSED = render(Notice(Code.OWNED_REFUSED))


class Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.state = self.root / "state"
        self.state.mkdir()

    def left(self) -> list[str]:
        return sorted(path.name for path in self.root.rglob("*") if path.name.endswith(".part"))


class ReplaceOwnedFileTests(Fixture):
    def test_a_missing_file_is_created_and_an_existing_one_is_replaced_whole_for_its_owner_only(self) -> None:
        path = self.state / "health.json"
        replace_owned_file(path, "one\n", state_root=self.state)
        self.assertEqual("one\n", path.read_text())
        self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
        replace_owned_file(path, b"two\n", state_root=self.state, mode=0o640)
        self.assertEqual("two\n", path.read_text())
        self.assertEqual(0o640, stat.S_IMODE(path.stat().st_mode))
        self.assertEqual([], self.left())
        self.assertEqual(["health.json"], [entry.name for entry in self.state.iterdir()])

    def test_the_replacement_is_one_rename_so_a_reader_sees_the_old_or_the_new_file_whole(self) -> None:
        path = self.state / "health.json"
        path.write_text(ORIGINAL)
        with mock.patch("scripts.ingestion.output_file.os.replace", side_effect=OSError), self.assertRaises(ClosedInputError):
            replace_owned_file(path, "new\n", state_root=self.state)
        self.assertEqual(ORIGINAL, path.read_text())
        self.assertEqual([], self.left())
        self.assertEqual(["health.json"], [entry.name for entry in self.state.iterdir()])

    def test_a_path_outside_the_state_folder_is_refused_and_nothing_is_written(self) -> None:
        (self.state / "deeper").mkdir()
        outside = {
            "a sibling folder": self.root / "other" / "health.json",
            "a deeper folder": self.state / "deeper" / "health.json",
            "the folder above": self.root / "health.json",
            "a dot dot part": self.state / "deeper" / ".." / "health.json",
            "a part that leaves and returns": self.root / "other" / ".." / "state" / "health.json",
            "a path next to the folder that starts like it": Path(f"{self.state}_copy") / "health.json",
        }
        (self.root / "other").mkdir()
        Path(f"{self.state}_copy").mkdir()
        for label, path in outside.items():
            with self.subTest(path=label), self.assertRaises(ClosedInputError) as caught:
                replace_owned_file(path, "new\n", state_root=self.state)
            self.assertEqual(REFUSED, str(caught.exception))
        self.assertEqual([], [entry.name for entry in (self.root / "other").iterdir()])
        self.assertEqual([], [entry.name for entry in Path(f"{self.state}_copy").iterdir()])
        self.assertEqual(["deeper"], [entry.name for entry in self.state.iterdir()])
        self.assertEqual([], self.left())

    def test_a_link_is_refused_and_neither_the_link_nor_its_target_changes(self) -> None:
        target = self.root / "target.txt"
        target.write_text(ORIGINAL)
        forms = {"a link to a file": target, "a dangling link": self.root / "nowhere", "a link to a folder": self.root}
        for label, destination in forms.items():
            path = self.state / "health.json"
            path.symlink_to(destination)
            with self.subTest(path=label), self.assertRaises(ClosedInputError) as caught:
                replace_owned_file(path, "new\n", state_root=self.state)
            self.assertEqual(REFUSED, str(caught.exception))
            self.assertTrue(path.is_symlink())
            self.assertEqual(destination, Path(os.readlink(path)))
            path.unlink()
        self.assertEqual(ORIGINAL, target.read_text())
        self.assertFalse((self.root / "nowhere").exists())
        self.assertEqual([], self.left())

    def test_a_folder_a_pipe_and_a_state_folder_that_is_not_a_folder_are_refused(self) -> None:
        folder = self.state / "health.json"
        folder.mkdir()
        with self.assertRaises(ClosedInputError) as caught:
            replace_owned_file(folder, "new\n", state_root=self.state)
        self.assertEqual(REFUSED, str(caught.exception))
        self.assertEqual([], list(folder.iterdir()))
        pipe = self.state / "pipe"
        os.mkfifo(pipe)
        with self.assertRaises(ClosedInputError):
            replace_owned_file(pipe, "new\n", state_root=self.state)
        self.assertTrue(stat.S_ISFIFO(os.lstat(pipe).st_mode))
        plain = self.root / "plain"
        plain.write_text(ORIGINAL)
        for root in (plain, self.root / "absent"):
            with self.subTest(root=root.name), self.assertRaises(ClosedInputError):
                replace_owned_file(root / "health.json", "new\n", state_root=root)
        self.assertEqual(ORIGINAL, plain.read_text())

    def test_a_state_folder_that_is_a_link_to_a_folder_is_refused(self) -> None:
        real = self.root / "real"
        real.mkdir()
        link = self.root / "link"
        link.symlink_to(real)
        with self.assertRaises(ClosedInputError):
            replace_owned_file(link / "health.json", "new\n", state_root=link)
        self.assertEqual([], list(real.iterdir()))

    def test_the_refusal_is_fixed_text_that_names_no_path(self) -> None:
        marker = self.root / "secret_marker_folder"
        with self.assertRaises(ClosedInputError) as caught:
            replace_owned_file(marker / "x.json", "new\n", state_root=marker)
        self.assertNotIn("secret_marker", str(caught.exception))
        self.assertNotIn(str(self.root), str(caught.exception))


class WriteNewFileNameTests(Fixture):
    def test_a_name_of_255_bytes_is_written_because_the_temporary_name_does_not_repeat_it(self) -> None:
        path = self.state / ("n" * 251 + ".txt")
        self.assertEqual(255, len(path.name.encode()))
        write_new_file(path, "text\n")
        self.assertEqual("text\n", path.read_text())
        self.assertEqual([path.name], [entry.name for entry in self.state.iterdir()])

    def test_the_temporary_name_is_a_dot_eight_hex_digits_and_part_and_is_gone_afterwards(self) -> None:
        seen: list[str] = []
        real = os.open

        def spy(name: str | bytes | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
            seen.append(Path(str(name)).name)
            return real(name, flags, mode)

        with mock.patch("scripts.ingestion.output_file.os.open", side_effect=spy):
            write_new_file(self.state / "result.json", "text\n")
        self.assertEqual(1, len(seen))
        self.assertRegex(seen[0], re.compile(r"\.[0-9a-f]{8}\.part"))
        self.assertEqual(["result.json"], [entry.name for entry in self.state.iterdir()])

    def test_replace_owned_file_uses_the_same_short_temporary_name_so_a_long_name_works(self) -> None:
        path = self.state / ("n" * 251 + ".txt")
        replace_owned_file(path, "text\n", state_root=self.state)
        self.assertEqual([path.name], [entry.name for entry in self.state.iterdir()])


if __name__ == "__main__":
    unittest.main()
