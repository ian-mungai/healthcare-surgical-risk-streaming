"""The unlock key file: created once with a random harmless marker, owned by the numeric user of the OpenBao container, read by that user only, never printed.

The role's task files run through the real ``ansible-playbook`` on this computer against real files in a scratch folder. The marker is random, so the
cases look for its text in the playbook's output instead of a fixed value. A test user cannot give a file to another user, so the owners in these cases
are the test user's own groups: two groups of one user are two different owners.
"""

from __future__ import annotations

import base64
import grp
import os
import pwd
import re
import stat
import tempfile
import unittest
from pathlib import Path

from tests.slow.ansible_support import run_task_files

OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
UID, GID = os.getuid(), os.getgid()
FILE_GID = next((group for group in os.getgroups() if group != GID), GID)
MARKER = "ab" * 32
GOOD_CONTENT = f"OPENBAO_UNLOCK_KEY={MARKER}\n"
MESSAGE = "The unlock key file is missing"


def variables(folder: Path) -> dict[str, object]:
    return {
        "unlock_key_directory": str(folder / "platform"),
        "unlock_key_file": str(folder / "platform" / ".env"),
        "unlock_key_owner": OWNER,
        "unlock_key_group": GROUP,
        "openbao_user": f"{UID}:{FILE_GID}",
    }


def write_file(folder: Path, content: str, mode: int = 0o400, group: int = FILE_GID) -> Path:
    directory = folder / "platform"
    directory.mkdir(exist_ok=True)
    target = directory / ".env"
    target.write_text(content)
    os.chown(target, UID, group)
    target.chmod(mode)
    return target


class CreateTest(unittest.TestCase):
    def create(self, folder: Path, files: tuple[str, ...] = ("create_file.yml", "protect_file.yml", "verify_file.yml")) -> tuple[int, str]:
        result = run_task_files("unlock_key", list(files), variables(folder), verbose=True)
        return result.returncode, result.stdout + result.stderr

    def test_the_file_is_created_with_a_random_marker_owned_by_the_numeric_user_of_the_container_and_read_by_nobody_else(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.create(Path(scratch))
            target = Path(scratch) / "platform" / ".env"
            content = target.read_text()
            status = target.stat()
        self.assertEqual(0, code, output)
        self.assertRegex(content, r"\AOPENBAO_UNLOCK_KEY=[0-9a-f]{64}\n\Z")
        self.assertEqual((0o400, UID, FILE_GID), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))

    def test_the_new_file_is_readable_by_its_creator_only_from_the_moment_it_exists(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.create(Path(scratch), ("create_file.yml",))
            mode = stat.S_IMODE((Path(scratch) / "platform" / ".env").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual(0o400, mode)

    def test_the_marker_is_never_printed(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.create(Path(scratch))
            content = (Path(scratch) / "platform" / ".env").read_text()
        marker = content.split("=", 1)[1].strip()
        self.assertEqual(0, code, output)
        self.assertEqual(64, len(marker))
        self.assertNotIn(marker, output)
        self.assertNotIn(base64.b64encode(content.encode()).decode(), output)

    def test_a_second_run_changes_nothing_and_keeps_the_same_marker(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.create(Path(scratch))
            first = (Path(scratch) / "platform" / ".env").read_text()
            code, output = self.create(Path(scratch))
            second = (Path(scratch) / "platform" / ".env").read_text()
        self.assertEqual(0, code, output)
        self.assertEqual(first, second)
        self.assertRegex(output, r"changed=0\s")

    def test_two_hosts_get_different_markers(self) -> None:
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            self.create(Path(one))
            self.create(Path(two))
            self.assertNotEqual((Path(one) / "platform" / ".env").read_text(), (Path(two) / "platform" / ".env").read_text())

    def test_an_existing_file_is_never_replaced_and_its_mode_and_owner_are_repaired(self) -> None:
        for mode, group in ((0o644, FILE_GID), (0o600, FILE_GID), (0o440, FILE_GID), (0o400, GID)):
            with self.subTest(mode=oct(mode), group=group), tempfile.TemporaryDirectory() as scratch:
                target = write_file(Path(scratch), "OPENBAO_UNLOCK_KEY=" + "cd" * 32 + "\n", mode=mode, group=group)
                code, output = self.create(Path(scratch))
                content = target.read_text()
                status = target.stat()
                self.assertEqual(0, code, output)
                self.assertEqual("OPENBAO_UNLOCK_KEY=" + "cd" * 32 + "\n", content)
                self.assertEqual((0o400, UID, FILE_GID), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))


class VerifyTest(unittest.TestCase):
    def verdict(self, content: str, mode: int = 0o400, group: int = FILE_GID, expected: str = f"{UID}:{FILE_GID}") -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            write_file(Path(scratch), content, mode, group)
            result = run_task_files("unlock_key", ["verify_file.yml"], variables(Path(scratch)) | {"openbao_user": expected}, verbose=True)
        return result.returncode, result.stdout + result.stderr

    def test_a_file_of_the_container_user_with_mode_0400_and_a_key_passes_and_its_content_is_not_shown(self) -> None:
        code, output = self.verdict(GOOD_CONTENT)
        self.assertEqual(0, code, output)
        self.assertNotIn(MARKER, output)
        self.assertNotIn(base64.b64encode(GOOD_CONTENT.encode()).decode(), output)

    def test_a_file_that_anyone_but_its_owner_can_read_or_that_its_owner_can_write_is_refused(self) -> None:
        for mode in (0o600, 0o644, 0o640, 0o440, 0o404, 0o444, 0o500):
            with self.subTest(mode=oct(mode)):
                code, output = self.verdict(GOOD_CONTENT, mode)
                self.assertNotEqual(0, code)
                self.assertIn(MESSAGE, output)
                self.assertNotIn(MARKER, output)

    def test_a_file_that_another_user_owns_is_refused_by_the_number_of_the_user(self) -> None:
        for label, kwargs in (("another user", {"expected": f"{UID + 1}:{FILE_GID}"}), ("another group", {"group": GID})):
            with self.subTest(label):
                code, output = self.verdict(GOOD_CONTENT, **kwargs)
                self.assertNotEqual(0, code)
                self.assertIn(MESSAGE, output)

    def test_the_name_of_the_directory_owner_plays_no_part_in_the_check_of_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            write_file(Path(scratch), GOOD_CONTENT)
            result = run_task_files("unlock_key", ["verify_file.yml"], variables(Path(scratch)) | {"unlock_key_owner": "nobody_such_user"}, verbose=True)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_a_file_with_no_key_line_is_refused_without_printing_it(self) -> None:
        for content in (
            "",
            "OPENBAO_UNLOCK_KEY=\n",
            "OPENBAO_UNLOCK_KEY=" + "ab" * 31 + "\n",
            f"OTHER={MARKER}\n",
            GOOD_CONTENT + GOOD_CONTENT,
            "OPENBAO_UNLOCK_KEY=" + "zz" * 32 + "\n",
        ):
            with self.subTest(content=re.sub(r"[0-9a-f]{8,}", "<hex>", content)):
                code, output = self.verdict(content)
                self.assertNotEqual(0, code)
                self.assertIn(MESSAGE, output)
                self.assertNotIn(MARKER, output)
                self.assertNotIn("zz" * 8, output)
                if content:
                    self.assertNotIn(base64.b64encode(content.encode()).decode(), output)

    def test_a_missing_file_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            result = run_task_files("unlock_key", ["verify_file.yml"], variables(Path(scratch)))
        self.assertNotEqual(0, result.returncode)
        self.assertIn(MESSAGE, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
