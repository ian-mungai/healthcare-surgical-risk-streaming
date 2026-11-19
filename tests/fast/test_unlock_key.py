"""The unlock key file: created once on the host with a random harmless marker, readable by its owner only and never printed.

The role's task files run through the real ``ansible-playbook`` on this computer against real files in a scratch folder.
The marker is random, so the cases look for its text in the playbook's output instead of a fixed value.
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

from tests.fast.ansible_support import run_task_files

OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
MARKER = "ab" * 32
GOOD_CONTENT = f"OPENBAO_UNLOCK_KEY={MARKER}\n"
MESSAGE = "The unlock key file is missing"


def variables(folder: Path) -> dict[str, object]:
    return {
        "unlock_key_directory": str(folder / "platform"),
        "unlock_key_file": str(folder / "platform" / ".env"),
        "unlock_key_owner": OWNER,
        "unlock_key_group": GROUP,
    }


def write_file(folder: Path, content: str, mode: int = 0o600) -> Path:
    directory = folder / "platform"
    directory.mkdir(exist_ok=True)
    target = directory / ".env"
    target.write_text(content)
    target.chmod(mode)
    return target


class CreateTest(unittest.TestCase):
    def create(self, folder: Path, files: tuple[str, ...] = ("create_file.yml", "protect_file.yml", "verify_file.yml")) -> tuple[int, str]:
        result = run_task_files("unlock_key", list(files), variables(folder), verbose=True)
        return result.returncode, result.stdout + result.stderr

    def test_the_file_is_created_with_a_random_marker_for_its_owner_only(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.create(Path(scratch))
            target = Path(scratch) / "platform" / ".env"
            content = target.read_text()
            mode = stat.S_IMODE(target.stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertRegex(content, r"\AOPENBAO_UNLOCK_KEY=[0-9a-f]{64}\n\Z")
        self.assertEqual(0o600, mode)

    def test_the_new_file_is_for_its_owner_only_from_the_moment_it_exists(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.create(Path(scratch), ("create_file.yml", "verify_file.yml"))
        self.assertEqual(0, code, output)

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

    def test_an_existing_file_is_never_replaced_and_its_mode_is_repaired(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            target = write_file(Path(scratch), "OPENBAO_UNLOCK_KEY=" + "cd" * 32 + "\n", mode=0o644)
            code, output = self.create(Path(scratch))
            content = target.read_text()
            mode = stat.S_IMODE(target.stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual("OPENBAO_UNLOCK_KEY=" + "cd" * 32 + "\n", content)
        self.assertEqual(0o600, mode)


class VerifyTest(unittest.TestCase):
    def verdict(self, content: str, mode: int = 0o600, owner: str = OWNER) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            write_file(Path(scratch), content, mode)
            result = run_task_files("unlock_key", ["verify_file.yml"], variables(Path(scratch)) | {"unlock_key_owner": owner}, verbose=True)
        return result.returncode, result.stdout + result.stderr

    def test_a_file_of_the_owner_with_mode_0600_and_a_key_passes_and_its_content_is_not_shown(self) -> None:
        code, output = self.verdict(GOOD_CONTENT)
        self.assertEqual(0, code, output)
        self.assertNotIn(MARKER, output)
        self.assertNotIn(base64.b64encode(GOOD_CONTENT.encode()).decode(), output)

    def test_a_file_that_others_can_read_is_refused(self) -> None:
        for mode in (0o644, 0o640):
            with self.subTest(mode=oct(mode)):
                code, output = self.verdict(GOOD_CONTENT, mode)
                self.assertNotEqual(0, code)
                self.assertIn(MESSAGE, output)

    def test_a_file_of_another_owner_is_refused(self) -> None:
        code, output = self.verdict(GOOD_CONTENT, owner="nobody_such_user")
        self.assertNotEqual(0, code)
        self.assertIn(MESSAGE, output)

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
