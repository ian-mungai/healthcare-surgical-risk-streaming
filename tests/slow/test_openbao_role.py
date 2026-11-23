"""The OpenBao role's check of its secret files: the numeric owner and the mode of each file, read through the real ``ansible-playbook``.

The check reads the list of files and their owners that the bootstrap step reports. A file that anyone but its owner can read, or that another user owns,
is refused by name. A test user cannot give a file to another user, so the owners here are the test user's own groups.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from tests.slow.ansible_support import run_role_tasks

UID, GID = os.getuid(), os.getgid()
OTHER_GID = next((group for group in os.getgroups() if group != GID), GID)
FOLDER_MESSAGE = "The OpenBao secret folder"
FILE_MESSAGE = "The OpenBao secret file"


def verdict(
    *,
    mode: int = 0o400,
    group: int = OTHER_GID,
    expected: tuple[int, int] = (UID, OTHER_GID),
    create: bool = True,
    folder_mode: int = 0o700,
    root_owner: tuple[int, int] = (UID, GID),
) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch) / "secrets" / "openbao"
        folder.mkdir(parents=True)
        folder.chmod(folder_mode)
        target = folder / "nessie.secret_id"
        if create:
            target.write_text("a-long-random-value")
            os.chown(target, UID, group)
            target.chmod(mode)
        files = [{"path": str(target), "owner": f"{expected[0]}:{expected[1]}"}]
        variables: dict[str, object] = {
            "openbao_secret_files": files,
            "openbao_secrets_directory": str(Path(scratch) / "secrets"),
            "secret_files_root_owner": f"{root_owner[0]}:{root_owner[1]}",
        }
        result = run_role_tasks("openbao", ["verify_secrets.yml"], variables, verbose=True)
    return result.returncode, result.stdout + result.stderr


class SecretFilesCheckTest(unittest.TestCase):
    def test_a_private_file_of_its_owner_in_a_private_folder_passes(self) -> None:
        code, output = verdict()
        self.assertEqual(0, code, output)

    def test_a_file_that_anyone_but_its_owner_can_read_or_that_its_owner_can_write_is_refused_by_name_and_its_content_is_not_shown(self) -> None:
        for mode in (0o600, 0o640, 0o644, 0o440, 0o404, 0o444, 0o500, 0o000):
            with self.subTest(mode=oct(mode)):
                code, output = verdict(mode=mode)
                self.assertNotEqual(0, code)
                self.assertIn(FILE_MESSAGE, output)
                self.assertNotIn("a-long-random-value", output)

    def test_a_file_that_another_user_owns_is_refused_by_the_number_of_the_user(self) -> None:
        for label, expected in (("another user", (UID + 1, OTHER_GID)), ("another group", (UID, OTHER_GID + 1))):
            with self.subTest(label):
                code, output = verdict(expected=expected)
                self.assertNotEqual(0, code)
                self.assertIn(FILE_MESSAGE, output)

    def test_a_missing_file_is_refused(self) -> None:
        code, output = verdict(create=False)
        self.assertNotEqual(0, code)
        self.assertIn(FILE_MESSAGE, output)

    def test_a_folder_that_others_can_read_or_that_another_user_owns_is_refused(self) -> None:
        for label, kwargs in (("readable folder", {"folder_mode": 0o755}), ("owned elsewhere", {"root_owner": (UID + 1, GID)})):
            with self.subTest(label):
                code, output = verdict(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn(FOLDER_MESSAGE, output)


if __name__ == "__main__":
    unittest.main()
