"""The role that deploys the Compose stack: what its task files do to files, through the real ``ansible-playbook`` on this computer.

Starting the stack and the accounts step against a server need Docker and are proven by the acceptance test. These cases cover the file work:
the placed Compose files, the rendered limits and host settings, the password files and their check.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import re
import stat
import tempfile
import unittest
from pathlib import Path

from scripts.stack.limits import load_limits, render_environment
from tests.fast.ansible_support import ROLES, run_playbook

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "compose_stack"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
CHANGED_NONE = re.compile(r"changed=0\s")


def variables(folder: Path) -> dict[str, object]:
    return {
        "compose_stack_directory": str(folder),
        "compose_stack_compose_directory": str(folder / "compose"),
        "compose_stack_secrets_directory": str(folder / "secrets"),
        "compose_stack_owner": OWNER,
        "compose_stack_group": GROUP,
        "compose_stack_postgres_port": 15432,
    }


def run_tasks(task_files: list[str], values: dict[str, object]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        includes = "".join(f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {name}\n" for name in task_files)
        playbook = work / "check.yml"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{includes}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(values)])
    return result.returncode, result.stdout + result.stderr


class PlacementTest(unittest.TestCase):
    def test_the_compose_files_are_placed_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], variables(folder))
            placed = sorted(str(path.relative_to(folder / "compose")) for path in (folder / "compose").rglob("*") if path.is_file())
            again_code, again = run_tasks(["place_files.yml"], variables(folder))
            tracked = sorted(str(path.relative_to(ROOT / "compose")) for path in (ROOT / "compose").rglob("*") if path.is_file())
        self.assertEqual(0, code, output)
        self.assertEqual(tracked, placed)
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)


class RenderTest(unittest.TestCase):
    def test_the_limits_file_is_rendered_once_from_the_tracked_limits_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "compose").mkdir()
            code, output = run_tasks(["render_environment.yml"], variables(folder))
            limits = (folder / "compose" / "limits.env").read_text()
            host = (folder / "compose" / "host.env").read_text()
            again_code, again = run_tasks(["render_environment.yml"], variables(folder))
        self.assertEqual(0, code, output)
        self.assertEqual(render_environment(load_limits()), limits)
        self.assertEqual(f"SECRETS_DIR={folder / 'secrets'}\nPOSTGRES_HOST_PORT=15432\n", host)
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)


class PasswordFilesTest(unittest.TestCase):
    def create(self, folder: Path, tools: str = "nessie\nairbyte\n") -> tuple[int, str]:
        tools_file = folder / "tools.txt"
        tools_file.write_text(tools)
        return run_tasks(["passwords.yml", "verify_secrets.yml"], variables(folder) | {"compose_stack_tools_file": str(tools_file)})

    def test_the_server_and_each_tool_get_a_password_file_for_the_owner_only_and_nothing_prints_a_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.create(folder)
            files = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
            modes = {path.name: stat.S_IMODE(path.stat().st_mode) for path in (folder / "secrets" / "postgres").iterdir()}
            folder_mode = stat.S_IMODE((folder / "secrets" / "postgres").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual(["airbyte.password", "nessie.password", "postgres.password"], sorted(files))
        self.assertEqual({0o600}, set(modes.values()))
        self.assertEqual(0o700, folder_mode)
        for text in files.values():
            self.assertGreaterEqual(len(text), 40)
            self.assertNotIn(text, output)

    def test_a_second_run_changes_nothing_and_keeps_every_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.create(folder)
            before = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
            code, output = self.create(folder)
            after = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_tool_added_later_gets_its_own_password_and_the_others_are_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.create(folder)
            before = (folder / "secrets" / "postgres" / "nessie.password").read_text()
            code, output = self.create(folder, "nessie\nairbyte\nextra\n")
            after = (folder / "secrets" / "postgres" / "nessie.password").read_text()
            added = (folder / "secrets" / "postgres" / "extra.password").is_file()
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertTrue(added)

    def test_an_invalid_tool_name_stops_the_step_before_any_file_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.create(folder, "nessie\nbad-name\n")
            written = (folder / "secrets").exists()
        self.assertNotEqual(0, code)
        self.assertIn("tool name", output)
        self.assertFalse(written)


class SecretsCheckTest(unittest.TestCase):
    def verdict(self, *, mode: int = 0o600, owner: str = OWNER, create: bool = True) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "postgres" / "nessie.password"
            target.parent.mkdir(parents=True)
            target.parent.parent.chmod(0o700)
            target.parent.chmod(0o700)
            if create:
                target.write_text("a-long-random-value")
                target.chmod(mode)
            return run_tasks(["verify_secrets.yml"], variables(folder) | {"compose_stack_accounts": ["nessie"], "compose_stack_owner": owner})

    def test_a_file_of_the_owner_with_mode_0600_passes(self) -> None:
        code, output = self.verdict()
        self.assertEqual(0, code, output)

    def test_a_folder_or_file_that_others_can_read_that_another_user_owns_or_that_is_missing_is_refused_by_name(self) -> None:
        cases = {
            "file mode 0640": (self.verdict(mode=0o640), "The password file of nessie"),
            "file mode 0644": (self.verdict(mode=0o644), "The password file of nessie"),
            "missing file": (self.verdict(create=False), "The password file of nessie"),
            "another owner": (self.verdict(owner="nobody_such_user"), "The password folder"),
        }
        for label, ((code, output), message) in cases.items():
            with self.subTest(label):
                self.assertNotEqual(0, code)
                self.assertIn(message, output)
                self.assertNotIn("a-long-random-value", output)

    def test_a_folder_that_others_can_read_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "postgres" / "nessie.password"
            target.parent.mkdir(parents=True)
            target.parent.parent.chmod(0o700)
            target.parent.chmod(0o755)
            target.write_text("a-long-random-value")
            target.chmod(0o600)
            code, output = run_tasks(["verify_secrets.yml"], variables(folder) | {"compose_stack_accounts": ["nessie"]})
        self.assertNotEqual(0, code)
        self.assertIn("The password folder", output)


if __name__ == "__main__":
    unittest.main()
