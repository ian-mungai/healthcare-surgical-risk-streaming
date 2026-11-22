"""The step that places the project profile's access key on the host for Airbyte's two connectors, with no value anywhere but in its two root-only files.

The deployment runs on the owner's computer, reads the key with the AWS command line and hands it to a module that writes two files under the secrets
folder. These cases run the real ``ansible-playbook`` with a stand-in for the AWS command line that prints marker values, at the most verbose level, and then
search the whole output and every file for the markers: a value is found only in the file made for it. A second run changes nothing, and a rotated key is
replaced. A profile that yields a session token, an AWS command line that fails and a key in another form each stop the run with a clear message and write
nothing.
"""

from __future__ import annotations

import grp
import importlib.util
import json
import os
import pwd
import re
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

from scripts.process import run_command
from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import PLAYBOOK_SECONDS, ROLES, environment, shared_module
from tests.fast.test_airbyte_role import role_tasks

ROLE = ROLES / "airbyte"
MODULE = ROLE / "library" / "airbyte_aws_key.py"
OWNER = f"{os.getuid()}:{os.getgid()}"
KEY_ID = "AKIAMARKERKEYID00001"
KEY_VALUE = "marker/value+that/must/not/leak/0001="
OTHER_ID = "AKIAMARKERKEYID00002"
OTHER_VALUE = "marker/value+that/must/not/leak/0002="
TEMPORARY_MARKER = "session-token-marker-that-must-not-leak"
CHANGED_NONE = re.compile(r"changed=0\s")
CHANGED_ONE = re.compile(r"changed=1\s")


def load_module() -> ModuleType:
    shared_module("secret_files")
    spec = importlib.util.spec_from_file_location("airbyte_aws_key", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError("the module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["airbyte_aws_key"] = module
    spec.loader.exec_module(module)
    return module


KEY_MODULE = load_module()


def exported(key_id: str = KEY_ID, value: str = KEY_VALUE, **more: str) -> str:
    return json.dumps({"Version": 1, "AccessKeyId": key_id, "SecretAccessKey": value, **more})


class ParseTest(unittest.TestCase):
    def code(self, text: str, exit_code: int = 0) -> str:
        """The reason code of a refusal. The error holds the code and no other text, so nothing of an answer can be in it."""
        with self.assertRaises(KEY_MODULE.KeyFormError) as raised:
            KEY_MODULE.parse_export(text, exit_code)
        self.assertEqual(str(raised.exception), raised.exception.code)
        return str(raised.exception.code)

    def test_control_a_static_key_is_read(self) -> None:
        self.assertEqual((KEY_ID, KEY_VALUE), KEY_MODULE.parse_export(exported(), 0))

    def test_the_reason_codes_are_the_fixed_set_of_the_module(self) -> None:
        self.assertEqual(("ok", "session_token", "not_a_static_key", "empty", "export_failed", "cannot_write"), KEY_MODULE.REASONS)

    def test_a_profile_that_yields_a_session_token_is_refused_by_its_code_only(self) -> None:
        self.assertEqual("session_token", self.code(exported(SessionToken=TEMPORARY_MARKER)))
        self.assertEqual("session_token", self.code(exported(SessionToken="")))

    def test_a_failed_export_is_a_code_and_never_its_output(self) -> None:
        self.assertEqual("export_failed", self.code(f"error {KEY_VALUE}", 253))

    def test_an_empty_answer_or_empty_values_are_the_code_empty(self) -> None:
        for text in ("", "  \n", exported("", KEY_VALUE), exported(KEY_ID, "")):
            with self.subTest(text=text[:20]):
                self.assertEqual("empty", self.code(text))

    def test_an_answer_that_is_not_a_static_key_is_the_code_not_a_static_key(self) -> None:
        for name, text in {
            "not json": f"not json {KEY_VALUE}",
            "a list": "[]",
            "no id": json.dumps({"SecretAccessKey": KEY_VALUE}),
            "no secret": json.dumps({"AccessKeyId": KEY_ID}),
            "an id of another form": exported("not an id " + KEY_ID),
            "a secret of another form": exported(KEY_ID, "short"),
            "a number as the id": json.dumps({"AccessKeyId": 1, "SecretAccessKey": KEY_VALUE}),
        }.items():
            with self.subTest(name):
                self.assertEqual("not_a_static_key", self.code(text))


class PlaceTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="airbyte_aws_key_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.owner = (os.getuid(), os.getgid())

    def place(self, key_id: str = KEY_ID, value: str = KEY_VALUE, *, check: bool = False) -> list[str]:
        changed: list[str] = KEY_MODULE.place_key(self.folder, self.owner, key_id, value, check_only=check)
        return changed

    def path(self, name: str) -> Path:
        return self.folder / "airbyte" / name

    def test_each_file_is_made_with_its_value_for_its_owner_only(self) -> None:
        self.assertEqual(["aws_secret_access_key", "aws_access_key_id"], self.place())
        for name, value in (("aws_access_key_id", KEY_ID), ("aws_secret_access_key", KEY_VALUE)):
            self.assertEqual(value, self.path(name).read_text())
            self.assertEqual(0o400, stat.S_IMODE(self.path(name).stat().st_mode))
            self.assertEqual(self.owner, (self.path(name).stat().st_uid, self.path(name).stat().st_gid))

    def test_a_second_run_changes_nothing(self) -> None:
        self.place()
        self.assertEqual([], self.place())

    def test_a_rotated_key_replaces_both_files_and_keeps_the_mode(self) -> None:
        self.place()
        self.assertEqual(["aws_secret_access_key", "aws_access_key_id"], self.place(OTHER_ID, OTHER_VALUE))
        self.assertEqual((OTHER_ID, OTHER_VALUE), (self.path("aws_access_key_id").read_text(), self.path("aws_secret_access_key").read_text()))
        self.assertEqual(0o400, stat.S_IMODE(self.path("aws_access_key_id").stat().st_mode))
        self.assertEqual([], self.place(OTHER_ID, OTHER_VALUE))
        self.assertEqual([], [path.name for path in (self.folder / "airbyte").iterdir() if path.name.startswith(".")])

    def test_a_file_with_another_mode_is_given_mode_0400_and_keeps_its_value(self) -> None:
        self.place()
        self.path("aws_secret_access_key").chmod(0o644)
        self.assertEqual(["aws_secret_access_key"], self.place())
        self.assertEqual(0o400, stat.S_IMODE(self.path("aws_secret_access_key").stat().st_mode))
        self.assertEqual(KEY_VALUE, self.path("aws_secret_access_key").read_text())

    def test_an_empty_or_short_final_file_is_treated_as_absent_and_replaced(self) -> None:
        self.place()
        for content in ("", "AKIA", KEY_ID[:5]):
            with self.subTest(content=content):
                self.path("aws_access_key_id").chmod(0o600)
                self.path("aws_access_key_id").write_text(content)
                self.path("aws_access_key_id").chmod(0o400)
                self.assertEqual(["aws_access_key_id"], self.place())
                self.assertEqual(KEY_ID, self.path("aws_access_key_id").read_text())
                self.assertEqual(0o400, stat.S_IMODE(self.path("aws_access_key_id").stat().st_mode))

    def test_the_leftover_of_an_interrupted_rotation_is_removed_at_the_next_run_and_holds_no_key_afterwards(self) -> None:
        self.place()
        leftover = self.path(".aws_secret_access_key.0badf00d.new")
        leftover.write_text(OTHER_VALUE)
        unrelated = self.path(".aws_secret_access_key.new")
        unrelated.write_text("not this module's temporary file")
        self.place(OTHER_ID, OTHER_VALUE)
        self.assertFalse(leftover.exists())
        self.assertTrue(unrelated.exists())

    def test_the_leftover_of_an_interrupted_write_is_removed_even_when_the_key_has_not_changed(self) -> None:
        self.place()
        leftover = self.path(".aws_access_key_id.0badf00d.new")
        leftover.write_text(KEY_VALUE)
        self.assertEqual([], self.place())
        self.assertFalse(leftover.exists())

    def test_a_write_that_is_interrupted_leaves_neither_an_empty_file_nor_a_new_file_with_the_key(self) -> None:
        with mock.patch("ansible.module_utils.secret_files.os.fsync", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.place()
        names = sorted(item.name for item in (self.folder / "airbyte").glob("*")) if (self.folder / "airbyte").exists() else []
        self.assertEqual([], names)
        self.assertEqual(["aws_secret_access_key", "aws_access_key_id"], self.place())

    def test_a_file_that_cannot_be_written_is_the_code_cannot_write_and_nothing_else(self) -> None:
        failure = OSError(1, f"Operation not permitted {KEY_VALUE}")
        with mock.patch("ansible.module_utils.secret_files.os.fchown", side_effect=failure), self.assertRaises(KEY_MODULE.KeyFormError) as raised:
            self.place()
        self.assertEqual("cannot_write", raised.exception.code)
        self.assertNotIn(KEY_VALUE, str(raised.exception))

    def test_the_check_mode_reports_the_change_and_writes_nothing(self) -> None:
        self.assertEqual(["aws_secret_access_key", "aws_access_key_id"], self.place(check=True))
        self.assertFalse((self.folder / "airbyte").exists())

    def test_a_file_that_cannot_be_made_for_its_owner_leaves_no_half_made_file(self) -> None:
        other = next((group for group in grp.getgrall() if group.gr_gid not in os.getgroups()), None)
        if other is None or os.getuid() == 0:
            self.skipTest("this computer's user can give a file to any group")
        with self.assertRaises(KEY_MODULE.KeyFormError) as raised:
            KEY_MODULE.place_key(self.folder, (os.getuid(), other.gr_gid), KEY_ID, KEY_VALUE)
        self.assertEqual("cannot_write", raised.exception.code)
        self.assertEqual([], list((self.folder / "airbyte").glob("*")))


class PlaybookTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="airbyte_aws_key_play_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.secrets = self.folder / "secrets"
        self.user = pwd.getpwuid(os.getuid()).pw_name

    def fake_cli(self, text: str, status: int = 0, name: str = "aws") -> Path:
        (self.folder / "cli").mkdir(exist_ok=True)
        script = self.folder / "cli" / name
        script.write_text(f"#!/bin/sh\ncat <<'MARKER_END'\n{text}\nMARKER_END\nexit {status}\n")
        script.chmod(0o700)
        return script

    def play(self, cli: Path, *, env: dict[str, str] | None = None, args: tuple[str, ...] = (), **more: object) -> tuple[int, str]:
        variables = {
            "airbyte_aws_cli": str(cli),
            "airbyte_aws_profile": "example_profile",
            "airbyte_secrets_directory": str(self.secrets),
            "secret_files_root_owner": OWNER,
            **more,
        }
        playbook = self.folder / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: aws_key.yml\n"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        result = run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"),
            ["-vvv", *args, "-i", "localhost,", "--extra-vars", json.dumps(variables), str(playbook)],
            cwd=self.folder,
            env=environment(self.folder) | (env or {}),
            timeout=PLAYBOOK_SECONDS,
        )
        return result.returncode, result.stdout + result.stderr

    def everywhere(self) -> str:
        """The output of a run and every byte of every file of the run's folder, except the two key files, as one text."""
        text = ""
        for path in sorted(self.folder.rglob("*")):
            if path.is_file() and not path.is_relative_to(self.secrets / "airbyte") and not path.is_relative_to(self.folder / "cli"):
                text += path.read_text(errors="replace")
        return text

    def assert_no_marker(self, output: str, *markers: str) -> None:
        for marker in markers or (KEY_ID, KEY_VALUE, OTHER_ID, OTHER_VALUE, TEMPORARY_MARKER):
            self.assertNotIn(marker, output)
            self.assertNotIn(marker, self.everywhere())

    def test_control_the_key_is_placed_in_two_files_and_appears_nowhere_else(self) -> None:
        code, output = self.play(self.fake_cli(exported()))
        self.assertEqual(0, code, output)
        self.assert_no_marker(output)
        self.assertEqual(KEY_ID, (self.secrets / "airbyte" / "aws_access_key_id").read_text())
        self.assertEqual(KEY_VALUE, (self.secrets / "airbyte" / "aws_secret_access_key").read_text())
        self.assertEqual(0o400, stat.S_IMODE((self.secrets / "airbyte" / "aws_secret_access_key").stat().st_mode))

    def test_a_second_deployment_changes_nothing_and_a_rotated_key_is_replaced_without_a_trace(self) -> None:
        cli = self.fake_cli(exported())
        self.play(cli)
        code, output = self.play(cli)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        rotated = self.fake_cli(exported(OTHER_ID, OTHER_VALUE), name="aws_rotated")
        code, output = self.play(rotated)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_ONE)
        self.assert_no_marker(output)
        self.assertEqual(OTHER_ID, (self.secrets / "airbyte" / "aws_access_key_id").read_text())
        self.assertEqual(OTHER_VALUE, (self.secrets / "airbyte" / "aws_secret_access_key").read_text())

    def test_a_profile_that_yields_a_session_token_stops_the_run_says_so_and_writes_nothing(self) -> None:
        code, output = self.play(self.fake_cli(exported(SessionToken=TEMPORARY_MARKER)))
        self.assertNotEqual(0, code)
        self.assertIn("session token", output)
        self.assertIn("example_profile", output)
        self.assert_no_marker(output)
        self.assertFalse((self.secrets / "airbyte").exists())

    def test_an_aws_command_line_that_fails_stops_the_run_with_its_exit_status_and_shows_none_of_its_output(self) -> None:
        code, output = self.play(self.fake_cli(f"The config profile could not be found {KEY_VALUE}", 253))
        self.assertNotEqual(0, code)
        self.assertIn("253", output)
        self.assert_no_marker(output)
        self.assertFalse((self.secrets / "airbyte").exists())

    def test_the_safe_settings_at_the_most_verbose_level_leave_no_key_anywhere_including_the_temporary_folders(self) -> None:
        for name, env in {"explicitly off": {"ANSIBLE_DEBUG": "False", "ANSIBLE_KEEP_REMOTE_FILES": "False"}, "absent": {}}.items():
            with self.subTest(name):
                code, output = self.play(self.fake_cli(exported()), env=env)
                self.assertEqual(0, code, output)
                self.assert_no_marker(output)
                self.assertEqual([], [path.name for path in self.folder.rglob("AnsiballZ_*")])

    def unsafe(self, env: dict[str, str], *, args: tuple[str, ...] = (), config: str = "") -> tuple[int, str, bool]:
        """A run with an unsafe Ansible setting: its exit code, its output and whether the AWS command line ran. Nothing of a key may be anywhere."""
        ran = self.folder / "ran"
        cli = self.fake_cli(exported())
        cli.write_text(f"#!/bin/sh\ntouch {ran}\ncat <<'MARKER_END'\n{exported()}\nMARKER_END\n")
        extra = dict(env)
        if config:
            (self.folder / "ansible.cfg").write_text(config)
            extra["ANSIBLE_CONFIG"] = str(self.folder / "ansible.cfg")
        code, output = self.play(cli, env=extra, args=args)
        self.assert_no_marker(output)
        self.assertFalse((self.secrets / "airbyte").exists())
        return code, output, ran.exists()

    def test_a_deployment_with_debug_logging_on_stops_before_the_key_is_read_and_names_the_setting(self) -> None:
        for env in ({"ANSIBLE_DEBUG": "True"}, {"ANSIBLE_DEBUG": "1"}):
            with self.subTest(env=env):
                code, output, ran = self.unsafe(env)
                self.assertNotEqual(0, code)
                self.assertIn("ANSIBLE_DEBUG", output)
                self.assertFalse(ran)

    def test_a_deployment_that_keeps_remote_files_stops_before_the_key_is_read_and_names_the_setting(self) -> None:
        code, output, ran = self.unsafe({"ANSIBLE_KEEP_REMOTE_FILES": "True"})
        self.assertNotEqual(0, code)
        self.assertIn("ANSIBLE_KEEP_REMOTE_FILES", output)
        self.assertFalse(ran)

    def test_the_configuration_file_form_of_each_setting_stops_the_run_too(self) -> None:
        for config in ("[defaults]\ndebug = True\n", "[defaults]\nkeep_remote_files = True\n"):
            with self.subTest(config=config.strip()):
                code, output, ran = self.unsafe({}, config=config)
                self.assertNotEqual(0, code)
                self.assertFalse(ran)

    def test_the_check_mode_with_an_unsafe_setting_stops_too(self) -> None:
        code, output, ran = self.unsafe({"ANSIBLE_DEBUG": "True"}, args=("--check",))
        self.assertNotEqual(0, code)
        self.assertFalse(ran)

    def test_a_deployment_without_a_profile_is_refused_before_the_command_line_runs(self) -> None:
        marker = self.folder / "ran"
        cli = self.fake_cli(exported())
        cli.write_text(f"#!/bin/sh\ntouch {marker}\n")
        code, output = self.play(cli, airbyte_aws_profile="")
        self.assertNotEqual(0, code)
        self.assertIn("airbyte_aws_profile", output)
        self.assertFalse(marker.exists())


class TaskRulesTest(unittest.TestCase):
    def tasks(self) -> list[dict[str, object]]:
        return role_tasks()["aws_key.yml"]

    def test_the_command_that_reads_the_key_never_shows_its_output_has_a_deadline_and_runs_on_the_owners_computer(self) -> None:
        reader = next(task for task in self.tasks() if "ansible.builtin.command" in task)
        self.assertIs(True, reader["no_log"])
        self.assertIn("timeout", reader)
        self.assertEqual("localhost", reader["delegate_to"])
        self.assertIs(False, reader["become"])
        argv = [str(item) for item in reader["ansible.builtin.command"]["argv"]]  # type: ignore[index]
        self.assertEqual(["configure", "export-credentials", "--profile", "{{ airbyte_aws_profile }}", "--format", "process"], argv[1:])

    def test_the_module_takes_the_key_as_arguments_that_ansible_hides_and_no_task_puts_a_value_on_a_command_line(self) -> None:
        text = MODULE.read_text()
        for name in ("exported",):
            self.assertRegex(text, rf'"{name}": \{{[^}}]*"no_log": True')
        for task in self.tasks():
            if "ansible.builtin.command" in task:
                self.assertNotIn("airbyte_aws_export", json.dumps(task["ansible.builtin.command"]))

    def test_the_first_task_of_the_key_path_refuses_the_unsafe_settings_through_ansibles_own_configuration(self) -> None:
        first = self.tasks()[0]
        text = json.dumps(first)
        self.assertIn("ansible.builtin.assert", first)
        for name in ("DEFAULT_DEBUG", "DEFAULT_KEEP_REMOTE_FILES", "ANSIBLE_DEBUG", "ANSIBLE_KEEP_REMOTE_FILES"):
            self.assertIn(name, text)
        self.assertIn("ansible.builtin.config", text)

    def test_the_placement_task_hides_its_result_never_fails_itself_and_the_next_task_prints_a_message_from_a_fixed_table(self) -> None:
        tasks = self.tasks()
        placing = next(index for index, task in enumerate(tasks) if "airbyte_aws_key" in task)
        self.assertIs(True, tasks[placing]["no_log"])
        self.assertIs(False, tasks[placing]["failed_when"])
        following = tasks[placing + 1]
        self.assertIn("ansible.builtin.assert", following)
        self.assertIsNot(True, following.get("no_log"))
        table = load_yaml((ROLE / "vars" / "main.yml").read_text())["airbyte_aws_key_messages"]  # type: ignore[index]
        self.assertEqual(set(KEY_MODULE.REASONS) - {"ok"}, set(table))
        for reason, message in table.items():
            with self.subTest(reason=reason):
                self.assertEqual(
                    [], [name for name in re.findall(r"\{\{\s*([\w.]+)\s*\}\}", message) if name not in ("airbyte_aws_profile", "airbyte_aws_export.rc")]
                )

    def test_the_key_is_placed_before_the_connection_is_applied(self) -> None:
        tasks = role_tasks()["connection.yml"]
        key = next(index for index, task in enumerate(tasks) if task.get("ansible.builtin.include_tasks") == "aws_key.yml")
        module = next(index for index, task in enumerate(tasks) if "airbyte_connection" in task)
        self.assertLess(key, module)


if __name__ == "__main__":
    unittest.main()
