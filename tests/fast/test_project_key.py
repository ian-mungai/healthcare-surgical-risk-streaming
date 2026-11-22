"""The project profile's access key never enters Ansible: the launcher reads it on the owner's computer and pipes it to one script on the host.

The launcher exports the key with the AWS command line, checks it in memory and sends it to the host on standard input of one SSH command, which runs a fixed
root-owned script. The script checks the document again and writes the two root-only files. These cases use synthetic marker values and search every argument
list, every environment, every output, every exception text and every file of the run for them: a value is found only on the standard input of the SSH
command and in the two files made for it.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import re
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.infra import host
from scripts.infra.host import HostError, place_project_key
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from scripts.project_config import ROOT
from tests.fast.ansible_support import ROLES, SHARED_MODULE_UTILS
from tests.fast.test_airbyte_role import GROUP as ROLE_GROUP
from tests.fast.test_airbyte_role import OWNER as ROLE_OWNER
from tests.fast.test_airbyte_role import group_variables, run_tasks

SCRIPT = ROLES / "airbyte" / "files" / "airbyte_key_install.py"
OWNER = f"{os.getuid()}:{os.getgid()}"
KEY_ID = "AKIAMARKERKEYID00001"
KEY_VALUE = "marker/value+that/must/not/leak/0001="
OTHER_ID = "AKIAMARKERKEYID00002"
OTHER_VALUE = "marker/value+that/must/not/leak/0002="
TEMPORARY_MARKER = "session-token-marker-that-must-not-leak"
MARKERS = (KEY_ID, KEY_VALUE, OTHER_ID, OTHER_VALUE, TEMPORARY_MARKER)
PROFILE = "example_project_dev"
ADDRESS = "platform-dev.example.ts.net"


def exported(key_id: str = KEY_ID, value: str = KEY_VALUE, **more: object) -> str:
    return json.dumps({"Version": 1, "AccessKeyId": key_id, "SecretAccessKey": value, **more})


def payload(key_id: str = KEY_ID, value: str = KEY_VALUE) -> str:
    return json.dumps({"aws_access_key_id": key_id, "aws_secret_access_key": value})


def load_script() -> ModuleType:
    sys.path.insert(0, str(SHARED_MODULE_UTILS))
    try:
        spec = importlib.util.spec_from_file_location("airbyte_key_install", SCRIPT)
        if spec is None or spec.loader is None:
            raise ImportError("the script cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules["airbyte_key_install"] = module
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(SHARED_MODULE_UTILS))
    return module


INSTALL = load_script()


class Outcome:
    """What a stand-in program does: it returns a result, or raises."""

    def __init__(self, code: int = 0, out: str = "", err: str = "", raises: Exception | None = None) -> None:
        self.code, self.out, self.err, self.raises = code, out, err, raises


class Launcher:
    """The launcher's two external programs, recorded: every program, argument list and keyword the launcher gave them."""

    def __init__(self, aws: Outcome, ssh: Outcome) -> None:
        self.outcomes = {"aws": aws, "ssh": ssh}
        self.calls: list[tuple[str, list[str], dict[str, Any]]] = []

    def __call__(self, program: str, args: list[str], **kwargs: Any) -> CompletedProcess[str]:
        self.calls.append((program, list(args), dict(kwargs)))
        outcome = self.outcomes[program]
        if outcome.raises:
            raise outcome.raises
        return CompletedProcess([program], outcome.code, outcome.out, outcome.err)


class LauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="project_key_launcher_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

    def run_launcher(self, aws: Outcome, ssh: Outcome | None = None) -> tuple[Launcher, str | None, str]:
        launcher = Launcher(aws, ssh or Outcome(0, "placed\n", f"banner {KEY_VALUE}"))
        output, errors = io.StringIO(), io.StringIO()
        message: str | None = None
        with (
            mock.patch.object(host, "run_command", launcher),
            mock.patch.object(host, "ROOT", self.folder),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            try:
                place_project_key("run-one", ADDRESS, PROFILE)
            except HostError as error:
                message = str(error)
        self.assert_no_marker(launcher, message or "", output.getvalue() + errors.getvalue())
        return launcher, message, output.getvalue()

    def assert_no_marker(self, launcher: Launcher, *texts: str) -> None:
        """No marker in any argument list, any environment, any output, any exception text and any file under the run's folder."""
        for program, args, kwargs in launcher.calls:
            for marker in MARKERS:
                self.assertFalse(any(marker in argument for argument in args), f"{program} argument list")
                self.assertNotIn(marker, json.dumps(kwargs.get("env") or {}), f"{program} environment")
                if program == "aws":
                    self.assertIsNone(kwargs.get("input_text"), "the export has no input")
        for text in texts:
            for marker in MARKERS:
                self.assertNotIn(marker, text)
        files = "".join(path.read_text(errors="replace") for path in self.folder.rglob("*") if path.is_file())
        for marker in MARKERS:
            self.assertNotIn(marker, files)

    def test_control_the_key_is_exported_checked_and_piped_to_the_script_and_nowhere_else(self) -> None:
        launcher, message, output = self.run_launcher(Outcome(0, exported()))
        self.assertIsNone(message)
        self.assertEqual(["aws", "ssh"], [program for program, _, _ in launcher.calls])
        _, aws_args, aws_kwargs = launcher.calls[0]
        self.assertEqual(["configure", "export-credentials", "--profile", PROFILE, "--format", "process"], aws_args)
        self.assertGreater(aws_kwargs["timeout"], 0)
        _, ssh_args, ssh_kwargs = launcher.calls[1]
        self.assertEqual(["sudo", host.KEY_SCRIPT], ssh_args[-2:])
        self.assertIn(f"ubuntu@{ADDRESS}", ssh_args)
        self.assertEqual({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE}, json.loads(ssh_kwargs["input_text"]))
        self.assertGreater(ssh_kwargs["timeout"], 0)
        self.assertEqual("the project profile's access key is placed on the host\n", output)

    def test_a_second_run_is_reported_as_unchanged(self) -> None:
        _, message, output = self.run_launcher(Outcome(0, exported()), Outcome(0, "unchanged\n"))
        self.assertIsNone(message)
        self.assertEqual("the project profile's access key is in place on the host and unchanged\n", output)

    def refused(self, aws: Outcome, ssh: Outcome | None, expected: str, *, ssh_called: bool) -> None:
        launcher, message, _ = self.run_launcher(aws, ssh)
        self.assertEqual(expected, message)
        self.assertEqual(ssh_called, any(program == "ssh" for program, _, _ in launcher.calls))

    def test_an_export_that_fails_is_a_fixed_message_with_the_exit_status_and_no_output(self) -> None:
        self.refused(Outcome(253, f"error {KEY_VALUE}", f"error {KEY_ID}"), None, host.KEY_MESSAGES["export_failed"].format(status=253), ssh_called=False)

    def test_an_export_that_times_out_or_cannot_start_is_a_fixed_message(self) -> None:
        self.refused(
            Outcome(raises=TimeoutExpired("aws", 60)), None, host.KEY_MESSAGES["export_timeout"].format(seconds=host.KEY_EXPORT_SECONDS), ssh_called=False
        )
        self.refused(Outcome(raises=FileNotFoundError(f"aws {KEY_VALUE}")), None, host.KEY_MESSAGES["no_aws"], ssh_called=False)

    def test_a_profile_that_yields_a_session_token_is_refused_before_anything_is_sent(self) -> None:
        for token in (TEMPORARY_MARKER, ""):
            with self.subTest(token=token):
                self.refused(Outcome(0, exported(SessionToken=token)), None, host.KEY_MESSAGES["session_token"], ssh_called=False)

    def test_an_answer_that_is_not_a_static_key_is_refused_before_anything_is_sent(self) -> None:
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
                self.refused(Outcome(0, text), None, host.KEY_MESSAGES["not_a_static_key"], ssh_called=False)

    def test_an_answer_with_empty_values_is_refused_before_anything_is_sent(self) -> None:
        for text in ("", "  \n", exported("", KEY_VALUE), exported(KEY_ID, "")):
            with self.subTest(text=text[:16]):
                self.refused(Outcome(0, text), None, host.KEY_MESSAGES["empty"], ssh_called=False)

    def test_an_ssh_that_fails_or_times_out_is_a_fixed_message(self) -> None:
        self.refused(
            Outcome(0, exported()),
            Outcome(255, f"placed {KEY_VALUE}", f"Permission denied {KEY_ID}"),
            host.KEY_MESSAGES["ssh_failed"].format(status=255),
            ssh_called=True,
        )
        self.refused(
            Outcome(0, exported()),
            Outcome(raises=TimeoutExpired("ssh", 60)),
            host.KEY_MESSAGES["ssh_timeout"].format(seconds=host.KEY_SEND_SECONDS),
            ssh_called=True,
        )

    def test_each_refusal_of_the_script_is_a_fixed_message(self) -> None:
        for code in ("oversized", "not_json", "bad_keys", "session_token", "empty", "bad_form", "cannot_write"):
            with self.subTest(code=code):
                self.refused(Outcome(0, exported()), Outcome(3, f"{code}\n", f"text {KEY_VALUE}"), host.KEY_MESSAGES[f"script_{code}"], ssh_called=True)

    def test_an_answer_of_the_script_that_is_not_a_known_code_is_a_fixed_message_and_is_never_shown(self) -> None:
        self.refused(Outcome(0, exported()), Outcome(0, f"{KEY_VALUE}\n"), host.KEY_MESSAGES["script_unknown"], ssh_called=True)
        self.refused(Outcome(0, exported()), Outcome(3, "placed\n"), host.KEY_MESSAGES["script_unknown"], ssh_called=True)

    def test_the_profile_is_checked_as_a_profile_name_before_the_export(self) -> None:
        launcher = Launcher(Outcome(0, exported()), Outcome(0, "placed\n"))
        with mock.patch.object(host, "run_command", launcher), mock.patch.object(host, "ROOT", self.folder):
            for profile in ("", "has space", "a;b", "$(x)"):
                with self.subTest(profile=profile), self.assertRaises(HostError):
                    place_project_key("run-one", ADDRESS, profile)
        self.assertEqual([], launcher.calls)

    def test_the_script_path_is_the_one_the_role_installs(self) -> None:
        variables = group_variables()
        installed = str(variables["airbyte_key_script"]).replace("{{ airbyte_bin_directory }}", str(variables["airbyte_bin_directory"]))
        self.assertEqual(installed, host.KEY_SCRIPT)


class ScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="project_key_script_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.owner = (os.getuid(), os.getgid())

    def path(self, name: str) -> Path:
        return self.folder / "airbyte" / name

    def run_main(self, data: bytes | str) -> tuple[int, str, str]:
        raw = data.encode() if isinstance(data, str) else data
        output, errors = io.StringIO(), io.StringIO()
        stdin = io.TextIOWrapper(io.BytesIO(raw))
        with (
            mock.patch.object(sys, "stdin", stdin),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = INSTALL.main(["--directory", str(self.folder), "--owner", OWNER])
        return code, output.getvalue(), errors.getvalue()

    def test_control_a_static_key_is_placed_in_two_files_for_their_owner_only_and_the_one_line_is_the_code(self) -> None:
        code, output, errors = self.run_main(payload())
        self.assertEqual((0, "placed\n", ""), (code, output, errors))
        for name, value in (("aws_access_key_id", KEY_ID), ("aws_secret_access_key", KEY_VALUE)):
            self.assertEqual(value, self.path(name).read_text())
            self.assertEqual(0o400, stat.S_IMODE(self.path(name).stat().st_mode))
            self.assertEqual(self.owner, (self.path(name).stat().st_uid, self.path(name).stat().st_gid))
        self.assertEqual(0o700, stat.S_IMODE(self.path("aws_access_key_id").parent.stat().st_mode))

    def test_a_second_run_changes_nothing_and_a_rotated_key_is_replaced(self) -> None:
        self.run_main(payload())
        self.assertEqual((0, "unchanged\n"), self.run_main(payload())[:2])
        self.assertEqual((0, "placed\n"), self.run_main(payload(OTHER_ID, OTHER_VALUE))[:2])
        self.assertEqual((OTHER_ID, OTHER_VALUE), (self.path("aws_access_key_id").read_text(), self.path("aws_secret_access_key").read_text()))
        self.assertEqual((0, "unchanged\n"), self.run_main(payload(OTHER_ID, OTHER_VALUE))[:2])

    def test_an_empty_or_short_final_file_is_replaced_and_a_file_with_another_mode_is_repaired(self) -> None:
        self.run_main(payload())
        for content in ("", "AKIA"):
            self.path("aws_access_key_id").chmod(0o600)
            self.path("aws_access_key_id").write_text(content)
            self.path("aws_access_key_id").chmod(0o400)
            self.assertEqual((0, "placed\n"), self.run_main(payload())[:2])
            self.assertEqual(KEY_ID, self.path("aws_access_key_id").read_text())
        self.path("aws_secret_access_key").chmod(0o644)
        self.assertEqual((0, "placed\n"), self.run_main(payload())[:2])
        self.assertEqual(0o400, stat.S_IMODE(self.path("aws_secret_access_key").stat().st_mode))

    def test_the_leftover_of_an_interrupted_write_is_removed_also_when_nothing_changes(self) -> None:
        self.run_main(payload())
        leftover = self.path(".aws_secret_access_key.0badf00d.new")
        leftover.write_text(OTHER_VALUE)
        self.assertEqual((0, "unchanged\n"), self.run_main(payload())[:2])
        self.assertFalse(leftover.exists())

    def test_a_write_that_is_cut_short_leaves_no_file(self) -> None:
        with mock.patch("secret_files.os.fsync", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.run_main(payload())
        self.assertEqual([], sorted(item.name for item in (self.folder / "airbyte").glob("*")) if (self.folder / "airbyte").exists() else [])

    def refused(self, data: bytes | str, code: str) -> None:
        status, output, errors = self.run_main(data)
        self.assertEqual((3, f"{code}\n", ""), (status, output, errors))
        for marker in MARKERS:
            self.assertNotIn(marker, output + errors)
        self.assertFalse((self.folder / "airbyte").exists(), "a refused document writes nothing")

    def test_a_document_that_is_not_two_text_values_is_refused_by_code_and_writes_nothing(self) -> None:
        self.refused("x" * 5000, "oversized")
        self.refused(f"not json {KEY_VALUE}", "not_json")
        self.refused("[]", "not_json")
        self.refused(b"\xff\xfe", "not_json")
        self.refused(json.dumps({"aws_access_key_id": KEY_ID}), "bad_keys")
        self.refused(json.dumps({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE, "extra": "x"}), "bad_keys")
        self.refused(json.dumps({"AccessKeyId": KEY_ID, "SecretAccessKey": KEY_VALUE}), "bad_keys")
        self.refused(json.dumps({"aws_access_key_id": 1, "aws_secret_access_key": KEY_VALUE}), "bad_form")
        self.refused(json.dumps({"aws_access_key_id": "not an id", "aws_secret_access_key": KEY_VALUE}), "bad_form")
        self.refused(json.dumps({"aws_access_key_id": KEY_ID, "aws_secret_access_key": "short"}), "bad_form")

    def test_a_session_token_in_any_form_is_refused_by_code(self) -> None:
        for name in ("SessionToken", "aws_session_token", "session_token", "Token"):
            with self.subTest(name=name):
                self.refused(json.dumps({"aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE, name: TEMPORARY_MARKER}), "session_token")

    def test_empty_values_are_refused_by_code(self) -> None:
        self.refused(json.dumps({"aws_access_key_id": "", "aws_secret_access_key": KEY_VALUE}), "empty")
        self.refused(json.dumps({"aws_access_key_id": KEY_ID, "aws_secret_access_key": ""}), "empty")

    def test_a_file_that_cannot_be_written_is_the_code_cannot_write_and_nothing_else(self) -> None:
        failure = OSError(1, f"Operation not permitted {KEY_VALUE}")
        with mock.patch("secret_files.os.fchown", side_effect=failure):
            status, output, errors = self.run_main(payload())
        self.assertEqual((3, "cannot_write\n", ""), (status, output, errors))

    def test_the_script_runs_as_a_program_reads_standard_input_and_prints_one_code(self) -> None:
        shutil_path = self.folder / "bin"
        shutil_path.mkdir()
        for name, source in (("airbyte_key_install.py", SCRIPT), ("secret_files.py", SHARED_MODULE_UTILS / "secret_files.py")):
            (shutil_path / name).write_text(source.read_text())
        result = run_command(
            sys.executable,
            [str(shutil_path / "airbyte_key_install.py"), "--directory", str(self.folder / "s"), "--owner", OWNER],
            input_text=payload(),
            timeout=30,
            env={"PATH": "/usr/bin:/bin"},
        )
        self.assertEqual((0, "placed\n", ""), (result.returncode, result.stdout, result.stderr))
        self.assertEqual(KEY_VALUE, (self.folder / "s" / "airbyte" / "aws_secret_access_key").read_text())

    def test_the_script_reads_no_secret_from_an_argument_or_the_environment(self) -> None:
        text = SCRIPT.read_text()
        self.assertNotIn("os.environ", text)
        self.assertNotIn("getenv", text)
        self.assertEqual({"--directory", "--owner"}, set(re.findall(r'"(--[a-z-]+)"', text)))

    def test_the_default_folder_is_the_secrets_folder_of_the_role(self) -> None:
        variables = group_variables()
        self.assertEqual("{{ compose_stack_secrets_directory }}", variables["airbyte_secrets_directory"])
        self.assertEqual("{{ compose_stack_directory }}/secrets", variables["compose_stack_secrets_directory"])
        self.assertEqual(f"{variables['compose_stack_directory']}/secrets", INSTALL.DEFAULT_DIRECTORY)


class RoleTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="project_key_role_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

    def variables(self) -> dict[str, object]:
        return {
            "airbyte_bin_directory": str(self.folder / "bin"),
            "airbyte_key_script": str(self.folder / "bin" / "airbyte_key_install"),
            "airbyte_secrets_directory": str(self.folder / "secrets"),
            "airbyte_owner": ROLE_OWNER,
            "airbyte_group": ROLE_GROUP,
        }

    def test_the_script_and_its_helper_are_installed_for_the_owner_only_and_a_second_run_changes_nothing(self) -> None:
        code, output = run_tasks(["key_script.yml"], self.variables())
        self.assertEqual(0, code, output)
        script, helper = self.folder / "bin" / "airbyte_key_install", self.folder / "bin" / "secret_files.py"
        self.assertEqual(SCRIPT.read_text(), script.read_text())
        self.assertEqual((SHARED_MODULE_UTILS / "secret_files.py").read_text(), helper.read_text())
        self.assertEqual(0o700, stat.S_IMODE(script.stat().st_mode))
        self.assertEqual(0, stat.S_IMODE(helper.stat().st_mode) & 0o077)
        self.assertEqual(0o700, stat.S_IMODE(script.parent.stat().st_mode))
        self.assertEqual(0o700, stat.S_IMODE((self.folder / "secrets").stat().st_mode))
        again_code, again = run_tasks(["key_script.yml"], self.variables())
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, r"changed=0\s")

    def test_the_installed_script_runs_from_where_it_is_installed_and_places_the_key(self) -> None:
        run_tasks(["key_script.yml"], self.variables())
        result = run_command(
            sys.executable,
            [str(self.folder / "bin" / "airbyte_key_install"), "--directory", str(self.folder / "secrets"), "--owner", OWNER],
            input_text=payload(),
            timeout=30,
            env={"PATH": "/usr/bin:/bin"},
        )
        self.assertEqual((0, "placed\n"), (result.returncode, result.stdout))
        self.assertEqual(KEY_ID, (self.folder / "secrets" / "airbyte" / "aws_access_key_id").read_text())


class NoKeyInAnsibleTest(unittest.TestCase):
    def files(self) -> list[Path]:
        return sorted(
            path
            for path in (ROOT / "ansible").rglob("*")
            if path.is_file() and path.suffix in (".yml", ".py", ".j2", ".cfg", ".yaml") and "__pycache__" not in path.parts
        )

    def test_no_file_under_ansible_refers_to_the_export_or_the_key_values(self) -> None:
        for forbidden in ("export-credentials", "airbyte_aws_profile", "airbyte_aws_cli", "airbyte_aws_key", "aws_key.yml"):
            with self.subTest(forbidden=forbidden):
                self.assertEqual([], [str(path.relative_to(ROOT)) for path in self.files() if forbidden in path.read_text()])

    def test_no_file_of_the_airbyte_deployment_names_the_fields_of_an_exported_credential(self) -> None:
        """The credential helper of the workload roles reads other credentials (its own role sessions); every other file is held to this."""
        for forbidden in ("AccessKeyId", "SecretAccessKey", "SessionToken"):
            with self.subTest(forbidden=forbidden):
                held = [path for path in self.files() if "workload_access" not in path.parts]
                self.assertEqual([], [str(path.relative_to(ROOT)) for path in held if forbidden in path.read_text()])

    def test_the_names_of_the_two_key_files_appear_only_in_the_script_that_writes_them_and_the_module_that_reads_them(self) -> None:
        holders = {path.name for path in self.files() if "aws_secret_access_key" in path.read_text()}
        self.assertEqual({"airbyte_key_install.py", "airbyte_connection.py"}, holders)

    def test_no_task_or_module_of_the_role_takes_a_key_as_an_argument(self) -> None:
        for path in (ROOT / "ansible" / "roles" / "airbyte" / "tasks").glob("*.yml"):
            with self.subTest(task_file=path.name):
                self.assertNotRegex(path.read_text(), r"(?i)\b(secret_access_key|access_key_id|exported)\b\s*:")

    def test_the_launcher_has_no_ansible_setting_guard_because_ansible_never_holds_the_key(self) -> None:
        text = (ROOT / "scripts" / "infra" / "host.py").read_text()
        for gone in ("refuse_unsafe_ansible_settings", "UNSAFE_ANSIBLE_SETTINGS", "FALSE_SETTING_VALUES"):
            self.assertNotIn(gone, text)


if __name__ == "__main__":
    unittest.main()
