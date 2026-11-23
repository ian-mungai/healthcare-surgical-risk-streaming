"""The project profile's access key never enters Ansible: the launcher reads it on the owner's computer and pipes it to one script on the host.

The launcher exports the key with the AWS command line, checks it in memory and sends it to the host on standard input of one SSH command, which runs a fixed
root-owned script. The script checks the document again and writes the two root-only files. These cases use synthetic marker values and search every argument
list, every environment, every output, every exception text and every file of the run for them: a value is found only on the standard input of the SSH
command and in the two files made for it.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.machinery
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
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import ROLES, SHARED_MODULE_UTILS
from tests.fast.test_airbyte_role import GROUP as ROLE_GROUP
from tests.fast.test_airbyte_role import OWNER as ROLE_OWNER
from tests.fast.test_airbyte_role import group_variables, role_tasks, run_tasks

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


INSTALL: Any = None
INSTALLED = Path()
SUDOERS = ROLES / "airbyte" / "files" / "airbyte_key_install.sudoers"
SHEBANG = "#!/usr/bin/python3 -I"
VISUDO = "/usr/sbin/visudo"
POLICY_DIGEST = hashlib.sha256(SUDOERS.read_bytes()).hexdigest()


def install_form(folder: Path) -> Path:
    """The script and its helper side by side, as the role installs them, and the path of the script."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "secret_files.py").write_text((SHARED_MODULE_UTILS / "secret_files.py").read_text())
    script = folder / "airbyte_key_install"
    script.write_text(SCRIPT.read_text())
    script.chmod(0o700)
    return script


def load_script(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("airbyte_key_install", path, loader=importlib.machinery.SourceFileLoader("airbyte_key_install", str(path)))
    if spec is None or spec.loader is None:
        raise ImportError("the script cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules["airbyte_key_install"] = module
    spec.loader.exec_module(module)
    return module


def setUpModule() -> None:
    global INSTALL, INSTALLED
    folder = tempfile.TemporaryDirectory(prefix="project_key_installed_")
    unittest.addModuleCleanup(folder.cleanup)
    INSTALLED = Path(folder.name)
    INSTALL = load_script(install_form(INSTALLED / "bin"))


class Outcome:
    """What a stand-in program does: it returns a result, or raises."""

    def __init__(self, code: int = 0, out: str = "", err: str = "", raises: Exception | None = None) -> None:
        self.code, self.out, self.err, self.raises = code, out, err, raises


class Launcher:
    """The launcher's two external programs, recorded: every program, argument list and keyword the launcher gave them."""

    def __init__(self, aws: Outcome, ssh: Outcome, check: Outcome | None = None) -> None:
        self.outcomes = {"aws": aws, "ssh": ssh}
        self.check = check or Outcome(0, f"{POLICY_DIGEST}  {host.SUDOERS_FILE}\n")
        self.calls: list[tuple[str, list[str], dict[str, Any]]] = []

    def __call__(self, program: str, args: list[str], **kwargs: Any) -> CompletedProcess[str]:
        self.calls.append((program, list(args), dict(kwargs)))
        outcome = self.check if program == "ssh" and args[-4:] == ["sudo", "-n", "/usr/bin/sha256sum", host.SUDOERS_FILE] else self.outcomes[program]
        if outcome.raises:
            raise outcome.raises
        return CompletedProcess([program], outcome.code, outcome.out, outcome.err)


class LauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="project_key_launcher_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)

    def run_launcher(self, aws: Outcome, ssh: Outcome | None = None, check: Outcome | None = None) -> tuple[Launcher, str | None, str]:
        launcher = Launcher(aws, ssh or Outcome(0, "placed\n", f"banner {KEY_VALUE}"), check)
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
        self.assertEqual(["ssh", "aws", "ssh"], [program for program, _, _ in launcher.calls], "the policy check on the host comes before the export is read")
        _, check_args, check_kwargs = launcher.calls[0]
        self.assertEqual(["sudo", "-n", "/usr/bin/sha256sum", host.SUDOERS_FILE], check_args[-4:])
        self.assertIn(f"ubuntu@{ADDRESS}", check_args)
        self.assertIsNone(check_kwargs.get("input_text"))
        self.assertGreater(check_kwargs["timeout"], 0)
        _, aws_args, aws_kwargs = launcher.calls[1]
        self.assertEqual(["configure", "export-credentials", "--profile", PROFILE, "--format", "process"], aws_args)
        self.assertGreater(aws_kwargs["timeout"], 0)
        _, ssh_args, ssh_kwargs = launcher.calls[2]
        self.assertEqual(["sudo", "/usr/bin/python3", "-I", "/opt/platform/bin/airbyte_key_install"], ssh_args[-4:])
        self.assertEqual(["/usr/bin/python3", "-I", host.KEY_SCRIPT], host.KEY_COMMAND)
        self.assertEqual(["sudo", *host.KEY_COMMAND], ssh_args[-4:], "the remote command is the interpreter in isolated mode and the script, and nothing else")
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
        sends = [args for program, args, _ in launcher.calls if program == "ssh" and args[-4:] == ["sudo", *host.KEY_COMMAND][-4:]]
        self.assertEqual(ssh_called, bool(sends))

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

    def checked(self, check: Outcome, expected: str) -> None:
        launcher, message, _ = self.run_launcher(Outcome(0, exported()), None, check)
        self.assertEqual(expected, message)
        self.assertEqual(["ssh"], [program for program, _, _ in launcher.calls], "a refused policy check reads no export and sends nothing")

    def test_a_host_whose_sudo_policy_file_is_missing_or_unreadable_is_refused_before_the_export_is_read(self) -> None:
        self.checked(Outcome(1, "", f"sudo: no such file {KEY_VALUE}"), host.KEY_MESSAGES["policy_missing"])
        self.checked(Outcome(2, f"{POLICY_DIGEST}  {host.SUDOERS_FILE}\n"), host.KEY_MESSAGES["policy_missing"])

    def test_a_host_whose_sudo_policy_file_differs_from_the_repositorys_is_refused_before_the_export_is_read(self) -> None:
        for text in (f"{'0' * 64}  {host.SUDOERS_FILE}\n", "", "not a digest\n", f"{POLICY_DIGEST[:-1]}  x\n", f"{POLICY_DIGEST.upper()[:63]}\n"):
            with self.subTest(text=text[:20]):
                self.checked(Outcome(0, text), host.KEY_MESSAGES["policy_differs"])

    def test_a_policy_check_that_fails_or_times_out_over_ssh_is_a_fixed_message(self) -> None:
        self.checked(Outcome(255, "", "Permission denied"), host.KEY_MESSAGES["ssh_failed"].format(status=255))
        self.checked(Outcome(raises=TimeoutExpired("ssh", 60)), host.KEY_MESSAGES["ssh_timeout"].format(seconds=host.KEY_SEND_SECONDS))

    def test_the_digest_is_that_of_the_repositorys_file_and_the_file_names_the_launchers_command(self) -> None:
        self.assertEqual(SUDOERS.resolve(), host.KEY_POLICY_SOURCE.resolve())
        self.assertEqual("/etc/sudoers.d/airbyte_key_install", host.SUDOERS_FILE)

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
            code = INSTALL.main(["--directory", str(self.folder), "--owner", OWNER, "--container-owner", OWNER])
        return code, output.getvalue(), errors.getvalue()

    def test_control_a_static_key_is_placed_in_two_files_for_their_owner_only_and_the_one_line_is_the_code(self) -> None:
        code, output, errors = self.run_main(payload())
        self.assertEqual((0, "placed\n", ""), (code, output, errors))
        for name, value in (("aws_access_key_id", KEY_ID), ("aws_secret_access_key", KEY_VALUE)):
            self.assertEqual(value, self.path(name).read_text())
            self.assertEqual(0o400, stat.S_IMODE(self.path(name).stat().st_mode))
            self.assertEqual(self.owner, (self.path(name).stat().st_uid, self.path(name).stat().st_gid))
        self.assertEqual(0o700, stat.S_IMODE(self.path("aws_access_key_id").parent.stat().st_mode))

    def copy(self, name: str) -> Path:
        return self.folder / "airbyte_api" / name

    def test_the_key_is_also_placed_as_the_two_copies_that_the_code_location_mounts_for_the_container_user_only(self) -> None:
        self.assertEqual((0, "placed\n", ""), self.run_main(payload()))
        for name, value in (("aws_access_key_id", KEY_ID), ("aws_secret_access_key", KEY_VALUE)):
            self.assertEqual(value, self.copy(name).read_text(), name)
            self.assertEqual(0o400, stat.S_IMODE(self.copy(name).stat().st_mode))
            self.assertEqual(self.owner, (self.copy(name).stat().st_uid, self.copy(name).stat().st_gid))
        self.assertEqual(0o700, stat.S_IMODE(self.copy("aws_access_key_id").parent.stat().st_mode))
        self.assertEqual({"aws_access_key_id", "aws_secret_access_key"}, {item.name for item in self.copy("x").parent.iterdir()}, "no file but the two copies")

    def test_the_root_files_are_given_to_the_owner_and_the_copies_to_the_container_owner(self) -> None:
        given: list[tuple[int, int]] = []
        real = os.fchown

        def record(descriptor: int, user: int, group: int) -> None:
            given.append((user, group))
            real(descriptor, self.owner[0], self.owner[1])

        output, errors = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(sys, "stdin", io.TextIOWrapper(io.BytesIO(payload().encode()))),
            mock.patch("secret_files.os.fchown", side_effect=record),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = INSTALL.main(["--directory", str(self.folder), "--owner", "0:0", "--container-owner", "10001:10001"])
        self.assertEqual((0, "placed\n"), (code, output.getvalue()))
        self.assertEqual([(0, 0), (0, 0), (10001, 10001), (10001, 10001)], given)

    def test_without_the_option_the_copies_belong_to_the_code_locations_user_and_group_the_compose_service_runs_as(self) -> None:
        self.assertEqual("10001:10001", INSTALL.DEFAULT_CONTAINER_OWNER)
        document: Any = load_yaml((ROOT / "compose" / "services" / "dagster_code_location.yml").read_text())
        service = document["services"]["dagster_code_location"]
        self.assertEqual(INSTALL.DEFAULT_CONTAINER_OWNER, service["user"])
        defaults = re.findall(
            r"airbyte_record_container_(?:user|group) \| default\('([0-9]+)'", (ROLES / "airbyte" / "tasks" / "record_client_files.yml").read_text()
        )
        self.assertEqual(["10001", "10001"], defaults, "the role's defaults for the four files it places are the same user and group")

    def test_a_copy_that_is_missing_changed_or_given_another_mode_is_repaired_and_the_run_says_placed(self) -> None:
        self.run_main(payload())
        self.copy("aws_secret_access_key").unlink()
        self.assertEqual((0, "placed\n"), self.run_main(payload())[:2])
        self.assertEqual(KEY_VALUE, self.copy("aws_secret_access_key").read_text())
        self.copy("aws_access_key_id").chmod(0o640)
        self.assertEqual((0, "placed\n"), self.run_main(payload())[:2])
        self.assertEqual(0o400, stat.S_IMODE(self.copy("aws_access_key_id").stat().st_mode))
        self.copy("aws_access_key_id").chmod(0o600)
        self.copy("aws_access_key_id").write_text(OTHER_ID)
        self.copy("aws_access_key_id").chmod(0o400)
        self.assertEqual((0, "placed\n"), self.run_main(payload())[:2])
        self.assertEqual(KEY_ID, self.copy("aws_access_key_id").read_text())
        self.assertEqual((0, "unchanged\n"), self.run_main(payload())[:2])

    def test_a_rotated_key_replaces_the_copies_too_and_the_leftover_of_an_interrupted_copy_is_removed(self) -> None:
        self.run_main(payload())
        leftover = self.copy(".aws_secret_access_key.0badf00d.new")
        leftover.write_text(OTHER_VALUE)
        self.assertEqual((0, "placed\n"), self.run_main(payload(OTHER_ID, OTHER_VALUE))[:2])
        self.assertEqual((OTHER_ID, OTHER_VALUE), (self.copy("aws_access_key_id").read_text(), self.copy("aws_secret_access_key").read_text()))
        self.assertFalse(leftover.exists())

    def test_the_four_files_hold_the_one_key_and_no_other_text(self) -> None:
        self.run_main(payload())
        held = {path.read_text() for path in (*self.folder.joinpath("airbyte").iterdir(), *self.folder.joinpath("airbyte_api").iterdir())}
        self.assertEqual({KEY_ID, KEY_VALUE}, held)

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
        self.assertFalse((self.folder / "airbyte_api").exists(), "a refused document writes no copy either")

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

    def deployed(self, command: list[str], directory: str) -> tuple[int, str, str]:
        result = run_command(
            command[0],
            [*command[1:], "--directory", str(self.folder / directory), "--owner", OWNER, "--container-owner", OWNER],
            input_text=payload(),
            timeout=30,
            env={"PATH": "/usr/bin:/bin"},
        )
        return result.returncode, result.stdout, result.stderr

    def test_the_script_runs_the_way_the_deployed_command_starts_it_through_the_interpreter_in_isolated_mode(self) -> None:
        script = install_form(self.folder / "bin")
        self.assertEqual((0, "placed\n", ""), self.deployed([sys.executable, "-I", str(script)], "s"))
        self.assertEqual(KEY_VALUE, (self.folder / "s" / "airbyte" / "aws_secret_access_key").read_text())

    def test_isolated_mode_drops_the_script_folder_from_the_path_yet_the_script_finds_its_helper_beside_it(self) -> None:
        script = install_form(self.folder / "bin")
        plain = run_command(sys.executable, ["-I", "-c", "import secret_files"], cwd=script.parent, timeout=30)
        self.assertNotEqual(0, plain.returncode, "the helper is not importable by name in isolated mode, so the script must load it by its path")
        self.assertEqual("placed\n", self.deployed([sys.executable, "-I", str(script)], "s")[1])

    def test_the_script_is_an_executable_file_with_an_interpreter_line_so_it_never_fails_to_start(self) -> None:
        text = SCRIPT.read_text()
        self.assertEqual(SHEBANG, text.splitlines()[0])
        script = install_form(self.folder / "bin")
        script.write_text(text.replace(SHEBANG, f"#!{sys.executable} -I", 1))
        script.chmod(0o700)
        self.assertEqual((0, "placed\n", ""), self.deployed([str(script)], "d"))

    def test_the_interpreter_line_names_the_hosts_interpreter_in_isolated_mode_and_runs_where_that_interpreter_exists(self) -> None:
        interpreter = Path("/usr/bin/python3")
        if not interpreter.exists():
            self.skipTest("this computer has no /usr/bin/python3")
        version = run_command(str(interpreter), ["-c", "import sys; print(sys.version_info >= (3, 12))"], timeout=30)
        if version.returncode != 0 or version.stdout.strip() != "True":
            self.skipTest("the interpreter of this computer is older than the host's")
        script = install_form(self.folder / "bin")
        self.assertEqual((0, "placed\n", ""), self.deployed([str(script)], "r"))

    def test_the_script_reads_no_secret_from_an_argument_or_the_environment(self) -> None:
        text = SCRIPT.read_text()
        self.assertNotIn("os.environ", text)
        self.assertNotIn("getenv", text)
        self.assertEqual({"--directory", "--owner", "--container-owner"}, set(re.findall(r'"(--[a-z-]+)"', text)))

    def test_the_command_that_sudo_sees_is_the_one_the_launcher_runs_and_the_script_installed_by_the_role(self) -> None:
        self.assertEqual(f"/usr/bin/python3 -I {group_variables()['airbyte_bin_directory']}/airbyte_key_install", " ".join(host.KEY_COMMAND))

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

    @staticmethod
    def variables_for(folder: Path) -> dict[str, object]:
        return {
            "airbyte_bin_directory": str(folder / "bin"),
            "airbyte_key_script": str(folder / "bin" / "airbyte_key_install"),
            "airbyte_key_sudoers": str(folder / "sudoers.d" / "airbyte_key_install"),
            "airbyte_visudo": VISUDO,
            "airbyte_secrets_directory": str(folder / "secrets"),
            "airbyte_owner": ROLE_OWNER,
            "airbyte_group": ROLE_GROUP,
        }

    def variables(self) -> dict[str, object]:
        return self.variables_for(self.folder)

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
            ["-I", str(self.folder / "bin" / "airbyte_key_install"), "--directory", str(self.folder / "secrets"), "--owner", OWNER, "--container-owner", OWNER],
            input_text=payload(),
            timeout=30,
            env={"PATH": "/usr/bin:/bin"},
        )
        self.assertEqual((0, "placed\n"), (result.returncode, result.stdout))
        self.assertEqual(KEY_ID, (self.folder / "secrets" / "airbyte" / "aws_access_key_id").read_text())


class SudoersTest(unittest.TestCase):
    """The sudoers drop-in turns off every recording of input and output for exactly the command that the launcher runs, so sudo never keeps the key.

    The options are those of the sudoers manual of sudo 1.9.15, the version of Ubuntu 24.04: log_input and log_output, and log_stdin, log_stdout, log_stderr,
    log_ttyin and log_ttyout, which that manual lists. A per-command Defaults entry may not hold arguments, so the command is a Cmnd_Alias.
    """

    OPTIONS = "!log_input, !log_output, !log_stdin, !log_stdout, !log_stderr, !log_ttyin, !log_ttyout"

    def lines(self) -> list[str]:
        return [line for line in SUDOERS.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#")]

    def test_the_drop_in_names_the_exact_command_that_the_launcher_runs_and_turns_every_recording_off_for_it(self) -> None:
        self.assertEqual(
            [f"Cmnd_Alias AIRBYTE_KEY_INSTALL = {' '.join(host.KEY_COMMAND)}", f"Defaults!AIRBYTE_KEY_INSTALL {self.OPTIONS}"],
            self.lines(),
        )

    def test_the_drop_in_gives_no_one_any_privilege_it_only_sets_defaults_for_the_command(self) -> None:
        for line in self.lines():
            self.assertRegex(line, r"^(Cmnd_Alias |Defaults!)")
        self.assertNotRegex(SUDOERS.read_text(), r"(?m)^[^#\n]*\b(ALL|NOPASSWD)\b")

    @unittest.skipUnless(Path(VISUDO).exists(), "this computer has no visudo")
    def test_visudo_accepts_the_drop_in_and_refuses_an_unknown_option_or_arguments_in_a_per_command_default(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:

            def check(text: str) -> int:
                path = Path(scratch) / "drop_in"
                path.write_text(text)
                return run_command(VISUDO, ["-cf", str(path)], timeout=30).returncode

            good = SUDOERS.read_text()
            self.assertEqual(0, check(good))
            self.assertNotEqual(0, check(good.replace("!log_ttyout", "!log_no_such_option")))
            self.assertNotEqual(0, check(good.replace("Defaults!AIRBYTE_KEY_INSTALL", f"Defaults!{' '.join(host.KEY_COMMAND)}")))

    def test_the_role_installs_it_for_root_only_in_mode_0440_and_validates_it_with_visudo_before_it_takes_its_place(self) -> None:
        task = next(
            task for task in role_tasks()["key_script.yml"] if "ansible.builtin.copy" in task and "sudoers" in str(task["ansible.builtin.copy"]["dest"])
        )
        copy = task["ansible.builtin.copy"]
        self.assertEqual("0440", copy["mode"])
        self.assertEqual("{{ airbyte_visudo }} -cf %s", copy["validate"])
        self.assertEqual("{{ airbyte_key_sudoers }}", copy["dest"])
        self.assertEqual("{{ role_path }}/files/airbyte_key_install.sudoers", copy["src"])
        self.assertEqual("/usr/sbin/visudo", group_variables()["airbyte_visudo"])
        self.assertEqual(host.SUDOERS_FILE, group_variables()["airbyte_key_sudoers"])

    @unittest.skipUnless(Path(VISUDO).exists(), "this computer has no visudo")
    def test_the_installed_drop_in_is_the_file_with_mode_0440_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory(prefix="project_key_sudoers_") as scratch:
            variables = RoleTest.variables_for(Path(scratch))
            code, output = run_tasks(["key_script.yml"], variables)
            placed = Path(scratch) / "sudoers.d" / "airbyte_key_install"
            self.assertEqual(0, code, output)
            self.assertEqual(SUDOERS.read_text(), placed.read_text())
            self.assertEqual(0o440, stat.S_IMODE(placed.stat().st_mode))
            again_code, again = run_tasks(["key_script.yml"], variables)
            self.assertEqual(0, again_code, again)
            self.assertRegex(again, r"changed=0\s")

    def test_a_drop_in_that_fails_the_validation_leaves_no_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="project_key_sudoers_") as scratch:
            code, output = run_tasks(["key_script.yml"], RoleTest.variables_for(Path(scratch)) | {"airbyte_visudo": "/usr/bin/false"})
            self.assertNotEqual(0, code, output)
            self.assertFalse((Path(scratch) / "sudoers.d" / "airbyte_key_install").exists())


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
