"""The step that gives Airbyte its secrets: generated password files and the Kubernetes secrets made from them, with no value in a command line or a message.

The files step runs through the real ``ansible-playbook``. The cluster step needs k3s, which the acceptance test provides, so isolated cases here cover what a
fresh cluster never shows: a drifted secret, a secret with keys that Airbyte added itself, a failed call and the form of every call that carries a value.
"""

from __future__ import annotations

import base64
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
from typing import Any

from tests.fast.ansible_support import ROLES, run_playbook

ROLE = ROLES / "airbyte"
MODULE = ROLE / "library" / "airbyte_secrets.py"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
CHANGED_NONE = re.compile(r"changed=0\s")
DATABASE_VALUE = "database-value-that-must-not-leak"
KUBECTL = ["k3s", "kubectl"]


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("airbyte_secrets", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


secrets = load_module()


def run_files_step(folder: Path) -> tuple[int, str]:
    variables = {"airbyte_secrets_directory": str(folder / "secrets"), "airbyte_owner": OWNER, "airbyte_group": GROUP}
    with tempfile.TemporaryDirectory() as scratch:
        playbook = Path(scratch) / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: secret_files.yml\n"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        result = run_playbook(Path(scratch), playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(variables)])
    return result.returncode, result.stdout + result.stderr


def database_password(folder: Path, text: str = DATABASE_VALUE) -> None:
    target = folder / "secrets" / "postgres" / "airbyte.password"
    target.parent.mkdir(parents=True)
    target.parent.parent.chmod(0o700)
    target.parent.chmod(0o700)
    target.write_text(text)
    target.chmod(0o600)


class FilesStepTest(unittest.TestCase):
    def test_the_missing_files_are_created_for_the_owner_only_and_a_second_run_keeps_them_and_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            database_password(folder)
            code, output = run_files_step(folder)
            directory = folder / "secrets" / "airbyte"
            first = {path.name: path.read_text() for path in directory.iterdir()}
            modes = {stat.S_IMODE(path.stat().st_mode) for path in directory.iterdir()}
            folder_mode = stat.S_IMODE(directory.stat().st_mode)
            again_code, again = run_files_step(folder)
            second = {path.name: path.read_text() for path in directory.iterdir()}
        self.assertEqual(0, code, output)
        self.assertEqual(["admin.password", "storage_access_key_id", "storage_secret_access_key"], sorted(first))
        self.assertEqual(({0o600}, 0o700), (modes, folder_mode))
        self.assertGreaterEqual(min(len(text) for text in first.values()), 16)
        for text in first.values():
            self.assertNotIn(text, output)
        self.assertEqual(0, again_code, again)
        self.assertEqual(first, second)
        self.assertRegex(again, CHANGED_NONE)

    def test_a_missing_database_password_file_stops_the_step_and_names_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = run_files_step(Path(scratch))
        self.assertNotEqual(0, code)
        self.assertIn("airbyte.password", output)


class FakeCluster:
    """A cluster that keeps secrets and records each call, with the argument list and the data on standard input."""

    def __init__(self, existing: dict[str, dict[str, str]] | None = None, fail_apply: bool = False) -> None:
        self.stored: dict[str, dict[str, str]] = {name: dict(data) for name, data in (existing or {}).items()}
        self.calls: list[tuple[list[str], str | None]] = []
        self.fail_apply = fail_apply

    def __call__(self, args: list[str], data: str | None) -> tuple[int, str, str]:
        self.calls.append((args, data))
        if args[len(KUBECTL)] == "get":
            name = args[len(KUBECTL) + 2]
            if name not in self.stored:
                return 1, "", f'Error from server (NotFound): secrets "{name}" not found'
            return 0, json.dumps({"data": {key: base64.b64encode(value.encode()).decode() for key, value in self.stored[name].items()}}), ""
        if self.fail_apply:
            return 1, "", f"error applying {data}"
        document = json.loads(data or "{}")
        self.stored.setdefault(document["metadata"]["name"], {}).update({key: base64.b64decode(value).decode() for key, value in document["data"].items()})
        return 0, "", ""


def store_with_files(folder: Path) -> Any:
    database_password(folder)
    store = secrets.SecretFiles(folder / "secrets")
    store.ensure_generated()
    return store


class ClusterStepTest(unittest.TestCase):
    def apply(self, cluster: FakeCluster, folder: Path) -> list[str]:
        result: list[str] = secrets.apply_secrets(cluster, KUBECTL, "airbyte", secrets.SecretFiles(folder / "secrets"), "airbyte")
        return result

    def test_the_three_secrets_are_created_from_the_files_and_the_database_secret_holds_its_user_and_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            cluster = FakeCluster()
            changed = self.apply(cluster, folder)
            admin = (folder / "secrets" / "airbyte" / "admin.password").read_text()
        self.assertEqual(["airbyte-auth-secrets", "airbyte-database", "airbyte-storage"], sorted(changed))
        self.assertEqual({"database-user": "airbyte", "database-password": DATABASE_VALUE}, cluster.stored["airbyte-database"])
        self.assertEqual({"instance-admin-password": admin}, cluster.stored["airbyte-auth-secrets"])
        self.assertEqual({"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}, set(cluster.stored["airbyte-storage"]))

    def test_no_call_carries_a_value_on_its_command_line_and_each_apply_is_server_side_so_no_copy_is_annotated(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            cluster = FakeCluster()
            self.apply(cluster, folder)
            values = {value for data in cluster.stored.values() for value in data.values()} - {"airbyte"}
        applies = [(args, data) for args, data in cluster.calls if "apply" in args]
        self.assertEqual(3, len(applies))
        for args, _data in cluster.calls:
            for value in values:
                self.assertNotIn(value, " ".join(args))
        for args, data in applies:
            self.assertIn("--server-side", args)
            self.assertIn("--namespace", args)
            self.assertEqual("-", args[args.index("--filename") + 1])
            self.assertIsNotNone(data)

    def test_a_second_pass_over_a_matching_cluster_applies_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            cluster = FakeCluster()
            self.apply(cluster, folder)
            before = len(cluster.calls)
            changed = self.apply(cluster, folder)
            applies = [call for call in cluster.calls[before:] if "apply" in call[0]]
        self.assertEqual([], changed)
        self.assertEqual([], applies)

    def test_a_secret_whose_value_drifted_is_repaired_and_the_keys_that_airbyte_added_are_left_and_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            cluster = FakeCluster()
            self.apply(cluster, folder)
            cluster.stored["airbyte-auth-secrets"]["instance-admin-client-id"] = "added-by-airbyte"
            self.assertEqual([], self.apply(cluster, folder))
            cluster.stored["airbyte-database"]["database-password"] = "drifted"
            changed = self.apply(cluster, folder)
        self.assertEqual(["airbyte-database"], changed)
        self.assertEqual(DATABASE_VALUE, cluster.stored["airbyte-database"]["database-password"])
        self.assertEqual("added-by-airbyte", cluster.stored["airbyte-auth-secrets"]["instance-admin-client-id"])

    def test_a_failed_call_stops_the_step_and_its_message_holds_no_value_of_any_secret(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            values = [
                store.read(name)
                for store in (secrets.SecretFiles(folder / "secrets"),)
                for name in ("postgres/airbyte.password", *(f"airbyte/{n}" for n in secrets.GENERATED))
            ]
            with self.assertRaises(secrets.SecretError) as caught:
                self.apply(FakeCluster(fail_apply=True), folder)
        message = str(caught.exception)
        for value in values:
            self.assertNotIn(value, message)
            self.assertNotIn(base64.b64encode(value.encode()).decode(), message)
        self.assertIn("airbyte-", message)

    def test_a_failed_call_on_the_database_secret_hides_its_value_too(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)
            cluster = FakeCluster(existing={name: {} for name in ("airbyte-auth-secrets", "airbyte-storage")})
            for name, data in secrets.wanted_secrets(secrets.SecretFiles(folder / "secrets"), "airbyte").items():
                if name != "airbyte-database":
                    cluster.stored[name] = dict(data)
            cluster.fail_apply = True
            with self.assertRaises(secrets.SecretError) as caught:
                self.apply(cluster, folder)
        self.assertNotIn(DATABASE_VALUE, str(caught.exception))
        self.assertNotIn(base64.b64encode(DATABASE_VALUE.encode()).decode(), str(caught.exception))
        self.assertIn("airbyte-database", str(caught.exception))

    def test_a_cluster_that_cannot_be_read_stops_the_step_instead_of_being_taken_for_an_empty_one(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            store_with_files(folder)

            def unreachable(args: list[str], data: str | None) -> tuple[int, str, str]:
                return 1, "", "The connection to the server was refused"

            with self.assertRaises(secrets.SecretError) as caught:
                secrets.apply_secrets(unreachable, KUBECTL, "airbyte", secrets.SecretFiles(folder / "secrets"), "airbyte")
        self.assertIn("refused", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
