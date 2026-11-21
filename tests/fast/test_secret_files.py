"""The one place that knows where a PostgreSQL password file is and how it is made, shared by the role of the stack and the role of OpenBao.

The store is a module of the role ``secret_files``. Both roles' modules import it, and Ansible must find it for each role on its own, without the other role
in the play. The cases run the real ``ansible-playbook`` for that proof and use real files in a scratch folder for the rest.
"""

from __future__ import annotations

import importlib.util
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

from tests.fast.ansible_support import ROLES, run_role_tasks, shared_module

store_module: Any = shared_module("secret_files")


def load_library_module(role: str, name: str) -> ModuleType:
    path = ROLES / role / "library" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


accounts = load_library_module("compose_stack", "postgres_accounts")
bootstrap = load_library_module("openbao", "openbao_bootstrap")


class PasswordStoreTest(unittest.TestCase):
    def test_a_missing_password_is_created_for_its_owner_only_and_is_long_and_random(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch) / "postgres")
            created = store.ensure("nessie")
            other = store.ensure("airbyte")
            first, second = store.read("nessie"), store.read("airbyte")
            file_mode = stat.S_IMODE(store.path("nessie").stat().st_mode)
            folder_mode = stat.S_IMODE(store.path("nessie").parent.stat().st_mode)
        self.assertEqual((True, True), (created, other))
        self.assertRegex(first, r"^[A-Za-z0-9_-]{40,}$")
        self.assertNotEqual(first, second)
        self.assertEqual((0o600, 0o700), (file_mode, folder_mode))

    def test_an_existing_password_is_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.ensure("nessie")
            before = store.read("nessie")
            again = store.ensure("nessie")
            after = store.read("nessie")
        self.assertFalse(again)
        self.assertEqual(before, after)

    def test_an_empty_or_missing_file_is_refused_by_account_name_and_is_never_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            with self.assertRaisesRegex(store_module.SecretFileError, "nessie"):
                store.read("nessie")
            store.path("nessie").write_text("")
            with self.assertRaisesRegex(store_module.SecretFileError, "nessie"):
                store.ensure("nessie")
            self.assertEqual("", store.path("nessie").read_text())

    def test_a_trailing_newline_is_not_part_of_the_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.path("nessie").write_text("abc\n")
            self.assertEqual("abc", store.read("nessie"))

    def test_the_store_of_a_secrets_folder_is_its_postgres_folder(self) -> None:
        self.assertEqual(Path("/s/postgres/nessie.password"), store_module.PasswordStore.in_secrets(Path("/s")).path("nessie"))


class OneStoreTest(unittest.TestCase):
    def test_both_modules_use_the_shared_store_and_keep_no_path_logic_of_their_own(self) -> None:
        self.assertIs(store_module.PasswordStore, accounts.PasswordStore)
        self.assertIs(store_module.PasswordStore, bootstrap.PasswordStore)
        self.assertFalse(hasattr(bootstrap, "password_file"))
        self.assertFalse(hasattr(bootstrap, "POSTGRES_FOLDER"))
        self.assertFalse(hasattr(accounts, "ACCOUNT_FOLDER"))


class RoleResolutionTest(unittest.TestCase):
    def test_the_openbao_role_finds_the_shared_module_without_the_role_of_the_stack_in_the_play(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            unlock = folder / "unlock.env"
            unlock.write_text("OPENBAO_UNLOCK_KEY=" + "ab" * 32 + "\n")
            variables: dict[str, object] = {
                "openbao_secrets_directory": str(folder / "secrets"),
                "openbao_unlock_key_file": str(unlock),
                "openbao_user": f"{os.getuid()}:{os.getgid()}",
            }
            result = run_role_tasks("openbao", ["prepare.yml"], variables, verbose=True)
            created = folder / "secrets" / "openbao" / "bootstrap.secret_id"
            mode = stat.S_IMODE(created.stat().st_mode) if created.exists() else None
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(0o400, mode)

    def test_the_role_of_the_stack_finds_the_shared_module_without_the_role_of_openbao_in_the_play(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            tools = folder / "tools.txt"
            tools.write_text("nessie\n")
            variables: dict[str, object] = {"compose_stack_tools_file": str(tools), "compose_stack_secrets_directory": str(folder / "secrets")}
            result = run_role_tasks("compose_stack", ["passwords.yml"], variables, verbose=True)
            created = sorted(path.name for path in (folder / "secrets" / "postgres").glob("*.password"))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["nessie.password", "postgres.password"], created)


if __name__ == "__main__":
    unittest.main()
