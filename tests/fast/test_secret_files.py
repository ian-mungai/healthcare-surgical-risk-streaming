"""The one place that knows where a secret file is, who owns it and how it is made, shared by the role of the stack and the role of OpenBao.

The code is a module of the role ``secret_files``. Both roles' modules import it, and Ansible must find it for each role on its own, without the other role
in the play. The cases run the real ``ansible-playbook`` for that proof and use real files in a scratch folder for the rest. A test user cannot give a file
to another user, so the owners in these cases are the test user's own groups: two groups of one user are two different owners.
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

from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import ROLES, load_variable, run_role_tasks, shared_module

store_module: Any = shared_module("secret_files")
UID, GID = os.getuid(), os.getgid()
OTHER_GIDS = [group for group in os.getgroups() if group != GID]
MY_OWNER = (UID, GID)
NEEDS_GROUP = unittest.skipUnless(OTHER_GIDS, "the test user belongs to one group only")
NOT_ROOT = unittest.skipIf(UID == 0, "root can give a file to any user")


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
            created = store.ensure("nessie", MY_OWNER)
            other = store.ensure("airbyte", MY_OWNER)
            first, second = store.read("nessie"), store.read("airbyte")
            status = store.path("nessie").stat()
            folder_mode = stat.S_IMODE(store.path("nessie").parent.stat().st_mode)
        self.assertEqual((True, True), (created, other))
        self.assertRegex(first, r"^[A-Za-z0-9_-]{40,}$")
        self.assertNotEqual(first, second)
        self.assertEqual((0o400, 0o700, MY_OWNER), (stat.S_IMODE(status.st_mode), folder_mode, (status.st_uid, status.st_gid)))

    def test_an_existing_password_is_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.ensure("nessie", MY_OWNER)
            before = store.read("nessie")
            again = store.ensure("nessie", MY_OWNER)
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
                store.ensure("nessie", MY_OWNER)
            self.assertEqual("", store.path("nessie").read_text())

    def test_a_trailing_newline_is_not_part_of_the_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.path("nessie").write_text("abc\n")
            self.assertEqual("abc", store.read("nessie"))

    def test_the_store_of_a_secrets_folder_is_its_postgres_folder(self) -> None:
        self.assertEqual(Path("/s/postgres/nessie.password"), store_module.PasswordStore.in_secrets(Path("/s")).path("nessie"))

    @NEEDS_GROUP
    def test_a_new_file_is_owned_by_the_owner_it_is_made_for_and_nobody_else_reads_it(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.ensure("nessie", (UID, OTHER_GIDS[0]))
            status = store.path("nessie").stat()
        self.assertEqual((0o400, UID, OTHER_GIDS[0]), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))

    def test_an_existing_file_with_another_mode_is_made_private_and_a_private_file_needs_no_change(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.ensure("nessie", MY_OWNER)
            before = store.read("nessie")
            results = []
            for mode in (0o600, 0o440, 0o404, 0o644):
                store.path("nessie").chmod(mode)
                results.append((store.protect("nessie", MY_OWNER), stat.S_IMODE(store.path("nessie").stat().st_mode)))
            final = store.protect("nessie", MY_OWNER)
            after = store.read("nessie")
        self.assertEqual([(True, 0o400)] * 4, results)
        self.assertFalse(final)
        self.assertEqual(before, after)

    @NEEDS_GROUP
    def test_an_existing_file_of_another_owner_is_given_to_its_owner_with_its_content_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            store.ensure("nessie", MY_OWNER)
            before = store.read("nessie")
            changed = store.protect("nessie", (UID, OTHER_GIDS[0]))
            status = store.path("nessie").stat()
            after = store.read("nessie")
        self.assertTrue(changed)
        self.assertEqual((UID, OTHER_GIDS[0]), (status.st_uid, status.st_gid))
        self.assertEqual(before, after)

    def test_a_file_that_is_missing_cannot_be_protected_and_is_named(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, self.assertRaisesRegex(store_module.SecretFileError, "nessie"):
            store_module.PasswordStore(Path(scratch)).protect("nessie", MY_OWNER)

    @NOT_ROOT
    def test_a_file_that_cannot_be_given_to_its_owner_is_refused_by_name_and_is_not_left_behind(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore(Path(scratch))
            with self.assertRaisesRegex(store_module.SecretFileError, "nessie"):
                store.ensure("nessie", (UID + 1, GID))
            left = store.path("nessie").exists()
        self.assertFalse(left)


class SecretFileTest(unittest.TestCase):
    def test_a_file_is_written_for_its_owner_only_from_the_moment_it_exists_and_a_replacement_swaps_it_whole(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "folder" / "a.secret_id"
            store_module.write_private_file(path, "first", replace=False, owner=MY_OWNER)
            first = (path.read_text(), stat.S_IMODE(path.stat().st_mode), path.parent.stat().st_mode & 0o777)
            store_module.write_private_file(path, "second", replace=True, owner=MY_OWNER)
            second = (path.read_text(), stat.S_IMODE(path.stat().st_mode))
            leftovers = sorted(item.name for item in path.parent.iterdir())
        self.assertEqual(("first", 0o400, 0o700), first)
        self.assertEqual(("second", 0o400), second)
        self.assertEqual(["a.secret_id"], leftovers)

    def test_any_mode_but_0400_differs_and_so_does_any_owner_but_the_one_asked_for(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "a"
            store_module.write_private_file(path, "x", replace=False, owner=MY_OWNER)
            control = store_module.access_differs(path, MY_OWNER)
            other_user = store_module.access_differs(path, (UID + 1, GID))
            other_group = store_module.access_differs(path, (UID, GID + 1))
            modes = {}
            for mode in (0o600, 0o440, 0o404, 0o444, 0o640, 0o000, 0o500):
                path.chmod(mode)
                modes[oct(mode)] = store_module.access_differs(path, MY_OWNER)
        self.assertFalse(control)
        self.assertTrue(other_user and other_group)
        self.assertEqual({True}, set(modes.values()), modes)

    @NEEDS_GROUP
    def test_set_access_repairs_the_mode_and_the_group(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "a"
            store_module.write_private_file(path, "x", replace=False, owner=MY_OWNER)
            path.chmod(0o644)
            store_module.set_access(path, (UID, OTHER_GIDS[0]))
            status = path.stat()
        self.assertEqual((0o400, UID, OTHER_GIDS[0]), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))


class ListTest(unittest.TestCase):
    def test_the_tracked_service_list_is_valid_and_every_postgres_tool_is_a_service(self) -> None:
        services = store_module.parse_names((ROOT / "compose" / "openbao_services.txt").read_text(), "service")
        tools = store_module.parse_names((ROOT / "compose" / "postgres_tools.txt").read_text(), "tool")
        self.assertTrue({"nessie", "airbyte"} <= set(services))
        self.assertEqual([], sorted(set(tools) - set(services)), "a PostgreSQL tool has no OpenBao role to read its password")

    def test_comments_and_blank_lines_are_ignored_in_order(self) -> None:
        self.assertEqual(["b", "a"], store_module.parse_names("# services\nb\n\n  a  # trailing\n", "service"))

    def test_a_name_that_could_alter_a_policy_or_an_api_path_is_refused(self) -> None:
        for name in ("Nessie", "nes-sie", "1nessie", 'nes"sie', "nes/sie", "../sie", "nes*", "bootstrap", "a" * 32, "default", "root"):
            with self.subTest(name=name), self.assertRaises(store_module.SecretFileError) as caught:
                store_module.parse_names(f"{name}\n", "service")
            self.assertIn("service name", str(caught.exception))

    def test_a_repeated_name_or_an_empty_list_is_refused(self) -> None:
        with self.assertRaisesRegex(store_module.SecretFileError, "more than once"):
            store_module.parse_names("nessie\nairbyte\nnessie\n", "service")
        with self.assertRaisesRegex(store_module.SecretFileError, "no service"):
            store_module.parse_names("# nothing\n", "service")

    def test_a_service_line_may_name_the_numeric_user_its_container_runs_as(self) -> None:
        entries = store_module.parse_entries("nessie 185:185  # runs as a fixed user\nairbyte\n", "service")
        self.assertEqual([("nessie", (185, 185)), ("airbyte", None)], [(entry.name, entry.owner) for entry in entries])

    def test_an_owner_that_is_not_a_numeric_user_other_than_root_is_refused(self) -> None:
        for owner in ("185", "a:b", "-1:5", "0:5", "5:-1", "1:2:3", "185:", ":185", "5:0x10", "+5:6", "0:0"):
            with self.subTest(owner=owner), self.assertRaises(store_module.SecretFileError) as caught:
                store_module.parse_entries(f"nessie {owner}\n", "service")
            self.assertIn("owner", str(caught.exception))

    def test_a_group_of_zero_is_allowed_only_with_a_user_other_than_root(self) -> None:
        self.assertEqual((5, 0), store_module.parse_entries("nessie 5:0\n", "service")[0].owner)

    def test_a_tool_line_holds_a_name_only_and_a_line_holds_nothing_after_the_owner(self) -> None:
        for text, what in (("nessie 185:185\n", "tool"), ("nessie 185:185 extra\n", "service")):
            with self.subTest(text=text), self.assertRaises(store_module.SecretFileError):
                store_module.parse_entries(text, what)

    def test_the_owner_of_root_only_files_is_a_numeric_user_and_group_and_root_may_be_named_there(self) -> None:
        self.assertEqual((0, 0), store_module.parse_owner("0:0", allow_root=True))
        for text in ("root:root", "0", "0:0:0", ""):
            with self.subTest(text=text), self.assertRaises(store_module.SecretFileError):
                store_module.parse_owner(text, allow_root=True)


class OwnerDataTest(unittest.TestCase):
    """The owners are data. Each one that a file needs is stated in one place, and every copy of it is held to that place."""

    def test_every_compose_service_of_a_listed_service_runs_as_the_numeric_user_on_its_line(self) -> None:
        entries = store_module.parse_entries((ROOT / "compose" / "openbao_services.txt").read_text(), "service")
        checked = []
        for entry in entries:
            files = sorted((ROOT / "compose" / "services").glob(f"{entry.name}.yml")) + sorted((ROOT / "compose" / "services").glob(f"{entry.name}_*.yml"))
            if not files:
                self.assertIsNone(entry.owner, f"{entry.name} runs outside Compose, so its files stay with root until it has a numeric user")
            for service_file in files:
                compose: Any = load_yaml(service_file.read_text())
                (service,) = compose["services"].values()
                user = service.get("user")
                checked.append(service_file.stem)
                self.assertEqual(None if user is None else tuple(int(part) for part in str(user).split(":")), entry.owner, service_file.stem)
        self.assertLessEqual({"nessie", "dagster_webserver", "dagster_daemon", "dagster_code_location"}, set(checked))

    def test_the_openbao_user_in_the_group_variables_is_the_user_of_its_compose_service_and_is_not_root(self) -> None:
        compose: Any = load_yaml((ROOT / "compose" / "services" / "openbao.yml").read_text())
        user = str(compose["services"]["openbao"]["user"])
        self.assertRegex(user, r"^[1-9]\d*:\d+$")
        self.assertEqual(user, load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "openbao_user"))

    def test_the_owner_of_root_only_files_is_root(self) -> None:
        self.assertEqual("0:0", load_variable(ROOT / "ansible" / "group_vars" / "all.yml", "secret_files_root_owner"))


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
                "openbao_user": f"{UID}:{GID}",
                "secret_files_root_owner": f"{UID}:{GID}",
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
            services = folder / "services.txt"
            services.write_text("nessie\n")
            variables: dict[str, object] = {
                "compose_stack_tools_file": str(tools),
                "compose_stack_services_file": str(services),
                "compose_stack_secrets_directory": str(folder / "secrets"),
                "secret_files_root_owner": f"{UID}:{GID}",
            }
            result = run_role_tasks("compose_stack", ["passwords.yml"], variables, verbose=True)
            created = sorted(path.name for path in (folder / "secrets" / "postgres").glob("*.password"))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["nessie.password", "postgres.password"], created)


if __name__ == "__main__":
    unittest.main()
