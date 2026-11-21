"""The account step of the PostgreSQL service: the tool list, the password files, the SCRAM verifier and the plan that makes the server match the list.

Real runs against a server are the acceptance test's job. Isolated cases are kept for what a fresh server never shows: a server that drifted
(a changed password, a widened role, a grant on a database, a database with another owner) and the verifier arithmetic, which is checked against
a verifier that PostgreSQL 18.7 itself computed for the password ``pencil``.
"""

from __future__ import annotations

import base64
import importlib.util
import re
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "compose_stack" / "library" / "postgres_accounts.py"
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
SERVER_VERIFIER = "SCRAM-SHA-256$4096:VH4EErGPaqJpsf05kuJoBw==$b6XFM3jOXIKRGszgsSSbt0ahRBPMegngJx/ydQrhm40=:bSQil7LYTkLF1VD90t0B4/MdLazKHP7AXsaxyynhAAU="
SERVER_SALT = base64.b64decode("VH4EErGPaqJpsf05kuJoBw==")


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("postgres_accounts", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


accounts = load_module()


class ToolListTest(unittest.TestCase):
    def test_the_tracked_list_is_valid_and_names_each_tool_once(self) -> None:
        tools = accounts.parse_tools(TOOLS_FILE.read_text())
        self.assertEqual(len(tools), len(set(tools)))
        self.assertTrue({"nessie", "airbyte"} <= set(tools))

    def test_comments_and_blank_lines_are_ignored_in_order(self) -> None:
        self.assertEqual(["b", "a"], accounts.parse_tools("# tools\nb\n\n  a  # trailing\n"))

    def test_a_name_that_could_alter_a_statement_or_clash_with_the_server_is_refused(self) -> None:
        for name in ("Nessie", "nes-sie", "1nessie", "nes sie", 'nes"sie', "nes;sie", "postgres", "pg_monitor", "template1", "a" * 32, "public"):
            with self.subTest(name=name), self.assertRaises(accounts.AccountError) as caught:
                accounts.parse_tools(f"{name}\n")
            self.assertIn("tool name", str(caught.exception))

    def test_a_repeated_name_or_an_empty_list_is_refused(self) -> None:
        with self.assertRaisesRegex(accounts.AccountError, "more than once"):
            accounts.parse_tools("nessie\nairbyte\nnessie\n")
        with self.assertRaisesRegex(accounts.AccountError, "no tool"):
            accounts.parse_tools("# nothing\n")


class PasswordFileTest(unittest.TestCase):
    def test_a_missing_password_is_created_for_its_owner_only_and_is_long_and_random(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = accounts.PasswordStore(Path(scratch) / "postgres")
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
            store = accounts.PasswordStore(Path(scratch))
            store.ensure("nessie")
            before = store.read("nessie")
            again = store.ensure("nessie")
            after = store.read("nessie")
        self.assertFalse(again)
        self.assertEqual(before, after)

    def test_an_empty_or_missing_file_is_refused_by_account_name_and_is_never_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = accounts.PasswordStore(Path(scratch))
            with self.assertRaisesRegex(accounts.AccountError, "nessie"):
                store.read("nessie")
            store.path("nessie").write_text("")
            with self.assertRaisesRegex(accounts.AccountError, "nessie"):
                store.ensure("nessie")
            self.assertEqual("", store.path("nessie").read_text())

    def test_a_trailing_newline_is_not_part_of_the_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            store = accounts.PasswordStore(Path(scratch))
            store.path("nessie").write_text("abc\n")
            self.assertEqual("abc", store.read("nessie"))


class VerifierTest(unittest.TestCase):
    def test_the_verifier_equals_the_one_that_the_server_computed_for_the_same_salt(self) -> None:
        self.assertEqual(SERVER_VERIFIER, accounts.scram_verifier("pencil", SERVER_SALT, 4096))

    def test_the_stored_verifier_matches_only_its_own_password(self) -> None:
        self.assertTrue(accounts.verifier_matches(SERVER_VERIFIER, "pencil"))
        self.assertFalse(accounts.verifier_matches(SERVER_VERIFIER, "pencil2"))
        self.assertFalse(accounts.verifier_matches(SERVER_VERIFIER, ""))

    def test_a_verifier_that_is_missing_or_malformed_matches_nothing_and_does_not_raise(self) -> None:
        for stored in ("", "plain", "SCRAM-SHA-256$x:y$z", "SCRAM-SHA-256$4096:%%%$a:b", SERVER_VERIFIER[:-5]):
            with self.subTest(stored=stored):
                self.assertFalse(accounts.verifier_matches(stored, "pencil"))

    def test_each_verifier_has_its_own_salt_and_matches_its_password(self) -> None:
        one, two = accounts.scram_verifier("pencil"), accounts.scram_verifier("pencil")
        self.assertNotEqual(one, two)
        self.assertTrue(accounts.verifier_matches(one, "pencil") and accounts.verifier_matches(two, "pencil"))
        self.assertRegex(one, r"^SCRAM-SHA-256\$4096:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$")


PASSWORDS = {"nessie": "n-secret-value", "airbyte": "a-secret-value", "extra": "e-secret-value"}
TOOLS = ["nessie", "airbyte"]


def converged(tool: str) -> tuple[Any, Any]:
    role = accounts.RoleState(accounts.scram_verifier(PASSWORDS[tool]), True)
    database = accounts.DatabaseState(tool, ())
    return role, database


def state_of(*tools: str) -> tuple[dict[str, Any], dict[str, Any]]:
    roles = {tool: converged(tool)[0] for tool in tools}
    databases = {tool: converged(tool)[1] for tool in tools}
    return roles, databases


def plan(tools: list[str], roles: dict[str, Any], databases: dict[str, Any]) -> list[Any]:
    return list(accounts.plan(tools, roles, databases, PASSWORDS.__getitem__))


def descriptions(actions: list[Any]) -> list[str]:
    return [action.description for action in actions]


class PlanTest(unittest.TestCase):
    def test_an_empty_server_gets_each_account_then_its_database_then_its_guard(self) -> None:
        actions = plan(TOOLS, {}, {})
        self.assertEqual(
            [
                "create account nessie",
                "create database nessie",
                "refuse every other account on database nessie",
                "create account airbyte",
                "create database airbyte",
                "refuse every other account on database airbyte",
            ],
            descriptions(actions),
        )

    def test_a_server_that_matches_the_list_needs_no_action(self) -> None:
        self.assertEqual([], plan(TOOLS, *state_of(*TOOLS)))

    def test_a_tool_added_later_gets_only_its_own_actions(self) -> None:
        roles, databases = state_of(*TOOLS)
        actions = plan([*TOOLS, "extra"], roles, databases)
        self.assertEqual(["create account extra", "create database extra", "refuse every other account on database extra"], descriptions(actions))

    def test_no_statement_holds_a_password_and_each_account_statement_holds_a_verifier(self) -> None:
        for action in plan(TOOLS, {}, {}):
            for password in PASSWORDS.values():
                self.assertNotIn(password, action.sql)
        creates = [action.sql for action in plan(TOOLS, {}, {}) if action.description.startswith("create account")]
        self.assertTrue(all(re.search(r"PASSWORD 'SCRAM-SHA-256\$4096:", sql) for sql in creates))
        self.assertTrue(all("NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS" in sql and "LOGIN" in sql for sql in creates))

    def test_the_guard_of_a_new_database_removes_public(self) -> None:
        sql = next(action.sql for action in plan(TOOLS, {}, {}) if action.description == "refuse every other account on database nessie")
        self.assertEqual('REVOKE ALL ON DATABASE "nessie" FROM PUBLIC', sql)

    def test_a_changed_password_file_resets_that_accounts_password_only(self) -> None:
        roles, databases = state_of(*TOOLS)
        roles["nessie"] = accounts.RoleState(accounts.scram_verifier("an-older-password"), True)
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["reset the password of account nessie"], descriptions(actions))
        self.assertTrue(actions[0].sql.startswith('ALTER ROLE "nessie" PASSWORD \'SCRAM-SHA-256$'))

    def test_a_widened_account_is_narrowed_and_its_password_is_not_touched(self) -> None:
        roles, databases = state_of(*TOOLS)
        roles["airbyte"] = accounts.RoleState(roles["airbyte"].verifier, False)
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["narrow the attributes of account airbyte"], descriptions(actions))
        self.assertEqual('ALTER ROLE "airbyte" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', actions[0].sql)

    def test_a_grant_on_a_tools_database_is_revoked_for_that_grantee(self) -> None:
        roles, databases = state_of(*TOOLS)
        databases["nessie"] = accounts.DatabaseState("nessie", ("PUBLIC", '"someone"'))
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["refuse every other account on database nessie"], descriptions(actions))
        self.assertEqual('REVOKE ALL ON DATABASE "nessie" FROM PUBLIC, "someone"', actions[0].sql)

    def test_a_database_with_another_owner_is_refused_and_named_and_nothing_is_planned_for_it(self) -> None:
        roles, databases = state_of(*TOOLS)
        databases["airbyte"] = accounts.DatabaseState("postgres", ())
        with self.assertRaisesRegex(accounts.AccountError, "database airbyte is owned by another account"):
            plan(TOOLS, roles, databases)


class StateTest(unittest.TestCase):
    def test_the_output_of_the_state_query_is_read_back(self) -> None:
        output = f'role|nessie|{SERVER_VERIFIER}|true\nrole|airbyte||false\ndatabase|nessie|nessie|\ndatabase|airbyte|postgres|PUBLIC,"x"\n'
        roles, databases = accounts.parse_state(output)
        self.assertEqual({"nessie": accounts.RoleState(SERVER_VERIFIER, True), "airbyte": accounts.RoleState("", False)}, roles)
        self.assertEqual({"nessie": accounts.DatabaseState("nessie", ()), "airbyte": accounts.DatabaseState("postgres", ("PUBLIC", '"x"'))}, databases)

    def test_a_line_of_another_shape_is_refused_without_echoing_it(self) -> None:
        with self.assertRaises(accounts.AccountError) as caught:
            accounts.parse_state("role|nessie|SCRAM-SHA-256$secret-looking-text\n")
        self.assertNotIn("secret-looking-text", str(caught.exception))

    def test_the_query_holds_no_tool_name_and_reads_the_list_from_one_variable(self) -> None:
        self.assertEqual("nessie,airbyte", accounts.tool_variable(TOOLS))
        self.assertNotIn("nessie", accounts.STATE_QUERY)
        self.assertEqual(2, accounts.STATE_QUERY.count(":'tools'"))


if __name__ == "__main__":
    unittest.main()
