"""The account step of the PostgreSQL service: the tool list, the password files, the SCRAM verifier and the plan that makes the server match the list.

Real runs against a server are the acceptance test's job. Isolated cases are kept for what a fresh server never shows: a server that drifted
(a changed password, a widened role, a grant on a database, a database with another owner) and the verifier arithmetic, which is checked against
a verifier that PostgreSQL 18.7 itself computed for the password ``pencil``. A tool may own several databases: the list's form and the plan's
treatment of each database are covered here.
"""

from __future__ import annotations

import base64
import importlib.util
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

from tests.fast.ansible_paths import shared_module

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "compose_stack" / "library" / "postgres_accounts.py"
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
SERVER_VERIFIER = "SCRAM-SHA-256$4096:VH4EErGPaqJpsf05kuJoBw==$b6XFM3jOXIKRGszgsSSbt0ahRBPMegngJx/ydQrhm40=:bSQil7LYTkLF1VD90t0B4/MdLazKHP7AXsaxyynhAAU="
SERVER_SALT = base64.b64decode("VH4EErGPaqJpsf05kuJoBw==")


store_module: Any = shared_module("secret_files")


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
    def test_the_tracked_list_is_valid_and_names_each_tool_and_each_database_once(self) -> None:
        tools = accounts.parse_tools(TOOLS_FILE.read_text())
        owned = [database for databases in tools.values() for database in databases]
        self.assertEqual(len(owned), len(set(owned)))
        self.assertTrue({"nessie", "airbyte"} <= set(tools))

    def test_the_tracked_list_gives_airbyte_its_own_database_and_the_two_that_temporal_needs(self) -> None:
        tools = accounts.parse_tools(TOOLS_FILE.read_text())
        self.assertEqual(["nessie"], tools["nessie"])
        self.assertEqual(["airbyte", "airbyte_temporal", "airbyte_temporal_visibility"], tools["airbyte"])

    def test_comments_and_blank_lines_are_ignored_in_order(self) -> None:
        self.assertEqual({"b": ["b"], "a": ["a"]}, accounts.parse_tools("# tools\nb\n\n  a  # trailing\n"))
        self.assertEqual(["b", "a"], list(accounts.parse_tools("b\na\n")))

    def test_a_tool_with_a_colon_owns_the_databases_after_it_and_a_bare_name_owns_one_named_for_it(self) -> None:
        tools = accounts.parse_tools("nessie\nairbyte: airbyte  airbyte_temporal # trailing\n")
        self.assertEqual({"nessie": ["nessie"], "airbyte": ["airbyte", "airbyte_temporal"]}, tools)

    def test_a_database_named_twice_by_one_tool_is_refused_by_its_name(self) -> None:
        with self.assertRaisesRegex(accounts.AccountError, "database name 'x' is listed more than once"):
            accounts.parse_tools("a: x y x\n")

    def test_a_database_that_another_tool_owns_is_refused_by_its_name_and_its_owner(self) -> None:
        for text in ("nessie\nairbyte: airbyte nessie\n", "airbyte: airbyte nessie\nnessie\n", "a: x\nb: x\n"):
            with self.subTest(text=text), self.assertRaisesRegex(accounts.AccountError, "database name '(nessie|x)' belongs to another tool"):
                accounts.parse_tools(text)

    def test_a_tool_with_no_database_after_its_colon_is_refused(self) -> None:
        with self.assertRaisesRegex(accounts.AccountError, "tool 'airbyte' lists no database"):
            accounts.parse_tools("airbyte:\n")

    def test_a_database_name_that_could_alter_a_statement_or_clash_with_the_server_is_refused(self) -> None:
        for name in ("Temporal", "tem-poral", "1temporal", 'tem"poral', "tem;poral", "postgres", "pg_catalog", "template0", "a" * 32):
            with self.subTest(name=name), self.assertRaisesRegex(accounts.AccountError, "database name"):
                accounts.parse_tools(f"airbyte: airbyte {name}\n")

    def test_a_repeated_tool_is_refused_in_either_form(self) -> None:
        with self.assertRaisesRegex(accounts.AccountError, "more than once"):
            accounts.parse_tools("airbyte: airbyte airbyte_temporal\nairbyte\n")

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


class PasswordOwnerTest(unittest.TestCase):
    entries = [store_module.Entry("nessie", (10000, 10001)), store_module.Entry("airbyte", None)]

    def test_the_server_and_a_tool_with_no_user_of_its_own_get_the_root_owner_and_a_tool_gets_the_owner_of_its_line(self) -> None:
        owners = accounts.password_owners(["nessie", "airbyte"], self.entries, (0, 0))
        self.assertEqual({"postgres": (0, 0), "nessie": (10000, 10001), "airbyte": (0, 0)}, owners)

    def test_a_tool_that_no_service_line_names_is_refused_by_name_because_no_container_user_would_own_its_password(self) -> None:
        with self.assertRaisesRegex(accounts.AccountError, "tool extra has no line in the service list"):
            accounts.password_owners(["nessie", "extra"], self.entries, (0, 0))

    def test_the_server_is_always_for_the_root_owner_even_if_a_tool_is_named_for_it(self) -> None:
        self.assertEqual((7, 8), accounts.password_owners(["nessie"], self.entries, (7, 8))["postgres"])


class ReaderCharacterTest(unittest.TestCase):
    """SmallRye Config 3.17.2 expands ``${...}`` and drops a backslash before ``$`` after the parse, so Nessie's reader takes neither character."""

    def made(self, folder: Path, content: str, *, check_only: bool = False) -> tuple[Any, Path]:
        store = store_module.PasswordStore.in_secrets(folder)
        path = store.path("nessie")
        path.parent.mkdir(parents=True)
        path.write_text(content)
        path.chmod(0o600)
        accounts.make_password_files(store, {"nessie": (os.getuid(), os.getgid())}, check_only=check_only)
        return store, path

    def test_a_nessie_password_with_a_dollar_sign_or_a_backslash_is_refused_by_name_and_the_value_is_not_shown(self) -> None:
        for content in ("pre$fix-value-0123456789", "pre\\fix-value-0123456789", "${HOME}-value-0123456789", "tail-value-0123456789\\"):
            for check_only in (False, True):
                with self.subTest(content=content, check_only=check_only), tempfile.TemporaryDirectory() as scratch:
                    with self.assertRaises(accounts.AccountError) as caught:
                        self.made(Path(scratch), content, check_only=check_only)
                    self.assertIn("password file of nessie", str(caught.exception))
                    self.assertNotIn("value-0123456789", str(caught.exception))
                    self.assertEqual(content, (Path(scratch) / "postgres" / "nessie.password").read_text(), "the file is left as it was")

    def test_a_nessie_password_without_them_and_another_tools_password_with_them_pass(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            self.made(Path(scratch), "plain-value_0123456789")
        with tempfile.TemporaryDirectory() as scratch:
            store = store_module.PasswordStore.in_secrets(Path(scratch))
            store.path("airbyte").parent.mkdir(parents=True)
            store.path("airbyte").write_text("a$b\\c-value-0123456789")
            store.path("airbyte").chmod(0o600)
            accounts.make_password_files(store, {"airbyte": (os.getuid(), os.getgid())}, check_only=False)

    def test_a_password_that_the_step_generates_never_holds_them(self) -> None:
        for _ in range(50):
            with tempfile.TemporaryDirectory() as scratch:
                store = store_module.PasswordStore.in_secrets(Path(scratch))
                accounts.make_password_files(store, {"nessie": (os.getuid(), os.getgid())}, check_only=False)
                self.assertNotRegex(store.read("nessie"), r"[$\\]")


PASSWORDS = {"nessie": "n-secret-value", "airbyte": "a-secret-value", "extra": "e-secret-value"}
TEMPORAL = ["airbyte", "airbyte_temporal", "airbyte_temporal_visibility"]
TOOLS = {"nessie": ["nessie"], "airbyte": TEMPORAL}


def state_of(tools: dict[str, list[str]]) -> tuple[dict[str, Any], dict[str, Any]]:
    roles = {tool: accounts.RoleState(accounts.scram_verifier(PASSWORDS[tool]), True) for tool in tools}
    databases = {database: accounts.DatabaseState(tool, ()) for tool, owned in tools.items() for database in owned}
    return roles, databases


def plan(tools: dict[str, list[str]], roles: dict[str, Any], databases: dict[str, Any]) -> list[Any]:
    return list(accounts.plan(tools, roles, databases, PASSWORDS.__getitem__))


def descriptions(actions: list[Any]) -> list[str]:
    return [action.description for action in actions]


class PlanTest(unittest.TestCase):
    def test_an_empty_server_gets_each_account_then_each_of_its_databases_then_the_guard_of_each(self) -> None:
        actions = plan(TOOLS, {}, {})
        self.assertEqual(
            [
                "create account nessie",
                "create database nessie",
                "refuse every other account on database nessie",
                "create account airbyte",
                "create database airbyte",
                "refuse every other account on database airbyte",
                "create database airbyte_temporal",
                "refuse every other account on database airbyte_temporal",
                "create database airbyte_temporal_visibility",
                "refuse every other account on database airbyte_temporal_visibility",
            ],
            descriptions(actions),
        )

    def test_a_server_that_matches_the_list_needs_no_action(self) -> None:
        self.assertEqual([], plan(TOOLS, *state_of(TOOLS)))

    def test_a_tool_added_later_gets_only_its_own_actions(self) -> None:
        roles, databases = state_of(TOOLS)
        actions = plan({**TOOLS, "extra": ["extra"]}, roles, databases)
        self.assertEqual(["create account extra", "create database extra", "refuse every other account on database extra"], descriptions(actions))

    def test_each_database_of_a_tool_is_created_owned_by_that_tool_and_never_by_the_administrator(self) -> None:
        creates = {action.description: action.sql for action in plan(TOOLS, {}, {}) if action.description.startswith("create database")}
        self.assertEqual(
            {
                "create database airbyte": 'CREATE DATABASE "airbyte" OWNER "airbyte"',
                "create database airbyte_temporal": 'CREATE DATABASE "airbyte_temporal" OWNER "airbyte"',
                "create database airbyte_temporal_visibility": 'CREATE DATABASE "airbyte_temporal_visibility" OWNER "airbyte"',
                "create database nessie": 'CREATE DATABASE "nessie" OWNER "nessie"',
            },
            creates,
        )

    def test_every_database_removes_public_so_no_other_tools_account_can_connect(self) -> None:
        guards = {action.description: action.sql for action in plan(TOOLS, {}, {}) if action.description.startswith("refuse every other account")}
        self.assertEqual(4, len(guards))
        for description, sql in guards.items():
            database = description.rsplit(" ", 1)[1]
            with self.subTest(database=database):
                self.assertEqual(f'REVOKE ALL ON DATABASE "{database}" FROM PUBLIC', sql)

    def test_no_statement_gives_any_account_the_right_to_create_a_database(self) -> None:
        statements = [action.sql for action in plan(TOOLS, {}, {})]
        accounts_made = [sql for sql in statements if sql.startswith("CREATE ROLE")]
        self.assertEqual(2, len(accounts_made))
        for sql in accounts_made:
            self.assertIn("NOCREATEDB", sql)
        self.assertFalse([sql for sql in statements if re.search(r"(?<!NO)CREATEDB", sql)])

    def test_a_missing_temporal_database_is_created_alone_and_the_ones_that_exist_are_left(self) -> None:
        roles, databases = state_of(TOOLS)
        del databases["airbyte_temporal"]
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["create database airbyte_temporal", "refuse every other account on database airbyte_temporal"], descriptions(actions))

    def test_a_grant_on_a_temporal_database_is_revoked_for_that_grantee_only(self) -> None:
        roles, databases = state_of(TOOLS)
        databases["airbyte_temporal_visibility"] = accounts.DatabaseState("airbyte", ("PUBLIC", '"nessie"'))
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["refuse every other account on database airbyte_temporal_visibility"], descriptions(actions))
        self.assertEqual('REVOKE ALL ON DATABASE "airbyte_temporal_visibility" FROM PUBLIC, "nessie"', actions[0].sql)

    def test_a_temporal_database_with_another_owner_is_refused_and_named(self) -> None:
        roles, databases = state_of(TOOLS)
        databases["airbyte_temporal"] = accounts.DatabaseState("postgres", ())
        with self.assertRaisesRegex(accounts.AccountError, "database airbyte_temporal is owned by another account"):
            plan(TOOLS, roles, databases)

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
        roles, databases = state_of(TOOLS)
        roles["nessie"] = accounts.RoleState(accounts.scram_verifier("an-older-password"), True)
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["reset the password of account nessie"], descriptions(actions))
        self.assertTrue(actions[0].sql.startswith('ALTER ROLE "nessie" PASSWORD \'SCRAM-SHA-256$'))

    def test_a_widened_account_is_narrowed_and_its_password_is_not_touched(self) -> None:
        roles, databases = state_of(TOOLS)
        roles["airbyte"] = accounts.RoleState(roles["airbyte"].verifier, False)
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["narrow the attributes of account airbyte"], descriptions(actions))
        self.assertEqual('ALTER ROLE "airbyte" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', actions[0].sql)

    def test_a_grant_on_a_tools_database_is_revoked_for_that_grantee(self) -> None:
        roles, databases = state_of(TOOLS)
        databases["nessie"] = accounts.DatabaseState("nessie", ("PUBLIC", '"someone"'))
        actions = plan(TOOLS, roles, databases)
        self.assertEqual(["refuse every other account on database nessie"], descriptions(actions))
        self.assertEqual('REVOKE ALL ON DATABASE "nessie" FROM PUBLIC, "someone"', actions[0].sql)

    def test_a_database_with_another_owner_is_refused_and_named_and_nothing_is_planned_for_it(self) -> None:
        roles, databases = state_of(TOOLS)
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

    def test_the_query_holds_no_name_and_reads_the_accounts_and_the_databases_from_one_variable_each(self) -> None:
        self.assertEqual("nessie,airbyte", accounts.tool_variable(TOOLS))
        self.assertEqual("nessie,airbyte,airbyte_temporal,airbyte_temporal_visibility", accounts.database_variable(TOOLS))
        self.assertNotIn("nessie", accounts.STATE_QUERY)
        self.assertEqual(1, accounts.STATE_QUERY.count(":'tools'"))
        self.assertEqual(1, accounts.STATE_QUERY.count(":'databases'"))


class ServerFailureTest(unittest.TestCase):
    """A failed psql call shows the service, the exit code and one fixed reason word. psql repeats the failed statement; the server's text can hold a name."""

    LEAKED = "/srv/example.invalid/pg/leaked.conf token=eyJleaked-pg-token-9"
    TEXTS = {
        "refused": f'psql: error: connection to server failed: FATAL:  password authentication failed for user "{LEAKED}"',
        "unreachable": f"psql: error: connection to server on socket {LEAKED} failed: could not connect to server",
        "not found": f'ERROR:  role "{LEAKED}" does not exist',
        "failed": f"ERROR:  syntax error at or near {LEAKED}\nLINE 1: CREATE ROLE x PASSWORD 'SCRAM-SHA-256$4096:aaaa$bbbb:cccc'",
    }

    def failure(self, error: str, code: int = 1) -> str:
        server = accounts.Server(lambda command, data: (code, "the answer", error), ["docker", "compose"])
        with self.assertRaises(accounts.AccountError) as caught:
            server.psql("SELECT 1;", {"nessie": ["nessie"]})
        return str(caught.exception)

    def test_each_failed_call_shows_the_service_the_code_and_one_fixed_reason_and_none_of_psqls_text(self) -> None:
        for reason, text in self.TEXTS.items():
            with self.subTest(reason=reason):
                message = self.failure(text, code=2)
                self.assertEqual(f"service postgres: psql exited with 2 (reason: {reason})", message)
                for leaked in ("leaked", "eyJleaked", "SCRAM", "CREATE ROLE", "<verifier>"):
                    self.assertNotIn(leaked, message)

    def test_a_server_that_is_not_started_or_a_service_that_does_not_run_gets_the_word_that_names_it(self) -> None:
        self.assertIn("(reason: unreachable)", self.failure("FATAL:  the database system is starting up"))
        self.assertIn("(reason: not found)", self.failure('service "postgres" is not running'))
        self.assertIn("(reason: refused)", self.failure("ERROR:  permission denied for database postgres"))

    def test_an_empty_text_is_a_plain_failure(self) -> None:
        self.assertEqual("service postgres: psql exited with 1 (reason: failed)", self.failure(""))

    def test_a_text_with_the_signs_of_two_reasons_gets_the_first_of_refused_then_unreachable_then_not_found(self) -> None:
        refused, unreachable, missing = "password authentication failed", "could not connect to server", "does not exist"
        self.assertIn("(reason: refused)", self.failure(f"{missing} {unreachable} {refused}"))
        self.assertIn("(reason: unreachable)", self.failure(f"{missing} {unreachable}"))
        self.assertIn("(reason: not found)", self.failure(missing))

    SIGNS = {
        "refused": ("authentication failed", "permission denied", "must be superuser", "no pg_hba.conf entry"),
        "unreachable": (
            "could not connect to server",
            "Connection refused",
            "is starting up",
            "is shutting down",
            "closed the connection unexpectedly",
            "Is the server running",
        ),
        "not found": ("does not exist", "is not running", "no such service", "executable file not found"),
    }

    def test_every_sign_of_each_reason_is_named_by_its_word_and_the_list_holds_no_other_sign(self) -> None:
        for reason, signs in self.SIGNS.items():
            for sign in signs:
                with self.subTest(reason=reason, sign=sign):
                    self.assertIn(f"(reason: {reason})", self.failure(f"psql: {sign}"))
        self.assertEqual(self.SIGNS, {word: tuple(signs) for word, signs in accounts.REASONS})


if __name__ == "__main__":
    unittest.main()
