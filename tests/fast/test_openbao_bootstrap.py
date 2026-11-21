"""The OpenBao bootstrap's rules: the service list, the policy each service gets and the plan that makes the server match the list.

Real runs against a server are the acceptance test's job. Isolated cases are kept for what a fresh server never shows: a list entry that could alter
a policy or an API path, and a server that drifted (a policy or role that was widened, a stale password, a secret ID that no longer works).
"""

from __future__ import annotations

import importlib.util
import re
import sys
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "openbao" / "library" / "openbao_bootstrap.py"
SERVICES_FILE = ROOT / "compose" / "openbao_services.txt"
TOOLS_FILE = ROOT / "compose" / "postgres_tools.txt"
COMPOSE_FILE = ROOT / "compose" / "services" / "openbao.yml"
DEFAULTS_FILE = ROOT / "ansible" / "roles" / "openbao" / "defaults" / "main.yml"
TTL, MAX_TTL = 3600, 86400


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("openbao_bootstrap", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bootstrap = load_module()


def good_state(services: list[str], tools: list[str]) -> Any:
    """A server that already matches the desired state of ``services``."""
    return bootstrap.State(
        policies={f"service_{name}": bootstrap.policy_text(name) for name in services},
        roles={name: {**bootstrap.role_settings(name, TTL, MAX_TTL), "role_id": name} for name in services},
        secret_id_valid={name: True for name in services},
        passwords={name: f"password of {name}" for name in tools},
    )


def planned(services: list[str], tools: list[str], state: Any) -> list[str]:
    actions = bootstrap.plan(services, tools, state, lambda name: f"password of {name}", TTL, MAX_TTL)
    return [action.description for action in actions]


class ServiceListTest(unittest.TestCase):
    def test_the_tracked_lists_are_valid_and_every_postgres_tool_is_a_service(self) -> None:
        services = bootstrap.parse_names(SERVICES_FILE.read_text(), "service")
        tools = bootstrap.parse_names(TOOLS_FILE.read_text(), "tool")
        self.assertTrue({"nessie", "airbyte"} <= set(services))
        self.assertEqual([], sorted(set(tools) - set(services)), "a PostgreSQL tool has no OpenBao role to read its password")

    def test_comments_and_blank_lines_are_ignored_in_order(self) -> None:
        self.assertEqual(["b", "a"], bootstrap.parse_names("# services\nb\n\n  a  # trailing\n", "service"))

    def test_a_name_that_could_alter_a_policy_or_an_api_path_is_refused(self) -> None:
        for name in ("Nessie", "nes-sie", "1nessie", 'nes"sie', "nes/sie", "../sie", "nes*", "bootstrap", "a" * 32, "default", "root"):
            with self.subTest(name=name), self.assertRaises(bootstrap.BootstrapError) as caught:
                bootstrap.parse_names(f"{name}\n", "service")
            self.assertIn("service name", str(caught.exception))

    def test_a_repeated_name_or_an_empty_list_is_refused(self) -> None:
        with self.assertRaisesRegex(bootstrap.BootstrapError, "more than once"):
            bootstrap.parse_names("nessie\nairbyte\nnessie\n", "service")
        with self.assertRaisesRegex(bootstrap.BootstrapError, "no service"):
            bootstrap.parse_names("# nothing\n", "service")


class OwnerTest(unittest.TestCase):
    def test_a_service_line_may_name_the_numeric_user_its_container_runs_as(self) -> None:
        entries = bootstrap.parse_entries("nessie 185:185  # runs as a fixed user\nairbyte\n", "service")
        self.assertEqual([("nessie", (185, 185)), ("airbyte", None)], [(entry.name, entry.owner) for entry in entries])

    def test_an_owner_that_is_not_a_numeric_user_other_than_root_is_refused(self) -> None:
        for owner in ("185", "a:b", "-1:5", "0:5", "5:-1", "1:2:3", "185:", ":185", "5:0x10", "+5:6"):
            with self.subTest(owner=owner), self.assertRaises(bootstrap.BootstrapError) as caught:
                bootstrap.parse_entries(f"nessie {owner}\n", "service")
            self.assertIn("owner", str(caught.exception))

    def test_a_group_of_zero_is_allowed_only_with_a_user_other_than_root(self) -> None:
        self.assertEqual((5, 0), bootstrap.parse_entries("nessie 5:0\n", "service")[0].owner)

    def test_a_tool_line_holds_a_name_only_and_a_line_holds_nothing_after_the_owner(self) -> None:
        for text, what in (("nessie 185:185\n", "tool"), ("nessie 185:185 extra\n", "service")):
            with self.subTest(text=text), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.parse_entries(text, what)

    def test_the_compose_service_runs_as_the_numeric_user_that_owns_the_secret_files_and_it_is_not_root(self) -> None:
        compose: Any = load_yaml(COMPOSE_FILE.read_text())
        defaults: Any = load_yaml(DEFAULTS_FILE.read_text())
        user = str(compose["services"]["openbao"]["user"])
        self.assertRegex(user, r"^[1-9]\d*:\d+$")
        self.assertEqual(user, defaults["openbao_user"])


class PolicyTest(unittest.TestCase):
    def test_a_policy_reaches_only_the_path_of_its_own_service(self) -> None:
        for name in ("nessie", "nessie_two"):
            paths = re.findall(r'path "([^"]+)"', bootstrap.policy_text(name))
            with self.subTest(service=name):
                self.assertTrue(paths)
                for path in paths:
                    self.assertRegex(path, rf"^secret/(data|metadata)/services/{name}/\*$")

    def test_a_policy_grants_reading_and_nothing_that_writes(self) -> None:
        capabilities = set(re.findall(r'"(\w+)"', bootstrap.policy_text("nessie").replace('path "', "")))
        self.assertEqual({"read", "list"}, capabilities)


class RoleTest(unittest.TestCase):
    def test_a_role_is_bound_to_its_own_policy_with_a_secret_id_and_a_numbered_role_id_of_the_service_name(self) -> None:
        settings = bootstrap.role_settings("nessie", TTL, MAX_TTL)
        self.assertEqual(["service_nessie"], settings["token_policies"])
        self.assertEqual((TTL, MAX_TTL), (settings["token_ttl"], settings["token_max_ttl"]))
        self.assertTrue(settings["bind_secret_id"])
        self.assertEqual("nessie", settings["role_id"])


class PlanTest(unittest.TestCase):
    services = ("nessie", "airbyte", "dagster")
    tools = ("nessie", "airbyte")

    def test_a_server_that_matches_needs_no_action(self) -> None:
        self.assertEqual([], planned(list(self.services), list(self.tools), good_state(list(self.services), list(self.tools))))

    def test_an_empty_server_gets_a_policy_a_role_and_a_secret_id_for_each_service_and_a_password_for_each_tool(self) -> None:
        empty = bootstrap.State(policies={}, roles={}, secret_id_valid={}, passwords={})
        actions = planned(["nessie", "dagster"], ["nessie"], empty)
        self.assertEqual(
            [
                "write policy service_nessie",
                "write role nessie",
                "issue a secret ID for nessie",
                "write the PostgreSQL password of nessie",
                "write policy service_dagster",
                "write role dagster",
                "issue a secret ID for dagster",
            ],
            actions,
        )

    def test_a_service_added_to_the_list_changes_nothing_for_the_others(self) -> None:
        state = good_state(["nessie", "airbyte"], ["nessie", "airbyte"])
        self.assertEqual(
            ["write policy service_dagster", "write role dagster", "issue a secret ID for dagster"],
            planned(["nessie", "airbyte", "dagster"], ["nessie", "airbyte"], state),
        )

    def test_a_widened_policy_is_written_back(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.policies["service_nessie"] += 'path "secret/data/*" { capabilities = ["read"] }\n'
        self.assertEqual(["write policy service_nessie"], planned(list(self.services), list(self.tools), state))

    def test_a_role_that_drifted_in_any_setting_is_written_back(self) -> None:
        for key, value in (
            ("token_policies", ["service_nessie", "bootstrap_admin"]),
            ("token_ttl", 7200),
            ("token_max_ttl", 1),
            ("bind_secret_id", False),
            ("role_id", "other"),
        ):
            state = good_state(list(self.services), list(self.tools))
            state.roles["nessie"][key] = value
            with self.subTest(setting=key):
                self.assertEqual(["write role nessie"], planned(list(self.services), list(self.tools), state))

    def test_a_secret_id_that_no_longer_works_is_issued_again(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.secret_id_valid["airbyte"] = False
        self.assertEqual(["issue a secret ID for airbyte"], planned(list(self.services), list(self.tools), state))

    def test_a_stale_or_missing_password_is_written_and_a_password_that_matches_is_not(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        state.passwords["nessie"] = "an older password"
        state.passwords["airbyte"] = None
        self.assertEqual(
            ["write the PostgreSQL password of nessie", "write the PostgreSQL password of airbyte"],
            planned(list(self.services), list(self.tools), state),
        )

    def test_a_service_that_is_no_tool_gets_no_password(self) -> None:
        state = good_state(list(self.services), list(self.tools))
        self.assertNotIn("dagster", state.passwords)
        self.assertEqual([], planned(list(self.services), list(self.tools), state))


if __name__ == "__main__":
    unittest.main()
