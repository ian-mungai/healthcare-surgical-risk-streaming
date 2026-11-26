"""Each external secret is read by its consumer's OpenBao policy and by no other service's policy; the admin policy writes the values."""

from __future__ import annotations

import re
import unittest
from typing import Any

from scripts.project_config import ROOT
from tests.fast.ansible_paths import shared_module
from tests.fast.test_openbao_bootstrap import MAX_TTL, TTL, bootstrap, good_state

LIST_TEXT = (ROOT / "compose" / "external_secrets.txt").read_text()
SERVICES_TEXT = (ROOT / "compose" / "openbao_services.txt").read_text()
HCL = (ROOT / "compose" / "openbao" / "openbao.hcl").read_text()
RULE = re.compile(r'path\s+"([^"]+)"\s*\{\s*capabilities\s*=\s*\[([^\]]*)\]')


def declared() -> list[Any]:
    return list(shared_module("external_secrets").parse_external(LIST_TEXT))


def services() -> list[str]:
    return list(shared_module("secret_files").parse_names(SERVICES_TEXT, "service"))


def policies() -> dict[str, str]:
    paths = bootstrap.external_paths(LIST_TEXT, services())
    return {name: bootstrap.policy_text(name, paths.get(name, [])) for name in services()}


def grants(policy: str, path: str, capability: str) -> bool:
    """Whether a policy text grants the capability on the path, by exact path or by a trailing ``*`` (OpenBao's prefix glob)."""
    for glob, listed in RULE.findall(policy):
        matches = path.startswith(glob[:-1]) if glob.endswith("*") else path == glob
        if matches and capability in re.findall(r'"(\w+)"', listed):
            return True
    return False


class ReaderTest(unittest.TestCase):
    def test_every_declared_path_is_read_by_the_policy_of_its_consumer_and_by_no_other_service_policy(self) -> None:
        for item in declared():
            readers = {name for name, text in policies().items() if grants(text, item.path, "read")}
            with self.subTest(secret=item.name):
                self.assertEqual({item.consumer, "external_intake"}, readers)

    def test_no_service_policy_but_the_intake_role_s_can_write_an_external_path_and_none_can_list_one(self) -> None:
        for item in declared():
            for name, text in policies().items():
                for capability in ("create", "update", "delete", "list"):
                    with self.subTest(secret=item.name, service=name, capability=capability):
                        self.assertEqual(name == "external_intake" and capability != "list", grants(text, item.path, capability))

    def test_a_consumer_policy_reads_the_declared_path_and_its_own_folder_and_nothing_else(self) -> None:
        paths = re.findall(r'path "([^"]+)"', policies()["deploy"])
        self.assertEqual(
            ["secret/data/services/deploy/*", "secret/metadata/services/deploy/*", "secret/data/external/dockerhub_read"],
            paths,
        )

    def test_a_service_that_consumes_nothing_external_has_no_external_path_in_its_policy(self) -> None:
        for name, text in policies().items():
            if name not in {item.consumer for item in declared()} | {"external_intake"}:
                with self.subTest(service=name):
                    self.assertNotIn("external", text)

    def test_a_consumer_that_is_no_listed_service_is_refused_by_name(self) -> None:
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.external_paths("alpha ghost secret/data/external/alpha dockerhub_token\n", ["nessie"])
        self.assertIn("ghost", str(caught.exception))


class AdminPolicyTest(unittest.TestCase):
    def admin_text(self) -> str:
        return HCL.split("write_admin_policy", 1)[1].split("EOT", 2)[1]

    def test_the_admin_policy_has_no_external_rule_because_the_intake_role_holds_them_and_a_host_made_before_them_could_never_get_one(self) -> None:
        self.assertEqual([], [glob for glob, _ in RULE.findall(self.admin_text()) if "external" in glob])


class IntakePolicyTest(unittest.TestCase):
    def intake(self) -> str:
        return policies()["external_intake"]

    def test_the_intake_role_creates_reads_updates_and_removes_each_declared_value_and_reads_its_metadata(self) -> None:
        for item in declared():
            for capability in ("create", "read", "update", "delete"):
                with self.subTest(secret=item.name, capability=capability):
                    self.assertTrue(grants(self.intake(), item.path, capability))
            self.assertTrue(grants(self.intake(), item.path.replace("/data/", "/metadata/"), "read"))

    def test_the_intake_policy_holds_exactly_the_declared_paths_and_their_metadata_and_cannot_list(self) -> None:
        expected = sorted(path for item in declared() for path in (item.path, item.path.replace("/data/", "/metadata/")))
        self.assertEqual(expected, sorted(re.findall(r'path "([^"]+)"', self.intake())))
        for item in declared():
            self.assertFalse(grants(self.intake(), item.path, "list"))
            self.assertFalse(grants(self.intake(), item.path.replace("/data/", "/metadata/"), "update"))
        for other in (
            "secret/data/services/nessie/postgres",
            "secret/data/external/not_declared",
            "sys/policies/acl/service_nessie",
            "auth/approle/role/nessie",
        ):
            self.assertFalse(grants(self.intake(), other, "read"), other)

    def test_the_intake_role_is_a_listed_service_that_runs_outside_compose_so_root_owns_its_secret_id_file(self) -> None:
        lines = [line.split("#", 1)[0].split() for line in SERVICES_TEXT.splitlines() if line.split("#", 1)[0].split()]
        self.assertIn(["external_intake"], lines)


class PlanTest(unittest.TestCase):
    def state_with_policies(self) -> tuple[list[str], dict[str, list[str]], Any]:
        names = services()
        state = good_state(names, [])
        paths = bootstrap.external_paths(LIST_TEXT, names)
        state.policies = {f"service_{name}": bootstrap.policy_text(name, paths.get(name, [])) for name in names}
        return names, paths, state

    def actions(self, names: list[str], paths: dict[str, list[str]], state: Any) -> list[str]:
        return [action.description for action in bootstrap.plan(names, [], state, lambda name: "", TTL, MAX_TTL, paths)]

    def test_a_server_whose_policies_match_the_list_needs_no_action(self) -> None:
        self.assertEqual([], self.actions(*self.state_with_policies()))

    def test_a_consumer_policy_without_its_external_path_is_written_back(self) -> None:
        names, paths, state = self.state_with_policies()
        state.policies["service_deploy"] = bootstrap.policy_text("deploy")
        self.assertEqual(["write policy service_deploy"], self.actions(names, paths, state))

    def test_a_retired_policy_that_holds_an_external_grant_is_rewritten_without_it_and_one_that_holds_none_is_left_alone(self) -> None:
        names, paths, state = self.state_with_policies()
        with_grant = bootstrap.policy_text("old", ["secret/data/external/dockerhub_read"])
        state.retired = {"old": with_grant, "older": bootstrap.policy_text("older")}
        actions = bootstrap.plan(names, [], state, lambda name: "", TTL, MAX_TTL, paths)
        self.assertEqual(["drop the external grants of the policy of a service that left the list"], [action.description for action in actions])
        self.assertEqual(bootstrap.policy_text("old"), actions[0].policy)

    def test_a_policy_widened_to_another_services_external_path_is_written_back(self) -> None:
        names, paths, state = self.state_with_policies()
        state.policies["service_nessie"] += 'path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n'
        self.assertEqual(["write policy service_nessie"], self.actions(names, paths, state))


if __name__ == "__main__":
    unittest.main()
