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
                self.assertEqual({item.consumer}, readers)

    def test_no_service_policy_can_list_or_write_an_external_path(self) -> None:
        for item in declared():
            for name, text in policies().items():
                for capability in ("create", "update", "delete", "list"):
                    with self.subTest(secret=item.name, service=name, capability=capability):
                        self.assertFalse(grants(text, item.path, capability))

    def test_a_consumer_policy_reads_the_declared_path_and_its_own_folder_and_nothing_else(self) -> None:
        paths = re.findall(r'path "([^"]+)"', policies()["deploy"])
        self.assertEqual(
            ["secret/data/services/deploy/*", "secret/metadata/services/deploy/*", "secret/data/external/dockerhub_read"],
            paths,
        )

    def test_a_service_that_consumes_nothing_external_has_no_external_path_in_its_policy(self) -> None:
        for name, text in policies().items():
            if name not in {item.consumer for item in declared()}:
                with self.subTest(service=name):
                    self.assertNotIn("external", text)

    def test_a_consumer_that_is_no_listed_service_is_refused_by_name(self) -> None:
        with self.assertRaises(bootstrap.BootstrapError) as caught:
            bootstrap.external_paths("alpha ghost secret/data/external/alpha dockerhub_token\n", ["nessie"])
        self.assertIn("ghost", str(caught.exception))


class AdminPolicyTest(unittest.TestCase):
    def admin_text(self) -> str:
        return HCL.split("write_admin_policy", 1)[1].split("EOT", 2)[1]

    def test_the_admin_policy_writes_reads_and_removes_an_external_value_and_reads_its_metadata(self) -> None:
        text = self.admin_text()
        for item in declared():
            for capability in ("create", "read", "update", "delete"):
                with self.subTest(secret=item.name, capability=capability):
                    self.assertTrue(grants(text, item.path, capability))
            self.assertTrue(grants(text, item.path.replace("/data/", "/metadata/"), "read"))

    def test_the_admin_policy_has_no_other_external_rule(self) -> None:
        rules = [glob for glob, _ in RULE.findall(self.admin_text()) if "external" in glob]
        self.assertEqual(["secret/data/external/*", "secret/metadata/external/*"], rules)


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

    def test_a_policy_widened_to_another_services_external_path_is_written_back(self) -> None:
        names, paths, state = self.state_with_policies()
        state.policies["service_nessie"] += 'path "secret/data/external/dockerhub_read" { capabilities = ["read"] }\n'
        self.assertEqual(["write policy service_nessie"], self.actions(names, paths, state))


if __name__ == "__main__":
    unittest.main()
