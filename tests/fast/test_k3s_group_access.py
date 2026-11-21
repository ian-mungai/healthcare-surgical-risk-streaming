"""The service account of the group commands may read and scale Airbyte's workloads in Airbyte's namespace and do nothing else.

The first group renders the real template. The second plants one widening at a time in a copy of its documents and expects the checker to name it.
"""

from __future__ import annotations

import copy
import unittest
from pathlib import Path
from typing import Any

import jinja2
import yaml

TEMPLATE = Path(__file__).resolve().parents[2] / "ansible" / "roles" / "airbyte" / "templates" / "group_access.yaml.j2"
ACCOUNT = "dagster-groups"
NAMESPACE = "airbyte"
ALLOWED = {
    ("apps", "deployments", "get"),
    ("apps", "deployments", "list"),
    ("apps", "statefulsets", "get"),
    ("apps", "statefulsets", "list"),
    ("", "pods", "get"),
    ("", "pods", "list"),
    ("apps", "deployments/scale", "get"),
    ("apps", "deployments/scale", "patch"),
    ("apps", "statefulsets/scale", "get"),
    ("apps", "statefulsets/scale", "patch"),
}


def render(namespace: str = NAMESPACE, account: str = ACCOUNT) -> list[dict[str, Any]]:
    text = (
        jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=True)
        .from_string(TEMPLATE.read_text())
        .render(airbyte_namespace=namespace, airbyte_group_account=account)
    )
    return [document for document in yaml.safe_load_all(text) if document]


def granted(rules: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    return {
        (group, resource, verb)
        for rule in rules
        for group in rule.get("apiGroups", [])
        for resource in rule.get("resources", [])
        for verb in rule.get("verbs", [])
    }


def access_problems(documents: list[dict[str, Any]]) -> list[str]:
    """Each way that the rendered objects give the account more than the allowed list, or give it outside the namespace."""
    problems: list[str] = []
    kinds = [document["kind"] for document in documents]
    for forbidden in ("ClusterRole", "ClusterRoleBinding"):
        if forbidden in kinds:
            problems.append(f"{forbidden}: nothing may be granted cluster-wide")
    for document in documents:
        if document["kind"] in ("Role", "RoleBinding", "ServiceAccount", "Secret") and document["metadata"].get("namespace") != NAMESPACE:
            problems.append(f"{document['kind']}: must be in the namespace {NAMESPACE}")
    for role in (document for document in documents if document["kind"] == "Role"):
        rules = role.get("rules", [])
        if any("resourceNames" in rule or "nonResourceURLs" in rule for rule in rules):
            problems.append("Role: a rule that names objects or addresses is not in the allowed list")
        extra = granted(rules) - ALLOWED
        missing = ALLOWED - granted(rules)
        if extra:
            problems.append(f"Role: grants more than the allowed list: {sorted(extra)}")
        if missing:
            problems.append(f"Role: lacks part of the allowed list: {sorted(missing)}")
    for binding in (document for document in documents if document["kind"] == "RoleBinding"):
        subjects = [(item.get("kind"), item.get("name"), item.get("namespace")) for item in binding.get("subjects", [])]
        if subjects != [("ServiceAccount", ACCOUNT, NAMESPACE)]:
            problems.append(f"RoleBinding: must bind only the service account {ACCOUNT}, found {subjects}")
        if binding.get("roleRef", {}).get("kind") != "Role" or binding["roleRef"].get("name") != ACCOUNT:
            problems.append("RoleBinding: must refer to the Role of the account")
    secrets = [document for document in documents if document["kind"] == "Secret"]
    if len(secrets) != 1 or secrets[0].get("type") != "kubernetes.io/service-account-token":
        problems.append("Secret: there must be exactly one, of the type that holds the account's token")
    return problems


class RealTemplateTest(unittest.TestCase):
    def test_the_rendered_objects_grant_exactly_the_allowed_list_in_the_namespace(self) -> None:
        documents = render()
        self.assertEqual([], access_problems(documents))
        self.assertEqual(["ServiceAccount", "Role", "RoleBinding", "Secret"], [document["kind"] for document in documents])

    def test_the_token_secret_belongs_to_the_account(self) -> None:
        secret = next(document for document in render() if document["kind"] == "Secret")
        self.assertEqual(ACCOUNT, secret["metadata"]["annotations"]["kubernetes.io/service-account.name"])


class PlantedWideningTest(unittest.TestCase):
    def widened(self, mutate: Any) -> list[str]:
        documents = copy.deepcopy(render())
        mutate(documents)
        return access_problems(documents)

    def role(self, documents: list[dict[str, Any]]) -> dict[str, Any]:
        return next(document for document in documents if document["kind"] == "Role")

    def test_an_extra_verb_is_named(self) -> None:
        for verb in ("delete", "create", "update", "patch", "*"):
            with self.subTest(verb=verb):
                problems = self.widened(lambda documents, verb=verb: self.role(documents)["rules"][1]["verbs"].append(verb))
                self.assertEqual(1, len(problems))
                self.assertIn("grants more than the allowed list", problems[0])

    def test_an_extra_resource_is_named(self) -> None:
        for resource in ("secrets", "pods/exec", "pods/log", "configmaps", "*"):
            with self.subTest(resource=resource):
                problems = self.widened(lambda documents, resource=resource: self.role(documents)["rules"][1]["resources"].append(resource))
                self.assertEqual(1, len(problems))
                self.assertIn("grants more than the allowed list", problems[0])

    def test_an_extra_group_is_named(self) -> None:
        problems = self.widened(lambda documents: self.role(documents)["rules"][0]["apiGroups"].append("*"))
        self.assertEqual(1, len(problems))

    def test_a_missing_part_of_the_list_is_named(self) -> None:
        problems = self.widened(lambda documents: self.role(documents)["rules"][2]["verbs"].remove("patch"))
        self.assertEqual(1, len(problems))
        self.assertIn("lacks", problems[0])

    def test_a_rule_that_names_objects_is_named(self) -> None:
        problems = self.widened(lambda documents: self.role(documents)["rules"][0].update(resourceNames=["x"]))
        self.assertTrue(any("names objects" in problem for problem in problems))

    def test_a_cluster_wide_role_or_binding_is_named(self) -> None:
        problems = self.widened(lambda documents: documents[1].update(kind="ClusterRole"))
        self.assertTrue(any("ClusterRole: nothing may be granted cluster-wide" in problem for problem in problems))
        problems = self.widened(lambda documents: documents[2].update(kind="ClusterRoleBinding"))
        self.assertTrue(any("ClusterRoleBinding" in problem for problem in problems))

    def test_a_binding_to_another_subject_is_named(self) -> None:
        for subject in ({"kind": "Group", "name": "system:authenticated"}, {"kind": "ServiceAccount", "name": "default", "namespace": NAMESPACE}):
            with self.subTest(subject=subject):
                problems = self.widened(lambda documents, subject=subject: documents[2]["subjects"].append(subject))
                self.assertTrue(any("must bind only the service account" in problem for problem in problems))

    def test_an_object_in_another_namespace_is_named(self) -> None:
        problems = self.widened(lambda documents: documents[0]["metadata"].update(namespace="kube-system"))
        self.assertTrue(any("must be in the namespace" in problem for problem in problems))


if __name__ == "__main__":
    unittest.main()
