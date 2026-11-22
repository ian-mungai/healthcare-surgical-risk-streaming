"""The role's step that applies the raw connection: its pins, its place in the deployment and the document it renders from the pinned release.

Applying the document to a running Airbyte needs the cluster and is proven in the host's live batch. These cases run the rendering step through the real
``ansible-playbook`` and read the task files for the rules that every task of the role keeps.
"""

from __future__ import annotations

import json
import re
import unittest
from typing import Any

from scripts.stack.yaml_text import load_yaml
from tests.fast.test_airbyte_role import ROLE, flatten, group_variables, role_tasks, run_tasks

BUCKET = "imungai-example-project"
FULL_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
VARIABLES: dict[str, object] = {
    "airbyte_landing_bucket": BUCKET,
    "workload_access_region": "us-west-2",
    "airbyte_source_s3_version": "4.17.1",
    "airbyte_destination_s3_version": "1.9.11",
    "airbyte_web_port": 8000,
    "airbyte_namespace": "airbyte",
    "airbyte_connection_deadline_seconds": 600,
    "airbyte_connection_scope": "full_release",
}


def render(variables: dict[str, object]) -> tuple[int, str]:
    return run_tasks(["connection_document.yml"], variables)


class PinsTest(unittest.TestCase):
    def test_both_connectors_are_pinned_by_a_full_version_in_the_group_variables(self) -> None:
        variables = group_variables()
        for name in ("airbyte_source_s3_version", "airbyte_destination_s3_version"):
            with self.subTest(name=name):
                self.assertRegex(str(variables[name]), FULL_VERSION.pattern)

    def test_the_staging_size_has_one_explicit_value_that_the_host_settings_take(self) -> None:
        self.assertIn("airbyte_staging_size", group_variables())
        template = (ROLE / "templates" / "host.values.yaml.j2").read_text()
        self.assertEqual(2, template.count("{{ airbyte_staging_size | to_json }}"))

    def test_the_landing_bucket_has_no_default_so_a_deployment_without_one_is_refused(self) -> None:
        self.assertEqual("", group_variables().get("airbyte_landing_bucket", ""))


class PlacementTest(unittest.TestCase):
    def test_the_connection_is_applied_last_after_airbyte_answers(self) -> None:
        tasks = load_yaml((ROLE / "tasks" / "main.yml").read_text())
        if not isinstance(tasks, list):
            self.fail("the task list must be a list")
        names = [str(task.get("ansible.builtin.include_tasks")) for task in tasks if isinstance(task, dict)]
        self.assertEqual("connection.yml", names[-1])
        self.assertLess(names.index("verify.yml"), names.index("connection.yml"))

    def test_the_module_gets_the_document_the_local_address_the_namespace_and_a_deadline(self) -> None:
        module = next(task for task in role_tasks()["connection.yml"] if "airbyte_connection" in task)
        arguments = module["airbyte_connection"]
        self.assertEqual("{{ airbyte_connection_document }}", arguments["document"])
        self.assertIn("127.0.0.1:{{ airbyte_web_port }}", arguments["base_url"])
        self.assertEqual("{{ airbyte_namespace }}", arguments["namespace"])
        self.assertIn("timeout", module)

    def test_every_command_of_the_new_files_has_a_deadline_and_runs_on_this_computer_for_the_repository(self) -> None:
        for task in role_tasks()["connection_document.yml"]:
            if "ansible.builtin.command" in task:
                self.assertIn("timeout", task)
                self.assertEqual("localhost", task["delegate_to"])
                self.assertIs(False, task["become"])


class DocumentTest(unittest.TestCase):
    def document(self, variables: dict[str, object]) -> dict[str, Any]:
        code, output = render(variables)
        self.assertEqual(0, code, output)
        marker = '"airbyte_connection_document": '
        if marker not in output:
            self.fail("the rendering step shows no document")
        parsed: dict[str, Any] = json.JSONDecoder().raw_decode(output, output.index(marker) + len(marker))[0]
        return parsed

    def test_the_rendered_document_names_the_bucket_the_region_and_both_pinned_versions(self) -> None:
        document = self.document(VARIABLES)
        self.assertEqual(BUCKET, document["destination"]["configuration"]["s3_bucket_name"])
        self.assertEqual("us-west-2", document["destination"]["configuration"]["s3_bucket_region"])
        self.assertEqual("4.17.1", document["source"]["definition"]["dockerImageTag"])
        self.assertEqual("1.9.11", document["destination"]["definition"]["dockerImageTag"])
        self.assertEqual(6390, len(document["source"]["configuration"]["streams"][0]["globs"]))

    def test_the_scope_full_release_selects_the_pins_6390_files_and_fixed_sample_the_15_of_the_sample(self) -> None:
        self.assertEqual(6390, len(self.document(VARIABLES | {"airbyte_connection_scope": "full_release"})["source"]["configuration"]["streams"][0]["globs"]))
        self.assertEqual(15, len(self.document(VARIABLES | {"airbyte_connection_scope": "fixed_sample"})["source"]["configuration"]["streams"][0]["globs"]))

    def test_another_scope_is_refused_and_the_scope_defaults_to_the_full_release(self) -> None:
        code, output = render(VARIABLES | {"airbyte_connection_scope": "everything"})
        self.assertNotEqual(0, code)
        self.assertIn("airbyte_connection_scope", output)
        self.assertEqual("full_release", group_variables()["airbyte_connection_scope"])

    def test_a_deployment_without_a_bucket_or_a_region_is_refused_and_says_which(self) -> None:
        for name in ("airbyte_landing_bucket", "workload_access_region"):
            with self.subTest(name=name):
                code, output = render(VARIABLES | {name: ""})
                self.assertNotEqual(0, code)
                self.assertIn(name, output)

    def test_a_bucket_that_is_not_a_bucket_name_is_refused_by_the_renderer(self) -> None:
        code, output = render(VARIABLES | {"airbyte_landing_bucket": "Not A Bucket"})
        self.assertNotEqual(0, code)
        self.assertIn("the bucket is not a valid bucket name", output)


class FlattenTest(unittest.TestCase):
    def test_every_task_of_the_new_files_is_named(self) -> None:
        for name in ("connection.yml", "connection_document.yml"):
            for task in flatten(role_tasks()[name]):
                with self.subTest(file=name):
                    self.assertIn("name", task)


if __name__ == "__main__":
    unittest.main()
