"""The record load is wired into the tracked deployment: the Dagster definitions merge its job, the job is declared in its service group with the heavy tag
rule, and the code location's Compose service carries the environment and the six secret files that the load reads.

Dagster is not installed in the tools environment, so the Python files are read as source. The load itself runs in the code location image.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path
from typing import Any

from orchestration.surgical_risk.concurrency import JOB_GROUPS
from scripts.ingestion.record_run import SECRET_FILES
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
DEFINITIONS = ROOT / "orchestration" / "surgical_risk" / "definitions.py"
RECORD_LOAD = ROOT / "orchestration" / "surgical_risk" / "record_load.py"
SERVICE = ROOT / "compose" / "services" / "dagster_code_location.yml"
SECRETS_FOLDER = "${SECRETS_DIR:?render the host settings first}/airbyte_api/"
SECRET_NAMES = {
    "airbyte_api_url": "api_url",
    "airbyte_nessie_url": "nessie_url",
    "airbyte_client_id": "client_id",
    "airbyte_client_secret": "client_secret",
    "airbyte_aws_access_key_id": "aws_access_key_id",
    "airbyte_aws_secret_access_key": "aws_secret_access_key",
}
ENVIRONMENT_FILES = {
    "AIRBYTE_API_URL_FILE": "airbyte_api_url",
    "AIRBYTE_NESSIE_URL_FILE": "airbyte_nessie_url",
    "AIRBYTE_CLIENT_ID_FILE": "airbyte_client_id",
    "AIRBYTE_CLIENT_SECRET_FILE": "airbyte_client_secret",
    "AIRBYTE_AWS_ACCESS_KEY_ID_FILE": "airbyte_aws_access_key_id",
    "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE": "airbyte_aws_secret_access_key",
}


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


class DagsterWiringTest(unittest.TestCase):
    def test_the_definitions_import_the_record_load_module_and_merge_its_definitions(self) -> None:
        text = DEFINITIONS.read_text()
        tree = ast.parse(text)
        imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.level == 1 for alias in node.names}
        self.assertIn("record_load", imported)
        self.assertIn("record_load.defs", text.replace(" ", ""))

    def test_the_job_is_declared_in_its_service_group_and_takes_its_tags_from_that_declaration(self) -> None:
        self.assertEqual("ingestion", JOB_GROUPS["record_load_job"])
        tree = ast.parse(RECORD_LOAD.read_text())
        jobs = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "record_load_job"]
        self.assertEqual(1, len(jobs))
        decorator = jobs[0].decorator_list[0]
        if not isinstance(decorator, ast.Call):
            self.fail("the job's decorator is not a call")
        keywords = {keyword.arg: ast.unparse(keyword.value) for keyword in decorator.keywords}
        self.assertEqual("job_tags('record_load_job')", keywords["tags"])
        self.assertEqual("in_process_executor", keywords["executor_def"])
        self.assertIn("GroupResource(group=GROUP)", keywords["resource_defs"])

    def test_the_group_that_the_job_runs_in_exists_in_the_limits_file(self) -> None:
        limits = mapping(load_yaml((ROOT / "compose" / "limits.yml").read_text()))
        groups = {entry["group"] for entry in limits["services"].values()}
        self.assertIn("ingestion", groups)


class ComposeServiceTest(unittest.TestCase):
    def document(self) -> dict[str, Any]:
        return mapping(load_yaml(SERVICE.read_text()))

    def test_the_code_location_mounts_the_six_files_as_compose_secrets_from_the_airbyte_api_folder(self) -> None:
        document = self.document()
        service = document["services"]["dagster_code_location"]
        self.assertEqual(set(SECRET_NAMES), set(service["secrets"]) & set(SECRET_NAMES))
        for name, file in SECRET_NAMES.items():
            self.assertEqual({"file": f"{SECRETS_FOLDER}{file}"}, document["secrets"][name], name)

    def test_each_file_setting_names_the_mount_of_its_secret_and_no_secret_is_a_value_in_the_environment(self) -> None:
        environment = self.document()["services"]["dagster_code_location"]["environment"]
        for setting, secret in ENVIRONMENT_FILES.items():
            self.assertEqual(f"/run/secrets/{secret}", environment[setting], setting)
        for name, value in environment.items():
            if re.search("CLIENT|AWS|SECRET|KEY|TOKEN|PASSWORD", name) and name.startswith(("AIRBYTE_", "RECORD_")):
                self.assertTrue(name.endswith("_FILE") and str(value).startswith("/run/secrets/"), name)

    def test_the_settings_of_the_load_that_the_environment_names_are_the_ones_the_load_reads(self) -> None:
        environment = self.document()["services"]["dagster_code_location"]["environment"]
        for name in (*SECRET_FILES, "AIRBYTE_API_URL_FILE", "AIRBYTE_NESSIE_URL_FILE"):
            self.assertIn(name, environment)

    def test_the_bucket_the_region_and_nessie_come_from_settings_that_already_exist(self) -> None:
        service = self.document()["services"]["dagster_code_location"]
        self.assertIn("NESSIE_URI", service["environment"])
        self.assertEqual(
            [{"path": "../host.env", "required": False}], service["env_file"], "WAREHOUSE_LOCATION and AWS_REGION come from the host settings file"
        )


if __name__ == "__main__":
    unittest.main()
