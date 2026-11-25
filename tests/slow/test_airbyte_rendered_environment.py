"""The Airbyte release as Helm renders it: which pods reach the database, the pool caps in each pod's final environment and how each one rolls out.

The fast test reads the tracked values file. This one renders chart 2.5.0 with the three values files of the release (the tracked file, the limits file and host
settings) and reads the manifests that Kubernetes would get, so a name that the chart does not forward, a pod that the lists miss and a rollout that surges are
all differences. It needs the pinned chart archive and Helm 3, which a computer without them does not have, so each case is skipped there with that reason:

    AIRBYTE_CHART_ARCHIVE=/path/to/airbyte-2.5.0.tgz HELM=/path/to/helm .venv/bin/python -m unittest tests.slow.test_airbyte_rendered_environment

The archive must match the pinned hash of ``ansible/group_vars/all.yml``. On the host the archive is ``/opt/platform/airbyte/airbyte-2.5.0.tgz``.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

import yaml

from scripts.process import run_command
from scripts.project_config import ROOT
from scripts.stack.limits import load_limits
from tests.fast.ansible_paths import group_variables
from tests.fast.test_postgres_connection_budget import DATABASE_CLIENT_SERVICES, EXPECTED_AIRBYTE_ENV_VARS, VALUES_FILE_PATH

ARCHIVE = os.environ.get("AIRBYTE_CHART_ARCHIVE", "")
HELM = os.environ.get("HELM") or shutil.which("helm") or ""
WHY_SKIPPED = "set AIRBYTE_CHART_ARCHIVE to the pinned chart archive and HELM (or put helm on PATH) to render the release"
HOST_VALUES = """\
global:
  database:
    host: "10.42.0.1"
    port: 5432
    name: "airbyte"
server:
  service:
    port: 8001
temporal:
  extraEnv:
    - name: DBNAME
      value: "airbyte_temporal"
    - name: VISIBILITY_DBNAME
      value: "airbyte_temporal_visibility"
    - name: SKIP_DB_CREATE
      value: "true"
"""
# The component of the values file that renders each workload, the workload's name in a release called "airbyte" and the services of the limits file that it is.
WORKLOADS = {
    "temporal": ("airbyte-temporal", {"airbyte_temporal", "airbyte_temporal_admin"}),
    "server": ("airbyte-server", {"airbyte_server"}),
    "workloadApiServer": ("airbyte-workload-api-server", {"airbyte_workload_api_server"}),
    "worker": ("airbyte-worker", {"airbyte_worker"}),
    "cron": ("airbyte-cron", {"airbyte_cron"}),
    "workloadLauncher": ("airbyte-workload-launcher", {"airbyte_workload_launcher"}),
    "airbyteBootloader": ("airbyte-bootloader", {"airbyte_bootloader"}),
}
CAP_NAMES = {
    "CONFIG_DB_MAX_POOL_SIZE",
    "CONFIG_DB_REPLICA_MAX_POOL_SIZE",
    "JAVA_TOOL_OPTIONS",
    "SQL_MAX_CONNS",
    "SQL_MAX_IDLE_CONNS",
    "SQL_VIS_MAX_CONNS",
    "SQL_VIS_MAX_IDLE_CONNS",
}


def render(folder: Path) -> list[dict[str, Any]]:
    limits = run_command(str(ROOT / ".venv" / "bin" / "python"), ["-m", "scripts.stack.k3s_limits", "values"], cwd=ROOT, timeout=60, check=True)
    (folder / "limits.values.yaml").write_text(limits.stdout)
    (folder / "host.values.yaml").write_text(HOST_VALUES)
    result = run_command(
        HELM,
        [
            "template",
            "airbyte",
            ARCHIVE,
            "--namespace",
            "airbyte",
            "--values",
            str(ROOT / VALUES_FILE_PATH),
            "--values",
            str(folder / "limits.values.yaml"),
            "--values",
            str(folder / "host.values.yaml"),
        ],
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"helm template failed with status {result.returncode}")
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def pod_spec(document: dict[str, Any]) -> dict[str, Any] | None:
    spec: dict[str, Any] = document["spec"]
    if document["kind"] == "Pod":
        return spec
    if document["kind"] == "Deployment":
        template: dict[str, Any] = spec["template"]["spec"]
        return template
    return None


def effective_environment(container: dict[str, Any], config_maps: dict[str, dict[str, str]]) -> dict[str, str]:
    """The environment of a container as the manifest shows it: each ConfigMap of ``envFrom`` first, then each entry of ``env`` over it, as Kubernetes does.

    An entry that reads a key of a ConfigMap takes that key's value. An entry that reads a Secret has an empty value here, so no secret value is read.
    """
    found: dict[str, str] = {}
    for source in container.get("envFrom", []):
        found.update(config_maps.get(source.get("configMapRef", {}).get("name", ""), {}))
    for item in container.get("env", []):
        reference = item.get("valueFrom", {}).get("configMapKeyRef")
        if "value" in item:
            found[item["name"]] = item["value"]
        elif reference is not None:
            found[item["name"]] = config_maps.get(reference["name"], {}).get(reference["key"], "")
        else:
            found[item["name"]] = ""
    return found


@unittest.skipUnless(ARCHIVE and HELM, WHY_SKIPPED)
class RenderedReleaseTest(unittest.TestCase):
    documents: list[dict[str, Any]]
    config_maps: dict[str, dict[str, str]]

    @classmethod
    def setUpClass(cls) -> None:
        pinned = str(group_variables()["airbyte_chart_sha256"])
        if hashlib.sha256(Path(ARCHIVE).read_bytes()).hexdigest() != pinned:
            raise AssertionError("the chart archive does not match airbyte_chart_sha256 of ansible/group_vars/all.yml")
        with tempfile.TemporaryDirectory() as scratch:
            cls.documents = render(Path(scratch))
        cls.config_maps = {item["metadata"]["name"]: item.get("data", {}) for item in cls.documents if item["kind"] == "ConfigMap"}

    def workloads(self) -> dict[str, dict[str, Any]]:
        return {item["metadata"]["name"]: item for item in self.documents if item["kind"] in ("Deployment", "Pod")}

    def containers(self, name: str) -> list[dict[str, Any]]:
        spec = pod_spec(self.workloads()[name])
        return [] if spec is None else [*spec.get("initContainers", []), *spec["containers"]]

    def test_the_pods_that_reach_the_database_are_the_airbyte_services_of_the_fixed_client_list(self) -> None:
        reaching = set()
        for name in self.workloads():
            if any("DATABASE_URL" in effective_environment(container, self.config_maps) for container in self.containers(name)) or name == "airbyte-temporal":
                reaching.add(name)
        expected = {workload for workload, _ in WORKLOADS.values()}
        self.assertEqual(expected, reaching)
        services = {service for _, group in WORKLOADS.values() for service in group}
        self.assertEqual({item for item in DATABASE_CLIENT_SERVICES if item.startswith("airbyte_")}, services)
        self.assertLessEqual(services, set(load_limits().services))

    def test_each_database_pod_has_exactly_the_cap_names_and_values_of_the_fixed_environment_in_its_rendered_containers(self) -> None:
        for component, (name, _) in WORKLOADS.items():
            expected = EXPECTED_AIRBYTE_ENV_VARS[component]
            for container in self.containers(name):
                found = {key: value for key, value in effective_environment(container, self.config_maps).items() if key in CAP_NAMES}
                with self.subTest(pod=name, container=container["name"]):
                    if container["name"] == "temporal-namespace-setup":
                        self.assertEqual({}, found)
                        continue
                    # The chart renders server.configDbMaxPoolSize only into the server's container, as a read of the shared ConfigMap's key.
                    rendered = {"CONFIG_DB_MAX_POOL_SIZE": "4"} if component == "server" else {}
                    self.assertEqual(rendered | expected, found)

    def test_no_other_workload_carries_a_cap_name(self) -> None:
        database = {name for name, _ in WORKLOADS.values()}
        for name in self.workloads():
            if name in database:
                continue
            for container in self.containers(name):
                found = {key for key in effective_environment(container, self.config_maps) if key in CAP_NAMES}
                with self.subTest(pod=name, container=container["name"]):
                    self.assertEqual(set(), found)

    def test_the_deployments_that_the_chart_leaves_to_surge_are_exactly_the_ones_the_role_makes_recreate(self) -> None:
        rolling = set()
        for name, _ in WORKLOADS.values():
            document = self.workloads()[name]
            if document["kind"] == "Deployment" and document["spec"].get("strategy", {}).get("type") != "Recreate":
                rolling.add(name.removeprefix("airbyte-"))
        self.assertEqual(rolling, set(group_variables()["airbyte_recreate_deployments"]))
