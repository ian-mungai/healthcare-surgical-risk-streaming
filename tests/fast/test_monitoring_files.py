"""The monitoring files hold the decisions that no running check sees until a service is deployed: 30 days of logs, bounded metrics, a collector with no
secrets folder and no extra privilege, and an administrator password that comes from a file.

The first group reads the tracked files. The second group plants one defect at a time in a copy and expects the checker to name it.
"""

from __future__ import annotations

import copy
import json
import re
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
MONITORING = COMPOSE / "monitoring"
SERVICES = COMPOSE / "services"
THIRTY_DAYS = "720h"
SECRETS_FOLDER = "${SECRETS_DIR"
DATASOURCE_UIDS = {"prometheus", "loki"}
GRAFANA_CONFIGURATION = MONITORING / "grafana" / "grafana.ini"


def load(path: Path) -> dict[str, Any]:
    document = load_yaml(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path.name} is not a mapping")
    return document


def service(name: str) -> dict[str, Any]:
    found: dict[str, Any] = load(SERVICES / f"{name}.yml")["services"][name]
    return found


def loki_problems(config: dict[str, Any]) -> list[str]:
    problems = []
    if (config.get("limits_config") or {}).get("retention_period") != THIRTY_DAYS:
        problems.append("retention_period must be 720h")
    compactor = config.get("compactor") or {}
    if compactor.get("retention_enabled") is not True:
        problems.append("the compactor's retention must be on")
    if not compactor.get("delete_request_store"):
        problems.append("the compactor needs a delete request store")
    return problems


def prometheus_problems(definition: dict[str, Any]) -> list[str]:
    flags = [str(item) for item in definition.get("command") or []]
    problems = []
    if not any(re.fullmatch(r"--storage\.tsdb\.retention\.time=\d+[dwy]", flag) for flag in flags):
        problems.append("retention by time must be set")
    if not any(re.fullmatch(r"--storage\.tsdb\.retention\.size=\d+[KMG]?B", flag) for flag in flags):
        problems.append("retention by size must be set")
    return problems


def collector_problems(definition: dict[str, Any], declared_secrets: dict[str, Any]) -> list[str]:
    problems = []
    if definition.get("privileged"):
        problems.append("the collector must not be privileged")
    if definition.get("cap_drop") != ["ALL"] or definition.get("cap_add"):
        problems.append("the collector must drop every capability and add none")
    if definition.get("pid") == "host" or definition.get("network_mode") == "host":
        problems.append("the collector must not join the host's process or network namespace")
    for item in definition.get("volumes") or []:
        if SECRETS_FOLDER in str(item):
            problems.append("the collector must not mount the secrets folder")
    for item in definition.get("secrets") or []:
        source = item if isinstance(item, str) else item.get("source")
        path = str((declared_secrets.get(source) or {}).get("file", ""))
        if SECRETS_FOLDER in path and not path.endswith("/postgres/alloy.password"):
            problems.append(f"secret {source} is a file of the secrets folder other than the collector's own account")
    return problems


GRAFANA_PASSWORD_LINE = re.compile(r"^admin_password\s*=\s*\$__file\{/run/secrets/grafana_admin_password\}\s*$", re.MULTILINE)


def grafana_problems(definition: dict[str, Any], configuration: str) -> list[str]:
    """Grafana's own ``$__file{}`` provider reads the password in the configuration; its entrypoint exports the content of a ``__FILE`` variable."""
    environment = definition.get("environment") or {}
    problems = []
    if not GRAFANA_PASSWORD_LINE.search(configuration):
        problems.append("the configuration must read the administrator password with $__file{} from the secrets file")
    if any(key.endswith("__FILE") for key in environment):
        problems.append("no __FILE variable may name the password: Grafana's entrypoint exports its content into the process environment")
    if "GF_SECURITY_ADMIN_PASSWORD" in environment:
        problems.append("the administrator password must not be a value")
    mounted = [item for item in definition.get("configs") or [] if isinstance(item, dict) and item.get("target") == "/etc/grafana/grafana.ini"]
    if not mounted:
        problems.append("the configuration file is not mounted at /etc/grafana/grafana.ini")
    return problems


def dashboard_problems(dashboard: dict[str, Any]) -> list[str]:
    problems = []
    for panel in dashboard.get("panels", []):
        uids = {panel.get("datasource", {}).get("uid")} | {target.get("datasource", {}).get("uid") for target in panel.get("targets", [])}
        if not uids <= DATASOURCE_UIDS:
            problems.append(f"panel {panel.get('title')} names a data source that is not provisioned: {sorted(map(str, uids - DATASOURCE_UIDS))}")
    return problems


class TrackedFilesTest(unittest.TestCase):
    def test_loki_keeps_logs_for_30_days_with_the_compactors_retention_on(self) -> None:
        self.assertEqual([], loki_problems(load(MONITORING / "loki.yml")))

    def test_prometheus_keeps_a_bounded_amount_of_data(self) -> None:
        self.assertEqual([], prometheus_problems(service("prometheus")))

    def test_the_collector_has_no_secrets_folder_and_no_extra_privilege(self) -> None:
        document = load(SERVICES / "alloy.yml")
        self.assertEqual([], collector_problems(document["services"]["alloy"], document.get("secrets") or {}))

    def test_grafana_takes_its_administrator_password_from_a_file_through_its_own_provider(self) -> None:
        self.assertEqual([], grafana_problems(service("grafana"), GRAFANA_CONFIGURATION.read_text()))

    def test_the_dashboard_names_only_provisioned_data_sources(self) -> None:
        dashboard = json.loads((MONITORING / "grafana" / "dashboards" / "service_memory.json").read_text())
        self.assertEqual([], dashboard_problems(dashboard))
        uids = {item["uid"] for item in load(MONITORING / "grafana" / "datasources.yml")["datasources"]}
        self.assertEqual(DATASOURCE_UIDS, uids)


def without_flag(prefix: str) -> Callable[[dict[str, Any]], object]:
    return lambda d: d.update(command=[x for x in d["command"] if not x.startswith(prefix)])


class PlantedDefectTest(unittest.TestCase):
    def planted(self, document: dict[str, Any], mutate: Callable[[dict[str, Any]], object], check: Callable[[dict[str, Any]], list[str]]) -> list[str]:
        changed = copy.deepcopy(document)
        mutate(changed)
        return check(changed)

    def test_a_loki_retention_that_is_not_30_days_or_is_not_enforced_is_named(self) -> None:
        config = load(MONITORING / "loki.yml")
        for mutate in (
            lambda c: c["limits_config"].update(retention_period="24h"),
            lambda c: c["compactor"].update(retention_enabled=False),
            lambda c: c["compactor"].pop("delete_request_store"),
            lambda c: c.pop("limits_config"),
        ):
            self.assertEqual(1, len(self.planted(config, mutate, loki_problems)))

    def test_a_prometheus_without_a_time_or_size_bound_is_named(self) -> None:
        definition = service("prometheus")
        for prefix in ("--storage.tsdb.retention.time=", "--storage.tsdb.retention.size="):
            problems = self.planted(definition, without_flag(prefix), prometheus_problems)
            self.assertEqual(1, len(problems))

    def test_a_collector_with_extra_privilege_or_the_secrets_folder_is_named(self) -> None:
        document = load(SERVICES / "alloy.yml")
        definition, declared = document["services"]["alloy"], document.get("secrets") or {}

        def check(changed: dict[str, Any]) -> list[str]:
            return collector_problems(changed, declared)

        for mutate in (
            lambda d: d.update(privileged=True),
            lambda d: d.update(cap_drop=[]),
            lambda d: d.update(cap_add=["SYS_ADMIN"]),
            lambda d: d.update(pid="host"),
            lambda d: d["volumes"].append("${SECRETS_DIR}:/secrets:ro"),
        ):
            self.assertEqual(1, len(self.planted(definition, mutate, check)))
        other = copy.deepcopy(declared)
        other["postgres_alloy_password"]["file"] = "${SECRETS_DIR:?x}/postgres/postgres.password"
        self.assertEqual(1, len(collector_problems(definition, other)))

    def test_a_grafana_password_as_a_value_a_file_variable_or_missing_is_named(self) -> None:
        definition, configuration = service("grafana"), GRAFANA_CONFIGURATION.read_text()

        def check(changed: dict[str, Any]) -> list[str]:
            return grafana_problems(changed, configuration)

        self.assertEqual(1, len(self.planted(definition, lambda d: d["environment"].update({"GF_SECURITY_ADMIN_PASSWORD__FILE": "/run/secrets/x"}), check)))
        self.assertEqual(1, len(self.planted(definition, lambda d: d["environment"].update({"GF_SECURITY_ADMIN_PASSWORD": "x"}), check)))
        self.assertEqual(
            1, len(self.planted(definition, lambda d: d.update(configs=[item for item in d["configs"] if item["target"] != "/etc/grafana/grafana.ini"]), check))
        )
        for text in ("", "[security]\nadmin_password = changeme\n", "[security]\nadmin_password = $__env{PASSWORD}\n"):
            self.assertEqual(1, len(grafana_problems(definition, text)), text)

    def test_a_panel_on_an_unprovisioned_data_source_is_named(self) -> None:
        dashboard = json.loads((MONITORING / "grafana" / "dashboards" / "service_memory.json").read_text())
        problems = self.planted(dashboard, lambda d: d["panels"][0]["targets"][0].update(datasource={"uid": "elsewhere"}), dashboard_problems)
        self.assertEqual(1, len(problems))


if __name__ == "__main__":
    unittest.main()
