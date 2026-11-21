"""Airbyte's k3s workloads and the limits file: every workload has an entry, every entry has a workload and the figures reach the chart's values.

The chart's own defaults are a trimmed stand-in with the keys of chart 2.5.0 that matter here, so a planted change shows which check names it.
The real chart is checked against the same rules on the host by the deployment role, which gives its defaults to ``check-chart``.
"""

from __future__ import annotations

import copy
import unittest
from collections.abc import Callable
from typing import Any

from scripts.stack.k3s_limits import (
    CHART_WORKLOAD_KEYS,
    GROUP,
    LIMIT_RANGE_ENTRY,
    TRACKED_VALUES,
    WORKLOADS,
    chart_problems,
    entry_problems,
    exposure_problems,
    render_limit_range,
    render_values,
    secret_value_problems,
    typed_memory_problems,
)
from scripts.stack.limits import LIMITS_FILE, Limits, ServiceLimit, load_limits
from scripts.stack.yaml_text import load_yaml

ENABLED_BY_THE_CHART = {
    "server",
    "worker",
    "workloadLauncher",
    "airbyteBootloader",
    "temporal",
    "postgresql",
    "minio",
    "cron",
    "manifestServer",
    "workloadApiServer",
}
JOB_KINDS = ("mainContainer", "check", "discover", "replication", "sidecar")


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def tracked_values() -> dict[str, Any]:
    return mapping(load_yaml(TRACKED_VALUES.read_text()))


def chart_defaults() -> dict[str, Any]:
    chart: dict[str, Any] = {key: {"enabled": key in ENABLED_BY_THE_CHART, "resources": {"requests": {}, "limits": {}}} for key in CHART_WORKLOAD_KEYS}
    chart["temporal"]["adminTools"] = {"resources": {"requests": {"memory": "64Mi"}, "limits": {"memory": "256Mi"}}}
    chart["ingress"] = {"enabled": False}
    chart["global"] = {"workloads": {"resources": {kind: {"cpu": {}, "memory": {}} for kind in JOB_KINDS}}}
    return chart


def limits_with(**changes: ServiceLimit | None) -> Limits:
    services = dict(load_limits().services)
    for name, entry in changes.items():
        if entry is None:
            services.pop(name)
        else:
            services[name] = entry
    return Limits(load_limits().host_reserve_mib, services)


class EntriesTest(unittest.TestCase):
    def test_the_tracked_limits_file_and_the_workload_table_name_the_same_workloads(self) -> None:
        self.assertEqual([], entry_problems(load_limits(LIMITS_FILE)))

    def test_every_airbyte_workload_belongs_to_the_ingestion_group_and_runs_in_k3s(self) -> None:
        for name in WORKLOADS:
            with self.subTest(entry=name):
                entry = load_limits().services[name]
                self.assertEqual(("k3s", GROUP), (entry.runtime, entry.group))

    def test_a_workload_without_an_entry_is_named(self) -> None:
        problems = entry_problems(limits_with(airbyte_server=None))
        self.assertEqual(1, len(problems))
        self.assertIn("airbyte_server", problems[0])
        self.assertIn("no entry", problems[0])

    def test_an_entry_without_a_workload_is_named(self) -> None:
        problems = entry_problems(limits_with(airbyte_extra=ServiceLimit("k3s", GROUP, 256)))
        self.assertEqual(1, len(problems))
        self.assertIn("airbyte_extra", problems[0])
        self.assertIn("no workload", problems[0])

    def test_an_airbyte_entry_in_another_group_or_runtime_is_named(self) -> None:
        for change in (ServiceLimit("k3s", "supporting", 512), ServiceLimit("compose", GROUP, 512)):
            with self.subTest(change=change):
                problems = entry_problems(limits_with(airbyte_cron=change))
                self.assertEqual(1, len(problems))
                self.assertIn("airbyte_cron", problems[0])


class RenderTest(unittest.TestCase):
    def test_each_chart_block_gets_its_entry_as_memory_request_and_limit(self) -> None:
        rendered = mapping(load_yaml(render_values(load_limits())))
        limits = load_limits().services
        for entry, setting in WORKLOADS.items():
            if entry == LIMIT_RANGE_ENTRY:
                continue
            node: Any = rendered
            for part in setting.path:
                node = node[part]
            figure = f"{limits[entry].memory_limit_mib}Mi"
            expected = (
                {"requests": {"memory": figure}, "limits": {"memory": figure}}
                if setting.shape == "resources"
                else {"memory": {"request": figure, "limit": figure}}
            )
            with self.subTest(entry=entry):
                self.assertEqual(expected, node)

    def test_the_job_settings_have_the_shape_that_the_chart_reads(self) -> None:
        rendered = mapping(load_yaml(render_values(load_limits())))
        figure = f"{load_limits().services['airbyte_job_check'].memory_limit_mib}Mi"
        self.assertEqual({"memory": {"request": figure, "limit": figure}}, rendered["global"]["workloads"]["resources"]["check"])

    def test_the_hook_pod_entry_is_not_a_chart_value_and_reaches_the_limit_range(self) -> None:
        self.assertNotIn(LIMIT_RANGE_ENTRY, str(load_yaml(render_values(load_limits()))))
        figure = f"{load_limits().services[LIMIT_RANGE_ENTRY].memory_limit_mib}Mi"
        manifest = mapping(load_yaml(render_limit_range(load_limits(), "airbyte")))
        self.assertEqual(("LimitRange", "airbyte"), (manifest["kind"], manifest["metadata"]["namespace"]))
        (item,) = manifest["spec"]["limits"]
        self.assertEqual(("Container", {"memory": figure}, {"memory": figure}), (item["type"], item["default"], item["defaultRequest"]))

    def test_a_changed_figure_in_the_limits_file_changes_the_rendered_values_and_nothing_else_is_typed(self) -> None:
        changed = mapping(load_yaml(render_values(limits_with(airbyte_server=ServiceLimit("k3s", GROUP, 3333)))))
        self.assertEqual("3333Mi", changed["server"]["resources"]["limits"]["memory"])

    def test_the_output_is_the_same_on_every_run(self) -> None:
        self.assertEqual(render_values(load_limits()), render_values(load_limits()))


class TrackedValuesTest(unittest.TestCase):
    def test_the_tracked_values_agree_with_the_chart_and_the_limits(self) -> None:
        self.assertEqual([], chart_problems(chart_defaults(), tracked_values(), load_limits()))

    def test_the_tracked_values_type_no_memory_figure_so_the_limits_file_stays_the_one_source(self) -> None:
        self.assertEqual([], typed_memory_problems(tracked_values()))

    def test_a_memory_figure_typed_under_a_workload_is_named(self) -> None:
        values = tracked_values()
        values.setdefault("server", {})["resources"] = {"limits": {"memory": "1Gi"}}
        problems = typed_memory_problems(values)
        self.assertEqual(1, len(problems))
        self.assertIn("server", problems[0])

    def test_a_workload_that_the_chart_starts_and_the_values_do_not_mention_is_named(self) -> None:
        chart = chart_defaults()
        chart["telemetry"] = {"enabled": True}
        problems = chart_problems(chart, tracked_values(), load_limits())
        self.assertEqual(1, len(problems))
        self.assertIn("telemetry", problems[0])

    def test_a_workload_that_the_values_start_without_an_entry_is_named(self) -> None:
        values = tracked_values()
        values["webapp"]["enabled"] = True
        problems = chart_problems(chart_defaults(), values, load_limits())
        self.assertEqual(1, len(problems))
        self.assertIn("webapp", problems[0])

    def test_a_mapped_path_that_the_chart_does_not_have_is_named(self) -> None:
        chart = chart_defaults()
        del chart["temporal"]["adminTools"]
        problems = chart_problems(chart, tracked_values(), load_limits())
        self.assertEqual(1, len(problems))
        self.assertIn("temporal.adminTools.resources", problems[0])

    def test_a_workload_that_the_values_leave_off_but_an_entry_names_is_named(self) -> None:
        values = tracked_values()
        values["cron"]["enabled"] = False
        problems = chart_problems(chart_defaults(), values, load_limits())
        self.assertEqual(1, len(problems))
        self.assertIn("cron", problems[0])


class ExposureTest(unittest.TestCase):
    def test_the_values_start_no_ingress_no_load_balancer_and_no_node_port(self) -> None:
        self.assertEqual([], exposure_problems(tracked_values()))

    def test_each_way_to_expose_a_service_is_named(self) -> None:
        plants: dict[str, Callable[[dict[str, Any]], object]] = {
            "ingress.enabled": lambda v: v["ingress"].update(enabled=True),
            "webapp.ingress": lambda v: v["webapp"].setdefault("ingress", {}).update(enabled=True),
            "load balancer": lambda v: v["server"].setdefault("service", {}).update(type="LoadBalancer"),
            "node port": lambda v: v["server"].setdefault("service", {}).update(type="NodePort", nodePort=30001),
            "host network": lambda v: v["server"].update(hostNetwork=True),
            "node port on a cluster IP": lambda v: v["server"].setdefault("service", {}).update(type="ClusterIP", nodePort=30001),
        }
        for label, plant in plants.items():
            with self.subTest(label):
                values = copy.deepcopy(tracked_values())
                plant(values)
                problems = exposure_problems(values)
                self.assertEqual(1, len(problems), problems)

    def test_the_web_service_stays_a_cluster_internal_service_in_the_values(self) -> None:
        service = tracked_values()["server"].get("service", {})
        self.assertIn(service.get("type", "ClusterIP"), {"ClusterIP"})


class SecretValuesTest(unittest.TestCase):
    def test_no_secret_value_is_in_the_tracked_values(self) -> None:
        self.assertEqual([], secret_value_problems(tracked_values()))

    def test_a_typed_password_token_or_key_is_named_and_a_secret_name_or_key_name_is_not(self) -> None:
        values = tracked_values()
        values["global"]["database"].update({"password": "x"})
        values["global"]["storage"]["minio"] = {"secretAccessKey": "y"}
        problems = secret_value_problems(values)
        self.assertEqual(2, len(problems))
        self.assertTrue(any("global.database.password" in problem for problem in problems))
        self.assertTrue(any("global.storage.minio.secretAccessKey" in problem for problem in problems))
        self.assertEqual([], secret_value_problems({"global": {"database": {"secretName": "airbyte-database", "passwordSecretKey": "database-password"}}}))


if __name__ == "__main__":
    unittest.main()
