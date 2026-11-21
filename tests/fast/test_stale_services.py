"""Which services run with an older copy of a configuration file than the one placed on the host.

Compose compares a service's definition when it starts the stack, not the content of the files that its ``configs`` name, so a changed file leaves the
running container on the old copy. The module reads the files each service mounts from ``docker compose config`` and the start time of each container,
and names the services whose files are newer than their container. The cases here hold the rules on data; the real command order is in the role's tests.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "ansible" / "roles" / "compose_stack" / "library" / "stale_services.py"


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("stale_services", MODULE)
    if spec is None or spec.loader is None:
        raise ImportError(str(MODULE))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


stale: Any = load_module()

CONFIG = {
    "services": {
        "alloy": {"configs": [{"source": "alloy_config", "target": "/etc/alloy/config.alloy"}]},
        "grafana": {"configs": [{"source": "provider"}, "datasource"]},
        "loki": {"configs": ["alloy_config"]},
        "nessie": {},
    },
    "configs": {
        "alloy_config": {"file": "/p/alloy.alloy"},
        "provider": {"file": "/p/dashboards.yml"},
        "datasource": {"file": "/p/datasources.yml"},
        "inline": {"content": "x"},
    },
}


class FilesOfServicesTest(unittest.TestCase):
    def test_each_service_gets_the_files_of_its_configs_in_both_forms_and_a_shared_file_belongs_to_each_user(self) -> None:
        files = stale.service_files(CONFIG)
        self.assertEqual(
            {
                "alloy": [Path("/p/alloy.alloy")],
                "grafana": [Path("/p/dashboards.yml"), Path("/p/datasources.yml")],
                "loki": [Path("/p/alloy.alloy")],
                "nessie": [],
            },
            files,
        )

    def test_a_config_with_no_file_is_not_a_file_to_watch_and_a_document_of_another_shape_is_refused(self) -> None:
        config: dict[str, Any] = {"services": {"a": {"configs": ["inline"]}}, "configs": {"inline": {"content": "x"}}}
        self.assertEqual({"a": []}, stale.service_files(config))
        bad_documents: list[Any] = [
            [],
            {"services": []},
            {"services": {"a": {"configs": [5]}}, "configs": {}},
            {"services": {"a": {"configs": ["missing"]}}, "configs": {}},
        ]
        for bad in bad_documents:
            with self.subTest(bad=bad), self.assertRaises(stale.StaleError):
                stale.service_files(bad)


class StartTimeTest(unittest.TestCase):
    def test_a_start_time_with_nanoseconds_and_a_zone_is_read_to_the_second_fraction_that_python_keeps(self) -> None:
        self.assertEqual(1767225600.123456, stale.parse_started("2026-01-01T00:00:00.123456789Z"))
        self.assertEqual(1767225600.0, stale.parse_started("2026-01-01T00:00:00Z"))

    def test_text_that_is_not_a_start_time_is_refused(self) -> None:
        for text in ("", "never", "0001-01-01T00:00:00Z extra", "2026-13-01T00:00:00Z"):
            with self.subTest(text=text), self.assertRaises(stale.StaleError):
                stale.parse_started(text)


class StaleTest(unittest.TestCase):
    files = {"alloy": [Path("/p/a")], "grafana": [Path("/p/b"), Path("/p/c")], "nessie": []}

    def names(self, started: dict[str, float], times: dict[str, float]) -> list[str]:
        names: list[str] = stale.stale_services(self.files, started, lambda path: times[str(path)])
        return names

    def test_a_service_with_a_file_newer_than_its_container_is_named_and_the_others_are_not(self) -> None:
        started = {"alloy": 100.0, "grafana": 100.0, "nessie": 100.0}
        self.assertEqual(["grafana"], self.names(started, {"/p/a": 90.0, "/p/b": 90.0, "/p/c": 101.0}))

    def test_a_file_as_old_as_the_start_is_not_newer_and_a_second_older_container_is_stale(self) -> None:
        self.assertEqual([], self.names({"alloy": 100.0}, {"/p/a": 100.0}))
        self.assertEqual(["alloy"], self.names({"alloy": 100.0}, {"/p/a": 100.5}))

    def test_a_service_with_no_container_is_not_stale_because_its_first_start_reads_the_file(self) -> None:
        self.assertEqual([], self.names({}, {"/p/a": 999.0, "/p/b": 999.0, "/p/c": 999.0}))

    def test_a_service_without_a_config_file_is_never_stale(self) -> None:
        self.assertEqual([], self.names({"nessie": 1.0}, {}))

    def test_the_names_come_in_the_order_of_the_service_list(self) -> None:
        started = {"alloy": 1.0, "grafana": 1.0}
        self.assertEqual(["alloy", "grafana"], self.names(started, {"/p/a": 2.0, "/p/b": 2.0, "/p/c": 2.0}))


class ContainersTest(unittest.TestCase):
    def test_the_container_list_is_read_in_both_forms_that_compose_prints(self) -> None:
        lines = '{"Service":"alloy","ID":"a1"}\n{"Service":"grafana","ID":"g1"}\n'
        array = json.dumps([{"Service": "alloy", "ID": "a1"}, {"Service": "grafana", "ID": "g1"}])
        for text in (lines, array, ""):
            with self.subTest(text=text[:12]):
                expected = {} if not text else {"alloy": "a1", "grafana": "g1"}
                self.assertEqual(expected, stale.parse_containers(text))

    def test_a_list_of_another_shape_is_refused(self) -> None:
        for text in ("not json", '{"Service":"a"}', '[{"ID":"x"}]'):
            with self.subTest(text=text), self.assertRaises(stale.StaleError):
                stale.parse_containers(text)


if __name__ == "__main__":
    unittest.main()
