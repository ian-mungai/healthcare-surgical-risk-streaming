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


class SecretsOfServicesTest(unittest.TestCase):
    """A secret that a service mounts from a file is a file to watch as well: a replaced file is a new file, and a running container keeps the old one."""

    DOCUMENT = {
        "services": {
            "dagster_code_location": {
                "secrets": [{"source": "airbyte_client_id", "target": "airbyte_client_id"}, "airbyte_client_secret"],
                "configs": ["limits"],
            },
            "postgres": {"secrets": ["postgres_password"]},
            "alloy": {},
        },
        "configs": {"limits": {"file": "/p/limits.yml"}},
        "secrets": {
            "airbyte_client_id": {"file": "/s/airbyte_api/client_id"},
            "airbyte_client_secret": {"file": "/s/airbyte_api/client_secret"},
            "postgres_password": {"file": "/s/postgres/password"},
            "from_environment": {"environment": "SOME_VALUE"},
        },
    }

    def test_the_files_of_the_secrets_of_each_service_are_watched_in_both_forms_beside_its_config_files(self) -> None:
        self.assertEqual(
            {
                "dagster_code_location": [Path("/p/limits.yml"), Path("/s/airbyte_api/client_id"), Path("/s/airbyte_api/client_secret")],
                "postgres": [Path("/s/postgres/password")],
                "alloy": [],
            },
            stale.service_files(self.DOCUMENT),
        )

    def test_a_secret_that_has_no_file_is_not_a_file_to_watch_and_an_undefined_secret_is_refused(self) -> None:
        document: dict[str, Any] = {"services": {"a": {"secrets": ["from_environment"]}}, "secrets": {"from_environment": {"environment": "X"}}}
        self.assertEqual({"a": []}, stale.service_files(document))
        bad_documents: list[Any] = [
            {"services": {"a": {"secrets": ["missing"]}}, "secrets": {}},
            {"services": {"a": {"secrets": ["missing"]}}},
            {"services": {"a": {"secrets": [5]}}, "secrets": {}},
        ]
        for bad in bad_documents:
            with self.subTest(bad=bad), self.assertRaises(stale.StaleError):
                stale.service_files(bad)

    def test_a_service_whose_secret_file_is_newer_than_its_container_is_stale_and_one_whose_secret_is_older_is_not(self) -> None:
        files = stale.service_files(self.DOCUMENT)
        modified = {Path("/s/airbyte_api/client_secret"): 200.0}
        names = stale.stale_services(files, {"dagster_code_location": 100.0, "postgres": 100.0}, lambda path: modified.get(path, 50.0))
        self.assertEqual(["dagster_code_location"], names)


class TargetsTest(unittest.TestCase):
    """Only the files of the services that a start names are looked at, so a file of another service that is not there yet cannot stop the start."""

    FILES = {"postgres": [Path("/s/postgres/password")], "dagster_code_location": [Path("/s/airbyte_api/client_id")], "alloy": []}
    STARTED = {"postgres": 100.0, "dagster_code_location": 100.0}

    def modified(self, looked: list[Path]) -> Any:
        def read(path: Path) -> float:
            looked.append(path)
            if path.parts[2] == "airbyte_api":
                raise FileNotFoundError(2, "No such file or directory", str(path))
            return 50.0

        return read

    def test_the_files_of_a_service_that_the_start_does_not_name_are_never_looked_at(self) -> None:
        looked: list[Path] = []
        names = stale.stale_services(self.FILES, self.STARTED, self.modified(looked), ["postgres"])
        self.assertEqual([], names)
        self.assertEqual([Path("/s/postgres/password")], looked)

    def test_a_start_that_names_no_service_looks_at_every_service_that_has_a_container(self) -> None:
        looked: list[Path] = []
        with self.assertRaises(stale.StaleError):
            stale.stale_services(self.FILES, self.STARTED, self.modified(looked), [])
        self.assertIn(Path("/s/airbyte_api/client_id"), looked)

    def test_a_named_service_with_a_container_whose_file_is_not_there_is_a_refusal_that_names_the_service_and_the_file(self) -> None:
        with self.assertRaises(stale.StaleError) as raised:
            stale.stale_services(self.FILES, self.STARTED, self.modified([]), ["dagster_code_location"])
        self.assertEqual("service dagster_code_location mounts a file that is not there: /s/airbyte_api/client_id", str(raised.exception))

    def test_a_named_service_with_no_container_is_not_looked_at_because_its_first_start_reads_the_file(self) -> None:
        looked: list[Path] = []
        self.assertEqual([], stale.stale_services(self.FILES, {"postgres": 100.0}, self.modified(looked), ["dagster_code_location", "postgres"]))
        self.assertEqual([Path("/s/postgres/password")], looked)

    def test_a_missing_file_later_in_the_list_is_a_refusal_even_when_an_earlier_file_is_newer_than_the_container(self) -> None:
        files = {"dagster_code_location": [Path("/s/configs/dagster_limits"), Path("/s/airbyte_api/aws_secret_access_key")]}

        def modified(path: Path) -> float:
            if path.parts[2] == "airbyte_api":
                raise FileNotFoundError(2, "No such file or directory", str(path))
            return 200.0

        with self.assertRaises(stale.StaleError) as raised:
            stale.stale_services(files, {"dagster_code_location": 100.0}, modified, ["dagster_code_location"])
        self.assertEqual("service dagster_code_location mounts a file that is not there: /s/airbyte_api/aws_secret_access_key", str(raised.exception))


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


class DockerFailureTest(unittest.TestCase):
    """A failed docker call shows the step, its exit code and one fixed reason word. Docker's text can hold a socket path, a registry address or a token."""

    LEAKED = "/srv/example.invalid/docker/leaked.sock token=eyJleaked-docker-token-3"
    TEXTS = {
        "refused": f"permission denied while trying to connect to the Docker daemon socket at {LEAKED}",
        "unreachable": f"Cannot connect to the Docker daemon at {LEAKED}. Is the docker daemon running?",
        "not found": f"Error response from daemon: No such container: {LEAKED}",
        "failed": f"error: {LEAKED}: yaml: line 9: did not find expected key",
    }

    class FakeModule:
        def __init__(self, code: int, error: str) -> None:
            self.reply = (code, "the answer", error)

        def run_command(self, arguments: list[str], check_rc: bool = False) -> tuple[int, str, str]:
            return self.reply

    def failure(self, arguments: list[str], error: str, code: int = 1) -> str:
        with self.assertRaises(stale.StaleError) as caught:
            stale.run(self.FakeModule(code, error), arguments)
        return str(caught.exception)

    def test_each_failed_call_shows_the_step_the_code_and_one_fixed_reason_and_none_of_dockers_text(self) -> None:
        for reason, text in self.TEXTS.items():
            for step in ("config", "ps", "inspect"):
                with self.subTest(reason=reason, step=step):
                    message = self.failure(["docker", "compose", step, "--format", "json"], text, code=125)
                    self.assertEqual(f"docker {step} exited with 125 (reason: {reason})", message)
                    self.assertNotIn(self.LEAKED.split()[0], message)
                    self.assertNotIn("eyJleaked", message)

    def test_a_text_that_matches_no_word_gets_the_word_failed_and_a_call_with_no_known_step_is_named_command(self) -> None:
        self.assertEqual("docker command exited with 1 (reason: failed)", self.failure(["docker", "version"], self.TEXTS["failed"]))
        self.assertEqual("docker ps exited with 2 (reason: failed)", self.failure(["docker", "ps"], "", code=2))

    def test_a_call_that_succeeds_returns_its_output_whatever_it_printed_on_standard_error(self) -> None:
        self.assertEqual("the answer", stale.run(self.FakeModule(0, self.TEXTS["unreachable"]), ["docker", "ps"]))

    def test_a_text_with_the_signs_of_two_reasons_gets_the_first_of_refused_then_unreachable_then_not_found(self) -> None:
        refused, unreachable, missing = "permission denied", "Cannot connect to the Docker daemon", "No such container"
        self.assertIn("(reason: refused)", self.failure(["docker", "ps"], f"{missing} {unreachable} {refused}"))
        self.assertIn("(reason: unreachable)", self.failure(["docker", "ps"], f"{missing} {unreachable}"))
        self.assertIn("(reason: not found)", self.failure(["docker", "ps"], missing))

    SIGNS = {
        "refused": ("permission denied", "unauthorized", "access denied"),
        "unreachable": ("Cannot connect to the Docker daemon", "error during connect", "connection refused", "no such host", "i/o timeout"),
        "not found": ("No such container", "No such object", "no such service", "is not running", "executable file not found"),
    }

    def test_every_sign_of_each_reason_is_named_by_its_word_and_the_list_holds_no_other_sign(self) -> None:
        for reason, signs in self.SIGNS.items():
            for sign in signs:
                with self.subTest(reason=reason, sign=sign):
                    self.assertIn(f"(reason: {reason})", self.failure(["docker", "ps"], f"docker: {sign}"))
        self.assertEqual(self.SIGNS, {word: tuple(signs) for word, signs in stale.REASONS})


if __name__ == "__main__":
    unittest.main()
