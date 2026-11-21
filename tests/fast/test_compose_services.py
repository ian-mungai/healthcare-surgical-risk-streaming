"""Every Compose service file holds the properties that the design requires, and the limits file and the Compose files name the same services.

The first group reads the real files, so a later service is held to the same rules. The second group plants one defect at a time in a copy of
the PostgreSQL service and expects the checker to name that defect, so a check that stopped looking would fail here.
"""

from __future__ import annotations

import copy
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.limits import load_limits, variable_name
from scripts.stack.yaml_text import load_yaml

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
ROOT_FILE = COMPOSE / "compose.yml"
SERVICES = COMPOSE / "services"
IMAGE = re.compile(r"^[a-z0-9./_-]+:(?!latest@)[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$")
RESTART_POLICIES = {"always", "unless-stopped"}
HEALTHCHECK_FIELDS = {"test", "interval", "timeout", "retries", "start_period"}
HOST_NETWORK_SERVICES = {"caddy"}
HOST_PATH_VOLUMES = {"caddy": {"/var/run/tailscale:/var/run/tailscale:ro"}}


def load(path: Path) -> dict[str, Any]:
    document = load_yaml(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path.name} is not a mapping")
    return document


def service_problems(name: str, document: dict[str, Any]) -> list[str]:
    """Each way that one service file departs from the design, as a sentence that names the property."""
    problems: list[str] = []
    services = document.get("services")
    if not isinstance(services, dict) or list(services) != [name]:
        return [f"{name}: the file must define exactly one service, named like the file"]
    service = services[name]
    if not IMAGE.match(str(service.get("image", ""))):
        problems.append(f"{name}: image must be pinned by a tag other than latest and by a sha256 digest")
    if service.get("platform") != "linux/amd64":
        problems.append(f"{name}: platform must be linux/amd64")
    if not re.fullmatch(rf"\$\{{{variable_name(name)}:\?[^}}]+\}}", str(service.get("mem_limit", ""))):
        problems.append(f"{name}: mem_limit must read {variable_name(name)} from the rendered limits and fail when it is unset")
    health = service.get("healthcheck")
    if not isinstance(health, dict) or not set(health) >= HEALTHCHECK_FIELDS:
        problems.append(f"{name}: healthcheck must set {', '.join(sorted(HEALTHCHECK_FIELDS))}")
    if service.get("restart") not in RESTART_POLICIES:
        problems.append(f"{name}: restart must be one of {', '.join(sorted(RESTART_POLICIES))}")
    volumes = service.get("volumes") or []
    declared = document.get("volumes") or {}
    named = [str(volume).split(":")[0] for volume in volumes if str(volume) not in HOST_PATH_VOLUMES.get(name, set())]
    if not volumes or any(not re.fullmatch(r"[a-z][a-z0-9_]*", source) or source not in declared for source in named):
        problems.append(f"{name}: data must live in declared named volumes, with no host path")
    if service.get("network_mode") == "host" and (name not in HOST_NETWORK_SERVICES or service.get("ports")):
        problems.append(f"{name}: network_mode host would listen on every address")
    for port in service.get("ports") or []:
        if not isinstance(port, str) or not re.match(r"127\.0\.0\.1:", port):
            problems.append(f"{name}: every published port must bind 127.0.0.1")
    for key, value in (service.get("environment") or {}).items():
        if re.search(r"PASSWORD|SECRET|TOKEN|KEY", str(key)) and not str(key).endswith("_FILE") and str(value):
            problems.append(f"{name}: {key} must come from a secrets file, not a value in the file")
    for secret in service.get("secrets") or []:
        source = secret if isinstance(secret, str) else secret.get("source")
        if source not in (document.get("secrets") or {}):
            problems.append(f"{name}: secret {source} is not declared in the file")
    return problems


class ServiceFilesTest(unittest.TestCase):
    def test_the_root_file_names_the_project_and_includes_every_service_file_and_nothing_else(self) -> None:
        root = load(ROOT_FILE)
        self.assertRegex(str(root.get("name")), r"^[a-z][a-z0-9_]*$")
        self.assertEqual(sorted(f"services/{path.name}" for path in SERVICES.glob("*.yml")), sorted(root["include"]))
        self.assertEqual({"name", "include"}, set(root))

    def test_every_service_file_holds_the_designed_properties(self) -> None:
        files = sorted(SERVICES.glob("*.yml"))
        self.assertTrue(files)
        for path in files:
            with self.subTest(service=path.stem):
                self.assertEqual([], service_problems(path.stem, load(path)))

    def test_the_compose_services_and_the_limits_file_name_the_same_services(self) -> None:
        in_compose = {path.stem for path in SERVICES.glob("*.yml")}
        in_limits = {name for name, entry in load_limits().services.items() if entry.runtime == "compose"}
        self.assertEqual([], sorted(in_compose - in_limits), "a Compose service has no entry in the limits file")
        self.assertEqual([], sorted(in_limits - in_compose), "a Compose entry in the limits file has no service")

    def test_every_published_port_variable_has_a_default_so_a_service_starts_without_one(self) -> None:
        for path in SERVICES.glob("*.yml"):
            for port in load(path)["services"][path.stem].get("ports") or []:
                with self.subTest(service=path.stem, port=port):
                    self.assertRegex(port, r"^127\.0\.0\.1:(\d+|\$\{[A-Z_]+:-\d+\}):\d+$")


class HostNetworkExceptionTest(unittest.TestCase):
    def caddy(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "caddy.yml"))

    def test_the_real_caddy_file_has_no_problem(self) -> None:
        self.assertEqual([], service_problems("caddy", self.caddy()))

    def test_caddy_with_a_published_port_is_named(self) -> None:
        document = self.caddy()
        document["services"]["caddy"]["ports"] = ["127.0.0.1:9440:9440"]
        self.assertIn("network_mode", service_problems("caddy", document)[0])

    def test_caddy_with_another_host_path_is_named(self) -> None:
        document = self.caddy()
        document["services"]["caddy"]["volumes"].append("/etc:/etc:ro")
        self.assertIn("named volumes", service_problems("caddy", document)[0])

    def test_another_service_on_the_host_network_or_with_the_socket_folder_is_named(self) -> None:
        document = copy.deepcopy(load(SERVICES / "postgres.yml"))
        document["services"]["postgres"]["network_mode"] = "host"
        self.assertIn("network_mode", service_problems("postgres", document)[0])
        document = copy.deepcopy(load(SERVICES / "postgres.yml"))
        document["services"]["postgres"]["volumes"].append("/var/run/tailscale:/var/run/tailscale:ro")
        self.assertIn("named volumes", service_problems("postgres", document)[0])


class PlantedDefectTest(unittest.TestCase):
    def good(self) -> dict[str, Any]:
        return copy.deepcopy(load(SERVICES / "postgres.yml"))

    def defect(self, mutate: Any) -> list[str]:
        document = self.good()
        mutate(document, document["services"]["postgres"])
        return service_problems("postgres", document)

    def test_the_real_postgres_file_has_no_problem(self) -> None:
        self.assertEqual([], service_problems("postgres", self.good()))

    def test_a_missing_mem_limit_is_named(self) -> None:
        problems = self.defect(lambda _document, service: service.pop("mem_limit"))
        self.assertEqual(1, len(problems))
        self.assertIn("mem_limit", problems[0])

    def test_a_typed_mem_limit_is_refused_so_a_limit_is_never_typed_twice(self) -> None:
        for value in ("1g", "1024m", "${MEM_LIMIT_POSTGRES:-1024m}", "${MEM_LIMIT_OTHER:?x}", "${MEM_LIMIT_POSTGRES}"):
            with self.subTest(value=value):
                problems = self.defect(lambda _document, service, value=value: service.update(mem_limit=value))
                self.assertEqual(1, len(problems))
                self.assertIn("mem_limit", problems[0])

    def test_an_image_without_a_digest_or_with_latest_is_named(self) -> None:
        for image in ("postgres:18.7", "postgres@sha256:" + "a" * 64, "postgres:latest@sha256:" + "a" * 64, "postgres:18.7@sha256:abc"):
            with self.subTest(image=image):
                problems = self.defect(lambda _document, service, image=image: service.update(image=image))
                self.assertEqual(1, len(problems))
                self.assertIn("image", problems[0])

    def test_a_missing_or_other_platform_is_named(self) -> None:
        for mutate in (lambda _d, s: s.pop("platform"), lambda _d, s: s.update(platform="linux/arm64")):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("platform", problems[0])

    def test_a_healthcheck_missing_any_field_is_named(self) -> None:
        for field in HEALTHCHECK_FIELDS:
            with self.subTest(field=field):
                problems = self.defect(lambda _document, service, field=field: service["healthcheck"].pop(field))
                self.assertEqual(1, len(problems))
                self.assertIn("healthcheck", problems[0])

    def test_a_restart_policy_that_does_not_restart_is_named(self) -> None:
        for mutate in (lambda _d, s: s.pop("restart"), lambda _d, s: s.update(restart="no")):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("restart", problems[0])

    def test_a_port_on_every_address_or_the_host_network_is_named(self) -> None:
        for port in ("5432:5432", "0.0.0.0:5432:5432", "${POSTGRES_HOST_PORT:-5432}:5432"):
            with self.subTest(port=port):
                problems = self.defect(lambda _document, service, port=port: service.update(ports=[port]))
                self.assertEqual(1, len(problems))
                self.assertIn("127.0.0.1", problems[0])
        problems = self.defect(lambda _document, service: service.update(network_mode="host"))
        self.assertIn("network_mode", problems[0])

    def test_data_outside_a_declared_named_volume_is_named(self) -> None:
        for volumes in ([], ["/srv/data:/var/lib/postgresql"], ["undeclared:/var/lib/postgresql"]):
            with self.subTest(volumes=volumes):
                problems = self.defect(lambda _document, service, volumes=volumes: service.update(volumes=volumes))
                self.assertEqual(1, len(problems))
                self.assertIn("named volumes", problems[0])

    def test_a_password_value_in_the_environment_is_named_and_a_file_reference_is_not(self) -> None:
        problems = self.defect(lambda _document, service: service["environment"].update({"POSTGRES_PASSWORD": "x"}))
        self.assertEqual(1, len(problems))
        self.assertIn("POSTGRES_PASSWORD", problems[0])
        self.assertEqual([], self.defect(lambda _document, service: service["environment"].update({"OTHER_PASSWORD_FILE": "/run/secrets/x"})))

    def test_a_secret_that_the_file_does_not_declare_is_named(self) -> None:
        problems = self.defect(lambda document, _service: document.pop("secrets"))
        self.assertEqual(1, len(problems))
        self.assertIn("not declared", problems[0])

    def test_a_service_named_differently_from_its_file_is_named(self) -> None:
        document = self.good()
        document["services"]["other"] = document["services"].pop("postgres")
        self.assertIn("exactly one service", service_problems("postgres", document)[0])


if __name__ == "__main__":
    unittest.main()
