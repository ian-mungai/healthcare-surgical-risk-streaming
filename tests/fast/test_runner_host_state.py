"""The host mode of the monitoring runner and of the Caddy runner reads the containers of the deployed Compose project, not of a project of their own.

The runners stay untracked, so each test skips when its runner is not on this machine. Docker is a stand-in that answers as the live host did on
2026-11-24: the project is ``surgical_risk``, and a command that names another project finds no container. No SSH, Docker or host runs here.

Failure modes covered here:

- The monitoring runner asks Compose for the project ``p2_join3_monitoring`` on the host, finds no container and reports five healthy services as absent.
- The Caddy runner takes the interfaces that run from a flag the lead may not pass, so it expects the gateway error 502 from interfaces that answer 200.
  The running interfaces are read from the deployed project, and an interface outside Compose (Airbyte) is named with ``--running``.
- A route whose service is stopped is counted as running, or a running service is missed because its route name is not its Compose service name.
- The host mode of the Caddy runner starts without the public address, spends a host run and ends in a failed case.
"""

from __future__ import annotations

import contextlib
import importlib
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

from scripts.stack.caddy import Route, load_routes
from tests.e2e.run_record import Record
from tests.fast.docker_host_stand_in import HEALTHY, PROJECT, DockerHost

E2E = Path(__file__).resolve().parents[1] / "e2e"
SERVICES = ("postgres", "prometheus", "grafana", "loki", "alloy")


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


@contextlib.contextmanager
def host_at(module: ModuleType, host: DockerHost) -> Iterator[None]:
    with (
        mock.patch.object(module, "load_configuration", return_value={"ENVIRONMENT": "dev"}),
        mock.patch.object(module, "load_run", return_value=SimpleNamespace(run_id="r")),
        mock.patch.object(module, "host_peer", return_value=SimpleNamespace(address="192.0.2.7")),
        mock.patch.object(module, "ssh_arguments", return_value=["-o", "x"]),
        mock.patch.object(module, "run_command", host.run_command),
    ):
        yield


class MonitoringHostState(unittest.TestCase):
    def test_the_remote_target_names_the_deployed_project(self) -> None:
        module = runner("stack_monitoring")
        with host_at(module, DockerHost()):
            self.assertEqual(module.Remote().project, PROJECT)

    def test_the_five_services_of_the_deployed_stack_read_as_running_and_healthy(self) -> None:
        module = runner("stack_monitoring")
        host = DockerHost()
        with host_at(module, host):
            stack = module.Stack(module.Remote(), Record(feature="x", slug="x", rerun="x"), [])
            states = {service: stack.health(service) for service in SERVICES}
        self.assertEqual(states, {service: "running/healthy" for service in SERVICES})

    def test_a_stopped_service_reads_as_absent_and_an_unhealthy_one_as_it_is(self) -> None:
        module = runner("stack_monitoring")
        host = DockerHost({**HEALTHY, "loki": "unhealthy"})
        del host.services["alloy"]
        with host_at(module, host):
            stack = module.Stack(module.Remote(), Record(feature="x", slug="x", rerun="x"), [])
            self.assertEqual((stack.health("alloy"), stack.health("loki")), ("absent", "running/unhealthy"))

    def test_the_local_mode_keeps_its_own_project(self) -> None:
        module = runner("stack_monitoring")
        self.assertEqual(module.Local.project, module.PROJECT)
        self.assertNotEqual(module.PROJECT, PROJECT)

    def test_the_host_names_the_project_of_the_deployed_file(self) -> None:
        module = runner("stack_monitoring")
        self.assertEqual(module.compose_project_name(), PROJECT)


class CaddyHostState(unittest.TestCase):
    def stack(self, module: ModuleType, host: DockerHost) -> object:
        target = module.Remote()
        return module.Stack(target, Record(feature="x", slug="x", rerun="x"), [], ["docker", "compose", "--project-name", module.compose_project_name()])

    def test_the_running_interfaces_are_read_from_the_deployed_project(self) -> None:
        module = runner("stack_caddy")
        host = DockerHost({**HEALTHY, "openbao": "healthy", "nessie": "healthy", "trino": "healthy", "apicurio": "healthy", "dagster_webserver": "healthy"})
        with host_at(module, host):
            running = module.running_interfaces(self.stack(module, host), load_routes(), set())
        self.assertEqual(running, {"openbao", "grafana", "prometheus", "trino", "nessie", "apicurio", "dagster"})

    def test_an_interface_that_is_not_in_the_project_is_running_only_when_named(self) -> None:
        module = runner("stack_caddy")
        host = DockerHost()
        with host_at(module, host):
            stack = self.stack(module, host)
            without = module.running_interfaces(stack, load_routes(), set())
            named = module.running_interfaces(stack, load_routes(), {"airbyte"})
        self.assertNotIn("airbyte", without)
        self.assertIn("airbyte", named)

    def test_a_service_that_is_not_in_the_project_is_not_a_running_interface(self) -> None:
        module = runner("stack_caddy")
        host = DockerHost({"caddy": "healthy", "grafana": "healthy"})
        with host_at(module, host):
            running = module.running_interfaces(self.stack(module, host), load_routes(), set())
        self.assertEqual(running, {"grafana"})

    def test_a_service_that_exists_and_is_stopped_is_not_a_running_interface(self) -> None:
        module = runner("stack_caddy")
        host = DockerHost({"caddy": "healthy", "grafana": "healthy", "dagster_webserver": "healthy"})
        host.stopped.add("dagster_webserver")
        with host_at(module, host):
            running = module.running_interfaces(self.stack(module, host), load_routes(), set())
        self.assertEqual(running, {"grafana"})

    def test_the_gateway_checks_pass_for_the_answers_of_the_host_and_fail_for_a_running_interface_taken_as_stopped(self) -> None:
        module = runner("stack_caddy")
        routes: list[Route] = load_routes()
        running = {"openbao", "grafana", "prometheus", "trino", "nessie", "airbyte", "apicurio"}

        def answer(address: str, port: int, name: str, path: str, *, verify: bool) -> object:
            label = next((route.name for route in routes if route.port == port), "gateway")
            if label == "gateway":
                return module.Answer(200, "gateway ok", "c")
            if label in running:
                return module.Answer(406 if label == "trino" else 200, "", "c")
            return module.Answer(502, f"Gateway error 502: the service behind {label} is not running", "c")

        with mock.patch.object(module, "try_get", answer):
            right, wrong = Record(feature="x", slug="x", rerun="x"), Record(feature="x", slug="x", rerun="x")
            module.reach_checks(right, "n", "a", routes, running, expect="allowed", verify=True, where="owner device")
            module.reach_checks(wrong, "n", "a", routes, set(), expect="allowed", verify=True, where="owner device")
        self.assertEqual([item.case for item in right.observations if not item.passed], [])
        self.assertEqual(len([item for item in wrong.observations if not item.passed]), len(running))

    def test_the_host_mode_without_the_public_address_stops_before_it_runs(self) -> None:
        module = runner("stack_caddy")
        with mock.patch("sys.argv", ["stack_caddy", "host"]), mock.patch.object(module, "run_host") as run, self.assertRaises(SystemExit) as stopped:
            module.main()
        self.assertEqual(stopped.exception.code, 2)
        run.assert_not_called()

    def test_the_usage_text_names_the_public_address_as_required_for_the_host_mode(self) -> None:
        module = runner("stack_caddy")
        self.assertIn("--public-address", module.__doc__ or "")
        usage = [line for line in (module.__doc__ or "").splitlines() if "stack_caddy host" in line]
        self.assertTrue(usage and all("--public-address" in line for line in usage))


if __name__ == "__main__":
    unittest.main()
