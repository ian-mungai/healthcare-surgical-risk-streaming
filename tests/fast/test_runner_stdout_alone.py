"""The four stack runners parse the standard output of a command and never its standard error, so a warning that SSH prints is not taken as the answer.

The runners stay untracked, so each test skips when they are not on this machine. ``Noisy`` is a target whose ``run`` ends every answer with an SSH warning, as
the live host's did (``run`` is the call whose output is for logs and messages) and whose ``read`` gives the standard output alone. A ``Remote`` is built
without its configuration and its ``run_command`` answers with the answer on stdout and the warning on stderr. No Docker, SSH or host runs here.

Failure modes covered here:

- A container ID, a health word, a secret file, a stat answer, a JSON document or a status line is read from the merged output, so the warning is part of it:
  the ID names a container that does not exist, a secret is stored with the warning in it, and a scan for the secret in the logs then misses it.
- A listing of services is split into words from the merged output, so a word of the warning that names a service reads as a running service.
- The answer of a command that fails is the warning alone and reads as an answer.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"
WARNING = "Warning: Permanently added '100.64.0.9' (ED25519) to the list of known hosts.\n"


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


class Noisy:
    """A target that holds nothing but answers. ``answer`` maps the command to its exit code and its standard output."""

    name = "live host"
    compose_directory = "/opt/platform/compose"
    secrets_directory = "/opt/platform/secrets"
    env_files = ("/opt/platform/compose/limits.env",)
    ports: dict[str, int] = {}
    project = "surgical_risk"

    def __init__(self, answer: Callable[[list[str]], tuple[int, str]], warning: str = WARNING) -> None:
        self.answer = answer
        self.warning = warning
        self.calls: list[list[str]] = []

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        self.calls.append(args)
        code, output = self.answer(args)
        return code, output + self.warning

    def read(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        self.calls.append(args)
        return self.answer(args)

    def read_text(self, path: str) -> str:
        return self.answer(["cat", path])[1]


def docker_answers(args: list[str], memory: int = 1073741824, ports: dict[str, Any] | None = None) -> tuple[int, str]:
    words = " ".join(args)
    if "compose" in args and "ps" in args:
        return 0, "abc123\n"
    if "State.Health.Status" in words and "State.Status" in words:
        return 0, "running/healthy\n"
    if "State.Health.Status" in words:
        return 0, "healthy\n"
    if "HostConfig.Memory" in words:
        return 0, f"{memory}\n"
    if "NetworkSettings.Ports" in words:
        return 0, json.dumps(ports or {}) + "\n"
    if "ps" in args and any(item.startswith("name=") for item in args):
        return 0, "abc123\n"
    return 1, "unsupported\n"


def patched(module: ModuleType, stdout: str, code: int = 0) -> Any:
    answer = SimpleNamespace(returncode=code, stdout=stdout, stderr=WARNING)
    return mock.patch.multiple(module, ssh_arguments=lambda run_id: [], run_command=lambda *a, **k: answer)


def new_remote(module: ModuleType) -> Any:
    remote = module.Remote.__new__(module.Remote)
    remote.run_id, remote.address, remote.port = "run", "host", 8200
    return remote


class RemoteReadsStandardOutput(unittest.TestCase):
    def test_a_secret_file_is_read_without_the_warning_of_ssh(self) -> None:
        for name in ("stack_monitoring", "stack_openbao", "stack_nessie"):
            module = runner(name)
            with self.subTest(runner=name), patched(module, "s3cr3t\n"):
                self.assertEqual(new_remote(module).read_text("/opt/platform/secrets/x"), "s3cr3t\n")

    def test_the_read_call_gives_the_standard_output_and_the_run_call_gives_both(self) -> None:
        for name in ("stack_monitoring", "stack_caddy"):
            module = runner(name)
            with self.subTest(runner=name), patched(module, "out\n"):
                remote = new_remote(module)
                self.assertEqual(remote.read(["docker", "ps"]), (0, "out\n"))
                self.assertEqual(remote.run(["docker", "ps"]), (0, "out\n" + WARNING))

    def test_a_command_that_fails_reads_as_nothing(self) -> None:
        for name in ("stack_monitoring", "stack_caddy"):
            module = runner(name)
            with self.subTest(runner=name), patched(module, "", code=1):
                self.assertEqual(new_remote(module).read(["docker", "ps"]), (1, ""))

    def test_a_timeout_reads_as_exit_255_with_no_text_as_data(self) -> None:
        for name in ("stack_monitoring", "stack_caddy"):
            module = runner(name)

            def late(*arguments: Any, error: Any = module.TimeoutExpired, **keywords: Any) -> Any:
                raise error("ssh", 1)

            with self.subTest(runner=name), mock.patch.multiple(module, ssh_arguments=lambda run_id: [], run_command=late):
                code, output = new_remote(module).read(["docker", "ps"])
                self.assertEqual((code, output), (255, ""))

    def test_the_status_line_of_a_request_is_read_from_the_standard_output(self) -> None:
        module = runner("stack_monitoring")
        with patched(module, '{"a": 1}\n200'):
            self.assertEqual(new_remote(module).http(9090, "GET", "/x"), (200, '{"a": 1}'))

    def test_the_mode_of_a_file_is_read_from_the_standard_output(self) -> None:
        module = runner("stack_openbao")
        with patched(module, "400 100:1000\n"):
            self.assertEqual(new_remote(module).mode("/opt/platform/secrets/x"), "400 100:1000")


class MonitoringStack(unittest.TestCase):
    def stack(self, answer: Callable[[list[str]], tuple[int, str]] = docker_answers) -> tuple[ModuleType, Any]:
        module = runner("stack_monitoring")
        return module, module.Stack(Noisy(answer), Record(feature="x", slug="x", rerun="x"), [])

    def test_the_container_and_its_health_are_read_without_the_warning(self) -> None:
        _, stack = self.stack()
        self.assertEqual(stack.container("postgres"), "abc123")
        self.assertEqual(stack.health("postgres"), "running/healthy")

    def test_a_service_with_no_container_reads_as_absent_and_not_as_the_warning(self) -> None:
        _, stack = self.stack(lambda args: (0, ""))
        self.assertEqual(stack.container("postgres"), "")
        self.assertEqual(stack.health("postgres"), "absent")

    def test_a_named_container_that_does_not_exist_is_not_found_because_of_a_warning(self) -> None:
        _, stack = self.stack(lambda args: (0, ""))
        self.assertFalse(stack.container_exists("p2m_marker_x"))

    def test_a_container_of_another_name_is_not_found(self) -> None:
        _, stack = self.stack(lambda args: (0, "p2m_marker_other\n"))
        self.assertFalse(stack.container_exists("p2m_marker_x"))

    def test_a_container_of_the_name_is_found(self) -> None:
        _, stack = self.stack(lambda args: (0, "p2m_marker_x\n"))
        self.assertTrue(stack.container_exists("p2m_marker_x"))

    def test_the_standard_output_of_a_docker_command_is_read_alone(self) -> None:
        _, stack = self.stack(lambda args: (0, "42\n"))
        self.assertEqual(stack.docker_out("inspect", "x"), (0, "42\n"))
        self.assertEqual(stack.docker("inspect", "x"), (0, "42\n" + WARNING))


class AlloyAccount(unittest.TestCase):
    def created(self, code: int, output: str) -> bool:
        module = runner("stack_monitoring")
        stack = module.Stack(Noisy(lambda args: (code, output), warning=""), Record(feature="x", slug="x", rerun="x"), [])
        stack.passwords = {"postgres/postgres": "p", "postgres/alloy": "a"}
        return bool(module.create_alloy_account(stack))

    def test_the_account_that_exists_already_reads_as_made(self) -> None:
        self.assertTrue(self.created(3, 'psql:<stdin>:1: ERROR:  role "alloy" already exists\n'))
        self.assertTrue(self.created(3, 'psql:<stdin>:2: ERROR:  database "alloy" already exists\n'))

    def test_a_made_account_reads_as_made(self) -> None:
        self.assertTrue(self.created(0, ""))

    def test_another_error_that_mentions_an_account_that_exists_does_not_pass(self) -> None:
        self.assertFalse(self.created(3, 'psql: ERROR:  permission denied: role "alloy" already exists is not the cause\n'))
        self.assertFalse(self.created(3, 'ERROR:  role "other" already exists\n'))

    def test_a_failure_with_no_error_line_does_not_pass(self) -> None:
        self.assertFalse(self.created(255, "already exists\n"))
        self.assertFalse(self.created(255, ""))

    def test_a_second_error_line_that_is_not_the_known_one_does_not_pass(self) -> None:
        self.assertFalse(self.created(3, 'ERROR:  role "alloy" already exists\nERROR:  connection to server failed\n'))


class CaddyStack(unittest.TestCase):
    def stack(self, answer: Callable[[list[str]], tuple[int, str]], warning: str = WARNING) -> tuple[ModuleType, Any]:
        module = runner("stack_caddy")
        target = Noisy(answer, warning)
        return module, module.Stack(target, Record(feature="x", slug="x", rerun="x"), [], ["docker", "compose"])

    def test_the_container_and_its_health_are_read_without_the_warning(self) -> None:
        _, stack = self.stack(docker_answers)
        self.assertEqual(stack.container(), "abc123")
        self.assertEqual(stack.health(), "healthy")
        self.assertEqual(stack.memory_limit(), (0, "1073741824\n"))

    def test_the_services_that_run_are_the_lines_of_the_standard_output_only(self) -> None:
        module, _ = self.stack(docker_answers)
        routes = module.load_routes()

        def service(route: Any) -> str:
            return str(module.ROUTE_SERVICE.get(route.name, route.name))

        noise = f"{service(routes[1])}\n"
        _, stack = self.stack(lambda args: (0, f"{service(routes[0])}\n"), warning=noise)
        found = module.running_interfaces(stack, routes, set())
        self.assertEqual(found, {routes[0].name})

    def test_a_service_with_no_container_reads_as_none_and_not_as_the_warning(self) -> None:
        _, stack = self.stack(lambda args: (0, ""))
        self.assertEqual(stack.container(), "")

    def test_a_listing_that_fails_is_a_discovery_error_whatever_it_prints(self) -> None:
        module, _ = self.stack(docker_answers)
        _, stack = self.stack(lambda args: (1, "grafana\n"))
        with self.assertRaises(module.DiscoveryError):
            module.running_interfaces(stack, module.load_routes(), set())

    def test_the_facts_of_the_host_are_read_from_the_standard_output(self) -> None:
        module, stack = self.stack(lambda args: (0, json.dumps({"Self": {"DNSName": "h.ts.net.", "TailscaleIPs": ["100.1.1.1", "fd7a::1"]}}) + "\n"))
        run = Record(feature="x", slug="x", rerun="x")
        self.assertEqual(module.host_facts(stack.target, run), ("h.ts.net", "100.1.1.1"))
        self.assertEqual([item.case for item in run.observations], [])

    def test_the_root_certificate_is_hashed_without_the_warning(self) -> None:
        module, stack = self.stack(lambda args: (0, "abc123\n") if "compose" in args else (0, "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n"))
        self.assertEqual(module.authority(stack), hashlib.sha256(b"-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n").hexdigest())


class OpenbaoStack(unittest.TestCase):
    def stack(self) -> tuple[ModuleType, Any]:
        module = runner("stack_openbao")
        limit = module.load_limits().services["openbao"].memory_limit_mib * 1024 * 1024
        published = {"8200/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8200"}]}
        target = Noisy(lambda args: docker_answers(args, limit, published))
        return module, module.Stack(target, Record(feature="x", slug="x", rerun="x"), [], [], [], {})

    def test_the_container_and_its_health_are_read_without_the_warning(self) -> None:
        _, stack = self.stack()
        self.assertEqual(stack.container(), "abc123")
        self.assertEqual(stack.health(), "healthy")

    def test_a_service_with_no_container_reads_as_none_and_not_as_the_warning(self) -> None:
        _, stack = self.stack()
        stack.target.answer = lambda args: (0, "")
        self.assertEqual(stack.container(), "")

    def test_the_memory_limit_and_the_published_port_are_read_from_the_standard_output(self) -> None:
        module, stack = self.stack()
        module.published_port(stack)
        module.memory(stack)
        self.assertEqual([item.case for item in stack.record.observations if not item.passed], [])
        self.assertEqual([item.case for item in stack.record.observations], ["published port binding", "memory limit"])


class NessieStack(unittest.TestCase):
    def stack(self) -> tuple[ModuleType, Any]:
        module = runner("stack_nessie")
        limit = module.load_limits().services["nessie"].memory_limit_mib * 1024 * 1024
        published = {f"{module.API_PORT_IN_CONTAINER}/tcp": [{"HostIp": "127.0.0.1", "HostPort": "19120"}]}
        target = Noisy(lambda args: docker_answers(args, limit, published))
        return module, module.Stack(target, Record(feature="x", slug="x", rerun="x"), [])

    def test_the_container_and_its_health_are_read_without_the_warning(self) -> None:
        _, stack = self.stack()
        self.assertEqual(stack.container("nessie"), "abc123")
        self.assertEqual(stack.health("nessie"), "healthy")

    def test_the_memory_limit_and_the_published_port_are_read_from_the_standard_output(self) -> None:
        module, stack = self.stack()
        module.memory(stack)
        module.ports(stack, None)
        self.assertEqual([item.case for item in stack.record.observations if not item.passed], [])

    def test_a_service_with_no_container_reads_as_none_and_not_as_the_warning(self) -> None:
        _, stack = self.stack()
        stack.target.answer = lambda args: (0, "")
        self.assertEqual(stack.container("nessie"), "")

    def test_the_answer_of_curl_in_the_container_is_read_from_the_standard_output(self) -> None:
        module, stack = self.stack()
        stack.target.answer = lambda args: (0, '{"a": 1}\n200')
        self.assertEqual(module.ExecClient(stack).send("GET", "/x", None), (200, b'{"a": 1}'))

    def test_the_start_time_is_read_from_the_standard_output(self) -> None:
        module, stack = self.stack()
        stack.target.answer = lambda args: (0, "abc123\n") if "compose" in args else (0, "2026-11-25T00:00:00Z\n")
        module.idempotence(stack)
        self.assertEqual([item.case for item in stack.record.observations if not item.passed], [])


class CatalogStack(unittest.TestCase):
    def test_the_published_ports_are_read_from_the_standard_output_alone(self) -> None:
        module = runner("catalog_proof")

        def run(args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str, str]:
            if "ps" in args and "compose" in args:
                return 0, "abc123\n", WARNING
            return 0, json.dumps({"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}) + "\n", WARNING

        target = SimpleNamespace(compose_directory="/c", secrets_directory="/s", project="p", run=run)
        run_record = Record(feature="x", slug="x", rerun="x")
        module.ports_case(module.Stack(target, run_record, []), run_record, None)
        self.assertEqual([(item.case, item.passed) for item in run_record.observations], [("web interface address", True)])

    def test_the_standard_output_of_a_docker_command_is_read_alone(self) -> None:
        module = runner("catalog_proof")
        target = SimpleNamespace(run=lambda args, **_: (0, "42\n", WARNING))
        stack = module.Stack(target, Record(feature="x", slug="x", rerun="x"), [])
        self.assertEqual(stack.docker_out("inspect", "x"), (0, "42\n"))
        self.assertEqual(stack.docker("inspect", "x"), (0, "42\n" + WARNING))
        self.assertEqual(stack.docker_split("inspect", "x"), (0, "42\n", WARNING))


if __name__ == "__main__":
    unittest.main()
