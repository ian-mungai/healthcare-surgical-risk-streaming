"""The host mode of the Caddy runner checks its flags before it opens an SSH connection, and a Docker error in route discovery is a failed setup case.

The runner stays untracked, so each test skips when it is not on this machine. The Docker is the stand-in of the live host. No SSH, Docker or host runs here.

Failure modes covered here:

- ``--public-address`` is a name, a part of an address, an IPv6 address, an address with a prefix or an address with a space, and the run spends host calls
  and a record before it fails. Every such value is a fixed usage error with exit 2 before any SSH, and the error repeats no part of the value.
- ``docker ps`` on the host fails (the daemon is down, sudo is refused), the runner reads no service as running and expects the gateway error 502 from
  every route, so the failure shows as route cases and not as the Docker error.
- ``--running`` names a route of the Compose project (grafana), which the runner reads from Docker itself, so an interface that Docker says is stopped
  reads as running. Only a route outside Compose (airbyte) may be named.
- The reach mode, which takes the whole list of running interfaces in ``--running``, is changed by the host mode's rule.
"""

from __future__ import annotations

import contextlib
import io
import unittest
from collections.abc import Iterator
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.stack.caddy import load_routes
from tests.e2e.run_record import Record
from tests.fast.docker_host_stand_in import DockerHost
from tests.fast.test_runner_host_state import host_at, runner

BAD_ADDRESSES = (
    "",
    "host.example.com",
    "example",
    "192.0.2",
    "192.0.2.7.9",
    "999.0.2.7",
    "::1",
    "2001:db8::1",
    "192.0.2.7/32",
    " 192.0.2.7",
    "192.0.2.7 ",
    "192.0.2.07",
)


@contextlib.contextmanager
def entered(module: ModuleType, *arguments: str) -> Iterator[SimpleNamespace]:
    """``main`` with these arguments, with every call that leaves this process replaced and counted."""
    calls = SimpleNamespace(remote=mock.Mock(side_effect=AssertionError("SSH target made")), host=mock.Mock(), commands=[], errors=io.StringIO())
    with (
        mock.patch("sys.argv", ["stack_caddy", *arguments]),
        mock.patch.object(module, "Remote", calls.remote),
        mock.patch.object(module, "run_host", calls.host),
        mock.patch.object(module, "run_command", lambda *a, **k: calls.commands.append(a) or SimpleNamespace(returncode=0, stdout="", stderr="")),
        mock.patch.object(Record, "close", return_value=0),
        mock.patch("sys.stderr", calls.errors),
    ):
        yield calls


def main_code(module: ModuleType) -> int:
    try:
        return int(module.main())
    except SystemExit as stopped:
        return int(stopped.code or 0)


class PublicAddress(unittest.TestCase):
    def test_a_value_that_is_not_a_literal_ipv4_address_stops_with_exit_2_before_any_call(self) -> None:
        module = runner("stack_caddy")
        for value in BAD_ADDRESSES:
            with self.subTest(value=value), entered(module, "host", "--public-address", value) as calls:
                self.assertEqual(main_code(module), 2)
                calls.host.assert_not_called()
                calls.remote.assert_not_called()
                self.assertEqual(calls.commands, [])

    def test_the_usage_error_is_fixed_and_repeats_no_part_of_the_value(self) -> None:
        module = runner("stack_caddy")
        texts = set()
        for value in ("host.example.com", "192.0.2.7/32"):
            with entered(module, "host", "--public-address", value) as calls:
                main_code(module)
            text = calls.errors.getvalue()
            texts.add(text.partition("error:")[2])
            self.assertNotIn(value, text)
            self.assertIn("--public-address", text)
        self.assertEqual(len(texts), 1)

    def test_a_literal_ipv4_address_goes_on_to_the_host_run(self) -> None:
        module = runner("stack_caddy")
        with entered(module, "host", "--public-address", "192.0.2.7") as calls:
            self.assertEqual(main_code(module), 0)
            calls.host.assert_called_once()

    def test_the_local_mode_does_not_need_the_address(self) -> None:
        module = runner("stack_caddy")
        with entered(module, "local") as calls, mock.patch.object(module, "run_local") as local:
            self.assertEqual(main_code(module), 0)
            local.assert_called_once()
            calls.host.assert_not_called()


class RunningFlag(unittest.TestCase):
    def test_a_route_of_the_compose_project_is_a_fixed_usage_error(self) -> None:
        module = runner("stack_caddy")
        for value in ("grafana", "airbyte,grafana", "dagster", "openbao,airbyte"):
            with self.subTest(value=value), entered(module, "host", "--public-address", "192.0.2.7", "--running", value) as calls:
                self.assertEqual(main_code(module), 2)
                calls.host.assert_not_called()
                self.assertIn("--running", calls.errors.getvalue())

    def test_a_name_that_is_no_route_is_a_usage_error(self) -> None:
        module = runner("stack_caddy")
        with entered(module, "host", "--public-address", "192.0.2.7", "--running", "nothing") as calls:
            self.assertEqual(main_code(module), 2)
            calls.host.assert_not_called()

    def test_the_route_outside_compose_and_no_flag_are_accepted(self) -> None:
        module = runner("stack_caddy")
        for flags in (["--running", "airbyte"], []):
            with self.subTest(flags=flags), entered(module, "host", "--public-address", "192.0.2.7", *flags) as calls:
                self.assertEqual(main_code(module), 0)
                calls.host.assert_called_once()

    def test_the_usage_error_names_the_routes_that_may_be_given(self) -> None:
        module = runner("stack_caddy")
        with entered(module, "host", "--public-address", "192.0.2.7", "--running", "grafana") as calls:
            main_code(module)
        self.assertIn("airbyte", calls.errors.getvalue())
        self.assertNotIn("grafana is", calls.errors.getvalue())

    def test_the_reach_mode_keeps_taking_the_whole_list(self) -> None:
        module = runner("stack_caddy")
        with (
            entered(module, "reach", "--name", "n", "--address", "100.64.0.1", "--running", "grafana,openbao") as calls,
            mock.patch.object(module, "reach_checks") as reach,
        ):
            self.assertEqual(main_code(module), 0)
            reach.assert_called_once()
            calls.host.assert_not_called()


def no_network(module: ModuleType) -> contextlib.AbstractContextManager[Any]:
    """A connection from the runner to the host's addresses fails the test at once and does not wait for a timeout."""
    refuse = mock.Mock(side_effect=AssertionError("a network call was made"))
    stack = contextlib.ExitStack()
    for name in ("try_get", "tcp_open"):
        stack.enter_context(mock.patch.object(module, name, refuse))
    return stack


class FailingPs(DockerHost):
    """The host's Docker when ``docker ps`` fails, as when the daemon is down or sudo is refused."""

    def answer(self, command: str) -> tuple[int, str]:
        if "docker ps" in command:
            self.commands.append(command)
            return 1, "Cannot connect to the Docker daemon at unix:///var/run/docker.sock\n"
        return super().answer(command)


class Discovery(unittest.TestCase):
    def stack(self, module: ModuleType, host: DockerHost) -> Any:
        return module.Stack(module.Remote(), Record(feature="x", slug="x", rerun="x"), [], ["docker", "compose"])

    def test_a_docker_error_is_not_an_empty_list_of_running_interfaces(self) -> None:
        module = runner("stack_caddy")
        host = FailingPs()
        with host_at(module, host), self.assertRaises(module.DiscoveryError):
            module.running_interfaces(self.stack(module, host), load_routes(), set())

    def test_an_empty_project_is_still_an_empty_list(self) -> None:
        module = runner("stack_caddy")
        host = DockerHost({})
        with host_at(module, host):
            self.assertEqual(module.running_interfaces(self.stack(module, host), load_routes(), set()), set())

    def test_the_host_mode_fails_the_setup_case_and_stops_there(self) -> None:
        module = runner("stack_caddy")
        record = Record(feature="x", slug="x", rerun="x")
        arguments = SimpleNamespace(running="", public_address="192.0.2.7")
        with host_at(module, FailingPs()), mock.patch.object(module, "host_facts", return_value=("host-a.ts.net", "100.64.0.1")), no_network(module):
            module.run_host(record, [], arguments)
        self.assertEqual([(item.case, item.step, item.passed) for item in record.observations], [("interfaces that run", "setup", False)])

    def test_the_host_mode_with_a_working_docker_reads_the_interfaces(self) -> None:
        module = runner("stack_caddy")
        record = Record(feature="x", slug="x", rerun="x")
        arguments = SimpleNamespace(running="airbyte", public_address="192.0.2.7")
        with (
            host_at(module, DockerHost({"caddy": "healthy", "grafana": "healthy"})),
            mock.patch.object(module, "host_facts", return_value=("host-a.ts.net", "100.64.0.1")),
            mock.patch.object(module.Stack, "health", return_value="unhealthy"),
            no_network(module),
        ):
            module.run_host(record, [], arguments)
        first = record.observations[0]
        self.assertEqual((first.case, first.passed), ("interfaces that run", True))
        self.assertIn("grafana", first.observed)


if __name__ == "__main__":
    unittest.main()
