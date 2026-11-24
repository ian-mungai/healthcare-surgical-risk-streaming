"""The routes and the files that the record load needs on the host, through the real ``ansible-playbook``.

The code location container reaches Airbyte's API and Airbyte's pods reach Nessie through socket-activated forwarders on the k3s bridge address, in the form
of the PostgreSQL route. A network policy admits the Airbyte namespace to the Nessie forwarder and no pod to the API forwarder. The container reads the
instance's client credentials and the destination's AWS access key from files that its own user owns. Starting the units, applying the policy and reading the
cluster's secret need k3s and are proven on the host; these cases cover what a clean computer shows.
"""

from __future__ import annotations

import base64
import json
import os
import re
import stat
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.stack.yaml_text import load_yaml
from tests.slow.ansible_support import environment
from tests.slow.test_airbyte_role import BRIDGE, CHANGED_NONE, ROLE, flatten, run_tasks, values

ROLE_TASKS = ROLE / "tasks"
CLIENT_ID = "client-id-value-1"
CLIENT_VALUE = "client-value-2"
KEY_ID = "AKIAEXAMPLEEXAMPLE0"
KEY_VALUE = "access-value-3"
USER = str(os.getuid())


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def record_values(folder: Path, **changes: object) -> dict[str, object]:
    (folder / "secrets" / "airbyte").mkdir(parents=True, exist_ok=True)
    return (
        values(folder)
        | {
            "airbyte_api_route_port": 8001,
            "airbyte_nessie_route_port": 19120,
            "compose_stack_nessie_port": 19120,
            "airbyte_secrets_directory": str(folder / "secrets"),
            "airbyte_record_container_user": USER,
            "airbyte_record_container_group": str(os.getgid()),
            "airbyte_record_client_id": CLIENT_ID,
            "airbyte_record_client_secret": CLIENT_VALUE,
        }
        | changes
    )


def place_aws_files(folder: Path) -> None:
    """The two key files of the raw connection, as the key script places them: bait that the client files must never read or copy."""
    (folder / "secrets" / "airbyte").mkdir(parents=True, exist_ok=True)
    (folder / "secrets" / "airbyte" / "aws_access_key_id").write_text(KEY_ID + "\n")
    (folder / "secrets" / "airbyte" / "aws_secret_access_key").write_text(KEY_VALUE + "\n")


def unit_files(folder: Path, name: str) -> tuple[str, str]:
    socket, service = (folder / "units" / f"{name}.{kind}" for kind in ("socket", "service"))
    return (socket.read_text() if socket.exists() else "", service.read_text() if service.exists() else "")


class RouteUnitsTest(unittest.TestCase):
    def units(self, route: dict[str, object], **changes: object) -> tuple[int, str, tuple[str, str]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["record_route_units.yml"], record_values(folder, **route) | changes)
            return code, output, unit_files(folder, str(route["airbyte_route_unit"]))

    API = {"airbyte_route_unit": "airbyte-api-route", "airbyte_route_listen_port": 8001, "airbyte_route_target_port": 8000}
    NESSIE = {"airbyte_route_unit": "airbyte-nessie-route", "airbyte_route_listen_port": 19120, "airbyte_route_target_port": 19120}

    def test_each_socket_listens_on_the_bridge_address_and_its_port_only_and_may_bind_before_the_bridge_exists(self) -> None:
        for route in (self.API, self.NESSIE):
            with self.subTest(unit=route["airbyte_route_unit"]):
                code, output, (socket, _) = self.units(route)
                self.assertEqual(0, code, output)
                self.assertEqual(
                    [f"ListenStream={BRIDGE}:{route['airbyte_route_listen_port']}"], [line for line in socket.splitlines() if line.startswith("Listen")]
                )
                self.assertIn("FreeBind=true", socket)
                self.assertIn("WantedBy=sockets.target", socket)
                self.assertNotRegex(socket, r"0\.0\.0\.0|\[::\]|ListenStream=\d+\s")

    def test_each_service_forwards_to_the_servers_local_address_with_the_shipped_proxy_and_stays_in_the_host_network(self) -> None:
        for route in (self.API, self.NESSIE):
            with self.subTest(unit=route["airbyte_route_unit"]):
                _, _, (_, service) = self.units(route)
                command = next(line for line in service.splitlines() if line.startswith("ExecStart="))
                self.assertEqual(f"ExecStart=/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:{route['airbyte_route_target_port']}", command)
                self.assertIn(f"Requires={route['airbyte_route_unit']}.socket", service)
                self.assertIn("Type=notify", service)
                self.assertIn("NoNewPrivileges=yes", service)
                self.assertNotIn("PrivateNetwork", service)

    def test_the_bridge_address_comes_from_the_variable_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["record_route_units.yml"], record_values(folder, **self.API, airbyte_bridge_address="10.42.9.1"))
            socket, _ = unit_files(folder, "airbyte-api-route")
            code, output = run_tasks(["record_route_units.yml"], record_values(folder, **self.API, airbyte_bridge_address="10.42.9.1"))
        self.assertIn("ListenStream=10.42.9.1:8001", socket)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_port_that_is_not_a_port_number_or_a_unit_name_that_is_not_a_plain_name_stops_before_any_unit_is_written(self) -> None:
        for change in (
            {"airbyte_route_listen_port": 0},
            {"airbyte_route_listen_port": 70000},
            {"airbyte_route_target_port": "80;rm"},
            {"airbyte_route_unit": "../x"},
            {"airbyte_route_unit": "a b"},
        ):
            with self.subTest(change=change):
                code, output, (socket, service) = self.units(self.API | change)
                self.assertNotEqual(0, code, output)
                self.assertEqual(("", ""), (socket, service))


class PolicyTest(unittest.TestCase):
    def policy(self, **changes: object) -> tuple[int, str, list[dict[str, Any]]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["record_route_policy.yml"], record_values(folder, **changes))
            written = folder / "airbyte" / "record_route_policy.yaml"
            documents = [item for item in re.split(r"^---$", written.read_text(), flags=re.MULTILINE) if item.strip()] if written.exists() else []
            return code, output, [mapping(load_yaml(item)) for item in documents]

    def test_one_policy_admits_the_airbyte_namespace_to_the_bridge_address_on_the_nessie_port_only(self) -> None:
        code, output, documents = self.policy()
        self.assertEqual(0, code, output)
        self.assertEqual(1, len(documents))
        policy = documents[0]
        self.assertEqual(("NetworkPolicy", "airbyte", "nessie-bridge"), (policy["kind"], policy["metadata"]["namespace"], policy["metadata"]["name"]))
        self.assertEqual({}, policy["spec"]["podSelector"])
        self.assertEqual(["Egress"], policy["spec"]["policyTypes"])
        self.assertEqual([{"to": [{"ipBlock": {"cidr": f"{BRIDGE}/32"}}], "ports": [{"protocol": "TCP", "port": 19120}]}], policy["spec"]["egress"])

    def test_no_pod_is_admitted_to_the_api_forwarder_and_no_other_namespace_to_anything(self) -> None:
        _, _, documents = self.policy(airbyte_api_route_port=8001)
        text = json.dumps(documents)
        self.assertNotIn("8001", text)
        self.assertEqual({"airbyte"}, {document["metadata"]["namespace"] for document in documents})

    def test_the_port_and_the_namespace_follow_the_variables(self) -> None:
        _, _, documents = self.policy(airbyte_nessie_route_port=19999, airbyte_namespace="other")
        self.assertEqual("other", documents[0]["metadata"]["namespace"])
        self.assertEqual(19999, documents[0]["spec"]["egress"][0]["ports"][0]["port"])

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["record_route_policy.yml"], record_values(folder))
            code, output = run_tasks(["record_route_policy.yml"], record_values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


class ClientFilesTest(unittest.TestCase):
    def write(self, folder: Path, **changes: object) -> tuple[int, str]:
        return run_tasks(["record_client_files.yml"], record_values(folder, **changes))

    def files(self, folder: Path) -> dict[str, tuple[str, int, int]]:
        found = {}
        for path in sorted((folder / "secrets" / "airbyte_api").glob("*")):
            status = path.stat()
            found[path.name] = (path.read_text(), stat.S_IMODE(status.st_mode), status.st_uid)
        return found

    def test_the_four_files_are_owned_by_the_container_user_with_mode_0400_in_a_folder_only_its_owner_enters_and_no_key_file_is_among_them(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            found = self.files(folder)
            mode = stat.S_IMODE((folder / "secrets" / "airbyte_api").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual({"api_url", "nessie_url", "client_id", "client_secret"}, set(found))
        self.assertEqual(
            {
                "api_url": f"http://{BRIDGE}:8001",
                "nessie_url": f"http://{BRIDGE}:19120/api/v2",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_VALUE,
            },
            {name: text.rstrip("\n") for name, (text, _, _) in found.items()},
        )
        self.assertEqual({0o400}, {file_mode for _, file_mode, _ in found.values()})
        self.assertEqual({int(USER)}, {owner for _, _, owner in found.values()})
        self.assertEqual(0o700, mode)

    def test_the_run_never_reads_the_key_files_so_it_succeeds_without_them_and_copies_no_key_value_when_they_exist(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.write(folder)
            self.assertEqual(0, code, output)
            self.assertEqual({"api_url", "nessie_url", "client_id", "client_secret"}, set(self.files(folder)), "no key file is needed")
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            held = "".join(text for text, _, _ in self.files(folder).values())
        self.assertEqual(0, code, output)
        for value in (KEY_ID, KEY_VALUE):
            self.assertNotIn(value, held)
            for form in (value, base64.b64encode(value.encode()).decode(), base64.b64encode((value + "\n").encode()).decode()):
                self.assertNotIn(form, output)

    def test_the_address_file_holds_the_bridge_address_and_the_api_route_port_the_deploy_configured(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            self.write(folder, airbyte_bridge_address="10.42.9.1", airbyte_api_route_port=8123)
            found = self.files(folder)
        self.assertEqual("http://10.42.9.1:8123", found["api_url"][0].rstrip("\n"))
        self.assertEqual("http://10.42.9.1:19120/api/v2", found["nessie_url"][0].rstrip("\n"))

    def test_the_nessie_address_file_follows_the_nessie_route_port(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            self.write(folder, airbyte_nessie_route_port=19999)
            found = self.files(folder)
        self.assertEqual(f"http://{BRIDGE}:19999/api/v2", found["nessie_url"][0].rstrip("\n"))

    def test_a_bridge_address_that_is_not_a_private_IPv4_literal_or_a_port_that_is_not_a_port_stops_the_run_before_any_file_is_placed(self) -> None:
        for change in (
            {"airbyte_bridge_address": "airbyte.example"},
            {"airbyte_bridge_address": "8.8.8.8"},
            {"airbyte_bridge_address": "127.0.0.1"},
            {"airbyte_bridge_address": "10.42.0.1@8.8.8.8"},
            {"airbyte_bridge_address": "10.42.0.1/x"},
            {"airbyte_bridge_address": ""},
            {"airbyte_bridge_address": "172.32.0.1"},
            {"airbyte_bridge_address": "100.64.0.1"},
            {"airbyte_bridge_address": "10.042.0.1"},
            {"airbyte_api_route_port": "8001?"},
            {"airbyte_api_route_port": "8001/"},
            {"airbyte_api_route_port": "8001#"},
            {"airbyte_api_route_port": "08001"},
            {"airbyte_api_route_port": 0},
            {"airbyte_api_route_port": 70000},
            {"airbyte_api_route_port": "80/x"},
            {"airbyte_nessie_route_port": 0},
            {"airbyte_nessie_route_port": "19120/x"},
        ):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                place_aws_files(folder)
                code, output = self.write(folder, **change)
                self.assertNotEqual(0, code, output)
                self.assertEqual({}, self.files(folder))

    def test_no_secret_value_appears_in_the_playbook_output(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            held = {name: content.rstrip("\n") for name, (content, _, _) in self.files(folder).items()}
        self.assertEqual(0, code, output[-2000:])
        self.assertEqual(
            (CLIENT_ID, CLIENT_VALUE), (held["client_id"], held["client_secret"]), "control: the files hold the values that the output is searched for"
        )
        for value in (CLIENT_ID, CLIENT_VALUE):
            for form in (value, base64.b64encode(value.encode()).decode(), base64.b64encode((value + "\n").encode()).decode()):
                self.assertNotIn(form, output)

    def test_a_second_run_changes_nothing_and_a_rotated_client_value_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.write(folder)
            code, output = self.write(folder)
            self.write(folder, **{"airbyte_record_client_secret": "rotated-value-4"})
            rotated = self.files(folder)["client_secret"][0].rstrip("\n")
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual("rotated-value-4", rotated)

    def test_an_empty_client_credential_stops_the_run(self) -> None:
        for name in ("airbyte_record_client_id", "airbyte_record_client_secret"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                place_aws_files(folder)
                code, output = self.write(folder, **{name: ""})
                self.assertNotEqual(0, code, output)
                self.assertEqual({}, self.files(folder))


class ReadWaitTest(unittest.TestCase):
    """The lookup of the client credentials has one elapsed-time deadline, enforced by ``timeout`` around one loop, and no task outlasts it by a fresh bound."""

    def task(self) -> dict[str, Any]:
        tasks = flatten(load_yaml((ROLE_TASKS / "record_client_read.yml").read_text()))
        return next(task for task in tasks if task.get("name", "").startswith("Wait for and read"))

    def argv(self, deadline: int) -> list[str]:
        words = [
            str(word).replace("{{ airbyte_record_client_deadline_seconds | default(120) }}", str(deadline)).replace("{{ airbyte_namespace }}", "airbyte")
            for word in self.task()["ansible.builtin.command"]["argv"]
        ]
        self.assertNotIn("{{", "".join(words))
        return words

    def fake_k3s(self, folder: Path, ready_after: int) -> dict[str, str]:
        bin_directory = folder / "bin"
        bin_directory.mkdir()
        count = folder / "count"
        count.write_text("0")
        (bin_directory / "k3s").write_text(
            f'#!/bin/sh\nn=$(($(cat {count}) + 1))\necho $n > {count}\nif [ "$n" -ge {ready_after} ]; then printf "aWQ=:c2VjcmV0"; else printf ":"; fi\n'
        )
        (bin_directory / "k3s").chmod(0o755)
        return {"PATH": f"{bin_directory}:/usr/bin:/bin"}

    def run_argv(self, folder: Path, ready_after: int, deadline: int) -> tuple[int, str, float]:
        started = time.monotonic()
        argv = self.argv(deadline)
        result = run_command(argv[0], argv[1:], env=self.fake_k3s(folder, ready_after), timeout=deadline + 20)
        return result.returncode, result.stdout, time.monotonic() - started

    def test_the_command_is_wrapped_by_one_deadline_and_the_task_limit_is_a_little_above_it(self) -> None:
        task = self.task()
        argv = [str(word) for word in task["ansible.builtin.command"]["argv"]]
        self.assertEqual("timeout", argv[0])
        self.assertIn("airbyte_record_client_deadline_seconds | default(120)", argv[1])
        self.assertNotIn("retries", task)
        self.assertNotIn("until", task)
        self.assertIn("airbyte_record_client_deadline_seconds | default(120)", str(task["timeout"]))
        self.assertTrue(str(task["timeout"]).strip().endswith("+ 15 }}") or "+ 15" in str(task["timeout"]))

    def test_control_the_values_are_read_as_soon_as_both_exist(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, _ = self.run_argv(Path(scratch), 2, 30)
        self.assertEqual((0, "aWQ=:c2VjcmV0"), (code, output))

    def test_a_secret_that_never_holds_them_ends_at_the_one_deadline_with_the_timeout_status(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output, seconds = self.run_argv(Path(scratch), 10**6, 3)
        self.assertEqual(124, code)
        self.assertEqual("", output)
        self.assertGreaterEqual(seconds, 3.0)
        self.assertLess(seconds, 3.0 + 4.0, "the total wait is the deadline and one poll, not a deadline for each attempt")

    def test_only_a_value_with_both_parts_ends_the_wait(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            env = self.fake_k3s(folder, 10**6)
            (folder / "bin" / "k3s").write_text('#!/bin/sh\nprintf "aWQ=:"\n')
            argv = self.argv(2)
            result = run_command(argv[0], argv[1:], env=env, timeout=20)
        self.assertEqual(124, result.returncode)


KUBECTL_ERROR = 'Error from server (Forbidden): secrets "airbyte-auth-secrets" is forbidden: User "nobody" cannot get resource'
READ_ID = "read-id-value-5"
READ_VALUE = "read-value-6"


def encoded(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


class ReadFailureTest(unittest.TestCase):
    """A kubectl failure shows kubectl's own message at the deadline. It was discarded, so a refused call looked like a secret that is not filled yet.

    The message is no secret: it is the one stream of kubectl that never holds a value. The values themselves stay in the hidden task.
    """

    def argv(self, deadline: int) -> list[str]:
        return ReadWaitTest().argv(deadline)

    def fake_k3s(self, folder: Path, body: str) -> dict[str, str]:
        (folder / "bin").mkdir()
        (folder / "bin" / "k3s").write_text(f"#!/bin/sh\n{body}\n")
        (folder / "bin" / "k3s").chmod(0o755)
        return {"PATH": f"{folder / 'bin'}:/usr/bin:/bin"}

    def playbook(self, folder: Path, body: str, **changes: object) -> tuple[int, str]:
        env = environment(folder) | self.fake_k3s(folder, body)
        variables = record_values(folder, airbyte_record_client_deadline_seconds=2, **changes)
        playbook = folder / "check.yml"
        include = f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: record_client_read.yml\n"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{include}")
        result = run_command(
            str(Path(__file__).resolve().parents[2] / ".venv/bin/ansible-playbook"),
            ["-v", "-i", "localhost,", "--extra-vars", json.dumps(variables), str(playbook)],
            cwd=folder,
            env=env,
            timeout=120,
        )
        return result.returncode, result.stdout + result.stderr

    def test_the_wait_prints_the_last_kubectl_error_at_the_deadline_and_keeps_the_timeout_status(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            argv = self.argv(2)
            result = run_command(argv[0], argv[1:], env=self.fake_k3s(folder, f"echo '{KUBECTL_ERROR}' >&2\nexit 1"), timeout=30)
        self.assertEqual((124, ""), (result.returncode, result.stdout))
        self.assertEqual(KUBECTL_ERROR, result.stderr.strip().splitlines()[-1], "the shell may add a line about the stopped child before it")

    def test_control_a_wait_that_ends_with_both_values_prints_nothing_on_stderr_and_leaves_no_file_behind(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder, temporary = Path(scratch), Path(scratch) / "tmp"
            temporary.mkdir()
            argv = self.argv(10)
            env = self.fake_k3s(folder, "printf 'aWQ=:c2VjcmV0'") | {"TMPDIR": str(temporary)}
            result = run_command(argv[0], argv[1:], env=env, timeout=30)
            left = list(temporary.iterdir())
        self.assertEqual((0, "aWQ=:c2VjcmV0", "", []), (result.returncode, result.stdout, result.stderr, left))

    def test_a_wait_that_ends_at_the_deadline_leaves_no_file_behind(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder, temporary = Path(scratch), Path(scratch) / "tmp"
            temporary.mkdir()
            argv = self.argv(2)
            env = self.fake_k3s(folder, "echo oops >&2\nexit 1") | {"TMPDIR": str(temporary)}
            run_command(argv[0], argv[1:], env=env, timeout=30)
            left = list(temporary.iterdir())
        self.assertEqual([], left)

    def test_the_run_stops_with_the_secret_name_and_kubectls_message_and_shows_no_value(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.playbook(Path(scratch), f"echo '{KUBECTL_ERROR}' >&2\nexit 1")
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        self.assertIn(KUBECTL_ERROR, output)

    def test_a_secret_that_exists_but_holds_no_values_stops_with_its_name_and_no_message_of_kubectl(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.playbook(Path(scratch), "printf ':'")
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        self.assertNotIn("Error from server", output)

    def test_a_half_filled_secret_that_never_completes_shows_no_part_of_its_values(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.playbook(Path(scratch), f"printf '%s:' '{encoded(READ_ID)}'")
        self.assertNotEqual(0, code, output)
        self.assertIn("airbyte-auth-secrets", output)
        for secret in (READ_ID, encoded(READ_ID)):
            self.assertNotIn(secret, output)

    def test_control_both_values_pass_and_neither_the_encoded_nor_the_decoded_value_is_shown(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            code, output = self.playbook(Path(scratch), f"printf '%s' '{encoded(READ_ID)}:{encoded(READ_VALUE)}'")
        self.assertEqual(0, code, output)
        for secret in (READ_ID, READ_VALUE, encoded(READ_ID), encoded(READ_VALUE)):
            self.assertNotIn(secret, output)


class RoleIncludeTest(unittest.TestCase):
    def test_the_role_includes_the_record_routes_after_the_release_and_the_web_forward_and_before_the_verification(self) -> None:
        main = (ROLE_TASKS / "main.yml").read_text()
        self.assertEqual(1, main.count("record_routes.yml"))
        for earlier in ("release.yml", "web_forward.yml", "network_policy.yml", "secrets.yml"):
            self.assertLess(main.index(earlier), main.index("record_routes.yml"))
        self.assertLess(main.index("record_routes.yml"), main.index("verify.yml"))

    def test_the_include_is_one_task_with_a_name(self) -> None:
        tasks = flatten(load_yaml((ROLE_TASKS / "main.yml").read_text()))
        include = [task for task in tasks if task.get("ansible.builtin.include_tasks") == "record_routes.yml"]
        self.assertEqual(1, len(include))
        self.assertTrue(include[0].get("name"))


class StaticRulesTest(unittest.TestCase):
    FILES = ("record_routes.yml", "record_route.yml", "record_route_units.yml", "record_route_policy.yml", "record_client_files.yml", "record_client_read.yml")

    def tasks(self) -> dict[str, list[dict[str, Any]]]:
        return {name: flatten(load_yaml((ROLE_TASKS / name).read_text())) for name in self.FILES}

    def test_the_entry_file_runs_the_units_the_policy_and_the_client_files_in_that_order(self) -> None:
        text = (ROLE_TASKS / "record_routes.yml").read_text()
        order = [text.index(name) for name in ("record_route.yml", "record_route_policy.yml", "record_client_read.yml", "record_client_files.yml")]
        self.assertEqual(sorted(order), order)
        self.assertEqual(2, text.count("record_route.yml"), "one route for Airbyte's API and one for Nessie")

    def test_every_task_that_holds_a_credential_hides_its_values(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                body = json.dumps(task)
                if (
                    any(word in body for word in ("airbyte_record_client", "instance-admin-client", "aws_access_key_id", "aws_secret_access_key"))
                    and "include_tasks" not in task
                ):
                    with self.subTest(file=name, task=task.get("name")):
                        self.assertIs(True, task.get("no_log"))

    def test_no_file_types_the_bridge_address_a_credential_or_a_literal_secret_on_a_command_line(self) -> None:
        for name in self.FILES:
            text = (ROLE_TASKS / name).read_text()
            with self.subTest(file=name):
                self.assertNotIn("10.42.0.1", text)
                for forbidden in ("--from-literal", "from-file", "stringData", "password=", "PASSWORD="):
                    self.assertNotIn(forbidden, text)

    def test_every_systemd_action_has_a_task_deadline(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                if "ansible.builtin.systemd_service" in task:
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)
                        self.assertLessEqual(int(task["timeout"]), 300)

    def test_every_command_and_every_wait_of_the_files_is_bounded(self) -> None:
        for name, tasks in self.tasks().items():
            for task in tasks:
                if "ansible.builtin.command" in task:
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)
                        self.assertNotIn("until", task, "a retry loop gives each attempt a fresh bound, so the total is not the deadline")

    def test_no_task_of_the_client_files_names_a_key_file_or_a_key_value_or_reads_a_file_of_the_raw_connection(self) -> None:
        text = (ROLE_TASKS / "record_client_files.yml").read_text()
        for forbidden in ("aws_access_key_id", "aws_secret_access_key", "airbyte/", "slurp", "b64decode"):
            self.assertNotIn(forbidden, text)
        for task in self.tasks()["record_client_files.yml"]:
            self.assertNotIn("ansible.builtin.slurp", task)

    def test_the_files_are_owned_by_the_container_user_with_mode_0400(self) -> None:
        place = next(task for task in self.tasks()["record_client_files.yml"] if task.get("name", "").startswith("Place the four files"))
        copy = place["ansible.builtin.copy"]
        self.assertIn("airbyte_record_container_user", copy["owner"])
        self.assertIn("airbyte_record_container_user", copy["group"])
        self.assertEqual("0400", copy["mode"])

    def test_the_deploy_checks_the_api_address_with_the_codes_own_check_and_holds_no_second_list_of_ranges(self) -> None:
        tasks = self.tasks()["record_client_files.yml"]
        checks = [task for task in tasks if "ansible.builtin.command" in task and "scripts.ingestion.airbyte_api" in json.dumps(task)]
        self.assertEqual(1, len(checks))
        argv = checks[0]["ansible.builtin.command"]["argv"]
        self.assertEqual(["check", "--address"], argv[3:5])
        self.assertEqual("http://{{ airbyte_bridge_address }}:{{ airbyte_api_route_port | default(8001) }}", argv[5])
        self.assertEqual("localhost", checks[0]["delegate_to"])
        text = (ROLE_TASKS / "record_client_files.yml").read_text()
        for range_text in ("169", "172\\.", "192\\.168", "match('^(10"):
            self.assertNotIn(range_text, text, "the allowed ranges are defined once, in scripts/ingestion/airbyte_api.py")


if __name__ == "__main__":
    unittest.main()
