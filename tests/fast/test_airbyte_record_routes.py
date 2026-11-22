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
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml
from tests.fast.test_airbyte_role import BRIDGE, CHANGED_NONE, ROLE, flatten, run_tasks, values

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

    def test_the_four_files_are_owned_by_the_container_user_with_mode_0400_in_a_folder_only_root_enters(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            code, output = self.write(folder)
            found = self.files(folder)
            mode = stat.S_IMODE((folder / "secrets" / "airbyte_api").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual({"client_id", "client_secret", "aws_access_key_id", "aws_secret_access_key"}, set(found))
        self.assertEqual(
            {"client_id": CLIENT_ID, "client_secret": CLIENT_VALUE, "aws_access_key_id": KEY_ID, "aws_secret_access_key": KEY_VALUE},
            {name: text.rstrip("\n") for name, (text, _, _) in found.items()},
        )
        self.assertEqual({0o400}, {file_mode for _, file_mode, _ in found.values()})
        self.assertEqual({int(USER)}, {owner for _, _, owner in found.values()})
        self.assertEqual(0o700, mode)

    def test_no_secret_value_appears_in_the_playbook_output(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            _, output = self.write(folder)
        for value in (CLIENT_ID, CLIENT_VALUE, KEY_ID, KEY_VALUE):
            for form in (value, base64.b64encode(value.encode()).decode(), base64.b64encode((value + "\n").encode()).decode()):
                self.assertNotIn(form, output)

    def test_a_second_run_changes_nothing_and_a_rotated_value_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            place_aws_files(folder)
            self.write(folder)
            code, output = self.write(folder)
            (folder / "secrets" / "airbyte" / "aws_secret_access_key").write_text("rotated-value-4\n")
            self.write(folder)
            rotated = self.files(folder)["aws_secret_access_key"][0].rstrip("\n")
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertEqual("rotated-value-4", rotated)

    def test_a_missing_or_empty_source_file_stops_the_run_and_names_its_role_and_no_value(self) -> None:
        for name in ("aws_access_key_id", "aws_secret_access_key"):
            for change in ("missing", "empty"):
                with self.subTest(name=name, change=change), tempfile.TemporaryDirectory() as scratch:
                    folder = Path(scratch)
                    place_aws_files(folder)
                    source = folder / "secrets" / "airbyte" / name
                    source.unlink() if change == "missing" else source.write_text("\n")
                    code, output = self.write(folder)
                    self.assertNotEqual(0, code, output)
                    self.assertIn(name, output)
                    for value in (KEY_ID, KEY_VALUE, CLIENT_VALUE):
                        self.assertNotIn(value, output)
                    self.assertEqual({}, self.files(folder), "no file is placed when a source is missing")

    def test_an_empty_client_credential_stops_the_run(self) -> None:
        for name in ("airbyte_record_client_id", "airbyte_record_client_secret"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                place_aws_files(folder)
                code, output = self.write(folder, **{name: ""})
                self.assertNotEqual(0, code, output)
                self.assertEqual({}, self.files(folder))


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

    def test_the_files_are_owned_by_the_container_user_with_mode_0400(self) -> None:
        place = next(task for task in self.tasks()["record_client_files.yml"] if task.get("name", "").startswith("Place the four files"))
        copy = place["ansible.builtin.copy"]
        self.assertIn("airbyte_record_container_user", copy["owner"])
        self.assertIn("airbyte_record_container_user", copy["group"])
        self.assertEqual("0400", copy["mode"])


if __name__ == "__main__":
    unittest.main()
