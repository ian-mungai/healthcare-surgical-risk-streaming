"""The role that deploys Airbyte on k3s: its pins, its files and the checks that run without a cluster, through the real ``ansible-playbook``.

Installing Helm, creating the secrets and the release need k3s and are proven by the acceptance test. These cases cover what a clean computer shows:
the pinned downloads, the placed values and their idempotence, the web forwarder's unit, the chart check and the rules that every task of the role keeps.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.k3s_limits import CHART_WORKLOAD_KEYS
from scripts.stack.limits import load_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import ROLES, run_playbook
from tests.fast.test_postgres_accounts import TOOLS_FILE, accounts

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "airbyte"
GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
BRIDGE = "10.42.0.1"
HEX64 = re.compile(r"[0-9a-f]{64}")
CHANGED_NONE = re.compile(r"changed=0\s")
COMMAND_MODULES = ("ansible.builtin.command", "ansible.builtin.shell")


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def group_variables() -> dict[str, Any]:
    return mapping(load_yaml(GROUP_VARIABLES.read_text()))


def flatten(tasks: object) -> list[dict[str, Any]]:
    """Every task of a task list, with the tasks inside blocks, rescues and always sections in file order."""
    found: list[dict[str, Any]] = []
    for task in tasks if isinstance(tasks, list) else []:
        found.append(task)
        for section in ("block", "rescue", "always"):
            found.extend(flatten(task.get(section)))
    return found


def role_tasks() -> dict[str, list[dict[str, Any]]]:
    return {path.name: flatten(load_yaml(path.read_text())) for path in sorted((ROLE / "tasks").glob("*.yml"))}


def values(folder: Path) -> dict[str, object]:
    (folder / "units").mkdir(exist_ok=True)
    return {
        "airbyte_directory": str(folder / "airbyte"),
        "airbyte_owner": OWNER,
        "airbyte_group": GROUP,
        "airbyte_bridge_address": BRIDGE,
        "airbyte_database_port": 5432,
        "compose_stack_postgres_port": 5432,
        "airbyte_postgres_unit_directory": str(folder / "units"),
        "airbyte_postgres_unit": "airbyte-postgres",
        "airbyte_flannel_subnet_file": str(folder / "subnet.env"),
        "airbyte_bridge_deadline_seconds": 2,
        "airbyte_database_name": "airbyte",
        "airbyte_temporal_database": "airbyte_temporal",
        "airbyte_temporal_visibility_database": "airbyte_temporal_visibility",
        "airbyte_staging_size": "7G",
        "airbyte_server_service_port": 8001,
        "airbyte_public_url": "",
        "airbyte_namespace": "airbyte",
        "airbyte_release": "airbyte",
        "airbyte_web_port": 8000,
        "airbyte_web_unit": str(folder / "airbyte-web.service"),
        "airbyte_kubeconfig": "/etc/rancher/k3s/k3s.yaml",
    }


def run_tasks(task_files: list[str], variables: dict[str, object]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        includes = "".join(f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {name}\n" for name in task_files)
        playbook = work / "check.yml"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{includes}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(variables)])
    return result.returncode, result.stdout + result.stderr


class PinsTest(unittest.TestCase):
    def test_each_pinned_hash_is_a_sha256_of_64_hex_characters_and_each_version_is_set(self) -> None:
        variables = group_variables()
        for name in ("helm_archive_sha256", "airbyte_chart_sha256"):
            with self.subTest(name=name):
                self.assertRegex(str(variables[name]), HEX64.pattern)
        self.assertRegex(str(variables["helm_version"]), r"^v\d+\.\d+\.\d+$")
        self.assertRegex(str(variables["airbyte_chart_version"]), r"^\d+\.\d+\.\d+$")

    def test_every_download_of_the_role_names_the_version_and_is_checked_against_a_pinned_hash(self) -> None:
        variables = group_variables()
        downloads = [task for tasks in role_tasks().values() for task in tasks if "ansible.builtin.get_url" in task]
        self.assertEqual(2, len(downloads), "the role downloads the Helm archive and the chart archive")
        for task in downloads:
            module = task["ansible.builtin.get_url"]
            with self.subTest(task=task["name"]):
                match = re.fullmatch(r"sha256:\{\{ (\w+_sha256) \}\}", str(module["checksum"]))
                if match is None:
                    self.fail("the checksum must name a pinned hash variable")
                self.assertRegex(str(variables[match.group(1)]), HEX64.pattern)
                self.assertTrue(re.search(r"\{\{ \w*(version)\w* \}\}", str(module["url"])), "the url must name the pinned version")

    def test_the_chart_is_installed_from_the_pinned_archive_and_never_from_a_repository_index(self) -> None:
        text = "".join(path.read_text() for path in (ROLE / "tasks").glob("*.yml"))
        self.assertNotIn("repo add", text)
        self.assertNotIn("--repo", text)
        self.assertNotIn("--set", text)


class TaskRulesTest(unittest.TestCase):
    def test_every_command_of_the_role_has_a_deadline(self) -> None:
        for name, tasks in role_tasks().items():
            for task in tasks:
                if any(module in task for module in COMMAND_MODULES):
                    with self.subTest(file=name, task=task["name"]):
                        self.assertIn("timeout", task)

    def test_no_secret_value_goes_through_a_command_line_a_literal_or_a_rendered_file(self) -> None:
        text = "".join(path.read_text() for path in (ROLE / "tasks").glob("*.yml"))
        for forbidden in ("--from-literal", "from-file", "stringData", "password=", "PASSWORD="):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, text)

    def test_the_release_takes_its_three_values_files_in_order_and_waits_with_a_deadline(self) -> None:
        release = next(task for task in role_tasks()["release.yml"] if "ansible.builtin.command" in task and "upgrade" in str(task["ansible.builtin.command"]))
        arguments = [str(item) for item in release["ansible.builtin.command"]["argv"]]
        files = [argument for index, argument in enumerate(arguments) if index and arguments[index - 1] == "--values"]
        self.assertEqual(["values.yaml", "limits.values.yaml", "host.values.yaml"], [Path(item).name for item in files])
        self.assertIn("--wait", arguments)
        self.assertIn("--timeout", arguments)
        self.assertIn("timeout", release)


class PlacementTest(unittest.TestCase):
    def test_the_values_are_placed_with_the_limits_and_the_host_settings_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], values(folder) | {"airbyte_bridge_address": "10.42.5.1"})
            placed = folder / "airbyte"
            tracked = (placed / "values.yaml").read_text()
            limits = mapping(load_yaml((placed / "limits.values.yaml").read_text()))
            host = mapping(load_yaml((placed / "host.values.yaml").read_text()))
            again_code, again = run_tasks(["place_files.yml"], values(folder) | {"airbyte_bridge_address": "10.42.5.1"})
        self.assertEqual(0, code, output)
        self.assertEqual((ROLE / "files" / "values.yaml").read_text(), tracked)
        self.assertEqual(f"{load_limits().services['airbyte_server'].memory_limit_mib}Mi", limits["server"]["resources"]["limits"]["memory"])
        self.assertEqual({"host": "10.42.5.1", "port": 5432, "name": "airbyte"}, host["global"]["database"])
        self.assertEqual({"request": "7G", "limit": "7G"}, host["global"]["workloads"]["resources"]["fileTransfer"]["storage"])
        self.assertEqual(8001, host["server"]["service"]["port"])
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)

    def test_temporal_keeps_its_two_databases_apart_and_creates_none(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            host = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
        environment = {item["name"]: item["value"] for item in host["temporal"]["extraEnv"]}
        self.assertEqual({"DBNAME": "airbyte_temporal", "VISIBILITY_DBNAME": "airbyte_temporal_visibility", "SKIP_DB_CREATE": "true"}, environment)

    def test_the_public_url_is_set_only_when_given(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            without = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
            run_tasks(["place_files.yml"], values(folder) | {"airbyte_public_url": "https://airbyte.example.invalid:9446"})
            with_url = mapping(load_yaml((folder / "airbyte" / "host.values.yaml").read_text()))
        self.assertNotIn("airbyteUrl", without["global"])
        self.assertEqual("https://airbyte.example.invalid:9446", with_url["global"]["airbyteUrl"])

    def test_a_changed_limit_reaches_the_placed_file_on_the_next_run(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["place_files.yml"], values(folder))
            (folder / "airbyte" / "limits.values.yaml").write_text("server: {}\n")
            code, output = run_tasks(["place_files.yml"], values(folder))
            restored = mapping(load_yaml((folder / "airbyte" / "limits.values.yaml").read_text()))
        self.assertEqual(0, code, output)
        self.assertIn("resources", restored["server"])


class WebForwarderTest(unittest.TestCase):
    def unit(self, **changes: object) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["web_forward_unit.yml"], values(folder) | changes)
            written = Path(str(values(folder)["airbyte_web_unit"]))
            text = written.read_text() if written.exists() else ""
        return code, output, text

    def test_the_unit_forwards_the_server_service_to_the_local_address_only_and_restarts(self) -> None:
        code, output, text = self.unit()
        self.assertEqual(0, code, output)
        command = next(line for line in text.splitlines() if line.startswith("ExecStart="))
        self.assertIn("--address 127.0.0.1", command)
        self.assertIn("service/airbyte-airbyte-server-svc 8000:8001", command)
        self.assertIn("--namespace airbyte", command)
        self.assertIn("Restart=always", text)
        self.assertNotRegex(text, r"--address\s+(?!127\.0\.0\.1)")

    def test_the_unit_follows_the_namespace_release_and_ports_it_is_given(self) -> None:
        _, _, text = self.unit(airbyte_namespace="other", airbyte_release="named", airbyte_web_port=8123, airbyte_server_service_port=9001)
        command = next(line for line in text.splitlines() if line.startswith("ExecStart="))
        self.assertIn("--namespace other", command)
        self.assertIn("service/named-airbyte-server-svc 8123:9001", command)

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["web_forward_unit.yml"], values(folder))
            code, output = run_tasks(["web_forward_unit.yml"], values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


def write_subnet(folder: Path, text: str) -> None:
    (folder / "subnet.env").write_text(text)


class BridgeAddressTest(unittest.TestCase):
    """The pods reach PostgreSQL at the k3s bridge's address, which the role reads from k3s and never types."""

    def read(self, text: str | None, **changes: object) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            if text is not None:
                write_subnet(folder, text)
            variables = {key: value for key, value in values(folder).items() if key != "airbyte_bridge_address"} | changes
            code, output = run_tasks(["bridge_address.yml", "postgres_route_units.yml"], variables)
            socket = folder / "units" / "airbyte-postgres.socket"
            return code, output, socket.read_text() if socket.exists() else ""

    def test_the_address_is_the_first_address_of_the_nodes_subnet_in_flannels_file(self) -> None:
        code, output, socket = self.read("FLANNEL_NETWORK=10.42.0.0/16\nFLANNEL_SUBNET=10.42.0.1/24\nFLANNEL_MTU=8951\nFLANNEL_IPMASQ=true\n")
        self.assertEqual(0, code, output)
        self.assertIn(f"ListenStream={BRIDGE}:5432", socket)

    def test_another_subnet_gives_another_address(self) -> None:
        _, _, socket = self.read("FLANNEL_NETWORK=10.42.0.0/16\nFLANNEL_SUBNET=10.42.7.1/24\n")
        self.assertIn("ListenStream=10.42.7.1:5432", socket)

    def test_a_missing_file_stops_the_deployment_at_the_deadline_and_names_the_file_and_the_deadline(self) -> None:
        code, output, socket = self.read(None)
        self.assertNotEqual(0, code)
        self.assertIn("subnet.env", output)
        self.assertIn("2 seconds", output)
        self.assertEqual("", socket)

    def test_a_file_with_no_subnet_line_or_a_malformed_one_stops_the_deployment_and_places_no_unit(self) -> None:
        for text in (
            "FLANNEL_NETWORK=10.42.0.0/16\n",
            "FLANNEL_SUBNET=not-an-address/24\n",
            "FLANNEL_SUBNET=10.42.0.1\n",
            "FLANNEL_SUBNET=10.42.0.1/24 extra\n",
        ):
            with self.subTest(text=text):
                code, output, socket = self.read(text)
                self.assertNotEqual(0, code, output)
                self.assertEqual("", socket)

    def test_no_file_of_the_role_types_the_bridge_address(self) -> None:
        for path in [*(ROLE / "tasks").glob("*.yml"), *(ROLE / "templates").glob("*"), ROLE / "files" / "values.yaml", GROUP_VARIABLES]:
            with self.subTest(path=path.name):
                self.assertNotIn("10.42.0.1", path.read_text())


class PostgresRouteTest(unittest.TestCase):
    """The socket-activated forwarder from the bridge address to the server's local address."""

    def units(self, **changes: object) -> tuple[int, str, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["postgres_route_units.yml"], values(folder) | changes)
            socket, service = (folder / "units" / f"airbyte-postgres.{kind}" for kind in ("socket", "service"))
            return code, output, socket.read_text() if socket.exists() else "", service.read_text() if service.exists() else ""

    def test_the_socket_listens_on_the_bridge_address_and_the_port_only_and_may_bind_before_the_bridge_exists(self) -> None:
        code, output, socket, _ = self.units()
        self.assertEqual(0, code, output)
        listens = [line for line in socket.splitlines() if line.startswith("Listen")]
        self.assertEqual([f"ListenStream={BRIDGE}:5432"], listens)
        self.assertIn("FreeBind=true", socket)
        self.assertIn("WantedBy=sockets.target", socket)
        self.assertNotRegex(socket, r"0\.0\.0\.0|\[::\]|ListenStream=\d+\s")

    def test_the_service_forwards_to_the_servers_local_address_with_the_shipped_proxy_and_stays_in_the_host_network(self) -> None:
        _, _, _, service = self.units()
        command = next(line for line in service.splitlines() if line.startswith("ExecStart="))
        self.assertEqual("ExecStart=/usr/lib/systemd/systemd-socket-proxyd 127.0.0.1:5432", command)
        self.assertIn("Requires=airbyte-postgres.socket", service)
        self.assertIn("After=airbyte-postgres.socket", service)
        self.assertIn("Type=notify", service)
        self.assertNotIn("PrivateNetwork", service)
        self.assertNotRegex(service, r"ExecStart=.*(0\.0\.0\.0|\b10\.)")

    def test_the_units_follow_the_address_and_the_ports_they_are_given(self) -> None:
        _, _, socket, service = self.units(airbyte_bridge_address="10.42.9.1", airbyte_database_port=6543, compose_stack_postgres_port=6000)
        self.assertIn("ListenStream=10.42.9.1:6543", socket)
        self.assertIn("systemd-socket-proxyd 127.0.0.1:6000", service)

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["postgres_route_units.yml"], values(folder))
            code, output = run_tasks(["postgres_route_units.yml"], values(folder))
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_the_route_is_started_before_the_release_that_needs_it(self) -> None:
        main = (ROLE / "tasks" / "main.yml").read_text()
        for earlier, later in (("bridge_address.yml", "postgres_route.yml"), ("postgres_route.yml", "release.yml"), ("cluster.yml", "network_policy.yml")):
            with self.subTest(earlier=earlier, later=later):
                self.assertLess(main.index(earlier), main.index(later))
        self.assertLess(main.index("network_policy.yml"), main.index("release.yml"))


def policies(text: str) -> list[dict[str, Any]]:
    return [mapping(load_yaml(part)) for part in text.split("\n---\n") if part.strip()]


class NetworkPolicyTest(unittest.TestCase):
    def render(self, others: list[str] | None = None, **changes: object) -> tuple[int, str, list[dict[str, Any]]]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "airbyte").mkdir()
            variables = values(folder) | {"airbyte_other_namespaces": ["kube-system", "default"] if others is None else others} | changes
            code, output = run_tasks(["network_policy_files.yml"], variables)
            written = folder / "airbyte" / "network_policies.yaml"
            return code, output, policies(written.read_text()) if written.exists() else []

    def rule_addresses(self, policy: dict[str, Any]) -> list[tuple[str, list[str], list[tuple[str, int]]]]:
        found = []
        for rule in policy["spec"].get("egress", []):
            ports = [(item["protocol"], int(item["port"])) for item in rule.get("ports", [])]
            for peer in rule.get("to", []):
                block = peer["ipBlock"]
                found.append((block["cidr"], list(block.get("except", [])), ports))
        return found

    def test_airbytes_pods_may_open_connections_to_the_bridge_address_on_the_database_port_and_nowhere_else_on_it(self) -> None:
        code, output, found = self.render()
        self.assertEqual(0, code, output)
        (own,) = [policy for policy in found if policy["metadata"]["namespace"] == "airbyte"]
        self.assertEqual(
            ("NetworkPolicy", "postgres-bridge", {}, ["Egress"]), (own["kind"], own["metadata"]["name"], own["spec"]["podSelector"], own["spec"]["policyTypes"])
        )
        self.assertEqual(
            [(f"{BRIDGE}/32", [], [("TCP", 5432)]), ("0.0.0.0/0", [f"{BRIDGE}/32"], [])],
            self.rule_addresses(own),
        )

    def test_every_other_namespace_may_reach_everything_except_the_bridge_address(self) -> None:
        _, _, found = self.render(["kube-system", "default"])
        others = {policy["metadata"]["namespace"]: policy for policy in found if policy["metadata"]["namespace"] != "airbyte"}
        self.assertEqual({"kube-system", "default"}, set(others))
        for namespace, policy in others.items():
            with self.subTest(namespace=namespace):
                self.assertEqual(
                    ("deny-postgres-bridge", {}, ["Egress"]), (policy["metadata"]["name"], policy["spec"]["podSelector"], policy["spec"]["policyTypes"])
                )
                self.assertEqual([("0.0.0.0/0", [f"{BRIDGE}/32"], [])], self.rule_addresses(policy))

    def test_no_rule_gives_a_path_to_the_bridge_address_except_the_one_for_airbytes_own_namespace(self) -> None:
        _, _, found = self.render(["a", "b", "c"])
        granting = [policy["metadata"]["namespace"] for policy in found for cidr, _, _ in self.rule_addresses(policy) if cidr == f"{BRIDGE}/32"]
        self.assertEqual(["airbyte"], granting)

    def test_the_airbyte_namespace_is_never_given_the_denying_policy_even_when_listed(self) -> None:
        _, _, found = self.render(["airbyte", "other"])
        names = sorted((policy["metadata"]["namespace"], policy["metadata"]["name"]) for policy in found)
        self.assertEqual([("airbyte", "postgres-bridge"), ("other", "deny-postgres-bridge")], names)

    def test_the_policies_follow_the_address_and_the_port_they_are_given(self) -> None:
        _, _, found = self.render(["other"], airbyte_bridge_address="10.42.9.1", airbyte_database_port=6543)
        (own,) = [policy for policy in found if policy["metadata"]["namespace"] == "airbyte"]
        self.assertEqual(("10.42.9.1/32", [], [("TCP", 6543)]), self.rule_addresses(own)[0])

    def test_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "airbyte").mkdir()
            variables = values(folder) | {"airbyte_other_namespaces": ["kube-system"]}
            run_tasks(["network_policy_files.yml"], variables)
            code, output = run_tasks(["network_policy_files.yml"], variables)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)


class TemporalDatabasesTest(unittest.TestCase):
    def test_the_two_databases_that_temporal_is_given_are_owned_by_the_airbyte_tool_in_the_tracked_list(self) -> None:
        owned = accounts.parse_tools(TOOLS_FILE.read_text())["airbyte"]
        variables = group_variables()
        self.assertIn(variables["airbyte_temporal_database"], owned)
        self.assertIn(variables["airbyte_temporal_visibility_database"], owned)
        self.assertIn(variables["airbyte_database_name"], owned)


class ChartCheckTest(unittest.TestCase):
    def chart_defaults(self, extra: dict[str, object] | None = None) -> str:
        enabled = {
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
        chart: dict[str, object] = {key: {"enabled": key in enabled, "resources": {"requests": {}, "limits": {}}} for key in CHART_WORKLOAD_KEYS}
        mapping(chart["temporal"])["adminTools"] = {"resources": {"limits": {}}}
        chart["ingress"] = {"enabled": False}
        chart["global"] = {
            "workloads": {"resources": {kind: {"cpu": {}, "memory": {}} for kind in ("mainContainer", "check", "discover", "replication", "sidecar")}}
        }
        return json.dumps(chart | (extra or {}))

    def verdict(self, text: str) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            return run_tasks(["verify_chart.yml"], values(Path(scratch)) | {"airbyte_chart_defaults": {"stdout": text}})

    def test_the_chart_that_matches_the_tracked_values_and_the_limits_passes(self) -> None:
        code, output = self.verdict(self.chart_defaults())
        self.assertEqual(0, code, output)

    def test_a_workload_that_a_newer_chart_adds_stops_the_deployment_and_is_named(self) -> None:
        code, output = self.verdict(self.chart_defaults({"telemetryCollector": {"enabled": True}}))
        self.assertNotEqual(0, code)
        self.assertIn("telemetryCollector", output)


class PlaybookTest(unittest.TestCase):
    def test_the_role_is_added_after_the_compose_stack_in_the_playbook(self) -> None:
        playbook = load_yaml((ROOT / "ansible" / "host.yml").read_text())
        if not isinstance(playbook, list):
            self.fail("the playbook must be a list")
        roles = [str(role) for role in playbook[0]["roles"]]
        self.assertEqual("airbyte", roles[-1])
        self.assertLess(roles.index("compose_stack"), roles.index("airbyte"))


if __name__ == "__main__":
    unittest.main()
