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

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "airbyte"
GROUP_VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
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
    return {
        "airbyte_directory": str(folder / "airbyte"),
        "airbyte_owner": OWNER,
        "airbyte_group": GROUP,
        "airbyte_database_host": "10.42.0.1",
        "airbyte_database_port": 5432,
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
            code, output = run_tasks(["place_files.yml"], values(folder))
            placed = folder / "airbyte"
            tracked = (placed / "values.yaml").read_text()
            limits = mapping(load_yaml((placed / "limits.values.yaml").read_text()))
            host = mapping(load_yaml((placed / "host.values.yaml").read_text()))
            again_code, again = run_tasks(["place_files.yml"], values(folder))
        self.assertEqual(0, code, output)
        self.assertEqual((ROLE / "files" / "values.yaml").read_text(), tracked)
        self.assertEqual(f"{load_limits().services['airbyte_server'].memory_limit_mib}Mi", limits["server"]["resources"]["limits"]["memory"])
        self.assertEqual({"host": "10.42.0.1", "port": 5432, "name": "airbyte"}, host["global"]["database"])
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
