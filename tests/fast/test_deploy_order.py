"""The order of a clean deploy: every file that a Compose secret of a service names exists before the task that starts that service.

The order is read from the real playbook, not described: the roles and post tasks of ``ansible/host.yml`` are expanded in the order Ansible runs them, each
role's dependencies first, each task file with the files it includes right after it. A service starts at the first ``start.yml`` that names it, or that
names none and so starts every service that is not left out (``compose_stack_start_except``). A secret file is made by the task file that the table below
names for its folder. A redeploy finds every file there already, so it needs no order of its own; a changed file is the one case that needs the service to
start again, which the last test holds.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
ROLES = ROOT / "ansible" / "roles"
HOST = ROOT / "ansible" / "host.yml"
COMPOSE = ROOT / "compose"
VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
PRODUCERS = {
    "postgres": ("compose_stack", "passwords.yml"),
    "grafana": ("compose_stack", "grafana_password.yml"),
    "k3s": ("airbyte", "group_access.yml"),
    "airbyte_api": ("airbyte", "record_client_files.yml"),
    "openbao": ("openbao", "prepare.yml"),
}


@dataclass
class Event:
    role: str
    file: str
    vars: dict[str, Any] = field(default_factory=dict)


def mapping(document: object) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("expected a mapping")
    return document


def task_list(path: Path) -> list[dict[str, Any]]:
    document = load_yaml(path.read_text())
    return [task for task in document if isinstance(task, dict)] if isinstance(document, list) else []


def expand_file(role: str, file: str, variables: dict[str, Any]) -> list[Event]:
    """The file as an event, then, in the order of its tasks, the files it includes."""
    events = [Event(role, file, variables)]
    for task in task_list(ROLES / role / "tasks" / file):
        events += expand_task(role, task)
    return events


def expand_task(role: str, task: dict[str, Any]) -> list[Event]:
    variables = dict(task.get("vars") or {})
    if "ansible.builtin.include_tasks" in task:
        return expand_file(role, task["ansible.builtin.include_tasks"], variables)
    if "ansible.builtin.include_role" in task:
        included = task["ansible.builtin.include_role"]
        tasks_from = str(included.get("tasks_from", "main"))
        return expand_file(included["name"], tasks_from if tasks_from.endswith(".yml") else f"{tasks_from}.yml", variables)
    return []


def expand_role(role: str) -> list[Event]:
    events: list[Event] = []
    meta = ROLES / role / "meta" / "main.yml"
    if meta.is_file():
        for dependency in mapping(load_yaml(meta.read_text())).get("dependencies") or []:
            events += expand_role(dependency["role"])
    return events + (expand_file(role, "main.yml", {}) if (ROLES / role / "tasks" / "main.yml").is_file() else [])


def playbook_events() -> list[Event]:
    play = task_list(HOST)[0]
    events: list[Event] = []
    for role in play["roles"]:
        events += expand_role(role if isinstance(role, str) else role["role"])
    for task in play.get("post_tasks") or []:
        events += expand_task("", task)
    return events


def all_services() -> dict[str, dict[str, str]]:
    """Each service of the Compose project with the secret files it mounts, as ``folder/name`` of the secrets folder."""
    services: dict[str, dict[str, str]] = {}
    for include in mapping(load_yaml((COMPOSE / "compose.yml").read_text()))["include"]:
        document = mapping(load_yaml((COMPOSE / include).read_text()))
        declared = document.get("secrets") or {}
        for name, service in document["services"].items():
            services[name] = {
                secret: declared[secret]["file"].split("}/", 1)[1]
                for secret in service.get("secrets") or []
                if str(declared[secret]["file"]).startswith("${SECRETS_DIR")
            }
    return services


def variable(name: str) -> Any:
    return mapping(load_yaml(VARIABLES.read_text()))[name]


def start_positions(events: list[Event]) -> dict[str, int]:
    """The position of the first ``start.yml`` that starts each service."""
    services = set(all_services())
    left_out_name = "compose_stack_orchestration_services"
    first: dict[str, int] = {}
    for position, event in enumerate(events):
        if event.file != "start.yml":
            continue
        named = event.vars.get("compose_stack_start_services")
        if named == "{{ " + left_out_name + " }}":
            named = list(variable(left_out_name))
        if named is not None and not isinstance(named, list):
            raise AssertionError("a start must name its services as a list or by the one variable, or leave them out")
        excluded = event.vars.get("compose_stack_start_except")
        left_out = set(variable(left_out_name)) if excluded == "{{ " + left_out_name + " }}" else set()
        if excluded not in (None, "{{ " + left_out_name + " }}"):
            raise AssertionError("a start leaves services out only through the one variable")
        started = set(named) if named else services - left_out
        for service in started:
            first.setdefault(service, position)
    return first


class DeployOrderTest(unittest.TestCase):
    def test_every_secret_file_of_every_service_is_made_before_the_task_that_starts_the_service(self) -> None:
        events = playbook_events()
        starts = start_positions(events)
        positions = {(event.role, event.file): index for index, event in reversed(list(enumerate(events)))}
        problems = []
        for service, secrets in all_services().items():
            for secret_file in secrets.values():
                folder = secret_file.split("/")[0]
                producer = PRODUCERS.get(folder)
                if producer is None:
                    problems.append(f"{service}: no task is named for the folder {folder}")
                elif producer not in positions:
                    problems.append(f"{service}: {producer[1]} never runs")
                elif service not in starts:
                    problems.append(f"{service}: nothing starts it")
                elif positions[producer] >= starts[service]:
                    problems.append(f"{service}: {secret_file} is made by {producer[1]} after the service starts")
        self.assertEqual([], problems)

    def test_the_producers_in_the_table_are_the_files_that_write_those_folders(self) -> None:
        for folder, (role, file) in PRODUCERS.items():
            with self.subTest(folder):
                text = (ROLES / role / "tasks" / file).read_text()
                self.assertIn(folder, text)

    def test_the_code_location_and_its_dependants_start_after_the_airbyte_role_made_the_six_files(self) -> None:
        events = playbook_events()
        starts = start_positions(events)
        made = max(index for index, event in enumerate(events) if (event.role, event.file) == ("airbyte", "record_client_files.yml"))
        for service in variable("compose_stack_orchestration_services"):
            with self.subTest(service):
                self.assertGreater(starts[service], made)
        for service in set(all_services()) - set(variable("compose_stack_orchestration_services")):
            with self.subTest(service):
                self.assertLess(starts[service], made, "everything else starts first, so the Airbyte role finds its database and the stack")

    def test_the_orchestration_services_are_the_code_location_and_every_service_that_waits_for_it(self) -> None:
        services = mapping(load_yaml((COMPOSE / "services" / "dagster_code_location.yml").read_text()))
        self.assertIn("dagster_code_location", services["services"])
        dependants = set()
        for path in sorted((COMPOSE / "services").glob("*.yml")):
            for name, service in mapping(load_yaml(path.read_text()))["services"].items():
                if "dagster_code_location" in (service.get("depends_on") or {}):
                    dependants.add(name)
        self.assertEqual({"dagster_code_location", *dependants}, set(variable("compose_stack_orchestration_services")))
        for path in sorted((COMPOSE / "services").glob("*.yml")):
            for name, service in mapping(load_yaml(path.read_text()))["services"].items():
                if name not in variable("compose_stack_orchestration_services"):
                    self.assertEqual(set(), set(service.get("depends_on") or {}) & set(variable("compose_stack_orchestration_services")), name)

    def test_a_changed_file_recreates_the_code_location_because_a_mounted_file_that_was_replaced_is_not_seen_by_a_running_container(self) -> None:
        text = (ROLES / "compose_stack" / "tasks" / "start_orchestration.yml").read_text()
        self.assertIn("airbyte_record_files_changed", text)
        self.assertIn("--force-recreate", text)
        files = (ROLES / "airbyte" / "tasks" / "record_client_files.yml").read_text()
        self.assertIn("airbyte_record_files_changed", files)


if __name__ == "__main__":
    unittest.main()
