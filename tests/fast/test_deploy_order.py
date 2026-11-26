"""The order of a clean deploy: every file that a Compose secret of a service names exists before the task that starts that service.

The order is read from the real playbooks, not described: the roles and post tasks of ``ansible/host.yml`` are expanded in the order Ansible runs them, each
role's dependencies first, each task file with the files it includes right after it; then the launcher's step that gives the host's OpenBao the external secrets
(the intake), the tasks of ``ansible/stack.yml``, the launcher's step that places AIStor's licence, the tasks of ``ansible/storage.yml``, the launcher's step
that places the project profile's key and the tasks of ``ansible/connection.yml`` (the launcher runs the steps between the playbooks in ``host_up``, which
``test_host_commands`` holds). A service starts at the first
``start.yml`` that names it, or that names none and so starts every service that is not left out (``compose_stack_start_except``). A secret file is made by the
task file that the tables below name for it or for its folder. On a redeploy a file can be new (a host deployed before the file existed) or replaced:
the stale check reads only the files of the services that a start names, so a file made later in the order cannot stop an earlier start; a named service
whose file is missing is refused by name; a replaced file starts the service again, which ``stale_services`` holds for configs and for file secrets and
``test_compose_stack_start`` holds as the Compose command.
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
STACK = ROOT / "ansible" / "stack.yml"
COMPOSE = ROOT / "compose"
VARIABLES = ROOT / "ansible" / "group_vars" / "all.yml"
PRODUCERS = {
    "postgres": ("compose_stack", "passwords.yml"),
    "grafana": ("compose_stack", "grafana_password.yml"),
    "aistor": ("compose_stack", "aistor_credentials.yml"),
    "k3s": ("airbyte", "group_access.yml"),
    "airbyte_api": ("airbyte", "record_client_files.yml"),
    "openbao": ("openbao", "prepare.yml"),
}
# The two copies of the project profile's key are made by the launcher's step after the host playbook, never by a task, so Ansible never holds the key.
KEY_STEP = ("launcher", "place_project_key")
# The launcher's intake writes the external secrets into the host's OpenBao after the host playbook made OpenBao and its policies, and before the stack playbook
# pulls the images with the Docker Hub token that the intake stored.
INTAKE_STEP = ("launcher", "place_external_secrets")
# AIStor's licence is placed by the launcher too, between the stack playbook and the AIStor step: it reads the licence in the owner's OpenBao and pipes it on.
LICENCE_STEP = ("launcher", "place_aistor_licence")
FILE_PRODUCERS = {"airbyte_api/aws_access_key_id": KEY_STEP, "airbyte_api/aws_secret_access_key": KEY_STEP, "aistor/licence": LICENCE_STEP}
CONNECTION = ROOT / "ansible" / "connection.yml"
STORAGE = ROOT / "ansible" / "storage.yml"
STORAGE_NAME = "compose_stack_storage_services"
DEFERRED = "{{ compose_stack_orchestration_services + " + STORAGE_NAME + " }}"


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
    if "block" in task:
        return [event for inner in task["block"] for event in expand_task(role, inner)]
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


def play_events(path: Path) -> list[Event]:
    play = task_list(path)[0]
    events: list[Event] = []
    for task in play.get("pre_tasks") or []:
        events += expand_task("", task)
    for role in play.get("roles") or []:
        events += expand_role(role if isinstance(role, str) else role["role"])
    for task in play.get("tasks") or []:
        events += expand_task("", task)
    for task in play.get("post_tasks") or []:
        events += expand_task("", task)
    return events


def playbook_events() -> list[Event]:
    """Every playbook with the launcher's steps between them: host, intake, stack, licence, AIStor, key and connection, in the order ``host_up`` runs them."""
    first = [*play_events(HOST), Event(*INTAKE_STEP), *play_events(STACK)]
    return [*first, Event(*LICENCE_STEP), *play_events(STORAGE), Event(*KEY_STEP), *play_events(CONNECTION)]


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


def named_services(event: Event) -> list[str] | None:
    """The services that a start names: a list, or one of the two variables of the services that start apart, or None when it names none."""
    named = event.vars.get("compose_stack_start_services")
    for name in ("compose_stack_orchestration_services", STORAGE_NAME):
        if named == "{{ " + name + " }}":
            named = list(variable(name))
    if named is not None and not isinstance(named, list):
        raise AssertionError("a start must name its services as a list or by one of the two variables, or leave them out")
    return named


def start_positions(events: list[Event]) -> dict[str, int]:
    """The position of the first ``start.yml`` that starts each service."""
    services = set(all_services())
    left_out_name = "compose_stack_orchestration_services"
    first: dict[str, int] = {}
    for position, event in enumerate(events):
        if event.file != "start.yml":
            continue
        named = named_services(event)
        excluded = event.vars.get("compose_stack_start_except")
        left_out = {"{{ " + left_out_name + " }}": set(variable(left_out_name)), DEFERRED: set(variable(left_out_name)) | set(variable(STORAGE_NAME))}.get(
            str(excluded), set()
        )
        if excluded not in (None, "{{ " + left_out_name + " }}", DEFERRED):
            raise AssertionError("a start leaves services out only through the orchestration variable or the orchestration and storage variables")
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
                producer = FILE_PRODUCERS.get(secret_file, PRODUCERS.get(folder))
                if producer is None:
                    problems.append(f"{service}: no task is named for the folder {folder}")
                elif producer not in positions:
                    problems.append(f"{service}: {producer[1]} never runs")
                elif service not in starts:
                    problems.append(f"{service}: nothing starts it")
                elif positions[producer] >= starts[service]:
                    problems.append(f"{service}: {secret_file} is made by {producer[1]} after the service starts")
        self.assertEqual([], problems)

    def test_the_intake_runs_after_openbao_has_its_policies_and_the_intake_script_and_before_the_images_are_pulled_with_the_stored_token(self) -> None:
        events = playbook_events()
        position = {
            key: events.index(next(event for event in events if (event.role, event.file) == key))
            for key in (
                ("openbao", "prepare.yml"),
                ("openbao", "bootstrap.yml"),
                ("openbao", "intake_script.yml"),
                INTAKE_STEP,
                ("compose_stack", "pull_images.yml"),
            )
        }
        openbao_start = next(
            index for index, event in enumerate(events) if event.file == "start.yml" and "openbao" in (event.vars.get("compose_stack_start_services") or [])
        )
        self.assertLess(position[("openbao", "prepare.yml")], openbao_start, "OpenBao reads its admin secret ID file at its first start")
        self.assertLess(openbao_start, position[("openbao", "bootstrap.yml")], "the policies and roles are made on a running server")
        self.assertLess(position[("openbao", "bootstrap.yml")], position[INTAKE_STEP])
        self.assertLess(position[("openbao", "intake_script.yml")], position[INTAKE_STEP])
        self.assertLess(position[INTAKE_STEP], position[("compose_stack", "pull_images.yml")])

    def test_the_grafana_password_is_made_from_the_running_openbao_before_grafana_starts(self) -> None:
        events = playbook_events()
        grafana = next(index for index, event in enumerate(events) if (event.role, event.file) == ("compose_stack", "grafana_password.yml"))
        bootstrap = next(index for index, event in enumerate(events) if (event.role, event.file) == ("openbao", "bootstrap.yml"))
        self.assertGreater(grafana, bootstrap)
        self.assertLess(grafana, start_positions(events)["grafana"])

    def test_the_storage_service_starts_alone_after_the_licence_is_placed_and_checked_and_after_its_credential_files_and_no_other_start_names_it(self) -> None:
        events = playbook_events()
        starts = start_positions(events)
        positions = {(event.role, event.file): index for index, event in reversed(list(enumerate(events)))}
        self.assertEqual(["aistor"], variable(STORAGE_NAME))
        for service in variable(STORAGE_NAME):
            with self.subTest(service):
                self.assertGreater(starts[service], positions[LICENCE_STEP], "the launcher places the licence before the AIStor step starts the server")
                self.assertGreater(starts[service], positions[("compose_stack", "aistor_licence.yml")])
                self.assertGreater(starts[service], positions[("compose_stack", "aistor_credentials.yml")])
                self.assertGreater(
                    starts[service], positions[("compose_stack", "aistor_licence_script.yml")], "the script that places the licence is installed first"
                )
                named = [index for index, event in enumerate(events) if event.file == "start.yml" and service in (named_services(event) or [])]
                self.assertEqual([starts[service]], named, "only the storage step names the service")
        general = [event for event in events if event.file == "start.yml" and not named_services(event)]
        self.assertEqual([DEFERRED], [event.vars.get("compose_stack_start_except") for event in general])

    def test_the_producers_in_the_table_are_the_files_that_write_those_folders(self) -> None:
        for folder, (role, file) in PRODUCERS.items():
            with self.subTest(folder):
                text = (ROLES / role / "tasks" / file).read_text()
                self.assertIn(folder, text)

    def test_the_code_location_and_its_dependants_start_after_the_airbyte_role_made_its_files_and_the_key_step_placed_the_two_copies(self) -> None:
        events = playbook_events()
        starts = start_positions(events)
        made = max(index for index, event in enumerate(events) if (event.role, event.file) == ("airbyte", "record_client_files.yml"))
        key = events.index(Event(*KEY_STEP))
        self.assertLess(made, key, "the Airbyte role makes the folder that the key step writes into")
        for service in variable("compose_stack_orchestration_services"):
            with self.subTest(service):
                self.assertGreater(starts[service], key)
        for service in set(all_services()) - set(variable("compose_stack_orchestration_services")) - set(variable(STORAGE_NAME)):
            with self.subTest(service):
                self.assertLess(starts[service], made, "everything else starts first, so the Airbyte role finds its database and the stack")
        for service in variable(STORAGE_NAME):
            with self.subTest(service):
                self.assertGreater(starts[service], made, "AIStor starts in its own step after the host playbook, and the Airbyte role needs nothing of it")
                self.assertLess(starts[service], key)

    def test_the_aistor_step_is_a_playbook_of_its_own_between_the_host_playbook_and_the_key_step(self) -> None:
        host_files = {(event.role, event.file) for event in play_events(HOST)}
        storage_files = {(event.role, event.file) for event in play_events(STORAGE)}
        for name in ("storage.yml", "aistor_licence.yml", "aistor_project_user.yml"):
            self.assertIn(("compose_stack", name), storage_files)
            self.assertNotIn(("compose_stack", name), host_files)
        self.assertIn(("compose_stack", "aistor_licence_script.yml"), host_files)
        events = playbook_events()
        self.assertLess(events.index(Event(*LICENCE_STEP)), events.index(Event(*KEY_STEP)))
        self.assertLess(max(index for index, event in enumerate(events) if event.file == "aistor_project_user.yml"), events.index(Event(*KEY_STEP)))

    def test_the_orchestration_start_is_a_task_of_the_connection_playbook_and_not_of_the_host_playbook(self) -> None:
        host_files = {(event.role, event.file) for event in play_events(HOST)}
        connection_files = {(event.role, event.file) for event in play_events(CONNECTION)}
        self.assertIn(("compose_stack", "start_orchestration.yml"), connection_files)
        self.assertNotIn(("compose_stack", "start_orchestration.yml"), host_files)

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

    def test_a_replaced_file_that_the_code_location_mounts_is_found_by_the_stale_check_of_the_start_and_no_task_of_the_airbyte_role_decides_it(self) -> None:
        tasks = task_list(ROLES / "compose_stack" / "tasks" / "start_orchestration.yml")
        self.assertEqual(["start.yml", "nessie.yml"], [task["ansible.builtin.include_tasks"] for task in tasks if "ansible.builtin.include_tasks" in task])
        self.assertNotIn("--force-recreate", (ROLES / "compose_stack" / "tasks" / "start_orchestration.yml").read_text())
        self.assertIn("stale_services", (ROLES / "compose_stack" / "tasks" / "start.yml").read_text())
        self.assertIn('"secrets"', (ROLES / "compose_stack" / "library" / "stale_services.py").read_text())
        self.assertNotIn("airbyte_record_files_changed", (ROLES / "airbyte" / "tasks" / "record_client_files.yml").read_text())


if __name__ == "__main__":
    unittest.main()
