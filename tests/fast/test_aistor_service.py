"""The AIStor service file pins the server by digest, keeps its licence and its root credentials in files, and publishes only the local S3 port.

The generic checks of ``test_compose_services`` hold every service to the shared rules. These checks hold what only this service can get wrong: a licence
or a root credential written as a value, a licence that is not mounted read-only from the variable's file, a published port on another address and a health
check that asks another path. ``aistor_problems`` names each defect, and the planted cases prove that each check still looks.
"""

from __future__ import annotations

import copy
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.limits import load_limits
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import group_variables

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
PIN = "quay.io/minio/aistor/minio:RELEASE.2026-09-19T17-05-25Z@sha256:a94e1fe399bd050fe6d6148839597a4995350c0e62c9e83f0cec8b5630ed0a1e"
SECRETS = {
    "aistor_licence": "${AISTOR_LICENCE_FILE:?render the host settings first}",
    "aistor_root_user": "${SECRETS_DIR:?render the host settings first}/aistor/root_user",
    "aistor_root_password": "${SECRETS_DIR:?render the host settings first}/aistor/root_password",
}
FILE_VARIABLES = {"MINIO_ROOT_USER_FILE": "/run/secrets/aistor_root_user", "MINIO_ROOT_PASSWORD_FILE": "/run/secrets/aistor_root_password"}
LICENCE_PATH = "/run/secrets/aistor_licence"
HEALTH_PATH = "/minio/health/live"


def aistor() -> dict[str, Any]:
    document = load_yaml((COMPOSE / "services" / "aistor.yml").read_text())
    if not isinstance(document, dict):
        raise TypeError("aistor.yml is not a mapping")
    return document


def aistor_problems(document: dict[str, Any]) -> list[str]:
    """Each way that the AIStor file departs from the design, as a sentence that names the property."""
    service = document["services"]["aistor"]
    found: list[str] = []
    if service.get("image") != PIN:
        found.append("image: must be the pinned AIStor server release and digest")
    if (document.get("secrets") or {}) != {name: {"file": path} for name, path in SECRETS.items()}:
        found.append("secrets: must be the licence file named by the variable and the two root credential files, and nothing else")
    if sorted(service.get("secrets") or []) != sorted(SECRETS):
        found.append("secrets: the service must read exactly the licence and the two root credential files")
    environment = service.get("environment") or {}
    if {key: value for key, value in environment.items() if key.startswith("MINIO_ROOT_")} != FILE_VARIABLES:
        found.append("environment: the root user and password must be file references and nothing else")
    for key in environment:
        if "LICEN" in key.upper():
            found.append(f"environment: {key} would put the licence in the environment")
    command = [str(part) for part in service.get("command") or []]
    if "--license" not in command or command[command.index("--license") + 1 : command.index("--license") + 2] != [LICENCE_PATH]:
        found.append("command: --license must be followed by the path of the mounted licence file")
    if any(re.search(r"(?i)password|secret|key", part) and not part.startswith("/run/secrets/") for part in command):
        found.append("command: no part may carry a credential")
    if command[:1] != ["server"] or "/data" not in command:
        found.append("command: must start the server on /data")
    ports = service.get("ports") or []
    if len(ports) != 1 or not re.fullmatch(r"127\.0\.0\.1:\$\{AISTOR_HOST_PORT:-\d+\}:9000", str(ports[0])):
        found.append("ports: only the S3 port, bound to 127.0.0.1")
    test = " ".join(str(part) for part in (service.get("healthcheck") or {}).get("test") or [])
    if f"http://127.0.0.1:9000{HEALTH_PATH}" not in test:
        found.append(f"healthcheck: must ask {HEALTH_PATH} on the local port")
    if service.get("volumes") != ["aistor_data:/data"]:
        found.append("volumes: one data volume at /data and nothing else")
    if service.get("cap_drop") != ["ALL"] or service.get("security_opt") != ["no-new-privileges:true"]:
        found.append("security: every capability dropped and no new privileges")
    return found


class AistorFileTest(unittest.TestCase):
    def test_the_real_file_has_no_problem(self) -> None:
        self.assertEqual([], aistor_problems(aistor()))

    def test_the_service_is_in_the_always_on_group_with_a_limit_of_its_own(self) -> None:
        entry = load_limits().services["aistor"]
        self.assertEqual(("compose", "supporting"), (entry.runtime, entry.group))
        self.assertGreaterEqual(entry.memory_limit_mib, 1024)

    def test_the_licence_file_is_the_variable_the_deploy_renders_and_the_role_names_the_same_variable(self) -> None:
        role = (COMPOSE.parent / "ansible" / "roles" / "compose_stack" / "tasks" / "render_environment.yml").read_text()
        self.assertIn("AISTOR_LICENCE_FILE={{ compose_stack_aistor_licence_file }}", role)

    def test_the_licence_file_variable_is_a_file_of_the_secrets_folder_and_the_storage_services_are_aistor_alone(self) -> None:
        variables = group_variables()
        self.assertEqual("{{ compose_stack_secrets_directory }}/aistor/licence", variables["compose_stack_aistor_licence_file"])
        self.assertEqual(["aistor"], variables["compose_stack_storage_services"])

    def test_the_licence_and_the_credentials_are_never_a_value_of_the_file(self) -> None:
        text = (COMPOSE / "services" / "aistor.yml").read_text()
        self.assertNotRegex(text, r"MINIO_LICENSE\b|MINIO_ROOT_(USER|PASSWORD):")
        self.assertNotRegex(text, r"\bexport\b")


class RoleTaskFilesTest(unittest.TestCase):
    """The tasks that touch the credentials and the licence keep their values out of the output and run only for the files of AIStor."""

    TASKS = COMPOSE.parent / "ansible" / "roles" / "compose_stack" / "tasks"

    def tasks(self, name: str) -> list[dict[str, Any]]:
        document = load_yaml((self.TASKS / name).read_text())
        flat: list[dict[str, Any]] = []
        for task in document if isinstance(document, list) else []:
            flat.append(task)
            flat.extend(task.get("block") or [])
        return flat

    def test_the_task_that_makes_a_credential_file_prints_no_value_and_the_licence_file_is_never_made_or_read_into_a_variable(self) -> None:
        made = [task for task in self.tasks("aistor_credentials.yml") if "secret_file" in task]
        self.assertEqual(1, len(made))
        self.assertIs(True, made[0].get("no_log"))
        for task in self.tasks("aistor_licence.yml"):
            with self.subTest(task=task.get("name")):
                self.assertFalse({"secret_file", "ansible.builtin.slurp", "ansible.builtin.copy", "ansible.builtin.lineinfile"} & set(task))
                if "ansible.builtin.file" in task:
                    # The one use of the file module removes the files that an interrupted write left: it can make no licence file.
                    self.assertEqual("absent", task["ansible.builtin.file"]["state"])
                    self.assertEqual("{{ item.path }}", task["ansible.builtin.file"]["path"])
                    self.assertEqual("{{ compose_stack_aistor_licence_leftovers.files }}", task["loop"])

    def test_the_licence_check_looks_only_for_the_temporary_names_of_the_licence_file_and_removes_only_what_it_found(self) -> None:
        found = [task["ansible.builtin.find"] for task in self.tasks("aistor_licence.yml") if "ansible.builtin.find" in task]
        self.assertEqual(1, len(found))
        self.assertIs(True, found[0]["use_regex"])
        self.assertIs(True, found[0]["hidden"])
        self.assertIs(False, found[0]["follow"])
        self.assertEqual("file", found[0]["file_type"])
        self.assertEqual("{{ compose_stack_aistor_licence_file | dirname }}", found[0]["paths"])
        self.assertEqual(r"^\.{{ compose_stack_aistor_licence_file | basename | regex_escape }}\.[0-9a-f]{8}\.new$", found[0]["patterns"])

    def test_the_licence_check_stats_the_file_without_following_a_link_and_the_credentials_check_does_too(self) -> None:
        for name in ("aistor_licence.yml", "verify_aistor_credentials.yml"):
            stats = [task["ansible.builtin.stat"] for task in self.tasks(name) if "ansible.builtin.stat" in task]
            self.assertEqual(1, len(stats), name)
            self.assertIs(False, stats[0]["follow"], name)


class PlantedDefectTest(unittest.TestCase):
    def defect(self, mutate: Any) -> list[str]:
        document = copy.deepcopy(aistor())
        mutate(document, document["services"]["aistor"])
        return aistor_problems(document)

    def test_a_changed_digest_or_tag_is_named(self) -> None:
        for image in (PIN.replace("a94e1fe3", "b94e1fe3"), PIN.replace("2026-09-19T17-05-25Z", "2026-09-19T17-05-26Z"), PIN.split("@")[0]):
            with self.subTest(image=image):
                problems = self.defect(lambda _d, s, image=image: s.update(image=image))
                self.assertEqual(["image: must be the pinned AIStor server release and digest"], problems)

    def test_a_licence_in_the_environment_is_named_whatever_the_variable(self) -> None:
        for key in ("MINIO_LICENSE", "AISTOR_LICENSE", "AISTOR_LICENCE"):
            with self.subTest(key=key):
                problems = self.defect(lambda _d, s, key=key: s["environment"].update({key: "x"}))
                self.assertEqual(1, len(problems), problems)
                self.assertIn("licence in the environment", problems[0])

    def test_a_root_credential_as_a_value_is_named(self) -> None:
        for key in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"):
            with self.subTest(key=key):
                problems = self.defect(lambda _d, s, key=key: s["environment"].update({key: "x"}))
                self.assertEqual(["environment: the root user and password must be file references and nothing else"], problems)

    def test_a_root_credential_file_that_points_elsewhere_is_named(self) -> None:
        problems = self.defect(lambda _d, s: s["environment"].update({"MINIO_ROOT_PASSWORD_FILE": "/etc/passwd"}))
        self.assertEqual(1, len(problems))

    def test_a_licence_given_by_value_or_from_another_path_is_named(self) -> None:
        for command in (
            ["server", "/data", "--license", "abc"],
            ["server", "/data", "--license", "/etc/licence"],
            ["server", "/data"],
            ["server", "/data", "--license"],
        ):
            with self.subTest(command=command):
                problems = self.defect(lambda _d, s, command=command: s.update(command=command))
                self.assertIn("command: --license must be followed by the path of the mounted licence file", problems)

    def test_a_credential_on_the_command_line_or_a_server_that_is_not_on_data_is_named(self) -> None:
        problems = self.defect(lambda _d, s: s.update(command=["server", "/data", "--license", LICENCE_PATH, "--secret-key", "x"]))
        self.assertEqual(["command: no part may carry a credential"], problems)
        problems = self.defect(lambda _d, s: s.update(command=["server", "/srv", "--license", LICENCE_PATH]))
        self.assertEqual(["command: must start the server on /data"], problems)

    def test_a_licence_file_that_a_service_reads_but_the_file_does_not_declare_or_the_reverse_is_named(self) -> None:
        self.assertEqual(1, len(self.defect(lambda _d, s: s["secrets"].remove("aistor_licence"))))
        self.assertEqual(1, len(self.defect(lambda d, _s: d["secrets"].update(aistor_licence={"file": "/etc/licence"}))))
        self.assertEqual(1, len(self.defect(lambda d, _s: d["secrets"].pop("aistor_licence"))))

    def test_a_port_on_every_address_a_second_port_or_the_console_is_named(self) -> None:
        for ports in (
            ["9000:9000"],
            ["0.0.0.0:9000:9000"],
            ["127.0.0.1:${AISTOR_HOST_PORT:-9000}:9000", "127.0.0.1:9001:9001"],
            ["127.0.0.1:${AISTOR_HOST_PORT:-9000}:9001"],
            [],
        ):
            with self.subTest(ports=ports):
                self.assertEqual(["ports: only the S3 port, bound to 127.0.0.1"], self.defect(lambda _d, s, ports=ports: s.update(ports=ports)))

    def test_a_health_check_on_another_path_is_named(self) -> None:
        for path in ("/minio/health/ready", "/", "/minio/health/cluster"):
            with self.subTest(path=path):
                problems = self.defect(lambda _d, s, path=path: s["healthcheck"].update(test=["CMD-SHELL", f"curl --fail http://127.0.0.1:9000{path}"]))
                self.assertEqual([f"healthcheck: must ask {HEALTH_PATH} on the local port"], problems)

    def test_a_second_volume_or_a_host_path_is_named(self) -> None:
        for volumes in (["aistor_data:/data", "/etc:/etc:ro"], ["aistor_data:/srv"], []):
            with self.subTest(volumes=volumes):
                self.assertEqual(["volumes: one data volume at /data and nothing else"], self.defect(lambda _d, s, volumes=volumes: s.update(volumes=volumes)))

    def test_a_capability_kept_or_privileges_gained_is_named(self) -> None:
        self.assertEqual(1, len(self.defect(lambda _d, s: s.pop("cap_drop"))))
        self.assertEqual(1, len(self.defect(lambda _d, s: s.pop("security_opt"))))


if __name__ == "__main__":
    unittest.main()
