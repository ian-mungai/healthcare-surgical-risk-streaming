"""The role that deploys the Compose stack: what its task files do to files, through the real ``ansible-playbook`` on this computer.

Starting the stack and the accounts step against a server need Docker and are proven by the acceptance test. These cases cover the file work:
the placed Compose files, the rendered limits and host settings, the password files and their check.
"""

from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import re
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.images import pins
from scripts.stack.limits import load_limits, render_environment
from scripts.stack.yaml_text import load_yaml
from tests.slow.ansible_support import ROLES, run_playbook

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "compose_stack"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
UID, GID = os.getuid(), os.getgid()
TOOL_GID = next((group for group in os.getgroups() if group != GID), GID)
CHANGED_NONE = re.compile(r"changed=0\s")


def pin_file(folder: Path) -> Path:
    """A complete pin file of a placeholder repository, because the tracked one is empty until the first push."""
    path = folder / "image_pins.yml"
    repository = "example/project_images"
    entries = "".join(
        f"  {image['name']}:\n    tag: {repository}:{image['name']}-{image['tag'].partition(':')[2]}-0123456789ab\n    digest: {repository}@sha256:{'a' * 64}\n"
        for image in pins.load_images()
    )
    path.write_text("images:\n" + entries)
    return path


def variables(folder: Path) -> dict[str, object]:
    return {
        "compose_stack_pin_file": str(pin_file(folder)),
        "compose_stack_directory": str(folder),
        "compose_stack_compose_directory": str(folder / "compose"),
        "compose_stack_secrets_directory": str(folder / "secrets"),
        "compose_stack_owner": OWNER,
        "compose_stack_group": GROUP,
        "compose_stack_postgres_port": 15432,
        "openbao_port": 18200,
        "unlock_key_file": str(folder / "unlock.env"),
        "secret_files_root_owner": f"{UID}:{GID}",
        "compose_stack_docker_socket": str(folder),
        "compose_stack_platform_commit": "c" * 40,
        "compose_stack_warehouse_location": "s3://example-bucket/warehouse/",
        "workload_access_region": "us-west-2",
        "compose_stack_heavy_memory_mib": 11008,
        "airbyte_namespace": "airbyte",
        "airbyte_release": "airbyte",
        "compose_stack_aistor_licence_file": str(folder / "secrets" / "aistor" / "licence"),
    }


def run_tasks(task_files: list[str], values: dict[str, object]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as scratch:
        work = Path(scratch)
        includes = "".join(f"    - ansible.builtin.include_role:\n        name: {ROLE}\n        tasks_from: {name}\n" for name in task_files)
        playbook = work / "check.yml"
        playbook.write_text(f"- hosts: localhost\n  connection: local\n  gather_facts: false\n  tasks:\n{includes}")
        result = run_playbook(work, playbook, ["-v", "-i", "localhost,", "--extra-vars", json.dumps(values)])
    return result.returncode, result.stdout + result.stderr


class PlacementTest(unittest.TestCase):
    def test_the_compose_files_are_placed_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["place_files.yml"], variables(folder))
            placed = sorted(str(path.relative_to(folder / "compose")) for path in (folder / "compose").rglob("*") if path.is_file())
            again_code, again = run_tasks(["place_files.yml"], variables(folder))
            tracked = sorted(str(path.relative_to(ROOT / "compose")) for path in (ROOT / "compose").rglob("*") if path.is_file())
        self.assertEqual(0, code, output)
        self.assertEqual(tracked, placed)
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)


class RenderTest(unittest.TestCase):
    def test_the_limits_file_is_rendered_once_from_the_tracked_limits_and_a_second_run_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "compose").mkdir()
            code, output = run_tasks(["render_environment.yml"], variables(folder))
            limits = (folder / "compose" / "limits.env").read_text()
            host = (folder / "compose" / "host.env").read_text()
            heavy = (folder / "compose" / "heavy.env").read_text()
            heavy_mode = (folder / "compose" / "heavy.env").stat().st_mode
            socket_gid = folder.stat().st_gid
            again_code, again = run_tasks(["render_environment.yml"], variables(folder))
        self.assertEqual(0, code, output)
        self.assertEqual(render_environment(load_limits()), limits)
        self.assertEqual(
            f"SECRETS_DIR={folder / 'secrets'}\nPOSTGRES_HOST_PORT=15432\nOPENBAO_HOST_PORT=18200\nOPENBAO_UNLOCK_KEY_FILE={folder / 'unlock.env'}\n"
            f"DOCKER_SOCKET_GID={socket_gid}\nSTACK_K3S_NAMESPACE=airbyte\nSTACK_K3S_RELEASE=airbyte\nPLATFORM_COMMIT={'c' * 40}\n"
            "WAREHOUSE_LOCATION=s3://example-bucket/warehouse/\nAWS_REGION=us-west-2\n"
            f"AISTOR_LICENCE_FILE={folder / 'secrets' / 'aistor' / 'licence'}\n"
            + "".join(f"IMAGE_{name}=example/project_images@sha256:{'a' * 64}\n" for name in ("DAGSTER", "REPLAY", "SPARK")),
            host,
        )
        self.assertEqual("HEAVY_MEMORY_MIB=11008\n", heavy)
        self.assertEqual(0o644, stat.S_IMODE(heavy_mode))
        self.assertEqual(0, again_code, again)
        self.assertRegex(again, CHANGED_NONE)

    def test_the_socket_group_that_containers_see_is_taken_when_it_is_given(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "compose").mkdir()
            code, output = run_tasks(["render_environment.yml"], variables(folder) | {"compose_stack_docker_socket_gid": "991"})
            host = (folder / "compose" / "host.env").read_text()
        self.assertEqual(0, code, output)
        self.assertIn("\nDOCKER_SOCKET_GID=991\n", host)

    def test_a_changed_heavy_figure_is_written_to_the_heavy_file_alone(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "compose").mkdir()
            run_tasks(["render_environment.yml"], variables(folder))
            host_before = (folder / "compose" / "host.env").read_text()
            code, output = run_tasks(["render_environment.yml"], variables(folder) | {"compose_stack_heavy_memory_mib": 9984})
            heavy = (folder / "compose" / "heavy.env").read_text()
            host_after = (folder / "compose" / "host.env").read_text()
        self.assertEqual(0, code, output)
        self.assertEqual("HEAVY_MEMORY_MIB=9984\n", heavy)
        self.assertEqual(host_before, host_after)
        self.assertNotIn("HEAVY_MEMORY_MIB", host_after)

    def test_a_missing_socket_stops_the_step_with_the_socket_named_and_writes_no_host_settings(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "compose").mkdir()
            code, output = run_tasks(["render_environment.yml"], variables(folder) | {"compose_stack_docker_socket": str(folder / "no.sock")})
            written = (folder / "compose" / "host.env").exists()
        self.assertNotEqual(0, code)
        self.assertIn(str(folder / "no.sock"), output)
        self.assertFalse(written)

    def test_a_deploy_with_no_commit_warehouse_location_or_heavy_figure_stops_before_it_writes_the_host_settings(self) -> None:
        for missing in ("compose_stack_platform_commit", "compose_stack_warehouse_location", "compose_stack_heavy_memory_mib"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                (folder / "compose").mkdir()
                values = {name: value for name, value in variables(folder).items() if name != missing}
                code, output = run_tasks(["render_environment.yml"], values)
                written = [name for name in ("host.env", "heavy.env") if (folder / "compose" / name).exists()]
            self.assertNotEqual(0, code)
            self.assertIn(missing, output)
            # The heavy file is written first: a deploy with no commit leaves it, valid, beside no host settings, and a deploy with no figure writes neither.
            self.assertEqual([] if missing == "compose_stack_heavy_memory_mib" else ["heavy.env"], written)


def service_list(*, tool_group: int) -> str:
    """The service list: nessie runs as a user of its own, airbyte has none yet."""
    return f"nessie {UID}:{tool_group}\nairbyte\n"


def step_order(file: str = "main.yml") -> list[str]:
    """The steps of a task file of the role in the order they run: a task file by name (with its services), or another role's file as ``role/file``."""
    tasks: Any = load_yaml((ROLE / "tasks" / file).read_text())
    steps: list[str] = []
    for task in tasks:
        if "ansible.builtin.include_tasks" in task:
            services = (task.get("vars") or {}).get("compose_stack_start_services")
            named = services if isinstance(services, str) else ",".join(services or []) or "all"
            steps.append(task["ansible.builtin.include_tasks"] + (f" {named}" if services is not None else ""))
        else:
            role = task["ansible.builtin.include_role"]
            steps.append(f"{role['name']}/{role['tasks_from']}")
    return steps


class StepOrderTest(unittest.TestCase):
    def test_the_host_playbook_part_prepares_openbao_starts_it_alone_makes_it_match_the_lists_and_installs_the_intake_script(self) -> None:
        self.assertEqual(
            [
                "place_files.yml",
                "caddy_host.yml",
                "render_environment.yml",
                "passwords.yml",
                "verify_secrets.yml",
                "aistor_credentials.yml",
                "aistor_licence_script.yml",
                "openbao/prepare.yml",
                "start.yml openbao",
                "openbao/bootstrap.yml",
                "openbao/intake_script.yml",
            ],
            step_order(),
        )

    def test_the_stack_playbook_part_makes_the_grafana_password_pulls_with_the_stored_token_starts_postgresql_first_and_then_the_rest(self) -> None:
        self.assertEqual(
            [
                "grafana_password.yml",
                "pull_images.yml",
                "airbyte/cluster.yml",
                "airbyte/group_access.yml",
                "start.yml postgres",
                "provision.yml",
                "start.yml all",
                "kafka.yml",
                "registry.yml",
            ],
            step_order("deploy.yml"),
        )

    def test_the_k3s_access_files_exist_before_the_first_start_so_a_fresh_host_deploys_in_one_pass(self) -> None:
        """The code location reads the k3s token and certificate authority as Compose secrets, which the Airbyte role's access step writes."""
        steps = step_order("deploy.yml")
        first_start = next(index for index, step in enumerate(steps) if step.startswith("start.yml"))
        self.assertLess(steps.index("airbyte/cluster.yml"), steps.index("airbyte/group_access.yml"))
        self.assertLess(steps.index("airbyte/group_access.yml"), first_start)
        document: Any = load_yaml((ROOT / "compose" / "services" / "dagster_code_location.yml").read_text())
        secrets = document["secrets"]
        self.assertEqual({"k3s/dagster.token", "k3s/ca.crt"}, {secrets[name]["file"].split("}/", 1)[1] for name in ("k3s_token", "k3s_ca")})


class OrchestrationStartTest(unittest.TestCase):
    def test_the_orchestration_start_starts_those_services_then_gives_nessie_its_first_commit(self) -> None:
        tasks: Any = load_yaml((ROLE / "tasks" / "start_orchestration.yml").read_text())
        files = [task["ansible.builtin.include_tasks"] for task in tasks if "ansible.builtin.include_tasks" in task]
        self.assertEqual(["start.yml", "nessie.yml"], files)
        self.assertEqual("{{ compose_stack_orchestration_services }}", tasks[0]["vars"]["compose_stack_start_services"])

    def test_the_host_playbook_does_not_start_them_and_the_connection_playbook_does_after_the_raw_connection(self) -> None:
        host_document: Any = load_yaml((ROOT / "ansible" / "host.yml").read_text())
        host = host_document[0]
        self.assertEqual("compose_stack", host["roles"][-1])
        stack_document: Any = load_yaml((ROOT / "ansible" / "stack.yml").read_text())
        stack = stack_document[0]
        stack_roles = [task["ansible.builtin.include_role"]["name"] for task in stack["tasks"]]
        self.assertEqual(["compose_stack", "airbyte"], stack_roles)
        for play in (host, stack):
            self.assertNotIn("post_tasks", play, "the key that the code location mounts is placed after these playbooks")
        connection_document: Any = load_yaml((ROOT / "ansible" / "connection.yml").read_text())
        connection = connection_document[0]
        names = [task["ansible.builtin.include_role"]["name"] + "/" + task["ansible.builtin.include_role"]["tasks_from"] for task in connection["tasks"]]
        self.assertEqual(["airbyte/connection.yml", "compose_stack/start_orchestration.yml"], names)


class PasswordFilesTest(unittest.TestCase):
    def create(self, folder: Path, tools: str = "nessie\nairbyte\n", services: str | None = None) -> tuple[int, str]:
        tools_file = folder / "tools.txt"
        tools_file.write_text(tools)
        services_file = folder / "services.txt"
        services_file.write_text(service_list(tool_group=TOOL_GID) if services is None else services)
        values = variables(folder) | {"compose_stack_tools_file": str(tools_file), "compose_stack_services_file": str(services_file)}
        return run_tasks(["passwords.yml", "verify_secrets.yml"], values)

    def test_the_server_gets_a_password_file_for_root_and_each_tool_one_for_the_user_of_its_service_and_nothing_prints_a_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.create(folder)
            files = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
            status = {path.name: path.stat() for path in (folder / "secrets" / "postgres").iterdir()}
            folder_mode = stat.S_IMODE((folder / "secrets" / "postgres").stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertEqual(["airbyte.password", "nessie.password", "postgres.password"], sorted(files))
        self.assertEqual({0o400}, {stat.S_IMODE(item.st_mode) for item in status.values()})
        self.assertEqual(
            {"airbyte.password": (UID, GID), "nessie.password": (UID, TOOL_GID), "postgres.password": (UID, GID)},
            {name: (item.st_uid, item.st_gid) for name, item in status.items()},
        )
        self.assertEqual(0o700, folder_mode)
        for text in files.values():
            self.assertGreaterEqual(len(text), 40)
            self.assertNotIn(text, output)

    def test_a_second_run_changes_nothing_and_keeps_every_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.create(folder)
            before = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
            code, output = self.create(folder)
            after = {path.name: path.read_text() for path in (folder / "secrets" / "postgres").iterdir()}
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_file_that_an_earlier_deploy_made_for_root_only_is_given_to_its_user_with_its_password_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.create(folder, services=service_list(tool_group=GID))
            target = folder / "secrets" / "postgres" / "nessie.password"
            target.chmod(0o600)
            before = target.read_text()
            code, output = self.create(folder)
            status = target.stat()
            after = target.read_text()
        self.assertEqual(0, code, output)
        self.assertNotRegex(output, CHANGED_NONE)
        self.assertEqual((0o400, UID, TOOL_GID), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))
        self.assertEqual(before, after)

    def test_a_tool_added_later_gets_its_own_password_and_the_others_are_kept(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            self.create(folder)
            before = (folder / "secrets" / "postgres" / "nessie.password").read_text()
            code, output = self.create(folder, "nessie\nairbyte\nextra\n", service_list(tool_group=TOOL_GID) + "extra\n")
            after = (folder / "secrets" / "postgres" / "nessie.password").read_text()
            added = (folder / "secrets" / "postgres" / "extra.password").is_file()
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertTrue(added)

    def test_an_invalid_tool_name_stops_the_step_before_any_file_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.create(folder, "nessie\nbad-name\n")
            written = (folder / "secrets").exists()
        self.assertNotEqual(0, code)
        self.assertIn("tool name", output)
        self.assertFalse(written)

    def test_a_tool_with_no_line_in_the_service_list_stops_the_step_before_any_file_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = self.create(folder, "nessie\nextra\n")
            written = (folder / "secrets").exists()
        self.assertNotEqual(0, code)
        self.assertIn("tool extra has no line in the service list", output)
        self.assertFalse(written)


class GrafanaPasswordTest(unittest.TestCase):
    """Grafana's administrator password is made once, kept in OpenBao and written from there to a file of its own container user, mode 0400."""

    def values(self, folder: Path, owner: tuple[int, int]) -> dict[str, object]:
        return variables(folder) | {"grafana_user": f"{owner[0]}:{owner[1]}"}

    def test_the_password_is_kept_in_openbao_and_written_from_there_to_the_file_of_the_grafana_user_and_then_checked(self) -> None:
        """What the step does to OpenBao and the file is proven by ``tests/fast/test_openbao_secret_steps.py`` against a stand-in server."""
        tasks: Any = load_yaml((ROLE / "tasks" / "grafana_password.yml").read_text())
        include = tasks[0]["ansible.builtin.include_role"]
        self.assertEqual(("openbao", "generated_secret.yml"), (include["name"], include["tasks_from"]))
        self.assertEqual(
            {
                "openbao_generated_name": "grafana/admin_password",
                "openbao_generated_dest": "{{ compose_stack_secrets_directory }}/grafana/admin.password",
                "openbao_generated_owner": "{{ grafana_user }}",
            },
            tasks[0]["vars"],
        )
        self.assertEqual("verify_grafana_password.yml", tasks[1]["ansible.builtin.include_tasks"])

    def check(self, *, mode: int = 0o400, group: int = TOOL_GID, expected: tuple[int, int] = (UID, TOOL_GID), create: bool = True) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "grafana" / "admin.password"
            target.parent.mkdir(parents=True, mode=0o700)
            if create:
                target.write_text("a-long-random-value")
                os.chown(target, UID, group)
                target.chmod(mode)
            return run_tasks(["verify_grafana_password.yml"], self.values(folder, expected))

    def test_the_check_passes_a_private_file_of_the_grafana_user_and_refuses_every_other_file_by_name(self) -> None:
        code, output = self.check()
        self.assertEqual(0, code, output)
        for label, kwargs in (
            ("mode 0600", {"mode": 0o600}),
            ("mode 0640", {"mode": 0o640}),
            ("mode 0444", {"mode": 0o444}),
            ("mode 0404", {"mode": 0o404}),
            ("another user", {"expected": (UID + 1, TOOL_GID)}),
            ("another group", {"expected": (UID, TOOL_GID + 1)}),
            ("missing", {"create": False}),
        ):
            with self.subTest(label):
                code, output = self.check(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn("The Grafana password file", output)
                self.assertNotIn("a-long-random-value", output)


class AistorCredentialsTest(unittest.TestCase):
    """AIStor's root user and password and the project and audit passwords are four files of root, mode 0400, made once, never printed or replaced."""

    FILES = ("root_user", "root_password", "project_password", "audit_password")

    def values(self, folder: Path) -> dict[str, object]:
        return variables(folder)

    def test_each_file_is_made_for_root_with_mode_0400_in_a_folder_for_root_and_no_output_holds_a_value(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["aistor_credentials.yml"], self.values(folder))
            made = {name: (folder / "secrets" / "aistor" / name) for name in self.FILES}
            statuses = {name: path.stat() for name, path in made.items()}
            texts = {name: path.read_text() for name, path in made.items()}
            folder_status = (folder / "secrets" / "aistor").stat()
        self.assertEqual(0, code, output)
        for name in self.FILES:
            with self.subTest(name):
                self.assertEqual((0o400, UID, GID), (stat.S_IMODE(statuses[name].st_mode), statuses[name].st_uid, statuses[name].st_gid))
                self.assertRegex(texts[name], r"^[A-Za-z0-9_-]{40,}$")
                self.assertNotIn(texts[name], output)
        self.assertEqual(len(self.FILES), len(set(texts.values())), "each file has a value of its own")
        self.assertEqual((0o700, UID, GID), (stat.S_IMODE(folder_status.st_mode), folder_status.st_uid, folder_status.st_gid))

    def test_a_second_run_changes_nothing_and_keeps_both_values(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["aistor_credentials.yml"], self.values(folder))
            before = {name: (folder / "secrets" / "aistor" / name).read_text() for name in self.FILES}
            code, output = run_tasks(["aistor_credentials.yml"], self.values(folder))
            after = {name: (folder / "secrets" / "aistor" / name).read_text() for name in self.FILES}
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_file_with_another_mode_is_given_mode_0400_with_its_value_kept(self) -> None:
        for mode in (0o600, 0o640, 0o444):
            with self.subTest(mode=oct(mode)), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                run_tasks(["aistor_credentials.yml"], self.values(folder))
                target = folder / "secrets" / "aistor" / "root_password"
                value = target.read_text()
                target.chmod(mode)
                code, output = run_tasks(["aistor_credentials.yml"], self.values(folder))
                self.assertEqual(0, code, output)
                self.assertEqual((0o400, value), (stat.S_IMODE(target.stat().st_mode), target.read_text()))

    def refused(self, content: str) -> tuple[int, str, str]:
        """Make the files, put ``content`` in root_user, run the task again and give the exit code, Ansible's output and the file's text afterwards."""
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["aistor_credentials.yml"], self.values(folder))
            target = folder / "secrets" / "aistor" / "root_user"
            target.chmod(0o600)
            target.write_text(content)
            target.chmod(0o400)
            code, output = run_tasks(["aistor_credentials.yml"], self.values(folder))
            return code, output, target.read_text()

    def test_an_empty_credential_file_is_refused_by_name_under_no_log_and_is_never_filled_in(self) -> None:
        code, output, text = self.refused("")
        self.assertNotEqual(0, code)
        self.assertIn("the secret file root_user is empty", output)
        self.assertEqual("", text)
        # The task is hidden (no_log): each result of the loop is censored, so the only text about the file is the module's fixed message.
        self.assertIn("the output has been hidden due to the fact that 'no_log: true' was specified for this result", output)
        self.assertIn("failed: [localhost] (item=(censored due to no_log))", output)
        self.assertNotIn("item=root_user", output, "the loop's labels are hidden with the results")

    def test_a_value_with_whitespace_at_an_edge_is_refused_by_a_fixed_message_under_no_log_and_never_printed(self) -> None:
        for content in ("  planted-value-0123456789  ", "\tplanted-value-0123456789", "planted-value-0123456789 "):
            with self.subTest(content=content):
                code, output, text = self.refused(content)
                self.assertNotEqual(0, code)
                self.assertIn("has whitespace at its start or its end", output)
                self.assertNotIn("planted-value-0123456789", output)
                self.assertEqual(content, text, "the value is kept as it was")
                self.assertIn("the output has been hidden due to the fact that 'no_log: true' was specified for this result", output)


class AistorCredentialVerifyTest(unittest.TestCase):
    """The check after the files are made passes a plain file of root with content and mode 0400 and refuses every other file by a message that names it."""

    def check(
        self, *, mode: int = 0o400, expected: tuple[int, int] = (UID, GID), create: bool = True, content: str = "a-long-random-value", link: bool = False
    ) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "aistor"
            target.mkdir(parents=True, mode=0o700)
            for name in ("root_user", "root_password", "project_password", "audit_password"):
                if not create and name == "root_password":
                    continue
                if link and name == "root_password":
                    real = folder / "real"
                    real.write_text(content)
                    real.chmod(0o400)
                    (target / name).symlink_to(real)
                    continue
                (target / name).write_text(content if name == "root_password" else "a-long-random-value")
                (target / name).chmod(mode if name == "root_password" else 0o400)
            return run_tasks(["verify_aistor_credentials.yml"], variables(folder) | {"secret_files_root_owner": f"{expected[0]}:{expected[1]}"})

    def test_a_private_plain_file_of_root_with_content_passes_and_every_other_file_is_refused_by_name_with_no_value_printed(self) -> None:
        code, output = self.check()
        self.assertEqual(0, code, output)
        for label, kwargs in (
            ("mode 0600", {"mode": 0o600}),
            ("mode 0440", {"mode": 0o440}),
            ("mode 0444", {"mode": 0o444}),
            ("mode 0404", {"mode": 0o404}),
            ("another user", {"expected": (UID + 1, GID)}),
            ("another group", {"expected": (UID, GID + 1)}),
            ("missing", {"create": False}),
            ("empty", {"content": ""}),
            ("a link", {"link": True}),
        ):
            with self.subTest(label):
                code, output = self.check(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn("The AIStor credential file root_password", output)
                self.assertNotIn("a-long-random-value", output)


class AistorStatOutputTest(unittest.TestCase):
    """A stat is printed by a verbose run (the slow runner's): for a credential or the licence file it must hold no checksum and no content type."""

    def test_the_verbose_output_of_the_checks_holds_no_checksum_and_no_mime_type(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "aistor"
            target.mkdir(parents=True, mode=0o700)
            for name in ("root_user", "root_password", "project_password", "audit_password", "licence"):
                (target / name).write_text("planted-value-0123456789")
                (target / name).chmod(0o400)
            digest = hashlib.sha1(b"planted-value-0123456789", usedforsecurity=False).hexdigest()
            for task_file in ("verify_aistor_credentials.yml", "aistor_licence.yml"):
                with self.subTest(task_file=task_file):
                    code, output = run_tasks([task_file], variables(folder))
                    self.assertEqual(0, code, output)
                    self.assertNotIn(digest, output)
                    self.assertNotIn("checksum", output)
                    self.assertNotIn("mimetype", output)
                    self.assertNotIn("planted-value-0123456789", output)


class AistorLicenceTest(unittest.TestCase):
    """The licence check refuses a file that is there and empty, blank, not plain or not root's alone, by one fixed message; a missing file is no failure."""

    MESSAGE = "the AIStor licence file is empty or is not a plain file that only"

    def check(
        self,
        content: str | None,
        *,
        mode: int = 0o400,
        link: bool = False,
        variable: str | None = None,
        unset: bool = False,
        placement: str | None = None,
        leftovers: tuple[str, ...] = (),
    ) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "aistor" / "licence"
            target.parent.mkdir(parents=True, mode=0o700)
            if content is not None:
                if link:
                    real = folder / "real"
                    real.write_text(content)
                    real.chmod(0o400)
                    target.symlink_to(real)
                else:
                    target.write_text(content)
                    target.chmod(mode)
            for name in leftovers:
                (target.parent / name).write_text("left-behind-licence-text")
                (target.parent / name).chmod(0o400)
            values = variables(folder)
            if variable is not None:
                values["compose_stack_aistor_licence_file"] = variable
            if unset:
                del values["compose_stack_aistor_licence_file"]
            if placement is not None:
                values["aistor_licence_placement"] = placement
            result = run_tasks(["aistor_licence.yml"], values)
            self.remaining = sorted(entry.name for entry in target.parent.iterdir())
            return result

    def test_a_private_file_with_content_passes_and_nothing_is_changed_or_printed(self) -> None:
        code, output = self.check("a-licence-key-with-some-length\n")
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)
        self.assertNotIn("a-licence-key", output)

    def test_a_missing_file_is_not_a_failure_because_the_aistor_step_skips_aistor_for_it(self) -> None:
        code, output = self.check(None)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_an_empty_or_blank_file_a_link_or_another_mode_is_refused_by_the_fixed_message_and_no_content_is_printed(self) -> None:
        for label, kwargs in (
            ("empty", {"content": ""}),
            ("spaces and line breaks only", {"content": " \n\t\n  \n"}),
            ("mode 0600", {"content": "secret-licence-text", "mode": 0o600}),
            ("mode 0440", {"content": "secret-licence-text", "mode": 0o440}),
            ("mode 0444", {"content": "secret-licence-text", "mode": 0o444}),
            ("a link", {"content": "secret-licence-text", "link": True}),
        ):
            with self.subTest(label):
                code, output = self.check(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn(self.MESSAGE, output)
                self.assertNotIn("secret-licence-text", output)

    def test_a_licence_that_the_launcher_placed_and_that_is_missing_on_the_host_fails_with_a_fixed_message(self) -> None:
        code, output = self.check(None, placement="placed")
        self.assertNotEqual(0, code)
        self.assertIn("the launcher placed the AIStor licence, but the licence file is missing on the host", output)

    def test_a_licence_that_the_launcher_placed_and_is_on_the_host_passes(self) -> None:
        code, output = self.check("a-licence-key-with-some-length\n", placement="placed")
        self.assertEqual(0, code, output)

    def test_no_licence_from_the_launcher_is_no_failure_whether_the_host_has_a_file_or_not(self) -> None:
        for label, content in (("no file", None), ("an older file", "a-licence-key-with-some-length\n")):
            with self.subTest(label):
                code, output = self.check(content, placement="absent")
                self.assertEqual(0, code, output)
                self.assertRegex(output, CHANGED_NONE)

    def test_a_placement_that_is_neither_placed_nor_absent_is_refused_by_a_message_that_names_the_variable(self) -> None:
        for value in ("maybe", "", "PLACED", "unchanged"):
            with self.subTest(value=value):
                code, output = self.check("a-licence-key-with-some-length\n", placement=value)
                self.assertNotEqual(0, code)
                self.assertIn("aistor_licence_placement", output)

    def test_the_second_0400_file_that_an_interrupted_write_left_is_removed_at_every_deploy_and_nothing_else_is_touched(self) -> None:
        left = (".licence.0123abcd.new", ".licence.ffffffff.new")
        kept = (".licence.0123abc.new", "licence.0123abcd.new", ".licence.0123abcd.old", ".other.0123abcd.new")
        code, output = self.check("a-licence-key-with-some-length\n", leftovers=left + kept)
        self.assertEqual(0, code, output)
        self.assertEqual(sorted(["licence", *kept]), self.remaining)
        self.assertNotIn("left-behind-licence-text", output)

    def test_the_leftover_is_removed_even_when_the_licence_is_absent_and_a_clean_folder_changes_nothing(self) -> None:
        code, output = self.check(None, leftovers=(".licence.0123abcd.new",))
        self.assertEqual(0, code, output)
        self.assertEqual([], self.remaining)
        code, output = self.check("a-licence-key-with-some-length\n")
        self.assertRegex(output, CHANGED_NONE)

    def test_an_empty_variable_or_no_variable_is_refused_by_a_message_that_names_the_variable(self) -> None:
        for label, kwargs in (
            ("an empty variable", {"content": "secret-licence-text", "variable": ""}),
            ("no variable", {"content": "secret-licence-text", "unset": True}),
        ):
            with self.subTest(label):
                code, output = self.check(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn("compose_stack_aistor_licence_file", output)
                self.assertNotIn("secret-licence-text", output)

    def test_a_file_that_another_user_owns_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "aistor" / "licence"
            target.parent.mkdir(parents=True, mode=0o700)
            target.write_text("secret-licence-text")
            target.chmod(0o400)
            code, output = run_tasks(["aistor_licence.yml"], variables(folder) | {"compose_stack_owner": "nobody"})
        self.assertNotEqual(0, code)
        self.assertIn(self.MESSAGE, output)


class AistorStepWithoutLicenceTest(unittest.TestCase):
    """With no licence file the AIStor step skips AIStor, records it, starts nothing and succeeds, so the rest of the deploy goes on."""

    def run_step(self, folder: Path, placement: str | None = None) -> tuple[int, str]:
        values = variables(folder) | {
            "compose_stack_aistor_state_file": str(folder / "compose" / "aistor.state"),
            "compose_stack_storage_services": ["aistor"],
        }
        if placement is not None:
            values["aistor_licence_placement"] = placement
        (folder / "compose").mkdir(exist_ok=True)
        return run_tasks(["storage.yml"], values)

    def test_a_missing_licence_gives_a_recorded_skip_and_a_successful_step_that_starts_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "secrets" / "aistor").mkdir(parents=True, mode=0o700)
            code, output = self.run_step(folder)
            state = folder / "compose" / "aistor.state"
            text, mode = state.read_text(), stat.S_IMODE(state.stat().st_mode)
        self.assertEqual(0, code, output)
        self.assertTrue(text.startswith("skipped"))
        self.assertEqual(0o644, mode)
        self.assertIn("AIStor is skipped", output)
        for name in ("Start AIStor and wait for its health", "Verify the AIStor project user as itself", "Record that AIStor started"):
            self.assertRegex(output, rf"{name}[^\n]*\] \*+\nskipping: \[localhost\]", f"{name} is skipped")
        self.assertNotIn("included: ", output.split("Say that AIStor is skipped")[1], "no task file is included after the skip")

    def test_the_skip_notice_and_the_record_come_only_when_the_host_has_no_licence_file_and_the_launcher_sent_none(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "secrets" / "aistor").mkdir(parents=True, mode=0o700)
            code, output = self.run_step(folder, "absent")
            state = (folder / "compose" / "aistor.state").read_text()
        self.assertEqual(0, code, output)
        self.assertTrue(state.startswith("skipped:"))
        self.assertIn("AIStor is skipped", output)

    def test_a_launcher_that_placed_the_licence_when_the_file_is_missing_fails_the_step_with_no_skip_notice_and_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "secrets" / "aistor").mkdir(parents=True, mode=0o700)
            (folder / "compose").mkdir()
            (folder / "compose" / "aistor.state").write_text("started\n")
            code, output = self.run_step(folder, "placed")
            left = (folder / "compose" / "aistor.state").exists()
        self.assertNotEqual(0, code)
        self.assertIn("the launcher placed the AIStor licence, but the licence file is missing on the host", output)
        self.assertNotIn("AIStor is skipped", output, "no skip is claimed for a licence that was placed")
        self.assertFalse(left)

    def test_a_second_run_with_the_licence_still_missing_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            (folder / "secrets" / "aistor").mkdir(parents=True, mode=0o700)
            self.run_step(folder)
            code, output = self.run_step(folder)
        self.assertEqual(0, code, output)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_licence_that_is_there_but_empty_fails_the_step_and_leaves_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            licence = folder / "secrets" / "aistor" / "licence"
            licence.parent.mkdir(parents=True, mode=0o700)
            licence.write_text("")
            licence.chmod(0o400)
            (folder / "compose").mkdir()
            (folder / "compose" / "aistor.state").write_text("started\n")
            code, output = self.run_step(folder)
            left = (folder / "compose" / "aistor.state").exists()
        self.assertNotEqual(0, code)
        self.assertIn("the AIStor step failed and its record is removed", output)
        self.assertFalse(left, "a step that fails leaves no earlier claim")


class SecretsCheckTest(unittest.TestCase):
    """The check reads the numeric owner and the mode of each file. A file that anyone else can read, or that another user owns, is refused."""

    def verdict(
        self,
        *,
        mode: int = 0o400,
        group: int = TOOL_GID,
        expected: tuple[int, int] = (UID, TOOL_GID),
        create: bool = True,
        folder_mode: int = 0o700,
        root_owner: tuple[int, int] = (UID, GID),
        owner_name: str = "root",
    ) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            target = folder / "secrets" / "postgres" / "nessie.password"
            target.parent.mkdir(parents=True)
            target.parent.parent.chmod(0o700)
            target.parent.chmod(folder_mode)
            if create:
                target.write_text("a-long-random-value")
                os.chown(target, UID, group)
                target.chmod(mode)
            values = variables(folder) | {
                "compose_stack_accounts": ["nessie"],
                "compose_stack_owners": {"nessie": f"{expected[0]}:{expected[1]}"},
                "secret_files_root_owner": f"{root_owner[0]}:{root_owner[1]}",
                "compose_stack_owner": owner_name,
            }
            return run_tasks(["verify_secrets.yml"], values)

    def test_a_private_file_of_its_owner_passes_whatever_the_name_of_that_owner_is(self) -> None:
        for name in ("root", "nobody_such_user"):
            with self.subTest(owner_name=name):
                code, output = self.verdict(owner_name=name)
                self.assertEqual(0, code, output)

    def test_a_file_that_anyone_but_its_owner_can_read_or_that_its_owner_can_write_is_refused_by_name_and_its_content_is_not_shown(self) -> None:
        for mode in (0o600, 0o640, 0o644, 0o440, 0o404, 0o444, 0o500, 0o000):
            with self.subTest(mode=oct(mode)):
                code, output = self.verdict(mode=mode)
                self.assertNotEqual(0, code)
                self.assertIn("The password file of nessie", output)
                self.assertNotIn("a-long-random-value", output)

    def test_a_file_that_another_user_owns_is_refused_by_the_number_of_the_user(self) -> None:
        for label, expected in (("another user", (UID + 1, TOOL_GID)), ("another group", (UID, TOOL_GID + 1))):
            with self.subTest(label):
                code, output = self.verdict(expected=expected)
                self.assertNotEqual(0, code)
                self.assertIn("The password file of nessie", output)

    def test_a_missing_file_is_refused(self) -> None:
        code, output = self.verdict(create=False)
        self.assertNotEqual(0, code)
        self.assertIn("The password file of nessie", output)

    def test_a_folder_that_others_can_read_or_that_another_user_owns_is_refused(self) -> None:
        for label, kwargs in (("readable folder", {"folder_mode": 0o755}), ("owned elsewhere", {"root_owner": (UID + 1, GID)})):
            with self.subTest(label):
                code, output = self.verdict(**kwargs)
                self.assertNotEqual(0, code)
                self.assertIn("The password folder", output)


if __name__ == "__main__":
    unittest.main()
