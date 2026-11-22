"""The role that deploys the Compose stack: what its task files do to files, through the real ``ansible-playbook`` on this computer.

Starting the stack and the accounts step against a server need Docker and are proven by the acceptance test. These cases cover the file work:
the placed Compose files, the rendered limits and host settings, the password files and their check.
"""

from __future__ import annotations

import grp
import json
import os
import pwd
import re
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.limits import load_limits, render_environment
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_support import ROLES, run_playbook

ROOT = Path(__file__).resolve().parents[2]
ROLE = ROLES / "compose_stack"
OWNER = pwd.getpwuid(os.getuid()).pw_name
GROUP = grp.getgrgid(os.getgid()).gr_name
UID, GID = os.getuid(), os.getgid()
TOOL_GID = next((group for group in os.getgroups() if group != GID), GID)
CHANGED_NONE = re.compile(r"changed=0\s")


def variables(folder: Path) -> dict[str, object]:
    return {
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
            "WAREHOUSE_LOCATION=s3://example-bucket/warehouse/\nAWS_REGION=us-west-2\n",
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


def step_order() -> list[str]:
    """The steps of the role in the order they run: a task file by name (with the services it starts), or a file of another role as ``role/file``."""
    tasks: Any = load_yaml((ROLE / "tasks" / "main.yml").read_text())
    steps: list[str] = []
    for task in tasks:
        if "ansible.builtin.include_tasks" in task:
            services = (task.get("vars") or {}).get("compose_stack_start_services")
            steps.append(task["ansible.builtin.include_tasks"] + (f" {','.join(services) or 'all'}" if services is not None else ""))
        else:
            role = task["ansible.builtin.include_role"]
            steps.append(f"{role['name']}/{role['tasks_from']}")
    return steps


class StepOrderTest(unittest.TestCase):
    def test_the_role_starts_postgresql_first_makes_its_accounts_prepares_openbao_starts_the_rest_gives_nessie_its_first_commit_and_makes_openbao_match(
        self,
    ) -> None:
        self.assertEqual(
            [
                "place_files.yml",
                "caddy_host.yml",
                "render_environment.yml",
                "passwords.yml",
                "verify_secrets.yml",
                "grafana_password.yml",
                "build_images.yml",
                "airbyte/cluster.yml",
                "airbyte/group_access.yml",
                "start.yml postgres",
                "provision.yml",
                "openbao/prepare.yml",
                "start.yml all",
                "nessie.yml",
                "kafka.yml",
                "registry.yml",
                "openbao/bootstrap.yml",
            ],
            step_order(),
        )

    def test_the_k3s_access_files_exist_before_the_first_start_so_a_fresh_host_deploys_in_one_pass(self) -> None:
        """The code location reads the k3s token and certificate authority as Compose secrets, which the Airbyte role's access step writes."""
        steps = step_order()
        first_start = next(index for index, step in enumerate(steps) if step.startswith("start.yml"))
        self.assertLess(steps.index("airbyte/cluster.yml"), steps.index("airbyte/group_access.yml"))
        self.assertLess(steps.index("airbyte/group_access.yml"), first_start)
        document: Any = load_yaml((ROOT / "compose" / "services" / "dagster_code_location.yml").read_text())
        secrets = document["secrets"]
        self.assertEqual({"k3s/dagster.token", "k3s/ca.crt"}, {secrets[name]["file"].split("}/", 1)[1] for name in ("k3s_token", "k3s_ca")})


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
    """Grafana's administrator password is a file of its own container user, mode 0400, made once and never replaced."""

    def values(self, folder: Path, owner: tuple[int, int]) -> dict[str, object]:
        return variables(folder) | {"grafana_user": f"{owner[0]}:{owner[1]}"}

    def test_the_file_is_made_for_the_grafana_user_with_mode_0400_in_a_folder_for_root_and_no_output_holds_the_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            code, output = run_tasks(["grafana_password.yml"], self.values(folder, (UID, TOOL_GID)))
            target = folder / "secrets" / "grafana" / "admin.password"
            status = target.stat()
            folder_status = target.parent.stat()
            text = target.read_text()
        self.assertEqual(0, code, output)
        self.assertEqual((0o400, UID, TOOL_GID), (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid))
        self.assertEqual((0o700, UID, GID), (stat.S_IMODE(folder_status.st_mode), folder_status.st_uid, folder_status.st_gid))
        self.assertRegex(text, r"^[A-Za-z0-9_-]{40,}$")
        self.assertNotIn(text, output)

    def test_a_second_run_changes_nothing_and_keeps_the_password(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            run_tasks(["grafana_password.yml"], self.values(folder, (UID, TOOL_GID)))
            target = folder / "secrets" / "grafana" / "admin.password"
            before = target.read_text()
            code, output = run_tasks(["grafana_password.yml"], self.values(folder, (UID, TOOL_GID)))
            after = target.read_text()
        self.assertEqual(0, code, output)
        self.assertEqual(before, after)
        self.assertRegex(output, CHANGED_NONE)

    def test_a_file_with_another_mode_or_another_owner_is_given_the_right_ones_with_its_password_kept(self) -> None:
        for mode, group in ((0o600, TOOL_GID), (0o640, TOOL_GID), (0o444, TOOL_GID), (0o400, GID)):
            with self.subTest(mode=oct(mode), group=group), tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                target = folder / "secrets" / "grafana" / "admin.password"
                target.parent.mkdir(parents=True, mode=0o700)
                target.write_text("an-existing-password-of-forty-characters-or-more")
                os.chown(target, UID, group)
                target.chmod(mode)
                code, output = run_tasks(["grafana_password.yml"], self.values(folder, (UID, TOOL_GID)))
                status = target.stat()
                self.assertEqual(0, code, output)
                self.assertEqual(
                    (0o400, UID, TOOL_GID, "an-existing-password-of-forty-characters-or-more"),
                    (stat.S_IMODE(status.st_mode), status.st_uid, status.st_gid, target.read_text()),
                )

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
