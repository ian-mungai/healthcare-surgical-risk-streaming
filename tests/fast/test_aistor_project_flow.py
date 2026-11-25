"""AIStor's root pair is used once, to make the project user, and by nothing else: the files that name it, the order of the deploy and the project user's files.

The owner decided that root credentials are kept in a file, secured on the disk and not used; one project user does the project's work. The cases read the
real files. They hold that no file outside the server's own definition, the credential files' tasks and the one bootstrap module names a root file or a root
variable; that no root variable is ever set as a value in the environment (the ``_FILE`` forms are the only ones); that the project user's password file is
made for root with mode 0400 and checked like the root files; that the bootstrap runs after AIStor is healthy and after the password file exists, and that the
OpenBao step that keeps the sign-in runs after the file exists and before nothing reads it. ``root_use_problems`` names each defect, and the planted cases prove
that each check still looks.
"""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import group_variables

ROOT = Path(__file__).resolve().parents[2]
SKIPPED_FOLDERS = {".git", ".venv", ".tools", "runs", "__pycache__", ".worktrees", "node_modules", ".terraform"}
ALLOWED = {
    "README.md",
    "compose/services/aistor.yml",
    "ansible/roles/compose_stack/tasks/aistor_credentials.yml",
    "ansible/roles/compose_stack/tasks/verify_aistor_credentials.yml",
    "ansible/roles/compose_stack/library/aistor_project_user.py",
}
ROOT_NAMES = re.compile(r"aistor_root_|aistor/root_|MINIO_ROOT|\broot_user\b|\broot_password\b")
ROOT_VALUE = re.compile(r"MINIO_ROOT_(?:USER|PASSWORD)(?!_FILE)|MINIO_USER|MINIO_PASSWORD|MC_HOST_")
TASKS = ROOT / "ansible" / "roles" / "compose_stack" / "tasks"


def repository_files() -> dict[str, str]:
    """Every text file of the repository outside the tests, the tool folders and the ignored run folders, by its path from the root."""
    found: dict[str, str] = {}
    for folder, names, files in os.walk(ROOT):
        names[:] = [name for name in names if name not in SKIPPED_FOLDERS and not (Path(folder) == ROOT and name == "tests")]
        for file in files:
            path = Path(folder) / file
            try:
                found[str(path.relative_to(ROOT))] = path.read_text()
            except UnicodeDecodeError, OSError:
                continue
    return found


def root_use_problems(files: dict[str, str]) -> list[str]:
    """Each file that names a root file or variable without being allowed to, and each root variable set as a value anywhere."""
    found = [f"{name}: names AIStor's root pair" for name, text in files.items() if name not in ALLOWED and ROOT_NAMES.search(text)]
    found += [f"{name}: sets a root variable as a value" for name, text in files.items() if re.search(r"MINIO_ROOT_(?:USER|PASSWORD)(?!_FILE)", text)]
    return found


def tasks_of(name: str) -> list[dict[str, Any]]:
    document = load_yaml((TASKS / name).read_text())
    return [task for task in document if isinstance(task, dict)] if isinstance(document, list) else []


def includes() -> list[str]:
    return [str(task["ansible.builtin.include_tasks"]) for task in tasks_of("main.yml") if "ansible.builtin.include_tasks" in task]


class RootUseTest(unittest.TestCase):
    def test_only_the_servers_definition_the_credential_tasks_and_the_one_bootstrap_name_the_root_pair(self) -> None:
        self.assertEqual([], root_use_problems(repository_files()))

    def test_the_allowed_files_are_the_ones_that_exist(self) -> None:
        for name in ALLOWED:
            with self.subTest(name):
                self.assertTrue((ROOT / name).is_file())

    def test_the_check_finds_a_root_file_a_root_variable_and_a_root_value_in_a_planted_file(self) -> None:
        for label, text in (
            ("a root file", "cat /opt/platform/secrets/aistor/root_password"),
            ("a compose secret name", "source: aistor_root_user"),
            ("a root variable", "MINIO_ROOT_PASSWORD_FILE=/x"),
            ("a variable name", "x = root_user"),
        ):
            with self.subTest(label):
                self.assertEqual(["scripts/new.py: names AIStor's root pair"], root_use_problems({"scripts/new.py": text}))
        self.assertEqual(
            ["scripts/new.py: names AIStor's root pair", "scripts/new.py: sets a root variable as a value"],
            root_use_problems({"scripts/new.py": "MINIO_ROOT_PASSWORD=abc"}),
        )

    def test_a_root_variable_as_a_value_is_found_even_in_an_allowed_file(self) -> None:
        self.assertEqual(
            ["compose/services/aistor.yml: sets a root variable as a value"], root_use_problems({"compose/services/aistor.yml": "MINIO_ROOT_USER: admin"})
        )

    def test_terraform_and_the_storage_command_name_no_root_variable_and_take_the_project_users_pair_only_as_the_providers_environment(self) -> None:
        files = repository_files()
        for name in ("scripts/infra/storage.py",):
            self.assertNotRegex(files[name], r"MINIO_ROOT|aistor_root|root_user|root_password", name)
        for name, text in files.items():
            if name.startswith("infra/storage/") and name.endswith(".tf"):
                self.assertNotRegex(text, ROOT_VALUE, name)

    def test_the_bootstrap_modules_calls_hold_the_root_pair_only_as_a_read_of_the_containers_files_in_the_one_command_environment(self) -> None:
        text = (ROOT / "ansible/roles/compose_stack/library/aistor_project_user.py").read_text()
        self.assertEqual(1, len(re.findall(r"ROOT_SIGN_IN = ", text)))
        self.assertIn("$(cat {ROOT_PAIR_FILES[0]})", text)
        self.assertNotRegex(text, r"os\.environ|environ\[|env=")


class ProjectPasswordFileTest(unittest.TestCase):
    def test_the_password_file_is_made_for_root_with_the_module_that_makes_the_root_files_and_with_the_same_whitespace_rule(self) -> None:
        make = tasks_of("aistor_credentials.yml")[0]
        self.assertEqual("{{ secret_files_root_owner }}", make["secret_file"]["owner"])
        self.assertTrue(make["secret_file"]["refuse_edge_whitespace"])
        self.assertEqual(["root_user", "root_password", "project_password"], make["loop"])
        self.assertIs(True, make["no_log"])

    def test_the_check_asserts_a_plain_file_with_content_mode_0400_and_the_root_owner_for_each_of_the_three_files(self) -> None:
        text = (TASKS / "verify_aistor_credentials.yml").read_text()
        self.assertIn("loop: [root_user, root_password, project_password]", text)
        for needle in ("item.stat.isreg", "item.stat.size | default(0) | int > 0", "item.stat.mode == '0400'", "secret_files_root_owner.split(':')[0]"):
            self.assertIn(needle, text)

    def test_the_project_user_is_not_the_root_name_and_the_policy_is_not_a_built_in_one(self) -> None:
        variables = group_variables()
        self.assertNotIn(str(variables["compose_stack_aistor_project_user"]).lower(), {"minioadmin", "root", "admin"})
        self.assertNotIn(variables["compose_stack_aistor_project_policy"], {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"})
        self.assertEqual("{{ compose_stack_secrets_directory }}/aistor/project_password", variables["compose_stack_aistor_project_password_file"])


class OrderTest(unittest.TestCase):
    def test_the_password_file_exists_before_openbao_keeps_it_and_before_the_bootstrap_reads_it(self) -> None:
        names = includes()
        self.assertLess(names.index("aistor_credentials.yml"), names.index("aistor_project_user.yml"))
        main = (TASKS / "main.yml").read_text()
        self.assertLess(main.index("aistor_credentials.yml"), main.index("tasks_from: bootstrap.yml"))

    def test_the_bootstrap_runs_after_aistor_is_started_and_healthy_and_after_the_licence_check_and_nothing_follows_it_that_needs_root(self) -> None:
        names = includes()
        self.assertEqual("aistor_project_user.yml", names[-1])
        start = [
            index
            for index, task in enumerate(tasks_of("main.yml"))
            if (task.get("vars") or {}).get("compose_stack_start_services") == "{{ compose_stack_storage_services }}"
        ]
        self.assertEqual(1, len(start))
        order = [task.get("ansible.builtin.include_tasks") for task in tasks_of("main.yml")]
        self.assertLess(order.index("aistor_licence.yml"), start[0])
        self.assertLess(start[0], order.index("aistor_project_user.yml"))

    def test_the_bootstrap_task_is_hidden_names_the_password_by_file_only_and_gives_the_bucket_from_the_launcher(self) -> None:
        (task,) = tasks_of("aistor_project_user.yml")
        self.assertIs(True, task["no_log"])
        self.assertEqual(
            {"compose_command", "service", "user", "policy", "bucket", "password_file"},
            set(task["aistor_project_user"]),
        )
        self.assertEqual("{{ aistor_bucket }}", task["aistor_project_user"]["bucket"])
        self.assertEqual("{{ compose_stack_aistor_project_password_file }}", task["aistor_project_user"]["password_file"])

    def test_the_launcher_gives_the_bucket_the_policy_reaches(self) -> None:
        from scripts.infra import host

        variables = host.deploy_variables({"S3_BUCKET": "example-project"}, "c" * 40)
        self.assertEqual("example-project", variables["aistor_bucket"])


class OpenBaoStepTest(unittest.TestCase):
    def test_the_bootstrap_step_gets_the_client_service_user_and_password_file_and_no_value(self) -> None:
        (task,) = [task for task in tasks_of_openbao("bootstrap.yml") if "openbao_bootstrap" in task]
        params = task["openbao_bootstrap"]
        self.assertEqual("{{ openbao_client_service }}", params["client_service"])
        self.assertEqual("{{ openbao_client_user }}", params["client_user"])
        self.assertEqual("{{ openbao_client_password_file }}", params["client_password_file"])
        self.assertNotIn("password:", str(params))

    def test_the_defaults_point_at_the_compose_stack_variables_and_the_service_is_a_listed_one(self) -> None:
        defaults: Any = load_yaml((ROOT / "ansible/roles/openbao/defaults/main.yml").read_text())
        self.assertEqual("{{ compose_stack_aistor_service }}", defaults["openbao_client_service"])
        listed = [line.split()[0] for line in (ROOT / "compose/openbao_services.txt").read_text().splitlines() if line.strip() and not line.startswith("#")]
        self.assertIn(group_variables()["compose_stack_aistor_service"], listed)
        self.assertIn(group_variables()["compose_stack_aistor_service"], group_variables()["compose_stack_storage_services"])


def tasks_of_openbao(name: str) -> list[dict[str, Any]]:
    document = load_yaml((ROOT / "ansible" / "roles" / "openbao" / "tasks" / name).read_text())
    return [task for task in document if isinstance(task, dict)] if isinstance(document, list) else []


if __name__ == "__main__":
    unittest.main()
