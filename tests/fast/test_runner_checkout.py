"""The runner checkout role, read from its files without running Ansible: its place in the host playbook, a deadline on every wait, a pinned interpreter and a
hash-pinned install, the code from ``git archive`` and a folder that only its owner reads. ``tests/slow/test_runner_checkout_role.py`` runs the role."""

from __future__ import annotations

import unittest
from typing import Any

from scripts.infra import host
from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import ROLES, group_variables
from tests.fast.test_host_budget import flatten, module_name, playbook_occurrences

ROLE = ROLES / "runner_checkout"
WAITING_MODULES = ("command", "shell", "get_url", "unarchive", "uri")
SHELL_MODULES = ("shell", "raw", "script")
FORBIDDEN_SOURCES = ("git clone", "git checkout", "git worktree", "git fetch", "rsync", "github.com/", "git+")


def role_tasks() -> list[tuple[str, dict[str, Any]]]:
    """Every task of every task file of the role, with its file's name."""
    found: list[tuple[str, dict[str, Any]]] = []
    for path in sorted((ROLE / "tasks").glob("*.yml")):
        found += [(path.name, task) for task in flatten(load_yaml(path.read_text()))]
    return found


def role_text() -> str:
    return "".join(path.read_text() for path in sorted((ROLE / "tasks").glob("*.yml")))


class HostPlaybookTest(unittest.TestCase):
    def test_the_host_playbook_runs_the_role_after_the_compose_stack_and_the_budget_counts_its_tasks(self) -> None:
        playbook: Any = load_yaml((ROOT / "ansible" / "host.yml").read_text())
        roles = [role if isinstance(role, str) else role["role"] for role in playbook[0]["roles"]]
        self.assertIn("runner_checkout", roles)
        self.assertGreater(roles.index("runner_checkout"), roles.index("compose_stack"))
        counted = {item.role for item in playbook_occurrences("host.yml")}
        self.assertIn("runner_checkout", counted)

    def test_the_deploy_gives_the_role_the_commit_that_the_stack_is_deployed_from(self) -> None:
        variables = host.deploy_variables({"S3_BUCKET": "example-bucket"}, "d" * 40)
        self.assertEqual("d" * 40, variables["runner_checkout_commit"])
        self.assertEqual(variables["compose_stack_platform_commit"], variables["runner_checkout_commit"])


class DeadlineTest(unittest.TestCase):
    def test_every_task_that_waits_for_a_command_a_download_or_an_unpack_has_its_own_deadline(self) -> None:
        waiting = [(file, task) for file, task in role_tasks() if module_name(task, WAITING_MODULES) is not None]
        self.assertGreaterEqual(len(waiting), 6, "the interpreter download and unpack, the archive, the environment, the install and the checks")
        for file, task in waiting:
            with self.subTest(file=file, task=task["name"]):
                key = str(module_name(task, WAITING_MODULES))
                inner = task[key] if isinstance(task[key], dict) else {}
                self.assertTrue("timeout" in task or "timeout" in inner, "a wait with no deadline")

    def test_no_task_runs_a_shell_line_or_a_script_whose_deadline_and_arguments_the_budget_cannot_read(self) -> None:
        for file, task in role_tasks():
            with self.subTest(file=file, task=task["name"]):
                self.assertIsNone(module_name(task, SHELL_MODULES))


class PinnedInstallTest(unittest.TestCase):
    def test_the_interpreter_is_one_exact_version_from_an_archive_with_a_pinned_hash(self) -> None:
        variables = group_variables()
        self.assertRegex(str(variables["runner_checkout_python_version"]), r"^3\.14\.\d+$", "the version the project's Dockerfiles and Ruff target")
        self.assertRegex(str(variables["runner_checkout_python_archive_sha256"]), r"^[0-9a-f]{64}$")
        download = next(task for _, task in role_tasks() if module_name(task, ("get_url",)) is not None)
        arguments = download["ansible.builtin.get_url"]
        self.assertRegex(str(arguments["checksum"]), r"^sha256:\{\{ runner_checkout_python_archive_sha256 \}\}$")
        default_url = str(variables["runner_checkout_python_url"])
        self.assertIn("{{ runner_checkout_python_version }}", default_url.replace("%2B", "+"))
        self.assertTrue(default_url.startswith("https://"))

    def test_the_install_is_hash_pinned_binary_only_and_names_each_requirements_file(self) -> None:
        installs = [task for _, task in role_tasks() if "pip" in str(task) and "install" in str(task) and module_name(task, ("command",)) is not None]
        self.assertEqual(1, len(installs))
        arguments = str(installs[0])
        for required in ("--require-hashes", "--only-binary", ":all:", "--requirement"):
            with self.subTest(argument=required):
                self.assertIn(required, arguments)
        for refused in ("--index-url", "--extra-index-url", "--no-deps", "--trusted-host", "--no-verify", "--upgrade", "git+"):
            with self.subTest(refused=refused):
                self.assertNotIn(refused, arguments)

    def test_the_requirements_are_the_projects_hash_pinned_file_and_every_entry_is_a_plain_path(self) -> None:
        files = group_variables()["runner_checkout_requirements"]
        self.assertEqual(["requirements.txt"], files)
        for name in files:
            with self.subTest(name=name):
                text = (ROOT / name).read_text()
                self.assertIn("--hash=sha256:", text)
                self.assertNotRegex(text, r"(?m)^[A-Za-z0-9_.\[\]-]+\s*(?:>=|<=|~=|>|<)", "an unpinned requirement")

    def test_the_interpreter_and_the_environment_are_checked_against_the_pinned_version(self) -> None:
        checks = [task for _, task in role_tasks() if "stdout" in str(task.get("ansible.builtin.assert", "")) and "runner_checkout_python_version" in str(task)]
        self.assertEqual(2, len(checks), "the shared interpreter and the new environment's interpreter")


class SourceTest(unittest.TestCase):
    def test_the_code_comes_from_git_archive_of_the_commit_and_never_from_a_clone_a_checkout_or_the_working_tree(self) -> None:
        text = role_text()
        self.assertIn("archive", text)
        for refused in FORBIDDEN_SOURCES:
            with self.subTest(refused=refused):
                self.assertNotIn(refused, text)
        archives = [
            task
            for _, task in role_tasks()
            if "ansible.builtin.command" in task
            and task["ansible.builtin.command"]["argv"][0] == "git"
            and "archive" in task["ansible.builtin.command"]["argv"]
        ]
        self.assertEqual(1, len(archives))
        self.assertEqual("localhost", archives[0]["delegate_to"])
        self.assertIs(archives[0]["become"], False)

    def test_no_copy_task_places_the_working_tree_and_the_role_never_touches_the_stack(self) -> None:
        for file, task in role_tasks():
            with self.subTest(file=file, task=task["name"]):
                self.assertIsNone(module_name(task, ("synchronize", "template")), "a copy could place files that the commit does not hold")
                key = module_name(task, ("copy",))
                if key is not None:
                    arguments = task[key]
                    self.assertTrue(
                        str(arguments.get("src", "")).endswith("/runner.tar") or "content" in arguments,
                        "a copy places the commit's archive or writes the marker's text and nothing else: it could place files that the commit does not hold",
                    )
        self.assertNotRegex(role_text(), r"compose_stack_|docker|k3s|airbyte|openbao|secrets")


class ProtectionTest(unittest.TestCase):
    def test_the_folders_are_for_the_owner_and_its_group_only(self) -> None:
        variables = group_variables()
        for name in ("runner_checkout_mode", "runner_checkout_file_mode"):
            with self.subTest(name=name):
                self.assertRegex(str(variables[name]), r"^(?:0?[0-7][0-5]0|u=[rwxX]+,g=[rxX]*,o=)$", "no permission for others")
        self.assertEqual("/opt/platform/runner", variables["runner_checkout_directory"])
        self.assertEqual("ubuntu", variables["runner_checkout_owner"])

    def test_the_placement_tool_uses_the_roles_folder_and_marker_name(self) -> None:
        from scripts.infra import runner_place

        self.assertEqual(group_variables()["runner_checkout_directory"], runner_place.RUNNER_DIRECTORY)
        self.assertTrue(str(group_variables()["runner_checkout_marker"]).endswith(f"/{runner_place.MARKER}"))


if __name__ == "__main__":
    unittest.main()
