"""AIStor's root pair is used once, to make the project user, and by nothing else: the files that name it, the order of the deploy and the project user's files.

The owner decided that root credentials are kept in a file, secured on the disk and not used; one project user does the project's work. The cases read the
real files. They hold that no file outside the server's own definition, the credential files' tasks and the one bootstrap module names a root file or a root
variable; that no root variable is ever set as a value in the environment (the ``_FILE`` forms are the only ones); that the project user's password file is
made for root with mode 0400 and checked like the root files; that the bootstrap runs after AIStor is healthy and after the password file exists, and that the
OpenBao step that keeps the sign-in runs after the file exists and before nothing reads it. ``root_use_problems`` names each defect, and the planted cases prove
that each check still looks.
"""

from __future__ import annotations

import ast
import os
import re
import unittest
from pathlib import Path
from typing import Any

from jinja2 import Environment

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


MODULE_FILE = "ansible/roles/compose_stack/library/aistor_project_user.py"
ROOT_SCRIPT_KINDS = {"read", "user_add", "policy_create"}
ROOT_CALL_KINDS = {"read", "user_add", "policy_create", "sign_in"}
# Every call the module may make with the root pair, as the first three words that mc gets. A new one needs a new line here, and so a reviewer's eye.
ROOT_READS = {
    ("admin", "policy", "info"),
    ("admin", "user", "info"),
    ("admin", "policy", "detach"),
    ("admin", "policy", "attach"),
    ("admin", "user", "enable"),
    ("admin", "group", "remove"),
}


def leading_words(node: ast.AST) -> tuple[str, ...]:
    """The string constants at the start of a tuple or of the arguments of a call, up to the first value that is not a string constant."""
    items = node.elts if isinstance(node, ast.Tuple) else getattr(node, "args", [])
    words: list[str] = []
    for item in items:
        if not (isinstance(item, ast.Constant) and isinstance(item.value, str)):
            break
        words.append(item.value)
    return tuple(words)


def docstring_nodes(tree: ast.AST) -> set[int]:
    """The ids of the constants that are a docstring: they describe the root pair and are not a use of it."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                found.add(id(first.value))
    return found


def is_helper_words(node: ast.AST) -> bool:
    """The one call of the module that passes words it was given: ``tuple(words)`` inside the ``read`` helper."""
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "tuple" and ast.unparse(node) == "tuple(words)"


def module_root_problems(text: str) -> list[str]:
    """Each use of the root pair in the bootstrap module's code that its allowlist does not name: the scripts that read the pair, the calls that run with it and
    the places that name a root file. The module is allowed to name the root pair, so this check looks inside it."""
    tree = ast.parse(text)
    problems: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "SCRIPTS" for target in node.targets)
            and isinstance(node.value, ast.Dict)
        ):
            for key, value in zip(node.value.keys, node.value.values, strict=True):
                kind, source = ast.literal_eval(key) if key is not None else "", ast.get_source_segment(text, value) or ""
                if ("ROOT_SIGN_IN" in source or "aistor_root" in source) and kind not in ROOT_SCRIPT_KINDS:
                    problems.append(f"the script {kind} reads the root pair")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Call" and node.args:
            first = node.args[0]
            kind = first.value if isinstance(first, ast.Constant) else None
            if kind not in ROOT_CALL_KINDS:
                problems.append(f"a call of the kind {kind!r}")
            if kind == "read" and len(node.args) > 1 and not is_helper_words(node.args[1]) and leading_words(node.args[1])[:3] not in ROOT_READS:
                problems.append(f"a root read of {leading_words(node.args[1])[:3]}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "read" and len(node.args) > 1:
            words = leading_words(ast.Tuple(elts=node.args[1:]))
            if words[:3] not in ROOT_READS:
                problems.append(f"a root read of {words[:3]}")
    skipped = docstring_nodes(tree)
    literals = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and "aistor_root" in node.value and id(node) not in skipped
    ]
    if len(literals) != 2:
        problems.append(f"the module names a root file in {len(literals)} places, not the 2 of ROOT_PAIR_FILES")
    if re.search(r"\bos\.environ|\benviron\[|\benv=", text):
        problems.append("the module reads or sets an environment")
    return problems


def evaluate_assertions(task: dict[str, Any], **facts: Any) -> bool:
    """Whether every condition of the real ``assert`` task holds for the facts: the YAML is parsed, so a comment or a message cannot satisfy a condition."""
    conditions = task["ansible.builtin.assert"]["that"]
    environment = Environment(autoescape=True)
    return all(bool(environment.compile_expression(str(condition))(**facts)) for condition in (conditions if isinstance(conditions, list) else [conditions]))


def stat_facts(*, mode: str = "0400", uid: int = 0, gid: int = 0, size: int = 5, regular: bool = True, exists: bool = True) -> dict[str, Any]:
    return {"exists": exists, "isreg": regular, "size": size, "mode": mode, "uid": uid, "gid": gid, "pw_name": "root"}


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


class ModuleRootUseTest(unittest.TestCase):
    """The one module that may name the root pair is checked inside: only the three scripts read it, and only the named reads run with it."""

    def text(self) -> str:
        return (ROOT / MODULE_FILE).read_text()

    def test_the_real_module_passes(self) -> None:
        self.assertEqual([], module_root_problems(self.text()))

    def test_an_extra_root_read_inside_the_module_is_found(self) -> None:
        planted = self.text().replace(
            'write(runner, Call("read", ("admin", "user", "enable", ALIAS, user)), "enable the user")', 'read(runner, "admin", "config", "get", ALIAS)'
        )
        self.assertNotEqual(self.text(), planted)
        self.assertEqual(["a root read of ('admin', 'config', 'get')"], module_root_problems(planted))

    def test_an_extra_root_call_by_the_call_class_is_found(self) -> None:
        planted = self.text() + '\n\ndef extra(runner):\n    return runner(Call("read", ("admin", "service", "restart", ALIAS)))\n'
        self.assertEqual(["a root read of ('admin', 'service', 'restart')"], module_root_problems(planted))

    def test_a_call_of_a_kind_that_no_script_has_is_found(self) -> None:
        planted = self.text() + '\n\ndef extra(runner):\n    return runner(Call("shell", ("ls",)))\n'
        self.assertEqual(["a call of the kind 'shell'"], module_root_problems(planted))

    def test_the_sign_in_script_that_reads_the_root_files_is_found(self) -> None:
        planted = self.text().replace("IFS= read -r secret_value && MC_HOST_", "cat /run/secrets/aistor_root_user; IFS= read -r secret_value && MC_HOST_")
        self.assertNotEqual(self.text(), planted)
        problems = module_root_problems(planted)
        self.assertIn("the script sign_in reads the root pair", problems)

    def test_a_script_that_uses_the_root_sign_in_without_being_one_of_the_three_is_found(self) -> None:
        planted = self.text().replace('"sign_in": f\'IFS= read', '"sign_in": f\'{ROOT_SIGN_IN}; IFS= read')
        self.assertNotEqual(self.text(), planted)
        self.assertIn("the script sign_in reads the root pair", module_root_problems(planted))

    def test_a_third_place_that_names_a_root_file_and_an_environment_are_found(self) -> None:
        self.assertIn(
            "the module names a root file in 3 places, not the 2 of ROOT_PAIR_FILES",
            module_root_problems(self.text() + '\nEXTRA = "/run/secrets/aistor_root_user"\n'),
        )
        self.assertIn("the module reads or sets an environment", module_root_problems(self.text() + "\nimport os\nVALUE = os.environ\n"))

    def test_the_project_users_sign_in_script_is_not_among_the_scripts_that_read_the_pair(self) -> None:
        self.assertEqual({"read", "user_add", "policy_create"}, ROOT_SCRIPT_KINDS)
        self.assertNotIn("sign_in", ROOT_SCRIPT_KINDS)


class ModeCheckTest(unittest.TestCase):
    """The checks of the credential files and of the licence file are evaluated as they are in the task files, so a comment cannot stand in for a condition."""

    def verify_task(self) -> dict[str, Any]:
        return next(task for task in tasks_of("verify_aistor_credentials.yml") if "ansible.builtin.assert" in task)

    def facts(self, **kwargs: Any) -> dict[str, Any]:
        return {"item": {"stat": stat_facts(**kwargs), "item": "root_password"}, "secret_files_root_owner": "0:0"}

    def test_a_private_plain_file_with_content_of_root_passes(self) -> None:
        self.assertTrue(evaluate_assertions(self.verify_task(), **self.facts()))

    def test_every_other_mode_fails_whatever_a_comment_or_a_message_says(self) -> None:
        for mode in ("0444", "0440", "0600", "0640", "0404", "0200"):
            with self.subTest(mode=mode):
                self.assertFalse(evaluate_assertions(self.verify_task(), **self.facts(mode=mode)))

    def test_another_owner_a_missing_file_a_link_and_an_empty_file_fail(self) -> None:
        for label, kwargs in (("uid", {"uid": 1}), ("gid", {"gid": 1}), ("a link", {"regular": False}), ("empty", {"size": 0})):
            with self.subTest(label):
                self.assertFalse(evaluate_assertions(self.verify_task(), **self.facts(**kwargs)))

    def test_a_planted_task_with_0444_and_the_right_mode_only_in_a_comment_fails_the_evaluation(self) -> None:
        planted = load_yaml(
            "- name: x\n  ansible.builtin.assert:\n    that:\n      - item.stat.mode == '0444'  # item.stat.mode == '0400'\n"
            "    fail_msg: item.stat.mode == '0400'\n"
        )
        self.assertTrue(isinstance(planted, list) and not evaluate_assertions(planted[0], item={"stat": stat_facts(mode="0400")}))

    def test_a_text_search_for_the_condition_is_not_the_check(self) -> None:
        text = (TASKS / "verify_aistor_credentials.yml").read_text().replace(
            "item.stat.mode == '0400'", "item.stat.mode == '0444'"
        ) + "\n# item.stat.mode == '0400'\n"
        document = load_yaml(text)
        task = next(task for task in (document if isinstance(document, list) else []) if "ansible.builtin.assert" in task)
        self.assertIn("item.stat.mode == '0400'", text)
        self.assertTrue(evaluate_assertions(task, **self.facts(mode="0444")), "the planted file accepts 0444: the text search would have passed it")
        self.assertFalse(evaluate_assertions(self.verify_task(), **self.facts(mode="0444")))


class StatTasksTest(unittest.TestCase):
    """A stat of a credential file computes no checksum and no content type: a checksum of a secret is a value of it, and ``-v`` prints it."""

    def test_each_stat_task_of_a_credential_or_licence_file_sets_get_checksum_and_get_mime_to_false(self) -> None:
        found = 0
        for name in ("verify_aistor_credentials.yml", "aistor_licence.yml"):
            for task in tasks_of(name) + [inner for outer in tasks_of(name) for inner in outer.get("block", [])]:
                if "ansible.builtin.stat" in task:
                    found += 1
                    with self.subTest(file=name, task=task.get("name")):
                        options = task["ansible.builtin.stat"]
                        self.assertIs(False, options.get("get_checksum"))
                        self.assertIs(False, options.get("get_mime"))
                        self.assertIs(False, options.get("follow"))
        self.assertEqual(2, found)


class ProjectPasswordFileTest(unittest.TestCase):
    def test_the_password_file_is_made_for_root_with_the_module_that_makes_the_root_files_and_with_the_same_whitespace_rule(self) -> None:
        make = tasks_of("aistor_credentials.yml")[0]
        self.assertEqual("{{ secret_files_root_owner }}", make["secret_file"]["owner"])
        self.assertTrue(make["secret_file"]["refuse_edge_whitespace"])
        self.assertEqual(["root_user", "root_password", "project_password"], make["loop"])
        self.assertIs(True, make["no_log"])

    def test_the_check_covers_each_of_the_three_files(self) -> None:
        stat_task = next(task for task in tasks_of("verify_aistor_credentials.yml") if "ansible.builtin.stat" in task)
        self.assertEqual(["root_user", "root_password", "project_password"], stat_task["loop"])

    def test_the_project_user_is_not_the_root_name_and_the_policy_is_not_a_built_in_one(self) -> None:
        variables = group_variables()
        self.assertNotIn(str(variables["compose_stack_aistor_project_user"]).lower(), {"minioadmin", "root", "admin"})
        self.assertNotIn(variables["compose_stack_aistor_project_policy"], {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"})
        self.assertEqual("{{ compose_stack_secrets_directory }}/aistor/project_password", variables["compose_stack_aistor_project_password_file"])


class OrderTest(unittest.TestCase):
    def test_the_password_file_exists_before_the_aistor_step_reads_it_and_openbao_is_matched_before_that_step_logs_in_to_it(self) -> None:
        names = includes()
        self.assertLess(names.index("aistor_credentials.yml"), names.index("aistor_project_user.yml"))
        main = (TASKS / "main.yml").read_text()
        self.assertLess(
            main.index("tasks_from: bootstrap.yml"),
            main.index("aistor_project_user.yml"),
            "the admin role and its secret ID file exist before the step logs in",
        )

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
            {"compose_command", "service", "user", "policy", "bucket", "password_file", "openbao_address", "openbao_secret_id_file", "openbao_path", "rotate"},
            set(task["aistor_project_user"]),
        )
        params = task["aistor_project_user"]
        self.assertEqual("{{ aistor_bucket }}", params["bucket"])
        self.assertEqual("{{ compose_stack_aistor_project_password_file }}", params["password_file"])
        self.assertEqual("{{ compose_stack_aistor_rotate_password }}", params["rotate"])
        self.assertEqual("{{ compose_stack_secrets_directory }}/openbao/bootstrap.secret_id", params["openbao_secret_id_file"])
        self.assertEqual("{{ compose_stack_aistor_openbao_path }}", params["openbao_path"])
        self.assertNotIn("password:", str(params).replace("password_file", "").replace("rotate_password", ""))

    def test_the_rotation_is_off_by_default_so_a_new_password_file_is_never_set_on_the_user_without_the_owner(self) -> None:
        self.assertIs(False, group_variables()["compose_stack_aistor_rotate_password"])

    def test_the_openbao_path_is_the_one_that_the_aistor_role_reads_and_that_the_storage_command_asks_for(self) -> None:
        from scripts.infra import storage

        variables = group_variables()
        self.assertEqual(f"secret/data/services/{variables['compose_stack_aistor_service']}/aistor", "secret/data/services/" + storage.OPENBAO_ROLE + "/aistor")
        self.assertEqual("secret/data/services/{{ compose_stack_aistor_service }}/aistor", variables["compose_stack_aistor_openbao_path"])
        self.assertEqual(storage.OPENBAO_PATH, f"secret/data/services/{variables['compose_stack_aistor_service']}/aistor")

    def test_the_launcher_gives_the_bucket_the_policy_reaches(self) -> None:
        from scripts.infra import host

        variables = host.deploy_variables({"S3_BUCKET": "example-project"}, "c" * 40)
        self.assertEqual("example-project", variables["aistor_bucket"])


class OpenBaoStepTest(unittest.TestCase):
    def test_the_bootstrap_step_gets_the_client_service_only_and_is_hidden_because_it_handles_secret_ids_and_passwords(self) -> None:
        (task,) = [task for task in tasks_of_openbao("bootstrap.yml") if "openbao_bootstrap" in task]
        params = task["openbao_bootstrap"]
        self.assertEqual("{{ openbao_client_service }}", params["client_service"])
        for gone in ("client_user", "client_password_file"):
            self.assertNotIn(gone, params, "the AIStor step keeps the sign-in, after it has signed in, and not this step")
        self.assertIs(True, task["no_log"])

    def test_the_defaults_point_at_the_compose_stack_variables_and_the_service_is_a_listed_one(self) -> None:
        defaults: Any = load_yaml((ROOT / "ansible/roles/openbao/defaults/main.yml").read_text())
        self.assertEqual("{{ compose_stack_aistor_service }}", defaults["openbao_client_service"])
        self.assertNotIn("openbao_client_user", defaults)
        self.assertNotIn("openbao_client_password_file", defaults)
        listed = [line.split()[0] for line in (ROOT / "compose/openbao_services.txt").read_text().splitlines() if line.strip() and not line.startswith("#")]
        self.assertIn(group_variables()["compose_stack_aistor_service"], listed)
        self.assertIn(group_variables()["compose_stack_aistor_service"], group_variables()["compose_stack_storage_services"])


def tasks_of_openbao(name: str) -> list[dict[str, Any]]:
    document = load_yaml((ROOT / "ansible" / "roles" / "openbao" / "tasks" / name).read_text())
    return [task for task in document if isinstance(task, dict)] if isinstance(document, list) else []


if __name__ == "__main__":
    unittest.main()
