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
# The name of the owner's approval of one root repair holds "aistor_root_" but names no root file and no root variable, so the scans below do not count it.
APPROVAL_NAME = "compose_stack_aistor_root_repair"
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
    found = [f"{name}: names AIStor's root pair" for name, text in files.items() if name not in ALLOWED and ROOT_NAMES.search(text.replace(APPROVAL_NAME, ""))]
    found += [f"{name}: sets a root variable as a value" for name, text in files.items() if re.search(r"MINIO_ROOT_(?:USER|PASSWORD)(?!_FILE)", text)]
    return found


MODULE_FILE = "ansible/roles/compose_stack/library/aistor_project_user.py"
ROOT_SCRIPT_KINDS = {"read", "user_add", "policy_create"}
ROOT_CALL_KINDS = {"read", "user_add", "policy_create", "sign_in", "audit", "audit_user"}
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
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and "aistor_root" in node.value.replace(APPROVAL_NAME, "") and id(node) not in skipped
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


def roles_of(name: str) -> list[dict[str, Any]]:
    return [task["ansible.builtin.include_role"] for task in tasks_of(name) if "ansible.builtin.include_role" in task]


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

    def test_the_name_of_the_approval_is_not_a_root_file_but_a_root_file_next_to_it_is_still_found(self) -> None:
        self.assertEqual([], root_use_problems({"scripts/new.py": f"{APPROVAL_NAME}: true"}))
        self.assertEqual(
            ["scripts/new.py: names AIStor's root pair"], root_use_problems({"scripts/new.py": f"{APPROVAL_NAME}: true\ncat aistor/root_password"})
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

    def test_the_project_users_sign_in_script_and_the_audit_script_are_not_among_the_scripts_that_read_the_pair(self) -> None:
        self.assertEqual({"read", "user_add", "policy_create"}, ROOT_SCRIPT_KINDS)
        self.assertNotIn("sign_in", ROOT_SCRIPT_KINDS)
        self.assertNotIn("audit", ROOT_SCRIPT_KINDS)

    def test_the_audit_script_that_reads_the_root_files_is_found(self) -> None:
        planted = self.text().replace('"audit": (\n        f\'IFS= read', '"audit": (\n        f\'{ROOT_SIGN_IN}; IFS= read')
        self.assertNotEqual(self.text(), planted)
        self.assertIn("the script audit reads the root pair", module_root_problems(planted))


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
        self.assertEqual(["root_user", "root_password", "project_password", "audit_password"], make["loop"])
        self.assertIs(True, make["no_log"])

    def test_the_check_covers_each_of_the_four_files(self) -> None:
        stat_task = next(task for task in tasks_of("verify_aistor_credentials.yml") if "ansible.builtin.stat" in task)
        self.assertEqual(["root_user", "root_password", "project_password", "audit_password"], stat_task["loop"])

    def test_the_project_user_is_not_the_root_name_and_the_policy_is_not_a_built_in_one(self) -> None:
        variables = group_variables()
        self.assertNotIn(str(variables["compose_stack_aistor_project_user"]).lower(), {"minioadmin", "root", "admin"})
        self.assertNotIn(variables["compose_stack_aistor_project_policy"], {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"})
        self.assertEqual("{{ compose_stack_secrets_directory }}/aistor/project_password", variables["compose_stack_aistor_project_password_file"])


class OrderTest(unittest.TestCase):
    def test_the_password_file_exists_before_the_aistor_step_reads_it_and_openbao_is_matched_before_that_step_logs_in_to_it(self) -> None:
        self.assertIn("aistor_credentials.yml", includes(), "the host playbook makes the credential files; the AIStor playbook runs after it")
        self.assertNotIn("aistor_project_user.yml", includes(), "the AIStor step is not part of the host playbook")
        self.assertIn(
            "openbao/bootstrap.yml",
            [f"{role['name']}/{role['tasks_from']}" for role in roles_of("main.yml")],
            "the admin role and its secret ID file are made in the host play",
        )
        self.assertIn("aistor_project_user.yml", [task.get("ansible.builtin.include_tasks") for task in StoragePlaybookTest().tasks()])

    def test_the_aistor_step_checks_the_licence_then_starts_aistor_then_makes_the_project_user_and_nothing_of_it_is_in_the_host_tasks(self) -> None:
        order = [task.get("ansible.builtin.include_tasks") for task in StoragePlaybookTest().tasks()]
        self.assertEqual(["aistor_licence.yml", "start.yml", "aistor_project_user.yml"], [name for name in order if name])
        starts = [task for task in StoragePlaybookTest().tasks() if task.get("ansible.builtin.include_tasks") == "start.yml"]
        self.assertEqual("{{ compose_stack_storage_services }}", starts[0]["vars"]["compose_stack_start_services"])
        for name in ("aistor_licence.yml", "aistor_project_user.yml"):
            self.assertNotIn(name, includes())
        self.assertEqual(
            [],
            [task for task in tasks_of("main.yml") if (task.get("vars") or {}).get("compose_stack_start_services") == "{{ compose_stack_storage_services }}"],
        )

    def test_the_bootstrap_task_is_hidden_names_the_passwords_by_file_only_and_gives_the_bucket_from_the_launcher(self) -> None:
        (task,) = [task for task in tasks_of("aistor_project_user.yml") if "aistor_project_user" in task]
        self.assertIs(True, task["no_log"])
        self.assertEqual(
            {
                "compose_command",
                "service",
                "user",
                "policy",
                "bucket",
                "password_file",
                "audit_user",
                "audit_policy",
                "audit_password_file",
                "marker_file",
                "openbao_address",
                "openbao_secret_id_file",
                "openbao_path",
                "audit_openbao_path",
                "repair",
            },
            set(task["aistor_project_user"]),
        )
        params = task["aistor_project_user"]
        self.assertEqual("{{ aistor_bucket }}", params["bucket"])
        self.assertEqual("{{ compose_stack_aistor_project_password_file }}", params["password_file"])
        self.assertEqual("{{ compose_stack_aistor_audit_password_file }}", params["audit_password_file"])
        self.assertEqual("{{ aistor_repair_once | default(false) | bool }}", params["repair"])
        self.assertEqual("{{ compose_stack_aistor_bootstrap_marker }}", params["marker_file"])
        self.assertEqual("{{ compose_stack_secrets_directory }}/openbao/bootstrap.secret_id", params["openbao_secret_id_file"])
        self.assertEqual("{{ compose_stack_aistor_openbao_path }}", params["openbao_path"])
        self.assertEqual("{{ compose_stack_aistor_audit_openbao_path }}", params["audit_openbao_path"])
        self.assertNotIn("password:", str(params).replace("password_file", "").replace("root_repair", ""))

    def repair(self, **facts: Any) -> bool:
        """What the task gives the module's ``repair``, evaluated as Ansible evaluates the template."""
        (task,) = [task for task in tasks_of("aistor_project_user.yml") if "aistor_project_user" in task]
        environment = Environment(autoescape=True)
        environment.filters["bool"] = lambda value: str(value).lower() in ("true", "yes", "1")
        return environment.from_string(task["aistor_project_user"]["repair"]).render(**facts) == "True"

    def test_the_root_repair_is_off_unless_the_one_run_names_it_so_the_root_pair_is_never_used_again_without_the_owner(self) -> None:
        self.assertIs(False, self.repair())
        self.assertIs(False, self.repair(aistor_repair_once=False))
        self.assertIs(False, self.repair(aistor_repair_once="false"))
        self.assertIs(True, self.repair(aistor_repair_once=True))
        self.assertNotIn("compose_stack_aistor_rotate_password", group_variables())

    def test_no_inventory_file_holds_the_approval_so_it_cannot_stay_true_for_later_deploys(self) -> None:
        variables = group_variables()
        for name in ("compose_stack_aistor_root_repair", "aistor_repair_once"):
            self.assertNotIn(name, variables)
        holders = [
            str(path.relative_to(ROOT))
            for path in (ROOT / "ansible").rglob("*")
            if path.is_file() and path.suffix in (".yml", ".yaml", ".ini", ".cfg", ".json") and "aistor_repair_once" in path.read_text()
        ]
        self.assertEqual(["ansible/roles/compose_stack/tasks/aistor_project_user.yml"], holders, "only the task that gives it to the module names it")

    def test_an_inventory_value_of_the_old_approval_is_refused_by_name_before_the_module_runs(self) -> None:
        tasks = tasks_of("aistor_project_user.yml")
        guard = tasks[0]
        self.assertIn("ansible.builtin.assert", guard)
        self.assertLess(0, len([task for task in tasks if "aistor_project_user" in task]))
        self.assertIs(True, evaluate_assertions(guard))
        for value in (True, False, "true"):
            with self.subTest(value=value):
                self.assertIs(False, evaluate_assertions(guard, compose_stack_aistor_root_repair=value), "any value of it is refused, even false")
        message = guard["ansible.builtin.assert"]["fail_msg"]
        self.assertIn("compose_stack_aistor_root_repair", message)
        self.assertIn("make host-up", message)
        self.assertIn("AISTOR_REPAIR=1", message)

    def test_the_marker_is_a_root_only_file_beside_the_state_file_and_the_audit_identity_has_names_and_paths_of_its_own(self) -> None:
        variables = group_variables()
        self.assertEqual("{{ compose_stack_compose_directory }}/aistor.bootstrap", variables["compose_stack_aistor_bootstrap_marker"])
        self.assertNotEqual(variables["compose_stack_aistor_project_user"], variables["compose_stack_aistor_audit_user"])
        self.assertNotEqual(variables["compose_stack_aistor_project_policy"], variables["compose_stack_aistor_audit_policy"])
        self.assertNotIn(str(variables["compose_stack_aistor_audit_user"]).lower(), {"minioadmin", "root", "admin"})
        self.assertNotIn(variables["compose_stack_aistor_audit_policy"], {"consoleAdmin", "readwrite", "readonly", "writeonly", "diagnostics"})
        self.assertEqual("{{ compose_stack_secrets_directory }}/aistor/audit_password", variables["compose_stack_aistor_audit_password_file"])

    def test_the_audit_sign_in_is_at_a_path_that_the_aistor_services_role_cannot_read_and_that_terraforms_identity_never_names(self) -> None:
        variables = group_variables()
        self.assertEqual("secret/data/services/{{ compose_stack_aistor_service }}/audit", variables["compose_stack_aistor_audit_openbao_path"])
        self.assertNotEqual(variables["compose_stack_aistor_openbao_path"], variables["compose_stack_aistor_audit_openbao_path"])
        from scripts.infra import storage

        self.assertNotIn("audit", Path(storage.__file__).read_text().lower(), "the storage command and Terraform sign in as the bucket-only project user")
        for name, text in repository_files().items():
            if name.startswith("infra/storage/") and name.endswith(".tf"):
                self.assertNotIn("audit", text.lower(), name)

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


class StoragePlaybookTest(unittest.TestCase):
    """The AIStor step skips AIStor, with a recorded state, when the licence is absent, and starts it when the licence is on the host."""

    def tasks(self) -> list[dict[str, Any]]:
        """The tasks of the one block that holds the step."""
        (block,) = tasks_of("storage.yml")
        return list(block["block"])

    def rescue(self) -> list[dict[str, Any]]:
        (block,) = tasks_of("storage.yml")
        return list(block["rescue"])

    def test_the_playbook_is_one_play_on_the_platform_that_includes_the_role_file_of_the_aistor_step(self) -> None:
        document = load_yaml((ROOT / "ansible" / "storage.yml").read_text())
        self.assertIsInstance(document, list)
        (play,) = document if isinstance(document, list) else []
        self.assertEqual(("platform", True, False), (play["hosts"], play["become"], play["gather_facts"]))
        (task,) = play["tasks"]
        self.assertEqual({"name": "compose_stack", "tasks_from": "storage.yml"}, task["ansible.builtin.include_role"])

    def test_the_host_playbook_has_no_aistor_start_so_a_missing_licence_stops_nothing_before_it(self) -> None:
        text = (ROOT / "ansible" / "host.yml").read_text()
        self.assertNotIn("storage.yml", text)
        self.assertNotIn("aistor", text)

    def test_every_step_after_the_licence_check_runs_only_when_the_licence_file_is_present_and_the_skip_runs_only_when_it_is_not(self) -> None:
        by_include = {task.get("ansible.builtin.include_tasks"): task for task in self.tasks() if task.get("ansible.builtin.include_tasks")}
        self.assertNotIn("when", by_include["aistor_licence.yml"])
        for name in ("start.yml", "aistor_project_user.yml"):
            self.assertEqual("compose_stack_aistor_licence_present", by_include[name]["when"], name)
        skip = next(task for task in self.tasks() if "ansible.builtin.copy" in task and "skipped" in str(task))
        self.assertEqual("not compose_stack_aistor_licence_present", skip["when"])

    def test_the_state_file_records_skipped_or_started_in_a_file_that_anyone_may_read_and_only_root_writes_and_holds_no_secret(self) -> None:
        records = [task for task in self.tasks() if "ansible.builtin.copy" in task]
        self.assertEqual(2, len(records))
        contents = {task["when"]: task["ansible.builtin.copy"] for task in records}
        skipped, started = contents["not compose_stack_aistor_licence_present"], contents["compose_stack_aistor_licence_present"]
        self.assertTrue(skipped["content"].startswith("skipped"))
        self.assertTrue(started["content"].startswith("started"))
        for record in (skipped, started):
            self.assertEqual("{{ compose_stack_aistor_state_file }}", record["dest"])
            self.assertEqual("0644", record["mode"])
            self.assertEqual("{{ compose_stack_owner }}", record["owner"])
        self.assertEqual("{{ compose_stack_compose_directory }}/aistor.state", group_variables()["compose_stack_aistor_state_file"])

    def test_the_started_record_follows_the_project_user_so_a_failed_step_never_leaves_started_behind(self) -> None:
        order = [
            task.get("ansible.builtin.include_tasks") or "copy " + str(task["ansible.builtin.copy"]["content"]).split(":")[0].strip()
            for task in self.tasks()
            if task.get("ansible.builtin.include_tasks") or "ansible.builtin.copy" in task
        ]
        self.assertEqual(["aistor_licence.yml", "copy skipped", "start.yml", "aistor_project_user.yml", "copy started"], [name.strip() for name in order])

    def test_a_step_that_fails_removes_its_record_and_stops_with_a_fixed_message_so_no_earlier_claim_stays(self) -> None:
        remove, stop = self.rescue()
        self.assertEqual("absent", remove["ansible.builtin.file"]["state"])
        self.assertEqual("{{ compose_stack_aistor_state_file }}", remove["ansible.builtin.file"]["path"])
        self.assertEqual("ansible.builtin.fail", next(key for key in stop if key.startswith("ansible.")))
        self.assertNotIn("{{", stop["ansible.builtin.fail"]["msg"], "no variable of the failure is in the message")

    def test_the_licence_script_is_installed_by_the_host_playbook_with_the_sudoers_drop_in_checked_by_visudo(self) -> None:
        names = includes()
        self.assertLess(names.index("aistor_credentials.yml"), names.index("aistor_licence_script.yml"))
        tasks = tasks_of("aistor_licence_script.yml")
        copies = {task["ansible.builtin.copy"]["dest"]: task["ansible.builtin.copy"] for task in tasks if "ansible.builtin.copy" in task}
        script = copies["{{ compose_stack_aistor_licence_script }}"]
        self.assertEqual(("0700", "{{ role_path }}/files/aistor_licence_install.py"), (script["mode"], script["src"]))
        sudoers = copies["{{ compose_stack_aistor_licence_sudoers }}"]
        self.assertEqual(("0440", "{{ compose_stack_visudo }} -cf %s"), (sudoers["mode"], sudoers["validate"]))
        helper = copies["{{ compose_stack_bin_directory }}/secret_files.py"]
        self.assertEqual("0600", helper["mode"])


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
