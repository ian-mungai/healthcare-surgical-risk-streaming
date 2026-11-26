"""No ``stat`` task of a secret file computes a checksum or a content type: a checksum is a value of the secret, and a verbose run prints the result of a stat.

Ansible's ``stat`` computes a SHA-1 of a readable file unless ``get_checksum`` is false, and the joined stack runner runs the deploy with ``-v``. The rule is
closed: every ``stat`` task of every tracked playbook and task file (``.yml`` and ``.yaml``) sets both options to false, except the ones listed here, each of
which reads no secret. A new ``stat`` task therefore sets both options or is added here by a change that a review sees. A task names the module as its key or
through ``action`` or ``local_action``; its options count only as a mapping, so a ``stat`` that gives them as a text never passes. A mapping with a nested
``args``, or with arguments in its ``module`` text, never passes either: Ansible lets both replace the options beside them. The task's own ``args`` keyword
only adds options that the module's mapping does not set. The verbose runs of the secret checks are in ``tests/slow``.
"""

from __future__ import annotations

import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
ANSIBLE = ROOT / "ansible"
STAT = ("ansible.builtin.stat", "ansible.legacy.stat", "stat")
# The task keys that name their module in their value: as the first word of a text, or as the first word of the ``module`` entry of a mapping.
NAMED_IN_VALUE = ("action", "local_action")
# The stat tasks that read no secret, as (file under ansible/, task name): public configuration, the Docker socket and the runner checkout, which holds none.
NO_SECRET = {
    ("roles/airbyte/tasks/release.yml", "Read the checksum of each values file"),
    ("roles/compose_stack/tasks/render_environment.yml", "Read the group of the Docker socket"),
    ("roles/docker/tasks/main.yml", "Read when the log settings file was written"),
    ("roles/runner_checkout/tasks/archive.yml", "Look at the new folder without following a link"),
    ("roles/runner_checkout/tasks/finish.yml", "Look at the folder itself and the Git folder"),
    ("roles/runner_checkout/tasks/folders.yml", "Look at the runner folder without following a link"),
    ("roles/runner_checkout/tasks/folders.yml", "Look at the runner folder and the commit's folder without following a link"),
    ("roles/runner_checkout/tasks/folders.yml", "Look for the finished marker of this commit's folder"),
    ("roles/runner_checkout/tasks/python.yml", "Look at the published name of the installed interpreter without following a link"),
    ("roles/runner_checkout/tasks/python.yml", "Look at the folder that the link names without following a link"),
    ("roles/runner_checkout/tasks/python.yml", "Look for the marker that names the archive of the installed interpreter"),
    ("roles/runner_checkout/tasks/python.yml", "Look at the folder of the interpreters without following a link"),
}


def mappings(node: Any) -> Iterator[dict[str, Any]]:
    """Every mapping of a document at any depth: a task of a play, of a block, of a rescue or of a handler list is one of them."""
    if isinstance(node, dict):
        yield node
    for value in node.values() if isinstance(node, dict) else node if isinstance(node, list) else []:
        yield from mappings(value)


def stat_options(task: dict[str, Any]) -> Iterator[Any]:
    """The options of each ``stat`` that a mapping runs."""
    yield from (task[key] for key in STAT if key in task)
    for value in (task[key] for key in NAMED_IN_VALUE if key in task):
        named = value.get("module") if isinstance(value, dict) else value
        if isinstance(named, str) and any(word in STAT for word in named.split()[:1]):
            yield value


def stat_tasks(text: str) -> list[tuple[str, Any]]:
    return [(str(task.get("name")), options) for task in mappings(load_yaml(text)) for options in stat_options(task)]


def computes_nothing(options: Any) -> bool:
    """True only for a mapping that sets both options to false and holds nothing that Ansible's parser lets replace them: no nested ``args``, and no text
    after the module's name in ``module``."""
    if not isinstance(options, dict) or "args" in options or len(str(options.get("module", "")).split()) > 1:
        return False
    return options.get("get_checksum") is False and options.get("get_mime") is False


class SecretStatTest(unittest.TestCase):
    def found(self, folder: Path = ANSIBLE) -> dict[tuple[str, str], list[Any]]:
        tasks: dict[tuple[str, str], list[Any]] = {}
        for path in sorted(path for suffix in ("*.yml", "*.yaml") for path in folder.rglob(suffix)):
            for name, options in stat_tasks(path.read_text()):
                tasks.setdefault((path.relative_to(folder).as_posix(), name), []).append(options)
        return tasks

    def test_every_stat_task_but_the_listed_ones_that_read_no_secret_computes_no_checksum_and_no_content_type(self) -> None:
        wrong = sorted(key for key, options in self.found().items() if key not in NO_SECRET and not all(map(computes_nothing, options)))
        self.assertEqual([], wrong)

    def test_each_listed_exception_is_one_stat_task_that_is_still_there(self) -> None:
        found = self.found()
        self.assertEqual([], sorted(key for key in NO_SECRET if len(found.get(key, [])) != 1))

    def test_each_secret_check_is_among_the_tasks_that_the_rule_reads(self) -> None:
        files = {file for file, _ in self.found()}
        for file in (
            "roles/compose_stack/tasks/aistor_licence.yml",
            "roles/compose_stack/tasks/verify_aistor_credentials.yml",
            "roles/compose_stack/tasks/verify_grafana_password.yml",
            "roles/compose_stack/tasks/verify_secrets.yml",
            "roles/openbao/tasks/verify_secrets.yml",
            "roles/unlock_key/tasks/verify_file.yml",
            "roles/workload_access/tasks/main.yml",
        ):
            with self.subTest(file=file):
                self.assertIn(file, files)

    def test_a_planted_task_in_a_block_with_either_option_missing_or_not_false_is_found(self) -> None:
        for options in ("{path: /x}", "{path: /x, get_checksum: false}", "{path: /x, get_mime: false}", "{path: /x, get_checksum: 'no', get_mime: false}"):
            with self.subTest(options=options):
                planted = f"- block:\n    - name: x\n      ansible.builtin.stat: {options}\n      register: found\n"
                self.assertEqual([False], [computes_nothing(found) for _, found in stat_tasks(planted)])

    def planted_forms(self, options: dict[str, str]) -> Iterator[tuple[str, str]]:
        """A task that runs ``stat`` through ``action`` or ``local_action``, as a text and as a mapping, under each name of the module."""
        words = "".join(f" {key}={value}" for key, value in options.items())
        entries = "".join(f", {key}: {value}" for key, value in options.items())
        for key in ("action", "local_action"):
            for module in ("ansible.builtin.stat", "ansible.legacy.stat", "stat"):
                for form, value in (("text", f"{module} path=/x{words}"), ("mapping", f"{{module: {module}, path: /x{entries}}}")):
                    yield form, f"- block:\n    - name: x\n      {key}: {value}\n      register: found\n"

    def test_a_planted_stat_through_action_or_local_action_without_both_options_is_found(self) -> None:
        for options in ({}, {"get_checksum": "false"}, {"get_mime": "false"}):
            for _, planted in self.planted_forms(options):
                with self.subTest(planted=planted):
                    self.assertEqual([False], [computes_nothing(found) for _, found in stat_tasks(planted)])

    def test_a_stat_through_action_passes_only_as_a_mapping_with_both_options_false(self) -> None:
        for form, planted in self.planted_forms({"get_checksum": "false", "get_mime": "false"}):
            with self.subTest(planted=planted):
                self.assertEqual([form == "mapping"], [computes_nothing(found) for _, found in stat_tasks(planted)])

    def test_a_stat_with_arguments_in_its_module_text_or_in_a_nested_args_never_passes(self) -> None:
        safe = "path: /x, get_checksum: false, get_mime: false"
        override = "{get_checksum: true}"
        values = [
            (key, value)
            for key in ("action", "local_action")
            for value in (
                f"{{module: stat get_checksum=true, {safe}}}",
                f"{{module: ansible.builtin.stat get_mime=true, {safe}}}",
                f"{{module: stat, {safe}, args: {override}}}",
                f"{{module: stat, {safe}, args: get_checksum=true}}",
                'stat path=/x get_checksum=false get_mime=false args="get_checksum=true"',
                "stat path=/x get_checksum=true",
            )
        ]
        values += [(module, f"{{{safe}, args: {override}}}") for module in ("ansible.builtin.stat", "ansible.legacy.stat", "stat")]
        for key, value in values:
            with self.subTest(key=key, value=value):
                planted = f"- block:\n    - name: x\n      {key}: {value}\n      args: {{get_checksum: false, get_mime: false}}\n"
                self.assertEqual([False], [computes_nothing(found) for _, found in stat_tasks(planted)])

    def test_another_module_through_action_or_local_action_is_not_a_stat_task(self) -> None:
        for value in ("ansible.builtin.command stat /x", "{module: ansible.builtin.command, cmd: stat /x}", "{path: /x}", "''"):
            for key in ("action", "local_action"):
                with self.subTest(key=key, value=value):
                    self.assertEqual([], stat_tasks(f"- name: x\n  {key}: {value}\n"))

    def test_a_task_file_with_either_yaml_suffix_is_read(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            for suffix in ("yml", "yaml"):
                tasks = Path(folder) / "roles" / "planted" / "tasks" / f"main.{suffix}"
                tasks.parent.mkdir(parents=True, exist_ok=True)
                tasks.write_text("- name: x\n  ansible.builtin.stat: {path: /x}\n")
            self.assertEqual({("roles/planted/tasks/main.yml", "x"), ("roles/planted/tasks/main.yaml", "x")}, set(self.found(Path(folder))))


if __name__ == "__main__":
    unittest.main()
