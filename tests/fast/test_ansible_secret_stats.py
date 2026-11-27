"""No ``stat`` task of a secret file computes a checksum or a content type: a checksum is a value of the secret, and a verbose run prints the result of a stat.

Ansible's ``stat`` computes a SHA-1 of a readable file unless ``get_checksum`` is false, and the joined stack runner runs the deploy with ``-v``. The rule is
closed: every ``stat`` task of every tracked playbook and task file (``.yml`` and ``.yaml``) sets both options to false, except the ones listed here, each of
which reads no secret. A new ``stat`` task therefore sets both options or is added here by a change that a review sees. A task names the module as its key or
through ``action`` or ``local_action``; its options count only as a mapping, so a ``stat`` that gives them as a text never passes.

Setting both to false is enough only while nothing else can set them. The module takes an alias of an option over the option itself (``mime`` turns
``get_mime`` back on), and it reads the aliases after Ansible has merged each source of arguments. So the options and the task's own ``args`` may name only
the options and aliases of the installed module's argument spec, without the aliases of ``get_checksum`` and ``get_mime``: an alias that a later Ansible adds
to either is refused too. This also refuses a nested ``args``, the internal keys through which Ansible adds templated arguments and a templated name. Both
sources must be mappings, as a template in the place of ``args`` can add any name, and ``module`` holds no arguments after the module's name. No
``module_defaults``, wherever it is set, names the module or an action group, which can hold it: no file is followed to find the tasks that inherit it. The
verbose runs of the secret checks are in ``tests/slow``.
"""

from __future__ import annotations

import contextlib
import importlib
import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
ANSIBLE = ROOT / "ansible"
STAT = ("ansible.builtin.stat", "ansible.legacy.stat", "stat")
# The task keys that name their module in their value: as the first word of a text, or as the first word of the ``module`` entry of a mapping.
NAMED_IN_VALUE = ("action", "local_action")
# The options that make a checksum and a content type.
COMPUTING = ("get_checksum", "get_mime")
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


# The names beside ``get_mime`` that Ansible 2.22.0 reads as it, even when ``get_mime`` is also there.
MIME_ALIASES = ("mime", "mime_type", "mime-type")
BOTH_OFF = "path: /x, get_checksum: false, get_mime: false"


def argument_sources(extra: str) -> Iterator[tuple[str, str]]:
    """A task named x with both options false and ``extra`` added, once in each place where a task can give a stat its arguments as a mapping."""
    for module in STAT:
        yield f"{module} options", f"- name: x\n  {module}: {{{BOTH_OFF}, {extra}}}\n"
        yield f"args beside {module}", f"- name: x\n  {module}: {{{BOTH_OFF}}}\n  args: {{{extra}}}\n"
    for key in NAMED_IN_VALUE:
        yield f"{key} options", f"- name: x\n  {key}: {{module: stat, {BOTH_OFF}, {extra}}}\n"
        yield f"args beside {key}", f"- name: x\n  {key}: {{module: stat, {BOTH_OFF}}}\n  args: {{{extra}}}\n"


class SpecRead(Exception):
    """Ends the ``main`` of Ansible's ``stat`` module at its call of ``AnsibleModule``, whose arguments are then known."""


def plain_names() -> frozenset[str]:
    """The names that an argument of a ``stat`` may have: each option of the installed module's argument spec and its aliases, without the aliases of the two
    options that compute."""
    module: Any = importlib.import_module("ansible.modules.stat")
    with mock.patch.object(module, "AnsibleModule", side_effect=SpecRead) as made, contextlib.suppress(SpecRead):
        module.main()
    spec: dict[str, dict[str, Any]] = made.call_args.kwargs["argument_spec"]
    return frozenset(name for option, entry in spec.items() for name in (option, *(() if option in COMPUTING else entry.get("aliases", ()))))


PLAIN_NAMES = plain_names()


def mappings(node: Any) -> Iterator[dict[str, Any]]:
    """Every mapping of a document at any depth: a task of a play, of a block, of a rescue or of a handler list is one of them. What ``module_defaults``
    holds is no task (``reaching_defaults`` reads it)."""
    if isinstance(node, dict):
        yield node
    values = [value for key, value in node.items() if key != "module_defaults"] if isinstance(node, dict) else node if isinstance(node, list) else []
    for value in values:
        yield from mappings(value)


def reaching_defaults(node: dict[str, Any]) -> Iterator[str]:
    """What a mapping's ``module_defaults`` holds that can give a ``stat`` its arguments: a name of the module, an action group, or a part that is no mapping,
    whose modules cannot be read."""
    value = node.get("module_defaults")
    for defaults in value if isinstance(value, list) else [] if value is None else [value]:
        if isinstance(defaults, dict):
            yield from (str(name) for name in defaults if name in STAT or str(name).startswith("group/"))
        else:
            yield "not a mapping"


def stat_options(task: dict[str, Any]) -> Iterator[Any]:
    """The options of each ``stat`` that a mapping runs."""
    yield from (task[key] for key in STAT if key in task)
    for value in (task[key] for key in NAMED_IN_VALUE if key in task):
        named = value.get("module") if isinstance(value, dict) else value
        if isinstance(named, str) and any(word in STAT for word in named.split()[:1]):
            yield value


def stat_tasks(text: str) -> list[tuple[str, tuple[Any, Any]]]:
    """Each ``stat`` of a file as (task name, (its options, the task's own ``args``))."""
    return [(str(task.get("name")), (options, task.get("args"))) for task in mappings(load_yaml(text)) for options in stat_options(task)]


def plain(arguments: Any, *others: str) -> bool:
    """True for a mapping whose names are all names that an argument may have, or among ``others``."""
    return isinstance(arguments, dict) and set(arguments) <= PLAIN_NAMES.union(others)


def computes_nothing(found: tuple[Any, Any]) -> bool:
    """True only for options that set both computations to false while neither they nor the task's ``args`` can set one again."""
    options, added = found
    if not plain(options, "module") or not (added is None or plain(added)) or len(str(options.get("module", "")).split()) > 1:
        return False
    return all(options.get(option) is False for option in COMPUTING)


def task_files(folder: Path) -> Iterator[tuple[str, str]]:
    """Each playbook and task file under a folder as (its path under the folder, its text)."""
    for path in sorted(path for suffix in ("*.yml", "*.yaml") for path in folder.rglob(suffix)):
        yield path.relative_to(folder).as_posix(), path.read_text()


class SecretStatTest(unittest.TestCase):
    def found(self, folder: Path = ANSIBLE) -> dict[tuple[str, str], list[tuple[Any, Any]]]:
        tasks: dict[tuple[str, str], list[tuple[Any, Any]]] = {}
        for file, text in task_files(folder):
            for name, found in stat_tasks(text):
                tasks.setdefault((file, name), []).append(found)
        return tasks

    def refused(self, folder: Path = ANSIBLE) -> list[tuple[str, str]]:
        """What the rule refuses in the files of a folder, each as (file under the folder, name)."""
        tasks = [key for key, runs in self.found(folder).items() if key not in NO_SECRET and not all(map(computes_nothing, runs))]
        defaults = [
            (file, f"module_defaults: {entry}") for file, text in task_files(folder) for node in mappings(load_yaml(text)) for entry in reaching_defaults(node)
        ]
        return sorted(tasks + defaults)

    def refused_of(self, text: str) -> list[str]:
        """The names of what the rule refuses in one planted task file."""
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "planted.yaml").write_text(text)
            return [name for _, name in self.refused(Path(folder))]

    def test_every_stat_task_but_the_listed_ones_computes_no_checksum_or_content_type_and_no_module_defaults_reaches_one(self) -> None:
        self.assertEqual([], self.refused())

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

    def test_an_alias_of_the_content_type_option_is_refused_beside_the_options_and_in_the_args_of_the_task(self) -> None:
        for alias in MIME_ALIASES:
            for source, planted in argument_sources(f"{alias}: true"):
                with self.subTest(alias=alias, source=source):
                    self.assertEqual(["x"], self.refused_of(planted))

    def test_arguments_that_a_template_or_an_internal_key_can_add_are_refused(self) -> None:
        for words, extra in (
            ("variable params", "_variable_params: '{{ extra }}'"),
            ("raw params", "_raw_params: '{{ extra }}'"),
            ("a templated name", "'{{ name }}': true"),
        ):
            for source, planted in argument_sources(extra):
                with self.subTest(words, source=source):
                    self.assertEqual(["x"], self.refused_of(planted))
        for added in ("'{{ extra }}'", "get_mime=true", "[{mime: true}]"):
            for module in STAT:
                with self.subTest(added=added, module=module):
                    self.assertEqual(["x"], self.refused_of(f"- name: x\n  {module}: {{{BOTH_OFF}}}\n  args: {added}\n"))

    def test_options_and_args_that_cannot_turn_a_checksum_or_a_content_type_on_pass(self) -> None:
        for extra in (
            "follow: false",
            "get_attributes: false",
            "attr: false",
            "attributes: false",
            "get_selinux_context: false",
            "checksum_algorithm: sha256",
            "checksum: sha256",
            "checksum_algo: sha256",
            "dest: /y",
            "name: /y",
        ):
            for source, planted in argument_sources(extra):
                with self.subTest(extra=extra, source=source):
                    self.assertEqual([], self.refused_of(planted))
        for added in ("", " {get_checksum: false, get_mime: false}"):
            with self.subTest(added=added):
                self.assertEqual([], self.refused_of(f"- name: x\n  ansible.builtin.stat: {{{BOTH_OFF}}}\n  args:{added}\n"))

    def test_module_defaults_that_can_reach_a_stat_are_refused_wherever_they_are_set(self) -> None:
        task = f"    - name: y\n      ansible.builtin.stat: {{{BOTH_OFF}}}\n"
        defaults = [f"{module}: {{{alias}: true}}" for module in STAT for alias in MIME_ALIASES]
        defaults += [f"{module}: {value}" for module in STAT for value in ("{get_mime: true}", "{get_checksum: true}", "{follow: true}", "'{{ extra }}'")]
        defaults += ["group/community.general.planted: {mime: true}", "group/aws: {mime: true}"]
        for entry in defaults:
            for words, planted in (
                ("a play", f"- hosts: all\n  module_defaults:\n    {entry}\n  tasks:\n{task}"),
                ("a block, as a list", f"- module_defaults:\n    - ansible.builtin.copy: {{mode: '0600'}}\n    - {entry}\n  block:\n{task}"),
                ("a task", f"- name: y\n  ansible.builtin.stat: {{{BOTH_OFF}}}\n  module_defaults:\n    {entry}\n"),
                ("an apply", f"- ansible.builtin.include_tasks:\n    file: z.yml\n    apply:\n      module_defaults:\n        {entry}\n"),
            ):
                with self.subTest(entry=entry, where=words):
                    self.assertEqual([f"module_defaults: {entry.split(':', 1)[0]}"], self.refused_of(planted))

    def test_module_defaults_whose_modules_cannot_be_read_are_refused(self) -> None:
        for value in ("'{{ extra }}'", "['{{ extra }}']", "[[{stat: {mime: true}}]]"):
            with self.subTest(value=value):
                self.assertEqual(["module_defaults: not a mapping"], self.refused_of(f"- module_defaults: {value}\n  block: []\n"))

    def test_module_defaults_of_other_modules_pass(self) -> None:
        for value in ("{ansible.builtin.copy: {mode: '0600'}}", "[{ansible.builtin.command: {chdir: /x}}, {uri: {timeout: 5}}]", "", "[]"):
            with self.subTest(value=value):
                self.assertEqual([], self.refused_of(f"- module_defaults: {value}\n  block: []\n"))

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
