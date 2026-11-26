"""No ``stat`` task of a secret file computes a checksum or a content type: a checksum is a value of the secret, and a verbose run prints the result of a stat.

Ansible's ``stat`` computes a SHA-1 of a readable file unless ``get_checksum`` is false, and the joined stack runner runs the deploy with ``-v``. The rule is
closed: every ``stat`` task of every tracked playbook and task file sets both options to false, except the ones listed here, each of which reads no secret. A
new ``stat`` task therefore sets both options or is added here by a change that a review sees. The verbose runs of the secret checks are in ``tests/slow``.
"""

from __future__ import annotations

import unittest
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
ANSIBLE = ROOT / "ansible"
STAT = ("ansible.builtin.stat", "ansible.legacy.stat", "stat")
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


def stat_tasks(text: str) -> list[tuple[str, Any]]:
    return [(str(task.get("name")), task[key]) for task in mappings(load_yaml(text)) for key in STAT if key in task]


def computes_nothing(options: Any) -> bool:
    return isinstance(options, dict) and options.get("get_checksum") is False and options.get("get_mime") is False


class SecretStatTest(unittest.TestCase):
    def found(self) -> dict[tuple[str, str], list[Any]]:
        tasks: dict[tuple[str, str], list[Any]] = {}
        for path in sorted(ANSIBLE.rglob("*.yml")):
            for name, options in stat_tasks(path.read_text()):
                tasks.setdefault((path.relative_to(ANSIBLE).as_posix(), name), []).append(options)
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


if __name__ == "__main__":
    unittest.main()
