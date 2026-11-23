"""No two tasks of a role or a playbook register the same variable.

Ansible stores a result under ``register`` even when the task is skipped, so a skipped second task that registers the same name replaces the first
result with one that has no ``stdout``, and the next condition that reads it fails. The live host's k3s role met this: on a cluster that was already
encrypted, the skipped re-read inside the enable block replaced the first status, and the rotation's condition failed on the missing ``stdout``.
"""

from __future__ import annotations

import collections
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
ANSIBLE = ROOT / "ansible"
SECTIONS = ("block", "rescue", "always")
PLAY_SECTIONS = ("pre_tasks", "tasks", "post_tasks", "handlers")


def registers(tasks: object, where: str, found: dict[str, list[str]]) -> None:
    """Add each ``register`` name in ``tasks``, with the blocks' tasks, to ``found`` with the place that registers it."""
    for task in tasks if isinstance(tasks, list) else []:
        if not isinstance(task, dict):
            continue
        if "register" in task:
            found[str(task["register"])].append(f"{where}: {task.get('name', '(unnamed)')}")
        for section in SECTIONS:
            registers(task.get(section), where, found)


def repeated(found: dict[str, list[str]]) -> dict[str, list[str]]:
    return {name: places for name, places in found.items() if len(places) > 1}


def role_registers(role: Path) -> dict[str, list[str]]:
    found: dict[str, list[str]] = collections.defaultdict(list)
    for folder in ("tasks", "handlers"):
        for path in sorted((role / folder).glob("*.yml")):
            registers(load_yaml(path.read_text()), f"{folder}/{path.name}", found)
    return found


def playbook_registers(path: Path) -> dict[str, list[str]]:
    found: dict[str, list[str]] = collections.defaultdict(list)
    document: Any = load_yaml(path.read_text())
    for play in document if isinstance(document, list) else []:
        if isinstance(play, dict):
            for section in PLAY_SECTIONS:
                registers(play.get(section), path.name, found)
    return found


class RegisterNamesTest(unittest.TestCase):
    def test_each_role_registers_each_name_in_one_task(self) -> None:
        roles = sorted(path for path in (ANSIBLE / "roles").iterdir() if path.is_dir())
        self.assertTrue(roles)
        for role in roles:
            with self.subTest(role=role.name):
                self.assertEqual(repeated(role_registers(role)), {}, "a skipped task would replace the other task's result")

    def test_each_playbook_registers_each_name_in_one_task(self) -> None:
        for path in sorted(ANSIBLE.glob("*.yml")):
            with self.subTest(playbook=path.name):
                self.assertEqual(repeated(playbook_registers(path)), {}, "a skipped task would replace the other task's result")

    def test_a_planted_second_register_inside_a_block_is_found(self) -> None:
        """The control: the shape that failed on the host, a top-level read and a re-read inside a conditional block."""
        tasks = [{"name": "read", "register": "status"}, {"when": "x", "block": [{"name": "read again", "register": "status"}]}]
        found: dict[str, list[str]] = collections.defaultdict(list)
        registers(tasks, "planted.yml", found)
        self.assertEqual(repeated(found), {"status": ["planted.yml: read", "planted.yml: read again"]})
