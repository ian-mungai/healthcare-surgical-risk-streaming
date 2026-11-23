"""Tasks delegated to the controller use a temporary folder the project owns.

``local_tmp`` in ``ansible.cfg`` covers the controller's own files, but a task with ``delegate_to: localhost`` runs as a remote task on the
controller and uses ``remote_tmp``, which defaults to ``~/.ansible/tmp``. On a machine where that folder is not writable, the live host setup
stopped as unreachable at the first delegated task (Nov 23 2026). The inventory names ``localhost`` with a temporary folder under ``.tools``.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from scripts.stack.yaml_text import load_yaml

ANSIBLE = Path(__file__).resolve().parents[2] / "ansible"


def localhost() -> dict[str, object]:
    document = load_yaml((ANSIBLE / "inventory.yml").read_text())
    if not isinstance(document, dict):
        raise TypeError("the inventory must be a mapping")
    host = document["all"]["hosts"].get("localhost")
    if not isinstance(host, dict):
        raise AssertionError("the inventory names no localhost entry, so delegated tasks fall back to ~/.ansible/tmp")
    return host


class LocalTemporaryFolderTest(unittest.TestCase):
    def test_delegated_tasks_run_locally_with_a_temporary_folder_under_the_project_tools(self) -> None:
        host = localhost()
        self.assertEqual("local", host["ansible_connection"])
        self.assertIn(".tools/ansible/", str(host["ansible_remote_tmp"]))
        self.assertNotIn("~", str(host["ansible_remote_tmp"]))

    def test_no_play_runs_on_localhost_itself(self) -> None:
        for path in sorted(ANSIBLE.glob("*.yml")):
            plays = load_yaml(path.read_text())
            if not isinstance(plays, list):
                continue
            for play in plays:
                if isinstance(play, dict) and "hosts" in play:
                    with self.subTest(playbook=path.name):
                        self.assertNotIn(play["hosts"], ("all", "localhost"))


if __name__ == "__main__":
    unittest.main()
