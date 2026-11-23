"""The commit hook and ``make test-fast`` run the quick group, ``tests/fast``, with stock ``unittest`` discovery; the slow group,
``tests/slow`` (Ansible playbook runs, tests that bind a local server or start a stand-in program, and the planted workflow faults), runs
with ``make test-slow`` and in its own CI job.

The split is by folder. Accepted limit (owner, Nov 23 2026: "simplest redesign fix", after seven review rounds of an import blocker): no
check stops a quick test from importing or running slow code; the guard is the run time itself, which the hosted hooks job and the guard
harness bound with their deadlines, and review of each change that adds a test.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
QUICK_ARGUMENTS = "-m unittest discover --start-directory tests/fast --top-level-directory ."


class TestGroupsTest(unittest.TestCase):
    def test_the_commit_hook_and_make_run_the_quick_folder_only(self) -> None:
        config: Any = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
        hooks = [hook for repo in config["repos"] for hook in repo["hooks"]]
        self.assertEqual([hook["entry"] for hook in hooks if hook["id"] == "fast-tests"], [f".venv/bin/python {QUICK_ARGUMENTS}"])
        makefile = (ROOT / "Makefile").read_text()
        self.assertEqual(makefile.split("\ntest-fast:\n", 1)[1].split("\n\n", 1)[0].strip(), f"$(PYTHON) {QUICK_ARGUMENTS}")
        self.assertEqual(
            makefile.split("\ntest-slow:\n", 1)[1].split("\n\n", 1)[0].strip(),
            "$(PYTHON) -m unittest discover --start-directory tests/slow --top-level-directory .",
        )

    def test_the_slow_group_is_a_package_that_discovery_finds(self) -> None:
        self.assertTrue((ROOT / "tests" / "slow" / "__init__.py").is_file())
        self.assertTrue(any((ROOT / "tests" / "slow").glob("test_*.py")))
