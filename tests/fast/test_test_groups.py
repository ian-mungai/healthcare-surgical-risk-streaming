"""The commit hook runs the quick group under ``tests/fast``; the slow group under ``tests/slow`` runs with ``make test-slow`` and its own CI job.

A quick test that imports the slow group would bring its playbook helpers or its planted loops back into every commit, so none does.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def slow_imports(path: Path) -> list[str]:
    """The modules of the slow group that the file at ``path`` imports."""
    found: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""] if isinstance(node, ast.ImportFrom) else []
        found += [name for name in names if name == "tests.slow" or name.startswith("tests.slow.")]
    return found


class TestGroupsTest(unittest.TestCase):
    def test_no_quick_test_imports_the_slow_group(self) -> None:
        for path in sorted((ROOT / "tests" / "fast").glob("*.py")):
            with self.subTest(path=path.name):
                self.assertEqual(slow_imports(path), [], f"{path.name} imports the slow group, so the commit hook would run its work")

    def test_the_slow_group_is_a_package_that_discovery_finds(self) -> None:
        self.assertTrue((ROOT / "tests" / "slow" / "__init__.py").is_file())
        self.assertTrue(any((ROOT / "tests" / "slow").glob("test_*.py")))

    def test_a_planted_import_of_the_slow_group_is_found(self) -> None:
        """The control: each import form that reaches the slow group is named."""
        for source in ("import tests.slow.ansible_support\n", "from tests.slow.ansible_support import ROLES\n", "from tests.slow import ansible_support\n"):
            with self.subTest(source=source):
                path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "planted.py"
                path.write_text(source)
                self.assertNotEqual(slow_imports(path), [])
