"""The commit hook runs the quick group under ``tests/fast`` through ``tests/run_quick.py``, which refuses any import of the slow group.

A quick test that reaches ``tests/slow`` in any form would bring its playbook runs, local servers or planted loops back into every commit. The
runner uses Python's own import machinery: while the quick group loads and runs, a module finder refuses every ``tests.slow`` name, however
the import is written (absolute, relative, a package attribute, inside a method or built at run time).
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "run_quick.py"
RUN_SECONDS = 120
QUICK_COMMAND = ".venv/bin/python -P tests/run_quick.py"
SLOW_MODULE = "import unittest\nVALUE = 1\nclass PlantedSlowTest(unittest.TestCase):\n    def test_x(self):\n        pass\n"
QUICK_TEST = "import unittest\nclass PlantedQuickTest(unittest.TestCase):\n    def test_q(self):\n{body}"


def planted_tree(top: Path, files: dict[str, str]) -> None:
    """A copy of the runner in a tree of empty packages, with a slow module and the given quick files."""
    for package in ("tests", "tests/fast", "tests/fast/nested", "tests/slow"):
        (top / package).mkdir(parents=True, exist_ok=True)
        (top / package / "__init__.py").write_text("")
    shutil.copy(RUNNER, top / "tests" / "run_quick.py")
    (top / "tests/slow/test_planted_slow.py").write_text(SLOW_MODULE)
    for relative, text in files.items():
        (top / relative).write_bytes(text.encode("utf-8-sig") if relative.endswith("_bom.py") else text.encode())


def run_runner(top: Path) -> tuple[int, str]:
    # The hook's own form: -P keeps the script's folder off the path, so a file there cannot shadow a standard module.
    result = run_command(sys.executable, ["-P", str(top / "tests" / "run_quick.py")], cwd=top, timeout=RUN_SECONDS)
    return result.returncode, result.stdout + result.stderr


class QuickRunnerTest(unittest.TestCase):
    def test_the_commit_hook_and_make_run_the_quick_group_through_the_runner(self) -> None:
        config: Any = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
        hooks = [hook for repo in config["repos"] for hook in repo["hooks"]]
        self.assertEqual([hook["entry"] for hook in hooks if hook["id"] == "fast-tests"], [QUICK_COMMAND])
        recipe = (ROOT / "Makefile").read_text().split("\ntest-fast:\n", 1)[1].split("\n\n", 1)[0]
        self.assertEqual(recipe.strip(), "$(PYTHON) -P tests/run_quick.py")

    def test_the_slow_group_is_a_package_that_discovery_finds(self) -> None:
        self.assertTrue((ROOT / "tests" / "slow" / "__init__.py").is_file())
        self.assertTrue(any((ROOT / "tests" / "slow").glob("test_*.py")))

    def test_a_quick_group_that_imports_nothing_slow_passes(self) -> None:
        """The control: the same tree with a quick test that imports nothing of the slow group passes and runs its test."""
        top = Path(self.enterContext(tempfile.TemporaryDirectory()))
        planted_tree(top, {"tests/fast/test_planted_quick.py": QUICK_TEST.format(body="        pass\n")})
        code, output = run_runner(top)
        self.assertEqual(code, 0, output)
        self.assertIn("Ran 1 test", output)

    def test_a_helper_named_like_a_standard_module_beside_the_runner_does_not_shadow_it(self) -> None:
        top = Path(self.enterContext(tempfile.TemporaryDirectory()))
        planted_tree(top, {"tests/fast/test_planted_quick.py": QUICK_TEST.format(body="        pass\n"), "tests/pathlib.py": "raise SystemExit('shadowed')\n"})
        code, output = run_runner(top)
        self.assertEqual(code, 0, output)
        self.assertIn("Ran 1 test", output)

    def test_a_run_that_finds_no_test_fails_as_unittest_does(self) -> None:
        """unittest exits 5 when it ran no test; a quick group that loads nothing must not pass the hook."""
        empty_suite = "def load_tests(loader, tests, pattern):\n    import unittest\n    return unittest.TestSuite()\n"
        for label, files in {"no test file": {}, "a load_tests that returns nothing": {"tests/fast/test_planted_quick.py": empty_suite}}.items():
            with self.subTest(case=label):
                top = Path(self.enterContext(tempfile.TemporaryDirectory()))
                planted_tree(top, files)
                code, output = run_runner(top)
                self.assertEqual(code, 5, output)

    def test_each_form_of_slow_import_fails_the_quick_run_by_name(self) -> None:
        forms = {
            "absolute at load": {"tests/fast/test_planted_quick.py": "import tests.slow.test_planted_slow\n"},
            "package attribute": {"tests/fast/test_planted_quick.py": "from tests import slow\n"},
            "relative at load": {"tests/fast/test_planted_quick.py": "from ..slow.test_planted_slow import PlantedSlowTest\n"},
            "inside a method": {"tests/fast/test_planted_quick.py": QUICK_TEST.format(body="        from tests.slow.test_planted_slow import VALUE\n")},
            "nested relative inside a method": {
                "tests/fast/nested/test_planted_quick.py": QUICK_TEST.format(body="        from ...slow.test_planted_slow import VALUE\n")
            },
            "built at run time": {
                "tests/fast/test_planted_quick.py": QUICK_TEST.format(
                    body="        import importlib\n        importlib.import_module('.'.join(['tests', 'slow']))\n"
                )
            },
            "a file with a byte-order mark": {"tests/fast/test_planted_quick_bom.py": "import tests.slow\n"},
            "a re-export in the tests package": {"tests/__init__.py": "from tests.slow import test_planted_slow\n"},
            "discovery of the slow folder by path": {
                "tests/fast/test_planted_quick.py": (
                    "import os, unittest\n"
                    "def load_tests(loader, tests, pattern):\n"
                    "    tests.addTests(unittest.TestLoader().discover(os.path.join(os.path.dirname(__file__), '..', 'slow')))\n"
                    "    return tests\n"
                )
            },
        }
        for label, files in forms.items():
            with self.subTest(form=label):
                top = Path(self.enterContext(tempfile.TemporaryDirectory()))
                planted_tree(top, files)
                code, output = run_runner(top)
                self.assertNotEqual(code, 0, f"the {label} import passed: {output}")
                self.assertRegex(output, r"the quick group (imported tests\.slow|loaded tests/slow/test_planted_slow\.py)")
