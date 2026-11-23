"""The commit hook runs the quick group under ``tests/fast``; the slow group under ``tests/slow`` runs with ``make test-slow`` and its own CI job.

A quick test that imports the slow group in any form would bring its playbook runs, local servers or planted loops back into every commit. Two
checks: every import statement in every quick file, inside functions and methods included, with relative and package forms resolved; and the
quick group loaded the way ``unittest discover`` does, in a fresh interpreter, for imports made while loading. Limit: a name built at run time
inside a test (``importlib.import_module`` with a computed string) is seen by neither.
"""

from __future__ import annotations

import ast
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
LOAD_SECONDS = 120
# Loads tests/fast from the given top-level folder and prints the slow-group modules it imported and the modules its tests come from.
PROBE = textwrap.dedent(
    """
    import json, sys, unittest
    top = sys.argv[1]
    sys.path.insert(0, top)
    suite = unittest.TestLoader().discover(top + "/tests/fast", top_level_dir=top)
    def cases(item):
        for child in item:
            yield from cases(child) if isinstance(child, unittest.TestSuite) else [child]
    slow = lambda name: name == "tests.slow" or name.startswith("tests.slow.")
    imported = sorted(name for name in sys.modules if slow(name))
    collected = sorted({type(case).__module__ for case in cases(suite) if slow(type(case).__module__)})
    print(json.dumps({"imported": imported, "collected": collected}))
    """
)


def slow_reach(top: Path) -> dict[str, list[str]]:
    """The slow-group modules that loading the quick group under ``top`` imports, and the slow-group modules its collected tests come from."""
    result = run_command(sys.executable, ["-I", "-c", PROBE, str(top)], cwd=top, timeout=LOAD_SECONDS)
    if result.returncode != 0:
        raise AssertionError(f"loading the quick group failed: {result.stderr[-2000:]}")
    reach = json.loads(result.stdout.strip().splitlines()[-1])
    return {"imported": [str(name) for name in reach["imported"]], "collected": [str(name) for name in reach["collected"]]}


def imported_names(path: Path, package: str) -> list[str]:
    """Every module name that an import statement anywhere in the file names, inside functions included, with relative imports resolved
    against ``package`` and ``from X import Y`` also read as ``X.Y``, since ``Y`` may be a module."""
    names: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(), str(path))):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            parts = package.split(".")
            base = ".".join(parts[: len(parts) - node.level + 1]) if node.level else ""
            module = ".".join(part for part in (base, node.module or "") if part)
            names += [module, *(f"{module}.{alias.name}" for alias in node.names)]
    return names


def slow_imports(path: Path, package: str) -> list[str]:
    return sorted({name for name in imported_names(path, package) if name == "tests.slow" or name.startswith("tests.slow.")})


class TestGroupsTest(unittest.TestCase):
    def test_the_quick_group_loads_nothing_of_the_slow_group(self) -> None:
        self.assertEqual(slow_reach(ROOT), {"imported": [], "collected": []}, "a quick test reaches the slow group, so the commit hook would run its work")

    def test_no_quick_module_names_the_slow_group_in_any_import(self) -> None:
        for path in sorted((ROOT / "tests" / "fast").rglob("*.py")):
            with self.subTest(path=path.name):
                self.assertEqual(slow_imports(path, "tests.fast"), [], f"{path.name} imports the slow group")

    def test_the_slow_group_is_a_package_that_discovery_finds(self) -> None:
        self.assertTrue((ROOT / "tests" / "slow" / "__init__.py").is_file())
        self.assertTrue(any((ROOT / "tests" / "slow").glob("test_*.py")))

    def test_each_planted_form_of_import_is_found(self) -> None:
        """The control: in a copy with a slow module, each import form in a quick module is seen as an import, and a planted class as collected."""
        forms = {
            "absolute": "import tests.slow.test_planted_slow\n",
            "from absolute": "from tests.slow.test_planted_slow import VALUE\n",
            "from package": "from tests import slow\n",
            "relative": "from ..slow.test_planted_slow import PlantedSlowTest\n",
            "run time": "import importlib\nimportlib.import_module('tests.slow.test_planted_slow')\n",
            "inside a method": "import unittest\nclass Q(unittest.TestCase):\n    def test_q(self):\n        from tests.slow.test_planted_slow import VALUE\n",
        }
        for label, source in forms.items():
            with self.subTest(form=label):
                top = Path(self.enterContext(tempfile.TemporaryDirectory()))
                for package in ("tests", "tests/fast", "tests/slow"):
                    (top / package).mkdir(parents=True, exist_ok=True)
                    (top / package / "__init__.py").write_text("")
                (top / "tests/slow/test_planted_slow.py").write_text(
                    "import unittest\nVALUE = 1\nclass PlantedSlowTest(unittest.TestCase):\n    def test_x(self):\n        pass\n"
                )
                (top / "tests/fast/test_planted_quick.py").write_text(source)
                seen_in_source = slow_imports(top / "tests/fast/test_planted_quick.py", "tests.fast") != []
                reach = slow_reach(top)
                self.assertTrue(seen_in_source or reach["imported"], f"the {label} import was not seen")
                if label != "run time":
                    self.assertTrue(seen_in_source, f"the source check missed the {label} import")
                if label == "relative":
                    self.assertEqual(reach["collected"], ["tests.slow.test_planted_slow"])
