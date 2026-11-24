"""The Spark image holds every project module that the Silver signal job imports, and the job's Python worker can import the validity definition.

Failure modes covered here:

- A module that the job imports, directly or through another, is left out of the image or of the build context, so the job stops at start (or a Python worker
  stops on its first row) and only a build and a run would show it. The imports are followed with ``ast`` from the job's own sources.
- The validity definition imports a package that the image does not hold, so the worker cannot load it: it may use the standard library only.
"""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

from tests.fast.test_replay_image import ignore_rules, imported_files
from tests.fast.test_spark_bronze_image import DOCKERFILE, IGNORE_FILE, image_problems

ROOT = Path(__file__).resolve().parents[2]


def silver_sources() -> list[str]:
    return sorted(str(path.relative_to(ROOT)) for path in (ROOT / "spark" / "silver").glob("*.py")) + ["spark/jobs/silver_signals.py"]


class Image(unittest.TestCase):
    def test_every_module_that_the_job_imports_is_in_the_image(self) -> None:
        needed = imported_files(silver_sources())
        for path in ("features/signal_validity.py", "features/__init__.py", "spark/jobs/patient_split.py", "spark/split_table.py", "scripts/stack/nessie.py"):
            self.assertIn(path, needed)
        self.assertEqual([], image_problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), needed))

    def test_a_module_left_out_of_the_image_or_the_context_is_named(self) -> None:
        needed = imported_files(silver_sources())
        text, rules = DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text())
        for path in ("features/signal_validity.py", "features/__init__.py"):
            with self.subTest(ignored=path):
                self.assertIn(f"{path}: the build's ignore file leaves it out", image_problems(text, [*rules, path], needed))
            with self.subTest(uncopied=path):
                without = "\n".join(line for line in text.splitlines() if path not in line)
                self.assertIn(f"{path}: no COPY line puts it at /opt/jobs/{path}", image_problems(without, rules, needed))

    def test_the_validity_definition_uses_the_standard_library_and_itself_only(self) -> None:
        tree = ast.parse((ROOT / "features" / "signal_validity.py").read_text())
        imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module} | {
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        }
        self.assertEqual(set(), {name for name in imported if name.split(".")[0] not in sys.stdlib_module_names})


if __name__ == "__main__":
    unittest.main()
