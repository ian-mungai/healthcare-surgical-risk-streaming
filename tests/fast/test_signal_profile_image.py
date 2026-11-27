"""The Spark image holds every project module that the signal profile job imports, and the job's Python worker can import the counting of the unscored cutoffs.

Failure modes covered here:

- A module that the job imports, directly or through another, is left out of the image or of the build context, so the job stops at start (or a Python worker
  stops on its first case) and only a build and a run would show it. The imports are followed with ``ast`` from the job's own sources.
- The unscored count, the warning grid rule or the registry of the feature limits imports a package that the image does not hold: they may use the standard
  library, and the unscored count may also import ``features`` and the grid rule.
"""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

from tests.fast.test_replay_image import ignore_rules, imported_files
from tests.fast.test_spark_bronze_image import DOCKERFILE, IGNORE_FILE, image_problems

ROOT = Path(__file__).resolve().parents[2]
LIMIT_MODULES = ("features/registry.py", "features/model.py", "features/stats.py")


def profile_sources() -> list[str]:
    return sorted(str(path.relative_to(ROOT)) for path in (ROOT / "spark" / "profile").glob("*.py")) + ["spark/jobs/signal_profile.py"]


class Image(unittest.TestCase):
    def test_every_module_that_the_job_imports_is_in_the_image(self) -> None:
        needed = imported_files(profile_sources())
        for path in (*LIMIT_MODULES, "features/signal_validity.py", "features/__init__.py", "spark/jobs/patient_split.py", "spark/silver/frames.py"):
            self.assertIn(path, needed)
        self.assertEqual([], image_problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), needed))

    def test_a_module_left_out_of_the_image_or_the_context_is_named(self) -> None:
        needed = imported_files(profile_sources())
        text, rules = DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text())
        for path in LIMIT_MODULES:
            with self.subTest(ignored=path):
                self.assertIn(f"{path}: the build's ignore file leaves it out", image_problems(text, [*rules, path], needed))
            with self.subTest(uncopied=path):
                without = "\n".join(line for line in text.splitlines() if path not in line)
                self.assertIn(f"{path}: no COPY line puts it at /opt/jobs/{path}", image_problems(without, rules, needed))

    def test_the_limits_and_the_count_use_the_standard_library_and_the_project_only(self) -> None:
        allowed = {"spark/profile/unscored.py": {"features", "spark.profile.grid_rule"}}
        for path in (*LIMIT_MODULES, "spark/profile/unscored.py", "spark/profile/grid_rule.py"):
            tree = ast.parse((ROOT / path).read_text())
            imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module} | {
                alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
            }
            project = allowed.get(path, {"features"} if path in LIMIT_MODULES else set())
            with self.subTest(path=path):
                self.assertEqual(
                    set(),
                    {
                        name
                        for name in imported
                        if name.split(".")[0] not in sys.stdlib_module_names and name not in project and name.split(".")[0] not in project
                    },
                )


if __name__ == "__main__":
    unittest.main()
