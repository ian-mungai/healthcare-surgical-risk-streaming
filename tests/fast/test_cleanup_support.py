"""The cleanup of the signal profile acceptance runner: every step runs and the scratch folder never outlives a run.

Failure modes covered here:

- A cleanup step times out or fails (``docker ps``, ``docker rmi``) and the scratch folder is never deleted.
- Cleanup stops at the first failed step, or hides the failure, or the scrub is skipped when a step raises something that is not an ``Exception``.
- The scrub leaves a folder that has a read-only subfolder, and reports success.
"""

from __future__ import annotations

import ast
import os
import tempfile
import unittest
from collections.abc import Callable
from functools import partial
from pathlib import Path
from unittest import mock

from tests.fast.cleanup_support import run_cleanup, scrub_tree

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"


class Expired(Exception):
    """Stands for the runner's own command timeout."""


def function_of(tree: ast.AST, name: str) -> ast.FunctionDef:
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


def calls_in(nodes: list[ast.stmt]) -> set[str]:
    names: set[str] = set()
    for node in nodes:
        for part in ast.walk(node):
            if isinstance(part, ast.Call):
                names.add(part.func.id if isinstance(part.func, ast.Name) else getattr(part.func, "attr", ""))
    return names


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class RunnerShape(unittest.TestCase):
    """The runner keeps its scrub in a ``finally`` of its own and runs the other cleanup steps through ``run_cleanup``."""

    def setUp(self) -> None:
        self.tree = ast.parse(RUNNER.read_text())

    def test_the_scratch_scrub_is_the_finally_of_the_function_that_makes_the_folder(self) -> None:
        function = function_of(self.tree, "run_local")
        tries = [node for node in ast.walk(function) if isinstance(node, ast.Try)]
        self.assertEqual(1, len(tries))
        self.assertIn("scrub_tree", calls_in(tries[0].finalbody))
        self.assertIn("run_prepared", calls_in(tries[0].body))
        self.assertIn("mkdtemp", calls_in(function.body[:3]) | calls_in([node for node in function.body if isinstance(node, ast.Assign)]))

    def test_the_other_cleanup_steps_run_through_run_cleanup_and_cannot_skip_the_scrub(self) -> None:
        function = function_of(self.tree, "run_prepared")
        finals = [calls_in(node.finalbody) for node in ast.walk(function) if isinstance(node, ast.Try) and node.finalbody]
        self.assertTrue(any("run_cleanup" in names for names in finals))
        self.assertNotIn("rmtree", calls_in(function.body))


class Cleanup(unittest.TestCase):
    def test_the_scrub_runs_after_every_step_and_the_first_error_is_raised(self) -> None:
        calls: list[str] = []

        def failing() -> None:
            calls.append("failing")
            raise Expired("docker ps")

        def later() -> None:
            calls.append("later")

        with self.assertRaises(Expired):
            run_cleanup([failing, later], lambda: calls.append("scrub"))
        self.assertEqual(["failing", "later", "scrub"], calls)

    def probes(self, failing: int) -> list[Callable[[], None]]:
        """Two cleanup steps (``docker ps`` and ``docker rmi``), of which the one at position ``failing`` times out."""

        def make(position: int) -> Callable[[], None]:
            def step() -> None:
                if position == failing:
                    raise Expired("timeout")

            return step

        return [make(0), make(1)]

    def test_a_timeout_in_either_probe_leaves_no_scratch_folder(self) -> None:
        for failing_step in (0, 1):
            with self.subTest(failing_step=failing_step), tempfile.TemporaryDirectory() as parent:
                scratch = Path(parent) / "scratch"
                (scratch / "data").mkdir(parents=True)
                (scratch / "data" / "clinical_data.csv").write_text("copy")

                with self.assertRaises(Expired):
                    run_cleanup(self.probes(failing_step), partial(scrub_tree, scratch))
                self.assertFalse(scratch.exists())

    def test_the_scrub_runs_when_a_step_is_interrupted(self) -> None:
        calls: list[str] = []

        def interrupted() -> None:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_cleanup([interrupted], lambda: calls.append("scrub"))
        self.assertEqual(["scrub"], calls)

    def test_a_step_that_is_interrupted_does_not_skip_the_steps_after_it(self) -> None:
        calls: list[str] = []

        def interrupted() -> None:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            run_cleanup([interrupted, lambda: calls.append("later")], lambda: calls.append("scrub"))
        self.assertEqual(["later", "scrub"], calls)

    def test_a_clean_run_raises_nothing_and_still_scrubs(self) -> None:
        calls: list[str] = []
        run_cleanup([lambda: calls.append("step")], lambda: calls.append("scrub"))
        self.assertEqual(["step", "scrub"], calls)

    def test_the_first_error_wins_over_a_later_one(self) -> None:
        def first() -> None:
            raise Expired("first")

        def second() -> None:
            raise ValueError("second")

        with self.assertRaises(Expired):
            run_cleanup([first, second], lambda: None)


class Scrub(unittest.TestCase):
    def test_a_folder_with_a_read_only_subfolder_is_removed_and_reported_gone(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            scratch = Path(parent) / "scratch"
            locked = scratch / "locked"
            locked.mkdir(parents=True)
            (locked / "secret").write_text("x")
            os.chmod(locked / "secret", 0o400)
            os.chmod(locked, 0o500)
            self.assertTrue(scrub_tree(scratch))
            self.assertFalse(scratch.exists())

    def test_a_folder_that_cannot_be_removed_is_reported_as_still_there(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            scratch = Path(parent) / "scratch"
            scratch.mkdir()
            with mock.patch("tests.fast.cleanup_support.shutil.rmtree"):
                self.assertFalse(scrub_tree(scratch))
            self.assertTrue(scratch.exists())

    @unittest.skipIf(os.geteuid() == 0, "root can search every folder")
    def test_a_folder_behind_an_unsearchable_parent_is_not_reported_as_gone(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            scratch = Path(parent) / "scratch"
            scratch.mkdir()
            os.chmod(parent, 0)
            try:
                with mock.patch("tests.fast.cleanup_support.shutil.rmtree"):
                    self.assertFalse(scrub_tree(scratch))
            finally:
                os.chmod(parent, 0o700)

    def test_a_folder_that_is_not_there_is_gone(self) -> None:
        with tempfile.TemporaryDirectory() as parent:
            self.assertTrue(scrub_tree(Path(parent) / "none"))


if __name__ == "__main__":
    unittest.main()
