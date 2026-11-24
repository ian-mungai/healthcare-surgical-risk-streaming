"""The replay stack runner writes each observation to its own file and counts a teardown that leaves anything as an incomplete cleanup.

Failure modes covered here:

- The control run observes the topics before and after the replay and writes both to one file, which the replay's ``observe`` command refuses the second time.
- The in-image comparison reads the file of the first observation.
- A teardown leaves a container, a volume or the image tag (or its removal exits non-zero, or a listing that found none failed) and the record still passes.
- A teardown step that raised is recorded in a text and not as a failed observation.
- The folders of the run stay and the record says that they were removed.
- A blocked run hides an incomplete cleanup.

The runner is untracked, so the tests run only where it is on disk. A stand-in ``Stack`` answers the commands; nothing starts.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, TimeoutExpired
from tests.e2e.run_record import Record

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "stack_replay.py"
HOST_OUT = "/opt/replay/run/"


def done(code: int = 0, text: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, "")


class StandInStack:
    """Answers ``replay`` the way the image does: ``observe --out`` refuses a file that is already there."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.calls: list[tuple[str, ...]] = []

    def replay(self, command: str, record_name: str, *extra: str, timeout: float) -> CompletedProcess[str]:
        self.calls.append((command, *extra))
        if command == "observe":
            target = self.run_dir / Path(extra[extra.index("--out") + 1]).name
            if target.exists():
                return CompletedProcess(["replay"], 1, "", "refused: the output file exists")
            target.write_text(json.dumps({"messages": len(self.calls)}))
        return done()


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class ObservationFiles(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.stack_replay")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.stack = StandInStack(Path(scratch.name))
        self.case = self.module.Case(self.stack, "control", {"run_id": "run_x"})

    def test_the_two_observations_of_one_case_are_both_written(self) -> None:
        before = self.case.observe("before")
        after = self.case.observe()
        self.assertNotEqual(before, after)

    def test_a_stage_that_is_observed_twice_is_blocked_before_the_command_runs(self) -> None:
        self.case.observe("before")
        calls = len(self.stack.calls)
        with self.assertRaises(self.module.Blocked):
            self.case.observe("before")
        self.assertEqual(calls, len(self.stack.calls))

    def test_the_comparison_in_the_image_reads_the_observation_after_the_replay(self) -> None:
        self.case.observe("before")
        self.case.observe()
        self.case.in_image_compare()
        arguments = self.stack.calls[-1]
        self.assertEqual("compare", arguments[0])
        self.assertEqual(f"{HOST_OUT}control_observed_after.json", arguments[arguments.index("--observed") + 1])

    def test_the_control_run_observes_before_and_after_and_names_each_once(self) -> None:
        tree = ast.parse(RUNNER.read_text())
        control = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "control_run")
        stages = [
            (call.args[0].value if call.args and isinstance(call.args[0], ast.Constant) else "after")
            for call in ast.walk(control)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "observe" and isinstance(call.func.value, ast.Name)
        ]
        self.assertEqual(["after", "before"], sorted(stages))


class Docker:
    """The answers of ``Stack.docker`` by the first argument or two of the command."""

    def __init__(self, answers: dict[str, CompletedProcess[str]]) -> None:
        self.answers = answers

    def __call__(self, *arguments: str, **options: Any) -> CompletedProcess[str]:
        key = "down" if "down" in arguments else arguments[0]
        return self.answers.get(key, done())


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class TeardownChecks(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.stack_replay")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.cache = self.scratch / "cache"
        self.cache.mkdir()

    def finish(
        self,
        answers: dict[str, CompletedProcess[str]] | None = None,
        *,
        rmi: Callable[..., CompletedProcess[str]] | None = None,
        keep_folders: bool = False,
        blocked: str = "",
    ) -> Record:
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        record.expect("run", "the cases", "pass", "pass", True)
        stack = self.module.Stack(record, [], "image:test", self.scratch, self.scratch)
        stack.up = True
        patches: list[Any] = [
            mock.patch.object(stack, "docker", Docker(answers or {})),
            mock.patch.object(self.module, "run_command", rmi or (lambda *args, **options: done())),
            mock.patch.object(record, "close", return_value=0),
        ]
        if keep_folders:
            patches.append(mock.patch.object(self.module.shutil, "rmtree", lambda *args, **options: None))
        with contextlib.ExitStack() as opened:
            for patch in patches:
                opened.enter_context(patch)
            self.module.finish(record, [], stack, self.work, self.cache, False, "image:test", blocked=blocked)
        record.settle(blocked)
        return record

    def test_a_clean_teardown_passes(self) -> None:
        record = self.finish()
        self.assertEqual("pass", record.outcome)
        self.assertFalse(record.cleanup.startswith("incomplete"))

    def test_a_container_that_is_left_fails_the_record(self) -> None:
        record = self.finish({"ps": done(0, "int-fix7-replay-1\n")})
        self.assertEqual("fail", record.outcome)
        self.assertTrue(record.cleanup.startswith("incomplete"))

    def test_a_volume_that_is_left_fails_the_record(self) -> None:
        record = self.finish({"volume": done(0, "int-fix7-volume\n")})
        self.assertEqual("fail", record.outcome)

    def test_a_listing_that_failed_fails_the_record(self) -> None:
        for command in ("ps", "volume"):
            with self.subTest(command=command):
                self.assertEqual("fail", self.finish({command: done(1, "")}).outcome)

    def test_a_down_that_exited_non_zero_fails_the_record(self) -> None:
        self.assertEqual("fail", self.finish({"down": done(1, "")}).outcome)

    def test_an_image_removal_that_exited_non_zero_fails_the_record(self) -> None:
        record = self.finish(rmi=lambda *args, **options: done(1, ""))
        self.assertEqual("fail", record.outcome)
        self.assertTrue(record.cleanup.startswith("incomplete"))

    def test_an_image_removal_that_timed_out_fails_the_record(self) -> None:
        def timed_out(*args: Any, **options: Any) -> CompletedProcess[str]:
            raise TimeoutExpired("docker", 120)

        self.assertEqual("fail", self.finish(rmi=timed_out).outcome)

    def test_a_stack_removal_that_raised_fails_the_record(self) -> None:
        def raised(*arguments: str, **options: Any) -> CompletedProcess[str]:
            raise TimeoutExpired("docker", 300)

        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        record.expect("run", "the cases", "pass", "pass", True)
        stack = self.module.Stack(record, [], "image:test", self.scratch, self.scratch)
        stack.up = True
        with (
            mock.patch.object(stack, "docker", raised),
            mock.patch.object(self.module, "run_command", lambda *args, **options: done()),
            mock.patch.object(record, "close", return_value=0),
        ):
            self.module.finish(record, [], stack, self.work, self.cache, False, "image:test")
        record.settle()
        self.assertEqual("fail", record.outcome)

    def test_folders_that_stay_fail_the_record(self) -> None:
        self.assertEqual("fail", self.finish(keep_folders=True).outcome)

    def test_a_blocked_run_still_says_that_its_cleanup_is_incomplete(self) -> None:
        record = self.finish({"ps": done(0, "left\n")}, blocked="a prerequisite is missing")
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertTrue(record.cleanup.startswith("incomplete"))


if __name__ == "__main__":
    unittest.main()
