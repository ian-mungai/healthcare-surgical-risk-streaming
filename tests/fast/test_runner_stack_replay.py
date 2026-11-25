"""The replay stack runner writes each observation to its own file and counts a teardown that leaves anything as an incomplete cleanup.

Failure modes covered here:

- The control run observes the topics before and after the replay and writes both to one file, which the replay's ``observe`` command refuses the second time.
- The in-image comparison reads the file of the first observation.
- A teardown leaves a container, a volume, a network or the image tag (or its removal exits non-zero, or a listing that found none failed) and the record
  still passes. Compose can exit 0 from ``down`` and leave the project's network.
- A run that stops before the image build (low memory) removes an image that was never built and records its cleanup as incomplete.
- A teardown step that raised is recorded in a text and not as a failed observation.
- The folders of the run stay and the record says that they were removed.
- A blocked run hides an incomplete cleanup.

The runner is untracked, so the tests run only where it is on disk. A stand-in ``Stack`` answers the commands; nothing starts.
"""

from __future__ import annotations

import ast
import contextlib
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
from tests.fast.runner_import import import_runner

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
        self.module: ModuleType = import_runner("stack_replay")
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
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, *arguments: str, **options: Any) -> CompletedProcess[str]:
        self.calls.append(arguments)
        key = "down" if "down" in arguments else arguments[0]
        return self.answers.get(key, done())


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class TeardownChecks(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("stack_replay")
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
        build_attempted: bool = True,
        docker: Docker | None = None,
    ) -> Record:
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        record.expect("run", "the cases", "pass", "pass", True)
        stack = self.module.Stack(record, [], "image:test", self.scratch, self.scratch)
        stack.up = True
        patches: list[Any] = [
            mock.patch.object(stack, "docker", docker or Docker(answers or {})),
            mock.patch.object(self.module, "run_command", rmi or (lambda *args, **options: done())),
            mock.patch.object(record, "close", return_value=0),
        ]
        if keep_folders:
            patches.append(mock.patch.object(self.module.shutil, "rmtree", lambda *args, **options: None))
        with contextlib.ExitStack() as opened:
            for patch in patches:
                opened.enter_context(patch)
            self.module.finish(record, [], stack, self.work, self.cache, False, "image:test", build_attempted=build_attempted, blocked=blocked)
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

    def test_a_network_that_is_left_fails_the_record_when_down_exited_zero(self) -> None:
        record = self.finish({"down": done(0, ""), "network": done(0, "int-fix7-replay-1_default\n")})
        self.assertEqual("fail", record.outcome)
        self.assertTrue(record.cleanup.startswith("incomplete"))

    def test_the_networks_are_listed_by_the_label_of_this_runs_project(self) -> None:
        docker = Docker({})
        self.finish(docker=docker)
        listings = [call for call in docker.calls if call[:2] == ("network", "ls")]
        self.assertEqual(1, len(listings))
        self.assertIn("--filter", listings[0])
        self.assertTrue(listings[0][listings[0].index("--filter") + 1].startswith("label=com.docker.compose.project=int-fix7-replay-"))
        self.assertIn("{{.Name}}", listings[0])

    def test_a_listing_that_failed_fails_the_record(self) -> None:
        for command in ("ps", "volume", "network"):
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

    def test_an_image_that_was_never_built_is_not_removed_and_the_cleanup_is_complete(self) -> None:
        commands: list[list[str]] = []

        def recorded(command: str, arguments: list[str], **options: Any) -> CompletedProcess[str]:
            commands.append([command, *arguments])
            return done(1, "")

        record = self.finish(rmi=recorded, build_attempted=False)
        self.assertEqual([], [command for command in commands if "rmi" in command])
        self.assertEqual("pass", record.outcome)
        self.assertFalse(record.cleanup.startswith("incomplete"))
        self.assertNotIn("image", " ".join(item.step for item in record.observations))

    def test_an_image_that_was_built_is_removed(self) -> None:
        commands: list[list[str]] = []

        def recorded(command: str, arguments: list[str], **options: Any) -> CompletedProcess[str]:
            commands.append([command, *arguments])
            return done()

        self.finish(rmi=recorded, build_attempted=True)
        self.assertEqual([["docker", "rmi", "image:test"]], [command for command in commands if "rmi" in command])

    def test_main_marks_the_build_as_started_before_it_runs_and_not_before_the_memory_check(self) -> None:
        main = next(node for node in ast.walk(ast.parse(RUNNER.read_text())) if isinstance(node, ast.FunctionDef) and node.name == "main")
        body = next(node.body for node in ast.walk(main) if isinstance(node, ast.Try))
        text = [ast.unparse(node) for node in body]
        marked = next(index for index, line in enumerate(text) if line == "build_attempted = True")
        build = next(index for index, line in enumerate(text) if line.startswith("built = run_command"))
        memory = next(index for index, line in enumerate(text) if line.startswith("free = free_memory_bytes"))
        self.assertTrue(memory < marked < build)
        self.assertEqual(1, len([line for line in text if line == "build_attempted = True"]))

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
            self.module.finish(record, [], stack, self.work, self.cache, False, "image:test", build_attempted=True)
        record.settle()
        self.assertEqual("fail", record.outcome)

    def test_folders_that_stay_fail_the_record(self) -> None:
        self.assertEqual("fail", self.finish(keep_folders=True).outcome)

    def test_a_blocked_run_still_says_that_its_cleanup_is_incomplete(self) -> None:
        record = self.finish({"ps": done(0, "left\n")}, blocked="a prerequisite is missing")
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertTrue(record.cleanup.startswith("incomplete"))


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class RunsThatStopBeforeTheBuild(unittest.TestCase):
    """``main`` against stand-ins that stop it before or at the image build: no Docker, Spark or network."""

    def run_main(
        self, *, free: int, build_code: int = 0, build_raises: BaseException | None = None, fetch_raises: BaseException | None = None
    ) -> tuple[Record, list[list[str]]]:
        module = import_runner("stack_replay")
        commands: list[list[str]] = []
        seen: list[Record] = []
        real_finish = module.finish

        def command(name: str, arguments: list[str], **options: Any) -> CompletedProcess[str]:
            commands.append([name, *arguments])
            if build_raises is not None and "build" in arguments:
                raise build_raises
            return done(build_code if "build" in arguments else 0)

        def finish(record: Record, *args: Any, **options: Any) -> int:
            seen.append(record)
            code: int = real_finish(record, *args, **options)
            return code

        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        reference = Path(scratch.name) / "reference.json"
        reference.write_text("{}")

        def fetch(*args: Any, **options: Any) -> dict[str, Path]:
            if fetch_raises is not None:
                raise fetch_raises
            return {}

        with (
            mock.patch.object(module, "free_memory_bytes", lambda: free),
            mock.patch.object(module, "run_command", command),
            mock.patch.object(module, "recorded_state", lambda root: ("rev", "tree")),
            mock.patch.object(module, "fill_landed_cache", fetch),
            mock.patch.object(module, "build_landed_copy", lambda *args, **options: (None, reference)),
            mock.patch.object(module, "finish", finish),
            mock.patch.object(Record, "close", lambda self, log, *, blocked="": (self.settle(blocked), 0)[1]),
        ):
            if isinstance(fetch_raises, RuntimeError):
                with self.assertRaises(RuntimeError):
                    module.main(["--iteration"])
            else:
                module.main(["--iteration"])
        return seen[0], commands

    def test_a_low_memory_stop_removes_no_image_and_its_cleanup_is_complete(self) -> None:
        record, commands = self.run_main(free=0)
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertEqual([], [command for command in commands if "rmi" in command or "build" in command])
        self.assertFalse(record.cleanup.startswith("incomplete"), record.cleanup)

    def test_a_build_that_passed_its_deadline_is_followed_by_the_removal_of_a_tag_that_it_may_have_left(self) -> None:
        record, commands = self.run_main(free=10**12, build_raises=TimeoutExpired("docker", 1200))
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertEqual(1, len([command for command in commands if "rmi" in command]))

    def test_a_download_that_passed_its_deadline_before_the_build_removes_no_image(self) -> None:
        record, commands = self.run_main(free=10**12, fetch_raises=TimeoutExpired("curl", 60))
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertEqual([], [command for command in commands if "rmi" in command or "build" in command])
        self.assertFalse(record.cleanup.startswith("incomplete"), record.cleanup)

    def test_an_unexpected_error_before_the_build_removes_no_image(self) -> None:
        record, commands = self.run_main(free=10**12, fetch_raises=RuntimeError("stand-in"))
        self.assertEqual([], [command for command in commands if "rmi" in command or "build" in command])
        self.assertFalse(record.cleanup.startswith("incomplete"), record.cleanup)

    def test_a_build_that_failed_is_followed_by_the_removal_of_a_tag_that_it_may_have_left(self) -> None:
        record, commands = self.run_main(free=10**12, build_code=1)
        self.assertTrue(record.outcome.startswith("blocked"))
        self.assertEqual(1, len([command for command in commands if "build" in command]))
        self.assertEqual(1, len([command for command in commands if "rmi" in command]))


if __name__ == "__main__":
    unittest.main()
