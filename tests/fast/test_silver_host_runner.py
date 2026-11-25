"""The host mode of the Silver signals and hypotension runners refuses a wrong setting before any work, takes its limits from the stated figures and stops a run
that has passed its deadline.

The runners (``tests/e2e/silver_signals.py`` and ``silver_hypotension.py``) stay untracked, so each test skips when they are not on this machine. No test starts
Docker: every call that would reach the daemon is replaced by a stand-in that fails the test when it is called.

Failure modes covered here:

- A host flag written as ``true`` or `` 1`` ran the local mode (local defaults, local limits, emulation text) on the host without a word.
- A host run with no landed folder, a relative folder or a bad ``SPARK_IMAGE`` started anyway, or its refusal repeated the value that was given.
- The host's limits were not the stated figures: the job's own deadline was not passed to the container, or the local limits stayed on the host.
- A run that had passed its deadline started one more job (so the run had no ceiling), or the check stopped a local run that has no deadline.
- A given image that the run did not build was removed at the end, or the run's own image was left behind.
- The host preflight passed a daemon that has the memory in total but not free, or a host that gave no free-memory figure.
"""

from __future__ import annotations

import ast
import contextlib
import importlib.util
import io
import os
import sys
import tempfile
import time
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.sample.candidates import CANDIDATES_FILE, read_candidates
from tests.e2e.run_record import Record
from tests.fast.host_mode import HOST_LIMITS, LOCAL_LIMITS
from tests.fast.test_runner_gold_sealed import runner

E2E = Path(__file__).resolve().parents[1] / "e2e"
FLAGS = {"silver_signals": "SILVER_SIGNALS_HOST", "silver_hypotension": "SILVER_HYPOTENSION_HOST"}
GIB = 1024**3
SETTING_NAMES = ("LANDED_CACHE", "SIGNAL_CASE_CACHE", "SPARK_IMAGE", *FLAGS.values())


def clean_environment(**given: str) -> dict[str, str]:
    """The environment of a test: no setting of the runners except the ones given."""
    return {**{name: value for name, value in os.environ.items() if name not in SETTING_NAMES}, **given}


def load_copy(name: str, **given: str) -> ModuleType:
    """A private copy of the runner, read with the given settings (the module reads its flag when it is imported)."""
    runner(name)
    spec = importlib.util.spec_from_file_location(f"host_copy_{name}", E2E / f"{name}.py")
    if spec is None or spec.loader is None:
        raise unittest.SkipTest("the runner cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(os.environ, clean_environment(**given), clear=True), mock.patch.dict(sys.modules, {spec.name: module}):
        spec.loader.exec_module(module)
    return module


def timeouts(seen: list[object]) -> Any:
    """A stand-in for ``run_discarding_output`` that notes the limit it was given and answers exit 0."""

    def run(*_: object, **given: object) -> int:
        seen.append(given.get("timeout"))
        return 0

    return run


def forbidden(*_: object, **__: object) -> None:
    raise AssertionError("a call that reaches the Docker daemon or a process was made")


@contextlib.contextmanager
def refused(module: ModuleType, **given: str) -> Iterator[io.StringIO]:
    """Run ``main`` with the given settings, with every process call replaced by a stand-in that fails the test, and give the standard error text."""
    errors = io.StringIO()
    with (
        mock.patch.dict(os.environ, clean_environment(**given), clear=True),
        mock.patch.object(module, "run_command", forbidden),
        mock.patch.object(module, "recorded_state", forbidden),
        contextlib.redirect_stderr(errors),
    ):
        yield errors


class HostRunnerCase(unittest.TestCase):
    NAME = ""

    def module(self, **given: str) -> ModuleType:
        return load_copy(self.NAME, **given)

    @property
    def flag(self) -> str:
        return FLAGS[self.NAME]

    def test_a_flag_that_is_not_exactly_one_is_refused_and_never_runs_the_local_mode(self) -> None:
        for value in ("true", "yes", " 1", "1 ", "01", "on"):
            with self.subTest(value=value):
                module = self.module(**{self.flag: value})
                with refused(module, **{self.flag: value}) as errors:
                    self.assertEqual(2, module.main())
                self.assertEqual(f"{self.NAME}: {self.flag} must be 1, 0 or not set\n", errors.getvalue())

    def test_a_host_run_without_a_landed_folder_is_refused_with_no_default_path(self) -> None:
        module = self.module(**{self.flag: "1"})
        self.assertTrue(module.HOST)
        with refused(module, **{self.flag: "1"}) as errors:
            self.assertEqual(2, module.main())
        self.assertEqual(f"{self.NAME}: LANDED_CACHE is not set\n", errors.getvalue())

    def test_a_relative_folder_is_refused_and_the_text_never_holds_the_value(self) -> None:
        module = self.module(**{self.flag: "1"})
        with refused(module, **{self.flag: "1"}, LANDED_CACHE="relative/private_name") as errors:
            self.assertEqual(2, module.main())
        self.assertEqual(f"{self.NAME}: LANDED_CACHE is not an absolute path\n", errors.getvalue())
        with refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache", SIGNAL_CASE_CACHE="private_name") as errors:
            self.assertEqual(2, module.main())
        self.assertEqual(f"{self.NAME}: SIGNAL_CASE_CACHE is not an absolute path\n", errors.getvalue())

    def test_an_image_that_docker_could_read_as_an_option_is_refused_and_the_text_never_holds_the_value(self) -> None:
        module = self.module(**{self.flag: "1"})
        for value in ("--private-option", "no tag", "image:"):
            with self.subTest(value=value), refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache", SPARK_IMAGE=value) as errors:
                self.assertEqual(2, module.main())
            self.assertEqual(f"{self.NAME}: SPARK_IMAGE is not an image name with a tag\n", errors.getvalue())

    def test_a_given_image_is_used_and_not_built_and_the_run_image_is_built_when_none_is_given(self) -> None:
        module = self.module(**{self.flag: "1"})
        environment = clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", SPARK_IMAGE="surgical-risk-spark:4.1.4")
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(module, "IMAGE", module.IMAGE),
            mock.patch.object(module, "BUILT_HERE", True),
            mock.patch.object(module, "EXTRA_CACHE", getattr(module, "EXTRA_CACHE", None), create=True),
        ):
            result = module.select_settings()
            self.assertEqual(None, result[-1])
            self.assertEqual(("surgical-risk-spark:4.1.4", False), (module.IMAGE, module.BUILT_HERE))
            self.assertEqual(Path("/cache"), result[0])
        with (
            mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", SPARK_IMAGE=""), clear=True),
            mock.patch.object(module, "IMAGE", module.IMAGE),
            mock.patch.object(module, "BUILT_HERE", False),
            mock.patch.object(module, "EXTRA_CACHE", getattr(module, "EXTRA_CACHE", None), create=True),
        ):
            self.assertEqual(None, module.select_settings()[-1])
            self.assertEqual((module.IMAGE_TAG, True), (module.IMAGE, module.BUILT_HERE))

    def test_the_case_folder_falls_back_to_the_landed_folder_on_the_host(self) -> None:
        module = self.module(**{self.flag: "1"})
        with (
            mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache"), clear=True),
            mock.patch.object(module, "IMAGE", module.IMAGE),
            mock.patch.object(module, "BUILT_HERE", True),
            mock.patch.object(module, "EXTRA_CACHE", getattr(module, "EXTRA_CACHE", None), create=True),
        ):
            landed, *rest = module.select_settings()
            extra = module.EXTRA_CACHE if self.NAME == "silver_signals" else rest[0]
            self.assertEqual((Path("/cache"), Path("/cache")), (landed, extra))

    def test_the_host_limits_are_the_stated_figures_and_the_local_limits_are_unchanged(self) -> None:
        host, local = self.module(**{self.flag: "1"}), self.module()
        self.assertIs(HOST_LIMITS, host.LIMITS)
        self.assertEqual(
            (1800, 1800, 300, 30, 180), (host.BUILD_SECONDS, host.JOB_SECONDS, host.HEALTH_DEADLINE_SECONDS, host.CALL_SECONDS, host.COMMAND_SECONDS)
        )
        self.assertEqual(["--env", "JOB_DEADLINE_SECONDS=1700"], host.JOB_ENVIRONMENT)
        self.assertIs(LOCAL_LIMITS, local.LIMITS)
        self.assertEqual(
            (3000, 3600, 300, 30, 180), (local.BUILD_SECONDS, local.JOB_SECONDS, local.HEALTH_DEADLINE_SECONDS, local.CALL_SECONDS, local.COMMAND_SECONDS)
        )
        self.assertEqual(["--env", "JOB_DEADLINE_SECONDS=3500"], local.JOB_ENVIRONMENT)

    def stack(self, module: ModuleType, folder: str) -> Any:
        return module.Stack(Path(folder) / "compose", "project", Path(folder) / "data", Record(feature="f", slug="s", rerun="r"), [])

    def test_a_host_run_past_its_deadline_starts_no_further_job(self) -> None:
        module = self.module(**{self.flag: "1"})
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, mock.patch.object(module, "run_discarding_output", forbidden):
            stack = self.stack(module, folder)
            stack.started = time.monotonic() - (HOST_LIMITS.run_seconds or 0)
            with mock.patch.object(module, "run_command", forbidden), self.assertRaises(RuntimeError) as caught:
                stack.job(["-m", "spark.jobs.x", "build", "--branch", "b"])
            self.assertIn("run deadline of 5400 seconds passed before the step -m spark.jobs.x build --branch started", str(caught.exception))
            self.assertTrue(any("the run deadline passed before the step started" in line for line in stack.log))

    def test_a_host_run_inside_its_deadline_starts_the_job(self) -> None:
        module = self.module(**{self.flag: "1"})
        started: list[object] = []
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder:
            stack = self.stack(module, folder)
            stack.started = time.monotonic() - (HOST_LIMITS.run_seconds or 0) + 60
            with (
                mock.patch.object(module, "run_discarding_output", timeouts(started)),
                mock.patch.object(module, "read_result", lambda *_: None),
            ):
                call = stack.job(["-m", "spark.jobs.x", "build"])
            self.assertEqual([1800], started)
            self.assertEqual(0, call.code)

    def test_a_local_run_has_no_deadline_however_long_it_has_run(self) -> None:
        module = self.module()
        started: list[object] = []
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder:
            stack = self.stack(module, folder)
            stack.started = time.monotonic() - 10**7
            with (
                mock.patch.object(module, "run_discarding_output", timeouts(started)),
                mock.patch.object(module, "read_result", lambda *_: None),
            ):
                stack.job(["-m", "spark.jobs.x", "build"])
            self.assertEqual([3600], started)

    def preflight(self, module: ModuleType, total: int, available: int | None) -> bool:
        record = Record(feature="f", slug="s", rerun="r")
        candidates = read_candidates(CANDIDATES_FILE)
        arguments: tuple[Any, ...] = (record, [], Path("/no/such/cache"), candidates if self.NAME == "silver_signals" else Path("/no/such/extra"))
        reply = mock.Mock(returncode=0, stdout=f"{total}\n")
        with (
            mock.patch.object(module, "run_command", lambda *_, **__: reply),
            mock.patch.object(module, "available_memory_mib", lambda: available),
            mock.patch.object(module, "EXTRA_CACHE", Path("/no/such/extra"), create=True),
        ):
            module.preflight(*arguments)
        (observation,) = [item for item in record.observations if item.case == "preflight"]
        return observation.passed

    def test_the_host_preflight_needs_the_memory_free_now_and_a_free_figure(self) -> None:
        module = self.module(**{self.flag: "1"})
        self.assertTrue(self.preflight(module, 32 * GIB, 7 * 1024))
        self.assertTrue(self.preflight(module, 32 * GIB, 6 * 1024))
        self.assertFalse(self.preflight(module, 32 * GIB, 6 * 1024 - 1))
        self.assertFalse(self.preflight(module, 32 * GIB, None))
        self.assertFalse(self.preflight(module, 8 * GIB, 20 * 1024))

    def test_the_local_preflight_asks_for_the_daemon_total_only(self) -> None:
        module = self.module()
        self.assertTrue(self.preflight(module, 12 * GIB, None))
        self.assertFalse(self.preflight(module, 12 * GIB - 1, None))

    def test_the_run_image_is_removed_only_when_the_run_built_it(self) -> None:
        tree = ast.parse((E2E / f"{self.NAME}.py").read_text())
        removals = [
            call
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "run_command"
            for call in [node]
            if call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value == "docker" and "rmi" in ast.unparse(call.args[1])
        ]
        self.assertEqual(1, len(removals))
        guards = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "BUILT_HERE" and removals[0] in ast.walk(node)
        ]
        self.assertTrue(guards)
        inspections = [node for node in ast.walk(tree) if isinstance(node, ast.Constant) and node.value == "inspect"]
        self.assertTrue(inspections)


class SignalsHostRunnerTest(HostRunnerCase):
    NAME = "silver_signals"


class HypotensionHostRunnerTest(HostRunnerCase):
    NAME = "silver_hypotension"


del HostRunnerCase

if __name__ == "__main__":
    unittest.main()
