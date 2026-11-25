"""The host mode of the Silver signals and hypotension runners refuses a wrong setting before any work, takes its limits from the stated figures and stops a run
that has passed its deadline.

The runners (``tests/e2e/silver_signals.py`` and ``silver_hypotension.py``) stay untracked, so each test skips when they are not on this machine. No test starts
Docker: every call that would reach the daemon is replaced by a stand-in that fails the test when it is called.

Failure modes covered here:

- A host flag written as ``true`` or `` 1`` ran the local mode (local defaults, local limits, emulation text) on the host without a word.
- A host run with no landed folder, a relative folder or a bad ``SPARK_IMAGE`` started anyway, or its refusal repeated the value that was given.
- The host's limits were not the stated figures: the job's own deadline was not passed to the container, or the local limits stayed on the host.
- A run that had passed its deadline started one more job (so the run had no ceiling), or the check stopped a local run that has no deadline.
- A given image that the run did not build was removed at the end, the run's own image was left behind, or the run removed a tag that it did not create (the
  fixed tag of an earlier run, after a preflight refusal that came before any build).
- A given image was taken by its tag, which can name another build than the tested one, and its immutable ID was not checked or not recorded.
- The host preflight passed a daemon that has the memory in total but not free, a host that gave no free-memory figure, or a host that has the memory free but
  not the reserve of ``compose/limits.yml``; the memory was read once, before a long build, and not again before the services started.
- The caller's environment reached Compose or Docker: an exported ``WAREHOUSE_LOCATION``, ``NESSIE_URI`` or ``COMPOSE_*`` setting beat the run's env files and
  Spark wrote to the deployed catalog or warehouse.
- A one-off container of the setup (the password file, the warehouse owner) had no project label, so the cleanup could not find it after a timeout.
- A timeout or an error in one cleanup command skipped the rest of the cleanup (the image, the scratch folder, the record), or an unconfirmed removal left the
  record passing.
- A build, a job, a health wait or a Docker command got its full limit with only seconds of the run left, so the run went past its deadline; the cleanup had no
  budget of its own; a job's own deadline was longer than its cap, so the runner stopped a job that was still inside its own deadline.
- The cleanup deleted the run's Nessie references one by one before it removed a container, so a slow Nessie spent the cleanup budget and the containers, the
  volumes and the image were left; a container listing that failed was followed by the removal of the volumes and networks that the container still held; a
  container that the last listing found was not removed.
- A call to Nessie and the launch of a job took the rest of the run before the steps that come first (the scratch folder, the container listing), so a step
  could start with a cap longer than the rest of the run.
- A given image with the recorded ID was taken as the tested build though the Python files in it were not the files of the reviewed commit; the record held
  the ID or a file name; the header of a runner promised a raw ``spark_image_id`` that the record does not hold.
- The memory was read before the password file and the warehouse volume were made, so their seconds were not between the reading and the first service.
- A fast test bound a real loopback port, and the scratch folder was left when the setup failed before the first service.
"""

from __future__ import annotations

import ast
import contextlib
import io
import os
import sys
import tempfile
import time
import types
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.sample.candidates import CANDIDATES_FILE, read_candidates
from scripts.stack.limits import load_limits
from tests.e2e.run_record import Record
from tests.fast import host_mode
from tests.fast.host_mode import HOST_LIMITS, LOCAL_LIMITS
from tests.fast.runner_import import safe_execute
from tests.fast.test_runner_gold_sealed import runner

E2E = Path(__file__).resolve().parents[1] / "e2e"
FLAGS = {"silver_signals": "SILVER_SIGNALS_HOST", "silver_hypotension": "SILVER_HYPOTENSION_HOST"}
GIB = 1024**3
SETTING_NAMES = ("LANDED_CACHE", "SIGNAL_CASE_CACHE", "SPARK_IMAGE", "SPARK_IMAGE_ID", *FLAGS.values())
IMAGE_ID = "sha256:" + "ab" * 32
OTHER_ID = "sha256:" + "cd" * 32
STAND_IN_DIGEST = "ianmungai/healthcare_surgical_risk_streaming@sha256:" + "ef" * 32
SHA_X, SHA_Y, SHA_Z = "a1" * 20, "b2" * 20, "c3" * 20
# What ``git ls-tree -r HEAD`` says of the reviewed commit, and what the image's own listing says of the Python files under /opt/jobs.
TREE = (
    f"100644 blob {SHA_X}\tspark/jobs/x.py\n100644 blob {SHA_Y}\tspark/silver/y.py\n100644 blob {SHA_Z}\tscripts/stack/nessie.py\n"
    f"100644 blob {'d4' * 20}\tspark/Dockerfile\n100644 blob {'e5' * 20}\tREADME.md\n"
)
IMAGE_FILES = f"{SHA_X} spark/jobs/x.py\n{SHA_Y} spark/silver/y.py\n{SHA_Z} scripts/stack/nessie.py\n"
# The Dockerfile of the same commit: what the image must hold under /opt/jobs is what its COPY instructions put there.
DOCKERFILE = "COPY spark /opt/jobs/spark\nCOPY scripts/stack/nessie.py /opt/jobs/scripts/stack/nessie.py\n"
# Its Docker ignore file: what the build leaves out of the context before ``COPY`` reads it.
DOCKERIGNORE = "*\n!spark\n!scripts/stack/nessie.py\n"
# Settings of the deployed stack and of Compose that a caller may have exported. None of them may reach a call of the run.
BOUND = {"WAREHOUSE_LOCATION", "NESSIE_URI", "AWS_REGION", "POSTGRES_HOST_PORT", "MEM_LIMIT_SPARK"}
DEPLOYED = {
    "WAREHOUSE_LOCATION": "s3://deployed/warehouse",
    "NESSIE_URI": "http://deployed:19120/api/v2",
    "AWS_REGION": "eu-west-1",
    "AWS_ACCESS_KEY_ID": "deployed-key",
    "COMPOSE_PROJECT_NAME": "deployed",
    "COMPOSE_FILE": "/deployed/compose.yml",
    "COMPOSE_PROFILES": "all",
    "MEM_LIMIT_SPARK": "1m",
    "SPARK_IMAGE": "deployed:1",
    "POSTGRES_HOST_PORT": "5432",
    "DOCKER_DEFAULT_PLATFORM": "linux/arm64",
}


def clean_environment(**given: str) -> dict[str, str]:
    """The environment of a test: no setting of the runners except the ones given."""
    return {**{name: value for name, value in os.environ.items() if name not in SETTING_NAMES}, **given}


def load_copy(name: str, **given: str) -> ModuleType:
    """A private copy of the runner, read with the given settings (the module reads its flag when it is imported)."""
    runner(name)
    with mock.patch.dict(os.environ, clean_environment(**given), clear=True), mock.patch.dict(sys.modules):
        return safe_execute(E2E / f"{name}.py", f"host_copy_{name}")


def observed(record: Record, case: str) -> str:
    """The text of the observation of one case of the record."""
    (item,) = [item for item in record.observations if item.case == case]
    return item.observed


def forbidden(*_: object, **__: object) -> None:
    raise AssertionError("a call that reaches the Docker daemon or a process was made")


class Calls:
    """The processes that a runner started: each one as its program, its arguments and the keyword arguments of the call. ``answer`` gives the standard output
    of a call (by its arguments) and ``code`` its exit code; a call whose answer is an exception is raised."""

    def __init__(self, answer: Any = None, code: Any = None) -> None:
        self.items: list[tuple[str, list[str], dict[str, Any]]] = []
        self.answer = answer or (lambda _: "")
        self.code = code or (lambda _: 0)

    def run_command(self, program: str, args: Any, **given: Any) -> Any:
        self.items.append((program, list(args), given))
        reply = self.answer(list(args))
        if isinstance(reply, BaseException):
            raise reply
        return mock.Mock(returncode=self.code(list(args)), stdout=reply, stderr="")

    def run_discarding_output(self, program: str, args: Any, **given: Any) -> int:
        self.items.append((program, list(args), given))
        reply = self.answer(list(args))
        if isinstance(reply, BaseException):
            raise reply
        return int(self.code(list(args)))

    def words(self) -> list[str]:
        return [" ".join(args) for _, args, _ in self.items]

    def patches(self, module: ModuleType) -> contextlib.ExitStack:
        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.object(module, "run_command", self.run_command))
        stack.enter_context(mock.patch.object(module, "run_discarding_output", self.run_discarding_output))
        return stack


@contextlib.contextmanager
def refused(module: ModuleType, **given: str) -> Iterator[io.StringIO]:
    """Run ``main`` with the given settings, with every process call replaced by a stand-in that fails the test, and give the standard error text."""
    errors = io.StringIO()
    with (
        mock.patch.dict(os.environ, clean_environment(**given), clear=True),
        mock.patch.object(module, "run_command", forbidden),
        mock.patch.object(module, "checkout_state", forbidden),
        contextlib.redirect_stderr(errors),
    ):
        yield errors


class HostRunnerCase(unittest.TestCase):
    NAME = ""

    def module(self, **given: str) -> ModuleType:
        """A private copy of the runner. A host copy reads the pin file of its checkout through ``pinned_reference``, which here names a stand-in digest (the
        tests of the pin file's own use give their own)."""
        loaded = load_copy(self.NAME, **given)
        vars(loaded)["pinned_reference"] = lambda _name: STAND_IN_DIGEST
        return loaded

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

    def given_image(self) -> dict[str, str]:
        return {"SPARK_IMAGE": "surgical-risk-spark:4.1.4", "SPARK_IMAGE_ID": IMAGE_ID}

    def test_an_image_that_docker_could_read_as_an_option_is_refused_and_the_text_never_holds_the_value(self) -> None:
        module = self.module(**{self.flag: "1"})
        for value in ("--private-option", "no tag", "image:"):
            with (
                self.subTest(value=value),
                refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache", SPARK_IMAGE=value, SPARK_IMAGE_ID=IMAGE_ID) as errors,
            ):
                self.assertEqual(2, module.main())
            self.assertEqual(f"{self.NAME}: SPARK_IMAGE is not an image name with a tag or a digest\n", errors.getvalue())

    def test_a_given_image_without_the_id_that_the_deploy_recorded_is_refused_before_any_work(self) -> None:
        module = self.module(**{self.flag: "1"})
        with refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache", SPARK_IMAGE="surgical-risk-spark:4.1.4") as errors:
            self.assertEqual(2, module.main())
        self.assertEqual(f"{self.NAME}: SPARK_IMAGE_ID is not set\n", errors.getvalue())
        with refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache", **{**self.given_image(), "SPARK_IMAGE_ID": "sha256:private"}) as errors:
            self.assertEqual(2, module.main())
        self.assertNotIn("private", errors.getvalue())

    def settings(self, module: ModuleType, **given: str) -> Any:
        with (
            mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", **given), clear=True),
            mock.patch.object(module, "IMAGE", module.IMAGE),
            mock.patch.object(module, "BUILT_HERE", module.BUILT_HERE),
            mock.patch.object(module, "SOURCE_IMAGE", None, create=True),
            mock.patch.object(module, "EXPECTED_IMAGE_ID", None, create=True),
            mock.patch.object(module, "EXTRA_CACHE", getattr(module, "EXTRA_CACHE", None), create=True),
        ):
            result = module.select_settings()
            return result, (module.IMAGE, module.BUILT_HERE, module.SOURCE_IMAGE, module.EXPECTED_IMAGE_ID)

    def test_a_given_image_is_read_and_never_used_by_its_tag_and_the_run_has_a_tag_of_its_own(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, (tag, built, source, expected) = self.settings(module, **self.given_image())
        self.assertEqual(None, result[-1])
        self.assertEqual(Path("/cache"), result[0])
        self.assertEqual((False, "surgical-risk-spark:4.1.4", IMAGE_ID), (built, source, expected))
        self.assertTrue(tag.startswith(f"{module.IMAGE_TAG}-") and tag != module.IMAGE_TAG and tag != source)

    def test_the_run_image_is_built_under_a_tag_that_is_new_for_every_run_when_none_is_given_on_this_computer(self) -> None:
        module = self.module()
        first, (tag, built, source, expected) = self.settings(module, SPARK_IMAGE="")
        self.assertEqual(None, first[-1])
        self.assertEqual((True, None, None), (built, source, expected))
        self.assertTrue(tag.startswith(f"{module.IMAGE_TAG}-") and tag != module.IMAGE_TAG)
        self.assertNotEqual(tag, self.settings(module)[1][0])

    DIGEST = "ianmungai/healthcare_surgical_risk_streaming@sha256:" + "cd" * 32

    def pinned(self, module: ModuleType, reference: Any) -> Any:
        return mock.patch.object(module, "pinned_reference", reference, create=True)

    def test_a_host_that_is_given_no_image_runs_the_digest_that_the_pin_file_of_its_checkout_names(self) -> None:
        module = self.module(**{self.flag: "1"})
        asked: list[str] = []

        def reference(name: str) -> str:
            asked.append(name)
            return self.DIGEST

        with self.pinned(module, reference):
            result, (tag, built, source, expected) = self.settings(module, SPARK_IMAGE="")
        self.assertEqual(None, result[-1])
        self.assertEqual(["spark"], asked)
        self.assertEqual((False, self.DIGEST, None), (built, source, expected))
        self.assertTrue(tag.startswith(f"{module.IMAGE_TAG}-") and tag != self.DIGEST)

    def test_a_host_that_is_given_an_image_never_reads_the_pin_file(self) -> None:
        module = self.module(**{self.flag: "1"})
        with self.pinned(module, mock.Mock(side_effect=AssertionError("the pin file was read"))):
            result, (_, built, source, expected) = self.settings(module, **self.given_image())
        self.assertEqual((None, False, "surgical-risk-spark:4.1.4", IMAGE_ID), (result[-1], built, source, expected))

    def test_a_host_whose_pin_file_cannot_be_used_is_refused_in_fixed_words_before_any_work(self) -> None:
        module = self.module(**{self.flag: "1"})
        failure = module.HostCheckoutError("compose/image_pins.yml differs from the source manifest")
        with self.pinned(module, mock.Mock(side_effect=failure)), refused(module, **{self.flag: "1"}, LANDED_CACHE="/cache") as errors:
            self.assertEqual(2, module.main())
        self.assertEqual(f"{self.NAME}: compose/image_pins.yml differs from the source manifest\n", errors.getvalue())

    def test_a_run_on_this_computer_that_builds_never_reads_the_pin_file(self) -> None:
        module = self.module()
        with self.pinned(module, mock.Mock(side_effect=AssertionError("the pin file was read"))):
            result, (_, built, source, expected) = self.settings(module, SPARK_IMAGE="")
        self.assertEqual((None, True, None, None), (result[-1], built, source, expected))

    def test_a_given_digest_reference_needs_no_id_and_the_id_that_docker_reads_is_the_one_that_is_tagged(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, record = self.image_run(
            module,
            given=True,
            image_settings={"SPARK_IMAGE": self.DIGEST},
            answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "",
        )
        self.assertEqual((True, True), result)
        inspected = next(args for _, args, _ in calls.items if args[:2] == ["image", "inspect"])
        self.assertEqual(self.DIGEST, inspected[-1])
        tag = next(args for _, args, _ in calls.items if args[:2] == ["image", "tag"])
        self.assertEqual(["image", "tag", IMAGE_ID, module.IMAGE], tag)
        self.assertTrue(all(item.passed for item in record.observations))

    def test_a_given_digest_reference_that_docker_cannot_read_an_id_for_is_refused(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, _ = self.image_run(module, given=True, image_settings={"SPARK_IMAGE": self.DIGEST}, answer=lambda _: "not an id\n")
        self.assertEqual((False, False), result)
        self.assertFalse(any(words.startswith("image tag") for words in calls.words()))

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
        self.assertEqual((1700, 5400), (host.JOB_DEADLINE_SECONDS, host.RUN_SECONDS))
        self.assertIs(LOCAL_LIMITS, local.LIMITS)
        self.assertEqual(
            (3000, 3600, 300, 30, 180), (local.BUILD_SECONDS, local.JOB_SECONDS, local.HEALTH_DEADLINE_SECONDS, local.CALL_SECONDS, local.COMMAND_SECONDS)
        )
        self.assertEqual((3500, None), (local.JOB_DEADLINE_SECONDS, local.RUN_SECONDS))

    def stack(self, module: ModuleType, folder: str) -> Any:
        return module.Stack(Path(folder) / "compose", "project", Path(folder) / "data", Record(feature="f", slug="s", rerun="r"), [])

    def left(self, stack: Any, seconds: float) -> None:
        """Set the run clock so that ``seconds`` of the run deadline are left."""
        stack.started = time.monotonic() - ((HOST_LIMITS.run_seconds or 0) - seconds)

    def test_a_host_run_past_its_deadline_starts_no_further_job(self) -> None:
        module = self.module(**{self.flag: "1"})
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, mock.patch.object(module, "run_discarding_output", forbidden):
            stack = self.stack(module, folder)
            stack.started = time.monotonic() - (HOST_LIMITS.run_seconds or 0)
            with mock.patch.object(module, "run_command", forbidden), self.assertRaises(RuntimeError) as caught:
                stack.job(["-m", "spark.jobs.x", "build", "--branch", "b"])
            self.assertIn("run deadline of 5400 seconds passed before the step -m spark.jobs.x build --branch started", str(caught.exception))
            self.assertTrue(any("the run deadline passed before the step started" in line for line in stack.log))

    def job_started(self, module: ModuleType, remaining: float | None) -> Calls:
        """Run one job with ``remaining`` seconds of the run left (None: the run has no deadline) and give the calls that it made."""
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module), mock.patch.object(module, "read_result", lambda *_: None):
            stack = self.stack(module, folder)
            if remaining is None:
                stack.started = time.monotonic() - 10**7
            else:
                self.left(stack, remaining)
            stack.job(["-m", "spark.jobs.x", "build"])
        return calls

    def test_a_host_job_with_the_whole_run_left_gets_its_own_limit_and_its_own_deadline(self) -> None:
        module = self.module(**{self.flag: "1"})
        ((_, command, given),) = self.job_started(module, 4000).items
        self.assertEqual(1800, given["timeout"])
        self.assertIn("JOB_DEADLINE_SECONDS=1700", command)

    def test_a_host_job_near_the_end_of_the_run_gets_only_the_rest_of_the_run_and_a_deadline_inside_it(self) -> None:
        module = self.module(**{self.flag: "1"})
        ((_, command, given),) = self.job_started(module, 500).items
        self.assertLessEqual(given["timeout"], 500)
        self.assertGreater(given["timeout"], 450)
        deadline = int(command[command.index("--env") + 1].split("=")[1])
        self.assertLessEqual(deadline, given["timeout"] - (HOST_LIMITS.job_seconds - HOST_LIMITS.job_deadline_seconds))
        self.assertGreater(deadline, 350)

    def test_a_job_takes_the_rest_of_the_run_when_it_launches_and_not_before_its_scratch_folder_is_made(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module), mock.patch.object(module, "read_result", lambda *_: None):
            stack = self.stack(module, folder)
            self.left(stack, 600)
            made = module.tempfile.mkdtemp

            def slow(*args: Any, **given: Any) -> str:
                stack.started -= 200
                return str(made(*args, **given))

            with mock.patch.object(module.tempfile, "mkdtemp", slow):
                stack.job(["-m", "spark.jobs.x", "build"])
        ((_, command, given),) = calls.items
        self.assertLessEqual(given["timeout"], 400)
        deadline = int(command[command.index("--env") + 1].split("=")[1])
        self.assertLessEqual(deadline, given["timeout"] - (HOST_LIMITS.job_seconds - HOST_LIMITS.job_deadline_seconds))

    def test_a_job_whose_rest_of_the_run_is_spent_while_its_scratch_folder_is_made_is_not_started(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            self.left(stack, 600)
            made = module.tempfile.mkdtemp

            def slow(*args: Any, **given: Any) -> str:
                stack.started -= 590
                return str(made(*args, **given))

            with mock.patch.object(module.tempfile, "mkdtemp", slow), self.assertRaises(RuntimeError):
                stack.job(["-m", "spark.jobs.x", "build"])
        self.assertEqual([], calls.items)

    def test_the_stack_gives_the_rest_of_the_run_to_a_call_to_nessie_and_a_local_run_has_no_rest_to_give(self) -> None:
        host, local = self.module(**{self.flag: "1"}), self.module()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder:
            stack = self.stack(host, folder)
            self.left(stack, 40)
            left = stack.run_left()
            self.assertIsNotNone(left)
            self.assertLessEqual(left or 0, 40)
            self.assertGreater(left or 0, 30)
            self.left(stack, -5)
            self.assertEqual(0, stack.run_left())
            self.assertIsNone(self.stack(local, folder).run_left())

    def test_the_client_of_the_run_asks_the_stack_for_the_rest_of_the_run_at_the_start_of_each_call(self) -> None:
        function = next(
            node for node in ast.walk(ast.parse((E2E / f"{self.NAME}.py").read_text())) if isinstance(node, ast.FunctionDef) and node.name == "run_local"
        )
        clients = [node for node in ast.walk(function) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "NessieClient"]
        self.assertEqual(1, len(clients))
        (remaining,) = [keyword.value for keyword in clients[0].keywords if keyword.arg == "remaining"]
        self.assertEqual("stack.run_left", ast.unparse(remaining))

    def test_a_host_job_with_no_more_than_its_margin_left_is_not_started(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            self.left(stack, 90)
            with self.assertRaises(RuntimeError) as caught:
                stack.job(["-m", "spark.jobs.x", "build"])
        self.assertIn("run deadline of 5400 seconds passed before the step", str(caught.exception))
        self.assertEqual([], calls.items)

    def test_a_local_run_has_no_deadline_however_long_it_has_run(self) -> None:
        module = self.module()
        ((_, command, given),) = self.job_started(module, None).items
        self.assertEqual(3600, given["timeout"])
        self.assertIn("JOB_DEADLINE_SECONDS=3500", command)

    def test_a_build_a_command_and_a_health_wait_get_the_rest_of_the_run_and_none_starts_after_it(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            self.left(stack, 100)
            stack.docker("build", "--tag", "x", timeout=module.BUILD_SECONDS)
            stack.docker("image", "ls")
            stack.up("postgres")
            self.left(stack, 20)
            stack.docker("image", "ls")
            self.left(stack, 0)
            late: list[Any] = [lambda: stack.docker("image", "ls"), lambda: stack.up("nessie"), lambda: stack.compose_run("ps")]
            for call in late:
                with self.assertRaises(RuntimeError):
                    call()
        build, listing, up, brief = calls.items
        self.assertLessEqual(build[2]["timeout"], 100)
        self.assertGreater(build[2]["timeout"], 80)
        self.assertLessEqual(listing[2]["timeout"], 100)
        self.assertLessEqual(up[2]["timeout"], 100)
        self.assertLessEqual(brief[2]["timeout"], 20)
        waited = int(up[1][up[1].index("--wait-timeout") + 1])
        self.assertLessEqual(waited, 100 - module.CALL_SECONDS)
        self.assertEqual(4, len(calls.items))

    def test_a_health_wait_with_the_whole_run_left_waits_the_stated_figure_and_the_command_outlasts_it(self) -> None:
        for flag in (True, False):
            with self.subTest(host=flag):
                module = self.module(**{self.flag: "1"}) if flag else self.module()
                calls = Calls()
                with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
                    self.stack(module, folder).up("postgres")
                ((_, command, given),) = calls.items
                waited = int(command[command.index("--wait-timeout") + 1])
                self.assertEqual(300, waited)
                self.assertGreater(given["timeout"], waited)

    def test_the_cleanup_has_a_budget_of_its_own_and_runs_after_the_run_deadline_has_passed(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls()
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            self.left(stack, -10)
            self.assertEqual((0, ""), stack.cleanup_run(["docker", "ps"]))
            self.assertEqual(180, calls.items[0][2]["timeout"])
            stack.cleanup_started = time.monotonic() - (HOST_LIMITS.cleanup_seconds - 40)
            self.assertEqual((0, ""), stack.cleanup_run(["docker", "ps"]))
            self.assertLessEqual(calls.items[1][2]["timeout"], 40)
            stack.cleanup_started = time.monotonic() - HOST_LIMITS.cleanup_seconds
            self.assertIsNone(stack.cleanup_run(["docker", "ps"]))
        self.assertEqual(2, len(calls.items))

    def test_a_cleanup_command_that_times_out_or_cannot_start_is_not_confirmed_and_never_raises(self) -> None:
        module = self.module(**{self.flag: "1"})
        for error in (TimeoutExpired("docker", 1), FileNotFoundError("docker"), RuntimeError("group survived")):
            with self.subTest(error=type(error).__name__):
                calls = Calls(lambda _, error=error: error)
                with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
                    self.assertIsNone(self.stack(module, folder).cleanup_run(["docker", "ps"]))

    def preflight(self, module: ModuleType, total: int, available: int | None) -> bool:
        record = Record(feature="f", slug="s", rerun="r")
        candidates = read_candidates(CANDIDATES_FILE)
        arguments: tuple[Any, ...] = (record, [], Path("/no/such/cache"), candidates if self.NAME == "silver_signals" else Path("/no/such/extra"))
        with (
            tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
            Calls(lambda _: f"{total}\n").patches(module),
            mock.patch.object(module, "available_memory_mib", lambda: available),
            mock.patch.object(module, "EXTRA_CACHE", Path("/no/such/extra"), create=True),
        ):
            module.preflight(*arguments, self.stack(module, folder))
        (observation,) = [item for item in record.observations if item.case == "preflight"]
        return observation.passed

    def needed_mib(self) -> int:
        limits = load_limits()
        return sum(limits.services[name].memory_limit_mib for name in ("postgres", "nessie", "spark")) + limits.host_reserve_mib

    def test_the_host_preflight_needs_the_services_limits_and_the_host_reserve_free_now_and_a_free_figure(self) -> None:
        module = self.module(**{self.flag: "1"})
        needed = self.needed_mib()
        self.assertEqual(6 * 1024 + load_limits().host_reserve_mib, needed)
        self.assertTrue(self.preflight(module, 32 * GIB, needed + 1024))
        self.assertTrue(self.preflight(module, 32 * GIB, needed))
        self.assertFalse(self.preflight(module, 32 * GIB, needed - 1))
        self.assertFalse(self.preflight(module, 32 * GIB, 6 * 1024))
        self.assertFalse(self.preflight(module, 32 * GIB, None))
        self.assertFalse(self.preflight(module, 8 * GIB, 20 * 1024))

    def test_the_local_preflight_asks_for_the_daemon_total_only(self) -> None:
        module = self.module()
        self.assertTrue(self.preflight(module, 12 * GIB, None))
        self.assertFalse(self.preflight(module, 12 * GIB - 1, None))

    def test_the_memory_is_read_again_before_the_services_start_and_a_short_reading_stops_the_run(self) -> None:
        module = self.module(**{self.flag: "1"})
        needed = self.needed_mib()
        for available, passed in ((needed, True), (needed - 1, False), (None, False)):
            with self.subTest(available=available):
                record = Record(feature="f", slug="s", rerun="r")
                with mock.patch.object(module, "available_memory_mib", lambda available=available: available):
                    self.assertIs(passed, module.memory_before_start(record, []))
                (observation,) = [item for item in record.observations if item.case == "memory before start"]
                self.assertIs(passed, observation.passed)

    def test_the_local_run_reads_no_free_memory_before_the_services_start(self) -> None:
        module = self.module()
        record = Record(feature="f", slug="s", rerun="r")
        with mock.patch.object(module, "available_memory_mib", forbidden):
            self.assertTrue(module.memory_before_start(record, []))
        self.assertEqual([], record.observations)

    def run_local_calls(self, module: ModuleType) -> list[tuple[str, int]]:
        function = next(
            node for node in ast.walk(ast.parse((E2E / f"{self.NAME}.py").read_text())) if isinstance(node, ast.FunctionDef) and node.name == "run_local"
        )
        found: list[tuple[str, int]] = []
        for node in ast.walk(function):
            if isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
                found.append((name, node.lineno))
        return found

    def test_the_memory_is_read_after_the_image_the_warehouse_volume_and_the_password_file_and_right_before_the_first_service_starts(self) -> None:
        module = self.module()
        found = self.run_local_calls(module)
        first = {
            name: min(line for found_name, line in found if found_name == name) for name in ("prepare_image", "prepare_warehouse", "place_nessie_password")
        }
        memory = min(line for name, line in found if name == "memory_before_start")
        first_start = min(line for name, line in found if name == "up")
        for name, line in first.items():
            with self.subTest(step=name):
                self.assertLess(line, memory)
        self.assertLess(memory, first_start)
        between = [name for name, line in found if memory < line < first_start and name not in ("memory_before_start", "up")]
        self.assertEqual([], between, "a step runs between the memory reading and the first service")

    def deployed(self) -> Any:
        return mock.patch.dict(os.environ, {**clean_environment(), **DEPLOYED, "PATH": os.environ.get("PATH", "/usr/bin")}, clear=True)

    def written_stack(self, module: ModuleType, folder: str) -> Any:
        stack = self.stack(module, folder)
        compose = Path(folder) / "compose"
        compose.mkdir()
        (compose / "host.env").write_text(
            "WAREHOUSE_LOCATION=file:///warehouse/\nNESSIE_URI=http://nessie:19120/api/v2\nAWS_REGION=\nPOSTGRES_HOST_PORT=60001\n"
        )
        (compose / "limits.env").write_text("MEM_LIMIT_SPARK=4096m\n")
        return stack

    def test_no_deployment_setting_of_the_caller_reaches_any_compose_or_docker_call_and_the_run_binding_does(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda args: "abc\n" if args[:2] == ["ps", "-aq"] else "1\n")
        with (
            tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
            self.deployed(),
            calls.patches(module),
            mock.patch.object(module, "read_result", lambda *_: None),
        ):
            stack = self.written_stack(module, folder)
            password = Path(folder) / "password"
            password.write_text("secret\n")
            stack.docker("image", "ls")
            stack.compose_run("ps")
            stack.up("postgres")
            stack.job(["-m", "spark.jobs.x", "build"])
            stack.job(["/data/stand_in.py", "x", "b", "/result/result.json"], kind="decoder")
            stack.cleanup_run(["docker", "ps"])
            module.remove_project(stack, [])
            module.place_nessie_password(stack, password, Record(feature="f", slug="s", rerun="r"), [])
            module.prepare_warehouse(stack)
            module.remove_image(stack)
        self.assertGreater(len(calls.items), 15)
        for program, args, given in calls.items:
            with self.subTest(command=" ".join([program, *args[:3]])):
                environment = given["env"]
                self.assertEqual([], sorted(set(DEPLOYED) & (set(environment) - BOUND)))
                self.assertEqual("file:///warehouse/", environment["WAREHOUSE_LOCATION"])
                self.assertEqual("http://nessie:19120/api/v2", environment["NESSIE_URI"])
                self.assertEqual("", environment["AWS_REGION"])
                self.assertEqual("60001", environment["POSTGRES_HOST_PORT"])
                self.assertEqual("4096m", environment["MEM_LIMIT_SPARK"])
                self.assertEqual(os.environ["PATH"], environment["PATH"])

    def test_the_preflight_call_gets_the_explicit_environment_too(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda _: "34359738368\n")
        record = Record(feature="f", slug="s", rerun="r")
        arguments: tuple[Any, ...] = (
            record,
            [],
            Path("/no/such/cache"),
            read_candidates(CANDIDATES_FILE) if self.NAME == "silver_signals" else Path("/no/such/extra"),
        )
        with (
            tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
            self.deployed(),
            calls.patches(module),
            mock.patch.object(module, "available_memory_mib", lambda: 64 * 1024),
            mock.patch.object(module, "EXTRA_CACHE", Path("/no/such/extra"), create=True),
        ):
            module.preflight(*arguments, self.stack(module, folder))
        ((_, _, given),) = calls.items
        self.assertEqual([], sorted(set(DEPLOYED) & set(given["env"])))

    def test_every_process_call_of_the_runner_passes_an_explicit_environment(self) -> None:
        tree = ast.parse((E2E / f"{self.NAME}.py").read_text())
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("run_command", "run_discarding_output")
        ]
        self.assertGreaterEqual(len(calls), 3)
        for call in calls:
            with self.subTest(line=call.lineno):
                self.assertIn("env", [keyword.arg for keyword in call.keywords])

    def test_every_container_that_the_run_creates_carries_the_project_label_and_every_volume_too(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda _: "10000:10001 400\n")
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.written_stack(module, folder)
            password = Path(folder) / "password"
            password.write_text("secret\n")
            module.place_nessie_password(stack, password, Record(feature="f", slug="s", rerun="r"), [])
            module.prepare_warehouse(stack)
        label = "com.docker.compose.project=project"
        runs = [args for _, args, _ in calls.items if args[:1] == ["run"]]
        creates = [args for _, args, _ in calls.items if args[:2] == ["volume", "create"]]
        self.assertEqual(3, len(runs))
        self.assertEqual(2, len(creates))
        for args in [*runs, *creates]:
            with self.subTest(command=" ".join(args[:2])):
                self.assertIn(label, args)
                self.assertEqual("--label", args[args.index(label) - 1])

    def test_a_job_container_is_a_compose_run_in_the_project_so_that_it_carries_the_label_by_itself(self) -> None:
        module = self.module(**{self.flag: "1"})
        ((_, command, _),) = self.job_started(module, 4000).items
        self.assertIn("--project-name", command)
        self.assertEqual("project", command[command.index("--project-name") + 1])

    def removal(self, module: ModuleType, answer: Any, code: Any = None) -> tuple[bool, Calls]:
        calls = Calls(answer, code)
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            return module.remove_project(self.stack(module, folder), []), calls

    def test_a_removal_that_lists_nothing_left_is_confirmed(self) -> None:
        module = self.module(**{self.flag: "1"})
        listings = {"ps": ["abc\n", ""], "volume": ["vol\n", ""], "network": ["net\n", ""]}

        def answer(args: list[str]) -> str:
            return listings[args[0]].pop(0) if "--filter" in args and args[0] in listings else ""

        confirmed, calls = self.removal(module, answer)
        self.assertTrue(confirmed)
        self.assertEqual(3, sum(1 for words in calls.words() if words.startswith(("rm ", "volume rm", "network rm"))))

    def test_a_container_listing_that_times_out_is_tried_again_a_bounded_number_of_times_and_no_dependent_resource_is_removed(self) -> None:
        module = self.module(**{self.flag: "1"})
        confirmed, calls = self.removal(module, lambda args: TimeoutExpired("docker", 1) if args[0] == "ps" else "")
        self.assertFalse(confirmed)
        words = calls.words()
        self.assertEqual(module.CLEANUP_ROUNDS, sum(1 for line in words if line.startswith("ps -aq")))
        self.assertFalse(
            any(line.startswith(("volume rm", "network rm", "rm ")) for line in words), "a volume or a network was removed while a container may hold it"
        )

    def test_a_container_listing_that_fails_once_is_tried_again_and_the_removal_goes_on(self) -> None:
        module = self.module(**{self.flag: "1"})
        steps = {"ps": [(1, ""), (0, "abc\n"), (0, "")], "volume": [(0, "vol\n"), (0, "")], "network": [(0, "net\n"), (0, "")]}
        last = {"code": 0}

        def answer(args: list[str]) -> str:
            if "--filter" in args and args[0] in steps:
                last["code"], output = steps[args[0]].pop(0)
                return output
            last["code"] = 0
            return ""

        confirmed, calls = self.removal(module, answer, lambda _: last["code"])
        words = calls.words()
        self.assertTrue(confirmed)
        self.assertEqual(3, sum(1 for line in words if line.startswith("ps -aq")))
        self.assertLess(
            max(i for i, line in enumerate(words) if line.startswith("ps -aq")), min(i for i, line in enumerate(words) if line.startswith("volume rm"))
        )

    def test_a_container_that_the_last_listing_finds_is_removed_too(self) -> None:
        module = self.module(**{self.flag: "1"})
        listings = {"ps": ["abc\n", "def\n", ""], "volume": [""], "network": [""]}
        confirmed, calls = self.removal(module, lambda args: listings[args[0]].pop(0) if "--filter" in args and args[0] in listings else "")
        self.assertTrue(confirmed)
        removed = [line for line in calls.words() if line.startswith("rm -f")]
        self.assertEqual(["rm -f --volumes abc", "rm -f --volumes def"], removed)

    def test_containers_that_keep_coming_back_are_removed_each_time_but_the_removal_stays_bounded_and_unconfirmed(self) -> None:
        module = self.module(**{self.flag: "1"})
        counter = iter(range(100))
        confirmed, calls = self.removal(module, lambda args: f"c{next(counter)}\n" if args[0] == "ps" and "--filter" in args else "")
        words = calls.words()
        self.assertFalse(confirmed)
        self.assertEqual(module.CLEANUP_ROUNDS, sum(1 for line in words if line.startswith("ps -aq")))
        self.assertEqual(module.CLEANUP_ROUNDS, sum(1 for line in words if line.startswith("rm -f")), "the container of the last listing was not removed")
        self.assertFalse(any(line.startswith(("volume rm", "network rm")) for line in words))

    def test_the_cleanup_is_bounded_by_a_few_rounds(self) -> None:
        self.assertLessEqual(self.module(**{self.flag: "1"}).CLEANUP_ROUNDS, 3)

    def test_a_removal_that_fails_and_leaves_the_object_in_the_listing_is_not_confirmed(self) -> None:
        module = self.module(**{self.flag: "1"})
        confirmed, _ = self.removal(
            module, lambda args: "abc\n" if args[0] == "network" and "ls" in args else "", lambda args: 1 if args[:2] == ["network", "rm"] else 0
        )
        self.assertFalse(confirmed)

    def test_a_listing_that_fails_is_never_an_empty_listing(self) -> None:
        module = self.module(**{self.flag: "1"})
        confirmed, _ = self.removal(module, lambda _: "", lambda args: 1 if args[0] == "ps" else 0)
        self.assertFalse(confirmed)

    def test_a_listing_that_fails_after_the_removal_is_never_an_empty_listing_either(self) -> None:
        module = self.module(**{self.flag: "1"})
        seen: list[str] = []

        def code(args: list[str]) -> int:
            if args[:2] == ["network", "ls"]:
                seen.append("ls")
                return 0 if len(seen) == 1 else 1
            return 0

        confirmed, _ = self.removal(module, lambda args: "net\n" if args[:2] == ["network", "ls"] else "", code)
        self.assertEqual(module.CLEANUP_ROUNDS, len(seen))
        self.assertFalse(confirmed)

    def timed_out(self, module: ModuleType, listings: list[Any], removals: Any = None, known: set[str] | None = None) -> tuple[Calls, Any]:
        """Run ``remove_timed_out`` with the listings of the Spark containers in this order (a list of IDs, or an exception for a listing that times out; the
        last one repeats) and give the calls and the error that it raised (None when it confirmed that none is left)."""
        position = {"at": 0}

        def answer(args: list[str]) -> Any:
            if args[:1] == ["ps"] and "--filter" in args:
                reply = listings[min(position["at"], len(listings) - 1)]
                position["at"] += 1
                return reply if isinstance(reply, BaseException) else "".join(f"{item}\n" for item in reply)
            return ""

        calls = Calls(answer, lambda args: removals(args) if removals and args[:1] == ["rm"] else 0)
        error = None
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            stack.known_containers = set(known or ())
            try:
                stack.remove_timed_out()
            except RuntimeError as caught:
                error = caught
        return calls, error

    def removed_ids(self, calls: Calls) -> list[list[str]]:
        return [args[2:] for _, args, _ in calls.items if args[:2] == ["rm", "-f"]]

    def test_a_container_that_a_live_listing_returns_after_an_earlier_listing_timed_out_is_removed_and_the_removal_is_confirmed(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [TimeoutExpired("docker", 1), ["c1"], []])
        self.assertIsNone(error)
        self.assertEqual([["c1"]], self.removed_ids(calls))

    def test_a_container_that_a_live_listing_returns_is_removed_whatever_listing_it_comes_after(self) -> None:
        module = self.module(**{self.flag: "1"})
        for before in ([OSError("docker")], [TimeoutExpired("docker", 1), TimeoutExpired("docker", 1)]):
            with self.subTest(failed=len(before)):
                calls, error = self.timed_out(module, [*before, ["c1", "c2"], []])
                self.assertIsNone(error)
                self.assertEqual([["c1", "c2"]], self.removed_ids(calls))

    def test_a_container_that_is_listed_and_whose_removal_fails_is_removed_again_when_the_listings_fail_afterwards_and_the_run_fails(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [["c1"], TimeoutExpired("docker", 1)], removals=lambda _: 1)
        self.assertIsNotNone(error)
        removed = self.removed_ids(calls)
        self.assertGreater(len(removed), 1)
        self.assertTrue(all(ids == ["c1"] for ids in removed))

    def test_when_every_listing_fails_the_containers_that_an_earlier_listing_returned_are_still_removed_and_the_run_fails(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [TimeoutExpired("docker", 1)], known={"c9", "c7"})
        self.assertIsNotNone(error)
        self.assertEqual(["c7", "c9"], self.removed_ids(calls)[0])
        self.assertLessEqual(len(self.removed_ids(calls)), module.CLEANUP_ROUNDS)

    def test_a_removal_that_works_with_no_listing_afterwards_is_not_a_confirmation(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [["c1"], TimeoutExpired("docker", 1)])
        self.assertIsNotNone(error)
        self.assertEqual(["c1"], self.removed_ids(calls)[0])

    def test_a_listing_that_fails_with_no_container_known_removes_nothing_and_fails_the_run(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [TimeoutExpired("docker", 1)])
        self.assertIsNotNone(error)
        self.assertEqual([], self.removed_ids(calls))

    def test_containers_that_keep_coming_back_after_a_timeout_are_each_removed_and_the_steps_stay_bounded_and_the_run_fails(self) -> None:
        module = self.module(**{self.flag: "1"})
        counter = iter(range(100))

        def fresh() -> list[str]:
            return [f"c{next(counter)}"]

        calls, error = self.timed_out(module, [fresh() for _ in range(20)])
        self.assertIsNotNone(error)
        self.assertEqual(module.CLEANUP_ROUNDS, len(self.removed_ids(calls)))
        self.assertLessEqual(sum(1 for line in calls.words() if line.startswith("ps ")), module.CLEANUP_ROUNDS + 1)

    def test_an_empty_listing_after_a_timeout_confirms_with_no_removal(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls, error = self.timed_out(module, [[]])
        self.assertIsNone(error)
        self.assertEqual([], self.removed_ids(calls))

    def test_a_job_that_times_out_removes_a_container_that_only_a_later_listing_returns_and_fails_with_the_step_and_not_with_a_container_left(self) -> None:
        module = self.module(**{self.flag: "1"})
        listings = iter([TimeoutExpired("docker", 1), "c1\n", ""])

        def answer(args: list[str]) -> Any:
            if "--entrypoint" in args:
                return TimeoutExpired("docker", 1)
            return next(listings) if args[:1] == ["ps"] and "--filter" in args else ""

        calls = Calls(answer)
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module), self.assertRaises(RuntimeError) as caught:
            self.stack(module, folder).job(["-m", "spark.jobs.x", "build"])
        self.assertIn("did not finish within", str(caught.exception))
        self.assertEqual([["c1"]], self.removed_ids(calls))

    def test_known_containers_are_removed_when_the_listings_of_the_project_keep_failing(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda args: TimeoutExpired("docker", 1) if args[:2] == ["ps", "-aq"] else "")
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            stack.known_containers = {"c1"}
            confirmed = module.remove_project(stack, [])
        self.assertFalse(confirmed)
        self.assertIn("rm -f --volumes c1", calls.words())
        self.assertFalse(any(line.startswith(("volume rm", "network rm")) for line in calls.words()))

    def test_a_container_that_a_listing_of_the_run_returned_is_kept_for_the_removal(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda args: "c1\nc2\n" if args[:1] == ["ps"] and "--filter" in args else "")
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            stack = self.stack(module, folder)
            self.assertEqual(["c1", "c2"], stack.spark_containers())
            self.assertEqual({"c1", "c2"}, stack.known_containers)
            self.assertEqual(["c1", "c2"], stack.spark_containers(cleaning=True))

    def test_the_record_says_that_nessie_and_its_references_are_gone_only_when_the_removal_of_the_project_is_confirmed(self) -> None:
        module = self.module(**{self.flag: "1"})
        claim = "every reference in it went with"
        for confirmed in (True, False):
            with self.subTest(confirmed=confirmed):
                record, _, lines = self.cleaned(module, created=False, remove=lambda *_, confirmed=confirmed: confirmed)
                self.assertEqual(confirmed, any(claim in line for line in lines))
                self.assertEqual(confirmed, self.passed(record, "compose project removed"))
                if not confirmed:
                    self.assertTrue(any("may still hold" in line for line in lines))

    def cleaned(
        self, module: ModuleType, *, created: bool, answer: Any = None, code: Any = None, remove: Any = None, scratch_gone: bool = True
    ) -> tuple[Any, Calls, list[str]]:
        record = Record(feature="f", slug="s", rerun="r")
        calls = Calls(answer, code)
        with tempfile.TemporaryDirectory(prefix="host_runner_") as folder, calls.patches(module):
            scratch = Path(folder) / "scratch"
            scratch.mkdir()
            stack = self.stack(module, folder)
            stack.image_created = created
            patched = mock.patch.object(module, "remove_project", remove) if remove else contextlib.nullcontext()
            kept = mock.patch.object(module.shutil, "rmtree", lambda *_, **__: None) if not scratch_gone else contextlib.nullcontext()
            with patched, kept:
                lines: list[str] = []
                module.clean_up(stack, scratch, record, [], lines)
        return record, calls, lines

    def passed(self, record: Any, case: str) -> bool:
        (observation,) = [item for item in record.observations if item.case == case]
        return bool(observation.passed)

    def test_the_cleanup_removes_the_containers_first_then_the_volumes_the_networks_and_the_image_tag(self) -> None:
        module = self.module(**{self.flag: "1"})
        listings = {"ps": ["abc\n", ""], "volume": ["vol\n", ""], "network": ["net\n", ""]}
        record, calls, _ = self.cleaned(
            module, created=True, answer=lambda args: listings[args[0]].pop(0) if "--filter" in args and args[0] in listings else ""
        )
        words = calls.words()
        order = [next(i for i, line in enumerate(words) if line.startswith(prefix)) for prefix in ("rm -f --volumes", "volume rm", "network rm", "rmi ")]
        self.assertEqual(sorted(order), order)
        self.assertTrue(all(item.passed for item in record.observations))

    def test_the_run_deletes_no_reference_one_by_one_because_its_own_nessie_goes_with_the_project_and_still_keeps_its_facts(self) -> None:
        module = self.module(**{self.flag: "1"})
        nessie = mock.Mock()
        stack = types.SimpleNamespace(stalls=[], cleanup_left=forbidden)
        run = types.SimpleNamespace(tags=[("tag", "hash")], made=["branch", "other"], nessie=nessie, facts={"kept": "fact"}, stack=stack)
        record = Record(feature="f", slug="s", rerun="r")
        with mock.patch.object(module, "run_command", forbidden), mock.patch.object(module, "run_discarding_output", forbidden):
            self.assertTrue(module.finish(run, record))
        self.assertEqual([], nessie.mock_calls)
        self.assertEqual("fact", record.environment["kept"])
        self.assertNotIn("delete_reference", (E2E / f"{self.NAME}.py").read_text())

    def test_a_cleanup_step_that_raises_does_not_skip_the_image_the_scratch_folder_or_the_record(self) -> None:
        module = self.module(**{self.flag: "1"})

        def broken(*_: object, **__: object) -> bool:
            raise RuntimeError("the daemon cannot be asked")

        record, calls, lines = self.cleaned(module, created=True, remove=broken)
        self.assertFalse(self.passed(record, "compose project removed"))
        self.assertTrue(any(line.startswith("rmi ") for line in calls.words()))
        self.assertTrue(self.passed(record, "run image removed"))
        self.assertTrue(self.passed(record, "scratch folder removed"))
        self.assertTrue(lines)

    def test_a_scratch_folder_or_an_image_that_cannot_be_confirmed_gone_fails_the_record(self) -> None:
        module = self.module(**{self.flag: "1"})
        record, _, _ = self.cleaned(module, created=False, scratch_gone=False)
        self.assertFalse(self.passed(record, "scratch folder removed"))
        record, _, _ = self.cleaned(
            module, created=True, answer=lambda args: "image-id\n" if args[:2] == ["image", "ls"] else "", code=lambda args: 1 if args[0] == "rmi" else 0
        )
        self.assertFalse(self.passed(record, "run image removed"))

    def test_an_image_whose_removal_failed_but_is_not_listed_any_more_counts_as_gone(self) -> None:
        module = self.module(**{self.flag: "1"})
        record, _, _ = self.cleaned(module, created=True, code=lambda args: 1 if args[0] == "rmi" else 0)
        self.assertTrue(self.passed(record, "run image removed"))

    def test_the_cleanup_removes_no_image_when_the_run_created_none(self) -> None:
        module = self.module(**{self.flag: "1"})
        _, calls, lines = self.cleaned(module, created=False)
        self.assertFalse(any("rmi" in words for words in calls.words()))
        self.assertTrue(any("no image tag was created" in line for line in lines))

    def test_the_cleanup_removes_only_the_tag_of_the_run_and_never_the_given_image_or_the_fixed_tag(self) -> None:
        module = self.module(**{self.flag: "1"})
        with mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", **self.given_image()), clear=True):
            module.select_settings()
            _, calls, _ = self.cleaned(module, created=True)
            tag = module.IMAGE
        removed = [args for _, args, _ in calls.items if args[:1] == ["rmi"]]
        self.assertEqual([["rmi", tag]], removed)
        self.assertNotEqual(module.IMAGE_TAG, tag)
        self.assertNotIn("surgical-risk-spark:4.1.4", [word for args in removed for word in args])

    def image_run(
        self,
        module: ModuleType,
        *,
        given: bool,
        answer: Any,
        code: Any = None,
        files: str = IMAGE_FILES,
        tree: str = TREE,
        dockerfile: str = DOCKERFILE,
        dockerignore: str = DOCKERIGNORE,
        image_settings: dict[str, str] | None = None,
    ) -> tuple[tuple[bool, bool], Calls, Record]:
        def with_source(args: list[str]) -> Any:
            """The answers of the four source calls (the commit's tree, its Dockerfile, its Docker ignore file and the Python files in the image) beside the
            test's own."""
            if "ls-tree" in args:
                return tree
            if "show" in args:
                return dockerignore if args[-1] == "HEAD:spark/Dockerfile.dockerignore" else dockerfile
            if args[:1] == ["run"] and args[-1] == host_mode.SOURCE_SCRIPT:
                return files
            return answer(args)

        record = Record(feature="f", slug="s", rerun="r")
        calls = Calls(with_source, code)
        settings = image_settings if image_settings is not None else self.given_image() if given else {}
        with (
            mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", **settings), clear=True),
            tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
            calls.patches(module),
            mock.patch.object(module, "HOST", given or image_settings is not None),
        ):
            module.select_settings()
            stack = self.stack(module, folder)
            result = (module.prepare_image(stack, record), stack.image_created)
        return result, calls, record

    def test_a_run_that_builds_checks_that_its_tag_is_new_builds_under_it_and_has_created_it(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, record = self.image_run(module, given=False, answer=lambda _: "")
        self.assertEqual((True, True), result)
        words = calls.words()
        self.assertTrue(words[0].startswith("image ls --quiet "))
        build = next(args for _, args, _ in calls.items if args[:1] == ["build"])
        self.assertEqual(module.IMAGE, build[build.index("--tag") + 1])
        self.assertTrue(all(item.passed for item in record.observations))

    def test_a_run_tag_that_already_exists_is_never_built_over_and_never_removed_later(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, record = self.image_run(module, given=False, answer=lambda args: "an-image-id\n" if args[:2] == ["image", "ls"] else "")
        self.assertEqual((False, False), result)
        self.assertFalse(any(words.startswith(("build", "image tag")) for words in calls.words()))
        self.assertFalse(all(item.passed for item in record.observations))

    def test_a_tag_check_that_fails_is_not_a_tag_that_is_free(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, _ = self.image_run(module, given=False, answer=lambda _: "", code=lambda args: 1 if args[:2] == ["image", "ls"] else 0)
        self.assertEqual((False, False), result)
        self.assertFalse(any(words.startswith("build") for words in calls.words()))

    def test_a_given_image_with_the_recorded_id_is_tagged_by_that_id_and_the_id_is_recorded(self) -> None:
        module = self.module(**{self.flag: "1"})

        def answer(args: list[str]) -> str:
            return f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else ""

        result, calls, record = self.image_run(module, given=True, answer=answer)
        self.assertEqual((True, True), result)
        tag = next(args for _, args, _ in calls.items if args[:2] == ["image", "tag"])
        self.assertEqual(["image", "tag", IMAGE_ID, module.IMAGE], tag)
        self.assertFalse(any(words.startswith("build") for words in calls.words()))
        self.assertIn("the image ID is the recorded one True", observed(record, "image"))
        self.assertTrue(all(item.passed for item in record.observations))

    def test_a_given_image_with_another_id_is_refused_and_nothing_is_tagged_or_built(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, record = self.image_run(module, given=True, answer=lambda args: f"{OTHER_ID}\n" if args[:2] == ["image", "inspect"] else "")
        self.assertEqual((False, False), result)
        self.assertFalse(any(words.startswith(("build", "image tag")) for words in calls.words()))
        self.assertFalse(all(item.passed for item in record.observations))
        self.assertIn("the image ID is the recorded one False", observed(record, "image"))
        self.assertNotIn(OTHER_ID, " ".join(item.observed for item in record.observations))

    def test_a_given_image_that_docker_cannot_inspect_is_refused(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, _ = self.image_run(module, given=True, answer=lambda _: "", code=lambda args: 1 if args[:2] == ["image", "inspect"] else 0)
        self.assertEqual((False, False), result)
        self.assertFalse(any(words.startswith("image tag") for words in calls.words()))

    def test_a_given_image_with_the_recorded_id_is_read_for_its_files_by_its_id_in_a_container_that_carries_the_project_label_and_has_no_network(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, calls, record = self.image_run(module, given=True, answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "")
        self.assertEqual((True, True), result)
        (probe,) = [args for _, args, _ in calls.items if args[:1] == ["run"]]
        self.assertIn("com.docker.compose.project=project", probe)
        self.assertEqual("--label", probe[probe.index("com.docker.compose.project=project") - 1])
        self.assertEqual("none", probe[probe.index("--network") + 1])
        self.assertIn(IMAGE_ID, probe)
        self.assertNotIn("surgical-risk-spark:4.1.4", probe)
        (tree,) = [args for _, args, _ in calls.items if "ls-tree" in args]
        self.assertEqual("HEAD", tree[tree.index("ls-tree") + 2])
        self.assertEqual(["HEAD:spark/Dockerfile", "HEAD:spark/Dockerfile.dockerignore"], [args[-1] for _, args, _ in calls.items if "show" in args])
        self.assertTrue(self.passed(record, "image source"))

    def test_a_given_image_that_lacks_a_file_that_the_dockerfile_copies_from_a_folder_other_than_spark_is_refused_and_nothing_is_tagged(self) -> None:
        module = self.module(**{self.flag: "1"})
        tree = TREE + f"100644 blob {SHA_Z}\tfeatures/signal_validity.py\n"
        dockerfile = DOCKERFILE + "COPY features/signal_validity.py /opt/jobs/features/signal_validity.py\n"
        for label, files, problem in (
            ("the file is missing", IMAGE_FILES, False),
            ("the file is there", IMAGE_FILES + f"{SHA_Z} features/signal_validity.py\n", True),
        ):
            with self.subTest(case=label):
                answer = lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else ""  # noqa: E731
                result, calls, record = self.image_run(
                    module,
                    given=True,
                    answer=answer,
                    files=files,
                    tree=tree,
                    dockerfile=dockerfile,
                    dockerignore=DOCKERIGNORE + "!features/signal_validity.py\n",
                )
                self.assertEqual((problem, problem), result)
                self.assertEqual(problem, any(words.startswith("image tag") for words in calls.words()))
                self.assertEqual(problem, self.passed(record, "image source"))

    def test_a_file_that_the_docker_ignore_file_leaves_out_is_not_in_the_image_that_the_run_accepts(self) -> None:
        module = self.module(**{self.flag: "1"})
        answer = lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else ""  # noqa: E731
        without_x = f"{SHA_Y} spark/silver/y.py\n{SHA_Z} scripts/stack/nessie.py\n"
        for label, files, ignore, accepted in (
            ("the file is left out and the image lacks it", without_x, DOCKERIGNORE + "spark/jobs/x.py\n", True),
            ("the file is left out and the image holds it", IMAGE_FILES, DOCKERIGNORE + "spark/jobs/x.py\n", False),
            ("the file is not left out and the image lacks it", without_x, DOCKERIGNORE, False),
            ("a rule of a form that is not read", without_x, DOCKERIGNORE + "spark/jobs/*.py\n", False),
        ):
            with self.subTest(case=label):
                result, calls, record = self.image_run(module, given=True, answer=answer, files=files, dockerignore=ignore)
                self.assertEqual((accepted, accepted), result)
                self.assertEqual(accepted, any(words.startswith("image tag") for words in calls.words()))
                self.assertEqual(accepted, self.passed(record, "image source"))

    def test_the_docker_ignore_file_is_read_as_docker_reads_it_before_the_run_accepts_an_image(self) -> None:
        module = self.module(**{self.flag: "1"})
        answer = lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else ""  # noqa: E731
        without_x = f"{SHA_Y} spark/silver/y.py\n{SHA_Z} scripts/stack/nessie.py\n"
        for label, files, ignore, accepted in (
            ("a hash after a space is a rule of a form that is not read", IMAGE_FILES, DOCKERIGNORE + " #*/drop.py\n", False),
            ("a hash after a tab is a rule of a form that is not read", IMAGE_FILES, DOCKERIGNORE + "\t#spark/jobs/*.py\n", False),
            ("a hash in the first column is a comment", IMAGE_FILES, DOCKERIGNORE + "#*/drop.py\n", True),
            ("a mark before the rule leaves the file out and the image holds it", IMAGE_FILES, "\ufeffspark/jobs/x.py\n", False),
            ("a mark before the rule leaves the file out and the image lacks it", without_x, "\ufeffspark/jobs/x.py\n", True),
            ("a mark alone on the first line changes no rule", without_x, "\ufeff\nspark/jobs/x.py\n", True),
            ("a mark before a comment is a comment", IMAGE_FILES, "\ufeff# spark/jobs/x.py\n", True),
            ("line ends of a carriage return and a line feed change no rule", without_x, DOCKERIGNORE.replace("\n", "\r\n") + "spark/jobs/x.py\r\n", True),
        ):
            with self.subTest(case=label):
                result, calls, record = self.image_run(module, given=True, answer=answer, files=files, dockerignore=ignore)
                self.assertEqual((accepted, accepted), result)
                self.assertEqual(accepted, any(words.startswith("image tag") for words in calls.words()))
                self.assertEqual(accepted, self.passed(record, "image source"))

    def test_a_given_image_with_the_recorded_id_but_other_files_than_the_commit_is_refused_and_nothing_is_tagged(self) -> None:
        module = self.module(**{self.flag: "1"})
        for label, files in (
            ("a file that differs", IMAGE_FILES.replace(SHA_Y, "f6" * 20)),
            ("a file that the commit does not hold", IMAGE_FILES + f"{SHA_X} spark/jobs/extra.py\n"),
            ("a job file that is missing", f"{SHA_X} spark/jobs/x.py\n"),
            ("no listing", ""),
        ):
            with self.subTest(case=label):
                result, calls, record = self.image_run(
                    module, given=True, answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "", files=files
                )
                self.assertEqual((False, False), result)
                self.assertFalse(any(words.startswith(("build", "image tag")) for words in calls.words()))
                self.assertFalse(self.passed(record, "image source"))

    def test_a_given_image_whose_files_cannot_be_read_or_whose_commit_cannot_be_listed_is_refused(self) -> None:
        module = self.module(**{self.flag: "1"})
        for failing in ("probe", "tree", "dockerfile", "dockerignore"):
            with self.subTest(failing=failing):

                def code(args: list[str], failing: str = failing) -> int:
                    probed = args[:1] == ["run"] and args[-1] == host_mode.SOURCE_SCRIPT
                    named = {"dockerfile": "HEAD:spark/Dockerfile", "dockerignore": "HEAD:spark/Dockerfile.dockerignore"}.get(failing)
                    return (
                        1 if (failing == "probe" and probed) or (failing == "tree" and "ls-tree" in args) or (named is not None and args[-1:] == [named]) else 0
                    )

                result, calls, record = self.image_run(
                    module, given=True, answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "", code=code
                )
                self.assertEqual((False, False), result)
                self.assertFalse(any(words.startswith("image tag") for words in calls.words()))
                self.assertFalse(self.passed(record, "image source"))

    def test_the_record_holds_only_booleans_of_the_image_check_and_never_the_id_a_file_name_or_a_file_id(self) -> None:
        module = self.module(**{self.flag: "1"})
        for files in (IMAGE_FILES, IMAGE_FILES.replace(SHA_Y, "f6" * 20)):
            _, _, record = self.image_run(module, given=True, answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "", files=files)
            text = " ".join(item.observed for item in record.observations) + " " + str(record.environment)
            for private in (IMAGE_ID, "ab" * 32, SHA_X, SHA_Y, SHA_Z, "f6" * 20, "spark/jobs", "x.py", "nessie.py"):
                self.assertNotIn(private, text)
        self.assertIn("the image files are the files of the commit", observed(record, "image source"))

    def test_a_run_that_builds_the_image_reads_no_source_listing_because_the_build_is_from_the_checkout(self) -> None:
        module = self.module(**{self.flag: "1"})
        _, calls, _ = self.image_run(module, given=False, answer=lambda _: "")
        self.assertFalse(any("ls-tree" in args or args[:1] == ["run"] for _, args, _ in calls.items))

    def test_the_header_of_the_runner_promises_no_raw_image_id_and_names_the_check_of_the_files(self) -> None:
        text = (E2E / f"{self.NAME}.py").read_text()
        header = text[: text.index("from __future__")]
        self.assertNotIn("spark_image_id", header)
        self.assertNotIn("spark_image_id", text)
        self.assertIn("SPARK_IMAGE_ID", header)
        self.assertIn("/opt/jobs", header)
        self.assertIn("Boolean", header)

    def test_the_two_accepted_limits_of_the_host_mode_are_in_the_header_and_in_the_limits_of_the_host_record(self) -> None:
        text = (E2E / f"{self.NAME}.py").read_text()
        header = " ".join(text[: text.index("from __future__")].split())
        limits = (
            "a call to Nessie can finish up to about one socket timeout after the run deadline",
            "a very slow read of an environment file can let Spark start with a memory cap that was computed before the read",
        )
        host_branch = text[text.index("record.limits = [") :]
        for limit in limits:
            with self.subTest(limit=limit):
                self.assertIn(limit, header)
                self.assertEqual(1, host_branch.count(f'record.limits.append("{limit}")'))
                self.assertLess(host_branch.index("if HOST:"), host_branch.index(limit))
                self.assertLess(host_branch.index(limit), host_branch.index("else:"))

    def test_a_tag_that_could_not_be_made_from_the_verified_id_is_not_counted_as_created_by_a_success(self) -> None:
        module = self.module(**{self.flag: "1"})
        result, _, record = self.image_run(
            module,
            given=True,
            answer=lambda args: f"{IMAGE_ID}\n" if args[:2] == ["image", "inspect"] else "",
            code=lambda args: 1 if args[:2] == ["image", "tag"] else 0,
        )
        self.assertFalse(result[0])
        self.assertFalse(all(item.passed for item in record.observations))

    def test_a_build_or_tag_that_is_stopped_by_a_timeout_still_counts_as_started_so_that_the_cleanup_removes_what_it_left(self) -> None:
        module = self.module(**{self.flag: "1"})
        for given, command in ((False, "build"), (True, "image")):
            with self.subTest(given=given):
                record = Record(feature="f", slug="s", rerun="r")
                settings = self.given_image() if given else {}

                def answer(args: list[str], given: bool = given, command: str = command) -> Any:
                    if args[:2] == ["image", "inspect"]:
                        return f"{IMAGE_ID}\n"
                    if "ls-tree" in args:
                        return TREE
                    if "show" in args:
                        return DOCKERIGNORE if args[-1] == "HEAD:spark/Dockerfile.dockerignore" else DOCKERFILE
                    if args[:1] == ["run"] and args[-1] == host_mode.SOURCE_SCRIPT:
                        return IMAGE_FILES
                    if args[:1] == [command] and (args[:2] == ["image", "tag"] or not given):
                        return TimeoutExpired("docker", 1)
                    return ""

                calls = Calls(answer)
                with (
                    mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", **settings), clear=True),
                    tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
                    calls.patches(module),
                    mock.patch.object(module, "HOST", given),
                ):
                    module.select_settings()
                    stack = self.stack(module, folder)
                    with self.assertRaises(TimeoutExpired):
                        module.prepare_image(stack, record)
                self.assertTrue(stack.image_created)

    def extra(self) -> tuple[Any, ...]:
        return (read_candidates(CANDIDATES_FILE),) if self.NAME == "silver_signals" else (Path("/no/such/extra"),)

    def run_local_with(self, module: ModuleType, record: Record, *patches: Any) -> Calls:
        """Run ``run_local`` with the host settings, no process, no real port (the allocation is a stand-in, any socket fails the test) and the patches."""
        calls = Calls(lambda _: "")
        ports = iter(range(60001, 60100))
        with (
            mock.patch.dict(os.environ, clean_environment(**{self.flag: "1"}, LANDED_CACHE="/cache", **self.given_image()), clear=True),
            calls.patches(module),
            mock.patch.object(module, "free_port", lambda taken: next(ports)),
            mock.patch.object(module.socket, "socket", forbidden),
            contextlib.ExitStack() as stack,
        ):
            for patch in patches:
                stack.enter_context(patch)
            module.select_settings()
            module.run_local(record, [], Path("/no/such/cache"), *self.extra())
        return calls

    def run_refused_at_preflight(self, module: ModuleType) -> Calls:
        return self.run_local_with(module, Record(feature="f", slug="s", rerun="r"), mock.patch.object(module, "preflight", lambda *_, **__: None))

    def test_a_fast_run_of_the_setup_binds_no_port_of_its_own(self) -> None:
        module = self.module(**{self.flag: "1"})
        self.run_refused_at_preflight(module)

    def test_the_scratch_folder_is_removed_when_the_setup_fails_before_the_first_service(self) -> None:
        module = self.module(**{self.flag: "1"})
        failures = {
            "no free port": mock.patch.object(module, "free_port", mock.Mock(side_effect=RuntimeError("no free port"))),
            "no compose copy": mock.patch.object(module.shutil, "copytree", mock.Mock(side_effect=OSError("disk"))),
        }
        for label, failure in failures.items():
            with self.subTest(case=label), tempfile.TemporaryDirectory(prefix="host_runner_") as parent:
                scratch = Path(parent) / "scratch"
                record = Record(feature="f", slug="s", rerun="r")

                def made(*_: Any, scratch: Path = scratch, **__: Any) -> str:
                    scratch.mkdir()
                    return str(scratch)

                with mock.patch.object(module.tempfile, "mkdtemp", made), self.assertRaises((RuntimeError, OSError)):
                    self.run_local_with(module, record, failure)
                self.assertFalse(scratch.exists(), "the scratch folder was left")
                self.assertTrue(self.passed(record, "scratch folder removed"))

    def test_a_preflight_refusal_removes_no_image_and_builds_none_and_every_call_has_the_environment(self) -> None:
        module = self.module(**{self.flag: "1"})
        calls = self.run_refused_at_preflight(module)
        words = calls.words()
        self.assertFalse(any(line.startswith(("rmi", "build", "image tag", "image ls", "image inspect")) for line in words))
        self.assertTrue(any(line.startswith("ps -aq") for line in words))
        for _, _, given in calls.items:
            self.assertIn("env", given)


class SignalsHostRunnerTest(HostRunnerCase):
    NAME = "silver_signals"

    def test_the_gold_dimensions_runner_still_calls_preflight_and_the_password_placement_in_their_old_forms(self) -> None:
        """``tests/e2e/gold_dimensions.py`` (untracked) calls ``preflight(record, log, cache, candidates)`` and ``place_nessie_password(project, ...)``."""
        module = self.module(**{self.flag: "1"})
        calls = Calls(lambda args: "34359738368\n" if args[:1] == ["info"] else "10000:10001 400\n")
        record = Record(feature="f", slug="s", rerun="r")
        with (
            tempfile.TemporaryDirectory(prefix="host_runner_") as folder,
            self.deployed(),
            calls.patches(module),
            mock.patch.object(module, "available_memory_mib", lambda: 64 * 1024),
            mock.patch.object(module, "EXTRA_CACHE", Path("/no/such/extra"), create=True),
        ):
            module.preflight(record, [], Path("/no/such/cache"), read_candidates(CANDIDATES_FILE))
            password = Path(folder) / "password"
            password.write_text("secret\n")
            self.assertTrue(module.place_nessie_password("project", password, record, []))
        self.assertEqual(4, len(calls.items))
        for _, args, given in calls.items:
            self.assertEqual([], sorted(set(DEPLOYED) & set(given["env"])))
            if args[:1] == ["run"]:
                self.assertIn("com.docker.compose.project=project", args)


class HypotensionHostRunnerTest(HostRunnerCase):
    NAME = "silver_hypotension"


del HostRunnerCase

if __name__ == "__main__":
    unittest.main()
