"""The end-to-end runners that start a Spark job name its service, profile, credential folder and memory limit through ``scripts/stack/job_workloads.py``.

The runners are not tracked (``tests/e2e/*`` is ignored but for four files), so each check skips when the runners are absent. The one ``spark`` service, the
``batch`` profile, ``SPARK_ACCESS_DIR`` and ``MEM_LIMIT_SPARK`` are gone: a runner that still names one would start no job, or start it under no identity. The
scan reads every runner for those names, and the launch helper of the runners (``tests/e2e/spark_launch.py``) is held to what the services and the launcher
do. The planted cases put each stale name into a clean text and expect the scan to name it.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import io
import re
import types
import unittest
from collections.abc import Callable, Sequence
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, SPARK_SERVICES
from scripts.stack.spark_slot import OWNER_LABEL, SLOT_NAME, SlotBusy, SlotError
from tests.fast.spark_engine import Box, Engine, finishes

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = sorted((ROOT / "tests" / "e2e").glob("*.py")) + sorted((ROOT / "tests" / "e2e").glob("*/*.py"))
# A runner whose own Compose file (a stream stack, a replay stack) has its own ``batch`` service is not about the Spark services.
OWN_COMPOSE = frozenset({"stream_features.py", "runner_checks.py", "batch_features.py", "batch_oracle.py"})
STALE = {
    "the old access setting": re.compile(r"SPARK_ACCESS_DIR"),
    "the old memory limit": re.compile(r"MEM_LIMIT_SPARK(?!_)"),
    "the old service": re.compile(r"""["']spark["']"""),
    "the old profile": re.compile(r"""["']batch["']"""),
    "the old service file": re.compile(r"spark\.yml"),
    "the old workload role": re.compile(r"workload_spark"),
    "the old container filter": re.compile(r"com\.docker\.compose\.service=spark(?!_)"),
}
# The only line that names the ``spark`` folder as a string: a path, not a service.
PATHS = re.compile(r"""ROOT / ["']spark["']|ROOT / 'spark'|\{ROOT / 'spark'\}""")


def stale_names(text: str) -> list[str]:
    """Each stale name in ``text``, as ``what: line``, ignoring the ``spark`` folder as a path."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if PATHS.search(line):
            continue
        found += [f"{what}: line {number}" for what, pattern in STALE.items() if pattern.search(line)]
    return found


def repeated_filters(text: str) -> list[str]:
    """Each line of ``text`` that lists Spark containers with a repeated filter key: the old list ``SPARK_FILTERS``, or two values of the service label."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if "SPARK_FILTERS" in line:
            found.append(f"SPARK_FILTERS: line {number}")
        if len(re.findall(r"label=com\.docker\.compose\.service=", line)) > 1:
            found.append(f"the service label twice in one request: line {number}")
    return found


@unittest.skipUnless(RUNNERS, "the untracked runners are not here")
class RunnerScanTest(unittest.TestCase):
    def test_no_runner_names_a_removed_service_profile_or_setting(self) -> None:
        found = {path.relative_to(ROOT).as_posix(): stale_names(path.read_text()) for path in RUNNERS if path.name not in OWN_COMPOSE}
        self.assertEqual({}, {name: hits for name, hits in found.items() if hits})

    def test_the_scan_names_each_stale_name_when_one_is_planted(self) -> None:
        planted = {
            "the old access setting": 'f"SPARK_ACCESS_DIR={access}"',
            "the old memory limit": 're.sub(r"MEM_LIMIT_SPARK=\\d+m", "x", text)',
            "the old service": 'words = ["docker", "compose", "run", "spark", "x"]',
            "the old profile": 'command = ["--profile", "batch", "run"]',
            "the old service file": 'path = compose / "services" / "spark.yml"',
            "the old workload role": '"workload_spark" in identity',
            "the old container filter": '"label=com.docker.compose.service=spark"',
        }
        for what, text in planted.items():
            with self.subTest(what=what):
                self.assertEqual([f"{what}: line 1"], stale_names(text))
        self.assertEqual([], stale_names('build = ["--file", str(ROOT / "spark" / "Dockerfile")]\nservice = "spark_silver"\nprofile = "batch_silver"'))


@unittest.skipUnless((ROOT / "tests" / "e2e" / "spark_launch.py").is_file(), "the untracked runner helper is not here")
class LaunchHelperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.launch: Any = importlib.import_module("tests.e2e.spark_launch")

    def test_a_launch_keeps_the_python_entry_point_and_names_the_service_of_the_job_that_its_arguments_run(self) -> None:
        job = ["-m", "spark.jobs.silver_cases", "build", "--branch", "b"]
        self.assertEqual(["--entrypoint", "python", *self.launch.slot_words()], self.launch.launch_options(job, "gold_dimensions"))
        self.assertEqual(["spark_silver", *job], self.launch.launch_words(job, "gold_dimensions"))
        script = ["/data/stand_in.py", "build", "b"]
        self.assertEqual(["spark_gold", *script], self.launch.launch_words(script, "gold_dimensions"))

    def test_every_job_resolves_to_its_workloads_service_profile_and_credential_folder(self) -> None:
        for job, workload in JOB_WORKLOADS.items():
            if workload not in SERVICE_WORKLOADS:
                continue
            arguments = ["-m", f"spark.jobs.{job}", "x"]
            with self.subTest(job=job):
                self.assertEqual(f"spark_{workload}", self.launch.service_for(arguments, "catalog_proof"))
                self.assertEqual(f"batch_{workload}", self.launch.profile_for(arguments, "catalog_proof"))
                self.assertEqual([ROOT / "a" / workload], self.launch.access_folders(ROOT / "a", [job]))
                self.assertEqual([f"{workload.upper()}_ACCESS_DIR={ROOT / 'a' / workload}"], self.launch.access_settings(ROOT / "a", [job]))

    LABEL = "com.docker.compose"

    def docker_ps(self, containers: list[tuple[str, str, dict[str, str]]], arguments: list[str]) -> str:
        """The output of ``docker ps`` over ``containers`` (ID, state, labels), with the label filters of Docker 29.9.3: the filters of one key are ANDed
        (``daemon/internal/filters/parse.go``, ``GetBoolOrDefault`` and ``Match`` over a list of values), so two values for one label key match no container."""
        self.assertEqual("ps", arguments[0])
        wanted = [arguments[position + 1].removeprefix("label=").split("=", 1) for position, word in enumerate(arguments) if word == "--filter"]
        every = "--all" in arguments
        lines = []
        for identifier, state, labels in containers:
            if (every or state == "running") and all(labels.get(key) == value for key, value in wanted):
                lines.append(f"{identifier} {labels.get(self.LABEL + '.service', '')}")
        return "\n".join(lines) + "\n" if lines else ""

    def stack(self, live: str) -> list[tuple[str, str, dict[str, str]]]:
        project = {f"{self.LABEL}.project": "stack_example"}
        return [
            ("c_nessie", "running", project | {f"{self.LABEL}.service": "nessie"}),
            ("c_live", "running", project | {f"{self.LABEL}.service": live}),
            ("c_old", "exited", project | {f"{self.LABEL}.service": "spark_gold"}),
            ("c_other", "running", {f"{self.LABEL}.project": "another", f"{self.LABEL}.service": live}),
        ]

    def test_one_live_container_of_each_spark_service_is_listed_with_the_filters_of_docker_29_9_3(self) -> None:
        """One live Silver container gave an empty list, so the stall check and the cleanup saw no Spark container: four values of one key are ANDed."""
        for service in SPARK_SERVICES:
            with self.subTest(service=service):
                listed = self.docker_ps(self.stack(service), self.launch.ps_arguments("stack_example", all_states=False))
                self.assertEqual(["c_live"], self.launch.spark_ids(listed))

    def test_the_cleanup_listing_keeps_a_stopped_spark_container_and_the_live_listing_does_not(self) -> None:
        containers = self.stack("spark_silver")
        self.assertEqual(["c_live"], self.launch.spark_ids(self.docker_ps(containers, self.launch.ps_arguments("stack_example", all_states=False))))
        self.assertEqual(["c_live", "c_old"], self.launch.spark_ids(self.docker_ps(containers, self.launch.ps_arguments("stack_example", all_states=True))))

    def test_the_listing_asks_once_by_project_with_no_service_filter_and_selects_the_spark_services_in_python(self) -> None:
        arguments = self.launch.ps_arguments("stack_example", all_states=True)
        filters = [arguments[position + 1] for position, word in enumerate(arguments) if word == "--filter"]
        self.assertEqual(["label=com.docker.compose.project=stack_example"], filters)
        self.assertEqual(["c_a", "c_b"], self.launch.spark_ids("c_a spark_bronze\nc_n nessie\nc_b spark_maintenance\n\nc_x\nc_y spark_bronze_extra\n"))
        self.assertEqual([], self.launch.spark_ids(""))

    def test_the_old_helper_list_is_gone(self) -> None:
        self.assertFalse(hasattr(self.launch, "SPARK_FILTERS"))


SLOT_OPTIONS = frozenset({"slot_words", "launch_options"})
SERVICE_NAMES = frozenset({"service_of", "service_for", "profile_of_job", "profile_for", "launch_words"})
# A removal of every container that a listing returned, in place of the one cleanup path that removes the launch's own container.
HAND_REMOVAL = re.compile(r"""docker\("rm", "-f", \*|def remove_timed_out""")


def called_name(node: ast.AST) -> str:
    return getattr(node.func, "id", getattr(node.func, "attr", "")) if isinstance(node, ast.Call) else ""


def spark_launch_problems(text: str) -> list[str]:
    """What is wrong with the Spark one-offs of a runner: the words of a ``docker compose run`` (a list or a call that holds ``run``, ``--no-deps`` and a name
    of the job helper or the slot's options) must take the slot, must not use ``--rm``, and the function that holds them must start them through
    ``launch_in_slot``, the one launch and cleanup path. A removal of listed containers by hand is a second cleanup path."""
    tree = ast.parse(text)
    outers: list[ast.Module | ast.ClassDef] = [tree, *(node for node in tree.body if isinstance(node, ast.ClassDef))]
    functions = [node for outer in outers for node in outer.body if isinstance(node, ast.FunctionDef)]
    found = [f"a removal by hand: line {number}" for number, line in enumerate(text.splitlines(), start=1) if HAND_REMOVAL.search(line)]
    for function in functions:
        shared = any(called_name(node) == "launch_in_slot" for node in ast.walk(function))
        for node in ast.walk(function):
            if not isinstance(node, (ast.List, ast.Call)):
                continue
            words = node.elts if isinstance(node, ast.List) else node.args
            constants = {word.value for word in words if isinstance(word, ast.Constant)}
            if not {"run", "--no-deps"} <= constants:
                continue
            takes = any(isinstance(word, ast.Starred) and called_name(word.value) in SLOT_OPTIONS for word in words)
            if not takes and not any(called_name(inner) in SERVICE_NAMES for word in words for inner in ast.walk(word)):
                continue
            wrong = [] if takes else ["no slot"]
            wrong += ["--rm"] if "--rm" in constants else []
            wrong += [] if shared else ["outside the shared path"]
            found += [f"{what}: line {node.lineno}" for what in wrong]
    return found


class FakeTarget:
    """The target of the catalog proof runner: its ``run`` answers like ``Local.run`` and ``Remote.run`` over the stand-in engine."""

    compose_directory = "/c"
    project = "p"
    secrets_directory = "/s"

    def __init__(self, engine: Engine, *, over_ssh: bool = False) -> None:
        self.engine = engine
        self.over_ssh = over_ssh

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 60) -> tuple[int, str, str]:
        try:
            reply = self.engine.docker(args[1:] if args[0] == "docker" else args, timeout)
        except TimeoutExpired as error:
            if not self.over_ssh:
                raise
            return 255, "", type(error).__name__
        return reply.returncode, reply.stdout, reply.stderr


@unittest.skipUnless((ROOT / "tests" / "e2e" / "catalog_proof.py").is_file(), "the untracked catalog proof runner is not here")
class CatalogProofLaunchTest(unittest.TestCase):
    """The catalog proof starts its Spark job in the one slot: the same container name that ``spark_run`` and the table checks use."""

    def setUp(self) -> None:
        self.proof: Any = importlib.import_module("tests.e2e.catalog_proof")
        self.launch: Any = importlib.import_module("tests.e2e.spark_launch")
        self.engine = Engine()

    def stack(self, *, over_ssh: bool = False) -> Any:
        return self.proof.Stack(FakeTarget(self.engine, over_ssh=over_ssh), types.SimpleNamespace(commands=[]), [])

    def test_the_job_is_refused_while_a_container_holds_the_slots_name_in_any_state_and_that_container_is_left_alone(self) -> None:
        for state in ("running", "paused", "restarting", "exited"):
            engine = Engine()
            engine.hold(state=state)
            self.engine = engine
            with self.subTest(state=state), self.assertRaisesRegex(SlotBusy, SLOT_NAME):
                self.stack().job(["--branch", "b"])
            self.assertEqual([], engine.launches())
            self.assertEqual(1, len(engine.boxes))

    def test_the_job_container_has_the_slots_name_and_this_runs_owner_before_the_service(self) -> None:
        code, _, _, _ = self.stack().job(["--branch", "b"], {"JOB_DEADLINE_SECONDS": "3"})
        (launch,) = self.engine.launches()
        self.assertEqual(0, code)
        self.assertEqual(SLOT_NAME, launch[launch.index("--name") + 1])
        self.assertEqual(f"{OWNER_LABEL}={self.launch.RUN_OWNER}", launch[launch.index("--label") + 1])
        self.assertLess(launch.index("--label"), launch.index("spark_maintenance"))
        self.assertIn("--env", launch)

    def test_a_start_that_loses_the_name_to_another_launch_is_refused_and_that_container_is_left_alone(self) -> None:
        listed: list[bool] = []
        real = self.engine.docker

        def racing(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
            reply = real(arguments, timeout)
            if list(arguments[:2]) == ["container", "ls"] and not listed:
                listed.append(True)
                self.engine.hold(owner="the_other_launch")
            return reply

        with mock.patch.object(self.engine, "docker", racing), self.assertRaisesRegex(SlotBusy, "taken by another launch"):
            self.stack().job(["--branch", "b"])
        self.assertEqual(["the_other_launch"], [box.labels[OWNER_LABEL] for box in self.engine.boxes.values()])

    def test_a_job_that_passes_its_deadline_has_its_own_container_stopped_and_removed_on_a_local_run_and_over_ssh(self) -> None:
        for over_ssh in (False, True):
            engine = Engine()
            engine.on_start = lambda _engine, _box: (_ for _ in ()).throw(TimeoutExpired("docker", 5))
            self.engine = engine
            with self.subTest(over_ssh=over_ssh):
                if over_ssh:
                    code, _, err, _ = self.stack(over_ssh=True).job(["--branch", "b"])
                    self.assertEqual((255, "TimeoutExpired"), (code, err))
                else:
                    with self.assertRaises(TimeoutExpired):
                        self.stack().job(["--branch", "b"])
                self.assertEqual({}, engine.boxes)

    def test_an_interrupt_in_the_second_cleanup_over_ssh_removes_the_container_or_names_it_with_its_state(self) -> None:
        """The probe: the launch times out, the first cleanup listing fails, and one SIGINT lands in the stop of the second cleanup."""
        for removal_fails in (False, True):
            engine = Engine(on_start=lambda _engine, _box: (_ for _ in ()).throw(TimeoutExpired("docker", 5)), removal_fails=removal_fails)
            failed: list[bool] = []
            pressed: list[bool] = []

            def flaky(
                arguments: Sequence[str],
                timeout: float,
                engine: Engine = engine,
                real: Callable[[Sequence[str], float], CompletedProcess[str]] = engine.docker,
                failed: list[bool] = failed,
                pressed: list[bool] = pressed,
            ) -> CompletedProcess[str]:
                if list(arguments[:2]) == ["container", "ls"] and engine.launches() and not failed:
                    failed.append(True)
                    return CompletedProcess(list(arguments), 255, "", "HostError")
                if list(arguments[:2]) == ["container", "stop"] and not pressed:
                    pressed.append(True)
                    raise KeyboardInterrupt
                return real(arguments, timeout)

            self.engine = engine
            with self.subTest(removal_fails=removal_fails):
                with mock.patch.object(engine, "docker", flaky), contextlib.redirect_stderr(io.StringIO()) as error, self.assertRaises(KeyboardInterrupt):
                    self.stack(over_ssh=True).job(["--branch", "b"])
                self.assertEqual(([True], [True], 1), (failed, pressed, len(engine.launches())))
                self.assertEqual(1 if removal_fails else 0, len(engine.boxes))
                self.assertEqual(removal_fails, f"container {1:064x} (exited)" in error.getvalue())

    def test_a_container_that_cannot_be_removed_after_the_deadline_is_a_slot_error(self) -> None:
        self.engine.on_start = lambda _engine, _box: (_ for _ in ()).throw(TimeoutExpired("docker", 5))
        self.engine.removal_fails = True
        with self.assertRaisesRegex(SlotError, "still there"):
            self.stack().job(["--branch", "b"])

    def test_the_job_of_a_failed_run_is_not_started_twice_and_its_result_is_returned(self) -> None:
        self.engine.on_start = finishes(3, "out", "err")
        self.assertEqual((3, "out", "err"), self.stack().job(["--branch", "b"])[:3])
        self.assertEqual(1, len(self.engine.launches()))


def interrupted(engine: Engine, box: Box) -> CompletedProcess[str]:
    raise KeyboardInterrupt


@unittest.skipUnless((ROOT / "tests" / "e2e" / "spark_launch.py").is_file(), "the untracked runners are not here")
class RunnerInterruptTest(unittest.TestCase):
    """An interrupt during a runner's Spark job stops and removes the runner's own container, or names the one that survives, and starts nothing again."""

    def catalog_proof_on_the_host(self, engine: Engine) -> Callable[[], object]:
        proof: Any = importlib.import_module("tests.e2e.catalog_proof")
        stack = proof.Stack(FakeTarget(engine, over_ssh=True), types.SimpleNamespace(commands=[]), [])
        return lambda: stack.job(["--branch", "b"])

    def bronze_decode(self, engine: Engine) -> Callable[[], object]:
        module: Any = importlib.import_module("tests.e2e.bronze_decode")
        run = module.Run.__new__(module.Run)
        run.compose_dir, run.record, run.log = Path("/c"), types.SimpleNamespace(commands=[]), []
        self.enterContext(mock.patch.object(module, "run_command", engine.run_command))
        return lambda: run.job(["--branch", "b"])

    def retention(self, engine: Engine) -> Any:
        module: Any = importlib.import_module("tests.e2e.snapshot_retention")
        retention = module.Retention.__new__(module.Retention)
        retention.stack = types.SimpleNamespace(compose=["docker", "compose"], target=FakeTarget(engine), log=[])
        retention.record = types.SimpleNamespace(commands=[])
        return retention

    def retention_script(self, engine: Engine) -> Callable[[], object]:
        retention = self.retention(engine)
        return lambda: retention.python_job("print()")

    def retention_maintenance(self, engine: Engine) -> Callable[[], object]:
        retention = self.retention(engine)
        return lambda: retention.maintenance("--dry-run")

    def joined_stack(self, engine: Engine) -> Callable[[], object]:
        module: Any = importlib.import_module("tests.e2e.stack_joined")
        run = module.Run.__new__(module.Run)
        run.docker_command, run.project, run.environment = ["docker", "compose"], "p", {}
        run.record, run.log, run.ports = types.SimpleNamespace(commands=[], expect=mock.Mock()), [], {"nessie": 1}
        self.enterContext(mock.patch.object(module, "run_command", engine.run_command))
        nessie = mock.Mock()
        nessie.return_value.reference.return_value.hash = "h"
        self.enterContext(mock.patch.object(module, "NessieClient", nessie))
        return lambda: module.spark_proof(run)

    def labels_stack(self, engine: Engine) -> Any:
        module: Any = importlib.import_module("tests.e2e.silver_labels")
        stack = module.Stack.__new__(module.Stack)
        stack.record, stack.log, stack.stalls, stack.project = types.SimpleNamespace(commands=[]), [], [], "p"
        stack.watch = lambda done, stalled: None
        stack.run = lambda args, timeout=60, stdin=None: FakeTarget(engine).run(args, timeout=timeout)
        self.enterContext(mock.patch.object(module.Stack, "compose", new_callable=mock.PropertyMock, return_value=["docker", "compose"]))
        return stack

    def labels(self, engine: Engine) -> Callable[[], object]:
        stack = self.labels_stack(engine)
        return lambda: stack.job(["-m", "spark.jobs.silver_labels", "build"])

    def runners(self) -> list[Callable[[Engine], Callable[[], object]]]:
        return [self.catalog_proof_on_the_host, self.bronze_decode, self.retention_script, self.retention_maintenance, self.joined_stack, self.labels]

    def test_an_interrupt_removes_the_runners_own_container_and_no_other_job_starts(self) -> None:
        for runner in self.runners():
            engine = Engine(on_start=interrupted)
            start = runner(engine)
            with self.subTest(runner=runner.__name__):
                with self.assertRaises(KeyboardInterrupt):
                    start()
                self.assertEqual({}, engine.boxes)
                self.assertEqual(1, len(engine.launches()))

    def test_an_interrupt_whose_removal_fails_names_the_surviving_container_and_its_state(self) -> None:
        for runner in self.runners():
            engine = Engine(on_start=interrupted, removal_fails=True)
            start = runner(engine)
            with self.subTest(runner=runner.__name__):
                with contextlib.redirect_stderr(io.StringIO()) as error, self.assertRaises(KeyboardInterrupt):
                    start()
                self.assertIn(f"container {1:064x} (exited)", error.getvalue())
                self.assertEqual(1, len(engine.launches()))

    def test_a_finished_job_of_each_runner_leaves_no_container(self) -> None:
        for runner in self.runners():
            engine = Engine(on_start=finishes(3, "", "failed\n"))
            start = runner(engine)
            with self.subTest(runner=runner.__name__):
                with contextlib.suppress(Exception):
                    start()
                self.assertEqual(({}, 1), (engine.boxes, len(engine.launches())))

    def test_a_stalled_step_whose_container_survives_is_not_undone_or_tried_again(self) -> None:
        """The launch returns after a stall, and the removal fails: the step is not undone while its own container may still run, and nothing starts again."""
        for name in ("silver_labels", "silver_cases"):
            engine = Engine(on_start=finishes(137), removal_fails=True)
            module: Any = importlib.import_module(f"tests.e2e.{name}")
            stack = module.Stack.__new__(module.Stack)
            stack.record, stack.log, stack.stalls, stack.project = types.SimpleNamespace(commands=[]), [], [], "p"
            stack.watch = lambda done, stalled: stalled.set()
            stack.run = lambda args, timeout=60, stdin=None, engine=engine: FakeTarget(engine).run(args, timeout=timeout)
            reset: list[bool] = []
            with (
                self.subTest(name=name),
                mock.patch.object(module.Stack, "compose", new_callable=mock.PropertyMock, return_value=["docker", "compose"]),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaisesRegex(SlotBusy, f"{1:064x} \\(exited\\)"),
            ):
                stack.job(["-m", f"spark.jobs.{name}", "build"], reset=lambda reset=reset: reset.append(True))
            self.assertEqual((1, []), (len(engine.launches()), reset))

    def test_a_labels_step_that_passes_its_limit_is_not_tried_again_while_its_own_container_survives(self) -> None:
        def killed(engine: Engine, box: Box) -> CompletedProcess[str]:
            raise TimeoutExpired("docker", 5)

        engine = Engine(on_start=killed, removal_fails=True)
        reset: list[bool] = []
        stack = self.labels_stack(engine)
        with self.assertRaisesRegex(SlotError, f"container {1:064x} \\(exited\\)"):
            stack.job(["-m", "spark.jobs.silver_labels", "build"], reset=lambda: reset.append(True))
        self.assertEqual((1, []), (len(engine.launches()), reset))

    def test_a_labels_step_that_passes_its_limit_is_tried_again_only_after_its_container_is_gone(self) -> None:
        held: list[int] = []

        def killed(engine: Engine, box: Box) -> CompletedProcess[str]:
            held.append(len(engine.named()))
            raise TimeoutExpired("docker", 5)

        engine = Engine(on_start=killed)
        stack = self.labels_stack(engine)
        with self.assertRaisesRegex(RuntimeError, "stalled"):
            stack.job(["-m", "spark.jobs.silver_labels", "build"])
        self.assertEqual(({}, {1}), (engine.boxes, set(held)))
        self.assertEqual(len(held), len(engine.launches()))


@unittest.skipUnless(RUNNERS, "the untracked runners are not here")
class RunnerSlotScanTest(unittest.TestCase):
    """A runner that starts a one-off of a Spark service takes the slot and goes through the one launch and cleanup path."""

    def test_every_runner_starts_its_spark_one_offs_through_the_shared_path(self) -> None:
        found = {path.relative_to(ROOT).as_posix(): spark_launch_problems(path.read_text()) for path in RUNNERS if path.name != "spark_launch.py"}
        self.assertEqual({}, {name: hits for name, hits in found.items() if hits})

    def test_the_scan_names_each_planted_way_around_the_shared_path(self) -> None:
        good = (
            "def job(self):\n"
            '    command = ["--profile", profile_of_job(job), "run", "--no-deps", *slot_words(), service_of(job)]\n'
            "    return launch_in_slot(self.run, command, 9)\n"
        )
        self.assertEqual([], spark_launch_problems(good))
        planted = {
            "no slot: line 2": good.replace("*slot_words(), ", ""),
            "--rm: line 2": good.replace('"run", ', '"run", "--rm", '),
            "outside the shared path: line 2": good.replace("launch_in_slot(self.run, command, 9)", "self.run(command, 9)"),
            "a removal by hand: line 3": good.replace("    return", '    self.docker("rm", "-f", *self.spark_containers())\n    return').replace(
                "    command", "    command", 1
            ),
        }
        for expected, text in planted.items():
            with self.subTest(expected=expected):
                self.assertEqual([expected], spark_launch_problems(text))


@unittest.skipUnless(RUNNERS, "the untracked runners are not here")
class RunnerListingScanTest(unittest.TestCase):
    """A runner lists its Spark containers through the helper. A list of several values for one label filter key is ANDed by Docker, so it lists nothing."""

    def test_no_runner_uses_the_filters_that_listed_nothing(self) -> None:
        found = {path.relative_to(ROOT).as_posix(): repeated_filters(path.read_text()) for path in RUNNERS}
        self.assertEqual({}, {name: hits for name, hits in found.items() if hits})

    def test_the_scan_names_a_planted_repeat_and_a_planted_use_of_the_old_list(self) -> None:
        self.assertEqual(["SPARK_FILTERS: line 1"], repeated_filters("*SPARK_FILTERS,"))
        planted = '"--filter", "label=com.docker.compose.service=spark_silver", "--filter", "label=com.docker.compose.service=spark_gold",'
        self.assertEqual(["the service label twice in one request: line 1"], repeated_filters(planted))
        self.assertEqual([], repeated_filters('"--filter", "label=com.docker.compose.project=p", "--filter", f"label=com.docker.compose.service={service}",'))


if __name__ == "__main__":
    unittest.main()
