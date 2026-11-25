"""The end-to-end runners that start a Spark job name its service, profile, credential folder and memory limit through ``scripts/stack/job_workloads.py``.

The runners are not tracked (``tests/e2e/*`` is ignored but for four files), so each check skips when the runners are absent. The one ``spark`` service, the
``batch`` profile, ``SPARK_ACCESS_DIR`` and ``MEM_LIMIT_SPARK`` are gone: a runner that still names one would start no job, or start it under no identity. The
scan reads every runner for those names, and the launch helper of the runners (``tests/e2e/spark_launch.py``) is held to what the services and the launcher
do. The planted cases put each stale name into a clean text and expect the scan to name it.
"""

from __future__ import annotations

import importlib
import re
import types
import unittest
from collections.abc import Sequence
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, SPARK_SERVICES
from scripts.stack.spark_slot import OWNER_LABEL, SLOT_NAME, SlotBusy, SlotError
from tests.fast.spark_engine import Engine, finishes

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


def spark_launches_without_the_slot(text: str) -> list[str]:
    """Each line of a runner that starts a one-off of a Spark service (``"run", "--rm"`` with a Spark profile of the job helper) in a file that never names the
    slot's options (``slot_words`` or ``launch_options``), as ``line N``."""
    names_the_slot = "slot_words(" in text or "launch_options(" in text or "launch_in_slot(" in text
    starts = [
        number for number, line in enumerate(text.splitlines(), start=1) if '"run", "--rm"' in line and ("profile_of_job" in line or "profile_for" in line)
    ]
    starts += [number for number, line in enumerate(text.splitlines(), start=1) if '"run",' in line and "--no-deps" in line and "profile_of_job" in line]
    return [] if names_the_slot else [f"line {number}" for number in sorted(set(starts))]


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

    def test_a_container_that_cannot_be_removed_after_the_deadline_is_a_slot_error(self) -> None:
        self.engine.on_start = lambda _engine, _box: (_ for _ in ()).throw(TimeoutExpired("docker", 5))
        self.engine.removal_fails = True
        with self.assertRaisesRegex(SlotError, "still there"):
            self.stack().job(["--branch", "b"])

    def test_the_job_of_a_failed_run_is_not_started_twice_and_its_result_is_returned(self) -> None:
        self.engine.on_start = finishes(3, "out", "err")
        self.assertEqual((3, "out", "err"), self.stack().job(["--branch", "b"])[:3])
        self.assertEqual(1, len(self.engine.launches()))


@unittest.skipUnless(RUNNERS, "the untracked runners are not here")
class RunnerSlotScanTest(unittest.TestCase):
    """A runner that starts a one-off of a Spark service names the slot's options, so Docker refuses a second Spark container on the engine."""

    def test_every_runner_that_starts_a_spark_one_off_names_the_slot(self) -> None:
        found = {path.relative_to(ROOT).as_posix(): spark_launches_without_the_slot(path.read_text()) for path in RUNNERS if path.name != "spark_launch.py"}
        self.assertEqual({}, {name: hits for name, hits in found.items() if hits})

    def test_the_scan_names_a_planted_launch_without_the_slot_and_accepts_each_way_to_take_it(self) -> None:
        planted = 'command = [*self.compose, "--profile", profile_of_job(job), "run", "--rm", "--no-TTY", "--no-deps", service_of(job)]'
        self.assertEqual(["line 1"], spark_launches_without_the_slot(planted))
        for taken in ("*slot_words(), ", "*launch_options(arguments, JOB), ", "launch_in_slot(run, build, 9), "):
            with self.subTest(taken=taken):
                self.assertEqual([], spark_launches_without_the_slot(planted.replace("service_of(job)", taken + "service_of(job)")))


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
