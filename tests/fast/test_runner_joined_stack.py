"""The local joined-stack runner follows the deploy's order with task files that exist, never builds an image, and checks the admission check and the group
command as the code behaves now.

The runner is not tracked, so each check skips when it is absent. The admission step runs the project's own admission and group commands: both answer from
their arguments and refuse a Spark group before any Docker call, so no Docker, server or stand-in program is used.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import shutil
import tempfile
import threading
import time
import types
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, TimeoutExpired
from scripts.stack.groups import LICENSED_SERVICES
from scripts.stack.limits import load_limits
from tests.fast.runner_import import ABSENT, E2E, import_runner, present

ROOT = Path(__file__).resolve().parents[2]
STEP = re.compile(r"name: (?P<role>\w+)\n        tasks_from: (?P<file>\w+\.yml)\n(?:      vars:\n(?P<vars>(?:        .*\n)+))?")


@unittest.skipUnless(present("stack_joined"), ABSENT)
class JoinedStackSteps(unittest.TestCase):
    def setUp(self) -> None:
        self.runner: Any = import_runner("stack_joined")

    def steps(self, name: str) -> list[tuple[str, str, str]]:
        text = "".join(getattr(self.runner, name)("10.0.0.1"))
        return [(found["role"], found["file"], found["vars"] or "") for found in STEP.finditer(text)]

    def test_every_role_step_names_a_task_file_that_exists(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            for role, file, _ in self.steps(name):
                with self.subTest(order=name, step=f"{role}/{file}"):
                    self.assertTrue((ROOT / "ansible" / "roles" / role / "tasks" / file).is_file())

    def test_the_grafana_password_is_made_after_openbao_started_alone_and_got_its_bootstrap(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            with self.subTest(order=name):
                found = self.steps(name)
                alone = next(index for index, step in enumerate(found) if step[1] == "start.yml" and '["openbao"]' in step[2])
                files = [(role, file) for role, file, _ in found]
                self.assertLess(files.index(("openbao", "prepare.yml")), alone)
                self.assertLess(alone, files.index(("openbao", "bootstrap.yml")))
                self.assertLess(files.index(("openbao", "bootstrap.yml")), files.index(("compose_stack", "grafana_password.yml")))

    def test_every_start_of_the_rest_leaves_out_the_storage_services_and_the_always_on_group_does_not_wait_for_them(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            rest = [step for step in self.steps(name) if step[1] == "start.yml" and "compose_stack_start_services: []" in step[2]]
            with self.subTest(order=name):
                self.assertEqual(1, len(rest))
                self.assertIn('compose_stack_start_except: "{{ compose_stack_storage_services }}"', rest[0][2])
        for service in LICENSED_SERVICES:
            self.assertNotIn(service, self.runner.ALWAYS_ON)
            self.assertNotIn(service, self.runner.START_SERVICES)

    def test_the_run_stops_before_any_step_when_a_pinned_image_is_not_on_this_computer(self) -> None:
        asked: list[tuple[str, ...]] = []

        def absent(*arguments: str) -> tuple[int, str]:
            asked.append(arguments)
            return 1, "no such image"

        references = {"IMAGE_DAGSTER": "example/x@sha256:" + "a" * 64}
        with mock.patch.object(self.runner, "committed_references", return_value=references):
            with self.assertRaisesRegex(self.runner.Blocked, "IMAGE_DAGSTER.*not on this computer"):
                self.runner.require_pinned_images(absent)
            self.assertIsNone(self.runner.require_pinned_images(lambda *arguments: (0, "[]")))
        self.assertEqual([("image", "inspect", references["IMAGE_DAGSTER"])], asked)


class Listings:
    """What the two Docker listings of the runner answer: the running services of the project, and the containers of one service (none)."""

    def __init__(self, running: list[str], *, services: tuple[int, str] | None = None, containers: tuple[int, str] = (0, "")) -> None:
        self.services = services if services is not None else (0, "".join(f"{name}\n" for name in running))
        self.containers = containers
        self.asked: list[tuple[str, ...]] = []

    def __call__(self, *arguments: str) -> tuple[int, str]:
        self.asked.append(arguments)
        return self.containers if "-aq" in arguments else self.services


FAILED_ANSWERS = ((1, "Cannot connect to the Docker daemon"), (1, ""))


@unittest.skipUnless(present("stack_joined"), ABSENT)
class JoinedStackAdmission(unittest.TestCase):
    def setUp(self) -> None:
        self.runner: Any = import_runner("stack_joined")
        limits = load_limits()
        self.running = sorted(name for name, entry in limits.services.items() if entry.group == "supporting" and name not in LICENSED_SERVICES)
        self.observed: list[tuple[str, bool, str]] = []

    def run_with(self, listings: Listings) -> Any:
        """The runner's own ``Run`` with its Docker command replaced, so its lookups are the ones that the host's check uses."""

        def expect(case: str, kind: str, expected: str, seen: str, passed: bool) -> bool:
            self.observed.append((case, passed, seen))
            return passed

        run = self.runner.Run.__new__(self.runner.Run)
        run.record = types.SimpleNamespace(expect=expect)
        run.docker = listings
        run.compose, run.project, run.environment = ROOT / "compose", "p2_join3_test", {}
        return run

    def passed(self) -> list[str]:
        return [case for case, passed, _ in self.observed if passed]

    def test_the_boundary_and_the_spark_group_refusal_hold_against_the_real_commands_with_aistor_skipped(self) -> None:
        self.runner.admission_cases(self.run_with(Listings(self.running)))
        self.assertEqual(4, len(self.observed), self.observed)
        self.assertEqual([], [(case, seen) for case, passed, seen in self.observed if not passed])

    def test_a_listing_of_the_running_services_that_fails_stops_the_cases_before_the_boundary(self) -> None:
        for answer in FAILED_ANSWERS:
            with self.subTest(answer=answer), mock.patch.object(self.runner, "run_command") as command:
                self.observed.clear()
                self.runner.admission_cases(self.run_with(Listings(self.running, services=answer, containers=answer)))
                self.assertEqual([False], [passed for _, passed, _ in self.observed], self.observed)
                self.assertIn("listing exit 1", self.observed[0][2])
                command.assert_not_called()

    def test_running_services_other_than_the_always_on_ones_stop_the_cases_before_the_boundary(self) -> None:
        for words, running, named in (
            ("one missing", self.running[1:], f"missing: ['{self.running[0]}']"),
            ("one more", [*self.running, "spark_bronze"], "unexpected: ['spark_bronze']"),
            ("none", [], f"missing: {self.running}"),
        ):
            with self.subTest(words), mock.patch.object(self.runner, "run_command") as command:
                self.observed.clear()
                self.runner.admission_cases(self.run_with(Listings(running)))
                self.assertEqual([False], [passed for _, passed, _ in self.observed], self.observed)
                self.assertIn(named, self.observed[0][2])
                command.assert_not_called()

    def test_a_container_lookup_that_fails_is_an_error_and_never_proof_that_no_container_exists(self) -> None:
        self.assertTrue(issubclass(self.runner.ListingFailed, RuntimeError), "the run's own handler records it as a failed run")
        for answer in FAILED_ANSWERS:
            with self.subTest(answer=answer):
                self.observed.clear()
                run = self.run_with(Listings(self.running, containers=answer))
                with self.assertRaisesRegex(self.runner.ListingFailed, "exit 1"):
                    run.container("spark_bronze")
                with self.assertRaisesRegex(self.runner.ListingFailed, "exit 1"):
                    self.runner.admission_cases(run)
                self.assertNotIn("group command on the Spark group (planted)", self.passed())

    def test_the_removal_of_the_project_is_proven_only_by_listings_that_succeed_and_are_empty(self) -> None:
        for words, answer, removed in (("empty", (0, ""), True), *((f"failed {text!r}", (code, text), False) for code, text in FAILED_ANSWERS)):
            with self.subTest(words):
                asked: list[tuple[str, ...]] = []

                def docker(*arguments: str, answer: tuple[int, str] = answer, asked: list[tuple[str, ...]] = asked) -> tuple[int, str]:
                    asked.append(arguments)
                    return answer

                run = self.run_with(Listings(self.running))
                run.docker = docker
                self.assertIs(removed, run.remove_project())
                self.assertEqual([], [words for words in asked if "rm" in words], "the words of a failed listing are no names to remove")


class ProjectDocker:
    """The Docker commands of the removal of a project that still holds a container. ``late`` names the calls that pass their deadline, each as the first two
    words of the command and the number of that call among the commands with those words."""

    def __init__(self, *late: tuple[str, int], failure: type[BaseException] | None = None) -> None:
        self.late, self.failure = set(late), failure
        self.asked: list[str] = []

    def __call__(self, *arguments: str) -> tuple[int, str]:
        words = " ".join(arguments[:2])
        self.asked.append(words)
        if (words, self.asked.count(words)) in self.late:
            if self.failure is not None:
                raise self.failure("planted")
            raise TimeoutExpired("docker", 120)
        listed = {"ps -aq": "a-container\n", "volume ls": "a-volume\n", "network ls": "a-network\n"}
        return 0, listed.get(words, "") if self.asked.count(words) == 1 or words == "ps -aq" else ""


def nothing_left(*arguments: str) -> tuple[int, str]:
    """Docker's answers for a project that holds nothing, so its removal succeeds."""
    return 0, ""


class Stats:
    """The ``docker stats`` command of the memory sampler. It answers the planted reads in their order, an exception being raised, and then complete samples.
    ``answered`` is set at the first read after the planted ones, when the sampler has finished each of them."""

    def __init__(self, *reads: Any) -> None:
        self.reads = list(reads)
        self.answered = threading.Event()

    @staticmethod
    def completed(code: int, out: str, error: str = "") -> CompletedProcess[str]:
        return CompletedProcess(["docker", "stats"], code, out, error)

    @classmethod
    def sample(cls) -> CompletedProcess[str]:
        return cls.completed(0, "p2_join3_test-kafka-1\t512MiB / 1GiB\n")

    def __call__(self, program: str, arguments: list[str], **_: object) -> CompletedProcess[str]:
        if (program, arguments[0]) != ("docker", "stats"):
            raise AssertionError("no command of the computer runs in this test")
        if not self.reads:
            self.answered.set()
            return self.sample()
        planted = self.reads.pop(0)
        if isinstance(planted, BaseException):
            raise planted
        answer: CompletedProcess[str] = planted
        return answer


@unittest.skipUnless(present("stack_joined"), ABSENT)
class JoinedStackCleanup(unittest.TestCase):
    """The runner's own ``main`` with the images, the landed copy and Docker replaced. Whatever stops a step or the project's removal, the scratch folder is
    removed and the run's record is saved with the failure."""

    def setUp(self) -> None:
        self.runner: Any = import_runner("stack_joined")
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name).resolve()
        (self.root / "manifest_reference.json").write_text("{}")
        self.scratch: list[Path] = []
        self.peaks_evaluated: list[Any] = []

    def sampler(self, *, ends: bool) -> threading.Thread:
        """A thread in the place of the memory sampler. With ``ends`` False it stays alive, as a sampler does whose command outlasts the runner's wait."""
        release = threading.Event()
        thread = threading.Thread(target=release.wait, args=(30,), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(release.set)
        if ends:
            release.set()
        return thread

    def main(
        self,
        docker: Callable[..., tuple[int, str]],
        step: BaseException | None = None,
        sampler: threading.Thread | None = None,
        stats: Stats | None = None,
    ) -> int:
        """With ``stats`` the runner's own sampler runs, its ``docker stats`` command answered by it, and the first deploy ends after its planted reads."""
        real_run = self.runner.Run

        def make_run(record: Any, log: list[str], scratch: Path, *others: object) -> Any:
            self.scratch.append(scratch)
            self.addCleanup(shutil.rmtree, scratch, True)
            run = real_run.__new__(real_run)
            run.record, run.log, run.scratch, run.project, run.docker = record, log, scratch, "p2_join3_test", docker
            run.sampling, run.sampler = self.runner.threading.Event(), sampler
            run.place_k3s_stand_ins = lambda: None
            if stats is None:
                run.start_sampling = lambda: None
            run.peaks, run.failed_reads, run.grafana_mib = {}, [], None
            return run

        def first_deploy(run: Any) -> bool:
            if step is not None:
                raise step
            if stats is not None:
                deadline = time.monotonic() + 5
                while not stats.answered.wait(0.01) and run.sampler.is_alive() and time.monotonic() < deadline:
                    pass
                self.assertTrue(stats.answered.is_set() or not run.sampler.is_alive(), "the sampler made its planted reads or ended")
            return False

        replaced: dict[str, Any] = {
            "Run": make_run,
            "first_deploy": first_deploy,
            "peak_memory": self.peaks_evaluated.append,
            "COMMAND_SECONDS": 0.2 if sampler is not None else self.runner.COMMAND_SECONDS,
            "require_pinned_images": lambda docker: None,
            "recorded_state": lambda root: ("0" * 40, "0" * 64),
            "fill_landed_cache": lambda folder, cases: {},
            "build_landed_copy": lambda paths, folder: (None, self.root / "manifest_reference.json"),
            "run_command": stats or mock.Mock(side_effect=AssertionError("no command of the computer runs in this test")),
            "SAMPLE_SECONDS": 0.01 if stats is not None else self.runner.SAMPLE_SECONDS,
        }
        with (
            mock.patch.multiple(self.runner, **replaced),
            mock.patch("tests.e2e.run_record.ROOT", self.root),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            return int(self.runner.main([]))

    def saved(self) -> dict[str, Any]:
        records = list((self.root / "runs").glob("*/record.json"))
        self.assertEqual(1, len(records), "the run's record is saved once")
        self.assertEqual([False], [scratch.exists() for scratch in self.scratch], "the scratch folder is removed")
        document: dict[str, Any] = json.loads(records[0].read_text())
        self.assertNotEqual("pending", document["cleanup"])
        return document

    @staticmethod
    def failed(document: dict[str, Any]) -> str:
        return "\n".join(f"{item['case']}: {item['observed']}" for item in document["observations"] if not item["passed"])

    def test_a_sampler_that_misses_its_deadline_fails_the_run_before_the_memory_peaks_are_evaluated(self) -> None:
        self.assertEqual(1, self.main(ProjectDocker(), sampler=self.sampler(ends=False)))
        document = self.saved()
        self.assertEqual("fail", document["outcome"])
        self.assertEqual([], self.peaks_evaluated, "peaks that the sampler can still change are not evaluated")
        self.assertIn("memory sampling ended before the peaks: the sampler is still running", self.failed(document))
        self.assertIn("sampling stopped: stopped: False", self.failed(document))
        self.assertRegex(document["cleanup"], r"^cleanup failed \(sampling stopped\b")

    def test_a_sampler_that_ends_in_time_lets_the_memory_peaks_be_evaluated_and_is_a_clean_stop(self) -> None:
        self.main(ProjectDocker(), sampler=self.sampler(ends=True))
        document = self.saved()
        self.assertEqual(1, len(self.peaks_evaluated))
        self.assertNotIn("sampl", self.failed(document))
        self.assertIn("sampling stopped: True", document["cleanup"])

    def test_a_stats_read_that_fails_after_a_complete_sample_fails_the_run_before_the_memory_peaks_are_evaluated(self) -> None:
        for words, read, named in (
            ("a deadline", TimeoutExpired("docker", 120), "TimeoutExpired"),
            ("a command that cannot start", OSError("planted"), "OSError: planted"),
            ("an exit that is not zero", Stats.completed(1, "", "Cannot connect to the Docker daemon"), "exit 1: Cannot connect to the Docker daemon"),
            ("a figure that is no number", Stats.completed(0, "p2_join3_test-kafka-1\t1.2.3MiB / 1GiB\n"), "ValueError"),
        ):
            with self.subTest(words):
                self.root.joinpath("runs").exists() and shutil.rmtree(self.root / "runs")
                self.scratch.clear()
                self.peaks_evaluated.clear()
                stats = Stats(Stats.sample(), read, read, Stats.sample())
                self.assertEqual(1, self.main(nothing_left, stats=stats))
                document = self.saved()
                self.assertEqual("fail", document["outcome"])
                self.assertEqual([], self.peaks_evaluated, "peaks of a sampling with a failed read are not evaluated")
                failed = self.failed(document)
                self.assertIn("memory samples read without a failure: 2 failed reads", failed)
                self.assertEqual(2, failed.count(named), "every failed read is recorded")
                self.assertIn("the memory peaks are not evaluated", failed)
                self.assertEqual([], stats.reads, "the sampler reads on after a failed read")

    def test_sampling_with_no_failed_read_lets_the_memory_peaks_be_evaluated_and_the_run_passes(self) -> None:
        self.assertEqual(0, self.main(nothing_left, stats=Stats(Stats.sample(), Stats.sample())))
        document = self.saved()
        self.assertEqual("", self.failed(document))
        self.assertEqual([{"kafka": 512.0}], [run.peaks for run in self.peaks_evaluated])
        self.assertIn("memory samples read without a failure", [item["case"] for item in document["observations"]])

    def test_a_listing_that_times_out_while_a_container_remains_is_a_cleanup_failure_and_the_record_is_saved(self) -> None:
        docker = ProjectDocker(("volume ls", 2))
        self.assertEqual(1, self.main(docker))
        document = self.saved()
        self.assertEqual("fail", document["outcome"])
        self.assertTrue(document["cleanup"].startswith("cleanup failed"), document["cleanup"])
        self.assertIn("timed out: volume ls", self.failed(document))
        self.assertEqual(2, docker.asked.count("network ls"), "the listing after the one that timed out is still asked")

    def test_a_command_of_the_removal_that_times_out_leaves_the_other_kinds_to_be_removed(self) -> None:
        for words in ("ps -aq", "rm -f", "volume ls", "volume rm"):
            with self.subTest(words=words):
                docker = ProjectDocker((words, 1))
                self.root.joinpath("runs").exists() and shutil.rmtree(self.root / "runs")
                self.scratch.clear()
                self.assertEqual(1, self.main(docker))
                self.assertIn(f"timed out: {words}", self.failed(self.saved()))
                self.assertIn("network rm", docker.asked)

    def test_a_removal_that_raises_any_other_error_still_removes_the_scratch_folder_and_saves_the_record(self) -> None:
        self.assertEqual(1, self.main(ProjectDocker(("ps -aq", 1), failure=ValueError)))
        self.assertIn("ValueError stopped the step", self.failed(self.saved()))

    def test_a_step_whose_command_times_out_is_a_failed_run_with_a_saved_record(self) -> None:
        self.assertEqual(1, self.main(ProjectDocker(), TimeoutExpired("ansible-playbook", 3600)))
        document = self.saved()
        self.assertEqual("fail", document["outcome"])
        self.assertIn("run: stopped by TimeoutExpired", self.failed(document))

    def test_a_step_that_raises_an_unexpected_error_or_an_interrupt_still_saves_the_record_and_the_error_goes_on(self) -> None:
        for error, outcome in ((KeyError("planted"), "fail"), (KeyboardInterrupt(), "blocked: interrupted")):
            with self.subTest(error=type(error).__name__):
                self.root.joinpath("runs").exists() and shutil.rmtree(self.root / "runs")
                self.scratch.clear()
                with self.assertRaises(type(error)):
                    self.main(ProjectDocker(), error)
                document = self.saved()
                self.assertEqual(outcome, document["outcome"])
                self.assertIn(f"run: stopped by {type(error).__name__}", self.failed(document))

    def test_an_interrupt_during_the_removal_still_removes_the_scratch_folder_and_saves_a_blocked_record(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self.main(ProjectDocker(("ps -aq", 1), failure=KeyboardInterrupt))
        self.assertEqual("blocked: interrupted", self.saved()["outcome"])


@unittest.skipUnless(present("stack_monitoring"), ABSENT)
class MonitoringLocalMode(unittest.TestCase):
    def test_the_local_mode_says_that_the_grafana_step_needs_a_configured_openbao_before_it_runs_it(self) -> None:
        text = (E2E / "stack_monitoring.py").read_text()
        local = text.split("def run_local(", 1)[1].split("\ndef ", 1)[0]
        self.assertLess(local.index("grafana_needs_openbao("), local.index("grafana_password_file("))
        runner: Any = import_runner("stack_monitoring")
        seen: list[tuple[str, bool, str]] = []

        def expect(case: str, kind: str, expected: str, observed: str, passed: bool) -> bool:
            seen.append((case, passed, observed))
            return passed

        record = types.SimpleNamespace(expect=expect)
        self.assertTrue(runner.grafana_needs_openbao(record, Path("/no/such/secrets")))
        self.assertEqual([False], [passed for _, passed, _ in seen])
        self.assertIn("starts no OpenBao", seen[0][2])


if __name__ == "__main__":
    unittest.main()
