"""The parallel mode of the commit-guard run: one record that lists every observation of every worker.

``--parallel N`` runs the shards ``1/N`` to ``N/N`` of ``tests/e2e/commit_guards.py`` at the same time, each in its own scratch repository,
and writes one record.
Failure modes covered here (the fixture and the real commits are replaced, because a real run takes minutes):

- The workers share one scratch repository or run one after the other, so the run is not faster than the serial run.
- A case is run twice or never, or a worker's failure is lost, and the record still passes.
- The merged observations differ from the serial run's, or their order depends on which worker ended first.
- An interrupt leaves a worker running in a scratch repository that is removed, or leaves a scratch repository, or writes no record.
- The worker count ignores the free cores or the free memory, or the plan is not in the record.
- ``--parallel`` is zero, negative, not a number or combined with ``--shard``.
- The comparison of two records passes records of different code, different cases or different results, a partial or interrupted record, or two
  records made from different copied files.
- A second interrupt during the worker shutdown, the merge or the cleanup leaves a repository, skips the merge or removes a repository under a live worker.
- A worker that does not end hangs the run, or its repository is removed while it runs.
- A fault attempt that raises, or a stop after the control, loses the control or starts the fault; an exit with no coverage observation passes with a case
  missing.
- An unreadable memory admits many workers, and two runs on one computer each admit all of them.
"""

from __future__ import annotations

import copy
import fcntl
import io
import json
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from unittest import mock

from scripts.quality import worktree_scan_hook
from tests.e2e import commit_guards, run_record
from tests.e2e.run_record import Observation, Record

CASES = commit_guards.planted_cases()
MEMINFO = "MemTotal:       16000000 kB\nMemFree:          100000 kB\nMemAvailable:    8192000 kB\nBuffers:           10000 kB\n"
VM_STAT = (
    "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
    "Pages free:                               1000.\n"
    "Pages active:                             5000.\n"
    "Pages inactive:                           2000.\n"
    "Pages speculative:                         500.\n"
    "Pages wired down:                         3000.\n"
)
TIMEOUT_SECONDS = 20
HELD_PROBES: list[Callable[[], bool]] = []


class StubFixture:
    """A fixture that makes no repository: it records that it was filled and removed."""

    instances: list[StubFixture] = []

    def __init__(self, record: Record) -> None:
        self.record = record
        self.filled = False
        self.removed = False
        self.workers_alive_at_removal: list[str] = []
        self.lock_held_at_removal: bool | None = None
        StubFixture.instances.append(self)

    def populate(self, paths: list[str]) -> None:
        self.filled = True

    def remove(self) -> None:
        self.workers_alive_at_removal = [thread.name for thread in threading.enumerate() if thread.name.startswith("guard_shard")]
        self.lock_held_at_removal = HELD_PROBES[0]() if HELD_PROBES else None
        self.removed = True


def case_observations(case: commit_guards.Case, passed: bool = True) -> list[Observation]:
    return [Observation(case.name, step, f"{step} expected", f"{step} observed", passed) for step in ("control", "fault")]


def stub_run_cases(
    fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None, *, skip: str = ""
) -> None:
    """Stands for ``run_cases``: the baseline and the two observations of each case of the shard, from the real case list."""
    index, count = shard
    record.observations.append(Observation("baseline", "control", "exit 0", "exit 0", True))
    for number, case in enumerate(CASES):
        if number % count == index - 1 and case.name != skip:
            record.observations.extend(case_observations(case))
    log.append(f"shard {index} of {count}")


class ParallelRunCase(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="commit_guard_parallel_")
        self.addCleanup(scratch.cleanup)
        self.runs = Path(scratch.name) / "root"
        self.temp = Path(scratch.name) / "temp"
        self.temp.mkdir()
        StubFixture.instances = []
        HELD_PROBES[:] = [self.lock_is_held]
        self.addCleanup(HELD_PROBES.clear)
        self.memory_reads: list[bool] = []
        self.output = io.StringIO()
        self.cores = 16
        self.available: int | None = 64 * commit_guards.WORKER_MIB
        self.lock = Path(scratch.name) / "guards.lock"
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.runs),
            mock.patch.object(tempfile, "tempdir", str(self.temp)),
            mock.patch.object(sys, "stdout", self.output),
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "Fixture", StubFixture),
            mock.patch.object(commit_guards, "source_files", return_value=["a.txt"]),
            mock.patch.object(commit_guards, "tree_digest", return_value="digest"),
            mock.patch.object(commit_guards, "MACHINE_LOCK", self.lock),
            mock.patch.object(os, "cpu_count", side_effect=lambda: self.cores),
            mock.patch.object(commit_guards, "available_memory_mib", side_effect=self.read_memory),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def read_memory(self) -> int | None:
        self.memory_reads.append(self.lock_is_held())
        return self.available

    def lock_is_held(self) -> bool:
        """Whether another open file of this process cannot take the machine lock now, so the run holds it."""
        descriptor = os.open(self.lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        finally:
            os.close(descriptor)
        return False

    def hold_lock(self) -> None:
        descriptor = os.open(self.lock, os.O_RDWR | os.O_CREAT, 0o600)
        self.addCleanup(os.close, descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def run_main(self, *argv: str, cases: Any = stub_run_cases) -> int:
        with mock.patch.object(commit_guards, "run_cases", side_effect=cases):
            return commit_guards.main(list(argv))

    def records(self) -> list[dict[str, Any]]:
        loaded = [json.loads(path.read_text()) for path in sorted((self.runs / "runs").glob("*/record.json"))]
        return sorted(loaded, key=lambda record: str(record["mode"]))

    def only_record(self) -> dict[str, Any]:
        records = self.records()
        self.assertEqual(1, len(records), "the run must write exactly one record")
        return records[0]


class ParallelRecordTest(ParallelRunCase):
    def test_every_case_is_listed_once_in_case_order_after_the_baselines(self) -> None:
        self.assertEqual(0, self.run_main("--parallel", "4"))
        record = self.only_record()
        self.assertEqual("pass", record["outcome"])
        steps = [(item["case"], item["step"]) for item in record["observations"]]
        expected = (
            [("baseline", "control")] * 4
            + [(case.name, step) for case in CASES for step in ("control", "fault")]
            + [("source tree", "snapshot"), ("shards", "coverage")]
        )
        self.assertEqual(expected, steps)

    def test_the_order_does_not_depend_on_which_worker_ends_first(self) -> None:
        late = threading.Event()

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            if shard[0] == 1:
                self.assertTrue(late.wait(TIMEOUT_SECONDS), "worker 1 must finish after worker 2")
            stub_run_cases(fixture, record, log, shard)
            if shard[0] == 2:
                late.set()

        self.assertEqual(0, self.run_main("--parallel", "2", cases=cases))
        names = [item["case"] for item in self.only_record()["observations"] if item["step"] == "fault"]
        self.assertEqual([case.name for case in CASES], names)

    def test_the_parallel_run_gives_the_serial_runs_observations_on_the_same_code(self) -> None:
        self.assertEqual(0, self.run_main())
        self.assertEqual(0, self.run_main("--parallel", "4"))
        serial, parallel = (next(record for record in self.records() if record["mode"] == mode) for mode in ("full", "parallel 4"))
        self.assertEqual([], commit_guards.compare_records(serial, parallel))
        keep = [item for item in serial["observations"] if item["case"] != "baseline"]
        self.assertEqual(keep, [item for item in parallel["observations"] if item["case"] not in ("baseline", "shards")])
        self.assertEqual(2 * len(CASES) + 1, len(keep))

    def test_the_record_holds_the_digest_of_the_files_that_were_copied(self) -> None:
        for argv in ((), ("--parallel", "3")):
            with self.subTest(argv=argv):
                self.assertEqual(0, self.run_main(*argv))
        for record in self.records():
            self.assertEqual("digest", record["environment"]["copied_files_sha256"], record["mode"])

    def test_the_workers_run_at_the_same_time(self) -> None:
        barrier = threading.Barrier(4, timeout=TIMEOUT_SECONDS)

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            barrier.wait()
            stub_run_cases(fixture, record, log, shard)

        self.assertEqual(0, self.run_main("--parallel", "4", cases=cases))
        self.assertEqual(4, len(StubFixture.instances))
        self.assertEqual(4, len({id(fixture) for fixture in StubFixture.instances}))

    def test_each_worker_has_its_own_fixture_that_is_filled_and_removed(self) -> None:
        self.assertEqual(0, self.run_main("--parallel", "3"))
        self.assertEqual([True] * 3, [fixture.filled for fixture in StubFixture.instances])
        self.assertEqual([True] * 3, [fixture.removed for fixture in StubFixture.instances])
        self.assertEqual("3 temporary repositories removed", self.only_record()["cleanup"])

    def test_the_log_holds_every_workers_log_in_shard_order(self) -> None:
        self.assertEqual(0, self.run_main("--parallel", "3"))
        folder = next((self.runs / "runs").iterdir())
        self.assertEqual(
            ["shard 1 of 3", "shard 2 of 3", "shard 3 of 3"], [line for line in (folder / "log.txt").read_text().split("\n") if line.startswith("shard")]
        )

    def test_a_case_that_no_worker_ran_fails_the_run(self) -> None:
        missing = CASES[3].name
        code = self.run_main("--parallel", "4", cases=lambda *args, **kwargs: stub_run_cases(*args, skip=missing))
        self.assertEqual(1, code)
        record = self.only_record()
        self.assertEqual("fail", record["outcome"])
        failed = [item for item in record["observations"] if not item["passed"]]
        self.assertEqual(["shards"], [item["case"] for item in failed])
        self.assertIn(missing, failed[0]["observed"])

    def test_a_case_that_two_workers_ran_fails_the_run(self) -> None:
        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            stub_run_cases(fixture, record, log, shard)
            if shard[0] == 1:
                record.observations.extend(case_observations(CASES[1]))

        self.assertEqual(1, self.run_main("--parallel", "2", cases=cases))
        failed = [item for item in self.only_record()["observations"] if not item["passed"]]
        self.assertEqual(["shards"], [item["case"] for item in failed])
        self.assertIn(CASES[1].name, failed[0]["observed"])

    def test_a_failed_observation_of_one_worker_fails_the_run_and_keeps_the_others(self) -> None:
        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            stub_run_cases(fixture, record, log, shard)
            if shard[0] == 2:
                record.observations[-1].passed = False

        self.assertEqual(1, self.run_main("--parallel", "3", cases=cases))
        record = self.only_record()
        self.assertEqual("fail", record["outcome"])
        self.assertEqual(2 * len(CASES) + 3 + 2, len(record["observations"]))
        self.assertEqual(1, sum(not item["passed"] for item in record["observations"]))

    def test_a_worker_that_raises_is_named_and_the_others_still_finish(self) -> None:
        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            if shard[0] == 2:
                raise OSError("disk full")
            stub_run_cases(fixture, record, log, shard)

        self.assertEqual(1, self.run_main("--parallel", "3", cases=cases))
        record = self.only_record()
        self.assertEqual("fail", record["outcome"])
        failed = [item for item in record["observations"] if not item["passed"] and item["case"] != "shards"]
        self.assertEqual(["shard 2 of 3"], [item["case"] for item in failed])
        self.assertIn("OSError", failed[0]["observed"])
        self.assertEqual([True] * 3, [fixture.removed for fixture in StubFixture.instances])

    def test_an_interrupt_stops_the_workers_removes_every_repository_and_writes_the_record(self) -> None:
        started = threading.Barrier(3, timeout=TIMEOUT_SECONDS)
        ended: list[int] = []

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            started.wait()
            self.assertIsNotNone(stop)
            self.assertTrue(cast(threading.Event, stop).wait(TIMEOUT_SECONDS), "the interrupt must set the stop event")
            self.assertFalse(fixture.removed, "a repository must not be removed while its worker runs")
            ended.append(shard[0])

        real_join = threading.Thread.join
        calls: list[int] = []

        def join(thread: threading.Thread, timeout: float | None = None) -> None:
            calls.append(1)
            if len(calls) == 1:
                started.wait()
                raise KeyboardInterrupt
            real_join(thread, timeout)

        with mock.patch.object(threading.Thread, "join", autospec=True, side_effect=join), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "2", cases=cases)
        self.assertEqual([1, 2], sorted(ended))
        record = self.only_record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual([True, True], [fixture.removed for fixture in StubFixture.instances])
        self.assertEqual([], [thread for thread in threading.enumerate() if thread is not threading.main_thread() and thread.name.startswith("guard_shard")])
        coverage = [item for item in record["observations"] if item["case"] == "shards"]
        self.assertEqual([False], [item["passed"] for item in coverage], "an interrupted run records its coverage, and no case was run")
        self.assertIn(CASES[0].name, coverage[0]["observed"])


class WorkerPlanTest(ParallelRunCase):
    def test_the_count_is_the_smallest_of_the_request_the_cases_the_cores_less_one_and_the_memory(self) -> None:
        plan = commit_guards.plan_workers
        memory = commit_guards.WORKER_MIB
        self.assertEqual(4, plan(4, 15, 16, 64 * memory))
        self.assertEqual(3, plan(8, 3, 16, 64 * memory))
        self.assertEqual(5, plan(8, 15, 6, 64 * memory))
        self.assertEqual(2, plan(8, 15, 16, 2 * memory + memory // 2))
        self.assertEqual(8, plan(8, 15, 16, 8 * memory))

    def test_an_unreadable_memory_gives_one_worker(self) -> None:
        for requested, cases, cores in ((4, 15, 16), (8, 3, 16), (8, 15, 6), (2, 15, 2)):
            with self.subTest(requested=requested, cases=cases, cores=cores):
                self.assertEqual(1, commit_guards.plan_workers(requested, cases, cores, None))

    def test_a_run_with_unreadable_memory_runs_one_worker_and_names_the_reason(self) -> None:
        self.available = None
        self.assertEqual(0, self.run_main("--parallel", "8"))
        record = self.only_record()
        self.assertEqual("full", record["mode"])
        self.assertEqual(1, len(StubFixture.instances))
        plan = [line for line in record["limits"] if "workers" in line]
        self.assertEqual(1, len(plan), record["limits"])
        for text in ("1 of 8 requested", "not readable"):
            self.assertIn(text, plan[0])

    def test_there_is_always_at_least_one_worker(self) -> None:
        self.assertEqual(1, commit_guards.plan_workers(4, 15, 1, 64 * commit_guards.WORKER_MIB))
        self.assertEqual(1, commit_guards.plan_workers(4, 15, 16, 0))
        self.assertEqual(1, commit_guards.plan_workers(4, 0, 16, 64 * commit_guards.WORKER_MIB))

    def test_a_reduced_count_is_in_the_record_with_its_reason(self) -> None:
        self.cores = 3
        self.available = 100_000
        self.assertEqual(0, self.run_main("--parallel", "4"))
        record = self.only_record()
        self.assertEqual("parallel 2", record["mode"])
        plan = [line for line in record["limits"] if "workers" in line]
        self.assertEqual(1, len(plan), record["limits"])
        for text in ("2 of 4 requested", "3 cores", "100000 MiB"):
            self.assertIn(text, plan[0])

    def test_a_single_worker_runs_the_serial_path(self) -> None:
        self.cores = 2
        self.assertEqual(0, self.run_main("--parallel", "4"))
        record = self.only_record()
        self.assertEqual("full", record["mode"])
        self.assertEqual(1, len(StubFixture.instances))
        self.assertEqual("temporary repository removed", record["cleanup"])
        self.assertEqual(1, sum(item["case"] == "baseline" for item in record["observations"]))
        coverage = [item for item in record["observations"] if item["case"] == "shards"]
        self.assertEqual([True], [item["passed"] for item in coverage], "a parallel request capped at one worker still records its coverage")

    def test_a_parallel_request_capped_at_one_worker_fails_when_a_case_is_missing(self) -> None:
        self.cores = 2
        missing = CASES[3].name
        self.assertEqual(1, self.run_main("--parallel", "8", cases=lambda *args, **kwargs: stub_run_cases(*args, skip=missing)))
        record = self.only_record()
        self.assertEqual("fail", record["outcome"])
        failed = [item for item in record["observations"] if not item["passed"]]
        self.assertEqual(["shards"], [item["case"] for item in failed])
        self.assertIn(missing, failed[0]["observed"])

    def test_a_capped_run_that_raises_still_records_its_coverage(self) -> None:
        self.cores = 2

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            raise OSError("disk full")

        self.assertEqual(1, self.run_main("--parallel", "8", cases=cases))
        failed = [item["case"] for item in self.only_record()["observations"] if not item["passed"]]
        self.assertEqual(["run", "shards"], sorted(failed))

    def test_a_serial_run_without_parallel_adds_no_coverage_observation(self) -> None:
        self.assertEqual(0, self.run_main())
        self.assertNotIn("shards", [item["case"] for item in self.only_record()["observations"]])

    def test_the_meminfo_reader_gives_mebibytes_of_available_memory(self) -> None:
        self.assertEqual(8192000 // 1024, commit_guards.meminfo_available_mib(MEMINFO))
        self.assertIsNone(commit_guards.meminfo_available_mib("MemTotal: 1 kB\n"))

    def test_the_vm_stat_reader_counts_free_inactive_and_speculative_pages(self) -> None:
        self.assertEqual((1000 + 2000 + 500) * 16384 // 1024 // 1024, commit_guards.vm_stat_available_mib(VM_STAT))
        self.assertIsNone(commit_guards.vm_stat_available_mib("Pages free: 5.\n"))
        without_speculative = "\n".join(line for line in VM_STAT.splitlines() if "speculative" not in line) + "\n"
        self.assertIsNone(commit_guards.vm_stat_available_mib(without_speculative))
        self.assertIsNone(commit_guards.vm_stat_available_mib(VM_STAT.replace("page size of 16384 bytes", "page size unknown")))


class EarlyExitCoverageTest(ParallelRunCase):
    """Every exit of a parallel request saves the coverage observation once: the lock refusal, a source-read failure, an interrupt before the workers, and
    a failure to make a fixture after the cap to one worker."""

    def coverage(self) -> list[dict[str, Any]]:
        return [item for item in self.only_record()["observations"] if item["case"] == "shards"]

    def assert_coverage_once_and_failing(self) -> None:
        coverage = self.coverage()
        self.assertEqual(1, len(coverage), "exactly one coverage observation")
        self.assertFalse(coverage[0]["passed"], "no case ran, so the coverage fails")
        self.assertIn(CASES[0].name, coverage[0]["observed"])

    def test_a_refused_lock_saves_the_coverage(self) -> None:
        self.hold_lock()
        self.assertEqual(1, self.run_main("--parallel", "4"))
        self.assertTrue(self.only_record()["outcome"].startswith("blocked: another guard run"))
        self.assert_coverage_once_and_failing()

    def test_a_failed_source_read_saves_the_coverage(self) -> None:
        with mock.patch.object(commit_guards, "source_files", side_effect=OSError("unreadable")):
            self.assertEqual(1, self.run_main("--parallel", "4"))
        self.assertEqual("fail", self.only_record()["outcome"])
        self.assert_coverage_once_and_failing()

    def test_an_interrupt_before_the_workers_saves_the_coverage(self) -> None:
        with mock.patch.object(commit_guards, "tool_version", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "4")
        self.assertEqual("blocked: interrupted", self.only_record()["outcome"])
        self.assert_coverage_once_and_failing()

    def test_a_failed_fixture_after_the_cap_to_one_worker_saves_the_coverage(self) -> None:
        self.cores = 2
        with mock.patch.object(commit_guards, "Fixture", side_effect=OSError("no space")):
            self.assertEqual(1, self.run_main("--parallel", "8"))
        self.assertEqual("fail", self.only_record()["outcome"])
        self.assert_coverage_once_and_failing()

    def test_a_failed_fixture_in_a_worker_saves_the_coverage_once(self) -> None:
        with mock.patch.object(commit_guards, "Fixture", side_effect=OSError("no space")):
            self.assertEqual(1, self.run_main("--parallel", "3"))
        self.assert_coverage_once_and_failing()

    def test_a_complete_run_and_a_capped_run_save_the_coverage_once_and_passing(self) -> None:
        for requested, cores in (("3", 16), ("8", 2)):
            with self.subTest(requested=requested, cores=cores):
                self.cores = cores
                shutil.rmtree(self.runs / "runs", ignore_errors=True)
                self.assertEqual(0, self.run_main("--parallel", requested))
                self.assertEqual([True], [item["passed"] for item in self.coverage()])

    def test_the_coverage_is_computed_from_the_observations_of_the_finished_run(self) -> None:
        """The coverage is read after the workers' merge, so a case that a worker did run is not reported missing."""
        self.assertEqual(0, self.run_main("--parallel", "4"))
        self.assertEqual("every case observed once", self.coverage()[0]["observed"])


class StopTest(unittest.TestCase):
    """The real ``run_cases`` starts no case after the stop event is set."""

    def run_shard(self, stop: threading.Event | None, shard: tuple[int, int]) -> list[str]:
        fixture = mock.Mock()
        fixture.run.return_value = (0, "")
        started: list[str] = []

        def run_case(fixture: object, case: commit_guards.Case, log: list[str], stop: threading.Event | None = None) -> None:
            started.append(case.name)

        with mock.patch.object(commit_guards, "run_case", side_effect=run_case), mock.patch.object(worktree_scan_hook, "install"):
            commit_guards.run_cases(fixture, Record(feature="f", slug="s", rerun="r"), [], shard, stop)
        return started

    def test_a_shard_without_a_stop_runs_its_own_cases_only(self) -> None:
        self.assertEqual([case.name for number, case in enumerate(CASES) if number % 4 == 1], self.run_shard(None, (2, 4)))
        self.assertEqual([case.name for number, case in enumerate(CASES) if number % 4 == 1], self.run_shard(threading.Event(), (2, 4)))

    def test_a_set_stop_starts_no_case(self) -> None:
        stop = threading.Event()
        stop.set()
        self.assertEqual([], self.run_shard(stop, (2, 4)))


class RunCaseTest(unittest.TestCase):
    """The real ``run_case`` with a fixture whose commits are replaced: each step is recorded as it completes and the stop is read before the fault."""

    def setUp(self) -> None:
        self.record = Record(feature="f", slug="s", rerun="r")
        self.fixture = mock.Mock()
        self.fixture.record = self.record
        self.fixture.head.return_value = "start"
        self.case = CASES[0]
        self.log: list[str] = []

    def attempts(self, *results: object) -> None:
        self.fixture.attempt.side_effect = list(results)

    def steps(self) -> list[tuple[str, bool]]:
        return [(item.step, item.passed) for item in self.record.observations]

    def test_a_case_records_its_control_and_its_fault_in_order(self) -> None:
        self.attempts((0, "control ok", True), (1, "", False))
        commit_guards.run_case(self.fixture, self.case, self.log)
        self.assertEqual(["control", "fault"], [step for step, _ in self.steps()])
        self.assertTrue(self.steps()[0][1])

    def test_a_fault_attempt_that_raises_keeps_the_completed_control(self) -> None:
        self.attempts((0, "control ok", True), OSError("disk full"))
        with self.assertRaises(OSError):
            commit_guards.run_case(self.fixture, self.case, self.log)
        self.assertEqual([("control", True)], self.steps())
        self.assertIn("== " + self.case.name + ": control", self.log[0])

    def test_a_control_attempt_that_raises_records_nothing_for_the_case(self) -> None:
        self.attempts(OSError("disk full"))
        with self.assertRaises(OSError):
            commit_guards.run_case(self.fixture, self.case, self.log)
        self.assertEqual([], self.steps())

    def test_a_stop_set_during_the_control_starts_no_fault_and_keeps_the_control(self) -> None:
        stop = threading.Event()

        def control(change: commit_guards.Change) -> tuple[int, str, bool]:
            stop.set()
            return 0, "control ok", True

        self.fixture.attempt.side_effect = control
        commit_guards.run_case(self.fixture, self.case, self.log, stop)
        self.assertEqual(1, self.fixture.attempt.call_count, "the fault commit must not start after the stop")
        self.assertEqual([("control", True)], self.steps())
        self.fixture.reset.assert_called_once_with("start")

    def test_an_unset_stop_runs_the_fault(self) -> None:
        self.attempts((0, "control ok", True), (1, "", False))
        commit_guards.run_case(self.fixture, self.case, self.log, threading.Event())
        self.assertEqual(2, self.fixture.attempt.call_count)


class MachineLockTest(ParallelRunCase):
    """One machine-wide lock covers the planning and the whole run of a parallel request, so two runs never admit workers from the same free memory."""

    def test_a_parallel_run_holds_the_lock_from_before_the_planning_to_after_the_removal(self) -> None:
        held: list[bool] = []

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            held.append(self.lock_is_held())
            stub_run_cases(fixture, record, log, shard)

        self.assertEqual(0, self.run_main("--parallel", "3", cases=cases))
        self.assertEqual([True], self.memory_reads, "the memory is read under the lock")
        self.assertEqual([True] * 3, held)
        self.assertEqual([True] * 3, [fixture.lock_held_at_removal for fixture in StubFixture.instances])
        self.assertFalse(self.lock_is_held(), "the lock is free when the run has ended")

    def test_a_second_parallel_run_is_refused_while_the_lock_is_held(self) -> None:
        self.hold_lock()
        self.assertEqual(1, self.run_main("--parallel", "4"))
        record = self.only_record()
        self.assertTrue(record["outcome"].startswith("blocked: another guard run holds the machine-wide lock"), record["outcome"])
        self.assertEqual([], StubFixture.instances, "no repository is made without the lock")
        self.assertEqual([], self.memory_reads, "the memory is not read without the lock")

    def test_a_refused_run_leaves_the_lock_with_its_holder(self) -> None:
        self.hold_lock()
        self.run_main("--parallel", "4")
        self.assertTrue(self.lock_is_held())

    def test_a_run_that_ends_in_failure_frees_the_lock_for_the_next_run(self) -> None:
        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            raise OSError("disk full")

        self.assertEqual(1, self.run_main("--parallel", "3", cases=cases))
        self.assertFalse(self.lock_is_held())
        StubFixture.instances = []
        self.assertEqual(0, self.run_main("--parallel", "3"))

    def test_a_serial_run_and_a_shard_take_no_lock(self) -> None:
        self.hold_lock()
        self.assertEqual(0, self.run_main())
        self.assertEqual(0, self.run_main("--shard", "1/2"))

    def test_the_lock_file_is_left_in_place_by_a_run(self) -> None:
        self.assertEqual(0, self.run_main("--parallel", "2"))
        self.assertTrue(self.lock.is_file())


class InterruptTest(ParallelRunCase):
    """Interrupts that arrive during the worker shutdown, the merge or the cleanup are deferred until the record is written and the repositories are removed."""

    def setUp(self) -> None:
        super().setUp()
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)

    def interrupt(self) -> None:
        os.kill(os.getpid(), signal.SIGINT)

    def no_worker_threads(self) -> list[threading.Thread]:
        return [thread for thread in threading.enumerate() if thread is not threading.main_thread() and thread.name.startswith("guard_shard")]

    def test_a_second_interrupt_during_the_shutdown_join_neither_skips_the_merge_nor_removes_a_repository_under_a_live_worker(self) -> None:
        started = threading.Barrier(3, timeout=TIMEOUT_SECONDS)
        finish = threading.Event()

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            started.wait()
            self.assertTrue(cast(threading.Event, stop).wait(TIMEOUT_SECONDS))
            self.assertTrue(finish.wait(TIMEOUT_SECONDS), "the second interrupt must not stop the join of the workers")
            stub_run_cases(fixture, record, log, shard)

        real_join = threading.Thread.join
        calls: list[int] = []

        def join(thread: threading.Thread, timeout: float | None = None) -> None:
            calls.append(1)
            if len(calls) == 1:
                started.wait()
                self.interrupt()
            elif len(calls) == 2:
                self.interrupt()
                finish.set()
            real_join(thread, timeout)

        with mock.patch.object(threading.Thread, "join", autospec=True, side_effect=join), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "2", cases=cases)
        record = self.only_record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual([[], []], [fixture.workers_alive_at_removal for fixture in StubFixture.instances])
        self.assertEqual([True, True], [fixture.removed for fixture in StubFixture.instances])
        merged = [(item["case"], item["step"]) for item in record["observations"] if item["case"] != "baseline"]
        self.assertEqual([(case.name, step) for case in CASES for step in ("control", "fault")], [item for item in merged if item[0] != "shards"])
        self.assertEqual([], self.no_worker_threads())

    def test_a_worker_that_does_not_end_keeps_its_repository_and_does_not_hang_the_run(self) -> None:
        started = threading.Barrier(3, timeout=TIMEOUT_SECONDS)
        release = threading.Event()
        watchdog = threading.Timer(TIMEOUT_SECONDS / 2, release.set)
        watchdog.start()
        self.addCleanup(watchdog.cancel)

        def cases(fixture: StubFixture, record: Record, log: list[str], shard: tuple[int, int], stop: threading.Event | None = None) -> None:
            started.wait()
            if shard[0] == 2:
                release.wait(TIMEOUT_SECONDS)
            stub_run_cases(fixture, record, log, shard)

        real_join = threading.Thread.join
        calls: list[int] = []

        def join(thread: threading.Thread, timeout: float | None = None) -> None:
            calls.append(1)
            if len(calls) == 1:
                started.wait()
                self.interrupt()
            real_join(thread, timeout)

        began = time.monotonic()
        with (
            mock.patch.object(commit_guards, "SHUTDOWN_SECONDS", 0.5),
            mock.patch.object(threading.Thread, "join", autospec=True, side_effect=join),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.run_main("--parallel", "2", cases=cases)
        self.addCleanup(release.set)
        self.assertLess(time.monotonic() - began, TIMEOUT_SECONDS / 2, "the shutdown must end at its deadline, not when the worker ends")
        record = self.only_record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual([False, True], sorted(fixture.removed for fixture in StubFixture.instances), "the live worker's repository is kept")
        failed = [item for item in record["observations"] if not item["passed"] and item["case"] not in ("shards", "cleanup")]
        self.assertEqual(["shard 2 of 2"], [item["case"] for item in failed])
        self.assertIn("still running", failed[0]["observed"])
        self.assertIn("kept", record["cleanup"])
        release.set()
        for thread in self.no_worker_threads():
            thread.join(TIMEOUT_SECONDS)

    def test_an_interrupt_while_the_repositories_are_removed_lets_every_removal_end(self) -> None:
        real_remove = StubFixture.remove

        def remove(fixture: StubFixture) -> None:
            self.interrupt()
            real_remove(fixture)

        with mock.patch.object(StubFixture, "remove", autospec=True, side_effect=remove), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "2")
        record = self.only_record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual([True, True], [fixture.removed for fixture in StubFixture.instances])
        self.assertEqual("2 temporary repositories removed", record["cleanup"])

    def test_an_interrupt_while_the_record_is_merged_is_deferred_until_the_record_is_written(self) -> None:
        real_merge = commit_guards.merge_workers

        def merge(*args: Any) -> None:
            self.interrupt()
            real_merge(*args)

        with mock.patch.object(commit_guards, "merge_workers", side_effect=merge), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "2")
        record = self.only_record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual(2 * len(CASES), sum(item["step"] in ("control", "fault") and item["case"] != "baseline" for item in record["observations"]))
        self.assertEqual([True, True], [fixture.removed for fixture in StubFixture.instances])

    def test_the_interrupt_handler_is_put_back_after_every_run(self) -> None:
        self.assertEqual(0, self.run_main("--parallel", "2"))
        self.assertIs(signal.default_int_handler, signal.getsignal(signal.SIGINT))
        with mock.patch.object(StubFixture, "remove", autospec=True, side_effect=lambda fixture: self.interrupt()), self.assertRaises(KeyboardInterrupt):
            self.run_main("--parallel", "2")
        self.assertIs(signal.default_int_handler, signal.getsignal(signal.SIGINT))

    def test_a_run_outside_the_main_thread_works_without_the_handler(self) -> None:
        results: list[int] = []
        thread = threading.Thread(target=lambda: results.append(self.run_main("--parallel", "2")), name="not_a_guard_worker")
        thread.start()
        thread.join(TIMEOUT_SECONDS)
        self.assertEqual([0], results)


class ArgumentTest(ParallelRunCase):
    """Runs on the stubs of ``ParallelRunCase``, so an argument that is wrongly accepted starts no real repository or commit."""

    def refused(self, *argv: str) -> None:
        with mock.patch.object(sys, "stderr", io.StringIO()), mock.patch.object(commit_guards, "run_cases"), self.assertRaises(SystemExit) as caught:
            commit_guards.main(list(argv))
        self.assertEqual(2, caught.exception.code)

    def test_a_worker_count_below_one_or_not_a_number_is_refused(self) -> None:
        for value in ("0", "-2", "many", "1.5", ""):
            with self.subTest(value=value):
                self.refused("--parallel", value)

    def test_parallel_and_shard_cannot_be_combined(self) -> None:
        self.refused("--parallel", "2", "--shard", "1/2")

    def test_the_case_names_are_unique_because_the_merge_orders_by_name(self) -> None:
        names = [case.name for case in CASES]
        self.assertEqual(len(names), len(set(names)))
        self.assertNotIn("baseline", names)


class CompareTest(unittest.TestCase):
    def record(self) -> dict[str, Any]:
        observations = [{"case": "baseline", "step": "control", "expected": "e", "observed": "o", "passed": True}]
        for case in CASES:
            observations += [{"case": case.name, "step": step, "expected": step, "observed": f"{step} ok", "passed": True} for step in ("control", "fault")]
        observations.append({"case": "source tree", "step": "snapshot", "expected": "e", "observed": "unchanged: True", "passed": True})
        return {
            "revision": "abc",
            "tree_sha256": "def",
            "outcome": "pass",
            "environment": {"copied_files_sha256": "files"},
            "observations": observations,
        }

    def coverage(self, passed: bool = True) -> dict[str, Any]:
        return {"case": "shards", "step": "coverage", "expected": "e", "observed": "every case observed once", "passed": passed}

    def test_records_of_the_same_code_and_observations_agree(self) -> None:
        self.assertEqual([], commit_guards.compare_records(self.record(), self.record()))

    def test_baselines_may_differ_in_number(self) -> None:
        other = self.record()
        other["observations"] = [other["observations"][0], *other["observations"]]
        self.assertEqual([], commit_guards.compare_records(self.record(), other))

    def test_another_tree_or_revision_is_not_the_same_code(self) -> None:
        for key in ("tree_sha256", "revision"):
            with self.subTest(key=key):
                other = self.record()
                other[key] = "changed"
                self.assertIn(key, " ".join(commit_guards.compare_records(self.record(), other)))

    def test_a_different_observed_text_or_result_is_named_with_its_case(self) -> None:
        for field, value in (("observed", "fault changed"), ("passed", False), ("expected", "other")):
            with self.subTest(field=field):
                other = self.record()
                other["observations"][3][field] = value
                differences = [problem for problem in commit_guards.compare_records(self.record(), other) if "differs" in problem]
                self.assertEqual(1, len(differences), differences)
                self.assertIn(other["observations"][3]["case"], differences[0])

    def test_a_missing_or_extra_observation_is_named(self) -> None:
        shorter = self.record()
        removed = shorter["observations"].pop(5)
        self.assertIn(removed["case"], " ".join(commit_guards.compare_records(self.record(), shorter)))
        self.assertIn(removed["case"], " ".join(commit_guards.compare_records(shorter, self.record())))

    def test_a_record_with_a_coverage_observation_that_passed_agrees_with_one_without(self) -> None:
        parallel = self.record()
        parallel["observations"].append(self.coverage())
        self.assertEqual([], commit_guards.compare_records(self.record(), parallel))

    def test_a_partial_record_is_refused_even_when_both_records_are_the_same(self) -> None:
        for step in ("control", "fault"):
            with self.subTest(step=step):
                partial = self.record()
                partial["observations"] = [item for item in partial["observations"] if not (item["case"] == CASES[2].name and item["step"] == step)]
                problems = commit_guards.compare_records(partial, copy.deepcopy(partial))
                self.assertTrue(any(CASES[2].name in problem and step in problem for problem in problems), problems)

    def test_a_shard_record_is_refused(self) -> None:
        shard = self.record()
        keep = {case.name for number, case in enumerate(CASES) if number % 2 == 0}
        shard["observations"] = [item for item in shard["observations"] if item["case"] in keep | {"baseline", "source tree"}]
        self.assertTrue(commit_guards.compare_records(shard, copy.deepcopy(shard)))

    def test_a_record_with_no_case_is_refused_whatever_its_outcome(self) -> None:
        empty = self.record()
        empty["observations"] = [item for item in empty["observations"] if item["case"] == "baseline"]
        for outcome in ("blocked: interrupted", "pass", "fail"):
            with self.subTest(outcome=outcome):
                empty["outcome"] = outcome
                self.assertTrue(commit_guards.compare_records(empty, copy.deepcopy(empty)))

    def test_a_case_that_was_observed_twice_is_refused(self) -> None:
        doubled = self.record()
        doubled["observations"].append(copy.deepcopy(doubled["observations"][3]))
        problems = commit_guards.compare_records(doubled, copy.deepcopy(doubled))
        self.assertTrue(any(doubled["observations"][3]["case"] in problem for problem in problems), problems)

    def test_a_record_whose_outcome_is_not_pass_is_refused_on_either_side(self) -> None:
        for outcome in ("fail", "blocked: interrupted", "blocked", ""):
            for side in (0, 1):
                with self.subTest(outcome=outcome, side=side):
                    records = [self.record(), self.record()]
                    records[side]["outcome"] = outcome
                    problems = commit_guards.compare_records(*records)
                    self.assertTrue(any("outcome" in problem for problem in problems), problems)

    def test_a_record_with_no_outcome_is_refused(self) -> None:
        records = [self.record(), self.record()]
        del records[1]["outcome"]
        self.assertTrue(any("outcome" in problem for problem in commit_guards.compare_records(*records)))

    def test_a_failed_coverage_observation_is_refused_on_either_side(self) -> None:
        for side in (0, 1):
            with self.subTest(side=side):
                records = [self.record(), self.record()]
                records[side]["observations"].append(self.coverage(False))
                problems = commit_guards.compare_records(*records)
                self.assertTrue(any("coverage" in problem for problem in problems), problems)

    def test_a_failed_case_observation_is_refused_even_when_both_records_have_it(self) -> None:
        failed = self.record()
        failed["observations"][3]["passed"] = False
        problems = commit_guards.compare_records(failed, copy.deepcopy(failed))
        self.assertTrue(any(failed["observations"][3]["case"] in problem for problem in problems), problems)

    def test_records_made_from_different_copied_files_are_not_the_same_code(self) -> None:
        other = self.record()
        other["environment"]["copied_files_sha256"] = "other files"
        problems = commit_guards.compare_records(self.record(), other)
        self.assertTrue(any("copied_files_sha256" in problem for problem in problems), problems)

    def test_a_record_without_the_digest_of_its_copied_files_is_refused(self) -> None:
        for side in (0, 1):
            with self.subTest(side=side):
                records = [self.record(), self.record()]
                del records[side]["environment"]["copied_files_sha256"]
                self.assertTrue(any("copied_files_sha256" in problem for problem in commit_guards.compare_records(*records)))
        bare = self.record()
        del bare["environment"]
        self.assertTrue(any("copied_files_sha256" in problem for problem in commit_guards.compare_records(bare, bare)))

    def test_a_malformed_record_is_refused_without_a_traceback(self) -> None:
        for broken in ({}, {"observations": "none"}, {"outcome": "pass", "observations": [{"case": "x"}]}):
            with self.subTest(broken=broken):
                self.assertTrue(commit_guards.compare_records(broken, self.record()))

    def test_the_inputs_are_not_changed(self) -> None:
        first, second = self.record(), self.record()
        before = copy.deepcopy((first, second))
        commit_guards.compare_records(first, second)
        self.assertEqual(before, (first, second))

    def test_the_compare_command_returns_zero_for_agreeing_records_and_one_otherwise(self) -> None:
        with tempfile.TemporaryDirectory() as root, mock.patch.object(sys, "stdout", io.StringIO()) as printed:
            folders = []
            for name, change in (("a", {}), ("b", {}), ("c", {"tree_sha256": "other"})):
                folder = Path(root) / name
                folder.mkdir()
                (folder / "record.json").write_text(json.dumps({**self.record(), **change}))
                folders.append(str(folder))
            self.assertEqual(0, commit_guards.main(["--compare", folders[0], folders[1]]))
            self.assertEqual(1, commit_guards.main(["--compare", folders[0], folders[2]]))
            self.assertIn("tree_sha256", printed.getvalue())
            self.assertEqual(1, commit_guards.main(["--compare", folders[0], str(Path(root) / "absent")]))

    def test_the_compare_command_refuses_two_identical_partial_records_and_two_interrupted_records(self) -> None:
        partial = self.record()
        partial["observations"] = partial["observations"][:5]
        interrupted = {**self.record(), "outcome": "blocked: interrupted", "observations": []}
        with tempfile.TemporaryDirectory() as root, mock.patch.object(sys, "stdout", io.StringIO()) as printed:
            for name, record in (("partial", partial), ("interrupted", interrupted)):
                folders = []
                for number in (1, 2):
                    folder = Path(root) / f"{name}_{number}"
                    folder.mkdir()
                    (folder / "record.json").write_text(json.dumps(record))
                    folders.append(str(folder))
                with self.subTest(name=name):
                    self.assertEqual(1, commit_guards.main(["--compare", *folders]))
            self.assertIn("outcome", printed.getvalue())
            self.assertIn(CASES[-1].name, printed.getvalue())


if __name__ == "__main__":
    unittest.main()
