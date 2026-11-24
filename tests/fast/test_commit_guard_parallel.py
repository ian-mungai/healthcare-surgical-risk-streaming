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
- The comparison of two records passes records of different code, different cases or different results.
"""

from __future__ import annotations

import copy
import io
import json
import os
import sys
import tempfile
import threading
import unittest
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


class StubFixture:
    """A fixture that makes no repository: it records that it was filled and removed."""

    instances: list[StubFixture] = []

    def __init__(self, record: Record) -> None:
        self.record = record
        self.filled = False
        self.removed = False
        StubFixture.instances.append(self)

    def populate(self, paths: list[str]) -> None:
        self.filled = True

    def remove(self) -> None:
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
        self.output = io.StringIO()
        self.cores = 16
        self.available: int | None = None
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.runs),
            mock.patch.object(tempfile, "tempdir", str(self.temp)),
            mock.patch.object(sys, "stdout", self.output),
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "Fixture", StubFixture),
            mock.patch.object(commit_guards, "source_files", return_value=["a.txt"]),
            mock.patch.object(commit_guards, "tree_digest", return_value="digest"),
            mock.patch.object(os, "cpu_count", side_effect=lambda: self.cores),
            mock.patch.object(commit_guards, "available_memory_mib", side_effect=lambda: self.available),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

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
            + [("shards", "coverage"), ("source tree", "snapshot")]
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


class WorkerPlanTest(ParallelRunCase):
    def test_the_count_is_the_smallest_of_the_request_the_cases_the_cores_less_one_and_the_memory(self) -> None:
        plan = commit_guards.plan_workers
        self.assertEqual(4, plan(4, 15, 16, None))
        self.assertEqual(3, plan(8, 3, 16, None))
        self.assertEqual(5, plan(8, 15, 6, None))
        memory = commit_guards.WORKER_MIB
        self.assertEqual(2, plan(8, 15, 16, 2 * memory + memory // 2))
        self.assertEqual(8, plan(8, 15, 16, 8 * memory))

    def test_there_is_always_at_least_one_worker(self) -> None:
        self.assertEqual(1, commit_guards.plan_workers(4, 15, 1, None))
        self.assertEqual(1, commit_guards.plan_workers(4, 15, 16, 0))
        self.assertEqual(1, commit_guards.plan_workers(4, 0, 16, None))

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

    def test_the_meminfo_reader_gives_mebibytes_of_available_memory(self) -> None:
        self.assertEqual(8192000 // 1024, commit_guards.meminfo_available_mib(MEMINFO))
        self.assertIsNone(commit_guards.meminfo_available_mib("MemTotal: 1 kB\n"))

    def test_the_vm_stat_reader_counts_free_inactive_and_speculative_pages(self) -> None:
        self.assertEqual((1000 + 2000 + 500) * 16384 // 1024 // 1024, commit_guards.vm_stat_available_mib(VM_STAT))
        self.assertIsNone(commit_guards.vm_stat_available_mib("Pages free: 5.\n"))
        without_speculative = "\n".join(line for line in VM_STAT.splitlines() if "speculative" not in line) + "\n"
        self.assertIsNone(commit_guards.vm_stat_available_mib(without_speculative))
        self.assertIsNone(commit_guards.vm_stat_available_mib(VM_STAT.replace("page size of 16384 bytes", "page size unknown")))


class StopTest(unittest.TestCase):
    """The real ``run_cases`` starts no case after the stop event is set."""

    def run_shard(self, stop: threading.Event | None, shard: tuple[int, int]) -> list[str]:
        fixture = mock.Mock()
        fixture.run.return_value = (0, "")
        started: list[str] = []

        def run_case(fixture: object, case: commit_guards.Case, log: list[str]) -> list[Observation]:
            started.append(case.name)
            return case_observations(case)

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
        return {"revision": "abc", "tree_sha256": "def", "observations": observations}

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
                problems = commit_guards.compare_records(self.record(), other)
                self.assertEqual(1, len(problems), problems)
                self.assertIn(other["observations"][3]["case"], problems[0])

    def test_a_missing_or_extra_observation_is_named(self) -> None:
        shorter = self.record()
        removed = shorter["observations"].pop(5)
        self.assertIn(removed["case"], " ".join(commit_guards.compare_records(self.record(), shorter)))
        self.assertIn(removed["case"], " ".join(commit_guards.compare_records(shorter, self.record())))

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


if __name__ == "__main__":
    unittest.main()
