"""The capacity probe is one loop on one deadline that collects what it can read, writes it down with its gaps and judges nothing: it reads the Spark
state, takes a sample and reads the events in a fixed order, ends only after the job exited, takes nothing late, obeys a signal without ending a step half way
and cleans up in order with proof, also when a step failed.

Each case changes one fact of a good setup and expects the result to follow from that fact alone. Time is a fake clock, so no case waits.
"""

from __future__ import annotations

import contextlib
import io
import os
import signal
import sys
import tempfile
import time
import unittest
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, run_command
from scripts.stack.capacity_probe import (
    EVENT_OVERLAP_NS,
    Budget,
    Deadline,
    Monitor,
    Options,
    Probe,
    Reader,
    Steps,
    StopFlag,
    Stopped,
    execute,
    exit_status,
    guarded,
    job_arguments,
    parse_job_result,
    parser,
    preflight,
    probe_settings,
    remove_branch,
    remove_container,
    remove_scratch,
    resolve_nessie,
)
from scripts.stack.groups import Settings
from scripts.stack.host_figures import CgroupMemory, FiguresError, HostSources
from scripts.stack.limits import parse_limits
from scripts.stack.nessie import NessieError, Reference
from scripts.stack.probe_docker import ContainerEvent, ContainerInfo, ContainerState, Docker, ProbeError
from scripts.stack.probe_record import CleanupStep, JobFacts, Sample, ServiceSnapshot, write_document

MIB = 1048576
ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "proc"
LIMITS_TEXT = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  nessie:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  spark:
    runtime: compose
    group: batch
    memory_limit_mib: 4096
"""
RESULT_LINE = (
    'RESULT {"branch": "capacity_probe_x", "branch_hash": "h", "table": "t.n", "rows": 10, "memory_limit_mib": 4096, "python_version": "3.14", "uid": 1, '
    '"snapshot_id": 7, "spark_version": "4.1.4", "jvm_max_heap_mib": 100, "extra": "dropped"}\n'
)


class Clock:
    """A clock that only moves when a command or a wait moves it. ``ns`` is the same time in nanoseconds."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def pause(self, seconds: float) -> None:
        self.now += seconds

    def ns(self) -> int:
        return round(self.now * 1e9)


def running(pid: int = 42, started: float = 0.0) -> ContainerState:
    return ContainerState("running", 0, False, pid, 0, 4096 * MIB, started)


def exited(code: int = 0, *, oom: bool = False) -> ContainerState:
    return ContainerState("exited", code, oom, 0, 0, 4096 * MIB, 0.0)


class FakeDocker(Docker):
    """A Docker whose state and events come from a script (an exception in the script is raised) and whose every command takes ``cost`` seconds."""

    def __init__(
        self,
        clock: Clock,
        states: Sequence[ContainerState | BaseException] = (),
        *,
        cost: float = 0.01,
        answers: Sequence[list[ContainerEvent] | BaseException] = (),
    ) -> None:
        super().__init__()
        self.clock, self.cost = clock, cost
        self.states = list(states)
        self.answers = list(answers)
        self.calls: list[str] = []
        self.windows: list[tuple[int, int]] = []

    def state(self, container: str) -> ContainerState:
        self.calls.append("state")
        self.clock.now += self.cost
        answer = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def events(self, project: str, since_nano: int, until_nano: int, *, seconds: float) -> list[ContainerEvent]:
        self.calls.append("events")
        self.clock.now += self.cost
        self.windows.append((since_nano, until_nano))
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, BaseException):
            raise answer
        return answer


class FakeReader(Reader):
    docker: FakeDocker

    def __init__(self, clock: Clock, docker: FakeDocker, *, cost: float = 0.0) -> None:
        self.clock, self.docker, self.cost = clock, docker, cost

    def take(self, *, spark_running: bool) -> tuple[Sample, int]:
        self.docker.calls.append("sample")
        self.clock.now += self.cost
        usage = {"nessie": MIB, "postgres": MIB, **({"spark": MIB} if spark_running else {})}
        taken = self.clock.ns()
        return Sample(taken, "t", taken - 1, taken, (), spark_running, {"nessie": True, "postgres": True, "spark": spark_running}, None, usage), 0


def monitor(
    clock: Clock, docker: FakeDocker, *, total: float = 1000.0, stop: StopFlag | None = None, interval: float = 5.0, cgroup: Any = None, cost: float = 0.0
) -> Monitor:
    return Monitor(
        docker,
        "p",
        FakeReader(clock, docker, cost=cost),
        Budget(total, clock=clock.monotonic),
        stop or StopFlag(),
        interval,
        read_cgroup=cgroup,
        clock_ns=clock.ns,
        monotonic=clock.monotonic,
        pause=clock.pause,
    )


class BudgetTest(unittest.TestCase):
    def test_a_wait_is_cut_to_the_time_that_is_left_and_a_run_with_none_left_fails_as_a_deadline(self) -> None:
        clock = Clock()
        budget = Budget(10.0, clock=clock.monotonic)
        self.assertEqual(4.0, budget.limit(4.0))
        clock.now += 8.0
        self.assertEqual(2.0, budget.limit(30.0))
        clock.now += 2.0
        with self.assertRaisesRegex(Deadline, "run's deadline of 10 seconds has passed"):
            budget.limit(1.0)

    def test_a_result_is_taken_before_the_deadline_and_refused_at_or_after_it(self) -> None:
        clock = Clock()
        budget = Budget(10.0, clock=clock.monotonic)
        clock.now += 9.9
        budget.accept("a sample")
        clock.now += 0.1
        with self.assertRaisesRegex(Deadline, "a sample finished after the deadline of 10 seconds"):
            budget.accept("a sample")


class StopFlagTest(unittest.TestCase):
    def test_a_signal_only_counts_and_a_second_one_counts_again(self) -> None:
        flag = StopFlag()
        self.assertFalse(flag.requested)
        flag.handle(signal.SIGINT, None)
        flag.handle(signal.SIGTERM, None)
        self.assertEqual((True, 2), (flag.requested, flag.count))

    def test_a_real_signal_sets_the_flag_and_raises_nothing_and_the_old_handlers_come_back(self) -> None:
        old = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        flag = StopFlag()
        flag.install()
        try:
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGTERM)
            for _ in range(200):
                if flag.count >= 2:
                    break
                time.sleep(0.01)
        finally:
            flag.restore()
        self.assertEqual(2, flag.count)
        self.assertEqual(old, {number: signal.getsignal(number) for number in old})

    def test_a_sleep_is_split_into_short_slices_and_ends_at_once_when_a_signal_came(self) -> None:
        flag, slices = StopFlag(), []

        def pause(seconds: float) -> None:
            slices.append(seconds)
            if len(slices) == 3:
                flag.handle(signal.SIGINT, None)

        flag.sleep(5.0, pause=pause)
        self.assertEqual([0.2, 0.2, 0.2], slices)


class SignalChildTest(unittest.TestCase):
    """The real loop in a child process that gets real signals: the end of the run finishes after one signal and after two."""

    def run_child(self, *signals: str) -> tuple[int, dict[str, str]]:
        kills = "; sleep 0.5; ".join(f"kill -{name} $pid" for name in signals)
        script = (
            'folder="$1"; shift; "$@" "$folder" & pid=$!; i=0; '
            'until [ -f "$folder/ready" ] || [ $i -ge 150 ]; do i=$((i+1)); sleep 0.1; done; '
            f"{kills}; wait $pid; echo EXIT=$?"
        )
        with tempfile.TemporaryDirectory() as folder:
            result = run_command("sh", ["-c", script, "sh", folder, sys.executable, "-B", "-m", "tests.fast.probe_signal_child"], cwd=ROOT, timeout=60)
            markers = {path.name: path.read_text() for path in Path(folder).iterdir()}
        return int(result.stdout.rsplit("EXIT=", 1)[1]), markers

    def test_one_signal_ends_the_loop_and_the_cleanup_and_the_record_finish(self) -> None:
        code, markers = self.run_child("INT")
        self.assertEqual(130, code)
        self.assertEqual({"ready", "cleanup_started", "cleanup_done", "record_written"}, set(markers))
        self.assertEqual("1 interrupted stop_flag", markers["record_written"])

    def test_a_second_signal_during_the_cleanup_changes_nothing(self) -> None:
        code, markers = self.run_child("INT", "TERM")
        self.assertEqual(130, code)
        self.assertIn("cleanup_done", markers)
        self.assertEqual("2 interrupted stop_flag", markers["record_written"])


class ReaderTest(unittest.TestCase):
    def reader(self, docker: Docker, *, host: bool, folder: str = "") -> tuple[Reader, Clock]:
        clock = Clock()
        sources = (
            HostSources(FIXTURES / "meminfo_before.txt", FIXTURES / "stat_before.txt", FIXTURES / "diskstats_before.txt", Path(folder or "/"), (259, 1))
            if host
            else None
        )
        return Reader("p", sources, docker, ("postgres", "nessie"), clock_ns=clock.ns, now=clock.monotonic), clock

    def docker(self, *, stats_error: bool = False) -> Docker:
        class Containers(Docker):
            def usage_texts(self, project: str) -> tuple[str, str]:
                if stats_error:
                    raise ProbeError("reading the container statistics: docker exited 1")
                return "a1b2c3d4e5f6|postgres\nb2c3d4e5f6a1|spark\n", "a1b2c3d4e5f6|300MiB / 1GiB\nb2c3d4e5f6a1|900MiB / 4GiB\n"

        return Containers()

    def test_a_local_sample_names_the_host_figures_as_missing_and_that_is_not_an_error(self) -> None:
        reader, clock = self.reader(self.docker(), host=False)
        clock.now += 5
        sample, errors = reader.take(spark_running=True)
        self.assertEqual((("host_figures",), 0, None), (sample.missing, errors, sample.host))
        self.assertEqual({"postgres": True, "nessie": False, "spark": True}, sample.services_running)
        self.assertEqual(300 * MIB, sample.containers_bytes["postgres"])

    def test_a_host_sample_covers_the_interval_between_its_two_readings_and_nothing_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            reader, clock = self.reader(self.docker(), host=True, folder=folder)
            start = clock.ns()
            clock.now += 5
            sample, errors = reader.take(spark_running=False)
        self.assertEqual((0, (), start, clock.ns()), (errors, sample.missing, sample.covers_start_ns, sample.covers_end_ns))
        self.assertEqual(sample.taken_ns, sample.covers_end_ns)
        if sample.host is None:
            self.fail("no host figures")
        self.assertEqual(20971520, sample.host.memory_available_kib)

    def test_a_host_reading_that_fails_is_named_and_counted_and_the_next_sample_covers_from_the_last_good_reading(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            reader, clock = self.reader(self.docker(), host=True, folder=folder)
            start = clock.ns()
            good = reader.sources
            reader.sources = HostSources(Path(folder) / "missing", Path(folder) / "missing", Path(folder) / "missing", Path(folder), (259, 1))
            clock.now += 5
            bad, errors = reader.take(spark_running=False)
            reader.sources = good
            clock.now += 5
            later, _ = reader.take(spark_running=False)
        self.assertEqual((("host_figures",), 1), (bad.missing, errors))
        self.assertEqual(start, later.covers_start_ns)

    def test_a_docker_reading_that_fails_is_named_and_counted_and_the_host_figures_are_kept(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            reader, clock = self.reader(self.docker(stats_error=True), host=True, folder=folder)
            clock.now += 5
            sample, errors = reader.take(spark_running=False)
        self.assertEqual((("container_memory",), 1, {}), (sample.missing, errors, dict(sample.containers_bytes)))
        self.assertIsNotNone(sample.host)


class MonitorTest(unittest.TestCase):
    def test_one_iteration_reads_the_state_then_takes_the_sample_then_reads_the_events(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker)
        loop.begin()
        clock.now += 1
        state = loop.tick("job")
        self.assertEqual(["state", "sample", "events"], docker.calls)
        self.assertEqual("running", state.status if state else "")
        self.assertTrue(loop.samples[0].spark_running)

    def test_a_tick_with_no_container_reads_no_state(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker)
        loop.begin()
        clock.now += 1
        self.assertIsNone(loop.tick())
        self.assertEqual(["sample", "events"], docker.calls)
        self.assertFalse(loop.samples[0].spark_running)

    def test_the_windows_chain_and_each_command_asks_from_a_few_seconds_before_the_cursor_and_an_event_seen_twice_is_kept_once(self) -> None:
        clock = Clock()
        late = ContainerEvent(clock.ns() + 4_000_000_000, "kill", "postgres", "p-postgres-1", "signal 9")
        docker = FakeDocker(clock, [running()], answers=[[], [late], [late]])
        loop = monitor(clock, docker)
        loop.begin()
        start = loop.run_start_ns
        clock.now += 20
        loop.tick()
        first_end = loop.cursor_ns
        clock.now += 20
        loop.tick()
        second_end = loop.cursor_ns
        clock.now += 1
        loop.tick()
        self.assertEqual([(start, first_end), (first_end, second_end), (second_end, loop.cursor_ns)], loop.windows)
        self.assertEqual(first_end - EVENT_OVERLAP_NS, docker.windows[1][0])
        self.assertEqual(["kill"], [event.action for event in loop.events])

    def test_an_events_command_that_fails_does_not_move_the_cursor_and_is_named_in_its_sample_and_counted_and_the_loop_goes_on(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()], answers=[[], ProbeError("reading the container events: docker exited 1"), []])
        loop = monitor(clock, docker)
        loop.begin()
        start = loop.run_start_ns
        for _ in range(3):
            clock.now += 1
            loop.tick()
        self.assertEqual(("events",), loop.samples[1].missing)
        self.assertEqual((1, 2), (loop.sample_errors, len(loop.windows)))
        self.assertEqual(start, loop.windows[0][0])
        self.assertEqual(loop.windows[0][1], loop.windows[1][0], "the next window starts where the last good one ended")

    def test_a_state_that_cannot_be_read_is_named_in_its_sample_and_counted_and_the_loop_still_ends_after_the_exit(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [ProbeError("reading a container's state: docker exited 1"), running(), exited()])
        loop = monitor(clock, docker)
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual((("spark_state",), 1), (loop.samples[0].missing, loop.sample_errors))
        self.assertFalse(loop.samples[0].spark_running)

    def test_a_reader_that_reports_failed_readings_is_counted_once_for_the_iteration(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker)
        loop.reader.take = lambda *, spark_running: (Sample(1, "t", 0, 1, ("host_figures",), spark_running, {}, None, {}), 2)  # type: ignore[method-assign]
        loop.begin()
        clock.now += 1
        loop.tick()
        self.assertEqual(1, loop.sample_errors)

    def test_the_loop_ends_after_the_exit_is_seen_and_one_more_iteration_has_run(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running(), running(), exited()])
        loop = monitor(clock, docker)
        loop.begin()
        state = loop.watch_job("job", 600.0)
        self.assertEqual("exited", state.status)
        self.assertEqual([True, True, False, False], [item.spark_running for item in loop.samples])
        self.assertGreaterEqual(loop.windows[-1][1], loop.spark.exit_ns or 0)

    def test_it_waits_the_rest_of_the_interval_between_iterations_but_not_after_the_exit(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running(), running(), exited()]))
        loop.begin()
        began = clock.now
        loop.watch_job("job", 600.0)
        self.assertAlmostEqual(2 * 5.0, clock.now - began, delta=0.5)

    def test_a_signal_leaves_the_loop_at_its_next_check_and_one_that_comes_during_the_wait_ends_the_wait(self) -> None:
        clock = Clock()
        flag = StopFlag()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker, stop=flag)
        loop.begin()
        flag.handle(signal.SIGINT, None)
        with self.assertRaises(Stopped):
            loop.watch_job("job", 600.0)
        self.assertEqual([], docker.calls)
        flag, clock = StopFlag(), Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]), stop=flag)
        loop.begin()

        def pause_and_signal(seconds: float) -> None:
            clock.pause(seconds)
            flag.handle(signal.SIGTERM, None)

        loop.pause = pause_and_signal
        with self.assertRaises(Stopped):
            loop.watch_job("job", 600.0)
        self.assertLess(clock.now - 1000.0, 1.0)

    def test_a_job_past_its_deadline_and_a_run_past_its_own_are_deadlines(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]))
        loop.begin()
        with self.assertRaisesRegex(Deadline, "the Spark job timed out after 8 seconds"):
            loop.watch_job("job", 8.0)
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]), total=7.0)
        loop.begin()
        with self.assertRaisesRegex(Deadline, "run's deadline of 7 seconds has passed"):
            loop.watch_job("job", 600.0)
        self.assertLessEqual(clock.now - 1000.0, 7.5)

    def test_a_sample_that_finishes_after_the_run_deadline_is_kept_as_read_and_ends_the_run(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]), total=3.0, cost=10.0)
        loop.begin()
        with self.assertRaisesRegex(Deadline, "a sample finished after the deadline of 3 seconds"):
            loop.tick()
        self.assertEqual(1, len(loop.samples))

    def test_the_spark_cgroup_is_read_each_iteration_with_its_read_time_and_an_unreadable_one_is_counted(self) -> None:
        clock = Clock()
        readings = [CgroupMemory(100, 4096 * MIB, {"oom_kill": 0}), FiguresError("gone"), CgroupMemory(900, 4096 * MIB, {"oom_kill": 1})]
        reader = mock.Mock(side_effect=readings)
        loop = monitor(clock, FakeDocker(clock, [running(), running(), running(), exited()]), cgroup=reader)
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual([100, 900], [item.memory.peak_bytes for item in loop.spark_reads])
        self.assertTrue(all(item.read_at_ns > 0 for item in loop.spark_reads))
        self.assertEqual(1, loop.cgroup_unreadable)
        self.assertEqual(3, reader.call_count, "no reading once the container has exited")

    def test_without_a_cgroup_reader_nothing_is_read_and_nothing_is_counted(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running(), exited()]))
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual(([], 0), (loop.spark_reads, loop.cgroup_unreadable))

    def test_the_group_command_progress_takes_an_iteration_only_when_one_is_due_and_a_signal_leaves_at_once(self) -> None:
        clock = Clock()
        flag = StopFlag()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker, stop=flag)
        loop.begin()
        clock.now += 1
        loop.hook()
        loop.hook()
        self.assertEqual(1, docker.calls.count("sample"))
        clock.now += 5
        loop.hook()
        self.assertEqual(2, docker.calls.count("sample"))
        flag.handle(signal.SIGINT, None)
        with self.assertRaises(Stopped):
            loop.hook()


class FakeSteps:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail: dict[str, BaseException] = {}
        self.hooks: dict[str, Callable[[], None]] = {}
        self.cleanup_result = [CleanupStep("job container", True, "no container remains"), CleanupStep("nessie branch", True, "no branch existed")]

    def step(self, name: str) -> None:
        self.calls.append(name)
        if name in self.hooks:
            self.hooks[name]()
        if name in self.fail:
            raise self.fail[name]

    def steps(self) -> Steps:
        def start(hook: Callable[[], None]) -> None:
            self.step("start")
            hook()

        def snapshot() -> Mapping[str, ServiceSnapshot]:
            self.calls.append("snapshot")
            return {"postgres": ServiceSnapshot(True, 0, 1.0, False, 1, None)}

        def finish(state: ContainerState) -> JobFacts:
            self.step("finish")
            return JobFacts(0, False, 1.0, {}, True)

        def cleanup() -> list[CleanupStep]:
            self.step("cleanup")
            return self.cleanup_result

        return Steps(start, snapshot, lambda: self.step("catalog"), lambda: self.step("start_job"), "job", 600.0, finish, cleanup, "scratch_x")


class ExecuteTest(unittest.TestCase):
    def execute(self, fake: FakeSteps, *, states: Sequence[ContainerState] | None = None, stop: StopFlag | None = None) -> Any:
        clock = Clock()
        return execute(fake.steps(), monitor(clock, FakeDocker(clock, states or [running(), exited()]), stop=stop), "host")

    def test_a_run_that_reaches_the_end_of_the_job_is_recorded_with_its_snapshots_and_its_cleanup(self) -> None:
        fake = FakeSteps()
        run = self.execute(fake)
        self.assertEqual(["start", "snapshot", "catalog", "start_job", "finish", "snapshot", "cleanup"], fake.calls)
        self.assertEqual(("recorded", "", "", "host", "scratch_x"), (run.run_state, run.cause, run.step, run.mode, run.scratch))
        self.assertEqual({"postgres"}, set(run.start_snapshot) & set(run.end_snapshot))
        self.assertIsNotNone(run.job)
        self.assertIsNotNone(run.job_exit_ns)
        self.assertGreater(len(run.windows), 0)

    def test_each_step_that_fails_interrupts_the_run_with_a_fixed_cause_and_step_and_the_cleanup_still_runs(self) -> None:
        for name, step in (("start", "always_on_start"), ("catalog", "catalog_check"), ("start_job", "job_start"), ("finish", "job_watch")):
            with self.subTest(step=name):
                fake = FakeSteps()
                fake.fail[name] = ProbeError("a message with https://nessie.example/x in it")
                run = self.execute(fake)
                self.assertEqual(("interrupted", "error", step), (run.run_state, run.cause, run.step))
                self.assertEqual("cleanup", fake.calls[-1])

    def test_a_start_that_failed_has_no_snapshot_and_a_failure_later_has_both(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = ProbeError("x")
        early = self.execute(fake)
        self.assertEqual((0, 0), (len(early.start_snapshot), len(early.end_snapshot)))
        late = FakeSteps()
        late.fail["start_job"] = ProbeError("x")
        run = self.execute(late)
        self.assertEqual((1, 1), (len(run.start_snapshot), len(run.end_snapshot)))

    def test_a_job_that_never_ends_is_an_interruption_by_deadline_in_the_job_watch(self) -> None:
        run = self.execute(FakeSteps(), states=[running()])
        self.assertEqual(("interrupted", "deadline", "job_watch"), (run.run_state, run.cause, run.step))
        self.assertIsNone(run.job)

    def test_a_deadline_while_the_group_starts_is_a_deadline_and_not_an_error(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = Deadline("the run's deadline of 9 seconds has passed")
        self.assertEqual(("deadline", "always_on_start"), (lambda run: (run.cause, run.step))(self.execute(fake)))

    def test_a_signal_during_the_job_is_an_interruption_by_the_stop_flag_and_the_cleanup_runs_after_it(self) -> None:
        fake = FakeSteps()
        flag = StopFlag()
        fake.hooks["start_job"] = lambda: flag.handle(signal.SIGINT, None)
        run = self.execute(fake, stop=flag)
        self.assertEqual(("interrupted", "stop_flag", "job_watch"), (run.run_state, run.cause, run.step))
        self.assertNotIn("finish", fake.calls)
        self.assertEqual("cleanup", fake.calls[-1])
        self.assertEqual((1, 1), (len(run.start_snapshot), len(run.end_snapshot)))

    def test_a_signal_while_the_group_starts_names_that_step(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = Stopped("a signal came while the always-on group started")
        run = self.execute(fake)
        self.assertEqual(("stop_flag", "always_on_start"), (run.cause, run.step))

    def test_a_cleanup_that_crashes_is_a_step_that_was_not_verified_and_the_record_is_still_made(self) -> None:
        fake = FakeSteps()
        fake.fail["cleanup"] = RuntimeError("secret body")
        run = self.execute(fake)
        self.assertEqual([CleanupStep("cleanup", False, "stopped with RuntimeError")], run.cleanup)
        self.assertEqual("recorded", run.run_state)

    def test_the_exit_status_says_only_that_the_tool_ran(self) -> None:
        recorded, interrupted = {"run_state": "recorded"}, {"run_state": "interrupted"}
        self.assertEqual((0, 1, 130), (exit_status(recorded, False), exit_status(interrupted, False), exit_status(interrupted, True)))
        self.assertEqual(0, exit_status({"run_state": "recorded", "gaps": [{"code": "sample_error", "count": 9}]}, False))


class FakeNessie:
    def __init__(
        self, *, exists: bool = True, delete_error: BaseException | None = None, lookups: Sequence[NessieError | bool | BaseException] | None = None
    ) -> None:
        self.exists, self.delete_error = exists, delete_error
        self.lookups = list(lookups) if lookups is not None else None
        self.deleted: list[str] = []
        self.configured = 0

    def config(self) -> dict[str, str]:
        self.configured += 1
        return {}

    def reference(self, name: str) -> Reference:
        if self.lookups is not None:
            answer = self.lookups.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            if not answer:
                raise NessieError("GET failed", status=404)
            return Reference("BRANCH", name, "hash")
        if not self.exists:
            raise NessieError("GET failed", status=404)
        return Reference("BRANCH", name, "hash")

    def delete_reference(self, name: str, hash_value: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(name)
        self.exists = False


class FakeContainers(Docker):
    def __init__(
        self, *, remove_error: BaseException | None = None, still_there: bool = False, volume_still_there: bool = False, volume_error: ProbeError | None = None
    ) -> None:
        super().__init__()
        self.remove_error, self.still_there, self.volume_still_there, self.volume_error = remove_error, still_there, volume_still_there, volume_error
        self.events_log: list[str] = []

    def create_volume(self, name: str, project: str) -> None:
        self.events_log.append("create volume")

    def remove(self, container: str) -> None:
        self.events_log.append("remove container")
        if self.remove_error is not None:
            raise self.remove_error

    def exists(self, container: str) -> bool:
        self.events_log.append("look for container")
        return self.still_there

    def remove_volume(self, name: str) -> None:
        self.events_log.append("remove volume")
        if self.volume_error is not None:
            raise self.volume_error

    def volume_exists(self, name: str) -> bool:
        self.events_log.append("look for volume")
        return self.volume_still_there


class CleanupStepTest(unittest.TestCase):
    def test_a_container_a_volume_and_a_branch_each_removed_and_looked_up_again_are_verified_steps(self) -> None:
        self.assertEqual(CleanupStep("job container", True, "no container remains"), remove_container(FakeContainers(), "job"))
        self.assertEqual(CleanupStep("scratch volume", True, "no volume remains"), remove_scratch(FakeContainers(), "v"))
        client = FakeNessie()
        self.assertEqual(CleanupStep("nessie branch", True, "removed and absent"), remove_branch(client, "b"))
        self.assertEqual(["b"], client.deleted)
        self.assertEqual(CleanupStep("nessie branch", True, "no branch existed"), remove_branch(FakeNessie(exists=False), "b"))

    def test_a_thing_that_is_still_there_or_whose_look_fails_is_not_verified(self) -> None:
        self.assertIn("still present", remove_container(FakeContainers(still_there=True), "job").detail)
        self.assertFalse(remove_scratch(FakeContainers(volume_still_there=True), "v").verified_absent)
        self.assertFalse(remove_scratch(FakeContainers(volume_error=ProbeError("removing the job's scratch volume: docker exited 125")), "v").verified_absent)
        self.assertFalse(remove_container(FakeContainers(remove_error=ProbeError("removing a container: docker exited 125")), "job").verified_absent)
        self.assertIn("still present", remove_branch(FakeNessie(lookups=[True, True, True]), "b").detail)

    def test_a_refused_deletion_holds_the_status_and_never_the_response_text_or_an_address(self) -> None:
        error = NessieError("DELETE http://127.0.0.1:19120/api/v2/trees/x was refused with status 500, error code X: SECRET BODY", status=500)
        step = remove_branch(FakeNessie(delete_error=error), "b")
        self.assertEqual(CleanupStep("nessie branch", False, "Nessie answered with status 500"), step)
        self.assertEqual("Nessie did not answer", remove_branch(FakeNessie(lookups=[NessieError("GET failed: ConnectionRefusedError")]), "b").detail)

    def test_a_step_that_fails_in_an_unforeseen_way_is_that_steps_failure_only(self) -> None:
        def boom() -> CleanupStep:
            raise KeyError("secret")

        self.assertEqual(CleanupStep("x", False, "stopped with KeyError"), guarded("x", boom))


def compose_runner(code: int = 0) -> Callable[..., CompletedProcess[str]]:
    calls: list[list[str]] = []

    def run(program: str, args: Sequence[str], *, timeout: float, **_: object) -> CompletedProcess[str]:
        calls.append([program, *args])
        return CompletedProcess([program, *args], code, "name\n", "")

    run.calls = calls  # type: ignore[attr-defined]
    return run


def probe_for(
    clock: Clock, docker: Docker, *, host: bool = True, runner: Callable[..., CompletedProcess[str]] | None = None, client: Any = None, budget: float = 1000.0
) -> Probe:
    return Probe(
        Settings(compose_directory=Path("/stack/compose"), project="p"),
        parse_limits(LIMITS_TEXT),
        Options(host_figures=host, rows=10),
        docker=docker,
        budget=Budget(budget, clock=clock.monotonic),
        arguments=["--branch", "capacity_probe_x", "--rows", "10"],
        container="p_capacity_probe_x",
        scratch="p_capacity_probe_scratch_x",
        client=client or FakeNessie(),
        runner=runner or compose_runner(),
        clock_ns=clock.ns,
    )


class CleanupOrderTest(unittest.TestCase):
    def cleanup(self, docker: FakeContainers, client: FakeNessie, *, started: bool = True, scratch: bool = True) -> tuple[list[CleanupStep], FakeContainers]:
        probe = probe_for(Clock(), docker, client=client)
        probe.job_started, probe.scratch_named = started, scratch
        return probe.cleanup(docker), docker

    def test_the_container_is_removed_and_verified_first_then_the_volume_then_the_branch(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie())
        self.assertEqual(["job container", "scratch volume", "nessie branch"], [step.name for step in steps])
        self.assertTrue(all(step.verified_absent for step in steps))
        self.assertEqual(["remove container", "look for container", "remove volume", "look for volume"], docker.events_log)

    def test_a_container_that_is_still_there_blocks_the_volume_and_the_branch_which_are_then_not_tried(self) -> None:
        client = FakeNessie()
        steps, docker = self.cleanup(FakeContainers(still_there=True), client)
        self.assertEqual([False, False, False], [step.verified_absent for step in steps])
        self.assertIn("not tried", steps[1].detail)
        self.assertEqual([], client.deleted)
        self.assertEqual(["remove container", "look for container"], docker.events_log)

    def test_a_malformed_nessie_answer_fails_the_branch_step_only(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie(lookups=[KeyError("garbled")]))
        self.assertEqual([True, True, False], [step.verified_absent for step in steps])
        self.assertEqual("stopped with KeyError", steps[2].detail)
        self.assertEqual(4, len(docker.events_log))

    def test_a_job_that_never_started_has_nothing_to_remove_but_the_container_is_still_looked_for(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie(lookups=[AssertionError("Nessie was asked")]), started=False, scratch=False)
        self.assertEqual([True, True, True], [step.verified_absent for step in steps])
        self.assertEqual(["remove container", "look for container"], docker.events_log)

    def test_a_scratch_volume_whose_creation_failed_is_still_removed_by_its_recorded_name_and_verified(self) -> None:
        class TimedOut(FakeContainers):
            def create_volume(self, name: str, project: str) -> None:
                self.events_log.append("create volume")
                raise ProbeError("creating the job's scratch volume: docker timed out after 30 seconds")

        docker = TimedOut()
        probe = probe_for(Clock(), docker)
        with self.assertRaises(ProbeError):
            probe.start_job()
        steps = probe.cleanup(docker)
        self.assertEqual(["create volume", "remove container", "look for container", "remove volume", "look for volume"], docker.events_log)
        self.assertEqual([True, True, True], [step.verified_absent for step in steps])


class ProbeStepTest(unittest.TestCase):
    def test_the_job_starts_detached_with_its_own_name_and_its_scratch_volume_named_first_and_is_not_removed_by_compose(self) -> None:
        runner = compose_runner()
        docker = FakeContainers()
        probe = probe_for(Clock(), docker, runner=runner)
        probe.start_job()
        call = runner.calls[0]  # type: ignore[attr-defined]
        for word in ("--detach", "--no-deps", "run", "--name", "p_capacity_probe_x", "spark", "--rows", "10"):
            self.assertIn(word, call)
        self.assertEqual("p_capacity_probe_scratch_x:/scratch", call[call.index("--volume") + 1])
        self.assertNotIn("--rm", call)
        self.assertEqual(["create volume"], docker.events_log)
        self.assertTrue(probe.job_started and probe.scratch_named)

    def test_a_job_that_cannot_be_started_names_the_step_and_the_status_and_a_run_with_no_time_left_does_not_start_it(self) -> None:
        with self.assertRaisesRegex(ProbeError, "starting the Spark job: docker exited 125"):
            probe_for(Clock(), FakeContainers(), runner=compose_runner(125)).start_job()
        runner = compose_runner()
        with self.assertRaisesRegex(Deadline, "deadline of 0 seconds has passed"):
            probe_for(Clock(), FakeContainers(), runner=runner, budget=0.0).start_job()
        self.assertEqual([], runner.calls)  # type: ignore[attr-defined]

    def test_the_job_facts_are_the_exit_status_the_flag_the_time_and_the_documented_result_as_read(self) -> None:
        class Logs(FakeContainers):
            def logs(self, container: str) -> str:
                return RESULT_LINE.replace('"rows": 10', '"rows": 9')

        probe = probe_for(Clock(), Logs())
        probe.job_began = time.monotonic()
        facts = probe.finish_job(exited(137, oom=True))
        self.assertEqual((137, True, True), (facts.exit_code, facts.oom_killed_flag, facts.result_readable))
        self.assertEqual(9, facts.result["rows"])
        self.assertNotIn("extra", facts.result)

    def test_a_result_that_cannot_be_read_is_written_down_as_not_readable_and_does_not_stop_the_run(self) -> None:
        class NoLogs(FakeContainers):
            def logs(self, container: str) -> str:
                return "no result here\n"

        probe = probe_for(Clock(), NoLogs())
        probe.job_began = time.monotonic()
        facts = probe.finish_job(exited())
        self.assertEqual((False, {}), (facts.result_readable, dict(facts.result)))

    def test_a_snapshot_has_what_each_required_service_shows_and_the_cgroup_with_its_read_time_on_the_host_only(self) -> None:
        clock = Clock()

        class Services(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [] if service == "nessie" else [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                return ContainerState("running", 0, False, 7, 2, 1024 * MIB, 1_763_800_000.5)

        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=CgroupMemory(1, 1024 * MIB, {"oom_kill": 3})):
            host = probe_for(clock, Services()).snapshot()
        postgres = host["postgres"]
        self.assertEqual(
            (True, 2, 1_763_800_000.5, False, 1024 * MIB),
            (postgres.running, postgres.restarts, postgres.started_at, postgres.oom_killed_flag, postgres.docker_memory_cap_bytes),
        )
        if postgres.cgroup is None:
            self.fail("no cgroup")
        self.assertEqual((clock.ns(), {"oom_kill": 3}), (postgres.cgroup.read_at_ns, postgres.cgroup.memory.events))
        self.assertEqual(ServiceSnapshot(False, None, None, None, None, None), host["nessie"])
        local = probe_for(clock, Services(), host=False).snapshot()
        self.assertIsNone(local["postgres"].cgroup)

    def test_a_stopped_service_is_a_snapshot_that_says_it_is_not_running(self) -> None:
        class Stopped(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                return ContainerState("exited", 137, True, 0, 3, 1024 * MIB, 1.0)

        found = probe_for(Clock(), Stopped(), host=False).snapshot()
        self.assertEqual((False, 3, True), (found["postgres"].running, found["postgres"].restarts, found["postgres"].oom_killed_flag))

    def test_a_cgroup_that_cannot_be_read_in_a_snapshot_is_a_snapshot_without_it_and_never_an_error(self) -> None:
        class Services(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                return running()

        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", side_effect=FiguresError("gone")):
            found = probe_for(Clock(), Services()).snapshot()
        self.assertTrue(found["postgres"].running)
        self.assertIsNone(found["postgres"].cgroup)

    def test_the_catalog_check_asks_the_one_catalog_client_and_names_the_status_only(self) -> None:
        client = FakeNessie()
        probe_for(Clock(), FakeContainers(), client=client).check_catalog()
        self.assertEqual(1, client.configured)

        class Down(FakeNessie):
            def config(self) -> dict[str, str]:
                raise NessieError("GET http://127.0.0.1:19120/api/v2/config was refused with status 503: SECRET BODY", status=503)

        with self.assertRaisesRegex(ProbeError, "^Nessie answered with status 503$"):
            probe_for(Clock(), FakeContainers(), client=Down()).check_catalog()

    def test_the_always_on_group_starts_without_any_admission_and_start_up_and_readiness_share_the_one_deadline(self) -> None:
        clock = Clock()
        probe = probe_for(clock, FakeContainers())
        waited: list[float] = []

        def up(arguments: Sequence[str], *, timeout: float, label: str, env: Mapping[str, str] | None = None) -> str:
            clock.now += 100
            waited.append(timeout)
            return ""

        def ready(settings: Settings, group: Any, deadline_seconds: float, log: Callable[[str], None]) -> None:
            waited.append(deadline_seconds)

        with (
            mock.patch("scripts.stack.capacity_probe.read_states", return_value={}),
            mock.patch("scripts.stack.capacity_probe.run_group_docker", up),
            mock.patch("scripts.stack.capacity_probe.wait_ready", ready),
        ):
            probe.options = Options(start_deadline_seconds=300.0)
            probe.start_supporting(lambda: None)
        self.assertEqual([300.0, 200.0], waited, "the readiness wait gets what the start left, so the two never add up to more than the one deadline")

    def test_a_start_that_used_the_whole_deadline_leaves_nothing_for_the_readiness_wait(self) -> None:
        clock = Clock()
        probe = probe_for(clock, FakeContainers())
        probe.options = Options(start_deadline_seconds=50.0)

        def up(arguments: Sequence[str], *, timeout: float, label: str, env: Mapping[str, str] | None = None) -> str:
            clock.now += 60
            return ""

        with (
            mock.patch("scripts.stack.capacity_probe.read_states", return_value={}),
            mock.patch("scripts.stack.capacity_probe.run_group_docker", up),
            self.assertRaisesRegex(Deadline, "the start of the always-on group finished after its deadline"),
        ):
            probe.start_supporting(lambda: None)


class NessieAddressTest(unittest.TestCase):
    def settings(self, folder: Path, host_env: str | None = None) -> Settings:
        services = folder / "services"
        services.mkdir()
        (services / "spark.yml").write_text("      NESSIE_URI: ${NESSIE_URI:-http://nessie:19120/api/v2}\n")
        (services / "nessie.yml").write_text("    ports:\n      - 127.0.0.1:${NESSIE_HOST_PORT:-19120}:19120\n")
        env = folder / "host.env"
        if host_env is not None:
            env.write_text(host_env)
        return Settings(compose_directory=folder, project="p", host_env=env if host_env is not None else None)

    def test_the_defaults_of_the_service_files_give_one_nessie_for_the_job_and_for_the_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            target = resolve_nessie(self.settings(Path(folder)), {})
        self.assertEqual(("http://nessie:19120/api/v2", "http://127.0.0.1:19120/api/v2"), (target.job_uri, target.cleanup_uri))

    def test_the_published_port_comes_from_the_environment_first_then_from_the_host_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder), "# a comment\nNESSIE_HOST_PORT=50123\nOTHER=1\n")
            self.assertEqual("http://127.0.0.1:50123/api/v2", resolve_nessie(settings, {}).cleanup_uri)
            self.assertEqual("http://127.0.0.1:50999/api/v2", resolve_nessie(settings, {"NESSIE_HOST_PORT": "50999"}).cleanup_uri)

    def test_a_job_address_that_is_not_the_projects_nessie_service_is_refused_and_the_message_holds_no_address(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder))
            for environ in ({"NESSIE_URI": "http://10.255.255.1:19120/api/v2"}, {"NESSIE_URI": "http://nessie:19121/api/v2"}):
                with self.subTest(environ=environ), self.assertRaises(ProbeError) as caught:
                    resolve_nessie(settings, environ)
                self.assertIn("cleanup cannot be resolved", str(caught.exception))
                self.assertNotRegex(str(caught.exception), r"https?://|10\.255")

    def test_a_port_that_is_not_a_port_and_service_files_that_cannot_be_read_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder))
            for port in ("abc", "0", "70000", "-1"):
                with self.subTest(port=port), self.assertRaisesRegex(ProbeError, "not a port number"):
                    resolve_nessie(settings, {"NESSIE_HOST_PORT": port})
            with self.assertRaisesRegex(ProbeError, "cannot read the Nessie settings"):
                resolve_nessie(Settings(compose_directory=Path(folder) / "none", project="p"), {})

    def test_the_repositorys_own_service_files_give_the_default_nessie(self) -> None:
        self.assertEqual("http://127.0.0.1:19120/api/v2", resolve_nessie(Settings(compose_directory=ROOT / "compose", project="p"), {}).cleanup_uri)


class SettingsTest(unittest.TestCase):
    def test_the_settings_never_read_the_test_memory_variable_and_the_probe_has_no_path_to_admission(self) -> None:
        with mock.patch.dict(os.environ, {"STACK_TEST_AVAILABLE_MIB": "1"}):
            settings = probe_settings(ROOT / "compose", "p")
        self.assertIsNone(settings.available_mib)
        source = (ROOT / "scripts" / "stack" / "capacity_probe.py").read_text()
        for name in ("STACK_TEST_AVAILABLE_MIB", "available_mib", "check_group", "admit(", "start_group", "from scripts.stack.admission"):
            self.assertNotIn(name, source)

    def test_the_settings_follow_the_arguments_then_the_environment_then_the_repository(self) -> None:
        with mock.patch.dict(os.environ, {"STACK_COMPOSE_DIRECTORY": "/env/compose", "COMPOSE_PROJECT_NAME": "from_env"}):
            self.assertEqual((Path("/env/compose"), "from_env"), (lambda s: (s.compose_directory, s.project))(probe_settings(None, None)))
            self.assertEqual((Path("/arg"), "named"), (lambda s: (s.compose_directory, s.project))(probe_settings(Path("/arg"), "named")))


class PreflightTest(unittest.TestCase):
    def test_a_spark_container_of_the_project_stops_the_run_with_its_name_even_when_it_is_stopped(self) -> None:
        asked: list[bool] = []

        class Existing(Docker):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                asked.append(include_stopped)
                return [ContainerInfo("a1b2c3d4e5f6", "p-spark-run-9", "spark")]

        with tempfile.TemporaryDirectory() as folder, self.assertRaisesRegex(ProbeError, "Spark container of this project exists \\(p-spark-run-9\\)"):
            preflight(Existing(), "p", Path(folder) / "record")
        self.assertEqual([True], asked)

    def test_a_folder_that_holds_a_record_stops_the_run_before_any_docker_command(self) -> None:
        class Never(Docker):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                raise AssertionError("docker was asked")

        with tempfile.TemporaryDirectory() as folder:
            write_document(Path(folder), {"run_state": "recorded"})
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                preflight(Never(), "p", Path(folder))


class ArgumentTest(unittest.TestCase):
    def run(self, result: unittest.TestResult | None = None) -> unittest.TestResult | None:
        with contextlib.redirect_stderr(io.StringIO()):
            return super().run(result)

    def test_every_duration_must_be_finite_and_above_zero(self) -> None:
        for name in ("--interval-seconds", "--start-deadline-seconds", "--job-deadline-seconds"):
            for value in ("nan", "inf", "-inf", "0", "-1", "abc", ""):
                with self.subTest(name=name, value=value), self.assertRaises(SystemExit):
                    parser().parse_args([name, value])

    def test_the_memory_override_and_the_nessie_port_are_not_options(self) -> None:
        for name in ("--available-mib", "--nessie-port"):
            with self.assertRaises(SystemExit):
                parser().parse_args([name, "1"])

    def test_the_row_count_must_be_a_whole_number_of_at_least_one(self) -> None:
        for value in ("0", "-5", "1.5", "x"):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                parser().parse_args(["--rows", value])

    def test_the_usage_text_says_in_one_line_that_the_exit_status_tells_nothing_about_capacity(self) -> None:
        text = " ".join(parser().format_help().split())
        self.assertIn("The exit status says that the tool ran and nothing about capacity", text)


class ResultTest(unittest.TestCase):
    def test_the_result_line_of_the_job_is_read_and_a_missing_or_broken_line_gives_an_empty_result(self) -> None:
        self.assertEqual({"rows": 10}, parse_job_result('WARN something\nRESULT {"rows": 10}\n'))
        for text in ("no result here\n", "RESULT {broken\n", "RESULT [1, 2]\n"):
            self.assertEqual({}, parse_job_result(text))

    def test_the_job_arguments_name_a_branch_of_this_run_and_the_fixed_row_count(self) -> None:
        self.assertEqual(["--branch", "capacity_probe_20261122t100000z", "--rows", "2000000"], job_arguments("20261122T100000Z", 2000000))


if __name__ == "__main__":
    unittest.main()
