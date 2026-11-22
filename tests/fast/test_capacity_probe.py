"""The capacity probe is one loop on one deadline: it reads the Spark state, takes a sample and reads the events in a fixed order, ends only after the job
exited, accepts nothing late, obeys a signal without ending a step half way, cleans up in order with proof and refuses bad input.

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
    Monitor,
    Options,
    Probe,
    Reader,
    SparkWatch,
    Steps,
    StopFlag,
    Stopped,
    execute,
    guarded,
    job_arguments,
    main,
    parse_job_result,
    parser,
    preflight,
    remove_branch,
    remove_container,
    remove_scratch,
    resolve_nessie,
)
from scripts.stack.groups import Settings
from scripts.stack.host_figures import CgroupMemory, FiguresError
from scripts.stack.limits import parse_limits
from scripts.stack.nessie import NessieError, Reference
from scripts.stack.probe_docker import ContainerEvent, ContainerInfo, ContainerState, Docker, ProbeError
from scripts.stack.probe_record import CleanupStep, ContainerEvidence, JobOutcome, Sample, ServiceBaseline, write_document

MIB = 1048576
ROOT = Path(__file__).resolve().parents[2]
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
    """A Docker whose state and events come from a script and whose every command takes ``cost`` seconds of the fake clock."""

    def __init__(
        self, clock: Clock, states: Sequence[ContainerState] = (), *, cost: float = 0.01, answers: Sequence[list[ContainerEvent] | BaseException] = ()
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
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]

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

    def take(self, *, spark_running: bool) -> Sample:
        self.docker.calls.append("sample")
        self.clock.now += self.cost
        return Sample(self.clock.now, "t", None, {"nessie": MIB, "postgres": MIB, **({"spark": MIB} if spark_running else {})}, spark_running)


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
    def test_a_wait_is_cut_to_the_time_that_is_left_and_a_run_with_none_left_fails(self) -> None:
        clock = Clock()
        budget = Budget(10.0, clock=clock.monotonic)
        self.assertEqual(4.0, budget.limit(4.0))
        clock.now += 8.0
        self.assertEqual(2.0, budget.limit(30.0))
        self.assertEqual(2.0, budget.remaining())
        clock.now += 2.0
        with self.assertRaisesRegex(ProbeError, "run's deadline of 10 seconds has passed"):
            budget.limit(1.0)

    def test_a_result_is_accepted_before_the_deadline_and_refused_at_or_after_it(self) -> None:
        clock = Clock()
        budget = Budget(10.0, clock=clock.monotonic)
        clock.now += 9.9
        budget.accept("a sample")
        clock.now += 0.1
        with self.assertRaisesRegex(ProbeError, "a sample finished after the deadline of 10 seconds"):
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

    def test_a_sleep_of_a_fraction_of_a_slice_is_one_short_pause(self) -> None:
        slices: list[float] = []
        StopFlag().sleep(0.05, pause=slices.append)
        self.assertEqual([0.05], slices)


class SignalChildTest(unittest.TestCase):
    """The real loop in a child process that gets real signals: the end of the run is complete after one signal and after two."""

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

    def test_one_signal_ends_the_loop_and_the_cleanup_and_the_record_complete(self) -> None:
        code, markers = self.run_child("INT")
        self.assertEqual(130, code)
        self.assertEqual({"ready", "cleanup_started", "cleanup_done", "record_written"}, set(markers))
        self.assertEqual("1 True", markers["record_written"])

    def test_a_second_signal_during_the_cleanup_changes_nothing(self) -> None:
        code, markers = self.run_child("INT", "TERM")
        self.assertEqual(130, code)
        self.assertIn("cleanup_done", markers)
        self.assertEqual("2 True", markers["record_written"])


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

    def test_a_tick_with_no_container_reads_no_state_and_its_sample_is_not_a_spark_sample(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker)
        loop.begin()
        clock.now += 1
        self.assertIsNone(loop.tick())
        self.assertEqual(["sample", "events"], docker.calls)
        self.assertFalse(loop.samples[0].spark_running)

    def test_the_event_windows_chain_from_the_start_and_the_cursor_moves_only_after_a_command_that_succeeded(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()], answers=[[], ProbeError("reading the container events: docker exited 1"), []])
        loop = monitor(clock, docker)
        loop.begin()
        start = loop.run_start_ns
        clock.now += 1
        loop.tick()
        first_end = loop.cursor_ns
        clock.now += 1
        with self.assertRaisesRegex(ProbeError, "docker exited 1"):
            loop.tick()
        self.assertEqual(first_end, loop.cursor_ns, "the failed command did not move the cursor")
        self.assertEqual([(start, first_end)], loop.windows)
        clock.now += 1
        loop.tick()
        self.assertEqual(start, docker.windows[-1][0], "the command asks from the start of the run, because the overlap reaches back before it")
        self.assertEqual([(start, first_end), (first_end, loop.cursor_ns)], loop.windows, "the recorded windows chain with no overlap")

    def test_the_events_of_two_windows_that_share_a_boundary_are_kept_once(self) -> None:
        clock = Clock()
        die = ContainerEvent(5, "die", "nessie", "p-nessie-1", "")
        docker = FakeDocker(clock, [running()], answers=[[die], [die, ContainerEvent(9, "oom", "nessie", "p-nessie-1", "")]])
        loop = monitor(clock, docker)
        loop.begin()
        for _ in range(2):
            clock.now += 1
            loop.tick()
        self.assertEqual(["die", "oom"], [event.action for event in loop.events])

    def test_each_command_asks_from_a_few_seconds_before_the_cursor_so_that_an_event_published_late_is_not_lost(self) -> None:
        clock = Clock()
        late = ContainerEvent(clock.ns() + 4_000_000_000, "kill", "postgres", "p-postgres-1", "signal 9")
        docker = FakeDocker(clock, [running()], answers=[[], [late]])
        loop = monitor(clock, docker)
        loop.begin()
        clock.now += 20
        loop.tick()
        first_end = loop.cursor_ns
        clock.now += 20
        loop.tick()
        self.assertEqual(first_end - EVENT_OVERLAP_NS, docker.windows[1][0])
        self.assertEqual(["kill"], [event.action for event in loop.events])
        self.assertEqual([(loop.run_start_ns, first_end), (first_end, loop.cursor_ns)], loop.windows)

    def test_a_cursor_that_has_not_moved_reads_no_window(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running()], cost=0.0)
        loop = monitor(clock, docker)
        loop.begin()
        loop.advance_events()
        self.assertEqual([], docker.windows)

    def test_the_loop_ends_after_the_exit_is_seen_and_one_more_iteration_has_run(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running(), running(), exited()])
        loop = monitor(clock, docker)
        loop.begin()
        state = loop.watch_job("job", 600.0)
        self.assertEqual("exited", state.status)
        self.assertEqual(4, docker.calls.count("sample"))
        self.assertEqual([True, True, False, False], [item.spark_running for item in loop.samples])
        self.assertIsNotNone(loop.spark.exit_ns)
        self.assertLess(loop.windows[-2][1], loop.windows[-1][1])
        self.assertGreaterEqual(loop.windows[-1][1], loop.spark.exit_ns or 0, "the last window ends at or after the exit was seen")

    def test_it_waits_the_rest_of_the_interval_between_iterations_but_not_after_the_exit(self) -> None:
        clock = Clock()
        docker = FakeDocker(clock, [running(), running(), exited()])
        loop = monitor(clock, docker)
        loop.begin()
        began = clock.now
        loop.watch_job("job", 600.0)
        self.assertAlmostEqual(2 * 5.0, clock.now - began, delta=0.5)

    def test_a_signal_leaves_the_loop_at_its_next_check(self) -> None:
        clock = Clock()
        flag = StopFlag()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker, stop=flag)
        loop.begin()
        flag.handle(signal.SIGINT, None)
        with self.assertRaises(Stopped):
            loop.watch_job("job", 600.0)
        self.assertEqual([], docker.calls)

    def test_a_signal_that_comes_during_the_wait_ends_the_wait_and_the_next_check_leaves(self) -> None:
        clock = Clock()
        flag = StopFlag()
        docker = FakeDocker(clock, [running()])
        loop = monitor(clock, docker, stop=flag)
        loop.begin()

        def pause(seconds: float) -> None:
            clock.pause(seconds)
            flag.handle(signal.SIGTERM, None)

        loop.pause = pause
        with self.assertRaises(Stopped):
            loop.watch_job("job", 600.0)
        self.assertEqual(1, docker.calls.count("sample"))
        self.assertLess(clock.now - 1000.0, 1.0)

    def test_a_job_that_runs_past_its_deadline_is_a_failure_that_names_it(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]))
        loop.begin()
        with self.assertRaisesRegex(ProbeError, "the Spark job timed out after 8 seconds"):
            loop.watch_job("job", 8.0)

    def test_the_wait_is_cut_to_the_run_deadline_and_a_run_with_no_time_left_does_not_iterate_again(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]), total=7.0)
        loop.begin()
        with self.assertRaisesRegex(ProbeError, "run's deadline of 7 seconds has passed"):
            loop.watch_job("job", 600.0)
        self.assertLessEqual(clock.now - 1000.0, 7.5)

    def test_a_sample_that_finishes_after_the_run_deadline_is_refused(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running()]), total=3.0, cost=10.0)
        loop.begin()
        with self.assertRaisesRegex(ProbeError, "a sample finished after the deadline of 3 seconds"):
            loop.tick()

    def test_a_job_exit_that_is_seen_after_the_run_deadline_is_refused(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [exited()]), total=3.0)
        loop.begin()
        original = loop.tick

        def late_tick(container: str | None = None) -> ContainerState | None:
            state = original(container)
            clock.now += 10.0
            return state

        loop.tick = late_tick  # type: ignore[method-assign]
        with self.assertRaisesRegex(ProbeError, "the Spark job's exit finished after the deadline"):
            loop.watch_job("job", 600.0)

    def test_the_spark_cgroup_peak_is_the_highest_reading_and_the_cap_and_kills_are_the_last(self) -> None:
        clock = Clock()
        readings = [CgroupMemory(100, 4096 * MIB, 0, 0), CgroupMemory(900, 4096 * MIB, 0, 1), CgroupMemory(500, 4096 * MIB, 1, 2)]
        reader = mock.Mock(side_effect=readings)
        loop = monitor(clock, FakeDocker(clock, [running(), running(), running(), exited()]), cgroup=reader)
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual((900, 4096 * MIB, 1), (loop.spark.peak_bytes, loop.spark.cap_bytes, loop.spark.oom_kills))
        self.assertEqual(3, reader.call_count, "no reading once the container has exited")

    def test_a_cgroup_that_cannot_be_read_on_some_iterations_does_not_hide_the_ones_that_could(self) -> None:
        clock = Clock()
        reader = mock.Mock(side_effect=[FiguresError("gone"), CgroupMemory(300, 4096 * MIB, 0, 0), FiguresError("gone")])
        loop = monitor(clock, FakeDocker(clock, [running(), running(), running(), exited()]), cgroup=reader)
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual(300, loop.spark.peak_bytes)

    def test_without_a_cgroup_reader_there_is_no_cgroup_figure(self) -> None:
        clock = Clock()
        loop = monitor(clock, FakeDocker(clock, [running(), exited()]))
        loop.begin()
        loop.watch_job("job", 600.0)
        self.assertEqual((None, None, None), (loop.spark.peak_bytes, loop.spark.cap_bytes, loop.spark.oom_kills))

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
        self.cleanup_result = [CleanupStep("job container", True, "no container remains"), CleanupStep("nessie branch", True, "no branch existed")]

    def step(self, name: str) -> None:
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]

    def steps(self) -> Steps:
        def start(hook: Callable[[], None]) -> None:
            self.step("start")
            hook()

        def baseline() -> Mapping[str, ServiceBaseline]:
            self.step("baseline")
            return {"postgres": ServiceBaseline(0, 0, True)}

        def finish(state: ContainerState, watch: SparkWatch) -> JobOutcome:
            self.step("finish")
            return JobOutcome(0, False, 1.0, {}, "", None)

        def evidence() -> Mapping[str, ContainerEvidence]:
            self.step("evidence")
            return {}

        def cleanup() -> list[CleanupStep]:
            self.step("cleanup")
            return self.cleanup_result

        return Steps(
            start,
            baseline,
            lambda: self.step("catalog"),
            lambda: self.step("admit"),
            lambda: self.step("start_job"),
            "job",
            600.0,
            finish,
            evidence,
            cleanup,
            {"job_catalog": "a", "cleanup_catalog": "b"},
            "scratch_x",
        )


class ExecuteTest(unittest.TestCase):
    def execute(self, fake: FakeSteps, *, states: Sequence[ContainerState] | None = None, stop: StopFlag | None = None) -> Any:
        clock = Clock()
        docker = FakeDocker(clock, states or [running(), exited()])
        return execute(fake.steps(), monitor(clock, docker, stop=stop))

    def test_a_good_run_runs_every_step_in_order_and_the_loop_ran_to_its_end(self) -> None:
        fake = FakeSteps()
        result = self.execute(fake)
        self.assertEqual(["start", "baseline", "catalog", "admit", "start_job", "finish", "evidence", "cleanup"], fake.calls)
        self.assertEqual("", result.failure)
        self.assertTrue(result.loop_complete)
        self.assertFalse(result.interrupted)
        self.assertEqual({"job_catalog": "a", "cleanup_catalog": "b"}, result.addresses)
        self.assertEqual("scratch_x", result.scratch)
        self.assertGreater(len(result.windows), 0)
        self.assertIsNotNone(result.job_exit_ns)

    def test_a_service_that_does_not_start_ends_the_run_before_the_other_steps_and_still_cleans_up(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = ProbeError("group supporting: not healthy after 300 seconds")
        result = self.execute(fake)
        self.assertEqual(["start", "cleanup"], fake.calls)
        self.assertIn("starting the always-on group failed: group supporting: not healthy", result.failure)
        self.assertFalse(result.loop_complete)

    def test_each_step_before_the_job_names_itself_when_it_fails_and_the_job_does_not_start(self) -> None:
        for step, text in (
            ("catalog", "the catalog check failed"),
            ("admit", "admission of the Spark job refused"),
            ("start_job", "the Spark job could not start"),
        ):
            with self.subTest(step=step):
                fake = FakeSteps()
                fake.fail[step] = ProbeError("x")
                result = self.execute(fake)
                self.assertIn(f"{text}: x", result.failure)
                self.assertNotIn("finish", fake.calls)
                self.assertEqual("cleanup", fake.calls[-1])

    def test_a_job_that_cannot_run_is_a_failure_with_the_cleanup_done(self) -> None:
        fake = FakeSteps()
        fake.fail["finish"] = ProbeError("reading the job's output: docker exited 1")
        result = self.execute(fake)
        self.assertIn("the Spark job could not run: reading the job's output: docker exited 1", result.failure)
        self.assertEqual("cleanup", fake.calls[-1])

    def test_a_job_that_never_ends_fails_at_the_job_deadline_and_the_cleanup_runs(self) -> None:
        fake = FakeSteps()
        result = self.execute(fake, states=[running()])
        self.assertIn("the Spark job could not run: the Spark job timed out after 600 seconds", result.failure)
        self.assertFalse(result.loop_complete)
        self.assertEqual("cleanup", fake.calls[-1])

    def test_evidence_that_cannot_be_read_is_kept_apart_from_a_failure(self) -> None:
        fake = FakeSteps()
        fake.fail["evidence"] = FiguresError("cannot read the host's memory.peak")
        result = self.execute(fake)
        self.assertEqual("", result.failure)
        self.assertIn("memory.peak", result.evidence_error)

    def test_a_signal_during_the_job_is_recorded_and_the_cleanup_runs_after_it(self) -> None:
        fake = FakeSteps()
        flag = StopFlag()
        flag.handle(signal.SIGINT, None)
        result = self.execute(fake, stop=flag)
        self.assertTrue(result.interrupted)
        self.assertNotIn("finish", fake.calls)
        self.assertNotIn("evidence", fake.calls)
        self.assertEqual("cleanup", fake.calls[-1])
        self.assertEqual("", result.failure, "the interrupt is named once, by the verdict")

    def test_a_signal_while_the_group_starts_is_an_interrupt_and_not_a_start_failure(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = Stopped("a signal came while the always-on group started")
        result = self.execute(fake)
        self.assertTrue(result.interrupted)
        self.assertEqual("", result.failure)
        self.assertEqual(["start", "cleanup"], fake.calls)

    def test_a_cleanup_that_crashes_is_a_failed_step_and_never_a_silent_one(self) -> None:
        fake = FakeSteps()
        fake.fail["cleanup"] = RuntimeError("secret body")
        result = self.execute(fake)
        self.assertEqual([CleanupStep("cleanup", False, "stopped with RuntimeError")], result.cleanup)


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
    def test_a_container_that_is_removed_and_absent_is_a_verified_step(self) -> None:
        self.assertEqual(CleanupStep("job container", True, "no container remains"), remove_container(FakeContainers(), "job"))

    def test_a_container_that_is_still_there_or_whose_look_fails_is_a_failed_step(self) -> None:
        self.assertIn("still present", remove_container(FakeContainers(still_there=True), "job").detail)
        step = remove_container(FakeContainers(remove_error=ProbeError("removing a container: docker exited 125")), "job")
        self.assertFalse(step.ok)
        self.assertIn("docker exited 125", step.detail)

    def test_a_volume_that_is_removed_and_absent_is_a_verified_step_and_one_that_is_still_there_is_not(self) -> None:
        self.assertEqual(CleanupStep("scratch volume", True, "no volume remains"), remove_scratch(FakeContainers(), "v"))
        self.assertFalse(remove_scratch(FakeContainers(volume_still_there=True), "v").ok)
        self.assertFalse(remove_scratch(FakeContainers(volume_error=ProbeError("removing the job's scratch volume: docker exited 125")), "v").ok)

    def test_a_branch_that_never_existed_is_a_verified_step_and_one_that_exists_is_deleted_and_looked_up_again(self) -> None:
        self.assertEqual(CleanupStep("nessie branch", True, "no branch existed"), remove_branch(FakeNessie(exists=False), "b"))
        client = FakeNessie()
        self.assertEqual(CleanupStep("nessie branch", True, "removed and absent"), remove_branch(client, "b"))
        self.assertEqual(["b"], client.deleted)
        self.assertIn("still present", remove_branch(FakeNessie(lookups=[True, True, True]), "b").detail)

    def test_a_refused_deletion_holds_the_status_and_never_the_response_text(self) -> None:
        error = NessieError("DELETE /api/v2/trees/x was refused with status 500, error code X: SECRET BODY", status=500)
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
    loop = monitor(clock, FakeDocker(clock, [running()]), total=budget)
    loop.run_start_ns = clock.ns()
    return Probe(
        Settings(compose_directory=Path("/stack/compose"), project="p"),
        parse_limits(LIMITS_TEXT),
        Options(host_figures=host, rows=10),
        docker=docker,
        budget=Budget(budget, clock=clock.monotonic),
        monitor=loop,
        arguments=["--branch", "capacity_probe_x", "--rows", "10"],
        container="p_capacity_probe_x",
        scratch="p_capacity_probe_scratch_x",
        client=client or FakeNessie(),
        runner=runner or compose_runner(),
    )


class CleanupOrderTest(unittest.TestCase):
    def cleanup(self, docker: FakeContainers, client: FakeNessie, *, started: bool = True, scratch: bool = True) -> tuple[list[CleanupStep], FakeContainers]:
        probe = probe_for(Clock(), docker, client=client)
        probe.job_started, probe.scratch_made = started, scratch
        return probe.cleanup(docker), docker

    def test_the_container_is_removed_and_verified_first_then_the_volume_then_the_branch(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie())
        self.assertEqual(["job container", "scratch volume", "nessie branch"], [step.name for step in steps])
        self.assertTrue(all(step.ok for step in steps))
        self.assertEqual(["remove container", "look for container", "remove volume", "look for volume"], docker.events_log)

    def test_a_container_that_is_still_there_blocks_the_volume_and_the_branch_which_are_then_not_tried(self) -> None:
        client = FakeNessie()
        steps, docker = self.cleanup(FakeContainers(still_there=True), client)
        self.assertEqual([False, False, False], [step.ok for step in steps])
        self.assertIn("not tried", steps[1].detail)
        self.assertIn("not tried", steps[2].detail)
        self.assertEqual([], client.deleted)
        self.assertEqual(["remove container", "look for container"], docker.events_log)

    def test_a_container_removal_that_crashes_blocks_the_others_and_is_reported(self) -> None:
        steps, _ = self.cleanup(FakeContainers(remove_error=RuntimeError("secret")), FakeNessie())
        self.assertEqual("stopped with RuntimeError", steps[0].detail)
        self.assertFalse(steps[2].ok)

    def test_a_malformed_nessie_answer_fails_the_branch_step_only_and_the_container_and_volume_are_done_first(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie(lookups=[KeyError("garbled")]))
        self.assertEqual([True, True, False], [step.ok for step in steps])
        self.assertEqual("stopped with KeyError", steps[2].detail)
        self.assertEqual(4, len(docker.events_log))

    def test_a_job_that_never_started_has_nothing_to_remove_but_the_container_is_still_looked_for(self) -> None:
        client = FakeNessie(lookups=[AssertionError("Nessie was asked")])
        steps, docker = self.cleanup(FakeContainers(), client, started=False, scratch=False)
        self.assertEqual([True, True, True], [step.ok for step in steps])
        self.assertEqual("the job did not start", steps[2].detail)
        self.assertEqual(["remove container", "look for container"], docker.events_log)

    def test_a_scratch_volume_that_was_made_is_removed_and_verified(self) -> None:
        steps, docker = self.cleanup(FakeContainers(), FakeNessie(exists=False))
        self.assertIn("remove volume", docker.events_log)
        self.assertTrue(steps[1].ok)


class ProbeStepTest(unittest.TestCase):
    def test_the_job_starts_detached_with_its_own_name_and_its_own_scratch_volume_made_first_and_is_not_removed_by_compose(self) -> None:
        runner = compose_runner()
        created: list[str] = []

        class Volumes(FakeContainers):
            def create_volume(self, name: str, project: str) -> None:
                created.append(f"{name} {project}")

        probe = probe_for(Clock(), Volumes(), runner=runner)
        probe.start_job()
        call = runner.calls[0]  # type: ignore[attr-defined]
        for word in ("--detach", "--no-deps", "run", "--name", "p_capacity_probe_x", "spark", "--rows", "10"):
            self.assertIn(word, call)
        self.assertEqual("p_capacity_probe_scratch_x:/scratch", call[call.index("--volume") + 1])
        self.assertLess(call.index("--volume"), call.index("spark"))
        self.assertNotIn("--rm", call)
        self.assertEqual(["p_capacity_probe_scratch_x p"], created)
        self.assertTrue(probe.job_started and probe.scratch_made)

    def test_a_job_whose_volume_cannot_be_made_is_marked_started_so_that_the_cleanup_looks_for_everything(self) -> None:
        class Broken(FakeContainers):
            def create_volume(self, name: str, project: str) -> None:
                raise ProbeError("creating the job's scratch volume: docker exited 1")

        probe = probe_for(Clock(), Broken())
        with self.assertRaisesRegex(ProbeError, "scratch volume: docker exited 1"):
            probe.start_job()
        self.assertTrue(probe.job_started)
        self.assertFalse(probe.scratch_made)

    def test_a_job_that_cannot_be_started_names_the_step_and_the_status(self) -> None:
        with self.assertRaisesRegex(ProbeError, "starting the Spark job: docker exited 125"):
            probe_for(Clock(), FakeContainers(), runner=compose_runner(125)).start_job()

    def test_a_run_with_no_time_left_does_not_start_the_job(self) -> None:
        runner = compose_runner()
        probe = probe_for(Clock(), FakeContainers(), runner=runner, budget=0.0)

        class Volumes(FakeContainers):
            def create_volume(self, name: str, project: str) -> None:
                return None

        probe.docker = Volumes()
        with self.assertRaisesRegex(ProbeError, "deadline of 0 seconds has passed"):
            probe.start_job()
        self.assertEqual([], runner.calls)  # type: ignore[attr-defined]

    def test_the_job_outcome_has_the_exit_status_the_kill_flag_the_cap_and_peak_of_the_loop_and_a_validated_result(self) -> None:
        class Logs(FakeContainers):
            def logs(self, container: str) -> str:
                return RESULT_LINE

        probe = probe_for(Clock(), Logs())
        watch = SparkWatch(peak_bytes=900 * MIB, cap_bytes=4096 * MIB, oom_kills=0)
        outcome = probe.finish_job(exited(137, oom=True), watch)
        if outcome.evidence is None:
            self.fail("no evidence")
        self.assertEqual((137, True, ""), (outcome.exit_code, outcome.oom_killed, outcome.result_error))
        self.assertEqual(
            (4096 * MIB, 900 * MIB, True, "cgroup"),
            (outcome.evidence.cap_bytes, outcome.evidence.peak_bytes, outcome.evidence.oom_killed, outcome.evidence.source),
        )
        self.assertNotIn("extra", outcome.result)

    def test_on_another_computer_the_cap_comes_from_docker_and_a_result_that_is_not_the_requested_one_is_named(self) -> None:
        class Logs(FakeContainers):
            def logs(self, container: str) -> str:
                return RESULT_LINE.replace('"rows": 10', '"rows": 9')

        outcome = probe_for(Clock(), Logs(), host=False).finish_job(exited(), SparkWatch())
        if outcome.evidence is None:
            self.fail("no evidence")
        self.assertEqual((4096 * MIB, None, "docker"), (outcome.evidence.cap_bytes, outcome.evidence.peak_bytes, outcome.evidence.source))
        self.assertIn("wrote 9 rows and 10 were requested", outcome.result_error)

    def test_the_baseline_counts_restarts_and_kills_and_says_whether_each_container_started_before_the_run(self) -> None:
        clock = Clock()

        class Services(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                started = clock.now - 100 if "postgres" in container else clock.now + 5
                return ContainerState("running", 0, False, 7, 2, 1024 * MIB, started)

        probe = probe_for(clock, Services())
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=CgroupMemory(1, 1024 * MIB, 3, 0)):
            found = probe.baseline()
        self.assertEqual(ServiceBaseline(2, 3, True), found["postgres"])
        self.assertEqual(ServiceBaseline(2, 3, False), found["nessie"])

    def test_the_baseline_on_another_computer_has_no_kill_count(self) -> None:
        class Services(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                return ContainerState("running", 0, False, 7, 0, 1024 * MIB, 0.0)

        found = probe_for(Clock(), Services(), host=False).baseline()
        self.assertEqual(ServiceBaseline(0, None, None), found["postgres"])

    def test_the_end_evidence_comes_from_the_cgroup_on_the_host_and_from_docker_elsewhere(self) -> None:
        class Services(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]

            def state(self, container: str) -> ContainerState:
                return ContainerState("running", 0, False, 7, 0, 1024 * MIB, 0.0)

        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=CgroupMemory(300, 1024 * MIB, 2, 5)):
            host = probe_for(Clock(), Services()).final_evidence()
        self.assertEqual((1024 * MIB, 300, 2, "cgroup"), (host["nessie"].cap_bytes, host["nessie"].peak_bytes, host["nessie"].oom_kills, host["nessie"].source))
        local = probe_for(Clock(), Services(), host=False).final_evidence()
        self.assertEqual((1024 * MIB, None, "docker"), (local["postgres"].cap_bytes, local["postgres"].peak_bytes, local["postgres"].source))

    def test_a_service_that_is_not_running_at_the_end_is_recorded_as_not_running(self) -> None:
        class Empty(FakeContainers):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return []

        self.assertFalse(probe_for(Clock(), Empty()).final_evidence()["postgres"].running)

    def test_the_catalog_check_asks_the_one_catalog_client_and_names_the_status_only(self) -> None:
        client = FakeNessie()
        probe_for(Clock(), FakeContainers(), client=client).check_catalog()
        self.assertEqual(1, client.configured)

        class Down(FakeNessie):
            def config(self) -> dict[str, str]:
                raise NessieError("GET /config was refused with status 503: SECRET BODY", status=503)

        with self.assertRaisesRegex(ProbeError, "^Nessie answered with status 503$"):
            probe_for(Clock(), FakeContainers(), client=Down()).check_catalog()


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

    def test_a_job_address_that_is_not_the_projects_nessie_service_is_refused_before_anything_starts(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder))
            for environ in ({"NESSIE_URI": "http://10.255.255.1:19120/api/v2"}, {"NESSIE_URI": "http://nessie:19121/api/v2"}):
                with self.subTest(environ=environ), self.assertRaisesRegex(ProbeError, "cleanup cannot be resolved"):
                    resolve_nessie(settings, environ)

    def test_a_job_address_in_the_host_file_is_judged_the_same_way(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder), "NESSIE_URI=http://elsewhere:19120/api/v2\n")
            with self.assertRaisesRegex(ProbeError, "cleanup cannot be resolved"):
                resolve_nessie(settings, {})

    def test_a_port_that_is_not_a_port_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            settings = self.settings(Path(folder))
            for port in ("abc", "0", "70000", "-1"):
                with self.subTest(port=port), self.assertRaisesRegex(ProbeError, "not a port number"):
                    resolve_nessie(settings, {"NESSIE_HOST_PORT": port})

    def test_service_files_that_cannot_be_read_or_have_no_default_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ProbeError, "cannot read the Nessie settings"):
                resolve_nessie(Settings(compose_directory=Path(folder), project="p"), {})
            settings = self.settings(Path(folder))
            (Path(folder) / "services" / "nessie.yml").write_text("ports: []\n")
            with self.assertRaisesRegex(ProbeError, "no default"):
                resolve_nessie(settings, {})

    def test_the_repositorys_own_service_files_give_the_default_nessie(self) -> None:
        target = resolve_nessie(Settings(compose_directory=ROOT / "compose", project="p"), {})
        self.assertEqual("http://127.0.0.1:19120/api/v2", target.cleanup_uri)


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
            write_document(Path(folder), {"verdict": "x"})
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                preflight(Never(), "p", Path(folder))

    def test_a_project_with_no_spark_container_and_a_good_folder_passes(self) -> None:
        class Empty(Docker):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return []

        with tempfile.TemporaryDirectory() as folder:
            preflight(Empty(), "p", Path(folder) / "new")


class ArgumentTest(unittest.TestCase):
    def run(self, result: unittest.TestResult | None = None) -> unittest.TestResult | None:
        with contextlib.redirect_stderr(io.StringIO()):
            return super().run(result)

    def test_every_duration_must_be_finite_and_above_zero(self) -> None:
        for name in ("--interval-seconds", "--start-deadline-seconds", "--job-deadline-seconds", "--wait-seconds"):
            for value in ("nan", "inf", "-inf", "0", "-1", "abc", ""):
                with self.subTest(name=name, value=value), self.assertRaises(SystemExit):
                    parser().parse_args([name, value])

    def test_a_good_duration_is_accepted_and_the_wait_defaults_to_none(self) -> None:
        arguments = parser().parse_args(["--interval-seconds", "2.5", "--wait-seconds", "30"])
        self.assertEqual((2.5, 30.0), (arguments.interval_seconds, arguments.wait_seconds))
        self.assertIsNone(parser().parse_args([]).wait_seconds)

    def test_the_memory_override_is_refused_unless_this_is_not_the_linux_host(self) -> None:
        with self.assertRaises(SystemExit) as refused:
            main(["--available-mib", "30000"])
        self.assertEqual(2, refused.exception.code)

    def test_the_memory_override_is_accepted_with_the_local_flag(self) -> None:
        arguments = parser().parse_args(["--available-mib", "30000", "--docker-figures-only"])
        self.assertEqual(30000, arguments.available_mib)

    def test_the_nessie_port_is_no_longer_an_option_because_it_is_resolved_from_the_compose_settings(self) -> None:
        with self.assertRaises(SystemExit):
            parser().parse_args(["--nessie-port", "19120"])

    def test_the_row_count_must_be_a_whole_number_of_at_least_one(self) -> None:
        for value in ("0", "-5", "1.5", "x"):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                parser().parse_args(["--rows", value])


class ResultTest(unittest.TestCase):
    def test_the_result_line_of_the_job_is_read_and_a_missing_or_broken_line_gives_an_empty_result(self) -> None:
        self.assertEqual({"rows": 10}, parse_job_result('WARN something\nRESULT {"rows": 10}\n'))
        self.assertEqual({}, parse_job_result("no result here\n"))
        self.assertEqual({}, parse_job_result("RESULT {broken\n"))
        self.assertEqual({}, parse_job_result("RESULT [1, 2]\n"))

    def test_the_job_arguments_name_a_branch_of_this_run_and_the_fixed_row_count(self) -> None:
        self.assertEqual(["--branch", "capacity_probe_20261122t100000z", "--rows", "2000000"], job_arguments("20261122T100000Z", 2000000))


if __name__ == "__main__":
    unittest.main()
