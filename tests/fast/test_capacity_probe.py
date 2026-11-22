"""The capacity probe samples on one run deadline, confirms that its readers stopped, watches the job to its end, cleans up with proof and refuses bad input.

Each case changes one fact of a good setup and expects the result to follow from that fact alone. Time is a fake clock where a case depends on it.
"""

from __future__ import annotations

import contextlib
import io
import tempfile
import threading
import time
import unittest
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from unittest import mock

from scripts.process import CompletedProcess
from scripts.stack.capacity_probe import (
    BackgroundSampler,
    Budget,
    EventsReader,
    Options,
    Probe,
    Steps,
    collect,
    execute,
    job_arguments,
    parse_job_result,
    parser,
    preflight,
    remove_branch,
    remove_container,
)
from scripts.stack.groups import Settings
from scripts.stack.host_figures import CgroupMemory, FiguresError
from scripts.stack.limits import parse_limits
from scripts.stack.nessie import NessieError, Reference
from scripts.stack.probe_docker import ContainerEvent, ContainerInfo, ContainerState, Docker, ProbeError, SamplingError
from scripts.stack.probe_record import CleanupStep, ContainerEvidence, JobOutcome, Sample, write_document

MIB = 1048576
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


def sample(seconds: float, phase: str = "overlap") -> Sample:
    return Sample(seconds, f"2026-11-22T10:00:{int(seconds):02d}Z", phase, None, {"nessie": MIB})


class FakeTime:
    """A clock that only moves when a wait or a step moves it."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def wait(self, seconds: float) -> bool:
        self.now += seconds
        return False


def wait_for(condition: Callable[[], bool], seconds: float = 5.0) -> None:
    stop = threading.Event()
    for _ in range(round(seconds / 0.01)):
        if condition():
            return
        stop.wait(0.01)
    raise AssertionError(f"the state was not reached within {seconds:g} seconds")


class BudgetTest(unittest.TestCase):
    def test_a_wait_is_cut_to_the_time_that_is_left_and_a_run_with_none_left_fails(self) -> None:
        time_now = FakeTime()
        budget = Budget(10.0, clock=time_now.clock)
        self.assertEqual(4.0, budget.limit(4.0))
        time_now.now = 8.0
        self.assertEqual(2.0, budget.limit(30.0))
        self.assertEqual(2.0, budget.remaining())
        time_now.now = 10.0
        with self.assertRaisesRegex(ProbeError, "run's deadline of 10 seconds has passed"):
            budget.limit(1.0)


class CollectTest(unittest.TestCase):
    def take(self, time_now: FakeTime, taken: list[float]) -> Callable[[], Sample]:
        def inner() -> Sample:
            if len(taken) > 100:
                raise AssertionError("the sampling never ended")
            taken.append(time_now.now)
            return sample(time_now.now)

        return inner

    def run_collect(self, take: Callable[[], Sample], time_now: FakeTime, **changes: object) -> list[Sample]:
        samples: list[Sample] = []
        arguments: dict[str, object] = {
            "interval_seconds": 5,
            "deadline_seconds": 100,
            "stopped": lambda: False,
            "wait": time_now.wait,
            "clock": time_now.clock,
        }
        collect(take, samples=samples, **{**arguments, **changes})  # type: ignore[arg-type]
        return samples

    def test_samples_are_taken_at_the_interval_until_the_stop_and_one_more_after_it(self) -> None:
        time_now, taken = FakeTime(), list[float]()

        def wait(seconds: float) -> bool:
            time_now.wait(seconds)
            return time_now.now >= 12.0

        self.run_collect(self.take(time_now, taken), time_now, wait=wait)
        self.assertEqual([0.0, 5.0, 10.0, 15.0], taken)

    def test_the_time_a_sample_takes_is_taken_from_the_wait_so_the_interval_stays_fixed(self) -> None:
        time_now, taken, waited = FakeTime(), list[float](), list[float]()

        def slow_take() -> Sample:
            taken.append(time_now.now)
            time_now.now += 2.0
            return sample(time_now.now)

        def wait(seconds: float) -> bool:
            waited.append(seconds)
            time_now.wait(seconds)
            return len(taken) >= 3

        self.run_collect(slow_take, time_now, wait=wait)
        self.assertEqual([3.0, 3.0, 3.0], waited)

    def test_a_sample_that_takes_longer_than_the_interval_is_followed_at_once(self) -> None:
        time_now, waited = FakeTime(), list[float]()

        def slow_take() -> Sample:
            time_now.now += 9.0
            return sample(time_now.now)

        def wait(seconds: float) -> bool:
            waited.append(seconds)
            return len(waited) >= 2

        self.run_collect(slow_take, time_now, wait=wait)
        self.assertEqual([0.0, 0.0], waited)

    def test_a_stop_before_the_first_sample_still_leaves_one_sample(self) -> None:
        time_now = FakeTime()
        self.assertEqual(1, len(self.run_collect(self.take(time_now, []), time_now, stopped=lambda: True)))

    def test_the_first_sample_follows_a_wait_of_the_first_delay(self) -> None:
        time_now, taken = FakeTime(), list[float]()
        samples = self.run_collect(self.take(time_now, taken), time_now, first_delay_seconds=1.0, stopped=lambda: True)
        self.assertEqual([1.0], taken)
        self.assertEqual(1, len(samples))

    def test_a_stop_during_the_first_delay_still_leaves_one_sample(self) -> None:
        time_now, taken = FakeTime(), list[float]()

        def stop_at_once(seconds: float) -> bool:
            time_now.wait(seconds)
            return True

        self.assertEqual(1, len(self.run_collect(self.take(time_now, taken), time_now, first_delay_seconds=1.0, wait=stop_at_once)))
        self.assertEqual([1.0], taken)

    def test_an_early_wake_takes_the_next_sample_at_once_and_keeps_the_interval_after_it(self) -> None:
        time_now, taken, waited = FakeTime(), list[float](), list[float]()

        def wait(seconds: float) -> bool:
            waited.append(seconds)
            time_now.now += 1.0
            return len(waited) >= 2

        self.run_collect(self.take(time_now, taken), time_now, wait=wait)
        self.assertEqual([0.0, 1.0, 2.0], taken)
        self.assertEqual([5.0, 5.0], waited)

    def test_the_deadline_stops_the_sampling_with_an_error_that_keeps_the_samples_taken(self) -> None:
        time_now, taken, samples = FakeTime(), list[float](), list[Sample]()
        with self.assertRaisesRegex(SamplingError, "deadline of 20 seconds.*after 4 samples"):
            collect(
                self.take(time_now, taken),
                samples=samples,
                interval_seconds=5,
                deadline_seconds=20,
                stopped=lambda: False,
                wait=time_now.wait,
                clock=time_now.clock,
            )
        self.assertEqual(4, len(samples))

    def test_a_sample_that_finishes_after_the_deadline_is_refused_and_not_kept(self) -> None:
        time_now, samples = FakeTime(), list[Sample]()

        def slow() -> Sample:
            time_now.now += 30.0
            return sample(time_now.now)

        with self.assertRaisesRegex(SamplingError, "deadline of 5 seconds passed after 0 samples"):
            collect(slow, samples=samples, interval_seconds=5, deadline_seconds=5, stopped=lambda: True, wait=lambda seconds: True, clock=time_now.clock)
        self.assertEqual([], samples)

    def test_a_final_sample_that_finishes_after_the_deadline_is_refused_too(self) -> None:
        time_now, samples = FakeTime(), list[Sample]()
        calls = [0]

        def take() -> Sample:
            calls[0] += 1
            time_now.now += 1.0 if calls[0] == 1 else 30.0
            return sample(time_now.now)

        with self.assertRaisesRegex(SamplingError, "deadline of 10 seconds passed after 1 samples"):
            collect(take, samples=samples, interval_seconds=1, deadline_seconds=10, stopped=lambda: False, wait=lambda seconds: True, clock=time_now.clock)
        self.assertEqual(1, len(samples))

    def test_a_reading_that_fails_ends_the_sampling_and_names_the_sample_number_and_the_cause(self) -> None:
        time_now, samples, calls = FakeTime(), list[Sample](), [0]

        def broken() -> Sample:
            calls[0] += 1
            if calls[0] == 3:
                raise FiguresError("cannot read the host's meminfo")
            return sample(time_now.now)

        with self.assertRaisesRegex(SamplingError, "sample 3 failed: cannot read the host's meminfo"):
            collect(broken, samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: False, wait=time_now.wait, clock=time_now.clock)
        self.assertEqual(2, len(samples))


class BackgroundSamplerTest(unittest.TestCase):
    def sampler(self, take: Callable[[], Sample], *, interval: float = 0.01, deadline: float = 30.0, join: float = 5.0) -> BackgroundSampler:
        return BackgroundSampler(take, interval_seconds=interval, deadline_seconds=deadline, join_seconds=join)

    def test_the_samples_carry_the_phase_that_was_set_when_each_was_taken(self) -> None:
        sampler = self.sampler(lambda: sample(0.0, phase=sampler.phase))
        sampler.phase = "startup"
        sampler.start()
        wait_for(lambda: len(sampler.samples) >= 2)
        sampler.phase = "overlap"
        count = len(sampler.samples)
        wait_for(lambda: len(sampler.samples) >= count + 2)
        sampler.stop()
        phases = [item.phase for item in sampler.samples]
        self.assertEqual(("startup", "overlap"), (phases[0], phases[-1]))
        self.assertEqual("", sampler.error)

    def test_a_normal_stop_is_confirmed_and_a_sampler_that_was_never_asked_to_stop_is_not(self) -> None:
        sampler = self.sampler(lambda: sample(0.0))
        sampler.start()
        wait_for(lambda: len(sampler.samples) >= 1)
        self.assertFalse(sampler.confirmed)
        sampler.stop()
        self.assertTrue(sampler.confirmed)

    def test_a_failing_reading_is_kept_as_the_error_the_samples_before_it_stay_and_the_stop_is_not_confirmed(self) -> None:
        calls = [0]

        def broken() -> Sample:
            calls[0] += 1
            if calls[0] == 3:
                raise FiguresError("no MemAvailable line")
            return sample(float(calls[0]))

        sampler = self.sampler(broken)
        sampler.start()
        wait_for(lambda: bool(sampler.error))
        sampler.stop()
        self.assertIn("no MemAvailable line", sampler.error)
        self.assertEqual(2, len(sampler.samples))
        self.assertFalse(sampler.confirmed)

    def test_a_crash_of_any_type_is_kept_as_the_error_and_is_never_a_silent_end(self) -> None:
        def crash() -> Sample:
            raise KeyError("boom")

        sampler = self.sampler(crash)
        sampler.start()
        wait_for(lambda: bool(sampler.error))
        sampler.stop()
        self.assertIn("sampler stopped with KeyError", sampler.error)
        self.assertFalse(sampler.confirmed)

    def test_a_reading_that_never_returns_fails_the_stop_at_its_join_deadline(self) -> None:
        release = threading.Event()
        sampler = self.sampler(lambda: sample(0.0) if release.wait(10) else sample(-1.0), join=0.2)
        sampler.start()
        try:
            with self.assertRaisesRegex(SamplingError, "sampler did not stop within 0.2 seconds"):
                sampler.stop()
            self.assertFalse(sampler.confirmed)
        finally:
            release.set()

    def test_a_request_for_a_sample_takes_one_at_once_and_does_not_wait_for_the_interval(self) -> None:
        sampler = self.sampler(lambda: sample(0.0), interval=60.0)
        sampler.start()
        wait_for(lambda: len(sampler.samples) >= 1)
        sampler.sample_now()
        wait_for(lambda: len(sampler.samples) >= 2, seconds=3.0)
        sampler.stop()
        self.assertTrue(sampler.confirmed)

    def test_a_deadline_that_passes_is_an_error_and_not_a_confirmed_stop(self) -> None:
        sampler = self.sampler(lambda: sample(0.0), deadline=0.05)
        sampler.start()
        wait_for(lambda: bool(sampler.error))
        sampler.stop()
        self.assertIn("sampling deadline of 0.05 seconds", sampler.error)
        self.assertFalse(sampler.confirmed)


class FakeEvents(Docker):
    def __init__(self, answers: Sequence[list[ContainerEvent] | BaseException], now: list[int] | None = None) -> None:
        super().__init__()
        self.answers = list(answers)
        self.windows: list[tuple[int, int]] = []
        self.now = now

    def events(self, project: str, since_nano: int, until_nano: int, *, seconds: float) -> list[ContainerEvent]:
        self.windows.append((since_nano, until_nano))
        if self.now is not None:
            self.now[0] = max(self.now[0], until_nano)
        time.sleep(0.002)
        answer = self.answers.pop(0) if self.answers else []
        if isinstance(answer, BaseException):
            raise answer
        return answer


class EventsReaderTest(unittest.TestCase):
    def reader(self, docker: FakeEvents, clock: Callable[[], int]) -> EventsReader:
        return EventsReader(docker, "p", join_seconds=5.0, slice_seconds=1.0, clock_ns=clock)

    def test_the_windows_are_contiguous_from_the_start_and_the_last_ends_at_the_present_after_the_stop(self) -> None:
        now = [1_000_000_000_000]
        docker = FakeEvents([], now)
        reader = self.reader(docker, lambda: now[0])
        reader.start()
        wait_for(lambda: len(docker.windows) >= 3)
        now[0] += 400_000_000
        reader.stop()
        windows = docker.windows
        self.assertEqual(1_000_000_000_000, windows[0][0])
        for earlier, later in zip(windows, windows[1:], strict=False):
            self.assertEqual(earlier[1], later[0])
        self.assertEqual(now[0], windows[-1][1])
        self.assertTrue(reader.confirmed)

    def test_the_events_of_every_window_are_kept_once(self) -> None:
        die = ContainerEvent(5, "die", "nessie", "p-nessie-1", "exitCode 137")
        docker = FakeEvents([[die], [die, ContainerEvent(9, "oom", "nessie", "p-nessie-1", "")]])
        reader = self.reader(docker, lambda: 1_000_000_000_000)
        reader.start()
        wait_for(lambda: len(docker.windows) >= 3)
        reader.stop()
        self.assertEqual(["die", "oom"], [event.action for event in reader.events])

    def test_a_docker_failure_is_kept_as_the_error_and_the_stop_is_not_confirmed(self) -> None:
        docker = FakeEvents([ProbeError("reading the container events: docker exited 1")])
        reader = self.reader(docker, lambda: 1_000_000_000_000)
        reader.start()
        wait_for(lambda: bool(reader.error))
        reader.stop()
        self.assertIn("docker exited 1", reader.error)
        self.assertFalse(reader.confirmed)

    def test_a_crash_of_any_type_is_kept_as_the_error(self) -> None:
        reader = self.reader(FakeEvents([ValueError("boom")]), lambda: 1_000_000_000_000)
        reader.start()
        wait_for(lambda: bool(reader.error))
        reader.stop()
        self.assertIn("event reader stopped with ValueError", reader.error)
        self.assertFalse(reader.confirmed)


class FakeSteps:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail: dict[str, BaseException] = {}
        self.cleanup_result: list[CleanupStep] = [
            CleanupStep("job container", True, "no container remains"),
            CleanupStep("nessie branch", True, "no branch existed"),
        ]

    def step(self, name: str) -> None:
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]

    def steps(self) -> Steps:
        def start() -> None:
            self.step("start")

        def baseline() -> Mapping[str, int]:
            self.step("baseline")
            return {"postgres": 0}

        def admit() -> None:
            self.step("admit")

        def run(on_running: Callable[[], None]) -> JobOutcome:
            self.step("run")
            on_running()
            return JobOutcome(0, False, 1.0, {}, "", None)

        def evidence() -> Mapping[str, ContainerEvidence]:
            self.step("evidence")
            return {}

        def cleanup() -> list[CleanupStep]:
            self.step("cleanup")
            return self.cleanup_result

        return Steps(start, baseline, admit, run, evidence, cleanup)


class ExecuteTest(unittest.TestCase):
    def execute(
        self, fake: FakeSteps, *, sampler_take: Callable[[], Sample] | None = None, events: FakeEvents | None = None
    ) -> tuple[object, BackgroundSampler, EventsReader]:
        sampler = BackgroundSampler(sampler_take or (lambda: sample(0.0, phase=sampler.phase)), interval_seconds=0.01, deadline_seconds=30, join_seconds=5.0)
        reader = EventsReader(events or FakeEvents([]), "p", join_seconds=5.0, slice_seconds=1.0, clock_ns=lambda: 1_000_000_000_000)
        return execute(fake.steps(), sampler, reader), sampler, reader

    def test_a_good_run_runs_every_step_in_order_confirms_both_stops_and_labels_the_phases(self) -> None:
        fake = FakeSteps()
        result, sampler, reader = self.execute(fake)
        self.assertEqual(["start", "baseline", "admit", "run", "evidence", "cleanup"], fake.calls)
        self.assertEqual("", result.failure)  # type: ignore[attr-defined]
        self.assertTrue(result.sampler_confirmed and result.events_confirmed)  # type: ignore[attr-defined]
        self.assertTrue(sampler.confirmed and reader.confirmed)
        self.assertEqual({"startup", "overlap"}, {item.phase for item in result.samples} | {"startup", "overlap"})  # type: ignore[attr-defined]
        self.assertEqual(2, len(result.cleanup))  # type: ignore[attr-defined]

    def test_the_sampler_is_asked_for_a_sample_when_the_job_is_seen_running(self) -> None:
        fake = FakeSteps()
        sampler = BackgroundSampler(lambda: sample(0.0), interval_seconds=60.0, deadline_seconds=30, join_seconds=5.0)
        reader = EventsReader(FakeEvents([]), "p", join_seconds=5.0, slice_seconds=1.0, clock_ns=lambda: 1_000_000_000_000)
        with mock.patch.object(sampler, "sample_now", wraps=sampler.sample_now) as called:
            execute(fake.steps(), sampler, reader)
        called.assert_called_once()

    def test_a_service_that_does_not_start_ends_the_run_before_admission_and_the_job_and_names_the_step_but_still_cleans_up(self) -> None:
        fake = FakeSteps()
        fake.fail["start"] = ProbeError("group supporting: not healthy after 300 seconds")
        result, _, _ = self.execute(fake)
        self.assertEqual(["start", "cleanup"], fake.calls)
        self.assertIn("starting the always-on group failed: group supporting: not healthy", result.failure)  # type: ignore[attr-defined]
        self.assertIsNone(result.outcome)  # type: ignore[attr-defined]

    def test_a_job_that_does_not_fit_waits_and_never_starts(self) -> None:
        fake = FakeSteps()
        fake.fail["admit"] = ProbeError("after waiting 0 seconds: group batch waits: 3000 MiB short")
        result, _, _ = self.execute(fake)
        self.assertNotIn("run", fake.calls)
        self.assertIn("admission of the Spark job refused", result.failure)  # type: ignore[attr-defined]

    def test_a_job_that_cannot_run_is_a_failure_with_the_cleanup_done(self) -> None:
        fake = FakeSteps()
        fake.fail["run"] = ProbeError("the Spark job timed out after 5 seconds")
        result, _, _ = self.execute(fake)
        self.assertIn("the Spark job could not run: the Spark job timed out", result.failure)  # type: ignore[attr-defined]
        self.assertEqual("cleanup", fake.calls[-1])

    def test_evidence_that_cannot_be_read_is_kept_apart_from_a_failure(self) -> None:
        fake = FakeSteps()
        fake.fail["evidence"] = FiguresError("cannot read the host's memory.peak")
        result, _, _ = self.execute(fake)
        self.assertEqual("", result.failure)  # type: ignore[attr-defined]
        self.assertIn("memory.peak", result.evidence_error)  # type: ignore[attr-defined]

    def test_an_interrupt_is_recorded_and_the_cleanup_runs_after_it(self) -> None:
        fake = FakeSteps()
        fake.fail["run"] = KeyboardInterrupt()
        result, sampler, reader = self.execute(fake)
        self.assertTrue(result.interrupted)  # type: ignore[attr-defined]
        self.assertEqual("", result.failure, "the interrupt is named once, by the verdict")  # type: ignore[attr-defined]
        self.assertEqual("cleanup", fake.calls[-1])
        self.assertTrue(sampler.confirmed and reader.confirmed)

    def test_a_sampler_that_crashed_is_an_error_even_when_the_run_went_well(self) -> None:
        def crash() -> Sample:
            raise KeyError("boom")

        fake = FakeSteps()
        result, _, _ = self.execute(fake, sampler_take=crash)
        self.assertFalse(result.sampler_confirmed)  # type: ignore[attr-defined]
        self.assertIn("KeyError", result.sampler_error)  # type: ignore[attr-defined]

    def test_an_event_reader_that_failed_is_an_error(self) -> None:
        result, _, _ = self.execute(FakeSteps(), events=FakeEvents([ProbeError("reading the container events: docker exited 1")]))
        self.assertFalse(result.events_confirmed)  # type: ignore[attr-defined]
        self.assertIn("docker exited 1", result.events_error)  # type: ignore[attr-defined]

    def test_a_cleanup_that_crashes_is_a_failed_step_and_never_a_silent_one(self) -> None:
        fake = FakeSteps()
        fake.fail["cleanup"] = RuntimeError("secret body")
        result, _, _ = self.execute(fake)
        self.assertEqual([CleanupStep("cleanup", False, "stopped with RuntimeError")], result.cleanup)  # type: ignore[attr-defined]


class FakeNessie:
    def __init__(self, *, exists: bool = True, delete_error: NessieError | None = None, lookups: Sequence[NessieError | bool] | None = None) -> None:
        self.exists = exists
        self.delete_error = delete_error
        self.lookups = list(lookups) if lookups is not None else None
        self.deleted: list[str] = []

    def reference(self, name: str) -> Reference:
        if self.lookups is not None:
            answer = self.lookups.pop(0)
            if isinstance(answer, NessieError):
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
    def __init__(self, *, remove_error: ProbeError | None = None, still_there: bool = False, exists_error: ProbeError | None = None) -> None:
        super().__init__()
        self.remove_error, self.still_there, self.exists_error = remove_error, still_there, exists_error
        self.removed: list[str] = []

    def remove(self, container: str) -> None:
        if self.remove_error is not None:
            raise self.remove_error
        self.removed.append(container)

    def exists(self, container: str) -> bool:
        if self.exists_error is not None:
            raise self.exists_error
        return self.still_there


class CleanupTest(unittest.TestCase):
    def test_a_container_that_is_removed_and_absent_is_a_verified_step(self) -> None:
        docker = FakeContainers()
        self.assertEqual(CleanupStep("job container", True, "no container remains"), remove_container(docker, "job"))
        self.assertEqual(["job"], docker.removed)

    def test_a_container_that_is_still_there_after_its_removal_is_a_failed_step(self) -> None:
        step = remove_container(FakeContainers(still_there=True), "job")
        self.assertFalse(step.ok)
        self.assertIn("still present", step.detail)

    def test_a_removal_that_fails_or_a_look_that_cannot_be_read_is_a_failed_step_with_the_status_only(self) -> None:
        for docker in (
            FakeContainers(remove_error=ProbeError("removing a container: docker exited 125")),
            FakeContainers(exists_error=ProbeError("looking for a container: docker exited 1")),
        ):
            step = remove_container(docker, "job")
            self.assertFalse(step.ok)
            self.assertIn("docker exited", step.detail)

    def test_a_branch_that_never_existed_is_a_verified_step(self) -> None:
        client = FakeNessie(exists=False)
        self.assertEqual(CleanupStep("nessie branch", True, "no branch existed"), remove_branch(client, "capacity_probe_x"))
        self.assertEqual([], client.deleted)

    def test_a_branch_that_exists_is_deleted_and_looked_up_again(self) -> None:
        client = FakeNessie()
        self.assertEqual(CleanupStep("nessie branch", True, "removed and absent"), remove_branch(client, "capacity_probe_x"))
        self.assertEqual(["capacity_probe_x"], client.deleted)

    def test_a_branch_that_is_still_there_after_its_deletion_is_a_failed_step(self) -> None:
        client = FakeNessie(lookups=[True, True, True])
        step = remove_branch(client, "capacity_probe_x")
        self.assertFalse(step.ok)
        self.assertIn("still present", step.detail)

    def test_a_refused_deletion_is_a_failed_step_that_holds_the_status_and_never_the_response_text(self) -> None:
        error = NessieError("DELETE /api/v2/trees/x was refused with status 500, error code X: SECRET BODY", status=500)
        step = remove_branch(FakeNessie(delete_error=error), "capacity_probe_x")
        self.assertFalse(step.ok)
        self.assertEqual("Nessie answered with status 500", step.detail)

    def test_a_nessie_that_does_not_answer_is_an_unverified_step(self) -> None:
        step = remove_branch(FakeNessie(lookups=[NessieError("GET failed: ConnectionRefusedError")]), "capacity_probe_x")
        self.assertEqual(CleanupStep("nessie branch", False, "Nessie did not answer"), step)


class ScriptedDocker(Docker):
    """The container commands of a job run, from a script of states."""

    def __init__(self, states: Sequence[ContainerState], logs: str = RESULT_LINE) -> None:
        super().__init__()
        self.states = list(states)
        self.text = logs

    def state(self, container: str) -> ContainerState:
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]

    def logs(self, container: str) -> str:
        return self.text

    def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
        return [ContainerInfo("a1b2c3d4e5f6", f"p-{service}-1", service or "")]


def running(pid: int = 42) -> ContainerState:
    return ContainerState("running", 0, False, pid, 0, 4096 * MIB)


def exited(code: int = 0, *, oom: bool = False) -> ContainerState:
    return ContainerState("exited", code, oom, 0, 0, 4096 * MIB)


def compose_runner(code: int = 0) -> Callable[..., CompletedProcess[str]]:
    calls: list[list[str]] = []

    def run(program: str, args: Sequence[str], *, timeout: float, **_: object) -> CompletedProcess[str]:
        calls.append([program, *args])
        return CompletedProcess([program, *args], code, "name\n", "")

    run.calls = calls  # type: ignore[attr-defined]
    return run


class JobTest(unittest.TestCase):
    def probe(
        self, docker: Docker, *, host: bool = True, runner: Callable[..., CompletedProcess[str]] | None = None, deadline: float = 30.0, budget: float = 100.0
    ) -> Probe:
        options = Options(host_figures=host, job_deadline_seconds=deadline, rows=10)
        return Probe(
            Settings(compose_directory=Path("/stack/compose"), project="p"),
            parse_limits(LIMITS_TEXT),
            options,
            docker=docker,
            budget=Budget(budget),
            arguments=["--branch", "capacity_probe_x", "--rows", "10"],
            container="p_capacity_probe_x",
            runner=runner or compose_runner(),
            pause=lambda seconds: None,
        )

    def memory(self, peak: int, *, kills: int = 0) -> CgroupMemory:
        return CgroupMemory(peak, 4096 * MIB, kills, 0)

    def test_the_job_is_started_detached_with_its_own_name_and_is_not_removed_by_compose(self) -> None:
        runner = compose_runner()
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=self.memory(100)):
            self.probe(ScriptedDocker([running(), exited()]), runner=runner).run_job(lambda: None)
        call = runner.calls[0]  # type: ignore[attr-defined]
        for word in ("--detach", "--no-deps", "run", "--name", "p_capacity_probe_x", "spark", "--rows", "10"):
            self.assertIn(word, call)
        self.assertNotIn("--rm", call)

    def test_the_peak_is_the_highest_cgroup_reading_before_the_exit_and_the_cap_and_kills_come_from_the_last_reading(self) -> None:
        readings = [self.memory(100 * MIB), self.memory(900 * MIB), self.memory(500 * MIB, kills=0)]
        seen: list[str] = []
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", side_effect=readings):
            outcome = self.probe(ScriptedDocker([running(), running(), running(), exited()])).run_job(lambda: seen.append("running"))
        if outcome.evidence is None:
            self.fail("the outcome holds no evidence")
        self.assertEqual(900 * MIB, outcome.evidence.peak_bytes)
        self.assertEqual(4096 * MIB, outcome.evidence.cap_bytes)
        self.assertEqual(["running"], seen, "the sampler is asked once, when the container is first seen running")
        self.assertEqual((0, False, ""), (outcome.exit_code, outcome.oom_killed, outcome.result_error))
        self.assertEqual(
            {"branch", "branch_hash", "table", "rows", "memory_limit_mib", "python_version", "uid", "snapshot_id", "spark_version", "jvm_max_heap_mib"},
            set(outcome.result),
        )

    def test_a_job_that_the_kernel_killed_for_memory_is_read_from_its_state_before_the_container_is_removed(self) -> None:
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=self.memory(4000 * MIB, kills=1)):
            outcome = self.probe(ScriptedDocker([running(), exited(137, oom=True)], logs="")).run_job(lambda: None)
        if outcome.evidence is None:
            self.fail("the outcome holds no evidence")
        self.assertEqual((137, True), (outcome.exit_code, outcome.oom_killed))
        self.assertTrue(outcome.evidence.oom_killed)
        self.assertEqual(1, outcome.evidence.oom_kills)

    def test_a_cgroup_that_cannot_be_read_on_some_polls_does_not_hide_the_polls_that_could(self) -> None:
        readings = [FiguresError("gone"), self.memory(300 * MIB), FiguresError("gone")]
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", side_effect=readings):
            outcome = self.probe(ScriptedDocker([running(), running(), running(), exited()])).run_job(lambda: None)
        if outcome.evidence is None:
            self.fail("the outcome holds no evidence")
        self.assertEqual(300 * MIB, outcome.evidence.peak_bytes)

    def test_a_job_whose_cgroup_was_never_read_has_no_peak_and_no_cap(self) -> None:
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", side_effect=FiguresError("gone")):
            outcome = self.probe(ScriptedDocker([running(), exited()])).run_job(lambda: None)
        if outcome.evidence is None:
            self.fail("the outcome holds no evidence")
        self.assertEqual((None, None, None), (outcome.evidence.peak_bytes, outcome.evidence.cap_bytes, outcome.evidence.oom_kills))

    def test_on_another_computer_the_cap_comes_from_docker_and_the_peak_from_the_samples(self) -> None:
        outcome = self.probe(ScriptedDocker([running(), exited()]), host=False).run_job(lambda: None)
        if outcome.evidence is None:
            self.fail("the outcome holds no evidence")
        self.assertEqual((4096 * MIB, None, "docker"), (outcome.evidence.cap_bytes, outcome.evidence.peak_bytes, outcome.evidence.source))

    def test_a_result_that_is_not_the_requested_one_is_named_in_the_outcome(self) -> None:
        wrong = RESULT_LINE.replace('"rows": 10', '"rows": 9')
        outcome = self.probe(ScriptedDocker([exited()], logs=wrong), host=False).run_job(lambda: None)
        self.assertIn("wrote 9 rows and 10 were requested", outcome.result_error)
        self.assertEqual({}, outcome.result)

    def test_a_job_that_never_ends_fails_at_its_deadline_with_no_outcome(self) -> None:
        docker = ScriptedDocker([running()])
        with (
            mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=self.memory(1)),
            self.assertRaisesRegex(ProbeError, "the Spark job timed out after 0.05 seconds"),
        ):
            probe = self.probe(docker, deadline=0.05)
            probe.pause = lambda seconds: time.sleep(0.01)
            probe.run_job(lambda: None)

    def test_a_job_that_cannot_be_started_names_the_step_and_the_status(self) -> None:
        with self.assertRaisesRegex(ProbeError, "starting the Spark job: docker exited 125"):
            self.probe(ScriptedDocker([exited()]), runner=compose_runner(125)).run_job(lambda: None)

    def test_a_run_with_no_time_left_does_not_start_the_job(self) -> None:
        runner = compose_runner()
        with self.assertRaisesRegex(ProbeError, "deadline of 0 seconds has passed"):
            self.probe(ScriptedDocker([exited()]), runner=runner, budget=0.0).run_job(lambda: None)
        self.assertEqual([], runner.calls)  # type: ignore[attr-defined]

    def test_the_restarts_of_every_required_service_are_counted_at_the_start(self) -> None:
        docker = ScriptedDocker([ContainerState("running", 0, False, 7, 2, 1024 * MIB)])
        self.assertEqual({"nessie": 2, "postgres": 2}, self.probe(docker).baseline())

    def test_the_end_evidence_of_a_required_service_comes_from_its_cgroup_on_the_host(self) -> None:
        with mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", return_value=CgroupMemory(300, 1024 * MIB, 2, 5)):
            found = self.probe(ScriptedDocker([running()])).final_evidence()
        self.assertEqual(
            (1024 * MIB, 300, 2, True, "cgroup"),
            (found["nessie"].cap_bytes, found["nessie"].peak_bytes, found["nessie"].oom_kills, found["nessie"].running, found["nessie"].source),
        )

    def test_the_end_evidence_on_another_computer_comes_from_docker(self) -> None:
        found = self.probe(ScriptedDocker([ContainerState("running", 0, False, 7, 0, 1024 * MIB)]), host=False).final_evidence()
        self.assertEqual((1024 * MIB, None, "docker"), (found["postgres"].cap_bytes, found["postgres"].peak_bytes, found["postgres"].source))

    def test_a_job_that_never_started_has_no_branch_to_remove_and_nessie_is_not_asked(self) -> None:
        class Asked(FakeNessie):
            def reference(self, name: str) -> Reference:
                raise AssertionError("Nessie was asked")

        probe = self.probe(ScriptedDocker([exited()]))
        steps = probe.cleanup(FakeContainers(), Asked())
        self.assertEqual([CleanupStep("job container", True, "no container remains"), CleanupStep("nessie branch", True, "the job did not start")], steps)

    def test_a_job_that_started_has_its_branch_looked_up_even_when_the_start_failed(self) -> None:
        probe = self.probe(ScriptedDocker([exited()]), runner=compose_runner(125))
        with self.assertRaises(ProbeError):
            probe.run_job(lambda: None)
        steps = probe.cleanup(FakeContainers(), FakeNessie(exists=False))
        self.assertEqual("no branch existed", steps[1].detail)

    def test_a_cgroup_that_cannot_be_read_at_the_end_is_an_error_for_the_caller_to_keep_apart(self) -> None:
        with (
            mock.patch("scripts.stack.capacity_probe.read_cgroup_memory", side_effect=FiguresError("cannot read the host's memory.peak")),
            self.assertRaises(FiguresError),
        ):
            self.probe(ScriptedDocker([running()])).final_evidence()

    def test_a_service_that_is_not_running_at_the_end_is_recorded_as_not_running(self) -> None:
        class Empty(ScriptedDocker):
            def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
                return []

        found = self.probe(Empty([running()])).final_evidence()
        self.assertFalse(found["postgres"].running)


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
