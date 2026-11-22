"""The capacity probe keeps the peaks of its samples, judges them against the limits and the host reserve, samples on a deadline and saves its record.

Each case changes one fact of a good setup and expects the result to follow from that fact alone. Time is a fake clock, so no case waits.
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from collections.abc import Callable
from pathlib import Path

from scripts.stack.capacity_probe import (
    BackgroundSampler,
    Execution,
    JobOutcome,
    ProbeError,
    Sample,
    SamplingError,
    Steps,
    build_document,
    collect,
    execute,
    job_arguments,
    judge,
    parse_job_result,
    peaks_of,
    write_document,
)
from scripts.stack.host_figures import HostFigures

LIMITS = {"postgres": 1024, "nessie": 1024, "spark": 4096}
RESERVE_MIB = 4096


def figures(*, used: float = 10000.0, available: float = 20000.0, swap: float = 0.0, cpu: float = 10.0, cores: tuple[float, ...] = (10.0, 10.0),
            read: float = 1.0, write: float = 1.0, free: float = 100000.0) -> HostFigures:  # fmt: skip
    return HostFigures(used, available, swap, cpu, cores, read, write, free)


def sample(seconds: float, phase: str = "overlap", host: HostFigures | None = None, **containers: float) -> Sample:
    return Sample(seconds, f"2026-11-22T10:00:{int(seconds):02d}Z", phase, host, containers)


class FakeTime:
    """A clock that only moves when a wait or a step moves it."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def wait(self, seconds: float) -> bool:
        self.now += seconds
        return False


class PeaksTest(unittest.TestCase):
    def test_each_peak_is_the_extreme_of_its_figure_over_every_sample_and_not_the_last_value(self) -> None:
        series = [
            sample(5, host=figures(used=11000, available=19000, swap=0, cpu=20, cores=(40, 5), read=3, write=9, free=90000), postgres=300, spark=100),
            sample(10, host=figures(used=27000, available=5800, swap=512, cpu=95, cores=(99, 60), read=80, write=40, free=80000), postgres=250, spark=3900),
            sample(15, host=figures(used=12000, available=18000, swap=100, cpu=30, cores=(20, 20), read=2, write=1, free=85000), postgres=260, spark=200),
        ]
        peaks = peaks_of(series)
        self.assertEqual(
            {
                "memory_used_mib": 27000.0,
                "memory_available_lowest_mib": 5800.0,
                "swap_used_mib": 512.0,
                "cpu_percent": 95.0,
                "cpu_core_percent": 99.0,
                "disk_read_mib_s": 80.0,
                "disk_write_mib_s": 40.0,
                "free_disk_lowest_mib": 80000.0,
            },
            peaks["host"],
        )
        self.assertEqual(3, peaks["samples"])
        self.assertEqual({"postgres": 300.0, "spark": 3900.0}, peaks["containers_mib"])

    def test_a_series_with_no_host_figures_has_no_host_peaks_and_still_has_the_container_peaks(self) -> None:
        peaks = peaks_of([sample(5, spark=100), sample(10, spark=300)])
        self.assertIsNone(peaks["host"])
        self.assertEqual({"spark": 300.0}, peaks["containers_mib"])

    def test_an_empty_series_is_refused(self) -> None:
        with self.assertRaisesRegex(SamplingError, "no sample"):
            peaks_of([])


class JudgementTest(unittest.TestCase):
    def test_a_container_at_its_limit_is_within_it_and_one_mebibyte_over_is_not(self) -> None:
        at_limit = judge([sample(5, host=figures(), spark=4096.0)], LIMITS, RESERVE_MIB)
        self.assertEqual({}, at_limit.containers_over)
        over = judge([sample(5, host=figures(), spark=4097.0, postgres=10.0)], LIMITS, RESERVE_MIB)
        self.assertEqual({"spark": (4097.0, 4096)}, over.containers_over)

    def test_a_service_that_the_limits_file_does_not_hold_is_named_and_is_not_within_its_limit(self) -> None:
        judged = judge([sample(5, host=figures(), mystery=10.0)], LIMITS, RESERVE_MIB)
        self.assertEqual(("mystery",), judged.containers_unlisted)
        self.assertFalse(judged.containers_within_limits)

    def test_the_reserve_is_kept_when_the_lowest_available_memory_equals_it_and_lost_one_mebibyte_below(self) -> None:
        kept = judge([sample(5, host=figures(available=9000)), sample(10, host=figures(available=4096))], LIMITS, RESERVE_MIB)
        self.assertIs(True, kept.reserve_kept)
        self.assertEqual(4096.0, kept.lowest_available_mib)
        lost = judge([sample(5, host=figures(available=9000)), sample(10, host=figures(available=4095))], LIMITS, RESERVE_MIB)
        self.assertIs(False, lost.reserve_kept)

    def test_without_host_figures_the_reserve_is_unknown_and_never_kept(self) -> None:
        judged = judge([sample(5, spark=100.0)], LIMITS, RESERVE_MIB)
        self.assertIsNone(judged.reserve_kept)
        self.assertIsNone(judged.lowest_available_mib)


class CollectTest(unittest.TestCase):
    def take(self, time: FakeTime, taken: list[float]) -> Callable[[], Sample]:
        def inner() -> Sample:
            if len(taken) > 100:
                raise AssertionError("the sampling never ended")
            taken.append(time.now)
            return sample(time.now)

        return inner

    def test_samples_are_taken_at_the_interval_until_the_stop_and_one_more_after_it(self) -> None:
        time, taken, samples = FakeTime(), list[float](), list[Sample]()
        stop_at = 12.0

        def wait(seconds: float) -> bool:
            time.wait(seconds)
            return time.now >= stop_at

        collect(self.take(time, taken), samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: False, wait=wait, clock=time.clock)
        self.assertEqual([0.0, 5.0, 10.0, 15.0], taken)
        self.assertEqual(4, len(samples))

    def test_the_time_a_sample_takes_is_taken_from_the_wait_so_the_interval_stays_fixed(self) -> None:
        time, taken, samples = FakeTime(), list[float](), list[Sample]()
        waited: list[float] = []

        def slow_take() -> Sample:
            taken.append(time.now)
            time.now += 2.0
            return sample(time.now)

        def wait(seconds: float) -> bool:
            waited.append(seconds)
            time.wait(seconds)
            return len(taken) >= 3

        collect(slow_take, samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: False, wait=wait, clock=time.clock)
        self.assertEqual([3.0, 3.0, 3.0], waited)

    def test_a_sample_that_takes_longer_than_the_interval_is_followed_at_once(self) -> None:
        time, samples = FakeTime(), list[Sample]()
        waited: list[float] = []

        def slow_take() -> Sample:
            time.now += 9.0
            return sample(time.now)

        def wait(seconds: float) -> bool:
            waited.append(seconds)
            return len(waited) >= 2

        collect(slow_take, samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: False, wait=wait, clock=time.clock)
        self.assertEqual([0.0, 0.0], waited)

    def test_a_stop_before_the_first_sample_still_leaves_one_sample(self) -> None:
        time, taken, samples = FakeTime(), list[float](), list[Sample]()
        collect(self.take(time, taken), samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: True, wait=time.wait, clock=time.clock)
        self.assertEqual(1, len(samples))

    def test_the_deadline_stops_the_sampling_with_an_error_that_keeps_the_samples_taken(self) -> None:
        time, taken, samples = FakeTime(), list[float](), list[Sample]()
        with self.assertRaisesRegex(SamplingError, "deadline of 20 seconds.*after 4 samples"):
            collect(self.take(time, taken), samples=samples, interval_seconds=5, deadline_seconds=20, stopped=lambda: False, wait=time.wait, clock=time.clock)
        self.assertEqual(4, len(samples))

    def test_a_reading_that_fails_ends_the_sampling_and_names_the_sample_number_and_the_cause(self) -> None:
        time, samples = FakeTime(), list[Sample]()
        calls = [0]

        def broken() -> Sample:
            calls[0] += 1
            if calls[0] == 3:
                raise ProbeError("cannot read the host's meminfo")
            return sample(time.now)

        with self.assertRaisesRegex(SamplingError, "sample 3 failed: cannot read the host's meminfo"):
            collect(broken, samples=samples, interval_seconds=5, deadline_seconds=100, stopped=lambda: False, wait=time.wait, clock=time.clock)
        self.assertEqual(2, len(samples))


class BackgroundSamplerTest(unittest.TestCase):
    def sampler(self, take: Callable[[], Sample], *, deadline: float = 30.0) -> BackgroundSampler:
        return BackgroundSampler(take, interval_seconds=0.01, deadline_seconds=deadline, join_seconds=5.0)

    def test_the_samples_carry_the_phase_that_was_set_when_each_was_taken(self) -> None:
        sampler = self.sampler(lambda: sample(0.0, phase=sampler.phase))
        sampler.phase = "startup"
        sampler.start()
        self.wait_for(lambda: len(sampler.samples) >= 2)
        sampler.phase = "overlap"
        count = len(sampler.samples)
        self.wait_for(lambda: len(sampler.samples) >= count + 2)
        sampler.stop()
        phases = [item.phase for item in sampler.samples]
        self.assertEqual("startup", phases[0])
        self.assertEqual("overlap", phases[-1])
        self.assertIsNone(sampler.error)

    def test_a_failing_reading_is_kept_as_the_error_and_the_samples_before_it_stay(self) -> None:
        calls = [0]

        def broken() -> Sample:
            calls[0] += 1
            if calls[0] == 3:
                raise ProbeError("no MemAvailable line")
            return sample(float(calls[0]))

        sampler = self.sampler(broken)
        sampler.start()
        self.wait_for(lambda: sampler.error is not None)
        sampler.stop()
        self.assertIn("no MemAvailable line", str(sampler.error))
        self.assertEqual(2, len(sampler.samples))

    def test_a_reading_that_never_returns_fails_the_stop_at_its_join_deadline(self) -> None:
        release = threading.Event()
        sampler = BackgroundSampler(lambda: sample(0.0) if release.wait(10) else sample(-1.0), interval_seconds=0.01, deadline_seconds=30, join_seconds=0.2)
        sampler.start()
        try:
            with self.assertRaisesRegex(SamplingError, "did not stop within 0.2 seconds"):
                sampler.stop()
        finally:
            release.set()

    @staticmethod
    def wait_for(condition: Callable[[], bool]) -> None:
        stop = threading.Event()
        for _ in range(500):
            if condition():
                return
            stop.wait(0.01)
        raise AssertionError("the sampler did not reach the expected state within 5 seconds")


class ExecuteTest(unittest.TestCase):
    def steps(self, *, start: Callable[[], None] | None = None, admit: Callable[[], None] | None = None, run: Callable[[], JobOutcome] | None = None) -> Steps:
        return Steps(
            start_supporting=start or (lambda: None),
            admit_job=admit or (lambda: None),
            run_job=run or (lambda: JobOutcome(0, False, 12.0, {"rows": 10})),
        )

    @staticmethod
    def job_ran(ran: list[str]) -> JobOutcome:
        ran.append("job")
        return JobOutcome(0, False, 1.0, {})

    def sampler(self) -> BackgroundSampler:
        sampler = BackgroundSampler(lambda: sample(0.0, phase=sampler.phase, spark=1.0), interval_seconds=0.01, deadline_seconds=30, join_seconds=5.0)
        return sampler

    def test_a_good_run_starts_the_services_then_the_job_and_labels_the_samples_by_phase(self) -> None:
        order: list[str] = []
        sampler = self.sampler()

        def start() -> None:
            order.append("start")
            BackgroundSamplerTest.wait_for(lambda: len(sampler.samples) >= 2)

        def run() -> JobOutcome:
            order.append("job")
            BackgroundSamplerTest.wait_for(lambda: any(item.phase == "overlap" for item in sampler.samples))
            return JobOutcome(0, False, 3.0, {})

        result = execute(self.steps(start=start, run=run), sampler)
        self.assertEqual(["start", "job"], order)
        self.assertEqual("", result.failure)
        self.assertEqual(0, result.outcome.exit_code if result.outcome else -1)
        self.assertEqual({"startup", "overlap"}, {item.phase for item in result.samples})

    def test_a_service_that_does_not_start_ends_the_run_before_the_job_and_names_the_step(self) -> None:
        ran: list[str] = []

        def start() -> None:
            raise ProbeError("group supporting: not healthy after 300 seconds: nessie: state running, health starting")

        admitted: list[str] = []
        result = execute(self.steps(start=start, admit=lambda: admitted.append("admit"), run=lambda: self.job_ran(ran)), self.sampler())
        self.assertEqual([], ran)
        self.assertEqual([], admitted)
        self.assertIsNone(result.outcome)
        self.assertIn("starting the always-on group failed: group supporting: not healthy after 300 seconds", result.failure)

    def test_a_job_that_does_not_fit_waits_and_never_starts(self) -> None:
        ran: list[str] = []

        def admit() -> None:
            raise ProbeError("after waiting 0 seconds: group batch waits: 3000 MiB short")

        result = execute(self.steps(admit=admit, run=lambda: self.job_ran(ran)), self.sampler())
        self.assertEqual([], ran)
        self.assertIn("admission of the Spark job refused: after waiting 0 seconds: group batch waits: 3000 MiB short", result.failure)

    def test_a_sampling_failure_fails_the_run_even_when_the_job_completed(self) -> None:
        def broken() -> Sample:
            raise ProbeError("no MemAvailable line")

        sampler = BackgroundSampler(broken, interval_seconds=0.01, deadline_seconds=30, join_seconds=5.0)

        def run() -> JobOutcome:
            BackgroundSamplerTest.wait_for(lambda: sampler.error is not None)
            return JobOutcome(0, False, 1.0, {})

        result = execute(self.steps(run=run), sampler)
        self.assertEqual(0, result.outcome.exit_code if result.outcome else -1)
        self.assertIn("sampling failed", result.failure)
        self.assertIn("no MemAvailable line", result.failure)

    def test_the_sampler_is_stopped_after_every_outcome(self) -> None:
        sampler = self.sampler()

        def start() -> None:
            raise ProbeError("boom")

        execute(self.steps(start=start), sampler)
        self.assertFalse(sampler.running)


class DocumentTest(unittest.TestCase):
    def execution(self, *, exit_code: int = 0, oom: bool = False, failure: str = "", samples: list[Sample] | None = None, host: bool = True) -> Execution:
        series = samples or [
            sample(5, "startup", figures(available=9000) if host else None, postgres=300.0),
            sample(10, "overlap", figures(available=6000) if host else None, postgres=310.0, spark=3000.0),
        ]
        return Execution(JobOutcome(exit_code, oom, 20.0, {"rows": 10}), failure, series, None)

    def document(self, execution: Execution, *, host_measured: bool = True) -> dict[str, object]:
        return build_document(
            execution,
            limits_mib=LIMITS,
            reserve_mib=RESERVE_MIB,
            host_measured=host_measured,
            interval_seconds=5.0,
            deadline_seconds=900.0,
            started_utc="2026-11-22T10:00:00Z",
            finished_utc="2026-11-22T10:01:00Z",
            job_arguments=["--branch", "capacity_probe_x", "--rows", "10"],
        )

    def test_a_run_that_completed_within_every_limit_and_kept_the_reserve_is_complete(self) -> None:
        document = self.document(self.execution())
        self.assertEqual(
            {"completed": True, "sampling_ok": True, "containers_within_limits": True, "host_reserve_kept": True},
            document["checks"],
        )
        self.assertEqual("complete", document["verdict"])

    def test_a_job_that_exits_with_an_error_is_not_completed(self) -> None:
        document = self.document(self.execution(exit_code=1))
        self.assertIs(False, document["checks"]["completed"])  # type: ignore[index]
        self.assertEqual("failed", document["verdict"])

    def test_a_job_the_kernel_killed_for_memory_is_not_completed_and_the_record_says_so(self) -> None:
        document = self.document(self.execution(exit_code=137, oom=True))
        self.assertIs(False, document["checks"]["completed"])  # type: ignore[index]
        self.assertIs(True, document["workload"]["oom_killed"])  # type: ignore[index]

    def test_a_job_with_exit_zero_and_the_oom_flag_set_is_not_completed(self) -> None:
        self.assertEqual("failed", self.document(self.execution(exit_code=0, oom=True))["verdict"])

    def test_a_run_with_a_failure_text_is_failed_whatever_else_holds(self) -> None:
        document = self.document(self.execution(failure="sampling failed: x"))
        self.assertEqual("failed", document["verdict"])
        self.assertEqual("sampling failed: x", document["failure"])

    def test_a_run_whose_job_never_started_is_not_completed(self) -> None:
        execution = Execution(None, "starting the always-on group failed: x", [sample(5, "startup", figures())], None)
        document = self.document(execution)
        self.assertIs(False, document["checks"]["completed"])  # type: ignore[index]
        self.assertIsNone(document["workload"]["exit_code"])  # type: ignore[index]

    def test_a_container_over_its_limit_fails_the_run_and_names_the_service_and_the_figures(self) -> None:
        series = [sample(5, "overlap", figures(), spark=4200.0)]
        document = self.document(self.execution(samples=series))
        self.assertEqual("failed", document["verdict"])
        self.assertEqual({"spark": {"peak_mib": 4200.0, "limit_mib": 4096}}, document["containers_over_limit"])

    def test_a_lost_reserve_fails_the_run(self) -> None:
        series = [sample(5, "overlap", figures(available=4000), spark=100.0)]
        self.assertEqual("failed", self.document(self.execution(samples=series))["verdict"])

    def test_a_run_on_a_computer_that_is_not_the_linux_host_is_marked_and_never_claims_the_reserve(self) -> None:
        document = self.document(self.execution(host=False), host_measured=False)
        self.assertEqual("docker figures only", document["verdict"])
        self.assertIsNone(document["checks"]["host_reserve_kept"])  # type: ignore[index]
        self.assertIs(False, document["host_figures"]["measured"])  # type: ignore[index]
        self.assertIsNone(document["peaks"]["all"]["host"])  # type: ignore[index]

    def test_host_figures_that_were_not_asked_for_are_never_judged_against_the_reserve(self) -> None:
        document = self.document(self.execution(host=True), host_measured=False)
        self.assertIsNone(document["checks"]["host_reserve_kept"])  # type: ignore[index]
        self.assertEqual("docker figures only", document["verdict"])

    def test_a_run_that_expected_host_figures_and_got_none_is_failed(self) -> None:
        self.assertEqual("failed", self.document(self.execution(host=False), host_measured=True)["verdict"])

    def test_the_stream_lag_is_stated_as_not_measured_and_the_series_is_in_the_record(self) -> None:
        document = self.document(self.execution())
        self.assertIn("not measured", str(document["stream_lag"]))
        self.assertEqual(2, len(document["series"]))  # type: ignore[arg-type]

    def test_the_peaks_are_given_for_the_whole_run_and_for_each_phase(self) -> None:
        peaks = self.document(self.execution())["peaks"]
        self.assertEqual(["all", "overlap", "startup"], sorted(peaks))  # type: ignore[call-overload]
        self.assertEqual({"postgres": 300.0}, peaks["startup"]["containers_mib"])  # type: ignore[index]
        self.assertEqual({"postgres": 310.0, "spark": 3000.0}, peaks["overlap"]["containers_mib"])  # type: ignore[index]

    def test_the_record_is_written_once_as_json_and_a_second_write_to_the_same_folder_is_refused(self) -> None:
        document = self.document(self.execution())
        with tempfile.TemporaryDirectory() as folder:
            path = write_document(Path(folder) / "run", document)
            self.assertEqual(document["verdict"], json.loads(path.read_text())["verdict"])
            self.assertEqual([path.name], [item.name for item in path.parent.iterdir()])
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                write_document(Path(folder) / "run", document)

    def test_the_record_holds_no_value_that_json_cannot_carry(self) -> None:
        document = self.document(self.execution())
        json.dumps(document, allow_nan=False)


class JobTest(unittest.TestCase):
    def test_the_result_line_of_the_job_is_read_and_a_missing_or_broken_line_gives_an_empty_result(self) -> None:
        output = 'WARN something\nRESULT {"rows": 10, "snapshot_id": 5}\n'
        self.assertEqual({"rows": 10, "snapshot_id": 5}, parse_job_result(output))
        self.assertEqual({}, parse_job_result("no result here\n"))
        self.assertEqual({}, parse_job_result("RESULT {broken\n"))
        self.assertEqual({}, parse_job_result("RESULT [1, 2]\n"))

    def test_the_job_arguments_name_a_branch_of_this_run_and_the_fixed_row_count(self) -> None:
        arguments = job_arguments("20261122T100000Z", 2000000)
        self.assertEqual(["--branch", "capacity_probe_20261122t100000z", "--rows", "2000000"], arguments)


if __name__ == "__main__":
    unittest.main()
