"""The probe's verdict follows from its evidence and nothing else: a figure outside its limit, an event, a cleanup that failed and a missing measurement each
change it by name, and a run is complete only on the Linux host with nothing failed and nothing missing.

Each case changes one fact of a good run and expects the verdict to follow from that fact alone.
"""

from __future__ import annotations

import dataclasses
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.host_figures import HostFigures
from scripts.stack.probe_docker import ContainerEvent, ProbeError, SamplingError
from scripts.stack.probe_record import (
    Assessment,
    CleanupStep,
    ContainerEvidence,
    Execution,
    JobOutcome,
    Sample,
    assess,
    build_document,
    check_destination,
    overlap_sample_count,
    peaks_of,
    validate_result,
    write_document,
)

MIB = 1048576
LIMITS = {"postgres": 1024, "nessie": 1024, "spark": 4096}
REQUIRED = ("postgres", "nessie")
RESERVE_MIB = 4096
RESERVE_KIB = RESERVE_MIB * 1024


def figures(*, available_kib: int = 9_000_000, cpu: float | None = 10.0) -> HostFigures:
    return HostFigures(10000.0, round(available_kib / 1024, 1), available_kib, 0.0, cpu, (cpu, cpu), 1.0, 1.0, 100000.0)


def sample(seconds: float, *, host: HostFigures | None = None, spark: bool = True, phase: str = "overlap", **extra: int) -> Sample:
    usage = {"postgres": 300 * MIB, "nessie": 400 * MIB, **({"spark": 900 * MIB} if spark else {}), **extra}
    return Sample(seconds, f"2026-11-22T10:00:{int(seconds):02d}Z", phase, host, usage)


def evidence(
    service: str, *, cap_mib: int | None = None, peak_mib: float | None = 100.0, kills: int | None = 0, restarts: int = 0, running: bool = True
) -> ContainerEvidence:
    cap = LIMITS[service] if cap_mib is None else cap_mib
    return ContainerEvidence(service, running, cap * MIB, None if peak_mib is None else round(peak_mib * MIB), kills, False, restarts, "cgroup")


GOOD_RESULT = {
    "branch": "capacity_probe_x",
    "branch_hash": "abc",
    "table": "catalog_proof.numbers",
    "rows": 10,
    "memory_limit_mib": 4096,
    "python_version": "3.14.10",
    "uid": 10100,
    "snapshot_id": 5,
    "spark_version": "4.1.4",
    "jvm_max_heap_mib": 2926,
}


def good_execution(**changes: Any) -> Execution:
    base = Execution(
        outcome=JobOutcome(0, False, 20.0, GOOD_RESULT, "", evidence("spark", peak_mib=3000.0)),
        failure="",
        samples=[sample(5, host=figures()), sample(10, host=figures()), sample(15, host=figures())],
        sampler_error="",
        sampler_confirmed=True,
        events=[],
        events_error="",
        events_confirmed=True,
        baseline_restarts={"postgres": 0, "nessie": 0},
        evidence={"postgres": evidence("postgres"), "nessie": evidence("nessie")},
        cleanup=[CleanupStep("job container", True, "removed and absent"), CleanupStep("nessie branch", True, "no branch existed")],
    )
    return dataclasses.replace(base, **changes)


def run(execution: Execution, *, host: bool = True) -> Assessment:
    return assess(execution, limits_mib=LIMITS, required=REQUIRED, reserve_mib=RESERVE_MIB, host_measured=host)


def verdict(execution: Execution, *, host: bool = True) -> str:
    return run(execution, host=host).verdict(host)


def event(action: str, service: str = "nessie") -> ContainerEvent:
    return ContainerEvent(1, action, service, f"p-{service}-1", "")


class VerdictTest(unittest.TestCase):
    def test_a_good_run_on_the_host_is_complete_and_every_check_holds(self) -> None:
        result = run(good_execution())
        self.assertEqual("complete", result.verdict(True))
        self.assertEqual((), result.failures + result.gaps)
        self.assertTrue(all(value is True for value in result.checks.values()))

    def test_a_good_run_on_another_computer_is_local_only_and_never_complete_and_never_claims_the_reserve(self) -> None:
        execution = good_execution(samples=[sample(5), sample(10), sample(15)])
        result = run(execution, host=False)
        self.assertEqual("local_only", result.verdict(False))
        self.assertIsNone(result.checks["host_reserve_kept"])

    def test_host_figures_that_were_not_asked_for_are_never_judged_against_the_reserve(self) -> None:
        low = [sample(5, host=figures(available_kib=1)), sample(10, host=figures(available_kib=1)), sample(15, host=figures(available_kib=1))]
        result = run(good_execution(samples=low), host=False)
        self.assertEqual("local_only", result.verdict(False))
        self.assertIsNone(result.checks["host_reserve_kept"])

    def test_a_run_that_expected_host_figures_and_got_none_is_not_measured(self) -> None:
        self.assertEqual("not_measured", verdict(good_execution(samples=[sample(5), sample(10), sample(15)])))


class ReserveTest(unittest.TestCase):
    def lowest(self, kib: int) -> str:
        series = [sample(5, host=figures()), sample(10, host=figures(available_kib=kib)), sample(15, host=figures())]
        return verdict(good_execution(samples=series))

    def test_the_reserve_is_kept_at_exactly_the_reserve_in_kib_and_lost_one_kib_below(self) -> None:
        self.assertEqual("complete", self.lowest(RESERVE_KIB))
        self.assertEqual("failed", self.lowest(RESERVE_KIB - 1))

    def test_a_figure_that_rounds_up_to_the_reserve_in_mib_is_still_below_it(self) -> None:
        kib = 4095 * 1024 + 983
        self.assertEqual(4096.0, round(kib / 1024, 1))
        self.assertEqual("failed", self.lowest(kib))
        failures = run(good_execution(samples=[sample(5, host=figures(available_kib=kib))] * 3)).failures
        self.assertTrue(any("lost its reserve" in text and str(kib) in text for text in failures))


class ContainerTest(unittest.TestCase):
    def with_required(self, **evidences: ContainerEvidence) -> Execution:
        return good_execution(evidence={"postgres": evidence("postgres"), "nessie": evidence("nessie"), **evidences})

    def test_a_peak_at_the_limit_in_bytes_is_within_it_and_one_byte_over_is_not(self) -> None:
        at_limit = dataclasses.replace(evidence("postgres"), peak_bytes=1024 * MIB)
        self.assertEqual("complete", verdict(self.with_required(postgres=at_limit)))
        over = dataclasses.replace(evidence("postgres"), peak_bytes=1024 * MIB + 1)
        result = run(self.with_required(postgres=over))
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("service postgres: the peak memory is 1073741825 bytes, above the limit of 1073741824" in text for text in result.failures))

    def test_a_cap_that_differs_from_the_configured_limit_fails_even_when_the_peak_is_low(self) -> None:
        result = run(self.with_required(postgres=evidence("postgres", cap_mib=2048)))
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("memory cap is 2147483648 bytes and the configured limit is 1073741824" in text for text in result.failures))

    def test_a_container_that_shows_64_04_mib_of_a_configured_64_mib_with_a_128_mib_cap_fails(self) -> None:
        limits = {"postgres": 64, "nessie": 1024, "spark": 4096}
        wrong = ContainerEvidence("postgres", True, 128 * MIB, None, None, False, 0, "docker")
        execution = good_execution(
            evidence={"postgres": wrong, "nessie": evidence("nessie")},
            samples=[sample(5, host=figures(), postgres=round(64.04 * MIB))] * 3,
        )
        result = assess(execution, limits_mib=limits, required=REQUIRED, reserve_mib=RESERVE_MIB, host_measured=True)
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("service postgres: the memory cap is 134217728 bytes" in text for text in result.failures))
        self.assertTrue(any(f"service postgres: the peak memory is {round(64.04 * MIB)} bytes" in text for text in result.failures))

    def test_a_kill_for_memory_in_the_cgroup_or_in_the_container_flag_fails(self) -> None:
        killed = run(self.with_required(nessie=evidence("nessie", kills=1)))
        self.assertEqual("failed", killed.verdict(True))
        self.assertTrue(any("service nessie: the kernel killed a process for memory 1 time" in text for text in killed.failures))
        flagged = dataclasses.replace(evidence("nessie"), oom_killed=True)
        self.assertEqual("failed", verdict(self.with_required(nessie=flagged)))

    def test_a_restart_between_the_start_and_the_end_fails_and_no_start_count_is_a_gap(self) -> None:
        self.assertEqual("failed", verdict(self.with_required(nessie=evidence("nessie", restarts=1))))
        self.assertEqual("not_measured", verdict(good_execution(baseline_restarts={"postgres": 0})))

    def test_a_required_service_that_is_not_running_at_the_end_fails(self) -> None:
        self.assertEqual("failed", verdict(self.with_required(nessie=evidence("nessie", running=False))))

    def test_a_missing_cap_or_a_missing_peak_is_a_gap_and_never_a_pass(self) -> None:
        no_cap = dataclasses.replace(evidence("postgres"), cap_bytes=None)
        self.assertEqual("not_measured", verdict(self.with_required(postgres=no_cap)))
        no_peak_anywhere = good_execution(
            evidence={"postgres": evidence("postgres", peak_mib=None), "nessie": evidence("nessie")},
            samples=[sample(5, host=figures()), sample(10, host=figures()), sample(15, host=figures())],
        )
        self.assertEqual("complete", verdict(no_peak_anywhere), "a polled peak stands in for a missing cgroup peak")
        bare = dataclasses.replace(no_peak_anywhere, samples=[Sample(5, "t", "overlap", figures(), {"nessie": 1, "spark": 1})] * 3)
        self.assertEqual("not_measured", verdict(bare))

    def test_a_service_with_no_evidence_is_a_gap(self) -> None:
        self.assertEqual("not_measured", verdict(good_execution(evidence={"nessie": evidence("nessie")})))

    def test_a_polled_figure_above_the_limit_fails_when_the_cgroup_gave_no_peak(self) -> None:
        execution = good_execution(
            evidence={"postgres": evidence("postgres", peak_mib=None), "nessie": evidence("nessie")},
            samples=[sample(5, host=figures(), postgres=1024 * MIB + 1)] * 3,
        )
        self.assertEqual("failed", verdict(execution))

    def test_a_service_that_the_limits_file_does_not_hold_fails_by_name(self) -> None:
        result = run(good_execution(samples=[sample(5, host=figures(), mystery=1)] * 3))
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("service mystery: has no limit in the limits file" in text for text in result.failures))


class EventTest(unittest.TestCase):
    def test_a_die_oom_kill_or_restart_of_a_required_service_fails_and_names_it(self) -> None:
        for action in ("die", "oom", "kill", "restart"):
            with self.subTest(action=action):
                result = run(good_execution(events=[event(action, "postgres")]))
                self.assertEqual("failed", result.verdict(True))
                self.assertTrue(any(f"service postgres: {action} of postgres during the run" in text for text in result.failures))
                self.assertIs(False, result.checks["no_lifecycle_events"])

    def test_the_spark_container_dies_when_its_job_ends_and_that_is_not_a_failure_but_an_oom_event_is(self) -> None:
        self.assertEqual("complete", verdict(good_execution(events=[event("die", "spark"), event("kill", "spark")])))
        result = run(good_execution(events=[event("oom", "spark")]))
        self.assertEqual("failed", result.verdict(True))
        self.assertIs(False, result.checks["completed"])

    def test_an_event_of_a_service_that_is_not_required_is_not_a_failure(self) -> None:
        self.assertEqual("complete", verdict(good_execution(events=[event("die", "other_tool")])))


class MeasurementTest(unittest.TestCase):
    def test_fewer_than_three_samples_with_the_spark_container_and_every_required_service_is_not_measured(self) -> None:
        series = [sample(5, host=figures(), spark=False), sample(10, host=figures()), sample(15, host=figures())]
        result = run(good_execution(samples=series))
        self.assertEqual("not_measured", result.verdict(True))
        self.assertEqual(2, result.overlap_samples)
        self.assertEqual(2, overlap_sample_count(series, (*REQUIRED, "spark")))

    def test_three_samples_are_enough(self) -> None:
        self.assertEqual("complete", verdict(good_execution()))

    def test_a_sampler_or_an_event_reader_that_did_not_stop_normally_is_not_measured(self) -> None:
        for changes in (
            {"sampler_confirmed": False},
            {"sampler_error": "sample 4 failed: no MemAvailable line"},
            {"events_confirmed": False},
            {"events_error": "docker events line 1 is not in the expected form"},
        ):
            with self.subTest(changes=changes):
                result = run(good_execution(**changes))
                self.assertEqual("not_measured", result.verdict(True))

    def test_a_failure_wins_over_a_gap(self) -> None:
        self.assertEqual("failed", verdict(good_execution(sampler_confirmed=False, events=[event("die", "postgres")])))

    def test_a_job_that_did_not_run_has_the_failure_text_and_is_not_completed(self) -> None:
        result = run(good_execution(outcome=None, failure="starting the always-on group failed: x"))
        self.assertEqual("failed", result.verdict(True))
        self.assertIs(False, result.checks["completed"])

    def test_an_interrupted_run_fails(self) -> None:
        self.assertEqual("failed", verdict(good_execution(interrupted=True)))

    def test_a_cleanup_step_that_failed_or_could_not_be_verified_fails_and_is_listed(self) -> None:
        steps = [CleanupStep("job container", True, "removed and absent"), CleanupStep("nessie branch", False, "Nessie did not answer")]
        result = run(good_execution(cleanup=steps))
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("cleanup step nessie branch failed: Nessie did not answer" in text for text in result.failures))
        self.assertIs(False, result.checks["cleanup_ok"])


class JobTest(unittest.TestCase):
    def outcome(self, **changes: Any) -> Execution:
        return good_execution(outcome=dataclasses.replace(good_execution().outcome, **changes))  # type: ignore[type-var]

    def test_a_job_that_exits_with_an_error_is_not_completed(self) -> None:
        self.assertEqual("failed", verdict(self.outcome(exit_code=1)))

    def test_a_job_the_kernel_killed_for_memory_is_not_completed_even_with_exit_zero(self) -> None:
        result = run(self.outcome(oom_killed=True))
        self.assertEqual("failed", result.verdict(True))
        self.assertIs(False, result.checks["completed"])

    def test_a_job_whose_result_is_not_the_requested_one_is_not_completed(self) -> None:
        result = run(self.outcome(result_error="the job wrote 5 rows and 10 were requested"))
        self.assertEqual("failed", result.verdict(True))
        self.assertTrue(any("the job wrote 5 rows" in text for text in result.failures))

    def test_the_result_is_reduced_to_its_documented_fields_and_must_be_the_requested_one(self) -> None:
        clean, error = validate_result({**GOOD_RESULT, "secret": "x"}, branch="capacity_probe_x", rows=10, memory_limit_mib=4096)
        self.assertEqual("", error)
        self.assertEqual(GOOD_RESULT, clean)
        for changes, text in (
            ({"branch": "other"}, "another branch"),
            ({"rows": 9}, "wrote 9 rows and 10 were requested"),
            ({"memory_limit_mib": 2048}, "memory limit of 2048 MiB and 4096 MiB is configured"),
            ({"rows": True}, "no valid rows"),
            ({"rows": "10"}, "no valid rows"),
            ({"snapshot_id": None}, "no valid snapshot_id"),
            ({"table": "x" * 201}, "no valid table"),
            ({"python_version": ""}, "no valid python_version"),
        ):
            with self.subTest(changes=changes):
                clean, error = validate_result({**GOOD_RESULT, **changes}, branch="capacity_probe_x", rows=10, memory_limit_mib=4096)
                self.assertEqual({}, clean)
                self.assertIn(text, error)
        missing = {key: value for key, value in GOOD_RESULT.items() if key != "uid"}
        self.assertIn("no valid uid", validate_result(missing, branch="capacity_probe_x", rows=10, memory_limit_mib=4096)[1])
        self.assertIn("no valid branch", validate_result({}, branch="capacity_probe_x", rows=10, memory_limit_mib=4096)[1])


class PeaksTest(unittest.TestCase):
    def test_each_peak_is_the_extreme_of_its_figure_over_every_sample_and_not_the_last_value(self) -> None:
        hosts = [
            HostFigures(11000.0, 19000.0, 19456000, 0.0, 20.0, (40.0, 5.0), 3.0, 9.0, 90000.0),
            HostFigures(27000.0, 5800.0, 5939200, 512.0, 95.0, (99.0, 60.0), 80.0, 40.0, 80000.0),
            HostFigures(12000.0, 18000.0, 18432000, 100.0, None, (None, None), 2.0, 1.0, 85000.0),
        ]
        series = [sample(5, host=hosts[0], postgres=300 * MIB), sample(10, host=hosts[1], postgres=250 * MIB), sample(15, host=hosts[2], postgres=260 * MIB)]
        peaks = peaks_of(series)
        self.assertEqual(3, peaks["samples"])
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
        self.assertEqual(300.0, peaks["containers_mib"]["postgres"])

    def test_a_series_with_no_host_figures_has_no_host_peaks_and_one_with_no_cpu_figure_has_no_cpu_peak(self) -> None:
        self.assertIsNone(peaks_of([sample(5)])["host"])
        no_cpu = HostFigures(1.0, 2.0, 2048, 0.0, None, (None,), 0.0, 0.0, 9.0)
        self.assertIsNone(peaks_of([sample(5, host=no_cpu)])["host"]["cpu_percent"])

    def test_an_empty_series_is_refused(self) -> None:
        with self.assertRaisesRegex(SamplingError, "no sample"):
            peaks_of([])


class DocumentTest(unittest.TestCase):
    def document(self, execution: Execution | None = None, *, host: bool = True) -> dict[str, Any]:
        execution = execution or good_execution()
        return build_document(
            execution,
            run(execution, host=host),
            limits_mib=LIMITS,
            required=REQUIRED,
            reserve_mib=RESERVE_MIB,
            host_measured=host,
            interval_seconds=5.0,
            deadline_seconds=900.0,
            started_utc="2026-11-22T10:00:00Z",
            finished_utc="2026-11-22T10:01:00Z",
            job_arguments=["--branch", "capacity_probe_x", "--rows", "10"],
        )

    def test_the_record_holds_the_verdict_the_findings_the_exact_bytes_of_each_container_and_the_series(self) -> None:
        document = self.document()
        self.assertEqual("complete", document["verdict"])
        self.assertEqual([], document["failures"] + document["gaps"])
        self.assertEqual(round(100.0 * MIB), document["containers"]["postgres"]["peak_bytes"])
        self.assertEqual(1024 * MIB, document["containers"]["postgres"]["configured_bytes"])
        self.assertEqual(3, document["overlap_samples"])
        self.assertEqual(3, len(document["series"]))
        self.assertIn("not measured", document["stream_lag"])
        self.assertTrue(any("slice 13" in note for note in document["limits"]))
        self.assertEqual({"all", "overlap"}, set(document["peaks"]))

    def test_the_events_and_the_cleanup_steps_are_in_the_record(self) -> None:
        document = self.document(good_execution(events=[event("die", "spark")]))
        self.assertEqual([{"action": "die", "service": "spark", "detail": ""}], document["events"])
        self.assertEqual("job container", document["cleanup"][0]["step"])

    def test_a_run_with_no_job_has_an_empty_workload_and_no_spark_evidence(self) -> None:
        document = self.document(good_execution(outcome=None, failure="x"))
        self.assertIsNone(document["workload"]["exit_code"])
        self.assertIsNone(document["containers"]["spark"]["cap_bytes"])

    def test_the_record_holds_no_value_that_json_cannot_carry(self) -> None:
        json.dumps(self.document(), allow_nan=False)

    def test_the_record_is_written_once_and_a_second_write_to_the_same_folder_is_refused(self) -> None:
        document = self.document()
        with tempfile.TemporaryDirectory() as folder:
            path = write_document(Path(folder) / "run", document)
            self.assertEqual(document["verdict"], json.loads(path.read_text())["verdict"])
            self.assertEqual([path.name], [item.name for item in path.parent.iterdir()])
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                write_document(Path(folder) / "run", document)

    def test_the_destination_is_checked_before_anything_starts(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            fresh = Path(folder) / "new" / "run"
            check_destination(fresh)
            self.assertFalse((Path(folder) / "new").exists(), "the check leaves nothing behind")
            write_document(fresh, self.document())
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                check_destination(fresh)
            blocker = Path(folder) / "file"
            blocker.write_text("x")
            with self.assertRaisesRegex(ProbeError, "cannot write the record"):
                check_destination(blocker / "run")
            readonly = Path(folder) / "readonly"
            readonly.mkdir()
            readonly.chmod(0o500)
            try:
                with self.assertRaisesRegex(ProbeError, "cannot write the record"):
                    check_destination(readonly / "run")
            finally:
                readonly.chmod(0o700)


if __name__ == "__main__":
    unittest.main()
