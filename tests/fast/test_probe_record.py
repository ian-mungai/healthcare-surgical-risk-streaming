"""The probe's record states what was read and what was not, and never reads as a judgment: no field or fixed text says that anything passed.

Each case changes one fact of a good run and expects the record to follow from that fact alone.
"""

from __future__ import annotations

import dataclasses
import json
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.host_figures import CgroupMemory, HostFigures
from scripts.stack.probe_docker import ContainerEvent, ProbeError
from scripts.stack.probe_record import (
    GAP_CODES,
    NOTES,
    CgroupRead,
    CleanupStep,
    Collected,
    JobFacts,
    Sample,
    ServiceSnapshot,
    build_record,
    check_destination,
    event_window_holes,
    gaps_of,
    read_result,
    write_document,
)

MIB = 1048576
REQUIRED = ("postgres", "nessie")
LIMITS = {"postgres": 1024, "nessie": 1024, "spark": 4096}
ACCEPTANCE_WORDS = re.compile(r"\b(ok|pass|passed|complete|completed|within|sufficient|accepted|success|succeeded)\b", re.IGNORECASE)
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


def figures() -> HostFigures:
    return HostFigures(10000.0, 20000.0, 20480000, 0.0, 10.0, (10.0, 10.0), 1.0, 1.0, 100000.0)


def sample(number: int, *, spark: bool = True, missing: tuple[str, ...] = (), host: HostFigures | None = None) -> Sample:
    return Sample(
        taken_ns=1000 + number,
        utc=f"2026-11-22T10:00:{number:02d}Z",
        covers_start_ns=900 + number,
        covers_end_ns=1000 + number,
        missing=missing,
        spark_running=spark,
        services_running={"postgres": True, "nessie": True, "spark": spark},
        host=host,
        containers_bytes={"postgres": 300 * MIB, "nessie": 400 * MIB, **({"spark": 900 * MIB} if spark else {})},
    )


def cgroup(at: int = 1000) -> CgroupRead:
    return CgroupRead(at, CgroupMemory(123, 4096 * MIB, {"low": 0, "max": 2, "oom": 0, "oom_kill": 0}))


def snapshot() -> ServiceSnapshot:
    return ServiceSnapshot(True, 0, 1_763_800_000.0, False, 1024 * MIB, cgroup())


def collected(**changes: Any) -> Collected:
    base = Collected(
        run_state="recorded",
        cause="",
        step="",
        mode="host",
        samples=[sample(1, spark=False, host=figures()), sample(2, host=figures()), sample(3, host=figures())],
        spark_cgroup=[cgroup(2000), cgroup(3000)],
        events=[ContainerEvent(5, "die", "spark", "p-spark-1", "exitCode 0")],
        windows=[(100, 200), (200, 300)],
        run_start_ns=100,
        job_exit_ns=250,
        start_snapshot={"postgres": snapshot(), "nessie": snapshot()},
        end_snapshot={"postgres": snapshot(), "nessie": snapshot()},
        job=JobFacts(0, False, 20.0, GOOD_RESULT, True),
        cleanup=[CleanupStep("job container", True, "no container remains"), CleanupStep("nessie branch", True, "no branch existed")],
        sample_errors=0,
        cgroup_unreadable=0,
        scratch="p_scratch_x",
    )
    return dataclasses.replace(base, **changes)


def record(**changes: Any) -> dict[str, Any]:
    return build_record(
        collected(**changes),
        limits_mib=LIMITS,
        reserve_mib=4096,
        required=REQUIRED,
        interval_seconds=5.0,
        deadline_seconds=900.0,
        started_utc="2026-11-22T10:00:00Z",
        finished_utc="2026-11-22T10:01:00Z",
        job_arguments=["--branch", "capacity_probe_x", "--rows", "10"],
    )


def codes(document: dict[str, Any]) -> list[str]:
    return [gap["code"] for gap in document["gaps"]]


def every_string(value: Any) -> list[str]:
    """Every key and every text in a document, to the bottom."""
    if isinstance(value, dict):
        return [text for key, item in value.items() for text in (str(key), *every_string(item))]
    if isinstance(value, (list, tuple)):
        return [text for item in value for text in every_string(item)]
    return [value] if isinstance(value, str) else []


class WordsTest(unittest.TestCase):
    def test_no_key_and_no_text_of_a_full_record_reads_as_acceptance(self) -> None:
        full = record(
            samples=[sample(1, missing=("host_figures", "events"))],
            cleanup=[CleanupStep("job container", False, "still present after its removal")],
            sample_errors=1,
            cgroup_unreadable=1,
            job=JobFacts(137, True, 1.0, {}, False),
            windows=[],
        )
        for document in (record(), full):
            found = [text for text in every_string(document) if ACCEPTANCE_WORDS.search(text)]
            self.assertEqual([], found)

    def test_no_fixed_gap_code_and_no_fixed_note_reads_as_acceptance(self) -> None:
        self.assertEqual([], [text for text in (*GAP_CODES, *NOTES) if ACCEPTANCE_WORDS.search(text)])

    def test_the_scan_does_catch_an_acceptance_word(self) -> None:
        for text in ("ok", "all pass", "complete", "within limits", "sufficient samples", "Completed"):
            self.assertTrue(ACCEPTANCE_WORDS.search(text), text)


class RunStateTest(unittest.TestCase):
    def test_a_run_that_reached_the_end_of_the_job_is_recorded_and_has_no_interruption(self) -> None:
        document = record()
        self.assertEqual(("recorded", "host"), (document["run_state"], document["mode"]))
        self.assertEqual({"cause": "", "step": ""}, document["interruption"])
        self.assertEqual(4, document["schema"])

    def test_an_interrupted_run_says_by_a_fixed_cause_and_step_and_still_has_what_was_collected(self) -> None:
        document = record(run_state="interrupted", cause="deadline", step="job_watch", job=None)
        self.assertEqual("interrupted", document["run_state"])
        self.assertEqual({"cause": "deadline", "step": "job_watch"}, document["interruption"])
        self.assertEqual(3, len(document["samples"]))
        self.assertIsNone(document["workload"]["exit_code"])

    def test_the_mode_is_host_or_local(self) -> None:
        self.assertEqual("local", record(mode="local")["mode"])


class SampleTest(unittest.TestCase):
    def test_each_sample_has_its_own_time_the_interval_it_covers_and_what_was_running(self) -> None:
        first = record()["samples"][0]
        self.assertEqual((1001, 901, 1001), (first["taken_ns"], first["covers_start_ns"], first["covers_end_ns"]))
        self.assertEqual({"postgres": True, "nessie": True, "spark": False}, first["services_running"])
        self.assertIs(False, first["spark_running"])
        self.assertEqual(900 * MIB, record()["samples"][1]["containers_bytes"]["spark"])

    def test_a_figure_that_could_not_be_read_is_named_in_the_samples_own_list_and_nowhere_else(self) -> None:
        document = record(samples=[sample(1, missing=("host_figures",)), sample(2)])
        self.assertEqual(["host_figures"], document["samples"][0]["missing"])
        self.assertEqual([], document["samples"][1]["missing"])

    def test_the_host_figures_are_written_as_read_and_are_absent_when_not_read(self) -> None:
        document = record(samples=[sample(1, host=figures()), sample(2)])
        self.assertEqual(20480000, document["samples"][0]["host"]["memory_available_kib"])
        self.assertIsNone(document["samples"][1]["host"])

    def test_the_record_holds_the_cgroup_files_as_read_with_the_read_time(self) -> None:
        document = record()
        self.assertEqual(
            {"read_at_ns": 2000, "memory_peak_bytes": 123, "memory_max_bytes": 4096 * MIB, "memory_events": {"low": 0, "max": 2, "oom": 0, "oom_kill": 0}},
            document["spark_cgroup"][0],
        )
        self.assertEqual(1000, document["services_at_start"]["postgres"]["cgroup"]["read_at_ns"])
        self.assertEqual(
            (True, 0, 1024 * MIB),
            (
                document["services_at_end"]["nessie"]["running"],
                document["services_at_end"]["nessie"]["restarts"],
                document["services_at_end"]["nessie"]["docker_memory_cap_bytes"],
            ),
        )

    def test_the_configured_limits_are_copied_beside_the_figures_as_plain_values(self) -> None:
        document = record()
        self.assertEqual({"host_reserve_mib": 4096, "limits_mib": LIMITS}, document["configured"])
        self.assertEqual(list(REQUIRED), document["required_services"])

    def test_the_events_and_the_windows_are_written_as_read(self) -> None:
        document = record()
        self.assertEqual([{"action": "die", "service": "spark", "detail": "exitCode 0", "time_ns": 5}], document["events"])
        self.assertEqual([{"since_ns": 100, "until_ns": 200}, {"since_ns": 200, "until_ns": 300}], document["event_windows"])

    def test_the_job_is_written_as_read_with_its_documented_result_only(self) -> None:
        workload = record()["workload"]
        self.assertEqual((0, False, 20.0, GOOD_RESULT), (workload["exit_code"], workload["oom_killed_flag"], workload["seconds"], workload["result"]))

    def test_the_record_holds_no_text_that_could_be_a_uri_or_a_response_body(self) -> None:
        for text in every_string(record(cleanup=[CleanupStep("nessie branch", False, "Nessie answered with status 500")])):
            self.assertNotRegex(text, r"https?://")

    def test_the_record_holds_no_value_that_json_cannot_carry(self) -> None:
        json.dumps(record(), allow_nan=False)


class GapTest(unittest.TestCase):
    def test_a_run_with_nothing_missing_has_no_gap(self) -> None:
        self.assertEqual([], record()["gaps"])

    def test_windows_that_do_not_chain_from_the_start_to_after_the_exit_are_counted(self) -> None:
        self.assertEqual(0, event_window_holes([(100, 200), (200, 300)], 100, 250))
        self.assertEqual(1, event_window_holes([], 100, 250))
        self.assertEqual(1, event_window_holes([(101, 300)], 100, 250))
        self.assertEqual(1, event_window_holes([(100, 200), (201, 300)], 100, 250))
        self.assertEqual(1, event_window_holes([(100, 200)], 100, 250))
        self.assertEqual(3, event_window_holes([(150, 200), (201, 240)], 100, 250))

    def test_the_gap_for_events_carries_the_count_and_nothing_else(self) -> None:
        self.assertEqual([{"code": "events_window_missing", "count": 1}], record(windows=[(100, 200), (201, 300)])["gaps"])

    def test_a_sample_error_is_counted_and_does_not_stop_the_record(self) -> None:
        self.assertEqual([{"code": "sample_error", "count": 3}], record(sample_errors=3)["gaps"])

    def test_a_run_in_which_the_spark_container_was_never_seen_says_so(self) -> None:
        samples = [sample(1, spark=False), sample(2, spark=False)]
        self.assertEqual(["spark_not_seen"], codes(record(samples=samples)))

    def test_an_unreadable_cgroup_is_counted_on_the_host_from_the_loop_and_from_the_end_snapshots_and_never_in_local_mode(self) -> None:
        unreadable = dataclasses.replace(snapshot(), cgroup=None)
        self.assertEqual([{"code": "cgroup_unreadable", "count": 1}], record(cgroup_unreadable=1)["gaps"])
        self.assertEqual([{"code": "cgroup_unreadable", "count": 2}], record(end_snapshot={"postgres": unreadable, "nessie": unreadable})["gaps"])
        stopped = dataclasses.replace(snapshot(), running=False, cgroup=None)
        self.assertEqual([], record(end_snapshot={"postgres": stopped, "nessie": snapshot()})["gaps"])
        self.assertEqual([], record(mode="local", end_snapshot={"postgres": unreadable, "nessie": unreadable})["gaps"])

    def test_each_cleanup_step_that_could_not_be_verified_gets_its_own_gap_with_the_step_name(self) -> None:
        steps = [
            CleanupStep("job container", False, "still present"),
            CleanupStep("scratch volume", True, "no volume remains"),
            CleanupStep("nessie branch", False, "x"),
        ]
        self.assertEqual(
            [{"code": "cleanup_step_failed", "step": "job container"}, {"code": "cleanup_step_failed", "step": "nessie branch"}], record(cleanup=steps)["gaps"]
        )

    def test_a_job_result_that_could_not_be_read_is_a_gap_and_a_run_with_no_job_is_not(self) -> None:
        self.assertEqual(["job_result_unreadable"], codes(record(job=JobFacts(0, False, 1.0, {}, False))))
        self.assertEqual([], codes(record(run_state="interrupted", cause="stop_flag", step="job_watch", job=None, windows=[(100, 300)], job_exit_ns=None)))

    def test_every_code_a_run_can_write_is_in_the_fixed_list(self) -> None:
        unreadable = dataclasses.replace(snapshot(), cgroup=None)
        document = record(
            samples=[sample(1, spark=False)],
            windows=[],
            sample_errors=1,
            cgroup_unreadable=1,
            end_snapshot={"postgres": unreadable, "nessie": unreadable},
            cleanup=[CleanupStep("job container", False, "x")],
            job=JobFacts(0, False, 1.0, {}, False),
        )
        self.assertEqual(set(GAP_CODES), set(codes(document)))

    def test_gaps_of_is_the_list_the_record_holds(self) -> None:
        run = collected(sample_errors=2)
        self.assertEqual(
            gaps_of(run),
            build_record(
                run,
                limits_mib=LIMITS,
                reserve_mib=1,
                required=REQUIRED,
                interval_seconds=1.0,
                deadline_seconds=1.0,
                started_utc="a",
                finished_utc="b",
                job_arguments=[],
            )["gaps"],
        )


class ResultTest(unittest.TestCase):
    def test_the_documented_fields_are_kept_as_read_and_nothing_else(self) -> None:
        self.assertEqual(GOOD_RESULT, read_result({**GOOD_RESULT, "extra": "dropped"}))

    def test_a_result_with_a_missing_or_mistyped_field_is_not_readable_and_a_value_is_never_compared(self) -> None:
        self.assertIsNone(read_result({key: value for key, value in GOOD_RESULT.items() if key != "uid"}))
        for change in ({"rows": "10"}, {"rows": True}, {"snapshot_id": None}, {"table": "x" * 201}, {"python_version": ""}):
            with self.subTest(change=change):
                self.assertIsNone(read_result({**GOOD_RESULT, **change}))
        self.assertEqual({**GOOD_RESULT, "rows": 9, "branch": "another"}, read_result({**GOOD_RESULT, "rows": 9, "branch": "another"}))


class DestinationTest(unittest.TestCase):
    def test_the_record_is_written_once_and_a_second_write_to_the_same_folder_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = write_document(Path(folder) / "run", record())
            self.assertEqual("recorded", json.loads(path.read_text())["run_state"])
            self.assertEqual([path.name], [item.name for item in path.parent.iterdir()])
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                write_document(Path(folder) / "run", record())

    def test_the_destination_is_checked_before_anything_starts_and_the_check_creates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            fresh = Path(folder) / "new" / "run"
            check_destination(fresh)
            self.assertFalse((Path(folder) / "new").exists())
            write_document(fresh, record())
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                check_destination(fresh)

    def test_a_dangling_link_at_the_record_path_or_among_its_parents_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            dangling = Path(folder) / "dangling"
            dangling.mkdir()
            (dangling / "capacity_probe.json").symlink_to(Path(folder) / "nowhere")
            with self.assertRaisesRegex(ProbeError, "already holds a record"):
                check_destination(dangling)
            (Path(folder) / "link").symlink_to(Path(folder) / "nowhere")
            for target in (Path(folder) / "link", Path(folder) / "link" / "run"):
                with self.subTest(target=target), self.assertRaisesRegex(ProbeError, "cannot write the record"):
                    check_destination(target)

    def test_a_parent_that_is_a_file_or_cannot_be_written_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
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
