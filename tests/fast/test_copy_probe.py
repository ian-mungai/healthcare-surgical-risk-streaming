"""The copy probe's checks: the pinned selection, the landed payloads against the pins, the Airbyte job and its log, and the Platform sample.

Each refusal has a passing control and is judged by its own notice code, so a test fails for its intended cause only. The stand-in store records every read,
so a case also proves that no payload was read after the manifest check, and that nothing is derived before the passed manifest exists.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Sequence
from dataclasses import replace

from scripts.ingestion import copy_probe
from scripts.ingestion.check import CheckedFile, run_check
from scripts.ingestion.copy_probe import (
    PROBE_CASES,
    PROBE_NAME,
    Pins,
    ProbeRefused,
    job_problems,
    landed_problems,
    probe_selection,
    read_job,
    sample_problems,
    tracked_pins,
    verify_landed,
)
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import ManifestError, ManifestRef, publish_manifest
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.selection import SelectionError, pinned_release
from scripts.sample.candidates import Candidate
from scripts.sample.split import is_development
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, digest, landed_set, pin_the_synthetic_list, selection_for, tracked_pin

TABLE_KEY = SOURCE_PREFIX + "clinical_data.csv"


def setUpModule() -> None:
    pin_the_synthetic_list()


def synthetic_pins(landed: Landed) -> Pins:
    """The pins of the synthetic release: the checksum and size of each landed source file, one candidate for each case file and one table."""
    cases = [
        Candidate(number, number, f"{SOURCE_PREFIX}vital_files/{number:04d}.vital", len(data), digest(data), True, "slot")
        for number, data in ((1, SOURCE["vital_files/0001.vital"]), (2, SOURCE["vital_files/0002.vital"]))
    ]
    table = SOURCE["clinical_data.csv"]
    return Pins(tuple(cases), {TABLE_KEY: (len(table), digest(table))})


def published(landed: Landed) -> ManifestRef:
    return publish_manifest(landed.store, landed.selection, landed.check())


def codes(notices: Sequence[Notice]) -> list[Code]:
    return [notice.code for notice in notices]


def entries_of(landed: Landed) -> tuple[CheckedFile, ...]:
    return landed.check().files


class TrackedPinsTest(unittest.TestCase):
    def test_the_probe_cases_are_development_candidates_of_the_tracked_file_and_never_case_565(self) -> None:
        pins = tracked_pins()
        by_case = {item.case: item for item in pins.candidates}
        self.assertTrue(set(PROBE_CASES) <= set(by_case))
        self.assertTrue(all(is_development(by_case[case].patient) for case in PROBE_CASES))
        self.assertNotIn(565, PROBE_CASES)
        self.assertEqual(len(PROBE_CASES), len(set(PROBE_CASES)))

    def test_the_probe_cases_meet_the_rules_of_the_platform_sample(self) -> None:
        pins = tracked_pins()
        by_case = {item.case: item for item in pins.candidates}
        entries = [landed_entry(by_case[case]) for case in PROBE_CASES]
        self.assertEqual([], sample_problems(entries, pins))

    def test_the_tracked_pins_hold_both_tables_of_the_release(self) -> None:
        with tracked_pin():
            pins = tracked_pins()
            release = pinned_release()
        self.assertEqual({release.source_prefix + "clinical_data.csv", release.source_prefix + "lab_data.csv"}, set(pins.tables))
        for size, sha256 in pins.tables.values():
            self.assertGreater(size, 0)
            self.assertEqual(64, len(sha256))


class SelectionTest(unittest.TestCase):
    def test_control_the_selection_is_the_two_tables_and_the_pinned_cases_of_the_release(self) -> None:
        with tracked_pin():
            selection = probe_selection()
            release = pinned_release()
        expected = {release.source_prefix + name for name in ("clinical_data.csv", "lab_data.csv")} | {
            f"{release.source_prefix}vital_files/{case:04d}.vital" for case in PROBE_CASES
        }
        self.assertEqual(PROBE_NAME, selection.name)
        self.assertEqual(expected, set(selection.source_keys))
        self.assertEqual(release.checksum_list_sha256, selection.checksum_list_sha256)

    def test_a_test_patient_case_is_refused_for_the_cause_that_it_is_not_development_data(self) -> None:
        with self.assertRaises(SelectionError) as refused, tracked_pin():
            probe_selection((908, 565))
        self.assertEqual([Code.PROBE_CASE_NOT_DEVELOPMENT], codes(refused.exception.notices))
        self.assertEqual(2, refused.exception.notices[0].position)

    def test_a_case_that_is_not_a_candidate_is_refused_for_that_cause(self) -> None:
        with self.assertRaises(SelectionError) as refused, tracked_pin():
            probe_selection((908, 1))
        self.assertEqual([Code.PROBE_CASE_NOT_CANDIDATE], codes(refused.exception.notices))

    def test_a_case_named_twice_is_refused(self) -> None:
        with self.assertRaises(SelectionError) as refused, tracked_pin():
            probe_selection((908, 908))
        self.assertEqual([Code.FILE_TWICE], codes(refused.exception.notices))


class LandedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.landed = landed_set()
        self.pins = synthetic_pins(self.landed)

    def test_control_exactly_the_selected_keys_at_their_pinned_checksums_pass(self) -> None:
        self.assertEqual([], landed_problems(entries_of(self.landed), self.landed.selection, self.pins))

    def test_a_payload_that_is_not_in_the_selection_is_refused_for_the_key_set(self) -> None:
        extra = replace(entries_of(self.landed)[0], source_key=SOURCE_PREFIX + "vital_files/0003.vital")
        problems = landed_problems((*entries_of(self.landed), extra), self.landed.selection, self.pins)
        self.assertEqual([Code.PROBE_KEYS], codes(problems))
        self.assertEqual((1, 3), (problems[0].count, problems[0].total))

    def test_a_payload_that_is_missing_is_refused_for_the_key_set(self) -> None:
        problems = landed_problems(entries_of(self.landed)[1:], self.landed.selection, self.pins)
        self.assertEqual([Code.PROBE_KEYS], codes(problems))
        self.assertEqual(1, problems[0].count)

    def test_a_payload_listed_twice_is_refused_for_the_key_set(self) -> None:
        entries = entries_of(self.landed)
        self.assertEqual([Code.PROBE_KEYS], codes(landed_problems((*entries, entries[0]), self.landed.selection, self.pins)))

    def test_a_null_version_id_is_refused_for_that_payload(self) -> None:
        entries = list(entries_of(self.landed))
        entries[1] = replace(entries[1], version_id="null")
        problems = landed_problems(entries, self.landed.selection, self.pins)
        self.assertEqual([Code.PROBE_NULL_VERSION], codes(problems))
        self.assertEqual(2, problems[0].position)

    def test_an_empty_version_id_is_refused_too(self) -> None:
        entries = list(entries_of(self.landed))
        entries[0] = replace(entries[0], version_id="")
        self.assertEqual([Code.PROBE_NULL_VERSION], codes(landed_problems(entries, self.landed.selection, self.pins)))

    def test_a_checksum_that_differs_from_the_pin_is_refused_for_that_payload(self) -> None:
        entries = list(entries_of(self.landed))
        entries[0] = replace(entries[0], sha256=digest(b"other"))
        problems = landed_problems(entries, self.landed.selection, self.pins)
        self.assertEqual([Code.PROBE_PIN_DIFFERS], codes(problems))
        self.assertEqual(1, problems[0].position)

    def test_a_size_that_differs_from_the_pin_is_refused_for_that_payload(self) -> None:
        entries = list(entries_of(self.landed))
        entries[2] = replace(entries[2], size=entries[2].size + 1)
        problems = landed_problems(entries, self.landed.selection, self.pins)
        self.assertEqual([Code.PROBE_PIN_DIFFERS], codes(problems))
        self.assertEqual(3, problems[0].position)

    def test_a_table_whose_pin_differs_is_refused_for_that_payload(self) -> None:
        pins = Pins(self.pins.candidates, {TABLE_KEY: (self.pins.tables[TABLE_KEY][0], digest(b"another table"))})
        self.assertEqual([Code.PROBE_PIN_DIFFERS], codes(landed_problems(entries_of(self.landed), self.landed.selection, pins)))

    def test_a_payload_that_no_pin_names_is_refused_for_the_pin(self) -> None:
        pins = Pins(self.pins.candidates[:1], self.pins.tables)
        self.assertEqual([Code.PROBE_PIN_DIFFERS], codes(landed_problems(entries_of(self.landed), self.landed.selection, pins)))


class VerifyLandedTest(unittest.TestCase):
    def test_control_a_published_manifest_passes_and_no_payload_is_read(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.reads.clear()
        entries = verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE}, {entry.source_key for entry in entries})
        self.assertTrue(all(entry.version_id not in ("", "null") for entry in entries))
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))

    def test_an_extra_current_payload_in_the_landing_prefix_is_refused_for_that_cause(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_object(f"{LANDING_PREFIX}sync_99/{SOURCE_PREFIX}vital_files/0099.vital", b"a payload that no check covered")
        landed.store.reads.clear()
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual([Code.PROBE_REFUSED, Code.PROBE_LANDING_EXTRA], codes(refused.exception.notices))
        self.assertEqual(1, refused.exception.notices[1].count)
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))

    def test_a_version_that_the_caller_names_as_left_by_an_earlier_sync_is_no_extra_and_any_other_extra_is_still_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        key = f"{LANDING_PREFIX}sync_98/{SOURCE_PREFIX}vital_files/0001.vital"
        left = landed.store.put_object(key, b"left by the interrupted sync")
        landed.store.reads.clear()
        named = frozenset({(key, left.version_id), (f"{LANDING_PREFIX}sync_98/gone", "version_of_nothing")})
        self.assertEqual(len(SOURCE), len(verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed), named)))
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))
        landed.store.put_object(f"{LANDING_PREFIX}sync_99/{SOURCE_PREFIX}vital_files/0099.vital", b"a payload that no sync of the caller wrote")
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed), named)
        self.assertEqual([Code.PROBE_REFUSED, Code.PROBE_LANDING_EXTRA], codes(refused.exception.notices))
        self.assertEqual(1, refused.exception.notices[1].count)

    def test_a_newer_version_at_a_key_that_an_earlier_sync_left_is_an_extra_payload(self) -> None:
        landed = landed_set()
        reference = published(landed)
        key = f"{LANDING_PREFIX}sync_98/{SOURCE_PREFIX}vital_files/0001.vital"
        named = frozenset({(key, landed.store.put_object(key, b"left by the interrupted sync").version_id)})
        landed.store.put_object(key, b"written after the caller read what was left")
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed), named)
        self.assertEqual([Code.PROBE_REFUSED, Code.PROBE_LANDING_EXTRA], codes(refused.exception.notices))
        self.assertEqual(1, refused.exception.notices[1].count)

    def test_a_metadata_file_in_the_landing_prefix_is_no_payload(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_object(f"{LANDING_PREFIX}sync_00/{SOURCE_PREFIX}vital_files/0001.vital.jsonl", b"{}")
        self.assertEqual(len(SOURCE), len(verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))))

    def test_an_extra_payload_that_was_replaced_by_a_delete_marker_is_no_current_object(self) -> None:
        landed = landed_set()
        reference = published(landed)
        extra = f"{LANDING_PREFIX}sync_99/{SOURCE_PREFIX}vital_files/0099.vital"
        landed.store.put_object(extra, b"x")
        landed.store.put_delete_marker(extra)
        self.assertEqual(len(SOURCE), len(verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))))

    def test_a_manifest_payload_that_is_no_current_object_is_refused_for_that_cause(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_delete_marker(landed.key_for(SOURCE_PREFIX + "vital_files/0001.vital"))
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual([Code.PROBE_REFUSED, Code.PROBE_LANDING_MISSING], codes(refused.exception.notices))
        self.assertEqual(1, refused.exception.notices[1].count)

    def test_a_missing_manifest_is_refused_before_anything_is_derived(self) -> None:
        landed = landed_set()
        reference = published(landed)
        for version in landed.store.list_versions(reference.key):
            landed.store.remove_version(version.key, version.version_id)
        landed.store.reads.clear()
        with self.assertRaises((ProbeRefused, ManifestError, GateRefused)):
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))

    def test_a_version_substituted_after_the_manifest_is_refused_by_the_gate(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.land("vital_files/0001.vital", b"a substituted copy of the recording")
        with self.assertRaises(GateRefused):
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))

    def test_a_manifest_of_another_selection_is_refused_for_that_cause(self) -> None:
        landed = landed_set()
        other = selection_for(list(SOURCE), landed.list_bytes, name="other_set")
        reference = publish_manifest(landed.store, other, run_check(landed.store, other, landed.list_bytes, landed.pairings()))
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual([Code.PROBE_NOT_THIS], codes(refused.exception.notices))

    def test_a_manifest_of_fewer_files_than_the_probe_selection_is_refused_for_that_cause(self) -> None:
        landed = landed_set()
        smaller = selection_for(["vital_files/0001.vital", "clinical_data.csv"], landed.list_bytes, name=landed.selection.name)
        reference = publish_manifest(landed.store, smaller, run_check(landed.store, smaller, landed.list_bytes, landed.pairings()))
        with self.assertRaises(ProbeRefused) as refused:
            verify_landed(landed.store, reference, landed.selection, synthetic_pins(landed))
        self.assertEqual([Code.PROBE_NOT_THIS], codes(refused.exception.notices))


JOB = {"jobId": 7, "status": "succeeded", "jobType": "sync", "connectionId": "c1", "rowsSynced": 5, "bytesSynced": 2_048_000, "duration": "PT2M"}
LOG = "2026-11-24 INFO started\n2026-11-24 INFO copied 5 files\n2026-11-24 INFO completed\n"


def job_notices(job: dict[str, object], log: str, files: int = 5) -> list[Notice]:
    return job_problems(read_job(json.dumps(job)), log, files)


class JobTest(unittest.TestCase):
    def test_control_a_succeeded_job_that_synced_every_file_with_a_clean_log_passes(self) -> None:
        self.assertEqual([], job_notices(JOB, LOG))

    def test_a_job_that_did_not_succeed_is_refused_for_its_status(self) -> None:
        self.assertEqual([Code.JOB_STATUS], codes(job_notices({**JOB, "status": "failed"}, LOG)))

    def test_a_job_that_synced_fewer_records_than_files_is_refused_for_the_count(self) -> None:
        problems = job_notices({**JOB, "rowsSynced": 4}, LOG)
        self.assertEqual([Code.JOB_ROWS], codes(problems))
        self.assertEqual((4, 5), (problems[0].count, problems[0].total))

    def test_a_job_that_synced_more_records_than_files_is_refused_for_the_count(self) -> None:
        self.assertEqual([Code.JOB_ROWS], codes(job_notices({**JOB, "rowsSynced": 6}, LOG)))

    def test_a_job_that_synced_no_bytes_is_refused_for_the_bytes(self) -> None:
        self.assertEqual([Code.JOB_BYTES], codes(job_notices({**JOB, "bytesSynced": 0}, LOG)))

    def test_an_empty_log_is_refused(self) -> None:
        self.assertEqual([Code.LOG_EMPTY], codes(job_notices(JOB, "  \n")))

    def test_an_error_line_in_the_log_is_named_by_its_number(self) -> None:
        problems = job_notices(JOB, LOG.replace("copied 5 files", "ERROR could not write a file"))
        self.assertEqual([Code.LOG_ERROR_LINE], codes(problems))
        self.assertEqual(2, problems[0].position)

    def test_a_trace_in_the_log_is_an_error_line_too(self) -> None:
        self.assertEqual([Code.LOG_ERROR_LINE], codes(job_notices(JOB, LOG + "Traceback (most recent call last):\n")))

    def test_a_trace_and_an_error_after_the_capture_prefix_are_error_lines(self) -> None:
        prefix = "[pod/replication-job-7-attempt-0/main] 2026-11-23T17:01:02.123456789Z "
        for line in ("Traceback (most recent call last):", "ERROR could not write", "FATAL out of memory"):
            for head in (
                prefix,
                "[pod/replication-job-7-attempt-0/main] ",
                "2026-11-23T17:01:02.123456789Z ",
                "2026-11-23T17:45:27.123456789+00:00 ",
                "[pod/replication-job-7-attempt-0/main] 2026-11-23T17:45:27.123456789-08:00 ",
                "",
            ):
                with self.subTest(head=head, line=line):
                    self.assertEqual([Code.LOG_ERROR_LINE], codes(job_notices(JOB, LOG + head + line + "\n")))

    def test_a_bracketed_level_is_no_capture_prefix_so_its_error_and_fatal_lines_stay_error_lines(self) -> None:
        for line in ("[ERROR] uploaded object is incomplete", "[FATAL] could not write the object", "[pod/x/y] [ERROR] uploaded object is incomplete"):
            with self.subTest(line=line):
                self.assertEqual([Code.LOG_ERROR_LINE], codes(job_notices(JOB, LOG + line + "\n")))

    def test_a_bracketed_token_that_is_no_capture_prefix_is_not_stripped_from_a_trace(self) -> None:
        for line in ("[worker] Traceback (most recent call last):", "[pod/only-a-pod] Traceback (most recent call last):"):
            with self.subTest(line=line):
                self.assertEqual([], codes(job_notices(JOB, LOG + line + "\n")))

    def test_prefixed_lines_that_only_mention_a_trace_are_no_error_lines(self) -> None:
        prefix = "[pod/replication-job-7-attempt-0/main] 2026-11-23T17:01:02.123456789Z "
        self.assertEqual([], job_notices(JOB, LOG + prefix + "INFO copied 5 files\n" + prefix + "INFO the word Traceback is in a file name\n"))

    def test_a_word_that_only_contains_error_is_no_error_line(self) -> None:
        self.assertEqual([], job_notices(JOB, LOG + "INFO file errors_report.csv copied\n"))

    def test_each_fault_of_a_run_is_named_in_one_pass(self) -> None:
        problems = job_notices({**JOB, "status": "failed", "rowsSynced": 0, "bytesSynced": 0}, "")
        self.assertEqual([Code.JOB_STATUS, Code.JOB_ROWS, Code.JOB_BYTES, Code.LOG_EMPTY], codes(problems))

    def test_a_record_that_is_not_a_job_is_refused_by_its_form(self) -> None:
        for text in (
            "[]",
            "{}",
            json.dumps({**JOB, "rowsSynced": "5"}),
            json.dumps({**JOB, "rowsSynced": True}),
            json.dumps({k: v for k, v in JOB.items() if k != "bytesSynced"}),
            "not json",
        ):
            with self.subTest(text=text), self.assertRaises(ProbeRefused):
                read_job(text)


def candidate(case: int, patient: int, *, arterial: bool = True, slot: str = "slot") -> Candidate:
    return Candidate(case, patient, f"{SOURCE_PREFIX}vital_files/{case:04d}.vital", 1000 + case, f"{case:064x}", arterial, slot)


def landed_entry(item: Candidate) -> CheckedFile:
    return CheckedFile("bucket", f"landing/{item.key}", f"v{item.case}", item.sha256, item.key, item.size_bytes, "1.0.0", "2026-01-01T00:00:00Z")


def sample_notices(candidates: Sequence[Candidate], landed: Sequence[Candidate] | None = None) -> list[Notice]:
    chosen = candidates if landed is None else landed
    return sample_problems([landed_entry(item) for item in chosen], Pins(tuple(candidates), {}))


def development_patients(count: int) -> list[int]:
    found = [patient for patient in range(1, 400) if is_development(patient)]
    return found[:count]


class SampleTest(unittest.TestCase):
    def setUp(self) -> None:
        first, second, third = development_patients(3)
        self.repeat_a = candidate(10, first)
        self.repeat_b = candidate(11, first)
        self.plain = candidate(12, second, arterial=False)
        self.short = candidate(13, third, slot="short_development")
        self.full = [self.repeat_a, self.repeat_b, self.plain, self.short]

    def test_control_development_cases_with_a_repeat_patient_both_kinds_of_signal_and_a_short_case_pass(self) -> None:
        self.assertEqual([], sample_notices(self.full))

    def test_a_test_patient_case_is_refused_for_the_development_data_rule(self) -> None:
        test_patient = next(patient for patient in range(1, 400) if not is_development(patient))
        problems = sample_notices([*self.full, candidate(14, test_patient)])
        self.assertEqual([Code.SAMPLE_NOT_DEVELOPMENT], codes(problems))
        self.assertEqual(5, problems[0].position)

    def test_cases_with_no_repeat_patient_are_refused_for_that_cause(self) -> None:
        self.assertEqual([Code.SAMPLE_NO_REPEAT], codes(sample_notices([self.repeat_a, self.plain, self.short])))

    def test_cases_that_all_have_the_arterial_waveform_are_refused_for_that_cause(self) -> None:
        self.assertEqual([Code.SAMPLE_NO_NON_ARTERIAL], codes(sample_notices([self.repeat_a, self.repeat_b, self.short])))

    def test_cases_with_no_arterial_waveform_are_refused_for_that_cause(self) -> None:
        plain_a = replace(self.repeat_a, arterial_waveform=False)
        plain_b = replace(self.repeat_b, arterial_waveform=False)
        self.assertEqual([Code.SAMPLE_NO_ARTERIAL], codes(sample_notices([plain_a, plain_b, replace(self.short, arterial_waveform=False)])))

    def test_cases_without_the_short_case_are_refused_for_that_cause(self) -> None:
        self.assertEqual([Code.SAMPLE_NO_SHORT], codes(sample_notices([self.repeat_a, self.repeat_b, self.plain])))

    def test_a_landed_case_that_is_not_a_candidate_is_refused_for_that_cause(self) -> None:
        stranger = candidate(99, development_patients(1)[0])
        problems = sample_notices(self.full, [*self.full, stranger])
        self.assertEqual([Code.SAMPLE_NOT_CANDIDATE], codes(problems))
        self.assertEqual(5, problems[0].position)

    def test_a_landed_checksum_that_differs_from_the_candidate_is_refused_for_that_cause(self) -> None:
        entries = [landed_entry(item) for item in self.full]
        entries[0] = replace(entries[0], sha256=digest(b"x"))
        self.assertEqual([Code.PROBE_PIN_DIFFERS], codes(sample_problems(entries, Pins(tuple(self.full), {}))))

    def test_tables_among_the_landed_files_are_not_cases(self) -> None:
        table = CheckedFile("bucket", "landing/t", "v1", digest(b"t"), TABLE_KEY, 1, "1.0.0", "2026-01-01T00:00:00Z")
        entries = [*(landed_entry(item) for item in self.full), table]
        self.assertEqual([], sample_problems(entries, Pins(tuple(self.full), {TABLE_KEY: (1, digest(b"t"))})))


class FormatTest(unittest.TestCase):
    def test_the_pin_file_holds_each_case_with_its_key_size_checksum_and_slot_and_no_version_id(self) -> None:
        items = [candidate(10, 1), candidate(11, 1, slot="short_development")]
        text = copy_probe.format_sample(items)
        document = json.loads(text)
        self.assertEqual([10, 11], [entry["case"] for entry in document["cases"]])
        self.assertEqual(set(document["cases"][0]), {"case", "patient", "key", "size_bytes", "sha256", "arterial_waveform", "slot"})
        self.assertNotIn("version", text)
        self.assertEqual(text, copy_probe.format_sample(list(reversed(items))))


if __name__ == "__main__":
    unittest.main()
