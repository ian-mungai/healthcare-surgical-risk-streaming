"""The manifest of expected events and its comparison with what Kafka and the replay's admissions hold.

The manifest is derived from the decoded files and the frozen schedule alone, with its own code for the waveform seconds and the payloads, so it can fail a
replay that the replay's own code would pass. These tests hold the two derivations equal on the boundary cases, then plant each fault that the comparison
must name by its ID.

Failure modes covered here:

- The manifest and the replay derive the same event differently, so a reading is expected under an ID that is never sent (or the reverse) and nothing notices.
- A waveform second is assigned differently by the two derivations at a second boundary or at a rate that is not a whole number.
- A skipped in-window occurrence, a missing in-window case-end marker or a skipped first occurrence is not reported, or is reported without its ID.
- A changed payload under the right ID, an ID that was never expected, or two different payloads under one ID pass as the same event.
- A repeated send of one ID with one payload is reported as a failure, so a restart that resends cannot pass.
- The first actual send is not checked against the event due at the frozen first instant.
- A string event or a marker lacks its payload in the manifest, so its text or kind cannot be compared.

The numbers of the generated waveform records are timestamps and sample indices that stress the arithmetic. They are not readings.
"""

from __future__ import annotations

import hashlib
import json
import unittest
from array import array
from collections.abc import Iterable
from dataclasses import replace

from replay.events.manifest import Manifest, ManifestError, Observed, build_manifest, compare, reference_seconds
from replay.events.payload import payload_digest
from replay.events.recording import WaveformRecord, WaveformTrack
from replay.events.schedule import FrozenSchedule, admitted_occurrences
from replay.events.stream import occurrence_events, outgoing
from replay.events.waveform import second_samples, second_spans
from tests.fast.replay_support import RUN_ID, SECOND, boundary_recordings, boundary_schedule


def replayed(frozen: FrozenSchedule) -> Observed:
    """What a replay that follows its schedule sends, derived with the replay's own code, one message for each event."""
    recordings = boundary_recordings()
    digests: dict[str, list[str]] = {}
    first: tuple[int, str] | None = None
    admitted = set()
    for scheduled in admitted_occurrences(frozen):
        admitted.add(scheduled.occurrence.id)
        recording = recordings[scheduled.occurrence.case_id]
        for event in occurrence_events(recording):
            sent = outgoing(RUN_ID, scheduled, recording, event)
            digests.setdefault(sent.event_id, []).append(payload_digest(sent.payload))
            if first is None or (sent.due_us, sent.event_id) < first:
                first = (sent.due_us, sent.event_id)
    return Observed(digests, frozenset(admitted), first[1] if first else None)


def without(observed: Observed, ids: Iterable[str]) -> Observed:
    gone = set(ids)
    return replace(observed, events={key: value for key, value in observed.events.items() if key not in gone})


def ids_of(manifest: Manifest, occurrence_id: str) -> set[str]:
    return {event.event_id for event in manifest.events.values() if event.occurrence_id == occurrence_id}


class Derivation(unittest.TestCase):
    def setUp(self) -> None:
        self.frozen = boundary_schedule(rooms=2, window_us=40 * SECOND)
        self.manifest = build_manifest(self.frozen, boundary_recordings())

    def test_the_manifest_holds_the_same_ids_and_payload_digests_as_the_replay_derives(self) -> None:
        observed = replayed(self.frozen)
        self.assertEqual(set(self.manifest.events), set(observed.events))
        self.assertEqual(
            {key: payload.payload_sha256 for key, payload in self.manifest.events.items()}, {key: value[0] for key, value in observed.events.items()}
        )

    def test_the_manifest_holds_every_event_of_every_admitted_occurrence_and_no_other(self) -> None:
        admitted = {item.occurrence.id for item in admitted_occurrences(self.frozen)}
        self.assertEqual(admitted, {event.occurrence_id for event in self.manifest.events.values()})
        self.assertEqual(admitted, set(self.manifest.occurrences))
        self.assertEqual(
            19 * len([item for item in admitted if item.startswith("9001")]) + 9 * len([item for item in admitted if item.startswith("9002")]),
            len(self.manifest.events),
        )

    def test_a_string_event_and_a_marker_keep_their_payload_and_a_reading_keeps_only_its_digest(self) -> None:
        started = next(event for event in self.manifest.events.values() if event.kind == "string" and event.event_id.endswith("|Test/EVENT|0|0"))
        self.assertEqual("Case started", (started.payload or {}).get("text"))
        for event in self.manifest.events.values():
            if event.kind in ("string", "op_start", "final_score", "case_end"):
                self.assertIsNotNone(event.payload, event.event_id)
            else:
                self.assertIsNone(event.payload, event.event_id)

    def test_the_markers_and_the_string_events_are_distinct_kinds_with_distinct_ids(self) -> None:
        kinds = {event.kind for event in self.manifest.events.values()}
        self.assertEqual({"numeric", "waveform", "string", "op_start", "final_score", "case_end"}, kinds)
        marker_ids = {event.event_id for event in self.manifest.events.values() if event.kind in ("op_start", "final_score", "case_end")}
        source_ids = {event.event_id for event in self.manifest.events.values() if event.kind not in ("op_start", "final_score", "case_end")}
        self.assertFalse(marker_ids & source_ids)

    def test_each_event_is_flagged_by_whether_it_is_due_inside_the_half_open_window(self) -> None:
        for event in self.manifest.events.values():
            self.assertEqual(self.manifest.window_start_us <= event.due_us < self.manifest.window_end_us, event.in_window, event.event_id)
        self.assertTrue(any(not event.in_window for event in self.manifest.events.values()))

    def test_the_events_due_at_the_first_due_instant_are_listed(self) -> None:
        due_first = {event.event_id for event in self.manifest.events.values() if event.due_us == self.manifest.first_due_us}
        self.assertEqual(due_first, set(self.manifest.first_due_event_ids))
        self.assertTrue(due_first)

    def test_the_two_derivations_agree_on_a_case_whose_file_holds_events_outside_its_case(self) -> None:
        frozen = boundary_schedule(rooms=1, window_us=25 * SECOND, cases=(9003,))
        manifest = build_manifest(frozen, boundary_recordings())
        observed = replayed(frozen)
        self.assertEqual(set(manifest.events), set(observed.events))
        self.assertEqual({key: event.payload_sha256 for key, event in manifest.events.items()}, {key: value[0] for key, value in observed.events.items()})
        self.assertEqual(8 * 3, len(manifest.events))
        outside = next(event for event in manifest.events.values() if event.event_id.endswith("|Test/HR|10400000|0") and event.occurrence_id == "9003.0")
        self.assertEqual(10 * SECOND, outside.due_us)

    def test_a_manifest_written_and_read_back_is_the_same_manifest(self) -> None:
        self.assertEqual(self.manifest, Manifest.from_json(self.manifest.to_json()))

    def test_an_event_due_on_the_end_of_the_window_is_not_in_the_window_and_one_due_at_its_start_is(self) -> None:
        manifest = build_manifest(boundary_schedule(rooms=1, window_us=10 * SECOND, cases=(9001,)), boundary_recordings())
        by_id = {key: event for key, event in manifest.events.items()}
        on_end = next(event for key, event in by_id.items() if key.endswith("|Test/NIBP|10000000|0"))
        on_start = next(event for key, event in by_id.items() if key.endswith("|Test/HR|0|0"))
        self.assertEqual((10 * SECOND, False), (on_end.due_us, on_end.in_window))
        self.assertEqual((0, True), (on_start.due_us, on_start.in_window))

    def test_the_manifest_is_the_same_on_every_derivation(self) -> None:
        self.assertEqual(self.manifest, build_manifest(self.frozen, boundary_recordings()))


def draw(label: str, low: int, high: int) -> int:
    """A repeatable whole number in ``[low, high)`` that depends on its label only."""
    return low + int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "big") % (high - low)


class WaveformCrossCheck(unittest.TestCase):
    def test_the_replay_and_the_manifest_assign_every_sample_of_generated_records_to_the_same_second(self) -> None:
        for rate in (62.5, 100.0, 125.0, 250.0, 500.0, 0.3, 7.77, 1 / 3, 1.0, 2.5):
            records = []
            start = draw(f"{rate}:start", -5_000_000, 5_000_000)
            for number in range(12):
                count = draw(f"{rate}:{number}:count", 1, 900)
                records.append(WaveformRecord(start, array("d", (float(index) for index in range(count)))))
                start += draw(f"{rate}:{number}:step", -300_000, 1_800_000)
            track = WaveformTrack("Test/W", "mV", rate, 1.0, 0.0, 2, tuple(records))
            replay_side = {span.second: (span.start_offset_us, list(second_samples(track, span))) for span in second_spans(track)}
            self.assertEqual(reference_seconds(track), replay_side, f"rate {rate}")


class Comparison(unittest.TestCase):
    def setUp(self) -> None:
        self.frozen = boundary_schedule(rooms=2, window_us=120 * SECOND)
        self.manifest = build_manifest(self.frozen, boundary_recordings())
        self.observed = replayed(self.frozen)

    def test_control_a_replay_that_sent_every_event_once_and_admitted_every_occurrence_passes(self) -> None:
        report = compare(self.manifest, self.observed)
        self.assertTrue(report.ok, report.failures())
        self.assertEqual((), report.failures())

    def test_control_a_repeated_send_of_the_same_id_and_payload_is_a_dropped_duplicate_and_passes(self) -> None:
        events = dict(self.observed.events)
        repeated = sorted(events)[:3]
        for key in repeated:
            events[key] = [*events[key], *events[key]]
        report = compare(self.manifest, replace(self.observed, events=events))
        self.assertTrue(report.ok, report.failures())
        self.assertEqual(3, report.duplicate_count)

    def test_a_planted_skipped_in_window_occurrence_fails_by_its_missing_occurrence_id_and_event_ids(self) -> None:
        skipped = ids_of(self.manifest, "9001.1")
        self.assertTrue(skipped)
        report = compare(self.manifest, without(replace(self.observed, admitted=self.observed.admitted - {"9001.1"}), skipped))
        self.assertFalse(report.ok)
        self.assertEqual(("9001.1",), report.missing_occurrences)
        self.assertEqual(tuple(sorted(skipped)), report.missing_ids)
        self.assertEqual(("9001.1",), report.missing_admissions)
        self.assertEqual((), report.partial_occurrences)
        self.assertTrue(any("9001.1" in line for line in report.failures()))

    def test_a_planted_missing_in_window_case_end_marker_fails_by_its_missing_id(self) -> None:
        marker = next(event for event in self.manifest.events.values() if event.occurrence_id == "9002.0" and event.kind == "case_end")
        self.assertTrue(marker.in_window)
        report = compare(self.manifest, without(self.observed, [marker.event_id]))
        self.assertFalse(report.ok)
        self.assertEqual((marker.event_id,), report.missing_ids)
        self.assertEqual(("9002.0",), report.partial_occurrences)
        self.assertEqual((), report.missing_occurrences)
        self.assertIn(marker.event_id, "\n".join(report.failures()))

    def test_a_manifest_file_written_before_the_start_marker_is_refused_and_not_read_as_current(self) -> None:
        old = json.loads(self.manifest.to_json())
        old["format"] = "replay_manifest_1"
        with self.assertRaises(ManifestError):
            Manifest.from_json(json.dumps(old))

    def test_a_planted_missing_op_start_marker_fails_by_its_missing_id(self) -> None:
        marker = next(event for event in self.manifest.events.values() if event.occurrence_id == "9001.0" and event.kind == "op_start")
        report = compare(self.manifest, without(self.observed, [marker.event_id]))
        self.assertEqual((marker.event_id,), report.missing_ids)

    def test_a_planted_missing_final_score_marker_fails_by_its_missing_id(self) -> None:
        marker = next(event for event in self.manifest.events.values() if event.occurrence_id == "9001.0" and event.kind == "final_score")
        report = compare(self.manifest, without(self.observed, [marker.event_id]))
        self.assertEqual((marker.event_id,), report.missing_ids)

    def test_a_skipped_first_occurrence_in_a_bounded_run_fails_by_its_occurrence_id_and_the_first_send(self) -> None:
        frozen = boundary_schedule(rooms=2, stagger_us=5 * SECOND, window_us=40 * SECOND)
        manifest = build_manifest(frozen, boundary_recordings())
        control = replayed(frozen)
        self.assertTrue(compare(manifest, control).ok)
        first = ids_of(manifest, "9001.0")
        skipped = replace(without(control, first), admitted=control.admitted - {"9001.0"})
        later = min((event for event in manifest.events.values() if event.occurrence_id == "9002.0"), key=lambda event: (event.due_us, event.event_id))
        skipped = replace(skipped, first_send_event_id=later.event_id)
        report = compare(manifest, skipped)
        self.assertFalse(report.ok)
        self.assertEqual(("9001.0",), report.missing_occurrences)
        self.assertFalse(report.first_send_ok)
        self.assertIn("9001.0", "\n".join(report.failures()))

    def test_the_first_send_must_be_an_event_due_at_the_first_due_instant(self) -> None:
        for event_id in manifest_first_due(self.manifest):
            self.assertTrue(compare(self.manifest, replace(self.observed, first_send_event_id=event_id)).first_send_ok)
        late = next(event.event_id for event in self.manifest.events.values() if event.due_us > self.manifest.first_due_us)
        report = compare(self.manifest, replace(self.observed, first_send_event_id=late))
        self.assertFalse(report.first_send_ok)
        self.assertFalse(report.ok)
        self.assertFalse(compare(self.manifest, replace(self.observed, first_send_event_id=None)).first_send_ok)
        self.assertFalse(compare(self.manifest, replace(self.observed, first_send_event_id="not an expected id")).first_send_ok)

    def test_a_changed_payload_under_the_right_id_is_a_mismatch_that_names_both_digests(self) -> None:
        key = sorted(self.observed.events)[5]
        events = {**self.observed.events, key: ["0" * 64]}
        report = compare(self.manifest, replace(self.observed, events=events))
        self.assertEqual(((key, self.manifest.events[key].payload_sha256, "0" * 64),), report.payload_mismatches)
        self.assertEqual((), report.missing_ids)

    def test_two_different_payloads_under_one_id_are_a_conflict_even_when_one_matches(self) -> None:
        key = sorted(self.observed.events)[5]
        events = {**self.observed.events, key: [self.manifest.events[key].payload_sha256, "1" * 64]}
        report = compare(self.manifest, replace(self.observed, events=events))
        self.assertEqual((key,), report.conflicting_duplicates)
        self.assertFalse(report.ok)

    def test_an_id_that_the_manifest_does_not_hold_is_reported_as_unexpected(self) -> None:
        events = {**self.observed.events, "replay_0a1b2c3d|0|9001.0|Test/HR|1|0": ["2" * 64]}
        report = compare(self.manifest, replace(self.observed, events=events))
        self.assertEqual(("replay_0a1b2c3d|0|9001.0|Test/HR|1|0",), report.unexpected_ids)
        self.assertFalse(report.ok)

    def test_an_occurrence_admitted_that_the_schedule_does_not_hold_is_reported(self) -> None:
        report = compare(self.manifest, replace(self.observed, admitted=self.observed.admitted | {"9009.0"}))
        self.assertEqual(("9009.0",), report.unexpected_admissions)
        self.assertFalse(report.ok)

    def test_events_due_after_the_window_belong_to_the_drain_and_the_window_only_comparison_leaves_them_out(self) -> None:
        frozen = boundary_schedule(rooms=2, window_us=15 * SECOND)
        manifest = build_manifest(frozen, boundary_recordings())
        marker = next(event for event in manifest.events.values() if event.occurrence_id == "9002.0" and event.kind == "case_end")
        self.assertFalse(marker.in_window)
        observed = without(replayed(frozen), [marker.event_id])
        self.assertFalse(compare(manifest, observed).ok)
        self.assertTrue(compare(manifest, observed, window_only=True).ok)
        inside = next(event for event in manifest.events.values() if event.in_window)
        self.assertFalse(compare(manifest, without(replayed(frozen), [inside.event_id]), window_only=True).ok)


def manifest_first_due(manifest: Manifest) -> tuple[str, ...]:
    return tuple(sorted(manifest.first_due_event_ids))
