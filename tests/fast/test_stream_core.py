"""The streaming job's core: one case occurrence's state, its lateness decisions, its cutoffs and its feature rows.

Failure modes covered here:

- A row built from the job's buffers differs from the row built from the whole recording at the same cutoff (a lost reading, a pruned
  reading that a later window needs, a reading after the cutoff, a wrong validity), or a row depends on how the events were cut into batches
  or on a restart.
- A cutoff is emitted before an in-policy late event of it can still arrive, or never, or twice, or after ``opend``, or the final row is not
  the one at ``opend``.
- An event later than the limit changes a row, or is lost without a recorded decision, or an event inside the limit is dropped. A limit is
  read a microsecond off.
- A re-sent event (a replay that stopped after Kafka's acknowledgment and before its position write) is applied twice, or is not counted.
- Two readings of one track and time give a value that depends on their arrival order.
- A new case occurrence in a room carries a value of the occurrence before it, or a room's new occurrence or an uneven read discards an in-
  policy event of another room.
- A bad input stops the job instead of leaving an error record, an event after the case end changes a row, or a clinical value that is not
  yet available is read.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Sequence
from dataclasses import replace

from features.model import ClinicalValue
from replay.events.identity import marker_event_id
from spark.streaming.model import Context, DecisionRecord, ErrorRecord, Event, FeatureRecord, LedgerRecord, Policy, Record
from spark.streaming.occurrence import process
from tests.fast.stream_support import (
    CONTEXT,
    HEART_RATE,
    ORIGIN_US,
    RUN,
    SECOND,
    by_replay_time,
    events_for,
    features_of,
    oracle_eligibility,
    oracle_values,
    reading_event,
    run_stream,
    stamp,
    synthetic_recording,
    values_of,
)

LIMIT_US = Policy().late_limit_us
MINUTES = [60 * SECOND * minute for minute in range(1, 6)]
START = 30 * SECOND


def decisions(records: Sequence[Record], kind: str | None = None) -> list[DecisionRecord]:
    return [record for record in records if isinstance(record, DecisionRecord) and (kind is None or record.decision == kind)]


def rows(records: Sequence[Record]) -> dict[int, dict[str, object]]:
    return {cutoff: values_of(record) for cutoff, record in features_of(records).items()}


class OracleParity(unittest.TestCase):
    def test_every_row_equals_the_row_built_from_the_whole_recording_at_its_cutoff(self) -> None:
        recording = synthetic_recording()
        records, _ = run_stream(events_for(recording))
        found = features_of(records)
        self.assertEqual([START, *MINUTES], sorted(found))
        for cutoff, record in found.items():
            with self.subTest(cutoff=cutoff):
                self.assertEqual(oracle_values(recording, cutoff), values_of(record))
                self.assertEqual(ORIGIN_US + cutoff, record.cutoff_replay_time_us)
        self.assertIsNotNone(values_of(found[300 * SECOND])["solar8000_hr__last"])

    def test_a_long_case_agrees_after_the_buffers_are_pruned(self) -> None:
        recording = synthetic_recording(caseend_s=2_000, opend_s=1_900, rate=4, hr_step_s=10)
        records, states = run_stream(events_for(recording), batch_size=50, restart=True)
        found = features_of(records)
        self.assertEqual(sorted([START, 1_650 * SECOND, 1_900 * SECOND, *(60 * SECOND * minute for minute in range(1, 32))]), sorted(found))
        for cutoff in (60 * SECOND, 960 * SECOND, 1_200 * SECOND, 1_800 * SECOND, 1_900 * SECOND):
            with self.subTest(cutoff=cutoff):
                self.assertEqual(oracle_values(recording, cutoff), values_of(found[cutoff]))
        state = next(iter(states.values()))
        self.assertEqual({}, state.numeric)
        self.assertEqual({}, state.wave_samples)

    def test_a_buffer_older_than_every_window_still_needed_is_dropped_while_the_case_runs(self) -> None:
        recording = synthetic_recording(caseend_s=2_000, opend_s=1_900, rate=4, hr_step_s=10)
        events = events_for(recording)
        _, states = run_stream([event for event in events if event.replay_time_us <= ORIGIN_US + 1_500 * SECOND])
        state = next(iter(states.values()))
        oldest = min(time for readings in state.numeric.values() for time in readings)
        self.assertGreater(oldest, 510 * SECOND)
        self.assertLess(len(state.art_summary), 1_000)
        self.assertGreater(len(state.numeric), 0)

    def test_the_eligibility_of_a_row_is_the_feature_codes_eligibility_at_its_cutoff(self) -> None:
        recording = synthetic_recording(caseend_s=1_200, opend_s=1_100, rate=4, hr_step_s=10)
        records, _ = run_stream(events_for(recording))
        found = features_of(records)
        for cutoff in (60 * SECOND, 960 * SECOND, 1_100 * SECOND):
            with self.subTest(cutoff=cutoff):
                eligible, reasons = oracle_eligibility(recording, cutoff)
                self.assertEqual(eligible, found[cutoff].warning_eligible)
                self.assertEqual(reasons, found[cutoff].eligibility_reasons)
        self.assertTrue(any(not found[cutoff].warning_eligible for cutoff in found))


class Cutoffs(unittest.TestCase):
    def test_the_final_row_is_the_cutoff_of_the_final_score_marker_and_no_row_follows_it(self) -> None:
        recording = synthetic_recording(opend_s=290)
        records, _ = run_stream(events_for(recording))
        found = features_of(records)
        self.assertEqual([START] + [60 * SECOND * minute for minute in range(1, 5)] + [290 * SECOND], sorted(found))
        self.assertEqual([290 * SECOND], [cutoff for cutoff, record in found.items() if record.is_final])
        self.assertEqual(oracle_values(recording, 290 * SECOND), values_of(found[290 * SECOND]))

    def test_a_cutoff_on_opend_is_one_row_and_it_is_the_final_row(self) -> None:
        records, _ = run_stream(events_for(synthetic_recording(opend_s=300)))
        found = features_of(records)
        self.assertTrue(found[300 * SECOND].is_final)
        self.assertEqual(1, len([record for record in records if isinstance(record, FeatureRecord) and record.cutoff_case_time_us == 300 * SECOND]))

    def test_a_cutoff_waits_until_no_in_policy_event_can_still_arrive_for_it(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        at_limit = [event for event in events if event.replay_time_us <= ORIGIN_US + (60 * SECOND + LIMIT_US)]
        records, _ = run_stream(at_limit)
        self.assertNotIn(60 * SECOND, features_of(records))
        beyond = [*at_limit, next(event for event in events if event.replay_time_us > ORIGIN_US + (60 * SECOND + LIMIT_US))]
        records, _ = run_stream(beyond)
        self.assertIn(60 * SECOND, features_of(records))

    def test_without_a_final_score_marker_there_is_no_final_row_and_the_ledger_says_so(self) -> None:
        events = [event for event in events_for(synthetic_recording()) if event.kind != "final_score"]
        records, _ = run_stream(events)
        self.assertFalse(any(record.is_final for record in features_of(records).values()))
        ledger = [record for record in records if isinstance(record, LedgerRecord)]
        self.assertEqual(1, len(ledger))
        self.assertFalse(json.loads(ledger[0].counts_json)["final_seen"])

    def test_the_feature_id_names_the_occurrence_the_cutoff_and_the_version(self) -> None:
        records, _ = run_stream(events_for(synthetic_recording(), room=3, sequence=3))
        record = features_of(records)[60 * SECOND]
        self.assertEqual(f"{RUN}|3|101.0|{60 * SECOND}|features_test", record.feature_id)
        self.assertEqual(("features_test", 3, "101.0", 101), (record.feature_version, record.room, record.occurrence_id, record.case_id))


class RiskCutoffs(unittest.TestCase):
    def test_the_op_start_marker_gives_the_starting_row_and_the_frozen_offsets_give_the_later_risk_rows(self) -> None:
        self.assertEqual((1_620 * SECOND, 3_300 * SECOND, 4_920 * SECOND), Policy().risk_offsets_us)
        recording = synthetic_recording(caseend_s=2_000, opend_s=1_900, rate=4, hr_step_s=10)
        found = features_of(run_stream(events_for(recording))[0])
        for cutoff in (START, 1_650 * SECOND):
            with self.subTest(cutoff=cutoff):
                self.assertIn(cutoff, found)
                self.assertFalse(found[cutoff].is_final)
                self.assertEqual(oracle_values(recording, cutoff), values_of(found[cutoff]))

    def test_a_risk_cutoff_after_opend_is_never_built_and_one_on_a_minute_is_one_row(self) -> None:
        short = features_of(run_stream(events_for(synthetic_recording(opend_s=300)))[0])
        self.assertEqual([START, *MINUTES], sorted(short))
        on_minute = features_of(run_stream(events_for(synthetic_recording(opstart_s=60)))[0])
        self.assertEqual(MINUTES, sorted(on_minute))

    def test_a_start_that_equals_opend_is_the_final_row_only(self) -> None:
        found = features_of(run_stream(events_for(synthetic_recording(opstart_s=300, opend_s=300)))[0])
        self.assertEqual(MINUTES, sorted(found))
        self.assertTrue(found[300 * SECOND].is_final)

    def test_without_the_op_start_marker_there_is_no_starting_row(self) -> None:
        events = [event for event in events_for(synthetic_recording()) if event.kind != "op_start"]
        self.assertEqual(MINUTES, sorted(features_of(run_stream(events)[0])))

    def test_a_start_cutoff_late_in_a_long_case_still_has_its_whole_window_after_the_buffers_were_pruned(self) -> None:
        recording = synthetic_recording(caseend_s=3_000, opstart_s=2_020, opend_s=2_900, rate=4, hr_step_s=10)
        found = features_of(run_stream(events_for(recording), batch_size=40, restart=True)[0])
        self.assertIn(2_020 * SECOND, found)
        for cutoff in (2_020 * SECOND, 2_040 * SECOND, 2_900 * SECOND):
            with self.subTest(cutoff=cutoff):
                self.assertEqual(oracle_values(recording, cutoff), values_of(found[cutoff]))


class Lateness(unittest.TestCase):
    def setUp(self) -> None:
        self.base = synthetic_recording()
        self.late_time = 100 * SECOND + 500_000
        self.with_reading = synthetic_recording(extra_hr=[(self.late_time, 111.0)])
        self.arrival = events_for(self.base)
        self.baseline_rows = rows(run_stream(self.arrival)[0])

    def inserted_after(self, replay_us: int, event_replay_us: int) -> list[Event]:
        """The events in sending order with a late heart-rate reading of case time 100.5 s put in after the first event due after ``replay_us``."""
        late = reading_event(HEART_RATE, self.late_time, 111.0)
        late = replace(late, replay_time_us=event_replay_us)
        position = next(index for index, event in enumerate(self.arrival) if event.replay_time_us > replay_us)
        return [*self.arrival[:position], late, *self.arrival[position:]]

    def test_a_late_event_inside_the_limit_is_applied_and_recorded(self) -> None:
        arrival = self.inserted_after(ORIGIN_US + 120 * SECOND, ORIGIN_US + self.late_time)
        records, _ = run_stream(arrival)
        self.assertEqual(1, len(decisions(records, "late_accepted")))
        found = rows(records)
        self.assertEqual(rows(run_stream(events_for(self.with_reading))[0]), found)
        self.assertNotEqual(self.baseline_rows[120 * SECOND], found[120 * SECOND])
        self.assertEqual(oracle_values(self.with_reading, 120 * SECOND), found[120 * SECOND])

    def test_an_event_exactly_at_the_limit_is_accepted_and_one_microsecond_more_is_too_late(self) -> None:
        hw = ORIGIN_US + 120 * SECOND
        for lateness, kind in ((LIMIT_US, "late_accepted"), (LIMIT_US + 1, "too_late")):
            with self.subTest(lateness=lateness):
                arrival = self.inserted_after(hw, hw - lateness)
                records, _ = run_stream(arrival)
                found = decisions(records, kind)
                self.assertEqual(1, len(found))
                self.assertEqual(lateness, found[0].lateness_us)

    def test_an_event_beyond_the_limit_is_recorded_counted_and_never_applied(self) -> None:
        arrival = self.inserted_after(ORIGIN_US + 140 * SECOND, ORIGIN_US + self.late_time)
        records, _ = run_stream(arrival)
        too_late = decisions(records, "too_late")
        self.assertEqual(1, len(too_late))
        self.assertGreater(too_late[0].lateness_us, LIMIT_US)
        self.assertEqual(self.baseline_rows, rows(records))
        ledger = next(record for record in records if isinstance(record, LedgerRecord))
        self.assertEqual(1, json.loads(ledger.counts_json)["too_late"])

    def test_a_very_late_event_leaves_the_watermark_where_it_was(self) -> None:
        until = [event for event in self.arrival if event.replay_time_us <= ORIGIN_US + 140 * SECOND]
        late = replace(reading_event(HEART_RATE, self.late_time, 111.0), replay_time_us=ORIGIN_US + self.late_time)
        records, states = run_stream([*until, late])
        self.assertEqual(ORIGIN_US + 140 * SECOND, next(iter(states.values())).high_water_us)
        self.assertEqual(1, len(decisions(records, "too_late")))

    def test_a_late_event_does_not_lower_the_watermark_so_a_later_one_is_judged_against_the_highest(self) -> None:
        until = [event for event in self.arrival if event.replay_time_us <= ORIGIN_US + 120 * SECOND]
        late = replace(reading_event(HEART_RATE, self.late_time, 111.0), replay_time_us=ORIGIN_US + self.late_time)
        later = replace(reading_event(HEART_RATE, 85 * SECOND + 500_000, 99.0), replay_time_us=ORIGIN_US + 85 * SECOND + 500_000)
        records, states = run_stream([*until, late, later])
        self.assertEqual(["late_accepted", "too_late"], [record.decision for record in decisions(records) if record.decision != "source_duplicate"])
        self.assertEqual(ORIGIN_US + 120 * SECOND, next(iter(states.values())).high_water_us)

    def test_the_decision_names_the_event_the_watermark_and_the_kafka_position(self) -> None:
        arrival = self.inserted_after(ORIGIN_US + 140 * SECOND, ORIGIN_US + self.late_time)
        records, _ = run_stream(arrival)
        record = decisions(records, "too_late")[0]
        stamped = stamp(arrival)
        late = next(event for event in stamped if event.replay_time_us == ORIGIN_US + self.late_time and event.track == HEART_RATE and event.value == 111.0)
        self.assertEqual(late.event_id, record.event_id)
        self.assertEqual(f"{late.event_id}#0:{late.kafka_offset}", record.decision_id)
        self.assertEqual((late.kafka_partition, late.kafka_offset), (record.kafka_partition, record.kafka_offset))
        self.assertEqual(record.watermark_us - record.replay_time_us, record.lateness_us)


class Duplicates(unittest.TestCase):
    def test_events_sent_again_after_a_replay_restart_have_no_second_effect_and_are_counted(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        baseline = rows(run_stream(events)[0])
        cut = next(index for index, event in enumerate(events) if event.replay_time_us > ORIGIN_US + 120 * SECOND)
        resent = [*events[: cut + 20], *events[cut - 20 : cut + 20], *events[cut + 20 :]]
        records, _ = run_stream(resent)
        self.assertEqual(baseline, rows(records))
        self.assertEqual(40, len(decisions(records, "duplicate")))
        ledger = next(record for record in records if isinstance(record, LedgerRecord))
        self.assertEqual(40, json.loads(ledger.counts_json)["duplicate"])

    def test_an_event_sent_again_beyond_the_limit_is_not_applied_either(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        baseline = rows(run_stream(events)[0])
        old = events[10]
        position = next(index for index, event in enumerate(events) if event.replay_time_us > old.replay_time_us + 2 * LIMIT_US)
        records, _ = run_stream([*events[:position], old, *events[position:]])
        self.assertEqual(baseline, rows(records))
        self.assertEqual(1, len(decisions(records, "too_late")))

    def test_two_readings_of_one_track_and_time_keep_the_first_in_file_order_whatever_the_arrival_order(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        pair = [event for event in events if event.track == HEART_RATE and event.case_time_us == 41 * SECOND]
        self.assertEqual([0, 1], [event.discriminator for event in pair])
        in_order, _ = run_stream(events)
        self.assertEqual([pair[1].event_id], [record.event_id for record in decisions(in_order, "source_duplicate")])
        swapped = list(events)
        first, second = swapped.index(pair[0]), swapped.index(pair[1])
        swapped[first], swapped[second] = swapped[second], swapped[first]
        records, _ = run_stream(swapped)
        self.assertEqual(rows(in_order), rows(records))
        self.assertEqual(oracle_values(recording, 60 * SECOND), rows(records)[60 * SECOND])


class StateIsolation(unittest.TestCase):
    def test_a_new_occurrence_in_a_room_starts_with_no_value_of_the_one_before_it(self) -> None:
        first = synthetic_recording(101, hr_base=60.0)
        second = synthetic_recording(102, hr_base=150.0)
        second_start = ORIGIN_US + 500 * SECOND
        before = events_for(first, sequence=0, repeat=0)
        after = events_for(second, sequence=2, repeat=1, start_us=second_start)
        records, states = run_stream(by_replay_time(before, after))
        self.assertEqual(2, len(states))
        alone, _ = run_stream(after)
        mine = {cutoff: values_of(record) for cutoff, record in features_of([r for r in records if getattr(r, "occurrence_id", "") == "102.1"]).items()}
        self.assertEqual(rows(alone), mine)
        lowest = mine[60 * SECOND]["solar8000_hr__min"]
        self.assertIsInstance(lowest, float)
        self.assertGreater(float(str(lowest)), 149.0)
        self.assertEqual(oracle_values(second, 60 * SECOND), mine[60 * SECOND])

    def test_the_first_cutoff_of_a_new_occurrence_counts_only_its_own_readings(self) -> None:
        recording = synthetic_recording(101, hr_step_s=2)
        records, _ = run_stream(events_for(recording, sequence=1, repeat=1))
        self.assertEqual(30, values_of(features_of(records)[60 * SECOND])["solar8000_hr__valid_count"])

    def test_an_event_of_two_occurrences_in_one_call_is_refused(self) -> None:
        mixed = events_for(synthetic_recording(101))[:1] + events_for(synthetic_recording(102), room=1, sequence=1)[:1]
        with self.assertRaises(ValueError):
            process(None, stamp(mixed), CONTEXT)


class RoomTransition(unittest.TestCase):
    def test_a_rooms_next_case_and_an_uneven_read_do_not_discard_an_in_policy_event_of_another_room(self) -> None:
        room0_first = synthetic_recording(101, opend_s=395, hr_base=60.0)
        room0_next = synthetic_recording(103, opend_s=395, hr_base=120.0)
        straggler_time = 360 * SECOND + 500_000
        room1_with = synthetic_recording(102, opend_s=395, hr_base=90.0, extra_hr=[(straggler_time, 77.0)])
        room1_start = ORIGIN_US + 30 * SECOND
        room1 = events_for(synthetic_recording(102, opend_s=395, hr_base=90.0), room=1, sequence=1, start_us=room1_start)
        late = replace(reading_event(HEART_RATE, straggler_time, 77.0, room=1, case_id=102, start_us=room1_start), replay_time_us=room1_start + straggler_time)
        position = next(index for index, event in enumerate(room1) if event.replay_time_us > room1_start + straggler_time + 20 * SECOND)
        room1 = [*room1[:position], late, *room1[position:]]
        room0 = [*events_for(room0_first, room=0, sequence=0), *events_for(room0_next, room=0, sequence=2, repeat=1, start_us=ORIGIN_US + 410 * SECOND)]
        records, _ = run_stream([*room0, *room1])
        self.assertEqual([], decisions(records, "too_late"))
        self.assertEqual(["late_accepted"], [record.decision for record in decisions(records) if record.decision != "source_duplicate"])
        for occurrence, recording in (("101.0", room0_first), ("102.0", room1_with), ("103.1", room0_next)):
            mine = features_of([record for record in records if getattr(record, "occurrence_id", "") == occurrence])
            self.assertEqual(8, len(mine))
            for cutoff, record in mine.items():
                with self.subTest(occurrence=occurrence, cutoff=cutoff):
                    self.assertEqual(oracle_values(recording, cutoff), values_of(record))


class Restarts(unittest.TestCase):
    def test_the_rows_do_not_depend_on_how_the_events_are_cut_into_batches_or_on_a_restart_between_them(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        single, _ = run_stream(events)
        for size in (1, 7, 64):
            with self.subTest(size=size):
                cut, _ = run_stream(events, batch_size=size, restart=True)
                self.assertEqual(rows(single), rows(cut))
                self.assertEqual({r.decision_id for r in decisions(single)}, {r.decision_id for r in decisions(cut)})

    def test_a_batch_run_again_from_the_same_state_gives_the_same_records(self) -> None:
        events = stamp(events_for(synthetic_recording()))
        state, _ = process(None, events[:200], CONTEXT)
        again = type(state).from_json(state.to_json())
        first = process(state, events[200:400], CONTEXT)[1]
        second = process(again, events[200:400], CONTEXT)[1]
        self.assertEqual(first, second)

    def test_the_input_order_inside_a_batch_does_not_matter_only_the_kafka_offsets_do(self) -> None:
        events = stamp(events_for(synthetic_recording()))
        ordered = process(None, events, CONTEXT)[1]
        shuffled = process(None, events[::-1], CONTEXT)[1]
        self.assertEqual(ordered, shuffled)


class CaseEnd(unittest.TestCase):
    def test_the_case_end_marker_writes_the_ledger_once_and_clears_the_buffers(self) -> None:
        recording = synthetic_recording()
        records, states = run_stream(events_for(recording))
        ledger = [record for record in records if isinstance(record, LedgerRecord)]
        self.assertEqual(1, len(ledger))
        counts = json.loads(ledger[0].counts_json)
        self.assertEqual({"final_seen": True, "string_events": 1, "ignored": 2}, {name: counts[name] for name in ("final_seen", "string_events", "ignored")})
        self.assertEqual(f"{RUN}|0|101.0|ledger", ledger[0].ledger_id)
        state = next(iter(states.values()))
        self.assertTrue(state.closed)
        self.assertEqual(({}, {}, {}), (state.numeric, state.wave_samples, state.art_summary))

    def test_an_event_after_the_case_end_is_recorded_and_changes_no_row(self) -> None:
        recording = synthetic_recording()
        events = events_for(recording)
        baseline = run_stream(events)[0]
        extra = reading_event(HEART_RATE, 399 * SECOND + 500_000, 1.0)
        records, _ = run_stream([*events, extra])
        self.assertEqual(rows(baseline), rows(records))
        self.assertEqual(1, len(decisions(records, "after_case_end")))
        self.assertEqual(1, len([record for record in records if isinstance(record, LedgerRecord)]))

    def test_a_case_end_without_a_final_marker_still_emits_the_minute_rows_up_to_the_case_end(self) -> None:
        recording = synthetic_recording()
        events = [event for event in events_for(recording) if event.kind != "final_score"]
        found = features_of(run_stream(events)[0])
        self.assertEqual([START] + [60 * SECOND * minute for minute in range(1, 7)], sorted(found))

    def test_a_marker_event_id_is_the_replays_and_not_a_source_id(self) -> None:
        events = events_for(synthetic_recording())
        marker = next(event for event in events if event.kind == "final_score")
        self.assertEqual(marker_event_id(RUN, 0, "101.0", "final_score", 300 * SECOND), marker.event_id)


class BadInput(unittest.TestCase):
    def test_a_window_the_feature_code_refuses_leaves_an_error_record_and_the_job_goes_on(self) -> None:
        recording = synthetic_recording(arterial_rate=62.5)
        records, _ = run_stream(events_for(recording))
        errors = [record for record in records if isinstance(record, ErrorRecord)]
        self.assertEqual([START, *MINUTES], sorted(record.cutoff_case_time_us for record in errors))
        self.assertIn("sample rate", errors[0].message)
        self.assertEqual({}, features_of(records))
        self.assertEqual(1, len([record for record in records if isinstance(record, LedgerRecord)]))

    def test_an_unregistered_track_and_a_string_event_are_counted_and_never_reach_the_features(self) -> None:
        records, _ = run_stream(events_for(synthetic_recording()))
        counts = json.loads(next(record for record in records if isinstance(record, LedgerRecord)).counts_json)
        self.assertEqual(2, counts["ignored"])
        self.assertEqual(1, counts["string_events"])


class Clinical(unittest.TestCase):
    def test_a_clinical_value_is_read_only_from_its_availability_time(self) -> None:
        recording = synthetic_recording()
        clinical = {"age": ClinicalValue(61.0, 10 * SECOND), "asa": ClinicalValue(2.0, 100 * SECOND), "sex": ClinicalValue("F", None)}
        context = Context(Policy(), "features_test", lambda case_id: clinical if case_id == 101 else {})
        found = features_of(run_stream(events_for(recording), context)[0])
        for cutoff in (60 * SECOND, 120 * SECOND):
            with self.subTest(cutoff=cutoff):
                self.assertEqual(oracle_values(recording, cutoff, clinical), values_of(found[cutoff]))
        self.assertEqual(61.0, values_of(found[60 * SECOND])["age"])
        self.assertIsNone(values_of(found[60 * SECOND])["asa"])
        self.assertEqual(2.0, values_of(found[120 * SECOND])["asa"])
        self.assertIsNone(values_of(found[120 * SECOND])["sex"])


if __name__ == "__main__":
    unittest.main()
