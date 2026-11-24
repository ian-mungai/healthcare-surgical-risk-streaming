"""The events of one case occurrence: waveform seconds, the order of the events, the discriminators, the string events and the two generated markers.

Failure modes covered here:

- A waveform sample lands in the wrong second at a second boundary, in a record that straddles a second or at a rate that is not a whole number.
- A second with no sample gets a message, or a second with samples is lost.
- Two readings that share a track and a case time, or two string events at one time, are merged or reordered between derivations.
- A generated marker comes before a reading that is due at the same instant, so the final-score row is built before the cutoff reading arrived.
- A string event is sent as a reading, loses its text or its stable source ID, or a marker takes the form of a source event.
- The replay time of an event is not the start of its occurrence plus its due offset, so a room's events leave the replay clock.
- A waveform second carries no sample format, or the format of another track, so the stream cannot tell a gap marker from a reading or a float from an integer.
- A reading before the case start or after its whole-second end is due outside its occurrence, so the replay time of a room goes backwards.
"""

from __future__ import annotations

import unittest
from array import array
from collections.abc import Callable

from features.signal_validity import FLOAT_FORMATS, GAP_MARKERS
from replay.events.identity import Occurrence
from replay.events.recording import WAVEFORM_FORMATS, CaseRecording, CaseTimes, NumericTrack, RecordingError, StringTrack, WaveformRecord, WaveformTrack
from replay.events.schedule import ScheduledOccurrence, admitted_occurrences
from replay.events.stream import earliest_due_offset_us, occurrence_events, outgoing
from replay.events.waveform import second_samples, second_spans
from tests.fast.replay_support import RUN_ID, SECOND, boundary_recordings, boundary_schedule

WAVE_KEYS = {"event_id", "run_id", "room", "occurrence_id", "case_id", "track", "case_time_us", "discriminator", "replay_time_us"}


def waveform(rate: float, *records: tuple[int, list[float]], sample_format: int = 2) -> WaveformTrack:
    return WaveformTrack("Test/W", "mV", rate, 1.0, 0.0, sample_format, tuple(WaveformRecord(start, array("d", samples)) for start, samples in records))


def seconds_of(track: WaveformTrack) -> list[tuple[int, int, list[float]]]:
    return [(span.second, span.start_offset_us, list(second_samples(track, span))) for span in second_spans(track)]


class WaveformSeconds(unittest.TestCase):
    def test_each_sample_belongs_to_the_second_in_which_its_own_time_falls(self) -> None:
        track = boundary_recordings()[9001].waveforms[0]
        self.assertEqual(
            [(0, 250_000, [10, 11, 12]), (1, 0, [13, 14, 15]), (3, 250_000, [20, 21, 22]), (4, 0, [23]), (6, 900_000, [30]), (7, 150_000, [31, 32])],
            seconds_of(track),
        )

    def test_a_sample_exactly_on_a_second_boundary_opens_the_next_second(self) -> None:
        track = waveform(4.0, (0, [1, 2, 3, 4, 5]))
        self.assertEqual([(0, 0, [1, 2, 3, 4]), (1, 0, [5])], seconds_of(track))

    def test_a_second_with_no_sample_has_no_message(self) -> None:
        self.assertEqual([0, 1, 3, 4, 6, 7], [item[0] for item in seconds_of(boundary_recordings()[9001].waveforms[0])])

    def test_a_rate_that_is_not_a_whole_number_splits_where_the_rate_says(self) -> None:
        track = waveform(62.5, (0, [float(index) for index in range(125)]))
        got = seconds_of(track)
        self.assertEqual([0, 1], [item[0] for item in got])
        self.assertEqual((63, 62), (len(got[0][2]), len(got[1][2])))
        self.assertEqual(8000, got[1][1])
        self.assertEqual(list(map(float, range(125))), got[0][2] + got[1][2])

    def test_one_record_can_span_several_seconds_and_a_second_can_hold_samples_of_two_records(self) -> None:
        track = waveform(2.0, (0, [1, 2, 3, 4, 5, 6]), (2_750_000, [7, 8]))
        self.assertEqual([(0, 0, [1, 2]), (1, 0, [3, 4]), (2, 0, [5, 6, 7]), (3, 250_000, [8])], seconds_of(track))

    def test_a_record_that_starts_before_the_case_start_keeps_its_negative_seconds(self) -> None:
        track = waveform(2.0, (-1_250_000, [1, 2, 3, 4]))
        self.assertEqual([(-2, 750_000, [1]), (-1, 250_000, [2, 3]), (0, 250_000, [4])], seconds_of(track))

    def test_a_rate_that_is_zero_or_not_finite_is_refused(self) -> None:
        for rate in (0.0, -1.0, float("inf"), float("nan")):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                second_spans(waveform(rate, (0, [1.0])))


def keys_of(recording: CaseRecording) -> list[tuple[str, str, int, int, int]]:
    return [(event.kind, event.track, event.case_time_us, event.due_offset_us, event.discriminator) for event in occurrence_events(recording)]


class EventOrder(unittest.TestCase):
    def test_the_events_of_a_case_come_in_due_order_with_readings_before_markers_at_one_instant(self) -> None:
        wave = "Test/WAVE"
        self.assertEqual(
            [
                ("string", "Test/EVENT", 0, 0, 0),
                ("numeric", "Test/HR", 0, 0, 0),
                ("waveform", wave, 0, 1 * SECOND, 0),
                ("string", "Test/EVENT", 1_500_000, 1_500_000, 0),
                ("string", "Test/EVENT", 1_500_000, 1_500_000, 1),
                ("numeric", "Test/HR", 1_500_000, 1_500_000, 0),
                ("numeric", "Test/HR", 1_500_000, 1_500_000, 1),
                ("numeric", "Test/NIBP", 2 * SECOND, 2 * SECOND, 0),
                ("waveform", wave, 1 * SECOND, 2 * SECOND, 0),
                ("waveform", wave, 3 * SECOND, 4 * SECOND, 0),
                ("op_start", "!op_start", 4 * SECOND, 4 * SECOND, 0),
                ("waveform", wave, 4 * SECOND, 5 * SECOND, 0),
                ("waveform", wave, 6 * SECOND, 7 * SECOND, 0),
                ("numeric", "Test/HR", 7_250_000, 7_250_000, 0),
                ("waveform", wave, 7 * SECOND, 8 * SECOND, 0),
                ("numeric", "Test/NIBP", 10 * SECOND, 10 * SECOND, 0),
                ("final_score", "!final_score", 10 * SECOND, 10 * SECOND, 0),
                ("string", "Test/EVENT", 13 * SECOND, 13 * SECOND, 0),
                ("case_end", "!case_end", 14 * SECOND, 14 * SECOND, 0),
            ],
            keys_of(boundary_recordings()[9001]),
        )

    def test_the_order_does_not_depend_on_the_order_of_the_readings_in_the_file(self) -> None:
        recording = boundary_recordings()[9002]
        shuffled = CaseRecording(
            recording.times,
            numeric=tuple(NumericTrack(track.name, track.unit, tuple(reversed(track.readings))) for track in recording.numeric),
            strings=recording.strings,
            waveforms=recording.waveforms,
        )
        self.assertEqual(keys_of(recording), keys_of(shuffled))

    def test_every_reading_is_one_event_and_each_case_has_one_op_start_one_final_score_and_one_case_end_marker(self) -> None:
        kinds = [key[0] for key in keys_of(boundary_recordings()[9001])]
        self.assertEqual(
            {"numeric": 6, "string": 4, "waveform": 6, "op_start": 1, "final_score": 1, "case_end": 1}, {kind: kinds.count(kind) for kind in set(kinds)}
        )

    def test_a_case_with_no_waveform_has_no_waveform_event(self) -> None:
        self.assertNotIn("waveform", {key[0] for key in keys_of(boundary_recordings()[9002])})

    def test_the_op_start_marker_is_due_at_the_opstart_the_final_score_marker_at_the_opend_and_the_case_end_marker_at_the_caseend(self) -> None:
        for case_id, recording in boundary_recordings().items():
            with self.subTest(case=case_id):
                marks = {event.kind: event.due_offset_us for event in occurrence_events(recording) if event.kind in ("op_start", "final_score", "case_end")}
                times = recording.times
                self.assertEqual({"op_start": times.opstart_us, "final_score": times.opend_us, "case_end": times.caseend_us}, marks)

    def test_markers_that_are_due_together_come_in_the_order_start_final_end(self) -> None:
        times = CaseTimes(9100, 5 * SECOND, 5 * SECOND, 5 * SECOND)
        recording = CaseRecording(times, numeric=(NumericTrack("Test/HR", "", ((5 * SECOND, 1.0),)),))
        self.assertEqual(["numeric", "op_start", "final_score", "case_end"], [event.kind for event in occurrence_events(recording)])

    def test_a_reading_or_a_waveform_second_outside_the_case_is_due_inside_it_and_keeps_its_case_time(self) -> None:
        self.assertEqual(
            [
                ("numeric", "Test/HR", -500_000, 0, 0),
                ("numeric", "Test/HR", 0, 0, 0),
                ("op_start", "!op_start", 2 * SECOND, 2 * SECOND, 0),
                ("final_score", "!final_score", 6 * SECOND, 6 * SECOND, 0),
                ("numeric", "Test/HR", 10_400_000, 10 * SECOND, 0),
                ("waveform", "Test/WAVE", 9 * SECOND, 10 * SECOND, 0),
                ("waveform", "Test/WAVE", 10 * SECOND, 10 * SECOND, 0),
                ("case_end", "!case_end", 10 * SECOND, 10 * SECOND, 0),
            ],
            keys_of(boundary_recordings()[9003]),
        )

    def test_the_first_due_offset_of_a_case_is_the_due_offset_of_its_first_event(self) -> None:
        self.assertEqual({9001: 0, 9002: 0, 9003: 0}, {case: earliest_due_offset_us(recording) for case, recording in boundary_recordings().items()})

    def test_a_marker_track_name_a_repeated_track_name_or_a_delimiter_in_a_track_name_is_refused(self) -> None:
        times = CaseTimes(1, 0, 5, 9)
        bad: list[Callable[[], object]] = [
            lambda: CaseRecording(times, numeric=(NumericTrack("!final_score", "", ((0, 1.0),)),)),
            lambda: CaseRecording(times, numeric=(NumericTrack("A", "", ((0, 1.0),)), NumericTrack("A", "", ((1, 2.0),)))),
            lambda: CaseRecording(times, numeric=(NumericTrack("A", "", ((0, 1.0),)),), strings=(StringTrack("A", ((1, "x"),)),)),
            lambda: CaseRecording(times, numeric=(NumericTrack("A|B", "", ((0, 1.0),)),)),
            lambda: CaseRecording(times, numeric=(NumericTrack("", "", ((0, 1.0),)),)),
        ]
        for build in bad:
            with self.subTest(), self.assertRaises(RecordingError):
                build()


class SampleFormat(unittest.TestCase):
    def test_each_sample_format_of_the_file_format_is_accepted(self) -> None:
        for sample_format in range(1, 9):
            with self.subTest(sample_format=sample_format):
                self.assertEqual(sample_format, waveform(4.0, (0, [1.0]), sample_format=sample_format).sample_format)

    def test_a_format_that_is_not_one_of_the_eight_sample_formats_is_refused_when_the_recording_is_built(self) -> None:
        times = CaseTimes(1, 0, 5, 9)
        for sample_format in (0, 9, -1, 255):
            with self.subTest(sample_format=sample_format), self.assertRaises(RecordingError):
                CaseRecording(times, waveforms=(waveform(4.0, (0, [1.0]), sample_format=sample_format),))

    def test_the_formats_of_the_replay_are_the_formats_that_silver_decodes(self) -> None:
        self.assertEqual(set(FLOAT_FORMATS) | set(GAP_MARKERS), set(WAVEFORM_FORMATS))

    def test_the_waveform_event_carries_the_format_of_its_own_track_for_every_format(self) -> None:
        times = CaseTimes(1, 0, 5, 9_000_000)
        for sample_format in range(1, 9):
            recording = CaseRecording(times, waveforms=(waveform(4.0, (0, [1.0, 2.0, 3.0, 4.0]), sample_format=sample_format),))
            scheduled = ScheduledOccurrence(Occurrence(0, 1, 0, 0), times, 0)
            sent = [outgoing(RUN_ID, scheduled, recording, event).payload for event in occurrence_events(recording) if event.kind == "waveform"]
            with self.subTest(sample_format=sample_format):
                self.assertEqual([sample_format], [payload["sample_format"] for payload in sent])


class OutgoingEvents(unittest.TestCase):
    def setUp(self) -> None:
        self.recordings = boundary_recordings()
        self.schedule = boundary_schedule(rooms=2, stagger_us=0)
        self.by_id = {item.occurrence.id: item for item in admitted_occurrences(self.schedule)}

    def events(self, occurrence_id: str) -> dict[tuple[str, str, int, int], dict[str, object]]:
        scheduled = self.by_id[occurrence_id]
        recording = self.recordings[scheduled.occurrence.case_id]
        out: dict[tuple[str, str, int, int], dict[str, object]] = {}
        for event in occurrence_events(recording):
            sent = outgoing(RUN_ID, scheduled, recording, event)
            out[(event.kind, event.track, event.due_offset_us, event.discriminator)] = sent.payload
        return out

    def test_a_numeric_reading_carries_its_identity_its_two_times_and_its_value(self) -> None:
        payload = self.events("9001.0")[("numeric", "Test/HR", 1_500_000, 1)]
        self.assertEqual(
            {
                "event_id": f"{RUN_ID}|0|9001.0|Test/HR|1500000|1",
                "run_id": RUN_ID,
                "room": 0,
                "occurrence_id": "9001.0",
                "case_id": 9001,
                "track": "Test/HR",
                "case_time_us": 1_500_000,
                "discriminator": 1,
                "replay_time_us": 1_500_000,
                "value": 62.0,
            },
            payload,
        )

    def test_the_replay_time_of_a_later_occurrence_is_its_start_plus_the_case_time(self) -> None:
        payload = self.events("9001.1")[("numeric", "Test/HR", 7_250_000, 0)]
        self.assertEqual((14 * SECOND + 7_250_000, 7_250_000, 0), (payload["replay_time_us"], payload["case_time_us"], payload["room"]))
        self.assertEqual(f"{RUN_ID}|0|9001.1|Test/HR|7250000|0", payload["event_id"])

    def test_a_waveform_second_carries_its_samples_its_scale_and_the_offset_of_its_first_sample(self) -> None:
        payload = self.events("9001.0")[("waveform", "Test/WAVE", 8 * SECOND, 0)]
        self.assertEqual(
            {
                "event_id": f"{RUN_ID}|0|9001.0|Test/WAVE|7000000|0",
                "run_id": RUN_ID,
                "room": 0,
                "occurrence_id": "9001.0",
                "case_id": 9001,
                "track": "Test/WAVE",
                "case_time_us": 7 * SECOND,
                "discriminator": 0,
                "replay_time_us": 8 * SECOND,
                "unit": "mV",
                "sample_rate": 4.0,
                "gain": 0.5,
                "offset": -1.0,
                "sample_format": 5,
                "start_offset_us": 150_000,
                "samples": [31.0, 32.0],
            },
            payload,
        )

    def test_a_string_event_carries_its_text_its_original_time_and_its_stable_source_id(self) -> None:
        payload = self.events("9001.0")[("string", "Test/EVENT", 1_500_000, 1)]
        self.assertEqual(
            {
                "event_id": f"{RUN_ID}|0|9001.0|Test/EVENT|1500000|1",
                "run_id": RUN_ID,
                "room": 0,
                "occurrence_id": "9001.0",
                "case_id": 9001,
                "track": "Test/EVENT",
                "case_time_us": 1_500_000,
                "discriminator": 1,
                "replay_time_us": 1_500_000,
                "text": "Second text at the same time",
                "source_id": "9001/Test/EVENT/2",
            },
            payload,
        )

    def test_the_source_id_of_a_string_event_is_the_same_in_every_occurrence_of_the_case(self) -> None:
        first = self.events("9001.0")[("string", "Test/EVENT", 13 * SECOND, 0)]
        again = self.events("9001.1")[("string", "Test/EVENT", 13 * SECOND, 0)]
        self.assertEqual(first["source_id"], again["source_id"])
        self.assertNotEqual(first["event_id"], again["event_id"])

    def test_the_op_start_marker_names_its_kind_and_its_instant_and_has_the_reserved_track(self) -> None:
        marker = self.events("9002.0")[("op_start", "!op_start", 6 * SECOND, 0)]
        self.assertEqual(
            (f"{RUN_ID}|1|9002.0|!op_start|6000000|0", "OP_START", 6 * SECOND, 6 * SECOND),
            (marker["event_id"], marker["marker"], marker["case_time_us"], marker["replay_time_us"]),
        )

    def test_the_two_markers_name_their_kind_and_their_instants_and_have_no_track(self) -> None:
        events = self.events("9002.0")
        self.assertEqual(
            {
                "event_id": f"{RUN_ID}|1|9002.0|!final_score|20000000|0",
                "run_id": RUN_ID,
                "room": 1,
                "occurrence_id": "9002.0",
                "case_id": 9002,
                "marker": "FINAL_SCORE",
                "case_time_us": 20 * SECOND,
                "replay_time_us": 20 * SECOND,
            },
            events[("final_score", "!final_score", 20 * SECOND, 0)],
        )
        self.assertEqual(
            ("CASE_END", 30 * SECOND),
            (events[("case_end", "!case_end", 30 * SECOND, 0)]["marker"], events[("case_end", "!case_end", 30 * SECOND, 0)]["replay_time_us"]),
        )

    def test_no_event_id_repeats_within_an_occurrence_or_across_the_admitted_occurrences(self) -> None:
        ids = [payload["event_id"] for occurrence in self.by_id for payload in self.events(occurrence).values()]
        self.assertEqual(len(ids), len(set(ids)))

    def test_the_same_derivation_twice_gives_the_same_events(self) -> None:
        self.assertEqual(self.events("9001.0"), self.events("9001.0"))
