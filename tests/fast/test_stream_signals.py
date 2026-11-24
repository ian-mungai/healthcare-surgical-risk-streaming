"""The signal rules the streaming job applies to a waveform second: it calls ``features.signal_validity`` and ``features.pressure_series``, the one definition
that Silver's batch path calls (analysis plan row 4, "Invalid second"), and keeps what a later second needs of each arterial second.

Failure modes covered here:

- A physical value is not ``raw * gain + offset``, or a raw sample that is not a number is turned into a reading, so the stream and the batch path would
  disagree on every sample.
- The flat rule (a range under 20 mmHg over the 20 seconds that end with the second) is applied to a segment that is not complete, to fewer than 20 seconds, or
  not at all, so the stream marks a second valid that Silver marks invalid or the other way round.
- The jump across the second boundary reads the last sample of a second that is not the one just before, or of none.
- The per-second series bridges a gap, loses the second before its window, or does not carry the exact mean that the features compare with a limit.
- A second of a track that is not the arterial pressure is judged on pressure limits.
"""

from __future__ import annotations

import math
import unittest
from collections.abc import Mapping, Sequence
from fractions import Fraction

from features.model import SECOND, FeatureInputError, Reading
from features.registry import ARTERIAL_SERIES, NUMERIC_WINDOW_SECONDS, complete_seconds
from features.series_input import SeriesReadings
from features.signal_validity import FLAT, INCOMPLETE, JUMP, MISSING, OUT_OF_RANGE
from replay.events.recording import physical_value
from spark.streaming.signals import ARTERIAL_WAVEFORM, ArtSecond, art_second, physical_samples, second_valid, series_window

RATE = 4
WAVE = [60.0, 70.0, 80.0, 90.0]
SLOW = [80.0, 85.0, 90.0, 85.0]


def held_from(seconds: Mapping[int, Sequence[float | None]], rate: int = RATE) -> dict[int, ArtSecond]:
    """The records of the given seconds, made in order as the job makes them."""
    held: dict[int, ArtSecond] = {}
    for second, samples in sorted(seconds.items()):
        held[second] = art_second(second, tuple(samples), float(rate), 0, held)
    return held


class Physical(unittest.TestCase):
    def test_a_physical_sample_is_the_one_shared_definition(self) -> None:
        raw = [0.0, 1.0, -3.5, 100.0]
        self.assertEqual([physical_value(value, 0.25, 4.0) for value in raw], list(physical_samples(raw, 0.25, 4.0)))
        self.assertEqual((4.0, 4.25, 3.125, 29.0), physical_samples(raw, 0.25, 4.0))

    def test_a_raw_sample_that_is_not_a_finite_number_is_a_missing_sample_not_a_reading(self) -> None:
        self.assertEqual((None, 5.0, None, None), physical_samples([math.nan, 5.0, math.inf, -math.inf], 1.0, 0.0))

    def test_the_arterial_track_is_the_one_the_feature_registry_names(self) -> None:
        from features.registry import WAVEFORM_TRACKS

        self.assertIn(ARTERIAL_WAVEFORM, WAVEFORM_TRACKS)


class ArterialSecond(unittest.TestCase):
    def test_a_normal_second_is_valid_and_keeps_its_facts_for_the_series(self) -> None:
        record = art_second(10, tuple(WAVE), float(RATE), 0, {})
        self.assertTrue(record.valid)
        facts = record.facts(10)
        self.assertEqual((10, True, None, 4, math.fsum(WAVE)), (facts.second, facts.valid, facts.cause, facts.count, facts.total))
        self.assertEqual(90.0, record.last)

    def test_a_sample_on_a_pressure_limit_is_valid_and_one_beyond_it_is_not(self) -> None:
        self.assertTrue(art_second(0, (20.0, 30.0, 40.0, 50.0), float(RATE), 0, {}).valid)
        self.assertTrue(art_second(0, (200.0, 190.0, 180.0, 170.0), float(RATE), 0, {}).valid)
        self.assertEqual(OUT_OF_RANGE, art_second(0, (19.999, 30.0, 40.0, 50.0), float(RATE), 0, {}).cause)
        self.assertEqual(OUT_OF_RANGE, art_second(0, (200.001, 190.0, 180.0, 170.0), float(RATE), 0, {}).cause)

    def test_a_missing_sample_a_wrong_count_or_an_offset_start_is_named_by_its_cause(self) -> None:
        self.assertEqual(MISSING, art_second(0, (60.0, None, 80.0, 90.0), float(RATE), 0, {}).cause)
        self.assertEqual(INCOMPLETE, art_second(0, tuple(WAVE[:3]), float(RATE), 0, {}).cause)
        self.assertEqual(INCOMPLETE, art_second(0, tuple(WAVE), float(RATE), 1, {}).cause)
        self.assertEqual(INCOMPLETE, art_second(0, tuple(WAVE), 2.5, 0, {}).cause)

    def test_a_jump_of_exactly_30_is_valid_and_a_jump_over_30_is_not_inside_a_second_and_across_the_boundary(self) -> None:
        self.assertTrue(art_second(0, (60.0, 90.0, 60.0, 90.0), float(RATE), 0, {}).valid)
        self.assertEqual(JUMP, art_second(0, (60.0, 90.001, 60.0, 90.0), float(RATE), 0, {}).cause)
        before = held_from({4: [60.0, 70.0, 80.0, 90.0]})
        self.assertTrue(art_second(5, (120.0, 110.0, 100.0, 90.0), float(RATE), 0, before).valid)
        self.assertEqual(JUMP, art_second(5, (120.001, 110.0, 100.0, 90.0), float(RATE), 0, before).cause)

    def test_the_jump_across_the_boundary_reads_only_the_second_just_before(self) -> None:
        gap = held_from({3: [60.0, 70.0, 80.0, 90.0]})
        self.assertTrue(art_second(5, (150.0, 140.0, 130.0, 120.0), float(RATE), 0, gap).valid)

    def test_the_last_sample_of_a_second_with_a_missing_last_sample_is_no_reading_to_compare_with(self) -> None:
        before = held_from({4: [60.0, 70.0, 80.0, None]})
        self.assertIsNone(before[4].last)
        self.assertTrue(art_second(5, (150.0, 140.0, 130.0, 120.0), float(RATE), 0, before).valid)

    def test_the_flat_rule_reads_the_20_seconds_that_end_with_the_second_and_only_when_they_are_all_complete(self) -> None:
        seconds = {second: list(SLOW) for second in range(20)}
        held = held_from(seconds)
        self.assertTrue(all(held[second].valid for second in range(19)))
        self.assertEqual((False, FLAT), (held[19].valid, held[19].cause))
        self.assertTrue(held_from({second: list(SLOW) for second in range(19)})[18].valid)
        broken = {second: list(SLOW) for second in range(20)} | {7: list(SLOW[:3])}
        self.assertTrue(held_from(broken)[19].valid)
        absent = {second: list(SLOW) for second in range(20) if second != 7}
        self.assertTrue(held_from(absent)[19].valid)

    def test_a_range_of_exactly_20_over_the_segment_is_valid_and_one_under_it_is_flat(self) -> None:
        wide = {second: [80.0, 85.0, 90.0, 85.0] for second in range(19)} | {19: [80.0, 100.0, 90.0, 85.0]}
        narrow = {second: [80.0, 85.0, 90.0, 85.0] for second in range(19)} | {19: [80.0, 99.999, 90.0, 85.0]}
        self.assertTrue(held_from(wide)[19].valid)
        self.assertEqual(FLAT, held_from(narrow)[19].cause)

    def test_a_second_that_slides_out_of_the_segment_no_longer_counts(self) -> None:
        seconds = {second: list(SLOW) for second in range(40)} | {0: [60.0, 70.0, 80.0, 90.0]}
        held = held_from(seconds)
        self.assertTrue(held[19].valid)
        self.assertEqual(FLAT, held[20].cause)

    def test_a_second_of_another_track_is_valid_when_it_is_a_whole_block_of_present_samples_and_the_pressure_limits_do_not_apply(self) -> None:
        other = (0.0, 1.0, 0.5, 0.2)
        self.assertTrue(second_valid("SNUADC/PLETH", other, float(RATE), 0))
        self.assertFalse(second_valid("SNUADC/PLETH", other[:3], float(RATE), 0))
        self.assertFalse(second_valid("SNUADC/PLETH", other, float(RATE), 1))
        self.assertFalse(second_valid("SNUADC/PLETH", (0.0, None, 0.5, 0.2), float(RATE), 0))
        self.assertFalse(art_second(0, other, float(RATE), 0, {}).valid)


class Series(unittest.TestCase):
    def test_the_series_of_a_window_carries_the_exact_mean_of_both_seconds_and_is_made_by_the_one_definition(self) -> None:
        held = held_from({10: list(WAVE), 11: [85.0, 95.0, 105.0, 95.0]})
        readings = series_window(held, 12 * SECOND)
        self.assertIsInstance(readings, SeriesReadings)
        self.assertEqual([11 * SECOND], [reading.time_us for reading in readings if reading.valid])
        reading = next(reading for reading in readings if reading.valid)
        self.assertEqual(math.fsum([*WAVE, 85.0, 95.0, 105.0, 95.0]) / 8, reading.value)
        self.assertEqual(Fraction(sum(Fraction(value) for value in [*WAVE, 85.0, 95.0, 105.0, 95.0]), 8), reading.exact)

    def test_the_second_before_the_window_gives_the_first_reading_of_the_window_its_mean(self) -> None:
        first = complete_seconds(1_000 * SECOND, NUMERIC_WINDOW_SECONDS)[0]
        held = held_from({first - 1: list(WAVE), first: [85.0, 95.0, 105.0, 95.0]})
        readings = series_window(held, 1_000 * SECOND)
        self.assertEqual([(first, True)], [(reading.time_us // SECOND, reading.valid) for reading in readings])

    def test_a_gap_is_never_bridged_and_a_second_that_is_invalid_makes_its_own_reading_and_the_next_invalid(self) -> None:
        held = held_from({10: list(WAVE), 12: list(WAVE), 13: list(WAVE), 14: [300.0, 70.0, 80.0, 90.0], 15: list(WAVE)})
        readings = {reading.time_us // SECOND: reading.valid for reading in series_window(held, 20 * SECOND)}
        self.assertEqual({10: False, 12: False, 13: True, 14: False, 15: False}, readings)

    def test_a_reading_that_the_window_does_not_hold_is_left_out(self) -> None:
        held = held_from({second: list(WAVE) for second in range(1_000, 1_005)})
        self.assertEqual([1_000, 1_001, 1_002], [reading.time_us // SECOND for reading in series_window(held, 1_003 * SECOND)])

    def test_the_series_is_built_for_features_the_feature_code_accepts_and_a_plain_reading_it_refuses(self) -> None:
        from features.model import WindowInput
        from features.row import build_row

        held = held_from({10: list(WAVE), 11: list(WAVE)})
        build_row(WindowInput(12 * SECOND, {ARTERIAL_SERIES: series_window(held, 12 * SECOND)}, {}, {}))
        with self.assertRaises(FeatureInputError):
            build_row(WindowInput(12 * SECOND, {ARTERIAL_SERIES: [Reading(11 * SECOND, 80.0, True)]}, {}, {}))


if __name__ == "__main__":
    unittest.main()
