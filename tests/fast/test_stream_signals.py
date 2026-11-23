"""The signal rules the streaming job applies to a waveform second before the feature code reads it (analysis plan row 4, "Invalid second").

Failure modes covered here:

- A second on a limit (20 or 200 mmHg, a range of 20, a jump of 30) is judged on the wrong side of the limit.
- A second with a wrong sample count, an offset start or a non-finite sample is called valid, and the feature code then refuses the whole window.
- The trailing 2-second average of the per-second series uses one second, a second that is invalid, or a boundary with a jump.
- A physical value is not ``raw * gain + offset``, so the stream and the batch path would disagree on every sample.
"""

from __future__ import annotations

import math
import unittest

from replay.events.recording import physical_value
from spark.streaming.signals import (
    ARTERIAL_WAVEFORM,
    MAX_PRESSURE_MMHG,
    MIN_PRESSURE_MMHG,
    MIN_RANGE_MMHG,
    physical_samples,
    series_reading,
    summarise_second,
    whole_block,
)

RATE = 10
GOOD = [80.0, 90.0, 100.0, 90.0, 80.0, 70.0, 80.0, 90.0, 100.0, 90.0]


class ArterialSecond(unittest.TestCase):
    def test_a_normal_second_is_valid_and_keeps_its_first_last_and_sum(self) -> None:
        summary = summarise_second(GOOD, RATE, 0, arterial=True)
        self.assertTrue(summary.valid)
        self.assertEqual((80.0, 90.0, math.fsum(GOOD), 10), (summary.first, summary.last, summary.total, summary.count))

    def test_a_sample_on_a_pressure_limit_is_valid_and_one_beyond_it_is_not(self) -> None:
        on_low = [MIN_PRESSURE_MMHG, 40.0, 60.0, 50.0, 40.0, 30.0, 40.0, 50.0, 40.0, 30.0]
        on_high = [MAX_PRESSURE_MMHG, 180.0, 160.0, 170.0, 180.0, 190.0, 180.0, 170.0, 180.0, 190.0]
        self.assertTrue(summarise_second(on_low, RATE, 0, arterial=True).valid)
        self.assertTrue(summarise_second(on_high, RATE, 0, arterial=True).valid)
        self.assertFalse(summarise_second([MIN_PRESSURE_MMHG - 0.001, *on_low[1:]], RATE, 0, arterial=True).valid)
        self.assertFalse(summarise_second([MAX_PRESSURE_MMHG + 0.001, *on_high[1:]], RATE, 0, arterial=True).valid)

    def test_a_range_of_exactly_20_is_valid_and_a_range_under_20_is_not(self) -> None:
        exact = [80.0, 100.0] * 5
        under = [80.0, 99.999] * 5
        self.assertEqual(MIN_RANGE_MMHG, 20.0)
        self.assertTrue(summarise_second(exact, RATE, 0, arterial=True).valid)
        self.assertFalse(summarise_second(under, RATE, 0, arterial=True).valid)

    def test_a_jump_of_exactly_30_is_valid_and_a_jump_over_30_is_not(self) -> None:
        exact = [80.0, 110.0, 80.0, 110.0, 80.0, 110.0, 80.0, 110.0, 80.0, 110.0]
        over = [80.0, 110.001, 80.0, 110.0, 80.0, 110.0, 80.0, 110.0, 80.0, 110.0]
        self.assertTrue(summarise_second(exact, RATE, 0, arterial=True).valid)
        self.assertFalse(summarise_second(over, RATE, 0, arterial=True).valid)

    def test_a_second_that_is_not_a_whole_block_is_not_valid_for_any_track(self) -> None:
        for arterial in (True, False):
            with self.subTest(arterial=arterial):
                self.assertFalse(summarise_second(GOOD[:9], RATE, 0, arterial=arterial).valid)
                self.assertFalse(summarise_second([*GOOD, 90.0], RATE, 0, arterial=arterial).valid)
                self.assertFalse(summarise_second(GOOD, RATE, 1, arterial=arterial).valid)
                self.assertFalse(summarise_second([*GOOD[:9], math.nan], RATE, 0, arterial=arterial).valid)
                self.assertFalse(summarise_second([*GOOD[:9], math.inf], RATE, 0, arterial=arterial).valid)

    def test_the_limits_of_pressure_apply_to_the_arterial_track_only(self) -> None:
        other = [0.0, 1.0, 0.5, 0.2, 0.9, 0.1, 0.0, 0.3, 0.2, 0.1]
        self.assertTrue(summarise_second(other, RATE, 0, arterial=False).valid)
        self.assertFalse(summarise_second(other, RATE, 0, arterial=True).valid)

    def test_the_whole_block_rule_names_the_count_the_start_and_finiteness(self) -> None:
        self.assertTrue(whole_block(GOOD, RATE, 0))
        self.assertFalse(whole_block(GOOD, 20, 0))
        self.assertFalse(whole_block(GOOD, RATE, -1))
        self.assertFalse(whole_block([], RATE, 0))

    def test_the_arterial_track_is_the_one_the_feature_registry_names(self) -> None:
        from features.registry import WAVEFORM_TRACKS

        self.assertIn(ARTERIAL_WAVEFORM, WAVEFORM_TRACKS)


class PerSecondSeries(unittest.TestCase):
    def test_the_series_value_is_the_mean_of_both_seconds_and_is_valid_when_both_are(self) -> None:
        before = summarise_second(GOOD, RATE, 0, arterial=True)
        second = [85.0, 95.0, 105.0, 95.0, 85.0, 75.0, 85.0, 95.0, 105.0, 95.0]
        current = summarise_second(second, RATE, 0, arterial=True)
        value, valid = series_reading(before, current)
        self.assertTrue(valid)
        self.assertEqual(math.fsum([*GOOD, *second]) / 20, value)

    def test_the_series_is_invalid_when_the_earlier_second_is_missing_or_invalid_or_the_current_one_is(self) -> None:
        good = summarise_second(GOOD, RATE, 0, arterial=True)
        bad = summarise_second(GOOD[:9], RATE, 0, arterial=True)
        for before, current in ((None, good), (bad, good), (good, bad)):
            with self.subTest(before=before, current=current):
                self.assertFalse(series_reading(before, current)[1])

    def test_a_jump_across_the_second_boundary_of_exactly_30_is_valid_and_over_30_is_not(self) -> None:
        before = summarise_second([60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 100.0, 90.0, 80.0, 70.0], RATE, 0, arterial=True)
        ok = summarise_second([100.0, 90.0, 80.0, 70.0, 60.0, 70.0, 80.0, 90.0, 100.0, 90.0], RATE, 0, arterial=True)
        jump = summarise_second([100.001, 90.0, 80.0, 70.0, 60.0, 70.0, 80.0, 90.0, 100.0, 90.0], RATE, 0, arterial=True)
        self.assertTrue(series_reading(before, ok)[1])
        self.assertFalse(series_reading(before, jump)[1])


class PhysicalSamples(unittest.TestCase):
    def test_a_physical_sample_is_the_one_shared_definition(self) -> None:
        raw = [0.0, 1.0, -3.5, 100.0]
        self.assertEqual([physical_value(value, 0.25, 4.0) for value in raw], list(physical_samples(raw, 0.25, 4.0)))
        self.assertEqual([4.0, 4.25, 3.125, 29.0], list(physical_samples(raw, 0.25, 4.0)))


if __name__ == "__main__":
    unittest.main()
