"""The validity of a reading and of a waveform second, as the batch and the stream paths both decide it (analysis plan, row 4 and row 5).

Failure modes covered here:

- A boundary moves: exactly 20 or 200 mmHg, exactly a range of 20 mmHg and exactly a jump of 30 mmHg are valid, and the next value past each is
  invalid with its own cause.
- A difference that a float rounds onto its limit is decided by the exact values of the floats, so a second whose range is a hair under 20 mmHg is flat.
- A rule leaks to a track that has no sourced rule: ECG and pleth seconds, or a numeric track other than the arterial mean, are judged by a pressure limit.
- A second that is missing, short, late or on a rate that is not a whole number is called valid, or gets another cause than the one that comes first.
- A jump over the boundary between two seconds is not seen, or is seen against a sample that is itself missing.
- A raw sample is not turned into the physical value as the publisher's library does it (integer formats only), or a float format is scaled.
- A duplicate reading is told apart from the first reading of its time by anything but its position.
"""

from __future__ import annotations

import math
import unittest

from features.signal_validity import (
    FIRST_OCCURRENCE,
    FLAT,
    INCOMPLETE,
    INVALID_CAUSES,
    JUMP,
    MISSING,
    OUT_OF_RANGE,
    VALID,
    Verdict,
    difference_above,
    difference_below,
    is_duplicate,
    numeric_verdict,
    physical_samples,
    waveform_verdict,
)

ART = "SNUADC/ART"
ECG = "SNUADC/ECG_II"
RATE = 4


def second(*samples: float) -> tuple[float, ...]:
    return tuple(samples)


def arterial(samples: tuple[float | None, ...], previous_last: float | None = None, offset_us: int = 0, rate: float = RATE) -> Verdict:
    return waveform_verdict(ART, samples, rate, offset_us, previous_last)


class Numeric(unittest.TestCase):
    def test_a_value_inside_the_limits_of_the_arterial_mean_is_valid_and_the_limits_are_valid(self) -> None:
        for value in (20.0, 80.0, 200.0):
            with self.subTest(value=value):
                self.assertEqual(VALID, numeric_verdict("Solar8000/ART_MBP", value))

    def test_a_value_past_a_limit_of_the_arterial_mean_is_out_of_range(self) -> None:
        for value in (19.999999999999996, 200.00000000000003, -5.0, 1e9):
            with self.subTest(value=value):
                self.assertEqual(Verdict(False, OUT_OF_RANGE), numeric_verdict("Solar8000/ART_MBP", value))

    def test_a_missing_or_not_finite_value_is_missing_on_every_track(self) -> None:
        for track in ("Solar8000/ART_MBP", "Solar8000/HR"):
            for value in (None, math.nan, math.inf, -math.inf):
                with self.subTest(track=track, value=value):
                    self.assertEqual(Verdict(False, MISSING), numeric_verdict(track, value))

    def test_a_track_with_no_sourced_limit_is_never_out_of_range(self) -> None:
        for track in ("Solar8000/HR", "Solar8000/ART_SBP", "Solar8000/ART_DBP", "Primus/MV"):
            for value in (-1000.0, 0.0, 1e9):
                with self.subTest(track=track, value=value):
                    self.assertEqual(VALID, numeric_verdict(track, value))


class Boundaries(unittest.TestCase):
    def test_a_second_on_every_limit_is_valid(self) -> None:
        self.assertEqual(VALID, arterial(second(20.0, 40.0, 60.0, 80.0)))
        self.assertEqual(VALID, arterial(second(180.0, 200.0, 190.0, 185.0)))
        self.assertEqual(VALID, arterial(second(100.0, 120.0, 100.0, 120.0)))

    def test_a_sample_past_a_pressure_limit_is_out_of_range(self) -> None:
        self.assertEqual(Verdict(False, OUT_OF_RANGE), arterial(second(19.999999999999996, 40.0, 60.0, 80.0)))
        self.assertEqual(Verdict(False, OUT_OF_RANGE), arterial(second(120.0, 140.0, 160.0, 200.00000000000003)))

    def test_a_range_just_under_the_minimum_is_flat_and_the_minimum_itself_is_not(self) -> None:
        self.assertEqual(VALID, arterial(second(80.0, 90.0, 100.0, 85.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(80.0, 90.0, 99.99999999999999, 85.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0)))

    def test_a_jump_of_exactly_the_limit_is_valid_and_any_more_is_a_jump(self) -> None:
        self.assertEqual(VALID, arterial(second(80.0, 110.0, 90.0, 120.0)))
        self.assertEqual(Verdict(False, JUMP), arterial(second(80.0, 110.00000000000001, 90.0, 100.0)))
        self.assertEqual(Verdict(False, JUMP), arterial(second(80.0, 90.0, 121.0, 100.0)))

    def test_a_falling_jump_is_a_jump_too(self) -> None:
        self.assertEqual(Verdict(False, JUMP), arterial(second(140.0, 109.0, 120.0, 130.0)))

    def test_the_first_sample_is_compared_with_the_last_sample_of_the_second_before(self) -> None:
        samples = second(120.0, 100.0, 130.0, 110.0)
        self.assertEqual(VALID, arterial(samples, previous_last=90.0))
        self.assertEqual(Verdict(False, JUMP), arterial(samples, previous_last=89.0))
        self.assertEqual(VALID, arterial(samples, previous_last=None))

    def test_a_previous_sample_that_is_missing_is_not_a_reading_to_compare_with(self) -> None:
        samples = second(120.0, 100.0, 130.0, 110.0)
        for previous in (math.nan, math.inf, 5e9):
            with self.subTest(previous=previous):
                self.assertEqual(VALID, arterial(samples, previous_last=previous))

    def test_a_difference_that_a_float_rounds_onto_the_limit_is_decided_exactly(self) -> None:
        low, high = 1e-20, 20.0
        self.assertEqual(20.0, high - low)
        self.assertTrue(difference_below(high, low, 20.0))
        self.assertFalse(difference_below(21.0, 1.0, 20.0))
        self.assertFalse(difference_below(25.0, 1.0, 20.0))
        self.assertTrue(difference_above(30.0 + 1e-14, 0.0, 30.0))
        self.assertFalse(difference_above(30.0, 0.0, 30.0))
        self.assertFalse(difference_above(0.0, 30.0, 30.0))
        self.assertTrue(difference_above(0.0, 30.0 + 1e-14, 30.0))
        self.assertTrue(difference_above(30.0, -1e-20, 30.0))
        self.assertFalse(difference_above(30.0, 1e-20, 30.0))


class Causes(unittest.TestCase):
    def test_the_causes_are_in_the_order_that_decides_between_them(self) -> None:
        self.assertEqual((MISSING, INCOMPLETE, OUT_OF_RANGE, FLAT, JUMP), INVALID_CAUSES)

    def test_a_sample_that_is_missing_makes_the_second_missing_first(self) -> None:
        for bad in (None, math.nan, math.inf, -math.inf, 4.000000001e9):
            with self.subTest(bad=bad):
                self.assertEqual(Verdict(False, MISSING), arterial(second(100.0, 120.0, bad, 110.0)))  # type: ignore[arg-type]
        self.assertEqual(Verdict(False, MISSING), arterial(()))
        self.assertEqual(Verdict(False, MISSING), arterial((None, None, None)))

    def test_the_sentinel_value_itself_is_a_number_and_the_publishers_missing_marker_is_what_is_above_it(self) -> None:
        self.assertEqual(Verdict(False, OUT_OF_RANGE), arterial(second(100.0, 120.0, 4e9, 110.0)))

    def test_a_second_that_is_short_or_long_or_late_is_incomplete(self) -> None:
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0)))
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0, 120.0, 100.0)))
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0, 120.0), offset_us=1))
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0, 120.0), offset_us=-1))

    def test_a_rate_that_is_not_a_positive_whole_number_leaves_no_complete_second(self) -> None:
        for rate in (62.5, 0.0, -4.0, math.nan, math.inf):
            with self.subTest(rate=rate):
                self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0, 120.0), rate=rate))

    def test_a_whole_rate_given_as_a_float_is_a_whole_rate(self) -> None:
        self.assertEqual(VALID, arterial(second(100.0, 120.0, 100.0, 120.0), rate=4.0))

    def test_missing_comes_before_incomplete_and_incomplete_before_every_signal_cause(self) -> None:
        self.assertEqual(Verdict(False, MISSING), arterial(second(math.nan, 0.0, 0.0), offset_us=5))
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(0.0, 0.0, 0.0), offset_us=5))

    def test_out_of_range_comes_before_flat_and_flat_before_jump(self) -> None:
        self.assertEqual(Verdict(False, OUT_OF_RANGE), arterial(second(0.0, 0.0, 0.0, 0.0), previous_last=100.0))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0), previous_last=50.0))
        self.assertEqual(Verdict(False, JUMP), arterial(second(100.0, 150.0, 100.0, 150.0)))


class OtherWaveforms(unittest.TestCase):
    def test_a_waveform_with_no_sourced_rule_is_judged_by_its_structure_only(self) -> None:
        flat_and_wild = second(0.0, 0.0, 1e8, -1e8)
        self.assertEqual(VALID, waveform_verdict(ECG, flat_and_wild, RATE, 0, 1e8))
        self.assertEqual(VALID, waveform_verdict("SNUADC/PLETH", second(5.0, 5.0, 5.0, 5.0), RATE, 0, None))

    def test_structure_still_decides_for_them(self) -> None:
        self.assertEqual(Verdict(False, MISSING), waveform_verdict(ECG, second(0.0, math.nan, 0.0, 0.0), RATE, 0, None))
        self.assertEqual(Verdict(False, INCOMPLETE), waveform_verdict(ECG, second(0.0, 0.0), RATE, 0, None))
        self.assertEqual(Verdict(False, INCOMPLETE), waveform_verdict(ECG, second(0.0, 0.0, 0.0, 0.0), RATE, 7, None))


class Physical(unittest.TestCase):
    def test_an_integer_format_is_scaled_with_the_gain_and_the_offset(self) -> None:
        for fmt in (3, 4, 5, 6, 7, 8):
            with self.subTest(fmt=fmt):
                self.assertEqual([0.5 * 4 + 10.0, 0.5 * -3 + 10.0], physical_samples([4.0, -3.0], fmt, 0.5, 10.0))

    def test_a_float_format_is_already_physical(self) -> None:
        for fmt in (1, 2):
            with self.subTest(fmt=fmt):
                self.assertEqual([4.0, -3.0], physical_samples([4.0, -3.0], fmt, 0.5, 10.0))

    def test_a_missing_raw_sample_stays_missing_and_is_not_filled(self) -> None:
        result = physical_samples([4.0, None, math.nan], 5, 0.5, 10.0)
        self.assertEqual(12.0, result[0])
        self.assertIsNone(result[1])
        self.assertTrue(isinstance(result[2], float) and math.isnan(result[2]))

    def test_the_result_is_a_new_list_of_floats(self) -> None:
        raw = [1.0, 2.0]
        for fmt in (2, 5):
            result = physical_samples(raw, fmt, 1.0, 0.0)
            self.assertIsNot(raw, result)
            self.assertTrue(all(isinstance(sample, float) for sample in result))


class Duplicates(unittest.TestCase):
    def test_only_the_first_reading_of_a_time_is_kept(self) -> None:
        self.assertEqual(0, FIRST_OCCURRENCE)
        self.assertFalse(is_duplicate(0))
        self.assertTrue(is_duplicate(1))
        self.assertTrue(is_duplicate(7))


if __name__ == "__main__":
    unittest.main()
