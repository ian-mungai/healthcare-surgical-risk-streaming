"""The validity of a reading and of a waveform second, as the batch and the stream paths both decide it (analysis plan, row 4 and row 5).

Failure modes covered here:

- A boundary moves: exactly 20 or 200 mmHg, exactly a range of 20 mmHg and exactly a jump of 30 mmHg are valid, and the next value past each is
  invalid with its own cause.
- A difference that a float rounds onto its limit is decided by the exact values of the floats, so a segment whose range is a hair under 20 mmHg is flat.
- The flat rule looks at the second alone, or at fewer than the 20 seconds that end with it, or counts a sample that is missing or outside 20 to 200 mmHg, or a
  sample that has left the window, or moves the first second at which a segment is long enough.
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
    EMPTY_EXTENT,
    FIRST_OCCURRENCE,
    FLAT,
    FLAT_WINDOW_SECONDS,
    GAP_MARKERS,
    INCOMPLETE,
    INVALID_CAUSES,
    JUMP,
    MISSING,
    OUT_OF_RANGE,
    VALID,
    Extent,
    Verdict,
    combine_extents,
    difference_above,
    difference_below,
    is_duplicate,
    numeric_verdict,
    physical_samples,
    valid_extent,
    waveform_verdict,
)

ART = "SNUADC/ART"
ECG = "SNUADC/ECG_II"
RATE = 4


def second(*samples: float) -> tuple[float, ...]:
    return tuple(samples)


def arterial(
    samples: tuple[float | None, ...], previous_last: float | None = None, offset_us: int = 0, rate: float = RATE, prior: Extent = EMPTY_EXTENT
) -> Verdict:
    return waveform_verdict(ART, samples, rate, offset_us, previous_last, prior)


def full_prior(low: float, high: float, missing: int = 0) -> Extent:
    """The valid samples of the 19 seconds before a second of ``RATE`` samples, less ``missing`` of them."""
    return Extent((FLAT_WINDOW_SECONDS - 1) * RATE - missing, low, high)


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
        self.assertEqual(VALID, arterial(second(100.0, 100.0, 100.0, 100.0), prior=full_prior(100.0, 120.0)))
        self.assertEqual(VALID, arterial(second(80.0, 100.0, 80.0, 100.0), prior=full_prior(100.0, 100.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0), prior=full_prior(100.0, 119.99999999999999)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(81.0, 100.0, 81.0, 100.0), prior=full_prior(100.0, 100.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0), prior=full_prior(100.0, 100.0)))

    def test_the_segment_is_the_20_seconds_that_end_with_the_second_and_not_the_second_alone(self) -> None:
        self.assertEqual(20, FLAT_WINDOW_SECONDS)
        self.assertEqual(VALID, arterial(second(100.0, 100.0, 100.0, 100.0), prior=full_prior(80.0, 100.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0), prior=full_prior(90.0, 100.0)))

    def test_a_segment_shorter_than_20_seconds_of_samples_is_never_flat(self) -> None:
        constant = second(100.0, 100.0, 100.0, 100.0)
        self.assertEqual(VALID, arterial(constant))
        self.assertEqual(VALID, arterial(constant, prior=full_prior(100.0, 100.0, missing=1)))
        self.assertEqual(Verdict(False, FLAT), arterial(constant, prior=full_prior(100.0, 100.0)))
        self.assertEqual(VALID, arterial(constant, prior=Extent(0, None, None)))

    def test_the_length_of_a_segment_is_20_seconds_of_the_rate(self) -> None:
        constant = second(100.0, 100.0)
        self.assertEqual(VALID, arterial(constant, rate=2, prior=Extent(37, 100.0, 100.0)))
        self.assertEqual(Verdict(False, FLAT), arterial(constant, rate=2, prior=Extent(38, 100.0, 100.0)))

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

    def test_a_rate_with_a_fraction_is_not_cut_to_a_whole_rate(self) -> None:
        for rate in (4.5, 4.000001, 3.999999):
            with self.subTest(rate=rate):
                self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(100.0, 120.0, 100.0, 120.0), rate=rate))

    def test_a_whole_rate_given_as_a_float_is_a_whole_rate(self) -> None:
        self.assertEqual(VALID, arterial(second(100.0, 120.0, 100.0, 120.0), rate=4.0))

    def test_missing_comes_before_incomplete_and_incomplete_before_every_signal_cause(self) -> None:
        self.assertEqual(Verdict(False, MISSING), arterial(second(math.nan, 0.0, 0.0), offset_us=5))
        self.assertEqual(Verdict(False, INCOMPLETE), arterial(second(0.0, 0.0, 0.0), offset_us=5))

    def test_out_of_range_comes_before_flat_and_flat_before_jump(self) -> None:
        self.assertEqual(Verdict(False, OUT_OF_RANGE), arterial(second(0.0, 0.0, 0.0, 0.0), previous_last=100.0))
        self.assertEqual(Verdict(False, FLAT), arterial(second(100.0, 100.0, 100.0, 100.0), previous_last=50.0, prior=full_prior(100.0, 100.0)))
        self.assertEqual(Verdict(False, JUMP), arterial(second(100.0, 150.0, 100.0, 150.0)))


class Extents(unittest.TestCase):
    def test_the_extent_of_a_second_counts_the_samples_that_are_present_and_inside_the_pressure_limits(self) -> None:
        self.assertEqual(Extent(4, 20.0, 200.0), valid_extent((20.0, 200.0, 100.0, 150.0)))
        self.assertEqual(Extent(2, 50.0, 60.0), valid_extent((50.0, None, 60.0, math.nan, math.inf, 19.999999999999996, 200.00000000000003, 4.5e9)))
        self.assertEqual(EMPTY_EXTENT, valid_extent(()))
        self.assertEqual(EMPTY_EXTENT, valid_extent((None, math.nan, 0.0)))

    def test_extents_combine_by_adding_counts_and_taking_the_least_and_the_most(self) -> None:
        self.assertEqual(Extent(7, 30.0, 90.0), combine_extents([Extent(4, 30.0, 80.0), EMPTY_EXTENT, Extent(3, 40.0, 90.0)]))
        self.assertEqual(EMPTY_EXTENT, combine_extents([]))
        self.assertEqual(EMPTY_EXTENT, combine_extents([EMPTY_EXTENT, EMPTY_EXTENT]))


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

    def test_the_gap_marker_of_each_integer_format_is_a_missing_sample_before_calibration(self) -> None:
        markers = {3: -128.0, 4: 255.0, 5: -32768.0, 6: 65535.0, 7: -2147483648.0, 8: 4294967295.0}
        for fmt, marker in markers.items():
            with self.subTest(fmt=fmt):
                self.assertEqual(markers, GAP_MARKERS)
                self.assertEqual([None, 0.0001 * 100.0 + 80.0, None], physical_samples([marker, 100.0, marker], fmt, 0.0001, 80.0))

    def test_the_uint16_marker_does_not_become_a_calibrated_pressure(self) -> None:
        samples = physical_samples([65535.0, 65535.0, 65535.0, 65535.0], 6, 0.0001, 80.0)
        self.assertEqual([None] * 4, samples)
        self.assertEqual(Verdict(False, MISSING), arterial(tuple(samples)))

    def test_a_value_next_to_a_marker_and_a_marker_of_another_format_are_samples(self) -> None:
        self.assertEqual([65534.0 * 0.5, 65536.0 * 0.5], physical_samples([65534.0, 65536.0], 6, 0.5, 0.0))
        self.assertEqual([-32767.0 * 0.5, 32767.0 * 0.5], physical_samples([-32767.0, 32767.0], 5, 0.5, 0.0))
        self.assertEqual([65535.0 * 0.5], physical_samples([65535.0], 5, 0.5, 0.0))
        self.assertEqual([-32768.0 * 0.5], physical_samples([-32768.0], 6, 0.5, 0.0))

    def test_a_float_format_keeps_every_value_that_an_integer_marker_would_be(self) -> None:
        for fmt in (1, 2):
            with self.subTest(fmt=fmt):
                self.assertEqual([65535.0, 255.0, -32768.0], physical_samples([65535.0, 255.0, -32768.0], fmt, 0.5, 10.0))

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
