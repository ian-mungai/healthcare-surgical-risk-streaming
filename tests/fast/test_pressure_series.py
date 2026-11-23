"""The per-second arterial pressure series ``Silver/ART_MBP_1S``: the trailing 2-second average of ``SNUADC/ART``, with the validity of each second.

Failure modes covered here:

- The average takes the wrong seconds: the second alone, the next second (a value known after its time), or three seconds instead of two.
- The average is not of every sample of both seconds, so a second with more samples weighs the same as one with fewer.
- A second counts as valid when the second before it is invalid or absent (a gap, the first second of a recording), so an invalid sample reaches an average.
- A second that is invalid itself gets a value, or keeps the wrong cause.
- A reading is rounded before it is compared with a limit: two seconds whose exact mean is below 65 give a float of exactly 65.0, and a threshold test on that
  float calls the second not low. The exact mean is kept (a sum and the error of the sum of each second), and every limit test uses it.
- The batch frame and the stream path decide differently: both call ``series_second`` on the same two seconds, so the same two seconds give the same reading.
"""

from __future__ import annotations

import unittest
from fractions import Fraction

from features.pressure_series import (
    SERIES_TRACK,
    TRAILING_SECONDS,
    ExactMean,
    SecondFacts,
    SeriesReading,
    exact_below,
    second_facts,
    series_from_seconds,
    series_second,
)
from features.signal_validity import FLAT, JUMP, MISSING, VALID, Verdict


def valid(second: int, *samples: float) -> SecondFacts:
    return second_facts(second, VALID, samples)


def invalid(second: int, cause: str) -> SecondFacts:
    return second_facts(second, Verdict(False, cause), ())


class Definition(unittest.TestCase):
    def test_the_series_is_the_arterial_waveform_and_the_window_is_two_seconds(self) -> None:
        self.assertEqual("SNUADC/ART", SERIES_TRACK)
        self.assertEqual(2, TRAILING_SECONDS)

    def test_the_value_is_the_mean_of_the_samples_of_this_second_and_the_one_before(self) -> None:
        reading = series_second(valid(9, 60.0, 62.0), valid(10, 70.0, 72.0))
        self.assertEqual((66.0, True, None), (reading.value, reading.valid, reading.cause))

    def test_every_sample_counts_once_so_a_second_with_more_samples_weighs_more(self) -> None:
        # 1 sample of 60 and 3 samples of 80: the mean of the 4 samples is 75, and not the mean of the two second means (70).
        reading = series_second(valid(9, 60.0), valid(10, 80.0, 80.0, 80.0))
        self.assertEqual(75.0, reading.value)

    def test_the_second_after_is_never_read(self) -> None:
        before, current = valid(9, 60.0, 60.0), valid(10, 70.0, 70.0)
        self.assertEqual(65.0, series_second(before, current).value)
        # The same two seconds give the same reading whatever follows them.
        readings = dict(series_from_seconds([before, current, valid(11, 200.0, 200.0)]))
        self.assertEqual(65.0, readings[10].value)

    def test_a_value_is_the_exact_float_of_the_sums_with_no_drift_over_many_samples(self) -> None:
        samples = (0.1,) * 500
        self.assertEqual(0.1, series_second(valid(0, *samples), valid(1, *samples)).value)


def below(reading: SeriesReading, limit: float) -> bool:
    if reading.mean is None:
        raise AssertionError("the reading has no mean")
    return reading.mean.below(limit)


class ExactThreshold(unittest.TestCase):
    """The reading keeps its exact mean, so that no rounding of the value decides a limit test."""

    def tie(self) -> SeriesReading:
        # The exact mean of 65.0 and 64.99999999999999 (four samples each) is half way between two floats: 64.999999999999992895, which a float rounds to 65.0.
        return series_second(valid(9, *[65.0] * 4), valid(10, *[64.99999999999999] * 4))

    def test_a_mean_that_a_float_rounds_up_to_the_limit_is_still_below_it(self) -> None:
        reading = self.tie()
        self.assertEqual(65.0, reading.value)
        self.assertTrue(below(reading, 65.0))
        self.assertTrue(exact_below(Fraction(65) - Fraction(1, 10**15), 65.0))

    def test_a_mean_of_exactly_the_limit_is_not_below_it_and_one_hair_over_is_not_either(self) -> None:
        self.assertFalse(below(series_second(valid(9, 65.0, 65.0), valid(10, 65.0, 65.0)), 65.0))
        self.assertFalse(below(series_second(valid(9, 65.0, 65.0), valid(10, 65.00000000000001, 65.0)), 65.0))
        self.assertTrue(below(series_second(valid(9, 65.0, 65.0), valid(10, 64.99999999999999, 65.0)), 65.0))

    def test_the_exact_mean_is_the_sum_of_every_sample_over_their_count(self) -> None:
        first, second = [0.1, 0.2, 0.3, 1e-9], [64.0, 66.0, 65.5, 0.1]
        mean = series_second(valid(9, *first), valid(10, *second)).mean
        if mean is None:
            raise AssertionError("the reading has no mean")
        self.assertEqual(8, mean.count)
        self.assertEqual(sum((Fraction(x) for x in [*first, *second]), Fraction(0)) / 8, mean.exact)

    def test_the_error_of_the_sum_of_a_second_is_kept_so_the_two_floats_hold_the_exact_sum(self) -> None:
        samples = [100.0 + index * 1e-9 for index in range(500)]
        facts = valid(3, *samples)
        self.assertEqual(sum((Fraction(x) for x in samples), Fraction(0)), Fraction(facts.total) + Fraction(facts.residual))
        self.assertNotEqual(0.0, facts.residual)

    def test_the_value_is_the_exact_mean_rounded_once(self) -> None:
        samples = [100.0 + index * 1e-9 for index in range(500)]
        reading = series_second(valid(3, *samples), valid(4, *samples))
        if reading.mean is None:
            raise AssertionError("the reading has no mean")
        self.assertEqual(float(reading.mean.exact), reading.value)

    def test_a_single_sample_mean_helper_is_that_sample_exactly(self) -> None:
        mean = ExactMean.of(64.99999999999999)
        self.assertEqual((64.99999999999999, True), (mean.value, mean.below(65.0)))

    def test_an_invalid_reading_has_no_mean(self) -> None:
        self.assertIsNone(series_second(None, valid(0, 60.0)).mean)


class Validity(unittest.TestCase):
    def test_a_second_that_is_invalid_has_no_value_and_keeps_its_own_cause(self) -> None:
        reading = series_second(valid(9, 60.0), invalid(10, JUMP))
        self.assertEqual(SeriesReading(None, False, JUMP), reading)

    def test_a_valid_second_after_an_invalid_one_is_invalid_with_the_cause_of_the_one_before(self) -> None:
        reading = series_second(invalid(9, FLAT), valid(10, 60.0))
        self.assertEqual(SeriesReading(None, False, FLAT), reading)

    def test_the_cause_of_the_second_itself_comes_before_the_cause_of_the_one_before(self) -> None:
        self.assertEqual(JUMP, series_second(invalid(9, FLAT), invalid(10, JUMP)).cause)

    def test_the_first_second_of_a_recording_has_no_second_before_and_is_missing(self) -> None:
        self.assertEqual(SeriesReading(None, False, MISSING), series_second(None, valid(0, 60.0)))

    def test_a_gap_before_the_second_is_missing_and_never_bridged(self) -> None:
        self.assertEqual(SeriesReading(None, False, MISSING), series_second(valid(7, 60.0), valid(10, 60.0)))

    def test_a_second_that_is_not_after_the_one_given_before_it_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            series_second(valid(10, 60.0), valid(10, 60.0))
        with self.assertRaises(ValueError):
            series_second(valid(11, 60.0), valid(10, 60.0))

    def test_a_valid_second_with_no_sample_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            second_facts(3, VALID, ())

    def test_a_valid_second_with_a_sample_that_is_not_finite_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            second_facts(3, VALID, (60.0, float("nan")))


class FromSeconds(unittest.TestCase):
    def test_every_second_has_one_reading_in_order_and_a_gap_makes_two_invalid_readings(self) -> None:
        seconds = [valid(0, 60.0), valid(1, 60.0), invalid(2, JUMP), valid(3, 60.0), valid(4, 60.0)]
        got = [(second, reading.valid, reading.cause) for second, reading in series_from_seconds(reversed(seconds))]
        self.assertEqual([(0, False, MISSING), (1, True, None), (2, False, JUMP), (3, False, JUMP), (4, True, None)], got)

    def test_two_seconds_with_one_index_are_refused(self) -> None:
        with self.assertRaises(ValueError):
            list(series_from_seconds([valid(1, 60.0), valid(1, 61.0)]))


if __name__ == "__main__":
    unittest.main()
