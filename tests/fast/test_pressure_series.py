"""The per-second arterial pressure series ``Silver/ART_MBP_1S``: the trailing 2-second average of ``SNUADC/ART``, with the validity of each second.

Failure modes covered here:

- The average takes the wrong seconds: the second alone, the next second (a value known after its time), or three seconds instead of two.
- The average is not of every sample of both seconds, so a second with more samples weighs the same as one with fewer.
- A second counts as valid when the second before it is invalid or absent (a gap, the first second of a recording), so an invalid sample reaches an average.
- A second that is invalid itself gets a value, or keeps the wrong cause.
- The batch frame and the stream path decide differently: both call ``series_second`` on the same two seconds, so the same two seconds give the same reading.
"""

from __future__ import annotations

import unittest

from features.pressure_series import (
    SERIES_TRACK,
    TRAILING_SECONDS,
    SecondFacts,
    SeriesReading,
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
        self.assertEqual(SeriesReading(66.0, True, None), reading)

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
