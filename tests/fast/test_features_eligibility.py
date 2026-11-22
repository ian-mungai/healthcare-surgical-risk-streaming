"""The sample eligibility limits of the analysis plan (project standard, "Analysis decisions and their evidence", row 5).

Failure modes covered here:

- A window with one invalid waveform second, or a second that is missing, is eligible.
- The 10% limit moves: exactly 10% invalid seconds of the per-second arterial series is refused, or one second more is accepted.
- A missing second of the series is not counted as invalid.
- A second outside the window (before it, or after the cutoff) changes the answer.
- A failed limit gives no reason, or the wrong one, or two failed limits give one reason.
"""

from __future__ import annotations

import unittest
from typing import Any

from features.eligibility import Eligibility, warning_eligibility
from features.model import Reading, WaveformSecond, WaveformWindow
from features.registry import NUMERIC_OVER_10_PERCENT_INVALID, WAVEFORM_INVALID_SECOND
from tests.fast.features_support import CUTOFF_US, SECOND, alternating, second_of, series_window, waveform_window


def series(invalid: int, count: int = 900) -> list[Reading]:
    """A window of ``count`` seconds whose first ``invalid`` seconds are invalid."""
    return series_window([80.0] * count, valid=lambda index: index >= invalid)


def check(readings: list[Reading], window: WaveformWindow, cutoff_us: int = CUTOFF_US) -> Eligibility:
    return warning_eligibility(cutoff_us, readings, window)


class Waveform(unittest.TestCase):
    def test_a_window_with_every_second_valid_and_a_full_series_is_eligible(self) -> None:
        result = check(series(0), waveform_window(alternating))
        self.assertTrue(result.eligible)
        self.assertEqual((), result.reasons)

    def test_one_invalid_second_makes_the_sample_not_eligible(self) -> None:
        for position in (0, 7, 19):
            with self.subTest(position=position):
                result = check(series(0), waveform_window(alternating, invalid=frozenset({position})))
                self.assertFalse(result.eligible)
                self.assertEqual((WAVEFORM_INVALID_SECOND,), result.reasons)

    def test_a_missing_second_makes_the_sample_not_eligible(self) -> None:
        window = waveform_window(alternating)
        shorter = WaveformWindow(window.sample_rate, window.seconds[:9] + window.seconds[10:])
        self.assertEqual((WAVEFORM_INVALID_SECOND,), check(series(0), shorter).reasons)
        self.assertEqual((WAVEFORM_INVALID_SECOND,), check(series(0), WaveformWindow(window.sample_rate, ())).reasons)

    def test_an_invalid_second_outside_the_window_changes_nothing(self) -> None:
        window = waveform_window(alternating)
        before = WaveformSecond(window.seconds[0].second - 1, (999.0,) * 10, False)
        after = WaveformSecond(window.seconds[-1].second + 1, (999.0,) * 10, False)
        padded = WaveformWindow(window.sample_rate, (before, *window.seconds, after))
        self.assertTrue(check(series(0), padded).eligible)


class Numeric(unittest.TestCase):
    def test_exactly_10_percent_invalid_is_eligible(self) -> None:
        self.assertTrue(check(series(90), waveform_window(alternating)).eligible)

    def test_one_second_more_than_10_percent_invalid_is_not_eligible(self) -> None:
        result = check(series(91), waveform_window(alternating))
        self.assertFalse(result.eligible)
        self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), result.reasons)

    def test_a_missing_second_of_the_series_counts_as_invalid(self) -> None:
        self.assertTrue(check(series(0, count=810), waveform_window(alternating)).eligible)
        self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), check(series(0, count=809), waveform_window(alternating)).reasons)
        self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), check([], waveform_window(alternating)).reasons)

    def test_a_second_outside_the_window_changes_nothing(self) -> None:
        readings = series(90)
        outside = [Reading(CUTOFF_US - 901 * SECOND, 80.0, True), Reading(CUTOFF_US, 80.0, True), Reading(CUTOFF_US + 2 * SECOND, 80.0, False)]
        self.assertTrue(check(readings + outside, waveform_window(alternating)).eligible)
        self.assertFalse(check(series(91) + outside, waveform_window(alternating)).eligible)

    def test_both_limits_failed_give_both_reasons(self) -> None:
        result = check(series(91), waveform_window(alternating, invalid=frozenset({3})))
        self.assertFalse(result.eligible)
        self.assertEqual((WAVEFORM_INVALID_SECOND, NUMERIC_OVER_10_PERCENT_INVALID), result.reasons)

    def test_the_reasons_are_the_plan_names(self) -> None:
        self.assertEqual("waveform_invalid_second", WAVEFORM_INVALID_SECOND)
        self.assertEqual("numeric_over_10_percent_invalid", NUMERIC_OVER_10_PERCENT_INVALID)


class NullValues(unittest.TestCase):
    def window(self, invalid_with_null: int, absent: int) -> list[Reading]:
        """900 seconds: ``invalid_with_null`` readings flagged invalid with a null value, ``absent`` seconds left out, the rest valid."""
        seconds = range(CUTOFF_US // SECOND - 900, CUTOFF_US // SECOND)
        readings: list[Reading] = []
        for position, second in enumerate(seconds):
            if position < absent:
                continue
            junk: Any = None
            readings.append(Reading(second * SECOND, junk, False) if position < absent + invalid_with_null else Reading(second * SECOND, 80.0, True))
        return readings

    def test_89_absent_seconds_and_one_invalid_second_with_a_null_value_is_exactly_10_percent_and_eligible(self) -> None:
        self.assertEqual(810, sum(reading.valid for reading in self.window(1, 89)))
        self.assertTrue(check(self.window(1, 89), waveform_window(alternating)).eligible)

    def test_one_more_invalid_second_is_not_eligible(self) -> None:
        self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), check(self.window(1, 90), waveform_window(alternating)).reasons)
        self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), check(self.window(2, 89), waveform_window(alternating)).reasons)

    def test_an_invalid_waveform_second_with_no_samples_is_still_one_invalid_second(self) -> None:
        window = waveform_window(alternating)
        empty: Any = None
        seconds = (WaveformSecond(window.seconds[3].second, empty, False), *window.seconds[:3], *window.seconds[4:])
        self.assertEqual((WAVEFORM_INVALID_SECOND,), check(series(0), WaveformWindow(window.sample_rate, seconds)).reasons)


class Cutoffs(unittest.TestCase):
    """The windows are the newest complete seconds, so a cutoff a microsecond either side of a second boundary moves them as the definition says."""

    K = 3600

    def waveform(self, invalid_second: int) -> WaveformWindow:
        seconds = tuple(second_of(index, [90.0 + index % 7] * 10, index != invalid_second) for index in range(self.K - 30, self.K + 6))
        return WaveformWindow(10.0, seconds)

    def series(self, invalid_from: int, invalid_to: int) -> list[Reading]:
        return [Reading(second * SECOND, 80.0, not invalid_from <= second <= invalid_to) for second in range(self.K - 1000, self.K + 6)]

    def test_the_waveform_limit_looks_at_the_20_newest_complete_seconds(self) -> None:
        k = self.K
        # The invalid second k - 1 is the newest complete second at cutoffs from k seconds on and is not complete before.
        newest = self.waveform(k - 1)
        # The invalid second k - 21 is the oldest of the window at k seconds minus one microsecond and is outside it from k seconds on.
        oldest = self.waveform(k - 21)
        flat = series_window([80.0] * 900, cutoff_us=k * SECOND)
        for cutoff, newest_eligible, oldest_eligible in (
            (k * SECOND - 1, True, False),
            (k * SECOND, False, True),
            (k * SECOND + 1, False, True),
            (k * SECOND + 999_999, False, True),
        ):
            with self.subTest(cutoff=cutoff):
                self.assertEqual(newest_eligible, warning_eligibility(cutoff, flat, newest).eligible)
                self.assertEqual(oldest_eligible, warning_eligibility(cutoff, flat, oldest).eligible)

    def test_the_series_limit_looks_at_the_900_newest_complete_seconds(self) -> None:
        k = self.K
        readings = self.series(k - 91, k - 1)  # 91 invalid seconds ending with second k - 1
        window = self.waveform(-1)
        for cutoff, eligible in (
            (k * SECOND - 1, True),
            (k * SECOND, False),
            (k * SECOND + 1, False),
            (k * SECOND + 999_999, False),
            ((k + 1) * SECOND, False),
        ):
            with self.subTest(cutoff=cutoff):
                result = warning_eligibility(cutoff, readings, window)
                self.assertEqual(eligible, result.eligible)
                self.assertEqual((), result.reasons) if eligible else self.assertEqual((NUMERIC_OVER_10_PERCENT_INVALID,), result.reasons)


if __name__ == "__main__":
    unittest.main()
