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

from features.eligibility import Eligibility, warning_eligibility

from features.model import Reading, WaveformSecond, WaveformWindow
from features.registry import NUMERIC_OVER_10_PERCENT_INVALID, WAVEFORM_INVALID_SECOND
from tests.fast.features_support import CUTOFF_US, SECOND, alternating, numeric_window, waveform_window


def series(invalid: int, count: int = 900) -> list[Reading]:
    """A window of ``count`` seconds whose first ``invalid`` seconds are invalid."""
    return numeric_window([80.0] * count, valid=lambda index: index >= invalid)


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
        outside = [Reading(CUTOFF_US - 900 * SECOND, 80.0, True), Reading(CUTOFF_US + SECOND, 80.0, True), Reading(CUTOFF_US + 2 * SECOND, 80.0, False)]
        self.assertTrue(check(readings + outside, waveform_window(alternating)).eligible)
        self.assertFalse(check(series(91) + outside, waveform_window(alternating)).eligible)

    def test_both_limits_failed_give_both_reasons(self) -> None:
        result = check(series(91), waveform_window(alternating, invalid=frozenset({3})))
        self.assertFalse(result.eligible)
        self.assertEqual((WAVEFORM_INVALID_SECOND, NUMERIC_OVER_10_PERCENT_INVALID), result.reasons)

    def test_the_reasons_are_the_plan_names(self) -> None:
        self.assertEqual("waveform_invalid_second", WAVEFORM_INVALID_SECOND)
        self.assertEqual("numeric_over_10_percent_invalid", NUMERIC_OVER_10_PERCENT_INVALID)


if __name__ == "__main__":
    unittest.main()
