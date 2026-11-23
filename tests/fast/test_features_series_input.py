"""The batch feature input for the per-second arterial series is bound to the one definition in ``features.pressure_series`` (``features.series_input``).

Failure modes covered here:

- ``build_row`` accepts any list of readings for ``Silver/ART_MBP_1S``, so a series made by other code (another window, another rounding, another
  validity) is scored, and nothing says the batch and the stream features use one series.
- A feature compares the rounded value of a reading with 65 or 60: a reading whose exact mean is below 65 and whose float is 65.0 is not counted
  below, while the
  event rule (which compares the exact mean) calls the same second low. The two disagree at one cutoff.
- The JSON entry point reads the series as plain readings and so skips the binding, or its readings are not given the exact value of their float.
- A reading of the series is given a time from the wrong end of its second, or a second that is invalid is given a value.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from fractions import Fraction

from features.hypotension import SeriesPoint, is_low
from features.model import FeatureInputError, Reading, WindowInput
from features.pressure_series import ExactMean, SecondFacts, second_facts, series_from_seconds
from features.registry import LOW_PRESSURE_65
from features.row import build_row
from features.series_input import SeriesReadings, series_readings, series_readings_from_rows
from features.signal_validity import VALID, Verdict
from tests.fast.features_support import CUTOFF_US, SECOND, SERIES

PREFIX = "silver_art_mbp_1s__"
FIRST = CUTOFF_US // SECOND - 901


def seconds_of(sample_for: Callable[[int], list[float]]) -> list[SecondFacts]:
    """The facts of 901 consecutive valid seconds that end with the newest complete second of the cutoff, from ``sample_for(second)``."""
    return [second_facts(FIRST + index, VALID, sample_for(FIRST + index)) for index in range(901)]


def alternating(second: int) -> list[float]:
    return [65.0] * 4 if second % 2 == 0 else [64.99999999999999] * 4


def window(readings: list[Reading]) -> WindowInput:
    return WindowInput(CUTOFF_US, {SERIES: readings}, {}, {})


class Binding(unittest.TestCase):
    def test_a_series_that_the_definition_did_not_make_is_refused_by_name(self) -> None:
        plain = [Reading(CUTOFF_US - SECOND, 60.0, True)]
        with self.assertRaisesRegex(FeatureInputError, rf"{SERIES}.*features\.pressure_series"):
            build_row(window(plain))

    def test_a_series_from_the_definition_is_accepted(self) -> None:
        readings = series_readings(seconds_of(lambda second: [60.0] * 4))
        self.assertIsInstance(readings, SeriesReadings)
        self.assertEqual(900, build_row(window(readings)).values[PREFIX + "valid_count"])

    def test_no_series_at_all_is_not_refused(self) -> None:
        self.assertEqual(0, build_row(window([])).values[PREFIX + "valid_count"])

    def test_a_series_with_a_valid_reading_that_has_no_exact_mean_or_the_wrong_one_cannot_be_built(self) -> None:
        with self.assertRaises(FeatureInputError):
            SeriesReadings([Reading(SECOND, 60.0, True)])
        with self.assertRaises(FeatureInputError):
            SeriesReadings([Reading(SECOND, 60.0, True, Fraction(61))])

    def test_the_readings_have_the_time_of_their_second_the_value_of_their_mean_and_no_value_when_invalid(self) -> None:
        seconds = [second_facts(10, VALID, [60.0] * 4), second_facts(11, VALID, [70.0] * 4), second_facts(12, Verdict(False, "jump"), ())]
        got = series_readings(seconds)
        self.assertEqual([(10 * SECOND, False), (11 * SECOND, True), (12 * SECOND, False)], [(r.time_us, r.valid) for r in got])
        self.assertEqual((65.0, Fraction(65)), (got[1].value, got[1].exact))
        self.assertTrue(all(r.exact is None for r in got if not r.valid))

    def test_the_json_triples_of_a_series_are_bound_with_the_exact_value_of_their_float(self) -> None:
        got = series_readings_from_rows([(SECOND, 64.99999999999999, True), (2 * SECOND, 0.0, False)])
        self.assertIsInstance(got, SeriesReadings)
        self.assertEqual(Fraction(64.99999999999999), got[0].exact)
        self.assertIsNone(got[1].exact)


class ParityAtOneCutoff(unittest.TestCase):
    """The features and the event rule read the same series and call the same seconds low, at one cutoff."""

    def check(self, sample_for: Callable[[int], list[float]]) -> None:
        facts = seconds_of(sample_for)
        readings = series_readings(facts)
        points = [SeriesPoint(second, reading.mean, reading.valid) for second, reading in series_from_seconds(facts)]
        low_seconds = [point.second for point in points[-900:] if is_low(point)]
        values = build_row(window(readings)).values
        self.assertEqual(len(low_seconds) / 60, values[PREFIX + "minutes_below_65"])
        self.assertEqual((CUTOFF_US - (low_seconds[-1] + 1) * SECOND) / SECOND if low_seconds else None, values[PREFIX + "seconds_since_below_65"])

    def test_readings_whose_exact_mean_is_below_65_and_whose_float_is_65_are_below_in_both(self) -> None:
        self.check(alternating)
        row = build_row(window(series_readings(seconds_of(alternating)))).values
        self.assertEqual(15.0, row[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, row[PREFIX + "seconds_since_below_65"])

    def test_a_mixed_series_agrees(self) -> None:
        self.check(lambda second: [60.0] * 4 if second % 7 < 3 else [70.0] * 4)

    def test_the_limit_itself_is_not_below_in_either(self) -> None:
        self.check(lambda second: [65.0] * 4)
        self.assertEqual(0.0, build_row(window(series_readings(seconds_of(lambda second: [65.0] * 4)))).values[PREFIX + "minutes_below_65"])

    def test_the_limit_is_the_one_of_the_event_rule(self) -> None:
        self.assertEqual(LOW_PRESSURE_65, 65.0)
        self.assertTrue(ExactMean.of(64.9999).below(LOW_PRESSURE_65))


if __name__ == "__main__":
    unittest.main()
