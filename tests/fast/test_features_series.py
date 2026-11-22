"""The pressure features of the per-second arterial series: one value for each whole second with a valid flag, over the 15 minutes that end at the cutoff.

Failure modes covered here:

- A feature's formula is wrong: each value of a hand-built window is compared with its known answer.
- A reading equal to a limit counts as below it, or the time since the last low second is counted from the wrong end.
- The window edge moves by one second, or a second after the cutoff is used.
- An invalid second is counted as valid, in a value or in the coverage share.
- A series with few valid seconds is dropped as a whole, or a statistic that cannot be computed returns a number.
- A reading off the one-second grid or two readings of one second are taken silently.
"""

from __future__ import annotations

import math
import unittest

from features.model import FeatureInputError, FeatureValue, Reading
from features.numeric import numeric_group
from tests.fast.features_support import CUTOFF_US, SECOND, SERIES, arithmetic, numeric_window

PREFIX = "silver_art_mbp_1s__"
KEYS = {"valid_count", "coverage", "minutes_below_65", "minutes_below_60", "seconds_since_below_65", "projection_5min"}


def values_of(readings: list[Reading], cutoff_us: int = CUTOFF_US) -> dict[str, FeatureValue]:
    return numeric_group(SERIES, readings, cutoff_us).values


def get(result: dict[str, FeatureValue], key: str) -> float:
    value = result[PREFIX + key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AssertionError(f"{key} is not a number: {value!r}")
    return value


class KnownAnswers(unittest.TestCase):
    def test_a_falling_line_gives_every_value_its_known_answer(self) -> None:
        # Second j is 100 - j/20 for j = 0..899: 100 down to 55.05. Below 65 from j = 701 (199 seconds), below 60 from j = 801 (99 seconds).
        result = values_of(numeric_window(arithmetic(100.0, -0.05)))
        self.assertEqual({PREFIX + key for key in KEYS}, set(result))
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(1.0, result[PREFIX + "coverage"])
        self.assertAlmostEqual(199 / 60, get(result, "minutes_below_65"), places=9)
        self.assertAlmostEqual(99 / 60, get(result, "minutes_below_60"), places=9)
        self.assertEqual(0.0, result[PREFIX + "seconds_since_below_65"])
        self.assertAlmostEqual(55.05 + 5 * -3.0, get(result, "projection_5min"), places=9)

    def test_a_second_equal_to_a_limit_is_not_below_it_and_the_time_since_the_last_low_second_counts_back_from_the_cutoff(self) -> None:
        # The first 300 seconds are 60 (below 65, not below 60), the other 600 are 70.
        result = values_of(numeric_window([60.0] * 300 + [70.0] * 600))
        self.assertEqual(5.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_60"])
        self.assertEqual(600.0, result[PREFIX + "seconds_since_below_65"])

    def test_no_second_below_65_leaves_the_time_since_missing_and_the_minutes_zero(self) -> None:
        result = values_of(numeric_window([80.0] * 900))
        self.assertIsNone(result[PREFIX + "seconds_since_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_60"])
        self.assertEqual(80.0, result[PREFIX + "projection_5min"])

    def test_the_projection_is_the_last_valid_value_plus_five_times_the_slope_per_minute(self) -> None:
        # Rising 0.1 each second to 90 at the cutoff: the slope is 6 per minute and the projection 90 + 30.
        result = values_of(numeric_window(arithmetic(90.0 - 0.1 * 899, 0.1)))
        self.assertAlmostEqual(120.0, get(result, "projection_5min"), places=9)


class Window(unittest.TestCase):
    def test_the_second_15_minutes_before_the_cutoff_is_outside_and_a_second_after_the_cutoff_changes_nothing(self) -> None:
        base = numeric_window(arithmetic(100.0, -0.05))
        padded = [Reading(CUTOFF_US - 900 * SECOND, 0.0, True), *base, Reading(CUTOFF_US + SECOND, 0.0, True), Reading(CUTOFF_US + 100 * SECOND, 500.0, True)]
        self.assertEqual(values_of(base), values_of(padded))
        self.assertEqual(CUTOFF_US, numeric_group(SERIES, padded, CUTOFF_US).latest_us)

    def test_the_first_and_the_last_second_of_the_window_are_used(self) -> None:
        self.assertEqual(899, values_of(numeric_window([80.0] * 899))[PREFIX + "valid_count"])
        shifted = numeric_window([80.0] * 900, cutoff_us=CUTOFF_US - SECOND)  # the series ends one second before the cutoff and starts 900 s before it
        self.assertEqual(899, values_of(shifted)[PREFIX + "valid_count"])


class Validity(unittest.TestCase):
    def test_an_invalid_second_is_in_no_value_and_no_count(self) -> None:
        readings = [Reading(r.time_us, 50.0 if r.valid else 5000.0, r.valid) for r in numeric_window([0.0] * 900, valid=lambda index: index % 3 != 0)]
        result = values_of(readings)
        self.assertEqual(600, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(600 / 900, get(result, "coverage"), places=12)
        self.assertAlmostEqual(600 / 60, get(result, "minutes_below_65"), places=12)

    def test_a_missing_second_counts_in_no_count(self) -> None:
        self.assertAlmostEqual(450 / 900, get(values_of(numeric_window([80.0] * 450)), "coverage"), places=12)

    def test_a_series_with_few_valid_seconds_still_returns_its_values(self) -> None:
        result = values_of(numeric_window([60.0] * 900, valid=lambda index: index < 20))
        self.assertEqual(20, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(20 / 60, get(result, "minutes_below_65"), places=12)
        self.assertEqual(880.0, result[PREFIX + "seconds_since_below_65"])

    def test_one_valid_second_has_no_projection_and_every_other_value(self) -> None:
        result = values_of(numeric_window([72.0, 60.0], valid=lambda index: index == 1))
        self.assertEqual(1, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(1 / 60, get(result, "minutes_below_65"), places=12)
        self.assertIsNone(result[PREFIX + "projection_5min"])

    def test_a_series_with_no_valid_second_has_missing_values_and_a_zero_count_and_share(self) -> None:
        result = values_of([])
        self.assertEqual(0, result[PREFIX + "valid_count"])
        self.assertEqual(0.0, result[PREFIX + "coverage"])
        self.assertEqual({None}, {value for name, value in result.items() if name not in (PREFIX + "valid_count", PREFIX + "coverage")})

    def test_an_invalid_second_may_hold_not_a_number_and_a_valid_one_may_not(self) -> None:
        readings = [Reading(r.time_us, 80.0 if r.valid else math.nan, r.valid) for r in numeric_window([0.0] * 900, valid=lambda index: index % 2 == 0)]
        self.assertEqual(450, values_of(readings)[PREFIX + "valid_count"])
        readings[0] = Reading(readings[0].time_us, math.inf, True)
        with self.assertRaises(FeatureInputError):
            values_of(readings)


class Refusals(unittest.TestCase):
    def test_a_reading_off_the_one_second_grid_is_refused(self) -> None:
        for offset in (1, 500_000, 999_999):
            with self.subTest(offset=offset), self.assertRaisesRegex(FeatureInputError, "whole second"):
                values_of(numeric_window([80.0] * 900) + [Reading(CUTOFF_US - 2 * SECOND + offset, 81.0, True)])

    def test_two_readings_of_one_second_are_refused(self) -> None:
        readings = numeric_window([80.0] * 900)
        with self.assertRaisesRegex(FeatureInputError, "one time"):
            values_of(readings + [Reading(readings[5].time_us, 81.0, True)])


if __name__ == "__main__":
    unittest.main()
