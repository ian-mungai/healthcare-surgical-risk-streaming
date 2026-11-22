"""The pressure features of the per-second arterial series: one value for each whole second with a valid flag, over the newest 900 complete seconds.

A second ``s`` covers ``[s, s + 1)`` seconds and is complete at the cutoff when ``(s + 1) * 1_000_000 <= cutoff``. Its reading is at ``s * 1_000_000``.

Failure modes covered here:

- A feature's formula is wrong: each value of a hand-built window is compared with its known answer.
- A reading equal to a limit counts as below it, or the time since the last low second is counted from the wrong end.
- The window moves with a fractional cutoff, or a second that is not complete at the cutoff is used.
- An invalid second is counted as valid, in a value or in the coverage share.
- A series with few valid seconds is dropped as a whole, or a statistic that cannot be computed returns a number.
- A reading off the one-second grid, a flag that is not a boolean or two readings of one second are taken silently.
"""

from __future__ import annotations

import math
import unittest
from typing import Any

from features.model import FeatureInputError, FeatureValue, Reading
from features.numeric import numeric_group
from tests.fast.features_support import CUTOFF_US, SECOND, SERIES, arithmetic, series_window

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
        result = values_of(series_window(arithmetic(100.0, -0.05)))
        self.assertEqual({PREFIX + key for key in KEYS}, set(result))
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(1.0, result[PREFIX + "coverage"])
        self.assertAlmostEqual(199 / 60, get(result, "minutes_below_65"), places=9)
        self.assertAlmostEqual(99 / 60, get(result, "minutes_below_60"), places=9)
        self.assertEqual(1.0, result[PREFIX + "seconds_since_below_65"])  # the newest complete second starts one second before the cutoff
        self.assertAlmostEqual(55.05 + 5 * -3.0, get(result, "projection_5min"), places=9)

    def test_a_second_equal_to_a_limit_is_not_below_it_and_the_time_since_the_last_low_second_counts_back_from_the_cutoff(self) -> None:
        # The first 300 seconds are 60 (below 65, not below 60), the other 600 are 70. The last low second starts 601 s before the cutoff.
        result = values_of(series_window([60.0] * 300 + [70.0] * 600))
        self.assertEqual(5.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_60"])
        self.assertEqual(601.0, result[PREFIX + "seconds_since_below_65"])

    def test_no_second_below_65_leaves_the_time_since_missing_and_the_minutes_zero(self) -> None:
        result = values_of(series_window([80.0] * 900))
        self.assertIsNone(result[PREFIX + "seconds_since_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_60"])
        self.assertEqual(80.0, result[PREFIX + "projection_5min"])

    def test_the_projection_is_the_last_valid_value_plus_five_times_the_slope_per_minute(self) -> None:
        # Rising 0.1 each second to 90 at the newest second: the slope is 6 per minute and the projection 90 + 30.
        result = values_of(series_window(arithmetic(90.0 - 0.1 * 899, 0.1)))
        self.assertAlmostEqual(120.0, get(result, "projection_5min"), places=9)


class Window(unittest.TestCase):
    def marked(self, first: int, last: int) -> list[Reading]:
        """One valid reading for each second ``first`` to ``last``, whose value is its index minus ``first``."""
        return [Reading(second * SECOND, float(second - first), True) for second in range(first, last + 1)]

    def test_the_window_is_the_900_newest_complete_seconds_at_each_cutoff_around_a_second_boundary(self) -> None:
        k = 3600
        readings = self.marked(k - 1000, k + 5)
        cases = ((k * SECOND - 1, k - 2), (k * SECOND, k - 1), (k * SECOND + 1, k - 1), (k * SECOND + 999_999, k - 1), ((k + 1) * SECOND, k))
        for cutoff, newest in cases:
            with self.subTest(cutoff=cutoff):
                group = numeric_group(SERIES, readings, cutoff)
                self.assertEqual(900, group.values[PREFIX + "valid_count"])
                self.assertEqual(1.0, group.values[PREFIX + "coverage"])
                self.assertEqual(newest * SECOND, group.latest_us)
                self.assertLess(group.latest_us or 0, cutoff)
                # The value of the newest second is last + 5 * slope; with value = second - first the slope is 60 per minute.
                self.assertAlmostEqual(newest - (k - 1000) + 5 * 60.0, get(group.values, "projection_5min"), places=6)

    def test_a_second_that_is_not_complete_at_the_cutoff_is_not_used(self) -> None:
        base = series_window(arithmetic(100.0, -0.05))
        in_progress = Reading(CUTOFF_US, 0.0, True)
        later = Reading(CUTOFF_US + 100 * SECOND, 500.0, True)
        before = Reading(CUTOFF_US - 901 * SECOND, 0.0, True)
        self.assertEqual(values_of(base), values_of([before, *base, in_progress, later]))
        self.assertEqual(CUTOFF_US - SECOND, numeric_group(SERIES, [*base, in_progress, later], CUTOFF_US).latest_us)

    def test_the_first_and_the_last_second_of_the_window_are_used_and_the_neighbours_outside_are_not(self) -> None:
        oldest, newest = CUTOFF_US // SECOND - 900, CUTOFF_US // SECOND - 1
        self.assertEqual(1, values_of([Reading(oldest * SECOND, 50.0, True)])[PREFIX + "valid_count"])
        self.assertEqual(1, values_of([Reading(newest * SECOND, 50.0, True)])[PREFIX + "valid_count"])
        self.assertEqual(0, values_of([Reading((oldest - 1) * SECOND, 50.0, True)])[PREFIX + "valid_count"])
        self.assertEqual(0, values_of([Reading((newest + 1) * SECOND, 50.0, True)])[PREFIX + "valid_count"])


class Validity(unittest.TestCase):
    def test_an_invalid_second_is_in_no_value_and_no_count(self) -> None:
        readings = [Reading(r.time_us, 50.0 if r.valid else 5000.0, r.valid) for r in series_window([0.0] * 900, valid=lambda index: index % 3 != 0)]
        result = values_of(readings)
        self.assertEqual(600, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(600 / 900, get(result, "coverage"), places=12)
        self.assertAlmostEqual(600 / 60, get(result, "minutes_below_65"), places=12)

    def test_a_missing_second_counts_in_no_count(self) -> None:
        self.assertAlmostEqual(450 / 900, get(values_of(series_window([80.0] * 450)), "coverage"), places=12)

    def test_a_series_with_few_valid_seconds_still_returns_its_values(self) -> None:
        result = values_of(series_window([60.0] * 900, valid=lambda index: index < 20))
        self.assertEqual(20, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(20 / 60, get(result, "minutes_below_65"), places=12)
        self.assertEqual(881.0, result[PREFIX + "seconds_since_below_65"])

    def test_one_valid_second_has_no_projection_and_every_other_value(self) -> None:
        result = values_of(series_window([72.0, 60.0], valid=lambda index: index == 1))
        self.assertEqual(1, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(1 / 60, get(result, "minutes_below_65"), places=12)
        self.assertIsNone(result[PREFIX + "projection_5min"])

    def test_a_series_with_no_valid_second_has_missing_values_and_a_zero_count_and_share(self) -> None:
        result = values_of([])
        self.assertEqual(0, result[PREFIX + "valid_count"])
        self.assertEqual(0.0, result[PREFIX + "coverage"])
        self.assertEqual({None}, {value for name, value in result.items() if name not in (PREFIX + "valid_count", PREFIX + "coverage")})

    def test_an_invalid_second_may_hold_not_a_number_and_a_valid_one_may_not(self) -> None:
        readings = [Reading(r.time_us, 80.0 if r.valid else math.nan, r.valid) for r in series_window([0.0] * 900, valid=lambda index: index % 2 == 0)]
        self.assertEqual(450, values_of(readings)[PREFIX + "valid_count"])
        readings[0] = Reading(readings[0].time_us, math.inf, True)
        with self.assertRaisesRegex(FeatureInputError, SERIES):
            values_of(readings)


class Range(unittest.TestCase):
    def test_a_projection_that_a_float_cannot_hold_is_refused_with_its_name(self) -> None:
        newest = CUTOFF_US // SECOND - 1
        readings = [Reading((newest - 60) * SECOND, 0.0, True), Reading(newest * SECOND, 1.7e308, True)]
        with self.assertRaisesRegex(FeatureInputError, PREFIX + "projection_5min"):
            values_of(readings)


class Refusals(unittest.TestCase):
    def test_a_reading_off_the_one_second_grid_inside_the_window_is_refused(self) -> None:
        for offset in (1, 500_000, 999_999):
            with self.subTest(offset=offset), self.assertRaisesRegex(FeatureInputError, "whole second"):
                values_of(series_window([80.0] * 900) + [Reading(CUTOFF_US - 2 * SECOND + offset, 81.0, True)])

    def test_two_readings_of_one_second_are_refused(self) -> None:
        readings = series_window([80.0] * 900)
        with self.assertRaisesRegex(FeatureInputError, "one time"):
            values_of(readings + [Reading(readings[5].time_us, 81.0, True)])

    def test_a_flag_that_is_not_a_boolean_and_a_time_or_value_of_the_wrong_type_are_refused(self) -> None:
        base = series_window([80.0] * 900)
        bad_flag: Any = "false"
        bad_time: Any = float(CUTOFF_US - 5 * SECOND) + 0.9
        bad_value: Any = "80"
        for bad, pattern in (
            (Reading(CUTOFF_US - 5 * SECOND, 1.0, bad_flag), "valid flag"),
            (Reading(bad_time, 1.0, True), "time"),
            (Reading(CUTOFF_US - 5 * SECOND, bad_value, True), "value"),
        ):
            with self.subTest(pattern=pattern), self.assertRaisesRegex(FeatureInputError, pattern):
                values_of([*base, bad])


if __name__ == "__main__":
    unittest.main()
