"""The numeric features of one track over the 15-minute window that ends at the cutoff.

Failure modes covered here:

- A feature's formula is wrong: each value of a hand-built window is compared with its known answer.
- The window edge moves by one second, so the reading 15 minutes before the cutoff is used or the first reading inside is dropped.
- A reading after the cutoff is used, so the future leaks into the row.
- An invalid reading is counted in a value, in the valid count or in the coverage share.
- A group with less than half its window valid returns values, or returns a zero in place of missing.
- A reading below a pressure limit equal to the limit counts as below, or the time since the last low reading is measured from the wrong end.
- The result depends on the order of the input, so the batch and the stream disagree in the last bit.
- A duplicate or off-grid reading, or a track the registry does not know, is taken silently.
"""

from __future__ import annotations

import math
import unittest

from features.model import FeatureInputError, FeatureValue, Reading
from features.numeric import numeric_group
from tests.fast.features_support import ARTERIAL_MEAN, CUTOFF_US, SECOND, arithmetic, numeric_window

PREFIX = "solar8000_art_mbp__"


def values_of(readings: list[Reading], track: str = ARTERIAL_MEAN, cutoff_us: int = CUTOFF_US) -> dict[str, FeatureValue]:
    return numeric_group(track, readings, cutoff_us).values


def get(result: dict[str, FeatureValue], key: str) -> float:
    value = result[PREFIX + key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AssertionError(f"{key} is not a number: {value!r}")
    return value


class KnownAnswers(unittest.TestCase):
    def test_a_falling_line_gives_every_value_its_known_answer(self) -> None:
        # Reading j is 100 - j/20 for j = 0..899: 100 down to 55.05, one reading each second.
        result = values_of(numeric_window(arithmetic(100.0, -0.05)))
        self.assertAlmostEqual(55.05, get(result, "last"), places=9)
        self.assertAlmostEqual(77.525, get(result, "mean"), places=9)
        self.assertAlmostEqual(77.525, get(result, "median"), places=9)
        self.assertAlmostEqual(55.05, get(result, "min"), places=9)
        self.assertAlmostEqual(100.0, get(result, "max"), places=9)
        self.assertAlmostEqual(0.05 * math.sqrt(900 * 901 / 12), get(result, "std"), places=9)
        self.assertAlmostEqual(-3.0, get(result, "slope_per_min"), places=9)
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(1.0, result[PREFIX + "coverage"])

    def test_the_arterial_mean_features_count_minutes_below_the_limits_and_project_the_slope(self) -> None:
        # Below 65 from j = 701 (199 readings), below 60 from j = 801 (99 readings), each reading one second.
        result = values_of(numeric_window(arithmetic(100.0, -0.05)))
        self.assertAlmostEqual(199 / 60, get(result, "minutes_below_65"), places=9)
        self.assertAlmostEqual(99 / 60, get(result, "minutes_below_60"), places=9)
        self.assertEqual(0.0, result[PREFIX + "seconds_since_below_65"])
        self.assertAlmostEqual(55.05 + 5 * -3.0, get(result, "projection_5min"), places=9)

    def test_a_reading_equal_to_a_limit_is_not_below_it_and_the_time_since_the_last_low_reading_counts_back_from_the_cutoff(self) -> None:
        # The first 300 readings are 60 (below 65, not below 60), the other 600 are 70.
        result = values_of(numeric_window([60.0] * 300 + [70.0] * 600))
        self.assertEqual(5.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_60"])
        self.assertEqual(600.0, result[PREFIX + "seconds_since_below_65"])

    def test_no_reading_below_65_leaves_the_time_since_missing_and_the_minutes_zero(self) -> None:
        result = values_of(numeric_window([80.0] * 900))
        self.assertIsNone(result[PREFIX + "seconds_since_below_65"])
        self.assertEqual(0.0, result[PREFIX + "minutes_below_65"])
        self.assertEqual(0.0, result[PREFIX + "std"])
        self.assertEqual(0.0, result[PREFIX + "slope_per_min"])

    def test_the_median_of_an_even_count_is_the_mean_of_the_middle_pair(self) -> None:
        result = values_of(numeric_window([1.0, 2.0, 10.0, 100.0] + [5.0] * 896))
        self.assertEqual(5.0, result[PREFIX + "median"])
        result = values_of(numeric_window([4.0] * 450 + [6.0] * 450))
        self.assertEqual(5.0, result[PREFIX + "median"])

    def test_a_skewed_window_tells_the_mean_from_the_median_and_the_smallest_from_the_largest(self) -> None:
        result = values_of(numeric_window([80.0] * 600 + [20.0] * 300))
        self.assertEqual(60.0, result[PREFIX + "mean"])
        self.assertEqual(80.0, result[PREFIX + "median"])
        self.assertEqual(20.0, result[PREFIX + "min"])
        self.assertEqual(80.0, result[PREFIX + "max"])
        self.assertEqual(20.0, result[PREFIX + "last"])

    def test_the_other_tracks_have_the_common_features_only(self) -> None:
        result = numeric_group("Solar8000/HR", numeric_window([60.0] * 900), CUTOFF_US).values
        self.assertEqual(
            {"solar8000_hr__" + key for key in ("last", "mean", "median", "min", "max", "std", "slope_per_min", "valid_count", "coverage")}, set(result)
        )


class Window(unittest.TestCase):
    def test_the_reading_15_minutes_before_the_cutoff_is_outside_and_the_one_a_second_later_is_inside(self) -> None:
        readings = [Reading(CUTOFF_US - 900 * SECOND, 0.0, True)] + numeric_window([80.0] * 900)
        result = values_of(readings)
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(80.0, result[PREFIX + "min"])
        short = values_of(numeric_window([80.0] * 899))
        self.assertEqual(899, short[PREFIX + "valid_count"])

    def test_a_reading_at_the_cutoff_is_inside_and_a_reading_after_it_changes_nothing(self) -> None:
        base = numeric_window(arithmetic(100.0, -0.05))
        after = [Reading(CUTOFF_US + SECOND, 0.0, True), Reading(CUTOFF_US + 1, 0.0, True), Reading(CUTOFF_US + 600 * SECOND, 500.0, True)]
        self.assertEqual(values_of(base), values_of(base + after))
        group = numeric_group(ARTERIAL_MEAN, base + after, CUTOFF_US)
        self.assertEqual(CUTOFF_US, group.latest_us)

    def test_the_latest_reading_time_is_that_of_the_last_valid_reading_used(self) -> None:
        readings = numeric_window([80.0] * 900, valid=lambda index: index < 898)
        self.assertEqual(CUTOFF_US - 2 * SECOND, numeric_group(ARTERIAL_MEAN, readings, CUTOFF_US).latest_us)

    def test_the_cutoff_between_two_seconds_keeps_the_window_a_half_open_interval_of_900_seconds(self) -> None:
        cutoff = CUTOFF_US + 500_000
        readings = numeric_window(
            [80.0] * 901, cutoff_us=CUTOFF_US
        )  # whole seconds from 900 s before the second to the second; the cutoff is half a second later
        result = values_of(readings, cutoff_us=cutoff)
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(CUTOFF_US, numeric_group(ARTERIAL_MEAN, readings, cutoff).latest_us)


class Validity(unittest.TestCase):
    def test_an_invalid_reading_is_in_no_value_and_no_count(self) -> None:
        # Every third reading is invalid and holds an absurd value: 600 valid readings of 80.
        readings = [
            Reading(reading.time_us, 80.0 if reading.valid else 5000.0, reading.valid)
            for reading in numeric_window([0.0] * 900, valid=lambda index: index % 3 != 0)
        ]
        result = values_of(readings)
        self.assertEqual(600, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(600 / 900, get(result, "coverage"), places=12)
        self.assertEqual(80.0, result[PREFIX + "max"])
        self.assertEqual(80.0, result[PREFIX + "mean"])

    def test_an_invalid_reading_may_hold_not_a_number(self) -> None:
        readings = [
            Reading(reading.time_us, 80.0 if reading.valid else math.nan, reading.valid)
            for reading in numeric_window([0.0] * 900, valid=lambda index: index % 2 == 0)
        ]
        self.assertEqual(80.0, values_of(readings)[PREFIX + "mean"])

    def test_the_last_value_is_the_last_valid_one_not_the_last_reading(self) -> None:
        readings = numeric_window(arithmetic(100.0, -0.05), valid=lambda index: index != 899)
        self.assertAlmostEqual(100.0 - 0.05 * 898, get(values_of(readings), "last"), places=9)

    def test_exactly_half_the_window_valid_returns_values(self) -> None:
        result = values_of(numeric_window([80.0] * 900, valid=lambda index: index < 450))
        self.assertEqual(450, result[PREFIX + "valid_count"])
        self.assertEqual(80.0, result[PREFIX + "mean"])

    def test_less_than_half_valid_returns_every_value_missing_and_keeps_the_count_and_the_share(self) -> None:
        result = values_of(numeric_window([80.0] * 900, valid=lambda index: index < 449))
        self.assertEqual(449, result[PREFIX + "valid_count"])
        self.assertAlmostEqual(449 / 900, get(result, "coverage"), places=12)
        missing = {name for name, value in result.items() if value is None}
        self.assertEqual(set(result) - {PREFIX + "valid_count", PREFIX + "coverage"}, missing)
        self.assertNotIn(0.0, [value for name, value in result.items() if name not in (PREFIX + "valid_count", PREFIX + "coverage")])

    def test_a_track_with_no_reading_is_all_missing_with_a_zero_count(self) -> None:
        result = values_of([])
        self.assertEqual(0, result[PREFIX + "valid_count"])
        self.assertEqual(0.0, result[PREFIX + "coverage"])
        self.assertEqual(11, sum(value is None for value in result.values()))
        self.assertIsNone(result[PREFIX + "last"])
        self.assertIsNone(result[PREFIX + "minutes_below_65"])

    def test_a_valid_reading_that_is_not_a_number_is_refused(self) -> None:
        readings = numeric_window([80.0] * 900)
        readings[10] = Reading(readings[10].time_us, math.inf, True)
        with self.assertRaises(FeatureInputError):
            values_of(readings)


class Determinism(unittest.TestCase):
    def test_the_order_of_the_input_does_not_change_one_bit(self) -> None:
        values = [80.0 + 40.0 * math.sin(index * 12.9898) for index in range(900)]
        readings = numeric_window(values)
        shuffled = list(readings)
        shuffled = shuffled[::3] + shuffled[1::3] + shuffled[2::3]
        shuffled.reverse()
        first, second = values_of(readings), values_of(shuffled)
        self.assertEqual({name: repr(value) for name, value in first.items()}, {name: repr(value) for name, value in second.items()})
        self.assertEqual(first, values_of(readings))


class Refusals(unittest.TestCase):
    def test_two_readings_at_one_time_are_refused(self) -> None:
        readings = numeric_window([80.0] * 900)
        with self.assertRaisesRegex(FeatureInputError, "one reading"):
            values_of(readings + [Reading(readings[5].time_us, 81.0, True)])

    def test_a_reading_off_the_one_second_grid_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "whole second"):
            values_of(numeric_window([80.0] * 900) + [Reading(CUTOFF_US - 1_500_000 + 1, 81.0, True)])

    def test_a_track_the_registry_does_not_know_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "unknown track"):
            numeric_group("Solar8000/NOT_A_TRACK", numeric_window([80.0] * 900), CUTOFF_US)


if __name__ == "__main__":
    unittest.main()
