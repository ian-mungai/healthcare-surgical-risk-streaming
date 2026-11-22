"""The features of one source-time numeric track (a monitor track at its published times) over the 15-minute window that ends at the cutoff.

Failure modes covered here:

- A feature's formula is wrong: each value of a hand-built window is compared with its known answer.
- The slope is taken on the position of a reading and not on its time, so irregular spacing changes the answer.
- The window edge moves by one second, so the reading 15 minutes before the cutoff is used or the first reading inside is dropped.
- A reading after the cutoff is used, so the future leaks into the row.
- An invalid reading is counted in a value or in the valid count.
- A statistic that cannot be computed returns a number: no valid reading, or one reading for a standard deviation or a slope.
- A group with few valid readings is dropped as a whole, although the plan limits only sample eligibility.
- The result depends on the order of the input, so the batch and the stream disagree in the last bit.
- Two readings at one time, or a track the registry does not know, are taken silently.
"""

from __future__ import annotations

import math
import unittest

from features.model import FeatureInputError, FeatureValue, Reading
from features.numeric import numeric_group
from tests.fast.features_support import CUTOFF_US, SECOND, arithmetic, numeric_window, spaced_window

TRACK = "Solar8000/ART_MBP"
PREFIX = "solar8000_art_mbp__"
KEYS = {"last", "mean", "median", "min", "max", "std", "slope_per_min", "valid_count", "seconds_since_last"}


def values_of(readings: list[Reading], track: str = TRACK, cutoff_us: int = CUTOFF_US) -> dict[str, FeatureValue]:
    return numeric_group(track, readings, cutoff_us).values


def get(result: dict[str, FeatureValue], key: str) -> float:
    value = result[PREFIX + key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AssertionError(f"{key} is not a number: {value!r}")
    return value


class KnownAnswers(unittest.TestCase):
    def test_a_falling_line_read_every_two_seconds_gives_every_value_its_known_answer(self) -> None:
        # Reading j is 100 - j/10 for j = 0..449, one reading each 2 seconds: 100 down to 55.1.
        result = values_of(spaced_window(arithmetic(100.0, -0.1, 450), 2))
        self.assertEqual({PREFIX + key for key in KEYS}, set(result))
        self.assertAlmostEqual(55.1, get(result, "last"), places=9)
        self.assertAlmostEqual(77.55, get(result, "mean"), places=9)
        self.assertAlmostEqual(77.55, get(result, "median"), places=9)
        self.assertAlmostEqual(55.1, get(result, "min"), places=9)
        self.assertAlmostEqual(100.0, get(result, "max"), places=9)
        self.assertAlmostEqual(0.1 * math.sqrt(450 * 451 / 12), get(result, "std"), places=9)
        self.assertAlmostEqual(-3.0, get(result, "slope_per_min"), places=9)
        self.assertEqual(450, result[PREFIX + "valid_count"])
        self.assertEqual(0.0, result[PREFIX + "seconds_since_last"])

    def test_the_slope_is_taken_on_the_time_of_the_readings_and_not_on_their_position(self) -> None:
        # Minutes before the cutoff -10, -9 and -1 with values 0, 0 and 9: the slope is 153/146 per minute. On the position it would be 4.5.
        readings = [
            Reading(CUTOFF_US - 10 * 60 * SECOND, 0.0, True),
            Reading(CUTOFF_US - 9 * 60 * SECOND, 0.0, True),
            Reading(CUTOFF_US - 60 * SECOND, 9.0, True),
        ]
        result = values_of(readings)
        self.assertAlmostEqual(153 / 146, get(result, "slope_per_min"), places=12)
        self.assertEqual(60.0, result[PREFIX + "seconds_since_last"])

    def test_readings_at_microsecond_times_are_taken_at_their_times(self) -> None:
        readings = [Reading(CUTOFF_US - 2_500_000, 10.0, True), Reading(CUTOFF_US - 1, 20.0, True)]
        result = values_of(readings)
        self.assertEqual(20.0, result[PREFIX + "last"])
        self.assertEqual(1e-6, result[PREFIX + "seconds_since_last"])

    def test_the_median_of_an_even_count_is_the_mean_of_the_middle_pair(self) -> None:
        self.assertEqual(6.0, values_of(numeric_window([1.0, 2.0, 10.0, 100.0]))[PREFIX + "median"])
        self.assertEqual(3.0, values_of(numeric_window([1.0, 2.0, 4.0, 100.0]))[PREFIX + "median"])
        self.assertEqual(2.0, values_of(numeric_window([1.0, 2.0, 100.0]))[PREFIX + "median"])

    def test_a_skewed_window_tells_the_mean_from_the_median_and_the_smallest_from_the_largest(self) -> None:
        result = values_of(numeric_window([80.0] * 600 + [20.0] * 300))
        self.assertEqual(60.0, result[PREFIX + "mean"])
        self.assertEqual(80.0, result[PREFIX + "median"])
        self.assertEqual(20.0, result[PREFIX + "min"])
        self.assertEqual(80.0, result[PREFIX + "max"])
        self.assertEqual(20.0, result[PREFIX + "last"])

    def test_a_track_has_the_statistics_of_a_source_track_and_no_coverage_share_or_pressure_feature(self) -> None:
        result = numeric_group("Solar8000/HR", numeric_window([60.0] * 5), CUTOFF_US).values
        self.assertEqual({"solar8000_hr__" + key for key in KEYS}, set(result))


class Window(unittest.TestCase):
    def test_the_reading_15_minutes_before_the_cutoff_is_outside_and_the_one_a_second_later_is_inside(self) -> None:
        readings = [Reading(CUTOFF_US - 900 * SECOND, 0.0, True)] + numeric_window([80.0] * 900)
        result = values_of(readings)
        self.assertEqual(900, result[PREFIX + "valid_count"])
        self.assertEqual(80.0, result[PREFIX + "min"])
        self.assertEqual(899, values_of(numeric_window([80.0] * 899))[PREFIX + "valid_count"])

    def test_a_reading_at_the_cutoff_is_inside_and_a_reading_after_it_changes_nothing(self) -> None:
        base = numeric_window(arithmetic(100.0, -0.05))
        after = [Reading(CUTOFF_US + SECOND, 0.0, True), Reading(CUTOFF_US + 1, 0.0, True), Reading(CUTOFF_US + 600 * SECOND, 500.0, True)]
        self.assertEqual(values_of(base), values_of(base + after))
        self.assertEqual(CUTOFF_US, numeric_group(TRACK, base + after, CUTOFF_US).latest_us)

    def test_the_latest_reading_time_is_that_of_the_last_valid_reading_used(self) -> None:
        readings = numeric_window([80.0] * 900, valid=lambda index: index < 898)
        group = numeric_group(TRACK, readings, CUTOFF_US)
        self.assertEqual(CUTOFF_US - 2 * SECOND, group.latest_us)
        self.assertEqual(2.0, group.values[PREFIX + "seconds_since_last"])

    def test_the_cutoff_between_two_seconds_keeps_the_window_a_half_open_interval_of_900_seconds(self) -> None:
        cutoff = CUTOFF_US + 500_000
        readings = numeric_window([80.0] * 901, cutoff_us=CUTOFF_US)  # the first reading is 900 s before the second, so 899.5 s before the cutoff
        self.assertEqual(900, values_of(readings, cutoff_us=cutoff)[PREFIX + "valid_count"])
        self.assertEqual(CUTOFF_US, numeric_group(TRACK, readings, cutoff).latest_us)


class Validity(unittest.TestCase):
    def test_an_invalid_reading_is_in_no_value_and_no_count(self) -> None:
        readings = [Reading(r.time_us, 80.0 if r.valid else 5000.0, r.valid) for r in numeric_window([0.0] * 900, valid=lambda index: index % 3 != 0)]
        result = values_of(readings)
        self.assertEqual(600, result[PREFIX + "valid_count"])
        self.assertEqual(80.0, result[PREFIX + "max"])
        self.assertEqual(80.0, result[PREFIX + "mean"])

    def test_an_invalid_reading_may_hold_not_a_number(self) -> None:
        readings = [Reading(r.time_us, 80.0 if r.valid else math.nan, r.valid) for r in numeric_window([0.0] * 900, valid=lambda index: index % 2 == 0)]
        self.assertEqual(80.0, values_of(readings)[PREFIX + "mean"])

    def test_the_last_value_is_the_last_valid_one_not_the_last_reading(self) -> None:
        readings = numeric_window(arithmetic(100.0, -0.05), valid=lambda index: index != 899)
        self.assertAlmostEqual(100.0 - 0.05 * 898, get(values_of(readings), "last"), places=9)

    def test_a_group_with_few_valid_readings_still_returns_its_values(self) -> None:
        result = values_of(numeric_window([80.0] * 900, valid=lambda index: index < 10))
        self.assertEqual(10, result[PREFIX + "valid_count"])
        self.assertEqual(80.0, result[PREFIX + "mean"])
        self.assertEqual(0.0, result[PREFIX + "std"])

    def test_one_valid_reading_has_no_standard_deviation_and_no_slope_but_every_other_value(self) -> None:
        result = values_of(numeric_window([72.0, 80.0], valid=lambda index: index == 1))
        self.assertEqual(1, result[PREFIX + "valid_count"])
        for key in ("last", "mean", "median", "min", "max"):
            self.assertEqual(80.0, result[PREFIX + key])
        self.assertIsNone(result[PREFIX + "std"])
        self.assertIsNone(result[PREFIX + "slope_per_min"])
        self.assertEqual(0.0, result[PREFIX + "seconds_since_last"])

    def test_a_track_with_no_valid_reading_has_every_value_missing_and_a_zero_count(self) -> None:
        for readings in ([], numeric_window([80.0] * 5, valid=lambda index: False)):
            result = values_of(readings)
            self.assertEqual(0, result[PREFIX + "valid_count"])
            self.assertEqual({None}, {value for name, value in result.items() if name != PREFIX + "valid_count"})

    def test_a_valid_reading_that_is_not_a_number_is_refused(self) -> None:
        readings = numeric_window([80.0] * 900)
        readings[10] = Reading(readings[10].time_us, math.inf, True)
        with self.assertRaises(FeatureInputError):
            values_of(readings)


class Determinism(unittest.TestCase):
    def test_the_order_of_the_input_does_not_change_one_bit(self) -> None:
        readings = numeric_window([80.0 + 40.0 * math.sin(index * 12.9898) for index in range(900)])
        shuffled = readings[::3] + readings[1::3] + readings[2::3]
        shuffled.reverse()
        first, second = values_of(readings), values_of(shuffled)
        self.assertEqual({name: repr(value) for name, value in first.items()}, {name: repr(value) for name, value in second.items()})


class Refusals(unittest.TestCase):
    def test_two_readings_at_one_time_are_refused(self) -> None:
        readings = numeric_window([80.0] * 900)
        with self.assertRaisesRegex(FeatureInputError, "one time"):
            values_of(readings + [Reading(readings[5].time_us, 81.0, True)])

    def test_a_track_the_registry_does_not_know_is_refused(self) -> None:
        with self.assertRaisesRegex(FeatureInputError, "unknown track"):
            numeric_group("Solar8000/NOT_A_TRACK", numeric_window([80.0] * 900), CUTOFF_US)


if __name__ == "__main__":
    unittest.main()
