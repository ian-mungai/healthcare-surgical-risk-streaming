"""The intensive care label (analysis plan, "Stay in intensive care", analysis decision 8) and the measure of ``icu_days`` that comes before it.

Every case here is hand-built and kept apart from published data. Failure modes covered here:

- A valid zero is called a stay or unknown, or a value above zero is called no stay, or a fraction of a day (0.25) is not a stay.
- A missing value is a no stay (zero) instead of unknown, or a negative or non-finite value is a stay or a no stay instead of unknown.
- The measure counts a missing or invalid value as a recorded one, or leaves out a form of the value that the label must read (whole days or fractions).
- The measure is not the same for the same values in another order, or it reads a value of a patient who is not in the development sets.
"""

from __future__ import annotations

import unittest

from features.icu_label import INVALID, MISSING, RECORDED, icu_label
from features.label_values import NEGATIVE, POSITIVE, UNKNOWN
from spark.silver.label_measure import IcuMeasureRow, icu_measure_rows


class Label(unittest.TestCase):
    def test_a_valid_zero_is_no_stay(self) -> None:
        outcome = icu_label(0.0)
        self.assertEqual((NEGATIVE, True, RECORDED, 0.0), (outcome.label, outcome.eligible, outcome.reason, outcome.icu_days))

    def test_a_valid_value_above_zero_is_a_stay(self) -> None:
        for value in (1.0, 3.0, 0.25, 0.000001, 40.0):
            with self.subTest(value=value):
                outcome = icu_label(value)
                self.assertEqual((POSITIVE, True, RECORDED, value), (outcome.label, outcome.eligible, outcome.reason, outcome.icu_days))

    def test_a_missing_value_is_unknown_and_not_zero(self) -> None:
        outcome = icu_label(None)
        self.assertEqual((UNKNOWN, False, MISSING, None), (outcome.label, outcome.eligible, outcome.reason, outcome.icu_days))

    def test_a_value_that_is_not_valid_is_unknown(self) -> None:
        for value in (-1.0, -0.5, float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                outcome = icu_label(value)
                self.assertEqual((UNKNOWN, False, INVALID), (outcome.label, outcome.eligible, outcome.reason))

    def test_a_negative_zero_is_zero(self) -> None:
        self.assertEqual(NEGATIVE, icu_label(-0.0).label)


def measured(values: list[float | None]) -> dict[str, float]:
    return {row.statistic: row.value for row in icu_measure_rows(values)}


class MeasureTable(unittest.TestCase):
    def test_it_counts_each_kind_of_value(self) -> None:
        values: list[float | None] = [None, None, 0.0, 0.0, 0.0, 1.0, 2.0, 2.0, 7.0, 0.5, -1.0]
        counts = measured(values)
        self.assertEqual(11, counts["cases"])
        self.assertEqual((2, 1, 8), (counts["missing"], counts["invalid"], counts["recorded"]))
        self.assertEqual((3, 5), (counts["zero"], counts["above_zero"]))
        self.assertEqual(1, counts["fractional"])

    def test_the_least_the_greatest_and_the_percentiles_are_of_the_recorded_values(self) -> None:
        values: list[float | None] = [None, -4.0, *[float(day) for day in range(0, 20)]]
        counts = measured(values)
        self.assertEqual((0.0, 19.0), (counts["min"], counts["max"]))
        self.assertEqual(9.0, counts["p50"])

    def test_the_percentiles_are_by_nearest_rank_which_rounds_up(self) -> None:
        values: list[float | None] = [0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 7.0, None, -1.0]
        counts = measured(values)
        self.assertEqual((0.0, 0.0, 0.0, 1.0, 2.0, 7.0, 7.0), tuple(counts[f"p{percent}"] for percent in (5, 10, 25, 50, 75, 90, 95)))

    def test_the_order_does_not_matter(self) -> None:
        values: list[float | None] = [3.0, None, 0.0, 1.5, -2.0, 0.0]
        self.assertEqual(icu_measure_rows(values), icu_measure_rows(values[::-1]))

    def test_a_measure_has_every_statistic_each_once_and_its_unit(self) -> None:
        rows = icu_measure_rows([0.0, 1.0])
        names = [row.statistic for row in rows]
        self.assertEqual(len(names), len(set(names)))
        self.assertTrue(all(isinstance(row, IcuMeasureRow) and row.unit in {"cases", "days"} for row in rows))

    def test_no_recorded_value_has_no_least_or_greatest(self) -> None:
        counts = measured([None, -1.0])
        self.assertEqual(0, counts["recorded"])
        self.assertNotIn("min", counts)
        self.assertNotIn("max", counts)

    def test_no_case_is_refused_by_name(self) -> None:
        with self.assertRaises(ValueError):
            icu_measure_rows([])


if __name__ == "__main__":
    unittest.main()
