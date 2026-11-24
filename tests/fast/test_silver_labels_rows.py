"""The tables of the labels, and the row functions that the Spark job passes to its Python worker: thin adapters over the label rules.

Every case here is hand-built and kept apart from published data. Failure modes covered here:

- A label row does not carry what the card requires: the baseline value with its time and its source row, the windows, the measurements of each rise, the label
  and its eligibility reason, so a later check cannot show what a label read.
- The row function puts a value in another column than its name, or its length is not the table's, so Spark stores a shifted row.
- The worker reads the laboratory rows in a form that the table does not have (a missing result, a time that is not a number) or reads a test other than
  creatinine, or passes the results of another case.
- An intensive care value that Spark reads as a whole number is not the float that the rule reads, or a missing value becomes zero.
- The tables name their columns and keys twice (in the table module and in the row functions) and the two lists differ.
"""

from __future__ import annotations

import contextlib
import io
import unittest
from typing import Any

from features.label_values import LABEL_VALUES, NEGATIVE, POSITIVE, UNKNOWN
from spark.jobs.silver_labels import main
from spark.silver.label_frames import ICU_ROW_COLUMNS, KIDNEY_ROW_COLUMNS, icu_row, kidney_row
from spark.silver.tables import ICU_DAYS_MEASURE, ICU_LABELS, KIDNEY_INJURY_LABELS, LABEL_TABLES

DAY, HOUR = 86_400, 3_600
ANESTART = 10_000_000
OPEND = ANESTART + 6 * HOUR


def lab(time_s: int, value: float | None, occurrence: int = 1) -> dict[str, Any]:
    return {"dt": time_s, "source_occurrence": occurrence, "result": value}


def row_of(anestart: int | None, opend: int | None, results: list[dict[str, Any]] | None) -> dict[str, Any]:
    return dict(zip(KIDNEY_ROW_COLUMNS, kidney_row(anestart, opend, results), strict=True))


class Tables(unittest.TestCase):
    def test_the_three_tables_are_named_and_keyed(self) -> None:
        self.assertEqual({"icu_days_measure", "icu_labels", "kidney_injury_labels"}, set(LABEL_TABLES))
        self.assertEqual(("statistic",), ICU_DAYS_MEASURE.key)
        self.assertEqual(("case_id",), ICU_LABELS.key)
        self.assertEqual(("case_id",), KIDNEY_INJURY_LABELS.key)

    def test_the_row_columns_are_the_table_columns_after_the_case(self) -> None:
        self.assertEqual(tuple(name for name, _ in KIDNEY_INJURY_LABELS.columns[1:]), KIDNEY_ROW_COLUMNS)
        self.assertEqual("case_id", KIDNEY_INJURY_LABELS.columns[0][0])
        self.assertEqual(("opend_s", "icu_days", "label", "eligible", "eligibility_reason"), tuple(name for name, _ in ICU_LABELS.columns[1:]))
        self.assertEqual(("opend_s", "icu_days", "label", "eligible", "eligibility_reason"), ICU_ROW_COLUMNS)

    def test_a_kidney_row_records_the_baseline_the_windows_the_measurements_and_the_reason(self) -> None:
        wanted = {
            "baseline_value",
            "baseline_time_s",
            "baseline_occurrence",
            "baseline_window_start_s",
            "baseline_window_end_s",
            "followup_start_s",
            "absolute_window_end_s",
            "relative_window_end_s",
            "early_results",
            "late_results",
            "absolute_rise",
            "earlier_value",
            "earlier_time_s",
            "earlier_occurrence",
            "later_value",
            "later_time_s",
            "later_occurrence",
            "relative_rise",
            "relative_value",
            "relative_time_s",
            "relative_occurrence",
            "latest_time_s",
            "label",
            "eligible",
            "eligibility_reason",
        }
        self.assertEqual(wanted, set(KIDNEY_ROW_COLUMNS))

    def test_the_measure_holds_a_statistic_its_unit_and_its_value(self) -> None:
        self.assertEqual((("statistic", "string"), ("unit", "string"), ("value", "double")), ICU_DAYS_MEASURE.columns)

    def test_the_types_are_the_ones_that_a_value_needs(self) -> None:
        kinds = dict(KIDNEY_INJURY_LABELS.columns)
        for name in ("baseline_time_s", "later_time_s", "latest_time_s", "followup_start_s"):
            self.assertEqual("bigint", kinds[name], name)
        for name in ("baseline_value", "earlier_value", "later_value", "relative_value"):
            self.assertEqual("double", kinds[name], name)
        for name in ("baseline_occurrence", "early_results", "late_results"):
            self.assertEqual("int", kinds[name], name)
        self.assertEqual("double", dict(ICU_LABELS.columns)["icu_days"])


class Kidney(unittest.TestCase):
    def test_a_positive_case_carries_its_baseline_its_measurements_and_its_times(self) -> None:
        row = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1.0, 2), lab(OPEND + 10 * HOUR, 1.3, 1)])
        self.assertEqual((POSITIVE, True, "eligible"), (row["label"], row["eligible"], row["eligibility_reason"]))
        self.assertEqual((1.0, ANESTART - HOUR, 2), (row["baseline_value"], row["baseline_time_s"], row["baseline_occurrence"]))
        self.assertEqual((ANESTART - 30 * DAY, ANESTART), (row["baseline_window_start_s"], row["baseline_window_end_s"]))
        self.assertEqual((OPEND, OPEND + 48 * HOUR, OPEND + 7 * DAY), (row["followup_start_s"], row["absolute_window_end_s"], row["relative_window_end_s"]))
        self.assertEqual(
            (True, 1.0, ANESTART - HOUR, 2, 1.3, OPEND + 10 * HOUR, 1),
            tuple(
                row[k] for k in ("absolute_rise", "earlier_value", "earlier_time_s", "earlier_occurrence", "later_value", "later_time_s", "later_occurrence")
            ),
        )
        self.assertEqual((False, None, None, None), (row["relative_rise"], row["relative_value"], row["relative_time_s"], row["relative_occurrence"]))
        self.assertEqual((1, 0, OPEND + 10 * HOUR), (row["early_results"], row["late_results"], row["latest_time_s"]))

    def test_an_observed_negative_has_both_rises_false(self) -> None:
        row = row_of(ANESTART, OPEND, [lab(ANESTART - DAY, 1.0), lab(OPEND + DAY, 1.1), lab(OPEND + 5 * DAY, 1.2)])
        self.assertEqual((NEGATIVE, True, False, False), (row["label"], row["eligible"], row["absolute_rise"], row["relative_rise"]))
        self.assertEqual((1, 1), (row["early_results"], row["late_results"]))

    def test_an_unknown_case_has_its_reason_and_no_rise(self) -> None:
        row = row_of(ANESTART, OPEND, [lab(OPEND + HOUR, 9.0)])
        self.assertEqual(
            (UNKNOWN, False, "no_baseline", None, None, None),
            (row["label"], row["eligible"], row["eligibility_reason"], row["baseline_value"], row["absolute_rise"], row["relative_rise"]),
        )

    def test_a_case_with_no_laboratory_result_is_unknown_for_both_causes(self) -> None:
        nothing: list[list[dict[str, Any]] | None] = [None, []]
        for results in nothing:
            with self.subTest(results=results):
                row = row_of(ANESTART, OPEND, results)
                self.assertEqual(
                    (UNKNOWN, "no_baseline_and_no_followup_result", 0, 0, None),
                    (row["label"], row["eligibility_reason"], row["early_results"], row["late_results"], row["latest_time_s"]),
                )

    def test_a_result_with_no_value_is_not_a_measurement(self) -> None:
        row = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, None), lab(OPEND + HOUR, 1.0)])
        self.assertEqual((UNKNOWN, "no_baseline"), (row["label"], row["eligibility_reason"]))

    def test_the_whole_numbers_and_floats_that_spark_gives_are_read_the_same(self) -> None:
        as_int = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1), lab(OPEND + HOUR, 2)])
        as_float = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1.0), lab(OPEND + HOUR, 2.0)])
        self.assertEqual(as_float, as_int)
        self.assertEqual(POSITIVE, as_int["label"])

    def test_the_missing_start_of_anaesthesia_has_no_baseline_window(self) -> None:
        row = row_of(None, OPEND, [lab(OPEND + HOUR, 1.0)])
        self.assertEqual(
            (UNKNOWN, "no_baseline", None, None), (row["label"], row["eligibility_reason"], row["baseline_window_start_s"], row["baseline_window_end_s"])
        )

    def test_the_label_values_are_the_three_values_only(self) -> None:
        self.assertEqual((POSITIVE, NEGATIVE, UNKNOWN), LABEL_VALUES)


class Icu(unittest.TestCase):
    def test_a_stay_no_stay_and_unknown(self) -> None:
        self.assertEqual((5, 2.0, POSITIVE, True, "recorded"), icu_row(5, 2))
        self.assertEqual((5, 0.0, NEGATIVE, True, "recorded"), icu_row(5, 0))
        self.assertEqual((5, None, UNKNOWN, False, "missing"), icu_row(5, None))
        self.assertEqual((5, -1.0, UNKNOWN, False, "invalid"), icu_row(5, -1))

    def test_a_missing_value_is_not_zero(self) -> None:
        self.assertNotEqual(icu_row(5, 0), icu_row(5, None))


class CommandLine(unittest.TestCase):
    def test_the_job_prints_its_own_name_on_its_codes(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stop:
            main(["build"])
        self.assertEqual((2, "silver_labels: usage\n"), (stop.exception.code, err.getvalue()))


if __name__ == "__main__":
    unittest.main()
