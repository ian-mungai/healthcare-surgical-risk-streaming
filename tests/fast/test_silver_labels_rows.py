"""The tables of the labels, and the row functions that the Spark job passes to its Python worker: thin adapters over the label rules.

Every case here is hand-built and kept apart from published data. Failure modes covered here:

- A label row does not carry what the card requires: the baseline value with its time and its source row, the windows, the measurements of each rise, the label
  and its eligibility reason, so a later check cannot show what a label read.
- The row function puts a value in another column than its name, or its length is not the table's, so Spark stores a shifted row.
- The worker reads the laboratory rows in a form that the table does not have (a missing result, a time that is not a number) or reads a test other than
  creatinine, or passes the results of another case.
- An intensive care value that Spark reads as a whole number is not the float that the rule reads, or a missing value becomes zero.
- The tables name their columns and keys twice (in the table module and in the row functions) and the two lists differ.
- A negative row keeps no measurement that it used: one baseline with follow-up of 1.1 and of 1.2 gives the same row, or a recorded measurement does not name a
  row of the laboratory table (its time, its number among the results of that case, time and test, and its value) so that a later check cannot reproduce it.
- A negative row keeps only the highest follow-up results, not the 48-hour comparison that decided it: a baseline of 1.2 with follow-up of 1.25, 1.00 and 1.24
  gives the same row when 1.00 becomes 1.02, and the label turns positive with 0.90 while the stored maxima stay the same. The row stores the earlier and the
  later result of the largest rise, below the limit too, and the rule check reads them.
- A cases frame that holds only the declared input columns fails in a function that reads another column of the cases (the ownership check read ``opstart``).
"""

from __future__ import annotations

import contextlib
import io
import unittest
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

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


class Column:
    """A Spark column stand-in that answers every call and operator with another column."""

    def __getattr__(self, _name: str) -> Any:
        return lambda *_a, **_k: Column()

    def binary(self, _other: Any) -> Any:
        return Column()

    def __invert__(self) -> Any:
        return Column()

    __gt__ = __lt__ = __ge__ = __le__ = __eq__ = __ne__ = __and__ = __or__ = __rand__ = __ror__ = __add__ = __radd__ = __sub__ = __rsub__ = binary
    __mul__ = __rmul__ = binary


def column(names: set[str], name: str) -> Column:
    names.add(name)
    return Column()


def literal(values: list[Any], value: Any) -> Column:
    values.append(value)
    return Column()


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
            "early_highest_value",
            "early_highest_time_s",
            "early_highest_occurrence",
            "followup_highest_value",
            "followup_highest_time_s",
            "followup_highest_occurrence",
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
        for name in ("baseline_time_s", "later_time_s", "latest_time_s", "followup_start_s", "early_highest_time_s", "followup_highest_time_s"):
            self.assertEqual("bigint", kinds[name], name)
        for name in ("baseline_value", "earlier_value", "later_value", "relative_value", "early_highest_value", "followup_highest_value"):
            self.assertEqual("double", kinds[name], name)
        for name in ("baseline_occurrence", "early_results", "late_results", "early_highest_occurrence", "followup_highest_occurrence"):
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

    def test_one_baseline_with_follow_up_of_1_1_and_of_1_2_are_not_the_same_negative_row(self) -> None:
        low = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1.0), lab(OPEND + DAY, 1.1)])
        high = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1.0), lab(OPEND + DAY, 1.2)])
        self.assertEqual((NEGATIVE, NEGATIVE), (low["label"], high["label"]))
        self.assertNotEqual(low, high)
        self.assertEqual((1.1, 1.2), (low["followup_highest_value"], high["followup_highest_value"]))

    def test_the_three_variants_of_one_negative_are_three_different_rows(self) -> None:
        def variant(early: float) -> dict[str, Any]:
            return row_of(ANESTART, OPEND, [lab(ANESTART - 2 * DAY, 1.2), lab(OPEND + HOUR, 1.25), lab(OPEND + 10 * HOUR, early), lab(OPEND + 20 * HOUR, 1.24)])

        base, nearer, deeper = variant(1.00), variant(1.02), variant(0.90)
        self.assertEqual((NEGATIVE, NEGATIVE, POSITIVE), (base["label"], nearer["label"], deeper["label"]))
        self.assertEqual((False, 1.00, 1.24), (base["absolute_rise"], base["earlier_value"], base["later_value"]))
        self.assertEqual((False, 1.02, 1.24), (nearer["absolute_rise"], nearer["earlier_value"], nearer["later_value"]))
        self.assertEqual((True, 0.90, 1.24), (deeper["absolute_rise"], deeper["earlier_value"], deeper["later_value"]))
        self.assertEqual(3, len({tuple(row.items()) for row in (base, nearer, deeper)}))

    def test_every_measurement_a_row_records_names_one_row_of_the_laboratory_table(self) -> None:
        table = [
            lab(ANESTART - 5 * DAY, 0.8),
            lab(ANESTART - HOUR, 1.0, 1),
            lab(ANESTART - HOUR, 1.4, 2),
            lab(OPEND + HOUR, 1.1),
            lab(OPEND + 30 * HOUR, 1.5),
            lab(OPEND + 4 * DAY, 1.2),
            lab(OPEND + 9 * DAY, 9.0),
        ]
        row = row_of(ANESTART, OPEND, table)
        self.assertEqual((POSITIVE, True), (row["label"], row["absolute_rise"]))
        for prefix in ("baseline", "earlier", "later", "early_highest", "followup_highest"):
            with self.subTest(measurement=prefix):
                value, time_s, number = row[f"{prefix}_value"], row[f"{prefix}_time_s"], row[f"{prefix}_occurrence"]
                found = [item for item in table if (item["dt"], item["source_occurrence"], item["result"]) == (time_s, number, value)]
                self.assertEqual(1, len(found))

    def test_a_row_without_a_baseline_still_records_the_highest_follow_up_it_has(self) -> None:
        row = row_of(ANESTART, OPEND, [lab(OPEND + HOUR, 1.1), lab(OPEND + 3 * DAY, 1.2)])
        self.assertEqual((UNKNOWN, 1.1, 1.2), (row["label"], row["early_highest_value"], row["followup_highest_value"]))
        nothing = row_of(ANESTART, OPEND, [lab(ANESTART - HOUR, 1.0)])
        self.assertEqual((None, None, None), (nothing["early_highest_value"], nothing["followup_highest_time_s"], nothing["followup_highest_occurrence"]))

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


class Declared(unittest.TestCase):
    def test_every_published_case_column_that_a_frame_function_reads_is_a_declared_input_column(self) -> None:
        """Run the frame functions on recording stand-ins and collect every published column of the cases that they name."""
        from unittest.mock import MagicMock, patch

        from spark.jobs.silver_labels import CASE_FIELDS
        from spark.silver import label_frames
        from spark.silver_cases import PUBLISHED_CLINICAL_COLUMNS

        names: set[str] = set()

        def strings(value: Any) -> Iterator[str]:
            if isinstance(value, str):
                yield value
            elif isinstance(value, list | tuple):
                for item in value:
                    yield from strings(item)

        def collect(root: MagicMock) -> None:
            for call in root.mock_calls:
                for argument in (*call.args, *call.kwargs.values()):
                    names.update(strings(argument))

        functions = MagicMock()
        functions.col.side_effect = lambda name: column(names, name)
        cases, lab, split = MagicMock(), MagicMock(), MagicMock()
        with (
            patch.object(label_frames, "functions", return_value=functions),
            patch.object(label_frames, "types", return_value=MagicMock()),
            patch.object(label_frames, "typed", side_effect=lambda frame, columns: frame),
        ):
            label_frames.ownership_flag(cases)
            label_frames.input_facts(cases, split)
            label_frames.kidney_frame(cases, lab)
            label_frames.icu_frame(cases)
            label_frames.development_values(cases, split)
        for root in (cases, lab, split):
            collect(root)
        read = names & set(PUBLISHED_CLINICAL_COLUMNS)
        self.assertTrue({"caseid", "subjectid", "opend", "icu_days"} <= read, read)
        self.assertEqual(set(), read - set(CASE_FIELDS))


class Facts(unittest.TestCase):
    """The facts functions name exactly the checks of their table, and take the column checks from the Great Expectations runner and not from Spark."""

    def test_each_facts_function_names_every_check_of_its_table_and_asks_the_runner_for_its_column_flags(self) -> None:
        from unittest.mock import MagicMock, patch

        from spark.bronze.checks import CheckResult
        from spark.silver import label_frames
        from spark.silver.label_checks import COLUMN_FLAGS, ICU, KIDNEY, MEASURE, TABLE_CHECKS
        from spark.silver.label_measure import IcuMeasureRow

        calls: list[tuple[Any, str]] = []

        def runner(frame: Any, checks: Any) -> list[CheckResult]:
            calls.append((frame, checks[0].table))
            return [CheckResult(check, True, 0) for check in checks]

        functions = MagicMock()
        functions.col.side_effect = lambda _name: Column()
        stored, cases, lab, split, session = (MagicMock() for _ in range(5))
        with (
            patch.object(label_frames, "functions", return_value=functions),
            patch.object(label_frames, "types", return_value=MagicMock()),
            patch.object(label_frames, "typed", side_effect=lambda frame, columns: frame),
            patch.object(label_frames, "measure_rows", return_value=[IcuMeasureRow("cases", "cases", 1.0)]),
        ):
            found = {
                ICU: label_frames.icu_facts(stored, cases, runner),
                KIDNEY: label_frames.kidney_facts(stored, cases, lab, runner),
                MEASURE: label_frames.measure_facts(stored, session, cases, split, runner),
            }
        for table, facts in found.items():
            with self.subTest(table=table):
                self.assertEqual(set(TABLE_CHECKS[table]), set(facts.flags))
                self.assertTrue(set(COLUMN_FLAGS[table]) <= set(facts.flags))
                self.assertIn((stored, table), calls)
                self.assertTrue(all(value in (0, 1) for value in facts.flags.values()))


class RowCount(unittest.TestCase):
    """The row count of a label table is checked against the number of cases of the input that the facts function is given, by the library expectation."""

    def bounds(self, table: str) -> list[dict[str, Any]]:
        from unittest.mock import MagicMock, patch

        from spark.bronze.checks import CheckResult
        from spark.silver import label_frames

        seen: list[Any] = []

        def runner(_frame: Any, checks: Any) -> list[CheckResult]:
            seen.extend(checks)
            return [CheckResult(check, True, 0) for check in checks]

        functions = MagicMock()
        functions.col.side_effect = lambda _name: Column()
        stored, cases, lab = MagicMock(), MagicMock(), MagicMock()
        cases.count.return_value = 77
        with (
            patch.object(label_frames, "functions", return_value=functions),
            patch.object(label_frames, "types", return_value=MagicMock()),
            patch.object(label_frames, "typed", side_effect=lambda frame, columns: frame),
        ):
            if table == "icu":
                label_frames.icu_facts(stored, cases, runner)
            else:
                label_frames.kidney_facts(stored, cases, lab, runner)
        return [check.kwargs for check in seen if check.expectation == "ExpectTableRowCountToBeBetween"]

    def test_the_intensive_care_labels_are_checked_for_exactly_as_many_rows_as_the_cases_frame_holds(self) -> None:
        self.assertEqual([{"min_value": 77, "max_value": 77}], self.bounds("icu"))

    def test_the_kidney_injury_labels_are_checked_for_exactly_as_many_rows_as_the_cases_frame_holds(self) -> None:
        self.assertEqual([{"min_value": 77, "max_value": 77}], self.bounds("kidney"))


class Wiring(unittest.TestCase):
    """The Spark checks cannot run here (their meaning is proved by the acceptance run), but what they are made of can be read: they are made of the rule's
    constants and of the columns that the row records."""

    def recorded(self, function: Any, *arguments: Any) -> tuple[set[str], list[Any]]:
        from unittest.mock import MagicMock, patch

        from spark.silver import label_frames

        names: set[str] = set()
        literals: list[Any] = []
        functions = MagicMock()
        functions.col.side_effect = lambda name: column(names, name)
        functions.lit.side_effect = lambda value: literal(literals, value)
        stored = MagicMock()
        with (
            patch.object(label_frames, "functions", return_value=functions),
            patch.object(label_frames, "types", return_value=MagicMock()),
            patch.object(label_frames, "typed", side_effect=lambda frame, columns: frame),
            patch.object(label_frames, "INVALID_BELOW", -7.5),
            patch.object(label_frames, "STAY_ABOVE", 3.25),
        ):
            function(stored, *arguments, lambda frame, checks: [])
        return names, literals

    def test_the_intensive_care_value_rule_is_made_of_the_rules_constants(self) -> None:
        from spark.silver import label_frames

        _, literals = self.recorded(label_frames.icu_facts, MagicMock())
        self.assertIn(-7.5, literals)
        self.assertIn(3.25, literals)

    def test_the_kidney_rule_check_reads_the_value_and_the_time_of_each_highest_result_that_the_row_records(self) -> None:
        from spark.silver import label_frames

        names, _ = self.recorded(label_frames.kidney_facts, MagicMock(), MagicMock())
        for column in KIDNEY_ROW_COLUMNS:
            if column.startswith(("early_highest_", "followup_highest_")) and not column.endswith("_occurrence"):
                self.assertIn(column, names)

    def test_the_kidney_rule_check_reads_the_pair_of_the_largest_rise_of_a_negative(self) -> None:
        from spark.silver import label_frames

        names, literals = self.recorded(label_frames.kidney_facts, MagicMock(), MagicMock())
        for column in ("earlier_value", "earlier_time_s", "later_value", "later_time_s"):
            self.assertIn(column, names)
        self.assertIn(label_frames.ABSOLUTE_RISE + label_frames.TOLERANCE, literals)


class CommandLine(unittest.TestCase):
    def test_the_job_prints_its_own_name_on_its_codes(self) -> None:
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stop:
            main(["build"])
        self.assertEqual((2, "silver_labels: usage\n"), (stop.exception.code, err.getvalue()))


if __name__ == "__main__":
    unittest.main()
