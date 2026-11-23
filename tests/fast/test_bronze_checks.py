"""The column-level checks of the Bronze tables: which checks each table gets, and how their results decide whether the job may go on.

The checks themselves run in Great Expectations on a Spark table (``spark/bronze/quality.py``), which the fast tests cannot start. What decides a run is here:
the plan must cover every column and every key, it must never judge a published value, and a check that has no result must count as a failure.
"""

from __future__ import annotations

import re
import unittest

from spark.bronze.checks import (
    ALLOWED_EXPECTATIONS,
    GUARD_KEY,
    GUARD_STRUCTURE,
    Check,
    CheckError,
    CheckResult,
    checks_for,
    key_checks,
    plan,
    result_document,
    verdict,
)
from spark.bronze.tables import NUMERIC, STRINGS, TABLES, TRACKS, WAVEFORM

PUBLISHED_VALUE_COLUMNS = {
    "numeric_observations": {"value", "source_time"},
    "string_events": {"text", "source_time"},
    "waveform_seconds": {"samples", "start_offset_us"},
    "tracks": {"unit", "sample_rate", "gain", "value_offset", "min_display", "max_display", "color", "monitor_type", "device", "track_name"},
}


def passing(checks: list[Check]) -> list[CheckResult]:
    return [CheckResult(check, True, 0) for check in checks]


def failing(results: list[CheckResult], table: str, name: str, unexpected: int = 1) -> list[CheckResult]:
    return [CheckResult(item.check, False, unexpected) if (item.check.table, item.check.name) == (table, name) else item for item in results]


class PlanTest(unittest.TestCase):
    def test_every_column_of_every_table_has_a_not_null_check(self) -> None:
        for table in TABLES:
            covered = {check.kwargs["column"] for check in checks_for(table) if check.expectation == "ExpectColumnValuesToNotBeNull"}
            self.assertEqual({name for name, _ in table.columns}, covered, table.name)

    def test_a_key_column_is_judged_by_the_key_guard_and_every_other_column_by_the_structure_guard(self) -> None:
        for table in TABLES:
            for check in checks_for(table):
                if check.expectation == "ExpectColumnValuesToNotBeNull":
                    expected = GUARD_KEY if check.kwargs["column"] in table.key else GUARD_STRUCTURE
                    self.assertEqual(expected, check.guard, f"{table.name}.{check.kwargs['column']}")

    def test_each_table_has_one_unique_check_over_exactly_its_key_in_the_key_guard(self) -> None:
        for table in TABLES:
            unique = [check for check in checks_for(table) if check.expectation == "ExpectCompoundColumnsToBeUnique"]
            self.assertEqual(1, len(unique), table.name)
            self.assertEqual(list(table.key), list(unique[0].kwargs["column_list"]), table.name)
            self.assertEqual(GUARD_KEY, unique[0].guard)

    def test_no_check_judges_the_value_of_a_published_reading_beyond_whether_it_is_there(self) -> None:
        for table in TABLES:
            for check in checks_for(table):
                column = check.kwargs.get("column")
                if column in PUBLISHED_VALUE_COLUMNS[table.name]:
                    self.assertEqual("ExpectColumnValuesToNotBeNull", check.expectation, f"{table.name}.{column}: {check.name}")
                if check.expectation == "UnexpectedRowsExpectation":
                    for value_column in PUBLISHED_VALUE_COLUMNS[table.name] - {"samples"}:
                        self.assertNotIn(value_column, check.kwargs["unexpected_rows_query"], f"{table.name}: {check.name}")

    def test_only_expectations_that_were_read_in_the_pinned_release_are_used(self) -> None:
        for table in TABLES:
            for check in checks_for(table):
                self.assertIn(check.expectation, ALLOWED_EXPECTATIONS, check.name)

    def test_a_lineage_hash_must_be_sixty_four_lower_case_hex_digits(self) -> None:
        for table in TABLES:
            patterns = [check.kwargs["regex"] for check in checks_for(table) if check.expectation == "ExpectColumnValuesToMatchRegex"]
            self.assertEqual(1, len(patterns), table.name)
            self.assertIsNotNone(re.search(patterns[0], "a" * 64))
            for bad in ("a" * 63, "a" * 65, "A" * 64, "g" * 64, "", " " + "a" * 63, "a" * 64 + "\n"):
                self.assertIsNone(re.search(patterns[0], bad), repr(bad))

    def test_the_occurrence_of_a_reading_cannot_be_negative_and_the_first_is_zero(self) -> None:
        for table in (NUMERIC, STRINGS):
            found = [check for check in checks_for(table) if check.expectation == "ExpectColumnValuesToBeBetween"]
            self.assertEqual([("source_occurrence", 0, None)], [(c.kwargs["column"], c.kwargs["min_value"], c.kwargs.get("max_value")) for c in found])
        for table in (WAVEFORM, TRACKS):
            self.assertEqual([], [check for check in checks_for(table) if check.expectation == "ExpectColumnValuesToBeBetween"])

    def test_a_second_of_a_waveform_holds_at_least_one_sample(self) -> None:
        found = [check for check in checks_for(WAVEFORM) if check.name == "samples hold a sample"]
        self.assertEqual(1, len(found))
        self.assertEqual("UnexpectedRowsExpectation", found[0].expectation)
        self.assertIn("{batch}", found[0].kwargs["unexpected_rows_query"])
        self.assertIn("size(samples) < 1", found[0].kwargs["unexpected_rows_query"])
        self.assertEqual(GUARD_STRUCTURE, found[0].guard)

    def test_no_sample_of_a_waveform_second_is_null_but_a_nan_is_a_published_value(self) -> None:
        found = [check for check in checks_for(WAVEFORM) if check.name == "samples have no null element"]
        self.assertEqual(1, len(found))
        self.assertEqual("UnexpectedRowsExpectation", found[0].expectation)
        self.assertEqual(GUARD_STRUCTURE, found[0].guard)
        query = found[0].kwargs["unexpected_rows_query"]
        self.assertEqual("SELECT * FROM {batch} WHERE exists(samples, sample -> sample IS NULL)", query)
        self.assertNotIn("isnan", query)

    def test_only_the_waveform_table_has_a_query_check(self) -> None:
        for table in TABLES:
            found = [check for check in checks_for(table) if check.expectation == "UnexpectedRowsExpectation"]
            self.assertEqual(2 if table is WAVEFORM else 0, len(found), table.name)

    def test_the_kind_the_types_and_the_sample_type_of_a_track_are_ones_the_file_format_defines(self) -> None:
        sets = {check.kwargs["column"]: set(check.kwargs["value_set"]) for check in checks_for(TRACKS) if check.expectation == "ExpectColumnValuesToBeInSet"}
        self.assertEqual(
            {
                "kind": {"waveform", "numeric", "string"},
                "track_type": {1, 2, 5},
                "sample_type": {"float32", "float64", "int8", "uint8", "int16", "uint16", "int32", "uint32", "text"},
            },
            sets,
        )

    def test_only_the_tracks_table_must_not_be_empty(self) -> None:
        for table in TABLES:
            found = [check for check in checks_for(table) if check.expectation == "ExpectTableRowCountToBeBetween"]
            self.assertEqual(1 if table is TRACKS else 0, len(found), table.name)
            if found:
                self.assertEqual({"min_value": 1}, found[0].kwargs)

    def test_a_check_name_is_unique_within_its_table_so_a_result_can_be_matched_to_it(self) -> None:
        for table in TABLES:
            names = [check.name for check in checks_for(table)]
            self.assertEqual(len(names), len(set(names)), table.name)

    def test_the_plan_is_the_checks_of_the_four_tables_in_order(self) -> None:
        self.assertEqual([check for table in TABLES for check in checks_for(table)], plan())


class KeyChecksTest(unittest.TestCase):
    def test_the_key_checks_of_a_table_are_a_not_null_check_for_each_key_column_and_one_unique_check_over_the_key(self) -> None:
        found = key_checks("lab_results", ("caseid", "dt", "name", "source_occurrence"))
        self.assertEqual(
            [
                ("caseid not null", "ExpectColumnValuesToNotBeNull", {"column": "caseid"}),
                ("dt not null", "ExpectColumnValuesToNotBeNull", {"column": "dt"}),
                ("name not null", "ExpectColumnValuesToNotBeNull", {"column": "name"}),
                ("source_occurrence not null", "ExpectColumnValuesToNotBeNull", {"column": "source_occurrence"}),
                ("key unique", "ExpectCompoundColumnsToBeUnique", {"column_list": ["caseid", "dt", "name", "source_occurrence"]}),
            ],
            [(check.name, check.expectation, check.kwargs) for check in found],
        )
        self.assertEqual({GUARD_KEY}, {check.guard for check in found})
        self.assertEqual({"lab_results"}, {check.table for check in found})

    def test_the_plan_holds_exactly_the_key_checks_of_each_table(self) -> None:
        for table in TABLES:
            self.assertEqual(key_checks(table.name, table.key), [check for check in checks_for(table) if check.guard == GUARD_KEY])


class VerdictTest(unittest.TestCase):
    def test_all_checks_passing_pass_both_guards(self) -> None:
        result = verdict(plan(), passing(plan()))
        self.assertTrue(result.passed)
        self.assertEqual({GUARD_KEY: True, GUARD_STRUCTURE: True}, result.guards)
        self.assertEqual((), result.failed)

    def test_a_failed_key_check_trips_the_key_guard_alone(self) -> None:
        result = verdict(plan(), failing(passing(plan()), "numeric_observations", "case_id not null"))
        self.assertFalse(result.passed)
        self.assertEqual({GUARD_KEY: False, GUARD_STRUCTURE: True}, result.guards)
        self.assertEqual([("numeric_observations", "case_id not null", GUARD_KEY, 1)], [(f.table, f.name, f.guard, f.unexpected) for f in result.failed])

    def test_a_failed_structure_check_trips_the_structure_guard_alone(self) -> None:
        result = verdict(plan(), failing(passing(plan()), "tracks", "kind in set"))
        self.assertFalse(result.passed)
        self.assertEqual({GUARD_KEY: True, GUARD_STRUCTURE: False}, result.guards)

    def test_a_check_with_no_result_is_a_failure_of_its_guard(self) -> None:
        results = [item for item in passing(plan()) if (item.check.table, item.check.name) != ("waveform_seconds", "case_id not null")]
        result = verdict(plan(), results)
        self.assertFalse(result.passed)
        self.assertEqual(GUARD_KEY, result.failed[0].guard)
        self.assertEqual(-1, result.failed[0].unexpected)

    def test_a_result_for_a_check_that_the_plan_does_not_hold_is_refused(self) -> None:
        extra = CheckResult(checks_for(TRACKS)[0].__class__("tracks", GUARD_KEY, "invented", "ExpectColumnValuesToNotBeNull", {"column": "case_id"}), True, 0)
        with self.assertRaises(CheckError):
            verdict(plan(), [*passing(plan()), extra])

    def test_two_results_for_one_check_are_refused(self) -> None:
        with self.assertRaises(CheckError):
            verdict(plan(), [*passing(plan()), passing(plan())[0]])

    def test_the_document_holds_names_and_counts_and_never_a_value(self) -> None:
        document = result_document(verdict(plan(), failing(passing(plan()), "numeric_observations", "key unique", 3)))
        self.assertFalse(document["passed"])
        self.assertEqual(False, document["guards"][GUARD_KEY])
        row = next(item for item in document["checks"] if item["table"] == "numeric_observations" and item["name"] == "key unique")
        self.assertEqual({"table", "name", "guard", "passed", "unexpected"}, set(row))
        self.assertEqual(3, row["unexpected"])
        self.assertEqual(len(plan()), len(document["checks"]))


if __name__ == "__main__":
    unittest.main()
