"""No count of a held-out patient's table leaves the table checks (the owner's decision D). No Spark runs here.

The clinical table holds the outcome columns (``icu_days`` and ``death_inhosp``) and the laboratory table holds the creatinine results that the kidney
outcome is built from, so a row count, a count of rows that differ or any number that follows from them changes when only a held-out patient's row changes. The
result of the table checks holds, for these two tables, the snapshot ID, the column counts (the file's header and the table's schema hold no outcome) and one
flag with one fixed code for each comparison. The track list holds no outcome and keeps its row counts.

Failure modes covered here:

- A comparison of a sealed table holds a count: the row counts of the two sides, the rows that only one side holds or any number other than 0 or 1.
- Two tables that differ by one row and by five rows give the same result.
- A result that holds the counted codes or the row counts of a sealed table is accepted by the parser.
- A sealed table's result is written with row counts set by a caller.
- The track list loses its row counts.
- A result of the earlier format is accepted.
- A producer writes a counted flag (``table_compared;equal=23``), a check that its table does not have (``separator_in_field;rows=23`` for a sealed table), a
  flag its code does not give or a check out of order, and the receiver is the first to refuse it.
- A saved result of the earlier format, or one with a count of a sealed table, is returned again or written into a new receipt when a run is repeated.
"""

from __future__ import annotations

import importlib
import json
import re
import unittest
from collections.abc import Sequence
from typing import Any

from spark import table_checks as rules
from tests.fast.test_table_checks_job import COLUMNS, ROWS, frames
from tests.fast.test_table_checks_rules import ROWS as RESULT_ROWS
from tests.fast.test_table_checks_rules import the_plan

job: Any = importlib.import_module("spark.jobs.table_checks")
SEALED = ("clinical_data", "lab_data")
COMPARISONS = ("row count", "key multiset", "row payload")
FIXED_CODE = re.compile(r"table_compared;equal=[01]")
ALLOWED_NUMBERS = {"snapshot_id", "columns_source", "columns_table"}


def started(name: str, *, upto: str | None = None) -> rules.TableResult:
    """The result of a file table with every required check before ``upto`` added as passed, or all of them but the comparisons when ``upto`` is None."""
    result = rules.TableResult(name, 1, columns_source=3, columns_table=3)
    for check in rules.REQUIRED_CHECKS[name]:
        if check == (upto or "row count"):
            break
        result.add(check, ParserRefusesCounts.detail(check, True, 0))
    return result


def complete(name: str) -> rules.TableResult:
    """A result with every required check of the table, passed, as the job writes it, and the track list with its row counts."""
    result = rules.TableResult(name, 1, columns_source=3, columns_table=3)
    if not rules.SPEC_BY_NAME[name].sealed:
        result.rows_source = result.rows_table = 7
    for check in rules.REQUIRED_CHECKS[name]:
        result.add(check, ParserRefusesCounts.detail(check, rules.SPEC_BY_NAME[name].sealed, 7))
    return result


def compared(name: str, table_rows: Sequence[tuple[str, ...]]) -> rules.TableResult:
    result = started(name)
    source, table = frames(table_rows)
    job.compare_rows(result, rules.SPEC_BY_NAME[name], COLUMNS, source, table)
    return result


def numbers(value: Any, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """The path of every whole number, apart from a flag, in a document."""
    if isinstance(value, bool):
        return []
    if isinstance(value, int):
        return [path]
    if isinstance(value, dict):
        return [found for key, item in value.items() for found in numbers(item, (*path, key))]
    if isinstance(value, list):
        return [found for item in value for found in numbers(item, path)]
    return []


class SealedTables(unittest.TestCase):
    def test_the_spec_seals_the_clinical_and_the_laboratory_table_and_not_the_track_list(self) -> None:
        self.assertEqual({"clinical_data": True, "lab_data": True, "track_list": False}, {spec.name: spec.sealed for spec in rules.TABLES})


class JobComparisons(unittest.TestCase):
    def test_a_sealed_comparison_holds_one_fixed_code_whatever_the_difference(self) -> None:
        cases = {
            "equal": list(ROWS),
            "one row short": ROWS[:3],
            "three rows short": ROWS[:1],
            "no rows": [],
            "extra rows": [*ROWS, ("4", "10", "50"), ("5", "11", "51")],
            "a value changed": [("1", "07", "61"), *ROWS[1:]],
        }
        for name in SEALED:
            for label, rows in cases.items():
                with self.subTest(table=name, case=label):
                    result = compared(name, rows)
                    self.assertEqual(list(COMPARISONS), [check.name for check in result.checks[3:]])
                    for check in result.checks[3:]:
                        self.assertRegex(check.detail, FIXED_CODE)
                        self.assertEqual(check.detail.endswith("=1"), check.passed)

    def test_two_tables_that_differ_by_one_row_and_by_three_rows_give_the_same_result(self) -> None:
        for name in SEALED:
            one, three = compared(name, ROWS[:3]).document(), compared(name, ROWS[:1]).document()
            self.assertEqual(one, three)
            extra = [("4", "1", "1"), ("5", "1", "1"), ("6", "1", "1")]
            self.assertEqual(compared(name, [*ROWS, *extra[:1]]).document(), compared(name, [*ROWS, *extra]).document())

    def test_the_row_counts_of_the_result_stay_empty(self) -> None:
        for name in SEALED:
            result = compared(name, ROWS[:2])
            self.assertEqual((None, None), (result.rows_source, result.rows_table))
            document = result.document()
            self.assertEqual((None, None), (document["rows_source"], document["rows_table"]))

    def test_no_number_of_the_document_follows_from_a_row_count(self) -> None:
        for name in SEALED:
            document = compared(name, ROWS[:2]).document()
            self.assertEqual([], [path for path in numbers(document) if path[-1] not in ALLOWED_NUMBERS], document)

    def test_the_marker_values_of_the_rows_stay_out_of_the_document(self) -> None:
        rows = [("1", "07", "60"), ("2", "08", ""), ("MARKER_9d2", "09", "71")]
        self.assertNotIn("MARKER_9d2", json.dumps(compared("clinical_data", rows).document()))


class RulesRefuseCounts(unittest.TestCase):
    def test_a_sealed_table_may_hold_only_the_fixed_code_for_each_comparison(self) -> None:
        for name in SEALED:
            for check in COMPARISONS:
                self.assertEqual(("table_compared",), rules.allowed_codes(name, check), (name, check))

    def test_the_track_list_keeps_its_counted_row_check(self) -> None:
        self.assertEqual(("row_count",), rules.allowed_codes("track_list", "row count"))
        self.assertEqual({"source": 7, "table": 7}, rules.parse_detail(rules.detail("row_count", source=7, table=7))[1])

    def test_the_fixed_code_has_one_flag_field_and_passes_only_for_one(self) -> None:
        self.assertEqual(("equal",), rules.DETAIL_CODES["table_compared"])
        self.assertTrue(rules.detail_holds("table_compared;equal=1"))
        self.assertFalse(rules.detail_holds("table_compared;equal=0"))

    def test_no_code_of_a_comparison_of_a_sealed_table_holds_a_count(self) -> None:
        for name in SEALED:
            for check in COMPARISONS:
                for code in rules.allowed_codes(name, check):
                    self.assertEqual(("equal",), rules.DETAIL_CODES[code])
        self.assertNotIn("rows_differ", rules.DETAIL_CODES)

    def test_a_sealed_table_that_is_given_row_counts_is_not_written(self) -> None:
        for name in SEALED:
            for fields in ({"rows_source": 5}, {"rows_table": 5}, {"rows_source": 5, "rows_table": 5}):
                result = complete(name)
                self.assertEqual(name, result.document()["table"])
                for field, value in fields.items():
                    setattr(result, field, value)
                with self.subTest(table=name, fields=fields), self.assertRaises(rules.RulesError):
                    result.document()

    def test_a_count_that_is_not_a_whole_number_within_its_bound_is_not_written(self) -> None:
        for field, value in (
            ("rows_source", -1),
            ("rows_table", rules.MAX_ROWS + 1),
            ("rows_source", True),
            ("columns_source", rules.MAX_COLUMN_COUNT + 1),
            ("columns_table", -2),
        ):
            result = complete("track_list")
            self.assertEqual("track_list", result.document()["table"])
            setattr(result, field, value)
            with self.subTest(field=field, value=value), self.assertRaises(rules.RulesError):
                result.document()

    def test_a_sealed_table_refuses_a_counted_detail_whatever_its_check(self) -> None:
        result = rules.TableResult("clinical_data", 1)
        for check in COMPARISONS:
            with self.subTest(check=check), self.assertRaises(rules.RulesError):
                result.add(check, rules.detail("row_count", source=5, table=5))


class ProducerRefusesCounts(unittest.TestCase):
    """The producer holds its own checks to what the receiver accepts: a counted flag or a check of another table fails where it is made."""

    def test_a_sealed_table_refuses_a_flag_that_holds_a_number_whatever_the_check(self) -> None:
        for name in SEALED:
            for check in COMPARISONS:
                for equal in (2, 23, 4719238):
                    with self.subTest(table=name, check=check, equal=equal), self.assertRaises(rules.RulesError):
                        started(name, upto=check).add(check, f"table_compared;equal={equal}")

    def test_a_flag_of_0_or_1_is_the_only_flag_that_a_producer_may_write(self) -> None:
        for name in SEALED:
            for check in COMPARISONS:
                for equal in (0, 1):
                    self.assertEqual(bool(equal), started(name, upto=check).add(check, f"table_compared;equal={equal}"))

    def test_a_check_that_the_table_does_not_have_is_refused(self) -> None:
        for name in SEALED:
            for check, detail in (
                ("rows digest", "separator_in_field;rows=23"),
                ("rows digest", "digest_compared;equal=1"),
                ("extra", "ok"),
            ):
                with self.subTest(table=name, check=check, detail=detail), self.assertRaises(rules.RulesError):
                    started(name, upto="row payload").add(check, detail)
        with self.assertRaises(rules.RulesError):
            rules.TableResult("track_list", 1, columns_source=3, columns_table=3).add("key multiset", "table_compared;equal=1")

    def test_a_check_that_comes_before_one_already_added_or_is_added_twice_is_refused(self) -> None:
        for name in SEALED:
            for again in ("staged file", "snapshot", "columns", "row count", "key multiset"):
                with self.subTest(table=name, again=again), self.assertRaises(rules.RulesError):
                    started(name, upto="row payload").add(again, ParserRefusesCounts.detail(again, True, 0))

    def test_the_checks_of_each_table_in_order_are_accepted_as_a_control(self) -> None:
        for spec in rules.TABLES:
            result = rules.TableResult(spec.name, 1, columns_source=3, columns_table=3)
            if not spec.sealed:
                result.rows_source = result.rows_table = 7
            for name in rules.REQUIRED_CHECKS[spec.name]:
                result.add(name, ParserRefusesCounts.detail(name, spec.sealed, 7))
            self.assertTrue(result.passed)
            self.assertEqual(list(rules.REQUIRED_CHECKS[spec.name]), [check["name"] for check in result.document()["checks"]])

    def forged(self, table: str, checks: Sequence[tuple[str, bool, str]], **fields: int | None) -> rules.TableResult:
        result = rules.TableResult(table, 1, columns_source=3, columns_table=3, **fields)  # type: ignore[arg-type]
        result.checks.extend(rules.Check(*check) for check in checks)
        return result

    def test_a_document_refuses_a_counted_flag_a_check_of_another_table_a_wrong_pass_flag_and_a_check_out_of_order(self) -> None:
        columns = ("columns", True, rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0))
        first = (("staged file", True, "ok"), ("snapshot", True, "ok"), columns)
        cases = {
            "a counted flag": [*first, ("row count", False, "table_compared;equal=23")],
            "a counted flag marked passed": [*first, ("row count", True, "table_compared;equal=23")],
            "a flag the code does not give": [*first, ("row count", False, "table_compared;equal=1")],
            "a counted code of the earlier format": [*first, ("row count", True, "row_count;source=5;table=5")],
            "a check of the track list": [*first, ("row count", True, "table_compared;equal=1"), ("rows digest", False, "separator_in_field;rows=23")],
            "a check out of order": [("snapshot", True, "ok"), ("staged file", True, "ok")],
            "a check that is not required": [*first, ("extra", True, "ok")],
        }
        for label, checks in cases.items():
            with self.subTest(label), self.assertRaises(rules.RulesError):
                self.forged("clinical_data", checks).document()

    def test_a_document_of_the_track_list_refuses_a_count_outside_its_bounds_and_accepts_the_control(self) -> None:
        columns = ("columns", True, rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0))
        base = [("snapshot", True, "ok"), columns, ("row count", True, "row_count;source=7;table=7")]
        control = self.forged("track_list", [*base, ("rows digest", False, "separator_in_field;rows=3")], rows_source=7, rows_table=7)
        self.assertEqual("separator_in_field;rows=3", control.document()["checks"][3]["detail"])
        for label, checks in {
            "more rows than the table holds": [*base, ("rows digest", False, "separator_in_field;rows=23")],
            "a flag of 23": [*base, ("rows digest", False, "digest_compared;equal=23")],
            "a count that the table does not have": [*base[:2], ("row count", False, "row_count;source=7;table=9")],
        }.items():
            with self.subTest(label), self.assertRaises(rules.RulesError):
                self.forged("track_list", checks, rows_source=7, rows_table=7).document()

    def test_a_job_document_is_not_built_from_a_result_that_a_receiver_would_refuse(self) -> None:
        plan = the_plan()
        results = [
            self.forged(entry.name, [("staged file", True, "ok"), ("snapshot", True, "ok")] if entry.name != "track_list" else []) for entry in plan.tables
        ]
        results[0].checks.append(rules.Check("columns", True, "table_compared;equal=23"))
        with self.assertRaises(rules.RulesError):
            rules.job_document(plan, results)


class SavedEvidence(unittest.TestCase):
    """A result that an earlier run saved is read again by the form of the result that the receiver accepts, and any other form is refused."""

    def saved(self) -> dict[str, Any]:
        return ParserRefusesCounts().parse(ParserRefusesCounts().passing()).document()

    def parse(self, document: Any) -> rules.JobResult:
        return rules.parse_saved_result(document, the_plan())

    def test_control_the_document_that_the_controller_keeps_is_read_again_and_equal_to_itself(self) -> None:
        document = self.saved()
        self.assertEqual(document, self.parse(document).document())

    def test_a_saved_document_of_an_earlier_format_or_with_a_count_of_a_sealed_table_is_refused(self) -> None:
        def earlier(document: dict[str, Any]) -> None:
            document["format"] = "table_checks_4"

        def counted(document: dict[str, Any]) -> None:
            document["tables"][0].update(rows_source=2, rows_table=2)

        def legacy_code(document: dict[str, Any]) -> None:
            document["tables"][1]["checks"][4].update(detail="rows_differ;only_file=0;only_table=0")

        def counted_flag(document: dict[str, Any]) -> None:
            document["tables"][0]["checks"][3].update(detail="table_compared;equal=23")

        def row_count_code(document: dict[str, Any]) -> None:
            document["tables"][1]["checks"][3].update(detail="row_count;source=2;table=2")

        def top_flag(document: dict[str, Any]) -> None:
            document["passed"] = False

        def table_flag(document: dict[str, Any]) -> None:
            document["tables"][2]["passed"] = False

        def no_flag(document: dict[str, Any]) -> None:
            del document["tables"][0]["passed"]

        def extra_field(document: dict[str, Any]) -> None:
            document["rows"] = 2

        def another_run(document: dict[str, Any]) -> None:
            document["run_id"] = "b" * 32

        for change in (earlier, counted, legacy_code, counted_flag, row_count_code, top_flag, table_flag, no_flag, extra_field, another_run):
            document = self.saved()
            change(document)
            with self.subTest(change.__name__), self.assertRaises(rules.RulesError):
                self.parse(document)

    def test_a_saved_value_that_is_not_a_document_is_refused(self) -> None:
        values: list[Any] = [None, [], "table_checks_5", 5, {}]
        for value in values:
            with self.subTest(value=value), self.assertRaises(rules.RulesError):
                self.parse(value)


class ParserRefusesCounts(unittest.TestCase):
    def passing(self) -> dict[str, Any]:
        plan = the_plan()
        results = []
        for entry in plan.tables:
            sealed = rules.SPEC_BY_NAME[entry.name].sealed
            rows = None if sealed else entry.rows
            result = rules.TableResult(entry.name, entry.snapshot_id, rows_table=rows, rows_source=rows, columns_source=3, columns_table=3)
            for name in rules.REQUIRED_CHECKS[entry.name]:
                result.add(name, self.detail(name, sealed, entry.rows))
            results.append(result)
        document: dict[str, Any] = json.loads(json.dumps(rules.job_document(plan, results)))
        return document

    @staticmethod
    def detail(name: str, sealed: bool, rows: int) -> str:
        if name == "columns":
            return rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0)
        if name == "rows digest":
            return rules.detail("digest_compared", equal=1)
        if name == "row count":
            return rules.detail("table_compared", equal=1) if sealed else rules.detail("row_count", source=rows, table=rows)
        if name in ("key multiset", "row payload"):
            return rules.detail("table_compared", equal=1)
        return rules.detail("ok")

    def parse(self, document: dict[str, Any]) -> rules.JobResult:
        return rules.parse_result(json.dumps(document), the_plan())

    def test_control_the_sealed_result_passes_and_holds_no_row_count_of_a_sealed_table(self) -> None:
        document = self.passing()
        self.assertTrue(self.parse(document).passed)
        for index in (0, 1):
            self.assertEqual((None, None), (document["tables"][index]["rows_source"], document["tables"][index]["rows_table"]))
        self.assertEqual((7, 7), (document["tables"][2]["rows_source"], document["tables"][2]["rows_table"]))

    def test_a_result_with_row_counts_for_a_sealed_table_is_refused(self) -> None:
        for index in (0, 1):
            for fields in ({"rows_source": RESULT_ROWS}, {"rows_table": RESULT_ROWS}, {"rows_source": RESULT_ROWS, "rows_table": RESULT_ROWS}):
                document = self.passing()
                document["tables"][index].update(fields)
                with self.subTest(table=index, fields=fields), self.assertRaises(rules.RulesError):
                    self.parse(document)

    def test_a_result_with_a_counted_detail_for_a_sealed_table_is_refused(self) -> None:
        bad = (
            ("row count", "row_count;source=5;table=5"),
            ("row count", "row_count;source=5;table=6"),
            ("key multiset", "rows_differ;only_file=0;only_table=0"),
            ("row payload", "rows_differ;only_file=1;only_table=0"),
            ("key multiset", "table_compared;equal=2"),
            ("row payload", "table_compared;equal=4719238"),
            ("row payload", "table_compared;equal=1;only_file=0"),
            ("row count", "table_compared"),
        )
        for index in (0, 1):
            for name, detail in bad:
                document = self.passing()
                check = next(check for check in document["tables"][index]["checks"] if check["name"] == name)
                check.update(detail=detail, passed=detail.endswith("equal=1"))
                with self.subTest(table=index, check=name, detail=detail), self.assertRaises(rules.RulesError):
                    self.parse(document)

    def test_a_fixed_code_with_the_other_flag_is_refused_and_a_flag_that_matches_it_is_accepted(self) -> None:
        for equal, flag, accepted in ((1, True, True), (0, False, True), (1, False, False), (0, True, False)):
            document = self.passing()
            document["tables"][0]["checks"][3].update(detail=f"table_compared;equal={equal}", passed=flag)
            if accepted:
                if not flag:
                    del document["tables"][0]["checks"][4:]
                self.assertEqual(flag, self.parse(document).tables[0].checks[3].passed)
            else:
                with self.subTest(equal=equal, flag=flag), self.assertRaises(rules.RulesError):
                    self.parse(document)

    def test_the_track_list_keeps_its_row_counts_and_a_result_that_leaves_them_out_is_refused(self) -> None:
        document = self.passing()
        self.assertTrue(self.parse(document).passed)
        document["tables"][2].update(rows_source=None, rows_table=None)
        with self.assertRaises(rules.RulesError):
            self.parse(document)

    def test_a_result_of_the_earlier_format_is_refused(self) -> None:
        for earlier in ("table_checks_4", "table_checks_3"):
            document = self.passing()
            document["format"] = earlier
            with self.subTest(earlier), self.assertRaises(rules.RulesError):
                self.parse(document)
        self.assertEqual("table_checks_5", rules.RESULT_FORMAT)

    def test_the_document_that_the_controller_keeps_holds_no_row_count_of_a_sealed_table(self) -> None:
        kept = self.parse(self.passing()).document()
        for table in kept["tables"][:2]:
            self.assertEqual((None, None), (table["rows_source"], table["rows_table"]))
            self.assertEqual([], [path for path in numbers(table) if path[-1] not in ALLOWED_NUMBERS], table)


if __name__ == "__main__":
    unittest.main()
