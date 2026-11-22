"""The rules of the table checks: columns, the empty value, the track list's digest and the table list. No Spark runs here.

Each rule has a passing control beside its faults, so a test that cannot fail is caught by its own control.
"""

from __future__ import annotations

import hashlib
import json
import unittest
from typing import Any

from scripts.sample.publisher import TRACK_LIST_COLUMNS
from spark import table_checks as rules

FILE_COLUMNS = ["caseid", "subjectid", "age"]
STRINGS = [("caseid", "string"), ("subjectid", "string"), ("age", "string")]
OWN = [("_airbyte_raw_id", "string"), ("_airbyte_extracted_at", "timestamp")]


class HeaderTest(unittest.TestCase):
    def test_the_byte_order_mark_is_not_part_of_the_first_name(self) -> None:
        self.assertEqual(["caseid", "dx"], rules.header_columns("﻿caseid,dx\r\n"))
        self.assertEqual(["caseid", "dx"], rules.header_columns("caseid,dx"))

    def test_a_quoted_name_and_an_empty_name_are_read_by_the_csv_rules(self) -> None:
        self.assertEqual(["a,b", "c"], rules.header_columns('"a,b",c'))
        with self.assertRaises(rules.RulesError):
            rules.header_columns("caseid,,age")
        with self.assertRaises(rules.RulesError):
            rules.header_columns("")


class ColumnTest(unittest.TestCase):
    def test_control_equal_columns_pass_and_the_connectors_own_columns_are_ignored(self) -> None:
        self.assertIsNone(rules.column_detail(FILE_COLUMNS, STRINGS))
        self.assertIsNone(rules.column_detail(FILE_COLUMNS, [*OWN[:1], *STRINGS, *OWN[1:]]))

    def test_the_connectors_columns_are_the_four_named_by_its_documentation(self) -> None:
        self.assertEqual(("_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"), rules.CONNECTOR_COLUMNS)
        every = [(name, "string") for name in rules.CONNECTOR_COLUMNS]
        self.assertIsNone(rules.column_detail(FILE_COLUMNS, [*every, *STRINGS]))

    def test_a_missing_column_fails_with_counts_only(self) -> None:
        self.assertEqual("columns_differ;missing=1;extra=0;repeated=0;order=0;not_string=0", rules.column_detail(FILE_COLUMNS, STRINGS[:2]))

    def test_an_extra_column_fails(self) -> None:
        self.assertEqual("columns_differ;missing=0;extra=1;repeated=0;order=0;not_string=0", rules.column_detail(FILE_COLUMNS, [*STRINGS, ("extra", "string")]))

    def test_an_unknown_column_that_starts_like_a_connector_column_is_an_extra_column(self) -> None:
        self.assertEqual(
            "columns_differ;missing=0;extra=1;repeated=0;order=0;not_string=0", rules.column_detail(FILE_COLUMNS, [*STRINGS, ("_airbyte_flag", "string")])
        )
        self.assertEqual(
            "columns_differ;missing=0;extra=1;repeated=0;order=0;not_string=0", rules.column_detail(FILE_COLUMNS, [*STRINGS, ("_airbyte_loaded_at", "string")])
        )

    def test_a_reordered_column_fails_with_the_same_names(self) -> None:
        self.assertEqual(
            "columns_differ;missing=0;extra=0;repeated=0;order=1;not_string=0", rules.column_detail(FILE_COLUMNS, [STRINGS[1], STRINGS[0], STRINGS[2]])
        )

    def test_a_column_that_is_not_a_string_fails(self) -> None:
        self.assertEqual(
            "columns_differ;missing=0;extra=0;repeated=0;order=0;not_string=1", rules.column_detail(FILE_COLUMNS, [("caseid", "bigint"), *STRINGS[1:]])
        )

    def test_a_repeated_column_fails(self) -> None:
        self.assertEqual(
            "columns_differ;missing=0;extra=0;repeated=1;order=0;not_string=0",
            rules.column_detail(FILE_COLUMNS, [*STRINGS, STRINGS[0]]),
        )

    def test_no_detail_holds_a_column_name(self) -> None:
        detail = rules.column_detail(["secret_marker_a", "b"], [("other_marker_b", "string")])
        self.assertNotIn("marker", detail or "")
        self.assertRegex(detail or "", rules.DETAIL)


class EmptyValueTest(unittest.TestCase):
    def test_the_expected_form_is_the_empty_string_unless_a_run_says_otherwise(self) -> None:
        self.assertEqual("empty", rules.EMPTY_FORM)
        self.assertEqual(("null", "empty"), rules.EMPTY_FORMS)

    def test_the_text_that_stands_for_the_other_form_is_one_that_no_file_holds(self) -> None:
        self.assertNotEqual("", rules.NOT_THE_EMPTY_VALUE)
        self.assertIn("\x01", rules.NOT_THE_EMPTY_VALUE)


class TrackListDigestTest(unittest.TestCase):
    def test_the_digest_is_the_sha256_of_the_sorted_rows_joined_by_newlines_with_none_after_the_last(self) -> None:
        rows = [["2", "SNUADC/ART", "b"], ["1", "Solar8000/HR", "a"], ["1", "Solar8000/ART_MBP", "c"]]
        lines = sorted(rules.row_line(row) for row in rows)
        expected = hashlib.sha256(b"1\tSolar8000/ART_MBP\tc\n1\tSolar8000/HR\ta\n2\tSNUADC/ART\tb").hexdigest()
        self.assertEqual(expected, rules.rows_digest(lines))

    def test_one_row_and_no_row(self) -> None:
        self.assertEqual(hashlib.sha256(b"1\ta\tb").hexdigest(), rules.rows_digest(["1\ta\tb"]))
        self.assertEqual(hashlib.sha256(b"").hexdigest(), rules.rows_digest([]))

    def test_the_order_and_a_changed_row_change_the_digest(self) -> None:
        self.assertNotEqual(rules.rows_digest(["a", "b"]), rules.rows_digest(["b", "a"]))
        self.assertNotEqual(rules.rows_digest(["a", "b"]), rules.rows_digest(["a", "c"]))
        self.assertNotEqual(rules.rows_digest(["a\tb"]), rules.rows_digest(["a", "b"]))


class RowLineTest(unittest.TestCase):
    def test_control_fields_without_a_tab_or_a_newline_are_joined_by_a_tab(self) -> None:
        self.assertEqual("1\tSNUADC/ART\t", rules.row_line(["1", "SNUADC/ART", ""]))

    def test_a_field_with_a_tab_or_a_newline_is_refused_because_the_joined_form_would_be_ambiguous(self) -> None:
        for field in ("a\tb", "a\nb", "a\rb", "\t"):
            with self.subTest(field=field), self.assertRaisesRegex(rules.RulesError, "tab or a newline"):
                rules.row_line(["1", field, "x"])


class SeparatorPatternTest(unittest.TestCase):
    def test_the_pattern_the_job_uses_matches_a_tab_a_newline_and_a_carriage_return_and_nothing_else(self) -> None:
        import re

        for value in ("a\tb", "a\nb", "a\rb"):
            self.assertIsNotNone(re.search(rules.SEPARATOR_PATTERN, value))
        for value in ("", "ab", "a b", "a,b", "t", "n", "\\t"):
            self.assertIsNone(re.search(rules.SEPARATOR_PATTERN, value))


class PinTest(unittest.TestCase):
    def test_control_the_tracked_pins_other_fields_are_left_alone(self) -> None:
        pin = {"url": "https://example.test/trks", "size_bytes": 5, "sha256": "b" * 64, "rows": 5, "rows_sha256": "a" * 64}
        self.assertEqual(rules.TrackPin(5, "a" * 64), rules.track_pin({"track_list": pin}))

    def test_control_a_pin_with_a_count_and_a_digest(self) -> None:
        self.assertEqual(rules.TrackPin(5, "a" * 64), rules.track_pin({"track_list": {"rows": 5, "rows_sha256": "a" * 64}}))

    def test_a_missing_or_malformed_pin_is_refused(self) -> None:
        digest = "a" * 64
        releases: list[dict[str, Any]] = [
            {},
            {"track_list": {}},
            {"track_list": {"rows": "5", "rows_sha256": digest}},
            {"track_list": {"rows": 5, "rows_sha256": "A" * 64}},
            {"track_list": {"rows": -1, "rows_sha256": digest}},
            {"track_list": {"rows": 0, "rows_sha256": digest}},
            {"track_list": {"rows": True, "rows_sha256": digest}},
        ]
        for release in releases:
            with self.assertRaises(rules.RulesError, msg=str(release)):
                rules.track_pin(release)


class TableListTest(unittest.TestCase):
    def test_the_tables_are_the_three_loaded_ones_with_the_publishers_grain(self) -> None:
        self.assertEqual(["clinical_data", "lab_data", "track_list"], [spec.name for spec in rules.TABLES])
        self.assertEqual(("caseid",), rules.SPEC_BY_NAME["clinical_data"].key)
        self.assertIsNone(rules.SPEC_BY_NAME["lab_data"].key)
        self.assertEqual({"clinical_data": "file", "lab_data": "file", "track_list": "pin"}, {spec.name: spec.source for spec in rules.TABLES})

    def test_the_track_list_columns_equal_the_ones_the_sample_script_reads(self) -> None:
        self.assertEqual(TRACK_LIST_COLUMNS, rules.SPEC_BY_NAME["track_list"].pinned_columns)


def plan_document() -> dict[str, Any]:
    return {
        "format": rules.PLAN_FORMAT,
        "run_id": "f" * 32,
        "branch": "load_run_one",
        "branch_hash": "b" * 64,
        "empty_form": "null",
        "tables": [
            {"name": "clinical_data", "snapshot_id": 11, "file": "clinical_data.csv", "file_sha256": "c" * 64, "file_size": 10},
            {"name": "lab_data", "snapshot_id": 12, "file": "lab_data.csv", "file_sha256": "d" * 64, "file_size": 20},
            {"name": "track_list", "snapshot_id": 13, "rows": 7, "rows_sha256": "e" * 64},
        ],
    }


class PlanTest(unittest.TestCase):
    def parse(self, document: dict[str, Any]) -> rules.Plan:
        return rules.parse_plan(json.dumps(document))

    def refused(self, change: Any) -> None:
        document = plan_document()
        change(document)
        with self.assertRaises(rules.RulesError, msg=json.dumps(document)[:200]):
            self.parse(document)

    def test_control_a_plan_with_the_three_tables_is_read(self) -> None:
        plan = self.parse(plan_document())
        self.assertEqual(("f" * 32, "load_run_one", "b" * 64, "null"), (plan.run_id, plan.branch, plan.branch_hash, plan.empty_form))
        self.assertEqual(["clinical_data", "lab_data", "track_list"], [table.name for table in plan.tables])
        self.assertEqual(
            (11, "clinical_data.csv", "c" * 64, 10), (plan.tables[0].snapshot_id, plan.tables[0].file, plan.tables[0].file_sha256, plan.tables[0].file_size)
        )
        self.assertEqual((13, 7, "e" * 64), (plan.tables[2].snapshot_id, plan.tables[2].rows, plan.tables[2].rows_sha256))

    def test_a_plan_that_is_not_json_or_not_an_object_is_refused(self) -> None:
        for text in ("", "[]", "{", '{"format": "table_check_plan_1", "format": "x"}'):
            with self.assertRaises(rules.RulesError, msg=text):
                rules.parse_plan(text)

    def test_every_table_of_the_list_must_be_named_once_in_order(self) -> None:
        self.refused(lambda document: document["tables"].pop())
        self.refused(lambda document: document["tables"].reverse())
        self.refused(lambda document: document["tables"].append(dict(document["tables"][0])))
        self.refused(lambda document: document["tables"][1].update(name="other_data"))

    def test_the_fields_are_exact_and_typed(self) -> None:
        self.refused(lambda document: document.update(format="table_check_plan_0"))
        self.refused(lambda document: document.update(extra=1))
        self.refused(lambda document: document.pop("run_id"))
        self.refused(lambda document: document.update(run_id="F" * 32))
        self.refused(lambda document: document.update(run_id="f" * 31))
        self.refused(lambda document: document.update(empty_form="blank"))
        self.refused(lambda document: document.update(branch_hash="B" * 64))
        self.refused(lambda document: document.update(branch=""))
        self.refused(lambda document: document["tables"][0].update(snapshot_id="11"))
        self.refused(lambda document: document["tables"][0].update(snapshot_id=True))
        self.refused(lambda document: document["tables"][0].pop("file_sha256"))
        self.refused(lambda document: document["tables"][2].update(file="x.csv"))
        self.refused(lambda document: document["tables"][2].pop("rows"))

    def test_a_file_must_be_a_plain_name_in_the_staging_folder(self) -> None:
        for name in ("../clinical_data.csv", "/etc/passwd", "a/b.csv", ".", ""):
            self.refused(lambda document, name=name: document["tables"][0].update(file=name))


def the_plan() -> rules.Plan:
    return rules.parse_plan(json.dumps(plan_document()))


def results_for(plan: rules.Plan, *, only: dict[str, int] | None = None) -> list[rules.TableResult]:
    """Every check of every table, each passed, as the job writes them. ``only`` keeps that many checks of a table."""
    results = []
    for entry in plan.tables:
        result = rules.TableResult(entry.name, entry.snapshot_id)
        for name in rules.REQUIRED_CHECKS[entry.name][: (only or {}).get(entry.name, 99)]:
            result.add(name, True, "ok")
        results.append(result)
    return results


class RequiredChecksTest(unittest.TestCase):
    def test_each_table_names_its_checks_in_the_order_the_job_runs_them(self) -> None:
        file_checks = ("staged file", "snapshot", "columns", "row count", "key multiset", "row payload")
        self.assertEqual(
            {"clinical_data": file_checks, "lab_data": file_checks, "track_list": ("snapshot", "columns", "row count", "rows digest")},
            dict(rules.REQUIRED_CHECKS),
        )
        self.assertEqual([spec.name for spec in rules.TABLES], list(rules.REQUIRED_CHECKS))


class DetailTest(unittest.TestCase):
    def test_a_detail_is_a_code_and_counts_and_nothing_else(self) -> None:
        for good in ("ok", "rows_differ;only_file=1;only_table=0", "row_count;file=6388;table=6388"):
            self.assertRegex(good, rules.DETAIL)
        for bad in ("", "OK", "caseid 5 differs", "ok;caseid=x", "rows_differ;only_file=-1", "ok\n", "ok;a=1;", "ok; a=1"):
            self.assertNotRegex(bad, rules.DETAIL)

    def test_a_check_refuses_a_detail_that_is_not_in_that_form(self) -> None:
        result = rules.TableResult("t", 1)
        with self.assertRaises(rules.RulesError):
            result.add("columns", False, "the value MARKER_17 differs")


class ResultTest(unittest.TestCase):
    def document(self, plan: rules.Plan, results: list[rules.TableResult]) -> dict[str, Any]:
        document: dict[str, Any] = json.loads(json.dumps(rules.job_document(plan, results)))
        return document

    def parse(self, document: dict[str, Any], plan: rules.Plan | None = None) -> rules.JobResult:
        return rules.parse_result(json.dumps(document), plan or the_plan())

    def refused(self, change: Any) -> None:
        plan = the_plan()
        document = self.document(plan, results_for(plan))
        change(document)
        with self.assertRaises(rules.RulesError, msg=json.dumps(document)[:300]):
            self.parse(document, plan)

    def test_control_a_complete_passing_result_passes(self) -> None:
        plan = the_plan()
        result = self.parse(self.document(plan, results_for(plan)), plan)
        self.assertTrue(result.passed)
        self.assertEqual(["clinical_data", "lab_data", "track_list"], [table.table for table in result.tables])

    def test_the_job_echoes_the_plan_it_ran(self) -> None:
        plan = the_plan()
        document = self.document(plan, results_for(plan))
        self.assertEqual(
            ("f" * 32, "load_run_one", "b" * 64, "null"), (document["run_id"], document["branch"], document["branch_hash"], document["empty_form"])
        )
        self.assertEqual([11, 12, 13], [table["snapshot_id"] for table in document["tables"]])

    def test_a_failed_check_fails_the_result_whatever_the_summary_flags_say(self) -> None:
        plan = the_plan()
        results = results_for(plan)
        results[1].checks[3] = rules.Check("row count", False, "row_count;file=2;table=3")
        document = self.document(plan, results)
        document["passed"] = True
        for table in document["tables"]:
            table["passed"] = True
        self.assertFalse(self.parse(document, plan).passed)

    def test_a_table_that_stopped_at_its_first_failure_is_a_failed_result_not_a_malformed_one(self) -> None:
        plan = the_plan()
        results = results_for(plan, only={"lab_data": 3})
        results[1].checks[2] = rules.Check("columns", False, "columns_differ;missing=1;extra=0;repeated=0;order=0;not_string=0")
        self.assertFalse(self.parse(self.document(plan, results), plan).passed)

    def test_a_table_that_stopped_without_a_failure_is_refused(self) -> None:
        plan = the_plan()
        self.refused_results(results_for(plan, only={"lab_data": 5}))

    def refused_results(self, results: list[rules.TableResult]) -> None:
        plan = the_plan()
        with self.assertRaises(rules.RulesError):
            self.parse(self.document(plan, results), plan)

    def test_a_check_missing_from_the_middle_an_extra_check_and_a_renamed_check_are_refused(self) -> None:
        self.refused(lambda document: document["tables"][0]["checks"].pop(3))
        self.refused(lambda document: document["tables"][2]["checks"].append({"name": "extra", "passed": True, "detail": "ok"}))
        self.refused(lambda document: document["tables"][1]["checks"][2].update(name="column list"))
        self.refused(lambda document: document["tables"][0]["checks"].reverse())

    def test_a_table_missing_an_extra_table_or_a_table_in_another_place_is_refused(self) -> None:
        self.refused(lambda document: document["tables"].pop())
        self.refused(lambda document: document["tables"].append(dict(document["tables"][0])))
        self.refused(lambda document: document["tables"].reverse())

    def test_a_result_that_does_not_repeat_the_plan_is_refused(self) -> None:
        self.refused(lambda document: document.update(run_id="a" * 32))
        self.refused(lambda document: document.update(branch="load_other"))
        self.refused(lambda document: document.update(branch_hash="c" * 64))
        self.refused(lambda document: document.update(empty_form="empty"))
        self.refused(lambda document: document["tables"][2].update(snapshot_id=99))
        self.refused(lambda document: document.update(format="table_checks_1"))

    def test_the_shape_of_every_field_is_checked(self) -> None:
        self.refused(lambda document: document.update(extra=1))
        self.refused(lambda document: document.pop("passed"))
        self.refused(lambda document: document["tables"][0].update(passed="yes"))
        self.refused(lambda document: document["tables"][0].update(rows_table="5"))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(passed=1))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(extra=1))
        self.refused(lambda document: document["tables"][0]["checks"][0].pop("detail"))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(detail="MARKER_17 differs"))
        self.refused(lambda document: document["tables"][0].update(snapshot_id=True))

    def test_text_that_is_not_json_or_names_a_field_twice_is_refused(self) -> None:
        for text in ("", "[]", "{", '{"format": "table_checks_2", "format": "x"}'):
            with self.assertRaises(rules.RulesError, msg=text):
                rules.parse_result(text, the_plan())


if __name__ == "__main__":
    unittest.main()
