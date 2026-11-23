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
        equal = "columns_differ;missing=0;extra=0;repeated=0;order=0;not_string=0"
        self.assertEqual(equal, rules.column_detail(FILE_COLUMNS, STRINGS))
        self.assertEqual(equal, rules.column_detail(FILE_COLUMNS, [*OWN[:1], *STRINGS, *OWN[1:]]))
        self.assertTrue(rules.detail_holds(equal))

    def test_the_connectors_columns_are_the_four_named_by_its_documentation(self) -> None:
        self.assertEqual(("_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"), rules.CONNECTOR_COLUMNS)
        every = [(name, "string") for name in rules.CONNECTOR_COLUMNS]
        self.assertTrue(rules.detail_holds(rules.column_detail(FILE_COLUMNS, [*every, *STRINGS])))

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
        self.assertNotIn("marker", detail)
        self.assertFalse(rules.detail_holds(detail))


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


ROWS = 5
COLUMNS = 3


def passing_detail(name: str, rows: int = ROWS) -> str:
    """The detail a check holds when it passes, as the job writes it: counts always, for the checks that have counts."""
    return {
        "columns": rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0),
        "rows digest": rules.detail("digest_compared", equal=1),
        "row count": rules.detail("row_count", source=rows, table=rows),
        "key multiset": rules.detail("rows_differ", only_file=0, only_table=0),
        "row payload": rules.detail("rows_differ", only_file=0, only_table=0),
    }.get(name, rules.detail("ok"))


def results_for(plan: rules.Plan, *, only: dict[str, int] | None = None) -> list[rules.TableResult]:
    """Every check of every table, each passed, as the job writes them. ``only`` keeps that many checks of a table."""
    results = []
    for entry in plan.tables:
        rows = entry.rows if rules.SPEC_BY_NAME[entry.name].source == "pin" else ROWS
        result = rules.TableResult(entry.name, entry.snapshot_id, rows_table=rows, rows_source=rows, columns_source=COLUMNS, columns_table=COLUMNS)
        for name in rules.REQUIRED_CHECKS[entry.name][: (only or {}).get(entry.name, 99)]:
            result.add(name, passing_detail(name, rows))
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
    """A detail is one code of a closed table with exactly that code's integer fields. It is built and parsed from the same table: there is no free pattern."""

    def test_the_table_lists_every_code_with_its_named_integer_fields(self) -> None:
        self.assertEqual(
            {
                "ok": (),
                "bytes_differ": (),
                "table_missing": (),
                "snapshot_differs": (),
                "columns_differ": ("missing", "extra", "repeated", "order", "not_string"),
                "row_count": ("source", "table"),
                "rows_differ": ("only_file", "only_table"),
                "separator_in_field": ("rows",),
                "digest_compared": ("equal",),
            },
            dict(rules.DETAIL_CODES),
        )

    def test_a_detail_is_built_from_a_code_and_its_counts_and_parsed_back(self) -> None:
        self.assertEqual("ok", rules.detail("ok"))
        self.assertEqual("row_count;source=6388;table=6387", rules.detail("row_count", source=6388, table=6387))
        self.assertEqual(("rows_differ", {"only_file": 1, "only_table": 0}), rules.parse_detail("rows_differ;only_file=1;only_table=0"))
        for code, fields in rules.DETAIL_CODES.items():
            built = rules.detail(code, **dict.fromkeys(fields, 3))
            self.assertEqual((code, dict.fromkeys(fields, 3)), rules.parse_detail(built))

    def test_a_detail_that_is_not_in_the_table_is_refused_whatever_words_or_digits_it_holds(self) -> None:
        for bad in (
            "",
            "OK",
            "ok;a=1",
            "caseid 5 differs",
            "marker_9d2",
            "rows_differ_marker_9d2;only_file=1;only_table=0",
            "rows_differ;only_file=1",
            "rows_differ;only_table=0;only_file=1",
            "rows_differ;only_file=1;only_table=0;extra=1",
            "rows_differ;only_file=x;only_table=0",
            "rows_differ;only_file=-1;only_table=0",
            "rows_differ;only_file=01;only_table=0",
            "rows_differ;only_file=1;only_table=0\n",
            "rows_differ; only_file=1;only_table=0",
            "row_count;source=1;table=2;",
            "ok;",
        ):
            with self.subTest(bad=bad), self.assertRaises(rules.RulesError):
                rules.parse_detail(bad)

    def test_building_a_detail_refuses_an_unknown_code_the_wrong_fields_and_a_value_that_is_not_a_whole_number(self) -> None:
        for code, counts in (
            ("nope", {}),
            ("ok", {"extra": 1}),
            ("row_count", {"source": 1}),
            ("row_count", {"source": 1, "table": -1}),
            ("row_count", {"source": True, "table": 1}),
            ("row_count", {"source": "1", "table": 1}),
        ):
            with self.subTest(code=code, counts=counts), self.assertRaises(rules.RulesError):
                rules.detail(code, **counts)

    def test_a_check_refuses_a_detail_that_is_not_in_the_table_or_not_allowed_for_its_name(self) -> None:
        result = rules.TableResult("clinical_data", 1)
        for name, bad in (
            ("columns", "the value MARKER_17 differs"),
            ("columns", "marker_17"),
            ("columns", "rows_differ_marker;only_file=1;only_table=0"),
            ("columns", "ok"),
            ("row count", "ok"),
            ("snapshot", "row_count;source=1;table=1"),
            ("row payload", "digest_compared;equal=1"),
            ("rows digest", "ok"),
        ):
            with self.subTest(name=name, bad=bad), self.assertRaises(rules.RulesError):
                result.add(name, bad)
        self.assertTrue(result.add("snapshot", "ok"))

    def test_a_check_passes_exactly_when_the_rule_of_its_code_holds_for_its_counts(self) -> None:
        zeros = {"columns_differ": dict.fromkeys(rules.DETAIL_CODES["columns_differ"], 0)}
        self.assertTrue(rules.detail_holds(rules.detail("ok")))
        for code in ("bytes_differ", "table_missing", "snapshot_differs"):
            self.assertFalse(rules.detail_holds(rules.detail(code)), code)
        self.assertTrue(rules.detail_holds(rules.detail("digest_compared", equal=1)))
        self.assertFalse(rules.detail_holds(rules.detail("digest_compared", equal=0)))
        self.assertTrue(rules.detail_holds(rules.detail("row_count", source=7, table=7)))
        self.assertFalse(rules.detail_holds(rules.detail("row_count", source=7, table=8)))
        self.assertFalse(rules.detail_holds(rules.detail("row_count", source=8, table=7)))
        self.assertTrue(rules.detail_holds(rules.detail("rows_differ", only_file=0, only_table=0)))
        self.assertFalse(rules.detail_holds(rules.detail("rows_differ", only_file=1, only_table=0)))
        self.assertFalse(rules.detail_holds(rules.detail("rows_differ", only_file=0, only_table=1)))
        self.assertTrue(rules.detail_holds(rules.detail("columns_differ", **zeros["columns_differ"])))
        for name in rules.DETAIL_CODES["columns_differ"]:
            self.assertFalse(rules.detail_holds(rules.detail("columns_differ", **{**zeros["columns_differ"], name: 1})), name)
        for rows in (0, 1, 7):
            self.assertFalse(rules.detail_holds(rules.detail("separator_in_field", rows=rows)), rows)

    def test_every_code_has_exactly_one_rule_and_every_code_is_allowed_for_some_check(self) -> None:
        self.assertEqual(set(rules.DETAIL_CODES), set(rules.DETAIL_RULES))
        allowed = {code for codes in rules.CHECK_CODES.values() for code in codes}
        self.assertEqual(set(rules.DETAIL_CODES), allowed)
        self.assertEqual({name for names in rules.REQUIRED_CHECKS.values() for name in names}, set(rules.CHECK_CODES))


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

    def with_check(self, table: int, index: int, detail: str, *, passed: bool | None = None, **fields: int | None) -> dict[str, Any]:
        """A passing result with one check changed to ``detail`` (its flag derived unless ``passed`` is given) and table fields changed to ``fields``."""
        plan = the_plan()
        document = self.document(plan, results_for(plan))
        check = document["tables"][table]["checks"][index]
        check.update(detail=detail, passed=rules.detail_holds(detail) if passed is None else passed)
        document["tables"][table].update(fields)
        return document

    def stopped(self, document: dict[str, Any], table: int, index: int) -> dict[str, Any]:
        """The same result with the checks after ``index`` dropped, as the job drops them after a failure."""
        del document["tables"][table]["checks"][index + 1 :]
        return document

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

    def test_an_outcome_is_complete_only_with_every_required_check_and_a_result_only_with_every_table(self) -> None:
        required = rules.REQUIRED_CHECKS["lab_data"]
        passed = tuple(rules.Check(name, True, passing_detail(name)) for name in required)
        self.assertTrue(rules.TableOutcome("lab_data", passed).passed)
        self.assertFalse(rules.TableOutcome("lab_data", passed[:-1]).passed)
        failed = rules.Check(required[-1], False, rules.detail("rows_differ", only_file=1, only_table=0))
        self.assertFalse(rules.TableOutcome("lab_data", (*passed[:-1], failed)).passed)
        self.assertFalse(rules.TableOutcome("lab_data", ()).passed)
        every = tuple(
            rules.TableOutcome(name, tuple(rules.Check(check, True, passing_detail(check)) for check in names)) for name, names in rules.REQUIRED_CHECKS.items()
        )
        self.assertTrue(rules.JobResult("r", "b", "h", "empty", every).passed)
        self.assertFalse(rules.JobResult("r", "b", "h", "empty", every[:2]).passed)
        self.assertFalse(rules.JobResult("r", "b", "h", "empty", every[::-1]).passed)

    def test_a_failed_check_fails_the_result_whatever_the_summary_flags_say(self) -> None:
        plan = the_plan()
        results = results_for(plan, only={"lab_data": 4})
        results[1].rows_source, results[1].rows_table = 2, 3
        results[1].checks[3] = rules.Check("row count", False, rules.detail("row_count", source=2, table=3))
        document = self.document(plan, results)
        document["passed"] = True
        for table in document["tables"]:
            table["passed"] = True
        self.assertFalse(self.parse(document, plan).passed)

    def test_a_table_that_stopped_at_its_first_failure_is_a_failed_result_not_a_malformed_one(self) -> None:
        plan = the_plan()
        results = results_for(plan, only={"lab_data": 3})
        results[1].checks[2] = rules.Check("columns", False, rules.detail("columns_differ", missing=1, extra=0, repeated=0, order=0, not_string=0))
        self.assertFalse(self.parse(self.document(plan, results), plan).passed)

    def test_a_table_that_stopped_without_a_failure_is_refused(self) -> None:
        plan = the_plan()
        with self.assertRaises(rules.RulesError):
            self.parse(self.document(plan, results_for(plan, only={"lab_data": 5})), plan)

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
        self.refused(lambda document: document["tables"][0].update(columns_source="3"))
        self.refused(lambda document: document["tables"][0].update(columns_table=True))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(passed=1))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(extra=1))
        self.refused(lambda document: document["tables"][0]["checks"][0].pop("detail"))
        self.refused(lambda document: document["tables"][0]["checks"][0].update(detail="MARKER_17 differs"))
        self.refused(lambda document: document["tables"][0].update(snapshot_id=True))

    def test_the_reviewers_contradictions_are_refused(self) -> None:
        """Seven pinned rows and eight observed with a passing row count; a passing payload with differences; a row ID as a column count."""
        for label, document in (
            (
                "seven against eight, flagged passed",
                self.with_check(2, 2, rules.detail("row_count", source=7, table=8), passed=True, rows_source=7, rows_table=8),
            ),
            ("eight against eight with seven pinned", self.with_check(2, 2, rules.detail("row_count", source=8, table=8), rows_source=8, rows_table=8)),
            ("seven against seven with a flag failed", self.with_check(2, 2, rules.detail("row_count", source=7, table=7), passed=False)),
            ("payload passed with a difference", self.with_check(0, 5, rules.detail("rows_differ", only_file=1, only_table=0), passed=True)),
            ("key passed with a difference", self.with_check(0, 4, rules.detail("rows_differ", only_file=0, only_table=1), passed=True)),
            (
                "a row ID as a missing-column count",
                self.with_check(0, 2, rules.detail("columns_differ", missing=4719238, extra=0, repeated=0, order=0, not_string=0)),
            ),
        ):
            with self.subTest(label), self.assertRaises(rules.RulesError):
                self.parse(document)

    def test_the_reviewers_round_four_inputs_are_refused(self) -> None:
        clinical = rules.REQUIRED_CHECKS["clinical_data"]
        for label, document in (
            ("a separator code with a zero count, flagged passed", self.with_check(2, 3, rules.detail("separator_in_field", rows=0), passed=True)),
            ("a separator code with a zero count, flagged failed", self.with_check(2, 3, rules.detail("separator_in_field", rows=0))),
            ("a passing digest check with the code ok", self.with_check(2, 3, rules.detail("ok"), passed=True)),
            (
                "a passing column check with unequal totals",
                self.with_check(
                    0,
                    clinical.index("columns"),
                    rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0),
                    columns_source=3,
                    columns_table=4,
                ),
            ),
            (
                "a multiset result whose totals disagree with the row counts",
                self.with_check(0, clinical.index("row payload"), rules.detail("rows_differ", only_file=1, only_table=0)),
            ),
            (
                "a passing multiset result with rows that differ in number",
                self.with_check(0, clinical.index("row payload"), rules.detail("rows_differ", only_file=0, only_table=0), rows_source=5, rows_table=6),
            ),
        ):
            with self.subTest(label), self.assertRaises(rules.RulesError):
                self.parse(document)

    def test_the_three_count_rules_hold_over_a_generated_grid(self) -> None:
        """The multiset totals, the column totals and the digest evidence, checked over every small combination: accepted exactly when the rule allows it."""
        clinical = rules.REQUIRED_CHECKS["clinical_data"]
        for rows_source, rows_table in ((5, 5), (5, 6), (6, 5)):
            for only_file in range(rows_source + 2):
                for only_table in range(rows_table + 2):
                    allowed = only_file <= rows_source and only_table <= rows_table and rows_source - only_file == rows_table - only_table
                    detail = rules.detail("rows_differ", only_file=only_file, only_table=only_table)
                    row_count = clinical.index("row count")
                    document = self.with_check(
                        0, row_count, rules.detail("row_count", source=rows_source, table=rows_table), rows_source=rows_source, rows_table=rows_table
                    )
                    for name in ("key multiset", "row payload"):
                        document["tables"][0]["checks"][clinical.index(name)].update(detail=detail, passed=rules.detail_holds(detail))
                    with self.subTest(rows=(rows_source, rows_table), only=(only_file, only_table)):
                        if allowed:
                            self.parse(document)
                        else:
                            with self.assertRaises(rules.RulesError):
                                self.parse(document)
        zeros = rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0)
        for columns_source in range(1, 5):
            for columns_table in range(1, 5):
                document = self.with_check(0, clinical.index("columns"), zeros, columns_source=columns_source, columns_table=columns_table)
                with self.subTest(columns=(columns_source, columns_table)):
                    if columns_source == columns_table:
                        self.parse(document)
                    else:
                        with self.assertRaises(rules.RulesError):
                            self.parse(document)
        for equal in range(4):
            document = self.with_check(2, 3, rules.detail("digest_compared", equal=equal))
            with self.subTest(equal=equal):
                if equal <= 1:
                    self.assertEqual(equal == 1, self.parse(document).tables[2].checks[3].passed)
                else:
                    with self.assertRaises(rules.RulesError):
                        self.parse(document)
        for rows in range(0, 10):
            document = self.with_check(2, 3, rules.detail("separator_in_field", rows=rows))
            with self.subTest(separator=rows):
                if 1 <= rows <= 7:
                    self.assertFalse(self.parse(document).passed)
                else:
                    with self.assertRaises(rules.RulesError):
                        self.parse(document)

    def test_a_flag_that_disagrees_with_the_rule_of_its_code_is_refused_for_every_check_and_code(self) -> None:
        """Generated from the table: for each check and each code it may hold, the flag the rule gives is accepted and the opposite flag is refused."""
        examples = {
            "ok": [rules.detail("ok")],
            "bytes_differ": [rules.detail("bytes_differ")],
            "table_missing": [rules.detail("table_missing")],
            "snapshot_differs": [rules.detail("snapshot_differs")],
            "digest_compared": [rules.detail("digest_compared", equal=1), rules.detail("digest_compared", equal=0)],
            "rows_differ": [rules.detail("rows_differ", only_file=0, only_table=0), rules.detail("rows_differ", only_file=1, only_table=1)],
            "columns_differ": [
                rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0),
                rules.detail("columns_differ", missing=1, extra=0, repeated=0, order=0, not_string=0),
            ],
            "separator_in_field": [rules.detail("separator_in_field", rows=1)],
            "row_count": [rules.detail("row_count", source=ROWS, table=ROWS)],
        }
        table_of = {"staged file": 0, "snapshot": 0, "columns": 0, "row count": 0, "key multiset": 0, "row payload": 0, "rows digest": 2}
        for name, codes in rules.CHECK_CODES.items():
            table = table_of[name]
            index = rules.REQUIRED_CHECKS[("track_list" if table == 2 else "clinical_data")].index(name)
            for code in codes:
                for detail in examples[code]:
                    derived = rules.detail_holds(detail)
                    fields: dict[str, int | None] = {"rows_source": 7, "rows_table": 7} if table == 2 else {}
                    if code == "row_count" and table == 2:
                        detail = rules.detail("row_count", source=7, table=7)
                    with self.subTest(name=name, detail=detail):
                        self.parse(self.with_check(table, index, detail, passed=derived, **fields))
                        with self.assertRaises(rules.RulesError):
                            self.parse(self.with_check(table, index, detail, passed=not derived, **fields))

    def test_a_check_may_hold_only_the_codes_allowed_for_its_name(self) -> None:
        for name, allowed in rules.CHECK_CODES.items():
            table, index = (
                (2, rules.REQUIRED_CHECKS["track_list"].index(name)) if name == "rows digest" else (0, rules.REQUIRED_CHECKS["clinical_data"].index(name))
            )
            for code, fields in rules.DETAIL_CODES.items():
                if code in allowed:
                    continue
                detail = rules.detail(code, **dict.fromkeys(fields, 0))
                with self.subTest(name=name, code=code), self.assertRaises(rules.RulesError):
                    self.parse(self.with_check(table, index, detail))

    def test_every_count_has_its_bound_one_above_it_is_refused_and_the_bound_itself_is_accepted(self) -> None:
        """Rows: the differences are at most the row counts and a row count check repeats them. Columns: each count is at most the columns reported."""
        clinical = rules.REQUIRED_CHECKS["clinical_data"]
        for only, accepted in (((ROWS, ROWS), True), ((ROWS + 1, ROWS + 1), False), ((ROWS + 1, ROWS), False), ((ROWS, ROWS + 1), False)):
            detail = rules.detail("rows_differ", only_file=only[0], only_table=only[1])
            with self.subTest(only=only):
                if accepted:
                    self.assertFalse(self.parse(self.with_check(0, clinical.index("row payload"), detail)).passed)
                else:
                    with self.assertRaises(rules.RulesError):
                        self.parse(self.with_check(0, clinical.index("row payload"), detail))
        for field, bound in (("missing", COLUMNS), ("extra", COLUMNS), ("repeated", COLUMNS), ("not_string", COLUMNS), ("order", 1)):
            for offset, accepted in ((0, True), (1, False)):
                counts = {"missing": 0, "extra": 0, "repeated": 0, "order": 0, "not_string": 0, field: bound + offset}
                detail = rules.detail("columns_differ", **counts)
                with self.subTest(field=field, offset=offset):
                    if accepted:
                        self.assertFalse(self.parse(self.with_check(0, clinical.index("columns"), detail)).passed)
                    else:
                        with self.assertRaises(rules.RulesError):
                            self.parse(self.with_check(0, clinical.index("columns"), detail))
        for rows, accepted in ((7, True), (8, False)):
            detail = rules.detail("separator_in_field", rows=rows)
            with self.subTest(separator=rows):
                if accepted:
                    self.assertFalse(self.parse(self.with_check(2, 3, detail)).passed)
                else:
                    with self.assertRaises(rules.RulesError):
                        self.parse(self.with_check(2, 3, detail))

    def test_the_counts_of_a_row_count_check_are_the_table_row_counts_and_must_be_there(self) -> None:
        clinical = rules.REQUIRED_CHECKS["clinical_data"].index("row count")
        detail = rules.detail("row_count", source=5, table=6)
        self.assertFalse(self.parse(self.stopped(self.with_check(0, clinical, detail, rows_source=5, rows_table=6), 0, clinical)).passed)
        for fields in ({"rows_source": 5, "rows_table": 7}, {"rows_source": None, "rows_table": None}, {"rows_source": 4, "rows_table": 6}):
            with self.subTest(fields), self.assertRaises(rules.RulesError):
                self.parse(self.stopped(self.with_check(0, clinical, detail, **fields), 0, clinical))
        payload = rules.REQUIRED_CHECKS["clinical_data"].index("row payload")
        with self.assertRaises(rules.RulesError):
            self.parse(self.with_check(0, payload, rules.detail("rows_differ", only_file=0, only_table=0), rows_source=None, rows_table=None))

    def test_the_columns_reported_are_bounded_and_the_track_list_has_its_three(self) -> None:
        columns = rules.REQUIRED_CHECKS["clinical_data"].index("columns")
        detail = rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0)
        for fields in ({"columns_source": None}, {"columns_table": None}, {"columns_source": rules.MAX_COLUMN_COUNT + 1}, {"columns_source": -1}):
            with self.subTest(fields), self.assertRaises(rules.RulesError):
                self.parse(self.with_check(0, columns, detail, **fields))
        track = rules.REQUIRED_CHECKS["track_list"].index("columns")
        with self.assertRaises(rules.RulesError):
            self.parse(self.with_check(2, track, detail, columns_source=4))
        self.assertTrue(self.parse(self.with_check(2, track, detail, columns_source=3, columns_table=3)).passed)

    def test_the_pinned_row_count_of_the_track_list_is_the_one_the_result_names(self) -> None:
        plan = the_plan()
        results = results_for(plan)
        results[2].rows_source = 8
        with self.assertRaises(rules.RulesError):
            self.parse(self.document(plan, results), plan)

    def test_a_marker_that_fits_only_as_a_number_reaches_the_document_only_as_a_count_that_passed_its_bound(self) -> None:
        marker = 4719238
        plan = the_plan()
        for field in ("only_file", "only_table"):
            detail = rules.detail("rows_differ", **{"only_file": 0, "only_table": 0, field: marker})
            with self.subTest(field), self.assertRaises(rules.RulesError):
                self.parse(self.with_check(0, 5, detail))
        accepted = self.parse(self.with_check(0, 5, rules.detail("rows_differ", only_file=ROWS, only_table=ROWS)), plan).document()
        self.assertNotIn(str(marker), json.dumps(accepted))

    def test_the_canonical_document_holds_only_the_validated_fields_and_a_verdict_derived_here(self) -> None:
        plan = the_plan()
        document = self.document(plan, results_for(plan))
        document["passed"] = False
        for table in document["tables"]:
            table["passed"] = False
        result = self.parse(document, plan)
        canonical = result.document()
        self.assertTrue(canonical["passed"])
        self.assertEqual({"format", "run_id", "branch", "branch_hash", "empty_form", "passed", "tables"}, set(canonical))
        self.assertEqual(
            {"table", "snapshot_id", "rows_table", "rows_source", "columns_source", "columns_table", "passed", "checks"}, set(canonical["tables"][0])
        )
        self.assertEqual(self.document(plan, results_for(plan)), canonical)

    def test_text_that_is_not_json_or_names_a_field_twice_is_refused(self) -> None:
        for text in ("", "[]", "{", '{"format": "table_checks_2", "format": "x"}'):
            with self.assertRaises(rules.RulesError, msg=text):
                rules.parse_result(text, the_plan())


if __name__ == "__main__":
    unittest.main()
