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
    def test_control_equal_columns_pass_and_airbyte_columns_are_ignored(self) -> None:
        self.assertEqual([], rules.column_problems(FILE_COLUMNS, STRINGS))
        self.assertEqual([], rules.column_problems(FILE_COLUMNS, [*OWN[:1], *STRINGS, *OWN[1:]]))

    def test_a_missing_column_fails_and_names_it(self) -> None:
        problems = rules.column_problems(FILE_COLUMNS, STRINGS[:2])
        self.assertEqual(1, len(problems))
        self.assertIn("missing", problems[0])
        self.assertIn("age", problems[0])

    def test_an_extra_column_fails(self) -> None:
        problems = rules.column_problems(FILE_COLUMNS, [*STRINGS, ("extra", "string")])
        self.assertTrue(problems and "not in the file" in problems[0])

    def test_a_reordered_column_fails_with_the_same_names(self) -> None:
        problems = rules.column_problems(FILE_COLUMNS, [STRINGS[1], STRINGS[0], STRINGS[2]])
        self.assertEqual(["the table holds the file's columns in another order"], problems)

    def test_a_column_that_is_not_a_string_fails(self) -> None:
        problems = rules.column_problems(FILE_COLUMNS, [("caseid", "bigint"), *STRINGS[1:]])
        self.assertTrue(problems and "not string columns" in problems[0])

    def test_a_column_named_like_an_airbyte_column_is_not_a_source_column(self) -> None:
        problems = rules.column_problems([*FILE_COLUMNS, "_airbyte_flag"], STRINGS)
        self.assertEqual(1, len(problems))
        self.assertIn("missing", problems[0])


class EmptyValueTest(unittest.TestCase):
    def test_control_the_accepted_form_reads_as_the_published_empty_value(self) -> None:
        self.assertEqual("", rules.table_text(None, "null"))
        self.assertEqual("", rules.table_text("", "empty"))

    def test_the_other_form_is_a_different_value_that_no_file_holds(self) -> None:
        self.assertEqual(rules.NOT_THE_EMPTY_VALUE, rules.table_text("", "null"))
        self.assertEqual(rules.NOT_THE_EMPTY_VALUE, rules.table_text(None, "empty"))
        self.assertNotEqual("", rules.NOT_THE_EMPTY_VALUE)

    def test_a_value_is_never_changed(self) -> None:
        for value in ("07", "NA", " ", "x ", "0"):
            for form in rules.EMPTY_FORMS:
                self.assertEqual(value, rules.table_text(value, form))

    def test_an_unknown_form_is_refused(self) -> None:
        with self.assertRaises(rules.RulesError):
            rules.table_text("", "blank")


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


class PinTest(unittest.TestCase):
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
        self.assertEqual(("load_run_one", "b" * 64, "null"), (plan.branch, plan.branch_hash, plan.empty_form))
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


class ResultTest(unittest.TestCase):
    def test_a_table_passes_only_when_it_has_checks_and_all_pass(self) -> None:
        empty = rules.TableResult("t", 1)
        self.assertFalse(empty.passed)
        empty.add("columns", True, "ok")
        self.assertTrue(empty.passed)
        empty.add("rows", False, "differs")
        self.assertFalse(empty.passed)

    def test_the_job_passes_only_when_every_table_of_the_list_is_there_and_passes(self) -> None:
        def passing(name: str) -> rules.TableResult:
            result = rules.TableResult(name, 1)
            result.add("columns", True, "ok")
            return result

        every = [passing(spec.name) for spec in rules.TABLES]
        self.assertTrue(rules.job_document("b", "h", "null", every)["passed"])
        self.assertFalse(rules.job_document("b", "h", "null", every[:2])["passed"])
        every[1].add("rows", False, "differs")
        self.assertFalse(rules.job_document("b", "h", "null", every)["passed"])


if __name__ == "__main__":
    unittest.main()
