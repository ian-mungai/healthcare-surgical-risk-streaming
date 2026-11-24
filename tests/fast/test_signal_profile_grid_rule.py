"""The warning grid rule of the signal profile: its constants, its text, its row and the check of a stored row. No Spark runs here.

Failure modes covered here:

- A value of the rule is written in two places (the saved row and the grid of the unscored share), so a change of one leaves the other: the step of the
  unscored cutoffs comes from the same constant as the saved row.
- The rule text says something other than the constants (a constant that changes does not change the text, or the text holds a value that no constant holds).
- The row holds a case, a patient or a count, or lacks a constant, or holds another split snapshot than the one that was read.
- A stored row that differs from the planned row in any column is accepted, or the refusal holds a value, or a table with no row or two rows is accepted.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
import unittest
from unittest import mock

from spark.profile import grid_rule, unscored
from spark.profile.grid_rule import (
    FIRST_CUTOFF_OFFSET_SECONDS,
    GRID_ORIGIN,
    GRID_RULE_VERSION,
    GRID_STEP_SECONDS,
    LAST_CUTOFF_MARGIN_SECONDS,
    GridRule,
    RuleRead,
    planned_rule,
    rule_problems,
    rule_text,
)
from spark.profile.tables import GRID_RULE

SPLIT_SNAPSHOT = 7_412_000_000_000_000_001
BASE = planned_rule(SPLIT_SNAPSHOT)


def stored(rule: GridRule, *, rows: int = 1, dated: bool = True) -> RuleRead:
    return RuleRead(rows, rule, 55, dated)


class TheConstants(unittest.TestCase):
    def test_the_values_are_the_pending_ones_of_the_analysis_plan(self) -> None:
        self.assertEqual(("opstart", 60, 0, 300), (GRID_ORIGIN, GRID_STEP_SECONDS, FIRST_CUTOFF_OFFSET_SECONDS, LAST_CUTOFF_MARGIN_SECONDS))
        self.assertTrue(GRID_RULE_VERSION)

    def test_the_cutoff_step_of_the_unscored_share_is_the_step_of_the_rule(self) -> None:
        self.assertEqual(GRID_STEP_SECONDS, unscored.CUTOFF_STEP_SECONDS)
        spec = importlib.util.spec_from_file_location("unscored_with_another_step", unscored.__file__)
        if spec is None or spec.loader is None:
            self.fail("the unscored module has no file to load")
        copy = importlib.util.module_from_spec(spec)
        with mock.patch.object(grid_rule, "GRID_STEP_SECONDS", 120), mock.patch.dict(sys.modules, {spec.name: copy}):
            spec.loader.exec_module(copy)
        self.assertEqual(120, copy.CUTOFF_STEP_SECONDS)
        self.assertEqual([960, 1080], list(copy.cutoffs_of(0, 2399))[:2])
        self.assertEqual(GRID_STEP_SECONDS, unscored.CUTOFF_STEP_SECONDS)


class TheText(unittest.TestCase):
    def test_the_text_is_plain_words_with_every_constant_in_it(self) -> None:
        text = rule_text("opstart", 60, 0, 300)
        for part in ("opstart", "60 seconds", "opend", "300 seconds"):
            self.assertIn(part, text)

    def test_each_constant_changes_the_text(self) -> None:
        base = rule_text("opstart", 60, 0, 300)
        for changed in (("anesthesia_start", 60, 0, 300), ("opstart", 30, 0, 300), ("opstart", 60, 120, 300), ("opstart", 60, 0, 600)):
            with self.subTest(changed=changed):
                self.assertNotEqual(base, rule_text(*changed))

    def test_the_text_holds_no_value_that_no_constant_holds(self) -> None:
        text = rule_text("opstart", 60, 0, 300)
        digits = {part for part in text.replace(".", " ").split() if part.isdigit()}
        self.assertEqual({"60", "0", "300"}, digits)


class TheRow(unittest.TestCase):
    def test_the_row_is_the_constants_and_the_split_snapshot_and_nothing_else(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        self.assertEqual(
            (GRID_RULE_VERSION, rule_text(GRID_ORIGIN, GRID_STEP_SECONDS, FIRST_CUTOFF_OFFSET_SECONDS, LAST_CUTOFF_MARGIN_SECONDS)),
            (rule.rule_version, rule.rule_text),
        )
        self.assertEqual(
            (GRID_ORIGIN, GRID_STEP_SECONDS, FIRST_CUTOFF_OFFSET_SECONDS, LAST_CUTOFF_MARGIN_SECONDS, SPLIT_SNAPSHOT),
            (rule.grid_origin, rule.grid_step_seconds, rule.first_cutoff_offset_seconds, rule.last_cutoff_margin_seconds, rule.split_snapshot_id),
        )
        self.assertEqual(
            [
                "rule_version",
                "rule_text",
                "grid_origin",
                "grid_step_seconds",
                "first_cutoff_offset_seconds",
                "last_cutoff_margin_seconds",
                "split_snapshot_id",
            ],
            [field.name for field in dataclasses.fields(rule)],
        )

    def test_a_new_value_of_each_constant_changes_the_row_and_the_constants_only(self) -> None:
        for name, value, column in (
            ("GRID_RULE_VERSION", "next", "rule_version"),
            ("GRID_ORIGIN", "anesthesia_start", "grid_origin"),
            ("GRID_STEP_SECONDS", 30, "grid_step_seconds"),
            ("FIRST_CUTOFF_OFFSET_SECONDS", 120, "first_cutoff_offset_seconds"),
            ("LAST_CUTOFF_MARGIN_SECONDS", 600, "last_cutoff_margin_seconds"),
        ):
            with self.subTest(constant=name), mock.patch.object(grid_rule, name, value):
                rule = planned_rule(SPLIT_SNAPSHOT)
                self.assertEqual(value, getattr(rule, column))
                self.assertNotEqual(BASE, rule)
                if column != "rule_version":
                    self.assertIn(str(value), rule.rule_text)


class TheTable(unittest.TestCase):
    def test_the_columns_are_the_row_in_order_then_the_time_of_the_write_and_the_table_is_one_row_of_a_key(self) -> None:
        names = [name for name, _ in GRID_RULE.columns]
        self.assertEqual([field.name for field in dataclasses.fields(GridRule)] + ["written_at"], names)
        self.assertEqual(["string", "string", "string", "int", "int", "int", "bigint", "timestamp"], [kind for _, kind in GRID_RULE.columns])
        self.assertEqual(("silver", "warning_grid_rule"), GRID_RULE.table_key)
        self.assertEqual(("rule_version",), GRID_RULE.key)


class TheCheck(unittest.TestCase):
    def test_a_stored_row_equal_to_the_plan_has_no_problem(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        self.assertEqual([], rule_problems(stored(rule), rule))

    def test_a_row_that_differs_in_one_column_is_named_by_its_code_and_the_count_of_columns_and_holds_no_value(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        for column, changed in (
            ("rule_version", dataclasses.replace(rule, rule_version="other")),
            ("rule_text", dataclasses.replace(rule, rule_text="other")),
            ("grid_origin", dataclasses.replace(rule, grid_origin="other")),
            ("grid_step_seconds", dataclasses.replace(rule, grid_step_seconds=30)),
            ("first_cutoff_offset_seconds", dataclasses.replace(rule, first_cutoff_offset_seconds=5)),
            ("last_cutoff_margin_seconds", dataclasses.replace(rule, last_cutoff_margin_seconds=301)),
            ("split_snapshot_id", dataclasses.replace(rule, split_snapshot_id=SPLIT_SNAPSHOT + 1)),
        ):
            with self.subTest(column=column):
                self.assertEqual(["grid_rule_changed: 1 column"], rule_problems(stored(changed), rule))

    def test_two_differing_columns_are_counted_and_a_row_without_a_date_counts_as_one(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        both = dataclasses.replace(rule, grid_step_seconds=30, split_snapshot_id=1)
        self.assertEqual(["grid_rule_changed: 2 columns"], rule_problems(stored(both), rule))
        self.assertEqual(["grid_rule_changed: 1 column"], rule_problems(stored(rule, dated=False), rule))
        self.assertEqual(["grid_rule_changed: 3 columns"], rule_problems(stored(both, dated=False), rule))

    def test_a_table_with_no_row_or_two_rows_is_named_by_its_row_count(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        self.assertEqual(["grid_rule_rows: 0 rows"], rule_problems(RuleRead(0, None, 55, True), rule))
        self.assertEqual(["grid_rule_rows: 2 rows"], rule_problems(stored(rule, rows=2), rule))

    def test_no_problem_text_holds_a_value_of_the_rule(self) -> None:
        rule = planned_rule(SPLIT_SNAPSHOT)
        other = dataclasses.replace(rule, rule_text="SECRET-TEXT", grid_origin="SECRET-ORIGIN", split_snapshot_id=987654321)
        text = " ".join(rule_problems(stored(other), rule))
        for value in ("SECRET-TEXT", "SECRET-ORIGIN", "987654321", str(SPLIT_SNAPSHOT)):
            self.assertNotIn(value, text)


if __name__ == "__main__":
    unittest.main()
