"""The adapter that runs the label tables' column checks in Great Expectations and turns their results into flags. Great Expectations does not start here: a
stand-in runner answers each check, as the Bronze adapter tests do.

Failure modes covered here:

- A check of the plan is not run, or the results of a run are read for another table's checks, so a null key, a repeated key or a label outside its domain
  passes.
- A failed check sets another table flag than its own, or two flags, so the name of a defect in a message is wrong.
- A flag holds the count that the library found: the labels are outcomes, so a flag is 0 or 1 and the count never leaves the adapter.
- A check that the runner leaves out counts as passed, or a result for a check that the plan does not hold is accepted.
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from typing import Any

from spark.bronze.checks import Check, CheckError, CheckResult
from spark.silver.label_checks import COLUMN_FLAGS, ICU, KIDNEY, MEASURE, column_plan
from spark.silver.label_frames import quality_flags


class Runner:
    """A stand-in for ``spark.bronze.quality.column_checks``: it records its calls and fails the checks that it is told to, with a count of 4000."""

    def __init__(self, failing: set[str] | None = None, drop: set[str] | None = None, extra: Check | None = None) -> None:
        self.failing, self.drop, self.extra = failing or set(), drop or set(), extra
        self.calls: list[tuple[Any, list[str]]] = []

    def __call__(self, frame: Any, checks: Sequence[Check]) -> list[CheckResult]:
        self.calls.append((frame, [check.name for check in checks]))
        results = [
            CheckResult(check, check.name not in self.failing, 4000 if check.name in self.failing else 0) for check in checks if check.name not in self.drop
        ]
        if self.extra is not None:
            results.append(CheckResult(self.extra, True, 0))
        return results


def names(table: str, flag: str) -> list[str]:
    return [check.name for check in column_plan(table)[flag]]


class Flags(unittest.TestCase):
    def test_every_check_of_the_table_runs_once_on_the_stored_frame_and_a_clean_run_has_every_flag_at_zero(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            with self.subTest(table=table):
                runner, frame = Runner(), object()
                flags = quality_flags(frame, table, runner)
                self.assertEqual({flag: 0 for flag in COLUMN_FLAGS[table]}, flags)
                expected = [name for flag in COLUMN_FLAGS[table] for name in names(table, flag)]
                self.assertEqual([(frame, expected)], runner.calls)

    def test_a_failed_check_sets_its_own_flag_only_and_the_flag_is_one_never_the_count(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            for flag in COLUMN_FLAGS[table]:
                for name in names(table, flag):
                    with self.subTest(table=table, flag=flag, check=name):
                        flags = quality_flags(object(), table, Runner({name}))
                        self.assertEqual({other: int(other == flag) for other in COLUMN_FLAGS[table]}, flags)

    def test_a_check_that_the_runner_leaves_out_is_a_failure_of_its_flag(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            for flag in COLUMN_FLAGS[table]:
                for name in names(table, flag):
                    with self.subTest(table=table, check=name):
                        flags = quality_flags(object(), table, Runner(drop={name}))
                        self.assertEqual({other: int(other == flag) for other in COLUMN_FLAGS[table]}, flags)

    def test_a_result_for_a_check_that_the_plan_does_not_hold_is_refused(self) -> None:
        other = Check("elsewhere", "key", "stray", "ExpectColumnValuesToNotBeNull", {"column": "x"})
        with self.assertRaises(CheckError):
            quality_flags(object(), ICU, Runner(extra=other))

    def test_the_two_label_tables_have_their_own_checks_on_the_same_columns(self) -> None:
        self.assertEqual({"icu_labels"}, {check.table for checks in column_plan(ICU).values() for check in checks})
        self.assertEqual({"kidney_injury_labels"}, {check.table for checks in column_plan(KIDNEY).values() for check in checks})
        self.assertEqual({"icu_days_measure"}, {check.table for checks in column_plan(MEASURE).values() for check in checks})


if __name__ == "__main__":
    unittest.main()
