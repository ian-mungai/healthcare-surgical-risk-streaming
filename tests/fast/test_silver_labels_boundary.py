"""The refusals that the label job inherits (the patient split's rule check, the schema check and the source reads) leave the job as fixed codes.

Failure modes covered here:

- A refusal that the split job words with a count (``rule_mismatch: 1 patient``) passes through, so one misassigned patient and two give different
  output and the output tells how many patients of the (sealed) split are wrong.
- A schema refusal passes through with the number of columns, or a source refusal with the number of tables.
- A message that is not a code of the split job or of this job passes through as it is.
"""

from __future__ import annotations

import dataclasses
import re
import unittest
from unittest.mock import MagicMock

from spark.jobs.silver_labels import CASES_NEEDED, LabelsLake
from spark.silver.checks import SilverSignalsError
from spark.silver.label_checks import INHERITED_CODES, InputFacts, fixed_codes, input_problems, split_rule_problems
from spark.split_table import RULE_VERSION, SplitRow, build_assignments

FLAG_MESSAGE = re.compile(r"[a-z_]+: failed")
INPUT = InputFacts(cases=6388, ambiguous_owner=0, cases_without_split=0, split_problems=(), development_cases=5000)
OTHER_SET = {"training": "test", "validation": "training", "test": "validation"}


def split_rows(wrong: int) -> list[SplitRow]:
    """The rows of a patient split of 60 patients by the rule, with the set of the first ``wrong`` patients changed to another set."""
    assigned = build_assignments([str(number) for number in range(1, 61)])
    rows = [SplitRow(subjectid, name, RULE_VERSION, 7) for subjectid, name in assigned.items()]
    return [SplitRow(row.subjectid, OTHER_SET[row.set], row.rule_version, row.source_snapshot_id) if index < wrong else row for index, row in enumerate(rows)]


class SplitRefusals(unittest.TestCase):
    def test_one_misassigned_patient_and_two_give_the_same_output(self) -> None:
        one, two = split_rule_problems(split_rows(1)), split_rule_problems(split_rows(2))
        self.assertEqual(["rule_mismatch: failed"], one)
        self.assertEqual(one, two)
        self.assertEqual(
            input_problems(dataclasses.replace(INPUT, split_problems=tuple(one))),
            input_problems(dataclasses.replace(INPUT, split_problems=tuple(two))),
        )

    def test_every_message_of_the_split_rule_check_is_a_flag_of_its_own_code(self) -> None:
        rows = [
            SplitRow("1", "other", "old", 1),
            SplitRow("1", "test", "old", 2),
            SplitRow("x", "training", RULE_VERSION, 3),
            SplitRow("9", "training", "old", 3),
        ]
        problems = split_rule_problems(rows)
        self.assertTrue(problems)
        for problem in problems:
            self.assertRegex(problem, FLAG_MESSAGE)
            self.assertIn(problem.split(":")[0], INHERITED_CODES)
        self.assertEqual(["empty_table: failed"], split_rule_problems([]))

    def test_a_message_that_is_not_a_known_code_is_one_fixed_code(self) -> None:
        self.assertEqual(["inherited_problem: failed"], fixed_codes(["a path /x/y with 3 values: 5 rows"]))
        self.assertEqual(["inherited_problem: failed"], fixed_codes(["two_sets2: 1 patient"]))

    def test_the_same_code_twice_is_one_line_and_the_order_is_kept(self) -> None:
        self.assertEqual(
            ["two_sets: failed", "rule_mismatch: failed"], fixed_codes(["two_sets: 3 patients", "rule_mismatch: 1 patient", "two_sets: 1 patient"])
        )


def lake() -> LabelsLake:
    job = LabelsLake(MagicMock(), MagicMock(), "branch", {"nessie_uri": "u", "warehouse": "w", "region": "r"})
    job.catalog = MagicMock(return_value="pin")  # type: ignore[method-assign]
    return job


def field(name: str, kind: str) -> MagicMock:
    item = MagicMock()
    item.name = name
    item.dataType.simpleString.return_value = kind
    return item


def refusal(job: LabelsLake, method: str) -> list[str]:
    try:
        getattr(job, method)("h")
    except SilverSignalsError as error:
        return error.problems
    raise AssertionError(f"{method} did not refuse")


class JobRefusals(unittest.TestCase):
    def test_a_schema_refusal_names_no_column_count(self) -> None:
        for columns in (1, 4):
            for method in ("cases", "lab", "split"):
                with self.subTest(columns=columns, method=method):
                    job = lake()
                    frame = MagicMock()
                    frame.schema.fields = [MagicMock() for _ in range(columns)]
                    job.source_frame = MagicMock(return_value=(frame, "t"))  # type: ignore[method-assign]
                    self.assertEqual(["cases_schema_changed: failed" if method == "cases" else "schema: failed"], refusal(job, method))

    def test_an_inherited_cases_table_with_icu_days_as_a_whole_number_is_named_as_a_schema_change(self) -> None:
        for kind in ("bigint", "int", "string"):
            with self.subTest(icu_days=kind):
                job = lake()
                frame = MagicMock()
                frame.schema.fields = [field(name, kind if name == "icu_days" else column_type) for name, column_type in CASES_NEEDED]
                job.source_frame = MagicMock(return_value=(frame, "t"))  # type: ignore[method-assign]
                self.assertEqual(["cases_schema_changed: failed"], refusal(job, "cases"))

    def test_a_cases_table_with_the_double_icu_days_is_read(self) -> None:
        job = lake()
        frame = MagicMock()
        frame.schema.fields = [field(name, column_type) for name, column_type in CASES_NEEDED]
        job.source_frame = MagicMock(return_value=(frame, "t"))  # type: ignore[method-assign]
        self.assertEqual((frame, "t"), job.cases("h"))

    def test_a_missing_source_table_is_a_flag(self) -> None:
        job = lake()
        job.session.catalog.tableExists.return_value = False
        try:
            job.source_frame("h", "silver.cases")
        except SilverSignalsError as error:
            self.assertEqual(["source_table: failed"], error.problems)
        else:
            self.fail("no refusal")

    def test_a_source_without_a_snapshot_names_no_number_of_tables(self) -> None:
        for missing in (1, 2):
            with self.subTest(missing=missing):
                job = lake()
                frame = MagicMock()
                job.cases = MagicMock(return_value=(frame, "c"))  # type: ignore[method-assign]
                job.lab = MagicMock(return_value=(frame, "l"))  # type: ignore[method-assign]
                job.split = MagicMock(return_value=(frame, "s"))  # type: ignore[method-assign]
                job.snapshot_id = MagicMock(side_effect=[None, None if missing == 2 else 5, 6])  # type: ignore[method-assign]
                self.assertEqual(["source_no_snapshot: failed"], refusal(job, "read_input"))


if __name__ == "__main__":
    unittest.main()
