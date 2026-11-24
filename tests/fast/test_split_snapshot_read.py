"""The three jobs that read the patient split table in Python (Silver hypotension, Silver labels and the signal profile) refuse a row that is not in form.

Failure modes covered here:

- A null (or a text, a boolean or a decimal) ``source_snapshot_id`` or a ``subjectid`` that is not text reaches ``int()`` or a rule check and ends the job with
  ``internal_error`` and exit 70 instead of a refusal.
- The refusal names the value, or the number of broken rows. The split table holds the test set's rows, so a count of its broken rows is a fact about it: the
  refusal is the one fixed code ``table_row: failed`` however many rows are broken.
- A broken row hides the checks of the rows that are in form (the rule check runs on the rows that are in form and adds its own codes).
- A split table that is in form is refused (the check is too wide).
"""

from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import MagicMock

from spark.jobs.signal_profile import EMPTY_FACTS, ProfileLake
from spark.jobs.silver_hypotension import HypotensionLake
from spark.jobs.silver_labels import LabelsLake
from spark.profile.checks import CODES as PROFILE_CODES
from spark.profile.run import SOURCE_NAMES, InputRead, input_problems
from spark.silver import hypotension_checks, label_checks
from spark.split_table import RULE_VERSION, build_assignments

SETTINGS = {"nessie_uri": "u", "warehouse": "w", "region": "r"}
CODE = "table_row: failed"
MARKER = "MARKER-SNAPSHOT-9917"
SNAPSHOT = 7


def split_rows(*changes: dict[str, Any]) -> list[dict[str, Any]]:
    """The rows of a split table of 60 patients made by the rule, as Spark rows are read (``row["name"]``), whose last rows hold the changes."""
    assigned = build_assignments([str(number) for number in range(1, 61)])
    rows = [{"subjectid": patient, "set": name, "rule_version": RULE_VERSION, "source_snapshot_id": SNAPSHOT} for patient, name in assigned.items()]
    for index, change in enumerate(changes):
        rows[-1 - index] = {**rows[-1 - index], **change}
    return rows


class Frame:
    """A table of a stand-in session: ``collect`` gives the rows, and ``select`` keeps the columns that are asked for."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def collect(self) -> list[dict[str, Any]]:
        return self.rows

    def select(self, *names: str) -> Frame:
        return Frame([{name: row[name] for name in names} for row in self.rows])


BROKEN = (
    ("a snapshot that is null", {"source_snapshot_id": None}),
    ("a snapshot that is text", {"source_snapshot_id": MARKER}),
    ("a snapshot that is a boolean", {"source_snapshot_id": True}),
    ("a snapshot that is a decimal", {"source_snapshot_id": 7.5}),
    ("a subject that is null", {"subjectid": None}),
    ("a subject that is a number", {"subjectid": 10}),
)


def hypotension_problems(rows: list[dict[str, Any]]) -> list[str]:
    return HypotensionLake(MagicMock(), MagicMock(), "b", SETTINGS).split_problems(Frame(rows))


def label_problems(rows: list[dict[str, Any]]) -> list[str]:
    return LabelsLake(MagicMock(), MagicMock(), "b", SETTINGS).split_problems(Frame(rows))


def profile_problems(rows: list[dict[str, Any]]) -> list[str]:
    """The problems that the profile finds in the split table when the clinical table holds exactly the patients of the rows that are in form."""
    clinical = [{"caseid": str(number), "subjectid": str(number)} for number in range(1, 61)]
    held = {"patient_split": (Frame(rows), "s"), "clinical_cases": (Frame(clinical), "c")}
    snapshots = tuple(SNAPSHOT if name == "clinical_cases" else 1 for name in SOURCE_NAMES)
    return ProfileLake(MagicMock(), MagicMock(), "b", SETTINGS).development_problems(held, snapshots)


JOBS = (("silver_hypotension", hypotension_problems), ("silver_labels", label_problems), ("signal_profile", profile_problems))


class AnOutOfFormRow(unittest.TestCase):
    def test_the_cases_cover_a_null_a_text_a_boolean_a_decimal_and_a_subject_that_is_not_text(self) -> None:
        self.assertEqual(6, len(BROKEN))
        self.assertEqual(3, len(JOBS))

    def test_a_split_table_in_form_gives_no_table_row_problem(self) -> None:
        for job, problems in JOBS:
            with self.subTest(job=job):
                self.assertNotIn(CODE, problems(split_rows()))

    def test_a_row_that_is_not_in_form_is_the_fixed_refusal_and_not_a_failure(self) -> None:
        for job, problems in JOBS:
            for what, change in BROKEN:
                with self.subTest(job=job, row=what):
                    self.assertIn(CODE, problems(split_rows(change)))

    def test_the_refusal_names_no_value_and_no_count(self) -> None:
        for job, problems in JOBS:
            for what, change in BROKEN:
                with self.subTest(job=job, row=what):
                    named = problems(split_rows(change))
                    self.assertNotIn(MARKER, "".join(named))
                    self.assertEqual(1, named.count(CODE))

    def test_one_broken_row_and_three_give_the_same_one_flag(self) -> None:
        three = ({"source_snapshot_id": None}, {"subjectid": None}, {"source_snapshot_id": MARKER})
        for job, problems in JOBS:
            with self.subTest(job=job):
                self.assertEqual(
                    [line for line in problems(split_rows({"source_snapshot_id": None})) if line == CODE],
                    [line for line in problems(split_rows(*three)) if line == CODE],
                )

    def test_the_rule_check_still_runs_on_the_rows_that_are_in_form(self) -> None:
        rows = split_rows()
        wrong = next(index for index, row in enumerate(rows) if row["set"] == "test" and index > 0)
        rows[wrong] = {**rows[wrong], "set": "training"}
        rows[0] = {**rows[0], "source_snapshot_id": None}
        for job, problems in JOBS:
            with self.subTest(job=job):
                named = problems(rows)
                self.assertIn(CODE, named)
                self.assertIn("rule_mismatch: failed", named)


class TheRefusalLeavesTheJobAsItsCode(unittest.TestCase):
    """The problem of the table is the whole of the job's refusal: it passes its own mapping to the result file as the fixed code."""

    def test_the_hypotension_input_check_gives_the_code_alone(self) -> None:
        facts = hypotension_checks.InputFacts(0, 0, 0, 0, 0, tuple(hypotension_problems(split_rows({"source_snapshot_id": None}))))
        self.assertEqual([CODE], hypotension_checks.input_problems(facts))

    def test_the_label_input_check_gives_the_code_alone(self) -> None:
        facts = label_checks.InputFacts(0, 0, 0, tuple(label_problems(split_rows({"source_snapshot_id": None}))), 0)
        self.assertEqual([CODE], label_checks.input_problems(facts))

    def test_the_profile_input_check_gives_the_code_alone(self) -> None:
        read = InputRead(EMPTY_FACTS, (), tuple(profile_problems(split_rows({"source_snapshot_id": None}))))
        self.assertEqual([CODE], [line for line in input_problems(read) if line == CODE])

    def test_the_code_is_one_the_profile_lists_as_its_own(self) -> None:
        self.assertIn("table_row", PROFILE_CODES)


if __name__ == "__main__":
    unittest.main()
