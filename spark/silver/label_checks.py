"""The checks of the label tables, on counts that the Spark job reads from them. No Spark runs here.

``input_problems`` judges the input before anything is written, in this order, each step alone when it fails: a case with more than one row (an ambiguous
owner), a patient split that fails the split job's own rule check (a set is trusted only after it), a case with no split row, no case at all, then no
development case. ``table_problems`` judges a table that was written (or that a branch holds): the measure of ``icu_days``, the intensive care labels and the
kidney injury labels.

The labels are outcomes and the test set is sealed. Every check about the content of a label table returns a flag, ``code: failed``, and never a count; the
only counts are the cases, which are an input (a label table holds one row for each case, those of the held-out patients included). A check returns its
problems and never raises, so that one run names every cause. A problem holds no case, patient, time, value, label or set.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from spark.silver.checks import counted
from spark.silver.hypotension_checks import FLAG, flagged
from spark.split_table import SplitRow, unit
from spark.split_table import table_problems as split_table_problems

MEASURE = "icu_days_measure"
ICU = "icu_labels"
KIDNEY = "kidney_injury_labels"
TABLES = (MEASURE, ICU, KIDNEY)
ROW_CHECKS = {ICU: "icu_not_every_case", KIDNEY: "kidney_not_every_case"}
TABLE_CHECKS = {
    MEASURE: ("icu_measure_differs",),
    ICU: ("icu_duplicate_key", "icu_label_value", "icu_eligibility", "icu_value_rule", "icu_differs"),
    KIDNEY: ("kidney_duplicate_key", "kidney_label_value", "kidney_eligibility", "kidney_times", "kidney_rule", "kidney_differs"),
}
INPUT_CHECKS = ("ambiguous_case_owner", "case_without_split", "input_empty", "development_empty")
CHECK_NAMES = (*INPUT_CHECKS, *ROW_CHECKS.values(), *(name for table in TABLES for name in TABLE_CHECKS[table]))
JOB_CODES = frozenset({"schema", "table_missing", "readback_differs", "branch_moved", "catalog_hash", "source_table", "source_no_snapshot", "source_changed"})
CODES = frozenset(CHECK_NAMES) | JOB_CODES


@dataclass(frozen=True, slots=True)
class InputFacts:
    """What a read of the input at one hash shows: the cases of ``silver.cases``, the patient split and the development cases.

    ``ambiguous_owner`` and ``cases_without_split`` are flags (0 or 1). ``split_problems`` are the problems of the split table by itself
    (``split_rule_problems``); when it is not empty the other facts were not read, because a set is not trusted yet.
    """

    cases: int
    ambiguous_owner: int
    cases_without_split: int
    split_problems: tuple[str, ...]
    development_cases: int


@dataclass(frozen=True, slots=True)
class TableFacts:
    """What a read of a table shows: its rows and, for each check of the table, a number that is only tested for being positive."""

    rows: int
    flags: Mapping[str, int]


def split_rule_problems(rows: Sequence[SplitRow]) -> list[str]:
    """What is wrong with the patient split table by itself, by the split job's own checks. The set of a patient is trusted only when this is empty."""
    return split_table_problems(rows, pinned=False)


def input_problems(facts: InputFacts) -> list[str]:
    if facts.ambiguous_owner:
        return [f"ambiguous_case_owner: {FLAG}"]
    if facts.split_problems:
        return list(facts.split_problems)
    if facts.cases_without_split:
        return [f"case_without_split: {FLAG}"]
    if facts.cases == 0:
        return ["input_empty: 0 rows"]
    if facts.development_cases == 0:
        return [f"development_empty: {unit(0, 'case')}"]
    return []


def table_problems(cases: int, table: str, facts: TableFacts) -> list[str]:
    """The problems of one table: its rows against the cases (a label table), then each check of the table by name, each a flag."""
    problems: list[str] = []
    if table in ROW_CHECKS:
        problems += counted([(ROW_CHECKS[table], abs(cases - facts.rows), "case")])
    return problems + flagged([(name, facts.flags[name]) for name in TABLE_CHECKS[table]])
