"""The checks of the label tables, on counts that the Spark job reads from them. No Spark runs here.

``input_problems`` judges the input before anything is written, in this order, each step alone when it fails: a case with more than one row (an ambiguous
owner), a patient split that fails the split job's own rule check (a set is trusted only after it), a case with no split row, no case at all, then no
development case. ``table_problems`` judges a table that was written (or that a branch holds): the measure of ``icu_days``, the intensive care labels and the
kidney injury labels.

The labels are outcomes and the test set is sealed. Every check about a label table returns a flag, ``code: failed``, and never a count: that holds for the
check that a label table has a row for every case too, which says that it does not and never by how many. A check returns its problems and never raises, so
that one run names every cause. A problem holds no case, patient, time, value, label or set.

The required column checks (a key that is null or twice, a null in a column, a label or a reason outside its domain, and the row count of a label table:
the Great Expectations row-count expectation with both bounds equal to the number of cases) are data for Great Expectations
(``column_plan``, run through the Bronze adapter by ``spark.silver.label_frames.quality_flags``), as the project standard asks for column-level checks. The
checks of one flag are one group: the flag fails when any check of the group fails. The exact comparison with the rebuilt table and the rules that join
several columns stay Spark code.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from features.icu_label import REASONS as ICU_REASONS
from features.kidney_label import REASONS as KIDNEY_REASONS
from features.label_values import LABEL_VALUES
from spark.bronze.checks import GUARD_KEY, GUARD_STRUCTURE, Check
from spark.silver.checks import FLAG, flagged
from spark.silver.label_measure import UNITS
from spark.split_table import CODES as SPLIT_CODES
from spark.split_table import SplitRow
from spark.split_table import table_problems as split_table_problems

MEASURE = "icu_days_measure"
ICU = "icu_labels"
KIDNEY = "kidney_injury_labels"
TABLES = (MEASURE, ICU, KIDNEY)
ROW_CHECKS = {ICU: "icu_not_every_case", KIDNEY: "kidney_not_every_case"}
TABLE_CHECKS = {
    MEASURE: ("icu_measure_columns", "icu_measure_differs"),
    ICU: ("icu_not_every_case", "icu_duplicate_key", "icu_label_value", "icu_eligibility", "icu_value_rule", "icu_differs"),
    KIDNEY: ("kidney_not_every_case", "kidney_duplicate_key", "kidney_label_value", "kidney_eligibility", "kidney_times", "kidney_rule", "kidney_differs"),
}
COLUMN_FLAGS = {
    MEASURE: ("icu_measure_columns",),
    ICU: ("icu_not_every_case", "icu_duplicate_key", "icu_label_value"),
    KIDNEY: ("kidney_not_every_case", "kidney_duplicate_key", "kidney_label_value"),
}
NOT_NULL, UNIQUE, IN_SET = "ExpectColumnValuesToNotBeNull", "ExpectCompoundColumnsToBeUnique", "ExpectColumnValuesToBeInSet"
ROW_COUNT = "ExpectTableRowCountToBeBetween"
INPUT_CHECKS = ("ambiguous_case_owner", "case_without_split", "input_empty", "development_empty")
CHECK_NAMES = (*INPUT_CHECKS, *(name for table in TABLES for name in TABLE_CHECKS[table]))
JOB_CODES = frozenset(
    {
        "schema",
        "table_missing",
        "readback_differs",
        "branch_moved",
        "catalog_hash",
        "source_table",
        "source_no_snapshot",
        "source_changed",
        "inherited_problem",
        "cases_schema_changed",
    }
)
CODES = frozenset(CHECK_NAMES) | JOB_CODES
INHERITED_CODES = SPLIT_CODES | JOB_CODES
INHERITED = "inherited_problem"


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
    """What a read of a table shows: for each check of the table, a number that is only tested for being positive. The table's rows are not a fact here: the
    row count of a label table is a check of Great Expectations, a flag like the others."""

    flags: Mapping[str, int]


def fixed_codes(problems: Sequence[str]) -> list[str]:
    """Each inherited message (of the split job's checks, of a schema check or of a source read) as one fixed code and a flag, ``code: failed``: the count that
    the message holds (``rule_mismatch: 1 patient``) is dropped, because the split is sealed and a count of its wrong patients is a fact about the test set. A
    code that neither this job nor the split job has becomes ``inherited_problem``. A code that two messages share is named once, in the order of its first."""
    named: list[str] = []
    for problem in problems:
        code = problem.split(":", 1)[0]
        line = f"{code if code in INHERITED_CODES else INHERITED}: {FLAG}"
        if line not in named:
            named.append(line)
    return named


def split_rule_problems(rows: Sequence[SplitRow]) -> list[str]:
    """What is wrong with the patient split table by itself, by the split job's own checks, as fixed codes (``fixed_codes``). The set of a patient is
    trusted only when this is empty."""
    return fixed_codes(split_table_problems(rows, pinned=False))


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
        return ["development_empty: 0 cases"]
    return []


def not_null(table: str, column: str, guard: str = GUARD_STRUCTURE) -> Check:
    return Check(table, guard, f"{column} not null", NOT_NULL, {"column": column})


def key_group(table: str, column: str) -> list[Check]:
    return [not_null(table, column, GUARD_KEY), Check(table, GUARD_KEY, "key unique", UNIQUE, {"column_list": [column]})]


def in_set(table: str, column: str, values: tuple[str, ...]) -> Check:
    return Check(table, GUARD_STRUCTURE, f"{column} in set", IN_SET, {"column": column, "value_set": list(values)})


def row_count(table: str, cases: int) -> Check:
    """The Great Expectations row-count expectation of a label table, with both bounds equal to the number of cases: a row for every case, no row more."""
    return Check(table, GUARD_STRUCTURE, "rows equal the cases", ROW_COUNT, {"min_value": cases, "max_value": cases})


def column_plan(table: str, cases: int) -> dict[str, list[Check]]:
    """The Great Expectations checks of one table, in groups: the name of the flag that fails when any check of its group fails, and the checks. ``cases`` is
    the number of cases of the input, which the row count of a label table must equal."""
    if table == MEASURE:
        columns = [not_null(table, name) for name in ("unit", "value")]
        return {"icu_measure_columns": [*key_group(table, "statistic"), *columns, in_set(table, "unit", UNITS)]}
    prefix, reasons = ("icu", ICU_REASONS) if table == ICU else ("kidney", KIDNEY_REASONS)
    values = [not_null(table, name) for name in ("label", "eligible", "eligibility_reason")]
    return {
        ROW_CHECKS[table]: [row_count(table, cases)],
        f"{prefix}_duplicate_key": key_group(table, "case_id"),
        f"{prefix}_label_value": [*values, in_set(table, "label", LABEL_VALUES), in_set(table, "eligibility_reason", reasons)],
    }


def table_problems(table: str, facts: TableFacts) -> list[str]:
    """The problems of one table: each check of the table by name, each a flag and never a count."""
    return flagged([(name, facts.flags[name]) for name in TABLE_CHECKS[table]])
