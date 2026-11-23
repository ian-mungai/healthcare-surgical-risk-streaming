"""The column-level checks of the Bronze tables, as data, and the verdict that their results give. Pure code: no Spark, no Great Expectations.

Bronze changes no value, so no check judges a published value: not its range, not its noise, not whether a reading is a number. A check asks only whether the
table is what the decoder can have written. Two guards name the two kinds of fault:

- ``key``: a key column is never null and the key is unique, so every row can be told apart and found again.
- ``structure``: every other column is never null (the decoder makes every value from a number or a text, so a null is a broken row), a lineage hash has the
  form of a SHA-256, a reading's occurrence is not negative, a second of a waveform holds a sample, a track's kind, type and sample type are ones the file
  format defines and the tracks table is not empty.

``spark/bronze/quality.py`` runs each ``Check`` as a Great Expectations expectation on the table. The job stops when ``verdict`` does not pass. A check that has
no result counts as failed, so an executor that drops a check cannot let a table through.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from spark.bronze.tables import NUMERIC, STRINGS, TABLES, TRACKS, WAVEFORM, TableSpec

GUARD_KEY = "key"
GUARD_STRUCTURE = "structure"
GUARDS = (GUARD_KEY, GUARD_STRUCTURE)
NOT_FOUND = -1
SHA256_FORM = r"\A[0-9a-f]{64}\z"
KINDS = ("waveform", "numeric", "string")
TRACK_TYPES = (1, 2, 5)
SAMPLE_TYPES = ("float32", "float64", "int8", "uint8", "int16", "uint16", "int32", "uint32", "text")
ALLOWED_EXPECTATIONS = (
    "ExpectColumnValuesToNotBeNull",
    "ExpectCompoundColumnsToBeUnique",
    "ExpectColumnValuesToMatchRegex",
    "ExpectColumnValuesToBeBetween",
    "ExpectColumnValuesToBeInSet",
    "ExpectTableRowCountToBeBetween",
    "UnexpectedRowsExpectation",
)


class CheckError(RuntimeError):
    """A set of results that does not belong to the plan."""


@dataclass(frozen=True, slots=True)
class Check:
    """One check of one table: the guard it belongs to, a name that is unique within its table, the Great Expectations class and its arguments."""

    table: str
    guard: str
    name: str
    expectation: str
    kwargs: dict[str, Any] = field(compare=False, hash=False)

    @property
    def ident(self) -> tuple[str, str]:
        return (self.table, self.name)


@dataclass(frozen=True, slots=True)
class CheckResult:
    """What one check found: whether it passed and how many rows or values it found unexpected (a count, never a value)."""

    check: Check
    passed: bool
    unexpected: int


@dataclass(frozen=True, slots=True)
class Failure:
    table: str
    name: str
    guard: str
    unexpected: int


@dataclass(frozen=True, slots=True)
class Verdict:
    checks: tuple[CheckResult, ...]
    failed: tuple[Failure, ...]
    guards: dict[str, bool]

    @property
    def passed(self) -> bool:
        return not self.failed


def key_checks(table: str, key: Sequence[str]) -> list[Check]:
    """The key checks of a table named ``table``: a not-null check for each key column and one unique check over the whole key."""
    found = [Check(table, GUARD_KEY, f"{column} not null", "ExpectColumnValuesToNotBeNull", {"column": column}) for column in key]
    found.append(Check(table, GUARD_KEY, "key unique", "ExpectCompoundColumnsToBeUnique", {"column_list": list(key)}))
    return found


def checks_for(table: TableSpec) -> list[Check]:
    """The checks of one table, in a fixed order."""
    name = table.name
    key = key_checks(name, table.key)
    others = [
        Check(name, GUARD_STRUCTURE, f"{column} not null", "ExpectColumnValuesToNotBeNull", {"column": column})
        for column, _ in table.columns
        if column not in table.key
    ]
    found = [*key[:-1], *others, key[-1]]
    found.append(Check(name, GUARD_STRUCTURE, "source_sha256 form", "ExpectColumnValuesToMatchRegex", {"column": "source_sha256", "regex": SHA256_FORM}))
    if table in (NUMERIC, STRINGS):
        found.append(
            Check(name, GUARD_STRUCTURE, "source_occurrence at least 0", "ExpectColumnValuesToBeBetween", {"column": "source_occurrence", "min_value": 0})
        )
    if table is WAVEFORM:
        for check_name, query in (
            ("samples hold a sample", "SELECT * FROM {batch} WHERE size(samples) < 1"),
            ("samples have no null element", "SELECT * FROM {batch} WHERE exists(samples, sample -> sample IS NULL)"),
        ):
            found.append(Check(name, GUARD_STRUCTURE, check_name, "UnexpectedRowsExpectation", {"unexpected_rows_query": query}))
    if table is TRACKS:
        for column, values in (("kind", KINDS), ("track_type", TRACK_TYPES), ("sample_type", SAMPLE_TYPES)):
            found.append(Check(name, GUARD_STRUCTURE, f"{column} in set", "ExpectColumnValuesToBeInSet", {"column": column, "value_set": list(values)}))
        found.append(Check(name, GUARD_STRUCTURE, "rows at least 1", "ExpectTableRowCountToBeBetween", {"min_value": 1}))
    return found


def plan() -> list[Check]:
    """The checks of the four Bronze tables, table by table."""
    return [check for table in TABLES for check in checks_for(table)]


def verdict(checks: Sequence[Check], results: Sequence[CheckResult]) -> Verdict:
    """Whether the results of ``checks`` pass. A check with no result fails (its count is ``-1``).

    A result for a check that is not in ``checks``, or a second result for one, is refused.
    """
    declared = {check.ident: check for check in checks}
    seen: dict[tuple[str, str], CheckResult] = {}
    for result in results:
        ident = result.check.ident
        if declared.get(ident) != result.check:
            raise CheckError(f"a result names the check {ident[1]} of {ident[0]}, which the plan does not hold")
        if ident in seen:
            raise CheckError(f"the check {ident[1]} of {ident[0]} has two results")
        seen[ident] = result
    failed = []
    for check in checks:
        found = seen.get(check.ident)
        if found is None:
            failed.append(Failure(check.table, check.name, check.guard, NOT_FOUND))
        elif not found.passed:
            failed.append(Failure(check.table, check.name, check.guard, found.unexpected))
    ordered = tuple(seen[check.ident] for check in checks if check.ident in seen)
    return Verdict(ordered, tuple(failed), {guard: not any(f.guard == guard for f in failed) for guard in GUARDS})


def result_document(result: Verdict) -> dict[str, Any]:
    """The verdict as a JSON document of names and counts."""
    return {
        "passed": result.passed,
        "guards": dict(result.guards),
        "checks": [
            {"table": item.check.table, "name": item.check.name, "guard": item.check.guard, "passed": item.passed, "unexpected": item.unexpected}
            for item in result.checks
        ],
        "failed": [{"table": f.table, "name": f.name, "guard": f.guard, "unexpected": f.unexpected} for f in result.failed],
    }
