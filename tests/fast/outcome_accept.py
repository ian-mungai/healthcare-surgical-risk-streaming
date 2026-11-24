"""The pass or fail rules that the end-to-end runs of the Bronze clinical, patient split and Silver cases jobs share, as plain functions on a job's output.

Nothing here imports Spark or Docker. The runners ``tests/e2e/bronze_clinical_lab.py``, ``tests/e2e/silver_patient_split.py`` and ``tests/e2e/silver_cases.py``
stay untracked, so each rule that decides from a job's output lives here, where ``tests/fast/test_outcome_acceptance.py`` tests it. The clinical table holds
the outcome columns, so a result holds no count, digest or field that says whether a table was written. A write is read from the base hash and the branch
hash, an equality with the loaded table from the checks that compare them, and a rebuild after the source moved from the snapshot IDs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SEALED_KEYS = ("rows", "source_rows", "keys_digest", "rows_digest")
EQUALITY_CHECKS = ("row count", "no value changed", "key unique", "key not null")
REMOVED_FIELDS = ("outcome", "written", "patients", "counts", "rows", "source_rows", "keys_digest", "rows_digest", "digest", "checksum")
REMOVED_TOP_LEVEL = ("cases",)


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def snapshot(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def sealed_table(table: Mapping[str, Any]) -> bool:
    """Whether a table of a Bronze result holds no count, no digest and no check detail: each check is its name and its flag only."""
    checks = table.get("checks")
    if any(key in table for key in SEALED_KEYS) or not isinstance(checks, list) or not checks:
        return False
    return all(isinstance(check, Mapping) and set(check) == {"name", "passed"} for check in checks)


def passed_checks(table: Mapping[str, Any]) -> set[str]:
    return {check["name"] for check in table.get("checks") or [] if isinstance(check, Mapping) and check.get("passed") is True}


def equals_loaded(table: Mapping[str, Any]) -> bool:
    """Whether a sealed table equals the loaded table: it passed, both snapshot IDs are named and the checks of the row count, the values, the key and its
    null check passed. The value check compares both tables as multisets, in both directions."""
    return (
        sealed_table(table)
        and table.get("passed") is True
        and snapshot(table.get("source_snapshot_id"))
        and snapshot(table.get("bronze_snapshot_id"))
        and set(EQUALITY_CHECKS) <= passed_checks(table)
    )


def same_source(first: Mapping[str, Any], again: Mapping[str, Any]) -> bool:
    """Whether a second write read the same loaded table as the first and equals it."""
    return equals_loaded(first) and equals_loaded(again) and first["source_snapshot_id"] == again["source_snapshot_id"]


def moved(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    """Whether a write after the loaded table moved read a new source snapshot, wrote a new Bronze snapshot and equals the moved table."""
    return (
        equals_loaded(before)
        and equals_loaded(after)
        and after["source_snapshot_id"] != before["source_snapshot_id"]
        and after["bronze_snapshot_id"] != before["bronze_snapshot_id"]
    )


def wrote(result: Mapping[str, Any]) -> bool:
    """Whether a build moved its branch off the base: it names both hashes and they differ."""
    base, branch = result.get("base_hash"), result.get("branch_hash")
    return named(base) and named(branch) and base != branch


def kept_at_base(result: Mapping[str, Any]) -> bool:
    """Whether a build wrote nothing: the branch is at the base hash."""
    base, branch = result.get("base_hash"), result.get("branch_hash")
    return named(base) and named(branch) and base == branch


def all_passed(result: Mapping[str, Any], tables: Sequence[str]) -> bool:
    """Whether the result has one pass flag for each of ``tables``, each true, and no other."""
    flags = result.get("passed")
    return isinstance(flags, Mapping) and set(flags) == set(tables) and all(flags[table] is True for table in tables)


def keys_of(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        return {str(key) for key in value} | {name for item in value.values() for name in keys_of(item)}
    if isinstance(value, list | tuple):
        return {name for item in value for name in keys_of(item)}
    return set()


def no_removed_fields(result: Mapping[str, Any]) -> bool:
    """Whether the result holds none of the fields that a held-out outcome could change: no outcome, no written flag, no count and no digest at any depth, and
    no count of all cases at the top (``cases`` is also the name of a table in the pass flags)."""
    return not keys_of(result) & set(REMOVED_FIELDS) and not set(result) & set(REMOVED_TOP_LEVEL)


def refusal_is_flags(found: Sequence[str], wanted: Sequence[str]) -> bool:
    """Whether the lines that a job wrote are exactly the wanted ones, in order. A wanted code is the line ``code: failed``; a wanted line that holds a colon is
    taken as it is (a count of tables)."""
    expected = [item if ": " in item else f"{item}: failed" for item in wanted]
    return list(found) == expected
