"""Run the reconciliation's own Spark SQL on tiny tables in SQLite, so that a test can look at what a query returns and not only at its text.

The translation is small and exact: three-part table names become one name, ``<=>`` becomes ``IS``, a semi join and an anti join with the development
cases become a join and a left join with a ``WHERE``, ``RLIKE`` and ``TRY_CAST`` become a regular-expression function and a cast (the pattern of the typing
rule guards the cast), and the two ``explode`` forms (the ledger of a reading and the copies of a laboratory row) become a join to ``json_each`` and to a
table of numbers. A construct that is not one of these is not translated, so a query that gains one fails loudly here and the translation is extended on
purpose.

``World`` holds two cohorts. Case 1 of patient 10 is in the training set, case 2 of patient 20 is in the test set. Each table has the rows of a clean,
reconciled build for both. A test plants a defect in a cohort and reads what the query counts.
build for both. A test plants a defect in a cohort and reads what the query counts.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Mapping
from typing import Any

from spark.reconcile.checks import SOURCE_TABLES

DEVELOPMENT_CASE, HELD_OUT_CASE = 1, 2
DEVELOPMENT_PATIENT, HELD_OUT_PATIENT = "10", "20"
NUMBERS = 8

World = dict[str, list[dict[str, Any]]]


def regexp(pattern: str, value: Any) -> int:
    """``value RLIKE pattern`` as Spark reads the pattern: the text ``\\\\z`` of the literal is the end of the text."""
    if value is None:
        return 0
    return 1 if re.search(pattern.replace("\\\\z", "\\Z"), str(value)) else 0


STEPS: tuple[tuple[re.Pattern[str], Callable[[re.Match[str]], str]], ...] = (
    (re.compile(r"\blake\.(bronze|silver)\.(\w+)"), lambda m: f"{m[1]}_{m[2]}"),
    (re.compile(r"<=>"), lambda m: " IS "),
    (re.compile(r"\bRLIKE\b"), lambda m: "REGEXP"),
    (re.compile(r"TRY_CAST\((.+?) AS BIGINT\)"), lambda m: f"CAST({m[1]} AS INTEGER)"),
    (re.compile(r"TRY_CAST\((.+?) AS DOUBLE\)"), lambda m: f"CAST({m[1]} AS REAL)"),
    (
        re.compile(r"LEFT SEMI JOIN dev ON (\S+ = dev\.`case_id`)"),
        lambda m: " ".join(("JOIN (SELECT DISTINCT `case_id` FROM dev) dev ON", m[1])),
    ),
    (
        re.compile(r"LEFT ANTI JOIN dev ON (\S+ = dev\.`case_id`)"),
        lambda m: " ".join(("LEFT JOIN (SELECT DISTINCT `case_id` FROM dev) dev ON", m[1], "WHERE dev.`case_id` IS NULL")),
    ),
    (
        re.compile(r"explode\(`duplicate_source_occurrences`\) AS `duplicate` FROM (\w+)"),
        lambda m: f"je.value AS `duplicate` FROM {m[1]}, json_each({m[1]}.`duplicate_source_occurrences`) AS je",
    ),
    (
        re.compile(r"explode\(sequence\(1, GREATEST\((`\w+`), 1\)\)\) AS `copy` FROM (\w+)"),
        lambda m: f"seq.value AS `copy` FROM {m[2]} JOIN seq ON seq.value <= MAX(COALESCE({m[1]}, 1), 1)",
    ),
)


def translate(query: str) -> str:
    for pattern, replace in STEPS:
        query = pattern.sub(replace, query)
    return query


def table_name(table: str) -> str:
    return table.replace(".", "_")


def connect(world: World) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.create_function("REGEXP", 2, regexp, deterministic=True)
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE seq (value INTEGER)")
    connection.executemany("INSERT INTO seq VALUES (?)", [(number,) for number in range(1, NUMBERS + 1)])
    for table, columns in SOURCE_TABLES.items():
        names = [name for name, _ in columns]
        connection.execute(f"CREATE TABLE {table_name(table)} ({', '.join(f'`{name}`' for name in names)})")
        for row in world.get(table, []):
            unknown = set(row) - set(names)
            if unknown:
                raise KeyError(f"{table} has no column {sorted(unknown)}")
            values = [json.dumps(row[name]) if isinstance(row.get(name), list) else row.get(name) for name in names]
            connection.execute(" ".join(("INSERT INTO", table_name(table), "VALUES (" + ", ".join("?" for _ in names) + ")")), values)
    return connection


def rows_of(world: World, query: str) -> list[dict[str, Any]]:
    connection = connect(world)
    try:
        return [dict(row) for row in connection.execute(translate(query)).fetchall()]
    finally:
        connection.close()


def one_row(world: World, query: str) -> dict[str, Any]:
    found = rows_of(world, query)
    if len(found) != 1:
        raise AssertionError(f"a query that gives one row gave {len(found)}")
    return found[0]


def flag(world: World, query: str) -> int:
    return int(one_row(world, query)["n"])


def numeric(case: int, time: int, occurrence: int = 0) -> dict[str, Any]:
    return {"case_id": case, "track": "A", "case_time_us": time, "source_occurrence": occurrence}


def silver_numeric(case: int, time: int, valid: bool = True, cause: str | None = None, ledger: list[int] | None = None) -> dict[str, Any]:
    return {**numeric(case, time), "valid": int(valid), "invalid_cause": cause, "duplicate_source_occurrences": ledger or []}


def clean_world() -> World:
    """Both cohorts, reconciled: a reading with a dropped duplicate and a reading marked invalid, two waveform seconds, one track of each kind, a case with a
    split row, and laboratory rows (a result written twice with a time written two ways, and a result that is missing)."""
    cases = (DEVELOPMENT_CASE, HELD_OUT_CASE)
    bronze_numeric = [row for case in cases for row in (numeric(case, 100, 0), numeric(case, 100, 1), numeric(case, 200, 0))]
    silver = [row for case in cases for row in (silver_numeric(case, 100, ledger=[1]), silver_numeric(case, 200, valid=False, cause="missing"))]
    seconds = [{"case_id": case, "track": "W", "case_second": second} for case in cases for second in (0, 1)]
    verdicts = [(1, None), (0, "flat")] * 2
    lab = [
        {"caseid": str(case), "dt": dt, "name": name, "result": result, "source_occurrence": 1}
        for case in cases
        for dt, name, result in (("5", "na", "140"), ("05", "na", "140"), ("6", "k", None))
    ]
    groups = [
        {"caseid": case, "dt": dt, "name": name, "result": result, "source_copies": copies, "source_occurrence": 1}
        for case in cases
        for dt, name, result, copies in ((5, "na", 140.0, 2), (6, "k", None, 1))
    ]
    return {
        "bronze.numeric_observations": bronze_numeric,
        "silver.numeric_observations": silver,
        "bronze.waveform_seconds": [dict(row) for row in seconds],
        "silver.waveform_seconds": [{**row, "valid": valid, "invalid_cause": cause} for row, (valid, cause) in zip(seconds, verdicts, strict=True)],
        "bronze.tracks": [{"case_id": case, "track": track, "kind": kind} for case in cases for track, kind in (("A", "numeric"), ("W", "waveform"))],
        "bronze.clinical_cases": [
            {"caseid": str(DEVELOPMENT_CASE), "subjectid": DEVELOPMENT_PATIENT},
            {"caseid": str(HELD_OUT_CASE), "subjectid": HELD_OUT_PATIENT},
        ],
        "silver.cases": [{"caseid": case, "subjectid": patient} for case, patient in ((1, DEVELOPMENT_PATIENT), (2, HELD_OUT_PATIENT))],
        "silver.patient_split": [{"subjectid": DEVELOPMENT_PATIENT, "set": "training"}, {"subjectid": HELD_OUT_PATIENT, "set": "test"}],
        "bronze.lab_results": lab,
        "silver.lab_results": groups,
    }


def delete(world: World, table: str, **match: Any) -> World:
    """The world without the rows of ``table`` that hold every ``match`` value."""
    kept = [row for row in world[table] if not all(row.get(name) == value for name, value in match.items())]
    return {**world, table: kept}


def add(world: World, table: str, *rows: Mapping[str, Any]) -> World:
    return {**world, table: [*world[table], *(dict(row) for row in rows)]}


def change(world: World, table: str, values: Mapping[str, Any], **match: Any) -> World:
    changed = [{**row, **values} if all(row.get(name) == value for name, value in match.items()) else row for row in world[table]]
    return {**world, table: changed}
