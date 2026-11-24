"""Run the reconciliation's own Spark SQL on tiny tables in SQLite, so that a test can look at what a query returns and not only at its text.

The translation is small and exact: three-part table names become one name, ``<=>`` becomes ``IS``, ``LEFT SEMI JOIN dev ON <predicate>`` becomes
``WHERE EXISTS (SELECT 1 FROM dev WHERE <predicate>)`` and ``LEFT ANTI JOIN dev ON <predicate>`` becomes ``WHERE NOT EXISTS (...)``, each with the whole
predicate, ``RLIKE`` and ``TRY_CAST`` become a regular-expression function and a cast (the pattern of the typing rule guards the cast), and the two
``explode`` forms (the ledger of a reading and the copies of a laboratory row) become a join to ``json_each`` and to a table of numbers. A construct that is
not one of these is not translated and raises ``ValueError``, so a query that gains one fails loudly here and the translation is extended on purpose.

The query is cut once into quoted literals and other text. Every substitution and the join translation read the other text only: a literal is held aside
and put back unchanged. Only the quoting that the job's queries use is accepted (single-quoted literals, back-quoted plain names, and the backslash pair of the
typing-rule pattern); a double quote, a comment, a second statement, a bracket name, an escape, a doubled quote, an adjacent pair of quotes, two literals in a
row (with or without spaces between them) or a quote that does not close raises. An empty literal that stands alone (two quotes with a comma, a space and
something other than a quote, or the end of the text after them) is kept.

``World`` holds two cohorts. Case 1 of patient 10 is in the training set, case 2 of patient 20 is in the test set. Each table has the rows of a clean,
reconciled build for both. A test plants a defect in a cohort and reads what the query counts.
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
        re.compile(r"explode\(`duplicate_source_occurrences`\) AS `duplicate` FROM (\w+)"),
        lambda m: f"je.value AS `duplicate` FROM {m[1]}, json_each({m[1]}.`duplicate_source_occurrences`) AS je",
    ),
    (
        re.compile(r"explode\(sequence\(1, GREATEST\((`\w+`), 1\)\)\) AS `copy` FROM (\w+)"),
        lambda m: f"seq.value AS `copy` FROM {m[2]} JOIN seq ON seq.value <= MAX(COALESCE({m[1]}, 1), 1)",
    ),
)


ANY_SCOPE_JOIN = re.compile(r"\b(SEMI|ANTI)\s+JOIN\b")
EXACT_SCOPE_JOIN = re.compile(r"\bLEFT (SEMI|ANTI) JOIN dev ON ")
ENDS_PREDICATE = re.compile(r"(WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|JOIN|LEFT|RIGHT|FULL|INNER|CROSS|ON)\b")
NAME = re.compile(r"`\w+`")
HELD = "\x00"
HELD_LITERAL = re.compile(HELD + r"(\d+)" + HELD)
REFUSED_OUTSIDE = ('"', "[", "]", "\\", ";", "--", "/*", "*/")
TYPING_PAIR = "\\\\z"


def masked(text: str) -> str:
    """The text with the inside of every back-quoted name blanked, so that a keyword that is also a name is not read as SQL."""
    return NAME.sub(lambda m: m[0][0] + "_" * (len(m[0]) - 2) + m[0][-1], text)


def hold_literals(query: str) -> tuple[str, list[str]]:
    """The query with each single-quoted literal replaced by a marker that holds its place, and the literals in order.

    Anything the adapter does not handle raises: a literal that does not close, a doubled quote, a backslash that is not the typing-rule pair, a back-quoted
    name that is not a plain word, and outside a literal a double quote, a bracket name, a backslash, a comment or a second statement. A control character in
    the query raises as well, so that no text can imitate a held literal.
    """
    if any(ord(char) < 32 for char in query):
        raise ValueError("a query holds a control character, which the adapter uses to hold a literal")
    text, index = "", 0
    held: list[str] = []
    while index < len(query):
        character = query[index]
        if character == "'":
            end = query.find("'", index + 1)
            if end < 0:
                raise ValueError(f"a literal does not close: {query[index:][:40]!r}")
            if query.startswith("'", end + 1):
                raise ValueError(f"a doubled quote in a literal is not translated: {query[index : end + 2][:40]!r}")
            literal = query[index : end + 1]
            if "\\" in literal.replace(TYPING_PAIR, ""):
                raise ValueError(f"a backslash escape in a literal is not translated: {literal[:40]!r}")
            if query[end + 1 :].lstrip(" ").startswith("'"):
                raise ValueError(f"two literals in a row are not translated (Spark joins them into one, SQLite reads an alias): {query[index:][:40]!r}")
            text += f"{HELD}{len(held)}{HELD}"
            held.append(literal)
            index = end + 1
        elif character == "`":
            name = NAME.match(query, index)
            if name is None:
                raise ValueError(f"a back-quoted name that is not one plain word is not translated: {query[index:][:40]!r}")
            if query.startswith("`", name.end()):
                raise ValueError(f"a doubled back quote in a name is not translated: {query[index:][:40]!r}")
            text += name[0]
            index = name.end()
        else:
            text += character
            index += 1
    for refused in REFUSED_OUTSIDE:
        if refused in text:
            raise ValueError(f"quoting or syntax that the adapter does not handle: {refused!r}")
    return text, held


def restore_literals(text: str, held: list[str]) -> str:
    return HELD_LITERAL.sub(lambda m: held[int(m[1])], text)


def has_where_at_level(prefix: str) -> bool:
    """Whether the innermost open select of ``prefix`` already has a ``WHERE``."""
    levels = [False]
    for token in re.finditer(r"\(|\)|\bWHERE\b", masked(prefix)):
        if token[0] == "(":
            levels.append(False)
        elif token[0] == ")":
            levels.pop()
        else:
            levels[-1] = True
    return levels[-1]


def predicate_end(text: str, start: int) -> int:
    """The index where the predicate that begins at ``start`` ends: the bracket that closes the select that holds it, or the end of the text."""
    hidden, depth = masked(text), 0
    for index in range(start, len(hidden)):
        if hidden[index] == "(":
            depth += 1
        elif hidden[index] == ")":
            if depth == 0:
                return index
            depth -= 1
        elif depth == 0 and (index == 0 or not hidden[index - 1].isalnum()) and ENDS_PREDICATE.match(hidden, index):
            raise ValueError(f"a scope join is followed by more than its predicate: {text[index : index + 20]!r}")
    if depth:
        raise ValueError("a scope join predicate has an open bracket that is not closed")
    return len(hidden)


def scope_joins(query: str) -> str:
    """Each ``LEFT SEMI|ANTI JOIN dev ON <predicate>`` as ``WHERE [NOT] EXISTS (SELECT 1 FROM dev WHERE <the whole predicate>)``; any other form raises."""
    while True:
        hidden = masked(query)
        found, exact = ANY_SCOPE_JOIN.search(hidden), EXACT_SCOPE_JOIN.search(hidden)
        if found is None:
            return query
        if exact is None:
            raise ValueError(
                f"a semi join or an anti join that is not 'LEFT SEMI|ANTI JOIN dev ON <predicate>' is not translated: {query[found.start() :][:60]!r}"
            )
        if has_where_at_level(query[: exact.start()]):
            raise ValueError("a scope join follows a WHERE of its own select")
        end = predicate_end(query, exact.end())
        predicate = query[exact.end() : end]
        if not predicate.strip():
            raise ValueError("a scope join has no predicate")
        negation = "NOT " if exact[1] == "ANTI" else ""
        query = "".join((query[: exact.start()], "WHERE ", negation, "EXISTS (SELECT 1 FROM dev WHERE ", predicate, ")", query[end:]))


def translate(query: str) -> str:
    text, held = hold_literals(query)
    text = scope_joins(text)
    for pattern, replace in STEPS:
        text = pattern.sub(replace, text)
    return restore_literals(text, held)


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
