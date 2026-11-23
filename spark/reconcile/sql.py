"""The Spark SQL text of the reconciliation, built from the table and column rules of the Bronze and Silver modules so that no column, type or pattern is typed
twice.

Every query holds table and column names, the development set names and the dispositions, and no data value. For one table the comparison builds four sets:

- the Bronze rows, with the columns of their stable key;
- the claims of Silver: one row for each Bronze key that a Silver row accounts for, with the disposition of that key. A numeric reading claims its own key and,
  from its ledger, the key of each duplicate that was dropped. A laboratory row claims one key for each copy that it collapsed. A key of a signal that Silver
  marked invalid has the cause as its disposition;
- the keys of both sides, joined by one full outer join, so that a key that only Bronze has, a key that only Silver has and a key that two Silver rows claim are
  each found by the key and not by a count;
- the development cases (the cases of the development sets through Bronze's cases and the patient split), kept for the counts and refused for the flag.

The flag of the held-out patients is one number, 0 or 1, and never a count.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from features.signal_validity import INVALID_CAUSES
from spark.reconcile.checks import (
    BY_NAME,
    COLLAPSED_COPY,
    DUPLICATE,
    JOIN_NAMES,
    KEPT,
    UNRECORDED,
    VALID,
    JoinFacts,
    KeyFacts,
    ReconcileError,
)
from spark.settings import CATALOG
from spark.silver_cases import BIGINT, COPIES_COLUMN
from spark.silver_cases_sql import DEVELOPMENT_LIST, LAB_TYPES, reference, sql, typed_expression

BRONZE_NUMERIC = f"{CATALOG}.bronze.numeric_observations"
BRONZE_WAVEFORM = f"{CATALOG}.bronze.waveform_seconds"
BRONZE_TRACKS = f"{CATALOG}.bronze.tracks"
BRONZE_CASES = f"{CATALOG}.bronze.clinical_cases"
BRONZE_LAB = f"{CATALOG}.bronze.lab_results"
SPLIT = f"{CATALOG}.silver.patient_split"
SILVER_NUMERIC = f"{CATALOG}.silver.numeric_observations"
SILVER_WAVEFORM = f"{CATALOG}.silver.waveform_seconds"
SILVER_CASES = f"{CATALOG}.silver.cases"
SILVER_LAB = f"{CATALOG}.silver.lab_results"
DISPOSITION = "disposition"
DISPOSITION_PREFIX = "disp__"
LEDGER = "duplicate_source_occurrences"


@dataclass(frozen=True, slots=True)
class Sides:
    """The SQL of one table's comparison. ``claim`` is the columns that decide whether a Bronze key and a Silver key are the same key, ``own`` the columns of
    the key that Bronze itself holds, ``case`` the column that names the case and ``null_safe`` the claim columns that may be missing."""

    bronze: str
    silver: str
    claim: tuple[str, ...]
    own: tuple[str, ...]
    case: str
    null_safe: frozenset[str] = frozenset()


def names(columns: tuple[str, ...], alias: str | None = None) -> str:
    return ", ".join(reference(column, alias) for column in columns)


def verdict() -> str:
    """The disposition of a kept signal row: valid, its cause, or unrecorded when its verdict is missing, contradicts itself or names another cause."""
    causes = ", ".join(repr(cause) for cause in INVALID_CAUSES)
    valid, cause = reference("valid"), reference("invalid_cause")
    return (
        f"CASE WHEN {valid} IS NULL THEN '{UNRECORDED}' WHEN {valid} AND {cause} IS NULL THEN '{VALID}' "
        f"WHEN NOT {valid} AND {cause} IN ({causes}) THEN {cause} ELSE '{UNRECORDED}' END"
    )


def numeric_sides() -> Sides:
    key = ("case_id", "track", "case_time_us", "occurrence")
    base = names(key[:3])
    bronze = sql("SELECT", base + ",", f"{reference('source_occurrence')} AS {reference('occurrence')} FROM", BRONZE_NUMERIC)
    kept = sql(
        "SELECT",
        base + ",",
        f"{reference('source_occurrence')} AS {reference('occurrence')},",
        f"{verdict()} AS {reference(DISPOSITION)} FROM",
        SILVER_NUMERIC,
    )
    expanded = sql("SELECT", base + ",", f"explode({reference(LEDGER)}) AS {reference('duplicate')} FROM", SILVER_NUMERIC)
    dropped = sql(
        "SELECT", base + ",", f"{reference('duplicate')} AS {reference('occurrence')}, '{DUPLICATE}' AS {reference(DISPOSITION)} FROM (", expanded, ")"
    )
    return Sides(bronze, sql(kept, "UNION ALL", dropped), key, key, "case_id")


def waveform_sides() -> Sides:
    key = ("case_id", "track", "case_second")
    bronze = sql("SELECT", names(key), "FROM", BRONZE_WAVEFORM)
    silver = sql("SELECT", names(key) + ",", f"{verdict()} AS {reference(DISPOSITION)} FROM", SILVER_WAVEFORM)
    return Sides(bronze, silver, key, key, "case_id")


def cases_sides() -> Sides:
    bronze = sql("SELECT", f"{typed_expression('caseid', BIGINT)} AS {reference('caseid')} FROM", BRONZE_CASES)
    silver = sql("SELECT", f"{reference('caseid')}, '{KEPT}' AS {reference(DISPOSITION)} FROM", SILVER_CASES)
    return Sides(bronze, silver, ("caseid",), ("caseid",), "caseid")


def lab_sides() -> Sides:
    group = ("caseid", "dt", "name", "result")
    typed = ", ".join(f"{typed_expression(column, LAB_TYPES[column])} AS {reference(column)}" for column in group)
    inner = sql("SELECT", typed + ",", reference("source_occurrence"), "FROM", BRONZE_LAB)
    ranked = f"ROW_NUMBER() OVER (PARTITION BY {names(group)} ORDER BY {reference('source_occurrence')}) AS {reference('copy')}"
    bronze = sql("SELECT", names(group) + ",", reference("source_occurrence") + ",", ranked, "FROM (", inner, ")")
    copies = reference(COPIES_COLUMN)
    copy = reference("copy")
    label = f"CASE WHEN {copies} IS NULL OR {copies} < 1 THEN '{UNRECORDED}' WHEN {copy} = 1 THEN '{KEPT}' ELSE '{COLLAPSED_COPY}' END"
    expanded = sql("SELECT", names(group) + ",", copies + ",", f"explode(sequence(1, GREATEST({copies}, 1))) AS {copy} FROM", SILVER_LAB)
    silver = sql("SELECT", names(group) + ",", copy + ",", f"{label} AS {reference(DISPOSITION)} FROM (", expanded, ")")
    return Sides(bronze, silver, (*group, "copy"), ("caseid", "dt", "name", "source_occurrence"), "caseid", frozenset({"result"}))


BUILDERS = {"numeric_observations": numeric_sides, "waveform_seconds": waveform_sides, "cases": cases_sides, "lab_results": lab_sides}


def sides_of(name: str) -> Sides:
    if name not in BUILDERS or name not in BY_NAME:
        raise ReconcileError("bad_table: a table is not one that is reconciled")
    return BUILDERS[name]()


def development_cases() -> str:
    """The case numbers of the development sets: Bronze's cases (typed by the guarded rule) joined to their patient's split row."""
    case = typed_expression("caseid", BIGINT, "c")
    joined = sql(BRONZE_CASES, "c JOIN", SPLIT, "s ON c.`subjectid` = s.`subjectid`")
    return sql("SELECT", f"{case} AS {reference('case_id')} FROM", joined, "WHERE s.`set` IN (" + DEVELOPMENT_LIST + ")")


def cte(name: str, *parts: str) -> str:
    return f"{name} AS (" + sql(*parts) + ")"


def total(condition: str, alias: str) -> str:
    return f"COALESCE(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END), 0) AS {alias}"


def comparison(name: str, *, development: bool) -> str:
    """The ``WITH`` clause that holds the comparison of one table, for the development cases (``development``) or for every other case."""
    sides = sides_of(name)
    join = "LEFT SEMI JOIN" if development else "LEFT ANTI JOIN"
    claim, case = names(sides.claim), reference(sides.case)
    same = " AND ".join(
        f"bk.{reference(column)} <=> ck.{reference(column)}" if column in sides.null_safe else f"bk.{reference(column)} = ck.{reference(column)}"
        for column in sides.claim
    )
    disposition = reference(DISPOSITION)
    pick = sql("COUNT(*) AS `sc`,", f"MIN({disposition}) AS {disposition}")
    pairs = sql("COALESCE(bk." + case + ", ck." + case + ") AS `scope_case`, bk.`br` AS `br`, ck.`sc` AS `sc`,", f"ck.{disposition} AS {disposition}")
    clauses = [
        cte("dev", development_cases()),
        cte("bronze_rows", sides.bronze),
        cte("silver_claims", sides.silver),
        cte("bk", "SELECT", claim + ",", "COUNT(*) AS `br` FROM bronze_rows GROUP BY", claim),
        cte("ck", "SELECT", claim + ",", pick, "FROM silver_claims GROUP BY", claim),
        cte("bo", "SELECT", names(sides.own) + ",", "COUNT(*) AS `n` FROM bronze_rows GROUP BY", names(sides.own)),
        cte("k", "SELECT", pairs, "FROM bk FULL OUTER JOIN ck ON (" + same + ")"),
        cte("ks", "SELECT k.* FROM k", join, "dev ON k.`scope_case` = dev.`case_id`"),
        cte("bos", "SELECT bo.* FROM bo", join, "dev ON bo." + case + " = dev.`case_id`"),
        cte("bf", "SELECT COALESCE(SUM(`n`), 0) AS `bronze_keys`,", total("`n` > 1", "`bronze_repeats`"), "FROM bos"),
        cte("kf", "SELECT", key_totals(name), "FROM ks"),
    ]
    return "WITH " + ", ".join(clauses)


def key_totals(name: str) -> str:
    matched = "`br` IS NOT NULL AND `sc` = 1"
    items = [
        total(matched, "`matched`"),
        total("`br` IS NOT NULL AND `sc` IS NULL", "`unaccounted`"),
        total("`br` IS NOT NULL AND `sc` > 1", "`claimed_twice`"),
        total("`br` IS NULL AND `sc` IS NOT NULL", "`silver_unbacked`"),
    ]
    for disposition in (*BY_NAME[name].dispositions, UNRECORDED):
        items.append(total(f"{matched} AND {reference(DISPOSITION)} = '{disposition}'", f"`{DISPOSITION_PREFIX}{disposition}`"))
    return ", ".join(items)


def key_facts_query(name: str) -> str:
    """One row of counts of keys for the development patients: the Bronze keys, the keys that Bronze holds twice, the keys that were matched, found nowhere in
    Silver, claimed twice and found only in Silver, and the matched keys by disposition."""
    sides_of(name)
    columns = ["bf.`bronze_keys`", "bf.`bronze_repeats`", "kf.`matched`", "kf.`unaccounted`", "kf.`claimed_twice`", "kf.`silver_unbacked`"]
    columns += [f"kf.`{DISPOSITION_PREFIX}{disposition}`" for disposition in (*BY_NAME[name].dispositions, UNRECORDED)]
    items = ", ".join(f"{column} AS {column.split('.', 1)[1].strip('`')}" for column in columns)
    return sql(comparison(name, development=True), "SELECT", items, "FROM bf CROSS JOIN kf")


def held_out_query(name: str) -> str:
    """One flag, 0 or 1, never a count: whether any key of a patient outside the development sets has a defect that the development counts would name."""
    defects = f"bf.`bronze_repeats` + kf.`unaccounted` + kf.`claimed_twice` + kf.`silver_unbacked` + kf.`{DISPOSITION_PREFIX}{UNRECORDED}`"
    return sql(comparison(name, development=False), "SELECT CASE WHEN", defects, "> 0 THEN 1 ELSE 0 END AS n FROM bf CROSS JOIN kf")


@dataclass(frozen=True, slots=True)
class JoinSides:
    """A join: the table whose rows are counted, the columns that join it and the table that holds the partners, as a query of each key and its count."""

    rows: str
    keys: tuple[str, ...]
    partners: str


JOIN_SIDES = {
    "numeric_track": JoinSides(SILVER_NUMERIC, ("case_id", "track"), BRONZE_TRACKS),
    "waveform_track": JoinSides(SILVER_WAVEFORM, ("case_id", "track"), BRONZE_TRACKS),
    "lab_case": JoinSides(SILVER_LAB, ("caseid",), SILVER_CASES),
    "case_split": JoinSides(SILVER_CASES, ("subjectid",), SPLIT),
}


def join_query(name: str) -> str:
    """The rows of a Silver table, those with no partner row and those with more than one, by the join key that the Silver build uses."""
    if name not in JOIN_NAMES or name not in JOIN_SIDES:
        raise ReconcileError("bad_table: a join is not one that is checked")
    side = JOIN_SIDES[name]
    keys = names(side.keys)
    partners = sql("SELECT", keys + ", COUNT(*) AS `n` FROM", side.partners, "GROUP BY", keys)
    on = " AND ".join(f"x.{reference(column)} = p.{reference(column)}" for column in side.keys)
    items = [
        "COUNT(*) AS row_count",
        "COALESCE(SUM(CASE WHEN p.`n` IS NULL THEN 1 ELSE 0 END), 0) AS unmatched",
        "COALESCE(SUM(CASE WHEN p.`n` > 1 THEN 1 ELSE 0 END), 0) AS fanned",
    ]
    return sql("SELECT", ", ".join(items), "FROM", side.rows, "x LEFT JOIN (", partners, ") p ON", on)


def count_of(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if type(value) is not int or value < 0:
        raise ReconcileError("bad_fact: a count is missing or is not a count")
    return value


def decode_key_facts(row: Mapping[str, object]) -> KeyFacts:
    dispositions = {name.removeprefix(DISPOSITION_PREFIX): count_of(row, name) for name in row if name.startswith(DISPOSITION_PREFIX)}
    return KeyFacts(
        count_of(row, "bronze_keys"),
        count_of(row, "bronze_repeats"),
        count_of(row, "matched"),
        count_of(row, "unaccounted"),
        count_of(row, "claimed_twice"),
        count_of(row, "silver_unbacked"),
        dispositions,
    )


def decode_join_facts(row: Mapping[str, object]) -> JoinFacts:
    return JoinFacts(count_of(row, "row_count"), count_of(row, "unmatched"), count_of(row, "fanned"))
