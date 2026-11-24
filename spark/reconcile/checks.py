"""The checks of the reconciliation of Silver with Bronze, on counts that the Spark job reads from the tables. No Spark runs here.

Each Bronze table is compared with the Silver table that was built from it by its stable key, in both directions. A Silver row accounts for the Bronze
keys that it stands for: a numeric reading for its own key and, in its ledger, for the keys of the duplicates that were dropped; a waveform second, a
case for their own key; a laboratory row for one key for each copy that it collapsed. A key that Silver marked invalid keeps its cause, so every
accounted key has a disposition from the closed list of its table. The comparison never reads a count and calls it proof: it finds the Bronze keys
that no Silver key accounts for (``unaccounted``), the Silver keys that no Bronze key backs (``silver_unbacked``), the Bronze keys that two Silver
keys claim (``claimed_twice``) and the Bronze keys that Bronze holds twice (``bronze_repeats``). Four joins that the Silver build makes are checked
for their cardinality: each row must have exactly one partner, and a partner of a signal's track is of the signal's kind.

Before any count the job checks the patient split table by the split job's own rules and the owner of each Bronze case by the Silver cases job's own query. A
development set is only trusted after both pass, and a failure refuses the whole run with one fixed code and no count.

The test set is sealed. The counts and dispositions are those of development patients. For every other patient the job reads one flag for each table, set when
any key is not accounted for, and never a count. A problem is a code and a count and holds no case, track, time or value.

A table that a later slice joins is added here once: one ``Reconciliation`` with its dispositions in ``RECONCILIATIONS``, and its queries in ``sql.py``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from features.signal_validity import INVALID_CAUSES
from spark.bronze import tables as bronze
from spark.silver import tables as silver
from spark.silver_cases import CASE_COLUMNS, LAB_COLUMNS, OWNERSHIP_PROBLEM, SOURCE_SCHEMAS
from spark.split_table import unit

VALID = "valid"
DUPLICATE = "duplicate"
KEPT = "kept"
COLLAPSED_COPY = "collapsed_copy"
UNRECORDED = "unrecorded"
SPLIT_REFUSAL = "split_rule_broken: the patient split table does not follow the split rule"
OWNER_REFUSAL = OWNERSHIP_PROBLEM


class ReconcileError(ValueError):
    """The reconciliation cannot run or a check failed. ``problems`` holds one message for each cause, each a code and a count."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass(frozen=True, slots=True)
class Reconciliation:
    """A Bronze table and the Silver table built from it. ``dispositions`` are the closed list of what a Bronze key can have become."""

    name: str
    dispositions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Join:
    """A join that the Silver build makes. Every row of the first table must have exactly one partner in the second. ``table`` is the reconciled table whose
    held-out flag takes the failures of the join for patients outside the development sets."""

    name: str
    table: str


@dataclass(frozen=True, slots=True)
class KeyFacts:
    """What the comparison of one table's Bronze keys with its Silver keys shows, for development patients. Every count is of keys.

    ``bronze_keys`` is the Bronze rows. ``matched`` is the keys that Bronze holds and exactly one Silver key accounts for, and ``dispositions`` counts them by
    what they became (``unrecorded`` is a key whose Silver row has no disposition from the closed list). ``silver_repeats`` is the declared keys of the
    Silver table that two rows have.
    """

    bronze_keys: int
    bronze_repeats: int
    matched: int
    unaccounted: int
    claimed_twice: int
    silver_unbacked: int
    dispositions: dict[str, int]
    silver_repeats: int = 0


@dataclass(frozen=True, slots=True)
class JoinFacts:
    """What a join shows: the rows of the first table, the rows with no partner, the rows with more than one partner and the rows that have a partner of
    another kind than the join needs (a signal whose track is of the other kind, however many partners it has)."""

    rows: int
    unmatched: int
    fanned: int
    kind_wrong: int = 0


RECONCILIATIONS = (
    Reconciliation(silver.NUMERIC.name, (VALID, *INVALID_CAUSES, DUPLICATE)),
    Reconciliation(silver.WAVEFORM.name, (VALID, *INVALID_CAUSES)),
    Reconciliation("cases", (KEPT,)),
    Reconciliation("lab_results", (KEPT, COLLAPSED_COPY)),
)
BY_NAME = {reconciliation.name: reconciliation for reconciliation in RECONCILIATIONS}
JOINS = (
    Join("numeric_track", silver.NUMERIC.name),
    Join("waveform_track", silver.WAVEFORM.name),
    Join("lab_case", "lab_results"),
    Join("case_split", "cases"),
)
JOIN_NAMES = frozenset(join.name for join in JOINS)
JOIN_TABLE = {join.name: join.table for join in JOINS}

SOURCE_TABLES: dict[str, tuple[tuple[str, str], ...]] = {
    **{f"bronze.{table.name}": table.columns for table in (bronze.NUMERIC, bronze.WAVEFORM, bronze.TRACKS)},
    **{table: tuple(columns) for table, columns in SOURCE_SCHEMAS.items()},
    **{f"silver.{table.name}": table.columns for table in silver.SILVER_TABLES.values()},
    "silver.cases": tuple((column.name, column.type) for column in CASE_COLUMNS),
    "silver.lab_results": tuple((column.name, column.type) for column in LAB_COLUMNS),
}

KEY_CHECKS = (
    ("bronze_empty", "key"),
    ("bronze_key_repeated", "key"),
    ("silver_key_repeated", "key"),
    ("key_unaccounted", "key"),
    ("key_claimed_twice", "key"),
    ("key_unbacked", "key"),
    ("disposition_unrecorded", "key"),
    ("disposition_total", "key"),
    ("disposition_unknown", "disposition"),
)
JOIN_CHECKS = ("join_empty", "join_unmatched", "join_fanned", "join_kind_wrong")
REFUSAL_CHECKS = tuple(message.split(":")[0] for message in (SPLIT_REFUSAL, OWNER_REFUSAL))
CHECK_NAMES = (
    *REFUSAL_CHECKS,
    *(f"{reconciliation.name}_{code}" for reconciliation in RECONCILIATIONS for code, _ in KEY_CHECKS),
    *(f"{reconciliation.name}_held_out" for reconciliation in RECONCILIATIONS),
    *(f"{join.name}_{code}" for join in JOINS for code in JOIN_CHECKS),
    "source_missing",
    "source_schema",
)
JOB_CODES = frozenset({"branch_moved", "bad_fact", "catalog_hash"})
CODES = frozenset(CHECK_NAMES) | JOB_CODES


def counted(pairs: Sequence[tuple[str, int, str]]) -> list[str]:
    return [f"{code}: {unit(count, noun)}" for code, count, noun in pairs if count]


def key_problems(name: str, facts: KeyFacts) -> list[str]:
    """The problems of one table's key comparison, each a code that starts with the table's name and a count of keys."""
    allowed = set(BY_NAME[name].dispositions) | {UNRECORDED}
    if facts.bronze_keys == 0:
        return [f"{name}_bronze_empty: {unit(0, 'key')}"]
    unrecorded = facts.dispositions.get(UNRECORDED, 0)
    counts = {
        "bronze_key_repeated": facts.bronze_repeats,
        "silver_key_repeated": facts.silver_repeats,
        "key_unaccounted": facts.unaccounted,
        "key_claimed_twice": facts.claimed_twice,
        "key_unbacked": facts.silver_unbacked,
        "disposition_unrecorded": unrecorded,
        "disposition_total": abs(facts.matched - sum(facts.dispositions.values())),
    }
    nouns = dict(KEY_CHECKS)
    problems = counted([(f"{name}_{code}", counts[code], nouns[code]) for code in counts])
    unknown = sum(1 for disposition in facts.dispositions if disposition not in allowed)
    return problems + counted([(f"{name}_disposition_unknown", unknown, "disposition")])


def join_problems(name: str, facts: JoinFacts) -> list[str]:
    if facts.rows == 0:
        return [f"{name}_join_empty: {unit(0, 'row')}"]
    return counted(
        [
            (f"{name}_join_unmatched", facts.unmatched, "row"),
            (f"{name}_join_fanned", facts.fanned, "row"),
            (f"{name}_join_kind_wrong", facts.kind_wrong, "row"),
        ]
    )


def held_out_problems(flags: Mapping[str, bool]) -> list[str]:
    """One code for each table whose flag is set: a held-out key is not accounted for. The message never holds a count."""
    return [f"{name}_held_out: a held-out key is not accounted for" for name, flagged in flags.items() if flagged]


def schema_problems(found: Mapping[str, Sequence[tuple[str, str]] | None]) -> list[str]:
    """The tables that are missing and the tables whose columns or types are not the expected ones, each as a code and a count. ``found`` maps a table to its
    columns, or to ``None`` when the table is not there."""
    missing = sum(1 for table in SOURCE_TABLES if found.get(table) is None)
    wrong = sum(1 for table, columns in SOURCE_TABLES.items() if found.get(table) is not None and list(found[table] or ()) != list(columns))
    return counted([("source_missing", missing, "table"), ("source_schema", wrong, "table")])
