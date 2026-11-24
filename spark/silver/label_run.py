"""The order of a build and of a verify of the label tables, in plain data with no Spark.

The job ``spark/jobs/silver_labels.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in steps.
The order is that of ``spark/silver/hypotension_run.py``: the branch is resolved to one hash, the input is read and checked at it before anything is planned or
written, each table is compared by its fingerprint with the table that the branch holds and a table that is already the planned one is not written again, a
write is made only while the branch still has the checked hash, each changed table is one commit whose hash comes from the branch's log, and the input and all
tables are read again at the hash of the last write. The read-back must be the planned tables, with every check passing.

The measure of ``icu_days`` is written first, as a commit of its own, and the intensive care labels and the kidney injury labels after it: the values of the
field are measured and recorded before the label is built. A write that fails stops the build before the next one.

The labels are outcomes and the test set is sealed: the result holds the number of cases (an input), the fingerprints (a row count and a digest), the snapshot
IDs, the checks that ran and the measure of ``icu_days`` of the development patients. Every check about a label is a flag.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.label_checks import CHECK_NAMES, TABLES, InputFacts, TableFacts, input_problems, table_problems
from spark.silver.label_measure import IcuMeasureRow
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.split_table import unit


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of the input at one hash gives: the counts and flags of the checks and the snapshot IDs of the three tables it reads (the cases, the
    laboratory results and the patient split)."""

    facts: InputFacts
    snapshots: tuple[int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class Plan:
    """The fingerprints of the planned tables and the measure of ``icu_days`` of the development patients."""

    fingerprints: dict[str, Fingerprint]
    measure: tuple[IcuMeasureRow, ...]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A table at one hash: its fingerprint, its snapshot ID and, when asked for, the facts of its checks."""

    fingerprint: Fingerprint
    snapshot_id: int | None
    facts: TableFacts | None = None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps: every read is at the hash that is given and ``plan`` keeps the frames for ``write``.

    ``read_table`` gives ``None`` for a hash that holds no such table. ``write`` writes the planned frame of a table to the branch, over the rows of the table
    that exists or as a new table. ``write_hash`` is given the hash that the write was made on and gives the hash of the write, or ``None`` when the branch head
    is not the one commit of the write directly on it. ``prepare`` creates the namespace when the branch has none, as a commit of its own, and gives the hash
    that the first write is made on. ``release`` frees the planned frames.
    """

    head: Callable[[], str]
    read_input: Callable[[str], InputRead]
    plan: Callable[[str], Plan]
    read_table: Callable[[str, str, bool], TableRead | None]
    write: Callable[[str, bool], None]
    write_hash: Callable[[str, str], str | None]
    prepare: Callable[[str], str]
    release: Callable[[], None]


@dataclass(frozen=True, slots=True)
class Outcome:
    outcome: str
    checked_hash: str
    write_hash: str | None
    branch_hash: str
    source: InputRead
    tables: dict[str, TableRead]
    plan: Plan

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, the fingerprints, the checks that ran and the measure of ``icu_days`` of the development
        patients. It holds no count of a label."""
        return {
            "outcome": self.outcome,
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(("cases", "lab_results", "patient_split"), self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "rows": {table: read.fingerprint.rows for table, read in self.tables.items()},
            "fingerprints": {table: [read.fingerprint.rows, read.fingerprint.digest] for table, read in self.tables.items()},
            "icu_measure": [asdict(row) for row in self.plan.measure],
            "checks_run": list(CHECK_NAMES),
        }


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


def read_problems(source: InputRead, reads: dict[str, TableRead | None]) -> list[str]:
    """The tables that are not there, or else the problems of each table."""
    missing = sum(1 for read in reads.values() if read is None or read.facts is None)
    if missing:
        return [f"table_missing: {unit(missing, 'table')}"]
    problems: list[str] = []
    for table in TABLES:
        read = reads[table]
        if read is not None and read.facts is not None:
            problems += table_problems(source.facts.cases, table, read.facts)
    return problems


def differs(read: TableRead | None, planned: Fingerprint) -> bool:
    """Whether the table that a branch holds is not the planned one: it is not there, or its fingerprint is another."""
    return read is None or read.fingerprint != planned


def build_inner(ops: Operations, head: str) -> Outcome:
    source = ops.read_input(head)
    problems = input_problems(source.facts)
    if problems:
        raise SilverSignalsError(*problems)
    planned = ops.plan(head)
    held = read_tables(ops, head, False)
    changed = [table for table in TABLES if differs(held[table], planned.fingerprints[table])]
    at, written = head, None
    if changed:
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        on = ops.prepare(head)
        for table in changed:
            ops.write(table, held[table] is not None)
            written = ops.write_hash(on, table)
            if written is None:
                raise SilverSignalsError(BRANCH_MOVED)
            on = written
        at = on
    if ops.read_input(at) != source:
        raise SilverSignalsError(f"source_changed: {unit(1, 'read')} of the input differs")
    reads = read_tables(ops, at, True)
    problems = read_problems(source, reads)
    differing = len([table for table in TABLES if reads[table] is not None and differs(reads[table], planned.fingerprints[table])])
    if differing and not problems:
        problems.append(f"readback_differs: {unit(differing, 'table')}")
    if problems:
        raise SilverSignalsError(*problems)
    if ops.head() != at:
        raise SilverSignalsError(BRANCH_MOVED)
    return Outcome(
        "written" if changed else "unchanged", head, written, at, source, {table: read for table, read in reads.items() if read is not None}, planned
    )


def run_build(ops: Operations, head: str) -> Outcome:
    """Build the tables on the branch whose head is ``head``, or leave them as they are when nothing changes. The planned frames are released in every case."""
    try:
        return build_inner(ops, head)
    finally:
        ops.release()


def run_verify(ops: Operations, head: str) -> Outcome:
    """Run every check on the tables that the branch holds at ``head``, and confirm that the branch still has ``head``. Nothing is written. The input is
    checked first, so no table is read with a split that is not trusted."""
    try:
        source = ops.read_input(head)
        problems = input_problems(source.facts)
        if problems:
            raise SilverSignalsError(*problems)
        reads = read_tables(ops, head, True)
        problems = read_problems(source, reads)
        if problems:
            raise SilverSignalsError(*problems)
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome("verified", head, None, head, source, tables, Plan({table: read.fingerprint for table, read in tables.items()}, ()))
    finally:
        ops.release()
