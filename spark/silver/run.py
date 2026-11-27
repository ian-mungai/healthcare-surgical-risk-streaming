"""The order of a build and of a verify of the Silver signal tables, in plain data with no Spark.

The job ``spark/jobs/silver_signals.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in steps.

A build resolves the branch to one hash. It reads Bronze at that hash and refuses an input that cannot be cleaned (``bronze_problems``) before it plans or
writes anything. It plans both tables (each as a frame and a fingerprint: the number of rows and the sum of a hash of each row, which never leaves the job)
and compares each with the table that the branch holds. A table whose fingerprint is the planned one is left as it is, so a rerun that changes nothing writes
no snapshot. It writes only while the branch still has the checked hash, creates the namespace as a commit of its own when the branch has none, writes each
changed table as one commit (the hash of the write is taken from the branch's log: the head, one commit directly on the hash that the write was made on,
changing only that table) and reads Bronze and both tables at the hash of the last write. The read-back must be the planned tables, with every check passing.
The job ends only when the branch still has that hash.

The test set is sealed and the signals of the held-out cases are in these tables, so a count or a digest over the rows changes with a held-out row. The
fingerprints, the counts of the checks and the causes stay inside the job. The result holds the hashes, the snapshot IDs, the checks that ran and one pass flag
for each table, and every refusal is a code and a flag, never a count.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spark.silver.checks import (
    CHECK_NAMES,
    FLAG,
    BronzeFacts,
    NumericFacts,
    SilverSignalsError,
    WaveformFacts,
    bronze_problems,
    numeric_problems,
    waveform_problems,
)

NUMERIC = "numeric_observations"
WAVEFORM = "waveform_seconds"
TABLES = (NUMERIC, WAVEFORM)
BRANCH_MOVED = f"branch_moved: {FLAG}"


@dataclass(frozen=True, slots=True)
class Fingerprint:
    """The number of rows of a table and the sum of a hash of each row: equal tables have equal fingerprints, whatever the order of their rows."""

    rows: int
    digest: int


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of Bronze at one hash gives: the counts of the checks and the snapshot IDs of the numeric, waveform and track tables."""

    facts: BronzeFacts
    snapshots: tuple[int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A Silver table at one hash: its fingerprint, its snapshot ID and, when asked for, the counts of its checks."""

    fingerprint: Fingerprint
    snapshot_id: int | None
    numeric: NumericFacts | None = None
    waveform: WaveformFacts | None = None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps. ``head`` is the hash of the branch now and every read is at the hash that is given.

    ``plan`` builds both frames from Bronze at that hash, keeps them for ``write`` and gives their fingerprints. ``read_table`` gives ``None`` for a hash that
    holds no such table, and the counts of its checks only when asked. ``write`` writes the planned frame of a table to the branch, over the rows of the table
    that exists or as a new table. ``write_hash`` is given the hash that the write was made on and gives the hash of the write, or ``None`` when the branch head
    is not the one commit of the write directly on it. ``prepare`` creates the namespace when the branch has none, as a commit of its own, and gives the hash
    that the first write is made on. ``release`` frees the planned frames.
    """

    head: Callable[[], str]
    read_input: Callable[[str], InputRead]
    plan: Callable[[str], dict[str, Fingerprint]]
    read_table: Callable[[str, str, bool], TableRead | None]
    write: Callable[[str, bool], None]
    write_hash: Callable[[str, str], str | None]
    prepare: Callable[[str], str]
    release: Callable[[], None]


@dataclass(frozen=True, slots=True)
class Outcome:
    checked_hash: str
    write_hash: str | None
    branch_hash: str
    source: InputRead
    tables: dict[str, TableRead]
    passed: dict[str, bool]

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, one pass flag for each table and the checks that ran. It holds no count, no digest and no
        cause, and nothing in it names a case, a track, a time or a value. It holds no field that says whether a table was written: a held-out row alone
        changes that, and the write hash and the snapshot IDs show it."""
        return {
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(("numeric_observations", "waveform_seconds", "tracks"), self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "passed": dict(self.passed),
            "checks_run": list(CHECK_NAMES),
        }


def checked(table: str, read: TableRead | None) -> bool:
    """Whether the table is there with the counts of its checks."""
    return read is not None and (read.numeric if table == NUMERIC else read.waveform) is not None


def own_problems(table: str, read: TableRead) -> list[str]:
    if table == NUMERIC and read.numeric is not None:
        return numeric_problems(read.numeric)
    if read.waveform is not None:
        return waveform_problems(read.waveform)
    return []


def table_problems(reads: dict[str, TableRead | None]) -> list[str]:
    """The tables that are not there, or else the problems of both tables against the counts of the Bronze input."""
    missing = sum(1 for read in reads.values() if read is None)
    if missing:
        return [f"table_missing: {FLAG}"]
    if not all(checked(table, reads[table]) for table in TABLES):
        return [f"table_missing: {FLAG}"]
    problems: list[str] = []
    for table in TABLES:
        read = reads[table]
        if read is not None:
            problems += own_problems(table, read)
    return problems


def passed_flags(reads: dict[str, TableRead | None], planned: dict[str, Fingerprint] | None) -> dict[str, bool]:
    """One flag for each table: the table is there, every check of it passes and, in a build, it is the planned table."""
    flags = {}
    for table in TABLES:
        read = reads[table]
        flags[table] = read is not None and checked(table, read) and not own_problems(table, read) and (planned is None or not differs(read, planned[table]))
    return flags


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


def differs(read: TableRead | None, planned: Fingerprint) -> bool:
    """Whether the table that a branch holds is not the planned one: it is not there, or its fingerprint is another."""
    return read is None or read.fingerprint != planned


def build_inner(ops: Operations, head: str) -> Outcome:
    source = ops.read_input(head)
    problems = bronze_problems(source.facts)
    if problems:
        raise SilverSignalsError(*problems)
    planned = ops.plan(head)
    held = read_tables(ops, head, False)
    changed = [table for table in TABLES if differs(held[table], planned[table])]
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
    later = ops.read_input(at)
    if later != source:
        raise SilverSignalsError(f"source_changed: {FLAG}")
    reads = read_tables(ops, at, True)
    problems = table_problems(reads)
    differing = len([table for table in TABLES if reads[table] is not None and differs(reads[table], planned[table])])
    if differing and not problems:
        problems.append(f"readback_differs: {FLAG}")
    if problems:
        raise SilverSignalsError(*problems)
    if ops.head() != at:
        raise SilverSignalsError(BRANCH_MOVED)
    return Outcome(
        head,
        written,
        at,
        source,
        {table: read for table, read in reads.items() if read is not None},
        passed_flags(reads, planned),
    )


def run_build(ops: Operations, head: str) -> Outcome:
    """Build the tables on the branch whose head is ``head``, or leave them as they are when nothing changes. The planned frames are released in every case."""
    try:
        return build_inner(ops, head)
    finally:
        ops.release()


def run_verify(ops: Operations, head: str) -> Outcome:
    """Run every check on the tables that the branch holds at ``head``, and confirm that the branch still has ``head``. Nothing is written."""
    try:
        source = ops.read_input(head)
        reads = read_tables(ops, head, True)
        problems = bronze_problems(source.facts) + table_problems(reads)
        if problems:
            raise SilverSignalsError(*problems)
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome(head, None, head, source, tables, passed_flags(reads, None))
    finally:
        ops.release()
