"""The order of a build and of a verify of the label tables, in plain data with no Spark.

The job ``spark/jobs/silver_labels.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in steps.
The order is that of ``spark/silver/hypotension_run.py``: the branch is resolved to one hash, the input is read and checked at it before anything is planned or
written, each table is compared by its fingerprint with the table that the branch holds and a table that is already the planned one is not written again, a
write is made only while the branch still has the checked hash, each changed table is one commit whose hash comes from the branch's log, and the input and all
tables are read again at the hash of the last write. The read-back must be the planned tables, with every check passing.

The measure of ``icu_days`` comes first, as a commit of its own: it is planned, written and read back with every check passing before any intensive care label
is planned (evaluated), and the intensive care labels and the kidney injury labels come after it. A write that fails stops the build before the next one.

The labels are outcomes and the test set is sealed, and a digest of a label table changes with a held-out row. The fingerprints (a row count and a digest) are
used inside the run to compare a table with its plan and are never part of the result: the result holds the hashes, the snapshot IDs, one pass flag for each
table, the checks that ran and the measure of ``icu_days`` of the development patients. A refusal is a code and a flag, never a count.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.hypotension_checks import FLAG
from spark.silver.label_checks import CHECK_NAMES, ICU, KIDNEY, MEASURE, TABLES, InputFacts, TableFacts, input_problems, table_problems
from spark.silver.label_measure import IcuMeasureRow
from spark.silver.run import BRANCH_MOVED, Fingerprint

LABEL_TABLES = (ICU, KIDNEY)


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of the input at one hash gives: the facts of the checks and the snapshot IDs of the three tables it reads (the cases, the laboratory
    results and the patient split)."""

    facts: InputFacts
    snapshots: tuple[int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class MeasurePlan:
    """The fingerprint of the planned measure of ``icu_days`` and its rows, from the development patients' values only."""

    fingerprint: Fingerprint
    rows: tuple[IcuMeasureRow, ...]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A table at one hash: its fingerprint, its snapshot ID and, when asked for, the facts of its checks."""

    fingerprint: Fingerprint
    snapshot_id: int | None
    facts: TableFacts | None = None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps: every read is at the hash that is given.

    ``plan_measure`` plans the measure only. ``plan_labels`` plans the intensive care labels and the kidney injury labels at the hash that is given and gives
    their fingerprints: it is called only after the measure was written and read back. Both keep the planned frames for ``write``. ``read_table`` gives ``None``
    for a hash that holds no such table. ``write`` writes the planned frame of a table to the branch, over the rows of the table that exists or as a new table.
    ``write_hash`` is given the hash that the write was made on and gives the hash of the write, or ``None`` when the branch head is not the one commit of the
    write directly on it. ``prepare`` creates the namespace when the branch has none, as a commit of its own, and gives the hash that the first write is made
    on. ``release`` frees the planned frames.
    """

    head: Callable[[], str]
    read_input: Callable[[str], InputRead]
    plan_measure: Callable[[str], MeasurePlan]
    plan_labels: Callable[[str], dict[str, Fingerprint]]
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
    measure: tuple[IcuMeasureRow, ...]
    passed: dict[str, bool]

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, one pass flag for each table, the checks that ran and the measure of ``icu_days`` of the
        development patients. It holds no count, no digest and no value of a label, and no word that says whether the build wrote ("written" or "unchanged"): a
        reader judges a write by the hashes and the snapshot IDs."""
        return {
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(("cases", "lab_results", "patient_split"), self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "passed": dict(self.passed),
            "icu_measure": [asdict(row) for row in self.measure],
            "checks_run": list(CHECK_NAMES),
        }


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


def read_problems(reads: dict[str, TableRead | None]) -> list[str]:
    """The tables that are not there, or else the problems of each table."""
    if any(read is None or read.facts is None for read in reads.values()):
        return [f"table_missing: {FLAG}"]
    problems: list[str] = []
    for table in reads:
        read = reads[table]
        if read is not None and read.facts is not None:
            problems += table_problems(table, read.facts)
    return problems


def differs(read: TableRead | None, planned: Fingerprint) -> bool:
    """Whether the table that a branch holds is not the planned one: it is not there, or its fingerprint is another."""
    return read is None or read.fingerprint != planned


@dataclass(slots=True)
class Chain:
    """The commits of one build: the hash that the next write is made on and the hash of the last write."""

    head: str
    on: str
    written: str | None = None
    prepared: bool = False

    def write(self, ops: Operations, table: str, exists: bool) -> None:
        if not self.prepared:
            if ops.head() != self.head:
                raise SilverSignalsError(BRANCH_MOVED)
            self.on, self.prepared = ops.prepare(self.head), True
        ops.write(table, exists)
        self.written = ops.write_hash(self.on, table)
        if self.written is None:
            raise SilverSignalsError(BRANCH_MOVED)
        self.on = self.written


def passed_flags(reads: dict[str, TableRead | None], planned: dict[str, Fingerprint] | None) -> dict[str, bool]:
    """One flag for each table: the table is there, every check of it passes and, in a build, it is the planned table."""
    flags = {}
    for table in TABLES:
        read = reads.get(table)
        ok = read is not None and read.facts is not None and not table_problems(table, read.facts)
        flags[table] = bool(ok) and (planned is None or not differs(read, planned[table]))
    return flags


def build_inner(ops: Operations, head: str) -> Outcome:
    source = ops.read_input(head)
    problems = input_problems(source.facts)
    if problems:
        raise SilverSignalsError(*problems)
    measure = ops.plan_measure(head)
    chain = Chain(head, head)
    held_measure = ops.read_table(head, MEASURE, False)
    if differs(held_measure, measure.fingerprint):
        chain.write(ops, MEASURE, held_measure is not None)
    first = ops.read_table(chain.on, MEASURE, True)
    problems = read_problems({MEASURE: first})
    if not problems and differs(first, measure.fingerprint):
        problems = [f"readback_differs: {FLAG}"]
    if problems:
        raise SilverSignalsError(*problems)
    labels = ops.plan_labels(chain.on)
    planned = {MEASURE: measure.fingerprint, **labels}
    held = {table: ops.read_table(chain.on, table, False) for table in LABEL_TABLES}
    for table in LABEL_TABLES:
        if differs(held[table], labels[table]):
            chain.write(ops, table, held[table] is not None)
    at = chain.on
    if ops.read_input(at) != source:
        raise SilverSignalsError(f"source_changed: {FLAG}")
    reads = read_tables(ops, at, True)
    problems = read_problems(reads)
    differing = any(reads[table] is not None and differs(reads[table], planned[table]) for table in TABLES)
    if differing and not problems:
        problems.append(f"readback_differs: {FLAG}")
    if problems:
        raise SilverSignalsError(*problems)
    if ops.head() != at:
        raise SilverSignalsError(BRANCH_MOVED)
    written = chain.written
    flags = passed_flags(reads, planned)
    return Outcome(head, written, at, source, {t: r for t, r in reads.items() if r is not None}, measure.rows, flags)


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
        problems = read_problems(reads)
        if problems:
            raise SilverSignalsError(*problems)
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome(head, None, head, source, tables, (), passed_flags(reads, None))
    finally:
        ops.release()
