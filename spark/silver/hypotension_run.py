"""The order of a build and of a verify of the pressure series and the event table, in plain data with no Spark.

The job ``spark/jobs/silver_hypotension.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in
steps. The order is that of ``spark/silver/run.py``: the branch is resolved to one hash, the input is read and checked at it before anything is planned or
written, each table is compared by its fingerprint with the table that the branch holds and a table that is already the planned one is not written again, a
write is made only while the branch still has the checked hash, each changed table is one commit whose hash comes from the branch's log, and the input and
both tables are read again at the hash of the last write. The read-back must be the planned tables, with every check passing.

The events are outcomes of the warning, and a held-out patient's outcome is never counted: the result holds the row count and the causes of the series, which
are inputs, and only a digest of the event table, with no row count and no cause.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.hypotension_checks import CHECK_NAMES, EventFacts, InputFacts, SeriesFacts, event_problems, input_problems, series_problems
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.split_table import unit

SERIES = "art_mbp_1s"
EVENTS = "hypotension_events"
TABLES = (SERIES, EVENTS)


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of the input at one hash gives: the counts of the checks and the snapshot IDs of the two tables it reads."""

    facts: InputFacts
    snapshots: tuple[int | None, int | None]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A table at one hash: its fingerprint, its snapshot ID and, when asked for, the counts of its checks."""

    fingerprint: Fingerprint
    snapshot_id: int | None
    series: SeriesFacts | None = None
    events: EventFacts | None = None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps, as those of ``spark.silver.run.Operations``: every read is at the hash that is given and ``plan`` keeps the frames for ``write``."""

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
    outcome: str
    checked_hash: str
    write_hash: str | None
    branch_hash: str
    source: InputRead
    tables: dict[str, TableRead]
    fingerprints: dict[str, Fingerprint]

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, the series' counts and the checks that ran, and for the events only a digest."""
        series = self.tables[SERIES].series
        return {
            "outcome": self.outcome,
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(("waveform_seconds", "clinical_cases"), self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "rows": {SERIES: self.tables[SERIES].fingerprint.rows},
            "causes": {SERIES: dict(series.causes) if series else {}},
            "fingerprints": {SERIES: [self.fingerprints[SERIES].rows, self.fingerprints[SERIES].digest], EVENTS: [self.fingerprints[EVENTS].digest]},
            "checks_run": list(CHECK_NAMES),
        }


def table_problems(source: InputRead, reads: dict[str, TableRead | None]) -> list[str]:
    """The tables that are not there, or else the problems of both tables."""
    missing = sum(1 for read in reads.values() if read is None)
    if missing:
        return [f"table_missing: {unit(missing, 'table')}"]
    series, events = reads[SERIES], reads[EVENTS]
    if series is None or events is None or series.series is None or events.events is None:
        return [f"table_missing: {unit(1, 'table')}"]
    return series_problems(series.series, source.facts.art_seconds) + event_problems(events.events)


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


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
    if ops.read_input(at) != source:
        raise SilverSignalsError(f"source_changed: {unit(1, 'read')} of the input differs")
    reads = read_tables(ops, at, True)
    problems = table_problems(source, reads)
    differing = len([table for table in TABLES if reads[table] is not None and differs(reads[table], planned[table])])
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
    """Run every check on the tables that the branch holds at ``head``, and confirm that the branch still has ``head``. Nothing is written."""
    try:
        source = ops.read_input(head)
        reads = read_tables(ops, head, True)
        problems = input_problems(source.facts) + table_problems(source, reads)
        if problems:
            raise SilverSignalsError(*problems)
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome("verified", head, None, head, source, tables, {table: read.fingerprint for table, read in tables.items()})
    finally:
        ops.release()
