"""The order of a build and of a verify of the pressure series and the event table, in plain data with no Spark.

The job ``spark/jobs/silver_hypotension.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in
steps. The order is that of ``spark/silver/run.py``: the branch is resolved to one hash, the input is read and checked at it before anything is planned or
written, each table is compared by its fingerprint with the table that the branch holds and a table that is already the planned one is not written again, a
write is made only while the branch still has the checked hash, each changed table is one commit whose hash comes from the branch's log, and the input and
both tables are read again at the hash of the last write. The read-back must be the planned tables, with every check passing.

The test set is sealed. The series holds the held-out cases' seconds and the events are outcomes of the warning, so a count or a digest over the series
changes with a held-out row and no count of events is ever made. The fingerprint of the series, the counts of the checks and the causes stay inside the job.
The result holds the hashes, the snapshot IDs, one pass flag for each table, the checks that ran and a digest of the event table (``Digest``, which has no row
count), which is a measure of the development cases only because the events are made for them only. The checks about events are flags, and every refusal is a
code and a flag, except a count of tables.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spark.silver.checks import FLAG, SilverSignalsError
from spark.silver.hypotension_checks import CHECK_NAMES, EventFacts, InputFacts, SeriesFacts, event_problems, input_problems, series_problems
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.split_table import unit

SERIES = "art_mbp_1s"
EVENTS = "hypotension_events"
TABLES = (SERIES, EVENTS)


@dataclass(frozen=True, slots=True)
class Digest:
    """The sum of a hash of each row of a table, with no row count: equal tables have equal digests, whatever the order of their rows."""

    digest: int


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of the input at one hash gives: the counts and flags of the checks and the snapshot IDs of the three tables it reads (the waveform
    seconds, the clinical table and the patient split)."""

    facts: InputFacts
    snapshots: tuple[int | None, int | None, int | None]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A table at one hash: its fingerprint, its snapshot ID and, when asked for, the counts of its checks."""

    fingerprint: Fingerprint | Digest
    snapshot_id: int | None
    series: SeriesFacts | None = None
    events: EventFacts | None = None


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps, as those of ``spark.silver.run.Operations``: every read is at the hash that is given and ``plan`` keeps the frames for ``write``."""

    head: Callable[[], str]
    read_input: Callable[[str], InputRead]
    plan: Callable[[str], dict[str, Fingerprint | Digest]]
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
    events: Digest
    passed: dict[str, bool]

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, one pass flag for each table, the checks that ran and the digest of the development events.
        It holds no count, no digest of the series and no cause."""
        return {
            "outcome": self.outcome,
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(("waveform_seconds", "clinical_cases", "patient_split"), self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "passed": dict(self.passed),
            "development_events_digest": self.events.digest,
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


def passed_flags(source: InputRead, reads: dict[str, TableRead | None], planned: dict[str, Fingerprint | Digest] | None) -> dict[str, bool]:
    """One flag for each table: the table is there, every check of it passes and, in a build, it is the planned table."""
    series, events = reads[SERIES], reads[EVENTS]
    held = {
        SERIES: series is not None and series.series is not None and not series_problems(series.series, source.facts.art_seconds),
        EVENTS: events is not None and events.events is not None and not event_problems(events.events),
    }
    return {table: held[table] and (planned is None or not differs(reads[table], planned[table])) for table in TABLES}


def events_digest(read: TableRead | None) -> Digest:
    """The digest of the event table that was read, which is of the development cases only."""
    if read is None or not isinstance(read.fingerprint, Digest):
        raise SilverSignalsError(f"table_missing: {unit(1, 'table')}")
    return read.fingerprint


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


def read_checked(ops: Operations, at: str, source: InputRead) -> dict[str, TableRead | None]:
    """Both tables with the counts of their checks. The events are recomputed from the stored series, so the series is read and checked first: a series that
    fails its checks is reported with its own codes and the event table is not read. A series that is not there is named by ``table_problems`` beside the event
    table if that is missing too: the event table is then only looked for, without its facts."""
    series = ops.read_table(at, SERIES, True)
    if series is None or series.series is None:
        return {SERIES: series, EVENTS: ops.read_table(at, EVENTS, False)}
    problems = series_problems(series.series, source.facts.art_seconds)
    if problems:
        raise SilverSignalsError(*problems)
    return {SERIES: series, EVENTS: ops.read_table(at, EVENTS, True)}


def differs(read: TableRead | None, planned: Fingerprint | Digest) -> bool:
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
        raise SilverSignalsError(f"source_changed: {FLAG}")
    reads = read_checked(ops, at, source)
    problems = table_problems(source, reads)
    differing = len([table for table in TABLES if reads[table] is not None and differs(reads[table], planned[table])])
    if differing and not problems:
        problems.append(f"readback_differs: {unit(differing, 'table')}")
    if problems:
        raise SilverSignalsError(*problems)
    if ops.head() != at:
        raise SilverSignalsError(BRANCH_MOVED)
    return Outcome(
        "written" if changed else "unchanged",
        head,
        written,
        at,
        source,
        {table: read for table, read in reads.items() if read is not None},
        events_digest(reads[EVENTS]),
        passed_flags(source, reads, planned),
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
        reads = read_checked(ops, head, source)
        problems = table_problems(source, reads)
        if problems:
            raise SilverSignalsError(*problems)
        if ops.head() != head:
            raise SilverSignalsError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome("verified", head, None, head, source, tables, events_digest(reads[EVENTS]), passed_flags(source, reads, None))
    finally:
        ops.release()
