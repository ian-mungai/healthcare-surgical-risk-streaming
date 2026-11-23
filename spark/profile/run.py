"""The order of a build and of a verify of the signal profile tables, in plain data with no Spark.

The job ``spark/jobs/signal_profile.py`` passes its Spark steps to ``run_build`` and ``run_verify``, so that the order runs in the fast tests on stand-in steps.
It is the order of the Silver signal job (``spark/silver/run.py``) over three tables.

A build resolves the branch to one hash. It reads the inputs at that hash and refuses an input that cannot be profiled (a split that fails its rule check, no
development case, no development signal) before it plans or writes anything. It plans the three tables (each as a frame and a fingerprint) and compares each
with the table that the branch holds. A table whose fingerprint is the planned one is left as it is, so a rerun that changes nothing writes no snapshot. It
writes only while the branch still has the checked hash, creates the namespace as a commit of its own when the branch has none, writes each changed table as one
commit and reads the inputs and the three tables at the hash of the last write. The read-back must be the planned tables, with every check passing. The job
ends only when the branch still has that hash.

A prespecified count that the reproduced count does not equal is a finding, not a failure: its name is in the result (``source_counts.mismatched``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spark.profile.checks import (
    CHECK_NAMES,
    CountsFacts,
    InputFacts,
    ProfileFacts,
    SignalProfileError,
    UnscoredFacts,
    counts_problems,
    profile_problems,
    unscored_problems,
)
from spark.profile.tables import COUNTS as COUNTS_TABLE
from spark.profile.tables import PROFILE as PROFILE_TABLE
from spark.profile.tables import UNSCORED as UNSCORED_TABLE
from spark.silver.run import Fingerprint
from spark.split_table import unit

PROFILE = PROFILE_TABLE.name
UNSCORED = UNSCORED_TABLE.name
COUNTS = COUNTS_TABLE.name
TABLES = (PROFILE, UNSCORED, COUNTS)
SOURCE_NAMES = ("numeric_observations", "waveform_seconds", "patient_split", "clinical_cases", "tracks")
BRANCH_MOVED = "branch_moved: the branch head is not the one that was checked"


@dataclass(frozen=True, slots=True)
class InputRead:
    """What a read of the inputs at one hash gives: the development counts, the snapshot IDs of the five input tables and the problems of the development
    cases (the split's rule check and its match with the clinical cases)."""

    facts: InputFacts
    snapshots: tuple[int | None, ...]
    problems: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TableRead:
    """A table at one hash: its fingerprint, its snapshot ID and, when asked for, the counts of its checks.

    ``mismatched`` names the source counts whose flag is false.
    """

    fingerprint: Fingerprint
    snapshot_id: int | None
    profile: ProfileFacts | None = None
    unscored: UnscoredFacts | None = None
    counts: CountsFacts | None = None
    mismatched: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps. ``head`` is the hash of the branch now and every read is at the hash that is given.

    ``plan`` builds the three frames at that hash, keeps them for ``write`` and gives their fingerprints. ``read_table`` gives ``None`` for a hash that
    holds no such table, and the counts of its checks only when asked. ``write`` writes the planned frame of a table to the branch, over the rows of the table
    that exists or as a new table. ``write_hash`` is given the hash that the write was made on and gives the hash of the write, or ``None`` when the branch
    head is not the one commit of the write directly on it. ``prepare`` creates the namespace when the branch has none, as a commit of its own, and gives the
    hash that the first write is made on. ``release`` frees the planned frames.
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
    outcome: str
    checked_hash: str
    write_hash: str | None
    branch_hash: str
    source: InputRead
    tables: dict[str, TableRead]
    fingerprints: dict[str, Fingerprint]
    pinned: bool

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: hashes, snapshot IDs, counts of development data, the names of the source counts that do not match and the checks
        that ran. A count of the full cohort is a flag in the table and is not in the result."""
        unscored = self.tables[UNSCORED].unscored
        facts = self.source.facts
        return {
            "outcome": self.outcome,
            "checked_hash": self.checked_hash,
            "write_hash": self.write_hash,
            "branch_hash": self.branch_hash,
            "source_snapshots": dict(zip(SOURCE_NAMES, self.source.snapshots, strict=True)),
            "snapshots": {table: read.snapshot_id for table, read in self.tables.items()},
            "rows": {table: read.fingerprint.rows for table, read in self.tables.items()},
            "development": {
                "numeric_rows": facts.numeric_rows,
                "waveform_rows": facts.waveform_rows,
                "tracks": facts.tracks,
                "arterial_cases": facts.arterial_cases,
            },
            "unscored": {"cutoffs": unscored.cutoffs, "waveform": unscored.waveform, "numeric": unscored.numeric, "either": unscored.either}
            if unscored
            else {},
            "source_counts": {"pinned": self.pinned, "mismatched": list(self.tables[COUNTS].mismatched)},
            "fingerprints": {table: [value.rows, value.digest] for table, value in self.fingerprints.items()},
            "checks_run": list(CHECK_NAMES),
        }


def input_problems(source: InputRead) -> list[str]:
    """The problems of the development cases, or else an input that holds no development signal."""
    if source.problems:
        return list(source.problems)
    if source.facts.numeric_rows == 0 and source.facts.waveform_rows == 0:
        return [f"input_empty: {unit(0, 'row')}"]
    return []


def table_problems(source: InputRead, reads: dict[str, TableRead | None], *, pinned: bool) -> list[str]:
    """The tables that are not there, or else the problems of the three tables against the development counts of the input."""
    missing = sum(1 for read in reads.values() if read is None)
    if missing:
        return [f"table_missing: {unit(missing, 'table')}"]
    profile, unscored, counts = reads[PROFILE], reads[UNSCORED], reads[COUNTS]
    if profile is None or unscored is None or counts is None or profile.profile is None or unscored.unscored is None or counts.counts is None:
        return [f"table_missing: {unit(1, 'table')}"]
    return profile_problems(profile.profile, source.facts) + unscored_problems(unscored.unscored, source.facts) + counts_problems(counts.counts, pinned=pinned)


def read_tables(ops: Operations, at: str, with_facts: bool) -> dict[str, TableRead | None]:
    return {table: ops.read_table(at, table, with_facts) for table in TABLES}


def differs(read: TableRead | None, planned: Fingerprint) -> bool:
    """Whether the table that a branch holds is not the planned one: it is not there, or its fingerprint is another."""
    return read is None or read.fingerprint != planned


def build_inner(ops: Operations, head: str, *, pinned: bool) -> Outcome:
    source = ops.read_input(head)
    problems = input_problems(source)
    if problems:
        raise SignalProfileError(*problems)
    planned = ops.plan(head)
    held = read_tables(ops, head, False)
    changed = [table for table in TABLES if differs(held[table], planned[table])]
    at, written = head, None
    if changed:
        if ops.head() != head:
            raise SignalProfileError(BRANCH_MOVED)
        on = ops.prepare(head)
        for table in changed:
            ops.write(table, held[table] is not None)
            written = ops.write_hash(on, table)
            if written is None:
                raise SignalProfileError(BRANCH_MOVED)
            on = written
        at = on
    later = ops.read_input(at)
    if later != source:
        raise SignalProfileError(f"source_changed: {unit(1, 'read')} of the inputs differs")
    reads = read_tables(ops, at, True)
    problems = table_problems(source, reads, pinned=pinned)
    differing = len([table for table in TABLES if reads[table] is not None and differs(reads[table], planned[table])])
    if differing and not problems:
        problems.append(f"readback_differs: {unit(differing, 'table')}")
    if problems:
        raise SignalProfileError(*problems)
    if ops.head() != at:
        raise SignalProfileError(BRANCH_MOVED)
    tables = {table: read for table, read in reads.items() if read is not None}
    return Outcome("written" if changed else "unchanged", head, written, at, source, tables, planned, pinned)


def run_build(ops: Operations, head: str, *, pinned: bool) -> Outcome:
    """Build the tables on the branch whose head is ``head``, or leave them as they are when nothing changes. The planned frames are released in every case."""
    try:
        return build_inner(ops, head, pinned=pinned)
    finally:
        ops.release()


def run_verify(ops: Operations, head: str, *, pinned: bool) -> Outcome:
    """Run every check on the tables that the branch holds at ``head``, and confirm that the branch still has ``head``. Nothing is written."""
    try:
        source = ops.read_input(head)
        reads = read_tables(ops, head, True)
        problems = input_problems(source) + table_problems(source, reads, pinned=pinned)
        if problems:
            raise SignalProfileError(*problems)
        if ops.head() != head:
            raise SignalProfileError(BRANCH_MOVED)
        tables = {table: read for table, read in reads.items() if read is not None}
        return Outcome("verified", head, None, head, source, tables, {table: read.fingerprint for table, read in tables.items()}, pinned)
    finally:
        ops.release()
