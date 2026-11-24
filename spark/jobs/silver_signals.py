"""The Silver signal tables: ``silver.numeric_observations`` and ``silver.waveform_seconds``, cleaned at their own grain, each value marked valid or invalid.

``python -m spark.jobs.silver_signals build --branch <name> [--base <name>]``
``python -m spark.jobs.silver_signals verify --branch <name>``

``build`` creates the branch from ``main`` (or from ``--base``), refuses a branch that exists and takes the base hash from the answer that created it.
It reads ``bronze.numeric_observations``, ``bronze.waveform_seconds`` and ``bronze.tracks`` at that hash and writes the two Silver tables on the
branch (the tables, their columns and the rules are in ``spark/silver/tables.py`` and ``features/signal_validity.py``). It never writes to the base:
the merge is the caller's step after its checks. ``verify`` runs the checks on the tables that a branch holds and writes nothing. The order, the
freeze of a rerun that changes nothing, the writes and the read-back are in ``spark/silver/run.py``. The command line, its codes and its exit codes
are in ``spark/silver/cli.py``. The settings come from the environment, as the patient split's do: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``,
``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``.

No rule of the tables is estimated from data, so no patient set is read, and no outcome is read. The test set is sealed and Bronze holds the held-out cases'
signals, so a count or a digest over the rows changes with a held-out row: the result holds hashes, snapshot IDs, the checks that ran and one pass flag for
each table, and no count, digest, case, track, time or value. A refusal is a code and a flag, except a count of tables.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient
from spark.bronze import tables as bronze
from spark.jobs.patient_split import WRITE_CATALOG, Lake
from spark.session import build_session
from spark.silver import frames
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import JOB, run
from spark.silver.run import BRANCH_MOVED, NUMERIC, WAVEFORM, InputRead, Operations, TableRead, run_build, run_verify
from spark.silver.tables import NAMESPACE, SILVER_TABLES
from spark.split_cli import open_client, read_settings, start_deadline
from spark.split_table import PatientSplitError, schema_problems, unit

SOURCES = (bronze.NUMERIC, bronze.WAVEFORM, bronze.TRACKS)


class SignalsLake(Lake):
    """The Spark steps of a build, on one branch, over the read-only catalogs and the write catalog of the patient split's ``Lake``."""

    def __init__(self, session: Any, client: NessieClient, branch: str, settings: dict[str, str]) -> None:
        super().__init__(session, client, branch, settings)
        self.planned: dict[str, Any] = {}

    def prepare(self, checked: str) -> str:
        """The namespace commit of the patient split's ``Lake``, whose refusal for a branch that moved is this job's refusal ``branch_moved``."""
        try:
            return super().prepare(checked)
        except PatientSplitError:
            raise SilverSignalsError(BRANCH_MOVED) from None

    def fields_of(self, frame: Any) -> list[tuple[str, str]]:
        return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]

    def source(self, at: str, spec: bronze.TableSpec) -> tuple[Any, str]:
        table = f"{self.catalog(at)}.{bronze.NAMESPACE}.{spec.name}"
        if not self.session.catalog.tableExists(table):
            raise SilverSignalsError(f"source_table: {unit(0, 'table')}")
        frame = self.session.table(table)
        problems = schema_problems(self.fields_of(frame), spec.columns, f"bronze {spec.name} table")
        if problems:
            raise SilverSignalsError(*problems)
        return frame, table

    def sources(self, at: str) -> dict[str, tuple[Any, str]]:
        return {spec.name: self.source(at, spec) for spec in SOURCES}

    def read_input(self, at: str) -> InputRead:
        held = self.sources(at)
        snapshots = tuple(self.snapshot_id(held[spec.name][1]) for spec in SOURCES)
        if any(snapshot is None for snapshot in snapshots):
            raise SilverSignalsError(f"source_no_snapshot: {unit(sum(1 for snapshot in snapshots if snapshot is None), 'table')}")
        facts = frames.bronze_facts(*(held[spec.name][0] for spec in SOURCES))
        return InputRead(facts, (snapshots[0], snapshots[1], snapshots[2]))

    def plan(self, at: str) -> dict[str, Any]:
        """Build both frames from Bronze at ``at``, keep them on disk for the write and give their fingerprints, which stay inside the run."""
        disk = importlib.import_module("pyspark").StorageLevel.DISK_ONLY
        held = self.sources(at)
        numeric, waveform, tracks = (held[spec.name][0] for spec in SOURCES)
        self.planned = {
            NUMERIC: frames.numeric_frame(numeric).persist(disk),
            WAVEFORM: frames.waveform_frame(waveform, tracks).persist(disk),
        }
        return {table: frames.fingerprint(frame) for table, frame in self.planned.items()}

    def read_signal_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        name = f"{self.catalog(at)}.{NAMESPACE}.{table}"
        if not self.session.catalog.tableExists(name):
            return None
        frame = self.session.table(name)
        problems = schema_problems(self.fields_of(frame), SILVER_TABLES[table].columns, f"{table} table")
        if problems:
            raise SilverSignalsError(*problems)
        fingerprint, snapshot = frames.fingerprint(frame), self.snapshot_id(name)
        if not with_facts:
            return TableRead(fingerprint, snapshot)
        held = self.sources(at)
        if table == NUMERIC:
            return TableRead(fingerprint, snapshot, numeric=frames.numeric_facts(frame, held[bronze.NUMERIC.name][0]))
        return TableRead(fingerprint, snapshot, waveform=frames.waveform_facts(frame, held[bronze.WAVEFORM.name][0], held[bronze.TRACKS.name][0]))

    def write_signal_table(self, table: str, exists: bool) -> None:
        """Write the planned frame as the whole table, as one commit: create it, or overwrite every row of the table that exists.

        A replace of a table that exists stalls in Spark with the Nessie catalog, so an existing table is overwritten in place, by the condition that holds for
        every row. That overwrite clears the table when the planned frame is empty too: a dynamic overwrite of partitions does nothing for an empty input
        (Iceberg 1.12.0), so a table that must become empty would keep its rows. The read-back of the build proves the result.
        """
        target = f"{WRITE_CATALOG}.{NAMESPACE}.{table}"
        writer = self.planned[table].writeTo(target)
        if exists:
            writer.overwrite(frames.functions().lit(True))
            return
        writer.tableProperty("format-version", "2").create()

    def write_hash_of(self, checked: str, table: str) -> str | None:
        return self.write_commit(checked, (SILVER_TABLES[table].table_key,))

    def release(self) -> None:
        for frame in self.planned.values():
            frame.unpersist()
        self.planned = {}

    def operations(self) -> Operations:
        return Operations(
            self.head, self.read_input, self.plan, self.read_signal_table, self.write_signal_table, self.write_hash_of, self.prepare, self.release
        )


def execute(arguments: Any) -> dict[str, Any]:
    """Run one call. Every local step comes first: the settings, the Nessie client, the deadline and the Spark session. Then the first call to Nessie, which
    creates the branch (build) or finds it (verify), resolves its hash once and builds or checks the tables at that hash."""
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        start_deadline(settings.deadline_seconds, job=JOB)
        session = build_session(JOB, arguments.branch)
        lake = SignalsLake(session, client, arguments.branch, {"nessie_uri": settings.nessie_uri, "warehouse": settings.warehouse, "region": settings.region})
        client.config()
        building = arguments.mode == "build"
        base = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        outcome = (run_build if building else run_verify)(lake.operations(), base)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base if building else None,
            "job": JOB,
            "tables": [f"{NAMESPACE}.{table}" for table in SILVER_TABLES],
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
