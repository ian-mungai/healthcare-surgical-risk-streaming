"""The Silver pressure series and hypotension events: ``silver.art_mbp_1s`` and ``silver.hypotension_events``.

``python -m spark.jobs.silver_hypotension build --branch <name> [--base <name>]``
``python -m spark.jobs.silver_hypotension verify --branch <name>``

``build`` creates the branch from ``main`` (or from ``--base``, which must hold ``silver.waveform_seconds``), refuses a branch that exists and takes the base
hash from the answer that created it. It reads ``silver.waveform_seconds``, ``bronze.clinical_cases`` and ``silver.patient_split`` at that hash and writes the
two tables on the branch (the tables are in ``spark/silver/tables.py``, the rules in ``features/pressure_series.py`` and ``features/hypotension.py``). It never
writes to the base: the merge is the caller's step after its checks. ``verify`` runs the checks on the tables that a branch holds and writes nothing. The order,
the freeze of a rerun that changes nothing, the writes and the read-back are in ``spark/silver/hypotension_run.py``; the command line, its codes and its exit
codes are those of ``spark/silver/cli.py``. The settings come from the environment, as the Silver signals job's do.

The test set is sealed. No rule is estimated from data. The patient split is read after the check of who owns each case and only to find the development cases
(training and validation): it is trusted after its own rule check, which comes first, and the events are made for those cases only. The events are outcomes,
so the result holds the snapshot IDs, one pass flag for each table and a digest of the event table (no row count; the events are of the development cases
only), and every check is a flag, and so is every refusal. It never names a count of seconds or of events, a case, a patient, a set, a time or a value.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient
from spark.jobs.patient_split import WRITE_CATALOG, Lake
from spark.session import build_session
from spark.silver import hypotension_frames as frames
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run
from spark.silver.frames import fingerprint as fingerprint_of
from spark.silver.frames import functions
from spark.silver.hypotension_checks import InputFacts, split_rule_problems
from spark.silver.hypotension_run import EVENTS, SERIES, Digest, InputRead, Operations, TableRead, run_build, run_verify
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.silver.tables import HYPOTENSION_TABLES, NAMESPACE, WAVEFORM
from spark.split_cli import open_client, read_settings, start_deadline
from spark.split_table import COLUMNS as SPLIT_COLUMNS
from spark.split_table import PatientSplitError, SplitRow, schema_problems, unit

JOB = "silver_hypotension"
WAVEFORM_TABLE = f"{NAMESPACE}.{WAVEFORM.name}"
CLINICAL_TABLE = "bronze.clinical_cases"
CLINICAL_COLUMNS = (("caseid", "string"), ("subjectid", "string"), ("opstart", "string"), ("opend", "string"))
SPLIT_TABLE = f"{NAMESPACE}.patient_split"


class HypotensionLake(Lake):
    """The Spark steps of a build, on one branch, over the read-only catalogs and the write catalog of the patient split's ``Lake``."""

    def __init__(self, session: Any, client: NessieClient, branch: str, settings: dict[str, str]) -> None:
        super().__init__(session, client, branch, settings)
        self.planned: dict[str, Any] = {}

    def prepare(self, checked: str) -> str:
        """The namespace commit of ``Lake``, whose refusal for a branch that moved is this job's refusal ``branch_moved``."""
        try:
            return super().prepare(checked)
        except PatientSplitError:
            raise SilverSignalsError(BRANCH_MOVED) from None

    def fields_of(self, frame: Any) -> list[tuple[str, str]]:
        return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]

    def source_frame(self, at: str, name: str) -> tuple[Any, str]:
        table = f"{self.catalog(at)}.{name}"
        if not self.session.catalog.tableExists(table):
            raise SilverSignalsError(f"source_table: {unit(0, 'table')}")
        return self.session.table(table), table

    def waveform(self, at: str) -> tuple[Any, str]:
        frame, table = self.source_frame(at, WAVEFORM_TABLE)
        problems = schema_problems(self.fields_of(frame), WAVEFORM.columns, f"{WAVEFORM.name} table")
        if problems:
            raise SilverSignalsError(*problems)
        return frame, table

    def clinical(self, at: str) -> tuple[Any, str]:
        frame, table = self.source_frame(at, CLINICAL_TABLE)
        held = dict(self.fields_of(frame))
        problems = schema_problems([(name, held.get(name, "")) for name, _ in CLINICAL_COLUMNS], CLINICAL_COLUMNS, "clinical table")
        if problems:
            raise SilverSignalsError(*problems)
        return frame, table

    def split(self, at: str) -> tuple[Any, str]:
        frame, table = self.source_frame(at, SPLIT_TABLE)
        problems = schema_problems(self.fields_of(frame), SPLIT_COLUMNS, "split table")
        if problems:
            raise SilverSignalsError(*problems)
        return frame, table

    def split_problems(self, frame: Any) -> list[str]:
        """The patient split table checked by the split job's own rules: a set is trusted only when this is empty."""
        rows = [SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in frame.collect()]
        return split_rule_problems(rows)

    def read_input(self, at: str) -> InputRead:
        """The three inputs at ``at``. The checks of who owns a case and of the split come first, and the facts that join a case to a set are read only when
        both passed, so no set is used before it is trusted."""
        waveform, waveform_table = self.waveform(at)
        clinical, clinical_table = self.clinical(at)
        split, split_table = self.split(at)
        snapshots = (self.snapshot_id(waveform_table), self.snapshot_id(clinical_table), self.snapshot_id(split_table))
        if any(snapshot is None for snapshot in snapshots):
            raise SilverSignalsError(f"source_no_snapshot: {unit(sum(1 for snapshot in snapshots if snapshot is None), 'table')}")
        held = (snapshots[0], snapshots[1], snapshots[2])
        if frames.ownership_flag(clinical):
            return InputRead(InputFacts(0, 0, 0, 1, 0, ()), held)
        problems = self.split_problems(split)
        if problems:
            return InputRead(InputFacts(0, 0, 0, 0, 0, tuple(problems)), held)
        return InputRead(frames.input_facts(waveform, clinical, split), held)

    def plan(self, at: str) -> dict[str, Fingerprint | Digest]:
        """Build both frames at ``at``, keep them on disk for the write and give their fingerprints. The events are made from the planned series, for the
        development cases, and their digest has no row count."""
        disk = importlib.import_module("pyspark").StorageLevel.DISK_ONLY
        waveform, _ = self.waveform(at)
        clinical, _ = self.clinical(at)
        split, _ = self.split(at)
        series = frames.series_frame(waveform).persist(disk)
        self.planned = {SERIES: series, EVENTS: frames.events_frame(series, clinical, split).persist(disk)}
        return {SERIES: fingerprint_of(series), EVENTS: frames.digest_of(self.planned[EVENTS])}

    def read_signal_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        name = f"{self.catalog(at)}.{NAMESPACE}.{table}"
        if not self.session.catalog.tableExists(name):
            return None
        frame = self.session.table(name)
        problems = schema_problems(self.fields_of(frame), HYPOTENSION_TABLES[table].columns, f"{table} table")
        if problems:
            raise SilverSignalsError(*problems)
        held: Fingerprint | Digest = fingerprint_of(frame) if table == SERIES else frames.digest_of(frame)
        snapshot = self.snapshot_id(name)
        if not with_facts:
            return TableRead(held, snapshot)
        waveform, _ = self.waveform(at)
        if table == SERIES:
            return TableRead(held, snapshot, series=frames.series_facts(frame, waveform))
        series = self.session.table(f"{self.catalog(at)}.{NAMESPACE}.{SERIES}")
        clinical, _ = self.clinical(at)
        split, _ = self.split(at)
        return TableRead(held, snapshot, events=frames.event_facts(frame, series, clinical, split))

    def write_signal_table(self, table: str, exists: bool) -> None:
        """Write the planned frame as the whole table, as one commit: create it, or overwrite every row of the table that exists, as that job does.

        A replace of a table that exists stalls in Spark with the Nessie catalog, and a dynamic overwrite of partitions does nothing for an empty input (Iceberg
        1.12.0), so an existing table is overwritten in place by the condition that holds for every row.
        """
        writer = self.planned[table].writeTo(f"{WRITE_CATALOG}.{NAMESPACE}.{table}")
        if exists:
            writer.overwrite(functions().lit(True))
            return
        writer.tableProperty("format-version", "2").create()

    def release(self) -> None:
        for frame in self.planned.values():
            frame.unpersist()
        self.planned = {}

    def write_hash_of(self, checked: str, table: str) -> str | None:
        return self.write_commit(checked, (HYPOTENSION_TABLES[table].table_key,))

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
        lake = HypotensionLake(
            session, client, arguments.branch, {"nessie_uri": settings.nessie_uri, "warehouse": settings.warehouse, "region": settings.region}
        )
        client.config()
        building = arguments.mode == "build"
        base = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        outcome = (run_build if building else run_verify)(lake.operations(), base)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base if building else None,
            "job": JOB,
            "tables": [f"{NAMESPACE}.{table}" for table in HYPOTENSION_TABLES],
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute, JOB)


if __name__ == "__main__":
    sys.exit(main())
