"""The Silver outcome labels: ``silver.icu_days_measure``, ``silver.icu_labels`` and ``silver.kidney_injury_labels``.

``python -m spark.jobs.silver_labels build --branch <name> [--base <name>]``
``python -m spark.jobs.silver_labels verify --branch <name>``

``build`` creates the branch from ``main`` (or from ``--base``, which must hold ``silver.cases`` and ``silver.lab_results``), refuses a branch that exists and
takes the base hash from the answer that created it. It reads ``silver.cases``, ``silver.lab_results`` and ``silver.patient_split`` at that hash and writes the
three tables on the branch (the tables are in ``spark/silver/tables.py``, the rules in ``features/icu_label.py`` and ``features/kidney_label.py``). The measure
of ``icu_days`` is written first, from the development patients' values only, and the labels after it. It never writes to the base: the merge is the caller's
step after its checks. ``verify`` runs the checks on the tables that a branch holds and writes nothing. The order, the freeze of a rerun that changes nothing,
the writes and the read-back are in ``spark/silver/label_run.py``; the command line, its codes and its exit codes are those of ``spark/silver/cli.py``. The
settings come from the environment, as the other Silver jobs' do.

The test set is sealed. No rule is estimated from data. A label table has a row for every case, but the patient split is read after the check of who owns each
case, only to find the development cases (training and validation) for the measure, and it is trusted after its own rule check, which comes first. The labels
are outcomes, so the result holds the number of cases, the fingerprints, the snapshot IDs and the development measure, and every check about a label is a flag.
It never names a count of a label, a case, a patient, a set, a time or a value.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient
from spark.jobs.patient_split import WRITE_CATALOG, Lake
from spark.session import build_session
from spark.silver import label_frames as frames
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run
from spark.silver.frames import fingerprint as fingerprint_of
from spark.silver.frames import functions
from spark.silver.hypotension_frames import ownership_flag
from spark.silver.label_checks import ICU, KIDNEY, MEASURE, TABLES, InputFacts, TableFacts, split_rule_problems
from spark.silver.label_run import InputRead, Operations, Plan, TableRead, run_build, run_verify
from spark.silver.run import BRANCH_MOVED, Fingerprint
from spark.silver.tables import LABEL_TABLES, NAMESPACE
from spark.silver_cases import CASE_COLUMNS, LAB_COLUMNS
from spark.split_cli import open_client, read_settings, start_deadline
from spark.split_table import COLUMNS as SPLIT_COLUMNS
from spark.split_table import PatientSplitError, SplitRow, schema_problems, unit

JOB = "silver_labels"
CASES_TABLE = f"{NAMESPACE}.cases"
LAB_TABLE = f"{NAMESPACE}.lab_results"
SPLIT_TABLE = f"{NAMESPACE}.patient_split"
CASE_FIELDS = ("caseid", "subjectid", "anestart", "opend", "icu_days")
CASE_TYPES = {column.name: column.type for column in CASE_COLUMNS}
CASES_NEEDED = tuple((name, CASE_TYPES[name]) for name in CASE_FIELDS)
LAB_NEEDED = tuple((column.name, column.type) for column in LAB_COLUMNS)


class LabelsLake(Lake):
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

    def checked_frame(self, at: str, name: str, needed: tuple[tuple[str, str], ...], label: str) -> tuple[Any, str]:
        """A source table whose needed columns are there with their types."""
        frame, table = self.source_frame(at, name)
        held = dict(self.fields_of(frame))
        problems = schema_problems([(column, held.get(column, "")) for column, _ in needed], needed, label)
        if problems:
            raise SilverSignalsError(*problems)
        return frame, table

    def cases(self, at: str) -> tuple[Any, str]:
        return self.checked_frame(at, CASES_TABLE, CASES_NEEDED, "cases table")

    def lab(self, at: str) -> tuple[Any, str]:
        return self.checked_frame(at, LAB_TABLE, LAB_NEEDED, "laboratory table")

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
        cases, cases_table = self.cases(at)
        lab, lab_table = self.lab(at)
        split, split_table = self.split(at)
        snapshots = (self.snapshot_id(cases_table), self.snapshot_id(lab_table), self.snapshot_id(split_table))
        if any(snapshot is None for snapshot in snapshots):
            raise SilverSignalsError(f"source_no_snapshot: {unit(sum(1 for snapshot in snapshots if snapshot is None), 'table')}")
        held = (snapshots[0], snapshots[1], snapshots[2])
        if ownership_flag(cases):
            return InputRead(InputFacts(0, 1, 0, (), 0), held)
        problems = self.split_problems(split)
        if problems:
            return InputRead(InputFacts(0, 0, 0, tuple(problems), 0), held)
        return InputRead(frames.input_facts(cases, split), held)

    def plan(self, at: str) -> Plan:
        """Build the three frames at ``at``, keep them on disk for the write and give their fingerprints and the measure. The measure comes from the
        development patients' values only."""
        disk = importlib.import_module("pyspark").StorageLevel.DISK_ONLY
        cases, _ = self.cases(at)
        lab, _ = self.lab(at)
        split, _ = self.split(at)
        rows = frames.measure_rows(cases, split)
        self.planned = {
            MEASURE: frames.measure_frame(self.session, rows).persist(disk),
            ICU: frames.icu_frame(cases).persist(disk),
            KIDNEY: frames.kidney_frame(cases, lab).persist(disk),
        }
        return Plan({table: fingerprint_of(frame) for table, frame in self.planned.items()}, tuple(rows))

    def read_label_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        name = f"{self.catalog(at)}.{NAMESPACE}.{table}"
        if not self.session.catalog.tableExists(name):
            return None
        frame = self.session.table(name)
        problems = schema_problems(self.fields_of(frame), LABEL_TABLES[table].columns, f"{table} table")
        if problems:
            raise SilverSignalsError(*problems)
        held: Fingerprint = fingerprint_of(frame)
        snapshot = self.snapshot_id(name)
        if not with_facts:
            return TableRead(held, snapshot)
        cases, _ = self.cases(at)
        return TableRead(held, snapshot, self.table_facts(table, frame, cases, at))

    def table_facts(self, table: str, frame: Any, cases: Any, at: str) -> TableFacts:
        if table == ICU:
            return frames.icu_facts(frame, cases)
        if table == KIDNEY:
            lab, _ = self.lab(at)
            return frames.kidney_facts(frame, cases, lab)
        split, _ = self.split(at)
        return frames.measure_facts(frame, self.session, cases, split)

    def write_label_table(self, table: str, exists: bool) -> None:
        """Write the planned frame as the whole table, as one commit: create it, or overwrite every row of the table that exists, as the other Silver jobs do.

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
        return self.write_commit(checked, (LABEL_TABLES[table].table_key,))

    def operations(self) -> Operations:
        return Operations(self.head, self.read_input, self.plan, self.read_label_table, self.write_label_table, self.write_hash_of, self.prepare, self.release)


def execute(arguments: Any) -> dict[str, Any]:
    """Run one call. Every local step comes first: the settings, the Nessie client, the deadline and the Spark session. Then the first call to Nessie, which
    creates the branch (build) or finds it (verify), resolves its hash once and builds or checks the tables at that hash."""
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        start_deadline(settings.deadline_seconds, job=JOB)
        session = build_session(JOB, arguments.branch)
        lake = LabelsLake(session, client, arguments.branch, {"nessie_uri": settings.nessie_uri, "warehouse": settings.warehouse, "region": settings.region})
        client.config()
        building = arguments.mode == "build"
        base = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        outcome = (run_build if building else run_verify)(lake.operations(), base)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base if building else None,
            "job": JOB,
            "tables": [f"{NAMESPACE}.{table}" for table in TABLES],
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute, JOB)


if __name__ == "__main__":
    sys.exit(main())
