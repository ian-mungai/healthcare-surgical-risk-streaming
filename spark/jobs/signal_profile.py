"""The development signal profile: ``silver.signal_profile``, ``silver.unscored_share`` and ``silver.source_counts``.

``python -m spark.jobs.signal_profile build --branch <name> [--base <name>] [--pinned-release]``
``python -m spark.jobs.signal_profile verify --branch <name> [--pinned-release]``

``build`` creates the branch from ``main`` (or from ``--base``), refuses a branch that exists and takes the base hash from the answer that created it.
It reads ``silver.numeric_observations``, ``silver.waveform_seconds``, ``silver.patient_split``, ``bronze.clinical_cases`` and
``raw_records.track_list`` at that hash and writes the three tables on the branch (the tables are in ``spark/profile/tables.py``). It never writes to
the base: the merge is the caller's step after its checks.
``verify`` runs the checks on the tables that a branch holds and writes nothing. The order, the freeze of a rerun that changes nothing, the writes and the
read-back are in ``spark/profile/run.py``. The command line, its codes and its exit codes are in ``spark/profile/cli.py``. The settings come from the
environment, as the Silver signals job's do.

The profile is of development patients only: the training and validation sets of the patient split. The split is trusted only after its rule check (the one
of the patient split job: every patient once, the rule's set, one rule version, one source snapshot, exactly the patients of ``bronze.clinical_cases``), and no
signal row of a test patient enters a profile. ``--pinned-release`` is the full cohort: the split's shares and counts are required, every prespecified source
count is reproduced from the case and track tables (totals only, never an outcome) and judged, and a count of the full cohort is saved as a flag only. Without
it, nothing is judged. The result holds hashes, snapshot IDs, counts of development data and the names of the counts that do not match, and no case, track, time
or value.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient
from spark.jobs.patient_split import SOURCE_NAME, TARGET_NAME, WRITE_CATALOG, Lake
from spark.profile import frames
from spark.profile.checks import InputFacts, SignalProfileError
from spark.profile.cli import JOB, run
from spark.profile.counts import count_rows
from spark.profile.run import BRANCH_MOVED, COUNTS, PROFILE, SOURCE_NAMES, UNSCORED, InputRead, Operations, TableRead, run_build, run_verify
from spark.profile.tables import PROFILE_TABLES
from spark.session import build_session
from spark.silver import frames as silver_frames
from spark.silver.tables import NAMESPACE, NUMERIC, WAVEFORM
from spark.split_cli import open_client, read_settings, start_deadline
from spark.split_table import (
    COLUMNS,
    SOURCE_COLUMNS,
    PatientSplitError,
    SplitRow,
    schema_problems,
    source_cases,
    source_problems,
    table_problems,
    unit,
)

CLINICAL_COLUMNS = (*SOURCE_COLUMNS, ("caseend", "string"))
TRACK_LIST_COLUMNS = (("caseid", "string"), ("tname", "string"), ("tid", "string"))
EMPTY_FACTS = InputFacts(0, 0, 0, 0)
NO_INPUT_FACTS = "development_empty: 0 cases"
INPUTS = {
    "numeric_observations": (f"{NAMESPACE}.{NUMERIC.name}", NUMERIC.columns, False),
    "waveform_seconds": (f"{NAMESPACE}.{WAVEFORM.name}", WAVEFORM.columns, False),
    "patient_split": (TARGET_NAME, COLUMNS, False),
    "clinical_cases": (SOURCE_NAME, CLINICAL_COLUMNS, True),
    "track_list": ("raw_records.track_list", TRACK_LIST_COLUMNS, True),
}


class ProfileLake(Lake):
    """The Spark steps of a build, on one branch, over the read-only catalogs and the write catalog of the patient split's ``Lake``."""

    def __init__(self, session: Any, client: NessieClient, branch: str, settings: dict[str, str], *, pinned: bool = False) -> None:
        super().__init__(session, client, branch, settings)
        self.pinned_release = pinned
        self.planned: dict[str, Any] = {}
        self.development_ids: list[int] = []
        self.patients: dict[str, int] = {}

    def prepare(self, checked: str) -> str:
        """The namespace commit of the patient split's ``Lake``, whose refusal for a branch that moved is this job's refusal ``branch_moved``."""
        try:
            return super().prepare(checked)
        except PatientSplitError:
            raise SignalProfileError(BRANCH_MOVED) from None

    def fields_of(self, frame: Any) -> list[tuple[str, str]]:
        return [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]

    def input(self, at: str, name: str) -> tuple[Any, str]:
        """One input table at the hash ``at`` and its qualified name, after its columns are checked. The clinical table may hold more columns than it needs."""
        table, columns, subset = INPUTS[name]
        qualified = f"{self.catalog(at)}.{table}"
        if not self.session.catalog.tableExists(qualified):
            raise SignalProfileError(f"source_table: {unit(0, 'table')}")
        frame = self.session.table(qualified)
        fields = self.fields_of(frame)
        if subset:
            fields = sorted(field for field in fields if field[0] in dict(columns))
            columns = tuple(sorted(columns))
        problems = schema_problems(fields, columns, f"{name} table")
        if problems:
            raise SignalProfileError(*problems)
        return frame, qualified

    def inputs(self, at: str) -> dict[str, tuple[Any, str]]:
        return {name: self.input(at, name) for name in INPUTS}

    def development_problems(self, held: dict[str, tuple[Any, str]], snapshots: tuple[int | None, ...]) -> list[str]:
        """The rule check of the split and its match with the clinical cases, and the list of development case numbers when they pass."""
        rows = [SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in held["patient_split"][0].collect()]
        clinical = held["clinical_cases"][0].select("caseid", "subjectid").collect()
        try:
            cases, patients = source_cases((row["caseid"], row["subjectid"]) for row in clinical)
        except PatientSplitError as error:
            return error.problems
        clinical_snapshot = snapshots[SOURCE_NAMES.index("clinical_cases")]
        problems = table_problems(rows, pinned=self.pinned_release) + source_problems(rows, patients, int(clinical_snapshot or 0))
        if problems:
            return problems
        sets = {row.subjectid: row.set for row in rows}
        self.development_ids = sorted(case for case, patient in cases.items() if sets[patient] in frames.DEVELOPMENT_SETS)
        self.patients = {name: sum(1 for row in rows if row.set == name) for name in frames.DEVELOPMENT_SETS}
        return [] if self.development_ids else [NO_INPUT_FACTS]

    def read_input(self, at: str) -> InputRead:
        held = self.inputs(at)
        snapshots = tuple(self.snapshot_id(held[name][1]) for name in SOURCE_NAMES)
        if any(snapshot is None for snapshot in snapshots):
            raise SignalProfileError(f"source_no_snapshot: {unit(sum(1 for snapshot in snapshots if snapshot is None), 'table')}")
        problems = self.development_problems(held, snapshots)
        if problems:
            return InputRead(EMPTY_FACTS, snapshots, tuple(problems))
        facts = frames.input_facts(held["numeric_observations"][0], held["waveform_seconds"][0], self.development_ids)
        return InputRead(facts, snapshots, ())

    def plan(self, at: str) -> dict[str, Any]:
        """Build the three frames at ``at``, keep the two that need a pass over Silver on disk for the write and give their fingerprints."""
        disk = importlib.import_module("pyspark").StorageLevel.DISK_ONLY
        held = self.inputs(at)
        numeric, waveform = held["numeric_observations"][0], held["waveform_seconds"][0]
        clinical, split, track_list = held["clinical_cases"][0], held["patient_split"][0], held["track_list"][0]
        development = frames.development_cases(clinical, split)
        differing = abs(int(development.count()) - len(self.development_ids))
        if differing:
            raise SignalProfileError(f"development_differs: {unit(differing, 'case')}")
        values: dict[str, float | None] = {
            "development_cases": float(len(self.development_ids)),
            "development_patients_training": float(self.patients["training"]),
            "development_patients_validation": float(self.patients["validation"]),
        }
        if self.pinned_release:
            values |= frames.fixed_counts(clinical, track_list)
        self.planned = {
            PROFILE: frames.profile_frame(numeric, waveform, development).persist(disk),
            UNSCORED: frames.unscored_frame(waveform, development).persist(disk),
            COUNTS: frames.counts_frame(self.session, count_rows(values, compared=self.pinned_release)),
        }
        return {table: silver_frames.fingerprint(frame) for table, frame in self.planned.items()}

    def read_profile_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        name = f"{self.catalog(at)}.{NAMESPACE}.{table}"
        if not self.session.catalog.tableExists(name):
            return None
        frame = self.session.table(name)
        problems = schema_problems(self.fields_of(frame), PROFILE_TABLES[table].columns, f"{table} table")
        if problems:
            raise SignalProfileError(*problems)
        fingerprint, snapshot = silver_frames.fingerprint(frame), self.snapshot_id(name)
        if not with_facts:
            return TableRead(fingerprint, snapshot)
        if table == PROFILE:
            return TableRead(fingerprint, snapshot, profile=frames.profile_facts(frame))
        if table == UNSCORED:
            return TableRead(fingerprint, snapshot, unscored=frames.unscored_facts(frame))
        facts, mismatched = frames.counts_facts(frame)
        return TableRead(fingerprint, snapshot, counts=facts, mismatched=mismatched)

    def write_profile_table(self, table: str, exists: bool) -> None:
        """Write the planned frame as the whole table, as one commit: create it, or overwrite every row of the table that exists.

        A replace of a table that exists stalls in Spark with the Nessie catalog, so an existing table is overwritten in place, by the condition that holds for
        every row. That overwrite clears the table when the planned frame is empty too. The read-back of the build proves the result.
        """
        writer = self.planned[table].writeTo(f"{WRITE_CATALOG}.{NAMESPACE}.{table}")
        if exists:
            writer.overwrite(silver_frames.functions().lit(True))
            return
        writer.tableProperty("format-version", "2").create()

    def write_hash_of(self, checked: str, table: str) -> str | None:
        return self.write_commit(checked, (PROFILE_TABLES[table].table_key,))

    def release(self) -> None:
        for frame in self.planned.values():
            frame.unpersist()
        self.planned = {}

    def operations(self) -> Operations:
        return Operations(
            self.head, self.read_input, self.plan, self.read_profile_table, self.write_profile_table, self.write_hash_of, self.prepare, self.release
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
        lake = ProfileLake(
            session,
            client,
            arguments.branch,
            {"nessie_uri": settings.nessie_uri, "warehouse": settings.warehouse, "region": settings.region},
            pinned=arguments.pinned_release,
        )
        client.config()
        building = arguments.mode == "build"
        base = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        outcome = (run_build if building else run_verify)(lake.operations(), base, pinned=arguments.pinned_release)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base if building else None,
            "pinned_release": arguments.pinned_release,
            "job": JOB,
            "tables": [f"{NAMESPACE}.{table}" for table in PROFILE_TABLES],
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
