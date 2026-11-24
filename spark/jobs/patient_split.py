"""The patient split table: each patient with its set, frozen before any rule is estimated.

``python -m spark.jobs.patient_split build --branch <name> [--base <name>] [--pinned-release] [--fixed-sample <candidates.tsv>]``
``python -m spark.jobs.patient_split verify --branch <name> [--pinned-release] [--fixed-sample <candidates.tsv>]``

``build`` creates the branch from ``main`` (or from ``--base``), refuses a branch that exists and takes the base hash from the answer that created it. It
reads ``bronze.clinical_cases``, gives each distinct ``subjectid`` its set from the one assignment rule in ``scripts/sample/split.py`` and writes
``silver.patient_split`` on the branch: ``subjectid`` exactly as published, ``set``, the rule's version and the Bronze snapshot that the rows were built
from. It never writes to the base: the merge is the caller's step after its checks. ``verify`` runs the same checks on the table that a branch holds and
writes nothing.

The branch is resolved to one hash. Every read and check is at that hash (a read-only catalog pinned to it) and the write is made only while the branch
still has it. The hash of the write is taken from the branch's log: the head, one commit directly on the checked hash, changing only the table. A new
table's namespace is a commit of its own, made only after every check passed and the head was confirmed, so a failed check or a moved branch writes
nothing. The table is read again at that hash, and the job ends with ``branch_moved`` when the branch no longer has the hash that it checked.
The result names that hash, the hash of the write and nothing derived from the rows.
Every local input is validated before the first call to Nessie, so a bad input never leaves a branch that blocks the retry.

The table is frozen. When the branch already holds the table (merged earlier), that table must be valid by itself (rows, no patient twice, one rule
version) and a rebuild that would change a set, drop a patient or use another rule version fails before it writes. A rebuild that changes nothing writes
nothing. ``--pinned-release`` is the full cohort: it requires the shares of 70, 10 and 20 percent within one percentage point and the exact counts, and
refuses a new patient. ``--fixed-sample`` takes the complete tracked candidate file, refuses any other, and requires every case of it in Bronze in the
set that its slot expects. The result names the checks that ran.

The test set is sealed: a message or the result holds codes, counts and hashes, and never a patient, a case or a set beside one.
The command line prints fixed codes only (``usage``, ``branch_missing``, ``catalog_error``, ``settings``) and the codes of a failed check.
Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from scripts.stack.nessie import NessieClient
from spark.session import build_session
from spark.settings import CATALOG
from spark.split_cli import open_client, read_settings, run, start_deadline
from spark.split_table import (
    BRANCH_MOVED,
    COLUMNS,
    SOURCE_COLUMNS,
    Operations,
    PatientSplitError,
    Source,
    SplitRow,
    TableState,
    catalog_conf,
    read_fixed_sample,
    run_build,
    run_verify,
    schema_problems,
    source_cases,
    unit,
    values_query,
)
from spark.split_table import write_commit as write_commit_of

JOB = "patient_split"
SOURCE_NAME = "bronze.clinical_cases"
TARGET_NAMESPACE = "silver"
TARGET_NAME = f"{TARGET_NAMESPACE}.patient_split"
TABLE_KEY = (TARGET_NAMESPACE, "patient_split")
WRITE_CATALOG = CATALOG


class Lake:
    """The Spark steps of a build, on one branch. A read is made through a catalog that is pinned to a hash and cannot write; the write goes through the
    catalog of the branch itself. ``settings`` holds ``nessie_uri``, ``warehouse`` and ``region``."""

    def __init__(self, session: Any, client: NessieClient, branch: str, settings: dict[str, str]) -> None:
        self.session = session
        self.client = client
        self.branch = branch
        self.settings = settings
        self.pinned: set[str] = set()

    def catalog(self, at: str) -> str:
        """The name of a read-only catalog on the branch at the hash ``at``, registered on first use."""
        name = f"pin_{at[:20]}"
        if name not in self.pinned:
            for key, value in catalog_conf(name, branch=self.branch, at=at, **self.settings).items():
                self.session.conf.set(key, value)
            self.pinned.add(name)
        return name

    def head(self) -> str:
        return self.client.reference(self.branch).hash

    def write_commit(self, checked: str, allowed: tuple[tuple[str, ...], ...]) -> str | None:
        """The hash of the commit that the last write made, from the branch's log: the head, one commit directly on ``checked``, changing only ``allowed``."""
        log = self.client.request("GET", f"/trees/{quote(self.branch, safe='')}/history", query={"fetch": "ALL", "max-records": "2"}) or {}
        return write_commit_of(log.get("logEntries", []), checked, allowed)

    def prepare(self, checked: str) -> str:
        """Create the namespace of the table when the branch has none, as a commit of its own on ``checked``, and return the hash that the write is made on."""
        if self.client.has_namespace(self.branch, checked, TARGET_NAMESPACE):
            return checked
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {WRITE_CATALOG}.{TARGET_NAMESPACE}")
        made = self.write_commit(checked, ((TARGET_NAMESPACE,),))
        if made is None:
            raise PatientSplitError(BRANCH_MOVED)
        return made

    def write_hash(self, checked: str) -> str | None:
        return self.write_commit(checked, (TABLE_KEY,))

    def snapshot_id(self, table: str) -> int | None:
        snapshot = self.session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(self.session._jsparkSession, table).currentSnapshot()
        return None if snapshot is None else int(snapshot.snapshotId())

    def read_source(self, at: str) -> Source:
        table = f"{self.catalog(at)}.{SOURCE_NAME}"
        if not self.session.catalog.tableExists(table):
            raise PatientSplitError(f"source_table: {unit(0, 'table')}")
        frame = self.session.table(table)
        fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields if field.name in dict(SOURCE_COLUMNS)]
        problems = schema_problems(sorted(fields), sorted(SOURCE_COLUMNS))
        if problems:
            raise PatientSplitError(*problems)
        snapshot = self.snapshot_id(table)
        if snapshot is None:
            raise PatientSplitError(f"source_no_snapshot: {unit(0, 'snapshot')}")
        cases, patients = source_cases((row["caseid"], row["subjectid"]) for row in frame.select("caseid", "subjectid").collect())
        if not patients:
            raise PatientSplitError(f"source_empty: {unit(0, 'row')}")
        return Source(cases, patients, snapshot)

    def read_table(self, at: str) -> TableState | None:
        table = f"{self.catalog(at)}.{TARGET_NAME}"
        if not self.session.catalog.tableExists(table):
            return None
        frame = self.session.table(table)
        problems = schema_problems([(field.name, field.dataType.simpleString()) for field in frame.schema.fields], COLUMNS)
        if problems:
            raise PatientSplitError(*problems)
        rows = [SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in frame.collect()]
        return TableState(rows, self.snapshot_id(table))

    def write(self, rows: Sequence[SplitRow], *, exists: bool) -> None:
        """Write the rows as the whole table, as one commit: create it, or overwrite all of the rows of the table that exists.

        A replace of a table that exists stalls in Spark with the Nessie catalog (it did for minutes, and for good in some runs), so an existing table is
        overwritten in place. One partition gives one data file for a table of a few thousand rows. The overwrite of an unpartitioned table replaces every
        row, and the read-back check of the build proves it. The rows reach Spark as one ``VALUES`` text, built in the JVM, after each value was checked.
        """
        frame = self.session.sql(values_query(rows)).toDF(*(name for name, _ in COLUMNS)).coalesce(1)
        table = f"{WRITE_CATALOG}.{TARGET_NAME}"
        if exists:
            frame.writeTo(table).overwritePartitions()
            return
        frame.writeTo(table).tableProperty("format-version", "2").create()


def execute(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run one call. Every local step comes first: the settings, the Nessie client, the candidate file, the deadline and the Spark session. Then the first call
    to Nessie, which creates the branch (build) or finds it (verify), resolves its hash once and checks or builds the table at that hash."""
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        sample = None if arguments.fixed_sample is None else read_fixed_sample(arguments.fixed_sample)
        start_deadline(settings.deadline_seconds)
        session = build_session(JOB, arguments.branch)
        lake = Lake(session, client, arguments.branch, {"nessie_uri": settings.nessie_uri, "warehouse": settings.warehouse, "region": settings.region})
        client.config()
        building = arguments.mode == "build"
        base = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        operations = Operations(lake.head, lake.read_source, lake.read_table, lake.write, lake.write_hash, lake.prepare)
        outcome = (run_build if building else run_verify)(operations, base, pinned=arguments.pinned_release, sample=sample)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base if building else None,
            "pinned_release": arguments.pinned_release,
            "table": TARGET_NAME,
            "source_table": SOURCE_NAME,
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
