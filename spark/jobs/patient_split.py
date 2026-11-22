"""The patient split table: each patient with its set, frozen before any rule is estimated.

``python -m spark.jobs.patient_split build --branch <name> [--base <name>] [--pinned-release] [--fixed-sample <candidates.tsv>]``
``python -m spark.jobs.patient_split verify --branch <name> [--pinned-release] [--fixed-sample <candidates.tsv>]``

``build`` creates the branch from ``main`` (or from ``--base``), refuses a branch that exists and takes the base hash from the answer that created it. It
reads ``bronze.clinical_cases``, gives each distinct ``subjectid`` its set from the one assignment rule in ``scripts/sample/split.py`` and writes
``silver.patient_split`` on the branch: ``subjectid`` exactly as published, ``set``, the rule's version and the Bronze snapshot that the rows were built
from. It never writes to the base: the merge is the caller's step after its checks. ``verify`` runs the same checks on the table that a branch holds and
writes nothing.

The branch is resolved to one hash. Every read and check is at that hash (a read-only catalog pinned to it), the write is made only while the branch still
has it, the table is read again at the hash of the write, and the job ends with ``branch_moved`` when the branch no longer has the hash that it checked.
The result names that hash.

The table is frozen. When the branch already holds the table (merged earlier), that table must be valid by itself (rows, no patient twice, one rule
version) and a rebuild that would change a set, drop a patient or use another rule version fails before it writes. A rebuild that changes nothing writes
nothing. ``--pinned-release`` is the full cohort: it requires the shares of 70, 10 and 20 percent within one percentage point and the exact counts, and
refuses a new patient. ``--fixed-sample`` takes the complete tracked candidate file, refuses any other, and requires every case of it in Bronze in the
set that its slot expects. The result names the checks that ran.

The test set is sealed: a message or the result holds codes, counts, hashes and a digest, and never a patient, a case or a set beside one.
Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.session import build_session, required_environment, start_deadline
from spark.settings import CATALOG, SettingsError, catalog_options
from spark.split_table import (
    COLUMNS,
    SOURCE_COLUMNS,
    Operations,
    PatientSplitError,
    Source,
    SplitRow,
    TableState,
    read_fixed_sample,
    run_build,
    run_verify,
    schema_problems,
    source_cases,
    unit,
    values_query,
)

JOB = "patient_split"
SOURCE_NAME = "bronze.clinical_cases"
TARGET_NAMESPACE = "silver"
TARGET_NAME = f"{TARGET_NAMESPACE}.patient_split"
WRITE_CATALOG = CATALOG
RESULT_PREFIX = "RESULT "
HASH = re.compile(r"[0-9a-f]{64}")
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 900.0


class Lake:
    """The Spark steps of a build, on one branch. A read is made through a catalog that is pinned to a hash and cannot write; the write goes through the
    catalog of the branch itself."""

    def __init__(self, session: Any, client: NessieClient, branch: str) -> None:
        self.session = session
        self.client = client
        self.branch = branch
        self.pinned: set[str] = set()

    def catalog(self, at: str) -> str:
        """The name of a read-only catalog on the branch at the hash ``at``, registered on first use."""
        if HASH.fullmatch(at) is None:
            raise NessieError("a hash that is not a Nessie hash was given to a read")
        name = f"pin_{at[:20]}"
        if name not in self.pinned:
            options = catalog_options(
                nessie_uri=required_environment("NESSIE_URI"),
                warehouse=required_environment("WAREHOUSE_LOCATION"),
                ref=self.branch,
                region=os.environ.get("AWS_REGION", ""),
            )
            for key, value in options.items():
                self.session.conf.set(key.replace(f"spark.sql.catalog.{WRITE_CATALOG}", f"spark.sql.catalog.{name}", 1), value)
            self.session.conf.set(f"spark.sql.catalog.{name}.ref.hash", at)
            self.pinned.add(name)
        return name

    def head(self) -> str:
        return self.client.reference(self.branch).hash

    def snapshot_id(self, table: str) -> int | None:
        snapshot = self.session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(self.session._jsparkSession, table).currentSnapshot()
        return None if snapshot is None else int(snapshot.snapshotId())

    def read_source(self, at: str) -> Source:
        table = f"{self.catalog(at)}.{SOURCE_NAME}"
        if not self.session.catalog.tableExists(table):
            raise PatientSplitError(f"source_table: {unit(0, 'table')}")
        frame = self.session.table(table)
        fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields if field.name in dict(SOURCE_COLUMNS)]
        problems = schema_problems(sorted(fields), sorted(SOURCE_COLUMNS), "source table")
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
        problems = schema_problems([(field.name, field.dataType.simpleString()) for field in frame.schema.fields], COLUMNS, "split table")
        if problems:
            raise PatientSplitError(*problems)
        rows = [SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in frame.collect()]
        return TableState(rows, self.snapshot_id(table))

    def write(self, rows: Sequence[SplitRow], *, exists: bool) -> None:
        """Write the rows as the whole table: create it, or overwrite all of the rows of the table that exists.

        A replace of a table that exists stalls in Spark with the Nessie catalog (it did for minutes, and for good in some runs), so an existing table is
        overwritten in place. One partition gives one data file for a table of a few thousand rows. The overwrite of an unpartitioned table replaces every
        row, and the read-back check of the build proves it.
        """
        frame = self.session.sql(values_query(rows)).toDF(*(name for name, _ in COLUMNS)).coalesce(1)
        table = f"{WRITE_CATALOG}.{TARGET_NAME}"
        if exists:
            frame.writeTo(table).overwritePartitions()
            return
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {WRITE_CATALOG}.{TARGET_NAMESPACE}")
        frame.writeTo(table).tableProperty("format-version", "2").create()


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the patient split table on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the table on it"), ("verify", "check the table that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="the full cohort: require the shares and the exact counts and refuse a new patient")
        mode.add_argument("--fixed-sample", type=Path, help="the complete tracked candidate file of the fixed sample, to check each case's set")
        if name == "build":
            mode.add_argument("--base", default="main", help="the branch to create the private branch from")
    return command


def report(message: str) -> None:
    sys.stderr.write(f"{JOB}: {message}\n")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    session = None
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        sample = None if arguments.fixed_sample is None else read_fixed_sample(arguments.fixed_sample)
        client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
        client.config()
        building = arguments.mode == "build"
        head = client.create_branch(arguments.branch, arguments.base).hash if building else client.reference(arguments.branch).hash
        session = build_session(JOB, arguments.branch)
        lake = Lake(session, client, arguments.branch)
        operations = Operations(lake.head, lake.read_source, lake.read_table, lake.write)
        outcome = (run_build if building else run_verify)(operations, head, pinned=arguments.pinned_release, sample=sample)
        result = {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": head if building else None,
            "pinned_release": arguments.pinned_release,
            "table": TARGET_NAME,
            "source_table": SOURCE_NAME,
            **outcome.facts(),
        }
    except PatientSplitError as error:
        for problem in error.problems:
            report(problem)
        return 1
    except NessieConflict as error:
        report(f"Nessie refused the branch {arguments.branch}: {error}")
        return 1
    except NessieError as error:
        report(f"cannot work with Nessie: {error}")
        return 1
    except SettingsError as error:
        report(str(error))
        return 2
    finally:
        if session is not None:
            session.stop()
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
