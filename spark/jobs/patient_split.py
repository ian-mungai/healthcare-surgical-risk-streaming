"""The patient split table: each patient with its set, frozen before any rule is estimated.

``python -m spark.jobs.patient_split build --branch <name> [--base <name>] [--pinned-release] [--fixed-sample <candidates.tsv>]``
``python -m spark.jobs.patient_split verify --branch <name> [--pinned-release] [--fixed-sample <candidates.tsv>]``

``build`` creates the branch from ``main`` (or from ``--base``) and refuses a branch that exists. It reads ``bronze.clinical_cases`` on the branch, gives each
distinct ``subjectid`` its set from the one assignment rule in ``scripts/sample/split.py`` and writes ``silver.patient_split`` on the branch: ``subjectid``
exactly as published, ``set``, the rule's version and the Bronze snapshot that the rows were built from. It never writes to the base: the merge is the
caller's step after its checks. ``verify`` runs the same checks on the table that a branch holds and writes nothing.

The table is frozen. When the branch already holds the table (merged earlier), a rebuild that would change any patient's set, drop a patient or use another
version of the rule fails before it writes. A rebuild that changes nothing writes nothing. ``--pinned-release`` also refuses a new patient and requires the
pinned release's exact counts. The checks run before the write on the rows to be written, and again on the table read back. They are: no patient in two sets,
every set the one that the rule gives, the shares within one percentage point of 70, 10 and 20 percent, the table built from the branch's current Bronze
snapshot over exactly the patients of Bronze and, with ``--fixed-sample``, each fixed-sample case in the set that its slot expects.

The job prints one line that starts with ``RESULT `` and holds the branch, its hashes, the snapshot IDs and the counts. Settings come from the environment:
``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.session import build_session, required_environment, start_deadline
from spark.settings import SettingsError
from spark.split_table import (
    COLUMNS,
    MESSAGE_LIMIT,
    RULE_VERSION,
    SOURCE_COLUMNS,
    PatientSplitError,
    SampleCase,
    SplitRow,
    build_assignments,
    checks,
    counts_of,
    frozen_problems,
    read_fixed_sample,
    schema_problems,
    source_cases,
    values_query,
)

JOB = "patient_split"
SOURCE_TABLE = "lake.bronze.clinical_cases"
TARGET_NAMESPACE = "silver"
TARGET_TABLE = f"lake.{TARGET_NAMESPACE}.patient_split"
RESULT_PREFIX = "RESULT "
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 900.0


def snapshot_id(session: Any, table: str) -> int | None:
    snapshot = session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session._jsparkSession, table).currentSnapshot()
    return None if snapshot is None else int(snapshot.snapshotId())


def read_source(session: Any) -> tuple[dict[int, str], set[str], int]:
    """The cases and patients of ``bronze.clinical_cases`` on the session's branch and the snapshot that was read."""
    frame = session.table(SOURCE_TABLE)
    fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields if field.name in dict(SOURCE_COLUMNS)]
    problems = schema_problems(sorted(fields), sorted(SOURCE_COLUMNS), f"source table {SOURCE_TABLE}")
    if problems:
        raise PatientSplitError(*problems)
    snapshot = snapshot_id(session, SOURCE_TABLE)
    if snapshot is None:
        raise PatientSplitError(f"the source table {SOURCE_TABLE} has no snapshot: it was never written")
    cases, patients = source_cases((row["caseid"], row["subjectid"]) for row in frame.select("caseid", "subjectid").collect())
    if not patients:
        raise PatientSplitError(f"the source table {SOURCE_TABLE} has no row")
    return cases, patients, snapshot


def read_table(session: Any) -> list[SplitRow]:
    """The rows of the split table on the session's branch, after a check of its columns."""
    frame = session.table(TARGET_TABLE)
    problems = schema_problems([(field.name, field.dataType.simpleString()) for field in frame.schema.fields], COLUMNS, f"table {TARGET_TABLE}")
    if problems:
        raise PatientSplitError(*problems)
    return [SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in frame.collect()]


def table_exists(session: Any) -> bool:
    return bool(session.catalog.tableExists(TARGET_TABLE))


def write_table(session: Any, rows: Sequence[SplitRow], *, exists: bool) -> None:
    """Write the rows as the whole table: create it, or overwrite all of the rows of the table that exists.

    A replace of a table that exists stalls in Spark with the Nessie catalog (it did for minutes, and for good in some runs), so an existing table is
    overwritten in place. One partition gives one data file for a table of a few thousand rows. The overwrite of an unpartitioned table replaces every
    row, and the read-back check of ``build`` proves it.
    """
    frame = session.sql(values_query(rows)).toDF(*(name for name, _ in COLUMNS)).coalesce(1)
    if exists:
        frame.writeTo(TARGET_TABLE).overwritePartitions()
        return
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{TARGET_NAMESPACE}")
    frame.writeTo(TARGET_TABLE).tableProperty("format-version", "2").create()


def build(session: Any, *, pinned: bool, sample: Sequence[SampleCase] | None) -> dict[str, Any]:
    """Build the table on the session's branch, or leave it as it is when nothing changes. Every check runs before the write and on the table read back."""
    source = read_source(session)
    _, patients, snapshot = source
    assignments = build_assignments(patients)
    rows = [SplitRow(subjectid, assignments[subjectid], RULE_VERSION, snapshot) for subjectid in sorted(assignments, key=int)]
    exists = table_exists(session)
    existing = read_table(session) if exists else []
    problems = frozen_problems(existing, assignments, pinned=pinned) + checks(rows, source, sample, pinned=pinned)
    if problems:
        raise PatientSplitError(*problems)
    unchanged = bool(existing) and {(row.subjectid, row.set, row.source_snapshot_id) for row in existing} == {
        (row.subjectid, row.set, row.source_snapshot_id) for row in rows
    }
    if not unchanged:
        write_table(session, rows, exists=exists)
    written = read_table(session)
    problems = checks(written, source, sample, pinned=pinned)
    if sorted(written, key=lambda row: int(row.subjectid)) != rows:
        problems.append("the table read back differs from the rows that were written")
    if problems:
        raise PatientSplitError(*problems)
    return facts(session, written, source, sample, outcome="unchanged" if unchanged else "written")


def verify(session: Any, *, pinned: bool, sample: Sequence[SampleCase] | None) -> dict[str, Any]:
    """Run every check on the table that the session's branch holds."""
    source = read_source(session)
    rows = read_table(session)
    problems = checks(rows, source, sample, pinned=pinned)
    if problems:
        raise PatientSplitError(*problems)
    return facts(session, rows, source, sample, outcome="verified")


def facts(
    session: Any, rows: Sequence[SplitRow], source: tuple[dict[int, str], set[str], int], sample: Sequence[SampleCase] | None, outcome: str
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "table": TARGET_TABLE.removeprefix("lake."),
        "snapshot_id": snapshot_id(session, TARGET_TABLE),
        "source_table": SOURCE_TABLE.removeprefix("lake."),
        "source_snapshot_id": source[2],
        "rule_version": RULE_VERSION,
        "patients": len(rows),
        "cases": len(source[0]),
        "counts": counts_of(rows),
        "fixed_sample_cases_checked": None if sample is None else len(sample),
    }


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the patient split table on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the table on it"), ("verify", "check the table that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="require the pinned release's exact counts and refuse a new patient")
        mode.add_argument("--fixed-sample", type=Path, help="the candidate file of the fixed sample, to check each case's set")
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
        base_hash = client.reference(arguments.base).hash if arguments.mode == "build" else client.reference(arguments.branch).hash
        if arguments.mode == "build":
            client.create_branch(arguments.branch, arguments.base)
        session = build_session(JOB, arguments.branch)
        run = build if arguments.mode == "build" else verify
        result = {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base_hash,
            "pinned_release": arguments.pinned_release,
            **run(session, pinned=arguments.pinned_release, sample=sample),
        }
        result["branch_hash"] = client.reference(arguments.branch).hash
    except PatientSplitError as error:
        shown = error.problems[:MESSAGE_LIMIT]
        for problem in shown:
            report(problem)
        if len(error.problems) > len(shown):
            report(f"and {len(error.problems) - len(shown)} more")
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
