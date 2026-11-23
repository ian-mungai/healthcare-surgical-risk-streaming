"""The Bronze decoder job: the verified case files as four Iceberg tables on a private Nessie branch.

``python -m spark.jobs.bronze_decode [write] --branch <name> --manifest-reference <file> [--base <name>] [--cases <n,n,...>] [--publish]``
``    [--parallelism <n>] [--passes grouped|per_track] [--landed-folder <dir>]``
``python -m spark.jobs.bronze_decode verify --branch <name>``
``python -m spark.jobs.bronze_decode publish --branch <name> [--base <name>]``

``write`` (the default) decodes and checks. ``verify`` runs the checks of step 6 on a branch that exists and writes nothing. ``publish`` runs them on the head
of the branch and merges that head into ``--base`` only when every check passed (step 7); ``write --publish`` does the whole sequence in one job.

The steps of ``write``, in order:

1. The gate (``scripts/ingestion/gate.py``) confirms that the object version of the clinical table and of each wanted case file is the version of the passed
   manifest, before any payload is read. A refusal ends the job with nothing written.
2. Each case file is read by its bound version, checked against the manifest's SHA-256 and judged for structural corruption (``spark/bronze/structure.py``):
   the packet walk, the library's header, and the library's records of every track against the walk's counts. The records are read by the passes of the
   decode and dropped, so no row is written. One refused case ends the job, naming each such case, before the branch exists.
3. The branch is created from ``--base`` (default ``main``). A branch that exists is refused. Nothing is written to the base before step 7.
4. Each table is written in one pass over the cases: ``tracks``, ``numeric_observations``, ``string_events`` and ``waveform_seconds``. A worker reads
   one case file at a time, and Spark takes the rows as they are made. ``--passes`` chooses how a case file is read (``spark/bronze/plan.py``):
   grouped, the default, reads all the numeric tracks of a case in one pass and each waveform track in a pass of its own; ``per_track`` reads every
   track by itself. ``--parallelism`` is the number of cases decoded at once and each of them holds a pass's records in memory.
5. Each table's row count and a checksum of every row are read back, so a rerun can be compared with this run.
6. The column-level checks run in Great Expectations on the four tables as they are at the head of the branch (``spark/bronze/checks.py`` lists them and
   ``quality.py`` runs them). Guard ``key``: the key columns are never null and the key is unique. Guard ``structure``: every other column is never null, a
   lineage hash has the form of a SHA-256, an occurrence is not negative, a second of a waveform holds a sample and a track's kind and types are ones the file
   format defines. No check judges a published value: a noisy or out-of-range reading is kept. A failed check ends the job with exit code 1 and nothing merged.
7. With ``--publish``, or in ``publish``, the head that was checked is merged into the base (``spark/bronze/publication.py``), once, and only when the branch
   still has that head. The result names the merge's hash and the snapshot ID of each table on the base.

It prints one line that starts with ``RESULT `` and holds the branch, its new hash, for each table its snapshot ID, row count and checksum, the checks with
their counts, the publication when there is one, and the facts of the run: the memory limit, the peak memory of the container and the peak memory of the largest
worker, and how many times the library read the files for each table (``library_reads``). Settings come from the environment: ``NESSIE_URI``,
``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse and the bucket's reads, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``. Without
``--landed-folder`` the case files are read from the bucket that the manifest reference names, with boto3.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestError, ManifestRef
from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.bronze.checks import Verdict
from spark.bronze.commands import branch_report, command_problem, exit_code, failed_text
from spark.bronze.inputs import InputError, StoreSpec, verify_inputs
from spark.bronze.passes import MaxParameter, SumByName, partition_problems, partition_rows
from spark.bronze.plan import GROUPED, MODES
from spark.bronze.publication import MergeOutcomeOpen, NessiePublisher, PublicationError
from spark.bronze.quality import QualityError, run_checks
from spark.bronze.readers import caseend_times, open_store
from spark.bronze.rows import StructureError
from spark.bronze.tables import KIND_TABLE, NAMESPACE, TABLES, TableSpec
from spark.catalog import required_environment
from spark.session import build_session, start_deadline
from spark.settings import BYTES_PER_MIB, SettingsError, read_memory_limit_mib

JOB = "bronze_decode"
RESULT_PREFIX = "RESULT "
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 3600.0
PASSES = ("tracks", "numeric", "string", "waveform")
CGROUP_PEAK_FILES = (Path("/sys/fs/cgroup/memory.peak"), Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes"))
BUCKET_ENVIRONMENT = "BRONZE_LANDED_BUCKET"
COMMANDS = ("write", "verify", "publish")


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog=f"python -m spark.jobs.{JOB}", description="Decode the verified case files into the Bronze tables on a private Nessie branch."
    )
    command.add_argument("command", nargs="?", choices=COMMANDS, default="write", help="write (the default), verify the checks of a branch or publish a branch")
    command.add_argument("--branch", required=True, help="the private branch (write: to create from the base and write to)")
    command.add_argument("--base", default="main", help="write: the branch to create the private branch from. publish: the branch to merge into")
    command.add_argument("--manifest-reference", type=Path, help="write: the file that holds the reference of the passed manifest")
    command.add_argument("--publish", action="store_true", help="write: merge the branch into the base when every check passed")
    command.add_argument("--cases", default="", help="the case numbers to decode, separated by commas; every case file of the manifest when empty")
    command.add_argument(
        "--passes",
        choices=MODES,
        default=GROUPED,
        help="how a case file is read: all numeric tracks in one pass (grouped) or every track in a pass of its own (per_track)",
    )
    command.add_argument("--parallelism", type=int, default=1, help="how many cases are decoded at once")
    command.add_argument("--landed-folder", default="", help="a folder of object versions that stands in for the bucket, for a local run")
    command.add_argument("--read-log", default="", help="with --landed-folder: a file that gets the key and version of every object read")
    return command


def peak_memory_mib() -> int | None:
    """The peak memory of this container since it started, from its cgroup, or ``None`` when the cgroup does not say."""
    for path in CGROUP_PEAK_FILES:
        try:
            return int(path.read_text().strip()) // BYTES_PER_MIB
        except OSError, ValueError:
            continue
    return None


def write_table(session: Any, rows: Any, table: TableSpec) -> None:
    frame = session.createDataFrame(rows, schema=table.ddl, verifySchema=False)
    frame.writeTo(table.full_name).tableProperty("format-version", "2").createOrReplace()


def table_facts(session: Any, table: TableSpec) -> dict[str, Any]:
    """The snapshot, the row count, the number of keys used by more than one row and the checksum of every row of a table on the session's branch.

    The duplicate count is a fact of the run. The check that stops the job on a repeated key is the key check of ``spark/bronze/checks.py``.
    """
    from pyspark.sql import functions

    frame = session.table(table.full_name)
    hashed = functions.xxhash64(*(frame[name] for name, _ in table.columns)).cast("decimal(38, 0)")
    counted = frame.agg(functions.count(functions.lit(1)).alias("rows"), functions.sum(hashed).cast("string").alias("checksum")).collect()[0]
    duplicated = frame.groupBy(*table.key).count().filter("count > 1").count()
    loaded = session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session._jsparkSession, table.full_name)
    return {
        "snapshot_id": int(loaded.currentSnapshot().snapshotId()),
        "rows": int(counted["rows"]),
        "checksum": counted["checksum"] or "0",
        "duplicate_keys": int(duplicated),
    }


def decode(arguments: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    reference = ManifestRef.from_json(arguments.manifest_reference.read_text())
    spec = (
        StoreSpec("folder", reference.bucket, arguments.landed_folder, "", arguments.read_log)
        if arguments.landed_folder
        else StoreSpec("s3", reference.bucket, "", required_environment("AWS_REGION"))
    )
    store = open_store(spec)
    wanted = [int(text) for text in arguments.cases.split(",") if text] or None
    inputs = verify_inputs(store, reference, wanted)
    caseends = caseend_times(store, inputs)
    client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
    client.config()
    session = build_session(JOB, arguments.base)
    try:
        slices = max(1, min(arguments.parallelism, len(inputs.cases)))
        cases = session.sparkContext.parallelize(inputs.cases, slices)
        verdicts = cases.mapPartitions(partition_problems(spec, caseends, arguments.passes)).collect()
        problems = sorted((case, problem) for case, problem in verdicts if problem is not None)
        if problems:
            raise StructureError("; ".join(problem for _, problem in problems if problem))
        checked = time.monotonic()
        client.create_branch(arguments.branch, arguments.base)
    finally:
        session.stop()
    session = build_session(JOB, arguments.branch)
    try:
        peaks = session.sparkContext.accumulator(0, MaxParameter())
        reads = session.sparkContext.accumulator({}, SumByName())
        cases = session.sparkContext.parallelize(inputs.cases, slices)
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{NAMESPACE}")
        seconds: dict[str, float] = {}
        for kind in PASSES:
            begun = time.monotonic()
            write_table(session, cases.mapPartitions(partition_rows(kind, spec, peaks, reads, arguments.passes)), KIND_TABLE[kind])
            seconds[kind] = round(time.monotonic() - begun, 1)
        tables = {table.name: table_facts(session, table) for table in TABLES}
        versions = {"spark": str(session.version), "python": platform.python_version()}
    finally:
        session.stop()
    return {
        "branch": arguments.branch,
        "branch_hash": client.reference(arguments.branch).hash,
        "base": arguments.base,
        "cases": [file.case_id for file in inputs.cases],
        "manifest": {"key": reference.key, "version_id": reference.version_id, "sha256": reference.sha256},
        "tables": tables,
        "parallelism": slices,
        "passes": arguments.passes,
        "seconds": {"check": round(checked - started, 1), **seconds, "total": round(time.monotonic() - started, 1)},
        "memory_limit_mib": read_memory_limit_mib(),
        "container_peak_mib": peak_memory_mib(),
        "largest_worker_peak_mib": int(peaks.value),
        "library_reads": dict(sorted(reads.value.items())),
        "versions": versions,
        "uid": os.getuid(),
    }


def branch_checker(branch: str) -> Callable[[str], Verdict]:
    """The checks of ``spark/bronze/checks.py`` run in a session whose catalog is pinned to the hash that is checked."""

    def run(branch_hash: str) -> Verdict:
        session = build_session(JOB, branch, branch_hash)
        try:
            return run_checks(session)
        finally:
            session.stop()

    return run


def nessie_client() -> NessieClient:
    return NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))


def run_command(arguments: argparse.Namespace) -> dict[str, Any]:
    """``write``: the decode, then the checks (and the merge with ``--publish``). ``verify`` and ``publish`` work on a branch that exists."""
    problem = command_problem(
        arguments.command, arguments.manifest_reference is not None, bool(arguments.cases), bool(arguments.landed_folder), arguments.publish
    )
    if problem:
        raise SettingsError(problem)
    result = decode(arguments) if arguments.command == "write" else {"branch": arguments.branch}
    publishing = arguments.command == "publish" or arguments.publish
    keys = [f"{NAMESPACE}.{table.name}" for table in TABLES]
    report = branch_report(NessiePublisher(nessie_client()), branch_checker(arguments.branch), arguments.branch, arguments.base, keys, publishing=publishing)
    return {**result, **report}


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.parallelism < 1:
        sys.stderr.write(f"{JOB}: --parallelism must be at least 1\n")
        return 2
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        result = run_command(arguments)
    except GateRefused as error:
        sys.stderr.write(f"{JOB}: the gate refused the inputs before any payload was read: {error}\n")
        return 1
    except ManifestError as error:
        sys.stderr.write(f"{JOB}: the manifest cannot be trusted: {error}\n")
        return 1
    except (InputError, StructureError) as error:
        sys.stderr.write(f"{JOB}: refused: {error}\n")
        return 1
    except (MergeOutcomeOpen, PublicationError, QualityError) as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 1
    except NessieConflict as error:
        sys.stderr.write(f"{JOB}: Nessie refused the branch {arguments.branch}: {error}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {error}\n")
        return 1
    except ImportError as error:
        sys.stderr.write(f"{JOB}: the image does not hold a package that the checks need ({error.name})\n")
        return 2
    except SettingsError as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    code = exit_code(result)
    if code:
        sys.stderr.write(f"{JOB}: the checks stopped the job on branch {arguments.branch}, and {arguments.base} is unchanged: {failed_text(result)}\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
