"""The Bronze decoder job: the verified case files as four Iceberg tables on a private Nessie branch.

``python -m spark.jobs.bronze_decode --branch <name> --manifest-reference <file> [--base <name>] [--cases <n,n,...>]``
``    [--parallelism <n>] [--passes grouped|per_track] [--landed-folder <dir>]``

The steps, in order:

1. The gate (``scripts/ingestion/gate.py``) confirms that the object version of the clinical table and of each wanted case file is the version of the passed
   manifest, before any payload is read. A refusal ends the job with nothing written.
2. Each case file is read by its bound version, checked against the manifest's SHA-256 and judged for structural corruption (``spark/bronze/structure.py``).
   One corrupt file ends the job, naming each such case, before the branch exists.
3. The branch is created from ``--base`` (default ``main``). A branch that exists is refused. The job never writes to the base: the merge is a later step.
4. Each table is written in one pass over the cases: ``tracks``, ``numeric_observations``, ``string_events`` and ``waveform_seconds``. A worker reads
   one case file at a time, and Spark takes the rows as they are made. ``--passes`` chooses how a case file is read (``spark/bronze/plan.py``):
   grouped, the default, reads all the numeric tracks of a case in one pass and each waveform track in a pass of its own; ``per_track`` reads every
   track by itself. ``--parallelism`` is the number of cases decoded at once and each of them holds a pass's records in memory.
5. Each table's stable key is checked for duplicates, and its row count and a checksum of every row are read back, so a rerun can be compared with this run.

It prints one line that starts with ``RESULT `` and holds the branch, its new hash, for each table its snapshot ID, row count, key-duplicate count and
checksum, and the facts of the run: the memory limit, the peak memory of the container and the peak memory of the largest worker. Settings come from the
environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse and the bucket's reads, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``. Without ``--landed-folder`` the case files are read from the bucket that the manifest reference names, with boto3.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestError, ManifestRef
from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.bronze.inputs import InputError, StoreSpec, verify_inputs
from spark.bronze.passes import MaxParameter, partition_problems, partition_rows
from spark.bronze.plan import GROUPED, MODES
from spark.bronze.readers import caseend_times, open_store
from spark.bronze.rows import StructureError
from spark.bronze.tables import KIND_TABLE, NAMESPACE, TABLES, TableSpec
from spark.session import build_session, required_environment, start_deadline
from spark.settings import BYTES_PER_MIB, SettingsError, read_memory_limit_mib

JOB = "bronze_decode"
RESULT_PREFIX = "RESULT "
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 3600.0
PASSES = ("tracks", "numeric", "string", "waveform")
CGROUP_PEAK_FILES = (Path("/sys/fs/cgroup/memory.peak"), Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes"))
BUCKET_ENVIRONMENT = "BRONZE_LANDED_BUCKET"


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog=f"python -m spark.jobs.{JOB}", description="Decode the verified case files into the Bronze tables on a private Nessie branch."
    )
    command.add_argument("--branch", required=True, help="the private branch to create from the base and write to")
    command.add_argument("--base", default="main", help="the branch to create the private branch from")
    command.add_argument("--manifest-reference", required=True, type=Path, help="the file that holds the reference of the passed manifest")
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
    """The snapshot, the row count, the number of keys used by more than one row and the checksum of every row of a table on the session's branch."""
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
        verdicts = cases.mapPartitions(partition_problems(spec, caseends)).collect()
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
        cases = session.sparkContext.parallelize(inputs.cases, slices)
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{NAMESPACE}")
        seconds: dict[str, float] = {}
        for kind in PASSES:
            begun = time.monotonic()
            write_table(session, cases.mapPartitions(partition_rows(kind, spec, peaks, arguments.passes)), KIND_TABLE[kind])
            seconds[kind] = round(time.monotonic() - begun, 1)
        tables = {table.name: table_facts(session, table) for table in TABLES}
        versions = {"spark": str(session.version), "python": platform.python_version()}
    finally:
        session.stop()
    broken = sorted(name for name, facts in tables.items() if facts["duplicate_keys"])
    if broken:
        raise StructureError(f"the stable key of {', '.join(broken)} is used by more than one row")
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
        "versions": versions,
        "uid": os.getuid(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.parallelism < 1:
        sys.stderr.write(f"{JOB}: --parallelism must be at least 1\n")
        return 2
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        result = decode(arguments)
    except GateRefused as error:
        sys.stderr.write(f"{JOB}: the gate refused the inputs before any payload was read: {error}\n")
        return 1
    except ManifestError as error:
        sys.stderr.write(f"{JOB}: the manifest cannot be trusted: {error}\n")
        return 1
    except (InputError, StructureError) as error:
        sys.stderr.write(f"{JOB}: refused: {error}\n")
        return 1
    except NessieConflict as error:
        sys.stderr.write(f"{JOB}: Nessie refused the branch {arguments.branch}: {error}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {error}\n")
        return 1
    except SettingsError as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
