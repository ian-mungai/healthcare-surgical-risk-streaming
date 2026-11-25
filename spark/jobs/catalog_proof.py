"""The catalog proof job: one small numbers table, written through Iceberg's native Nessie catalog on a private branch.

``python -m spark.jobs.catalog_proof --branch <name> [--base <name>] [--rows <n>] [--fail-after-write]``

The job creates the branch from ``main`` (or from ``--base``) and refuses a branch that exists. It builds the rows itself in the JVM, the whole numbers 1 to n
and their squares, replaces the table ``lake.catalog_proof.numbers`` on the branch and prints one line that starts with ``RESULT `` and holds the
branch, its new hash, the table's snapshot ID and the facts of the run that the proof records: the memory limit, the heap the JVM got and the versions
of Python and Spark. It never writes to the base: the merge is the caller's step after its checks. ``--fail-after-write`` stops the job after the
write with an error, to show that a failed job leaves the base as it was. Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``,
``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from collections.abc import Sequence
from typing import Any

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.stack.nessie import CONFLICT_TEXT, NessieClient, NessieConflict, NessieError
from spark.catalog import required_environment
from spark.session import build_session, start_deadline
from spark.settings import BYTES_PER_MIB, MAX_DEADLINE_SECONDS, MAX_NESSIE_TIMEOUT_SECONDS, SettingsError, number_setting, read_memory_limit_mib

JOB = "catalog_proof"
NAMESPACE = "catalog_proof"
TABLE = "numbers"
RESULT_PREFIX = "RESULT "
DEFAULT_ROWS = 10
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 900.0


class PlannedFailure(RuntimeError):
    """The failure that ``--fail-after-write`` raises."""


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Write the proof table on a private Nessie branch.")
    command.add_argument("--branch", required=True, help="the private branch to create from main and write to")
    command.add_argument("--base", default="main", help="the branch to create the private branch from")
    command.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="how many rows to write")
    command.add_argument("--fail-after-write", action="store_true", help="fail after the write, to prove that main is untouched")
    return command


def write_table(branch: str, rows: int) -> dict[str, Any]:
    """Replace the proof table on ``branch`` and return its new snapshot ID with the facts of the session."""
    session = build_session(JOB, branch)
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{NAMESPACE}")
        frame = session.range(1, rows + 1, 1, 1).selectExpr("id AS n", "id * id AS n_squared")
        frame.writeTo(f"lake.{NAMESPACE}.{TABLE}").tableProperty("format-version", "2").createOrReplace()
        loaded = session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session._jsparkSession, f"lake.{NAMESPACE}.{TABLE}")
        return {
            "snapshot_id": int(loaded.currentSnapshot().snapshotId()),
            "spark_version": str(session.version),
            "jvm_max_heap_mib": int(session._jvm.java.lang.Runtime.getRuntime().maxMemory()) // BYTES_PER_MIB,
        }
    finally:
        session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.rows < 1:
        sys.stderr.write(f"{JOB}: --rows must be at least 1\n")
        return 2
    try:
        start_deadline(number_setting("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS, MAX_DEADLINE_SECONDS), JOB)
        client = NessieClient(
            required_environment("NESSIE_URI"), number_setting("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS, MAX_NESSIE_TIMEOUT_SECONDS)
        )
        client.config()
        client.create_branch(arguments.branch, arguments.base)
        written = write_table(arguments.branch, arguments.rows)
        if arguments.fail_after_write:
            raise PlannedFailure("planned failure after the write")
        result = {
            "branch": arguments.branch,
            "branch_hash": client.reference(arguments.branch).hash,
            "table": f"{NAMESPACE}.{TABLE}",
            "rows": arguments.rows,
            "memory_limit_mib": read_memory_limit_mib(),
            "python_version": platform.python_version(),
            "uid": os.getuid(),
            **written,
        }
    except NessieConflict:
        sys.stderr.write(f"{JOB}: {CONFLICT_TEXT}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {describe_error(error)}\n")
        return 1
    except SettingsError as error:
        sys.stderr.write(f"{JOB}: {describe_error(error)}\n")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
