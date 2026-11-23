"""The Bronze job for the clinical and laboratory tables: copy the loaded tables into ``bronze.clinical_cases`` and ``bronze.lab_results`` on a
private Nessie branch.

``python -m spark.jobs.bronze_clinical_lab write --branch <name> --source-hash <hash>``
``python -m spark.jobs.bronze_clinical_lab verify --branch <name> --source-hash <hash>``

The source hash is the merged hash that the table checks recorded. It is always looked up in the history of Nessie's configured default
branch (the name comes from Nessie's configuration, never from an argument), so a load that was never merged cannot be a source. ``write``
creates the branch ``--branch`` from that hash on the default branch and refuses a branch that exists. The loaded tables
``raw_records.clinical_data`` and ``raw_records.lab_data`` are always read at the pinned reference ``<default branch>@<source hash>``,
through a second catalog, and never through the run branch. ``write`` replaces the two Bronze tables on the run branch and then runs the
checks on what it wrote. The merge of the branch is the caller's step and the Bronze table checks' step, never this job's. ``verify`` re-
reads a branch and runs the same checks, so any later step can run it again.

Each Bronze table holds the loaded table's published columns, every one a string, with no value changed (``spark/bronze_clinical_lab.py``
has the rules). The laboratory table adds the integer column ``source_occurrence``. The checks of one table are, in this order:
``source_changed`` (the run branch's view of the loaded table has the metadata location and the snapshot that Nessie reports for it at the
source hash), the column list, the key columns not null in the loaded table and in Bronze (a null ends that table's checks there), the row
count, row equality with the loaded table in both directions as a multiset (a missing value stays missing: ``exceptAll`` treats two nulls as
equal and a null and an empty string as different), the key unique and, for the laboratory table, the occurrence numbers
(from 1 without a gap) and the independent occurrence rule: the rows of the repeated groups are
collected to the driver, numbered again by the pure Python rule and compared with the numbers that Spark wrote. More than 100,000 such rows
are refused.

The job prints one line that starts with ``RESULT `` and holds the branch, its hash, the default branch, the source hash, and for each table
its row count, the metadata location and snapshot ID of the loaded table at the source hash, Bronze's snapshot ID on the branch, its key
digest and its row digest, and the checks with their result. It exits 0 when every check passed, 1 when one failed or Nessie refused, and 2
for a setting or an argument that is wrong. Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for
an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``."""

from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Sequence
from typing import Any

from scripts.ingestion.notice import describe_error
from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.bronze_clinical_lab import BronzeRulesError
from spark.bronze_clinical_lab_run import JOB, NessieAdapter, SparkOps, finish, parser, run_job
from spark.bronze_clinical_lab_spark import SparkAdapter, job_session_settings
from spark.catalog import required_environment
from spark.session import SCRATCH_FOLDER, start_deadline
from spark.settings import SettingsError, driver_memory_mib, read_memory_limit_mib

DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0


def build_job_session(branch: str, default_branch: str, source_hash: str) -> Any:
    """A local Spark session with the settings that ``job_session_settings`` gives."""
    spark_session = importlib.import_module("pyspark.sql").SparkSession
    settings = job_session_settings(
        nessie_uri=required_environment("NESSIE_URI"),
        warehouse=required_environment("WAREHOUSE_LOCATION"),
        region=os.environ.get("AWS_REGION", ""),
        branch=branch,
        default_branch=default_branch,
        source_hash=source_hash,
        heap=f"{driver_memory_mib(read_memory_limit_mib())}m",
        scratch=SCRATCH_FOLDER,
    )
    builder = spark_session.builder.appName(JOB).master("local[*]")
    for key, value in settings.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def open_spark(branch: str, default_branch: str, source_hash: str) -> SparkOps:
    return SparkAdapter(
        build_job_session(branch, default_branch, source_hash),
        importlib.import_module("pyspark.sql.functions"),
        importlib.import_module("pyspark.sql").Window,
        importlib.import_module("pyspark.errors").AnalysisException,
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
        result = run_job(arguments.command, arguments.branch, arguments.source_hash, NessieAdapter(client), open_spark)
    except NessieConflict as error:
        sys.stderr.write(f"{JOB}: Nessie refused the branch (status {error.status}, code {error.error_code})\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {describe_error(error)}\n")
        return 1
    except (SettingsError, BronzeRulesError) as error:
        sys.stderr.write(f"{JOB}: {describe_error(error)}\n")
        return 2
    return finish(result, arguments.branch, sys.stdout, sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
