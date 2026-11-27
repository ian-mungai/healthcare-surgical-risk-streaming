"""The Bronze job for the clinical and laboratory tables: copy the loaded tables into ``bronze.clinical_cases`` and ``bronze.lab_results`` on a
private Nessie branch.

``python -m spark.jobs.bronze_clinical_lab write --branch <name> --source-hash <hash> --result <path>``
``python -m spark.jobs.bronze_clinical_lab verify --branch <name> --source-hash <hash> --result <path>``
``python -m spark.jobs.bronze_clinical_lab publish --branch <name> --source-hash <hash> --result <path>``

The source hash is the merged hash that the table checks recorded. It is always looked up in the history of Nessie's configured default
branch (the name comes from Nessie's configuration, never from an argument), so a load that was never merged cannot be a source. ``write``
creates the branch ``--branch`` from that hash on the default branch and refuses a branch that exists. The loaded tables
``raw_records.clinical_data`` and ``raw_records.lab_data`` are always read at the pinned reference ``<default branch>@<source hash>``,
through a second catalog, and never through the run branch. ``write`` replaces the two Bronze tables on the run branch and then runs the
checks on what it wrote. The key checks (not null, unique) run in Great Expectations, and the equality proofs are plain code. ``write`` never publishes.
``verify`` re-reads a branch and runs the same checks, so any later step can run it again. ``publish`` runs them at the head of the branch, every read of the
branch at that hash, and assigns the default branch to that head only when every one passed, the default branch is the commit that the branch was made on and
the change from it is exactly the two Bronze tables (``spark/bronze_clinical_lab_publish.py``). A failed check trips its guard (``key``, ``structure``,
``source`` or ``equality``) and the default branch is not changed.

Each Bronze table holds the loaded table's published columns, every one a string, with no value changed (``spark/bronze_clinical_lab.py``
has the rules). The laboratory table adds the integer column ``source_occurrence``. The checks of one table are, in this order:
``source_changed`` (the run branch's view of the loaded table has the metadata location and the snapshot that Nessie reports for it at the
source hash), the column list, the key columns not null in the loaded table and in Bronze (a null ends that table's checks there), the row
count, row equality with the loaded table in both directions as a multiset (a missing value stays missing: ``exceptAll`` treats two nulls as
equal and a null and an empty string as different), the key unique and, for the laboratory table, the occurrence numbers
(from 1 without a gap) and the independent occurrence rule: the rows of the repeated groups are
collected to the driver, numbered again by the pure Python rule and compared with the numbers that Spark wrote. More than 100,000 such rows
are refused.

The job writes its result to one file at ``--result`` (an absolute path that must not exist; the file is written whole into the reserved ``<path>.part`` file
and linked into place, ``spark/result_file.py``), and the caller reads that file and no line of the job's streams. The result holds the branch, its hash, the
default branch, the source hash and, for each table, the snapshot ID of the loaded table at the source hash, Bronze's snapshot ID on the branch, one pass flag
and the checks, each a name and a flag. Both tables are sealed: the clinical table holds the outcome columns and the laboratory table holds the creatinine
results that the kidney outcome is built from, so a row count, a digest, a metadata path or a check detail of either changes with a held-out patient, and none
leaves the job. The job still compares them inside (the loaded table's metadata location and snapshot against Nessie's, and the counts and the rows). An
ending that is not a run (a refusal) writes the job, the command, the branch, ``passed`` false and one fixed code (``spark/bronze_clinical_lab_cli.py``); no
error holds an argument, a column, a count or an upstream message. It exits 0 when every check passed, 1 when one failed or Nessie refused, 2 for a bad call,
a setting or a table that Bronze does not accept, and 70 for anything else. Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``,
``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``."""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieClient
from spark.bronze.publication import NessiePublisher
from spark.bronze.quality import column_checks
from spark.bronze_clinical_lab_cli import run
from spark.bronze_clinical_lab_publish import publish_job
from spark.bronze_clinical_lab_run import JOB, NessieAdapter, SparkOps, run_job
from spark.bronze_clinical_lab_spark import SparkAdapter, job_session_settings
from spark.catalog import required_environment
from spark.session import SCRATCH_FOLDER, start_deadline
from spark.settings import MAX_DEADLINE_SECONDS, MAX_NESSIE_TIMEOUT_SECONDS, driver_memory_mib, number_setting, read_memory_limit_mib

DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0


def build_job_session(branch: str, default_branch: str, source_hash: str, branch_hash: str | None = None) -> Any:
    """A local Spark session with the settings that ``job_session_settings`` gives, the run branch pinned to ``branch_hash`` when it is given."""
    spark_session = importlib.import_module("pyspark.sql").SparkSession
    settings = job_session_settings(
        nessie_uri=required_environment("NESSIE_URI"),
        warehouse=required_environment("WAREHOUSE_LOCATION"),
        region=os.environ.get("AWS_REGION", ""),
        branch=branch,
        default_branch=default_branch,
        source_hash=source_hash,
        branch_hash=branch_hash,
        heap=f"{driver_memory_mib(read_memory_limit_mib())}m",
        scratch=SCRATCH_FOLDER,
    )
    builder = spark_session.builder.appName(JOB).master("local[*]")
    for key, value in settings.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def open_spark(branch: str, default_branch: str, source_hash: str, branch_hash: str | None = None) -> SparkOps:
    return SparkAdapter(
        build_job_session(branch, default_branch, source_hash, branch_hash),
        importlib.import_module("pyspark.sql.functions"),
        importlib.import_module("pyspark.sql").Window,
        importlib.import_module("pyspark.errors").AnalysisException,
        column_checks,
    )


def execute(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run one call. Every error is mapped to a fixed code by ``spark/bronze_clinical_lab_cli.py``."""
    start_deadline(number_setting("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS, MAX_DEADLINE_SECONDS), JOB)
    client = NessieClient(
        required_environment("NESSIE_URI"), number_setting("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS, MAX_NESSIE_TIMEOUT_SECONDS)
    )
    if arguments.command == "publish":
        return publish_job(arguments.branch, arguments.source_hash, NessieAdapter(client), NessiePublisher(client), open_spark)
    return run_job(arguments.command, arguments.branch, arguments.source_hash, NessieAdapter(client), open_spark)


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
