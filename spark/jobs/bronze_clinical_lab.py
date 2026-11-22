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
source hash), the column list, the row count, row equality with the loaded table in both directions as a multiset (a missing value stays
missing: ``exceptAll`` treats two nulls as equal and a null and an empty string as different), the key (not null, unique) and, for the
laboratory table, the occurrence numbers (from 1 without a gap) and the independent occurrence rule: the rows of the repeated groups are
collected to the driver, numbered again by the pure Python rule and compared with the numbers that Spark wrote. More than 100,000 such rows
are refused.

The job prints one line that starts with ``RESULT `` and holds the branch, its hash, the default branch, the source hash, and for each table
its row count, the metadata location and snapshot ID of the loaded table at the source hash, Bronze's snapshot ID on the branch, its key
digest and its row digest, and the checks with their result. It exits 0 when every check passed, 1 when one failed or Nessie refused, and 2
for a setting or an argument that is wrong. Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for
an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and ``JOB_DEADLINE_SECONDS``."""

from __future__ import annotations

import functools
import importlib
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.bronze_clinical_lab import (
    BRONZE_NAMESPACE,
    GROUP_COLUMNS,
    MISSING,
    OCCURRENCE_COLUMN,
    SOURCE_NAMESPACE,
    TABLES,
    BronzeRulesError,
    KeyStatistics,
)
from spark.bronze_clinical_lab_run import JOB, NessieAdapter, Row, SparkOps, parser, run_job
from spark.session import SCRATCH_FOLDER, required_environment, start_deadline
from spark.settings import CATALOG, SettingsError, catalog_options, driver_memory_mib, read_memory_limit_mib

RESULT_PREFIX = "RESULT "
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0
FIELD_SEPARATOR = "|"
HALF_HEX = 16
DIGEST_TYPE = "decimal(38,0)"
SOURCE_CATALOG = "source"


def build_job_session(branch: str, default_branch: str, source_hash: str) -> Any:
    """A local Spark session with two catalogs: ``lake`` on the run branch, and ``source`` on the default branch pinned to the source hash."""
    spark_session = importlib.import_module("pyspark.sql").SparkSession
    heap = f"{driver_memory_mib(read_memory_limit_mib())}m"
    arguments = {
        "nessie_uri": required_environment("NESSIE_URI"),
        "warehouse": required_environment("WAREHOUSE_LOCATION"),
        "region": os.environ.get("AWS_REGION", ""),
    }
    run_options = catalog_options(**arguments, ref=branch)
    prefix = f"spark.sql.catalog.{CATALOG}"
    pinned_options = {
        f"spark.sql.catalog.{SOURCE_CATALOG}{key.removeprefix(prefix)}": value for key, value in catalog_options(**arguments, ref=default_branch).items()
    }
    pinned_options[f"spark.sql.catalog.{SOURCE_CATALOG}.ref.hash"] = source_hash
    builder = spark_session.builder.appName(JOB).master("local[*]")
    for key, value in {
        **run_options,
        **pinned_options,
        "spark.driver.memory": heap,
        "spark.executor.memory": heap,
        "spark.local.dir": SCRATCH_FOLDER,
        "spark.ui.enabled": "false",
    }.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def text_of(column: Any) -> Any:
    """The digest text of one column, the same as ``row_text`` in ``spark/bronze_clinical_lab.py`` gives for a value."""
    from pyspark.sql import functions as functions

    as_text = column.cast("string")
    return functions.coalesce(functions.concat(functions.length(as_text).cast("string"), functions.lit(":"), as_text), functions.lit(MISSING))


def digest(frame: Any, columns: Sequence[str]) -> str:
    """The order-free digest of ``frame`` over ``columns``, equal to ``digest_of`` of the same rows in ``spark/bronze_clinical_lab.py``."""
    from pyspark.sql import functions as functions

    line = functions.concat_ws(FIELD_SEPARATOR, *[text_of(functions.col(name)) for name in columns])
    hashed = functions.sha2(line, 256)
    high = functions.conv(functions.substring(hashed, 1, HALF_HEX), 16, 10).cast(DIGEST_TYPE)
    low = functions.conv(functions.substring(hashed, HALF_HEX + 1, HALF_HEX), 16, 10).cast(DIGEST_TYPE)
    row = frame.agg(functions.count(functions.lit(1)), functions.sum(high), functions.sum(low)).collect()[0]
    return f"{int(row[0])}:{int(row[1] or 0)}:{int(row[2] or 0)}"


def occurrences(frame: Any) -> Any:
    """The laboratory rows with ``source_occurrence``: from 1 within each case, time and test name, ranked by the result as text, a missing result first."""
    from pyspark.sql import Window
    from pyspark.sql import functions as functions

    window = Window.partitionBy(*GROUP_COLUMNS).orderBy(functions.col("result").asc_nulls_first())
    return frame.withColumn(OCCURRENCE_COLUMN, functions.row_number().over(window))


def timed(method: Callable[..., Any]) -> Callable[..., Any]:
    """Report on stderr the seconds that an operation took, so a slow or stalled job shows where it is."""

    @functools.wraps(method)
    def wrapper(*arguments: Any, **keywords: Any) -> Any:
        started = time.monotonic()
        try:
            return method(*arguments, **keywords)
        finally:
            kind = f" {arguments[1]}" if len(arguments) > 1 and isinstance(arguments[1], str) else ""
            sys.stderr.write(f"{JOB}: {method.__name__}{kind} took {time.monotonic() - started:.1f} seconds\n")
            sys.stderr.flush()

    return wrapper


class SparkAdapter:
    """``SparkOps`` over a session from ``build_job_session``. Every operation that runs a Spark job reports its seconds on stderr."""

    def __init__(self, session: Any) -> None:
        self.session = session

    def source(self, kind: str) -> Any:
        from pyspark.errors import AnalysisException

        source_table = TABLES[kind][0]
        try:
            return self.session.table(f"{SOURCE_CATALOG}.{SOURCE_NAMESPACE}.{source_table}")
        except AnalysisException as error:
            raise BronzeRulesError(f"the loaded table {SOURCE_NAMESPACE}.{source_table} is not at the source hash") from error

    def expected(self, kind: str, columns: list[str]) -> Any:
        """The loaded table at the source hash as Bronze must hold it."""
        frame = self.source(kind).select(*columns)
        return occurrences(frame) if kind == "lab" else frame

    def bronze(self, kind: str) -> Any:
        return self.session.table(f"lake.{BRONZE_NAMESPACE}.{TABLES[kind][1]}")

    @timed
    def source_columns(self, kind: str) -> list[tuple[str, str]]:
        return [(name, column_type) for name, column_type in self.source(kind).dtypes]

    @timed
    def prepare(self) -> None:
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{BRONZE_NAMESPACE}")

    @timed
    def write(self, kind: str, columns: list[str]) -> None:
        self.expected(kind, columns).writeTo(f"lake.{BRONZE_NAMESPACE}.{TABLES[kind][1]}").tableProperty("format-version", "2").createOrReplace()

    def bronze_columns(self, kind: str, columns: list[str]) -> list[tuple[str, str]] | None:
        return [(name, column_type) for name, column_type in self.bronze(kind).dtypes]

    @timed
    def source_count(self, kind: str, columns: list[str]) -> int:
        return int(self.expected(kind, columns).count())

    @timed
    def bronze_count(self, kind: str) -> int:
        return int(self.bronze(kind).count())

    @timed
    def difference(self, kind: str, columns: list[str]) -> tuple[int, int]:
        expected, bronze = self.expected(kind, columns), self.bronze(kind)
        return int(expected.exceptAll(bronze).count()), int(bronze.exceptAll(expected).count())

    @timed
    def key_statistics(self, kind: str) -> KeyStatistics:
        from pyspark.sql import functions as functions

        key = TABLES[kind][2]
        stats = (
            self.bronze(kind)
            .agg(
                functions.count(functions.lit(1)).alias("rows"),
                functions.countDistinct(*key).alias("distinct"),
                *[functions.sum(functions.col(name).isNull().cast("int")).alias(f"null_{name}") for name in key],
            )
            .collect()[0]
        )
        return KeyStatistics(int(stats["rows"]), int(stats["distinct"]), sum(int(stats[f"null_{name}"] or 0) for name in key))

    @timed
    def occurrence_gaps(self) -> int:
        from pyspark.sql import functions as functions

        groups = (
            self.bronze("lab")
            .groupBy(*GROUP_COLUMNS)
            .agg(functions.count(functions.lit(1)).alias("n"), functions.min(OCCURRENCE_COLUMN).alias("low"), functions.max(OCCURRENCE_COLUMN).alias("high"))
        )
        return int(groups.where((functions.col("low") != 1) | (functions.col("high") != functions.col("n"))).count())

    @timed
    def repeated_rows(self, limit: int) -> list[Row]:
        """The rows of the laboratory groups that hold more than one row, at most ``limit`` and one more, collected to the driver."""
        from pyspark.sql import Window
        from pyspark.sql import functions as functions

        sized = self.bronze("lab").withColumn("group_rows", functions.count(functions.lit(1)).over(Window.partitionBy(*GROUP_COLUMNS)))
        rows = sized.where(functions.col("group_rows") > 1).drop("group_rows").limit(limit + 1).collect()
        return [(row["caseid"], row["dt"], row["name"], row["result"], int(row[OCCURRENCE_COLUMN])) for row in rows]

    @timed
    def digests(self, kind: str, columns: list[str]) -> tuple[str, str]:
        wanted = [*columns, OCCURRENCE_COLUMN] if kind == "lab" else columns
        bronze = self.bronze(kind)
        return digest(bronze, TABLES[kind][2]), digest(bronze, wanted)

    def close(self) -> None:
        self.session.stop()


def open_spark(branch: str, default_branch: str, source_hash: str) -> SparkOps:
    return SparkAdapter(build_job_session(branch, default_branch, source_hash))


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
        result = run_job(arguments.command, arguments.branch, arguments.source_hash, NessieAdapter(client), open_spark)
    except NessieConflict as error:
        sys.stderr.write(f"{JOB}: Nessie refused the branch {arguments.branch}: {error}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {error}\n")
        return 1
    except (SettingsError, BronzeRulesError) as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    if not result["passed"]:
        failed = [
            f"{table['bronze_table']}: {check['name']} ({check['detail']})"
            for table in result["tables"].values()
            for check in table["checks"]
            if not check["passed"]
        ]
        sys.stderr.write(f"{JOB}: the table checks failed on branch {arguments.branch}: {'; '.join(failed)}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
