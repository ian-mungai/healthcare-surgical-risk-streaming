"""The Spark side of the Bronze clinical and laboratory job: the settings of its session, the rank expression and the adapter that runs its operations.

Nothing here imports Spark. The session, ``pyspark.sql.functions``, ``Window`` and the analysis error class are passed in, so the fast tests call these
production functions with recording doubles and assert the settings, the catalog that each read goes through and the exact expressions that are built.
"""

from __future__ import annotations

import functools
import sys
import time
from collections.abc import Callable, Sequence
from typing import Any

from spark.bronze.checks import Check, CheckResult, key_checks
from spark.bronze_clinical_lab import (
    BRONZE_NAMESPACE,
    GROUP_COLUMNS,
    MISSING,
    OCCURRENCE_COLUMN,
    SOURCE_NAMESPACE,
    TABLES,
    BronzeRulesError,
)
from spark.bronze_clinical_lab_run import JOB, Row
from spark.settings import CATALOG, catalog_options

SOURCE_CATALOG = "source"
RUN_CATALOG = CATALOG
FIELD_SEPARATOR = "|"
HALF_HEX = 16
DIGEST_TYPE = "decimal(38,0)"
GROUP_ROWS = "group_rows"


def job_session_settings(
    *, nessie_uri: str, warehouse: str, region: str, branch: str, default_branch: str, source_hash: str, heap: str, scratch: str, branch_hash: str | None = None
) -> dict[str, str]:
    """The settings of the job's session: the catalog ``lake`` on the run branch and the catalog ``source`` on the default branch pinned to the source hash.

    With ``branch_hash`` the catalog ``lake`` is pinned to that commit of the run branch too, so a read never sees a later commit.
    """
    arguments = {"nessie_uri": nessie_uri, "warehouse": warehouse, "region": region}
    prefix = f"spark.sql.catalog.{RUN_CATALOG}"
    pinned = {f"spark.sql.catalog.{SOURCE_CATALOG}{key.removeprefix(prefix)}": value for key, value in catalog_options(**arguments, ref=default_branch).items()}
    pinned[f"spark.sql.catalog.{SOURCE_CATALOG}.ref.hash"] = source_hash
    return {
        **catalog_options(**arguments, ref=branch, ref_hash=branch_hash),
        **pinned,
        "spark.driver.memory": heap,
        "spark.executor.memory": heap,
        "spark.local.dir": scratch,
        "spark.ui.enabled": "false",
    }


def text_of(column: Any, functions: Any) -> Any:
    """The digest text of one column, the same as ``row_text`` in ``spark/bronze_clinical_lab.py`` gives for a value."""
    as_text = column.cast("string")
    return functions.coalesce(functions.concat(functions.length(as_text).cast("string"), functions.lit(":"), as_text), functions.lit(MISSING))


def digest(frame: Any, columns: Sequence[str], functions: Any) -> str:
    """The order-free digest of ``frame`` over ``columns``, equal to ``digest_of`` of the same rows in ``spark/bronze_clinical_lab.py``."""
    line = functions.concat_ws(FIELD_SEPARATOR, *[text_of(functions.col(name), functions) for name in columns])
    hashed = functions.sha2(line, 256)
    high = functions.conv(functions.substring(hashed, 1, HALF_HEX), 16, 10).cast(DIGEST_TYPE)
    low = functions.conv(functions.substring(hashed, HALF_HEX + 1, HALF_HEX), 16, 10).cast(DIGEST_TYPE)
    row = frame.agg(functions.count(functions.lit(1)), functions.sum(high), functions.sum(low)).collect()[0]
    return f"{int(row[0])}:{int(row[1] or 0)}:{int(row[2] or 0)}"


def occurrences(frame: Any, functions: Any, window: Any) -> Any:
    """The laboratory rows with ``source_occurrence``: from 1 within each case, time and test name, ranked by the result as text, a missing result first."""
    ranked = window.partitionBy(*GROUP_COLUMNS).orderBy(functions.col("result").asc_nulls_first())
    return frame.withColumn(OCCURRENCE_COLUMN, functions.row_number().over(ranked))


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
    """``SparkOps`` over a session that ``job_session_settings`` configured: ``lake`` is the run branch, ``source`` the default branch at the source hash.

    Every operation that runs a Spark job reports its seconds on stderr.
    """

    def __init__(
        self, session: Any, functions: Any, window: Any, analysis_error: type[Exception], column_checks: Callable[[Any, Sequence[Check]], list[CheckResult]]
    ) -> None:
        self.session = session
        self.column_checks = column_checks
        self.functions = functions
        self.window = window
        self.analysis_error = analysis_error

    def source(self, kind: str) -> Any:
        """The loaded table at the source hash, always through the ``source`` catalog and never through the run branch."""
        source_table = TABLES[kind][0]
        try:
            return self.session.table(f"{SOURCE_CATALOG}.{SOURCE_NAMESPACE}.{source_table}")
        except self.analysis_error as error:
            raise BronzeRulesError(f"the loaded table {SOURCE_NAMESPACE}.{source_table} is not at the source hash") from error

    def expected(self, kind: str, columns: list[str]) -> Any:
        """The loaded table at the source hash as Bronze must hold it."""
        frame = self.source(kind).select(*columns)
        return occurrences(frame, self.functions, self.window) if kind == "lab" else frame

    def bronze(self, kind: str) -> Any:
        return self.session.table(f"{RUN_CATALOG}.{BRONZE_NAMESPACE}.{TABLES[kind][1]}")

    @timed
    def source_columns(self, kind: str) -> list[tuple[str, str]]:
        return [(name, column_type) for name, column_type in self.source(kind).dtypes]

    @timed
    def prepare(self) -> None:
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {RUN_CATALOG}.{BRONZE_NAMESPACE}")

    @timed
    def write(self, kind: str, columns: list[str]) -> None:
        target = f"{RUN_CATALOG}.{BRONZE_NAMESPACE}.{TABLES[kind][1]}"
        self.expected(kind, columns).writeTo(target).tableProperty("format-version", "2").createOrReplace()

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

    def not_null_count(self, frame: Any, table: str, key: Sequence[str]) -> int:
        """The values that the column checks find null in the key columns of ``frame``."""
        return sum(result.unexpected for result in self.column_checks(frame, key_checks(table, key)[:-1]))

    @timed
    def null_keys(self, kind: str, columns: list[str]) -> tuple[int, int]:
        """The null values in the key columns of the loaded table (``source_occurrence`` is Bronze's own) and in the key columns of Bronze."""
        key = TABLES[kind][2]
        loaded_key = tuple(name for name in key if name != OCCURRENCE_COLUMN)
        return self.not_null_count(self.source(kind), TABLES[kind][0], loaded_key), self.not_null_count(self.bronze(kind), TABLES[kind][1], key)

    @timed
    def key_duplicates(self, kind: str) -> int:
        """The rows of Bronze that share their key with another row: the unexpected rows of the unique check over the whole key."""
        (unique,) = [check for check in key_checks(TABLES[kind][1], TABLES[kind][2]) if check.name == "key unique"]
        return self.column_checks(self.bronze(kind), [unique])[0].unexpected

    @timed
    def occurrence_gaps(self) -> int:
        functions = self.functions
        groups = (
            self.bronze("lab")
            .groupBy(*GROUP_COLUMNS)
            .agg(functions.count(functions.lit(1)).alias("n"), functions.min(OCCURRENCE_COLUMN).alias("low"), functions.max(OCCURRENCE_COLUMN).alias("high"))
        )
        return int(groups.where((functions.col("low") != 1) | (functions.col("high") != functions.col("n"))).count())

    def repeated_frame(self) -> Any:
        """The Bronze laboratory rows of the groups that hold more than one row, each with its group size in ``group_rows``."""
        functions = self.functions
        sized = self.bronze("lab").withColumn(GROUP_ROWS, functions.count(functions.lit(1)).over(self.window.partitionBy(*GROUP_COLUMNS)))
        return sized.where(functions.col(GROUP_ROWS) > 1)

    @timed
    def repeated_row_count(self) -> int:
        return int(self.repeated_frame().count())

    @timed
    def repeated_rows(self, limit: int) -> list[Row]:
        """The rows of the repeated groups, at most ``limit`` and one more, collected to the driver."""
        rows = self.repeated_frame().drop(GROUP_ROWS).limit(limit + 1).collect()
        return [(row["caseid"], row["dt"], row["name"], row["result"], int(row[OCCURRENCE_COLUMN])) for row in rows]

    @timed
    def digests(self, kind: str, columns: list[str]) -> tuple[str, str]:
        wanted = [*columns, OCCURRENCE_COLUMN] if kind == "lab" else columns
        bronze = self.bronze(kind)
        return digest(bronze, TABLES[kind][2], self.functions), digest(bronze, wanted, self.functions)

    def close(self) -> None:
        self.session.stop()
