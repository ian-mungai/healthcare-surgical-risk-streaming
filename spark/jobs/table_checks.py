"""The table checks job: each loaded table is compared with its verified source, and the job prints one line that starts with ``RESULT ``.

``python -m spark.jobs.table_checks --staging <folder>``

The folder holds ``plan.json`` (written by the orchestrator, see ``spark/table_checks.py``) and the staged copies of the verified ``clinical_data.csv`` and
``lab_data.csv``. The job opens the catalog on the plan's branch **as of the plan's hash**, so it reads the tables exactly as the load step left them, and
it writes nothing: the merge is the orchestrator's step after every check passed. For each table it checks the column list, the row count, the key multiset
and the row payload against the staged file, and for the track list the pinned row count and digest. Rows are compared as multisets by Spark
(``exceptAll``), so no table or file is collected into the driver. The digest of the track list is built from the sorted rows streamed through the driver
one at a time.

Exit code 0: every check passed. 1: a check failed (the result line says which). 2: the plan or the settings cannot be used. Settings come from the
environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse and ``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import json
import operator
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from spark import table_checks as rules
from spark.session import build_session, start_deadline
from spark.settings import SettingsError
from spark.table_checks import Plan, PlanTable, RulesError, TableResult, TableSpec

JOB = "table_checks"
RESULT_PREFIX = "RESULT "
PLAN_FILE = "plan.json"
DEFAULT_JOB_DEADLINE_SECONDS = 900.0
HASH_BLOCK_BYTES = 1024 * 1024


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog=f"python -m spark.jobs.{JOB}", description="Check each loaded table against its verified source.")
    command.add_argument("--staging", required=True, help="the folder that holds plan.json and the staged verified files")
    return command


def file_digest(path: Path) -> tuple[str, int]:
    """The SHA-256 and size of a file, read in blocks."""
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as handle:
        while block := handle.read(HASH_BLOCK_BYTES):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def read_header(path: Path) -> list[str]:
    """The column names of the staged file's first line."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return rules.header_columns(handle.readline())


def source_frame(session: Any, path: Path, columns: Sequence[str]) -> Any:
    """The staged file as string columns, a value never trimmed and a malformed row an error.

    Spark reads an unquoted empty field as null whatever null text it is given, and a quoted empty field as the empty string. The file publishes both as the
    one empty value, so each null here is the empty string.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(name, StringType(), True) for name in columns])
    reader = session.read.format("csv").schema(schema)
    for option, value in {
        "header": "true",
        "mode": "FAILFAST",
        "encoding": "UTF-8",
        "quote": '"',
        "escape": '"',
        "multiLine": "true",
        "ignoreLeadingWhiteSpace": "false",
        "ignoreTrailingWhiteSpace": "false",
        "emptyValue": "",
    }.items():
        reader = reader.option(option, value)
    return reader.load(f"file://{path}").select(*[F.coalesce(F.col(f"`{name}`"), F.lit("")).alias(name) for name in columns])


def table_frame(frame: Any, columns: Sequence[str], empty_form: str) -> Any:
    """The loaded table's source columns, each value as the file would hold it: the accepted form of the empty value as the empty string."""
    from pyspark.sql import functions as F  # noqa: N812

    def value(name: str) -> Any:
        column = F.col(f"`{name}`")
        other = F.lit(rules.NOT_THE_EMPTY_VALUE)
        if empty_form == "null":
            return F.when(column.isNull(), F.lit("")).when(column == "", other).otherwise(column).alias(name)
        return F.when(column.isNull(), other).otherwise(column).alias(name)

    return frame.select(*[value(name) for name in columns])


def ticked(names: Sequence[str]) -> list[str]:
    return [f"`{name}`" for name in names]


def multiset_check(result: TableResult, name: str, source: Any, table: Any) -> None:
    """Pass when the rows of the two frames are the same multiset. A failure counts the rows each side alone holds and shows no row."""
    missing, extra = source.exceptAll(table).count(), table.exceptAll(source).count()
    same = missing == 0 and extra == 0
    result.add(name, same, "ok" if same else f"rows_differ;only_file={missing};only_table={extra}")


def compare_rows(result: TableResult, spec: TableSpec, columns: Sequence[str], source: Any, table: Any) -> None:
    """The three comparisons of a file table, in the order of its required checks: the row count, the key multiset and the row payload.

    The frames need ``select``, ``exceptAll`` and ``count`` only. The key is the spec's key columns, or the whole row when the table has none.
    """
    result.rows_source, result.rows_table = source.count(), table.count()
    same = result.rows_source == result.rows_table
    result.add("row count", same, "ok" if same else f"row_count;source={result.rows_source};table={result.rows_table}")
    keys = ticked(spec.key if spec.key else columns)
    multiset_check(result, "key multiset", source.select(*keys), table.select(*keys))
    multiset_check(result, "row payload", source, table)


def snapshot_of(session: Any, name: str) -> int | None:
    """The current snapshot ID of the table as the catalog's reference holds it, or None when the table has no snapshot."""
    loaded = session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session._jsparkSession, f"lake.{rules.NAMESPACE}.{name}")
    snapshot = loaded.currentSnapshot()
    return None if snapshot is None else int(snapshot.snapshotId())


def check_table(session: Any, staging: Path, plan: Plan, entry: PlanTable) -> TableResult:
    """Run the required checks of one table in order and stop at the first that fails. Every detail is a code and counts."""
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F  # noqa: N812

    spec = rules.SPEC_BY_NAME[entry.name]
    result = TableResult(entry.name, entry.snapshot_id)
    path = staging / entry.file if spec.source == "file" else None
    if path is not None:
        found, size = file_digest(path) if path.is_file() else ("", -1)
        same = found == entry.file_sha256 and size == entry.file_size
        if not result.add("staged file", same, "ok" if same else "bytes_differ"):
            return result
    try:
        current = snapshot_of(session, entry.name)
        frame = session.table(f"lake.{rules.NAMESPACE}.{entry.name}")
    except AnalysisException:
        result.add("snapshot", False, "table_missing")
        return result
    if not result.add("snapshot", current == entry.snapshot_id, "ok" if current == entry.snapshot_id else "snapshot_differs"):
        return result
    expected = read_header(path) if path is not None else list(spec.pinned_columns)
    problem = rules.column_detail(expected, [(field.name, field.dataType.simpleString()) for field in frame.schema.fields])
    if not result.add("columns", problem is None, problem or "ok"):
        return result
    table = table_frame(frame, expected, plan.empty_form)
    if path is not None:
        compare_rows(result, spec, expected, source_frame(session, path, expected), table)
        return result
    result.rows_source, result.rows_table = entry.rows, table.count()
    same = result.rows_table == entry.rows
    result.add("row count", same, "ok" if same else f"row_count;source={entry.rows};table={result.rows_table}")
    ambiguous = table.filter(functools.reduce(operator.or_, [F.col(f"`{name}`").rlike(rules.SEPARATOR_PATTERN) for name in expected])).count()
    if ambiguous:
        result.add("rows digest", False, f"separator_in_field;rows={ambiguous}")
        return result
    lines = table.select(F.concat_ws("\t", *[F.col(f"`{name}`") for name in expected]).alias("line")).orderBy("line")
    digest = rules.rows_digest(row.line for row in lines.toLocalIterator())
    result.add("rows digest", digest == entry.rows_sha256, "ok" if digest == entry.rows_sha256 else "digest_differs")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    staging = Path(arguments.staging)
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        plan = rules.parse_plan((staging / PLAN_FILE).read_text())
        session = build_session(JOB, plan.branch, plan.branch_hash)
    except OSError as error:
        sys.stderr.write(f"{JOB}: the plan cannot be read ({type(error).__name__})\n")
        return 2
    except (RulesError, SettingsError) as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    try:
        results = [check_table(session, staging, plan, entry) for entry in plan.tables]
    finally:
        session.stop()
    document = rules.job_document(plan, results)
    sys.stdout.write(RESULT_PREFIX + json.dumps(document, sort_keys=True) + "\n")
    return 0 if document["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
