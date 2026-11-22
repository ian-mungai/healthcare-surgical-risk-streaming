"""The Bronze job for the clinical and laboratory tables: copy the loaded tables into ``bronze.clinical_cases`` and ``bronze.lab_results`` on a
private Nessie branch.

``python -m spark.jobs.bronze_clinical_lab write --branch <name> --source-hash <hash> [--base <name>]``
``python -m spark.jobs.bronze_clinical_lab verify --branch <name> --source-hash <hash> [--base <name>]``

``write`` creates the branch ``--branch`` at the hash ``--source-hash`` of ``--base`` (``main``), the merged hash that the table checks
recorded, and refuses a branch that exists. It reads ``raw_records.clinical_data`` and ``raw_records.lab_data`` on that branch, so it reads
exactly the tables at that hash, replaces the two Bronze tables on the branch and then runs ``verify`` on what it wrote. The merge of the
branch is the caller's step and the Bronze table checks' step, never this job's. ``verify`` re-reads a branch and checks the Bronze tables
against the loaded tables, so any later step can run it again.

Each Bronze table holds the loaded table's published columns, every one a string, with no value changed (``spark/bronze_clinical_lab.py``
has the rules). The laboratory table adds the integer column ``source_occurrence``. The checks of one table are its column list, its row
count, row equality with the loaded table in both directions as a multiset (a missing value stays missing: ``exceptAll`` treats two nulls as
equal and a null and an empty string as different), its key (not null, unique) and, for the laboratory table, the occurrence numbers (from 1
without a gap). ``verify`` also checks that the loaded tables on the branch are the snapshots they had at ``--source-hash``.

The job prints one line that starts with ``RESULT `` and holds the branch, its hash, the source hash, and for each table its row count, its
snapshot IDs (the loaded table's and Bronze's, as Nessie holds them for the branch), its key digest and its row digest, and the checks with
their result. It exits 0 when every check passed, 1 when one failed or Nessie refused, and 2 for a setting or an argument that is wrong.
Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS``
and ``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError, Reference
from spark.bronze_clinical_lab import (
    BRONZE_NAMESPACE,
    GROUP_COLUMNS,
    MISSING,
    OCCURRENCE_COLUMN,
    SOURCE_NAMESPACE,
    TABLES,
    BronzeRulesError,
    payload_columns,
)
from spark.session import build_session, required_environment, start_deadline
from spark.settings import SettingsError

JOB = "bronze_clinical_lab"
RESULT_PREFIX = "RESULT "
HASH = re.compile(r"[0-9a-f]{64}")
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0
FIELD_SEPARATOR = "|"
HALF_HEX = 16
DIGEST_TYPE = "decimal(38,0)"
HISTORY_PAGE_SIZE = 500
HISTORY_PAGE_LIMIT = 200


class CheckFailure(RuntimeError):
    """A table check failed. The checks are in the job's result."""


@dataclass(slots=True)
class TableReport:
    kind: str
    source_table: str
    bronze_table: str
    rows: int = 0
    source_rows: int = 0
    source_snapshot_id: int | None = None
    bronze_snapshot_id: int | None = None
    keys_digest: str = ""
    rows_digest: str = ""
    checks: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check["passed"] for check in self.checks)

    def add(self, name: str, passed: bool, detail: str) -> bool:
        self.checks.append({"name": name, "passed": passed, "detail": detail})
        return passed

    def document(self) -> dict[str, Any]:
        return {
            "source_table": f"{SOURCE_NAMESPACE}.{self.source_table}",
            "bronze_table": f"{BRONZE_NAMESPACE}.{self.bronze_table}",
            "rows": self.rows,
            "source_rows": self.source_rows,
            "source_snapshot_id": self.source_snapshot_id,
            "bronze_snapshot_id": self.bronze_snapshot_id,
            "keys_digest": self.keys_digest,
            "rows_digest": self.rows_digest,
            "passed": self.passed,
            "checks": self.checks,
        }


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog=f"python -m spark.jobs.{JOB}", description="Copy the loaded clinical and laboratory tables into Bronze on a private branch."
    )
    commands = command.add_subparsers(dest="command", required=True)
    for name, text in (
        ("write", "create the branch at the source hash, write both Bronze tables and verify them"),
        ("verify", "check the Bronze tables of a branch"),
    ):
        sub = commands.add_parser(name, help=text)
        sub.add_argument("--branch", required=True, help="the private branch that holds (write: will hold) the Bronze tables")
        sub.add_argument("--source-hash", required=True, help="the hash of the base at which the table checks merged the loaded tables")
        sub.add_argument("--base", default="main", help="the branch that the source hash belongs to")
    return command


def snapshot_of(client: NessieClient, reference: str, table: str) -> int | None:
    """The snapshot ID that Nessie holds for ``table`` on ``reference`` (a name or ``name@hash``), or None when the reference has no such table."""
    try:
        document = client.request("GET", f"/trees/{quote(reference, safe='@')}/contents/{table}") or {}
    except NessieError as error:
        if error.status == 404:
            return None
        raise
    snapshot = (document.get("content") or {}).get("snapshotId")
    return int(snapshot) if snapshot is not None else None


def check_source_hash(client: NessieClient, base: str, source_hash: str) -> Reference:
    """The base at ``source_hash``, or a ``NessieError`` when the hash is not a commit in the base's history.

    Nessie serves ``name@hash`` for any commit it holds, also one of another branch, so the history of the base is read page by page until the hash is found.
    """
    if not HASH.fullmatch(source_hash):
        raise SettingsError("the source hash must be 64 lowercase hexadecimal digits")
    token: str | None = None
    for _ in range(HISTORY_PAGE_LIMIT):
        query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
        page = client.request("GET", f"/trees/{quote(base, safe='')}/history", query=query) or {}
        entries = page.get("logEntries") or []
        if any((entry.get("commitMeta") or {}).get("hash") == source_hash for entry in entries):
            return Reference("BRANCH", base, source_hash)
        token = page.get("token")
        if not token:
            break
    raise NessieError(f"the hash {source_hash} is not in the history of {base}")


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


def expected_frame(session: Any, kind: str, source_table: str) -> tuple[Any, list[str]]:
    """The loaded table as Bronze must hold it, and its published columns. A table that cannot be copied as it is raises ``BronzeRulesError``."""
    from pyspark.errors import AnalysisException

    try:
        loaded = session.table(f"lake.{SOURCE_NAMESPACE}.{source_table}")
    except AnalysisException as error:
        raise BronzeRulesError(f"the loaded table {SOURCE_NAMESPACE}.{source_table} is not on the branch") from error
    columns = payload_columns(kind, [(name, column_type) for name, column_type in loaded.dtypes])
    frame = loaded.select(*columns)
    return (occurrences(frame) if kind == "lab" else frame), columns


def verify_table(session: Any, client: NessieClient, branch: str, source_hash_reference: Reference, kind: str) -> TableReport:
    """Run the checks of one table on ``branch`` and return its report. Nothing is written."""
    from pyspark.sql import functions as functions

    source_table, bronze_table, key = TABLES[kind]
    report = TableReport(kind, source_table, bronze_table)
    reference = f"{source_hash_reference.name}@{source_hash_reference.hash}"
    report.source_snapshot_id = snapshot_of(client, branch, f"{SOURCE_NAMESPACE}.{source_table}")
    pinned = snapshot_of(client, reference, f"{SOURCE_NAMESPACE}.{source_table}")
    report.bronze_snapshot_id = snapshot_of(client, branch, f"{BRONZE_NAMESPACE}.{bronze_table}")
    report.add(
        "loaded table at the source hash",
        report.source_snapshot_id is not None and report.source_snapshot_id == pinned,
        f"snapshot on the branch {report.source_snapshot_id}, snapshot at the source hash {pinned}",
    )
    expected, columns = expected_frame(session, kind, source_table)
    if report.bronze_snapshot_id is None:
        report.add("bronze table exists", False, f"{BRONZE_NAMESPACE}.{bronze_table} is not on the branch")
        return report
    bronze = session.table(f"lake.{BRONZE_NAMESPACE}.{bronze_table}")
    wanted = [*columns, OCCURRENCE_COLUMN] if kind == "lab" else columns
    wanted_types = {name: "string" for name in columns} | ({OCCURRENCE_COLUMN: "int"} if kind == "lab" else {})
    found_types = dict(bronze.dtypes)
    report.add(
        "columns", [name for name, _ in bronze.dtypes] == wanted and found_types == wanted_types, f"expected {len(wanted)} columns, found {len(found_types)}"
    )
    if [name for name, _ in bronze.dtypes] != wanted:
        return report
    report.source_rows, report.rows = expected.count(), bronze.count()
    report.add("row count", report.rows == report.source_rows, f"loaded {report.source_rows}, bronze {report.rows}")
    missing, extra = expected.exceptAll(bronze).count(), bronze.exceptAll(expected).count()
    report.add(
        "no value changed", missing == 0 and extra == 0, f"rows of the loaded table not in bronze {missing}, rows of bronze not in the loaded table {extra}"
    )
    stats = bronze.agg(
        functions.count(functions.lit(1)).alias("rows"),
        functions.countDistinct(*key).alias("distinct"),
        *[functions.sum(functions.col(name).isNull().cast("int")).alias(f"null_{name}") for name in key],
    ).collect()[0]
    nulls = sum(int(stats[f"null_{name}"] or 0) for name in key)
    report.add("key not null", nulls == 0, f"{nulls} null value(s) in the key columns {', '.join(key)}")
    report.add("key unique", int(stats["rows"]) == int(stats["distinct"]) and nulls == 0, f"rows {int(stats['rows'])}, distinct keys {int(stats['distinct'])}")
    if kind == "lab":
        groups = bronze.groupBy(*GROUP_COLUMNS).agg(
            functions.count(functions.lit(1)).alias("n"), functions.min(OCCURRENCE_COLUMN).alias("low"), functions.max(OCCURRENCE_COLUMN).alias("high")
        )
        broken = groups.where((functions.col("low") != 1) | (functions.col("high") != functions.col("n"))).count()
        report.add("occurrence numbers from 1 without a gap", broken == 0, f"{broken} group(s) of case, time and test name with a gap or a start above 1")
    report.keys_digest, report.rows_digest = digest(bronze, key), digest(bronze, wanted)
    return report


def result_document(command: str, branch: str, branch_hash: str, source_hash: str, base: str, reports: Sequence[TableReport]) -> dict[str, Any]:
    return {
        "job": JOB,
        "command": command,
        "branch": branch,
        "branch_hash": branch_hash,
        "base": base,
        "source_hash": source_hash,
        "passed": len(reports) == len(TABLES) and all(report.passed for report in reports),
        "tables": {report.bronze_table: report.document() for report in reports},
    }


def write_tables(session: Any) -> None:
    """Replace both Bronze tables on the session's branch with the loaded tables."""
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{BRONZE_NAMESPACE}")
    for kind, (source_table, bronze_table, _) in TABLES.items():
        frame, _ = expected_frame(session, kind, source_table)
        frame.writeTo(f"lake.{BRONZE_NAMESPACE}.{bronze_table}").tableProperty("format-version", "2").createOrReplace()


def run(arguments: argparse.Namespace, client: NessieClient) -> dict[str, Any]:
    source = check_source_hash(client, arguments.base, arguments.source_hash)
    if arguments.command == "write":
        for kind, (source_table, _, _) in TABLES.items():
            if snapshot_of(client, f"{source.name}@{source.hash}", f"{SOURCE_NAMESPACE}.{source_table}") is None:
                raise BronzeRulesError(f"the loaded {kind} table {SOURCE_NAMESPACE}.{source_table} is not in {source.name} at the source hash")
        client.create_reference("BRANCH", arguments.branch, source)
    elif client.reference(arguments.branch).kind != "BRANCH":
        raise NessieError(f"{arguments.branch} is not a branch")
    session = build_session(JOB, arguments.branch)
    try:
        if arguments.command == "write":
            write_tables(session)
        reports = [verify_table(session, client, arguments.branch, source, kind) for kind in TABLES]
    finally:
        session.stop()
    return result_document(arguments.command, arguments.branch, client.reference(arguments.branch).hash, arguments.source_hash, arguments.base, reports)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
        client.config()
        result = run(arguments, client)
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
