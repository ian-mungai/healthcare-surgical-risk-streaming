"""The sequence of the Bronze clinical and laboratory job, with its Nessie and Spark operations as parameters, and the Nessie operations over HTTP.

``spark/jobs/bronze_clinical_lab.py`` is the command line and ``spark/bronze_clinical_lab_spark.py`` is the Spark side. This module imports no
Spark, so the whole sequence runs on fakes in the fast tests.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import quote

from scripts.stack.nessie import NessieClient, NessieError, Reference
from spark.bronze_clinical_lab import (
    BRONZE_NAMESPACE,
    OCCURRENCE_COLUMN,
    REPEATED_ROWS_LIMIT,
    SOURCE_NAMESPACE,
    TABLES,
    BronzeRulesError,
    TableContent,
    occurrence_differences,
    payload_columns,
)
from spark.result_file import add_result_option
from spark.settings import SettingsError

JOB = "bronze_clinical_lab"
HASH = re.compile(r"[0-9a-f]{64}")
# Every check that a table report can hold, and the checks of a table that passed, in order. The runner of the job reads its result file against these lists.
CHECK_NAMES = (
    "source_changed",
    "bronze table exists",
    "columns",
    "key not null",
    "row count",
    "no value changed",
    "key unique",
    "occurrence numbers from 1 without a gap",
    "occurrence rule",
)
CHECKS_PASSED = {
    "clinical": ("source_changed", "columns", "key not null", "row count", "no value changed", "key unique"),
    "lab": (
        "source_changed",
        "columns",
        "key not null",
        "row count",
        "no value changed",
        "key unique",
        "occurrence numbers from 1 without a gap",
        "occurrence rule",
    ),
}
HISTORY_PAGE_SIZE = 500
HISTORY_PAGE_LIMIT = 200
Row = tuple[str | None, str | None, str | None, str | None, int]


class NessieOps(Protocol):
    """What the sequence needs from Nessie."""

    def default_branch(self) -> str: ...

    def history_contains(self, branch: str, source_hash: str) -> bool: ...

    def content(self, reference: str, table: str) -> TableContent | None: ...

    def create_branch(self, name: str, from_branch: str, source_hash: str) -> None: ...

    def is_branch(self, name: str) -> bool: ...

    def head(self, branch: str) -> str: ...


class SparkOps(Protocol):
    """What the sequence needs from Spark. ``source_*`` read the loaded tables at the pinned reference, ``bronze_*`` the Bronze tables on the run branch."""

    def source_columns(self, kind: str) -> list[tuple[str, str]]: ...

    def prepare(self) -> None: ...

    def write(self, kind: str, columns: list[str]) -> None: ...

    def bronze_columns(self, kind: str, columns: list[str]) -> list[tuple[str, str]] | None: ...

    def source_count(self, kind: str, columns: list[str]) -> int: ...

    def bronze_count(self, kind: str) -> int: ...

    def difference(self, kind: str, columns: list[str]) -> tuple[int, int]: ...

    def null_keys(self, kind: str, columns: list[str]) -> tuple[int, int]: ...

    def key_duplicates(self, kind: str) -> int: ...

    def occurrence_gaps(self) -> int: ...

    def repeated_row_count(self) -> int: ...

    def repeated_rows(self, limit: int) -> list[Row]: ...

    def close(self) -> None: ...


class SourceNotInHistory(NessieError):
    """The source hash is not in the history of Nessie's default branch."""


class NotABranch(NessieError):
    """The name is not a branch."""


@dataclass(slots=True)
class TableReport:
    """One table's checks. Both tables are sealed: the clinical table holds the outcome columns and the laboratory table holds the creatinine results that the
    kidney outcome is built from, so a row count, a digest, a metadata path or a check detail of either changes with a held-out patient. The report holds the
    snapshot IDs, the names of the checks with a flag each and one pass flag."""

    source_table: str
    bronze_table: str
    source_snapshot_id: int | None = None
    bronze_snapshot_id: int | None = None
    checks: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check["passed"] for check in self.checks)

    def add(self, name: str, passed: bool) -> bool:
        self.checks.append({"name": name, "passed": passed})
        return passed

    def document(self) -> dict[str, Any]:
        return {
            "source_table": f"{SOURCE_NAMESPACE}.{self.source_table}",
            "bronze_table": f"{BRONZE_NAMESPACE}.{self.bronze_table}",
            "source_snapshot_id": self.source_snapshot_id,
            "bronze_snapshot_id": self.bronze_snapshot_id,
            "passed": self.passed,
            "checks": self.checks,
        }


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Copy the loaded clinical and laboratory tables into Bronze on a private branch.")
    commands = command.add_subparsers(dest="command", required=True)
    for name, text in (
        ("write", "create the branch from the source hash, write both Bronze tables and verify them"),
        ("verify", "check the Bronze tables of a branch"),
        ("publish", "check the Bronze tables of a branch and merge it into Nessie's default branch only when every check passed"),
    ):
        sub = commands.add_parser(name, help=text)
        sub.add_argument("--branch", required=True, help="the private branch that holds (write: will hold) the Bronze tables")
        sub.add_argument("--source-hash", required=True, help="the hash of Nessie's default branch at which the table checks merged the loaded tables")
        add_result_option(sub)
    return command


def verify_table(nessie: NessieOps, spark: SparkOps, branch: str, pinned: str, kind: str, columns: list[str]) -> TableReport:
    """Run the checks of one table on ``branch`` against the loaded table at ``pinned`` and return its report. Nothing is written.

    The counts that a check reads stay inside this function: the report holds a name and a flag for each check.
    """
    source_table, bronze_table, _ = TABLES[kind]
    report = TableReport(source_table, bronze_table)
    source_key, bronze_key = f"{SOURCE_NAMESPACE}.{source_table}", f"{BRONZE_NAMESPACE}.{bronze_table}"
    on_branch, at_hash = nessie.content(branch, source_key), nessie.content(pinned, source_key)
    report.source_snapshot_id = at_hash.snapshot_id if at_hash else None
    report.add("source_changed", at_hash is not None and on_branch == at_hash)
    bronze_content = nessie.content(branch, bronze_key)
    report.bronze_snapshot_id = bronze_content.snapshot_id if bronze_content else None
    if bronze_content is None:
        report.add("bronze table exists", False)
        return report
    wanted = [*columns, OCCURRENCE_COLUMN] if kind == "lab" else columns
    wanted_types = {name: "string" for name in columns} | ({OCCURRENCE_COLUMN: "int"} if kind == "lab" else {})
    found = spark.bronze_columns(kind, columns) or []
    report.add("columns", [name for name, _ in found] == wanted and dict(found) == wanted_types)
    if [name for name, _ in found] != wanted:
        return report
    loaded_nulls, bronze_nulls = spark.null_keys(kind, columns)
    report.add("key not null", loaded_nulls == 0 and bronze_nulls == 0)
    if loaded_nulls or bronze_nulls:
        return report
    source_rows = spark.source_count(kind, columns)
    report.add("row count", spark.bronze_count(kind) == source_rows)
    missing, extra = spark.difference(kind, columns)
    report.add("no value changed", missing == 0 and extra == 0)
    report.add("key unique", spark.key_duplicates(kind) == 0)
    if kind == "lab":
        report.add("occurrence numbers from 1 without a gap", spark.occurrence_gaps() == 0)
        in_groups = spark.repeated_row_count()
        if in_groups > REPEATED_ROWS_LIMIT:
            raise BronzeRulesError(
                f"the repeated laboratory groups hold more than {REPEATED_ROWS_LIMIT} rows: the check refuses to collect them", code="repeated_rows_over_limit"
            )
        repeated = spark.repeated_rows(REPEATED_ROWS_LIMIT)
        differences = occurrence_differences(repeated) if len(repeated) == in_groups else None
        report.add("occurrence rule", differences == 0)
    return report


def result_document(command: str, branch: str, branch_hash: str, default_branch: str, source_hash: str, reports: Sequence[TableReport]) -> dict[str, Any]:
    return {
        "job": JOB,
        "command": command,
        "branch": branch,
        "branch_hash": branch_hash,
        "default_branch": default_branch,
        "source_hash": source_hash,
        "passed": len(reports) == len(TABLES) and all(report.passed for report in reports),
        "tables": {report.bronze_table: report.document() for report in reports},
    }


def run_job(
    command: str, branch: str, source_hash: str, nessie: NessieOps, open_spark: Callable[[str, str, str, str | None], SparkOps], branch_hash: str | None = None
) -> dict[str, Any]:
    """The whole sequence of the job, with its Nessie and Spark operations as parameters.

    ``write``: the hash is checked, Nessie's default branch is asked for its history, each loaded table must exist at the hash, the run branch is
    created from that hash, the Spark session opens on the run branch and on the pinned reference, the columns are checked, both tables are written
    and then every check runs. ``verify``: the same up to the branch, which must exist, and the same checks without a write.

    ``verify`` reads the branch at one hash for everything it does: ``branch_hash``, or the head that it resolves once, after it has found that the branch is
    a branch, when none is given. With it every read of the branch is at that hash: Nessie's metadata through ``branch@hash`` and the Spark catalog of the run
    branch pinned to it, so the checks judge exactly the commit that a publication will merge, whatever the branch's head is by then.
    """
    if branch_hash is not None and (command != "verify" or not HASH.fullmatch(branch_hash)):
        raise SettingsError("only verify takes a branch hash, and it is 64 lowercase hexadecimal digits")
    if not HASH.fullmatch(source_hash):
        raise SettingsError("the source hash must be 64 lowercase hexadecimal digits")
    default_branch = nessie.default_branch()
    if not nessie.history_contains(default_branch, source_hash):
        raise SourceNotInHistory(f"the hash {source_hash} is not in the history of {default_branch}")
    pinned = f"{default_branch}@{source_hash}"
    if command == "write":
        for kind, (source_table, _, _) in TABLES.items():
            if nessie.content(pinned, f"{SOURCE_NAMESPACE}.{source_table}") is None:
                raise BronzeRulesError(
                    f"the loaded {kind} table {SOURCE_NAMESPACE}.{source_table} is not in {default_branch} at the source hash", code="source_table_missing"
                )
        nessie.create_branch(branch, default_branch, source_hash)
    elif not nessie.is_branch(branch):
        raise NotABranch(f"{branch} is not a branch")
    if command == "verify" and branch_hash is None:
        branch_hash = nessie.head(branch)
    reference = branch if branch_hash is None else f"{branch}@{branch_hash}"
    spark = open_spark(branch, default_branch, source_hash, branch_hash)
    try:
        columns = {kind: payload_columns(kind, spark.source_columns(kind)) for kind in TABLES}
        if command == "write":
            spark.prepare()
            for kind in TABLES:
                spark.write(kind, columns[kind])
        reports = [verify_table(nessie, spark, reference, pinned, kind, columns[kind]) for kind in TABLES]
    finally:
        spark.close()
    return result_document(command, branch, branch_hash or nessie.head(branch), default_branch, source_hash, reports)


class NessieAdapter:
    """``NessieOps`` over Nessie's HTTP API."""

    def __init__(self, client: NessieClient) -> None:
        self.client = client

    def default_branch(self) -> str:
        name = self.client.config().get("defaultBranch")
        if not isinstance(name, str) or not name:
            raise NessieError("Nessie's configuration answer holds no default branch")
        return name

    def history_contains(self, branch: str, source_hash: str) -> bool:
        """Whether ``source_hash`` is a commit in the history of ``branch``.

        Nessie serves ``name@hash`` for any commit it holds, so the history is read page by page.
        """
        token: str | None = None
        for _ in range(HISTORY_PAGE_LIMIT):
            query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
            page = self.client.request("GET", f"/trees/{quote(branch, safe='')}/history", query=query) or {}
            if any((entry.get("commitMeta") or {}).get("hash") == source_hash for entry in page.get("logEntries") or []):
                return True
            token = page.get("token")
            if not token:
                return False
        return False

    def content(self, reference: str, table: str) -> TableContent | None:
        """The metadata location and snapshot of ``table`` on ``reference`` (a name or ``name@hash``) as Nessie holds them.

        None when the reference has no such table.
        """
        try:
            document = self.client.request("GET", f"/trees/{quote(reference, safe='@')}/contents/{table}") or {}
        except NessieError as error:
            if error.status == 404:
                return None
            raise
        found = document.get("content") or {}
        location, snapshot = found.get("metadataLocation"), found.get("snapshotId")
        if not isinstance(location, str) or snapshot is None:
            raise NessieError(f"Nessie holds no metadata location and snapshot for {table} on {reference}")
        return TableContent(location, int(snapshot))

    def create_branch(self, name: str, from_branch: str, source_hash: str) -> None:
        self.client.create_reference("BRANCH", name, Reference("BRANCH", from_branch, source_hash))

    def is_branch(self, name: str) -> bool:
        return self.client.reference(name).kind == "BRANCH"

    def head(self, branch: str) -> str:
        return self.client.reference(branch).hash
