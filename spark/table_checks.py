"""The rules of the table checks that need no Spark: which tables, which columns and keys, what an empty value is, how a result reads.

The Spark job ``spark/jobs/table_checks.py`` applies these rules to the loaded Iceberg tables and the verified files. Each loaded table is compared with its
verified source on four comparisons: the column list (exact names, order and the string type; only the connector's own metadata columns, the closed list
``CONNECTOR_COLUMNS``, are set aside), the row count, the key multiset and the row payload. The keys and rows are compared as multisets of strings,
unchanged: a value is never trimmed, cast or case-folded, so a leading zero, a trailing blank or a changed empty value is a difference.

The clinical table holds the outcome columns and the laboratory table holds the results that the kidney outcome is built from, so a count of their rows, or
of the rows that differ, changes when a held-out patient's row changes (the owner's decision D: held-out sealing reaches every output that can reflect an
outcome). For these tables (``TableSpec.sealed``) the three row comparisons report one flag with the fixed code ``table_compared`` and no count, and the
result's row counts are null. The column counts stay: they follow the file's header and the table's schema, which hold no outcome. The track list holds no
outcome and keeps its counts.

An empty field of a CSV file is one published value, the empty string. A loaded table holds it in exactly one form, ``EMPTY_FORM``: ``empty`` (the
empty string, which the record connections are set to write) unless a run declares ``null`` (the connector's null). The other form is a different value and
fails the check. Which form the connector writes is measured on the host.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

NAMESPACE = "raw_records"
# The metadata columns of the Airbyte destination that writes the tables. Source: the Airbyte page "Airbyte metadata fields" and the bulk load toolkit's
# ``Meta.kt`` (``COLUMN_NAMES``), read for the S3 Data Lake destination. The names the real load writes are a measurement on the host.
CONNECTOR_COLUMNS = ("_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id")
STRING_TYPE = "string"
EMPTY_FORM = "empty"
EMPTY_FORMS = ("null", "empty")
BYTE_ORDER_MARK = "﻿"
NOT_THE_EMPTY_VALUE = "\x01not the published empty value\x01"
COUNT = re.compile(r"0|[1-9][0-9]{0,14}")
MAX_ROWS = 10**12
MAX_COLUMN_COUNT = 4096
SEPARATOR_PATTERN = "[\\t\\n\\r]"
LAB_DATA_NOTE = "lab_data has no unique key (some whole rows repeat), so its key is the whole row: its key check and its payload check are the same comparison"
HASH = re.compile(r"[0-9a-f]{64}")
RUN_ID = re.compile(r"[0-9a-f]{32}")
PLAN_FORMAT = "table_check_plan_2"
RESULT_FORMAT = "table_checks_5"
# The details of a check: each is one of these codes with exactly its integer fields, in this order, and one rule that says whether the check passed. The job
# reports numbers only (counts for the track list, one flag for each comparison of a sealed table): a check passes if and only if the rule of its code holds for
# its numbers, and a flag that says otherwise refuses the result. A detail is
# built from this table and parsed back against it, so no text of the data can pass as a detail. ``row_count`` is the track list's. ``table_compared`` is the
# comparison of a sealed table: its one field is a flag of 0 or 1 and no count.
DETAIL_RULES: Mapping[str, tuple[tuple[str, ...], Callable[[Mapping[str, int]], bool]]] = {
    "ok": ((), lambda counts: True),
    "bytes_differ": ((), lambda counts: False),
    "table_missing": ((), lambda counts: False),
    "snapshot_differs": ((), lambda counts: False),
    "digest_compared": (("equal",), lambda counts: counts["equal"] == 1),
    "columns_differ": (("missing", "extra", "repeated", "order", "not_string"), lambda counts: not any(counts.values())),
    "row_count": (("source", "table"), lambda counts: counts["source"] == counts["table"]),
    "table_compared": (("equal",), lambda counts: counts["equal"] == 1),
    "separator_in_field": (("rows",), lambda counts: False),
}
DETAIL_CODES: Mapping[str, tuple[str, ...]] = {code: fields for code, (fields, _) in DETAIL_RULES.items()}
# The codes that each check may hold, for any table. ``allowed_codes`` narrows them to one table.
CHECK_CODES: Mapping[str, tuple[str, ...]] = {
    "staged file": ("ok", "bytes_differ"),
    "snapshot": ("ok", "table_missing", "snapshot_differs"),
    "columns": ("columns_differ",),
    "row count": ("row_count", "table_compared"),
    "key multiset": ("table_compared",),
    "row payload": ("table_compared",),
    "rows digest": ("digest_compared", "separator_in_field"),
}


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One loaded table. ``source`` is ``file`` for a verified CSV or ``pin`` for a pinned row count and digest. ``key`` is None for the whole row. ``sealed``
    is True for a table whose rows can reflect an outcome: its result holds no row count and no count of differing rows."""

    name: str
    source: str
    key: tuple[str, ...] | None
    pinned_columns: tuple[str, ...] = ()
    sealed: bool = False


TABLES = (
    TableSpec("clinical_data", "file", ("caseid",), sealed=True),
    TableSpec("lab_data", "file", None, sealed=True),
    TableSpec("track_list", "pin", None, ("caseid", "tname", "tid")),
)
SPEC_BY_NAME: Mapping[str, TableSpec] = {spec.name: spec for spec in TABLES}
FILE_CHECKS = ("staged file", "snapshot", "columns", "row count", "key multiset", "row payload")
# The exact checks of each table, in the order the job runs them. A result that differs from this list in any way never authorises a merge.
REQUIRED_CHECKS: Mapping[str, tuple[str, ...]] = {
    "clinical_data": FILE_CHECKS,
    "lab_data": FILE_CHECKS,
    "track_list": ("snapshot", "columns", "row count", "rows digest"),
}


class RulesError(ValueError):
    """A plan or a pin is not of the form the checks accept. The message names the field and never holds a data value."""


def allowed_codes(table: str, name: str) -> tuple[str, ...]:
    """The codes that the check ``name`` of the table ``table`` may hold. The row count of a sealed table holds the fixed flag code and never a count."""
    barred = "row_count" if SPEC_BY_NAME[table].sealed else "table_compared"
    return tuple(code for code in CHECK_CODES.get(name, ()) if code != barred)


def header_columns(header_line: str) -> list[str]:
    """The column names of a CSV header line. A leading byte order mark is the file's encoding mark and is not part of the first name."""
    text = header_line.removeprefix(BYTE_ORDER_MARK).rstrip("\r\n")
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if len(rows) != 1 or not rows[0] or any(not name for name in rows[0]):
        raise RulesError("the header line holds no column names, or an empty one")
    return rows[0]


def detail(code: str, **counts: int) -> str:
    """A detail from a code of ``DETAIL_CODES`` and exactly its fields, each a whole number of at least 0."""
    fields = DETAIL_CODES.get(code)
    if fields is None or set(counts) != set(fields) or any(type(counts[name]) is not int or counts[name] < 0 for name in fields):
        raise RulesError("a detail is a code of the table with exactly its fields, each a whole number")
    return ";".join([code, *(f"{name}={counts[name]}" for name in fields)])


def parse_detail(text: str) -> tuple[str, dict[str, int]]:
    """The code and counts of a detail, or ``RulesError`` for any text that is not exactly a code of ``DETAIL_CODES`` with its fields in order."""
    code, *items = text.split(";")
    fields = DETAIL_CODES.get(code)
    pairs = [item.split("=", 1) for item in items]
    if fields is None or [pair[0] for pair in pairs] != list(fields) or any(len(pair) != 2 or not COUNT.fullmatch(pair[1]) for pair in pairs):
        raise RulesError("a detail is a code of the table with exactly its fields, each a whole number")
    return code, {name: int(value) for name, value in pairs}


def detail_holds(text: str) -> bool:
    """Whether the check that holds this detail passed: the rule of its code applied to its counts."""
    code, counts = parse_detail(text)
    return DETAIL_RULES[code][1](counts)


def column_detail(expected: Sequence[str], found: Sequence[tuple[str, str]]) -> str:
    """The detail of the column check: what differs between the file's columns and the table's ``(name, type)`` columns, as counts, all zero when equal.

    Only the names in ``CONNECTOR_COLUMNS`` are set aside: any other column of the table is one that the file does not have.
    """
    own = [(name, kind) for name, kind in found if name not in CONNECTOR_COLUMNS]
    names = [name for name, _ in own]
    missing = sum(name not in names for name in expected)
    extra = sum(name not in expected for name in names)
    repeated = len(names) - len(set(names))
    order = int(not missing and not extra and not repeated and names != list(expected))
    not_string = sum(kind != STRING_TYPE for _, kind in own)
    return detail("columns_differ", missing=missing, extra=extra, repeated=repeated, order=order, not_string=not_string)


def row_line(values: Iterable[str]) -> str:
    """One row as its digest reads it: the fields joined by a tab. A field with a tab or a newline is refused, as it makes that form ambiguous."""
    fields = list(values)
    if any(mark in field for field in fields for mark in "\t\n\r"):
        raise RulesError("a field holds a tab or a newline, so the tab-joined row of the digest would be ambiguous")
    return "\t".join(fields)


def rows_digest(lines: Iterable[str]) -> str:
    """SHA-256 over the rows in the order given, lines joined by a newline with none after the last. The caller gives them sorted by their full text."""
    digest = hashlib.sha256()
    first = True
    for line in lines:
        digest.update(line.encode() if first else b"\n" + line.encode())
        first = False
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class TrackPin:
    rows: int
    sha256: str


def track_pin(release: Mapping[str, Any]) -> TrackPin:
    """The pinned row count and digest of the track list from the tracked ``release.json``."""
    pin = release.get("track_list")
    rows, sha256 = (pin or {}).get("rows"), (pin or {}).get("rows_sha256")
    if type(rows) is not int or rows <= 0 or type(sha256) is not str or not HASH.fullmatch(sha256):
        raise RulesError("the release file's track_list pin has no positive row count `rows` and no digest `rows_sha256`")
    return TrackPin(rows, sha256)


@dataclass(frozen=True, slots=True)
class PlanTable:
    """What the job needs to check one table: the snapshot to read, and for a file table the staged file with its size and SHA-256, else the pin."""

    name: str
    snapshot_id: int
    file: str = ""
    file_sha256: str = ""
    file_size: int = 0
    rows: int = 0
    rows_sha256: str = ""


@dataclass(frozen=True, slots=True)
class Plan:
    run_id: str
    branch: str
    branch_hash: str
    empty_form: str
    tables: tuple[PlanTable, ...]


PLAN_FIELDS: Mapping[str, type] = {"format": str, "run_id": str, "branch": str, "branch_hash": str, "empty_form": str, "tables": list}
FILE_TABLE_FIELDS: Mapping[str, type] = {"name": str, "snapshot_id": int, "file": str, "file_sha256": str, "file_size": int}
PIN_TABLE_FIELDS: Mapping[str, type] = {"name": str, "snapshot_id": int, "rows": int, "rows_sha256": str}
PLAIN_FILE_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


def closed(value: Any, fields: Mapping[str, type], what: str) -> dict[str, Any]:
    """The object ``value`` when its fields are exactly ``fields``, each of exactly its type. The message names the object and fields, never a value."""
    if type(value) is not dict or set(value) != set(fields) or any(type(value[name]) is not kind for name, kind in fields.items() if kind is not object):
        raise RulesError(f"{what} must be an object with exactly the fields {', '.join(fields)}, each of its type")
    return value


def reject_repeats(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    if len({name for name, _ in pairs}) != len(pairs):
        raise RulesError("a plan object names one field twice")
    return dict(pairs)


def plan_table(spec: TableSpec, entry: Any) -> PlanTable:
    if spec.source == "file":
        fields = closed(entry, FILE_TABLE_FIELDS, f"the plan entry of {spec.name}")
        if (
            not PLAIN_FILE_NAME.fullmatch(fields["file"])
            or fields["file"].strip(".") == ""
            or not HASH.fullmatch(fields["file_sha256"])
            or fields["file_size"] < 0
        ):
            raise RulesError(f"the plan entry of {spec.name} holds a file that is not a plain name, a digest that is not SHA-256 or a negative size")
        table = PlanTable(spec.name, fields["snapshot_id"], fields["file"], fields["file_sha256"], fields["file_size"])
    else:
        fields = closed(entry, PIN_TABLE_FIELDS, f"the plan entry of {spec.name}")
        if fields["rows"] < 0 or not HASH.fullmatch(fields["rows_sha256"]):
            raise RulesError(f"the plan entry of {spec.name} holds a negative row count or a digest that is not SHA-256")
        table = PlanTable(spec.name, fields["snapshot_id"], rows=fields["rows"], rows_sha256=fields["rows_sha256"])
    if fields["name"] != spec.name or type(fields["snapshot_id"]) is bool:
        raise RulesError(f"the plan entry {spec.name} is out of place or has a snapshot ID that is not a number")
    return table


def parse_plan(text: str) -> Plan:
    """The plan the orchestrator wrote for the job: every table of ``TABLES`` once, in order, every field exactly. Raise ``RulesError`` otherwise."""
    try:
        document = json.loads(text, object_pairs_hook=reject_repeats)
    except ValueError as error:
        if isinstance(error, RulesError):
            raise
        raise RulesError("the plan is not JSON") from None
    fields = closed(document, PLAN_FIELDS, "the plan")
    if fields["format"] != PLAN_FORMAT or fields["empty_form"] not in EMPTY_FORMS or not fields["branch"] or not HASH.fullmatch(fields["branch_hash"]):
        raise RulesError("the plan has another format, an unknown empty form, no branch or a branch hash that is not 64 hex digits")
    if not RUN_ID.fullmatch(fields["run_id"]):
        raise RulesError("the plan's run ID is not 32 lower-case hex digits")
    if len(fields["tables"]) != len(TABLES):
        raise RulesError(f"the plan must name exactly the {len(TABLES)} tables of the list")
    tables = tuple(plan_table(spec, entry) for spec, entry in zip(TABLES, fields["tables"], strict=True))
    return Plan(fields["run_id"], fields["branch"], fields["branch_hash"], fields["empty_form"], tables)


@dataclass(slots=True)
class Check:
    """One check of one table: its name, whether it passed and its detail, which is a code and its numbers and never a value of the data. For a sealed table
    the numbers of a comparison are one flag of 0 or 1, never a count."""

    name: str
    passed: bool
    detail: str


@dataclass(slots=True)
class TableResult:
    table: str
    snapshot_id: int
    rows_table: int | None = None
    rows_source: int | None = None
    columns_source: int | None = None
    columns_table: int | None = None
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def add(self, name: str, text: str) -> bool:
        """Record a check by its detail. It passed if the rule of the detail's code holds.

        The check is held to what the receiver accepts: it is a required check of the table that comes after the checks already added, its code is one that
        the check and the table may hold, and its counts are within the bounds that the values of this result give them. A flag that holds a number, or a
        check of another table, fails here.
        """
        check = Check(name, detail_holds(text), text)
        validate_check(self.table, check, self.rows_source, self.rows_table, self.columns_source, self.columns_table)
        required = REQUIRED_CHECKS[self.table]
        if self.checks and required.index(name) <= required.index(self.checks[-1].name):
            raise RulesError("a check is added after a check that comes later in the required order, or twice")
        self.checks.append(check)
        return check.passed

    def document(self) -> dict[str, Any]:
        """The result as the job prints it, after the same checks of its counts and of each check that the receiver makes. ``RulesError`` otherwise."""
        check_counts(self.table, self.rows_table, self.rows_source, self.columns_source, self.columns_table)
        validate_checks(self.table, self.checks, self.rows_source, self.rows_table, self.columns_source, self.columns_table)
        return {
            "table": self.table,
            "snapshot_id": self.snapshot_id,
            "rows_table": self.rows_table,
            "rows_source": self.rows_source,
            "columns_source": self.columns_source,
            "columns_table": self.columns_table,
            "checks": [{"name": check.name, "passed": check.passed, "detail": check.detail} for check in self.checks],
        }


def job_passed(results: Sequence[TableResult]) -> bool:
    """Whether every table of the job passed: the job's exit code. It is never in the result, which holds the checks only."""
    return len(results) == len(TABLES) and all(result.passed for result in results)


def job_document(plan: Plan, results: Sequence[TableResult]) -> dict[str, Any]:
    """What the Spark job reports: the plan it ran (run ID, branch, hash, empty form) and each table. No summary flag: the controller derives the verdict."""
    return {
        "format": RESULT_FORMAT,
        "run_id": plan.run_id,
        "branch": plan.branch,
        "branch_hash": plan.branch_hash,
        "empty_form": plan.empty_form,
        "tables": [result.document() for result in results],
    }


RESULT_FIELDS: Mapping[str, type] = {"format": str, "run_id": str, "branch": str, "branch_hash": str, "empty_form": str, "tables": list}
TABLE_RESULT_FIELDS: Mapping[str, type] = {
    "table": str,
    "snapshot_id": int,
    "rows_table": object,
    "rows_source": object,
    "columns_source": object,
    "columns_table": object,
    "checks": list,
}
CHECK_FIELDS: Mapping[str, type] = {"name": str, "passed": bool, "detail": str}


@dataclass(frozen=True, slots=True)
class TableOutcome:
    table: str
    checks: tuple[Check, ...]
    snapshot_id: int = 0
    rows_table: int | None = None
    rows_source: int | None = None
    columns_source: int | None = None
    columns_table: int | None = None

    @property
    def passed(self) -> bool:
        """Every required check is there, in order, and passed. A check's flag was held to the rule of its code when the result was read."""
        return tuple(check.name for check in self.checks) == REQUIRED_CHECKS[self.table] and all(check.passed for check in self.checks)

    def document(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "snapshot_id": self.snapshot_id,
            "rows_table": self.rows_table,
            "rows_source": self.rows_source,
            "columns_source": self.columns_source,
            "columns_table": self.columns_table,
            "passed": self.passed,
            "checks": [{"name": check.name, "passed": check.passed, "detail": check.detail} for check in self.checks],
        }


@dataclass(frozen=True, slots=True)
class JobResult:
    run_id: str
    branch: str
    branch_hash: str
    empty_form: str
    tables: tuple[TableOutcome, ...]

    @property
    def passed(self) -> bool:
        return tuple(table.table for table in self.tables) == tuple(REQUIRED_CHECKS) and all(table.passed for table in self.tables)

    def document(self) -> dict[str, Any]:
        """The result as the controller keeps it: only the validated fields, with the verdict derived here and not the job's own."""
        return {
            "format": RESULT_FORMAT,
            "run_id": self.run_id,
            "branch": self.branch,
            "branch_hash": self.branch_hash,
            "empty_form": self.empty_form,
            "passed": self.passed,
            "tables": [table.document() for table in self.tables],
        }


def whole(value: Any, bound: int) -> bool:
    return type(value) is int and 0 <= value <= bound


def bounded(
    code: str, counts: Mapping[str, int], rows_source: int | None, rows_table: int | None, columns_source: int | None, columns_table: int | None
) -> bool:
    """Whether every count of a detail is within the bound that the other values of the same table give it. A bound that is not reported refuses."""
    if code == "row_count":
        return rows_source is not None and rows_table is not None and (counts["source"], counts["table"]) == (rows_source, rows_table)
    if code == "separator_in_field":
        return rows_table is not None and 1 <= counts["rows"] <= rows_table
    if code in ("digest_compared", "table_compared"):
        return counts["equal"] <= 1
    if code == "columns_differ":
        return (
            columns_source is not None
            and columns_table is not None
            and (any(counts.values()) or (columns_source == columns_table and columns_source >= 1))
            and counts["order"] <= 1
            and counts["missing"] <= columns_source
            and all(counts[name] <= columns_table for name in ("extra", "repeated", "not_string"))
        )
    return True


def check_counts(table: str, rows_table: int | None, rows_source: int | None, columns_source: int | None, columns_table: int | None) -> None:
    """The row and column counts of a table's result: each a whole number within its bound or null, and no row count at all for a sealed table."""
    if any(count is not None and not whole(count, MAX_ROWS) for count in (rows_table, rows_source)):
        raise RulesError("a row count of the result is not a whole number or null")
    if any(count is not None and not whole(count, MAX_COLUMN_COUNT) for count in (columns_source, columns_table)):
        raise RulesError("a column count of the result is not a whole number within its bound or null")
    if SPEC_BY_NAME[table].sealed and (rows_table is not None or rows_source is not None):
        raise RulesError("a sealed table of the result holds a row count")


def validate_check(table: str, check: Check, rows_source: int | None, rows_table: int | None, columns_source: int | None, columns_table: int | None) -> None:
    """One check of a table: it is one of the table's required checks, its code is allowed for its name and table, its flag is the one that the rule of its code
    gives and its counts are within the bounds. The producer and the receiver both use it, so a result that one writes the other accepts."""
    if check.name not in REQUIRED_CHECKS[table]:
        raise RulesError("a check is not one of the required checks of its table")
    code, counts = parse_detail(check.detail)
    if code not in allowed_codes(table, check.name) or check.passed != DETAIL_RULES[code][1](counts):
        raise RulesError("a check holds a code that its name may not hold, or a flag that the rule of its code does not give")
    if not bounded(code, counts, rows_source, rows_table, columns_source, columns_table):
        raise RulesError("a count of a check is outside the bound that the other values of its table give it")


def validate_checks(
    table: str, checks: Sequence[Check], rows_source: int | None, rows_table: int | None, columns_source: int | None, columns_table: int | None
) -> None:
    """The checks of a table: a first part of its required checks in order, each valid, and a failed one last when the part is not all of them."""
    required = REQUIRED_CHECKS[table]
    names = tuple(check.name for check in checks)
    if not names or names != required[: len(names)]:
        raise RulesError("the checks of a table are not the required checks in order")
    for check in checks:
        validate_check(table, check, rows_source, rows_table, columns_source, columns_table)
    if len(names) < len(required) and checks[-1].passed:
        raise RulesError("a table stopped before its last required check without a failed check")


def table_outcome(entry: Any, expected: PlanTable) -> TableOutcome:
    """One table of a result, held to its shape and to the plan. Every check's flag must be the one that the rule of its code gives for its counts."""
    table = closed(entry, TABLE_RESULT_FIELDS, "a table of the result")
    if table["table"] != expected.name or table["snapshot_id"] != expected.snapshot_id or type(table["snapshot_id"]) is bool:
        raise RulesError("a table of the result is not the planned table at the planned snapshot")
    rows_table, rows_source, columns_source, columns_table = (table[name] for name in ("rows_table", "rows_source", "columns_source", "columns_table"))
    check_counts(expected.name, rows_table, rows_source, columns_source, columns_table)
    spec = SPEC_BY_NAME[expected.name]
    if spec.source == "pin" and rows_source is not None and rows_source != expected.rows:
        raise RulesError("the pinned row count of the result is not the planned one")
    if spec.source == "pin" and columns_source is not None and columns_source != len(spec.pinned_columns):
        raise RulesError("the column count of the track list in the result is not the pinned one")
    checks = tuple(Check(**closed(check, CHECK_FIELDS, "a check of the result")) for check in table["checks"])
    validate_checks(expected.name, checks, rows_source, rows_table, columns_source, columns_table)
    return TableOutcome(expected.name, checks, expected.snapshot_id, rows_table, rows_source, columns_source, columns_table)


def digest_detail(lines: Iterable[str], expected: str) -> str:
    """The detail of the digest check: the rows' digest (rows sorted by full text, as ``rows_digest`` says) against the pinned one: ``equal`` is 1 or 0."""
    return detail("digest_compared", equal=int(rows_digest(lines) == expected))


def parse_result(text: str, plan: Plan) -> JobResult:
    """The job's result from its text, held to its shape and to the plan it was run for. Raise ``RulesError`` for any difference.

    The controller derives pass or fail from the checks it finds here, so a summary flag, a missing check, an extra check or another plan authorises nothing.
    """
    try:
        document = json.loads(text, object_pairs_hook=reject_repeats)
    except RulesError:
        raise
    except ValueError:
        raise RulesError("the result is not JSON") from None
    fields = closed(document, RESULT_FIELDS, "the result")
    repeated = (fields["format"], fields["run_id"], fields["branch"], fields["branch_hash"], fields["empty_form"])
    if repeated != (RESULT_FORMAT, plan.run_id, plan.branch, plan.branch_hash, plan.empty_form):
        raise RulesError("the result is not for the plan it was run for: another format, run, branch, hash or empty form")
    if len(fields["tables"]) != len(plan.tables):
        raise RulesError("the result names another number of tables than the plan")
    tables = tuple(table_outcome(entry, expected) for entry, expected in zip(fields["tables"], plan.tables, strict=True))
    return JobResult(plan.run_id, plan.branch, plan.branch_hash, plan.empty_form, tables)


SAVED_RESULT_FIELDS: Mapping[str, type] = {**RESULT_FIELDS, "passed": bool}


def parse_saved_result(document: Any, plan: Plan) -> JobResult:
    """The result that the controller kept (``JobResult.document``), held to the form that ``parse_result`` accepts and to the plan, and read again.

    Its verdict and the verdict of each table must be the ones that the checks give. A saved result of the earlier format, one with a count of a sealed table
    or any field beyond the kept ones is refused with ``RulesError``, so it is never returned again or written into a new receipt.
    """
    fields = closed(document, SAVED_RESULT_FIELDS, "the saved result")
    entries = fields["tables"]
    if any(type(entry) is not dict or type(entry.get("passed")) is not bool for entry in entries):
        raise RulesError("a table of the saved result has no verdict")
    stripped = {name: value for name, value in fields.items() if name != "passed"}
    stripped["tables"] = [{name: value for name, value in entry.items() if name != "passed"} for entry in entries]
    result = parse_result(json.dumps(stripped), plan)
    if fields["passed"] is not result.passed or any(entry["passed"] is not table.passed for entry, table in zip(entries, result.tables, strict=True)):
        raise RulesError("a verdict of the saved result is not the one that its checks give")
    return result
