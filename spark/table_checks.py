"""The rules of the table checks that need no Spark: which tables, which columns and keys, what an empty value is, how a result reads.

The Spark job ``spark/jobs/table_checks.py`` applies these rules to the loaded Iceberg tables and the verified files. Each loaded table is compared with its
verified source on four counts: the column list (exact names, order and the string type; Airbyte's own columns, which start with ``_airbyte_``, are ignored),
the row count, the key multiset and the row payload. The keys and rows are compared as multisets of strings, unchanged: a value is never trimmed, cast or
case-folded, so a leading zero, a trailing blank or a changed empty value is a difference.

An empty field of a CSV file is one published value, the empty string. A loaded table holds it in exactly one form, ``EMPTY_FORM``: ``null`` (the
connector's null) or ``empty`` (the connector's empty string). The other form is a different value and fails the check.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

NAMESPACE = "raw_records"
AIRBYTE_PREFIX = "_airbyte_"
STRING_TYPE = "string"
EMPTY_FORM = "null"
EMPTY_FORMS = ("null", "empty")
BYTE_ORDER_MARK = "﻿"
NOT_THE_EMPTY_VALUE = "\x01not the published empty value\x01"
SAMPLE_LIMIT = 3
HASH = re.compile(r"[0-9a-f]{64}")
PLAN_FORMAT = "table_check_plan_1"
RESULT_FORMAT = "table_checks_1"


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One loaded table. ``source`` is ``file`` for a verified CSV or ``pin`` for a pinned row count and digest. ``key`` is None for the whole row."""

    name: str
    source: str
    key: tuple[str, ...] | None
    pinned_columns: tuple[str, ...] = ()


TABLES = (
    TableSpec("clinical_data", "file", ("caseid",)),
    TableSpec("lab_data", "file", None),
    TableSpec("track_list", "pin", None, ("caseid", "tname", "tid")),
)
SPEC_BY_NAME: Mapping[str, TableSpec] = {spec.name: spec for spec in TABLES}


class RulesError(ValueError):
    """A plan or a pin is not of the form the checks accept. The message names the field and never holds a data value."""


def header_columns(header_line: str) -> list[str]:
    """The column names of a CSV header line. A leading byte order mark is the file's encoding mark and is not part of the first name."""
    text = header_line.removeprefix(BYTE_ORDER_MARK).rstrip("\r\n")
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if len(rows) != 1 or not rows[0] or any(not name for name in rows[0]):
        raise RulesError("the header line holds no column names, or an empty one")
    return rows[0]


def column_problems(expected: Sequence[str], found: Sequence[tuple[str, str]]) -> list[str]:
    """What differs between the file's column list and the table's ``(name, type)`` list once Airbyte's own columns are set aside. Empty when equal."""
    own = [(name, kind) for name, kind in found if not name.startswith(AIRBYTE_PREFIX)]
    names = [name for name, _ in own]
    problems: list[str] = []
    if len(set(names)) != len(names):
        problems.append("a column name is repeated in the table")
    missing = [name for name in expected if name not in names]
    extra = [name for name in names if name not in expected]
    if missing:
        problems.append(f"{len(missing)} column(s) of the file are missing from the table: {', '.join(missing[:SAMPLE_LIMIT])}")
    if extra:
        problems.append(f"{len(extra)} column(s) of the table are not in the file: {', '.join(extra[:SAMPLE_LIMIT])}")
    if not missing and not extra and names != list(expected):
        problems.append("the table holds the file's columns in another order")
    wrong = [name for name, kind in own if kind != STRING_TYPE]
    if wrong:
        problems.append(f"{len(wrong)} column(s) are not string columns: {', '.join(wrong[:SAMPLE_LIMIT])}")
    return problems


def table_text(value: str | None, empty_form: str = EMPTY_FORM) -> str:
    """The text of a loaded value for comparison: the one accepted form of the empty value reads as the empty string, the other form as a text no file holds."""
    if empty_form not in EMPTY_FORMS:
        raise RulesError(f"the empty form must be one of {', '.join(EMPTY_FORMS)}")
    if value is None:
        return "" if empty_form == "null" else NOT_THE_EMPTY_VALUE
    if value == "":
        return "" if empty_form == "empty" else NOT_THE_EMPTY_VALUE
    return value


def row_line(values: Iterable[str]) -> str:
    """One row of the track list as its digest reads it: the fields joined by a tab."""
    return "\t".join(values)


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
    if type(rows) is not int or rows < 0 or type(sha256) is not str or not HASH.fullmatch(sha256):
        raise RulesError("the release file's track_list pin has no row count `rows` and no digest `rows_sha256`")
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
    branch: str
    branch_hash: str
    empty_form: str
    tables: tuple[PlanTable, ...]


PLAN_FIELDS: Mapping[str, type] = {"format": str, "branch": str, "branch_hash": str, "empty_form": str, "tables": list}
FILE_TABLE_FIELDS: Mapping[str, type] = {"name": str, "snapshot_id": int, "file": str, "file_sha256": str, "file_size": int}
PIN_TABLE_FIELDS: Mapping[str, type] = {"name": str, "snapshot_id": int, "rows": int, "rows_sha256": str}
PLAIN_FILE_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


def closed(value: Any, fields: Mapping[str, type], what: str) -> dict[str, Any]:
    """The object ``value`` when its fields are exactly ``fields``, each of exactly its type. The message names the object and fields, never a value."""
    if type(value) is not dict or set(value) != set(fields) or any(type(value[name]) is not kind for name, kind in fields.items()):
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
    if len(fields["tables"]) != len(TABLES):
        raise RulesError(f"the plan must name exactly the {len(TABLES)} tables of the list")
    tables = tuple(plan_table(spec, entry) for spec, entry in zip(TABLES, fields["tables"], strict=True))
    return Plan(fields["branch"], fields["branch_hash"], fields["empty_form"], tables)


@dataclass(slots=True)
class Check:
    name: str
    passed: bool
    detail: str


@dataclass(slots=True)
class TableResult:
    table: str
    snapshot_id: int
    rows_table: int | None = None
    rows_source: int | None = None
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def add(self, name: str, passed: bool, detail: str) -> bool:
        self.checks.append(Check(name, passed, detail))
        return passed

    def document(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "snapshot_id": self.snapshot_id,
            "rows_table": self.rows_table,
            "rows_source": self.rows_source,
            "passed": self.passed,
            "checks": [{"name": check.name, "passed": check.passed, "detail": check.detail} for check in self.checks],
        }


def job_document(branch: str, branch_hash: str, empty_form: str, results: Sequence[TableResult]) -> dict[str, Any]:
    """What the Spark job reports: the branch and hash it read and one entry for each table, in the order of ``TABLES``."""
    return {
        "format": RESULT_FORMAT,
        "branch": branch,
        "branch_hash": branch_hash,
        "empty_form": empty_form,
        "passed": len(results) == len(TABLES) and all(result.passed for result in results),
        "tables": [result.document() for result in results],
    }
