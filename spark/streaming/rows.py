"""The rows that cross Spark's edge, and the lake tables of the job's records.

An input event is one flat row of the decoded Kafka message and its position. The keyed operator returns one flat row for every record, the same
columns for all four kinds, with the columns that a kind does not use empty. Each kind of record has its own lake table, with one column for each
field of the record and a stable ID as its key. The Spark schemas are DDL texts, so this module needs no Spark.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from spark.streaming.model import DecisionRecord, ErrorRecord, Event, FeatureRecord, LedgerRecord, Record

INPUT_COLUMNS: list[tuple[str, str]] = [
    ("kind", "string"),
    ("event_id", "string"),
    ("run_id", "string"),
    ("room", "int"),
    ("occurrence_id", "string"),
    ("case_id", "int"),
    ("case_time_us", "long"),
    ("replay_time_us", "long"),
    ("track", "string"),
    ("discriminator", "int"),
    ("value", "double"),
    ("samples", "array<double>"),
    ("sample_rate", "double"),
    ("gain", "double"),
    ("physical_offset", "double"),
    ("start_offset_us", "long"),
    ("kafka_partition", "int"),
    ("kafka_offset", "long"),
]
KEY_COLUMNS = ("run_id", "room", "occurrence_id")
OUTPUT_COLUMNS: list[tuple[str, str]] = [
    ("record_type", "string"),
    ("record_id", "string"),
    ("run_id", "string"),
    ("room", "int"),
    ("occurrence_id", "string"),
    ("case_id", "int"),
    ("feature_version", "string"),
    ("cutoff_case_time_us", "long"),
    ("cutoff_replay_time_us", "long"),
    ("is_final", "boolean"),
    ("row_json", "string"),
    ("warning_eligible", "boolean"),
    ("eligibility_reasons", "array<string>"),
    ("event_id", "string"),
    ("kind", "string"),
    ("track", "string"),
    ("case_time_us", "long"),
    ("replay_time_us", "long"),
    ("watermark_us", "long"),
    ("lateness_us", "long"),
    ("decision", "string"),
    ("kafka_partition", "int"),
    ("kafka_offset", "long"),
    ("counts_json", "string"),
    ("message", "string"),
]
STATE_COLUMN = ("doc", "string")


def ddl(columns: list[tuple[str, str]] | tuple[tuple[str, str], ...]) -> str:
    return ", ".join(f"{name} {kind}" for name, kind in columns)


INPUT_DDL = ddl(INPUT_COLUMNS)
OUTPUT_DDL = ddl(OUTPUT_COLUMNS)
STATE_DDL = ddl((STATE_COLUMN,))


def event_row(event: Event) -> dict[str, Any]:
    return {
        "kind": event.kind,
        "event_id": event.event_id,
        "run_id": event.run_id,
        "room": event.room,
        "occurrence_id": event.occurrence_id,
        "case_id": event.case_id,
        "case_time_us": event.case_time_us,
        "replay_time_us": event.replay_time_us,
        "track": event.track,
        "discriminator": event.discriminator,
        "value": event.value,
        "samples": list(event.samples),
        "sample_rate": event.sample_rate,
        "gain": event.gain,
        "physical_offset": event.physical_offset,
        "start_offset_us": event.start_offset_us,
        "kafka_partition": event.kafka_partition,
        "kafka_offset": event.kafka_offset,
    }


def event_from_row(row: Mapping[str, Any]) -> Event:
    values = {name: row[name] for name, _ in INPUT_COLUMNS}
    values["samples"] = tuple(values["samples"] or ())
    return Event(**values)


def record_row(record: Record) -> dict[str, Any]:
    row: dict[str, Any] = {name: None for name, _ in OUTPUT_COLUMNS}
    if isinstance(record, FeatureRecord):
        row |= {
            "record_type": "feature",
            "record_id": record.feature_id,
            "run_id": record.run_id,
            "room": record.room,
            "occurrence_id": record.occurrence_id,
            "case_id": record.case_id,
            "feature_version": record.feature_version,
            "cutoff_case_time_us": record.cutoff_case_time_us,
            "cutoff_replay_time_us": record.cutoff_replay_time_us,
            "is_final": record.is_final,
            "row_json": record.row_json,
            "warning_eligible": record.warning_eligible,
            "eligibility_reasons": list(record.eligibility_reasons),
        }
    elif isinstance(record, DecisionRecord):
        row |= {
            "record_type": "decision",
            "record_id": record.decision_id,
            "run_id": record.run_id,
            "room": record.room,
            "occurrence_id": record.occurrence_id,
            "event_id": record.event_id,
            "kind": record.kind,
            "track": record.track,
            "case_time_us": record.case_time_us,
            "replay_time_us": record.replay_time_us,
            "watermark_us": record.watermark_us,
            "lateness_us": record.lateness_us,
            "decision": record.decision,
            "kafka_partition": record.kafka_partition,
            "kafka_offset": record.kafka_offset,
        }
    elif isinstance(record, LedgerRecord):
        row |= {
            "record_type": "ledger",
            "record_id": record.ledger_id,
            "run_id": record.run_id,
            "room": record.room,
            "occurrence_id": record.occurrence_id,
            "case_id": record.case_id,
            "counts_json": record.counts_json,
        }
    else:
        row |= {
            "record_type": "error",
            "record_id": record.error_id,
            "run_id": record.run_id,
            "room": record.room,
            "occurrence_id": record.occurrence_id,
            "cutoff_case_time_us": record.cutoff_case_time_us,
            "message": record.message,
        }
    return row


def record_from_row(row: Mapping[str, Any]) -> Record:
    kind = row["record_type"]
    if kind == "feature":
        return FeatureRecord(
            row["record_id"],
            row["run_id"],
            row["room"],
            row["occurrence_id"],
            row["case_id"],
            row["feature_version"],
            row["cutoff_case_time_us"],
            row["cutoff_replay_time_us"],
            row["is_final"],
            row["row_json"],
            row["warning_eligible"],
            tuple(row["eligibility_reasons"] or ()),
        )
    if kind == "decision":
        return DecisionRecord(
            row["record_id"],
            row["event_id"],
            row["run_id"],
            row["room"],
            row["occurrence_id"],
            row["kind"],
            row["track"],
            row["case_time_us"],
            row["replay_time_us"],
            row["watermark_us"],
            row["lateness_us"],
            row["decision"],
            row["kafka_partition"],
            row["kafka_offset"],
        )
    if kind == "ledger":
        return LedgerRecord(row["record_id"], row["run_id"], row["room"], row["occurrence_id"], row["case_id"], row["counts_json"])
    if kind == "error":
        return ErrorRecord(row["record_id"], row["run_id"], row["room"], row["occurrence_id"], row["cutoff_case_time_us"], row["message"])
    raise ValueError(f"unknown record type {kind!r}")


@dataclass(frozen=True, slots=True)
class TableSpec:
    record_type: type
    name: str
    key: str
    columns: tuple[tuple[str, str], ...]


TABLES: dict[type, TableSpec] = {
    FeatureRecord: TableSpec(
        FeatureRecord,
        "lake.feature.stream_features",
        "feature_id",
        (
            ("feature_id", "string"),
            ("run_id", "string"),
            ("room", "int"),
            ("occurrence_id", "string"),
            ("case_id", "int"),
            ("feature_version", "string"),
            ("cutoff_case_time_us", "bigint"),
            ("cutoff_replay_time_us", "bigint"),
            ("is_final", "boolean"),
            ("row_json", "string"),
            ("warning_eligible", "boolean"),
            ("eligibility_reasons", "array<string>"),
        ),
    ),
    DecisionRecord: TableSpec(
        DecisionRecord,
        "lake.stream.event_decisions",
        "decision_id",
        (
            ("decision_id", "string"),
            ("event_id", "string"),
            ("run_id", "string"),
            ("room", "int"),
            ("occurrence_id", "string"),
            ("kind", "string"),
            ("track", "string"),
            ("case_time_us", "bigint"),
            ("replay_time_us", "bigint"),
            ("watermark_us", "bigint"),
            ("lateness_us", "bigint"),
            ("decision", "string"),
            ("kafka_partition", "int"),
            ("kafka_offset", "bigint"),
        ),
    ),
    LedgerRecord: TableSpec(
        LedgerRecord,
        "lake.stream.occurrence_ledger",
        "ledger_id",
        (
            ("ledger_id", "string"),
            ("run_id", "string"),
            ("room", "int"),
            ("occurrence_id", "string"),
            ("case_id", "int"),
            ("counts_json", "string"),
        ),
    ),
    ErrorRecord: TableSpec(
        ErrorRecord,
        "lake.stream.feature_errors",
        "error_id",
        (
            ("error_id", "string"),
            ("run_id", "string"),
            ("room", "int"),
            ("occurrence_id", "string"),
            ("cutoff_case_time_us", "bigint"),
            ("message", "string"),
        ),
    ),
}


def table_values(record: Record) -> tuple[Any, ...]:
    """The values of a record in the column order of its table. An array is a list."""
    spec = TABLES[type(record)]
    values = []
    for name, _ in spec.columns:
        value = getattr(record, name)
        values.append(list(value) if isinstance(value, tuple) else value)
    return tuple(values)
