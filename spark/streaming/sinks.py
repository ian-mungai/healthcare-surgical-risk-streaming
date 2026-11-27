"""The writes of one micro-batch, in their order: the lake tables, then the feature-ready events.

A lake write is a merge on the record's stable ID that inserts only the rows whose ID is new, so a batch that runs again after a stop adds
nothing. The feature-ready events are published only after every lake write of the batch succeeded, so a reader that sees the event can read the
row. A batch that stops between two writes runs again from its offsets, and its feature-ready events may then be published twice: a reader of the
topic drops a repeated feature ID. Kafka's own write guarantee is at least once (Spark 4.1.4 Kafka integration guide, "Writing Data to Kafka").
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

from spark.streaming.model import DecisionRecord, ErrorRecord, FeatureRecord, LedgerRecord, Record
from spark.streaming.rows import TABLES, TableSpec, ddl

SOURCE_VIEW = "stream_batch_updates"
WRITE_ORDER = (DecisionRecord, LedgerRecord, ErrorRecord, FeatureRecord)


def create_sql(spec: TableSpec) -> str:
    return f"CREATE TABLE IF NOT EXISTS {spec.name} ({ddl(spec.columns)}) USING iceberg"


def merge_sql(spec: TableSpec, source: str = SOURCE_VIEW) -> str:
    """Insert the rows of ``source`` whose key is not in the table yet. Nothing in the table is updated or deleted."""
    return f"MERGE INTO {spec.name} t USING {source} s ON t.{spec.key} = s.{spec.key} WHEN NOT MATCHED THEN INSERT *"


class BatchSinks(Protocol):
    def merge(self, spec: TableSpec, records: Sequence[Record]) -> None: ...

    def publish_ready(self, records: Sequence[FeatureRecord]) -> None: ...


def commit_batch(records: Sequence[Record], sinks: BatchSinks, after_merge: Callable[[int], None] | None = None) -> dict[str, int]:
    """Write the records of a batch, and return the count for each table. A failure stops it before the next write and before any feature-ready event."""
    counts: dict[str, int] = {}
    for index, spec in enumerate(TABLES[record_type] for record_type in WRITE_ORDER):
        mine = [record for record in records if isinstance(record, spec.record_type)]
        counts[spec.name] = len(mine)
        if mine:
            sinks.merge(spec, mine)
            if after_merge is not None:
                after_merge(index)
    features = [record for record in records if isinstance(record, FeatureRecord)]
    if features:
        sinks.publish_ready(features)
    return counts
