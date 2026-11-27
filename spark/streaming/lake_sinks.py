"""The Spark side of the sinks: Iceberg merges and the Kafka batch write.

Each micro-batch is collected on the driver once (its records are few: a feature row for each case occurrence and minute and the decisions that
are not on time), written to the lake tables with a merge that inserts only new IDs, and then published to the feature-ready topic with the
replay's ``Writer``, so the message bytes and the registry format ID are the replay's. The Kafka batch sink writes at least once (Spark 4.1.4,
Kafka integration guide).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from spark.streaming.model import FeatureRecord, Record
from spark.streaming.rows import TABLES, TableSpec, ddl, table_values
from spark.streaming.sinks import SOURCE_VIEW, create_sql, merge_sql
from spark.streaming.wire import feature_ready_payload

NAMESPACES = ("lake.feature", "lake.stream")
KAFKA_FRAME_DDL = "key binary, value binary"


class LakeSinks:
    def __init__(self, session: Any, writer: Any, bootstrap: str, topic: str) -> None:
        self.session = session
        self.writer = writer
        self.bootstrap = bootstrap
        self.topic = topic

    def create_tables(self) -> None:
        for namespace in NAMESPACES:
            self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        for spec in TABLES.values():
            self.session.sql(create_sql(spec))

    def merge(self, spec: TableSpec, records: Sequence[Record]) -> None:
        frame = self.session.createDataFrame([table_values(record) for record in records], schema=ddl(spec.columns))
        frame.createOrReplaceTempView(SOURCE_VIEW)
        self.session.sql(merge_sql(spec))

    def publish_ready(self, records: Sequence[FeatureRecord]) -> None:
        rows = [(str(record.room).encode(), self.writer.message("feature_ready", feature_ready_payload(record))) for record in records]
        frame = self.session.createDataFrame(rows, schema=KAFKA_FRAME_DDL)
        (
            frame.write.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap)
            .option("kafka.acks", "all")
            .option("kafka.enable.idempotence", "true")
            .option("topic", self.topic)
            .save()
        )
