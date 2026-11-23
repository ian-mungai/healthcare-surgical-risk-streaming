"""The streaming query: Kafka in, the decode step, the keyed operator, and the foreachBatch write.

Offsets and the operator's state are in the checkpoint folder on a volume that survives a restart, so a restarted query resumes from its last
committed batch and runs a batch that stopped midway again from the same offsets. ``foreachBatch`` gives at-least-once writes, and the merge on
the record IDs makes a second run of a batch add nothing (Spark 4.1.4, Structured Streaming guide, "ForeachBatch").
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable, Mapping
from typing import Any

from spark.streaming.decode import decode_row, registry_reader
from spark.streaming.processor import STATE_PROVIDER, make_processor
from spark.streaming.rows import INPUT_DDL, KEY_COLUMNS, OUTPUT_DDL, record_from_row
from spark.streaming.settings import StreamSettings
from spark.streaming.sinks import BatchSinks, commit_batch

SHUFFLE_PARTITIONS = "4"
TEST_EXIT_CODE = 75
SESSION_OPTIONS: Mapping[str, str] = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.streaming.stateStore.providerClass": STATE_PROVIDER,
    "spark.sql.shuffle.partitions": SHUFFLE_PARTITIONS,
}


def batch_handler(sinks: BatchSinks, settings: StreamSettings) -> Callable[[Any, int], None]:
    commits = 0

    def after_merge(index: int) -> None:
        nonlocal commits
        commits += 1
        if settings.test_exit_after_sink == commits:
            os.write(2, f"stream_features: test input: stopping after sink commit {commits}\n".encode())
            os._exit(TEST_EXIT_CODE)

    def handle(batch: Any, batch_id: int) -> None:
        records = [record_from_row(row.asDict()) for row in batch.collect()]
        commit_batch(records, sinks, after_merge)

    return handle


def start_query(session: Any, sinks: BatchSinks, settings: StreamSettings) -> Any:
    functions = importlib.import_module("pyspark.sql.functions")
    registry_url = settings.registry_url

    def decode(value: bytes, partition: int, offset: int) -> tuple[Any, ...]:
        return decode_row(registry_reader(registry_url), value, partition, offset)

    decode_udf = functions.udf(decode, INPUT_DDL)
    raw = (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap)
        .option("subscribe", settings.input_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .load()
    )
    events = raw.select(decode_udf(functions.col("value"), functions.col("partition"), functions.col("offset")).alias("event")).select("event.*")
    processor = make_processor(settings.late_limit_us, settings.cutoff_step_us, settings.clinical_file)
    keyed = events.groupBy(*KEY_COLUMNS).transformWithState(statefulProcessor=processor, outputStructType=OUTPUT_DDL, outputMode="Update", timeMode="None")
    return (
        keyed.writeStream.foreachBatch(batch_handler(sinks, settings))
        .outputMode("update")
        .option("checkpointLocation", settings.checkpoint_dir)
        .trigger(processingTime=f"{settings.trigger_seconds:g} seconds")
        .start()
    )
