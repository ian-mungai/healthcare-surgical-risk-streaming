"""The streaming query: Kafka in, the decode step, the keyed operator, and the foreachBatch write.

Offsets and the operator's state are in the checkpoint folder on a volume that survives a restart, so a restarted query resumes from its last
committed batch and runs a batch that stopped midway again from the same offsets. ``foreachBatch`` gives at-least-once writes, and the merge on
the record IDs makes a second run of a batch add nothing (Spark 4.1.4, Structured Streaming guide, "ForeachBatch").
"""

from __future__ import annotations

import importlib
import os
import time
from collections.abc import Callable, Mapping
from typing import Any

from spark.streaming.decode import decode_row, registry_reader
from spark.streaming.processor import STATE_PROVIDER, make_processor
from spark.streaming.rows import INPUT_DDL, KEY_COLUMNS, OUTPUT_DDL, record_from_row
from spark.streaming.settings import StreamSettings
from spark.streaming.sinks import BatchSinks, commit_batch

SHUFFLE_PARTITIONS = "4"
STOP_TIMEOUT = "30s"
TEST_EXIT_CODE = 75
SESSION_OPTIONS: Mapping[str, str] = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.streaming.stateStore.providerClass": STATE_PROVIDER,
    "spark.sql.shuffle.partitions": SHUFFLE_PARTITIONS,
    "spark.sql.streaming.stopTimeout": STOP_TIMEOUT,
}


class QueryEnded(RuntimeError):
    """The query is no longer active, nobody stopped it and Spark kept no exception."""


def wait_for_query(query: Any, deadline: float | None, stop_requested: Callable[[], bool], slice_seconds: float) -> BaseException | None:
    """Wait in short slices until the deadline passes, a stop is requested or the query ends. Returns the failure of a query that ended, or None.

    A query that is no longer active has failed whenever Spark stored an exception for it, also when a stop request or the deadline came at the same moment.
    A query that ended with no exception and no stop is a failure as well.
    """
    try:
        while query.isActive and not stop_requested() and (deadline is None or time.monotonic() < deadline):
            query.awaitTermination(slice_seconds if deadline is None else min(slice_seconds, max(0.1, deadline - time.monotonic())))
    except BaseException as error:
        return error
    try:
        if query.isActive:
            return None
        stored: BaseException | None = query.exception()
    except BaseException as error:
        return error
    if stored is not None:
        return stored
    return None if stop_requested() else QueryEnded("the query ended with no exception and was not stopped")


def stop_and_check(query: Any, failure: BaseException | None) -> BaseException | None:
    """Stop the query, then return the failure that the wait found, or else the exception that Spark stored.

    Spark 4.1.4 stores the failure of a query before it marks the query inactive, so a stop request or a deadline can end the wait while the query
    still looks active.
    """
    stop_error: BaseException | None = None
    try:
        query.stop()
    except BaseException as error:
        stop_error = error
    stored: BaseException | None = None
    try:
        stored = query.exception()
    except BaseException as error:
        stored = stored or error
    for found in (failure, stored, stop_error):
        if found is not None:
            return found
    return None


def shut_down(query: Any, session: Any, failure: BaseException | None) -> BaseException | None:
    """Stop the query and then the session, each on its own, and return the first failure.

    The stop of the query waits at most ``spark.sql.streaming.stopTimeout`` (Spark's default is to wait without end). A query stop that raises is a failure
    but never hides an earlier one, and the session is stopped whatever happened to the query.
    """
    first = stop_and_check(query, failure)
    try:
        session.stop()
    except BaseException as error:
        first = first or error
    return first


def run_query(query: Any, session: Any, deadline: float | None, stop_requested: Callable[[], bool], slice_seconds: float) -> tuple[Any, BaseException | None]:
    """Wait for the query, read its last progress, stop the query and then the session. Returns the progress and the first failure, in that order of time.

    The error of the wait comes first. A progress lookup that raises is a failure only when nothing failed before, and the stops run whatever happened.
    """
    failure = wait_for_query(query, deadline, stop_requested, slice_seconds)
    progress: Any = None
    try:
        progress = query.lastProgress
    except BaseException as error:
        failure = failure or error
    return progress, shut_down(query, session, failure)


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
