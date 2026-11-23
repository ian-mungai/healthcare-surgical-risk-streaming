"""The Spark glue of the streaming job, proved with stand-ins for PySpark: the keyed operator's state handling and the query's options.

The real Spark, Kafka and Iceberg are proved by the acceptance test ``tests/e2e/stream_features.py``. Here the stand-ins hold what Spark would
hold: a value state that keeps one tuple for a key, a row class, and a session that records every call made on it.

Failure modes covered here:

- The operator forgets its state between two calls for one key, keeps it across two keys, or writes it in a form Spark's value state does not
  take.
- The query reads from the wrong topic or from the latest offsets, drops the checkpoint folder, groups by something other than the run, room and
  case occurrence, uses the event-time mode (where Spark drops late rows before the operator sees them), or writes outside ``foreachBatch``.
- The job's session lacks the Iceberg extensions that a merge needs, or the state store that ``transformWithState`` needs.
- The test input that stops the job after a sink commit stops it at the wrong sink, or when it is not set.
"""

from __future__ import annotations

import sys
import unittest
from typing import Any
from unittest import mock

from spark.streaming.model import FeatureRecord
from spark.streaming.query import SESSION_OPTIONS, TEST_EXIT_CODE, batch_handler, start_query
from spark.streaming.rows import INPUT_DDL, KEY_COLUMNS, OUTPUT_DDL, event_row, record_from_row
from spark.streaming.settings import StreamSettings
from tests.fast.stream_support import events_for, stamp, synthetic_recording

SETTINGS = StreamSettings("kafka:9092", "http://registry/apis/registry/v3", "/checkpoints/stream")


class FakeState:
    def __init__(self) -> None:
        self.value: tuple[Any, ...] | None = None

    def exists(self) -> bool:
        return self.value is not None

    def get(self) -> tuple[Any, ...] | None:
        return self.value

    def update(self, new: tuple[Any, ...]) -> None:
        if not (isinstance(new, tuple) and len(new) == 1 and isinstance(new[0], str)):
            raise TypeError("a value state takes one tuple of one text")
        self.value = new


class FakeHandle:
    def __init__(self) -> None:
        self.asked: list[tuple[str, str]] = []
        self.state = FakeState()

    def getValueState(self, name: str, schema: str) -> FakeState:  # noqa: N802
        self.asked.append((name, schema))
        return self.state


class Base:
    pass


class FakeRow(dict[str, Any]):
    def __init__(self, **fields: Any) -> None:
        super().__init__(fields)

    def asDict(self) -> dict[str, Any]:  # noqa: N802
        return dict(self)


def fake_pyspark() -> dict[str, Any]:
    sql, streaming = mock.MagicMock(), mock.MagicMock()
    sql.Row = FakeRow
    streaming.StatefulProcessor = Base
    return {"pyspark": mock.MagicMock(), "pyspark.sql": sql, "pyspark.sql.streaming": streaming, "pyspark.sql.functions": mock.MagicMock()}


class Processor(unittest.TestCase):
    def test_the_state_of_a_key_is_kept_between_calls_and_one_json_text_in_one_tuple(self) -> None:
        from spark.streaming.processor import STATE_NAME, make_processor

        events = [event_row(event) for event in stamp(events_for(synthetic_recording()))]
        with mock.patch.dict(sys.modules, fake_pyspark()):
            processor = make_processor(30_000_000, 60_000_000, "")
            handle = FakeHandle()
            processor.init(handle)
            first = list(processor.handleInputRows(("r", 0, "101.0"), iter([FakeRow(**row) for row in events[:300]]), None))
            held = handle.state.value
            second = list(processor.handleInputRows(("r", 0, "101.0"), iter([FakeRow(**row) for row in events[300:]]), None))
            empty = list(processor.handleInputRows(("r", 0, "101.0"), iter([]), None))
        self.assertEqual([(STATE_NAME, "doc string")], handle.asked)
        self.assertIsNotNone(held)
        self.assertEqual([], empty)
        records = [record_from_row(row) for row in first + second]
        features = [record for record in records if isinstance(record, FeatureRecord)]
        self.assertEqual([60_000_000 * minute for minute in range(1, 6)], sorted(record.cutoff_case_time_us for record in features))
        self.assertTrue(any(isinstance(record, FeatureRecord) and record.is_final for record in records))
        self.assertEqual(0, len([record for record in records if isinstance(record, FeatureRecord) and record.cutoff_case_time_us < 60_000_000]))


class Recorder:
    """A stand-in for a Spark object: every attribute and call is written to one log, and a call gives back a recorder, so a chain of calls is read back."""

    def __init__(self, log: list[tuple[str, tuple[Any, ...], dict[str, Any]]], path: str = "") -> None:
        self.log, self.path = log, path

    def __getattr__(self, name: str) -> Recorder:
        return Recorder(self.log, f"{self.path}.{name}" if self.path else name)

    def __call__(self, *args: Any, **kwargs: Any) -> Recorder:
        self.log.append((self.path, args, kwargs))
        return Recorder(self.log, f"{self.path}()")


def calls(log: list[tuple[str, tuple[Any, ...], dict[str, Any]]], name: str) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    return [(args, kwargs) for path, args, kwargs in log if path.rsplit(".", 1)[-1] == name]


class Query(unittest.TestCase):
    def started(self) -> list[tuple[str, tuple[Any, ...], dict[str, Any]]]:
        log: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        with mock.patch.dict(sys.modules, fake_pyspark()):
            start_query(Recorder(log), mock.MagicMock(), SETTINGS)
        return log

    def test_the_query_reads_the_input_topic_from_the_earliest_offset_and_fails_on_data_loss(self) -> None:
        log = self.started()
        self.assertEqual([(("kafka",), {})], calls(log, "format"))
        options = {args[0]: args[1] for args, _ in calls(log, "option")}
        self.assertEqual("kafka:9092", options["kafka.bootstrap.servers"])
        self.assertEqual("replay_events", options["subscribe"])
        self.assertEqual("earliest", options["startingOffsets"])
        self.assertEqual("true", options["failOnDataLoss"])

    def test_the_operator_is_keyed_by_run_room_and_occurrence_with_no_time_mode(self) -> None:
        log = self.started()
        self.assertEqual([(KEY_COLUMNS, {})], calls(log, "groupBy"))
        ((_, keywords),) = calls(log, "transformWithState")
        self.assertEqual((OUTPUT_DDL, "Update", "None"), (keywords["outputStructType"], keywords["outputMode"], keywords["timeMode"]))
        self.assertEqual(("run_id", "room", "occurrence_id"), KEY_COLUMNS)

    def test_the_write_is_a_foreach_batch_with_a_checkpoint_on_the_persistent_folder(self) -> None:
        log = self.started()
        self.assertEqual(1, len(calls(log, "foreachBatch")))
        self.assertIn((("checkpointLocation", "/checkpoints/stream"), {}), calls(log, "option"))
        self.assertEqual(1, len(calls(log, "start")))

    def test_the_decode_step_is_given_the_input_schema(self) -> None:
        with mock.patch.dict(sys.modules, fake_pyspark()):
            start_query(Recorder([]), mock.MagicMock(), SETTINGS)
            functions = sys.modules["pyspark.sql.functions"]
            self.assertEqual(INPUT_DDL, functions.udf.call_args.args[1])

    def test_the_session_has_the_iceberg_extensions_and_the_rocksdb_state_store(self) -> None:
        self.assertEqual("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", SESSION_OPTIONS["spark.sql.extensions"])
        self.assertTrue(SESSION_OPTIONS["spark.sql.streaming.stateStore.providerClass"].endswith("RocksDBStateStoreProvider"))


class TestInput(unittest.TestCase):
    def run_handler(self, stop_after: int | None, indexes: list[int]) -> list[int]:
        settings = StreamSettings("k", "r", "c", test_exit_after_sink=stop_after)
        written: list[int] = []

        class Sinks:
            def merge(self, spec: Any, records: Any) -> None:
                written.append(len(written))

            def publish_ready(self, records: Any) -> None:
                written.append(99)

        batch = mock.MagicMock()
        batch.collect.return_value = []
        with mock.patch("spark.streaming.query.commit_batch") as commit, mock.patch("os._exit") as leave, mock.patch("os.write"):
            commit.side_effect = lambda records, sinks, hook: [hook(index) for index in indexes]
            batch_handler(Sinks(), settings)(batch, 1)
        return [call.args[0] for call in leave.call_args_list]

    def test_the_process_stops_after_the_numbered_sink_commit_and_not_when_the_input_is_off(self) -> None:
        self.assertEqual([TEST_EXIT_CODE], self.run_handler(1, [0, 1, 2, 3]))
        self.assertEqual([TEST_EXIT_CODE], self.run_handler(2, [3, 1]))
        self.assertEqual([], self.run_handler(None, [0, 1, 2, 3]))
        self.assertEqual([], self.run_handler(3, [0, 1]))


if __name__ == "__main__":
    unittest.main()
