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
from spark.streaming.query import SESSION_OPTIONS, TEST_EXIT_CODE, QueryEnded, batch_handler, run_query, shut_down, start_query, stop_and_check, wait_for_query
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
        self.assertEqual([30_000_000, *(60_000_000 * minute for minute in range(1, 6))], sorted(record.cutoff_case_time_us for record in features))
        self.assertTrue(any(isinstance(record, FeatureRecord) and record.is_final for record in records))
        self.assertEqual(0, len([record for record in records if isinstance(record, FeatureRecord) and record.cutoff_case_time_us < 30_000_000]))


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


class FakeQuery:
    def __init__(self, active_for: int, failure: BaseException | None = None) -> None:
        self.waits, self.active_for, self.failure, self.stopped = 0, active_for, failure, False

    @property
    def isActive(self) -> bool:  # noqa: N802
        return self.waits < self.active_for

    def awaitTermination(self, timeout: float) -> bool:  # noqa: N802
        self.waits += 1
        return not self.isActive

    def exception(self) -> BaseException | None:
        return None if self.isActive else self.failure


class Waiting(unittest.TestCase):
    def test_a_query_that_ended_with_a_stored_exception_is_a_failure_not_a_success(self) -> None:
        failure = RuntimeError("the batch failed")
        self.assertIs(failure, wait_for_query(FakeQuery(2, failure), None, lambda: False, 5.0))

    def test_a_query_that_ended_by_itself_with_no_exception_is_a_failure_too(self) -> None:
        self.assertIsInstance(wait_for_query(FakeQuery(1), None, lambda: False, 5.0), QueryEnded)

    def test_a_query_that_runs_to_its_deadline_or_is_asked_to_stop_is_no_failure(self) -> None:
        self.assertIsNone(wait_for_query(FakeQuery(10**6), 0.0, lambda: False, 5.0))
        stops = iter([False, False, True])
        self.assertIsNone(wait_for_query(FakeQuery(10**6), None, lambda: next(stops), 5.0))

    def test_a_query_that_failed_at_the_moment_of_a_stop_request_is_still_a_failure(self) -> None:
        failure = RuntimeError("late failure")
        self.assertIs(failure, wait_for_query(FakeQuery(0, failure), None, lambda: True, 5.0))


class StoresBeforeInactive(FakeQuery):
    """Spark 4.1.4 stores the failure of a query before it marks the query inactive, so a stop or a deadline can come in between."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__(10**6, failure)

    def exception(self) -> BaseException | None:
        return self.failure

    def stop(self) -> None:
        self.stopped = True


class StopAndCheck(unittest.TestCase):
    def test_a_failure_stored_while_the_query_still_looked_active_is_found_after_the_stop(self) -> None:
        failure = RuntimeError("stored first")
        query = StoresBeforeInactive(failure)
        self.assertIsNone(wait_for_query(query, 0.0, lambda: False, 5.0))
        self.assertIs(failure, stop_and_check(query, None))
        self.assertTrue(query.stopped)

    def test_the_failure_that_the_wait_found_is_kept_and_no_failure_is_none(self) -> None:
        first, query = RuntimeError("first"), StoresBeforeInactive(RuntimeError("second"))
        self.assertIs(first, stop_and_check(query, first))
        quiet = FakeQuery(10**6)
        quiet.stop = lambda: None  # type: ignore[attr-defined]
        self.assertIsNone(stop_and_check(quiet, None))


class Throwing(FakeQuery):
    def __init__(self, stop_error: BaseException | None, stored: BaseException | None = None) -> None:
        super().__init__(10**6, stored)
        self.stop_error, self.stored = stop_error, stored

    def exception(self) -> BaseException | None:
        return self.stored

    def stop(self) -> None:
        if self.stop_error is not None:
            raise self.stop_error


class Session:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error, self.stopped = error, 0

    def stop(self) -> None:
        self.stopped += 1
        if self.error is not None:
            raise self.error


class ShutDown(unittest.TestCase):
    def test_the_stop_timeout_of_the_session_is_finite_and_inside_the_job_deadline(self) -> None:
        text = SESSION_OPTIONS["spark.sql.streaming.stopTimeout"]
        self.assertRegex(text, r"^[1-9][0-9]*s$")
        self.assertLessEqual(int(text.removesuffix("s")), 60)

    def test_a_stop_that_raises_does_not_replace_the_first_failure_and_the_session_is_still_stopped(self) -> None:
        first, session = RuntimeError("first"), Session()
        self.assertIs(first, shut_down(Throwing(TimeoutError("stop timed out")), session, first))
        self.assertEqual(1, session.stopped)

    def test_a_stored_exception_comes_before_a_stop_error_and_a_stop_error_is_a_failure_when_nothing_else_is(self) -> None:
        stored, stop_error = RuntimeError("stored"), TimeoutError("stop timed out")
        self.assertIs(stored, shut_down(Throwing(stop_error, stored), Session(), None))
        self.assertIs(stop_error, shut_down(Throwing(stop_error), Session(), None))

    def test_a_session_that_will_not_stop_is_a_failure_only_when_nothing_failed_before_and_the_query_is_stopped_first(self) -> None:
        error, query = OSError("session"), Throwing(None)
        self.assertIs(error, shut_down(query, Session(error), None))
        first = RuntimeError("first")
        self.assertIs(first, shut_down(Throwing(None), Session(error), first))
        self.assertIsNone(shut_down(Throwing(None), Session(), None))


class Logged:
    """A query and a session that write what is called on them to one log, in the order of the calls."""

    def __init__(self, log: list[str], errors: dict[str, BaseException] | None = None) -> None:
        self.log, self.errors = log, errors or {}
        self.waits = 0

    def step(self, name: str) -> None:
        self.log.append(name)
        if name in self.errors:
            raise self.errors[name]

    @property
    def isActive(self) -> bool:  # noqa: N802
        return True

    @property
    def lastProgress(self) -> dict[str, int]:  # noqa: N802
        self.step("progress")
        return {"batchId": 3}

    def awaitTermination(self, timeout: float) -> bool:  # noqa: N802
        self.step("wait")
        return False

    def exception(self) -> BaseException | None:
        self.step("exception")
        return None

    def stop(self) -> None:
        self.step("stop")


class RunQuery(unittest.TestCase):
    def test_the_query_is_stopped_before_the_session_and_both_are_always_stopped(self) -> None:
        log: list[str] = []
        query, session = Logged(log), Logged(log)
        query.stop, session.stop = lambda: log.append("query stop"), lambda: log.append("session stop")  # type: ignore[method-assign]
        progress, failure = run_query(query, session, 0.0, lambda: False, 5.0)
        self.assertIsNone(failure)
        self.assertEqual({"batchId": 3}, progress)
        self.assertEqual(["query stop", "session stop"], [entry for entry in log if entry.endswith("stop")])

    def test_the_wait_error_stays_first_when_the_progress_lookup_the_stop_and_the_session_all_fail_too(self) -> None:
        log: list[str] = []
        wait_error = RuntimeError("the wait failed")
        query = Logged(log, {"wait": wait_error, "progress": OSError("progress"), "stop": TimeoutError("stop")})
        session = Logged(log, {"stop": ValueError("session")})
        session.stop = lambda: (_ for _ in ()).throw(ValueError("session"))  # type: ignore[method-assign]
        progress, failure = run_query(query, session, None, lambda: False, 5.0)
        self.assertIs(wait_error, failure)
        self.assertIsNone(progress)

    def test_a_progress_lookup_that_fails_is_a_failure_when_nothing_failed_before(self) -> None:
        log: list[str] = []
        error = OSError("progress")
        _, failure = run_query(Logged(log, {"progress": error}), Logged(log), 0.0, lambda: False, 5.0)
        self.assertIs(error, failure)


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
