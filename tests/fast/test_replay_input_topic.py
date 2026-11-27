"""A replay run can be told to use an input topic of its own, so a test batch never writes to or reads from the platform's ``replay_events``.

Failure modes covered here:

- The run record has no ``input_topic`` field, or accepts a name that is a platform topic (``feature_ready``, ``warnings``), an upper-case or path-like name.
- The sink sends an input event to ``replay_events`` when the run named another input topic, or sends the output format's event to the input topic.
- The partition lookup and the observe command read ``replay_events`` when the run named another input topic (checked in the module's source: it imports
  the publisher's library).
- A record that names no input topic stops using ``replay_events``.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from replay.events.formats import INPUT_TOPIC
from replay.events.stream import OutgoingEvent
from replay.service.run_record import RunRecordError, parse_run_record

BASE: dict[str, Any] = {
    "run_id": "replay_0123abcd",
    "rooms": 2,
    "speed": 300.0,
    "stagger_seconds": 0,
    "turnover_seconds": 0,
    "window_seconds": 60,
    "manifest_reference": "/opt/replay/run/manifest_reference.json",
    "object_store": {"kind": "folder", "root": "/landed"},
    "kafka_bootstrap": "kafka:9092",
    "registry_url": "http://apicurio:8080/apis/registry/v3",
    "state_dir": "/opt/replay/state/replay_0123abcd",
}


def record_text(**changes: Any) -> str:
    return json.dumps({**BASE, **changes})


class FakeWriter:
    def message(self, format_key: str, payload: dict[str, Any]) -> bytes:
        return b"x"


class RunRecordInputTopic(unittest.TestCase):
    def test_a_record_without_the_field_uses_the_deployed_input_topic(self) -> None:
        self.assertEqual(INPUT_TOPIC, parse_run_record(record_text()).input_topic)

    def test_a_run_owned_input_topic_is_accepted(self) -> None:
        for name in ("replay_events_p8h_0123abcd_control", "replay_events_x", INPUT_TOPIC):
            with self.subTest(name=name):
                self.assertEqual(name, parse_run_record(record_text(input_topic=name)).input_topic)

    def test_a_topic_that_is_not_an_input_topic_is_refused(self) -> None:
        for name in ("feature_ready", "warnings", "risk_scores", "replay_events_", "Replay_events_x", "replay_events_a/b", "replay_events_a b", "other", ""):
            with self.subTest(name=name), self.assertRaises(RunRecordError):
                parse_run_record(record_text(input_topic=name))

    def test_a_topic_that_is_not_text_is_refused(self) -> None:
        with self.assertRaises(RunRecordError):
            parse_run_record(record_text(input_topic=7))


LIBRARIES = ("confluent_kafka", "confluent_kafka.admin", "fastavro", "fastavro.schema")


class SinkTopics(unittest.TestCase):
    """The sink module needs the Kafka and Avro libraries of the replay image: where one is missing a stand-in takes its place for the import only."""

    def setUp(self) -> None:
        stand_ins = {name: mock.MagicMock() for name in LIBRARIES if importlib.util.find_spec(name.split(".")[0]) is None}
        patched = mock.patch.dict(sys.modules, stand_ins)
        patched.start()
        self.addCleanup(patched.stop)
        for name in ("replay.service.kafka_io", "replay.service.wire_codec"):
            sys.modules.pop(name, None)
            self.addCleanup(sys.modules.pop, name, None)
        self.kafka_io = importlib.import_module("replay.service.kafka_io")

    def sink(self, **options: Any) -> tuple[Any, mock.MagicMock]:
        with mock.patch.object(self.kafka_io, "Producer") as producer:
            sink = self.kafka_io.KafkaSink("kafka:9092", FakeWriter(), 10, **options)
        return sink, producer.return_value

    def event(self, kind: str = "numeric") -> OutgoingEvent:
        return OutgoingEvent(kind, 3, "9101.0", "id", 1, {})  # type: ignore[arg-type]

    def test_an_input_event_goes_to_the_named_input_topic(self) -> None:
        sink, producer = self.sink(input_topic="replay_events_p8h_x")
        sink.send(self.event())
        self.assertEqual("replay_events_p8h_x", producer.produce.call_args.args[0])

    def test_without_a_name_an_input_event_goes_to_the_deployed_input_topic(self) -> None:
        sink, producer = self.sink()
        sink.send(self.event())
        self.assertEqual(INPUT_TOPIC, producer.produce.call_args.args[0])

    def test_every_input_kind_follows_the_named_topic(self) -> None:
        sink, producer = self.sink(input_topic="replay_events_p8h_x")
        for kind in ("numeric", "waveform", "string", "op_start", "final_score", "case_end"):
            sink.send(self.event(kind))
        self.assertEqual({"replay_events_p8h_x"}, {call.args[0] for call in producer.produce.call_args_list})


class FakeConsumer:
    """A consumer over fixed messages of one partition: the end offset is the count, and ``poll`` returns them in order."""

    def __init__(self, messages: list[Any]) -> None:
        self.messages = list(messages)

    def list_topics(self, topic: str, timeout: float) -> Any:
        return mock.Mock(topics={topic: mock.Mock(partitions={0: None})})

    def get_watermark_offsets(self, partition: Any, timeout: float) -> tuple[int, int]:
        return 0, len(self.messages)

    def assign(self, partitions: Any) -> None:
        pass

    def poll(self, timeout: float) -> Any:
        return self.messages.pop(0) if self.messages else None

    def close(self) -> None:
        pass


class FakeMessage:
    def __init__(self, offset: int, stamp: tuple[int, int]) -> None:
        self.at, self.stamp = offset, stamp

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "replay_events_p8h_x"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self.at

    def value(self) -> bytes:
        return b"x"

    def timestamp(self) -> tuple[int, int]:
        return self.stamp


class FakeReader:
    def decode(self, value: bytes) -> tuple[int, dict[str, Any]]:
        return 1, {}

    def record_name(self, content_id: int) -> str:
        return "NumericReading"


class ReadAppendTimes(unittest.TestCase):
    """The reader keeps the broker's own append time of a message when the topic stamps it, and never takes a producer's time for it."""

    def setUp(self) -> None:
        stand_ins = {name: mock.MagicMock() for name in LIBRARIES if importlib.util.find_spec(name.split(".")[0]) is None}
        patched = mock.patch.dict(sys.modules, stand_ins)
        patched.start()
        self.addCleanup(patched.stop)
        for name in ("replay.service.kafka_io", "replay.service.wire_codec"):
            sys.modules.pop(name, None)
            self.addCleanup(sys.modules.pop, name, None)
        self.kafka_io = importlib.import_module("replay.service.kafka_io")

    def read(self, stamps: list[tuple[int, int]]) -> list[Any]:
        consumer = FakeConsumer([FakeMessage(offset, stamp) for offset, stamp in enumerate(stamps)])
        with mock.patch.object(self.kafka_io, "Consumer", lambda settings: consumer):
            received: list[Any] = self.kafka_io.read_all("kafka:9092", FakeReader(), ["replay_events_p8h_x"], 5.0)
        return received

    def test_a_message_of_a_topic_that_stamps_append_time_carries_it(self) -> None:
        received = self.read([(self.kafka_io.TIMESTAMP_LOG_APPEND_TIME, 1_700_000_000_123), (self.kafka_io.TIMESTAMP_LOG_APPEND_TIME, 1_700_000_000_456)])
        self.assertEqual([1_700_000_000_123, 1_700_000_000_456], [item.appended_ms for item in received])

    def test_a_producers_create_time_is_not_an_append_time(self) -> None:
        received = self.read([(CREATE_TIME, 5), (NOT_AVAILABLE, 0)])
        self.assertEqual([None, None], [item.appended_ms for item in received])


CREATE_TIME, NOT_AVAILABLE = 1, 0
SERVICE = Path(__file__).resolve().parents[2] / "replay" / "service" / "__main__.py"


def function_named(tree: ast.Module, name: str) -> ast.FunctionDef:
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


class CommandsUseTheRecordsTopic(unittest.TestCase):
    """The service module imports the publisher's library, which this check's environment does not hold, so the wiring is read from its source."""

    def setUp(self) -> None:
        self.tree = ast.parse(SERVICE.read_text())

    def calls(self, function: str, callee: str) -> list[str]:
        return [ast.unparse(node) for node in ast.walk(function_named(self.tree, function)) if isinstance(node, ast.Call) and ast.unparse(node.func) == callee]

    def names(self, function: str) -> set[str]:
        return {node.id for node in ast.walk(function_named(self.tree, function)) if isinstance(node, ast.Name)}

    def test_the_replay_looks_up_the_partitions_of_the_records_topic(self) -> None:
        self.assertEqual(["partition_count(record.kafka_bootstrap, record.input_topic)"], self.calls("replay_record", "partition_count"))

    def test_the_replays_settings_carry_the_records_topic_for_its_saved_state(self) -> None:
        settings = self.calls("replay_record", "RunSettings")
        self.assertEqual(1, len(settings))
        self.assertIn("record.input_topic", settings[0])

    def test_the_replay_builds_its_sink_for_the_records_topic(self) -> None:
        sinks = self.calls("replay_record", "KafkaSink")
        self.assertEqual(1, len(sinks))
        self.assertIn("input_topic=record.input_topic", sinks[0])

    def test_observe_reads_the_records_topic_only(self) -> None:
        self.assertEqual(["read_all(record.kafka_bootstrap, reader, [record.input_topic], READ_DEADLINE_SECONDS)"], self.calls("command_observe", "read_all"))

    def test_neither_command_names_the_deployed_topic_itself(self) -> None:
        for function in ("replay_record", "command_observe"):
            with self.subTest(function=function):
                self.assertNotIn("INPUT_TOPIC", self.names(function))


if __name__ == "__main__":
    unittest.main()
