"""A run's saved state is bound to the input topic that its messages went to, so a restart can never continue a run's position on another topic.

Failure modes covered here:

- The state does not say which topic its position counts, so a run record with another input topic resumes the position of the first topic and the new topic
  gets only the tail of the events (or the deployed ``replay_events`` gets events that a test batch meant for its own topic).
- A state that an older replay wrote (no topic in it) is refused or taken for another topic, when it counts the deployed ``replay_events``.
- A mismatch is found only after the first message was sent.
- The sink and the run record accept different names, so a name that the record refuses reaches Kafka, or the reverse.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import unittest
from typing import Any
from unittest import mock

from replay.events.formats import INPUT_TOPIC, is_input_topic
from replay.events.run import Replayer, ReplayError, RunSettings, RunState
from replay.service.run_record import RunRecordError, parse_run_record
from tests.fast.replay_support import FakeClock, MemorySink, MemoryStore, RecordingSource, boundary_recordings, boundary_schedule, saved_state
from tests.fast.test_replay_input_topic import BASE, LIBRARIES, FakeWriter, record_text

OWN = "replay_events_p8h_0123abcd_control"
OTHER = "replay_events_p8h_0123abcd_resent"
OPTIONS: dict[str, Any] = {"rooms": 2, "window_us": 40_000_000}


def settings(topic: str = INPUT_TOPIC) -> RunSettings:
    return RunSettings(speed=1000.0, checkpoint_every=5, idle_checkpoint_us=10**12, input_topic=topic)


def replayer(store: MemoryStore, sink: MemorySink, topic: str = INPUT_TOPIC) -> Replayer:
    return Replayer(boundary_schedule(**OPTIONS), settings(topic), RecordingSource(boundary_recordings()), sink, store, FakeClock())


class StateNamesItsTopic(unittest.TestCase):
    def test_a_saved_state_names_the_topic_of_its_run(self) -> None:
        store = MemoryStore()
        self.assertTrue(replayer(store, MemorySink(), OWN).run().finished)
        self.assertEqual(OWN, json.loads(saved_state(store).to_json())["input_topic"])
        self.assertEqual(OWN, RunState.from_json(saved_state(store).to_json()).input_topic)

    def test_a_run_without_a_named_topic_saves_the_deployed_one(self) -> None:
        store = MemoryStore()
        replayer(store, MemorySink()).run()
        self.assertEqual(INPUT_TOPIC, saved_state(store).input_topic)

    def test_a_state_that_names_a_topic_nobody_may_use_is_refused(self) -> None:
        store = MemoryStore()
        replayer(store, MemorySink(), OWN).run()
        for name in ("feature_ready", "Replay_events_x", "", 7, None):
            with self.subTest(name=name):
                document = json.loads(saved_state(store).to_json())
                document["input_topic"] = name
                with self.assertRaises(ReplayError):
                    RunState.from_json(json.dumps(document))


class MismatchIsRefusedBeforeAnySend(unittest.TestCase):
    def stopped_run(self, topic: str) -> MemoryStore:
        """A run on ``topic`` that stopped part way: its state holds a position."""
        store = MemoryStore()
        flag = {"stop": False}
        sink = MemorySink()
        sink.on_send = lambda event: flag.update(stop=len(sink.sent) >= 8)
        run = Replayer(boundary_schedule(**OPTIONS), settings(topic), RecordingSource(boundary_recordings()), sink, store, FakeClock(), lambda: flag["stop"])
        self.assertFalse(run.run().finished)
        self.assertGreater(len(saved_state(store).positions), 0)
        return store

    def test_a_record_with_another_topic_does_not_resume_the_position_of_the_first(self) -> None:
        store = self.stopped_run(OWN)
        sink = MemorySink()
        with self.assertRaisesRegex(ReplayError, "input topic"):
            replayer(store, sink, OTHER).run()
        self.assertEqual(([], 0), (sink.sent, sink.acknowledged))

    def test_the_deployed_topic_is_refused_for_the_state_of_a_run_topic(self) -> None:
        store = self.stopped_run(OWN)
        sink = MemorySink()
        with self.assertRaises(ReplayError):
            replayer(store, sink, INPUT_TOPIC).run()
        self.assertEqual([], sink.sent)

    def test_the_same_topic_resumes(self) -> None:
        store = self.stopped_run(OWN)
        self.assertTrue(replayer(store, MemorySink(), OWN).run().finished)

    def test_a_state_that_an_older_replay_wrote_counts_the_deployed_topic(self) -> None:
        store = self.stopped_run(INPUT_TOPIC)
        document = json.loads(saved_state(store).to_json())
        del document["input_topic"]
        legacy = json.dumps(document)
        self.assertEqual(INPUT_TOPIC, RunState.from_json(legacy).input_topic)

        class LegacyStore(MemoryStore):
            def load(self) -> RunState:
                return RunState.from_json(legacy)

        sink = MemorySink()
        with self.assertRaisesRegex(ReplayError, "input topic"):
            replayer(LegacyStore(), sink, OWN).run()
        self.assertEqual([], sink.sent)
        self.assertTrue(replayer(LegacyStore(), MemorySink()).run().finished)


class SinkAndRecordAgree(unittest.TestCase):
    NAMES = (
        INPUT_TOPIC,
        OWN,
        "replay_events_x",
        "feature_ready",
        "warnings",
        "risk_scores",
        "replay_events_",
        "Replay_events_x",
        "replay_events_a/b",
        "replay_events_a b",
        "replay_events_a\n",
        "replay_events_" + "a" * 201,
        "other",
        "",
        "__consumer_offsets",
    )

    def setUp(self) -> None:
        stand_ins = {name: mock.MagicMock() for name in LIBRARIES if importlib.util.find_spec(name.split(".")[0]) is None}
        patched = mock.patch.dict(sys.modules, stand_ins)
        patched.start()
        self.addCleanup(patched.stop)
        for name in ("replay.service.kafka_io", "replay.service.wire_codec"):
            sys.modules.pop(name, None)
            self.addCleanup(sys.modules.pop, name, None)
        self.kafka_io = importlib.import_module("replay.service.kafka_io")

    def sink_accepts(self, name: str) -> bool:
        try:
            with mock.patch.object(self.kafka_io, "Producer"):
                self.kafka_io.KafkaSink("kafka:9092", FakeWriter(), 10, input_topic=name)
        except self.kafka_io.KafkaStartError:
            return False
        return True

    def record_accepts(self, name: str) -> bool:
        try:
            parse_run_record(json.dumps({**BASE, "input_topic": name}))
        except RunRecordError:
            return False
        return True

    def test_the_sink_refuses_every_topic_that_is_not_the_deployed_one_or_a_run_owned_name(self) -> None:
        for name in self.NAMES:
            with self.subTest(name=name):
                self.assertEqual(name == INPUT_TOPIC or name.startswith("replay_events_p8h_") or name == "replay_events_x", self.sink_accepts(name))

    def test_the_sink_and_the_record_parser_and_the_shared_check_accept_the_same_names(self) -> None:
        for name in self.NAMES:
            with self.subTest(name=name):
                self.assertEqual(self.record_accepts(name), self.sink_accepts(name))
                self.assertEqual(self.record_accepts(name), is_input_topic(name))

    def test_a_refused_sink_sent_nothing(self) -> None:
        with mock.patch.object(self.kafka_io, "Producer") as producer, self.assertRaises(self.kafka_io.KafkaStartError):
            self.kafka_io.KafkaSink("kafka:9092", FakeWriter(), 10, input_topic="feature_ready")
        producer.assert_not_called()

    def test_the_record_text_helper_still_parses_a_good_name(self) -> None:
        self.assertEqual(OWN, parse_run_record(record_text(input_topic=OWN)).input_topic)


if __name__ == "__main__":
    unittest.main()
