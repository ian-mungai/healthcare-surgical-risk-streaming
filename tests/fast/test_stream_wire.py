"""The edge between the wire and the streaming job's core.

Failure modes covered here:

- A decoded record of the replay becomes an event with a field lost or renamed (the gain and offset of a waveform, the discriminator), so the job and the replay
  disagree about what an event is.
- A marker is read as a source event, or an unknown marker or record is taken for an input event.
- A message that does not follow the wire format (no magic byte, no content ID) is decoded.
- The feature-ready payload has a field the registered format does not have, lacks one it has, or holds a value of the wrong type, so the registry refuses it.
"""

from __future__ import annotations

import unittest
from typing import Any

from replay.events.formats import FORMATS, KIND_FORMAT, schema_of
from replay.events.identity import Occurrence
from replay.events.schedule import ScheduledOccurrence
from replay.events.stream import occurrence_events, outgoing
from replay.events.wire import WireError, frame
from spark.streaming.decode import decode_row
from spark.streaming.model import FeatureRecord
from spark.streaming.rows import INPUT_COLUMNS, event_row
from spark.streaming.wire import StreamEventError, decode_message, event_from_record, feature_ready_payload
from tests.fast.stream_support import ORIGIN_US, RUN, synthetic_recording


def sent_payloads() -> list[tuple[str, str, dict[str, Any]]]:
    recording = synthetic_recording()
    scheduled = ScheduledOccurrence(Occurrence(0, 101, 0, 0), recording.times, ORIGIN_US)
    result: list[tuple[str, str, dict[str, Any]]] = []
    for case_event in occurrence_events(recording):
        sent = outgoing(RUN, scheduled, recording, case_event)
        result.append((sent.kind, FORMATS[KIND_FORMAT[sent.kind]].record_name, sent.payload))
    return result


class Reader:
    def __init__(self, name: str, payload: dict[str, Any]) -> None:
        self.name, self.payload = name, payload
        self.seen: list[bytes] = []

    def decode(self, message: bytes) -> tuple[int, dict[str, Any]]:
        self.seen.append(message)
        return 9, self.payload

    def record_name(self, content_id: int) -> str:
        return self.name


class Events(unittest.TestCase):
    def test_every_kind_the_replay_sends_keeps_each_of_its_fields(self) -> None:
        kinds = set()
        for kind, record_name, payload in sent_payloads():
            event = event_from_record(record_name, payload, 3, 41)
            kinds.add(kind)
            self.assertEqual(kind, event.kind)
            self.assertEqual(
                (payload["event_id"], payload["run_id"], payload["room"], payload["occurrence_id"]),
                (event.event_id, event.run_id, event.room, event.occurrence_id),
            )
            self.assertEqual(
                (payload["case_id"], payload["case_time_us"], payload["replay_time_us"]), (event.case_id, event.case_time_us, event.replay_time_us)
            )
            self.assertEqual((3, 41), (event.kafka_partition, event.kafka_offset))
            if kind == "numeric":
                self.assertEqual((payload["track"], payload["discriminator"], payload["value"]), (event.track, event.discriminator, event.value))
            if kind == "waveform":
                self.assertEqual(tuple(payload["samples"]), event.samples)
                self.assertEqual(
                    (payload["sample_rate"], payload["gain"], payload["offset"], payload["start_offset_us"]),
                    (event.sample_rate, event.gain, event.physical_offset, event.start_offset_us),
                )
        self.assertEqual({"numeric", "waveform", "string", "final_score", "case_end"}, kinds)

    def test_a_marker_is_a_marker_and_its_id_is_not_a_source_id(self) -> None:
        markers = [event_from_record(name, payload, 0, 0) for kind, name, payload in sent_payloads() if kind in ("final_score", "case_end")]
        self.assertEqual(["final_score", "case_end"], [event.kind for event in markers])
        self.assertTrue(all("|!" in event.event_id for event in markers))

    def test_an_unknown_record_or_marker_is_refused(self) -> None:
        _, _, payload = sent_payloads()[0]
        with self.assertRaises(StreamEventError):
            event_from_record("FeatureReady", payload, 0, 0)
        marker = next(payload for kind, _, payload in sent_payloads() if kind == "final_score")
        with self.assertRaises(StreamEventError):
            event_from_record("CaseLifecycle", {**marker, "marker": "OTHER"}, 0, 0)

    def test_the_suffix_of_an_event_id_is_its_track_case_time_and_discriminator(self) -> None:
        _, name, payload = next(item for item in sent_payloads() if item[0] == "numeric")
        event = event_from_record(name, payload, 0, 0)
        self.assertEqual(f"{payload['track']}|{payload['case_time_us']}|{payload['discriminator']}", event.suffix)


class Messages(unittest.TestCase):
    def test_a_framed_message_is_decoded_by_the_reader_and_a_message_without_the_prefix_is_refused(self) -> None:
        kind, name, payload = sent_payloads()[0]
        reader = Reader(name, payload)
        message = frame(9, b"body")
        event = decode_message(reader, message, 2, 7)
        self.assertEqual([message], reader.seen)
        self.assertEqual((kind, 2, 7), (event.kind, event.kafka_partition, event.kafka_offset))
        for bad in (b"", b"{}", b"\x01\x00\x00\x00\x09body"):
            with self.subTest(bad=bad), self.assertRaises(WireError):
                decode_message(reader, bad, 0, 0)


class DecodeStep(unittest.TestCase):
    def test_a_message_becomes_one_row_in_the_input_column_order_and_a_bad_message_raises(self) -> None:
        kind, name, payload = next(item for item in sent_payloads() if item[0] == "waveform")
        row = decode_row(Reader(name, payload), frame(9, b"body"), 4, 11)
        event = event_from_record(name, payload, 4, 11)
        self.assertEqual(tuple(event_row(event)[column] for column, _ in INPUT_COLUMNS), row)
        self.assertEqual(len(INPUT_COLUMNS), len(row))
        with self.assertRaises(WireError):
            decode_row(Reader(name, payload), b"not framed", 0, 0)
        with self.assertRaises(StreamEventError):
            decode_row(Reader("FeatureReady", payload), frame(9, b"body"), 0, 0)


class FeatureReady(unittest.TestCase):
    def record(self) -> FeatureRecord:
        return FeatureRecord("id|1", RUN, 4, "101.0", 101, "features_x", 60_000_000, ORIGIN_US + 60_000_000, True, "{}", True, ())

    def test_the_payload_has_exactly_the_fields_of_the_registered_format_with_their_types(self) -> None:
        schema = schema_of("feature_ready")
        payload = feature_ready_payload(self.record())
        self.assertEqual([field["name"] for field in schema["fields"]], list(payload))
        python_types = {"string": str, "int": int, "long": int, "boolean": bool}
        for field in schema["fields"]:
            self.assertIs(python_types[field["type"]], type(payload[field["name"]]), field["name"])

    def test_the_payload_names_the_occurrence_the_feature_id_the_cutoff_and_the_replay_time(self) -> None:
        payload = feature_ready_payload(self.record())
        self.assertEqual(
            ("101.0", "id|1", 60_000_000, ORIGIN_US + 60_000_000, True),
            (payload["occurrence_id"], payload["feature_id"], payload["cutoff_case_time_us"], payload["cutoff_replay_time_us"], payload["is_final"]),
        )


if __name__ == "__main__":
    unittest.main()
