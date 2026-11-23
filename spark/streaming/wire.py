"""The edge between the wire and the core: a decoded message becomes an ``Event``, and a feature record becomes the payload of a feature-ready
message.

The bytes are framed by ``replay.events.wire`` and coded by the replay's ``Reader`` and ``Writer`` (``replay/service/wire_codec.py``), so the
replay and the streaming job use one definition of the wire bytes and of the registry format ID. Nothing here imports Avro or Kafka: a reader is
anything with ``decode``.
"""

from __future__ import annotations

from typing import Any, Protocol

from replay.events.errors import EventModelError
from replay.events.formats import FORMATS
from replay.events.wire import unframe
from spark.streaming.model import Event, FeatureRecord, Kind

RECORD_KINDS: dict[str, Kind] = {
    FORMATS["numeric"].record_name: "numeric",
    FORMATS["waveform"].record_name: "waveform",
    FORMATS["string_event"].record_name: "string",
}
MARKER_KINDS: dict[str, Kind] = {"FINAL_SCORE": "final_score", "CASE_END": "case_end"}


class StreamEventError(EventModelError):
    """A message that is not an input event of the stream."""


class MessageReader(Protocol):
    def decode(self, message: bytes) -> tuple[int, dict[str, Any]]: ...

    def record_name(self, content_id: int) -> str: ...


def event_from_record(record_name: str, payload: dict[str, Any], kafka_partition: int, kafka_offset: int) -> Event:
    """The event of one decoded record and the Kafka position it came from."""
    if record_name == FORMATS["case_lifecycle"].record_name:
        kind = MARKER_KINDS.get(str(payload["marker"]))
        if kind is None:
            raise StreamEventError(f"unknown marker {payload['marker']!r}")
    elif record_name in RECORD_KINDS:
        kind = RECORD_KINDS[record_name]
    else:
        raise StreamEventError(f"the record {record_name} is not an input event of the stream")
    common: dict[str, Any] = {
        "kind": kind,
        "event_id": payload["event_id"],
        "run_id": payload["run_id"],
        "room": payload["room"],
        "occurrence_id": payload["occurrence_id"],
        "case_id": payload["case_id"],
        "case_time_us": payload["case_time_us"],
        "replay_time_us": payload["replay_time_us"],
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }
    if kind == "numeric":
        common |= {"track": payload["track"], "discriminator": payload["discriminator"], "value": payload["value"]}
    elif kind == "string":
        common |= {"track": payload["track"], "discriminator": payload["discriminator"]}
    elif kind == "waveform":
        common |= {
            "track": payload["track"],
            "discriminator": payload["discriminator"],
            "samples": tuple(payload["samples"]),
            "sample_rate": payload["sample_rate"],
            "gain": payload["gain"],
            "physical_offset": payload["offset"],
            "start_offset_us": payload["start_offset_us"],
        }
    return Event(**common)


def decode_message(reader: MessageReader, message: bytes, kafka_partition: int, kafka_offset: int) -> Event:
    """Read one framed message from Kafka into an event. A message that does not follow the wire format or is not an input event is refused."""
    unframe(message)
    content_id, payload = reader.decode(message)
    return event_from_record(reader.record_name(content_id), payload, kafka_partition, kafka_offset)


def feature_ready_payload(record: FeatureRecord) -> dict[str, Any]:
    """The payload of the feature-ready message for a feature record, in the fields of ``feature_ready.avsc``."""
    return {
        "feature_id": record.feature_id,
        "run_id": record.run_id,
        "room": record.room,
        "occurrence_id": record.occurrence_id,
        "case_id": record.case_id,
        "feature_version": record.feature_version,
        "cutoff_case_time_us": record.cutoff_case_time_us,
        "cutoff_replay_time_us": record.cutoff_replay_time_us,
        "is_final": record.is_final,
    }
