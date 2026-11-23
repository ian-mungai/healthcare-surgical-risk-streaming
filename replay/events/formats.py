"""The Avro formats of the stream: which file holds each schema, which topic carries it and under which subject the registry holds it.

The registry holds the schemas. A message carries the registry's content ID in its first bytes (``replay/events/wire.py``), so one topic carries several
formats. Every replayed input (numeric readings, waveform seconds, string events and the two markers) goes to one topic, ``replay_events``, with each room
in the partition of its own number. So the events of one room, markers included, arrive in the order in which they were sent. The feature-ready format has
its own topic. The subject of a format is its topic and its record name. This table is the one place that names them.

The registry has one global compatibility rule, ``BACKWARD``: a new version of a format can read the data that the version before it wrote. Adding a field
with a default is accepted. Adding a field with no default or changing a type is refused.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_FOLDER = Path(__file__).resolve().parent / "schemas"
COMPATIBILITY_RULE = "BACKWARD"
INPUT_TOPIC = "replay_events"
FEATURE_READY_TOPIC = "feature_ready"
REGISTRY_GROUP = "surgical_risk"


@dataclass(frozen=True, slots=True)
class Format:
    key: str
    topic: str
    schema_file: str
    record_name: str

    @property
    def subject(self) -> str:
        return f"{self.topic}-{self.record_name}"


FORMATS: dict[str, Format] = {
    "numeric": Format("numeric", INPUT_TOPIC, "numeric.avsc", "NumericReading"),
    "waveform": Format("waveform", INPUT_TOPIC, "waveform.avsc", "WaveformSecond"),
    "string_event": Format("string_event", INPUT_TOPIC, "string_event.avsc", "StringEvent"),
    "case_lifecycle": Format("case_lifecycle", INPUT_TOPIC, "case_lifecycle.avsc", "CaseLifecycle"),
    "feature_ready": Format("feature_ready", FEATURE_READY_TOPIC, "feature_ready.avsc", "FeatureReady"),
}

KIND_FORMAT: dict[str, str] = {
    "numeric": "numeric",
    "waveform": "waveform",
    "string": "string_event",
    "op_start": "case_lifecycle",
    "final_score": "case_lifecycle",
    "case_end": "case_lifecycle",
}


def schema_text(key: str) -> str:
    return (SCHEMA_FOLDER / FORMATS[key].schema_file).read_text()


def schema_of(key: str) -> dict[str, Any]:
    schema: dict[str, Any] = json.loads(schema_text(key))
    return schema
