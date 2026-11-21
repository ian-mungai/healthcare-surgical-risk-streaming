"""The Avro formats of the stream: which file holds each schema, which topic carries it and under which subject the registry holds it.

The registry holds the schemas. A message carries the registry's content ID in its first bytes (``replay/service/wire.py``), so one topic could carry several
formats. Each format has its own topic here: one room's events stay in order within a topic, and the topics have separate retention limits. This table is the
one place that names them.

The registry's compatibility rule is ``BACKWARD``: a new version of a format can read data that an older version wrote. Adding a field with a default is
accepted. Adding a field with no default, removing a field that has none or changing a type is refused.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_FOLDER = Path(__file__).resolve().parent / "schemas"
COMPATIBILITY_RULE = "BACKWARD"
REGISTRY_GROUP = "surgical_risk"


@dataclass(frozen=True, slots=True)
class Format:
    key: str
    topic: str
    schema_file: str

    @property
    def subject(self) -> str:
        return f"{self.topic}-value"


FORMATS: dict[str, Format] = {
    "numeric": Format("numeric", "events.numeric", "numeric.avsc"),
    "waveform": Format("waveform", "events.waveform", "waveform.avsc"),
    "string_event": Format("string_event", "events.string", "string_event.avsc"),
    "case_lifecycle": Format("case_lifecycle", "events.lifecycle", "case_lifecycle.avsc"),
    "feature_ready": Format("feature_ready", "features.ready", "feature_ready.avsc"),
}

KIND_FORMAT: dict[str, str] = {
    "numeric": "numeric",
    "waveform": "waveform",
    "string": "string_event",
    "final_score": "case_lifecycle",
    "case_end": "case_lifecycle",
}


def schema_text(key: str) -> str:
    return (SCHEMA_FOLDER / FORMATS[key].schema_file).read_text()


def schema_of(key: str) -> dict[str, Any]:
    schema: dict[str, Any] = json.loads(schema_text(key))
    return schema
