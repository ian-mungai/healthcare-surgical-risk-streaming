"""Encode and decode the Avro body of a message with fastavro, and frame it with a content ID from the registry.

The writer holds one parsed schema for each format, with the content ID that the registry gave it. The reader looks a schema up by the content ID in the
message, asks the registry for it once and keeps it.
"""

from __future__ import annotations

import io
import json
from typing import Any

import fastavro

from replay.events.formats import FORMATS, REGISTRY_GROUP, schema_of, schema_text
from replay.events.wire import frame, unframe
from replay.service.registry import RegistryClient, RegistryError


class FormatMismatch(RuntimeError):
    """The registry's latest version of a format is not the schema that this image holds."""


def canonical(text: str) -> str:
    return json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))


def encode_body(parsed: Any, payload: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed, payload)
    return buffer.getvalue()


class Writer:
    """The formats of this image, each with its registry content ID. It refuses to start when the registry's latest version of a format differs."""

    def __init__(self, registry: RegistryClient) -> None:
        self.parsed: dict[str, Any] = {}
        self.content_ids: dict[str, int] = {}
        for key, format_ in FORMATS.items():
            latest = registry.latest(REGISTRY_GROUP, format_.subject)
            if latest is None:
                raise FormatMismatch(f"the registry holds no format {format_.subject}; register the formats first")
            held = registry.schema_by_content_id(latest.content_id)
            if canonical(held) != canonical(schema_text(key)):
                raise FormatMismatch(f"the registry's latest version of {format_.subject} is not the schema of this image")
            self.parsed[key] = fastavro.parse_schema(schema_of(key))
            self.content_ids[key] = latest.content_id

    def message(self, format_key: str, payload: dict[str, Any]) -> bytes:
        return frame(self.content_ids[format_key], encode_body(self.parsed[format_key], payload))


class Reader:
    """Decodes framed messages. A content ID is looked up in the registry once."""

    def __init__(self, registry: RegistryClient) -> None:
        self.registry = registry
        self.parsed: dict[int, Any] = {}

    def record_name(self, content_id: int) -> str:
        """The record name of a schema that ``decode`` has read."""
        return str(self.parsed[content_id]["name"]).rsplit(".", 1)[-1]

    def decode(self, message: bytes) -> tuple[int, dict[str, Any]]:
        content_id, body = unframe(message)
        if content_id not in self.parsed:
            try:
                self.parsed[content_id] = fastavro.parse_schema(json.loads(self.registry.schema_by_content_id(content_id)))
            except RegistryError, ValueError:
                raise RegistryError(f"the schema of content ID {content_id} cannot be read from the registry") from None
        record = fastavro.schemaless_reader(io.BytesIO(body), self.parsed[content_id])
        if not isinstance(record, dict):
            raise RegistryError(f"a message of content ID {content_id} is not a record")
        return content_id, record
