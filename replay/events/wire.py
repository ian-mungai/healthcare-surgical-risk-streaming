"""The wire format of one message: byte 0 is the magic byte 0, bytes 1 to 4 are the registry's content ID of the schema as a big-endian 32-bit integer, and
the rest is the Avro binary body of one record. The ID is never in a header. The replay writes it and the streaming job reads it with this definition.
"""

from __future__ import annotations

from replay.events.errors import EventModelError

MAGIC_BYTE = 0
PREFIX_LENGTH = 5
MAX_CONTENT_ID = 2**31 - 1


class WireError(EventModelError):
    """A message that does not follow the wire format."""


def frame(content_id: int, body: bytes) -> bytes:
    if not 0 < content_id <= MAX_CONTENT_ID:
        raise WireError("a content ID is a positive 32-bit integer")
    return bytes([MAGIC_BYTE]) + content_id.to_bytes(4, "big") + body


def unframe(message: bytes) -> tuple[int, bytes]:
    """The content ID and the Avro body of a message."""
    if len(message) < PREFIX_LENGTH or message[0] != MAGIC_BYTE:
        raise WireError("the message does not start with the magic byte and a content ID")
    return int.from_bytes(message[1:PREFIX_LENGTH], "big"), message[PREFIX_LENGTH:]
