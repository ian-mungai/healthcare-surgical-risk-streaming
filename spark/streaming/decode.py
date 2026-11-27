"""The decode step: one Kafka message to one flat input row, with the replay's own codec.

``decode_row`` takes any reader that decodes framed messages, so it is proved without Avro. The reader of a job is the replay's ``Reader``
(``replay/service/wire_codec.py``): it reads the registry content ID from the first bytes of the message and fetches that schema from the
registry once, so the streaming job and the replay use one definition of the wire bytes and of the format IDs. A message that is not an input
event raises, and the query fails visibly: a poison message is a fault to see, not to skip.
"""

from __future__ import annotations

from functools import cache
from typing import Any

from spark.streaming.rows import INPUT_COLUMNS, event_row
from spark.streaming.wire import MessageReader, decode_message


def decode_row(reader: MessageReader, message: bytes, kafka_partition: int, kafka_offset: int) -> tuple[Any, ...]:
    row = event_row(decode_message(reader, bytes(message), kafka_partition, kafka_offset))
    return tuple(row[name] for name, _ in INPUT_COLUMNS)


@cache
def registry_reader(registry_url: str) -> MessageReader:
    """The reader of this Python process. Avro and the registry client are imported here, so only a worker that decodes needs them."""
    from replay.service.registry import RegistryClient
    from replay.service.wire_codec import Reader

    return Reader(RegistryClient(registry_url))
