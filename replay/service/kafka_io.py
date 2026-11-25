"""The Kafka edges of the replay: the sink that writes the messages and the reader that fetches them back to check them.

**Sink.** One message for each event, keyed by room, so one room's events stay in order in one partition. The producer is idempotent and waits for all in-sync
replicas. ``flush`` returns only when every message given to ``send`` is acknowledged, within a deadline, and raises when one is not or when Kafka refused one.
A message is acknowledged before its position is written, so a stop between the two sends the same IDs again.

**Reader.** Every message of the topics from the start to the end offsets seen when it began, within a deadline, decoded through the registry.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from replay.events.formats import FORMATS, INPUT_TOPIC, KIND_FORMAT
from replay.events.identity import partition_of
from replay.events.stream import OutgoingEvent
from replay.service.check import Received
from replay.service.wire_codec import Reader, Writer

SEND_RETRY_SECONDS = 30.0
METADATA_TIMEOUT_SECONDS = 30.0


class KafkaStartError(RuntimeError):
    """Kafka cannot take the replay's messages: a topic is missing or the broker cannot be reached."""


class DeliveryError(RuntimeError):
    """A message was not acknowledged within the deadline, or Kafka refused it."""


def producer_settings(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "replay",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
        "allow.auto.create.topics": False,
        "message.timeout.ms": 60_000,
    }


def partition_count(bootstrap_servers: str, topic: str) -> int:
    """The number of partitions of a topic. Refuse to start when the topic does not exist."""
    try:
        listed = AdminClient({"bootstrap.servers": bootstrap_servers}).list_topics(timeout=METADATA_TIMEOUT_SECONDS).topics
    except KafkaException as error:
        raise KafkaStartError(f"the broker cannot be reached: {error.args[0].name()}") from None
    if topic not in listed or listed[topic].error is not None:
        raise KafkaStartError(f"the topic does not exist: {topic}")
    return len(listed[topic].partitions)


class KafkaSink:
    def __init__(self, bootstrap_servers: str, writer: Writer, partitions: int, flush_timeout_seconds: float = 60.0, *, input_topic: str = INPUT_TOPIC) -> None:
        self.input_topic = input_topic
        self.producer = Producer(producer_settings(bootstrap_servers))
        self.partitions = partitions
        self.writer = writer
        self.flush_timeout_seconds = flush_timeout_seconds
        self.failures: list[str] = []

    def delivered(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.failures.append(f"{message.topic()}: {error.name()}")

    def topic_of(self, format_key: str) -> str:
        """The topic of a format: the run's input topic for a format that the deployed list puts on ``replay_events``, else the format's own."""
        topic = FORMATS[format_key].topic
        return self.input_topic if topic == INPUT_TOPIC else topic

    def send(self, event: OutgoingEvent) -> None:
        format_key = KIND_FORMAT[event.kind]
        value = self.writer.message(format_key, event.payload)
        deadline = time.monotonic() + SEND_RETRY_SECONDS
        while True:
            try:
                self.producer.produce(
                    self.topic_of(format_key),
                    key=str(event.room).encode(),
                    value=value,
                    partition=partition_of(event.room, self.partitions),
                    on_delivery=self.delivered,
                )
                break
            except BufferError:
                if time.monotonic() > deadline:
                    raise DeliveryError(f"the producer's queue stayed full for {SEND_RETRY_SECONDS:g} seconds") from None
                self.producer.poll(0.1)
        self.producer.poll(0)

    def flush(self) -> None:
        remaining = self.producer.flush(self.flush_timeout_seconds)
        if remaining:
            raise DeliveryError(f"{remaining} messages were not acknowledged within {self.flush_timeout_seconds:g} seconds")
        if self.failures:
            raise DeliveryError(f"Kafka refused {len(self.failures)} messages, for example {self.failures[0]}")


def read_all(bootstrap_servers: str, reader: Reader, topics: Iterable[str], deadline_seconds: float) -> list[Received]:
    """Every message of the topics up to the end offsets at the start of the read. A deadline that passes before the end is an error."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "replay_check_unused",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "auto.offset.reset": "earliest",
        }
    )
    received: list[Received] = []
    try:
        ends: dict[tuple[str, int], int] = {}
        for topic in sorted(set(topics)):
            partitions = consumer.list_topics(topic, timeout=METADATA_TIMEOUT_SECONDS).topics[topic].partitions
            for partition in sorted(partitions):
                _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=METADATA_TIMEOUT_SECONDS)
                ends[(topic, partition)] = high
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING) for (topic, partition), high in ends.items() if high > 0])
        pending = {key for key, high in ends.items() if high > 0}
        deadline = time.monotonic() + deadline_seconds
        while pending:
            if time.monotonic() > deadline:
                raise DeliveryError(f"the read of the topics did not reach their end offsets within {deadline_seconds:g} seconds")
            message = consumer.poll(1.0)
            if message is None:
                continue
            failure = message.error()
            if failure is not None:
                raise DeliveryError(f"the read failed: {failure.name()}")
            where, number, offset, value = message.topic(), message.partition(), message.offset(), message.value()
            if where is None or number is None or offset is None or value is None:
                raise DeliveryError("a message has no topic, partition, offset or value")
            if offset < ends[(where, number)]:
                content_id, payload = reader.decode(value)
                received.append(Received(where, number, offset, content_id, reader.record_name(content_id), payload))
            if offset >= ends[(where, number)] - 1:
                pending.discard((where, number))
    finally:
        consumer.close()
    return received
