"""The files of the check of what Kafka holds: the observed messages and their comparison with the manifest.

``observed_document`` is the digest of every payload read from the topics under its event ID. ``observed_from`` joins it with the saved state of the
replay, which holds the occurrences it admitted and its first send, into the ``Observed`` that ``compare`` takes. This module imports no Kafka library, so
the acceptance test can plant a fault in the observed files on the computer that runs it and compare again.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from replay.events.manifest import Manifest, Observed, Report
from replay.events.payload import payload_digest
from replay.events.run import RunState

OBSERVED_FORMAT = "replay_observed_1"


@dataclass(frozen=True, slots=True)
class Received:
    """One message read back from a topic: where it was, the content ID in its first bytes and its decoded payload.

    ``appended_ms`` is the broker's own append time in milliseconds since the epoch, for a message of a topic that stamps it (``LogAppendTime``), and None for a
    message that carries its producer's time or none."""

    topic: str
    partition: int
    offset: int
    content_id: int
    record_name: str
    payload: dict[str, Any]
    appended_ms: int | None = None


def observed_document(received: Iterable[Received], run_id: str) -> str:
    """The observed file for one run: for each event ID, the digests of the payloads received under it, in the order received.

    It also holds the count of messages of each topic, the content IDs found in the first bytes of the messages of each record name, the partitions that each
    room's messages used and the number of times that the replay time went backwards between two messages of one partition in the order of their offsets.
    """
    events: dict[str, list[str]] = {}
    topics: dict[str, int] = {}
    content_ids: dict[str, set[int]] = {}
    room_partitions: dict[str, set[int]] = {}
    last: dict[tuple[str, int], int] = {}
    regressions = 0
    for message in sorted(received, key=lambda item: (item.topic, item.partition, item.offset)):
        payload = message.payload
        if payload.get("run_id") != run_id:
            continue
        events.setdefault(str(payload["event_id"]), []).append(payload_digest(payload))
        topics[message.topic] = topics.get(message.topic, 0) + 1
        content_ids.setdefault(message.record_name, set()).add(message.content_id)
        room_partitions.setdefault(str(payload["room"]), set()).add(message.partition)
        key = (message.topic, message.partition)
        if payload["replay_time_us"] < last.get(key, payload["replay_time_us"]):
            regressions += 1
        last[key] = payload["replay_time_us"]
    document = {
        "format": OBSERVED_FORMAT,
        "run_id": run_id,
        "messages": sum(topics.values()),
        "topics": topics,
        "content_ids": {topic: sorted(ids) for topic, ids in content_ids.items()},
        "room_partitions": {room: sorted(partitions) for room, partitions in room_partitions.items()},
        "replay_time_regressions": regressions,
        "events": events,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"


def observed_from(observed_text: str, state: RunState | None) -> Observed:
    document = json.loads(observed_text)
    if document.get("format") != OBSERVED_FORMAT:
        raise ValueError("the observed file is not of this format")
    admitted = frozenset(state.admissions) if state is not None else frozenset()
    first = state.first_send.event_id if state is not None and state.first_send is not None else None
    return Observed({key: list(value) for key, value in document["events"].items()}, admitted, first)


def report_document(manifest: Manifest, report: Report, *, window_only: bool) -> str:
    return (
        json.dumps(
            {
                "ok": report.ok,
                "window_only": window_only,
                "expected_events": len(manifest.events),
                "expected_occurrences": len(manifest.occurrences),
                "missing_ids": len(report.missing_ids),
                "unexpected_ids": len(report.unexpected_ids),
                "payload_mismatches": len(report.payload_mismatches),
                "conflicting_duplicates": len(report.conflicting_duplicates),
                "duplicate_ids": report.duplicate_count,
                "missing_occurrences": list(report.missing_occurrences),
                "partial_occurrences": list(report.partial_occurrences),
                "missing_admissions": list(report.missing_admissions),
                "unexpected_admissions": list(report.unexpected_admissions),
                "first_send_ok": report.first_send_ok,
                "first_send": report.first_send_detail,
                "failures": list(report.failures()),
            },
            indent=1,
            sort_keys=True,
        )
        + "\n"
    )
