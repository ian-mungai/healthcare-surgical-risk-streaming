"""The events of one case occurrence, in the order in which the replay sends them, and the payload of each.

Every numeric reading is one event, every waveform second is one event, every published string event is one event, and the occurrence ends with two generated
markers: the final-score marker, due at ``opend``, and the case-end marker, due at ``caseend``.

**Due instant.** A reading is due at its own case time. A waveform second is due at the end of the second, when all its samples exist. A marker is due at the
time its name says. The replay time of an event is the start of its occurrence plus its due offset. A due offset is held inside the case, between zero and the
``caseend``: the published recordings end within a second of the whole-second ``caseend``, so a reading or a waveform second can lie just outside it, and an
event that the replay sent outside its occurrence would break the order of replay time in its room. The event keeps its true case time.

**Order.** By due offset. At one due offset the source events come first, in the order of their track names, case times and discriminators, then the
final-score marker, then the case-end marker. So a marker comes after every reading that is due at its instant, and the order does not depend on the order
of the file.

**Discriminator.** The position of a reading among the readings of its track that share its case time, in the order of the file.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Literal

from replay.events.identity import CASE_END_TRACK, FINAL_SCORE_TRACK, marker_event_id, source_event_id
from replay.events.recording import CaseRecording, NumericTrack, StringTrack, WaveformTrack
from replay.events.schedule import ScheduledOccurrence
from replay.events.waveform import MICROSECONDS, SecondSpan, second_samples, second_spans

EventKind = Literal["numeric", "waveform", "string", "final_score", "case_end"]
MARKER_RANK: dict[str, int] = {"numeric": 0, "waveform": 0, "string": 0, "final_score": 1, "case_end": 2}


@dataclass(frozen=True, slots=True)
class CaseEvent:
    """One event of a case, before it belongs to an occurrence: its kind, track, times and discriminator, and where its data is."""

    kind: EventKind
    track: str
    case_time_us: int
    due_offset_us: int
    discriminator: int
    source_index: int = -1
    numeric: NumericTrack | None = None
    string: StringTrack | None = None
    waveform: WaveformTrack | None = None
    span: SecondSpan | None = None

    @property
    def order_key(self) -> tuple[int, int, str, int, int]:
        return (self.due_offset_us, MARKER_RANK[self.kind], self.track, self.case_time_us, self.discriminator)


@dataclass(frozen=True, slots=True)
class OutgoingEvent:
    """One message of the replay: its kind, room, occurrence and ID, the replay time at which it is due and its decoded payload."""

    kind: EventKind
    room: int
    occurrence_id: str
    event_id: str
    due_us: int
    payload: dict[str, Any]


def discriminators(times: list[int]) -> list[int]:
    """For each reading, its position among the readings that share its time, in the order given."""
    seen: defaultdict[int, int] = defaultdict(int)
    ranks = []
    for time in times:
        ranks.append(seen[time])
        seen[time] += 1
    return ranks


def held_in_case(due_offset_us: int, caseend_us: int) -> int:
    """A due offset moved to the nearest instant inside the case, ``0`` to ``caseend``."""
    return min(max(due_offset_us, 0), caseend_us)


def earliest_due_offset_us(recording: CaseRecording) -> int:
    """The due offset of the first event of a case: where its first due replay instant lies."""
    return occurrence_events(recording)[0].due_offset_us


def occurrence_events(recording: CaseRecording) -> list[CaseEvent]:
    """The events of the case in sending order."""
    caseend = recording.times.caseend_us
    events: list[CaseEvent] = []
    for numeric in recording.numeric:
        ranks = discriminators([time for time, _ in numeric.readings])
        events += [
            CaseEvent("numeric", numeric.name, time, held_in_case(time, caseend), ranks[index], index, numeric=numeric)
            for index, (time, _) in enumerate(numeric.readings)
        ]
    for strings in recording.strings:
        ranks = discriminators([time for time, _ in strings.readings])
        events += [
            CaseEvent("string", strings.name, time, held_in_case(time, caseend), ranks[index], index, string=strings)
            for index, (time, _) in enumerate(strings.readings)
        ]
    for waveform in recording.waveforms:
        events += [
            CaseEvent(
                "waveform", waveform.name, span.second * MICROSECONDS, held_in_case((span.second + 1) * MICROSECONDS, caseend), 0, waveform=waveform, span=span
            )
            for span in second_spans(waveform)
        ]
    times = recording.times
    events.append(CaseEvent("final_score", FINAL_SCORE_TRACK, times.opend_us, times.opend_us, 0))
    events.append(CaseEvent("case_end", CASE_END_TRACK, times.caseend_us, times.caseend_us, 0))
    return sorted(events, key=lambda event: event.order_key)


def outgoing(run_id: str, scheduled: ScheduledOccurrence, recording: CaseRecording, event: CaseEvent) -> OutgoingEvent:
    """The message for ``event`` in its occurrence: its ID, its due replay time and its payload."""
    occurrence = scheduled.occurrence
    due_us = scheduled.start_us + event.due_offset_us
    common = {"run_id": run_id, "room": occurrence.room, "occurrence_id": occurrence.id, "case_id": occurrence.case_id, "replay_time_us": due_us}
    if event.kind in ("final_score", "case_end"):
        identity = marker_event_id(run_id, occurrence.room, occurrence.id, event.kind, event.case_time_us)
        payload: dict[str, Any] = {"event_id": identity} | common | {"marker": event.kind.upper(), "case_time_us": event.case_time_us}
        return OutgoingEvent(event.kind, occurrence.room, occurrence.id, identity, due_us, payload)
    identity = source_event_id(run_id, occurrence.room, occurrence.id, event.track, event.case_time_us, event.discriminator)
    payload = {"event_id": identity} | common | {"track": event.track, "case_time_us": event.case_time_us, "discriminator": event.discriminator}
    if event.kind == "numeric" and event.numeric is not None:
        payload["value"] = float(event.numeric.readings[event.source_index][1])
    elif event.kind == "string" and event.string is not None:
        payload["text"] = event.string.readings[event.source_index][1]
        payload["source_id"] = f"{occurrence.case_id}/{event.track}/{event.source_index}"
    elif event.kind == "waveform" and event.waveform is not None and event.span is not None:
        waveform = event.waveform
        payload |= {
            "unit": waveform.unit,
            "sample_rate": float(waveform.sample_rate),
            "gain": float(waveform.gain),
            "offset": float(waveform.offset),
            "start_offset_us": event.span.start_offset_us,
            "samples": second_samples(waveform, event.span),
        }
    else:
        raise ValueError(f"the {event.kind} event of {event.track} has no data to send")
    return OutgoingEvent(event.kind, occurrence.room, occurrence.id, identity, due_us, payload)
