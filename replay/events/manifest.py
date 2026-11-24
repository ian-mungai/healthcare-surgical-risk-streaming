"""The manifest of expected events, and its comparison with what Kafka and the replay's admissions hold.

The manifest is derived from the decoded case files and the frozen schedule only. It never reads what the replay sent or admitted. It has its own code for the
waveform seconds (sample by sample, from the exact encoded values) and for the payloads, so a mistake in the replay's code is not a mistake in the manifest.
It lists every event of every admitted occurrence with its ID, its due replay time, whether it is due inside the half-open window and the digest of its
decoded payload. A string event and a marker keep their payload too, so their text and their kind can be read in a report.

An event is due inside its case, as ``replay/events/stream.py`` says. An admitted occurrence plays in full, also after the window ends (the drain). So the
manifest lists every event of every admitted occurrence, flags each by whether it is due inside the window, and ``compare`` can check all of them or only those
due inside the window.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from features.sampling import ANONYMISED_START
from replay.events.errors import EventModelError
from replay.events.identity import Marker, marker_event_id, source_event_id
from replay.events.payload import payload_digest
from replay.events.recording import CaseRecording, WaveformTrack
from replay.events.schedule import FrozenSchedule, ScheduledOccurrence, admitted_occurrences
from replay.events.waveform import MICROSECONDS, RATE_REFUSAL

MANIFEST_FORMAT = "replay_manifest_2"
REPORT_LINES_PER_KIND = 20


class ManifestError(EventModelError):
    """The manifest cannot be built or read."""


@dataclass(frozen=True, slots=True)
class ExpectedEvent:
    event_id: str
    kind: str
    room: int
    occurrence_id: str
    due_us: int
    in_window: bool
    payload_sha256: str
    payload: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class ExpectedOccurrence:
    room: int
    start_us: int
    sequence: int


@dataclass(frozen=True, slots=True)
class Manifest:
    run_id: str
    schedule_sha256: str
    first_due_us: int
    window_start_us: int
    window_end_us: int
    occurrences: dict[str, ExpectedOccurrence]
    events: dict[str, ExpectedEvent]

    @property
    def first_due_event_ids(self) -> frozenset[str]:
        return frozenset(event.event_id for event in self.events.values() if event.due_us == self.first_due_us)

    def to_json(self) -> str:
        document = {
            "format": MANIFEST_FORMAT,
            "run_id": self.run_id,
            "schedule_sha256": self.schedule_sha256,
            "first_due_us": self.first_due_us,
            "window_start_us": self.window_start_us,
            "window_end_us": self.window_end_us,
            "occurrences": {key: [value.room, value.start_us, value.sequence] for key, value in sorted(self.occurrences.items())},
            "events": [
                [e.event_id, e.kind, e.room, e.occurrence_id, e.due_us, e.in_window, e.payload_sha256, e.payload]
                for e in sorted(self.events.values(), key=lambda event: (event.due_us, event.event_id))
            ],
        }
        return json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"

    @classmethod
    def from_json(cls, text: str) -> Manifest:
        try:
            document = json.loads(text)
            if document["format"] != MANIFEST_FORMAT:
                raise ManifestError("the manifest is not of this format")
            return cls(
                document["run_id"],
                document["schedule_sha256"],
                document["first_due_us"],
                document["window_start_us"],
                document["window_end_us"],
                {key: ExpectedOccurrence(*value) for key, value in document["occurrences"].items()},
                {row[0]: ExpectedEvent(*row) for row in document["events"]},
            )
        except (ValueError, KeyError, TypeError) as error:
            if isinstance(error, ManifestError):
                raise
            raise ManifestError("the manifest file is not a manifest of this format") from None


def reference_seconds(track: WaveformTrack) -> dict[int, tuple[int, list[float]]]:
    """Each second of a waveform as ``(offset of its first sample, its raw samples)``, derived sample by sample from the exact encoded values.

    The time of a sample is the packet's encoded start, minus the anonymised start, plus its index over the rate, with every term an exact fraction and nothing
    rounded before the floor. It shares no step with ``features.sampling.packet_pieces``, which cuts whole packets.
    """
    if not math.isfinite(track.sample_rate) or track.sample_rate <= 0:
        raise ManifestError(RATE_REFUSAL)
    rate, origin = Fraction(track.sample_rate), Fraction(ANONYMISED_START)
    seconds: dict[int, tuple[int, list[float]]] = {}
    for record in track.records:
        if not math.isfinite(record.start_s):
            raise ManifestError("a waveform packet's start must be a finite number")
        for index, raw in enumerate(record.samples):
            instant = Fraction(record.start_s) - origin + Fraction(index) / rate
            second = math.floor(instant)
            if second not in seconds:
                seconds[second] = (math.floor((instant - second) * MICROSECONDS), [])
            seconds[second][1].append(float(raw))
    return seconds


def expected_events(frozen: FrozenSchedule, scheduled: ScheduledOccurrence, recording: CaseRecording) -> list[ExpectedEvent]:
    """The events one admitted occurrence must send, with the code of the manifest."""
    run, occurrence = frozen.plan.run_id, scheduled.occurrence
    found: list[ExpectedEvent] = []

    caseend = recording.times.caseend_us

    def add(kind: str, event_id: str, due_offset_us: int, payload: dict[str, Any], *, keep: bool) -> None:
        due = scheduled.start_us + (0 if due_offset_us < 0 else caseend if due_offset_us > caseend else due_offset_us)
        payload["event_id"] = event_id
        payload["replay_time_us"] = due
        in_window = frozen.window_start_us <= due < frozen.window_end_us
        found.append(ExpectedEvent(event_id, kind, occurrence.room, occurrence.id, due, in_window, payload_digest(payload), payload if keep else None))

    def base() -> dict[str, Any]:
        return {"run_id": run, "room": occurrence.room, "occurrence_id": occurrence.id, "case_id": occurrence.case_id}

    for numeric in recording.numeric:
        seen: dict[int, int] = {}
        for time, value in numeric.readings:
            rank = seen.get(time, 0)
            seen[time] = rank + 1
            identity = source_event_id(run, occurrence.room, occurrence.id, numeric.name, time, rank)
            add("numeric", identity, time, base() | {"track": numeric.name, "case_time_us": time, "discriminator": rank, "value": float(value)}, keep=False)
    for strings in recording.strings:
        seen = {}
        for position, (time, text) in enumerate(strings.readings):
            rank = seen.get(time, 0)
            seen[time] = rank + 1
            identity = source_event_id(run, occurrence.room, occurrence.id, strings.name, time, rank)
            fields = {
                "track": strings.name,
                "case_time_us": time,
                "discriminator": rank,
                "text": text,
                "source_id": f"{occurrence.case_id}/{strings.name}/{position}",
            }
            add("string", identity, time, base() | fields, keep=True)
    for waveform in recording.waveforms:
        for second, (offset, samples) in sorted(reference_seconds(waveform).items()):
            identity = source_event_id(run, occurrence.room, occurrence.id, waveform.name, second * MICROSECONDS, 0)
            fields = {
                "track": waveform.name,
                "case_time_us": second * MICROSECONDS,
                "discriminator": 0,
                "unit": waveform.unit,
                "sample_rate": float(waveform.sample_rate),
                "gain": float(waveform.gain),
                "offset": float(waveform.offset),
                "sample_format": int(waveform.sample_format),
                "start_offset_us": offset,
                "samples": samples,
            }
            add("waveform", identity, (second + 1) * MICROSECONDS, base() | fields, keep=False)
    markers: tuple[tuple[Marker, int], ...] = (
        ("op_start", recording.times.opstart_us),
        ("final_score", recording.times.opend_us),
        ("case_end", recording.times.caseend_us),
    )
    for marker, time in markers:
        identity = marker_event_id(run, occurrence.room, occurrence.id, marker, time)
        add(marker, identity, time, base() | {"marker": marker.upper(), "case_time_us": time}, keep=True)
    return found


def build_manifest(frozen: FrozenSchedule, recordings: Mapping[int, CaseRecording]) -> Manifest:
    """The manifest of a frozen schedule over the decoded recordings of its cases."""
    occurrences: dict[str, ExpectedOccurrence] = {}
    events: dict[str, ExpectedEvent] = {}
    for scheduled in admitted_occurrences(frozen):
        occurrence = scheduled.occurrence
        if occurrence.case_id not in recordings:
            raise ManifestError(f"case {occurrence.case_id} has no decoded recording")
        occurrences[occurrence.id] = ExpectedOccurrence(occurrence.room, scheduled.start_us, occurrence.sequence)
        for event in expected_events(frozen, scheduled, recordings[occurrence.case_id]):
            if event.event_id in events:
                raise ManifestError(f"two expected events have the ID {event.event_id}")
            events[event.event_id] = event
    manifest = Manifest(frozen.plan.run_id, frozen.sha256, frozen.first_due_us, frozen.window_start_us, frozen.window_end_us, occurrences, events)
    if not manifest.first_due_event_ids:
        raise ManifestError(f"no expected event is due at the frozen first due instant {frozen.first_due_us}")
    return manifest


@dataclass(frozen=True, slots=True)
class Observed:
    """What a run left behind: the digests of the payloads received under each event ID, the occurrences the replay admitted and its first send."""

    events: Mapping[str, Sequence[str]]
    admitted: frozenset[str]
    first_send_event_id: str | None


@dataclass(frozen=True, slots=True)
class Report:
    missing_ids: tuple[str, ...]
    unexpected_ids: tuple[str, ...]
    payload_mismatches: tuple[tuple[str, str, str], ...]
    conflicting_duplicates: tuple[str, ...]
    duplicate_count: int
    missing_occurrences: tuple[str, ...]
    partial_occurrences: tuple[str, ...]
    missing_admissions: tuple[str, ...]
    unexpected_admissions: tuple[str, ...]
    first_send_ok: bool
    first_send_detail: str

    @property
    def ok(self) -> bool:
        return (
            not (
                self.missing_ids
                or self.unexpected_ids
                or self.payload_mismatches
                or self.conflicting_duplicates
                or self.missing_admissions
                or self.unexpected_admissions
            )
            and self.first_send_ok
        )

    def failures(self) -> tuple[str, ...]:
        """One line for each failure, each naming IDs. A long list shows its first IDs and the count."""

        def listed(label: str, items: Sequence[str]) -> list[str]:
            shown = list(items[:REPORT_LINES_PER_KIND])
            lines = [f"{label}: {item}" for item in shown]
            return lines + ([f"{label}: and {len(items) - len(shown)} more"] if len(items) > len(shown) else [])

        lines = listed("missing occurrence", self.missing_occurrences)
        lines += listed("occurrence with missing events", self.partial_occurrences)
        lines += listed("missing event ID", self.missing_ids)
        lines += listed("unexpected event ID", self.unexpected_ids)
        lines += listed("payload differs", [f"{key} expected {expected[:12]} observed {seen[:12]}" for key, expected, seen in self.payload_mismatches])
        lines += listed("one ID with different payloads", self.conflicting_duplicates)
        lines += listed("occurrence not admitted", self.missing_admissions)
        lines += listed("occurrence admitted but not scheduled", self.unexpected_admissions)
        if not self.first_send_ok:
            lines.append(f"first send: {self.first_send_detail}")
        return tuple(lines)


def compare(manifest: Manifest, observed: Observed, *, window_only: bool = False) -> Report:
    """Compare what a run left behind with the manifest. With ``window_only`` the events due after the window (the drain) are not required."""
    required = {key: event for key, event in manifest.events.items() if event.in_window or not window_only}
    missing = sorted(key for key in required if key not in observed.events)
    unexpected = sorted(key for key in observed.events if key not in manifest.events)
    mismatches: list[tuple[str, str, str]] = []
    conflicts: list[str] = []
    duplicates = 0
    for key, digests in sorted(observed.events.items()):
        if key not in manifest.events:
            continue
        distinct = set(digests)
        if len(distinct) > 1:
            conflicts.append(key)
            continue
        if len(digests) > 1:
            duplicates += 1
        if digests and digests[0] != manifest.events[key].payload_sha256:
            mismatches.append((key, manifest.events[key].payload_sha256, digests[0]))
    absent = set(missing)
    gone: list[str] = []
    partial: list[str] = []
    for occurrence in sorted(manifest.occurrences):
        mine = [key for key, event in required.items() if event.occurrence_id == occurrence]
        lost = [key for key in mine if key in absent]
        if mine and len(lost) == len(mine):
            gone.append(occurrence)
        elif lost:
            partial.append(occurrence)
    first_ok = observed.first_send_event_id in manifest.first_due_event_ids if observed.first_send_event_id else False
    if observed.first_send_event_id is None:
        detail = "no first send was recorded"
    elif first_ok:
        detail = "the first send is an event due at the first due instant"
    elif observed.first_send_event_id in manifest.events:
        detail = f"{observed.first_send_event_id} is not due at the first due instant {manifest.first_due_us}"
    else:
        detail = f"{observed.first_send_event_id} is not an expected event"
    return Report(
        tuple(missing),
        tuple(unexpected),
        tuple(mismatches),
        tuple(conflicts),
        duplicates,
        tuple(gone),
        tuple(partial),
        tuple(sorted(set(manifest.occurrences) - observed.admitted)),
        tuple(sorted(observed.admitted - set(manifest.occurrences))),
        first_ok,
        detail,
    )
