"""The replay loop: sends the events of the admitted occurrences in replay-time order, paced against the wall clock, and keeps its position.

The loop knows no Kafka and no file. It talks to four small edges: a ``Sink`` that takes a message and later flushes (the flush returns once Kafka has
acknowledged every message), a ``StateStore`` that keeps the saved state, a ``Clock`` and a ``RecordingSource`` that decodes a case on demand.

**Order.** The next event to send is the one with the smallest ``(due replay time, place of its occurrence in the play order, ordinal in the occurrence)`` over
all rooms. The replay time of a send therefore never goes backwards, and the events of one room stay in their order.

**Pacing.** The wall time at which an event is sent is the wall time of the first send plus its replay time since the first due instant, over the speed. The
speed only moves the wall clock. A restart keeps the wall time of the first send, so a late event is sent at once and the pacing then continues.

**Position.** For each room, the number of its admitted occurrences that are complete and the ordinal of the next event of the current one. It is written only
after a flush, so it never covers a message that Kafka did not acknowledge. A stop between the acknowledgment and the write leaves the position behind: the
restart sends those events again, with the same IDs and the same payloads, and consumers drop them. The position is written every ``checkpoint_every``
events, before a long wait and at the end.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from replay.events.recording import CaseRecording
from replay.events.schedule import FrozenSchedule, ScheduledOccurrence, admitted_occurrences
from replay.events.stream import CaseEvent, OutgoingEvent, occurrence_events, outgoing

STATE_FORMAT = "replay_state_1"
DEFAULT_CHECKPOINT_EVENTS = 100
DEFAULT_IDLE_CHECKPOINT_US = 5_000_000


class ReplayError(RuntimeError):
    """The loop cannot run: a setting that cannot hold, or a saved state that belongs to another schedule."""


@dataclass(frozen=True, slots=True)
class RunSettings:
    """What the run record holds besides the schedule: the speed, how often the position is written and how long a wait must be to write it first."""

    speed: float
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVENTS
    idle_checkpoint_us: int = DEFAULT_IDLE_CHECKPOINT_US

    def __post_init__(self) -> None:
        if not math.isfinite(self.speed) or self.speed <= 0:
            raise ReplayError("the speed must be a positive finite number")
        if self.checkpoint_every < 1 or self.idle_checkpoint_us < 0:
            raise ReplayError("the checkpoint interval is at least one event and the idle checkpoint is not negative")


@dataclass(frozen=True, slots=True)
class RoomPosition:
    completed: int
    ordinal: int


@dataclass(frozen=True, slots=True)
class FirstSend:
    event_id: str
    wall_us: int


@dataclass(frozen=True, slots=True)
class RunState:
    schedule_sha256: str
    positions: dict[int, RoomPosition]
    anchor_wall_us: int | None = None
    first_send: FirstSend | None = None
    admissions: dict[str, int] = field(default_factory=dict)
    finished: bool = False

    def to_json(self) -> str:
        document = {
            "format": STATE_FORMAT,
            "schedule_sha256": self.schedule_sha256,
            "positions": {str(room): [position.completed, position.ordinal] for room, position in sorted(self.positions.items())},
            "anchor_wall_us": self.anchor_wall_us,
            "first_send": None if self.first_send is None else [self.first_send.event_id, self.first_send.wall_us],
            "admissions": dict(sorted(self.admissions.items())),
            "finished": self.finished,
        }
        return json.dumps(document, indent=1, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> RunState:
        try:
            document = json.loads(text)
            if document["format"] != STATE_FORMAT:
                raise ReplayError("the saved state is not of this format")
            first = document["first_send"]
            return cls(
                document["schedule_sha256"],
                {int(room): RoomPosition(*value) for room, value in document["positions"].items()},
                document["anchor_wall_us"],
                None if first is None else FirstSend(*first),
                {key: int(value) for key, value in document["admissions"].items()},
                bool(document["finished"]),
            )
        except (ValueError, KeyError, TypeError) as error:
            if isinstance(error, ReplayError):
                raise
            raise ReplayError("the saved state file is not a state of this format") from None


class Sink(Protocol):
    def send(self, event: OutgoingEvent) -> None: ...

    def flush(self) -> None:
        """Return once every message given to ``send`` is acknowledged, or raise."""
        ...


class StateStore(Protocol):
    def load(self) -> RunState | None: ...

    def save(self, state: RunState) -> None: ...


class Clock(Protocol):
    def now_us(self) -> int: ...

    def wait_until(self, wall_us: int) -> None: ...


RecordingSource = Callable[[int], CaseRecording]


@dataclass(frozen=True, slots=True)
class RunResult:
    sent: int
    finished: bool


@dataclass(slots=True)
class Cursor:
    """The occurrence a room is playing: its schedule, its decoded case, its events and the ordinal of the next one."""

    scheduled: ScheduledOccurrence
    recording: CaseRecording
    events: list[CaseEvent]
    ordinal: int

    @property
    def next_key(self) -> tuple[int, int, int]:
        return (self.scheduled.start_us + self.events[self.ordinal].due_offset_us, self.scheduled.occurrence.sequence, self.ordinal)


class Replayer:
    def __init__(self, frozen: FrozenSchedule, settings: RunSettings, source: RecordingSource, sink: Sink, store: StateStore, clock: Clock) -> None:
        self.frozen = frozen
        self.settings = settings
        self.source = source
        self.sink = sink
        self.store = store
        self.clock = clock
        self.rooms: dict[int, list[ScheduledOccurrence]] = {room: [] for room in range(frozen.plan.rooms)}
        for scheduled in admitted_occurrences(frozen):
            self.rooms[scheduled.occurrence.room].append(scheduled)

    def cursor_at(self, room: int, completed: int, ordinal: int) -> Cursor | None:
        if completed >= len(self.rooms[room]):
            return None
        scheduled = self.rooms[room][completed]
        recording = self.source(scheduled.occurrence.case_id)
        return Cursor(scheduled, recording, occurrence_events(recording), ordinal)

    def run(self) -> RunResult:
        """Play every admitted occurrence from the saved position to the end, and return what was sent in this call."""
        saved = self.store.load()
        if saved is not None and saved.schedule_sha256 != self.frozen.sha256:
            raise ReplayError("the saved state belongs to another schedule than the frozen one; it is not resumed")
        if saved is not None and saved.finished:
            return RunResult(0, True)
        state = saved or RunState(self.frozen.sha256, {room: RoomPosition(0, 0) for room in self.rooms})
        positions = dict(state.positions)
        admissions = dict(state.admissions)
        anchor, first_send = state.anchor_wall_us, state.first_send
        cursors = {room: self.cursor_at(room, positions[room].completed, positions[room].ordinal) for room in self.rooms}
        sent = unsaved = 0

        def checkpoint(*, finished: bool) -> None:
            nonlocal unsaved
            self.sink.flush()
            self.store.save(RunState(self.frozen.sha256, dict(positions), anchor, first_send, dict(admissions), finished))
            unsaved = 0

        while True:
            live = {room: cursor for room, cursor in cursors.items() if cursor is not None}
            if not live:
                break
            room = min(live, key=lambda candidate: live[candidate].next_key)
            cursor = live[room]
            due_us = cursor.next_key[0]
            if anchor is None:
                anchor = self.clock.now_us()
            target = anchor + int((due_us - self.frozen.window_start_us) / self.settings.speed)
            wait = target - self.clock.now_us()
            if wait > 0:
                if unsaved and wait >= self.settings.idle_checkpoint_us:
                    checkpoint(finished=False)
                self.clock.wait_until(target)
            message = outgoing(self.frozen.plan.run_id, cursor.scheduled, cursor.recording, cursor.events[cursor.ordinal])
            self.sink.send(message)
            sent += 1
            unsaved += 1
            now = self.clock.now_us()
            admissions.setdefault(cursor.scheduled.occurrence.id, now)
            if first_send is None:
                first_send = FirstSend(message.event_id, now)
            cursor.ordinal += 1
            completed = positions[room].completed
            if cursor.ordinal >= len(cursor.events):
                completed += 1
                cursors[room] = self.cursor_at(room, completed, 0)
                positions[room] = RoomPosition(completed, 0)
            else:
                positions[room] = RoomPosition(completed, cursor.ordinal)
            if unsaved >= self.settings.checkpoint_every:
                checkpoint(finished=False)
        checkpoint(finished=True)
        return RunResult(sent, True)
