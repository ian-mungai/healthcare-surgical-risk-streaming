"""Shared set-up for the fast tests of the replay's event model: the hand-built boundary cases, a plan over them and in-memory stand-ins for the loop's edges.

The stand-ins hold what a real sink, state file and clock would hold and nothing else: a sink that keeps every message it was given, a state store that
keeps the last saved state and a clock that moves only when the loop waits. They prove the loop's logic. The real broker, volume and clock are proved by
the acceptance test ``tests/e2e/stack_replay.py``.
"""

from __future__ import annotations

import json
from array import array
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from replay.events.recording import CaseRecording, CaseTimes, NumericTrack, StringTrack, WaveformRecord, WaveformTrack
from replay.events.run import RunState
from replay.events.schedule import FrozenSchedule, Plan, first_due_instant, freeze
from replay.events.stream import OutgoingEvent, earliest_due_offset_us

BOUNDARY_FILE = Path(__file__).resolve().parents[1] / "fixtures" / "boundary" / "replay_cases.json"
RUN_ID = "replay_0a1b2c3d"
SECOND = 1_000_000


def boundary_recordings() -> dict[int, CaseRecording]:
    """The cases of ``tests/fixtures/boundary/replay_cases.json`` as decoded recordings."""
    document = json.loads(BOUNDARY_FILE.read_text())
    recordings: dict[int, CaseRecording] = {}
    for name, case in document["cases"].items():
        recordings[int(name)] = CaseRecording(
            CaseTimes(int(name), case["opstart_us"], case["opend_us"], case["caseend_us"]),
            numeric=tuple(
                NumericTrack(track, body["unit"], tuple((time, value) for time, value in body["readings"])) for track, body in sorted(case["numeric"].items())
            ),
            strings=tuple(StringTrack(track, tuple((time, text) for time, text in readings)) for track, readings in sorted(case["string"].items())),
            waveforms=tuple(
                WaveformTrack(
                    track,
                    body["unit"],
                    body["sample_rate"],
                    body["gain"],
                    body["offset"],
                    body["sample_format"],
                    tuple(WaveformRecord(start, array("d", samples)) for start, samples in body["records"]),
                )
                for track, body in sorted(case["waveform"].items())
            ),
        )
    return recordings


def boundary_plan(*, rooms: int = 2, stagger_us: int = 0, turnover_us: int = 0, window_us: int = 120 * SECOND, cases: tuple[int, ...] = (9001, 9002)) -> Plan:
    recordings = boundary_recordings()
    return Plan(RUN_ID, rooms, tuple(recordings[case].times for case in cases), stagger_us, turnover_us, window_us)


def boundary_schedule(**options: Any) -> FrozenSchedule:
    plan = boundary_plan(**options)
    recordings = boundary_recordings()
    return freeze(plan, first_due_instant(plan, lambda case: earliest_due_offset_us(recordings[case])))


class MemorySink:
    """Keeps each message with the position at which the loop flushed it. A message counts as acknowledged only after a flush."""

    def __init__(self) -> None:
        self.sent: list[OutgoingEvent] = []
        self.acknowledged = 0
        self.on_send: Callable[[OutgoingEvent], None] | None = None

    def send(self, event: OutgoingEvent) -> None:
        if self.on_send is not None:
            self.on_send(event)
        self.sent.append(event)

    def flush(self) -> None:
        self.acknowledged = len(self.sent)


class StopHere(RuntimeError):
    """The process was stopped at a chosen moment: the planted stop of a restart case."""


class MemoryStore:
    """The state file as the loop sees it. ``stop_before_save`` stops the process at the nth save, before it writes: the Kafka acknowledgment came first."""

    def __init__(self) -> None:
        self.state: RunState | None = None
        self.saves = 0
        self.stop_before_save: int | None = None
        self.before_save: Callable[[RunState], None] | None = None

    def load(self) -> RunState | None:
        return self.state

    def save(self, state: RunState) -> None:
        self.saves += 1
        if self.before_save is not None:
            self.before_save(state)
        if self.stop_before_save is not None and self.saves == self.stop_before_save:
            raise StopHere(f"stopped before save {self.saves}")
        self.state = state


def saved_state(store: MemoryStore) -> RunState:
    """The state a store holds, which a finished or stopped run has saved at least once."""
    if store.state is None:
        raise AssertionError("the run saved no state")
    return store.state


class FakeClock:
    """Wall time in microseconds that moves only when the loop waits. ``waits`` records every wait."""

    def __init__(self, start_us: int = 1_800_000_000 * SECOND) -> None:
        self.now = start_us
        self.waits: list[int] = []

    def now_us(self) -> int:
        return self.now

    def wait_until(self, wall_us: int) -> None:
        if wall_us > self.now:
            self.waits.append(wall_us - self.now)
            self.now = wall_us


class RecordingSource:
    """The loop's reader of decoded cases. It records each read, so a test can see when a case was decoded."""

    def __init__(self, recordings: Mapping[int, CaseRecording]) -> None:
        self.recordings = recordings
        self.reads: list[int] = []

    def __call__(self, case_id: int) -> CaseRecording:
        self.reads.append(case_id)
        return self.recordings[case_id]
