"""The replay schedule: when each case occurrence starts in each room, the first due replay instant and the half-open window of a run.

All times here are replay time: whole microseconds since one origin that every room shares. Replay time only moves forward, and it does not depend on the
replay speed, which only paces the wall clock. The plan names the cases in play order, the number of rooms, the stagger between the starts of the rooms, the
turnover between two cases of a room and the length of the window. Room ``r`` starts at ``r`` times the stagger and plays its occurrences back to back, each
``caseend`` long, with the turnover between them.

The schedule is frozen before any admission: the plan and the first due instant, with a digest. The first due instant is the earliest instant at which any
event of the first occurrence of any room is due. The window is ``[first due, first due + window length)``. An occurrence is admitted when its mapped
``opstart``, which is its start plus the case's ``opstart``, falls inside the window. An admitted occurrence plays in full, also past the end of the window.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from replay.events.errors import EventModelError
from replay.events.identity import Occurrence, check_run_id, occurrence_for
from replay.events.recording import CaseTimes

SCHEDULE_FORMAT = "replay_schedule_1"


class ScheduleError(EventModelError):
    """A plan or a frozen schedule that cannot be used, or a saved schedule that differs from the one asked for."""


@dataclass(frozen=True, slots=True)
class Plan:
    run_id: str
    rooms: int
    cases: tuple[CaseTimes, ...]
    stagger_us: int
    turnover_us: int
    window_us: int

    def __post_init__(self) -> None:
        check_run_id(self.run_id)
        if self.rooms < 1:
            raise ScheduleError("a plan needs at least one room")
        if not self.cases or len({case.case_id for case in self.cases}) != len(self.cases):
            raise ScheduleError("a plan needs at least one case, each case once")
        if self.stagger_us < 0 or self.turnover_us < 0:
            raise ScheduleError("the stagger and the turnover are zero or more")
        if self.window_us <= 0:
            raise ScheduleError("the window is longer than zero")


@dataclass(frozen=True, slots=True)
class ScheduledOccurrence:
    occurrence: Occurrence
    times: CaseTimes
    start_us: int

    @property
    def mapped_opstart_us(self) -> int:
        return self.start_us + self.times.opstart_us

    @property
    def mapped_opend_us(self) -> int:
        return self.start_us + self.times.opend_us

    @property
    def end_us(self) -> int:
        return self.start_us + self.times.caseend_us


@dataclass(frozen=True, slots=True)
class FrozenSchedule:
    plan: Plan
    first_due_us: int

    @property
    def window_start_us(self) -> int:
        return self.first_due_us

    @property
    def window_end_us(self) -> int:
        return self.first_due_us + self.plan.window_us

    def document(self) -> dict[str, Any]:
        plan = self.plan
        return {
            "format": SCHEDULE_FORMAT,
            "run_id": plan.run_id,
            "rooms": plan.rooms,
            "stagger_us": plan.stagger_us,
            "turnover_us": plan.turnover_us,
            "window_us": plan.window_us,
            "cases": [
                {"case_id": case.case_id, "opstart_us": case.opstart_us, "opend_us": case.opend_us, "caseend_us": case.caseend_us} for case in plan.cases
            ],
            "first_due_us": self.first_due_us,
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(json.dumps(self.document(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def to_json(self) -> str:
        return json.dumps({**self.document(), "sha256": self.sha256}, indent=1, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> FrozenSchedule:
        try:
            document = json.loads(text)
            if document.pop("format") != SCHEDULE_FORMAT:
                raise ScheduleError("the saved schedule is not of this format")
            claimed = document.pop("sha256")
            plan = Plan(
                document["run_id"],
                document["rooms"],
                tuple(CaseTimes(case["case_id"], case["opstart_us"], case["opend_us"], case["caseend_us"]) for case in document["cases"]),
                document["stagger_us"],
                document["turnover_us"],
                document["window_us"],
            )
            frozen = cls(plan, document["first_due_us"])
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            if isinstance(error, ScheduleError):
                raise
            raise ScheduleError("the saved schedule is not a schedule of this format") from None
        if claimed != frozen.sha256:
            raise ScheduleError("the saved schedule does not match its digest: it was edited")
        return frozen


def freeze(plan: Plan, first_due_us: int) -> FrozenSchedule:
    return FrozenSchedule(plan, first_due_us)


def room_start_us(plan: Plan, room: int) -> int:
    return room * plan.stagger_us


def first_due_instant(plan: Plan, earliest_due_offset_us: Callable[[int], int]) -> int:
    """The earliest instant at which an event is due: over the first occurrence of each room, its start plus the case time of its first event."""
    return min(
        room_start_us(plan, room) + earliest_due_offset_us(occurrence_for(room, [case.case_id for case in plan.cases], plan.rooms).case_id)
        for room in range(plan.rooms)
    )


def room_chain(plan: Plan, room: int, limit_us: int) -> list[ScheduledOccurrence]:
    """The occurrences of a room that start before ``limit_us``, back to back from the start of the room."""
    case_ids = [case.case_id for case in plan.cases]
    times: Mapping[int, CaseTimes] = {case.case_id: case for case in plan.cases}
    chain: list[ScheduledOccurrence] = []
    start = room_start_us(plan, room)
    position = 0
    while start < limit_us:
        occurrence = occurrence_for(room + position * plan.rooms, case_ids, plan.rooms)
        scheduled = ScheduledOccurrence(occurrence, times[occurrence.case_id], start)
        chain.append(scheduled)
        start = scheduled.end_us + plan.turnover_us
        position += 1
    return chain


def admitted_occurrences(frozen: FrozenSchedule) -> list[ScheduledOccurrence]:
    """The occurrences whose mapped ``opstart`` falls in the half-open window, in the order of their start, then of their place in the play order."""
    chain = [
        scheduled
        for room in range(frozen.plan.rooms)
        for scheduled in room_chain(frozen.plan, room, frozen.window_end_us)
        if frozen.window_start_us <= scheduled.mapped_opstart_us < frozen.window_end_us
    ]
    return sorted(chain, key=lambda scheduled: (scheduled.start_us, scheduled.occurrence.sequence))


def ensure_same_schedule(saved: FrozenSchedule, wanted: FrozenSchedule) -> None:
    """Refuse a schedule that differs from the saved one, and name each part that differs."""
    pairs = (
        ("run ID", saved.plan.run_id, wanted.plan.run_id),
        ("room count", saved.plan.rooms, wanted.plan.rooms),
        ("cases and their times", saved.plan.cases, wanted.plan.cases),
        ("stagger", saved.plan.stagger_us, wanted.plan.stagger_us),
        ("turnover", saved.plan.turnover_us, wanted.plan.turnover_us),
        ("window length", saved.plan.window_us, wanted.plan.window_us),
        ("first due instant", saved.first_due_us, wanted.first_due_us),
    )
    differing = [label for label, was, now in pairs if was != now]
    if differing:
        raise ScheduleError("the schedule differs from the frozen one in: " + ", ".join(differing))
