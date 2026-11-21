"""The identities of the stream: the run, the case occurrence, the room and the event.

One place defines them, as data and small typed functions with no Kafka and no Spark in them. The replay, the streaming job and the batch feature path
use the same definitions.

- **Run.** ``<name>_<8 hex>``, plus ``_2``, ``_3`` for a later attempt of the same run.
- **Case occurrence.** One play of one case in a run. The occurrences are numbered in the play order, from 0. The case of occurrence ``n`` is the ``n``-th case
  of the sample in turn, and its repeat is the number of times the sample was played through before it. Its ID is ``<case number>.<repeat>``.
- **Room.** Occurrence ``n`` is played in room ``n`` modulo the number of rooms.
- **Event.** ``<run>|<room>|<occurrence>|<track>|<case time in microseconds>|<discriminator>``. The discriminator is the position of a reading among the
  readings of its track that share its case time, in the order of the file, so two readings of one track and one time never share an ID. A generated marker has
  a reserved track that starts with ``!``, so a marker ID never equals a source event ID.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from replay.events.errors import EventModelError

Marker = Literal["final_score", "case_end"]

DELIMITER = "|"
RESERVED_PREFIX = "!"
FINAL_SCORE_TRACK = "!final_score"
CASE_END_TRACK = "!case_end"
MARKER_TRACKS: dict[Marker, str] = {"final_score": FINAL_SCORE_TRACK, "case_end": CASE_END_TRACK}

RUN_NAME = re.compile(r"[a-z][a-z0-9_]{0,31}")
RUN_ID = re.compile(r"[a-z][a-z0-9_]*_[0-9a-f]{8}(?:_[2-9]|_[1-9][0-9]+)?")
CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")


class IdentityError(EventModelError):
    """A run ID, an occurrence or an event ID component that the identities do not allow."""


def run_id(name: str, seed: str) -> str:
    """A run ID from its name and a seed text: the same seed gives the same run, as a scheduled run needs."""
    if not RUN_NAME.fullmatch(name):
        raise IdentityError("a run name is lower-case letters, digits and underscores, starts with a letter and has at most 32 characters")
    return f"{name}_{hashlib.sha256(seed.encode()).hexdigest()[:8]}"


def check_run_id(text: str) -> str:
    if not RUN_ID.fullmatch(text):
        raise IdentityError("a run ID is a name, an underscore and 8 hex digits, then optionally an underscore and an attempt number from 2")
    return text


@dataclass(frozen=True, slots=True)
class Occurrence:
    """One play of one case: its place in the play order, the case, how many times the case was played before it and its room."""

    sequence: int
    case_id: int
    repeat: int
    room: int

    @property
    def id(self) -> str:
        return f"{self.case_id}.{self.repeat}"


def room_of(sequence: int, rooms: int) -> int:
    """The room of the occurrence at ``sequence`` in the play order."""
    if rooms < 1 or sequence < 0:
        raise IdentityError("a room needs at least one room and a sequence from zero")
    return sequence % rooms


def occurrence_for(sequence: int, case_ids: Sequence[int], rooms: int) -> Occurrence:
    """The occurrence at ``sequence`` when the cases are played in turn, over and over, in the rooms in turn."""
    if not case_ids or len(set(case_ids)) != len(case_ids) or any(case < 1 for case in case_ids):
        raise IdentityError("the sample needs at least one case, each case once, each with a positive case number")
    return Occurrence(sequence, case_ids[sequence % len(case_ids)], sequence // len(case_ids), room_of(sequence, rooms))


def component(text: str, what: str) -> str:
    if not text or DELIMITER in text or CONTROL_CHARACTER.search(text):
        raise IdentityError(f"the {what} of an event ID must not be empty and must not hold the delimiter or a control character")
    return text


def event_id_text(run: str, room: int, occurrence_id: str, track: str, case_time_us: int, discriminator: int) -> str:
    if room < 0 or discriminator < 0:
        raise IdentityError("a room and a discriminator are zero or more")
    parts = (component(run, "run"), str(room), component(occurrence_id, "occurrence"), component(track, "track"), str(case_time_us), str(discriminator))
    return DELIMITER.join(parts)


def source_event_id(run: str, room: int, occurrence_id: str, track: str, case_time_us: int, discriminator: int) -> str:
    """The ID of one source reading or string event. A reserved track name is refused: it belongs to the markers."""
    if track.startswith(RESERVED_PREFIX):
        raise IdentityError(f"a source track must not start with {RESERVED_PREFIX!r}: that prefix belongs to the generated markers")
    return event_id_text(run, room, occurrence_id, track, case_time_us, discriminator)


def marker_event_id(run: str, room: int, occurrence_id: str, marker: Marker, case_time_us: int) -> str:
    """The ID of the generated final-score marker or case-end marker of an occurrence."""
    return event_id_text(run, room, occurrence_id, MARKER_TRACKS[marker], case_time_us, 0)
