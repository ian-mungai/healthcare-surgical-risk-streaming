"""The plain data of the streaming job: a decoded input event, the policy, and the four kinds of record the core produces.

A time is a whole number of microseconds. A replay time is since the one origin that every room shares. A case time is since the start of the case.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from features.model import ClinicalValue

SECOND = 1_000_000
MINUTE = 60 * SECOND

Kind = Literal["numeric", "waveform", "string", "final_score", "case_end"]

ON_TIME = "on_time"
LATE_ACCEPTED = "late_accepted"
TOO_LATE = "too_late"
DUPLICATE = "duplicate"
SOURCE_DUPLICATE = "source_duplicate"
AFTER_CASE_END = "after_case_end"
RECORDED_DECISIONS = (LATE_ACCEPTED, TOO_LATE, DUPLICATE, SOURCE_DUPLICATE, AFTER_CASE_END)


@dataclass(frozen=True, slots=True)
class Policy:
    """The late-event limit and the step of the cutoffs, in microseconds.

    An event is late when its replay time is behind the highest replay time of its case occurrence. It is applied while it is at most ``late_limit_us`` behind,
    and recorded and counted but never applied when it is further behind. A feature row is built every ``cutoff_step_us`` of case time, and at ``opend``.
    """

    late_limit_us: int = 30 * SECOND
    cutoff_step_us: int = MINUTE

    def __post_init__(self) -> None:
        if self.late_limit_us < 0 or self.cutoff_step_us <= 0:
            raise ValueError("the late limit is zero or more and the cutoff step is longer than zero")


ClinicalLookup = Callable[[int], Mapping[str, ClinicalValue]]


@dataclass(frozen=True, slots=True)
class Context:
    """What the core needs besides the events and the state: the policy, the version of the feature code and the clinical facts of a case."""

    policy: Policy
    version: str
    clinical: ClinicalLookup


@dataclass(frozen=True, slots=True)
class Event:
    """One decoded input event with the position of its Kafka message. ``physical_offset`` is the offset of a waveform's physical value, not a Kafka offset."""

    kind: Kind
    event_id: str
    run_id: str
    room: int
    occurrence_id: str
    case_id: int
    case_time_us: int
    replay_time_us: int
    track: str = ""
    discriminator: int = 0
    value: float = 0.0
    samples: tuple[float, ...] = ()
    sample_rate: float = 0.0
    gain: float = 1.0
    physical_offset: float = 0.0
    start_offset_us: int = 0
    kafka_partition: int = 0
    kafka_offset: int = 0

    @property
    def key(self) -> tuple[str, int, str]:
        """The key of the state: run, room and case occurrence."""
        return (self.run_id, self.room, self.occurrence_id)

    @property
    def due_offset_us(self) -> int:
        """The offset from the start of the occurrence at which the replay sends the event: a waveform second is due at its end."""
        return self.case_time_us + (SECOND if self.kind == "waveform" else 0)

    @property
    def suffix(self) -> str:
        """The part of the event ID that names the event inside its occurrence: the track, the case time and the discriminator."""
        return self.event_id.split("|", 3)[3]


@dataclass(frozen=True, slots=True)
class FeatureRecord:
    feature_id: str
    run_id: str
    room: int
    occurrence_id: str
    case_id: int
    feature_version: str
    cutoff_case_time_us: int
    cutoff_replay_time_us: int
    is_final: bool
    row_json: str
    warning_eligible: bool
    eligibility_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    decision_id: str
    event_id: str
    run_id: str
    room: int
    occurrence_id: str
    kind: str
    track: str
    case_time_us: int
    replay_time_us: int
    watermark_us: int
    lateness_us: int
    decision: str
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    ledger_id: str
    run_id: str
    room: int
    occurrence_id: str
    case_id: int
    counts_json: str


@dataclass(frozen=True, slots=True)
class ErrorRecord:
    error_id: str
    run_id: str
    room: int
    occurrence_id: str
    cutoff_case_time_us: int
    message: str


Record = FeatureRecord | DecisionRecord | LedgerRecord | ErrorRecord
