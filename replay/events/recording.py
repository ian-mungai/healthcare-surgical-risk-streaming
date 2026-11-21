"""A decoded case recording as the event model sees it: plain data with no decoder in it.

A time is a whole number of microseconds from the start of the case, so a case time never passes through a float on its way into an event ID. A numeric
reading, a string event and a waveform record keep their order in the file: the position of a reading among the readings of its track is part of how a
discriminator is chosen. The decoder (``replay/service/decode.py``) makes these objects from a verified ``.vital`` file.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from replay.events.errors import EventModelError
from replay.events.identity import DELIMITER, RESERVED_PREFIX


class RecordingError(EventModelError):
    """A recording that the event model cannot replay."""


@dataclass(frozen=True, slots=True)
class CaseTimes:
    """The clinical times of a case, in microseconds from the case start: the start of the operation, its end and the end of the recording."""

    case_id: int
    opstart_us: int
    opend_us: int
    caseend_us: int

    def __post_init__(self) -> None:
        if self.case_id < 1:
            raise RecordingError("a case number is positive")
        if not 0 <= self.opstart_us <= self.opend_us <= self.caseend_us or self.caseend_us <= 0:
            raise RecordingError(f"case {self.case_id}: the times must satisfy 0 <= opstart <= opend <= caseend and 0 < caseend")


@dataclass(frozen=True, slots=True)
class NumericTrack:
    name: str
    unit: str
    readings: tuple[tuple[int, float], ...]


@dataclass(frozen=True, slots=True)
class StringTrack:
    name: str
    readings: tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True)
class WaveformRecord:
    """One record of a waveform track: the case time of its first sample and its raw samples."""

    start_us: int
    samples: Sequence[float]


@dataclass(frozen=True, slots=True)
class WaveformTrack:
    """A waveform track. A sample's physical value is ``raw * gain + offset``. Its time is its record's start plus its index over ``sample_rate``."""

    name: str
    unit: str
    sample_rate: float
    gain: float
    offset: float
    records: tuple[WaveformRecord, ...]


@dataclass(frozen=True, slots=True)
class CaseRecording:
    times: CaseTimes
    numeric: tuple[NumericTrack, ...] = field(default_factory=tuple)
    strings: tuple[StringTrack, ...] = field(default_factory=tuple)
    waveforms: tuple[WaveformTrack, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        names = [track.name for track in self.numeric] + [track.name for track in self.strings] + [track.name for track in self.waveforms]
        if len(names) != len(set(names)):
            raise RecordingError(f"case {self.times.case_id}: two tracks have one name")
        for name in names:
            if not name or name.startswith(RESERVED_PREFIX) or DELIMITER in name:
                raise RecordingError(f"case {self.times.case_id}: a track name is empty, starts with {RESERVED_PREFIX!r} or holds {DELIMITER!r}")


def physical_value(raw: float, gain: float, offset: float) -> float:
    """The physical value of a raw waveform sample: the one definition that the batch path and the stream path share."""
    return raw * gain + offset
