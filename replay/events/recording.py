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

WAVEFORM_FORMATS = frozenset(range(1, 9))


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
    """A waveform track. Its time is its record's start plus its index over ``sample_rate``.

    ``sample_format`` is the track's format code in the file, the one that Bronze keeps as ``sample_format``. It decides what a sample is: a sample of an
    integer format (3 to 8) is a raw count with a physical value of ``raw * gain + offset`` and a gap marker of the format, and a sample of a float format
    (1 and 2) is already physical, so the gain and the offset do not apply. The replay sends the code, and the stream and Silver read a sample by the same
    function.
    """

    name: str
    unit: str
    sample_rate: float
    gain: float
    offset: float
    sample_format: int
    records: tuple[WaveformRecord, ...]

    def __post_init__(self) -> None:
        if self.sample_format not in WAVEFORM_FORMATS:
            raise RecordingError(
                f"the waveform track {self.name} has the sample format {self.sample_format}, which is not one of the formats 1 to 8 of the file format"
            )


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
