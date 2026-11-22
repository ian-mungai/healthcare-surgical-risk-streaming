"""The plain inputs and outputs of the feature code. No Spark, no Kafka: a window is a few sequences of numbers.

A time is a whole number of microseconds from the start of the case. Silver decides whether a reading or a second is valid. The feature code only counts it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

SECOND = 1_000_000
MINUTE = 60 * SECOND

FeatureValue = float | int | str | None


class FeatureInputError(ValueError):
    """An input the feature code refuses: a duplicate, an off-grid time, a value that is not a number or a track the registry does not know."""


@dataclass(frozen=True, slots=True)
class Reading:
    """One reading of a numeric track on Silver's one-second grid."""

    time_us: int
    value: float
    valid: bool


@dataclass(frozen=True, slots=True)
class WaveformSecond:
    """The samples of one second of a waveform. ``second`` is its index from the case start. ``start_offset_us`` is the time of its first sample inside it."""

    second: int
    samples: Sequence[float]
    valid: bool
    start_offset_us: int = 0


@dataclass(frozen=True, slots=True)
class WaveformWindow:
    sample_rate: float
    seconds: tuple[WaveformSecond, ...]


@dataclass(frozen=True, slots=True)
class ClinicalValue:
    """A fact known before surgery and the time it became available. ``None`` means the time is not established, and the value is never used."""

    value: FeatureValue
    available_us: int | None


@dataclass(frozen=True, slots=True)
class WindowInput:
    """Everything the features of one case occurrence at one cutoff may read."""

    cutoff_us: int
    numeric: Mapping[str, Sequence[Reading]]
    waveforms: Mapping[str, WaveformWindow]
    clinical: Mapping[str, ClinicalValue]


@dataclass(frozen=True, slots=True)
class NumericWindow:
    """The valid readings of one track inside the window, oldest first."""

    cutoff_us: int
    times_us: Sequence[int]
    values: Sequence[float]


@dataclass(frozen=True, slots=True)
class WaveformValid:
    """The valid seconds of one waveform inside the window, oldest first, with the index of the window's first second."""

    sample_rate: float
    first_second: int
    seconds: Sequence[WaveformSecond]


@dataclass(frozen=True, slots=True)
class GroupResult:
    """The values of one feature group and the latest time of the input that a returned value used."""

    values: dict[str, FeatureValue]
    latest_us: int | None


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """The features of one window. ``latest_reading_us`` and ``latest_clinical_us`` are the latest times of the signal and of the clinical input used."""

    cutoff_us: int
    values: dict[str, FeatureValue]
    latest_reading_us: int | None
    latest_clinical_us: int | None
