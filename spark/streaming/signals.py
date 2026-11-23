"""The signal rules of one waveform second, and the per-second arterial series built from them.

Analysis plan row 4, "Invalid second", for the arterial pressure waveform: a second is invalid when it is not a whole block of finite samples,
when a sample is outside 20 to 200 mmHg, when its range is under 20 mmHg, or when a sample jumps by more than 30 mmHg from the one before it,
also across the second boundary. The stream applies these rules to each second and builds the series ``Silver/ART_MBP_1S`` as the trailing
2-second average of the arterial waveform. Every other waveform second is valid when it is a whole block of finite samples. The plan does not say
how long "the segment that ends at the second under test" is: the second itself is used. Silver, when it is built, must use these functions or
the same rules, so that batch and stream agree.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from replay.events.recording import physical_value

ARTERIAL_WAVEFORM = "SNUADC/ART"
MIN_PRESSURE_MMHG = 20.0
MAX_PRESSURE_MMHG = 200.0
MIN_RANGE_MMHG = 20.0
MAX_JUMP_MMHG = 30.0


@dataclass(frozen=True, slots=True)
class SecondSummary:
    """What later windows need of one second: whether it is valid on its own, its first and last sample, the sum of its samples and their count."""

    valid: bool
    first: float
    last: float
    total: float
    count: int


def physical_samples(raw: Sequence[float], gain: float, offset: float) -> tuple[float, ...]:
    return tuple(physical_value(value, gain, offset) for value in raw)


def whole_block(samples: Sequence[float], rate: int, start_offset_us: int) -> bool:
    """A second that starts on its boundary and holds exactly ``rate`` finite samples."""
    return start_offset_us == 0 and rate > 0 and len(samples) == rate and all(math.isfinite(sample) for sample in samples)


def arterial_pressure_ok(samples: Sequence[float]) -> bool:
    if any(not MIN_PRESSURE_MMHG <= sample <= MAX_PRESSURE_MMHG for sample in samples):
        return False
    if max(samples) - min(samples) < MIN_RANGE_MMHG:
        return False
    return all(abs(after - before) <= MAX_JUMP_MMHG for before, after in zip(samples, samples[1:], strict=False))


def summarise_second(samples: Sequence[float], rate: int, start_offset_us: int, *, arterial: bool) -> SecondSummary:
    """The summary of a second. The values of an invalid second are never read, so its numbers are zero when they cannot be taken."""
    if not whole_block(samples, rate, start_offset_us):
        return SecondSummary(False, 0.0, 0.0, 0.0, len(samples))
    valid = arterial_pressure_ok(samples) if arterial else True
    return SecondSummary(valid, samples[0], samples[-1], math.fsum(samples), len(samples))


def boundary_ok(before: SecondSummary, current: SecondSummary) -> bool:
    return abs(current.first - before.last) <= MAX_JUMP_MMHG


def series_reading(before: SecondSummary | None, current: SecondSummary) -> tuple[float, bool]:
    """The value of the per-second series at a second and whether it is valid: the mean of that second and the one before it.

    It is valid only when both seconds are valid and the jump across their boundary is at most 30 mmHg. An invalid reading has the value not-a-number.
    """
    if before is None or not before.valid or not current.valid or not boundary_ok(before, current):
        return math.nan, False
    return (before.total + current.total) / (before.count + current.count), True
