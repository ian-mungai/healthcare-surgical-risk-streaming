"""Shared builders for the fast tests of the feature package: hand-built windows with known answers."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence

from features.model import Reading, WaveformSecond, WaveformWindow

SECOND = 1_000_000
CUTOFF_US = 3_600 * SECOND
ARTERIAL_MEAN = "Solar8000/ART_MBP"
SERIES = "Silver/ART_MBP_1S"
HEART_RATE = "Solar8000/HR"
RATE_HZ = 10.0


def numeric_window(values: Sequence[float], cutoff_us: int = CUTOFF_US, valid: Callable[[int], bool] = lambda index: True) -> list[Reading]:
    """One reading for each second, the last at the cutoff: reading ``j`` of ``n`` is at ``cutoff - (n - 1 - j)`` seconds."""
    count = len(values)
    return [Reading(cutoff_us - (count - 1 - index) * SECOND, value, valid(index)) for index, value in enumerate(values)]


def spaced_window(values: Sequence[float], step_seconds: int, cutoff_us: int = CUTOFF_US, valid: Callable[[int], bool] = lambda index: True) -> list[Reading]:
    """Readings every ``step_seconds``, the last at the cutoff, as a monitor reports them."""
    count = len(values)
    return [Reading(cutoff_us - (count - 1 - index) * step_seconds * SECOND, value, valid(index)) for index, value in enumerate(values)]


def arithmetic(start: float, step: float, count: int = 900) -> list[float]:
    return [start + step * index for index in range(count)]


def window_seconds(count: int, cutoff_us: int = CUTOFF_US) -> list[int]:
    """The indexes of the ``count`` complete seconds that end at or before the cutoff."""
    last = cutoff_us // SECOND - 1
    return list(range(last - count + 1, last + 1))


def second_of(index: int, samples: Sequence[float], valid: bool = True) -> WaveformSecond:
    return WaveformSecond(index, tuple(samples), valid)


def waveform_window(
    per_second: Callable[[int], Sequence[float]], cutoff_us: int = CUTOFF_US, invalid: frozenset[int] = frozenset(), count: int = 20
) -> WaveformWindow:
    """A window of ``count`` seconds where second ``i`` of the window holds ``per_second(i)``. Seconds in ``invalid`` (window positions) are flagged invalid."""
    indexes = window_seconds(count, cutoff_us)
    return WaveformWindow(RATE_HZ, tuple(second_of(index, per_second(position), position not in invalid) for position, index in enumerate(indexes)))


def sine(frequency_hz: float, centre: float, amplitude: float) -> Callable[[int], list[float]]:
    """The samples of window second ``i`` for a sine of ``frequency_hz``, sampled at ``RATE_HZ`` from the start of the window."""
    per_second = int(RATE_HZ)
    return lambda position: [centre + amplitude * math.sin(2 * math.pi * frequency_hz * (position + sample / RATE_HZ)) for sample in range(per_second)]


PATTERN_A = (70.0, 120.0, 80.0, 110.0, 90.0, 100.0, 85.0, 105.0, 95.0, 100.0)
PATTERN_B = (85.0, 115.0, 90.0, 110.0, 95.0, 105.0, 92.0, 108.0, 98.0, 102.0)


def alternating(position: int) -> tuple[float, ...]:
    """Even window seconds hold pattern A (range 50, sum 955), odd ones pattern B (range 30, sum 1000)."""
    return PATTERN_A if position % 2 == 0 else PATTERN_B
