"""The arithmetic of the features. Every sum is an exact sum (``math.fsum``), so no value depends on the order of the input or on the platform."""

from __future__ import annotations

import math
from collections.abc import Sequence

from features.model import SECOND, WaveformSecond


def mean(values: Sequence[float]) -> float:
    return math.fsum(values) / len(values)


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def sample_std(values: Sequence[float]) -> float | None:
    """The standard deviation with ``n - 1`` in the divisor. Missing for a single value."""
    if len(values) < 2:
        return None
    centre = mean(values)
    return math.sqrt(math.fsum((value - centre) ** 2 for value in values) / (len(values) - 1))


def slope(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """The least-squares slope of ``ys`` on ``xs``. Missing when the ``xs`` do not differ."""
    x_centre, y_centre = mean(xs), mean(ys)
    spread = math.fsum((x - x_centre) ** 2 for x in xs)
    if spread == 0:
        return None
    return math.fsum((x - x_centre) * (y - y_centre) for x, y in zip(xs, ys, strict=True)) / spread


def dominant_frequency(
    seconds: Sequence[WaveformSecond], sample_rate: float, first_second: int, window_seconds: int, low_hz: float, high_hz: float
) -> float | None:
    """The frequency with the most power among ``k / window_seconds`` Hz inside the band, from the samples at their true times, so a gap shifts nothing.

    The mean is removed first. The lowest frequency wins a tie. A flat signal has no power at any frequency and gives no answer.
    """
    times: list[float] = []
    values: list[float] = []
    for second in seconds:
        base = (second.second - first_second) + second.start_offset_us / SECOND
        for position, sample in enumerate(second.samples):
            times.append(base + position / sample_rate)
            values.append(sample)
    centre = mean(values)
    centred = [value - centre for value in values]
    best_power, best_frequency = 0.0, None
    for step in range(math.ceil(low_hz * window_seconds), math.floor(high_hz * window_seconds) + 1):
        frequency = step / window_seconds
        omega = 2 * math.pi * frequency
        cosine = math.fsum(value * math.cos(omega * time) for value, time in zip(centred, times, strict=True))
        sine = math.fsum(value * math.sin(omega * time) for value, time in zip(centred, times, strict=True))
        power = cosine * cosine + sine * sine
        if power > best_power:
            best_power, best_frequency = power, frequency
    return best_frequency
