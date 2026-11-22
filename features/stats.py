"""The arithmetic of the features. Every sum is an exact sum (``math.fsum``), so no value depends on the order of the input or on the platform.

A value is scaled by a power of two before it is squared or summed, and the result is scaled back. A power of two changes no bit of a normal number, so a result
that a float can hold never overflows on the way. A window whose values are all equal is flat: its standard deviation and its slope are exactly 0, and it has no
dominant frequency.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from features.model import WaveformSecond

TIE_TOLERANCE = 1e-9


def exponent(values: Sequence[float]) -> int:
    """The binary exponent of the largest absolute value, so that every value divided by ``2 ** exponent`` is below 1."""
    top = max(abs(value) for value in values)
    return math.frexp(top)[1] if top else 0


def scaled(values: Sequence[float], power: int) -> list[float]:
    return [math.ldexp(value, -power) for value in values]


def is_flat(values: Sequence[float]) -> bool:
    return min(values) == max(values)


def mean(values: Sequence[float]) -> float:
    if is_flat(values):
        return values[0]
    power = exponent(values)
    return math.ldexp(math.fsum(scaled(values, power)) / len(values), power)


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else mean([ordered[middle - 1], ordered[middle]])


def sample_std(values: Sequence[float]) -> float | None:
    """The standard deviation with ``n - 1`` in the divisor. Missing for a single value, and exactly 0 for a flat window."""
    if len(values) < 2:
        return None
    if is_flat(values):
        return 0.0
    power = exponent(values)
    unit = scaled(values, power)
    centre = math.fsum(unit) / len(unit)
    return math.ldexp(math.sqrt(math.fsum((value - centre) ** 2 for value in unit) / (len(unit) - 1)), power)


def slope(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """The least-squares slope of ``ys`` on ``xs``. Missing when the ``xs`` do not differ, and exactly 0 when the ``ys`` are flat."""
    x_centre = mean(xs)
    spread = math.fsum((x - x_centre) ** 2 for x in xs)
    if spread == 0:
        return None
    if is_flat(ys):
        return 0.0
    power = exponent(ys)
    unit = scaled(ys, power)
    y_centre = math.fsum(unit) / len(unit)
    return math.ldexp(math.fsum((x - x_centre) * (y - y_centre) for x, y in zip(xs, unit, strict=True)) / spread, power)


def dominant_frequency(
    seconds: Sequence[WaveformSecond], sample_rate: int, first_second: int, window_seconds: int, low_hz: float, high_hz: float
) -> float | None:
    """The lowest frequency whose power is within a relative 1e-9 of the largest, among ``k / window_seconds`` Hz inside the band.

    A sample is at its block's start (block index minus ``first_second``, in seconds) plus its position over the rate. The mean is removed first. A flat signal
    has no power at any frequency and gives no answer.
    """
    times: list[float] = []
    values: list[float] = []
    for second in seconds:
        base = second.second - first_second
        for position, sample in enumerate(second.samples):
            times.append(base + position / sample_rate)
            values.append(sample)
    if is_flat(values):
        return None
    unit = scaled(values, exponent(values))
    centre = math.fsum(unit) / len(unit)
    centred = [value - centre for value in unit]
    powers: list[tuple[float, float]] = []
    for step in range(math.ceil(low_hz * window_seconds), math.floor(high_hz * window_seconds) + 1):
        frequency = step / window_seconds
        omega = 2 * math.pi * frequency
        cosine = math.fsum(value * math.cos(omega * time) for value, time in zip(centred, times, strict=True))
        sine = math.fsum(value * math.sin(omega * time) for value, time in zip(centred, times, strict=True))
        powers.append((frequency, cosine * cosine + sine * sine))
    top = max(power for _, power in powers)
    if top <= 0:
        return None
    return next(frequency for frequency, power in powers if power >= top * (1 - TIE_TOLERANCE))
