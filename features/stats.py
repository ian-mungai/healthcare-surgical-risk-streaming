"""The arithmetic of the features.

Mean, median, standard deviation and least-squares slope are exact: every float is taken as the rational number that it is, the sums are whole-number sums, and
the result is rounded to a float once at the end (the standard deviation after one square root). So a result that a float can hold never overflows or loses
a term on the way, a window whose values are all equal has a standard deviation and a slope of exactly 0, and no value depends on the order of the input. A
result that a float cannot hold raises ``OverflowError``, which the feature groups turn into a named input error.

The dominant frequency is a sum of sines and cosines and cannot be exact. It scales the samples by a power of two first, so that nothing overflows.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from fractions import Fraction

from features.model import WaveformSecond

Number = float | Fraction
TIE_TOLERANCE = 1e-9
BAND_POWER_FLOOR = 1e-9
ROOT_BITS = 130


def common_denominator(values: Sequence[Number]) -> tuple[list[int], int]:
    """The values as whole numbers over one common denominator: ``(numerators, denominator)``."""
    fractions = [Fraction(value) for value in values]
    denominator = math.lcm(*(fraction.denominator for fraction in fractions))
    return [fraction.numerator * (denominator // fraction.denominator) for fraction in fractions], denominator


def mean(values: Sequence[Number]) -> float:
    numerators, denominator = common_denominator(values)
    return float(Fraction(sum(numerators), denominator * len(values)))


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else float((Fraction(ordered[middle - 1]) + Fraction(ordered[middle])) / 2)


def square_root(value: Fraction) -> float:
    """The square root of a non-negative rational number, rounded to a float once: an integer root with at least 65 bits, then the nearest float."""
    if value == 0:
        return 0.0
    product = value.numerator * value.denominator
    shift = max(0, (ROOT_BITS - product.bit_length() + 1) // 2)
    return float(Fraction(math.isqrt(product << (2 * shift)), value.denominator << shift))


def sample_std(values: Sequence[Number]) -> float | None:
    """The standard deviation with ``n - 1`` in the divisor. Missing for a single value, and exactly 0 for a flat window."""
    count = len(values)
    if count < 2:
        return None
    numerators, denominator = common_denominator(values)
    total = sum(numerators)
    squares = sum((count * numerator - total) ** 2 for numerator in numerators)
    return square_root(Fraction(squares, (count * denominator) ** 2 * (count - 1)))


def slope_exact(xs: Sequence[Number], ys: Sequence[Number]) -> Fraction | None:
    """The exact least-squares slope of ``ys`` on ``xs``. Missing when the ``xs`` do not differ, and exactly 0 when the ``ys`` are flat."""
    count = len(xs)
    x_numerators, x_denominator = common_denominator(xs)
    y_numerators, y_denominator = common_denominator(ys)
    x_total, y_total = sum(x_numerators), sum(y_numerators)
    spread = count * sum(x * x for x in x_numerators) - x_total * x_total
    if spread == 0:
        return None
    cross = count * sum(x * y for x, y in zip(x_numerators, y_numerators, strict=True)) - x_total * y_total
    return Fraction(cross * x_denominator, spread * y_denominator)


def exponent(values: Sequence[float]) -> int:
    """The binary exponent of the largest absolute value, so that every value divided by ``2 ** exponent`` is below 1."""
    top = max(abs(value) for value in values)
    return math.frexp(top)[1] if top else 0


def dominant_frequency(
    seconds: Sequence[WaveformSecond], sample_rate: int, first_second: int, window_seconds: int, low_hz: float, high_hz: float
) -> float | None:
    """The lowest frequency whose power is within a relative 1e-9 of the largest, among ``k / window_seconds`` Hz in the band and below the Nyquist frequency.

    The bins are at ``low_hz`` or above, at ``high_hz`` or below, and strictly below ``sample_rate / 2``. A sample is at its block's start (block index minus
    ``first_second``, in seconds) plus its position over the rate. After the mean is removed, the frequency is missing when the largest power in the band is
    below 1e-9 of the total power (the number of samples times the sum of squared deviations), so a peak that is only roundoff or aliasing is never reported.
    A flat signal has no power at any frequency and gives no answer.
    """
    times: list[float] = []
    values: list[float] = []
    for second in seconds:
        base = second.second - first_second
        for position, sample in enumerate(second.samples):
            times.append(base + position / sample_rate)
            values.append(sample)
    if min(values) == max(values):
        return None
    power = exponent(values)
    unit = [math.ldexp(value, -power) for value in values]
    centre = math.fsum(unit) / len(unit)
    centred = [value - centre for value in unit]
    steps = [step for step in range(math.ceil(low_hz * window_seconds), math.floor(high_hz * window_seconds) + 1) if 2 * step < sample_rate * window_seconds]
    if not steps:
        return None
    powers: list[tuple[float, float]] = []
    for step in steps:
        omega = 2 * math.pi * step / window_seconds
        cosine = math.fsum(value * math.cos(omega * time) for value, time in zip(centred, times, strict=True))
        sine = math.fsum(value * math.sin(omega * time) for value, time in zip(centred, times, strict=True))
        powers.append((step / window_seconds, cosine * cosine + sine * sine))
    top = max(power for _, power in powers)
    if top <= 0 or top < BAND_POWER_FLOOR * len(centred) * math.fsum(value * value for value in centred):
        return None
    return next(frequency for frequency, power in powers if power >= top * (1 - TIE_TOLERANCE))
