"""The one definition of the per-second arterial pressure series ``Silver/ART_MBP_1S``: the trailing 2-second average of ``SNUADC/ART``.

The batch path (the Silver job), the event rule and the streaming job all call the functions of this module, so the same two seconds give the same reading.
The module uses the standard library and ``features.signal_validity`` only and imports no Spark.

**Value.** The reading of second ``s`` is the mean of every physical sample of the seconds ``s - 1`` and ``s``. It is known at the end of second ``s``, and by
the convention of ``features.registry`` its time is ``(s + 1) * 1_000_000`` microseconds. It never reads a second after ``s``, so only what is known at the
cutoff decides.

**Exactness.** No rounding decides a limit test. A second keeps the exact sum of its samples as two floats: ``total`` (the sum rounded once, ``math.fsum``)
and ``residual`` (what the rounding left out: the exact sum minus ``total``). A float holds the residual exactly, because the samples are limited to 20 to
200 mmHg and the exact sum then spans fewer than 106 bits. A reading keeps its two floats and its sample count as an ``ExactMean``. ``ExactMean.exact`` is the
mean as an exact fraction, ``ExactMean.value`` is that fraction rounded once to a float (for display and for the statistics of the features) and
``exact_below`` is the one place that a mean is compared with a limit: a mean below 65 mmHg whose float is exactly 65.0 is still below 65.

**Validity.** A reading is valid only when both seconds are valid by ``features.signal_validity`` (analysis plan row 4: the trailing rule judges each second
by what ends at it). A second that is invalid makes its own reading and the reading of the next second invalid, because that average would hold its samples. The
cause is the cause of the second itself when that is invalid, else the cause of the second before it. A second with no second before it (the first one of a
recording) or with a gap before it is ``missing``: an average is never made from one second, and a gap is never bridged. An invalid reading has no mean
and no value.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from fractions import Fraction

from features.signal_validity import ARTERIAL_WAVEFORM, MISSING, Verdict

SERIES_TRACK = ARTERIAL_WAVEFORM
TRAILING_SECONDS = 2


@dataclass(frozen=True, slots=True)
class SecondFacts:
    """What one second of ``SERIES_TRACK`` gives to the average: its verdict and, for a valid second, its number of samples and the exact sum of them as
    ``total`` plus ``residual``."""

    second: int
    valid: bool
    cause: str | None
    count: int
    total: float
    residual: float = 0.0


@dataclass(frozen=True, slots=True)
class ExactMean:
    """The mean of ``count`` samples whose exact sum is ``total_high + total_low``."""

    count: int
    total_high: float
    total_low: float

    @classmethod
    def of(cls, value: float) -> ExactMean:
        """The mean of one sample: that sample, exactly."""
        return cls(1, value, 0.0)

    @property
    def exact(self) -> Fraction:
        return (Fraction(self.total_high) + Fraction(self.total_low)) / self.count

    @property
    def value(self) -> float:
        """The exact mean rounded once to a float. Never use it to compare with a limit: ``below`` does."""
        return float(self.exact)

    def below(self, limit: float) -> bool:
        return exact_below(self.exact, limit)


@dataclass(frozen=True, slots=True)
class SeriesReading:
    """The reading of one second: its mean (``None`` when invalid), whether it is valid and the cause when it is not."""

    mean: ExactMean | None
    valid: bool
    cause: str | None

    @property
    def value(self) -> float | None:
        return None if self.mean is None else self.mean.value


def exact_below(exact: Fraction, limit: float) -> bool:
    """Whether an exact mean is below a limit, by the exact values: the one comparison of the series with a limit, for the events and for the features."""
    return exact < Fraction(limit)


def second_facts(second: int, verdict: Verdict, samples: Sequence[float | None]) -> SecondFacts:
    """The facts of a second from its verdict and its physical samples. A valid second must have samples that are all finite; an invalid one's are not read."""
    if not verdict.valid:
        return SecondFacts(second, False, verdict.cause, 0, 0.0, 0.0)
    if not samples or not all(sample is not None and math.isfinite(sample) for sample in samples):
        raise ValueError(f"second {second} is valid and has no samples that are all finite numbers")
    values = [float(sample) for sample in samples if sample is not None]
    total = math.fsum(values)
    return SecondFacts(second, True, None, len(values), total, math.fsum([*values, -total]))


def series_second(previous: SecondFacts | None, current: SecondFacts) -> SeriesReading:
    """The reading of ``current`` from the second before it. ``previous`` is ``None`` when there is none; any other than ``current.second - 1`` is a gap."""
    if previous is not None and previous.second >= current.second:
        raise ValueError(f"second {previous.second} is not before second {current.second}")
    if not current.valid:
        return SeriesReading(None, False, current.cause)
    if previous is None or previous.second != current.second - 1:
        return SeriesReading(None, False, MISSING)
    if not previous.valid:
        return SeriesReading(None, False, previous.cause)
    count = previous.count + current.count
    total = Fraction(previous.total) + Fraction(previous.residual) + Fraction(current.total) + Fraction(current.residual)
    high = float(total)
    return SeriesReading(ExactMean(count, high, float(total - Fraction(high))), True, None)


def series_from_seconds(seconds: Iterable[SecondFacts]) -> Iterator[tuple[int, SeriesReading]]:
    """The reading of every second, oldest first, whatever the order of the input. A second given twice is refused by ``series_second``."""
    previous: SecondFacts | None = None
    for current in sorted(seconds, key=lambda facts: facts.second):
        yield current.second, series_second(previous, current)
        previous = current
