"""The signal rules of one waveform second and the per-second arterial series built from them.

The streaming job calls the one definition that Silver's batch path calls: ``features.signal_validity`` for the verdict of a second, and
``features.pressure_series`` and ``features.series_input`` for the series. It keeps only what a later second needs of each arterial second.

A second of the arterial pressure is judged by its own samples, the last sample of the second just before it and the extent (the complete seconds and the least
and most valid sample) of the 19 seconds before it: the flat rule reads the 20 seconds that end with the second. ``ArtSecond`` keeps that extent,
that last sample and the facts of the second for the series, so a later second is judged without the samples of an earlier one.
The series of a window is made by ``series_readings`` from those facts, so every reading carries its exact mean and the features
compare the exact mean with a limit, as for the batch path.

The physical samples of a second are made by ``features.signal_validity.physical_samples``, the function that Silver calls, from the sample format that the
replay's event carries (the format that Bronze keeps for the track): a gap marker of an integer format (``GAP_MARKERS``) is a missing sample, an integer
sample is ``raw * gain + offset`` and a float sample ignores the gain and the offset. A raw sample that is not a finite number is a missing sample.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from features.model import SECOND
from features.pressure_series import SecondFacts, second_facts
from features.registry import NUMERIC_WINDOW_SECONDS, complete_seconds
from features.series_input import SeriesReadings, series_readings
from features.signal_validity import ARTERIAL_WAVEFORM, FLAT_WINDOW_SECONDS, Extent, combine_extents, second_extent, waveform_verdict
from features.signal_validity import physical_samples as silver_physical_samples

__all__ = ["ARTERIAL_WAVEFORM", "ArtSecond", "art_second", "physical_samples", "second_valid", "series_window"]


@dataclass(frozen=True, slots=True)
class ArtSecond:
    """What later seconds and the series need of one arterial second: its verdict, the exact sum of its samples (as ``SecondFacts``), its last sample and its
    extent. ``last`` is ``None`` for a second with no sample or whose last sample is missing."""

    valid: bool
    cause: str | None
    count: int
    total: float
    residual: float
    last: float | None
    seconds: int
    low: float | None
    high: float | None

    @property
    def extent(self) -> Extent:
        return Extent(self.seconds, self.low, self.high)

    def facts(self, second: int) -> SecondFacts:
        return SecondFacts(second, self.valid, self.cause, self.count, self.total, self.residual)


def physical_samples(raw: Sequence[float], sample_format: int, gain: float, offset: float) -> tuple[float | None, ...]:
    """The physical samples of a raw second, as Silver makes them (the track's format decides the gap marker, the gain and the offset), with ``None`` for a
    sample that is missing or not a finite number."""
    return tuple(None if sample is None or not math.isfinite(sample) else sample for sample in silver_physical_samples(raw, sample_format, gain, offset))


def art_second(second: int, samples: Sequence[float | None], sample_rate: float, start_offset_us: int, earlier: Mapping[int, ArtSecond]) -> ArtSecond:
    """The record of an arterial second from its physical samples and the records of the seconds before it (only those of the seconds ``second - 1`` to
    ``second - 19`` are read, and a second that is absent adds nothing)."""
    previous = earlier.get(second - 1)
    prior = combine_extents(earlier[before].extent for before in range(second - FLAT_WINDOW_SECONDS + 1, second) if before in earlier)
    verdict = waveform_verdict(ARTERIAL_WAVEFORM, samples, sample_rate, start_offset_us, None if previous is None else previous.last, prior)
    facts = second_facts(second, verdict, samples)
    extent = second_extent(samples, sample_rate, start_offset_us)
    return ArtSecond(
        verdict.valid, verdict.cause, facts.count, facts.total, facts.residual, samples[-1] if samples else None, extent.seconds, extent.low, extent.high
    )


def second_valid(track: str, samples: Sequence[float | None], sample_rate: float, start_offset_us: int) -> bool:
    """Whether a second of a track other than the arterial pressure is valid: a whole block of present samples (no pressure rule applies to it)."""
    return waveform_verdict(track, samples, sample_rate, start_offset_us, None).valid


def series_window(held: Mapping[int, ArtSecond], cutoff_us: int) -> SeriesReadings:
    """The readings of the per-second arterial series inside the window at the cutoff (``complete_seconds``). The second before the window is read for the
    reading of the first second, and the readings of the seconds that the window does not hold are left out."""
    window = complete_seconds(cutoff_us, NUMERIC_WINDOW_SECONDS)
    facts = [held[second].facts(second) for second in range(window[0] - 1, window[-1] + 1) if second in held]
    return SeriesReadings(reading for reading in series_readings(facts) if reading.time_us >= window[0] * SECOND)
