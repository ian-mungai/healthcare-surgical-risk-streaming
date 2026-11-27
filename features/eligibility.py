"""The sample eligibility limits of the analysis plan (project standard, "Analysis decisions and their evidence", row 5).

A warning sample is not scored when its 20-second waveform window holds an invalid second, or when its 15-minute window holds more than 10% invalid seconds
of the per-second arterial series. Both windows are the newest complete seconds at the cutoff (``features.registry.complete_seconds``). A second that is missing
counts as invalid. The limits are the named constants of ``features.registry``. These are limits on
a sample and not on a feature: a feature is missing only when it cannot be computed.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from features.model import Reading, WaveformWindow
from features.numeric import window_readings
from features.registry import (
    ARTERIAL_SERIES,
    MAX_INVALID_NUMERIC_PERCENT,
    MAX_INVALID_WAVEFORM_SECONDS,
    NUMERIC_OVER_10_PERCENT_INVALID,
    NUMERIC_WINDOW_SECONDS,
    WAVEFORM_INVALID_SECOND,
    WAVEFORM_WINDOW_SECONDS,
)
from features.waveform import window_seconds


@dataclass(frozen=True, slots=True)
class Eligibility:
    """``eligible`` is true when no limit failed. ``reasons`` names each failed limit, the waveform limit first."""

    eligible: bool
    reasons: tuple[str, ...]


def warning_eligibility(cutoff_us: int, arterial_series: Sequence[Reading], arterial_waveform: WaveformWindow) -> Eligibility:
    reasons: list[str] = []
    waveform = window_seconds("SNUADC/ART", arterial_waveform, cutoff_us)
    if WAVEFORM_WINDOW_SECONDS - len(waveform.seconds) > MAX_INVALID_WAVEFORM_SECONDS:
        reasons.append(WAVEFORM_INVALID_SECOND)
    series = window_readings(ARTERIAL_SERIES, arterial_series, cutoff_us, per_second=True)
    if (NUMERIC_WINDOW_SECONDS - len(series.values)) * 100 > MAX_INVALID_NUMERIC_PERCENT * NUMERIC_WINDOW_SECONDS:
        reasons.append(NUMERIC_OVER_10_PERCENT_INVALID)
    return Eligibility(not reasons, tuple(reasons))
