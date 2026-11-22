"""The features of one numeric track over the 15-minute window that ends at the cutoff."""

from __future__ import annotations

import math
from collections.abc import Sequence

from features.model import SECOND, FeatureInputError, FeatureValue, GroupResult, NumericWindow, Reading
from features.registry import NUMERIC_STATS, NUMERIC_TRACKS, NUMERIC_WINDOW_SECONDS, PER_SECOND, slug


def window_readings(track: str, readings: Sequence[Reading], cutoff_us: int, whole_seconds: bool) -> NumericWindow:
    """The valid readings with ``cutoff - 15 minutes < time <= cutoff``, oldest first. A reading after the cutoff is never read.

    A per-second series (``whole_seconds``) holds each reading at a whole second. Two readings of one track at one time are refused for every track.
    """
    lower = cutoff_us - NUMERIC_WINDOW_SECONDS * SECOND
    inside = sorted((reading for reading in readings if lower < reading.time_us <= cutoff_us), key=lambda reading: reading.time_us)
    times = [reading.time_us for reading in inside]
    if len(set(times)) != len(times):
        raise FeatureInputError(f"{track}: the window holds more than one reading at one time")
    for reading in inside:
        if whole_seconds and reading.time_us % SECOND:
            raise FeatureInputError(f"{track}: a reading of the per-second series is not at a whole second")
        if reading.valid and not math.isfinite(reading.value):
            raise FeatureInputError(f"{track}: a valid reading is not a finite number")
    valid = [reading for reading in inside if reading.valid]
    return NumericWindow(cutoff_us, [reading.time_us for reading in valid], [reading.value for reading in valid])


def numeric_group(track: str, readings: Sequence[Reading], cutoff_us: int) -> GroupResult:
    """The statistics of the track's input kind. One is missing only when it cannot be computed, and the valid count is always returned."""
    spec = NUMERIC_TRACKS.get(track)
    if spec is None:
        raise FeatureInputError(f"unknown track {track}")
    window = window_readings(track, readings, cutoff_us, spec.kind == PER_SECOND)
    values: dict[str, FeatureValue] = {}
    for stat in NUMERIC_STATS[spec.kind]:
        values[f"{slug(track)}__{stat.key}"] = stat.compute(window) if window.values or not stat.needs_value else None
    return GroupResult(values, window.times_us[-1] if window.times_us else None)
