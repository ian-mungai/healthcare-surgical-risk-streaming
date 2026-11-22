"""The features of one numeric track over the 15-minute window that ends at the cutoff."""

from __future__ import annotations

import math
from collections.abc import Sequence

from features.model import SECOND, FeatureInputError, FeatureValue, GroupResult, NumericWindow, Reading
from features.registry import ARTERIAL_MEAN_TRACK, MIN_VALID_SHARE, NUMERIC_STATS, NUMERIC_TRACKS, NUMERIC_WINDOW_SECONDS, slug


def window_readings(track: str, readings: Sequence[Reading], cutoff_us: int) -> NumericWindow:
    """The valid readings with ``cutoff - 15 minutes < time <= cutoff``, oldest first. A reading after the cutoff is never read."""
    lower = cutoff_us - NUMERIC_WINDOW_SECONDS * SECOND
    inside = sorted((reading for reading in readings if lower < reading.time_us <= cutoff_us), key=lambda reading: reading.time_us)
    times = [reading.time_us for reading in inside]
    if len(set(times)) != len(times):
        raise FeatureInputError(f"{track}: the window holds more than one reading at one time, and the grid holds one reading each second")
    for reading in inside:
        if reading.time_us % SECOND:
            raise FeatureInputError(f"{track}: a reading is not at a whole second")
        if reading.valid and not math.isfinite(reading.value):
            raise FeatureInputError(f"{track}: a valid reading is not a finite number")
    valid = [reading for reading in inside if reading.valid]
    return NumericWindow(cutoff_us, [reading.time_us for reading in valid], [reading.value for reading in valid])


def numeric_group(track: str, readings: Sequence[Reading], cutoff_us: int) -> GroupResult:
    if track not in NUMERIC_TRACKS:
        raise FeatureInputError(f"unknown track {track}")
    window = window_readings(track, readings, cutoff_us)
    enough = len(window.values) >= MIN_VALID_SHARE * NUMERIC_WINDOW_SECONDS
    values: dict[str, FeatureValue] = {}
    for stat in NUMERIC_STATS:
        if stat.arterial_mean_only and track != ARTERIAL_MEAN_TRACK:
            continue
        values[f"{slug(track)}__{stat.key}"] = stat.compute(window) if enough or stat.always else None
    return GroupResult(values, window.times_us[-1] if enough else None)
