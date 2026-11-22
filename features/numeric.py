"""The features of one numeric track over the 15-minute window that ends at the cutoff."""

from __future__ import annotations

import math
from collections.abc import Sequence

from features.model import SECOND, FeatureInputError, FeatureValue, GroupResult, NumericWindow, Reading
from features.registry import NUMERIC_STATS, NUMERIC_TRACKS, NUMERIC_WINDOW_SECONDS, PER_SECOND, complete_seconds, slug
from features.validate import boolean, number, whole_microseconds


def window_readings(track: str, readings: Sequence[Reading], cutoff_us: int, per_second: bool) -> NumericWindow:
    """The valid readings of the window, oldest first, with their values as floats. A reading after the cutoff is never read.

    A source-time track has ``cutoff - 15 minutes < time <= cutoff`` and keeps its published times. The per-second series has the newest 900 complete seconds
    (``complete_seconds``), each reading at the start of a whole second, and its times in the window are the ends of those seconds. Two readings of one track
    at one time are refused, and so is a reading with a time, a flag or a value of the wrong type.
    """
    whole_microseconds(cutoff_us, "the cutoff")
    for reading in readings:
        whole_microseconds(reading.time_us, f"{track}: a reading's time")
        boolean(reading.valid, f"{track}: a reading's valid flag")
        number(reading.value, f"{track}: a reading's value")
    if per_second:
        window = complete_seconds(cutoff_us, NUMERIC_WINDOW_SECONDS)
        lower, upper = window[0] * SECOND, (window[-1] + 1) * SECOND
        inside = [reading for reading in readings if lower <= reading.time_us < upper]
    else:
        lower = cutoff_us - NUMERIC_WINDOW_SECONDS * SECOND
        inside = [reading for reading in readings if lower < reading.time_us <= cutoff_us]
    inside.sort(key=lambda reading: reading.time_us)
    times = [reading.time_us for reading in inside]
    if len(set(times)) != len(times):
        raise FeatureInputError(f"{track}: the window holds more than one reading at one time")
    for reading in inside:
        if per_second and reading.time_us % SECOND:
            raise FeatureInputError(f"{track}: a reading of the per-second series is not at a whole second")
        if reading.valid and not math.isfinite(reading.value):
            raise FeatureInputError(f"{track}: a valid reading is not a finite number")
    valid = [reading for reading in inside if reading.valid]
    known = SECOND if per_second else 0
    return NumericWindow(cutoff_us, [reading.time_us + known for reading in valid], [float(reading.value) for reading in valid])


def numeric_group(track: str, readings: Sequence[Reading], cutoff_us: int) -> GroupResult:
    """The statistics of the track's input kind. One is missing only when it cannot be computed, and the valid count is always returned.

    Every count is an int and every other number a float, and a result that a float cannot hold is refused with its name.
    """
    spec = NUMERIC_TRACKS.get(track)
    if spec is None:
        raise FeatureInputError(f"unknown track {track}")
    window = window_readings(track, readings, cutoff_us, spec.kind == PER_SECOND)
    values: dict[str, FeatureValue] = {}
    for stat in NUMERIC_STATS[spec.kind]:
        name = f"{slug(track)}__{stat.key}"
        try:
            value = stat.compute(window) if window.values or not stat.needs_value else None
        except OverflowError as error:
            raise FeatureInputError(f"{name}: the result is outside the range of a float") from error
        if value is not None and not math.isfinite(value):
            raise FeatureInputError(f"{name}: the result is outside the range of a float")
        values[name] = value
    return GroupResult(values, window.times_us[-1] if window.times_us else None)
