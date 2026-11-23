"""The batch input of the per-second arterial series (``Silver/ART_MBP_1S``) for the feature code, bound to the one definition in
``features.pressure_series``.

``build_row`` takes the readings of this track only as ``SeriesReadings``, and every valid reading of one carries its exact mean. A ``SeriesReadings`` is
made in one of three ways, all by this module:

- ``series_readings(seconds)``: from the facts of the arterial seconds (``features.pressure_series.second_facts``), through ``series_from_seconds``. This is
  what the Silver job, the batch job that reads ``silver.art_mbp_1s`` and the streaming job call, so the three give the same readings for the same seconds.
  Each reading has the time of its second, the value of its exact mean and that exact mean.
- ``series_readings_from_means(rows)``: from ``(time, valid, mean)`` rows, as the batch job gives them from the stored table (``sample_count``, ``sum_high`` and
  ``sum_low`` make the mean) and as a test gives them.
- ``series_readings_from_json(rows)``: from the JSON entry point. A reading is ``[time, value, valid, sample count, sum high, sum low]``: the exact mean is
  the count and the two floats of the exact sum, so the same readings give the same features through the command line and through ``WindowInput``. A reading
  with no exact mean is refused by name when it is valid (its sums are not read when it is not). Nothing is converted to a float here: a reading after the
  cutoff is never read, as for every other track, and the checks of ``features.numeric`` refuse a value that is too large only inside the window.

``check_window`` (called by ``build_row``) refuses a valid reading inside the window whose value is a number and that has no exact mean, so a series that
other code made cannot be passed on by accident, and ``features.numeric`` refuses, inside the window, a valid reading whose value is not the float of its exact
mean. The window is applied first: a reading after the cutoff or before the window is neither converted nor checked, whatever its count or its sums are.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from fractions import Fraction

from features.model import SECOND, FeatureInputError, Reading
from features.pressure_series import ExactMean, SecondFacts, series_from_seconds
from features.registry import NUMERIC_WINDOW_SECONDS, complete_seconds


def exact_of(value: object) -> Fraction | None:
    """The exact value of a float that is a finite number, or ``None`` for anything else."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        return Fraction(value) if math.isfinite(value) else None
    except OverflowError:
        return None


class SeriesReadings(list[Reading]):
    """The readings of the per-second arterial series, made by this module. Each valid reading that a feature reads carries the exact value that its float
    stands for (``check_window``); nothing is converted or checked here, so a reading that no feature reads is never touched."""


def in_window(time_us: object, cutoff_us: int) -> bool:
    """Whether a reading at ``time_us`` is one that the features read: ``features.numeric.window_readings`` reads the newest complete seconds at the cutoff.
    A time that is not a whole number of microseconds is left to its checks, which refuse it."""
    if isinstance(time_us, bool) or not isinstance(time_us, int):
        return True
    window = complete_seconds(cutoff_us, NUMERIC_WINDOW_SECONDS)
    return window[0] * SECOND <= time_us < (window[-1] + 1) * SECOND


def is_read(time_us: object, cutoff_us: object) -> bool:
    """Whether a feature may read a reading at ``time_us``. Without a whole-number cutoff every reading may be, and ``build_row`` refuses the cutoff."""
    if isinstance(cutoff_us, bool) or not isinstance(cutoff_us, int):
        return True
    return in_window(time_us, cutoff_us)


def check_window(readings: Iterable[Reading], cutoff_us: object) -> None:
    """Refuse, by name, a valid reading inside the window whose value is a number and that has no exact mean. A reading outside the window is never read."""
    for reading in readings:
        if reading.valid is True and reading.exact is None and exact_of(reading.value) is not None and is_read(reading.time_us, cutoff_us):
            raise FeatureInputError("Silver/ART_MBP_1S: a valid reading has no exact mean; make the series with features.pressure_series")


def series_readings(seconds: Iterable[SecondFacts]) -> SeriesReadings:
    """The readings of the given arterial seconds, oldest first, by the one definition."""
    readings = []
    for second, reading in series_from_seconds(seconds):
        mean = reading.mean
        readings.append(Reading(second * SECOND, math.nan if mean is None else mean.value, reading.valid, None if mean is None else mean.exact))
    return SeriesReadings(readings)


def series_readings_from_means(rows: Iterable[tuple[int, bool, ExactMean | None]]) -> SeriesReadings:
    """The readings of ``(time, valid, mean)`` rows. A reading with no mean is invalid and has no value."""
    return SeriesReadings(Reading(time, math.nan if mean is None else mean.value, valid, None if mean is None else mean.exact) for time, valid, mean in rows)


def sum_part(value: object) -> Fraction | None:
    """A part of an exact sum as a fraction, made without a float so that a number too large for a float is kept, or ``None`` for a value that is not finite."""
    if isinstance(value, int) and not isinstance(value, bool):
        return Fraction(value)
    return Fraction(value) if isinstance(value, float) and math.isfinite(value) else None


def number_of(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int | float)


def series_readings_from_json(rows: Sequence[object], where: str, cutoff_us: object = None) -> SeriesReadings:
    """The readings of the JSON rows ``[time, value, valid, sample count, sum high, sum low]``. A row of another form is refused by name, a valid row without a
    finite exact mean is refused, and no float is made of a value (so no reading is converted that a feature would not read)."""
    form = f"{where}: a reading is an array of time, value, valid flag, sample count and the high and low parts of the exact sum"
    readings = []
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 6:
            raise FeatureInputError(f"{form} (reading {index})")
        time, value, valid, count, high, low = row
        exact = None
        if valid is True and is_read(time, cutoff_us):
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or not number_of(high) or not number_of(low):
                raise FeatureInputError(f"{form}, and a valid reading has a sample count above zero and a finite sum (reading {index})")
            exact, part = sum_part(high), sum_part(low)
            if exact is None or part is None:
                raise FeatureInputError(f"{where}: the exact sum of reading {index} is not a finite number")
            exact = (exact + part) / count
        readings.append(Reading(time, value, valid, exact))
    return SeriesReadings(readings)
