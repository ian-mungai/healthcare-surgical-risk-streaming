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

The constructor of ``SeriesReadings`` refuses a valid reading whose value is a finite number and that has no exact mean, so a series that other code made
cannot be passed on by accident. ``features.numeric`` refuses, inside the window, a valid reading whose value is not the float of its exact mean.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from fractions import Fraction

from features.model import SECOND, FeatureInputError, Reading
from features.pressure_series import ExactMean, SecondFacts, series_from_seconds


def exact_of(value: object) -> Fraction | None:
    """The exact value of a float that is a finite number, or ``None`` for anything else."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    try:
        return Fraction(value) if math.isfinite(value) else None
    except OverflowError:
        return None


class SeriesReadings(list[Reading]):
    """The readings of the per-second arterial series, each valid one with the exact value that its float stands for."""

    def __init__(self, readings: Iterable[Reading] = ()) -> None:
        super().__init__(readings)
        for reading in self:
            if reading.valid is not True or reading.value is None:
                continue
            exact = exact_of(reading.value)
            if exact is None:
                continue
            if reading.exact is None:
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


def series_readings_from_json(rows: Sequence[object], where: str) -> SeriesReadings:
    """The readings of the JSON rows ``[time, value, valid, sample count, sum high, sum low]``. A row of another form is refused by name, a valid row without a
    finite exact mean is refused, and no float is made of a value (so no reading is converted that a feature would not read)."""
    form = f"{where}: a reading is an array of time, value, valid flag, sample count and the high and low parts of the exact sum"
    readings = []
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 6:
            raise FeatureInputError(f"{form} (reading {index})")
        time, value, valid, count, high, low = row
        exact = None
        if valid is True:
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or not number_of(high) or not number_of(low):
                raise FeatureInputError(f"{form}, and a valid reading has a sample count above zero and a finite sum (reading {index})")
            exact, part = sum_part(high), sum_part(low)
            if exact is None or part is None:
                raise FeatureInputError(f"{where}: the exact sum of reading {index} is not a finite number")
            exact = (exact + part) / count
        readings.append(Reading(time, value, valid, exact))
    return SeriesReadings(readings)
