"""The batch input of the per-second arterial series (``Silver/ART_MBP_1S``) for the feature code, bound to the one definition in ``features.pressure_series``.

``build_row`` takes the readings of this track only as ``SeriesReadings``. A ``SeriesReadings`` is made in one of two ways:

- ``series_readings(seconds)``: from the facts of the arterial seconds (``features.pressure_series.second_facts``), through ``series_from_seconds``. This is
  what the Silver job, the batch job that reads ``silver.art_mbp_1s`` and the streaming job call, so the three give the same readings for the same seconds.
  Each reading has the time of its second, the value of its exact mean and that exact mean.
- ``series_readings_from_rows(rows)``: from ``(time, value, valid)`` rows, as the JSON entry point and a test give them. A row carries only a float, so the
  exact value of a valid reading is the exact value of that float.

The constructor of ``SeriesReadings`` refuses a valid reading that has no exact value, or an exact value whose float is not the reading's value, so a series
that other code made, without the exact mean, cannot be passed on by accident. A reading whose value is not a number is left to the checks of
``features.numeric``, which refuse it by name.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from fractions import Fraction

from features.model import SECOND, FeatureInputError, Reading
from features.pressure_series import SecondFacts, series_from_seconds


def exact_of(value: object) -> Fraction | None:
    """The exact value of a float that is a finite number, or ``None`` for anything else."""
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return Fraction(value)


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
            if float(reading.exact) != reading.value:
                raise FeatureInputError("Silver/ART_MBP_1S: the exact mean of a reading is not the one of its value")


def series_readings(seconds: Iterable[SecondFacts]) -> SeriesReadings:
    """The readings of the given arterial seconds, oldest first, by the one definition."""
    readings = []
    for second, reading in series_from_seconds(seconds):
        mean = reading.mean
        readings.append(Reading(second * SECOND, math.nan if mean is None else mean.value, reading.valid, None if mean is None else mean.exact))
    return SeriesReadings(readings)


def series_readings_from_rows(rows: Sequence[tuple[int, float, bool]] | Iterable[tuple[int, float, bool]]) -> SeriesReadings:
    """The readings of ``(time, value, valid)`` rows. The exact value of a valid reading is the exact value of its float."""
    return SeriesReadings(Reading(time, value, valid, exact_of(value) if valid is True else None) for time, value, valid in rows)
