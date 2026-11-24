"""The unscored share of the signal profile, from the validity of the seconds of one arterial waveform, on the saved warning grid. No Spark runs here.

Analysis decision 5 limits a sample: its 20-second waveform window tolerates no invalid second and its 15-minute window of the per-second arterial series
tolerates at most 10% invalid seconds (``features.registry``, ``features.eligibility``). Decision 3 takes one sample each minute. The profile measures the
share of the samples of the warning grid that the limits leave unscored, on development data.

**Grid.** The grid is the warning grid rule that the job saves in ``silver.warning_grid_rule`` (``spark.profile.grid_rule``), read from its constants and
copied nowhere: the cutoffs start at ``opstart`` plus the first cutoff offset, step ``GRID_STEP_SECONDS`` and end at the latest grid time at or before
``opend`` minus ``LAST_CUTOFF_MARGIN_SECONDS``. The grid is of the operation and not of the recorded span of the waveform: a cutoff whose windows reach before
the first or after the last recorded second is a cutoff, and its absent seconds are invalid (it is unscored, which is what the plan's "too little history"
means for a sample). A case with no ``opstart`` or no ``opend``, or an ``opend`` that is not after ``opstart``, or a time that is not a whole second (Silver's
seconds are whole), has no eligible monitored time (analysis plan, "Eligible monitored time": the time between ``opstart`` and ``opend``). It has no grid and
no cutoff, and it is counted in ``no_grid`` and in the cases. An operation too short for one cutoff has no cutoff and is not counted in ``no_grid``.

**Windows.** The windows are the newest complete seconds at the cutoff (``features.registry.complete_seconds``): the seconds ``T - 20`` to ``T - 1`` for the
waveform and ``T - 900`` to ``T - 1`` for the series. A second that the table does not hold counts as invalid.

**Series.** The reading of the per-second series for second ``s`` is valid only when the seconds ``s - 1`` and ``s`` are both valid
(``features.pressure_series``, the trailing 2-second average).

The count is the same as ``features.eligibility.warning_eligibility`` at each cutoff; the fast tests judge a seeded random case both ways.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from math import isfinite

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from spark.profile.grid_rule import FIRST_CUTOFF_OFFSET_SECONDS, GRID_STEP_SECONDS, LAST_CUTOFF_MARGIN_SECONDS


@dataclass(frozen=True, slots=True)
class Unscored:
    """The number of cutoffs, and of those the cutoffs that fail the waveform limit, the numeric limit and either, and whether the case has no grid (1 or 0)."""

    cutoffs: int
    waveform: int
    numeric: int
    either: int
    no_grid: int


NO_GRID = Unscored(0, 0, 0, 0, 1)


def whole_second(value: float | None) -> int | None:
    """The time as a whole second, or ``None`` for no time, a time that is not a number or a time that is not a whole second."""
    if value is None or not isfinite(value) or value != int(value):
        return None
    return int(value)


def grid_cutoffs(opstart: int, opend: int) -> range:
    """The cutoffs of a case: from ``opstart`` plus the first offset, in steps, through the latest at or before ``opend`` minus the margin."""
    return range(opstart + FIRST_CUTOFF_OFFSET_SECONDS, opend - LAST_CUTOFF_MARGIN_SECONDS + 1, GRID_STEP_SECONDS)


def within(held: list[int], low: int, high: int) -> int:
    """How many of the sorted whole numbers ``held`` lie in ``low`` to ``high`` inclusive."""
    return bisect_right(held, high) - bisect_left(held, low)


def count_unscored(valid_seconds: Iterable[int], opstart: float | None, opend: float | None) -> Unscored:
    """Count the cutoffs of a case and those that break a limit, from its valid seconds. A case with no grid gives ``NO_GRID``."""
    start, end = whole_second(opstart), whole_second(opend)
    if start is None or end is None or end <= start:
        return NO_GRID
    valid = sorted(set(valid_seconds))
    held = set(valid)
    readings = [second for second in valid if second - 1 in held]
    cutoffs = waveform = numeric = either = 0
    for cutoff in grid_cutoffs(start, end):
        cutoffs += 1
        short_waveform = WAVEFORM_WINDOW_SECONDS - within(valid, cutoff - WAVEFORM_WINDOW_SECONDS, cutoff - 1) > MAX_INVALID_WAVEFORM_SECONDS
        invalid = NUMERIC_WINDOW_SECONDS - within(readings, cutoff - NUMERIC_WINDOW_SECONDS, cutoff - 1)
        over = invalid * 100 > MAX_INVALID_NUMERIC_PERCENT * NUMERIC_WINDOW_SECONDS
        waveform += short_waveform
        numeric += over
        either += short_waveform or over
    return Unscored(cutoffs, waveform, numeric, either, 0)
