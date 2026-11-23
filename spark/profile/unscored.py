"""The unscored share of the signal profile, from the validity of the seconds of one arterial waveform. No Spark runs here.

Analysis decision 5 limits a sample: its 20-second waveform window tolerates no invalid second and its 15-minute window of the per-second arterial series
tolerates at most 10% invalid seconds (``features.registry``, ``features.eligibility``). Decision 3 takes one sample each minute. The profile measures the
share of those samples that the limits leave unscored, on development data.

**Grid.** A cutoff is a whole minute of case time, ``T = 60 * k`` seconds. It counts when its full 15-minute history lies inside the recorded span of the case's
arterial waveform: ``T - 900 >= first`` and ``T - 1 <= last``, where ``first`` and ``last`` are the first and the last second that the table holds for the
case, valid or not. A cutoff with less history is not a candidate (the stated result "no warning available" under "too little history" is a different
outcome, not an unscored sample). The grid does not look at the operation's start or end.

**Windows.** The windows are the newest complete seconds at the cutoff (``features.registry.complete_seconds``): the seconds ``T - 20`` to ``T - 1`` for the
waveform and ``T - 900`` to ``T - 1`` for the series. A second that the table does not hold counts as invalid.

**Series.** The reading of the per-second series for second ``s`` is valid only when the seconds ``s - 1`` and ``s`` are both valid
(``features.pressure_series``, the trailing 2-second average).

The count is the same as ``features.eligibility.warning_eligibility`` at each cutoff; the fast tests judge a seeded random case both ways.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS

CUTOFF_STEP_SECONDS = 60


@dataclass(frozen=True, slots=True)
class Unscored:
    """The number of cutoffs, and of those the cutoffs that fail the waveform limit, the numeric limit and either."""

    cutoffs: int
    waveform: int
    numeric: int
    either: int


def cutoffs_of(first: int, last: int) -> Iterator[int]:
    """The whole-minute cutoffs whose 15-minute history lies inside the recorded seconds ``first`` to ``last``."""
    start = -(-(first + NUMERIC_WINDOW_SECONDS) // CUTOFF_STEP_SECONDS) * CUTOFF_STEP_SECONDS
    stop = (last + 1) // CUTOFF_STEP_SECONDS * CUTOFF_STEP_SECONDS
    return iter(range(start, stop + 1, CUTOFF_STEP_SECONDS))


def within(held: list[int], low: int, high: int) -> int:
    """How many of the sorted whole numbers ``held`` lie in ``low`` to ``high`` inclusive."""
    return bisect_right(held, high) - bisect_left(held, low)


def count_unscored(valid_seconds: Iterable[int], first: int, last: int) -> Unscored:
    """Count the cutoffs of the grid and those that break a limit, from the valid seconds of a case."""
    valid = sorted(set(valid_seconds))
    held = set(valid)
    readings = [second for second in valid if second - 1 in held]
    cutoffs = waveform = numeric = either = 0
    for cutoff in cutoffs_of(first, last):
        cutoffs += 1
        short_waveform = WAVEFORM_WINDOW_SECONDS - within(valid, cutoff - WAVEFORM_WINDOW_SECONDS, cutoff - 1) > MAX_INVALID_WAVEFORM_SECONDS
        invalid = NUMERIC_WINDOW_SECONDS - within(readings, cutoff - NUMERIC_WINDOW_SECONDS, cutoff - 1)
        over = invalid * 100 > MAX_INVALID_NUMERIC_PERCENT * NUMERIC_WINDOW_SECONDS
        waveform += short_waveform
        numeric += over
        either += short_waveform or over
    return Unscored(cutoffs, waveform, numeric, either)


def share_of(counts: Unscored) -> float | None:
    """The unscored share of the cutoffs, or ``None`` when there is no cutoff."""
    return counts.either / counts.cutoffs if counts.cutoffs else None
