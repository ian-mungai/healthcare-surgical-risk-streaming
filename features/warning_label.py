"""The label of a sample of the blood pressure warning for one horizon, and its unknown rule, on the runs of ``features.hypotension`` (analysis plan,
"Blood pressure drop", items 7 to 9).

A sample is a cutoff (microseconds from the case start) and a horizon (seconds). Its window is every onset time after the cutoff through the end of the horizon:
``cutoff < onset_us <= cutoff + horizon * 1_000_000``, with no lead gap. The onset time of an event is ``(first second + 1) * 1_000_000``
(``features.hypotension``), so a run that began at or before the cutoff is not an onset in the window.

**Positive.** An event (a run of 60 valid low seconds inside the operation) has its onset in the window. Its 60 confirming seconds may end after the horizon.

**Unknown.** Otherwise the observations cannot establish a negative when either holds, and the reason names the first:

- ``open_run``: a run of fewer than 60 seconds with its onset in the window ended by an invalid second, a second the series lacks or the end of the operation
  (``features.hypotension.low_runs``): by the observations it cannot be told from the start of an event.
- ``uncovered``: some second of the window is a candidate onset (the second before it is not low) that is not established as no onset. A candidate is
  established as no onset when a valid second inside the operation at or above 65 mmHg lies in it or in the 59 seconds after it. A candidate with no such
  second is a lost signal, a second the series lacks, or the end of the case before the horizon.

**Negative.** Every run that has its onset in the window ended by a valid second at or above 65 mmHg, and every candidate is established as no onset
(``covered``).

The module uses the standard library and ``features.hypotension`` only and imports no Spark.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from features.hypotension import END_RECOVERED, EVENT_SECONDS, SECOND_US, SeriesPoint, inside, is_low, low_runs
from features.label_values import NEGATIVE, POSITIVE, UNKNOWN

EVENT = "event"
OPEN_RUN = "open_run"
UNCOVERED = "uncovered"
COVERED = "covered"
REASONS = (EVENT, OPEN_RUN, UNCOVERED, COVERED)
LOW, HIGH, UNOBSERVED = "low", "high", "unobserved"


@dataclass(frozen=True, slots=True)
class WarningLabel:
    """The label of one sample, why, and the onset time of the first event in its window (``None`` unless the label is positive)."""

    label: str
    reason: str
    onset_us: int | None


def state_of(points: dict[int, SeriesPoint], second: int, opstart: float, opend: float) -> str:
    point = points.get(second)
    if point is None or not point.valid or not inside(second, opstart, opend):
        return UNOBSERVED
    return LOW if is_low(point) else HIGH


def candidates_unestablished(points: dict[int, SeriesPoint], first: int, last: int, opstart: float, opend: float) -> bool:
    """Whether a candidate onset second in ``first`` through ``last`` has no valid second at or above 65 in it or the 59 seconds after it."""
    state = {second: state_of(points, second, opstart, opend) for second in range(first - 1, last + EVENT_SECONDS)}
    high_at = [second for second, value in state.items() if value == HIGH]
    for second in range(first, last + 1):
        if state[second - 1] == LOW:
            continue
        if not any(second <= other < second + EVENT_SECONDS for other in high_at):
            return True
    return False


def warning_label(points: Iterable[SeriesPoint], opstart: float, opend: float, cutoff_us: int, horizon_seconds: int) -> WarningLabel:
    """The label of the sample at ``cutoff_us`` for the horizon of ``horizon_seconds`` from the series of one case and its operation times."""
    if horizon_seconds <= 0:
        raise ValueError("the horizon must be a positive number of seconds")
    held = list(points)
    end_us = cutoff_us + horizon_seconds * SECOND_US
    runs = [run for run in low_runs(held, opstart, opend) if cutoff_us < (run.first_second + 1) * SECOND_US <= end_us]
    events = [run for run in runs if run.is_event]
    if events:
        return WarningLabel(POSITIVE, EVENT, (events[0].first_second + 1) * SECOND_US)
    if any(run.end_reason != END_RECOVERED for run in runs):
        return WarningLabel(UNKNOWN, OPEN_RUN, None)
    first, last = cutoff_us // SECOND_US, end_us // SECOND_US - 1
    if candidates_unestablished({point.second: point for point in held}, first, last, opstart, opend):
        return WarningLabel(UNKNOWN, UNCOVERED, None)
    return WarningLabel(NEGATIVE, COVERED, None)
