"""The hypotension event rule on the per-second arterial series (``features.pressure_series``): the one definition that Silver and any later path call.

**Rule** (analysis plan, rows 2 and 4, project standard "Analysis decisions and their evidence"). An event is 60 consecutive valid seconds of the series,
each below 65 mmHg (``EVENT_THRESHOLD_MMHG``; a value of exactly 65 is not low), inside the operation. A second of the series that is invalid, a second the
series does not hold and a second at or above 65 each end a run: nothing bridges a gap. The validity of a second is the trailing rule of
``features.signal_validity``, and the follow-up seconds that confirm an onset are judged by the same rule as every other second, because they are read from the
same series.

**Operation.** A second ``s`` is inside the operation when ``opstart <= s`` and ``s + 1 <= opend`` (both in seconds from the case start, as the clinical table
publishes them): only a complete second of the operation counts. A run is cut at either end of the operation.

**Times.** A reading of second ``s`` is known at the end of that second, ``(s + 1) * 1_000_000`` microseconds (``features.registry``). The onset of an event is
the time of its first low reading, ``onset_us``. The event is confirmed at the time of its 60th reading, ``confirmed_us``. A label that reads an event at a
cutoff may use ``onset_us`` for where the onset is, and ``confirmed_us`` for what had to be observed to establish it, which can be after the cutoff.

**Runs that are not events.** ``low_runs`` gives every run of consecutive valid low seconds with the reason that ended it: ``recovered`` (the next second is
valid and not low), ``invalid`` (the next second is invalid), ``missing`` (the series holds no next second inside the operation) or ``operation_end`` (the next
second is outside the operation). A run of fewer than 60 seconds that ended by ``invalid``, ``missing`` or ``operation_end`` cannot be told from the start of an
event by the observations, so a label must not call its stretch negative; one that ended by ``recovered`` is established as no event.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass

EVENT_SECONDS = 60
EVENT_THRESHOLD_MMHG = 65.0
SECOND_US = 1_000_000
END_RECOVERED = "recovered"
END_INVALID = "invalid"
END_MISSING = "missing"
END_OPERATION = "operation_end"
END_REASONS = (END_RECOVERED, END_INVALID, END_MISSING, END_OPERATION)


@dataclass(frozen=True, slots=True)
class SeriesPoint:
    """One second of the series: the value (``None`` when invalid) and its validity."""

    second: int
    value: float | None
    valid: bool


@dataclass(frozen=True, slots=True)
class LowRun:
    """Consecutive valid low seconds ``first_second`` to ``last_second``, and why the run ended."""

    first_second: int
    last_second: int
    end_reason: str

    @property
    def seconds(self) -> int:
        return self.last_second - self.first_second + 1

    @property
    def is_event(self) -> bool:
        return self.seconds >= EVENT_SECONDS


@dataclass(frozen=True, slots=True)
class HypotensionEvent:
    """A run of at least 60 seconds with the times it used."""

    onset_second: int
    onset_us: int
    confirmed_us: int
    last_low_second: int
    end_us: int
    duration_seconds: int
    end_reason: str


def is_low(point: SeriesPoint) -> bool:
    if not point.valid:
        return False
    if point.value is None or not math.isfinite(point.value):
        raise ValueError(f"second {point.second} is valid and has no finite value")
    return point.value < EVENT_THRESHOLD_MMHG


def inside(second: int, opstart: float, opend: float) -> bool:
    return opstart <= second and second + 1 <= opend


def low_runs(points: Iterable[SeriesPoint], opstart: float, opend: float) -> list[LowRun]:
    """Every run of consecutive valid low seconds inside the operation, oldest first, each with the reason that ended it."""
    if not (math.isfinite(opstart) and math.isfinite(opend)) or opstart > opend:
        raise ValueError("the operation start and end must be finite numbers with the start not after the end")
    held: dict[int, SeriesPoint] = {}
    for point in points:
        if point.second in held:
            raise ValueError(f"second {point.second} is given twice")
        held[point.second] = point
    return collect(held, opstart, opend)


def collect(held: dict[int, SeriesPoint], opstart: float, opend: float) -> list[LowRun]:
    runs: list[LowRun] = []
    first: int | None = None
    last = 0
    for second in sorted(held):
        low = inside(second, opstart, opend) and is_low(held[second])
        if low and first is not None and second == last + 1:
            last = second
            continue
        if first is not None:
            runs.append(LowRun(first, last, why_it_ended(held, last, opstart, opend)))
            first = None
        if low:
            first, last = second, second
    if first is not None:
        runs.append(LowRun(first, last, why_it_ended(held, last, opstart, opend)))
    return runs


def why_it_ended(held: dict[int, SeriesPoint], last: int, opstart: float, opend: float) -> str:
    following = last + 1
    if not inside(following, opstart, opend):
        return END_OPERATION
    point = held.get(following)
    if point is None:
        return END_MISSING
    return END_INVALID if not point.valid else END_RECOVERED


def events_of(points: Iterable[SeriesPoint], opstart: float, opend: float) -> list[HypotensionEvent]:
    """The events of one case: the runs of at least 60 seconds, with the times they used."""
    return [
        HypotensionEvent(
            run.first_second,
            (run.first_second + 1) * SECOND_US,
            (run.first_second + EVENT_SECONDS) * SECOND_US,
            run.last_second,
            (run.last_second + 1) * SECOND_US,
            run.seconds,
            run.end_reason,
        )
        for run in low_runs(points, opstart, opend)
        if run.is_event
    ]
