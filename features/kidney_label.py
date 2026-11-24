"""The postoperative kidney injury label from creatinine results (analysis plan, "Kidney injury after surgery"; analysis decisions 6 and 7).

**Times.** A result has the time of its specimen in seconds from the case start (the laboratory table's ``dt``), as ``anestart`` and ``opend`` have.

**Baseline.** The last dated result before ``anestart`` (strictly before it) that is at most 30 days old, the age being ``anestart`` minus its time. The
undated ``preop_cr`` is not an input of this module. Two results at the last time give the first by occurrence number (the lower value), because the
laboratory table numbers the rows of one case, time and test by their value.

**Follow-up.** A result after ``opend`` (strictly) through 7 days after it (inclusive). The origin is ``opend`` (a project choice).

**Eligibility.** The primary label is unknown, first, for a case with no qualifying dated baseline or no follow-up result. The reason says which of the two, or
both. An unknown label holds no rise.

**Rule.** For an eligible case the label is positive when either prespecified rise is observed and negative otherwise: (1) an absolute rise of at least
0.3 mg/dL between two results at most 48 hours apart, the later one being within the first 48 hours after ``opend`` (inclusive), the earlier one any
result of the case that is before it, or (2) a follow-up result of at least 1.5 times the baseline. A rise whose later result is outside its window does not
make a positive. Both tests are made with exact decimals, never floats: 1.2 minus 0.9 is not 0.3 in a float.

**What is reported.** The reported earlier and later result of the absolute rise are those of the first later result in time order (then by occurrence) that
has a rise, with the earlier result of the lowest value (then the latest, then the lowest occurrence); the reported result of the relative rise is the first
follow-up result in time order (then by occurrence) that reaches 1.5 times the baseline. ``latest_time_s`` is the latest time of any result the rule read:
the baseline and the follow-up results; a result after day 7 is never read, so a label cannot change with a later result.

A result with no value, or a value that is not a positive finite number, is not a measurement. The module uses the standard library only and imports no Spark.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from features.label_values import NEGATIVE, POSITIVE, UNKNOWN

SECONDS_PER_HOUR = 3_600
SECONDS_PER_DAY = 86_400
BASELINE_DAYS = 30
ABSOLUTE_HOURS = 48
FOLLOWUP_DAYS = 7
ABSOLUTE_RISE_MG_DL = Decimal("0.3")
RELATIVE_RATIO = Decimal("1.5")
CREATININE = "cr"
ELIGIBLE = "eligible"
NO_BASELINE = "no_baseline"
NO_FOLLOWUP = "no_followup_result"
NEITHER = "no_baseline_and_no_followup_result"
REASONS = (ELIGIBLE, NO_BASELINE, NO_FOLLOWUP, NEITHER)


@dataclass(frozen=True, slots=True)
class Result:
    """One creatinine result: its time in seconds from the case start, its number among the results of that case, time and test, and its value in mg/dL."""

    time_s: int
    occurrence: int
    value: float


@dataclass(frozen=True, slots=True)
class KidneyLabel:
    label: str
    eligible: bool
    reason: str
    baseline: Result | None
    baseline_window_start_s: int | None
    baseline_window_end_s: int | None
    followup_start_s: int | None
    absolute_window_end_s: int | None
    relative_window_end_s: int | None
    early_results: int
    late_results: int
    absolute_rise: bool | None
    earlier: Result | None
    later: Result | None
    relative_rise: bool | None
    relative_result: Result | None
    latest_time_s: int | None


def exact(value: float) -> Decimal:
    """The decimal that a float prints as: the published value, with no binary error."""
    return Decimal(repr(value))


def is_measurement(result: Result) -> bool:
    return math.isfinite(result.value) and result.value > 0


def order(result: Result) -> tuple[int, int]:
    return result.time_s, result.occurrence


def baseline_of(results: list[Result], start: int, end: int) -> Result | None:
    held = [result for result in results if start <= result.time_s < end]
    if not held:
        return None
    last = max(result.time_s for result in held)
    return min((result for result in held if result.time_s == last), key=order)


def absolute_pair(results: list[Result], opend: int) -> tuple[Result, Result] | None:
    for later in sorted((result for result in results if opend < result.time_s <= opend + ABSOLUTE_HOURS * SECONDS_PER_HOUR), key=order):
        near = [
            earlier
            for earlier in results
            if 0 < later.time_s - earlier.time_s <= ABSOLUTE_HOURS * SECONDS_PER_HOUR and exact(later.value) - exact(earlier.value) >= ABSOLUTE_RISE_MG_DL
        ]
        if near:
            return min(near, key=lambda earlier: (exact(earlier.value), -earlier.time_s, earlier.occurrence)), later
    return None


def relative_result(followup: list[Result], baseline: Result) -> Result | None:
    reaching = [result for result in sorted(followup, key=order) if exact(result.value) >= RELATIVE_RATIO * exact(baseline.value)]
    return reaching[0] if reaching else None


def unknown_reason(baseline: Result | None, followup: list[Result]) -> str:
    if baseline is None:
        return NEITHER if not followup else NO_BASELINE
    return NO_FOLLOWUP


def kidney_label(anestart: int | None, opend: int | None, results: Iterable[Result]) -> KidneyLabel:
    """The label of one case from its start of anaesthesia, its end of operation and every creatinine result of the case (any time)."""
    held = [result for result in results if is_measurement(result)]
    baseline_start = None if anestart is None else anestart - BASELINE_DAYS * SECONDS_PER_DAY
    absolute_end = None if opend is None else opend + ABSOLUTE_HOURS * SECONDS_PER_HOUR
    relative_end = None if opend is None else opend + FOLLOWUP_DAYS * SECONDS_PER_DAY
    baseline = None if anestart is None or baseline_start is None else baseline_of(held, baseline_start, anestart)
    followup = [] if opend is None or relative_end is None else [result for result in held if opend < result.time_s <= relative_end]
    early = 0 if absolute_end is None else sum(1 for result in followup if result.time_s <= absolute_end)
    used = [*followup, *([baseline] if baseline else [])]
    latest = max((result.time_s for result in used), default=None)
    windows = (baseline_start, anestart, opend, absolute_end, relative_end, early, len(followup) - early)
    if baseline is None or not followup:
        return KidneyLabel(UNKNOWN, False, unknown_reason(baseline, followup), baseline, *windows, None, None, None, None, None, latest)
    pair = absolute_pair(held, opend) if opend is not None else None
    reaching = relative_result(followup, baseline)
    positive = pair is not None or reaching is not None
    earlier, later = pair if pair else (None, None)
    return KidneyLabel(
        POSITIVE if positive else NEGATIVE, True, ELIGIBLE, baseline, *windows, pair is not None, earlier, later, reaching is not None, reaching, latest
    )
