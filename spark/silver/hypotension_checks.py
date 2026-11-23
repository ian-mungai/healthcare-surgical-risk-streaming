"""The checks of the pressure series and the event table, on counts that the Spark job reads from the tables. No Spark runs here.

``input_problems`` judges the input before anything is written, in this order, each step alone when it fails: a Bronze case with more than one row (an
ambiguous owner), a patient split that fails the split job's own rule check (``split_rule_problems``: a set is trusted only after it), a case with no split row,
then no arterial second at all, and a waveform second without a full key or twice. The other two judge a table that was written (or that a branch holds). The
series has one row for each arterial second of ``silver.waveform_seconds``, each key once, a complete verdict, a mean only for a valid reading, and each reading
is the one that ``features.pressure_series`` gives from the stored seconds. The events are each at least 60 seconds with the times that their onset and
duration give, are the events that ``features.hypotension`` gives from the stored series and the operation times of the development cases, and none is of a
case outside the development sets.

The events are outcomes and the test set is sealed. Every check about events returns a flag, ``code: failed``, and never a count; a check that reads the
patients outside the development sets is one flag too. A check returns its problems and never raises, so that one run names every cause. A problem is a code
with a count (only for the signal tables, which are inputs) or a flag, and holds no case, track, time, value or set.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from spark.silver.checks import counted
from spark.split_table import SET_NAMES, SplitRow, table_problems

DEVELOPMENT_SETS = (SET_NAMES["train"], SET_NAMES["validation"])
FLAG = "failed"

CHECK_NAMES = (
    "ambiguous_case_owner",
    "case_without_split",
    "input_empty",
    "waveform_null_key",
    "waveform_duplicate_second",
    "series_not_every_second",
    "series_duplicate_key",
    "series_verdict_null",
    "series_valid_with_cause",
    "series_invalid_without_cause",
    "series_unknown_cause",
    "series_value",
    "series_differs",
    "event_duplicate_key",
    "event_short",
    "event_times",
    "event_differs",
    "event_held_out",
)
JOB_CODES = frozenset({"schema", "table_missing", "readback_differs", "branch_moved", "catalog_hash", "source_table", "source_no_snapshot", "source_changed"})
CODES = frozenset(CHECK_NAMES) | JOB_CODES


@dataclass(frozen=True, slots=True)
class InputFacts:
    """What a read of the input at one hash shows: the arterial seconds of ``silver.waveform_seconds``, the clinical table and the patient split.

    ``ambiguous_owner`` and ``cases_without_split`` are flags (0 or 1). ``split_problems`` are the problems of the split table by itself
    (``split_rule_problems``); when it is not empty the other facts were not read, because a set is not trusted yet.
    """

    art_seconds: int
    null_keys: int
    duplicate_seconds: int
    ambiguous_owner: int
    cases_without_split: int
    split_problems: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SeriesFacts:
    """What a read of the series table shows. ``value_problem`` counts valid rows with no value and invalid rows with one. ``differs`` counts rows that are not
    the reading that the single definition gives from the stored seconds. ``causes`` counts the rows of each cause and of ``valid``."""

    rows: int
    duplicate_keys: int
    null_verdict: int
    valid_with_cause: int
    invalid_without_cause: int
    unknown_cause: int
    value_problem: int
    differs: int
    causes: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EventFacts:
    """What a read of the event table shows, as flags (0 or 1, or any positive number that is only tested for being positive): ``duplicate_keys``, ``short`` (an
    event of fewer than 60 seconds), ``times_differ`` (times that are not those of the onset and duration), ``differs`` (not the recomputed events, in both
    directions) and ``held_out_events`` (an event of a case outside the development sets). ``cases_without_operation`` is a fact of the development cases and
    is not a defect; the result does not name it."""

    duplicate_keys: int
    short: int
    times_differ: int
    differs: int
    held_out_events: int
    cases_without_operation: int = 0


def split_rule_problems(rows: Sequence[SplitRow]) -> list[str]:
    """What is wrong with the patient split table by itself, by the split job's own checks. The set of a patient is trusted only when this is empty."""
    return table_problems(rows, pinned=False)


def input_problems(facts: InputFacts) -> list[str]:
    if facts.ambiguous_owner:
        return [f"ambiguous_case_owner: {FLAG}"]
    if facts.split_problems:
        return list(facts.split_problems)
    if facts.cases_without_split:
        return [f"case_without_split: {FLAG}"]
    if facts.art_seconds == 0:
        return ["input_empty: 0 rows"]
    return counted([("waveform_null_key", facts.null_keys, "row"), ("waveform_duplicate_second", facts.duplicate_seconds, "row")])


def series_problems(facts: SeriesFacts, art_seconds: int) -> list[str]:
    return counted(
        [
            ("series_not_every_second", abs(art_seconds - facts.rows), "second"),
            ("series_duplicate_key", facts.duplicate_keys, "key"),
            ("series_verdict_null", facts.null_verdict, "row"),
            ("series_valid_with_cause", facts.valid_with_cause, "row"),
            ("series_invalid_without_cause", facts.invalid_without_cause, "row"),
            ("series_unknown_cause", facts.unknown_cause, "row"),
            ("series_value", facts.value_problem, "row"),
            ("series_differs", facts.differs, "row"),
        ]
    )


def flagged(pairs: list[tuple[str, int]]) -> list[str]:
    """One flag for each failed check: the count is only tested for being positive and is never written."""
    return [f"{code}: {FLAG}" for code, count in pairs if count]


def event_problems(facts: EventFacts) -> list[str]:
    return flagged(
        [
            ("event_duplicate_key", facts.duplicate_keys),
            ("event_short", facts.short),
            ("event_times", facts.times_differ),
            ("event_differs", facts.differs),
            ("event_held_out", facts.held_out_events),
        ]
    )
