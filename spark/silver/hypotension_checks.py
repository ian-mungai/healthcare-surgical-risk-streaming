"""The checks of the pressure series and the event table, on counts that the Spark job reads from the tables. No Spark runs here.

``input_problems`` judges the input before anything is written: no arterial second at all, a waveform second without a full key or twice, a case twice in the
clinical table. The other two judge a table that was written (or that a branch holds): the series has one row for each arterial second of
``silver.waveform_seconds``, each key once, a complete verdict, a value only for a valid reading, and each reading is the one that ``features.pressure_series``
gives from the stored seconds; the events are each at least 60 seconds with the times their onset and duration give, and are the events that
``features.hypotension`` gives from the stored series and the operation times. A check returns its problems and never raises, so that one run names every
cause. A problem is a code and a count, and holds no case, track, time or value.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from spark.silver.checks import counted

CHECK_NAMES = (
    "input_empty",
    "waveform_null_key",
    "waveform_duplicate_second",
    "clinical_duplicate_case",
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
)
JOB_CODES = frozenset({"schema", "table_missing", "readback_differs", "branch_moved", "catalog_hash", "source_table", "source_no_snapshot", "source_changed"})
CODES = frozenset(CHECK_NAMES) | JOB_CODES


@dataclass(frozen=True, slots=True)
class InputFacts:
    """What a read of the input at one hash shows: the arterial seconds of ``silver.waveform_seconds`` and the clinical table."""

    art_seconds: int
    null_keys: int
    duplicate_seconds: int
    clinical_duplicate_cases: int


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
    """What a read of the event table shows. ``times_differ`` counts events whose times are not those of their onset and duration, ``differs`` the events that
    are not the recomputed ones, in both directions. ``cases_without_operation`` counts the cases with arterial seconds and no usable operation times; it is not
    a defect, and the result does not name it."""

    duplicate_keys: int
    short: int
    times_differ: int
    differs: int
    cases_without_operation: int = 0


def input_problems(facts: InputFacts) -> list[str]:
    if facts.art_seconds == 0:
        return ["input_empty: 0 rows"]
    return counted(
        [
            ("waveform_null_key", facts.null_keys, "row"),
            ("waveform_duplicate_second", facts.duplicate_seconds, "row"),
            ("clinical_duplicate_case", facts.clinical_duplicate_cases, "case"),
        ]
    )


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


def event_problems(facts: EventFacts) -> list[str]:
    return counted(
        [
            ("event_duplicate_key", facts.duplicate_keys, "key"),
            ("event_short", facts.short, "event"),
            ("event_times", facts.times_differ, "event"),
            ("event_differs", facts.differs, "event"),
        ]
    )
