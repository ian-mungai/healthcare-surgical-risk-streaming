"""The checks of the Silver signal tables, on counts that the Spark job reads from the tables. No Spark runs here.

``bronze_problems`` judges the input before anything is written: a key that is missing, a waveform second twice, a reading whose positions among those
at its time are not 0 to n - 1 once each (the position is Bronze's ``source_occurrence``, and the first is ``FIRST_OCCURRENCE``) and a signal whose
track Bronze does not hold. The other two judge a Silver table that was written (or that a branch holds): the rows and the ledger add up to Bronze,
each key is once, the kept reading is the first of its time, each kept reading, ledger and stored second is the Bronze row that it came from
(``source_differs``, found by key against Bronze at the same hash, with the physical samples made again from the raw ones), and each verdict is
complete and is the one that the single definition in ``features.signal_validity`` gives for the stored samples. A check returns its problems and
never raises, so that one run names every cause. The test set is sealed and Bronze holds the held-out cases' signals, so a problem is a code and a flag
(``code: failed``) and never a count: a count of rows, readings, seconds or keys changes with a held-out row. A problem holds no case, track, time or value."""

from __future__ import annotations

from dataclasses import dataclass

FLAG = "failed"
CHECK_NAMES = (
    "numeric_null_key",
    "numeric_occurrence_gap",
    "numeric_without_track",
    "waveform_null_key",
    "waveform_duplicate_second",
    "waveform_without_track",
    "track_duplicate",
    "input_empty",
    "numeric_not_every_reading",
    "numeric_duplicate_key",
    "numeric_kept_not_first",
    "numeric_verdict_null",
    "numeric_valid_with_cause",
    "numeric_invalid_without_cause",
    "numeric_unknown_cause",
    "numeric_verdict_differs",
    "numeric_source_differs",
    "waveform_not_every_second",
    "waveform_duplicate_key",
    "waveform_shape",
    "waveform_verdict_null",
    "waveform_valid_with_cause",
    "waveform_invalid_without_cause",
    "waveform_unknown_cause",
    "waveform_verdict_differs",
    "waveform_source_differs",
)
JOB_CODES = frozenset(
    {
        "schema",
        "table_missing",
        "readback_differs",
        "branch_moved",
        "catalog_hash",
        "source_table",
        "source_no_snapshot",
        "source_changed",
    }
)
CODES = frozenset(CHECK_NAMES) | JOB_CODES


class SilverSignalsError(ValueError):
    """The tables cannot be built or a check failed. ``problems`` holds one message for each cause, each a code and a flag."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass(frozen=True, slots=True)
class BronzeFacts:
    """What a read of the Bronze signal tables at one hash shows. A count is of keys or rows where it says so. ``input_empty`` is a flag (0 or 1): neither
    signal table has a row, as Great Expectations' row-count expectation judges it. No count of rows is kept."""

    input_empty: int
    numeric_null_keys: int
    numeric_occurrence_gaps: int
    numeric_without_track: int
    waveform_null_keys: int
    waveform_duplicate_seconds: int
    waveform_without_track: int
    track_duplicates: int


@dataclass(frozen=True, slots=True)
class NumericFacts:
    """What a read of ``silver.numeric_observations`` shows.

    ``verdict_differs`` counts rows whose stored verdict is not the one that ``numeric_verdict`` gives for the stored value. ``row_count_differs`` is a flag
    (0 or 1): the rows and the dropped duplicates that they list do not add up to the Bronze rows, as Great Expectations' row-count expectation judges it.
    The counts are tested inside the job and never leave it.
    """

    duplicate_keys: int
    not_first: int
    null_verdict: int
    valid_with_cause: int
    invalid_without_cause: int
    unknown_cause: int
    verdict_differs: int
    source_differs: int = 0
    row_count_differs: int = 0


@dataclass(frozen=True, slots=True)
class WaveformFacts:
    """What a read of ``silver.waveform_seconds`` shows. ``shape_differs`` counts rows whose ``sample_count`` is not the number of their samples.
    ``row_count_differs`` is a flag (0 or 1): the rows are not the Bronze rows in number, as Great Expectations' row-count expectation judges it."""

    duplicate_keys: int
    null_verdict: int
    valid_with_cause: int
    invalid_without_cause: int
    unknown_cause: int
    shape_differs: int
    verdict_differs: int
    source_differs: int = 0
    row_count_differs: int = 0


def flagged(pairs: list[tuple[str, int]]) -> list[str]:
    """One flag for each failed check: the count is only tested for being positive and is never written."""
    return [f"{code}: {FLAG}" for code, count in pairs if count]


def bronze_problems(facts: BronzeFacts) -> list[str]:
    if facts.input_empty:
        return [f"input_empty: {FLAG}"]
    return flagged(
        [
            ("numeric_null_key", facts.numeric_null_keys),
            ("numeric_occurrence_gap", facts.numeric_occurrence_gaps),
            ("numeric_without_track", facts.numeric_without_track),
            ("waveform_null_key", facts.waveform_null_keys),
            ("waveform_duplicate_second", facts.waveform_duplicate_seconds),
            ("waveform_without_track", facts.waveform_without_track),
            ("track_duplicate", facts.track_duplicates),
        ]
    )


def numeric_problems(facts: NumericFacts) -> list[str]:
    return flagged(
        [
            ("numeric_not_every_reading", facts.row_count_differs),
            ("numeric_duplicate_key", facts.duplicate_keys),
            ("numeric_kept_not_first", facts.not_first),
            ("numeric_verdict_null", facts.null_verdict),
            ("numeric_valid_with_cause", facts.valid_with_cause),
            ("numeric_invalid_without_cause", facts.invalid_without_cause),
            ("numeric_unknown_cause", facts.unknown_cause),
            ("numeric_verdict_differs", facts.verdict_differs),
            ("numeric_source_differs", facts.source_differs),
        ]
    )


def waveform_problems(facts: WaveformFacts) -> list[str]:
    return flagged(
        [
            ("waveform_not_every_second", facts.row_count_differs),
            ("waveform_duplicate_key", facts.duplicate_keys),
            ("waveform_shape", facts.shape_differs),
            ("waveform_verdict_null", facts.null_verdict),
            ("waveform_valid_with_cause", facts.valid_with_cause),
            ("waveform_invalid_without_cause", facts.invalid_without_cause),
            ("waveform_unknown_cause", facts.unknown_cause),
            ("waveform_verdict_differs", facts.verdict_differs),
            ("waveform_source_differs", facts.source_differs),
        ]
    )
