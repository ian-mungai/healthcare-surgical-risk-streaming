"""The checks of the signal profile tables, on counts that the Spark job reads from the tables. No Spark runs here.

A check returns its problems and never raises, so that one run names every cause. A problem is a code and a count and holds no case, track, time or value.
``profile_problems`` reconciles ``signal_profile`` with the development rows of the Silver signal tables, counted by a route that does not use the plan's join
(the development case numbers as a list). ``unscored_problems`` judges the one row of ``unscored_share`` by itself and against the development cases that
hold the arterial waveform. ``counts_problems`` judges ``source_counts``: the names are the prespecified ones, a sealed count holds no value, every count is
judged on a pinned run and none on another.
"""

from __future__ import annotations

from dataclasses import dataclass

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from spark.profile.counts import PRESPECIFIED
from spark.profile.grid_rule import FIRST_CUTOFF_OFFSET_SECONDS, GRID_STEP_SECONDS, LAST_CUTOFF_MARGIN_SECONDS

CHECK_NAMES = (
    "input_empty",
    "profile_empty",
    "profile_duplicate_key",
    "profile_partition",
    "profile_share",
    "profile_kind",
    "profile_rows_differ",
    "profile_tracks_differ",
    "unscored_rows",
    "unscored_order",
    "unscored_share",
    "unscored_rule",
    "unscored_cases",
    "unscored_no_grid",
    "counts_rows",
    "counts_names",
    "counts_sealed_value",
    "counts_expected",
    "counts_not_judged",
    "counts_judged_unpinned",
)
JOB_CODES = frozenset(
    {
        "schema",
        "table_schema",
        "table_missing",
        "readback_differs",
        "plan_differs",
        "branch_moved",
        "catalog_hash",
        "source_table",
        "source_no_snapshot",
        "source_changed",
        "development_empty",
        "development_differs",
        "source_row",
        "case_two_patients",
        "source_missing",
        "source_extra",
        "source_snapshot",
        "bad_subjectid",
        "table_row",
        "two_sets",
        "repeated_patient",
        "unknown_set",
        "rule_mismatch",
        "rule_version",
        "mixed_snapshots",
        "empty_table",
        "shares",
        "pinned_counts",
        "grid_rule_rows",
        "grid_rule_changed",
    }
)
CODES = frozenset(CHECK_NAMES) | JOB_CODES
FLOAT_TOLERANCE = 1e-12


def counted(pairs: list[tuple[str, int, str]]) -> list[str]:
    """One problem for each check that failed, with its count. The profile reads development rows only, so its counts hold no test-set outcome."""
    return [f"{code}: {_unit(count, noun)}" for code, count, noun in pairs if count]


class SignalProfileError(ValueError):
    """The tables cannot be built or a check failed. ``problems`` holds one message for each cause, each a code and a count."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


@dataclass(frozen=True, slots=True)
class InputFacts:
    """The development rows of the Silver signal tables, counted from the list of development case numbers: the readings, the seconds, the distinct
    (kind, track) pairs and the cases that hold the arterial waveform."""

    numeric_rows: int
    waveform_rows: int
    tracks: int
    arterial_cases: int


@dataclass(frozen=True, slots=True)
class ProfileFacts:
    """What a read of ``signal_profile`` shows. ``partition_differs`` counts rows whose valid rows and cause columns do not add up to the rows,
    ``share_differs`` rows whose share is not valid rows over rows and ``unknown_kind`` rows of a kind that is neither numeric nor waveform."""

    rows: int
    duplicate_keys: int
    partition_differs: int
    share_differs: int
    unknown_kind: int
    numeric_rows: int
    waveform_rows: int
    tracks: int


@dataclass(frozen=True, slots=True)
class UnscoredFacts:
    """What a read of ``unscored_share`` shows: the number of rows, the first row's counts and share, and the rule columns of that row."""

    rows: int
    cutoffs: int
    waveform: int
    numeric: int
    either: int
    share: float | None
    cases: int
    no_grid: int
    cutoff_step: int
    cutoff_offset: int
    last_margin: int
    waveform_window: int
    waveform_limit: int
    numeric_window: int
    numeric_limit: int


@dataclass(frozen=True, slots=True)
class CountsFacts:
    """What a read of ``source_counts`` shows. ``names_differ`` counts the prespecified names that are not there and the rows that are not prespecified,
    ``sealed_with_value`` the full-cohort rows that keep a value, ``expected_differs`` the rows whose expected value is not the prespecified one,
    ``not_judged`` the rows with no flag and ``judged_unpinned`` the rows with a flag."""

    rows: int
    names_differ: int
    sealed_with_value: int
    expected_differs: int
    not_judged: int
    judged_unpinned: int


def profile_problems(facts: ProfileFacts, source: InputFacts) -> list[str]:
    if source.numeric_rows == 0 and source.waveform_rows == 0:
        return [f"input_empty: {_unit(0, 'row')}"]
    row_difference = abs(facts.numeric_rows - source.numeric_rows) + abs(facts.waveform_rows - source.waveform_rows)
    return counted(
        [
            ("profile_empty", 1 if facts.rows == 0 else 0, "table"),
            ("profile_duplicate_key", facts.duplicate_keys, "key"),
            ("profile_partition", facts.partition_differs, "row"),
            ("profile_share", facts.share_differs, "row"),
            ("profile_kind", facts.unknown_kind, "row"),
            ("profile_rows_differ", row_difference, "row"),
            ("profile_tracks_differ", abs(facts.tracks - source.tracks), "track"),
        ]
    )


def unscored_problems(facts: UnscoredFacts, source: InputFacts) -> list[str]:
    ordered = max(facts.waveform, facts.numeric) <= facts.either <= min(facts.waveform + facts.numeric, facts.cutoffs)
    expected_share = facts.either / facts.cutoffs if facts.cutoffs else None
    share_ok = (facts.share is None) if expected_share is None else (facts.share is not None and abs(facts.share - expected_share) <= FLOAT_TOLERANCE)
    rule = (facts.cutoff_step, facts.cutoff_offset, facts.last_margin, facts.waveform_window, facts.waveform_limit, facts.numeric_window, facts.numeric_limit)
    wanted = (
        GRID_STEP_SECONDS,
        FIRST_CUTOFF_OFFSET_SECONDS,
        LAST_CUTOFF_MARGIN_SECONDS,
        WAVEFORM_WINDOW_SECONDS,
        MAX_INVALID_WAVEFORM_SECONDS,
        NUMERIC_WINDOW_SECONDS,
        MAX_INVALID_NUMERIC_PERCENT,
    )
    rule_ok = rule == wanted
    return counted(
        [
            ("unscored_rows", abs(facts.rows - 1), "row"),
            ("unscored_order", 0 if ordered else 1, "row"),
            ("unscored_share", 0 if share_ok else 1, "row"),
            ("unscored_rule", 0 if rule_ok else 1, "row"),
            ("unscored_cases", abs(facts.cases - source.arterial_cases), "case"),
            ("unscored_no_grid", 0 if 0 <= facts.no_grid <= facts.cases else 1, "row"),
        ]
    )


def counts_problems(facts: CountsFacts, *, pinned: bool) -> list[str]:
    return counted(
        [
            ("counts_rows", abs(facts.rows - len(PRESPECIFIED)), "row"),
            ("counts_names", facts.names_differ, "name"),
            ("counts_sealed_value", facts.sealed_with_value, "row"),
            ("counts_expected", facts.expected_differs, "row"),
            ("counts_not_judged", facts.not_judged if pinned else 0, "row"),
            ("counts_judged_unpinned", 0 if pinned else facts.judged_unpinned, "row"),
        ]
    )


def _unit(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"
