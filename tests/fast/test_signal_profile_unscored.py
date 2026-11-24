"""The unscored share of the signal profile: the count of the cutoffs of the saved warning grid whose windows break the sample limits of analysis decision 5.

Failure modes covered here:

- The profile's fast count and the one definition of the limits (``features.eligibility.warning_eligibility``) disagree on a cutoff, so the profile reports
  a share that the feature code would not reproduce. A seeded case with gaps and invalid bursts is judged both ways at every cutoff of its grid, including
  cutoffs whose windows reach before the first or after the last recorded second.
- A limit moves: one invalid second is tolerated in the 20-second window, or exactly 10% invalid of the 15-minute window is refused, or 10% plus one is
  accepted.
- The grid moves: it starts at another time than ``opstart``, it steps another number of seconds, it ends after the latest grid time at or before ``opend``
  minus 300 seconds, or it reads the recorded span of the waveform instead of the operation.
- A second that is absent counts as valid, or the reading of a second whose predecessor is invalid counts as valid, or a cutoff whose history reaches before
  the recording is scored.
- A case with no ``opstart`` or no ``opend``, or an ``opend`` that is not after ``opstart``, or an operation too short for one cutoff, is mixed up: the first
  three are counted as cases with no grid and give no cutoff, the last gives no cutoff and is not counted as a case with no grid.
- The Spark side swaps a count of the row (the case's counts or the stored row's columns read into the wrong fact) or writes a rule column that is not the
  rule's constant (step, first cutoff offset, last cutoff margin) or the registry's limit, which the checks of the stored row would then accept.
"""

from __future__ import annotations

import unittest
from typing import cast
from unittest import mock

from features.eligibility import warning_eligibility
from features.model import Reading, WaveformSecond, WaveformWindow
from features.registry import NUMERIC_OVER_10_PERCENT_INVALID, NUMERIC_WINDOW_SECONDS, WAVEFORM_INVALID_SECOND, WAVEFORM_WINDOW_SECONDS
from spark.profile import frames
from spark.profile.frames import unscored_facts, unscored_row
from spark.profile.unscored import NO_GRID, Unscored, count_unscored, grid_cutoffs

SECOND = 1_000_000


def reference(valid: set[int], present: set[int], cutoff: int) -> tuple[str, ...]:
    """The reasons of the one definition for the cutoff ``cutoff`` seconds, from the validity of each present second."""
    seconds = tuple(WaveformSecond(s, (80.0,) * 4 if s in valid else (), s in valid) for s in range(cutoff - WAVEFORM_WINDOW_SECONDS, cutoff) if s in present)
    series = [Reading(s * SECOND, 80.0, True) for s in range(cutoff - NUMERIC_WINDOW_SECONDS, cutoff) if s in valid and s - 1 in valid]
    return warning_eligibility(cutoff * SECOND, series, WaveformWindow(4.0, seconds)).reasons


class Chance:
    """A small linear congruential generator, so that a seed gives the same case on every machine."""

    def __init__(self, seed: int) -> None:
        self.state = seed * 2654435761 + 12345

    def below(self, limit: int) -> int:
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) % 2**64
        return (self.state >> 33) % limit


def seeded_case(seed: int, length: int) -> tuple[set[int], set[int]]:
    """A case of ``length`` seconds from a start of 37: some seconds absent, bursts of invalid seconds, the rest valid."""
    chance = Chance(seed)
    present = set(range(37, 37 + length))
    for _ in range(6):
        start = 37 + chance.below(length)
        present -= set(range(start, start + 1 + chance.below(39)))
    valid = set(present)
    for _ in range(25):
        start = 37 + chance.below(length)
        valid -= set(range(start, start + 1 + chance.below(11)))
    for _ in range(60):
        valid.discard(37 + chance.below(length))
    return valid, present


def operation(seed: int, length: int) -> tuple[int, int]:
    """An operation whose grid starts before the first recorded second (37) and whose last cutoff falls after the last recorded second, in every seed."""
    return 37 - 300 + 13 * seed, 37 + length + 700 + 11 * seed


class AgainstTheOneDefinition(unittest.TestCase):
    def test_the_count_equals_the_reasons_of_the_one_definition_at_every_cutoff_of_the_grid(self) -> None:
        for seed in range(8):
            length = 3000 + 200 * seed
            valid, present = seeded_case(seed, length)
            opstart, opend = operation(seed, length)
            expected = [reference(valid, present, cutoff) for cutoff in range(opstart, opend - 300 + 1, 60)]
            counted = count_unscored(valid, opstart, opend)
            with self.subTest(seed=seed):
                self.assertGreater(counted.cutoffs, 20)
                self.assertEqual(len(expected), counted.cutoffs)
                self.assertEqual(sum(WAVEFORM_INVALID_SECOND in reasons for reasons in expected), counted.waveform)
                self.assertEqual(sum(NUMERIC_OVER_10_PERCENT_INVALID in reasons for reasons in expected), counted.numeric)
                self.assertEqual(sum(bool(reasons) for reasons in expected), counted.either)
                self.assertEqual(0, counted.no_grid)

    def test_the_seeded_cases_reach_both_limits_the_scored_cutoffs_and_the_edges_of_the_recording(self) -> None:
        totals = []
        for seed in range(8):
            length = 3000 + 200 * seed
            valid, _ = seeded_case(seed, length)
            opstart, opend = operation(seed, length)
            totals.append(count_unscored(valid, opstart, opend))
            self.assertLess(opstart, 37 + NUMERIC_WINDOW_SECONDS)
            self.assertGreater(opend - 300, 37 + length)
        self.assertTrue(any(item.waveform for item in totals))
        self.assertTrue(any(item.numeric for item in totals))
        self.assertTrue(any(item.either < item.cutoffs for item in totals))


class Limits(unittest.TestCase):
    def case(self, invalid: set[int]) -> Unscored:
        """A recording of the seconds -1 to 899 whose seconds in ``invalid`` are invalid or absent: one cutoff, ``opstart`` 900 (``opend`` 1200)."""
        return count_unscored(set(range(-1, 900)) - invalid, 900, 1200)

    def test_a_recording_with_no_invalid_second_has_one_scored_cutoff(self) -> None:
        self.assertEqual(Unscored(1, 0, 0, 0, 0), self.case(set()))

    def test_one_invalid_second_in_the_20_second_window_makes_the_cutoff_unscored(self) -> None:
        self.assertEqual(1, self.case({880}).waveform)
        self.assertEqual(1, self.case({899}).waveform)
        self.assertEqual(0, self.case({879}).waveform)

    def test_the_numeric_window_tolerates_exactly_10_percent_and_no_more(self) -> None:
        # A run of 89 invalid seconds costs 90 readings (the run and the reading after it): exactly 10% of 900.
        self.assertEqual(Unscored(1, 0, 0, 0, 0), self.case(set(range(100, 189))))
        self.assertEqual(Unscored(1, 0, 1, 1, 0), self.case(set(range(100, 190))))

    def test_the_reading_after_an_invalid_second_is_invalid(self) -> None:
        # Each isolated invalid second costs two readings: 45 of them cost 90 (eligible), 46 cost 92.
        self.assertEqual(0, self.case({100 + 10 * index for index in range(45)}).numeric)
        self.assertEqual(1, self.case({100 + 10 * index for index in range(46)}).numeric)

    def test_an_absent_second_counts_as_invalid_in_both_windows(self) -> None:
        self.assertEqual(1, self.case({890}).waveform)
        self.assertEqual(1, self.case(set(range(100, 190))).numeric)


class History(unittest.TestCase):
    def test_a_cutoff_with_a_full_history_before_the_operation_start_is_scored(self) -> None:
        self.assertEqual(Unscored(1, 0, 0, 0, 0), count_unscored(range(0, 3000), 900, 1200))

    def test_the_first_cutoff_is_unscored_when_the_recording_starts_less_than_15_minutes_before_it(self) -> None:
        # The window is the seconds -800 to 99: 100 seconds of recording, 99 readings, 801 invalid of 900.
        self.assertEqual(Unscored(1, 0, 1, 1, 0), count_unscored(range(0, 3000), 100, 400))
        self.assertEqual(Unscored(1, 1, 1, 1, 0), count_unscored(range(0, 3000), 10, 310))

    def test_a_cutoff_after_the_last_recorded_second_is_unscored_and_counted(self) -> None:
        self.assertEqual(Unscored(2, 1, 0, 1, 0), count_unscored(range(0, 1000), 1000, 1360))

    def test_a_case_with_no_valid_second_has_every_cutoff_unscored(self) -> None:
        self.assertEqual(Unscored(2, 2, 2, 2, 0), count_unscored([], 1000, 1360))


class Grid(unittest.TestCase):
    def test_the_grid_starts_at_opstart_and_steps_60_seconds(self) -> None:
        cutoffs = list(grid_cutoffs(1000, 2000))
        self.assertEqual(1000, cutoffs[0])
        self.assertEqual({60}, {b - a for a, b in zip(cutoffs, cutoffs[1:], strict=False)})

    def test_the_last_cutoff_is_the_latest_grid_time_at_or_before_opend_minus_300(self) -> None:
        self.assertEqual(1000 + 60 * 11, list(grid_cutoffs(1000, 1960))[-1])  # opend - 300 = 1660: 1000 + 660
        self.assertEqual(1000 + 60 * 10, list(grid_cutoffs(1000, 1959))[-1])
        self.assertEqual(1000 + 60 * 11, list(grid_cutoffs(1000, 1960 + 59))[-1])
        self.assertEqual(1000 + 60 * 12, list(grid_cutoffs(1000, 1960 + 60))[-1])

    def test_an_operation_of_exactly_300_seconds_has_one_cutoff_and_a_shorter_one_has_none(self) -> None:
        self.assertEqual([1000], list(grid_cutoffs(1000, 1300)))
        self.assertEqual([], list(grid_cutoffs(1000, 1299)))
        self.assertEqual(Unscored(0, 0, 0, 0, 0), count_unscored(range(0, 3000), 1000, 1299))

    def test_the_grid_does_not_read_the_recorded_span(self) -> None:
        self.assertEqual(Unscored(5, 5, 5, 5, 0), count_unscored([5], 100000, 100540))


class NoGrid(unittest.TestCase):
    def test_a_case_with_no_opstart_or_no_opend_or_an_opend_that_is_not_after_opstart_has_no_grid(self) -> None:
        for name, opstart, opend in (
            ("no opstart", None, 5000),
            ("no opend", 1000, None),
            ("neither", None, None),
            ("opend equals opstart", 1000, 1000),
            ("opend before opstart", 2000, 1000),
        ):
            with self.subTest(name):
                self.assertEqual(NO_GRID, count_unscored(range(0, 3000), opstart, opend))
        self.assertEqual(Unscored(0, 0, 0, 0, 1), NO_GRID)

    def test_a_value_that_is_not_a_whole_second_has_no_grid(self) -> None:
        for name, opstart, opend in (
            ("fractional opstart", 1000.5, 5000),
            ("fractional opend", 1000, 5000.25),
            ("not a number", float("nan"), 5000),
            ("infinite opend", 1000, float("inf")),
        ):
            with self.subTest(name):
                self.assertEqual(NO_GRID, count_unscored(range(0, 3000), opstart, opend))

    def test_a_whole_second_given_as_a_float_is_read_as_that_second(self) -> None:
        self.assertEqual(count_unscored(range(0, 3000), 1000, 1360), count_unscored(range(0, 3000), 1000.0, 1360.0))

    def test_a_short_operation_is_not_a_case_with_no_grid(self) -> None:
        self.assertEqual(0, count_unscored(range(0, 3000), 1000, 1100).no_grid)


class InTheFrame(unittest.TestCase):
    """``spark.profile.frames.unscored_row`` is the one call that Spark makes for each case: it takes the values as Spark gives them."""

    def test_the_row_is_the_count_of_the_case_and_a_missing_time_is_a_case_with_no_grid(self) -> None:
        self.assertEqual((2, 1, 0, 1, 0), unscored_row(list(range(0, 1000)), 1000.0, 1360.0))
        self.assertEqual((0, 0, 0, 0, 1), unscored_row(list(range(0, 1000)), None, 1360.0))
        self.assertEqual((0, 0, 0, 0, 1), unscored_row(None, 1000.0, None))
        self.assertEqual((1, 1, 1, 1, 0), unscored_row(None, 1000.0, 1300.0))
        self.assertEqual((1, 0, 1, 1, 0), unscored_row(list(range(0, 3000)), 100.0, 400.0))

    def test_every_column_of_the_stored_row_is_read_into_its_own_fact(self) -> None:
        row = {
            "scope": "development",
            "cases": 3,
            "no_grid": 4,
            "cutoffs": 5,
            "waveform": 6,
            "numeric": 7,
            "either": 8,
            "unscored_share": 0.5,
            "cutoff_step": 9,
            "cutoff_offset": 10,
            "last_margin": 11,
            "waveform_window": 12,
            "waveform_limit": 13,
            "numeric_window": 14,
            "numeric_limit": 15,
        }

        class Stored:
            def collect(self) -> list[dict[str, object]]:
                return [row]

        facts = unscored_facts(Stored())
        self.assertEqual(
            {name: row[name] for name in row if name not in ("scope", "unscored_share")},
            {name: getattr(facts, name) for name in row if name not in ("scope", "unscored_share")},
        )
        self.assertEqual((1, 0.5), (facts.rows, facts.share))

    def test_the_rule_columns_of_the_frame_are_the_constants_of_the_rule_and_the_registry(self) -> None:
        log: list[tuple[str, tuple[object, ...]]] = []

        class Recorder:
            def __getattr__(self, name: str) -> object:
                def call(*arguments: object, **_options: object) -> Recorder:
                    log.append((name, arguments))
                    return Recorder()

                return call

        stub = mock.MagicMock()
        stub.lit.side_effect = lambda value: ("lit", value)
        with (
            mock.patch.object(frames, "functions", return_value=stub),
            mock.patch.object(frames, "typed", side_effect=lambda frame, _columns: frame),
            mock.patch.object(frames, "unscored_udf", return_value=mock.MagicMock()),
        ):
            frames.unscored_frame(Recorder(), Recorder())
        (rule,) = [cast("dict[str, object]", arguments[0]) for name, arguments in log if name == "withColumns"]
        self.assertEqual(
            {
                "scope": ("lit", "development"),
                "cutoff_step": ("lit", 60),
                "cutoff_offset": ("lit", 0),
                "last_margin": ("lit", 300),
                "waveform_window": ("lit", 20),
                "waveform_limit": ("lit", 0),
                "numeric_window": ("lit", 900),
                "numeric_limit": ("lit", 10),
            },
            {name: value for name, value in rule.items() if name != "unscored_share"},
        )


if __name__ == "__main__":
    unittest.main()
