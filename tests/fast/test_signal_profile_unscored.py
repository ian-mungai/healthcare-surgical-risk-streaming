"""The unscored share of the signal profile: the count of whole-minute cutoffs whose windows break the sample limits of analysis decision 5.

Failure modes covered here:

- The profile's fast count and the one definition of the limits (``features.eligibility.warning_eligibility``) disagree on a cutoff, so the profile reports
  a share that the feature code would not reproduce. A seeded case with gaps and invalid bursts is judged both ways at every cutoff.
- A limit moves: one invalid second is tolerated in the 20-second window, or exactly 10% invalid of the 15-minute window is refused, or 10% plus one is
  accepted.
- The cutoff grid moves: a cutoff before 15 minutes of recording, after the last recorded second or off the whole minute is counted.
- A second that is absent counts as valid, or the reading of a second whose predecessor is invalid counts as valid.
- A case with too little recording gives a cutoff, or a share of 0 where it has no cutoff at all.
"""

from __future__ import annotations

import unittest

from features.eligibility import warning_eligibility
from features.model import Reading, WaveformSecond, WaveformWindow
from features.registry import NUMERIC_OVER_10_PERCENT_INVALID, NUMERIC_WINDOW_SECONDS, WAVEFORM_INVALID_SECOND, WAVEFORM_WINDOW_SECONDS
from spark.profile.unscored import CUTOFF_STEP_SECONDS, Unscored, count_unscored, cutoffs_of, share_of

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


class AgainstTheOneDefinition(unittest.TestCase):
    def test_the_count_equals_the_reasons_of_the_one_definition_at_every_cutoff(self) -> None:
        for seed in range(8):
            valid, present = seeded_case(seed, 3000 + 200 * seed)
            first, last = min(present), max(present)
            expected = [reference(valid, present, cutoff) for cutoff in cutoffs_of(first, last)]
            counted = count_unscored(valid, first, last)
            with self.subTest(seed=seed):
                self.assertGreater(counted.cutoffs, 20)
                self.assertEqual(len(expected), counted.cutoffs)
                self.assertEqual(sum(WAVEFORM_INVALID_SECOND in reasons for reasons in expected), counted.waveform)
                self.assertEqual(sum(NUMERIC_OVER_10_PERCENT_INVALID in reasons for reasons in expected), counted.numeric)
                self.assertEqual(sum(bool(reasons) for reasons in expected), counted.either)

    def test_the_seeded_cases_reach_both_limits_and_the_eligible_cutoffs(self) -> None:
        totals = []
        for seed in range(8):
            valid, present = seeded_case(seed, 3000 + 200 * seed)
            totals.append(count_unscored(valid, min(present), max(present)))
        self.assertTrue(any(item.waveform for item in totals))
        self.assertTrue(any(item.numeric for item in totals))
        self.assertTrue(any(item.either < item.cutoffs for item in totals))


class Limits(unittest.TestCase):
    def case(self, invalid: set[int]) -> Unscored:
        """A recording of the seconds -1 to 899 whose seconds in ``invalid`` are invalid or absent: one cutoff, 900."""
        return count_unscored(set(range(-1, 900)) - invalid, -1, 899)

    def test_a_recording_with_no_invalid_second_has_one_scored_cutoff(self) -> None:
        self.assertEqual(Unscored(1, 0, 0, 0), self.case(set()))

    def test_one_invalid_second_in_the_20_second_window_makes_the_cutoff_unscored(self) -> None:
        self.assertEqual(1, self.case({880}).waveform)
        self.assertEqual(1, self.case({899}).waveform)
        self.assertEqual(0, self.case({879}).waveform)
        self.assertEqual(60, CUTOFF_STEP_SECONDS)

    def test_the_numeric_window_tolerates_exactly_10_percent_and_no_more(self) -> None:
        # A run of 89 invalid seconds costs 90 readings (the run and the reading after it): exactly 10% of 900.
        self.assertEqual(Unscored(1, 0, 0, 0), self.case(set(range(100, 189))))
        self.assertEqual(Unscored(1, 0, 1, 1), self.case(set(range(100, 190))))

    def test_the_reading_after_an_invalid_second_is_invalid(self) -> None:
        # Each isolated invalid second costs two readings: 45 of them cost 90 (eligible), 46 cost 92.
        self.assertEqual(0, self.case({100 + 10 * index for index in range(45)}).numeric)
        self.assertEqual(1, self.case({100 + 10 * index for index in range(46)}).numeric)

    def test_an_absent_second_counts_as_invalid_in_both_windows(self) -> None:
        self.assertEqual(1, self.case({890}).waveform)
        self.assertEqual(1, self.case(set(range(100, 190))).numeric)


class Grid(unittest.TestCase):
    def test_the_cutoffs_are_whole_minutes_with_a_full_history_inside_the_recording(self) -> None:
        cutoffs = list(cutoffs_of(37, 4000))
        self.assertEqual(0, cutoffs[0] % 60)
        self.assertGreaterEqual(cutoffs[0] - NUMERIC_WINDOW_SECONDS, 37)
        self.assertLess(cutoffs[0] - 60 - NUMERIC_WINDOW_SECONDS, 37)
        self.assertEqual(0, cutoffs[-1] % 60)
        self.assertLessEqual(cutoffs[-1] - 1, 4000)
        self.assertGreater(cutoffs[-1] + 60 - 1, 4000)
        self.assertEqual({60}, {b - a for a, b in zip(cutoffs, cutoffs[1:], strict=False)})

    def test_a_recording_shorter_than_the_15_minute_window_has_no_cutoff(self) -> None:
        self.assertEqual([], list(cutoffs_of(0, 898)))
        self.assertEqual([], list(cutoffs_of(10, 10)))
        self.assertEqual(Unscored(0, 0, 0, 0), count_unscored([5, 6, 7], 0, 20))

    def test_a_recording_of_exactly_15_minutes_has_one_cutoff(self) -> None:
        self.assertEqual([900], list(cutoffs_of(0, 899)))
        self.assertEqual([960], list(cutoffs_of(37, 960)))

    def test_the_second_after_the_last_recorded_second_is_not_a_cutoff(self) -> None:
        self.assertEqual([900], list(cutoffs_of(0, 958)))
        self.assertEqual([900, 960], list(cutoffs_of(0, 959)))


class Share(unittest.TestCase):
    def test_the_share_is_the_unscored_cutoffs_over_all_cutoffs(self) -> None:
        self.assertEqual(0.25, share_of(Unscored(8, 2, 1, 2)))
        self.assertEqual(0.0, share_of(Unscored(5, 0, 0, 0)))
        self.assertEqual(1.0, share_of(Unscored(3, 3, 0, 3)))

    def test_no_cutoff_has_no_share(self) -> None:
        self.assertIsNone(share_of(Unscored(0, 0, 0, 0)))


if __name__ == "__main__":
    unittest.main()
