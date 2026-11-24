"""The unknown rule of the blood pressure drop label on the runs of ``features.hypotension`` (analysis plan, "Blood pressure drop", items 7 to 9).

Every case here is hand-built and kept apart from published data. A sample is a cutoff (microseconds from the case start) and a horizon (seconds). Failure
modes covered here:

- An onset at the cutoff or after the end of the horizon is positive, or one just after the cutoff or at the end of the horizon is dropped.
- A run that an invalid second, a gap in the series or the end of the operation cut short is called negative, though the observations cannot establish that
  it was not the start of an event, or a run that really ended (a valid second at or above 65) is called unknown.
- A window that the operation does not cover (the case ends before the horizon) or that the signal does not cover is called negative.
- One invalid second among valid seconds at or above 65 makes every sample unknown, or a window with no reading at all is called negative.
- A run that began before the cutoff makes the sample positive, so a drop that is already going on is a drop to predict.
- The label of a sample changes with the order of the points, or reads a second after the end of the horizon (but the 60 seconds that confirm an onset that is
  in the horizon may end after it).
"""

from __future__ import annotations

import unittest

from features.hypotension import SECOND_US, SeriesPoint
from features.label_values import NEGATIVE, POSITIVE, UNKNOWN
from features.pressure_series import ExactMean
from features.warning_label import COVERED, EVENT, OPEN_RUN, UNCOVERED, warning_label

OPSTART, OPEND = 100.0, 10_000.0
CUTOFF = 1_000
HORIZON = 300


def low(first: int, count: int) -> list[SeriesPoint]:
    return [SeriesPoint(second, ExactMean.of(60.0), True) for second in range(first, first + count)]


def high(first: int, count: int) -> list[SeriesPoint]:
    return [SeriesPoint(second, ExactMean.of(80.0), True) for second in range(first, first + count)]


def bad(first: int, count: int = 1) -> list[SeriesPoint]:
    return [SeriesPoint(second, None, False) for second in range(first, first + count)]


def steady(first: int, last: int) -> list[SeriesPoint]:
    """Valid seconds at 80 mmHg from ``first`` through ``last``."""
    return high(first, last - first + 1)


def label_of(points: list[SeriesPoint], cutoff: int = CUTOFF, horizon: int = HORIZON, opend: float = OPEND) -> tuple[str, str]:
    outcome = warning_label(points, OPSTART, opend, cutoff * SECOND_US, horizon)
    return outcome.label, outcome.reason


class Positive(unittest.TestCase):
    def test_an_onset_in_the_horizon_is_positive_with_its_onset_time(self) -> None:
        points = steady(900, 1_099) + low(1_100, 60) + steady(1_160, 1_500)
        outcome = warning_label(points, OPSTART, OPEND, CUTOFF * SECOND_US, HORIZON)
        self.assertEqual((POSITIVE, EVENT, 1_101 * SECOND_US), (outcome.label, outcome.reason, outcome.onset_us))

    def test_the_onset_is_after_the_cutoff_through_the_end_of_the_horizon(self) -> None:
        for first, expected in ((998, NEGATIVE), (999, NEGATIVE), (1_000, POSITIVE), (1_299, POSITIVE), (1_300, NEGATIVE)):
            with self.subTest(first=first):
                points = steady(900, first - 1) + low(first, 60) + steady(first + 60, 1_500)
                self.assertEqual(expected, label_of(points)[0])

    def test_the_second_that_confirms_the_onset_may_be_after_the_horizon(self) -> None:
        points = steady(900, 1_279) + low(1_280, 60) + steady(1_340, 1_500)
        self.assertEqual((POSITIVE, EVENT), label_of(points))

    def test_a_run_that_began_before_the_cutoff_is_not_an_onset_in_the_horizon(self) -> None:
        points = steady(900, 949) + low(950, 200) + steady(1_150, 1_500)
        self.assertEqual(NEGATIVE, label_of(points)[0])

    def test_a_run_of_59_seconds_is_not_positive(self) -> None:
        points = steady(900, 1_099) + low(1_100, 59) + steady(1_159, 1_500)
        self.assertEqual((NEGATIVE, COVERED), label_of(points))


class Unknown(unittest.TestCase):
    def test_a_short_run_that_an_invalid_second_cut_is_unknown(self) -> None:
        points = steady(900, 1_099) + low(1_100, 30) + bad(1_130) + steady(1_131, 1_500)
        self.assertEqual((UNKNOWN, OPEN_RUN), label_of(points))

    def test_a_short_run_that_a_gap_in_the_series_cut_is_unknown(self) -> None:
        points = steady(900, 1_099) + low(1_100, 30) + steady(1_140, 1_500)
        self.assertEqual((UNKNOWN, OPEN_RUN), label_of(points))

    def test_a_short_run_at_the_end_of_the_operation_is_unknown(self) -> None:
        opend = 1_130.0
        points = steady(900, 1_099) + low(1_100, 30)
        self.assertEqual((UNKNOWN, OPEN_RUN), label_of(points, opend=opend))

    def test_a_short_run_that_recovered_is_negative(self) -> None:
        points = steady(900, 1_099) + low(1_100, 30) + steady(1_130, 1_500)
        self.assertEqual((NEGATIVE, COVERED), label_of(points))

    def test_a_short_run_whose_onset_is_after_the_horizon_does_not_matter(self) -> None:
        points = steady(900, 1_299) + low(1_300, 30) + bad(1_330) + steady(1_331, 1_500)
        self.assertEqual((NEGATIVE, COVERED), label_of(points))

    def test_a_short_run_whose_onset_is_before_the_cutoff_does_not_matter(self) -> None:
        points = steady(900, 959) + low(960, 30) + bad(990) + steady(991, 1_500)
        self.assertEqual((NEGATIVE, COVERED), label_of(points))

    def test_a_case_that_ends_before_the_horizon_is_unknown(self) -> None:
        points = steady(900, 1_200)
        self.assertEqual((UNKNOWN, UNCOVERED), label_of(points, opend=1_201.0))

    def test_a_horizon_that_ends_exactly_with_the_operation_is_covered(self) -> None:
        self.assertEqual((NEGATIVE, COVERED), label_of(steady(900, 1_300), opend=1_301.0))

    def test_a_lost_signal_before_the_horizon_is_unknown(self) -> None:
        points = steady(900, 1_099) + bad(1_100, 300) + steady(1_400, 1_500)
        self.assertEqual((UNKNOWN, UNCOVERED), label_of(points))

    def test_no_reading_at_all_in_the_window_is_unknown(self) -> None:
        self.assertEqual((UNKNOWN, UNCOVERED), label_of(steady(900, 999)))

    def test_one_invalid_second_among_valid_seconds_at_or_above_65_is_negative(self) -> None:
        points = steady(900, 1_099) + bad(1_100) + steady(1_101, 1_500)
        self.assertEqual((NEGATIVE, COVERED), label_of(points))

    def test_a_positive_is_not_made_unknown_by_a_lost_signal_after_it(self) -> None:
        points = steady(900, 1_099) + low(1_100, 60) + bad(1_160, 400)
        self.assertEqual((POSITIVE, EVENT), label_of(points))

    def test_a_positive_is_not_made_unknown_by_a_case_end_before_the_horizon(self) -> None:
        points = steady(900, 1_099) + low(1_100, 60)
        self.assertEqual((POSITIVE, EVENT), label_of(points, opend=1_160.0))


class Order(unittest.TestCase):
    def test_the_order_of_the_points_does_not_change_the_label(self) -> None:
        points = steady(900, 1_099) + low(1_100, 30) + bad(1_130) + steady(1_131, 1_500)
        self.assertEqual(label_of(points), label_of(points[::-1]))

    def test_a_second_given_twice_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            label_of(steady(900, 1_500) + high(1_000, 1))

    def test_a_non_positive_horizon_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            label_of(steady(900, 1_500), horizon=0)


if __name__ == "__main__":
    unittest.main()
