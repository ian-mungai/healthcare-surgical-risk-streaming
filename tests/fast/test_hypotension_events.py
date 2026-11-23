"""The hypotension event rule: 60 consecutive valid seconds of the per-second series below 65 mmHg, inside the operation (analysis plan, rows 2 and 4).

Every case here is hand-built and kept apart from published data. Failure modes covered here:

- A run of 59 low seconds is an event, or a run of 60 is not one (the off-by-one at 60 seconds), at the start, in the middle or at the end of a case.
- A second at exactly 65 mmHg counts as low, or one at 64.999 does not.
- An invalid second inside a low run does not break it, so 30 + invalid + 30 is an event. A second that the series lacks does not break it either.
- The invalid seconds that follow an onset are not judged by the same trailing rule as the others, so follow-up seconds that cannot be trusted confirm an onset.
- Seconds before ``opstart`` or at and after ``opend`` count, or a second that straddles ``opend`` counts.
- A run that the operation or an invalid second cut short is called "recovered", so a label could not tell it from a run that really ended.
- The onset or the confirmation time is the wrong end of its second, so a feature at a cutoff could read data after it.
- Two runs separated by one second at 65 mmHg are joined into one event.
- The input order or a repeated second changes the answer.
"""

from __future__ import annotations

import unittest

from features.hypotension import (
    END_INVALID,
    END_MISSING,
    END_OPERATION,
    END_RECOVERED,
    EVENT_SECONDS,
    EVENT_THRESHOLD_MMHG,
    SECOND_US,
    SeriesPoint,
    events_of,
    low_runs,
)
from features.pressure_series import second_facts, series_from_seconds
from features.registry import LOW_PRESSURE_65
from features.signal_validity import FLAT_WINDOW_SECONDS, combine_extents, second_extent, waveform_verdict

OPSTART, OPEND = 100.0, 10_000.0


def low(first: int, count: int, value: float = 60.0) -> list[SeriesPoint]:
    return [SeriesPoint(second, value, True) for second in range(first, first + count)]


def bad(second: int) -> SeriesPoint:
    return SeriesPoint(second, None, False)


def high(second: int, value: float = 80.0) -> SeriesPoint:
    return SeriesPoint(second, value, True)


def events(points: list[SeriesPoint], opstart: float = OPSTART, opend: float = OPEND) -> list[tuple[int, int, str]]:
    return [(event.onset_second, event.duration_seconds, event.end_reason) for event in events_of(points, opstart, opend)]


class Boundary(unittest.TestCase):
    def test_the_constants_are_the_plan(self) -> None:
        self.assertEqual(60, EVENT_SECONDS)
        self.assertEqual(65.0, EVENT_THRESHOLD_MMHG)
        self.assertEqual(LOW_PRESSURE_65, EVENT_THRESHOLD_MMHG)

    def test_59_low_seconds_are_not_an_event(self) -> None:
        self.assertEqual([], events([*low(200, 59), high(259)]))

    def test_60_low_seconds_are_an_event(self) -> None:
        self.assertEqual([(200, 60, END_RECOVERED)], events([*low(200, 60), high(260)]))

    def test_61_low_seconds_are_one_event_of_61_seconds(self) -> None:
        self.assertEqual([(200, 61, END_RECOVERED)], events([*low(200, 61), high(261)]))

    def test_the_boundary_holds_at_the_first_second_of_the_operation_and_at_its_last(self) -> None:
        self.assertEqual([], events([*low(100, 59), high(159)]))
        self.assertEqual([(100, 60, END_RECOVERED)], events([*low(100, 60), high(160)]))
        # The last complete second of the operation is opend - 1: a run of 60 that ends there is whole, one of 59 is not.
        self.assertEqual([(9940, 60, END_OPERATION)], events(low(9940, 60)))
        self.assertEqual([], events(low(9941, 59)))

    def test_a_second_at_exactly_65_is_not_low_and_one_below_it_is(self) -> None:
        self.assertEqual([], events(low(200, 60, EVENT_THRESHOLD_MMHG)))
        self.assertEqual([(200, 60, END_OPERATION)], events(low(200, 60, 64.99999999999999), opend=260.0))

    def test_one_second_at_65_splits_two_low_runs_so_neither_of_30_is_an_event(self) -> None:
        points = [*low(200, 30), high(230, 65.0), *low(231, 30)]
        self.assertEqual([], events(points))

    def test_a_run_of_60_then_one_second_at_65_then_a_run_of_60_are_two_events(self) -> None:
        points = [*low(200, 60), high(260, 65.0), *low(261, 60), high(321)]
        self.assertEqual([(200, 60, END_RECOVERED), (261, 60, END_RECOVERED)], events(points))


class InvalidGap(unittest.TestCase):
    def test_an_invalid_second_inside_a_low_run_breaks_it(self) -> None:
        points = [*low(200, 30), bad(230), *low(231, 30)]
        self.assertEqual([], events(points))

    def test_a_run_of_60_that_an_invalid_second_follows_is_an_event_that_ends_by_the_invalid_second(self) -> None:
        self.assertEqual([(200, 60, END_INVALID)], events([*low(200, 60), bad(260)]))

    def test_a_run_of_59_then_an_invalid_second_is_not_an_event_and_is_told_apart_from_a_recovery(self) -> None:
        points = [*low(200, 59), bad(259)]
        self.assertEqual([], events(points))
        self.assertEqual([(200, 59, END_INVALID)], [(run.first_second, run.seconds, run.end_reason) for run in low_runs(points, OPSTART, OPEND)])

    def test_a_second_the_series_does_not_hold_breaks_the_run_like_an_invalid_one(self) -> None:
        points = [*low(200, 30), *low(231, 30)]
        self.assertEqual([], events(points))
        self.assertEqual([END_MISSING, END_OPERATION], [run.end_reason for run in low_runs(points, OPSTART, 261.0)])

    def test_the_invalid_second_after_a_run_does_not_start_another(self) -> None:
        points = [*low(200, 60), bad(260), *low(261, 10)]
        self.assertEqual([(200, 60, END_INVALID)], events(points))

    def test_the_two_invalid_seconds_that_one_bad_second_makes_in_the_series_both_break_the_run(self) -> None:
        # One flat or jumping second of the waveform is invalid, and so is the next series second, which averages it: 100..129, two bad, 132..161.
        points = [*low(100, 30), bad(130), bad(131), *low(132, 30)]
        self.assertEqual([], events(points))


class Operation(unittest.TestCase):
    def test_seconds_before_opstart_do_not_count(self) -> None:
        # 40 low seconds before opstart 100 (60..99) and 20 inside (100..119): 20 inside, not an event.
        self.assertEqual([], events([*low(60, 60), high(120)]))

    def test_a_second_that_straddles_opstart_is_outside_and_a_second_from_opstart_on_is_inside(self) -> None:
        # With opstart 100.5 the second 100 starts before it, so the run is 101..160 (60 seconds) and not 100..160.
        self.assertEqual([(101, 60, END_RECOVERED)], events([*low(100, 61), high(161)], opstart=100.5))

    def test_a_second_that_straddles_opend_is_outside(self) -> None:
        # opend 260.5: the second 260 ends after it, so a run of 200..260 is 60 seconds long (200..259), cut by the operation.
        self.assertEqual([(200, 60, END_OPERATION)], events(low(200, 61), opend=260.5))
        self.assertEqual([], events(low(201, 60), opend=260.5))

    def test_a_run_that_the_operation_cuts_short_is_not_an_event_and_is_marked_as_cut(self) -> None:
        points = low(9950, 50)
        self.assertEqual([], events(points))
        self.assertEqual([(9950, 50, END_OPERATION)], [(run.first_second, run.seconds, run.end_reason) for run in low_runs(points, OPSTART, OPEND)])

    def test_a_case_with_no_operation_has_no_event(self) -> None:
        self.assertEqual([], events(low(200, 100), opstart=500.0, opend=500.0))


class Times(unittest.TestCase):
    def test_the_onset_is_the_end_of_the_first_low_second_and_the_event_is_confirmed_at_the_end_of_the_60th(self) -> None:
        (event,) = events_of([*low(200, 75), high(275)], OPSTART, OPEND)
        self.assertEqual(200, event.onset_second)
        self.assertEqual(201 * SECOND_US, event.onset_us)
        self.assertEqual(260 * SECOND_US, event.confirmed_us)
        self.assertEqual(274, event.last_low_second)
        self.assertEqual(275 * SECOND_US, event.end_us)
        self.assertEqual(75, event.duration_seconds)

    def test_the_order_of_the_points_does_not_change_the_events(self) -> None:
        points = [*low(200, 60), high(260)]
        self.assertEqual(events(points), events(list(reversed(points))))

    def test_a_second_given_twice_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            events_of([*low(200, 3), SeriesPoint(200, 60.0, True)], OPSTART, OPEND)

    def test_a_valid_point_without_a_value_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            events_of([SeriesPoint(200, None, True)], OPSTART, OPEND)

    def test_the_operation_times_must_be_numbers_with_the_start_not_after_the_end_or_the_case_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            events_of(low(200, 60), float("nan"), OPEND)
        with self.assertRaises(ValueError):
            events_of(low(200, 60), 500.0, 400.0)


class TrailingValidityOnFollowUpSeconds(unittest.TestCase):
    """The same trailing rule judges the seconds that confirm an onset: the events are made from the series that the one definition builds."""

    RATE = 4

    def waveform(self, mean: float, spread: float = 14.0) -> tuple[float, ...]:
        """One second of a pulsatile pressure: 4 samples around ``mean`` with a range of ``2 * spread`` (over the 20 mmHg of a segment)."""
        return (mean - spread, mean, mean + spread, mean)

    def series(self, seconds: dict[int, tuple[float, ...]]) -> list[SeriesPoint]:
        facts = []
        for second in sorted(seconds):
            before = seconds.get(second - 1)
            prior = combine_extents(second_extent(seconds[s], self.RATE, 0) for s in range(second - FLAT_WINDOW_SECONDS + 1, second) if s in seconds)
            verdict = waveform_verdict("SNUADC/ART", seconds[second], self.RATE, 0, None if before is None else before[-1], prior)
            facts.append(second_facts(second, verdict, seconds[second]))
        return [SeriesPoint(second, reading.value, reading.valid) for second, reading in series_from_seconds(facts)]

    def test_a_low_pressure_waveform_of_60_seconds_is_an_event_from_the_second_after_its_first(self) -> None:
        # The series of the first second needs a second before it, so 61 low seconds give a run of 60 that starts at the second after the first.
        points = self.series({second: self.waveform(55.0) for second in range(200, 261)})
        self.assertEqual([(201, 60, END_OPERATION)], events(points, opend=261.0))

    def test_a_jump_in_a_follow_up_second_breaks_the_run_and_the_event_is_not_confirmed(self) -> None:
        seconds = {second: self.waveform(55.0) for second in range(200, 261)}
        seconds[250] = (50.0, 50.0, 90.0, 55.0)  # a jump of 40 mmHg: the second is invalid, and the one after it too
        points = self.series(seconds)
        self.assertEqual([], events(points, opend=261.0))
        self.assertFalse(points[50].valid)

    def test_a_flat_follow_up_segment_does_not_confirm_an_onset(self) -> None:
        # From second 230 on the signal is flat (range 0): the 20-second segment is flat from second 249 on, so the later seconds are invalid.
        seconds = {second: self.waveform(55.0) for second in range(200, 230)} | {second: (55.0, 55.0, 55.0, 55.0) for second in range(230, 262)}
        points = self.series(seconds)
        self.assertEqual([], events(points, opend=262.0))


if __name__ == "__main__":
    unittest.main()
