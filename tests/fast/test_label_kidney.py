"""The kidney injury label (analysis plan, "Kidney injury after surgery", and the project standard's analysis decisions 6 and 7).

Every case here is hand-built and kept apart from published data. Failure modes covered here:

- A baseline that is dated after the start of anaesthesia, at it, or more than 30 days before it is taken as the baseline, or one of exactly 30 days is refused.
- The undated ``preop_cr`` or a postoperative result stands in for a baseline, so a case without a qualifying dated baseline is labelled instead of unknown.
- A case with no creatinine result in the seven days after ``opend`` (day 7 included, a result at ``opend`` excluded) is labelled instead of unknown.
- The label is positive on a rise outside its window: an absolute rise of 0.3 whose later result is after the first 48 hours, or a rise at the 48-hour edge
  that is dropped, or a ratio of 1.5 on a result after day 7.
- A float comparison decides a limit: 1.2 minus 0.9 is not 0.3 in a float, and 0.8 times 1.5 is not 1.2 in a float.
- The absolute rule pairs measurements more than 48 hours apart, or pairs a result with itself or a result at the same time, or reads the baseline only.
- A follow-up on day one only or on day five only is not enough or too much to decide, or a negative is called for a case that is not eligible.
- A label row does not carry the baseline with its time and source, the windows, the measurements and the times that the rule read.
"""

from __future__ import annotations

import unittest

from features.kidney_label import (
    ABSOLUTE_HOURS,
    ABSOLUTE_RISE_MG_DL,
    BASELINE_DAYS,
    ELIGIBLE,
    FOLLOWUP_DAYS,
    NEITHER,
    NO_BASELINE,
    NO_FOLLOWUP,
    RELATIVE_RATIO,
    Result,
    kidney_label,
)
from features.label_values import NEGATIVE, POSITIVE, UNKNOWN

DAY = 86_400
HOUR = 3_600
ANESTART = 10_000_000
OPEND = ANESTART + 6 * HOUR


def result(time_s: int, value: float, occurrence: int = 1) -> Result:
    return Result(time_s, occurrence, value)


def before(seconds: int, value: float) -> Result:
    """A result that is ``seconds`` before the start of anaesthesia."""
    return result(ANESTART - seconds, value)


def after(seconds: int, value: float) -> Result:
    """A result that is ``seconds`` after ``opend``."""
    return result(OPEND + seconds, value)


def label_of(*results: Result) -> tuple[str, str]:
    outcome = kidney_label(ANESTART, OPEND, list(results))
    return outcome.label, outcome.reason


class Constants(unittest.TestCase):
    def test_the_constants_are_the_plan(self) -> None:
        self.assertEqual(30, BASELINE_DAYS)
        self.assertEqual(48, ABSOLUTE_HOURS)
        self.assertEqual(7, FOLLOWUP_DAYS)
        self.assertEqual("0.3", str(ABSOLUTE_RISE_MG_DL))
        self.assertEqual("1.5", str(RELATIVE_RATIO))


class Positives(unittest.TestCase):
    def test_a_rise_of_0_3_within_48_hours_of_an_earlier_result_is_positive(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(10 * HOUR, 1.3)])
        self.assertEqual((POSITIVE, ELIGIBLE, True, False), (outcome.label, outcome.reason, outcome.absolute_rise, outcome.relative_rise))
        self.assertEqual((before(HOUR, 1.0), after(10 * HOUR, 1.3)), (outcome.earlier, outcome.later))

    def test_a_result_of_1_5_times_the_baseline_by_day_7_is_positive(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(20 * DAY, 1.0), after(5 * DAY, 1.5)])
        self.assertEqual((POSITIVE, True, False), (outcome.label, outcome.relative_rise, outcome.absolute_rise))
        self.assertEqual(after(5 * DAY, 1.5), outcome.relative_result)

    def test_both_rules_can_hold_at_once(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.6)])
        self.assertEqual((POSITIVE, True, True), (outcome.label, outcome.absolute_rise, outcome.relative_rise))

    def test_a_rise_between_two_postoperative_results_is_positive(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(2 * DAY, 1.0), after(HOUR, 1.0), after(30 * HOUR, 1.3)])
        self.assertEqual((POSITIVE, True), (outcome.label, outcome.absolute_rise))
        self.assertEqual((after(HOUR, 1.0), after(30 * HOUR, 1.3)), (outcome.earlier, outcome.later))

    def test_the_rise_is_exact_where_a_float_is_not(self) -> None:
        self.assertNotEqual(0.3, 1.2 - 0.9)
        self.assertEqual((POSITIVE, ELIGIBLE), label_of(before(HOUR, 0.9), after(HOUR, 1.2)))
        self.assertNotEqual(1.2, 0.8 * 1.5)
        self.assertEqual((POSITIVE, ELIGIBLE), label_of(before(20 * DAY, 0.8), after(5 * DAY, 1.2)))

    def test_a_rise_a_hair_under_the_limits_is_not_positive(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(HOUR, 1.0), after(HOUR, 1.29)))
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(20 * DAY, 1.0), after(5 * DAY, 1.49)))

    def test_the_first_qualifying_pair_is_reported_in_a_fixed_order(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), before(2 * HOUR, 0.8), after(HOUR, 1.4), after(2 * HOUR, 1.5)])
        self.assertEqual((before(2 * HOUR, 0.8), after(HOUR, 1.4)), (outcome.earlier, outcome.later))
        self.assertEqual(after(2 * HOUR, 1.5), outcome.relative_result)


class Windows(unittest.TestCase):
    def test_the_later_result_of_the_absolute_rise_is_inside_48_hours_after_opend_inclusive(self) -> None:
        intraoperative = result(OPEND, 1.0)
        self.assertEqual(POSITIVE, label_of(before(HOUR, 1.0), intraoperative, after(48 * HOUR, 1.3))[0])
        self.assertEqual(NEGATIVE, label_of(before(HOUR, 1.0), intraoperative, after(48 * HOUR + 1, 1.3))[0])

    def test_a_rise_after_the_first_48_hours_is_negative_unless_it_is_1_5_times(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(HOUR, 1.0), after(5 * DAY, 1.4)))
        self.assertEqual(POSITIVE, label_of(before(HOUR, 1.0), after(5 * DAY, 1.5))[0])

    def test_a_result_after_day_7_is_not_follow_up(self) -> None:
        self.assertEqual(POSITIVE, label_of(before(HOUR, 1.0), after(7 * DAY, 1.5))[0])
        self.assertEqual((UNKNOWN, NO_FOLLOWUP), label_of(before(HOUR, 1.0), after(7 * DAY + 1, 3.0)))

    def test_a_result_at_opend_or_before_it_is_not_follow_up(self) -> None:
        self.assertEqual((UNKNOWN, NO_FOLLOWUP), label_of(before(HOUR, 1.0), after(0, 3.0)))
        self.assertEqual((UNKNOWN, NO_FOLLOWUP), label_of(before(HOUR, 1.0), result(OPEND - 1, 3.0)))

    def test_the_two_measurements_of_the_absolute_rise_are_at_most_48_hours_apart(self) -> None:
        baseline = before(30 * DAY - 1, 1.0)
        self.assertEqual(POSITIVE, label_of(baseline, result(OPEND, 1.0), result(OPEND + 48 * HOUR, 1.3))[0])
        self.assertEqual(NEGATIVE, label_of(baseline, result(OPEND - 1, 1.0), result(OPEND + 48 * HOUR, 1.3))[0])

    def test_two_results_at_one_time_are_not_a_rise(self) -> None:
        self.assertEqual(NEGATIVE, label_of(before(30 * DAY - 1, 1.0), result(OPEND + HOUR, 1.0, 1), result(OPEND + HOUR, 1.3, 2))[0])

    def test_the_counts_of_the_results_in_each_window_are_recorded(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0), after(48 * HOUR, 1.0), after(48 * HOUR + 1, 1.0), after(7 * DAY, 1.0)])
        self.assertEqual((2, 2), (outcome.early_results, outcome.late_results))
        self.assertEqual(OPEND + 7 * DAY, outcome.latest_time_s)

    def test_the_windows_are_recorded(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0)])
        self.assertEqual((ANESTART - 30 * DAY, ANESTART), (outcome.baseline_window_start_s, outcome.baseline_window_end_s))
        self.assertEqual((OPEND, OPEND + 48 * HOUR, OPEND + 7 * DAY), (outcome.followup_start_s, outcome.absolute_window_end_s, outcome.relative_window_end_s))


class Baseline(unittest.TestCase):
    def test_the_baseline_is_the_last_dated_result_before_anaesthesia_with_its_time_and_source(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(5 * DAY, 0.7), result(ANESTART - 2 * DAY, 1.1, 2), after(HOUR, 1.0)])
        self.assertEqual(result(ANESTART - 2 * DAY, 1.1, 2), outcome.baseline)

    def test_a_result_at_or_after_the_start_of_anaesthesia_is_not_a_baseline(self) -> None:
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(result(ANESTART, 1.0), after(HOUR, 3.0)))
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(result(ANESTART + HOUR, 1.0), after(HOUR, 3.0)))

    def test_a_baseline_is_at_most_30_days_old(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(30 * DAY, 1.0), after(HOUR, 1.0)))
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(before(30 * DAY + 1, 1.0), after(HOUR, 1.0)))

    def test_an_older_result_does_not_replace_the_last_one_even_when_the_last_is_too_old(self) -> None:
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(before(40 * DAY, 1.0), before(31 * DAY, 1.0), after(HOUR, 1.0)))

    def test_a_case_with_only_an_undated_baseline_is_unknown_whatever_the_rise(self) -> None:
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(after(HOUR, 9.0), after(2 * HOUR, 9.5)))

    def test_the_postoperative_results_alone_do_not_make_a_baseline(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [after(HOUR, 1.0), after(10 * HOUR, 1.5)])
        self.assertEqual(
            (UNKNOWN, NO_BASELINE, None, None, None), (outcome.label, outcome.reason, outcome.baseline, outcome.absolute_rise, outcome.relative_rise)
        )

    def test_two_results_at_the_last_time_give_the_first_by_occurrence(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [result(ANESTART - HOUR, 1.4, 2), result(ANESTART - HOUR, 1.0, 1), after(HOUR, 1.0)])
        self.assertEqual(result(ANESTART - HOUR, 1.0, 1), outcome.baseline)

    def test_a_result_with_no_value_or_a_value_that_is_not_positive_is_not_a_measurement(self) -> None:
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(before(HOUR, 0.0), after(HOUR, 1.0)))
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(before(HOUR, -1.0), after(HOUR, 1.0)))
        self.assertEqual((UNKNOWN, NO_BASELINE), label_of(before(HOUR, float("nan")), after(HOUR, 1.0)))
        self.assertEqual((UNKNOWN, NO_FOLLOWUP), label_of(before(HOUR, 1.0), after(HOUR, 0.0)))

    def test_the_missing_start_of_anaesthesia_has_no_baseline(self) -> None:
        outcome = kidney_label(None, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0)])
        self.assertEqual((UNKNOWN, NO_BASELINE, None), (outcome.label, outcome.reason, outcome.baseline_window_start_s))


class Eligibility(unittest.TestCase):
    def test_follow_up_on_day_one_only_is_enough(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(HOUR, 1.0), after(DAY, 1.1)))

    def test_follow_up_on_day_five_only_is_enough(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(HOUR, 1.0), after(5 * DAY, 1.1)))

    def test_a_rise_outside_its_window_leaves_the_label_negative(self) -> None:
        self.assertEqual((NEGATIVE, ELIGIBLE), label_of(before(HOUR, 1.0), after(3 * DAY, 1.45)))

    def test_no_follow_up_result_is_unknown_and_so_is_neither_baseline_nor_follow_up(self) -> None:
        self.assertEqual((UNKNOWN, NO_FOLLOWUP), label_of(before(HOUR, 1.0)))
        self.assertEqual((UNKNOWN, NEITHER), label_of())
        self.assertEqual((UNKNOWN, NEITHER), label_of(before(31 * DAY, 1.0), after(8 * DAY, 3.0)))

    def test_an_unknown_label_is_not_eligible_and_holds_no_rise(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0)])
        self.assertEqual((False, None, None, None, None), (outcome.eligible, outcome.absolute_rise, outcome.relative_rise, outcome.earlier, outcome.later))
        known = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0)])
        self.assertEqual((True, False, False), (known.eligible, known.absolute_rise, known.relative_rise))

    def test_an_observed_negative_has_a_baseline_a_follow_up_result_and_no_rise(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(2 * DAY, 1.0), after(DAY, 1.0), after(4 * DAY, 1.2)])
        self.assertEqual((NEGATIVE, ELIGIBLE, False, False), (outcome.label, outcome.reason, outcome.absolute_rise, outcome.relative_rise))

    def test_the_missing_end_of_the_operation_has_no_follow_up(self) -> None:
        outcome = kidney_label(ANESTART, None, [before(HOUR, 1.0), after(HOUR, 3.0)])
        self.assertEqual((UNKNOWN, NO_FOLLOWUP, None), (outcome.label, outcome.reason, outcome.followup_start_s))

    def test_the_order_of_the_results_does_not_matter(self) -> None:
        results = [before(HOUR, 1.0), before(3 * DAY, 0.8), after(HOUR, 1.1), after(40 * HOUR, 1.3), after(5 * DAY, 1.6)]
        self.assertEqual(kidney_label(ANESTART, OPEND, results), kidney_label(ANESTART, OPEND, results[::-1]))


class Times(unittest.TestCase):
    def test_the_label_never_reads_a_result_after_day_7(self) -> None:
        near = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0)])
        far = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0), after(20 * DAY, 9.0)])
        self.assertEqual(near, far)

    def test_the_latest_time_read_is_the_last_result_in_the_follow_up_window(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.0), after(3 * DAY, 1.1), after(9 * DAY, 5.0)])
        self.assertEqual(OPEND + 3 * DAY, outcome.latest_time_s)
        unknown = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0)])
        self.assertEqual(ANESTART - HOUR, unknown.latest_time_s)


if __name__ == "__main__":
    unittest.main()
