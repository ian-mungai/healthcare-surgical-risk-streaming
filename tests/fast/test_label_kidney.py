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
- A negative keeps no measurement that it used: with one baseline, follow-up values of 1.1 and of 1.2 give the same label, so a later check cannot
  reproduce it. The row records the highest follow-up result of the first 48 hours and of the 7 days, each with its value, time and number.
- A negative keeps only the highest follow-up results, so the 48-hour comparison that decided it is lost: with a baseline of 1.2 and follow-up of 1.25, 1.00
  and 1.24 (the largest rise is 0.24), the same row is stored when 1.00 becomes 1.02 (the largest rise is 0.22) and when 0.90 is the early result (0.34, a
  positive), because the maxima are the same. The row stores the earlier and the later result that give the largest rise, whether or not it reaches 0.3.
- A boundary rule is an unnamed comparison inside the code, so its status (decided by the plan, a lead assumption, decided by the owner) is not recorded and a
  changed answer of the owner is not one line. Each is a named constant and flipping it changes the result.
- A rule that the owner decided is still marked as pending, so a reader takes the lower value of a baseline tie for an open question.
"""

from __future__ import annotations

import unittest
from decimal import Decimal
from unittest import mock

from features import kidney_label as rule
from features.kidney_label import (
    ABSOLUTE_HOURS,
    ABSOLUTE_RISE_MG_DL,
    ABSOLUTE_WINDOW_END_INCLUSIVE,
    BASELINE_DAYS,
    BASELINE_MAX_AGE_INCLUSIVE,
    BASELINE_STRICTLY_BEFORE_ANESTART,
    BASELINE_TIE_TAKES_LOWER,
    BOUNDARY_STATUS,
    ELIGIBLE,
    FOLLOWUP_DAYS,
    FOLLOWUP_END_INCLUSIVE,
    FOLLOWUP_INCLUDES_OPEND,
    LEAD_ASSUMPTION,
    MEASUREMENT_MINIMUM_EXCLUSIVE,
    NEITHER,
    NO_BASELINE,
    NO_FOLLOWUP,
    OWNER,
    PLAN,
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


class BoundaryRules(unittest.TestCase):
    """Each boundary rule is a named constant with its status. Flipping a constant must change the result at its boundary, so none is dead."""

    def test_each_boundary_rule_has_its_value_and_its_status(self) -> None:
        self.assertEqual(
            {
                "FOLLOWUP_INCLUDES_OPEND": PLAN,
                "FOLLOWUP_END_INCLUSIVE": PLAN,
                "ABSOLUTE_WINDOW_END_INCLUSIVE": PLAN,
                "BASELINE_STRICTLY_BEFORE_ANESTART": PLAN,
                "BASELINE_MAX_AGE_INCLUSIVE": PLAN,
                "MEASUREMENT_MINIMUM_EXCLUSIVE": LEAD_ASSUMPTION,
                "BASELINE_TIE_TAKES_LOWER": OWNER,
            },
            BOUNDARY_STATUS,
        )
        self.assertEqual(
            (False, True, True, True, True, Decimal(0), True),
            (
                FOLLOWUP_INCLUDES_OPEND,
                FOLLOWUP_END_INCLUSIVE,
                ABSOLUTE_WINDOW_END_INCLUSIVE,
                BASELINE_STRICTLY_BEFORE_ANESTART,
                BASELINE_MAX_AGE_INCLUSIVE,
                MEASUREMENT_MINIMUM_EXCLUSIVE,
                BASELINE_TIE_TAKES_LOWER,
            ),
        )
        self.assertEqual(["BASELINE_TIE_TAKES_LOWER"], [name for name, status in BOUNDARY_STATUS.items() if status == OWNER])
        self.assertFalse([name for name, status in BOUNDARY_STATUS.items() if "pending" in status])
        self.assertFalse(hasattr(rule, "PENDING_OWNER"))

    def test_a_flipped_constant_changes_the_result_at_its_boundary(self) -> None:
        cases = (
            ("FOLLOWUP_INCLUDES_OPEND", True, (before(HOUR, 1.0), after(0, 3.0)), (UNKNOWN, NO_FOLLOWUP), (POSITIVE, ELIGIBLE)),
            ("FOLLOWUP_END_INCLUSIVE", False, (before(HOUR, 1.0), after(7 * DAY, 1.5)), (POSITIVE, ELIGIBLE), (UNKNOWN, NO_FOLLOWUP)),
            (
                "ABSOLUTE_WINDOW_END_INCLUSIVE",
                False,
                (before(HOUR, 1.0), result(OPEND, 1.0), after(48 * HOUR, 1.3)),
                (POSITIVE, ELIGIBLE),
                (NEGATIVE, ELIGIBLE),
            ),
            ("BASELINE_STRICTLY_BEFORE_ANESTART", False, (result(ANESTART, 1.0), after(HOUR, 1.0)), (UNKNOWN, NO_BASELINE), (NEGATIVE, ELIGIBLE)),
            ("BASELINE_MAX_AGE_INCLUSIVE", False, (before(30 * DAY, 1.0), after(HOUR, 1.0)), (NEGATIVE, ELIGIBLE), (UNKNOWN, NO_BASELINE)),
            ("MEASUREMENT_MINIMUM_EXCLUSIVE", Decimal(-1), (before(HOUR, 0.0), after(HOUR, 1.0)), (UNKNOWN, NO_BASELINE), (POSITIVE, ELIGIBLE)),
        )
        for name, flipped, results, held, changed in cases:
            with self.subTest(constant=name):
                self.assertEqual(held, label_of(*results))
                with mock.patch.object(rule, name, flipped):
                    self.assertEqual(changed, label_of(*results))

    def test_the_owners_rule_changes_with_one_constant_and_only_the_baseline_moves(self) -> None:
        results = (result(ANESTART - HOUR, 1.4, 2), result(ANESTART - HOUR, 1.0, 1), after(HOUR, 1.0))
        self.assertEqual(result(ANESTART - HOUR, 1.0, 1), kidney_label(ANESTART, OPEND, list(results)).baseline)
        with mock.patch.object(rule, "BASELINE_TIE_TAKES_LOWER", False):
            outcome = kidney_label(ANESTART, OPEND, list(results))
        self.assertEqual(result(ANESTART - HOUR, 1.4, 2), outcome.baseline)


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

    def test_the_pair_of_the_largest_rise_is_reported_in_a_fixed_order(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), before(2 * HOUR, 0.8), after(HOUR, 1.4), after(2 * HOUR, 1.5)])
        self.assertEqual((before(2 * HOUR, 0.8), after(2 * HOUR, 1.5)), (outcome.earlier, outcome.later))
        self.assertEqual(after(2 * HOUR, 1.5), outcome.relative_result)

    def test_equal_largest_rises_give_the_earliest_later_result_then_the_lowest_earlier_value_then_the_latest_then_the_lowest_number(self) -> None:
        tie = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.4), after(2 * HOUR, 1.4)])
        self.assertEqual((before(HOUR, 1.0), after(HOUR, 1.4)), (tie.earlier, tie.later))
        same_value = kidney_label(ANESTART, OPEND, [before(2 * HOUR, 1.0), before(HOUR, 1.0), after(HOUR, 1.4)])
        self.assertEqual(before(HOUR, 1.0), same_value.earlier)
        same_time = kidney_label(ANESTART, OPEND, [result(ANESTART - HOUR, 1.0, 2), result(ANESTART - HOUR, 1.0, 1), after(HOUR, 1.4)])
        self.assertEqual(result(ANESTART - HOUR, 1.0, 1), same_time.earlier)


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

    def test_two_results_at_the_last_time_give_the_lower_value(self) -> None:
        for numbers in ((2, 1), (1, 2)):
            with self.subTest(numbers=numbers):
                outcome = kidney_label(ANESTART, OPEND, [result(ANESTART - HOUR, 1.4, numbers[0]), result(ANESTART - HOUR, 1.0, numbers[1]), after(HOUR, 1.0)])
                self.assertEqual(result(ANESTART - HOUR, 1.0, numbers[1]), outcome.baseline)

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


class Measurements(unittest.TestCase):
    """What a label row keeps of the follow-up: the highest result of the first 48 hours and of the 7 days, so that a negative can be reproduced."""

    def test_a_negative_records_the_highest_result_of_each_window(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.1), after(30 * HOUR, 1.15), after(4 * DAY, 1.2)])
        self.assertEqual((NEGATIVE, ELIGIBLE), (outcome.label, outcome.reason))
        self.assertEqual((after(30 * HOUR, 1.15), after(4 * DAY, 1.2)), (outcome.early_highest, outcome.followup_highest))

    def test_one_baseline_with_follow_up_of_1_1_and_of_1_2_are_not_the_same_row(self) -> None:
        low = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(DAY, 1.1)])
        high = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(DAY, 1.2)])
        self.assertEqual((NEGATIVE, NEGATIVE), (low.label, high.label))
        self.assertNotEqual(low, high)

    def test_the_48_hour_edge_is_in_the_early_window_and_the_next_second_is_not(self) -> None:
        edge = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(48 * HOUR, 1.1), after(48 * HOUR + 1, 1.2)])
        self.assertEqual((after(48 * HOUR, 1.1), after(48 * HOUR + 1, 1.2)), (edge.early_highest, edge.followup_highest))
        late = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(48 * HOUR + 1, 1.2)])
        self.assertEqual((None, after(48 * HOUR + 1, 1.2)), (late.early_highest, late.followup_highest))

    def test_equal_highest_values_give_the_earliest_then_the_lowest_number(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(2 * HOUR, 1.1), result(OPEND + HOUR, 1.1, 2), result(OPEND + HOUR, 1.1, 1)])
        self.assertEqual(result(OPEND + HOUR, 1.1, 1), outcome.early_highest)

    def test_a_result_after_day_7_or_at_opend_is_never_the_highest(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(0, 9.0), after(HOUR, 1.1), after(7 * DAY + 1, 9.0)])
        self.assertEqual((after(HOUR, 1.1), after(HOUR, 1.1)), (outcome.early_highest, outcome.followup_highest))

    def test_an_unknown_label_records_its_follow_up_when_it_has_one(self) -> None:
        no_baseline = kidney_label(ANESTART, OPEND, [after(HOUR, 1.1), after(3 * DAY, 1.2)])
        self.assertEqual((UNKNOWN, after(HOUR, 1.1), after(3 * DAY, 1.2)), (no_baseline.label, no_baseline.early_highest, no_baseline.followup_highest))
        no_followup = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0)])
        self.assertEqual((None, None), (no_followup.early_highest, no_followup.followup_highest))

    def test_the_baseline_is_recorded_with_its_value_and_time_for_a_negative(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [result(ANESTART - 2 * HOUR, 0.9, 3), after(DAY, 1.0)])
        self.assertEqual((NEGATIVE, result(ANESTART - 2 * HOUR, 0.9, 3)), (outcome.label, outcome.baseline))


class Comparison(unittest.TestCase):
    """The row keeps the 48-hour comparison that decided the absolute rule, whether or not it reached 0.3."""

    BASELINE = before(2 * DAY, 1.2)

    def row(self, early: float) -> rule.KidneyLabel:
        return kidney_label(ANESTART, OPEND, [self.BASELINE, after(HOUR, 1.25), after(10 * HOUR, early), after(20 * HOUR, 1.24)])

    def test_a_negative_stores_the_pair_of_the_largest_rise_even_below_the_limit(self) -> None:
        outcome = self.row(1.00)
        self.assertEqual((NEGATIVE, False), (outcome.label, outcome.absolute_rise))
        self.assertEqual((after(10 * HOUR, 1.00), after(20 * HOUR, 1.24)), (outcome.earlier, outcome.later))

    def test_the_three_variants_of_the_reviewer_are_three_different_rows_or_labels(self) -> None:
        base, nearer, deeper = self.row(1.00), self.row(1.02), self.row(0.90)
        # The maxima that the old row kept are the same in all three.
        self.assertEqual((base.early_highest, base.followup_highest), (nearer.early_highest, nearer.followup_highest))
        self.assertEqual((base.early_highest, base.followup_highest), (deeper.early_highest, deeper.followup_highest))
        self.assertEqual((NEGATIVE, NEGATIVE, POSITIVE), (base.label, nearer.label, deeper.label))
        self.assertEqual(3, len({base, nearer, deeper}))
        self.assertEqual(after(10 * HOUR, 1.02), nearer.earlier)
        self.assertEqual(after(10 * HOUR, 0.90), deeper.earlier)

    def test_the_earlier_result_may_be_the_baseline_or_any_result_before_the_later_one_within_48_hours(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0), after(HOUR, 1.1)])
        self.assertEqual((NEGATIVE, before(HOUR, 1.0), after(HOUR, 1.1)), (outcome.label, outcome.earlier, outcome.later))

    def test_an_earlier_result_more_than_48_hours_before_the_later_one_is_not_paired(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 0.5), after(48 * HOUR, 1.4), after(HOUR, 1.4)])
        self.assertEqual((POSITIVE, before(HOUR, 0.5), after(HOUR, 1.4)), (outcome.label, outcome.earlier, outcome.later))
        far = kidney_label(ANESTART, OPEND, [before(30 * DAY - 1, 1.0), after(HOUR, 1.4)])
        self.assertEqual((NEGATIVE, False, None, None), (far.label, far.absolute_rise, far.earlier, far.later))

    def test_the_pair_of_the_largest_difference_is_stored_even_when_every_difference_is_a_fall(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.5), after(HOUR, 1.4), after(2 * HOUR, 1.45)])
        self.assertEqual((NEGATIVE, False, after(HOUR, 1.4), after(2 * HOUR, 1.45)), (outcome.label, outcome.absolute_rise, outcome.earlier, outcome.later))
        down = kidney_label(ANESTART, OPEND, [before(HOUR, 1.5), after(HOUR, 1.4)])
        self.assertEqual((NEGATIVE, before(HOUR, 1.5), after(HOUR, 1.4)), (down.label, down.earlier, down.later))

    def test_the_unknown_label_stores_no_pair(self) -> None:
        outcome = kidney_label(ANESTART, OPEND, [before(HOUR, 1.0)])
        self.assertEqual((None, None, None), (outcome.absolute_rise, outcome.earlier, outcome.later))


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
