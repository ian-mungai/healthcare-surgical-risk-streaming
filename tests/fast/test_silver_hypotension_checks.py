"""The checks of the pressure series and the event table, on counts that the Spark job reads. No Spark runs here.

Failure modes covered here:

- An input that cannot be used (no arterial second, a key missing, a second twice) reaches the write.
- The patient split is trusted before its own rule check, a case owned by two patients or with no split row is joined to a set, or an event is derived for a
  case of a patient outside the development sets: each of these is one refusal code that comes before every other input check. A check that touches the
  held-out patients' events returns a flag ("failed") and never a count, a case or a value.
- The series loses or adds a second: its rows are not the arterial seconds of ``silver.waveform_seconds``.
- A key is twice in either table, or a verdict is not complete (none, a cause on a valid second, none on an invalid one, a name that is not a cause).
- A valid second has no value, or an invalid second keeps one, so a feature could read a value that validity refused.
- A stored reading or a stored event is not the one that the single definition gives from the stored seconds, so a table passes on its own aggregates only.
- An event is shorter than 60 seconds, or its times are not the ones that its onset and duration give.
- A check that has no fault in the data reports one, or a problem message holds anything but a code and a count.
"""

from __future__ import annotations

import dataclasses
import re
import unittest
from typing import Any

from features.signal_validity import FLAT, MISSING
from scripts.sample.split import patient_part
from spark.silver.hypotension_checks import (
    CHECK_NAMES,
    DEVELOPMENT_SETS,
    EventFacts,
    InputFacts,
    SeriesFacts,
    event_problems,
    input_problems,
    series_problems,
    split_rule_problems,
)
from spark.split_table import RULE_VERSION, SET_NAMES, SplitRow

INPUT = InputFacts(art_seconds=5000, null_keys=0, duplicate_seconds=0, ambiguous_owner=0, cases_without_split=0, split_problems=())
SERIES = SeriesFacts(
    rows=5000,
    duplicate_keys=0,
    null_verdict=0,
    valid_with_cause=0,
    invalid_without_cause=0,
    unknown_cause=0,
    value_problem=0,
    differs=0,
    causes={MISSING: 3, FLAT: 7, "valid": 4990},
)
EVENTS = EventFacts(duplicate_keys=0, short=0, times_differ=0, differs=0, held_out_events=0, cases_without_operation=2)
MESSAGE = re.compile(r"[a-z_]+: (\d+ [a-z]+s?|failed)")


class Input(unittest.TestCase):
    def test_a_good_input_has_no_problem(self) -> None:
        self.assertEqual([], input_problems(INPUT))

    def test_an_input_with_no_arterial_second_is_empty(self) -> None:
        self.assertEqual(["input_empty: 0 rows"], input_problems(dataclasses.replace(INPUT, art_seconds=0)))

    def test_each_defect_is_named_once_with_its_count(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, null_keys=2, duplicate_seconds=1))
        self.assertEqual(["waveform_null_key: 2 rows", "waveform_duplicate_second: 1 row"], got)

    def test_a_case_with_more_than_one_owner_is_one_flag_and_comes_before_every_other_problem(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, ambiguous_owner=1, cases_without_split=1, null_keys=2, split_problems=("two_sets: 1 patient",)))
        self.assertEqual(["ambiguous_case_owner: failed"], got)

    def test_a_split_table_that_fails_its_own_rule_is_named_alone_before_any_dependent_check(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, split_problems=("two_sets: 1 patient", "unknown_set: 2 patients"), cases_without_split=1, null_keys=2))
        self.assertEqual(["two_sets: 1 patient", "unknown_set: 2 patients"], got)

    def test_a_case_with_no_split_row_is_one_flag(self) -> None:
        self.assertEqual(["case_without_split: failed"], input_problems(dataclasses.replace(INPUT, cases_without_split=1)))
        self.assertEqual(["case_without_split: failed"], input_problems(dataclasses.replace(INPUT, cases_without_split=7)))


def split_row(patient: int, name: str | None = None, version: str = RULE_VERSION, snapshot: int = 1) -> SplitRow:
    return SplitRow(str(patient), name or SET_NAMES[patient_part(patient)], version, snapshot)


class Development(unittest.TestCase):
    def test_only_the_training_and_the_validation_sets_are_development(self) -> None:
        self.assertEqual((SET_NAMES["train"], SET_NAMES["validation"]), DEVELOPMENT_SETS)
        self.assertNotIn(SET_NAMES["test"], DEVELOPMENT_SETS)

    def test_a_table_that_is_the_rule_has_no_problem(self) -> None:
        self.assertEqual([], split_rule_problems([split_row(patient) for patient in range(1, 400)]))

    def test_each_way_a_set_could_be_wrong_is_named_before_the_set_is_trusted(self) -> None:
        good = split_row(5)
        wrong = SET_NAMES["test"] if good.set != SET_NAMES["test"] else SET_NAMES["train"]
        cases = {
            "rule_mismatch": [split_row(5, wrong)],
            "two_sets": [good, split_row(5, wrong)],
            "unknown_set": [split_row(5, "holdout")],
            "rule_version": [split_row(5, version="other")],
            "mixed_snapshots": [split_row(5), split_row(6, snapshot=2)],
            "empty_table": [],
        }
        for code, rows in cases.items():
            with self.subTest(code=code):
                self.assertIn(code, [problem.split(":")[0] for problem in split_rule_problems(rows)])


class Series(unittest.TestCase):
    def test_a_good_table_has_no_problem(self) -> None:
        self.assertEqual([], series_problems(SERIES, INPUT.art_seconds))

    def test_a_lost_or_added_second_is_named_either_way(self) -> None:
        for rows in (4999, 5001):
            self.assertEqual(["series_not_every_second: 1 second"], series_problems(dataclasses.replace(SERIES, rows=rows), INPUT.art_seconds))

    def test_each_defect_is_named_by_its_own_code(self) -> None:
        for field, code in (
            ("duplicate_keys", "series_duplicate_key"),
            ("null_verdict", "series_verdict_null"),
            ("valid_with_cause", "series_valid_with_cause"),
            ("invalid_without_cause", "series_invalid_without_cause"),
            ("unknown_cause", "series_unknown_cause"),
            ("value_problem", "series_value"),
            ("differs", "series_differs"),
        ):
            with self.subTest(field=field):
                change: dict[str, Any] = {field: 1}
                (problem,) = series_problems(dataclasses.replace(SERIES, **change), INPUT.art_seconds)
                self.assertTrue(problem.startswith(code + ": 1 "), problem)


class Events(unittest.TestCase):
    def test_a_good_table_has_no_problem_and_a_case_without_an_operation_is_not_one(self) -> None:
        self.assertEqual([], event_problems(EVENTS))

    def test_each_defect_is_one_flag_by_its_own_code_whatever_the_count_is(self) -> None:
        for field, code in (
            ("duplicate_keys", "event_duplicate_key"),
            ("short", "event_short"),
            ("times_differ", "event_times"),
            ("differs", "event_differs"),
            ("held_out_events", "event_held_out"),
        ):
            for count in (1, 4, 1234):
                with self.subTest(field=field, count=count):
                    self.assertEqual([f"{code}: failed"], event_problems(dataclasses.replace(EVENTS, **{field: count})))

    def test_no_message_about_events_holds_a_digit(self) -> None:
        problems = event_problems(EventFacts(55, 66, 77, 88, 99, 111))
        self.assertEqual(5, len(problems))
        self.assertFalse([problem for problem in problems if any(char.isdigit() for char in problem)])


class Names(unittest.TestCase):
    def test_every_problem_is_a_code_with_a_count_or_a_flag_and_every_code_is_a_check_name(self) -> None:
        bad_input = dataclasses.replace(INPUT, art_seconds=7, null_keys=1, duplicate_seconds=1)
        bad_series = SeriesFacts(1, 1, 1, 1, 1, 1, 1, 1, {})
        bad_events = EventFacts(1, 1, 1, 1, 1)
        problems = (
            input_problems(bad_input)
            + input_problems(dataclasses.replace(INPUT, ambiguous_owner=1))
            + input_problems(dataclasses.replace(INPUT, cases_without_split=1))
            + series_problems(bad_series, 9)
            + event_problems(bad_events)
        )
        names = {problem.split(":")[0] for problem in problems}
        self.assertEqual(set(CHECK_NAMES) - {"input_empty"}, names)
        for problem in problems:
            self.assertRegex(problem, MESSAGE)


if __name__ == "__main__":
    unittest.main()
