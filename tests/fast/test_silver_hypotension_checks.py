"""The checks of the pressure series and the event table, on counts that the Spark job reads. No Spark runs here.

Failure modes covered here:

- An input that cannot be used (no arterial second, a key missing, a second twice, a case twice in the clinical table) reaches the write.
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
from spark.silver.hypotension_checks import (
    CHECK_NAMES,
    EventFacts,
    InputFacts,
    SeriesFacts,
    event_problems,
    input_problems,
    series_problems,
)

INPUT = InputFacts(art_seconds=5000, null_keys=0, duplicate_seconds=0, clinical_duplicate_cases=0)
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
EVENTS = EventFacts(duplicate_keys=0, short=0, times_differ=0, differs=0, cases_without_operation=2)
MESSAGE = re.compile(r"[a-z_]+: \d+ [a-z]+s?")


class Input(unittest.TestCase):
    def test_a_good_input_has_no_problem(self) -> None:
        self.assertEqual([], input_problems(INPUT))

    def test_an_input_with_no_arterial_second_is_empty(self) -> None:
        self.assertEqual(["input_empty: 0 rows"], input_problems(dataclasses.replace(INPUT, art_seconds=0)))

    def test_each_defect_is_named_once_with_its_count(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, null_keys=2, duplicate_seconds=1, clinical_duplicate_cases=3))
        self.assertEqual(["waveform_null_key: 2 rows", "waveform_duplicate_second: 1 row", "clinical_duplicate_case: 3 cases"], got)


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

    def test_each_defect_is_named_by_its_own_code(self) -> None:
        for field, code in (("duplicate_keys", "event_duplicate_key"), ("short", "event_short"), ("times_differ", "event_times"), ("differs", "event_differs")):
            with self.subTest(field=field):
                (problem,) = event_problems(dataclasses.replace(EVENTS, **{field: 4}))
                self.assertTrue(problem.startswith(code + ": 4 "), problem)


class Names(unittest.TestCase):
    def test_every_problem_is_a_code_and_a_count_and_every_code_is_a_check_name(self) -> None:
        bad_input = dataclasses.replace(INPUT, art_seconds=7, null_keys=1, duplicate_seconds=1, clinical_duplicate_cases=1)
        bad_series = SeriesFacts(1, 1, 1, 1, 1, 1, 1, 1, {})
        bad_events = EventFacts(1, 1, 1, 1, 1)
        problems = input_problems(bad_input) + series_problems(bad_series, 9) + event_problems(bad_events)
        self.assertEqual(set(CHECK_NAMES) - {"input_empty"}, {problem.split(":")[0] for problem in problems})
        for problem in problems:
            self.assertRegex(problem, MESSAGE)


if __name__ == "__main__":
    unittest.main()
