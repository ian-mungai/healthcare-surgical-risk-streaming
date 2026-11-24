"""The checks of the Silver signal tables, on counts that the Spark job reads. No Spark runs here.

Failure modes covered here:

- A Bronze input that cannot be cleaned (a key missing, a second twice, a reading whose positions are not 0 to n - 1 once each, a signal without its
  track) reaches the write.
- A reading is lost or counted twice: the Silver rows and the ledger of dropped duplicates do not add up to the Bronze rows.
- A key is twice in a Silver table, a kept reading is not the first of its time, or a second is lost.
- A kept reading, a ledger or a stored second is not the Bronze row (and the physical samples) that it came from, and the table still passes because
  only its own aggregates were checked.
- A verdict is not what the one validity definition gives for the stored samples, has no cause when invalid, has a cause when valid or names a cause
  that is not one.
- A check that has no fault in the data reports one."""

from __future__ import annotations

import dataclasses
import re
import unittest
from typing import Any

from spark.silver.checks import (
    CHECK_NAMES,
    CODES,
    BronzeFacts,
    NumericFacts,
    WaveformFacts,
    bronze_problems,
    numeric_problems,
    waveform_problems,
)

MESSAGE_FORM = re.compile(r"([a-z_]+): ")

BRONZE = BronzeFacts(
    input_empty=0,
    numeric_null_keys=0,
    numeric_occurrence_gaps=0,
    numeric_without_track=0,
    waveform_null_keys=0,
    waveform_duplicate_seconds=0,
    waveform_without_track=0,
    track_duplicates=0,
)
NUMERIC = NumericFacts(
    duplicate_keys=0,
    not_first=0,
    null_verdict=0,
    valid_with_cause=0,
    invalid_without_cause=0,
    unknown_cause=0,
    verdict_differs=0,
)
WAVEFORM = WaveformFacts(
    duplicate_keys=0,
    null_verdict=0,
    valid_with_cause=0,
    invalid_without_cause=0,
    unknown_cause=0,
    shape_differs=0,
    verdict_differs=0,
)


def with_count(facts: Any, field: str) -> Any:
    return dataclasses.replace(facts, **{field: 1})


def code_of(problem: str) -> str:
    match = MESSAGE_FORM.match(problem)
    if match is None:
        raise AssertionError(problem)
    return match.group(1)


def one(problems: list[str]) -> str:
    if len(problems) != 1:
        raise AssertionError(problems)
    return code_of(problems[0])


class Controls(unittest.TestCase):
    def test_a_clean_input_and_clean_tables_have_no_problem(self) -> None:
        self.assertEqual([], bronze_problems(BRONZE))
        self.assertEqual([], numeric_problems(NUMERIC))
        self.assertEqual([], waveform_problems(WAVEFORM))

    def test_every_message_is_a_known_code_and_a_flag_with_no_count(self) -> None:
        planted = [
            *bronze_problems(BronzeFacts(1, 1, 1, 1, 1, 1, 1, 1)),
            *bronze_problems(BronzeFacts(0, 0, 0, 0, 0, 0, 0, 0)),
            *numeric_problems(NumericFacts(1, 1, 1, 1, 1, 1, 1, source_differs=1, row_count_differs=1)),
            *waveform_problems(WaveformFacts(1, 1, 1, 1, 1, 1, 1, source_differs=1, row_count_differs=1)),
        ]
        self.assertGreaterEqual(len(planted), 16)
        for problem in planted:
            with self.subTest(problem=problem):
                self.assertIn(code_of(problem), CODES)
                self.assertEqual(f"{code_of(problem)}: failed", problem)

    def test_a_larger_count_gives_the_same_message_as_a_smaller_one(self) -> None:
        few = [*bronze_problems(BronzeFacts(0, 1, 1, 1, 1, 1, 1, 1)), *numeric_problems(NumericFacts(1, 1, 1, 1, 1, 1, 1, source_differs=1))]
        many = [*bronze_problems(BronzeFacts(0, 8, 8, 8, 8, 8, 8, 8)), *numeric_problems(NumericFacts(7, 6, 5, 4, 3, 2, 9, source_differs=8))]
        self.assertEqual(few, many)

    def test_the_facts_hold_no_count_of_a_cause(self) -> None:
        for facts in (NUMERIC, WAVEFORM):
            self.assertNotIn("causes", {field.name for field in dataclasses.fields(facts)})

    def test_the_result_names_the_checks_that_ran(self) -> None:
        self.assertEqual(len(CHECK_NAMES), len(set(CHECK_NAMES)))
        self.assertTrue(set(CHECK_NAMES) <= CODES)


class Inputs(unittest.TestCase):
    def test_each_unusable_input_is_one_code(self) -> None:
        for field, code in (
            ("numeric_null_keys", "numeric_null_key"),
            ("numeric_occurrence_gaps", "numeric_occurrence_gap"),
            ("numeric_without_track", "numeric_without_track"),
            ("waveform_null_keys", "waveform_null_key"),
            ("waveform_duplicate_seconds", "waveform_duplicate_second"),
            ("waveform_without_track", "waveform_without_track"),
            ("track_duplicates", "track_duplicate"),
        ):
            with self.subTest(field=field):
                self.assertEqual(code, one(bronze_problems(dataclasses.replace(BRONZE, **{field: 2}))))

    def test_an_input_with_no_reading_and_no_second_is_empty(self) -> None:
        self.assertEqual({"input_empty"}, {code_of(problem) for problem in bronze_problems(dataclasses.replace(BRONZE, input_empty=1))})
        self.assertEqual([], bronze_problems(dataclasses.replace(BRONZE, input_empty=0)))


class Numeric(unittest.TestCase):
    def test_a_reading_lost_or_counted_twice_is_the_flag_of_the_row_count_expectation(self) -> None:
        self.assertEqual("numeric_not_every_reading", one(numeric_problems(dataclasses.replace(NUMERIC, row_count_differs=1))))
        self.assertEqual([], numeric_problems(dataclasses.replace(NUMERIC, row_count_differs=0)))

    def test_a_key_twice_or_a_kept_reading_that_is_not_the_first_is_counted(self) -> None:
        self.assertEqual("numeric_duplicate_key", one(numeric_problems(dataclasses.replace(NUMERIC, duplicate_keys=3))))
        self.assertEqual("numeric_kept_not_first", one(numeric_problems(dataclasses.replace(NUMERIC, not_first=1))))

    def test_a_verdict_that_is_not_complete_is_counted(self) -> None:
        for field, code in (
            ("null_verdict", "numeric_verdict_null"),
            ("valid_with_cause", "numeric_valid_with_cause"),
            ("invalid_without_cause", "numeric_invalid_without_cause"),
            ("unknown_cause", "numeric_unknown_cause"),
            ("verdict_differs", "numeric_verdict_differs"),
            ("source_differs", "numeric_source_differs"),
        ):
            with self.subTest(field=field):
                self.assertEqual(code, one(numeric_problems(with_count(NUMERIC, field))))


class Waveform(unittest.TestCase):
    def test_a_second_lost_is_the_flag_of_the_row_count_expectation(self) -> None:
        self.assertEqual("waveform_not_every_second", one(waveform_problems(dataclasses.replace(WAVEFORM, row_count_differs=1))))
        self.assertEqual([], waveform_problems(dataclasses.replace(WAVEFORM, row_count_differs=0)))

    def test_a_key_twice_or_a_wrong_sample_count_is_counted(self) -> None:
        self.assertEqual("waveform_duplicate_key", one(waveform_problems(dataclasses.replace(WAVEFORM, duplicate_keys=1))))
        self.assertEqual("waveform_shape", one(waveform_problems(dataclasses.replace(WAVEFORM, shape_differs=1))))

    def test_a_verdict_that_is_not_complete_is_counted(self) -> None:
        for field, code in (
            ("null_verdict", "waveform_verdict_null"),
            ("valid_with_cause", "waveform_valid_with_cause"),
            ("invalid_without_cause", "waveform_invalid_without_cause"),
            ("unknown_cause", "waveform_unknown_cause"),
            ("verdict_differs", "waveform_verdict_differs"),
            ("source_differs", "waveform_source_differs"),
        ):
            with self.subTest(field=field):
                self.assertEqual(code, one(waveform_problems(with_count(WAVEFORM, field))))


if __name__ == "__main__":
    unittest.main()
