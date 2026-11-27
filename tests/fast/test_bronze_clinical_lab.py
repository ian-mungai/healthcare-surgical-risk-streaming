"""The rules of the Bronze clinical and laboratory tables: which columns pass through, how the source occurrence of a laboratory row is
numbered.

The Spark job applies these rules to the loaded tables. A wrong rule here changes a value in Bronze without a failure, so each test holds
one way that could happen.
"""

from __future__ import annotations

import itertools
import unittest

from spark.bronze_clinical_lab import (
    LAB_COLUMNS,
    OCCURRENCE_COLUMN,
    BronzeRulesError,
    assign_occurrences,
    occurrence_differences,
    payload_columns,
)

STRING = "string"
CLINICAL_FOUND = [("caseid", STRING), ("subjectid", STRING), ("age", STRING), ("_airbyte_raw_id", STRING), ("_airbyte_extracted_at", "timestamp")]
LAB_FOUND = [(name, STRING) for name in LAB_COLUMNS] + [("_airbyte_raw_id", STRING)]


def refusal(call: object) -> str:
    try:
        call()  # type: ignore[operator]
    except BronzeRulesError as error:
        return str(error)
    raise AssertionError("the rules accepted a table that they must refuse")


class ColumnTest(unittest.TestCase):
    def test_the_connectors_own_columns_are_dropped_and_the_published_ones_keep_their_order(self) -> None:
        self.assertEqual(["caseid", "subjectid", "age"], payload_columns("clinical", CLINICAL_FOUND))
        self.assertEqual(list(LAB_COLUMNS), payload_columns("lab", LAB_FOUND))

    def test_a_column_that_is_not_a_string_is_refused_because_a_cast_would_change_values(self) -> None:
        message = refusal(lambda: payload_columns("clinical", [("caseid", "bigint"), ("age", STRING)]))
        self.assertIn("caseid", message)
        self.assertIn("string", message)

    def test_a_clinical_table_without_its_key_column_is_refused(self) -> None:
        self.assertIn("caseid", refusal(lambda: payload_columns("clinical", [("subjectid", STRING)])))

    def test_a_laboratory_table_with_another_column_list_is_refused(self) -> None:
        self.assertIn("caseid, dt, name, result", refusal(lambda: payload_columns("lab", LAB_FOUND[:3])))
        self.assertIn("caseid, dt, name, result", refusal(lambda: payload_columns("lab", [*LAB_FOUND, ("unit", STRING)])))

    def test_a_published_column_named_like_the_occurrence_column_is_refused(self) -> None:
        self.assertIn(OCCURRENCE_COLUMN, refusal(lambda: payload_columns("clinical", [("caseid", STRING), (OCCURRENCE_COLUMN, STRING)])))

    def test_a_repeated_column_name_is_refused(self) -> None:
        self.assertIn("repeated", refusal(lambda: payload_columns("clinical", [("caseid", STRING), ("age", STRING), ("age", STRING)])))


class OccurrenceTest(unittest.TestCase):
    def test_rows_that_share_case_time_and_test_get_one_to_n_and_a_lone_row_gets_one(self) -> None:
        rows = [("1", "-5", "k", "4.1"), ("1", "-5", "k", "4.0"), ("1", "-5", "na", "139"), ("2", "-5", "k", "3.9")]
        numbered = {row[:3] + (row[4],): row[3] for row in assign_occurrences(rows)}
        self.assertEqual({("1", "-5", "k", 1): "4.0", ("1", "-5", "k", 2): "4.1", ("1", "-5", "na", 1): "139", ("2", "-5", "k", 1): "3.9"}, numbered)

    def test_two_identical_rows_are_two_occurrences_so_neither_is_lost(self) -> None:
        numbered = assign_occurrences([("98", "-2604209", "k", "3.9"), ("98", "-2604209", "k", "3.9")])
        self.assertEqual([1, 2], [row[4] for row in numbered])
        self.assertEqual(2, len(set(numbered)))

    def test_the_numbering_does_not_depend_on_the_order_the_rows_arrive_in(self) -> None:
        rows = [("464", "-132132", "alb", value) for value in ("3.1", "2.9", "3.1", "3.0")] + [("1", "7", "k", "4.2"), ("464", "9", "alb", "3.3")]
        expected = assign_occurrences(rows)
        for shift in range(len(rows)):
            for arranged in (rows[shift:] + rows[:shift], (rows[shift:] + rows[:shift])[::-1]):
                self.assertEqual(expected, assign_occurrences(arranged))

    def test_a_result_is_ranked_as_text_so_no_number_is_parsed(self) -> None:
        numbered = assign_occurrences([("1", "1", "k", "10"), ("1", "1", "k", "9"), ("1", "1", "k", "9.0")])
        self.assertEqual(["10", "9", "9.0"], [row[3] for row in numbered])

    def test_a_missing_result_ranks_first_and_stays_missing(self) -> None:
        numbered = assign_occurrences([("1", "1", "k", "4"), ("1", "1", "k", None)])
        self.assertEqual([(None, 1), ("4", 2)], [(row[3], row[4]) for row in numbered])

    def test_case_and_time_are_kept_as_published_so_a_leading_zero_makes_a_different_group(self) -> None:
        numbered = assign_occurrences([("007", "5", "k", "1"), ("7", "5", "k", "1")])
        self.assertEqual([1, 1], [row[4] for row in numbered])

    def test_every_key_is_unique_and_numbers_run_from_one_without_a_gap_for_any_order(self) -> None:
        base = [("1", "1", "k", value) for value in ("a", "a", "b")] + [("1", "2", "k", "a")]
        for permutation in itertools.permutations(base):
            numbered = assign_occurrences(permutation)
            keys = [row[:3] + (row[4],) for row in numbered]
            self.assertEqual(len(keys), len(set(keys)))
            self.assertEqual([1, 2, 3, 1], [row[4] for row in numbered])


class IndependentOccurrenceCheckTest(unittest.TestCase):
    ROWS = [("1", "5", "k", "4.0"), ("1", "5", "k", "4.0"), ("1", "5", "k", "4.1"), ("2", "6", "na", None), ("2", "6", "na", "139")]

    def test_the_numbers_of_assign_occurrences_have_no_difference_in_any_order(self) -> None:
        numbered = assign_occurrences(self.ROWS)
        self.assertEqual(0, occurrence_differences(numbered))
        self.assertEqual(0, occurrence_differences(numbered[::-1]))

    def test_a_dense_rank_differs_because_identical_rows_get_the_same_number(self) -> None:
        dense = [("1", "5", "k", "4.0", 1), ("1", "5", "k", "4.0", 1), ("1", "5", "k", "4.1", 2), ("2", "6", "na", None, 1), ("2", "6", "na", "139", 2)]
        self.assertGreater(occurrence_differences(dense), 0)

    def test_a_reversed_rank_a_swapped_pair_and_a_dropped_row_each_differ(self) -> None:
        good = assign_occurrences(self.ROWS)
        reversed_rank = [(*row[:4], {1: 2, 2: 1}[row[4]]) if row[:3] == ("2", "6", "na") else row for row in good]
        swapped = [(*row[:4], {2: 3, 3: 2}.get(row[4], row[4])) if row[:3] == ("1", "5", "k") else row for row in good]
        for name, rows in (("reversed", reversed_rank), ("swapped", swapped), ("dropped row", good[1:])):
            with self.subTest(name):
                self.assertGreater(occurrence_differences(rows), 0)


class CodeTest(unittest.TestCase):
    """A refusal carries a fixed code for the output boundary, so that no column name or count has to leave the job."""

    def code(self, call: object) -> str:
        try:
            call()  # type: ignore[operator]
        except BronzeRulesError as error:
            return error.code
        raise AssertionError("the rules accepted a table that they must refuse")

    def test_a_refused_table_has_the_code_columns_refused(self) -> None:
        for call in (
            lambda: payload_columns("clinical", [("caseid", "bigint")]),
            lambda: payload_columns("clinical", [("subjectid", STRING)]),
            lambda: payload_columns("lab", LAB_FOUND[:3]),
            lambda: payload_columns("clinical", [("caseid", STRING), (OCCURRENCE_COLUMN, STRING)]),
            lambda: payload_columns("clinical", [("caseid", STRING), ("caseid", STRING)]),
        ):
            self.assertEqual("columns_refused", self.code(call))

    def test_an_error_without_a_code_is_rules_broken_and_a_code_is_kept(self) -> None:
        self.assertEqual("rules_broken", BronzeRulesError("x").code)
        self.assertEqual("source_table_missing", BronzeRulesError("x", code="source_table_missing").code)


if __name__ == "__main__":
    unittest.main()
