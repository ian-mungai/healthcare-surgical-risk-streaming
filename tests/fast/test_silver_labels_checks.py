"""The checks of the label tables, on counts that the Spark job reads from them. No Spark runs here.

Failure modes covered here:

- A defect in the input (a case with two owners, a split that fails its rule, a case with no split row, no case, no development case) is not named alone and
  first, so a table is built on an input that cannot be trusted.
- A label table that does not hold every case, or a table that holds a key twice, is accepted.
- A check about a label returns a count: the labels are outcomes, so a message about a label holds a flag and never a digit (the held-out patients' labels are
  never counted). Only the counts of the cases, which are inputs, are named.
- A check is left out of the list of checks that a result names, or a check of a table is not read from the facts, so a defect passes.
- The facts of a table that omit a check are read as a pass.
"""

from __future__ import annotations

import dataclasses
import re
import unittest

from spark.silver.label_checks import (
    CHECK_NAMES,
    ICU,
    KIDNEY,
    MEASURE,
    ROW_CHECKS,
    TABLE_CHECKS,
    InputFacts,
    TableFacts,
    input_problems,
    table_problems,
)

INPUT = InputFacts(cases=6388, ambiguous_owner=0, cases_without_split=0, split_problems=(), development_cases=5000)
MESSAGE = re.compile(r"[a-z_]+: (\d+ [a-z]+s?|failed)")


def good(table: str, rows: int = 6388) -> TableFacts:
    return TableFacts(rows, {name: 0 for name in TABLE_CHECKS[table]})


class Input(unittest.TestCase):
    def test_a_good_input_has_no_problem(self) -> None:
        self.assertEqual([], input_problems(INPUT))

    def test_a_case_with_more_than_one_owner_is_one_flag_and_comes_before_every_other_problem(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, ambiguous_owner=1, cases_without_split=1, cases=0, split_problems=("two_sets: 1 patient",)))
        self.assertEqual(["ambiguous_case_owner: failed"], got)

    def test_a_split_table_that_fails_its_own_rule_is_named_alone_before_any_dependent_check(self) -> None:
        got = input_problems(dataclasses.replace(INPUT, split_problems=("two_sets: 1 patient", "unknown_set: 2 patients"), cases_without_split=1, cases=0))
        self.assertEqual(["two_sets: 1 patient", "unknown_set: 2 patients"], got)

    def test_a_case_with_no_split_row_is_one_flag_whatever_the_count(self) -> None:
        for count in (1, 7):
            self.assertEqual(["case_without_split: failed"], input_problems(dataclasses.replace(INPUT, cases_without_split=count)))

    def test_no_case_is_an_empty_input(self) -> None:
        self.assertEqual(["input_empty: 0 rows"], input_problems(dataclasses.replace(INPUT, cases=0, development_cases=0)))

    def test_no_development_case_is_named_when_there_are_cases(self) -> None:
        self.assertEqual(["development_empty: 0 cases"], input_problems(dataclasses.replace(INPUT, development_cases=0)))


class Tables(unittest.TestCase):
    def test_a_good_table_has_no_problem(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            with self.subTest(table=table):
                self.assertEqual([], table_problems(INPUT.cases, table, good(table)))

    def test_a_label_table_holds_every_case_and_names_how_many_are_missing_or_added(self) -> None:
        self.assertEqual(["icu_not_every_case: 3 cases"], table_problems(INPUT.cases, ICU, good(ICU, INPUT.cases - 3)))
        self.assertEqual(["kidney_not_every_case: 1 case"], table_problems(INPUT.cases, KIDNEY, good(KIDNEY, INPUT.cases + 1)))
        self.assertEqual({ICU: "icu_not_every_case", KIDNEY: "kidney_not_every_case"}, ROW_CHECKS)

    def test_the_measure_has_no_row_count_check(self) -> None:
        self.assertEqual([], table_problems(INPUT.cases, MEASURE, good(MEASURE, 3)))

    def test_each_defect_is_one_flag_by_its_own_code_whatever_the_count_is(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            for name in TABLE_CHECKS[table]:
                for count in (1, 4, 1234):
                    with self.subTest(table=table, name=name, count=count):
                        facts = TableFacts(INPUT.cases, {**good(table).flags, name: count})
                        self.assertEqual([f"{name}: failed"], table_problems(INPUT.cases, table, facts))

    def test_a_check_that_the_facts_omit_is_an_error_and_not_a_pass(self) -> None:
        for table in (MEASURE, ICU, KIDNEY):
            for name in TABLE_CHECKS[table]:
                with self.subTest(table=table, name=name):
                    flags = {other: 0 for other in TABLE_CHECKS[table] if other != name}
                    with self.assertRaises(KeyError):
                        table_problems(INPUT.cases, table, TableFacts(INPUT.cases, flags))

    def test_no_message_about_a_label_holds_a_digit_except_the_count_of_cases(self) -> None:
        for table in (ICU, KIDNEY):
            facts = TableFacts(INPUT.cases, {name: 77 for name in TABLE_CHECKS[table]})
            problems = table_problems(INPUT.cases, table, facts)
            self.assertEqual(len(TABLE_CHECKS[table]), len(problems))
            self.assertFalse([problem for problem in problems if any(char.isdigit() for char in problem)])


class Names(unittest.TestCase):
    def test_every_problem_is_a_code_with_a_count_or_a_flag_and_every_code_is_a_check_name(self) -> None:
        problems = input_problems(dataclasses.replace(INPUT, ambiguous_owner=1))
        problems += input_problems(dataclasses.replace(INPUT, cases_without_split=1))
        problems += input_problems(dataclasses.replace(INPUT, cases=0, development_cases=0))
        problems += input_problems(dataclasses.replace(INPUT, development_cases=0))
        for table in (MEASURE, ICU, KIDNEY):
            problems += table_problems(INPUT.cases, table, TableFacts(1, {name: 1 for name in TABLE_CHECKS[table]}))
        names = {problem.split(":")[0] for problem in problems}
        self.assertEqual(set(CHECK_NAMES), names)
        for problem in problems:
            self.assertRegex(problem, MESSAGE)


if __name__ == "__main__":
    unittest.main()
