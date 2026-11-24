"""The checks of the reconciliation of Silver with Bronze, on counts that the Spark job reads. No Spark runs here.

Failure modes covered here:

- A Bronze key that no Silver key accounts for (a lost row), a Silver key that no Bronze key backs (an invented row) or a Bronze key that two Silver keys
  claim (a join that fans out) passes because only the row counts were compared.
- A Bronze key that is twice in Bronze, so that "every key is accounted for" counts one key for two rows.
- A row that Silver removed or marked has no disposition from the closed set, or the dispositions do not add up to the keys that were matched.
- A join of a Silver table to the table that it reads (a signal to its track, a laboratory row to its case, a case to its patient's split row) matches no row
  or more than one.
- A held-out patient's rows are compared in the development counts, or a defect in them is not reported as a flag.
- A message holds a case, a track, a time or a value, or a code that is not in the closed list.
"""

from __future__ import annotations

import dataclasses
import re
import unittest
from typing import Any

from features.signal_validity import INVALID_CAUSES
from spark.reconcile.checks import (
    CHECK_NAMES,
    CODES,
    JOIN_TABLE,
    JOINS,
    OWNER_REFUSAL,
    RECONCILIATIONS,
    SOURCE_TABLES,
    SPLIT_REFUSAL,
    JoinFacts,
    KeyFacts,
    ReconcileError,
    held_out_problems,
    join_problems,
    key_problems,
    schema_problems,
)

MESSAGE_FORM = re.compile(r"([a-z_]+): ")
BY_NAME = {reconciliation.name: reconciliation for reconciliation in RECONCILIATIONS}


def clean(name: str) -> KeyFacts:
    dispositions = {disposition: 0 for disposition in BY_NAME[name].dispositions}
    first = BY_NAME[name].dispositions[0]
    dispositions[first] = 10
    return KeyFacts(bronze_keys=10, bronze_repeats=0, matched=10, unaccounted=0, claimed_twice=0, silver_unbacked=0, dispositions=dispositions)


class Closed(unittest.TestCase):
    def test_the_four_joined_tables_are_reconciled_each_with_its_own_dispositions(self) -> None:
        self.assertEqual(["numeric_observations", "waveform_seconds", "cases", "lab_results"], [reconciliation.name for reconciliation in RECONCILIATIONS])
        self.assertEqual(("valid", *INVALID_CAUSES, "duplicate"), BY_NAME["numeric_observations"].dispositions)
        self.assertEqual(("valid", *INVALID_CAUSES), BY_NAME["waveform_seconds"].dispositions)
        self.assertEqual(("kept",), BY_NAME["cases"].dispositions)
        self.assertEqual(("kept", "collapsed_copy"), BY_NAME["lab_results"].dispositions)

    def test_every_table_that_is_read_is_named_with_its_columns(self) -> None:
        wanted = {
            "bronze.numeric_observations",
            "bronze.waveform_seconds",
            "bronze.tracks",
            "bronze.clinical_cases",
            "bronze.lab_results",
            "silver.patient_split",
            "silver.numeric_observations",
            "silver.waveform_seconds",
            "silver.cases",
            "silver.lab_results",
        }
        self.assertEqual(wanted, set(SOURCE_TABLES))
        for table, columns in SOURCE_TABLES.items():
            with self.subTest(table=table):
                self.assertTrue(columns)

    def test_every_check_name_has_a_code_and_a_code_is_a_check_or_a_job_refusal(self) -> None:
        self.assertEqual(len(CHECK_NAMES), len(set(CHECK_NAMES)))
        self.assertTrue(set(CHECK_NAMES) <= CODES)
        self.assertGreaterEqual(len(CHECK_NAMES), 20)


class Keys(unittest.TestCase):
    def test_a_clean_comparison_has_no_problem(self) -> None:
        for name in BY_NAME:
            with self.subTest(name=name):
                self.assertEqual([], key_problems(name, clean(name)))

    def test_each_defect_is_named_with_the_table_and_a_count_of_keys(self) -> None:
        for field, code in (
            ("bronze_repeats", "bronze_key_repeated"),
            ("silver_repeats", "silver_key_repeated"),
            ("unaccounted", "key_unaccounted"),
            ("claimed_twice", "key_claimed_twice"),
            ("silver_unbacked", "key_unbacked"),
        ):
            with self.subTest(field=field):
                changes: dict[str, Any] = {field: 2}
                facts = dataclasses.replace(clean("cases"), **changes)
                self.assertEqual([f"cases_{code}: 2 keys"], key_problems("cases", facts))

    def test_a_single_key_is_named_in_the_singular(self) -> None:
        self.assertEqual(["cases_key_unaccounted: 1 key"], key_problems("cases", dataclasses.replace(clean("cases"), unaccounted=1)))

    def test_a_disposition_that_is_not_recorded_is_named(self) -> None:
        facts = dataclasses.replace(clean("numeric_observations"), matched=12, dispositions={**clean("numeric_observations").dispositions, "unrecorded": 2})
        self.assertEqual(["numeric_observations_disposition_unrecorded: 2 keys"], key_problems("numeric_observations", facts))

    def test_dispositions_that_do_not_add_up_to_the_matched_keys_are_named(self) -> None:
        facts = dataclasses.replace(clean("lab_results"), matched=11)
        self.assertEqual(["lab_results_disposition_total: 1 key"], key_problems("lab_results", facts))

    def test_dispositions_that_add_up_to_more_than_the_matched_keys_are_named_too(self) -> None:
        facts = dataclasses.replace(clean("lab_results"), matched=9)
        self.assertEqual(["lab_results_disposition_total: 1 key"], key_problems("lab_results", facts))

    def test_a_disposition_outside_the_closed_set_is_named(self) -> None:
        facts = dataclasses.replace(clean("cases"), dispositions={"kept": 9, "dropped": 1})
        self.assertEqual(["cases_disposition_unknown: 1 disposition"], key_problems("cases", facts))

    def test_a_table_with_no_bronze_key_is_named(self) -> None:
        empty = KeyFacts(0, 0, 0, 0, 0, 0, {"kept": 0})
        self.assertEqual(["cases_bronze_empty: 0 keys"], key_problems("cases", empty))

    def test_every_cause_is_named_in_one_run(self) -> None:
        facts = dataclasses.replace(clean("cases"), bronze_repeats=1, silver_repeats=5, unaccounted=2, claimed_twice=3, silver_unbacked=4)
        self.assertEqual(
            ["cases_bronze_key_repeated", "cases_silver_key_repeated", "cases_key_unaccounted", "cases_key_claimed_twice", "cases_key_unbacked"],
            [problem.split(":")[0] for problem in key_problems("cases", facts)],
        )


class Joins(unittest.TestCase):
    def test_every_join_has_a_name_and_a_clean_join_has_no_problem(self) -> None:
        self.assertEqual(["numeric_track", "waveform_track", "lab_case", "case_split"], [join.name for join in JOINS])
        for join in JOINS:
            with self.subTest(join=join.name):
                self.assertEqual([], join_problems(join.name, JoinFacts(rows=50, unmatched=0, fanned=0, kind_wrong=0)))

    def test_a_row_with_no_partner_a_row_with_more_than_one_and_a_row_of_the_wrong_kind_are_named_apart(self) -> None:
        self.assertEqual(
            ["lab_case_join_unmatched: 3 rows", "lab_case_join_fanned: 1 row", "lab_case_join_kind_wrong: 2 rows"],
            join_problems("lab_case", JoinFacts(rows=50, unmatched=3, fanned=1, kind_wrong=2)),
        )
        self.assertEqual(["numeric_track_join_kind_wrong: 1 row"], join_problems("numeric_track", JoinFacts(rows=50, unmatched=0, fanned=0, kind_wrong=1)))

    def test_a_join_that_is_told_nothing_of_the_kind_has_none_wrong(self) -> None:
        self.assertEqual(0, JoinFacts(rows=5, unmatched=1, fanned=1).kind_wrong)

    def test_a_join_over_no_row_is_named(self) -> None:
        self.assertEqual(["case_split_join_empty: 0 rows"], join_problems("case_split", JoinFacts(rows=0, unmatched=0, fanned=0, kind_wrong=0)))


class JoinOwners(unittest.TestCase):
    def test_each_join_belongs_to_the_table_whose_held_out_flag_takes_its_failures(self) -> None:
        self.assertEqual(
            {"numeric_track": "numeric_observations", "waveform_track": "waveform_seconds", "lab_case": "lab_results", "case_split": "cases"}, JOIN_TABLE
        )
        self.assertEqual({join.name for join in JOINS}, set(JOIN_TABLE))
        self.assertTrue(set(JOIN_TABLE.values()) <= set(BY_NAME))


class HeldOut(unittest.TestCase):
    def test_a_flag_that_is_clear_is_no_problem_and_a_set_flag_is_one_code_with_no_count(self) -> None:
        self.assertEqual([], held_out_problems({name: False for name in BY_NAME}))
        self.assertEqual(
            ["numeric_observations_held_out: a held-out key is not accounted for", "lab_results_held_out: a held-out key is not accounted for"],
            held_out_problems({"numeric_observations": True, "waveform_seconds": False, "cases": False, "lab_results": True}),
        )


class Schemas(unittest.TestCase):
    def found(self) -> dict[str, list[tuple[str, str]] | None]:
        return {table: list(columns) for table, columns in SOURCE_TABLES.items()}

    def test_the_expected_columns_are_no_problem(self) -> None:
        self.assertEqual([], schema_problems(self.found()))

    def test_a_missing_table_and_a_changed_table_are_counted_apart(self) -> None:
        found = self.found()
        found["silver.cases"] = None
        found["bronze.tracks"] = [*SOURCE_TABLES["bronze.tracks"], ("extra", "string")]
        found["silver.lab_results"] = list(SOURCE_TABLES["silver.lab_results"][:-1])
        self.assertEqual(["source_missing: 1 table", "source_schema: 2 tables"], schema_problems(found))

    def test_a_retyped_column_is_a_change(self) -> None:
        found = self.found()
        found["silver.patient_split"] = [(name, "int" if index == 0 else kind) for index, (name, kind) in enumerate(SOURCE_TABLES["silver.patient_split"])]
        self.assertEqual(["source_schema: 1 table"], schema_problems(found))


class Messages(unittest.TestCase):
    def test_every_message_is_a_known_code_and_never_a_value(self) -> None:
        bad = KeyFacts(5, 1, 3, 1, 1, 1, {"kept": 1, "dropped": 1})
        problems = [
            *(problem for reconciliation in RECONCILIATIONS for problem in key_problems(reconciliation.name, bad)),
            *(problem for join in JOINS for problem in join_problems(join.name, JoinFacts(5, 1, 1, 1))),
            SPLIT_REFUSAL,
            OWNER_REFUSAL,
            *held_out_problems({reconciliation.name: True for reconciliation in RECONCILIATIONS}),
            *schema_problems({}),
        ]
        self.assertTrue(problems)
        for problem in problems:
            with self.subTest(problem=problem):
                matched = MESSAGE_FORM.match(problem)
                if matched is None:
                    self.fail("a message that is not a code and a count")
                self.assertIn(matched.group(1), CODES)
                self.assertNotRegex(problem, r"[0-9]{4,}")

    def test_a_refusal_before_any_count_is_a_fixed_code_with_no_count(self) -> None:
        self.assertEqual("split_rule_broken: the patient split table does not follow the split rule", SPLIT_REFUSAL)
        self.assertEqual("ambiguous_case_owner: failed", OWNER_REFUSAL)
        for message in (SPLIT_REFUSAL, OWNER_REFUSAL):
            with self.subTest(message=message):
                self.assertNotRegex(message, r"[0-9]")
                self.assertIn(message.split(":")[0], CODES)
                self.assertIn(message.split(":")[0], CHECK_NAMES)

    def test_the_error_keeps_one_message_for_each_cause(self) -> None:
        error = ReconcileError("a: 1 key", "b: 2 keys")
        self.assertEqual(["a: 1 key", "b: 2 keys"], error.problems)
        self.assertEqual("a: 1 key; b: 2 keys", str(error))


if __name__ == "__main__":
    unittest.main()
