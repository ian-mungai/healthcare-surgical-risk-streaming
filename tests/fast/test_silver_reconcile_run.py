"""The order of the reconciliation job, with stand-in Spark steps.

Failure modes covered here:

- A patient split that breaks the split rule (a test-set patient labelled training) or a Bronze case with two owners is counted as development, and the run
  passes.
- A refusal comes after counts were made, or holds a count, so that a count of a held-out patient leaves the job.
- A table is compared before its columns are checked, so a table with another shape fails inside a query with text of its own.
- One defect stops the run, so the next run names the next cause: every table, join and flag is judged before the job refuses.
- The branch moves while the job reads and the job still reports success, or a result names a hash that the branch no longer has.
- The job writes: it holds no step that writes, and the order of its calls reads and then checks the head.
- The result holds a case, a track, a time, a value or a count of a held-out patient's rows, or leaves out a table, a disposition or a check.
- A table or a join that is added to the lists is not reconciled, or a check that ran is not named in the result.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from spark.reconcile.checks import JOINS, OWNER_REFUSAL, RECONCILIATIONS, SPLIT_REFUSAL, JoinFacts, KeyFacts, ReconcileError
from spark.reconcile.run import BRANCH_MOVED, Operations, run_verify

HEAD = "a" * 64
OTHER = "b" * 64
NAMES = [reconciliation.name for reconciliation in RECONCILIATIONS]


def clean(name: str) -> KeyFacts:
    dispositions = {disposition: 0 for disposition in next(r for r in RECONCILIATIONS if r.name == name).dispositions}
    dispositions[next(iter(dispositions))] = 7
    return KeyFacts(bronze_keys=8, bronze_repeats=0, matched=7, unaccounted=0, claimed_twice=0, silver_unbacked=0, dispositions=dispositions)


class Fake:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.moved_after: int | None = None
        self.schema: list[str] = []
        self.refused: list[str] = []
        self.keys = {name: clean(name) for name in NAMES}
        self.flags = {name: False for name in NAMES}
        self.joins = {join.name: JoinFacts(50, 0, 0) for join in JOINS}

    def head(self) -> str:
        self.calls.append("head")
        if self.moved_after is not None and self.calls.count("head") > self.moved_after:
            return OTHER
        return self.current

    def source_problems(self) -> list[str]:
        self.calls.append("source_problems")
        return self.schema

    def refusals(self) -> list[str]:
        self.calls.append("refusals")
        return self.refused

    def key_facts(self, name: str) -> KeyFacts:
        self.calls.append(f"key_facts {name}")
        return self.keys[name]

    def held_out_problem(self, name: str) -> bool:
        self.calls.append(f"held_out {name}")
        return self.flags[name]

    def join_facts(self, name: str) -> JoinFacts:
        self.calls.append(f"join_facts {name}")
        return self.joins[name]

    def snapshot_ids(self) -> dict[str, int | None]:
        self.calls.append("snapshot_ids")
        return {"silver.cases": 5, "bronze.clinical_cases": 4}

    def operations(self) -> Operations:
        return Operations(self.head, self.source_problems, self.refusals, self.key_facts, self.held_out_problem, self.join_facts, self.snapshot_ids)


def codes(error: ReconcileError) -> list[str]:
    return [problem.split(":")[0] for problem in error.problems]


class Verify(unittest.TestCase):
    def test_a_good_branch_checks_the_columns_first_then_each_table_and_join_and_ends_on_the_head(self) -> None:
        fake = Fake()
        outcome = run_verify(fake.operations(), HEAD)
        expected = ["source_problems", "refusals"]
        for name in NAMES:
            expected += [f"key_facts {name}", f"held_out {name}"]
        expected += [f"join_facts {join.name}" for join in JOINS]
        expected += ["snapshot_ids", "head"]
        self.assertEqual(expected, fake.calls)
        self.assertEqual(("verified", HEAD), (outcome.outcome, outcome.branch_hash))

    def test_the_result_holds_hashes_snapshots_counts_dispositions_and_flags_and_nothing_of_a_row(self) -> None:
        facts = run_verify(Fake().operations(), HEAD).facts()
        self.assertEqual("verified", facts["outcome"])
        self.assertEqual(HEAD, facts["branch_hash"])
        self.assertEqual({"silver.cases": 5, "bronze.clinical_cases": 4}, facts["snapshots"])
        self.assertEqual(NAMES, list(facts["reconciled"]))
        cases = facts["reconciled"]["cases"]
        self.assertEqual({"bronze_keys": 8, "matched_keys": 7, "dispositions": {"kept": 7}, "held_out_clean": True}, cases)
        self.assertEqual([join.name for join in JOINS], list(facts["joins"]))
        self.assertEqual({"rows": 50}, facts["joins"]["lab_case"])
        self.assertGreaterEqual(len(facts["checks_run"]), 20)
        self.assertEqual(sorted(facts["checks_run"]), sorted(set(facts["checks_run"])))

    def test_a_held_out_patient_is_a_flag_and_never_a_count(self) -> None:
        facts = run_verify(Fake().operations(), HEAD).facts()
        for name in NAMES:
            with self.subTest(name=name):
                self.assertEqual({"bronze_keys", "matched_keys", "dispositions", "held_out_clean"}, set(facts["reconciled"][name]))
                self.assertIs(True, facts["reconciled"][name]["held_out_clean"])

    def test_a_table_with_another_shape_is_named_before_any_query_runs(self) -> None:
        fake = Fake()
        fake.schema = ["source_schema: 1 table"]
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["source_schema"], codes(caught.exception))
        self.assertEqual(["source_problems"], fake.calls)

    def test_every_cause_is_named_in_one_run(self) -> None:
        fake = Fake()
        fake.keys["numeric_observations"] = dataclasses.replace(clean("numeric_observations"), unaccounted=1)
        fake.keys["lab_results"] = dataclasses.replace(clean("lab_results"), silver_unbacked=2)
        fake.flags["cases"] = True
        fake.joins["case_split"] = JoinFacts(50, 1, 0)
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(
            ["numeric_observations_key_unaccounted", "lab_results_key_unbacked", "cases_held_out", "case_split_join_unmatched"], codes(caught.exception)
        )

    def test_a_split_that_breaks_the_rule_refuses_the_run_before_any_count_is_made(self) -> None:
        fake = Fake()
        fake.refused = [SPLIT_REFUSAL]
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual([SPLIT_REFUSAL], caught.exception.problems)
        self.assertEqual(["source_problems", "refusals"], fake.calls)

    def test_an_ambiguous_owner_refuses_the_run_before_any_count_is_made(self) -> None:
        fake = Fake()
        fake.refused = [OWNER_REFUSAL]
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual([OWNER_REFUSAL], caught.exception.problems)
        self.assertEqual(["source_problems", "refusals"], fake.calls)

    def test_both_refusals_are_named_and_a_refusal_holds_no_count_even_when_every_count_is_bad(self) -> None:
        fake = Fake()
        fake.refused = [SPLIT_REFUSAL, OWNER_REFUSAL]
        fake.keys["cases"] = dataclasses.replace(clean("cases"), unaccounted=3)
        fake.flags["cases"] = True
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["split_rule_broken", "ambiguous_case_owner"], codes(caught.exception))
        for problem in caught.exception.problems:
            self.assertNotRegex(problem, r"[0-9]")

    def test_a_table_with_another_shape_is_refused_before_the_refusals_are_read(self) -> None:
        fake = Fake()
        fake.schema = ["source_missing: 1 table"]
        fake.refused = [SPLIT_REFUSAL]
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["source_missing"], codes(caught.exception))
        self.assertEqual(["source_problems"], fake.calls)

    def test_a_branch_that_moved_during_the_reads_is_named_and_no_result_is_given(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED.split(":")[0]], codes(caught.exception))

    def test_a_failure_after_every_flag_was_judged_carries_one_clean_flag_for_each_table(self) -> None:
        fake = Fake()
        fake.flags["cases"] = True
        fake.keys["lab_results"] = dataclasses.replace(clean("lab_results"), unaccounted=1)
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual({name: name != "cases" for name in NAMES}, caught.exception.held_out_clean)
        self.assertEqual(NAMES, list(caught.exception.held_out_clean or {}))
        moved = Fake()
        moved.moved_after = 0
        with self.assertRaises(ReconcileError) as caught:
            run_verify(moved.operations(), HEAD)
        self.assertEqual(dict.fromkeys(NAMES, True), caught.exception.held_out_clean)

    def test_a_failure_before_the_flags_are_judged_carries_no_flag(self) -> None:
        for field, value in (("schema", ["source_schema: 1 table"]), ("refused", [SPLIT_REFUSAL]), ("refused", [OWNER_REFUSAL])):
            fake = Fake()
            setattr(fake, field, value)
            with self.subTest(field=field, value=value), self.assertRaises(ReconcileError) as caught:
                run_verify(fake.operations(), HEAD)
            self.assertIsNone(caught.exception.held_out_clean)

    def test_an_error_made_by_hand_has_no_flag(self) -> None:
        self.assertIsNone(ReconcileError("cases_key_unaccounted: 1 key").held_out_clean)

    def test_a_defect_and_a_moved_branch_are_both_named(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        fake.flags["waveform_seconds"] = True
        with self.assertRaises(ReconcileError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["waveform_seconds_held_out", "branch_moved"], codes(caught.exception))

    def test_the_job_has_no_step_that_writes(self) -> None:
        names = {field.name for field in dataclasses.fields(Operations)}
        self.assertEqual({"head", "source_problems", "refusals", "key_facts", "held_out_problem", "join_facts", "snapshot_ids"}, names)

    def test_a_result_is_json_ready(self) -> None:
        import json

        facts: dict[str, Any] = run_verify(Fake().operations(), HEAD).facts()
        self.assertEqual(facts, json.loads(json.dumps(facts)))


if __name__ == "__main__":
    unittest.main()
