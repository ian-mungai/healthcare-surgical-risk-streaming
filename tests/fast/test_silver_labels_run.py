"""The order of a build and of a verify of the label tables, with stand-in Spark steps.

Failure modes covered here:

- A table is written although the input has a defect, so a branch holds labels built from an input that cannot be trusted.
- The labels are written before the measure of ``icu_days`` (the measure is recorded before the label is built), or a label table is written after the write
  of the measure failed.
- Any intensive care label is evaluated (planned) before the measure is written and read back with every check passing: the measure is verified first.
- A rebuild that changes nothing writes a new snapshot, or a rebuild that changes one table rewrites another.
- A table is written after the branch moved, or the hash of the write is not the one commit of the write directly on the checked hash.
- A table is read back at another hash than the write's, or the read-back is not the table that was planned, or its checks are not run.
- The input changes between the first read and the read-back and nothing says so.
- The result names a count, a digest or a value of a label: the labels are outcomes and a digest of a table changes with a held-out row. It holds the snapshot
  IDs, one pass flag for each table, the checks that ran and the measure of ``icu_days`` of the development patients, and nothing else about a table.
- A refusal message holds a count (the cases that a label table lacks, the tables that are missing or differ): it is fixed text, a code and a flag.
- The planned frames are not released at the end, also after a failure.
"""

from __future__ import annotations

import dataclasses
import json
import unittest
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.label_checks import ICU, KIDNEY, MEASURE, TABLE_CHECKS, InputFacts, TableFacts
from spark.silver.label_measure import IcuMeasureRow
from spark.silver.label_run import InputRead, MeasurePlan, Operations, TableRead, passed_flags, run_build, run_verify
from spark.silver.run import Fingerprint

HEAD, PREPARED, FIRST_WRITE, SECOND_WRITE, THIRD_WRITE = "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64
INPUT = InputRead(InputFacts(6388, 0, 0, (), 5000), (31, 32, 33))
MEASURE_ROWS = (IcuMeasureRow("cases", "cases", 5000.0), IcuMeasureRow("zero", "cases", 4000.0))
FINGERPRINTS = {MEASURE: Fingerprint(2, 7_000_001), ICU: Fingerprint(6388, 8_000_002), KIDNEY: Fingerprint(6388, 9_000_003)}
LABEL_TABLES = (ICU, KIDNEY)


def facts(table: str) -> TableFacts:
    return TableFacts({name: 0 for name in TABLE_CHECKS[table]})


def codes(error: SilverSignalsError) -> list[str]:
    return [problem.split(":")[0] for problem in error.problems]


class Fake:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.input = INPUT
        self.later_input: InputRead | None = None
        self.measure_plan = MeasurePlan(FINGERPRINTS[MEASURE], MEASURE_ROWS)
        self.label_plan = {table: FINGERPRINTS[table] for table in LABEL_TABLES}
        self.written: set[str] = set()
        self.held: dict[str, TableRead | None] = {MEASURE: None, ICU: None, KIDNEY: None}
        self.read_back: dict[str, TableRead | None] = {
            table: TableRead(FINGERPRINTS[table], 100 + index, facts(table)) for index, table in enumerate(FINGERPRINTS)
        }
        self.hashes: dict[str, str | None] = {MEASURE: FIRST_WRITE, ICU: SECOND_WRITE, KIDNEY: THIRD_WRITE}
        self.moved_after: int | None = None

    def head(self) -> str:
        self.calls.append("head")
        return "f" * 64 if self.moved_after is not None and self.calls.count("head") > self.moved_after else self.current

    def read_input(self, at: str) -> InputRead:
        self.calls.append(f"read_input {at[:1]}")
        return self.later_input if self.later_input is not None and at != HEAD else self.input

    def plan_measure(self, at: str) -> MeasurePlan:
        self.calls.append(f"plan_measure {at[:1]}")
        return self.measure_plan

    def plan_labels(self, at: str) -> dict[str, Fingerprint]:
        self.calls.append(f"plan_labels {at[:1]}")
        return self.label_plan

    def read_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        self.calls.append(f"read_table {at[:1]} {table} facts={with_facts}")
        return self.read_back[table] if table in self.written else self.held[table]

    def write(self, table: str, exists: bool) -> None:
        self.calls.append(f"write {table} exists={exists}")
        self.written.add(table)

    def write_hash(self, checked: str, table: str) -> str | None:
        self.calls.append(f"write_hash {checked[:1]} {table}")
        written = self.hashes[table]
        if written is not None:
            self.current = written
        return written

    def prepare(self, checked: str) -> str:
        self.calls.append(f"prepare {checked[:1]}")
        return PREPARED

    def release(self) -> None:
        self.calls.append("release")

    def operations(self) -> Operations:
        return Operations(
            self.head, self.read_input, self.plan_measure, self.plan_labels, self.read_table, self.write, self.write_hash, self.prepare, self.release
        )


class Build(unittest.TestCase):
    def test_a_clean_build_writes_and_verifies_the_measure_before_any_label_is_planned_then_writes_each_label_table(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD)
        self.assertEqual(
            [
                "read_input a",
                "plan_measure a",
                f"read_table a {MEASURE} facts=False",
                "head",
                "prepare a",
                f"write {MEASURE} exists=False",
                f"write_hash b {MEASURE}",
                f"read_table c {MEASURE} facts=True",
                "plan_labels c",
                f"read_table c {ICU} facts=False",
                f"read_table c {KIDNEY} facts=False",
                f"write {ICU} exists=False",
                f"write_hash c {ICU}",
                f"write {KIDNEY} exists=False",
                f"write_hash d {KIDNEY}",
                "read_input e",
                f"read_table e {MEASURE} facts=True",
                f"read_table e {ICU} facts=True",
                f"read_table e {KIDNEY} facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )
        self.assertEqual((HEAD, THIRD_WRITE), (outcome.checked_hash, outcome.write_hash))

    def test_the_labels_are_planned_only_after_the_measure_was_read_back_with_every_check_passing(self) -> None:
        for broken, wanted in (
            (TableRead(FINGERPRINTS[MEASURE], 100, TableFacts({"icu_measure_columns": 0, "icu_measure_differs": 1})), ["icu_measure_differs: failed"]),
            (TableRead(Fingerprint(2, 99), 100, facts(MEASURE)), ["readback_differs: failed"]),
            (None, ["table_missing: failed"]),
        ):
            with self.subTest(wanted=wanted):
                fake = Fake()
                fake.read_back[MEASURE] = broken
                with self.assertRaises(SilverSignalsError) as caught:
                    run_build(fake.operations(), HEAD)
                self.assertEqual(wanted, caught.exception.problems)
                self.assertFalse([call for call in fake.calls if call.startswith("plan_labels") or ICU in call or KIDNEY in call])
                self.assertEqual("release", fake.calls[-1])

    def test_a_rebuild_that_changes_a_label_table_only_still_verifies_the_unchanged_measure_before_planning_the_labels(self) -> None:
        fake = Fake()
        fake.held = {**fake.read_back, ICU: TableRead(Fingerprint(6388, 1), 98)}
        fake.written.add(MEASURE)
        fake.calls.clear()
        run_build(fake.operations(), HEAD)
        self.assertLess(fake.calls.index(f"read_table a {MEASURE} facts=True"), fake.calls.index("plan_labels a"))
        self.assertEqual([f"write {ICU} exists=True"], [call for call in fake.calls if call.startswith("write ")])

    def test_the_result_holds_the_snapshots_one_pass_flag_for_each_table_and_the_development_measure_only(self) -> None:
        facts_line: dict[str, Any] = run_build(Fake().operations(), HEAD).facts()
        self.assertEqual({MEASURE: 100, ICU: 101, KIDNEY: 102}, facts_line["snapshots"])
        self.assertEqual({MEASURE: True, ICU: True, KIDNEY: True}, facts_line["passed"])
        self.assertEqual({"cases": 31, "lab_results": 32, "patient_split": 33}, facts_line["source_snapshots"])
        self.assertEqual(
            [{"statistic": "cases", "unit": "cases", "value": 5000.0}, {"statistic": "zero", "unit": "cases", "value": 4000.0}], facts_line["icu_measure"]
        )
        self.assertEqual({"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "icu_measure", "checks_run"}, set(facts_line))
        self.assertGreaterEqual(len(facts_line["checks_run"]), 15)

    def test_no_result_names_an_outcome_because_a_held_out_row_alone_would_flip_it(self) -> None:
        fake = Fake()
        written = run_build(fake.operations(), HEAD)
        kept_fake = Fake()
        kept_fake.held = dict(kept_fake.read_back)
        kept = run_build(kept_fake.operations(), HEAD)
        verify_fake = Fake()
        verify_fake.held = dict(verify_fake.read_back)
        verify_fake.read_back = verify_fake.held
        verified = run_verify(verify_fake.operations(), HEAD)
        for outcome in (written, kept, verified):
            self.assertFalse(hasattr(outcome, "outcome"))
            self.assertNotIn("outcome", outcome.facts())
            self.assertNotIn("written", json.dumps(outcome.facts()))
            self.assertNotIn("unchanged", json.dumps(outcome.facts()))
        self.assertEqual((THIRD_WRITE, THIRD_WRITE), (written.facts()["write_hash"], written.facts()["branch_hash"]))
        self.assertEqual((None, HEAD), (kept.facts()["write_hash"], kept.facts()["branch_hash"]))

    def test_the_result_holds_no_digest_no_row_count_and_no_case_count(self) -> None:
        text = json.dumps(run_build(Fake().operations(), HEAD).facts())
        for number in (7_000_001, 8_000_002, 9_000_003, 6388):
            self.assertNotIn(str(number), text)
        for key in ("rows", "fingerprints"):
            self.assertNotIn(f'"{key}"', text)

    def test_a_verify_result_has_the_same_keys_and_no_digest(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        fake.read_back = fake.held
        facts_line = run_verify(fake.operations(), HEAD).facts()
        self.assertEqual({"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "icu_measure", "checks_run"}, set(facts_line))
        self.assertEqual({MEASURE: True, ICU: True, KIDNEY: True}, facts_line["passed"])
        for number in (7_000_001, 8_000_002, 9_000_003, 6388):
            self.assertNotIn(str(number), json.dumps(facts_line))

    def test_an_input_defect_writes_nothing_stops_before_any_plan_and_releases(self) -> None:
        for input_facts, wanted in (
            (InputFacts(6388, 1, 0, (), 5000), ["ambiguous_case_owner"]),
            (InputFacts(6388, 0, 0, ("two_sets: 1 patient",), 5000), ["two_sets"]),
            (InputFacts(6388, 0, 1, (), 5000), ["case_without_split"]),
            (InputFacts(0, 0, 0, (), 0), ["input_empty"]),
            (InputFacts(6388, 0, 0, (), 0), ["development_empty"]),
        ):
            with self.subTest(wanted=wanted):
                fake = Fake()
                fake.input = InputRead(input_facts, (31, 32, 33))
                with self.assertRaises(SilverSignalsError) as caught:
                    run_build(fake.operations(), HEAD)
                self.assertEqual(wanted, codes(caught.exception))
                self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_rebuild_that_changes_nothing_writes_nothing_and_still_reads_back_and_checks(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        outcome = run_build(fake.operations(), HEAD)
        self.assertIsNone(outcome.write_hash)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertIn(f"read_table a {KIDNEY} facts=True", fake.calls)
        self.assertEqual({MEASURE: 100, ICU: 101, KIDNEY: 102}, outcome.facts()["snapshots"])

    def test_a_rebuild_that_changes_one_table_rewrites_that_table_only_and_in_place(self) -> None:
        fake = Fake()
        fake.held = {**fake.read_back, KIDNEY: TableRead(Fingerprint(6388, 1), 99)}
        run_build(fake.operations(), HEAD)
        self.assertEqual([f"write {KIDNEY} exists=True"], [call for call in fake.calls if call.startswith("write ")])

    def test_a_rebuild_that_changes_the_measure_and_a_label_table_writes_the_measure_first(self) -> None:
        fake = Fake()
        fake.held = {**fake.read_back, MEASURE: TableRead(Fingerprint(2, 1), 99), ICU: TableRead(Fingerprint(6388, 1), 98)}
        run_build(fake.operations(), HEAD)
        self.assertEqual([f"write {MEASURE} exists=True", f"write {ICU} exists=True"], [call for call in fake.calls if call.startswith("write ")])
        self.assertLess(fake.calls.index(f"write {MEASURE} exists=True"), fake.calls.index("plan_labels c"))

    def test_a_branch_that_moved_before_the_write_gets_no_write(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])

    def test_a_write_of_the_measure_that_is_not_the_one_commit_stops_before_any_label_is_written(self) -> None:
        fake = Fake()
        fake.hashes[MEASURE] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertNotIn(f"write {ICU} exists=False", fake.calls)
        self.assertNotIn(f"write {KIDNEY} exists=False", fake.calls)

    def test_a_write_of_the_intensive_care_labels_that_fails_stops_before_the_kidney_labels(self) -> None:
        fake = Fake()
        fake.hashes[ICU] = None
        with self.assertRaises(SilverSignalsError):
            run_build(fake.operations(), HEAD)
        self.assertNotIn(f"write {KIDNEY} exists=False", fake.calls)

    def test_a_read_back_that_is_not_the_planned_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[ICU] = TableRead(Fingerprint(6388, 99), 101, facts(ICU))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["readback_differs"], codes(caught.exception))
        self.assertEqual(["readback_differs: failed"], caught.exception.problems)

    def test_a_table_that_is_not_there_at_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing"], codes(caught.exception))
        self.assertEqual(["table_missing: failed"], caught.exception.problems)

    def test_the_number_of_tables_that_are_not_there_is_never_written(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = None
        fake.read_back[ICU] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: failed"], caught.exception.problems)

    def test_the_checks_of_every_table_run_on_the_read_back_and_a_failed_check_is_a_flag(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = TableRead(FINGERPRINTS[KIDNEY], 102, TableFacts({**facts(KIDNEY).flags, "kidney_rule": 12}))
        fake.read_back[ICU] = TableRead(FINGERPRINTS[ICU], 101, TableFacts({**facts(ICU).flags, "icu_not_every_case": 1}))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["icu_not_every_case: failed", "kidney_rule: failed"], caught.exception.problems)
        self.assertFalse([problem for problem in caught.exception.problems if any(char.isdigit() for char in problem)])

    def test_an_input_that_changed_between_the_first_read_and_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.later_input = InputRead(InputFacts(6388, 0, 0, (), 5000), (31, 32, 34))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["source_changed"], codes(caught.exception))

    def test_a_branch_that_moved_after_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 1
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["branch_moved"], codes(caught.exception))

    def test_the_planned_frames_are_released_after_a_failure_too(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = None
        with self.assertRaises(SilverSignalsError):
            run_build(fake.operations(), HEAD)
        self.assertEqual("release", fake.calls[-1])


class PassFlags(unittest.TestCase):
    """The flag of a table is true only when the table is there, every check of it passes and, in a build, it is the planned table."""

    def reads(self) -> dict[str, TableRead | None]:
        return {table: TableRead(FINGERPRINTS[table], 100, facts(table)) for table in FINGERPRINTS}

    def test_a_good_read_back_is_true_for_every_table_with_or_without_a_plan(self) -> None:
        for planned in (None, FINGERPRINTS):
            self.assertEqual({MEASURE: True, ICU: True, KIDNEY: True}, passed_flags(self.reads(), planned))

    def test_a_table_that_is_missing_has_no_facts_or_fails_a_check_is_false_and_the_others_stay_true(self) -> None:
        reads = self.reads()
        reads[MEASURE] = None
        reads[ICU] = TableRead(FINGERPRINTS[ICU], 101, None)
        reads[KIDNEY] = TableRead(FINGERPRINTS[KIDNEY], 102, TableFacts({**facts(KIDNEY).flags, "kidney_rule": 1}))
        self.assertEqual({MEASURE: False, ICU: False, KIDNEY: False}, passed_flags(reads, None))
        reads = self.reads()
        reads[ICU] = TableRead(FINGERPRINTS[ICU], 101, TableFacts({**facts(ICU).flags, "icu_not_every_case": 1}))
        self.assertEqual({MEASURE: True, ICU: False, KIDNEY: True}, passed_flags(reads, None))

    def test_a_table_that_is_not_the_planned_one_is_false_in_a_build_only(self) -> None:
        reads = self.reads()
        reads[KIDNEY] = TableRead(Fingerprint(6388, 1), 102, facts(KIDNEY))
        self.assertEqual({MEASURE: True, ICU: True, KIDNEY: False}, passed_flags(reads, FINGERPRINTS))
        self.assertEqual({MEASURE: True, ICU: True, KIDNEY: True}, passed_flags(reads, None))


class Verify(unittest.TestCase):
    def test_a_verify_reads_the_input_first_then_the_tables_and_writes_nothing(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        fake.read_back = fake.held
        outcome = run_verify(fake.operations(), HEAD)
        self.assertIsNone(outcome.write_hash)
        self.assertEqual(
            ["read_input a", f"read_table a {MEASURE} facts=True", f"read_table a {ICU} facts=True", f"read_table a {KIDNEY} facts=True", "head", "release"],
            fake.calls,
        )

    def test_an_input_defect_stops_a_verify_before_any_table_is_read(self) -> None:
        fake = Fake()
        fake.input = InputRead(dataclasses.replace(INPUT.facts, cases_without_split=1), INPUT.snapshots)
        with self.assertRaises(SilverSignalsError):
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_verify_of_a_table_that_is_missing_or_fails_a_check_is_named(self) -> None:
        fake = Fake()
        fake.read_back = {**fake.read_back, MEASURE: None}
        fake.held = fake.read_back
        with self.assertRaises(SilverSignalsError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["table_missing: failed"], caught.exception.problems)

    def test_a_branch_that_moved_during_a_verify_is_named(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        fake.read_back = fake.held
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            run_verify(fake.operations(), HEAD)
        self.assertEqual(["branch_moved"], codes(caught.exception))


if __name__ == "__main__":
    unittest.main()
