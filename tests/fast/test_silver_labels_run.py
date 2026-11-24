"""The order of a build and of a verify of the label tables, with stand-in Spark steps.

Failure modes covered here:

- A table is written although the input has a defect, so a branch holds labels built from an input that cannot be trusted.
- The labels are written before the measure of ``icu_days`` (the measure is recorded before the label is built), or a label table is written after the write
  of the measure failed.
- A rebuild that changes nothing writes a new snapshot, or a rebuild that changes one table rewrites another.
- A table is written after the branch moved, or the hash of the write is not the one commit of the write directly on the checked hash.
- A table is read back at another hash than the write's, or the read-back is not the table that was planned, or its checks are not run.
- The input changes between the first read and the read-back and nothing says so.
- The result names a count or a value of a label: the labels are outcomes. It holds the number of cases (an input), digests, snapshot IDs, the checks that ran
  and the measure of ``icu_days`` of the development patients.
- The planned frames are not released at the end, also after a failure.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from spark.silver.checks import SilverSignalsError
from spark.silver.label_checks import ICU, KIDNEY, MEASURE, TABLE_CHECKS, InputFacts, TableFacts
from spark.silver.label_measure import IcuMeasureRow
from spark.silver.label_run import InputRead, Operations, Plan, TableRead, run_build, run_verify
from spark.silver.run import Fingerprint

HEAD, PREPARED, FIRST_WRITE, SECOND_WRITE, THIRD_WRITE = "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64
INPUT = InputRead(InputFacts(6388, 0, 0, (), 5000), (31, 32, 33))
MEASURE_ROWS = (IcuMeasureRow("cases", "cases", 5000.0), IcuMeasureRow("zero", "cases", 4000.0))
FINGERPRINTS = {MEASURE: Fingerprint(2, 7), ICU: Fingerprint(6388, 8), KIDNEY: Fingerprint(6388, 9)}


def facts(table: str) -> TableFacts:
    return TableFacts(2 if table == MEASURE else 6388, {name: 0 for name in TABLE_CHECKS[table]})


def codes(error: SilverSignalsError) -> list[str]:
    return [problem.split(":")[0] for problem in error.problems]


class Fake:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.input = INPUT
        self.later_input: InputRead | None = None
        self.planned = Plan(dict(FINGERPRINTS), MEASURE_ROWS)
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

    def plan(self, at: str) -> Plan:
        self.calls.append(f"plan {at[:1]}")
        return self.planned

    def read_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        self.calls.append(f"read_table {at[:1]} {table} facts={with_facts}")
        return self.held[table] if at == HEAD else self.read_back[table]

    def write(self, table: str, exists: bool) -> None:
        self.calls.append(f"write {table} exists={exists}")

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
        return Operations(self.head, self.read_input, self.plan, self.read_table, self.write, self.write_hash, self.prepare, self.release)


class Build(unittest.TestCase):
    def test_a_clean_build_writes_the_measure_then_each_label_table_and_reads_back_at_the_last_write(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD)
        self.assertEqual(
            [
                "read_input a",
                "plan a",
                f"read_table a {MEASURE} facts=False",
                f"read_table a {ICU} facts=False",
                f"read_table a {KIDNEY} facts=False",
                "head",
                "prepare a",
                f"write {MEASURE} exists=False",
                f"write_hash b {MEASURE}",
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
        self.assertEqual(("written", HEAD, THIRD_WRITE), (outcome.outcome, outcome.checked_hash, outcome.write_hash))

    def test_the_result_holds_the_cases_the_digests_the_snapshots_and_the_development_measure_only(self) -> None:
        facts_line: dict[str, Any] = run_build(Fake().operations(), HEAD).facts()
        self.assertEqual({ICU: 6388, KIDNEY: 6388, MEASURE: 2}, facts_line["rows"])
        self.assertEqual({MEASURE: [2, 7], ICU: [6388, 8], KIDNEY: [6388, 9]}, facts_line["fingerprints"])
        self.assertEqual({MEASURE: 100, ICU: 101, KIDNEY: 102}, facts_line["snapshots"])
        self.assertEqual({"cases": 31, "lab_results": 32, "patient_split": 33}, facts_line["source_snapshots"])
        self.assertEqual(
            [{"statistic": "cases", "unit": "cases", "value": 5000.0}, {"statistic": "zero", "unit": "cases", "value": 4000.0}], facts_line["icu_measure"]
        )
        self.assertEqual(
            {"outcome", "checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "rows", "fingerprints", "icu_measure", "checks_run"},
            set(facts_line),
        )
        self.assertGreaterEqual(len(facts_line["checks_run"]), 15)

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
        self.assertEqual(("unchanged", None), (outcome.outcome, outcome.write_hash))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertIn(f"read_table a {KIDNEY} facts=True", fake.calls)

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

    def test_a_table_that_is_not_there_at_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing"], codes(caught.exception))
        self.assertEqual(["table_missing: 1 table"], caught.exception.problems)

    def test_each_table_that_is_not_there_is_counted(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = None
        fake.read_back[ICU] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: 2 tables"], caught.exception.problems)

    def test_the_checks_of_every_table_run_on_the_read_back_and_a_failed_check_is_a_flag(self) -> None:
        fake = Fake()
        fake.read_back[KIDNEY] = TableRead(FINGERPRINTS[KIDNEY], 102, TableFacts(6388, {**facts(KIDNEY).flags, "kidney_rule": 12}))
        fake.read_back[ICU] = TableRead(FINGERPRINTS[ICU], 101, TableFacts(6380, facts(ICU).flags))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["icu_not_every_case: 8 cases", "kidney_rule: failed"], caught.exception.problems)

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


class Verify(unittest.TestCase):
    def test_a_verify_reads_the_input_first_then_the_tables_and_writes_nothing(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        fake.read_back = fake.held
        outcome = run_verify(fake.operations(), HEAD)
        self.assertEqual("verified", outcome.outcome)
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
        self.assertEqual(["table_missing: 1 table"], caught.exception.problems)

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
