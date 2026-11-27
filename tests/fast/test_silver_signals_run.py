"""The order of a build and of a verify of the Silver signal tables, with stand-in Spark steps.

Failure modes covered here:

- A table is written although the Bronze input has a defect, so a branch holds a table built from rows that cannot be cleaned.
- A rebuild that changes nothing writes a new snapshot, or a rebuild that changes one table rewrites the other.
- A table is written after the branch moved, or the hash of the write is not the one commit of the write directly on the checked hash.
- A table is read back at another hash than the write's, or the read-back is not the table that was planned, or its checks are not run.
- Bronze changes between the first read and the read-back and nothing says so.
- The branch moves while the job reads and the job still reports success.
- The frame that the job planned is not released at the end, also after a failure.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from spark.silver.checks import BronzeFacts, NumericFacts, SilverSignalsError, WaveformFacts
from spark.silver.run import BRANCH_MOVED, NUMERIC, WAVEFORM, Fingerprint, InputRead, Operations, TableRead, run_build, run_verify

HEAD = "a" * 64
PREPARED = "b" * 64
FIRST_WRITE = "c" * 64
SECOND_WRITE = "d" * 64
BRONZE = BronzeFacts(0, 0, 0, 0, 0, 0, 0, 0)
INPUT = InputRead(BRONZE, (11, 12, 13))
FINGERPRINTS = {NUMERIC: Fingerprint(990, 5), WAVEFORM: Fingerprint(300, 9)}
NUMERIC_FACTS = NumericFacts(0, 0, 0, 0, 0, 0, 0)
WAVEFORM_FACTS = WaveformFacts(0, 0, 0, 0, 0, 0, 0)


def messages(error: SilverSignalsError) -> list[str]:
    """The complete messages of a refusal: a code is not enough, because its text could carry a count."""
    return error.problems


def failed(*names: str) -> list[str]:
    return [f"{name}: failed" for name in names]


class Fake:
    """Stand-in steps that record their calls. A table that the branch holds is in ``held`` with its read-back."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.input = INPUT
        self.later_input: InputRead | None = None
        self.planned = dict(FINGERPRINTS)
        self.held: dict[str, TableRead | None] = {NUMERIC: None, WAVEFORM: None}
        self.read_back: dict[str, TableRead | None] = {
            NUMERIC: TableRead(FINGERPRINTS[NUMERIC], 101, numeric=NUMERIC_FACTS),
            WAVEFORM: TableRead(FINGERPRINTS[WAVEFORM], 102, waveform=WAVEFORM_FACTS),
        }
        self.hashes: dict[str, str | None] = {NUMERIC: FIRST_WRITE, WAVEFORM: SECOND_WRITE}
        self.reads = 0
        self.moved_after: int | None = None

    def head(self) -> str:
        self.calls.append("head")
        if self.moved_after is not None and self.calls.count("head") > self.moved_after:
            return "e" * 64
        return self.current

    def read_input(self, at: str) -> InputRead:
        self.calls.append(f"read_input {at[:1]}")
        return self.later_input if self.later_input is not None and at != HEAD else self.input

    def plan(self, at: str) -> dict[str, Fingerprint]:
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
    def test_a_clean_build_checks_the_input_then_writes_each_table_on_the_hash_before_and_reads_back_at_the_last_write(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD)
        self.assertEqual(
            [
                "read_input a",
                "plan a",
                "read_table a numeric_observations facts=False",
                "read_table a waveform_seconds facts=False",
                "head",
                "prepare a",
                "write numeric_observations exists=False",
                "write_hash b numeric_observations",
                "write waveform_seconds exists=False",
                "write_hash c waveform_seconds",
                "read_input d",
                "read_table d numeric_observations facts=True",
                "read_table d waveform_seconds facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )
        self.assertEqual((HEAD, SECOND_WRITE, SECOND_WRITE), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))

    def test_the_result_holds_hashes_snapshots_and_one_pass_flag_for_each_table_and_no_count_or_digest(self) -> None:
        facts = run_build(Fake().operations(), HEAD).facts()
        self.assertEqual(
            {
                "checked_hash": HEAD,
                "write_hash": SECOND_WRITE,
                "branch_hash": SECOND_WRITE,
                "source_snapshots": {"numeric_observations": 11, "waveform_seconds": 12, "tracks": 13},
                "snapshots": {NUMERIC: 101, WAVEFORM: 102},
                "passed": {NUMERIC: True, WAVEFORM: True},
                "checks_run": facts["checks_run"],
            },
            facts,
        )
        self.assertGreaterEqual(len(facts["checks_run"]), 20)

    def test_a_verify_gives_one_pass_flag_for_each_table(self) -> None:
        fake = Fake()
        fake.held = {NUMERIC: fake.read_back[NUMERIC], WAVEFORM: fake.read_back[WAVEFORM]}
        self.assertEqual({NUMERIC: True, WAVEFORM: True}, run_verify(fake.operations(), HEAD).facts()["passed"])

    def test_a_bronze_defect_writes_nothing_and_releases(self) -> None:
        fake = Fake()
        fake.input = InputRead(dataclasses.replace(BRONZE, waveform_duplicate_seconds=2, numeric_occurrence_gaps=1), (11, 12, 13))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("numeric_occurrence_gap", "waveform_duplicate_second"), messages(caught.exception))
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_rebuild_that_changes_nothing_writes_nothing_and_still_reads_back_and_checks(self) -> None:
        fake = Fake()
        fake.held = {NUMERIC: fake.read_back[NUMERIC], WAVEFORM: fake.read_back[WAVEFORM]}
        outcome = run_build(fake.operations(), HEAD)
        self.assertNotIn("outcome", outcome.facts())
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertIn("read_table a numeric_observations facts=True", fake.calls)
        self.assertEqual((HEAD, None), (outcome.checked_hash, outcome.write_hash))
        self.assertEqual(HEAD, outcome.facts()["branch_hash"])

    def test_a_rebuild_that_changes_one_table_rewrites_that_table_only_and_in_place(self) -> None:
        fake = Fake()
        fake.held = {NUMERIC: fake.read_back[NUMERIC], WAVEFORM: TableRead(Fingerprint(300, 8), 99)}
        run_build(fake.operations(), HEAD)
        writes = [call for call in fake.calls if call.startswith("write ")]
        self.assertEqual(["write waveform_seconds exists=True"], writes)
        self.assertIn("write_hash b waveform_seconds", fake.calls)

    def test_a_table_with_the_same_rows_and_another_hash_is_a_change(self) -> None:
        fake = Fake()
        fake.held = {NUMERIC: TableRead(Fingerprint(990, 6), 99), WAVEFORM: fake.read_back[WAVEFORM]}
        run_build(fake.operations(), HEAD)
        self.assertEqual(["write numeric_observations exists=True"], [call for call in fake.calls if call.startswith("write ")])

    def test_a_branch_that_moved_before_the_write_gets_no_write(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertEqual("release", fake.calls[-1])

    def test_a_write_that_is_not_the_one_commit_on_the_checked_hash_stops_the_job_before_the_next_write(self) -> None:
        fake = Fake()
        fake.hashes[NUMERIC] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))
        self.assertNotIn("write waveform_seconds exists=False", fake.calls)

    def test_a_read_back_that_is_not_the_planned_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[WAVEFORM] = TableRead(Fingerprint(300, 10), 102, waveform=WAVEFORM_FACTS)
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["readback_differs: failed"], messages(caught.exception))

    def test_a_table_that_is_not_there_at_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.read_back[NUMERIC] = None
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: failed"], messages(caught.exception))

    def test_a_table_that_is_read_back_without_its_checks_is_named_as_a_missing_table(self) -> None:
        fake = Fake()
        fake.read_back[NUMERIC] = TableRead(FINGERPRINTS[NUMERIC], 101)
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(["table_missing: failed"], messages(caught.exception))

    def test_the_checks_of_the_tables_run_on_the_read_back(self) -> None:
        fake = Fake()
        fake.read_back[NUMERIC] = TableRead(FINGERPRINTS[NUMERIC], 101, numeric=dataclasses.replace(NUMERIC_FACTS, verdict_differs=3))
        fake.read_back[WAVEFORM] = TableRead(FINGERPRINTS[WAVEFORM], 102, waveform=dataclasses.replace(WAVEFORM_FACTS, row_count_differs=1))
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual(failed("numeric_verdict_differs", "waveform_not_every_second"), messages(caught.exception))

    def test_bronze_that_changed_between_the_reads_is_named(self) -> None:
        for later in (InputRead(BRONZE, (11, 12, 14)), InputRead(dataclasses.replace(BRONZE, numeric_null_keys=1), (11, 12, 13))):
            with self.subTest(later=later):
                fake = Fake()
                fake.later_input = later
                with self.assertRaises(SilverSignalsError) as caught:
                    run_build(fake.operations(), HEAD)
                self.assertEqual(failed("source_changed"), messages(caught.exception))

    def test_a_branch_that_moved_during_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 1
        with self.assertRaises(SilverSignalsError) as caught:
            run_build(fake.operations(), HEAD)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))

    def test_a_failure_that_is_not_a_check_still_releases_the_plan(self) -> None:
        fake = Fake()

        def broken(_at: str) -> dict[str, Fingerprint]:
            raise RuntimeError("stop")

        operations = dataclasses.replace(fake.operations(), plan=broken)
        with self.assertRaises(RuntimeError):
            run_build(operations, HEAD)
        self.assertEqual("release", fake.calls[-1])


class Verify(unittest.TestCase):
    def verify(self, fake: Fake) -> Any:
        fake.held = {NUMERIC: fake.read_back[NUMERIC], WAVEFORM: fake.read_back[WAVEFORM]}
        return run_verify(fake.operations(), HEAD)

    def test_a_good_branch_is_verified_and_nothing_is_written(self) -> None:
        fake = Fake()
        outcome = self.verify(fake)
        self.assertNotIn("outcome", outcome.facts())
        self.assertEqual((HEAD, None, HEAD), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))
        self.assertEqual(
            ["read_input a", "read_table a numeric_observations facts=True", "read_table a waveform_seconds facts=True", "head", "release"], fake.calls
        )

    def test_a_branch_without_a_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[WAVEFORM] = None
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual(["table_missing: failed"], messages(caught.exception))

    def test_a_defect_in_a_table_or_in_bronze_is_named(self) -> None:
        fake = Fake()
        fake.read_back[NUMERIC] = TableRead(FINGERPRINTS[NUMERIC], 101, numeric=dataclasses.replace(NUMERIC_FACTS, valid_with_cause=1))
        fake.input = InputRead(dataclasses.replace(BRONZE, track_duplicates=1), (11, 12, 13))
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual(failed("track_duplicate", "numeric_valid_with_cause"), messages(caught.exception))

    def test_a_branch_that_moved_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SilverSignalsError) as caught:
            self.verify(fake)
        self.assertEqual([BRANCH_MOVED], messages(caught.exception))


if __name__ == "__main__":
    unittest.main()
