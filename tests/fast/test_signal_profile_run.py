"""The order of a build and of a verify of the signal profile tables, with stand-in Spark steps.

Failure modes covered here:

- A table is written although the development input has a defect (a split that fails its rule check, no development case, no development signal), so a branch
  holds a profile of the wrong cases.
- A rebuild that changes nothing writes a new snapshot, or a rebuild that changes one table rewrites another.
- A table is written after the branch moved, or the hash of the write is not the one commit of the write directly on the checked hash.
- A table is read back at another hash than the write's, or the read-back is not the planned table, or its checks are not run, or the pinned flag is not
  passed to the check of the counts.
- The input changes between the first read and the read-back and nothing says so.
- A count that does not match makes the job fail, or is not named in the result; a sealed count's value or any row is in the result.
- The frames that the job planned are not released, also after a failure.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from spark.profile.checks import CountsFacts, InputFacts, ProfileFacts, SignalProfileError, UnscoredFacts
from spark.profile.counts import PRESPECIFIED
from spark.profile.run import COUNTS, PROFILE, UNSCORED, InputRead, Operations, TableRead, run_build, run_verify
from spark.profile.unscored import CUTOFF_STEP_SECONDS
from spark.silver.run import Fingerprint

HEAD = "a" * 64
PREPARED = "b" * 64
HASHES = {PROFILE: "c" * 64, UNSCORED: "d" * 64, COUNTS: "e" * 64}
FACTS = InputFacts(numeric_rows=500, waveform_rows=300, tracks=12, arterial_cases=3)
INPUT = InputRead(FACTS, (11, 12, 13, 14, 15), ())
FINGERPRINTS = {PROFILE: Fingerprint(12, 5), UNSCORED: Fingerprint(1, 6), COUNTS: Fingerprint(len(PRESPECIFIED), 7)}
PROFILE_FACTS = ProfileFacts(12, 0, 0, 0, 0, 500, 300, 12)
UNSCORED_FACTS = UnscoredFacts(
    1,
    100,
    10,
    5,
    12,
    0.12,
    3,
    CUTOFF_STEP_SECONDS,
    WAVEFORM_WINDOW_SECONDS,
    MAX_INVALID_WAVEFORM_SECONDS,
    NUMERIC_WINDOW_SECONDS,
    MAX_INVALID_NUMERIC_PERCENT,
)
COUNTS_FACTS = CountsFacts(len(PRESPECIFIED), 0, 0, 0, 0, 0)


def codes(error: SignalProfileError) -> list[str]:
    return [problem.split(":")[0] for problem in error.problems]


class Fake:
    """Stand-in steps that record their calls. A table that the branch holds is in ``held`` with its read-back."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.current = HEAD
        self.input = INPUT
        self.later_input: InputRead | None = None
        self.held: dict[str, TableRead | None] = {PROFILE: None, UNSCORED: None, COUNTS: None}
        self.read_back: dict[str, TableRead | None] = {
            PROFILE: TableRead(FINGERPRINTS[PROFILE], 101, profile=PROFILE_FACTS),
            UNSCORED: TableRead(FINGERPRINTS[UNSCORED], 102, unscored=UNSCORED_FACTS),
            COUNTS: TableRead(FINGERPRINTS[COUNTS], 103, counts=COUNTS_FACTS),
        }
        self.hashes: dict[str, str | None] = dict(HASHES)
        self.moved_after: int | None = None

    def head(self) -> str:
        self.calls.append("head")
        if self.moved_after is not None and self.calls.count("head") > self.moved_after:
            return "f" * 64
        return self.current

    def read_input(self, at: str) -> InputRead:
        self.calls.append(f"read_input {at[:1]}")
        return self.later_input if self.later_input is not None and at != HEAD else self.input

    def plan(self, at: str) -> dict[str, Fingerprint]:
        self.calls.append(f"plan {at[:1]}")
        return dict(FINGERPRINTS)

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
    def test_a_clean_build_writes_each_table_on_the_hash_before_and_reads_back_at_the_last_write(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(
            [
                "read_input a",
                "plan a",
                "read_table a signal_profile facts=False",
                "read_table a unscored_share facts=False",
                "read_table a source_counts facts=False",
                "head",
                "prepare a",
                "write signal_profile exists=False",
                "write_hash b signal_profile",
                "write unscored_share exists=False",
                "write_hash c unscored_share",
                "write source_counts exists=False",
                "write_hash d source_counts",
                "read_input e",
                "read_table e signal_profile facts=True",
                "read_table e unscored_share facts=True",
                "read_table e source_counts facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )
        self.assertEqual(("written", HEAD, HASHES[COUNTS]), (outcome.outcome, outcome.checked_hash, outcome.write_hash))

    def test_the_result_holds_hashes_snapshots_and_counts_and_nothing_of_a_row(self) -> None:
        facts = run_build(Fake().operations(), HEAD, pinned=False).facts()
        self.assertEqual(
            {
                "outcome": "written",
                "checked_hash": HEAD,
                "write_hash": HASHES[COUNTS],
                "branch_hash": HASHES[COUNTS],
                "source_snapshots": {"numeric_observations": 11, "waveform_seconds": 12, "patient_split": 13, "clinical_cases": 14, "tracks": 15},
                "snapshots": {PROFILE: 101, UNSCORED: 102, COUNTS: 103},
                "rows": {PROFILE: 12, UNSCORED: 1, COUNTS: len(PRESPECIFIED)},
                "development": {"numeric_rows": 500, "waveform_rows": 300, "tracks": 12, "arterial_cases": 3},
                "unscored": {"cutoffs": 100, "waveform": 10, "numeric": 5, "either": 12},
                "source_counts": {"pinned": False, "mismatched": []},
                "fingerprints": {PROFILE: [12, 5], UNSCORED: [1, 6], COUNTS: [len(PRESPECIFIED), 7]},
                "checks_run": facts["checks_run"],
            },
            facts,
        )
        self.assertGreaterEqual(len(facts["checks_run"]), 15)

    def test_a_development_input_defect_writes_nothing_and_releases(self) -> None:
        fake = Fake()
        fake.input = InputRead(FACTS, (11, 12, 13, 14, 15), ("rule_mismatch: 2 patients", "source_missing: 1 patient"))
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["rule_mismatch", "source_missing"], codes(caught.exception))
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_an_input_with_no_development_signal_is_refused_before_the_plan(self) -> None:
        fake = Fake()
        fake.input = InputRead(InputFacts(0, 0, 0, 0), (11, 12, 13, 14, 15), ())
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["input_empty"], codes(caught.exception))
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_rebuild_that_changes_nothing_writes_nothing_and_still_reads_back_and_checks(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual("unchanged", outcome.outcome)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertIn("read_table a signal_profile facts=True", fake.calls)
        self.assertEqual((HEAD, None, HEAD), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))

    def test_a_rebuild_that_changes_one_table_rewrites_that_table_only_and_in_place(self) -> None:
        fake = Fake()
        fake.held = dict(fake.read_back)
        fake.held[UNSCORED] = TableRead(Fingerprint(1, 99), 90)
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["write unscored_share exists=True"], [call for call in fake.calls if call.startswith("write ")])

    def test_a_branch_that_moved_before_the_write_gets_no_write(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertEqual("release", fake.calls[-1])

    def test_a_write_that_is_not_the_one_commit_on_the_checked_hash_stops_before_the_next_write(self) -> None:
        fake = Fake()
        fake.hashes[PROFILE] = None
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertNotIn("write unscored_share exists=False", fake.calls)

    def test_a_read_back_that_is_not_the_planned_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[UNSCORED] = TableRead(Fingerprint(1, 10), 102, unscored=UNSCORED_FACTS)
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["readback_differs"], codes(caught.exception))

    def test_a_table_that_is_not_there_at_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.read_back[COUNTS] = None
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["table_missing"], codes(caught.exception))

    def test_the_checks_of_the_tables_run_on_the_read_back(self) -> None:
        fake = Fake()
        fake.read_back[PROFILE] = TableRead(FINGERPRINTS[PROFILE], 101, profile=dataclasses.replace(PROFILE_FACTS, partition_differs=2))
        fake.read_back[UNSCORED] = TableRead(FINGERPRINTS[UNSCORED], 102, unscored=dataclasses.replace(UNSCORED_FACTS, cases=9))
        fake.read_back[COUNTS] = TableRead(FINGERPRINTS[COUNTS], 103, counts=dataclasses.replace(COUNTS_FACTS, sealed_with_value=1))
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["profile_partition", "unscored_cases", "counts_sealed_value"], codes(caught.exception))

    def test_the_pinned_flag_reaches_the_check_of_the_counts(self) -> None:
        fake = Fake()
        fake.read_back[COUNTS] = TableRead(FINGERPRINTS[COUNTS], 103, counts=dataclasses.replace(COUNTS_FACTS, not_judged=4))
        run_build(fake.operations(), HEAD, pinned=False)
        again = Fake()
        again.read_back = fake.read_back
        with self.assertRaises(SignalProfileError) as caught:
            run_build(again.operations(), HEAD, pinned=True)
        self.assertEqual(["counts_not_judged"], codes(caught.exception))

    def test_a_count_that_does_not_match_is_named_in_the_result_and_does_not_fail_the_job(self) -> None:
        fake = Fake()
        fake.read_back[COUNTS] = TableRead(FINGERPRINTS[COUNTS], 103, counts=COUNTS_FACTS, mismatched=("tracks",))
        facts = run_build(fake.operations(), HEAD, pinned=True).facts()
        self.assertEqual({"pinned": True, "mismatched": ["tracks"]}, facts["source_counts"])

    def test_an_input_that_changed_between_the_reads_is_named(self) -> None:
        for later in (
            InputRead(FACTS, (11, 12, 13, 14, 16), ()),
            InputRead(dataclasses.replace(FACTS, numeric_rows=501), (11, 12, 13, 14, 15), ()),
        ):
            with self.subTest(later=later):
                fake = Fake()
                fake.later_input = later
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(["source_changed"], codes(caught.exception))

    def test_a_branch_that_moved_during_the_read_back_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 1
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))

    def test_a_failure_that_is_not_a_check_still_releases_the_plan(self) -> None:
        fake = Fake()

        def broken(_at: str) -> dict[str, Fingerprint]:
            raise RuntimeError("stop")

        with self.assertRaises(RuntimeError):
            run_build(dataclasses.replace(fake.operations(), plan=broken), HEAD, pinned=False)
        self.assertEqual("release", fake.calls[-1])


class Verify(unittest.TestCase):
    def verify(self, fake: Fake, pinned: bool = False) -> Any:
        fake.held = dict(fake.read_back)
        return run_verify(fake.operations(), HEAD, pinned=pinned)

    def test_a_good_branch_is_verified_and_nothing_is_written(self) -> None:
        fake = Fake()
        self.assertEqual("verified", self.verify(fake).outcome)
        self.assertEqual(
            [
                "read_input a",
                "read_table a signal_profile facts=True",
                "read_table a unscored_share facts=True",
                "read_table a source_counts facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )

    def test_a_branch_without_a_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[PROFILE] = None
        with self.assertRaises(SignalProfileError) as caught:
            self.verify(fake)
        self.assertEqual(["table_missing"], codes(caught.exception))

    def test_a_defect_in_the_input_or_in_a_table_is_named(self) -> None:
        fake = Fake()
        fake.read_back[PROFILE] = TableRead(FINGERPRINTS[PROFILE], 101, profile=dataclasses.replace(PROFILE_FACTS, share_differs=1))
        fake.input = InputRead(FACTS, (11, 12, 13, 14, 15), ("two_sets: 1 patient",))
        with self.assertRaises(SignalProfileError) as caught:
            self.verify(fake)
        self.assertEqual(["two_sets", "profile_share"], codes(caught.exception))

    def test_a_branch_that_moved_is_named(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SignalProfileError) as caught:
            self.verify(fake)
        self.assertEqual(["branch_moved"], codes(caught.exception))


if __name__ == "__main__":
    unittest.main()
