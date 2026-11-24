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
- The warning grid rule is written after the unscored share is measured, or not at all, or it is written again on a rerun that changes nothing.
- A rerun whose rule or split snapshot differs from the stored row writes anything, instead of refusing before the plan.
- The rule table is read back wrongly (no row, two rows, another value), or the patient split is written.
- The plan measures on the constants and not on the saved row: the plan is given the row that was read back from the table, and a stand-in that saves another
  margin refuses before any profile table is written.
- An output table has a schema that is not the card's: the first commit (the rule) is made and the table's schema refuses after it, leaving a partial result.
  Every output table's schema is classified before the first commit, the predecessor 12-column ``unscored_share`` is rebuilt on the job's branch by a drop and a
  create (never a replace, never an overwrite of other columns), and any other schema refuses with ``table_schema`` before anything is written.
- The drop of a table that is rebuilt is not the one commit directly on the checked hash, or the create is made on another hash than the drop's.
"""

from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from features.registry import MAX_INVALID_NUMERIC_PERCENT, MAX_INVALID_WAVEFORM_SECONDS, NUMERIC_WINDOW_SECONDS, WAVEFORM_WINDOW_SECONDS
from spark.profile.checks import CountsFacts, InputFacts, ProfileFacts, SignalProfileError, UnscoredFacts
from spark.profile.counts import PRESPECIFIED
from spark.profile.grid_rule import FIRST_CUTOFF_OFFSET_SECONDS, GRID_STEP_SECONDS, LAST_CUTOFF_MARGIN_SECONDS, GridRule, RuleRead, planned_rule
from spark.profile.run import COUNTS, PROFILE, RULE, TABLES, UNSCORED, InputRead, Operations, TableRead, run_build, run_verify
from spark.profile.tables import ABSENT, CURRENT, OTHER, PREDECESSOR
from spark.silver.run import Fingerprint

HEAD = "a" * 64
PREPARED = "b" * 64
HASHES = {RULE: "1" * 64, PROFILE: "c" * 64, UNSCORED: "d" * 64, COUNTS: "e" * 64}
DROPPED = "9" * 64
FACTS = InputFacts(numeric_rows=500, waveform_rows=300, tracks=12, arterial_cases=3)
SPLIT_SNAPSHOT = 13
INPUT = InputRead(FACTS, (11, 12, SPLIT_SNAPSHOT, 14, 15), ())
RULE_ROW = planned_rule(SPLIT_SNAPSHOT)
FINGERPRINTS = {PROFILE: Fingerprint(12, 5), UNSCORED: Fingerprint(1, 6), COUNTS: Fingerprint(len(PRESPECIFIED), 7)}
PROFILE_FACTS = ProfileFacts(12, 0, 0, 0, 0, 500, 300, 12)
UNSCORED_FACTS = UnscoredFacts(
    rows=1,
    cutoffs=100,
    waveform=10,
    numeric=5,
    either=12,
    share=0.12,
    cases=3,
    no_grid=1,
    cutoff_step=GRID_STEP_SECONDS,
    cutoff_offset=FIRST_CUTOFF_OFFSET_SECONDS,
    last_margin=LAST_CUTOFF_MARGIN_SECONDS,
    waveform_window=WAVEFORM_WINDOW_SECONDS,
    waveform_limit=MAX_INVALID_WAVEFORM_SECONDS,
    numeric_window=NUMERIC_WINDOW_SECONDS,
    numeric_limit=MAX_INVALID_NUMERIC_PERCENT,
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
        self.planned = dict(FINGERPRINTS)
        self.moved_after: int | None = None
        self.held_rule: RuleRead | None = None
        self.rule_back: RuleRead | None = RuleRead(1, RULE_ROW, 104, True)
        self.rules_written: list[GridRule] = []
        self.planned_with: list[GridRule] = []
        self.states: dict[str, str] = dict.fromkeys((*TABLES, RULE), ABSENT)
        self.drop_hash_of: str | None = DROPPED

    def settled(self) -> Fake:
        """A branch that holds every table already, the rule table too."""
        self.held = dict(self.read_back)
        self.held_rule = self.rule_back
        self.states = dict.fromkeys((*TABLES, RULE), CURRENT)
        return self

    def head(self) -> str:
        self.calls.append("head")
        if self.moved_after is not None and self.calls.count("head") > self.moved_after:
            return "f" * 64
        return self.current

    def read_input(self, at: str) -> InputRead:
        self.calls.append(f"read_input {at[:1]}")
        return self.later_input if self.later_input is not None and at != HEAD else self.input

    def schemas(self, at: str) -> dict[str, str]:
        self.calls.append(f"schemas {at[:1]}")
        return dict(self.states)

    def plan(self, at: str, rule: GridRule) -> dict[str, Fingerprint]:
        self.calls.append(f"plan {at[:1]}")
        self.planned_with.append(rule)
        return dict(self.planned)

    def drop(self, table: str) -> None:
        self.calls.append(f"drop {table}")

    def drop_hash(self, checked: str, table: str) -> str | None:
        self.calls.append(f"drop_hash {checked[:1]} {table}")
        if self.drop_hash_of is not None:
            self.current = self.drop_hash_of
        return self.drop_hash_of

    def read_table(self, at: str, table: str, with_facts: bool) -> TableRead | None:
        self.calls.append(f"read_table {at[:1]} {table} facts={with_facts}")
        return self.held[table] if at == HEAD else self.read_back[table]

    def read_rule(self, at: str) -> RuleRead | None:
        self.calls.append(f"read_rule {at[:1]}")
        return self.held_rule if at == HEAD else self.rule_back

    def write_rule(self, rule: GridRule) -> None:
        self.calls.append(f"write_rule {RULE}")
        self.rules_written.append(rule)

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
        return Operations(
            self.head,
            self.read_input,
            self.schemas,
            self.plan,
            self.read_table,
            self.write,
            self.write_hash,
            self.prepare,
            self.release,
            self.read_rule,
            self.write_rule,
            self.drop,
            self.drop_hash,
        )


class Build(unittest.TestCase):
    def test_a_clean_build_writes_each_table_on_the_hash_before_and_reads_back_at_the_last_write(self) -> None:
        fake = Fake()
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(
            [
                "read_input a",
                "schemas a",
                "read_rule a",
                "head",
                "prepare a",
                "write_rule warning_grid_rule",
                "write_hash b warning_grid_rule",
                "read_rule 1",
                "plan a",
                "read_table a signal_profile facts=False",
                "read_table a unscored_share facts=False",
                "read_table a source_counts facts=False",
                "head",
                "write signal_profile exists=False",
                "write_hash 1 signal_profile",
                "write unscored_share exists=False",
                "write_hash c unscored_share",
                "write source_counts exists=False",
                "write_hash d source_counts",
                "read_input e",
                "read_table e signal_profile facts=True",
                "read_table e unscored_share facts=True",
                "read_table e source_counts facts=True",
                "read_rule e",
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
                "source_snapshots": {"numeric_observations": 11, "waveform_seconds": 12, "patient_split": 13, "clinical_cases": 14, "track_list": 15},
                "grid_rule": {"version": RULE_ROW.rule_version, "snapshot": 104, "split_snapshot": SPLIT_SNAPSHOT, "written": True},
                "snapshots": {PROFILE: 101, UNSCORED: 102, COUNTS: 103},
                "rows": {PROFILE: 12, UNSCORED: 1, COUNTS: len(PRESPECIFIED)},
                "development": {"numeric_rows": 500, "waveform_rows": 300, "tracks": 12, "arterial_cases": 3},
                "unscored": {"cutoffs": 100, "waveform": 10, "numeric": 5, "either": 12, "no_grid": 1},
                "source_counts": {"pinned": False, "mismatched": []},
                "rebuilt": [],
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
        fake = Fake().settled()
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual("unchanged", outcome.outcome)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])
        self.assertIn("read_table a signal_profile facts=True", fake.calls)
        self.assertEqual((HEAD, None, HEAD), (outcome.checked_hash, outcome.write_hash, outcome.facts()["branch_hash"]))

    def test_a_rebuild_that_changes_one_table_rewrites_that_table_only_and_in_place(self) -> None:
        fake = Fake().settled()
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
        fake.moved_after = 2
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))

    def test_a_failure_that_is_not_a_check_still_releases_the_plan(self) -> None:
        fake = Fake()

        def broken(_at: str, _rule: GridRule) -> dict[str, Fingerprint]:
            raise RuntimeError("stop")

        with self.assertRaises(RuntimeError):
            run_build(dataclasses.replace(fake.operations(), plan=broken), HEAD, pinned=False)
        self.assertEqual("release", fake.calls[-1])


class WarningGridRule(unittest.TestCase):
    def test_the_rule_is_written_before_the_unscored_share_is_planned_and_measured_and_before_every_table(self) -> None:
        fake = Fake()
        run_build(fake.operations(), HEAD, pinned=False)
        rule_at = fake.calls.index(f"write_rule {RULE}")
        self.assertLess(rule_at, fake.calls.index("plan a"))
        self.assertLess(fake.calls.index(f"write_hash b {RULE}"), fake.calls.index("plan a"))
        self.assertTrue(all(rule_at < index for index, call in enumerate(fake.calls) if call.startswith("write ") and RULE not in call))

    def test_the_row_written_is_the_constants_and_the_snapshot_of_the_split_that_was_read(self) -> None:
        fake = Fake()
        fake.input = InputRead(FACTS, (11, 12, 777, 14, 15), ())
        fake.rule_back = RuleRead(1, planned_rule(777), 104, True)
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual([planned_rule(777)], fake.rules_written)

    def test_the_rule_is_read_back_at_the_last_write_and_must_be_the_planned_row(self) -> None:
        for back, expected in (
            (None, ["table_missing"]),
            (RuleRead(2, RULE_ROW, 104, True), ["grid_rule_rows"]),
            (RuleRead(0, None, 104, True), ["grid_rule_rows"]),
            (RuleRead(1, planned_rule(SPLIT_SNAPSHOT + 1), 104, True), ["grid_rule_changed"]),
            (RuleRead(1, RULE_ROW, 104, False), ["grid_rule_changed"]),
        ):
            with self.subTest(back=back):
                fake = Fake()
                fake.rule_back = back
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(expected, codes(caught.exception))

    def test_a_rerun_with_the_same_rule_and_split_writes_no_rule_and_changes_nothing(self) -> None:
        fake = Fake().settled()
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(("unchanged", None), (outcome.outcome, outcome.write_hash))
        self.assertEqual([], fake.rules_written)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])

    def test_a_rule_that_is_stored_does_not_stop_the_tables_from_being_written(self) -> None:
        fake = Fake()
        fake.held_rule = fake.rule_back
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual([], fake.rules_written)
        self.assertEqual(["prepare a", "write signal_profile exists=False"], [call for call in fake.calls if call.startswith(("prepare", "write "))][:2])

    def test_a_changed_rule_or_split_refuses_before_the_plan_and_writes_nothing(self) -> None:
        changed = dataclasses.replace(RULE_ROW, last_cutoff_margin_seconds=RULE_ROW.last_cutoff_margin_seconds + 60)
        for held in (
            RuleRead(1, changed, 104, True),
            RuleRead(1, dataclasses.replace(RULE_ROW, rule_version="older"), 104, True),
            RuleRead(1, planned_rule(SPLIT_SNAPSHOT + 1), 104, True),
            RuleRead(1, planned_rule(SPLIT_SNAPSHOT), 104, False),
        ):
            with self.subTest(held=held):
                fake = Fake().settled()
                fake.held_rule = held
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(["grid_rule_changed"], codes(caught.exception))
                self.assertEqual(["read_input a", "schemas a", "read_rule a", "release"], fake.calls)
                self.assertEqual([], fake.rules_written)

    def test_a_stored_table_with_no_row_or_two_rows_refuses_and_is_never_written_over(self) -> None:
        for held in (RuleRead(0, None, 104, True), RuleRead(2, RULE_ROW, 104, True)):
            with self.subTest(rows=held.rows):
                fake = Fake()
                fake.held_rule = held
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(["grid_rule_rows"], codes(caught.exception))
                self.assertEqual(["read_input a", "schemas a", "read_rule a", "release"], fake.calls)

    def test_a_rule_write_that_is_not_the_one_commit_on_the_checked_hash_stops_before_the_plan(self) -> None:
        fake = Fake()
        fake.hashes[RULE] = None
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertNotIn("plan a", fake.calls)
        self.assertEqual("release", fake.calls[-1])

    def test_a_branch_that_moved_before_the_rule_write_gets_no_write(self) -> None:
        fake = Fake()
        fake.moved_after = 0
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertEqual([], fake.rules_written)

    def test_the_patient_split_is_never_written_and_only_the_four_tables_are(self) -> None:
        fake = Fake()
        run_build(fake.operations(), HEAD, pinned=False)
        written = {call.split()[1] for call in fake.calls if call.startswith(("write ", "write_rule ")) and not call.startswith("write_hash")}
        hashed = {call.split()[2] for call in fake.calls if call.startswith("write_hash")}
        self.assertEqual({RULE, PROFILE, UNSCORED, COUNTS}, written)
        self.assertEqual({RULE, PROFILE, UNSCORED, COUNTS}, hashed)
        self.assertFalse([call for call in fake.calls if "patient_split" in call])

    def test_a_verify_checks_the_stored_rule_and_writes_nothing(self) -> None:
        for held, expected in (
            (None, ["table_missing"]),
            (RuleRead(1, planned_rule(SPLIT_SNAPSHOT + 1), 104, True), ["grid_rule_changed"]),
            (RuleRead(2, RULE_ROW, 104, True), ["grid_rule_rows"]),
        ):
            with self.subTest(held=held):
                fake = Fake().settled()
                fake.held_rule = held
                with self.assertRaises(SignalProfileError) as caught:
                    run_verify(fake.operations(), HEAD, pinned=False)
                self.assertEqual(expected, codes(caught.exception))
                self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare"))])


class PlanOnTheSavedRow(unittest.TestCase):
    def test_the_plan_is_given_the_row_that_was_read_back_after_the_write(self) -> None:
        fake = Fake()
        fake.input = InputRead(FACTS, (11, 12, 777, 14, 15), ())
        fake.rule_back = RuleRead(1, planned_rule(777), 104, True)
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual([planned_rule(777)], fake.planned_with)
        self.assertLess(fake.calls.index("read_rule 1"), fake.calls.index("plan a"))

    def test_the_plan_is_given_the_row_that_the_branch_stores_when_the_rule_is_not_written(self) -> None:
        fake = Fake().settled()
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual([RULE_ROW], fake.planned_with)

    def test_a_stand_in_that_saves_another_margin_refuses_before_the_plan_and_before_any_profile_table(self) -> None:
        saved = dataclasses.replace(RULE_ROW, last_cutoff_margin_seconds=RULE_ROW.last_cutoff_margin_seconds + 60)
        for name, back, expected in (
            ("another margin", RuleRead(1, saved, 104, True), ["grid_rule_changed"]),
            ("another step", RuleRead(1, dataclasses.replace(RULE_ROW, grid_step_seconds=30), 104, True), ["grid_rule_changed"]),
            ("two rows", RuleRead(2, RULE_ROW, 104, True), ["grid_rule_rows"]),
            ("no table", None, ["table_missing"]),
        ):
            with self.subTest(name):
                fake = Fake()
                fake.rule_back = back
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(expected, codes(caught.exception))
                self.assertEqual([], fake.planned_with)
                self.assertFalse([call for call in fake.calls if call.startswith(("plan", "write signal", "write unscored", "write source"))])
                self.assertEqual("release", fake.calls[-1])

    def test_a_verify_gives_the_plan_the_stored_row(self) -> None:
        fake = Fake().settled()
        run_verify(fake.operations(), HEAD, pinned=False)
        self.assertEqual([RULE_ROW], fake.planned_with)

    def test_a_verify_of_a_stored_row_that_differs_does_not_plan(self) -> None:
        fake = Fake().settled()
        fake.held_rule = RuleRead(1, dataclasses.replace(RULE_ROW, last_cutoff_margin_seconds=360), 104, True)
        with self.assertRaises(SignalProfileError):
            run_verify(fake.operations(), HEAD, pinned=False)
        self.assertEqual([], fake.planned_with)


class Preflight(unittest.TestCase):
    def test_a_table_with_another_schema_refuses_before_the_first_commit_and_writes_nothing(self) -> None:
        for table in (*TABLES, RULE):
            with self.subTest(table=table):
                fake = Fake()
                fake.states[table] = OTHER
                with self.assertRaises(SignalProfileError) as caught:
                    run_build(fake.operations(), HEAD, pinned=False)
                self.assertEqual(["table_schema"], codes(caught.exception))
                self.assertEqual("table_schema: 1 table", caught.exception.problems[0])
                self.assertEqual(["read_input a", "schemas a", "release"], fake.calls)

    def test_two_tables_with_another_schema_are_counted_and_none_is_named(self) -> None:
        fake = Fake()
        fake.states[PROFILE] = OTHER
        fake.states[RULE] = OTHER
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["table_schema: 2 tables"], caught.exception.problems)

    def test_the_rule_commit_is_not_made_when_the_unscored_table_has_another_schema(self) -> None:
        # Before the fix the rule was committed and the schema refused after it, which left a partial result.
        fake = Fake()
        fake.states[UNSCORED] = OTHER
        with self.assertRaises(SignalProfileError):
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual([], fake.rules_written)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare", "drop"))])

    def test_an_input_defect_is_named_before_the_schemas_are_read(self) -> None:
        fake = Fake()
        fake.input = InputRead(FACTS, (11, 12, 13, 14, 15), ("rule_mismatch: failed",))
        fake.states[PROFILE] = OTHER
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["rule_mismatch"], codes(caught.exception))
        self.assertEqual(["read_input a", "release"], fake.calls)

    def test_a_verify_refuses_a_table_with_another_schema_or_the_predecessor_schema_and_reads_no_table(self) -> None:
        for state in (OTHER, PREDECESSOR):
            with self.subTest(state=state):
                fake = Fake().settled()
                fake.states[UNSCORED] = state
                with self.assertRaises(SignalProfileError) as caught:
                    run_verify(fake.operations(), HEAD, pinned=False)
                self.assertEqual(["table_schema: 1 table"], caught.exception.problems)
                self.assertEqual(["read_input a", "schemas a", "release"], fake.calls)

    def test_a_verify_names_an_input_defect_with_the_schema_refusal(self) -> None:
        fake = Fake().settled()
        fake.input = InputRead(FACTS, (11, 12, 13, 14, 15), ("two_sets: 1 patient",))
        fake.states[COUNTS] = OTHER
        with self.assertRaises(SignalProfileError) as caught:
            run_verify(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["two_sets", "table_schema"], codes(caught.exception))

    def test_a_predecessor_schema_on_a_table_other_than_the_unscored_share_is_not_a_rebuild(self) -> None:
        fake = Fake()
        fake.states[PROFILE] = PREDECESSOR
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["table_schema: 1 table"], caught.exception.problems)
        self.assertFalse([call for call in fake.calls if call.startswith(("write", "prepare", "drop"))])


class PredecessorRebuild(unittest.TestCase):
    def predecessor(self) -> Fake:
        fake = Fake().settled()
        fake.states[UNSCORED] = PREDECESSOR
        fake.held[UNSCORED] = None
        return fake

    def test_the_12_column_unscored_share_is_dropped_and_created_as_two_commits_in_order_and_nothing_else_is_written(self) -> None:
        fake = self.predecessor()
        outcome = run_build(fake.operations(), HEAD, pinned=False)
        writes = [call for call in fake.calls if call.startswith(("drop", "write", "prepare"))]
        self.assertEqual(
            [
                "prepare a",
                "drop unscored_share",
                "drop_hash b unscored_share",
                "write unscored_share exists=False",
                f"write_hash {DROPPED[:1]} unscored_share",
            ],
            writes,
        )
        self.assertEqual(("written", HASHES[UNSCORED]), (outcome.outcome, outcome.write_hash))
        self.assertEqual(["unscored_share"], outcome.facts()["rebuilt"])

    def test_the_predecessor_table_is_never_read_as_a_table_of_the_card(self) -> None:
        fake = self.predecessor()
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertNotIn("read_table a unscored_share facts=False", fake.calls)
        self.assertIn("read_table a signal_profile facts=False", fake.calls)

    def test_a_table_is_never_replaced_or_overwritten_over_the_predecessor_columns(self) -> None:
        fake = self.predecessor()
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertNotIn("write unscored_share exists=True", fake.calls)

    def test_a_drop_that_is_not_the_one_commit_on_the_checked_hash_stops_before_the_create(self) -> None:
        fake = self.predecessor()
        fake.drop_hash_of = None
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertNotIn("write unscored_share exists=False", fake.calls)
        self.assertEqual("release", fake.calls[-1])

    def test_a_branch_that_moved_before_the_drop_gets_no_drop(self) -> None:
        fake = self.predecessor()
        fake.moved_after = 0
        with self.assertRaises(SignalProfileError) as caught:
            run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["branch_moved"], codes(caught.exception))
        self.assertFalse([call for call in fake.calls if call.startswith(("drop", "write", "prepare"))])

    def test_a_branch_with_the_predecessor_table_and_no_rule_writes_the_rule_first_and_then_rebuilds(self) -> None:
        fake = self.predecessor()
        fake.held_rule = None
        fake.states[RULE] = ABSENT
        run_build(fake.operations(), HEAD, pinned=False)
        order = [call for call in fake.calls if call.startswith(("write_rule", "drop ", "write "))]
        self.assertEqual(["write_rule warning_grid_rule", "drop unscored_share", "write unscored_share exists=False"], order)
        self.assertIn("drop_hash 1 unscored_share", fake.calls)

    def test_a_rebuild_with_a_table_that_is_not_there_creates_it_and_does_not_drop(self) -> None:
        fake = Fake().settled()
        fake.states[COUNTS] = ABSENT
        fake.held[COUNTS] = None
        run_build(fake.operations(), HEAD, pinned=False)
        self.assertEqual(["write source_counts exists=False"], [call for call in fake.calls if call.startswith(("write ", "drop"))])

    def test_the_result_of_a_build_that_rebuilt_nothing_names_no_table(self) -> None:
        self.assertEqual([], run_build(Fake().operations(), HEAD, pinned=False).facts()["rebuilt"])


class Verify(unittest.TestCase):
    def verify(self, fake: Fake, pinned: bool = False) -> Any:
        fake.settled()
        return run_verify(fake.operations(), HEAD, pinned=pinned)

    def test_a_good_branch_is_verified_and_nothing_is_written(self) -> None:
        fake = Fake()
        self.assertEqual("verified", self.verify(fake).outcome)
        self.assertEqual(
            [
                "read_input a",
                "schemas a",
                "read_rule a",
                "plan a",
                "read_table a signal_profile facts=True",
                "read_table a unscored_share facts=True",
                "read_table a source_counts facts=True",
                "head",
                "release",
            ],
            fake.calls,
        )

    def test_a_stored_table_that_is_not_the_plan_at_the_checked_hash_is_named(self) -> None:
        for pinned in (False, True):
            for table in TABLES:
                with self.subTest(table=table, pinned=pinned):
                    fake = Fake()
                    fake.planned[table] = Fingerprint(FINGERPRINTS[table].rows, FINGERPRINTS[table].digest + 1)
                    with self.assertRaises(SignalProfileError) as caught:
                        self.verify(fake, pinned)
                    self.assertEqual(["plan_differs"], codes(caught.exception))
                    self.assertEqual("1 table", caught.exception.problems[0].split(": ")[1])
                    self.assertEqual("release", fake.calls[-1])

    def test_a_stale_profile_with_the_same_counts_is_not_verified(self) -> None:
        for pinned in (False, True):
            with self.subTest(pinned=pinned):
                fake = Fake()
                fake.planned[PROFILE] = Fingerprint(FINGERPRINTS[PROFILE].rows, 999)
                fake.read_back[PROFILE] = TableRead(FINGERPRINTS[PROFILE], 101, profile=PROFILE_FACTS)
                with self.assertRaises(SignalProfileError):
                    self.verify(fake, pinned)

    def test_a_good_branch_is_verified_in_either_mode(self) -> None:
        for pinned in (False, True):
            with self.subTest(pinned=pinned):
                self.assertEqual({"pinned": pinned, "mismatched": []}, self.verify(Fake(), pinned).facts()["source_counts"])

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
