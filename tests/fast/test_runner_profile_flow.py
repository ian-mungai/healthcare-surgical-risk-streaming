"""The signal profile runner's cases run end to end on stand-ins, so that a case that raises, saves a value or records a failing flag is found without Docker.

The runner stays untracked, so each test skips when it is not on this machine. Nessie, the stack and the stand-in answers are fakes with healthy replies. Each
case must pass and raise nothing, record flags only, and save only what Nessie confirmed (fix round 7, owner decision D).

Failure modes covered here:

- A case of the profile runner passed a value that ``sealed_observed`` refuses (a flag that is not a Boolean, a name that is not a plain phrase, an exit code
  that is not a process code) and stopped the run in the middle, or recorded a value of a reply instead of a flag.
- The control saved a hash, a snapshot ID or a reference name that Nessie did not hold, or saved a field of the reply that is not an ID.
- A plant or a read that failed still let its case pass.
- Every flag and exit name that a runner passes to ``sealed_observed`` is a plain phrase (the helper raises on any other, in the middle of a heavy run).
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from tests.e2e.run_record import Record
from tests.fast.sealed_record import FLAG_NAME
from tests.fast.test_heldout_figures import figure_derivable
from tests.fast.test_runner_gold_sealed import runner

BRANCH_HASH, BASE_HASH = "b" * 64, "a" * 64
SECRET = 31337
PATIENT_COUNT_NAMES = ("patients", "development_patients_training", "development_patients_validation")
E2E = Path(__file__).resolve().parents[1] / "e2e"


class Nessie:
    """A Nessie that holds the references and tables that the test names."""

    def __init__(self, tables: dict[str, set[str]] | None = None) -> None:
        self.tables = tables or {}
        self.merged: list[Any] = []
        self.merge_keywords: list[dict[str, Any]] = []
        self.moved: dict[str, str] = {}
        self.tags: list[Any] = []

    def entries(self, branch: str) -> dict[str, int]:
        return {name: 1 for name in self.tables.get(branch, set())}

    def reference(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(hash=self.moved.get(name, BRANCH_HASH), kind="TAG" if name.startswith("run-") else "BRANCH")

    def merge(self, *arguments: Any, **keywords: Any) -> str:
        self.merged.append(arguments)
        self.merge_keywords.append(keywords)
        return BRANCH_HASH

    def create_tag(self, *arguments: Any) -> None:
        self.tags.append(arguments)


class ProfileCases(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner("signal_profile")
        self.record = Record(feature="test", slug="test", rerun="none")
        self.tables = {"silver.signal_profile", "silver.unscored_share", "silver.source_counts", "silver.warning_grid_rule"}
        self.nessie = Nessie({"p6_prof_profile_x1": self.tables, "p6_prof_trunk_x1": {"silver.patient_split"}})
        self.stack = SimpleNamespace(
            unknown_lines=0,
            stalls=[],
            spark_containers=lambda: [],
            write_spec=lambda *_: None,
        )
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.stack, run.nessie, run.trunk, run.facts, run.kept = self.record, self.stack, self.nessie, "p6_prof_trunk_x1", {}, {}
        run.made, run.tags, run.suffix, run.counter = ["p6_prof_trunk_x1"], [], "x1", 0
        run.files = {case: {"path": f"/data/vital_files/{case:04d}.vital", "sha256": "c" * 64} for case in self.module.CASES}
        run.patients = {case: str(1000 + case) for case in self.module.CASES}
        run.name = lambda base: f"p6_prof_{base}_x1"
        run.head = lambda branch: BASE_HASH if branch == "p6_prof_trunk_x1" else BRANCH_HASH
        run.fork = lambda base, label: f"p6_prof_{label}_x1"
        run.table_snapshot = lambda branch, table: 5 if table == "silver.patient_split" else 7
        run.commits_since = lambda branch, base: [
            [("PUT", ("silver", "signal_profile"))],
            [("PUT", ("silver", "unscored_share"))],
            [("PUT", ("silver", "source_counts"))],
            [("PUT", ("silver", "warning_grid_rule"))],
        ]
        self.flow = run

    def result(self, **edits: Any) -> dict[str, Any]:
        found = {
            "outcome": "written",
            "base_hash": BASE_HASH,
            "branch_hash": BRANCH_HASH,
            "checked_hash": BRANCH_HASH,
            "write_hash": BRANCH_HASH,
            "development": {"numeric_rows": SECRET, "waveform_rows": SECRET + 1, "tracks": SECRET + 2, "arterial_cases": 1},
            "grid_rule": {"version": self.module.GRID_RULE_VERSION, "snapshot": 7, "split_snapshot": 5, "written": True},
            "snapshots": {name: 7 for name in self.module.PROFILE_TABLE_NAMES},
            "source_snapshots": {**{name: 7 for name in self.module.SOURCE_NAMES}, "patient_split": 5},
            "rows": {"signal_profile": SECRET + 3},
            "fingerprints": {"signal_profile": [SECRET, "d" * 64]},
            "unscored": {"cutoffs": SECRET + 4},
            "source_counts": {"pinned": True, "passed": True},
            "rebuilt": [],
        }
        return {**found, **edits}

    def saved(self) -> str:
        self.record.environment.update(self.flow.facts)
        return json.dumps(asdict(self.record), default=str)

    def quiet(self) -> Any:
        return contextlib.redirect_stdout(io.StringIO())

    def test_the_control_passes_saves_only_what_nessie_confirmed_and_records_no_value_of_the_reply(self) -> None:
        self.stack.profile = lambda *_, **__: (0, self.result(), "", 0.5)
        built = self.flow.control()
        self.assertTrue(built)
        self.assertTrue(all(item.passed for item in self.record.observations), [item.observed for item in self.record.observations])
        self.assertEqual(3, len(self.record.observations))
        saved = self.saved()
        for value in (SECRET, SECRET + 1, SECRET + 2, SECRET + 3, SECRET + 4, "d" * 64):
            self.assertNotIn(str(value), saved)
        self.assertEqual(
            {"trunk", "branch", "base_hash", "branch_hash"}
            | {f"snapshot_{name}" for name in self.module.PROFILE_TABLE_NAMES}
            | {f"source_{name}" for name in self.module.SOURCE_NAMES},
            set(self.flow.facts),
        )
        self.assertEqual((BASE_HASH, BRANCH_HASH), (self.flow.facts["base_hash"], self.flow.facts["branch_hash"]))
        self.assertEqual({"7"}, {self.flow.facts[f"snapshot_{name}"] for name in self.module.PROFILE_TABLE_NAMES})
        self.assertEqual({"p6_prof_trunk_x1", "p6_prof_profile_x1"}, {self.flow.facts["trunk"], self.flow.facts["branch"]})
        self.assertEqual(self.result()["fingerprints"], self.flow.kept["fingerprints"])

    def test_the_control_saves_nothing_when_nessie_does_not_hold_what_the_reply_names(self) -> None:
        wrong: dict[str, dict[str, Any]] = {
            "a branch hash": {"branch_hash": "c" * 64},
            "a base hash": {"base_hash": "c" * 64},
            "a snapshot ID": {"snapshots": {**self.result()["snapshots"], "signal_profile": 8}},
            "an input snapshot ID": {"source_snapshots": {**self.result()["source_snapshots"], "track_list": 8}},
            "a missing input": {"source_snapshots": {"track_list": 7}},
            "a count as the branch hash": {"branch_hash": SECRET},
            "another outcome": {"outcome": "unchanged"},
            "no arterial case": {"development": {"numeric_rows": SECRET, "waveform_rows": SECRET, "tracks": SECRET, "arterial_cases": 0}},
            "a rule that was not written": {"grid_rule": {**self.result()["grid_rule"], "written": False}},
            "a rule of another version": {"grid_rule": {**self.result()["grid_rule"], "version": "another"}},
            "a rule row at another snapshot": {"grid_rule": {**self.result()["grid_rule"], "snapshot": 8}},
            "another split snapshot in the rule": {"grid_rule": {**self.result()["grid_rule"], "split_snapshot": 6}},
        }
        for label, edit in wrong.items():
            with self.subTest(label):
                self.setUp()
                self.stack.profile = lambda *_, edit=edit, **__: (0, self.result(**edit), "", 0.5)
                self.assertEqual({}, self.flow.control())
                self.assertEqual({}, self.flow.facts)
                self.assertFalse(all(item.passed for item in self.record.observations))
                self.assertNotIn(str(SECRET), self.saved())

    def test_the_control_saves_nothing_when_the_branch_lacks_a_table_or_the_trunk_already_holds_one(self) -> None:
        for label, trunk, branch in (
            ("a table that the branch lacks", {"silver.patient_split"}, self.tables - {"silver.source_counts"}),
            ("the rule table that the branch lacks", {"silver.patient_split"}, self.tables - {"silver.warning_grid_rule"}),
            ("a profile table that the trunk holds", {"silver.patient_split", "silver.signal_profile"}, self.tables),
            ("the rule table that the trunk holds", {"silver.patient_split", "silver.warning_grid_rule"}, self.tables),
        ):
            with self.subTest(label):
                self.setUp()
                self.nessie.tables = {"p6_prof_profile_x1": branch, "p6_prof_trunk_x1": trunk}
                self.stack.profile = lambda *_, **__: (0, self.result(), "", 0.5)
                self.assertEqual({}, self.flow.control())
                self.assertEqual({}, self.flow.facts)

    def test_a_failed_build_returns_nothing_and_records_no_field_of_its_reply(self) -> None:
        self.stack.profile = lambda *_, **__: (1, {}, "signal_profile: usage\n", 0.5)
        self.assertEqual({}, self.flow.control())
        self.assertEqual({}, self.flow.facts)
        self.assertEqual(1, len(self.record.observations))
        self.assertFalse(self.record.observations[0].passed)

    def test_the_verify_the_unpinned_verify_and_the_rebuild_pass_on_healthy_replies_and_fail_on_wrong_ones(self) -> None:
        built = {"branch": "p6_prof_profile_x1", "branch_hash": BRANCH_HASH}
        verified = {"outcome": "verified", "source_counts": {"pinned": True, "passed": True}, "branch_hash": BRANCH_HASH, "write_hash": None}
        unpinned = f"signal_profile: counts_judged_unpinned: {len(self.module.BY_NAME)} rows\n"
        for label, reply, wanted in (
            ("healthy", verified, True),
            ("no write hash field in the pinned verify", {key: value for key, value in verified.items() if key != "write_hash"}, False),
            ("another outcome", {**verified, "outcome": "written"}, False),
            ("another hash", {**verified, "branch_hash": "c" * 64}, False),
            ("a write hash in the pinned verify", {**verified, "write_hash": BRANCH_HASH}, False),
        ):
            with self.subTest(label):
                self.setUp()
                answers = iter([(0, reply, "", 0.5), (1, {}, unpinned, 0.5)])
                self.stack.profile = lambda *_, answers=answers, **__: next(answers)
                self.stack.unknown_lines = 0
                self.assertIs(wanted, self.flow.verify_control(built))
                self.assertEqual([wanted, True], [item.passed for item in self.record.observations])
        self.setUp()
        self.flow.kept = {"grid_rule": self.result()["grid_rule"], "fingerprints": self.result()["fingerprints"], "snapshots": self.result()["snapshots"]}
        again = self.result(outcome="unchanged", write_hash=None, base_hash=BRANCH_HASH, grid_rule={**self.result()["grid_rule"], "written": False})
        self.stack.profile = lambda *_, **__: (0, again, "", 0.5)
        self.nessie.tables["p6_prof_trunk_x1"] = {f"silver.{name}" for name in self.module.ALL_TABLE_NAMES}
        self.flow.head = lambda branch: BRANCH_HASH
        self.flow.merge_tag_rebuild(built)
        self.assertEqual(2, len(self.record.observations))
        self.assertTrue(all(item.passed for item in self.record.observations), [item.observed for item in self.record.observations])
        self.assertEqual({"merged_hash", "tag", "tag_hash"}, set(self.flow.facts))
        self.assertEqual(f"run-prof-{self.flow.suffix}", self.flow.facts["tag"])

    def test_the_rebuild_fails_when_its_reply_has_no_write_hash_field(self) -> None:
        built = {"branch": "p6_prof_profile_x1", "branch_hash": BRANCH_HASH}
        for label, present, wanted in (("the field is null", True, True), ("the field is absent", False, False)):
            with self.subTest(label):
                self.setUp()
                self.flow.kept = {
                    "grid_rule": self.result()["grid_rule"],
                    "fingerprints": self.result()["fingerprints"],
                    "snapshots": self.result()["snapshots"],
                }
                again = self.result(outcome="unchanged", write_hash=None, base_hash=BRANCH_HASH, grid_rule={**self.result()["grid_rule"], "written": False})
                if not present:
                    del again["write_hash"]
                self.stack.profile = lambda *_, again=again, **__: (0, again, "", 0.5)
                self.nessie.tables["p6_prof_trunk_x1"] = {f"silver.{name}" for name in self.module.ALL_TABLE_NAMES}
                self.flow.head = lambda branch: BRANCH_HASH
                self.flow.merge_tag_rebuild(built)
                self.assertIs(wanted, self.record.observations[-1].passed)

    def test_the_verify_fails_when_the_branch_moved_during_the_unpinned_verify_and_no_merge_of_the_new_hash_happens(self) -> None:
        built = {"branch": "p6_prof_profile_x1", "branch_hash": BRANCH_HASH}
        verified = {"outcome": "verified", "source_counts": {"pinned": True, "passed": True}, "branch_hash": BRANCH_HASH, "write_hash": None}
        unpinned = f"signal_profile: counts_judged_unpinned: {len(self.module.BY_NAME)} rows\n"
        moved = "c" * 64
        calls: list[str] = []

        def profile(mode: str, branch: str, base: str | None = None, pinned: bool = False) -> tuple[int, dict[str, Any], str, float]:
            calls.append("pinned" if pinned else "unpinned")
            return (0, verified, "", 0.5) if pinned else (1, {}, unpinned, 0.5)

        self.stack.profile = profile
        self.flow.head = lambda branch: moved if "unpinned" in calls else BRANCH_HASH
        self.nessie.moved["p6_prof_profile_x1"] = moved
        self.assertIs(False, self.flow.verify_control(built))
        self.assertEqual([True, False], [item.passed for item in self.record.observations])
        self.assertIn("the branch is still at the verified hash False", self.record.observations[1].observed)
        self.assertEqual([], self.nessie.merged)
        run = SimpleNamespace(
            setup=lambda: True,
            control=lambda: built,
            independent=lambda *_, **__: True,
            verify_control=lambda built: self.flow.verify_control(built),
            held_out=lambda: "held",
            merge_tag_rebuild=lambda built: self.nessie.merged.append(built),
            rule_refusals=lambda *_: None,
            schema_cases=lambda *_: None,
            wrong_pin=lambda *_: None,
            mismatch=lambda *_: None,
            tampers=lambda *_: None,
            input_faults=lambda *_: None,
            bounded_failure=lambda *_: None,
        )
        self.record.observations.clear()
        calls.clear()
        self.module.cases(run)
        self.assertEqual([], self.nessie.merged)

    def test_the_merge_names_the_verified_hash_and_not_the_hash_that_the_branch_holds_now(self) -> None:
        built = {"branch": "p6_prof_profile_x1", "branch_hash": BRANCH_HASH}
        self.nessie.moved["p6_prof_profile_x1"] = "c" * 64
        self.nessie.tables["p6_prof_trunk_x1"] = {f"silver.{name}" for name in self.module.ALL_TABLE_NAMES}
        self.stack.profile = lambda *_, **__: (0, self.result(outcome="unchanged", write_hash=None), "", 0.5)
        self.flow.head = lambda branch: BRANCH_HASH
        with self.quiet():
            self.flow.merge_tag_rebuild(built)
        self.assertEqual(1, len(self.nessie.merge_keywords))
        self.assertEqual(BRANCH_HASH, self.nessie.merge_keywords[0]["source_hash"])
        self.assertNotIn("c" * 64, [str(item) for item in self.nessie.merge_keywords[0].values()])

    def test_the_verify_returns_false_when_the_unpinned_check_fails(self) -> None:
        built = {"branch": "p6_prof_profile_x1", "branch_hash": BRANCH_HASH}
        verified = {"outcome": "verified", "source_counts": {"pinned": True, "passed": True}, "branch_hash": BRANCH_HASH, "write_hash": None}
        answers = iter([(0, verified, "", 0.5), (1, {}, "signal_profile: usage\n", 0.5)])
        self.stack.profile = lambda *_, **__: next(answers)
        self.assertIs(False, self.flow.verify_control(built))
        self.assertEqual([True, False], [item.passed for item in self.record.observations])

    def healthy_dump(self) -> dict[str, Any]:
        module = self.module
        zero = {name: 0 for name in ("cases", "no_grid", "cutoffs", "waveform", "numeric", "either")}
        rule = {
            "cutoff_step": 60,
            "cutoff_offset": 0,
            "last_margin": 300,
            "waveform_window": 20,
            "waveform_limit": 0,
            "numeric_window": 900,
            "numeric_limit": 10,
        }
        schema = {name: [list(item) for item in module.PROFILE_TABLES[name].columns] for name in module.PROFILE_TABLE_NAMES}
        schema[module.RULE_TABLE_NAME] = [list(item) for item in module.GRID_RULE.columns]
        return {
            "profile": {"absent": 0, "extra": 0, "value_mismatch": 0, "share_mismatch": 0, "expected_rows": 6, "stored_rows": 6},
            "development": {"numeric_rows": SECRET, "waveform_rows": SECRET + 1, "tracks": 3},
            "arterial": {},
            "grid": {},
            "unscored": [{**zero, **rule, "scope": "development", "unscored_share": None}],
            "count_names_equal": True,
            "development_values_equal": True,
            "expected_equal": True,
            "sealed_values_empty": True,
            "count_flags": ["None"],
            "rule": [{**asdict(module.planned_rule(5)), "written_at": "2026-11-24"}],
            "schema": schema,
        }

    def independent(self, dump: dict[str, Any]) -> bool:
        built = {
            "branch": "p6_prof_profile_x1",
            "rows": {"signal_profile": 6},
            "development": {"numeric_rows": SECRET, "waveform_rows": SECRET + 1, "tracks": 3, "arterial_cases": 0},
            "source_snapshots": {"patient_split": 5},
        }
        self.stack.write_spec = lambda *_: None
        self.stack.stand_in = lambda mode, branch: (0, dump, "", 0.1)
        self.flow.stand = lambda mode, branch, **values: self.stack.stand_in(mode, branch)
        return bool(self.flow.independent(built, [908], {"development_cases": 7.0}, "", "control", pinned=False, arterial=False))

    def test_the_independent_read_asks_the_container_for_the_names_of_the_full_cohort_counts_and_reads_no_value_of_them(self) -> None:
        asked: list[dict[str, Any]] = []
        self.stack.write_spec = lambda *_: None

        def stand(mode: str, branch: str, **values: Any) -> tuple[int, dict[str, Any], str, float]:
            asked.append(values)
            return 0, self.healthy_dump(), "", 0.1

        self.flow.stand = stand
        built = {"branch": "p6_prof_profile_x1", "rows": {"signal_profile": 6}, "development": {}, "source_snapshots": {"patient_split": 5}}
        pinned = {name: float(self.module.BY_NAME[name].expected) for name in ("development_cases", *PATIENT_COUNT_NAMES[1:])}
        self.flow.independent(built, [908], pinned, "", "control", pinned=False, arterial=False)
        wanted = sorted(count.name for count in self.module.PRESPECIFIED if count.scope == "full cohort")
        self.assertEqual([wanted], [values["sealed"] for values in asked])
        self.assertGreater(len(wanted), 0)
        self.assertNotIn("development_cases", wanted)

    def test_the_independent_read_sends_the_container_no_total_of_patients_and_nothing_that_the_test_count_follows_from(self) -> None:
        asked: list[dict[str, Any]] = []
        self.stack.write_spec = lambda *_: None

        def stand(mode: str, branch: str, **values: Any) -> tuple[int, dict[str, Any], str, float]:
            asked.append(values)
            return 0, self.healthy_dump(), "", 0.1

        self.flow.stand = stand
        built = {"branch": "p6_prof_profile_x1", "rows": {"signal_profile": 6}, "development": {}, "source_snapshots": {"patient_split": 5}}
        pinned = {name: float(self.module.BY_NAME[name].expected) for name in ("development_cases", *PATIENT_COUNT_NAMES[1:])}
        self.flow.independent(built, [908], pinned, "", "control", pinned=True, arterial=False)
        sent = asked[0]
        self.assertEqual(sorted(count.name for count in self.module.PRESPECIFIED), sent["names"])
        self.assertEqual(
            ["development_cases"], sorted(sent["want_values"]), "the development patient counts are the rule's pinned counts, read in the container"
        )
        self.assertTrue(set(PATIENT_COUNT_NAMES).isdisjoint(sent["count_expected"]), "an expected total of patients is sent")
        self.assertFalse(figure_derivable(sent), "the test count follows from the numbers that the runner sends")

    def test_the_independent_read_returns_true_when_every_comparison_passes_and_records_flags_only(self) -> None:
        self.assertIs(True, self.independent(self.healthy_dump()))
        self.assertEqual(6, len(self.record.observations))
        self.assertTrue(all(item.passed for item in self.record.observations), [item.observed for item in self.record.observations])
        self.assertNotIn(str(SECRET), self.saved())

    def test_the_independent_read_returns_false_and_fails_its_case_for_each_wrong_reply(self) -> None:
        def mutated(**edits: Any) -> dict[str, Any]:
            return {**self.healthy_dump(), **edits}

        dump = self.healthy_dump()
        wrong: dict[str, tuple[int, dict[str, Any]]] = {
            "a profile row left out": (0, mutated(profile={**dump["profile"], "absent": 1})),
            "rows that differ from the result's": (0, mutated(profile={**dump["profile"], "stored_rows": 7})),
            "development counts that differ": (0, mutated(development={**dump["development"], "tracks": 4})),
            "an unscored count that differs": (1, mutated(unscored=[{**dump["unscored"][0], "cutoffs": 1}])),
            "an unscored share with no cutoff": (1, mutated(unscored=[{**dump["unscored"][0], "unscored_share": 0.5}])),
            "another rule": (1, mutated(unscored=[{**dump["unscored"][0], "waveform_limit": 1}])),
            "an arterial second in a development case": (1, mutated(arterial={"908": [[1, True]]})),
            "a development count with another value": (3, mutated(development_values_equal=False)),
            "names of counts that are not the prespecified ones": (3, mutated(count_names_equal=False)),
            "an expected value that is not the prespecified one": (3, mutated(expected_equal=False)),
            "a development flag that is a text": (3, mutated(development_values_equal="True")),
            "no flag of the development values": (3, {key: value for key, value in self.healthy_dump().items() if key != "development_values_equal"}),
            "a flag of the names that is a number": (3, mutated(count_names_equal=1)),
            "no flag of the expected values": (3, {key: value for key, value in self.healthy_dump().items() if key != "expected_equal"}),
            "a row of the counts table in the reply": (3, mutated(counts=[{"name": "patients", "expected": 1.0}])),
            "a sealed count that held a value": (3, mutated(sealed_values_empty=False)),
            "no flag of the sealed counts": (3, {key: value for key, value in self.healthy_dump().items() if key != "sealed_values_empty"}),
            "a flag of the sealed counts that is a text": (3, mutated(sealed_values_empty="True")),
            "a rule row with another snapshot": (4, mutated(rule=[{**dump["rule"][0], "split_snapshot_id": 6}])),
            "a table of other columns": (5, mutated(schema={**dump["schema"], "signal_profile": []})),
        }
        for label, (position, reply) in wrong.items():
            with self.subTest(label):
                self.setUp()
                self.assertIs(False, self.independent(reply))
                self.assertFalse(self.record.observations[position].passed, [item.observed for item in self.record.observations])
                self.assertNotIn(str(SECRET), self.saved())

    def test_the_schema_the_mismatch_and_the_bounded_cases_pass_on_healthy_replies(self) -> None:
        self.stack.stand_in = lambda mode, branch: (0, {"as_planted": True}, "", 0.1)
        self.stack.profile = lambda *_, **__: (0, self.result(outcome="unchanged"), "", 0.5)
        self.flow.stand = lambda mode, branch, **values: self.stack.stand_in(mode, branch)
        self.flow.table_snapshot = lambda branch, table: 8 if branch.startswith("p6_prof_mismatch") else 7
        self.flow.mismatch()
        self.assertTrue(self.record.observations[-1].passed, self.record.observations[-1].observed)
        self.stack.job = lambda *_, **__: (1, {}, "signal_profile: branch_missing\n", 0.1)
        self.flow.bounded_failure()
        self.assertTrue(self.record.observations[-2].passed, self.record.observations[-2].observed)
        self.assertFalse(self.record.observations[-1].passed, "a bad argument that answers branch_missing is not usage")
        for item in self.record.observations:
            self.assertNotIn(str(SECRET), item.observed)

    def test_the_predecessor_case_records_flags_and_exit_codes_only(self) -> None:
        self.flow.stand = lambda mode, branch, **values: (0, {}, "", 0.1)
        self.stack.profile = lambda mode, branch, base=None, pinned=False: (0, self.result(rebuilt=["unscored_share"], grid_rule={"written": False}), "", 0.5)
        self.flow.commits_since = lambda branch, base: [[("PUT", ("silver", "unscored_share"))], [("DELETE", ("silver", "unscored_share"))]]
        self.flow.schema_cases()
        first = self.record.observations[0]
        self.assertIn("exit", first.observed)
        self.assertNotIn(str(SECRET), self.saved())
        self.assertEqual(5, len(self.record.observations))

    def test_the_refusals_pass_on_the_lines_that_the_job_prints_and_fail_on_any_other(self) -> None:
        self.flow.stand = lambda mode, branch, **values: (0, {"rows": 3}, "", 0.1)
        for lines, wanted in (("signal_profile: rule_mismatch: failed\n", True), ("signal_profile: usage\n", False)):
            with self.subTest(lines=lines):
                self.setUp()
                self.flow.stand = lambda mode, branch, **values: (0, {}, "", 0.1)
                self.stack.profile = lambda *_, lines=lines, **__: (1, {}, lines, 0.5)
                self.flow.nessie = Nessie()
                self.flow.head = lambda branch: BASE_HASH
                self.flow.patients = {case: "1000" for case in self.module.CASES}
                self.flow.input_faults(branch := "held")
                self.assertEqual(branch, "held")
                self.assertEqual(wanted, self.record.observations[0].passed)

    def finish(self, status: int | None) -> tuple[bool, list[bool]]:
        from scripts.stack.nessie import NessieError

        def delete(*_: Any, **__: Any) -> None:
            if status is not None:
                raise NessieError("refused", status=status)

        self.flow.nessie = SimpleNamespace(delete_reference=delete, reference=lambda name: SimpleNamespace(hash=BRANCH_HASH))
        self.flow.tags = [("run-prof-x1", BRANCH_HASH)]
        self.flow.made = ["p6_prof_trunk_x1", "p6_prof_profile_x1"]
        self.flow.stack = SimpleNamespace(stalls=[])
        return self.module.finish(self.flow, self.record), [item.passed for item in self.record.observations]

    def test_the_cleanup_of_references_passes_when_each_is_deleted_or_gone_and_fails_on_a_delete_error(self) -> None:
        self.assertEqual((True, [True]), self.finish(None))
        self.setUp()
        self.assertEqual((True, [True]), self.finish(404))
        self.setUp()
        self.assertEqual((False, [False]), self.finish(500))
        self.setUp()
        self.assertEqual((False, [False]), self.finish(409))
        self.assertEqual({"stalled": "False"}, {key: value for key, value in self.record.environment.items() if key == "stalled"})

    def test_the_merge_and_the_tag_wait_for_the_independent_read_and_the_verify(self) -> None:
        """The class of the Gold gate: a build whose independent read or verify failed is neither merged nor tagged, and no later case runs on it."""
        for independent_ok, verify_ok, wanted in ((True, True, 1), (False, True, 0), (True, False, 0), (None, True, 0), (True, None, 0)):
            with self.subTest(independent=independent_ok, verify=verify_ok):
                calls: list[str] = []

                def record(name: str, calls: list[str] = calls) -> Any:
                    return lambda *_, **__: calls.append(name)

                run = SimpleNamespace(
                    setup=lambda: True,
                    control=lambda: {"branch": "built"},
                    independent=lambda *_, ok=independent_ok, **__: ok,
                    verify_control=lambda built, ok=verify_ok: ok,
                    held_out=lambda calls=calls: calls.append("held_out") or "held",
                    merge_tag_rebuild=record("merge"),
                    rule_refusals=record("rule"),
                    schema_cases=record("schema"),
                    wrong_pin=record("pin"),
                    mismatch=record("mismatch"),
                    tampers=record("tampers"),
                    input_faults=record("input"),
                    bounded_failure=record("bounded"),
                )
                self.module.cases(run)
                self.assertEqual(wanted, calls.count("merge"))
                self.assertEqual(wanted, calls.count("held_out"))
                self.assertEqual(wanted * 9, len(calls))


class GoldCases(unittest.TestCase):
    """The Gold runner's planted-fault, bounded and save cases run on stand-ins, record flags and exit codes only, and raise nothing."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.trunk, run.facts, run.suffix = self.record, "p7_trunk_x1", {}, "x1"
        run.stack = SimpleNamespace(spark_containers=lambda: [], job=lambda *_, **__: (124, {}, "gold_dimensions: timeout\n", 0.1), unknown_lines=0)
        run.head = lambda branch: BASE_HASH
        run.name = lambda base: f"p7_{base}_x1"
        run.fork = lambda base, label: f"p7_{label}_x1"
        run.edit = lambda *_, **__: 0
        self.flow = run

    def quiet(self) -> Any:
        return contextlib.redirect_stdout(io.StringIO())

    def test_the_planted_faults_pass_when_the_job_prints_each_wanted_code_and_fail_when_it_prints_none(self) -> None:
        for lines, wanted in (("gold_dimensions: key_rule: dim_case, 1 member\n", True), ("gold_dimensions: usage\n", False)):
            with self.subTest(lines=lines):
                self.setUp()
                self.flow.gold = SimpleNamespace(gold=lambda *_, lines=lines, **__: (1, {}, lines, 0.1), unknown_lines=0)
                self.flow.refused_verify("planted", "branch", ["key_rule: dim_case, 1 member"])
                self.flow.refused_from("planted", "built", "source", ["key_rule: dim_case, 1 member"])
                self.assertEqual([wanted, wanted], [item.passed for item in self.record.observations])
                for item in self.record.observations:
                    self.assertIn("each wanted code is printed", item.observed)
                    self.assertNotIn("dim_case", item.observed)

    def test_the_bounded_cases_pass_on_the_codes_of_the_job(self) -> None:
        answers: Iterator[tuple[int, dict[str, Any], str, float]] = iter(
            [(1, {}, "gold_dimensions: branch_missing\n", 0.1), (2, {}, "gold_dimensions: usage\n", 0.1)]
        )
        self.flow.gold = SimpleNamespace(gold=lambda *_, **__: next(answers), unknown_lines=0)
        self.flow.bounded()
        self.assertEqual([True, True, True], [item.passed for item in self.record.observations], [item.observed for item in self.record.observations])

    def test_every_refusal_case_that_reads_codes_fails_when_the_adapter_dropped_a_line_of_the_job(self) -> None:
        """The lab refusal that also prints a sealed count: the adapter keeps ``empty_source: failed`` and drops ``empty_source: dim_lab_test, 2 members``."""
        wanted = ["key_rule: dim_case, 1 member"]
        for dropped in (0, 1):
            with self.subTest(dropped=dropped):
                self.setUp()
                self.flow.gold = SimpleNamespace(gold=lambda *_, **__: (1, {}, "gold_dimensions: key_rule: dim_case, 1 member\n", 0.1), unknown_lines=dropped)
                self.flow.refused_verify("planted", "branch", wanted)
                self.flow.refused_from("planted", "built", "source", wanted)
                self.assertEqual([dropped == 0] * 2, [item.passed for item in self.record.observations])
                for item in self.record.observations:
                    self.assertIn(f"no line was dropped {dropped == 0}", item.observed)
        for dropped in (0, 1):
            with self.subTest(case="bounded", dropped=dropped):
                self.setUp()
                answers: Iterator[tuple[int, dict[str, Any], str, float]] = iter(
                    [(1, {}, "gold_dimensions: branch_missing\n", 0.1), (2, {}, "gold_dimensions: usage\n", 0.1)]
                )
                self.flow.gold = SimpleNamespace(gold=lambda *_, answers=answers, **__: next(answers), unknown_lines=dropped)
                self.flow.stack = SimpleNamespace(
                    job=lambda *_, **__: (124, {}, "gold_dimensions: timeout\n", 0.1), spark_containers=lambda: [], unknown_lines=dropped
                )
                self.flow.bounded()
                self.assertEqual([dropped == 0] * 3, [item.passed for item in self.record.observations])

    def test_the_planted_history_cases_pass_only_when_the_history_names_the_control(self) -> None:
        night = self.module.NIGHTS[-1]
        self.flow.stack = SimpleNamespace(write_spec=lambda *_: None, stand_in=lambda mode, branch: (0, {"edited": 2}, "", 0.1))
        self.flow.history = lambda *_: None
        self.flow.planted_history("built", {}, night, frozenset())
        self.assertEqual([False, False], [item.passed for item in self.record.observations])
        for item in self.record.observations:
            self.assertIn("the history read is complete False", item.observed)

    def test_the_planted_save_failures_all_give_a_failing_verdict(self) -> None:
        with self.quiet():
            self.assertEqual({True}, set(self.module.planted_save_failures().values()))

    def test_a_save_that_fails_records_the_class_of_the_error_and_never_its_message(self) -> None:
        record = Record(feature="test", slug="test", rerun="none")
        record.expect("a passing step", "control", "it holds", "it held", True)

        def broken(path: Path, text: str) -> None:
            raise OSError(f"disk {SECRET} is full")

        with self.quiet(), contextlib.redirect_stderr(io.StringIO()), tempfile.TemporaryDirectory(prefix="gold_save_") as directory:
            self.module.save_run(record, ["a log line"], E2E / "gold_dimensions.py", set(), runs=Path(directory), write=broken, echo=False)
        text = json.dumps(asdict(record), default=str)
        self.assertNotIn(str(SECRET), text)
        self.assertIn("stopped by OSError", text)


class FlagNames(unittest.TestCase):
    """Every flag and exit name that a runner passes to ``sealed_observed`` is a constant plain phrase, so the helper never raises in the middle of a run."""

    def test_every_name_is_a_plain_phrase(self) -> None:
        present = [E2E / name for name in ("gold_dimensions.py", "signal_profile.py") if (E2E / name).is_file()]
        if not present:
            self.skipTest("the untracked runners are not on this machine")
        for path in present:
            names: list[Any] = []
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "sealed_observed":
                    dicts = [node.args[0]] if node.args and isinstance(node.args[0], ast.Dict) else []
                    dicts += [item.value for item in node.keywords if item.arg in ("flags", "exits") and isinstance(item.value, ast.Dict)]
                    names += [key for item in dicts for key in item.keys]
            with self.subTest(runner=path.name):
                self.assertGreater(len(names), 50)
                for key in names:
                    self.assertTrue(
                        isinstance(key, ast.Constant) and isinstance(key.value, str) and FLAG_NAME.fullmatch(key.value), ast.dump(key) if key else key
                    )


if __name__ == "__main__":
    unittest.main()
