"""The Gold dimensions and signal profile runners record through the sealed helper only (fix round 7, owner decision D).

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, Nessie and Spark are stand-ins. The syntax guard (``sink_leaks``)
reads both runners in ``tests/fast/test_outcome_acceptance.py``. This module tests what the guard cannot see: which replies reach a record at all.

Failure modes covered here:

- Gold recorded the member counts of a build reply before Nessie had confirmed its branch hash (``dim_case`` members), and a build reply reached a record as
  text. A value of a reply reaches a record only as a flag, a fixed code or a confirmed ID.
- Gold saved the IDs of a build and merged after its first build observation failed: a failed observation returns nothing.
- The profile runner recorded fields of the ``RESULT`` line of a job that exited 1. A failed reply is discarded entirely, in both runners.
- The profile filter dropped ``STEP first_read 31337`` and the wrong-pin checks passed. A dropped step or pin line of the stand-in is counted, and a case that
  expects exact lines refuses a run that dropped one.
- The profile and Gold adapters accepted lines that the producer never prints (``grid_rule_rows: 1 row``, the split-only ``sample_file: failed``) and raised
  ``ValueError`` on a count of 4,301 digits. Only the producer's reachable forms pass, and the digits of a count are bounded before the conversion.
- Gold had a loader predicate of its own beside the one of the Silver cases runner.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from spark import split_table
from tests.e2e.run_record import Record
from tests.fast.test_runner_gold_sealed import healthy_result, runner, texts
from tests.fast.test_runner_setup_builds import stand_in_stack

HUGE = "9" * 4301
MEMBERS = 4417
ROOT = Path(__file__).resolve().parents[2]


def saved(record: Record) -> str:
    return json.dumps(asdict(record), default=str)


class GoldBuildObservation(unittest.TestCase):
    """The first build of Gold records flags only, and saves IDs and returns the build only when its observation passed."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.names = [item.name for item in self.module.DIMENSIONS if not item.sealed]

    def run_of(self, answer: dict[str, Any], *, head: str = "b" * 64, code: int = 0, left: list[str] | None = None) -> tuple[Any, Record]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.trunk, run.facts = record, "p7_trunk_x1", {}
        run.gold = SimpleNamespace(gold=lambda *_, **__: (code, answer, "", 0.1))
        run.stack = SimpleNamespace(spark_containers=lambda: left or [])
        run.nessie = SimpleNamespace(entries=lambda branch: {item.table: 1 for item in self.module.DIMENSIONS})
        run.head = lambda branch: head if branch != "p7_trunk_x1" else "a" * 64
        run.table_snapshot = lambda branch, table: 7
        run.name = lambda base: f"p7_{base}_x1"
        return run, record

    def control(self, run: Any) -> Any:
        with contextlib.redirect_stdout(io.StringIO()):
            return run.control()

    def counted(self, **edits: Any) -> dict[str, Any]:
        found = healthy_result(self.module)
        return {**found, "members": {name: MEMBERS for name in self.names}, **edits}

    def test_the_member_counts_of_a_build_reply_are_never_recorded_even_when_nessie_confirms_it(self) -> None:
        run, record = self.run_of(self.counted())
        self.assertTrue(self.control(run))
        self.assertNotIn(str(MEMBERS), texts(record))
        self.assertNotIn(str(MEMBERS), saved(record))

    def test_the_member_counts_of_a_build_reply_whose_branch_hash_nessie_rejects_are_never_recorded(self) -> None:
        run, record = self.run_of(self.counted(), head="c" * 64)
        self.assertFalse(self.control(run))
        self.assertNotIn(str(MEMBERS), texts(record))
        self.assertNotIn(str(MEMBERS), saved(record))
        self.assertEqual({}, run.facts)

    def test_a_build_whose_first_observation_fails_saves_no_id_and_returns_nothing(self) -> None:
        wrong: dict[str, dict[str, Any]] = {
            "another outcome": {"outcome": "unchanged"},
            "a write indicator that is false": {"written": {name: False for name in self.names}},
            "a base hash that is not the trunk's": {"base_hash": "d" * 64},
        }
        for label, edits in wrong.items():
            with self.subTest(label):
                run, record = self.run_of(self.counted(**edits))
                self.assertFalse(self.control(run))
                self.assertEqual({}, run.facts)
                self.assertFalse(record.observations[0].passed)
                self.assertEqual(1, len(record.observations), "nothing was observed after the first failed observation")
        with self.subTest("a container left"):
            run, record = self.run_of(self.counted(), left=["spark"])
            self.assertFalse(self.control(run))
            self.assertEqual({}, run.facts)
            self.assertEqual(1, len(record.observations))

    def test_the_first_build_is_never_merged_after_its_observation_failed(self) -> None:
        merges: list[tuple[Any, ...]] = []
        run, _ = self.run_of(self.counted(outcome="unchanged"))
        run.read_dimensions = lambda built: True
        run.verify_and_merge = lambda *arguments, **_: merges.append(arguments)
        run.rebuild = run.planted_faults = run.nights = run.bounded = lambda: None
        run.setup = lambda: True
        with contextlib.redirect_stdout(io.StringIO()):
            self.module.cases(run)
        self.assertEqual([], merges)

    def test_a_trunk_or_a_branch_that_is_not_a_reference_name_saves_no_fact_and_stops_the_run(self) -> None:
        for label, trunk, name in (("a trunk", "trunk", "p7_gold_x1"), ("a trunk with a space", "p7 trunk", "p7_gold_x1"), ("a branch", "p7_trunk_x1", "gold")):
            with self.subTest(label):
                run, record = self.run_of(healthy_result(self.module))
                run.trunk = trunk
                run.name = lambda base, name=name: name
                run.head = lambda branch, trunk=trunk: "a" * 64 if branch == trunk else "b" * 64
                self.assertFalse(self.control(run))
                self.assertEqual({}, run.facts)
                self.assertEqual("b" * 64 in saved(record), False)


class GoldFailedReplies(unittest.TestCase):
    """A job that did not exit 0 returns an empty reply: no field of its ``RESULT`` line reaches a record."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def job(self, code: int) -> tuple[int, dict[str, Any], str, float]:
        record, log = Record(feature="test", slug="test", rerun="none"), list[str]()
        directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gold_failed_")))
        stack = self.module.GoldStack(Path("compose"), "proj_x1", directory, record, log)

        def answer(program: str, arguments: list[str], **_: Any) -> Any:
            if "--entrypoint" not in arguments:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(
                returncode=code, stdout=f'RESULT {{"mode": "verify", "branch": "b", "members": {MEMBERS}}}\n', stderr="gold_dimensions: branch_missing\n"
            )

        with mock.patch.object(self.module.base, "run_command", side_effect=answer):
            return stack.gold("verify", "b")  # type: ignore[no-any-return]

    def test_the_result_line_of_a_failed_job_is_discarded_and_its_codes_are_kept(self) -> None:
        code, result, err, _ = self.job(1)
        self.assertEqual((1, {}), (code, result))
        self.assertEqual("gold_dimensions: branch_missing\n", err)

    def test_the_result_line_of_a_job_that_exited_0_is_returned(self) -> None:
        code, result, _, _ = self.job(0)
        self.assertEqual((0, {"mode": "verify", "branch": "b", "members": MEMBERS}), (code, result))


def verify_reply(module: Any, **edits: Any) -> dict[str, Any]:
    """The result of a Gold verify of the branch ``b`` as the job prints it."""
    open_names = [item.name for item in module.DIMENSIONS if not item.sealed]
    return {
        **healthy_result(module),
        "mode": "verify",
        "branch": "b",
        "outcome": "verified",
        "base_hash": None,
        "written": {n: False for n in open_names},
        **edits,
    }


class GoldContradictoryReplies(unittest.TestCase):
    """A reply that names another mode or another branch than the one that was asked is discarded at the launcher (the code is kept), so no verify gate
    accepts it and nothing is merged."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def stack(self, reply: dict[str, Any], code: int = 0) -> Any:
        record, log = Record(feature="test", slug="test", rerun="none"), list[str]()
        directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gold_contra_")))
        stack = self.module.GoldStack(Path("compose"), "proj_x1", directory, record, log)

        def answer(program: str, arguments: list[str], **_: Any) -> Any:
            if "--entrypoint" not in arguments:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=code, stdout="RESULT " + json.dumps(reply) + "\n", stderr="")

        self.enterContext(mock.patch.object(self.module.base, "run_command", side_effect=answer))
        return stack

    def test_a_reply_of_another_mode_or_another_branch_is_discarded_and_the_code_is_kept(self) -> None:
        healthy = verify_reply(self.module)
        for label, reply, wanted in (
            ("healthy", healthy, True),
            ("another mode", {**healthy, "mode": "build"}, False),
            ("another branch", {**healthy, "branch": "other"}, False),
            ("no mode", {key: value for key, value in healthy.items() if key != "mode"}, False),
            ("no branch", {key: value for key, value in healthy.items() if key != "branch"}, False),
        ):
            with self.subTest(label):
                code, result, _, _ = self.stack(reply).gold("verify", "b")
                self.assertEqual(0, code)
                self.assertEqual(reply if wanted else {}, result)

    def test_the_branch_that_was_passed_to_the_job_is_the_one_that_the_reply_must_name(self) -> None:
        stack = self.stack(verify_reply(self.module, branch="argument"))
        self.assertEqual({}, stack.gold("verify", "b")[1])
        stack = self.stack(verify_reply(self.module, branch="argument"))
        self.assertEqual("argument", stack.gold("verify", "b", branch_argument="argument")[1]["branch"])
        stack = self.stack(verify_reply(self.module, branch="b"))
        self.assertEqual({}, stack.gold("verify", "b", branch_argument="argument")[1])

    def test_a_build_reply_of_another_mode_is_discarded_too(self) -> None:
        reply = {**healthy_result(self.module), "mode": "verify"}
        self.assertEqual({}, self.stack(reply).gold("build", "branch", base_branch="trunk")[1])
        self.assertEqual(healthy_result(self.module), self.stack(healthy_result(self.module)).gold("build", "branch", base_branch="trunk")[1])

    def verified(self, reply: dict[str, Any]) -> tuple[Any, list[Any], Record]:
        stack = self.stack(reply)
        record = stack.record
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        merges: list[Any] = []
        run.record, run.gold, run.trunk = record, stack, "p7_trunk_x1"
        run.head = lambda branch: "b" * 64

        def merge(*arguments: Any, **keywords: Any) -> str:
            merges.append((arguments, keywords))
            return "m" * 64

        run.merge = merge
        return run, merges, record

    def test_a_verify_gate_merges_a_healthy_reply_and_merges_nothing_for_a_contradictory_one(self) -> None:
        healthy = verify_reply(self.module)
        for label, reply, wanted in (
            ("healthy", healthy, True),
            ("a build reply to a verify", {**healthy, "mode": "build"}, False),
            ("a reply for another branch", {**healthy, "branch": "other"}, False),
        ):
            with self.subTest(label):
                run, merges, record = self.verified(reply)
                self.assertEqual("m" * 64 if wanted else None, run.verify_and_merge("b", "message"))
                self.assertEqual(1 if wanted else 0, len(merges))
                self.assertEqual(wanted, record.observations[0].passed, texts(record))


class GoldLines(unittest.TestCase):
    """The adapter keeps the lines that the producer can print and raises nothing on a count of any length."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def test_a_count_of_4301_digits_is_dropped_and_raises_nothing(self) -> None:
        for line in (
            f"key_rule: dim_case, {HUGE} members",
            f"unknown_member: dim_track, {HUGE} rows",
            f"source_missing: {HUGE} tables",
            f"empty_source: {HUGE} members",
        ):
            with self.subTest(line=line[:30]):
                self.assertFalse(self.module.known_line(line))
                self.assertEqual([], self.module.codes(f"gold_dimensions: {line}\n"))

    def test_a_count_is_bounded_by_its_digits_before_it_is_converted(self) -> None:
        digits = self.module.MAX_COUNT_DIGITS
        self.assertTrue(self.module.known_line(f"key_rule: dim_case, {'9' * digits} members"))
        self.assertFalse(self.module.known_line(f"key_rule: dim_case, {'9' * (digits + 1)} members"))
        self.assertFalse(self.module.known_line(f"key_rule: dim_case, 0{'9' * digits} members"))

    def test_the_split_only_codes_are_dropped(self) -> None:
        for line in ("sample_file: failed", "sample_empty: failed", "bad_subjectid: failed"):
            with self.subTest(line=line):
                self.assertFalse(self.module.known_line(line))
                self.assertEqual([], self.module.codes(f"gold_dimensions: {line}\n"))

    def test_the_runner_has_no_loader_predicate_of_its_own(self) -> None:
        source = (ROOT / "tests" / "e2e" / "gold_dimensions.py").read_text()
        self.assertNotIn("LOAD_FLAGS", source)
        self.assertNotIn("set(result) ==", source)
        self.assertIn("loader_answered(", source)


class ProfileFailedReplies(unittest.TestCase):
    """A profile job or stand-in that did not exit 0 returns an empty reply, and the codes of its stream are kept."""

    OUT = 'RESULT {"outcome": "written", "rows": {"signal_profile": 31337}, "development": {"numeric_rows": 31337}}\n'

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def test_the_result_line_of_a_failed_job_is_discarded_entirely(self) -> None:
        for arguments, err in (
            (["-m", "spark.jobs.signal_profile", "build", "--branch", "b"], "signal_profile: usage\n"),
            (["/data/stand_in.py", "load", "b"], "STEP pin_check\n"),
        ):
            for code in (1, 2, 124):
                with self.subTest(arguments=arguments[:2], code=code):
                    stack, _, _ = stand_in_stack(self, self.module, None, code, out=self.OUT, err=err)
                    got, result, kept, _ = stack.job(arguments)
                    self.assertEqual((code, {}), (got, result))
                    self.assertEqual(err, kept)

    def test_the_result_line_of_a_job_that_exited_0_is_returned(self) -> None:
        stack, _, _ = stand_in_stack(self, self.module, None, 0, out=self.OUT, err="")
        _, result, _, _ = stack.job(["/data/stand_in.py", "load", "b"])
        self.assertEqual(31337, result["rows"]["signal_profile"])

    def test_a_reply_of_another_mode_or_another_branch_is_discarded_by_the_profile_launcher_and_the_code_is_kept(self) -> None:
        healthy = {"mode": "verify", "branch": "b", "outcome": "verified"}
        for label, reply, wanted in (
            ("healthy", healthy, True),
            ("another mode", {**healthy, "mode": "build"}, False),
            ("another branch", {**healthy, "branch": "other"}, False),
            ("no mode", {"branch": "b"}, False),
            ("no branch", {"mode": "verify"}, False),
        ):
            with self.subTest(label):
                stack, _, _ = stand_in_stack(self, self.module, None, 0, out="RESULT " + json.dumps(reply) + "\n", err="")
                code, result, _, _ = stack.profile("verify", "b", pinned=True)
                self.assertEqual(0, code)
                self.assertEqual(reply if wanted else {}, result)
        stack, _, _ = stand_in_stack(self, self.module, None, 0, out='RESULT {"mode": "build", "branch": "b"}\n', err="")
        self.assertEqual({"mode": "build", "branch": "b"}, stack.profile("build", "b", base="trunk")[1])

    def test_an_observation_of_a_failed_build_holds_no_field_of_its_reply(self) -> None:
        record = Record(feature="test", slug="test", rerun="none")
        stack, _, _ = stand_in_stack(self, self.module, None, 1, out=self.OUT, err="")
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.stack, run.trunk, run.facts = record, stack, "trunk", {}
        run.name = lambda base: base
        run.head = lambda branch: "a" * 64
        stack.spark_containers = lambda: []
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(run.control())
        for text in (texts(record), saved(record)):
            self.assertNotIn("31337", text)
        self.assertEqual({}, run.facts)


class ProfileDroppedSteps(unittest.TestCase):
    """A step or pin line of the stand-in that the filter drops is counted, and a case that expects exact lines refuses a run that dropped one."""

    DROPPED = "STEP pin_check\nSTEP first_read 31337\npin_mismatch: clinical_data\n"

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def test_a_dropped_step_or_pin_line_of_the_stand_in_is_counted(self) -> None:
        for stream, wanted in (
            ("STEP pin_check\npin_mismatch: clinical_data\n", 0),
            (self.DROPPED, 1),
            ("STEP first_read 31337\n", 1),
            ("pin_mismatch: clinical_data 31337\n", 1),
            ("pin_mismatch: 31337\n", 1),
            ("STEP\n", 1),
            ("STEP first_read 31337\nSTEP anything\npin_ok\n", 3),
            ("WARN jvm 31337\nTraceback (most recent call last):\nsomething STEP first_read\n", 0),
        ):
            with self.subTest(stream=stream[:40]):
                kept, dropped = self.module.Stack.known_lines(["/data/stand_in.py", "load_clinical", "b"], stream)
                self.assertEqual(wanted, dropped)
                for line in kept:
                    self.assertTrue(self.module.STAND_IN_LINE.fullmatch(line))

    def test_the_stack_keeps_the_count_for_the_case(self) -> None:
        stack, _, _ = stand_in_stack(self, self.module, None, 1, out="", err=self.DROPPED)
        stack.job(["/data/stand_in.py", "load_clinical", "b"])
        self.assertEqual(1, stack.unknown_lines)

    def wrong_pin(self, unknown: int) -> list[bool]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.Run.__new__(self.module.Run)
        run.record = record
        run.fork = lambda base, label: label
        run.head = lambda branch: "h"
        run.name = lambda base: base
        run.nessie = SimpleNamespace(entries=lambda branch: {})
        names = {"load_clinical": "clinical_data", "load_track_list": "track_list"}
        run.stand = lambda mode, branch, **values: (1, {}, f"STEP pin_check\npin_mismatch: {names[mode]}\n", 0.1)
        run.stack = SimpleNamespace(unknown_lines=unknown)
        run.wrong_pin()
        return [item.passed for item in record.observations]

    def test_the_wrong_pin_cases_pass_with_the_clean_lines_and_fail_when_a_step_line_was_dropped(self) -> None:
        self.assertEqual([True, True], self.wrong_pin(0))
        self.assertEqual([False, False], self.wrong_pin(1))

    def test_the_whole_chain_from_the_stream_to_the_case_refuses_a_dropped_step(self) -> None:
        stack, _, _ = stand_in_stack(self, self.module, None, 1, out="", err=self.DROPPED)
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.stack = record, stack
        run.fork = lambda base, label: label
        run.head = lambda branch: "h"
        run.name = lambda base: base
        run.nessie = SimpleNamespace(entries=lambda branch: {})
        run.stand = lambda mode, branch, **values: stack.job(["/data/stand_in.py", mode, branch])
        run.wrong_pin()
        self.assertEqual([False, False], [item.passed for item in record.observations])


class ProfileLines(unittest.TestCase):
    """The profile adapter keeps the lines that the profile job can reach, and raises nothing on a count of any length."""

    PREFIX = "signal_profile: "

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def test_a_count_of_4301_digits_is_dropped_and_raises_nothing(self) -> None:
        for line in (f"counts_rows: {HUGE} rows", f"profile_rows_differ: {HUGE} rows", f"table_missing: {HUGE} tables", f"grid_rule_rows: {HUGE} rows"):
            with self.subTest(line=line[:30]):
                self.assertFalse(self.module.known_line(line))
                self.assertEqual([], self.module.codes(f"{self.PREFIX}{line}\n"))

    def test_a_count_is_bounded_by_its_digits_before_it_is_converted(self) -> None:
        digits = self.module.MAX_COUNT_DIGITS
        self.assertTrue(self.module.known_line(f"profile_rows_differ: {'9' * digits} rows"))
        self.assertFalse(self.module.known_line(f"profile_rows_differ: {'9' * (digits + 1)} rows"))
        self.assertFalse(self.module.known_line(f"profile_rows_differ: 0{'9' * digits} rows"))

    def test_a_stored_rule_table_with_one_row_is_never_a_row_count_refusal(self) -> None:
        self.assertFalse(self.module.known_line("grid_rule_rows: 1 row"))
        self.assertEqual([], self.module.codes(f"{self.PREFIX}grid_rule_rows: 1 row\n"))
        for count, noun in ((0, "rows"), (2, "rows"), (3, "rows")):
            line = f"grid_rule_rows: {count} {noun}"
            with self.subTest(line=line):
                self.assertTrue(self.module.known_line(line))

    def test_the_flag_codes_are_the_ones_that_the_profile_job_can_reach(self) -> None:
        reached = reachable_flag_codes()
        self.assertEqual(reached, self.module.FLAG_CODES)
        self.assertNotIn("sample_file", self.module.FLAG_CODES)
        self.assertFalse(self.module.known_line("sample_file: failed"))
        for code in sorted(split_table.CODES - reached):
            with self.subTest(code=code):
                self.assertFalse(self.module.known_line(f"{code}: failed"))
        for code in sorted(reached):
            with self.subTest(code=code):
                self.assertTrue(self.module.known_line(f"{code}: failed"))


def reachable_flag_codes() -> set[str]:
    """The ``<code>: failed`` lines that the profile job can print, read from the producer's own source: the codes of the functions that the job calls on
    the split table (``source_cases``, ``table_problems`` with ``share_problems``, ``source_problems`` and ``schema_problems``), and the two lines of the
    profile plan."""
    tree = ast.parse((ROOT / "spark" / "split_table.py").read_text())
    called = {"source_cases", "table_problems", "share_problems", "source_problems", "schema_problems"}
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in called:
            found |= {
                item.value for item in ast.walk(node) if isinstance(item, ast.Constant) and isinstance(item.value, str) and item.value in split_table.CODES
            }
    return found | {"plan_differs", "readback_differs"}


if __name__ == "__main__":
    unittest.main()
