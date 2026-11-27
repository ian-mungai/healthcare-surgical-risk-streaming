"""The sequence and the command line of the Gold dimensions job, on a stand-in for its Spark steps.

The order of the steps is the safety of the job: every input check and the check of the held members come before the first write, every check of the written
tables comes after the last, a rerun that changes nothing writes nothing, and no message or result names a case, a patient, a value or an argument.

Failure modes covered here:

- A write before a failed input check or a held member that the rebuild would remove or change, so a dimension that facts already point at loses a key.
- A written table that is not checked, or a check that is skipped on a rerun that writes nothing.
- A verify that writes, or that passes when a table is missing.
- A message that echoes an argument, a branch name or an upstream error, or a result that holds a key or a natural key.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from argparse import Namespace
from typing import Any

from scripts.stack.nessie import NessieError
from spark.gold_dimensions import DIMENSIONS, Dimension, GoldDimensionsError
from spark.gold_dimensions_run import CHECKS, JOB, run, run_build, run_verify
from spark.jobs import gold_dimensions
from spark.settings import SettingsError
from tests.fast.internal_line_support import ENTRY_FRAME, assert_internal, error_with_raising_loader

NAMES = tuple(item.name for item in DIMENSIONS)
OPEN_NAMES = tuple(item.name for item in DIMENSIONS if item.name != "dim_lab_test")
MARKER = "MARKER_4417"
INPUT = {"source_members": 12, "ambiguous_members": 0, "key_null_rows": 0}
OUTPUT = {
    "members": 12,
    "unknown_rows": 1,
    "unknown_not_empty": 0,
    "key_duplicates": 0,
    "key_out_of_range": 0,
    "natural_null": 0,
    "natural_duplicates": 0,
    "key_rule_wrong": 0,
    "source_without_member": 0,
    "member_without_source": 0,
    "member_differs": 0,
    "interval_wrong": 0,
    "chain_broken": 0,
}


class Ops:
    """The steps of the job on a log. ``missing`` is the count of dimension tables that the branch does not hold."""

    def __init__(self, *, writes: bool = True, missing: int = 0, held: dict[str, tuple[int, int]] | None = None) -> None:
        self.log: list[str] = []
        self.writes = writes
        self.missing = missing
        self.input = {name: dict(INPUT) for name in NAMES}
        self.output = {name: dict(OUTPUT) for name in NAMES}
        self.held = held or {name: (0, 0) for name in NAMES}
        self.wrong_schema = 0
        self.written_by_name: dict[str, bool] = {}
        self.snapshots: dict[str, int] = {}

    def source_facts(self) -> dict[str, dict[str, int]]:
        self.log.append("source_facts")
        return self.input

    def held_changes(self) -> dict[str, tuple[int, int]]:
        self.log.append("held_changes")
        return self.held

    def write(self, item: Dimension) -> bool:
        self.log.append(f"write {item.name}")
        return self.written_by_name.get(item.name, self.writes)

    def tables_missing(self) -> int:
        self.log.append("tables_missing")
        return self.missing

    def output_schema_tables_wrong(self) -> int:
        self.log.append("output_schema")
        return self.wrong_schema

    def table_facts(self) -> dict[str, dict[str, int]]:
        self.log.append("table_facts")
        return self.output

    def snapshot_ids(self) -> dict[str, int | None]:
        self.log.append("snapshot_ids")
        return {item.table: self.snapshots.get(item.table, 1000 + number) for number, item in enumerate(DIMENSIONS)}


class BuildTests(unittest.TestCase):
    def test_a_clean_build_checks_the_inputs_and_the_held_members_then_writes_each_dimension_then_checks_what_it_wrote(self) -> None:
        ops = Ops()
        outcome = run_build(ops)
        self.assertEqual(["source_facts", "held_changes", *(f"write {name}" for name in NAMES), "output_schema", "table_facts", "snapshot_ids"], ops.log)
        self.assertEqual("written", outcome.outcome)
        self.assertEqual({name: True for name in NAMES}, outcome.written)
        self.assertEqual({name: True for name in OPEN_NAMES}, outcome.facts()["written"])

    def test_a_rerun_that_changes_nothing_writes_nothing_and_is_still_checked(self) -> None:
        ops = Ops(writes=False)
        outcome = run_build(ops)
        self.assertEqual("unchanged", outcome.outcome)
        self.assertIn("table_facts", ops.log)

    def test_a_failed_input_check_stops_every_write(self) -> None:
        ops = Ops()
        ops.input["dim_track"] = INPUT | {"ambiguous_members": 2}
        with self.assertRaises(GoldDimensionsError) as caught:
            run_build(ops)
        self.assertEqual(["ambiguous_members: dim_track, 2 members"], caught.exception.problems)
        self.assertFalse([line for line in ops.log if line.startswith("write")])

    def test_a_member_that_the_rebuild_would_remove_or_change_stops_every_write(self) -> None:
        ops = Ops(held={**{name: (0, 0) for name in NAMES}, "dim_case": (1, 2)})
        with self.assertRaises(GoldDimensionsError) as caught:
            run_build(ops)
        self.assertEqual(["member_removed: dim_case, 1 member", "version_date_not_after: dim_case, 2 members"], caught.exception.problems)
        self.assertFalse([line for line in ops.log if line.startswith("write")])

    def test_every_cause_of_a_failed_input_check_is_named_together(self) -> None:
        ops = Ops()
        ops.input["dim_case"] = INPUT | {"key_null_rows": 1}
        ops.input["dim_lab_test"] = INPUT | {"source_members": 0}
        with self.assertRaises(GoldDimensionsError) as caught:
            run_build(ops)
        self.assertEqual(["key_null: dim_case, 1 row", "empty_source: failed"], caught.exception.problems)

    def test_a_written_table_with_another_schema_is_refused_before_any_value_is_read(self) -> None:
        ops = Ops()
        ops.wrong_schema = 2
        with self.assertRaises(GoldDimensionsError) as caught:
            run_build(ops)
        self.assertEqual(["output_schema: 2 tables"], caught.exception.problems)
        self.assertNotIn("table_facts", ops.log)

    def test_a_written_table_that_fails_a_check_is_refused_after_the_write(self) -> None:
        ops = Ops()
        ops.output["dim_department"] = OUTPUT | {"key_rule_wrong": 3}
        with self.assertRaises(GoldDimensionsError) as caught:
            run_build(ops)
        self.assertEqual(["key_rule: dim_department, 3 members"], caught.exception.problems)

    def test_the_result_holds_counts_and_snapshot_ids_and_no_key_or_natural_key(self) -> None:
        facts = run_build(Ops()).facts()
        self.assertEqual({"outcome", "written", "checks_run", "members", "passed", "snapshot_ids"}, set(facts))
        self.assertEqual({name: 12 for name in OPEN_NAMES}, facts["members"])
        self.assertEqual({name: True for name in OPEN_NAMES}, facts["written"])
        self.assertEqual({name: True for name in NAMES}, facts["passed"])
        self.assertEqual(CHECKS, facts["checks_run"])
        self.assertEqual(len(DIMENSIONS), len(facts["snapshot_ids"]))
        json.dumps(facts)


class SealedLabTests(unittest.TestCase):
    """The laboratory table holds the results of held-out patients, so a held-out laboratory change can change what the lab dimension holds: its members, its
    write and every count that a check makes over it. None of these leaves the job: the result holds one pass flag for the dimension, and a lab failure is
    ``<code>: failed``.

    Failure modes covered here:

    - A member count or a write indicator of the lab dimension in the result, or in the overall outcome.
    - A count in a failure message that comes from the lab dimension.
    - A lab change that changes the printed result apart from the snapshot IDs.
    """

    def lab_changed(self) -> Ops:
        """The run in which a held-out case gained a laboratory test: the lab dimension has one more member and is written, the other tables are unchanged."""
        ops = Ops(writes=False)
        ops.output["dim_lab_test"] = OUTPUT | {"members": OUTPUT["members"] + 1}
        ops.written_by_name = {"dim_lab_test": True}
        return ops

    def printed(self, ops: Ops, mode: str = "build") -> dict[str, Any]:
        outcome = run_build(ops) if mode == "build" else run_verify(ops)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = run(["verify", "--branch", "b"], lambda arguments: {"mode": mode, **outcome.facts()})
        self.assertEqual(0, code)
        document: dict[str, Any] = json.loads(out.getvalue().removeprefix("RESULT "))
        return document

    def test_a_held_out_lab_change_leaves_the_printed_result_identical_apart_from_the_snapshot_ids(self) -> None:
        before, after = Ops(writes=False), self.lab_changed()
        after.snapshots = {"gold.dim_lab_test": 9999}
        one, two = self.printed(before), self.printed(after)
        self.assertNotEqual(one["snapshot_ids"], two["snapshot_ids"])
        one.pop("snapshot_ids")
        two.pop("snapshot_ids")
        self.assertEqual(one, two)
        self.assertEqual("unchanged", two["outcome"])

    def test_a_held_out_lab_change_leaves_a_verify_result_identical_too(self) -> None:
        before, after = Ops(), self.lab_changed()
        self.assertEqual(self.printed(before, "verify"), self.printed(after, "verify"))

    def test_the_lab_dimension_has_no_member_count_and_no_write_indicator_in_the_result(self) -> None:
        facts = run_build(Ops()).facts()
        self.assertNotIn("dim_lab_test", facts["members"])
        self.assertNotIn("dim_lab_test", facts["written"])
        self.assertTrue(facts["passed"]["dim_lab_test"])

    def test_the_lab_write_has_no_effect_on_the_overall_outcome(self) -> None:
        self.assertEqual("unchanged", run_build(self.lab_changed()).facts()["outcome"])
        self.assertEqual("written", run_build(Ops()).facts()["outcome"])

    def test_every_lab_failure_is_the_code_and_failed_whatever_the_count(self) -> None:
        for count in (1, 7):
            with self.subTest(count=count):
                ops = Ops()
                ops.input["dim_lab_test"] = INPUT | {"source_members": 0, "ambiguous_members": count, "key_null_rows": count}
                with self.assertRaises(GoldDimensionsError) as caught:
                    run_build(ops)
                self.assertEqual(["empty_source: failed", "ambiguous_members: failed"], caught.exception.problems)
                ops = Ops(held={**{name: (0, 0) for name in NAMES}, "dim_lab_test": (count, count)})
                with self.assertRaises(GoldDimensionsError) as caught:
                    run_build(ops)
                self.assertEqual(["member_removed: failed", "version_date_not_after: failed"], caught.exception.problems)
                ops = Ops()
                ops.output["dim_lab_test"] = OUTPUT | {"key_duplicates": count, "member_without_source": count}
                with self.assertRaises(GoldDimensionsError) as caught:
                    run_build(ops)
                self.assertEqual(["key_duplicate: failed", "member_without_source: failed"], caught.exception.problems)


class VerifyTests(unittest.TestCase):
    def test_a_verify_checks_and_writes_nothing(self) -> None:
        ops = Ops()
        outcome = run_verify(ops)
        self.assertEqual("verified", outcome.outcome)
        self.assertEqual({name: False for name in NAMES}, outcome.written)
        self.assertEqual({name: False for name in OPEN_NAMES}, outcome.facts()["written"])
        self.assertFalse([line for line in ops.log if line.startswith("write")])
        self.assertEqual(["source_facts", "tables_missing", "output_schema", "table_facts", "snapshot_ids"], ops.log)

    def test_a_verify_refuses_a_branch_that_misses_a_table_before_it_reads_one(self) -> None:
        ops = Ops(missing=3)
        with self.assertRaises(GoldDimensionsError) as caught:
            run_verify(ops)
        self.assertEqual(["table_missing: 3 tables"], caught.exception.problems)
        self.assertNotIn("table_facts", ops.log)

    def test_a_verify_refuses_a_table_that_fails_a_check(self) -> None:
        ops = Ops()
        ops.output["dim_track"] = OUTPUT | {"unknown_rows": 0}
        with self.assertRaises(GoldDimensionsError) as caught:
            run_verify(ops)
        self.assertEqual(["unknown_member: dim_track, 0 rows"], caught.exception.problems)

    def test_a_verify_runs_the_input_checks_too(self) -> None:
        ops = Ops()
        ops.input["dim_patient"] = INPUT | {"key_null_rows": 4}
        with self.assertRaises(GoldDimensionsError) as caught:
            run_verify(ops)
        self.assertEqual(["key_null: dim_patient, 4 rows"], caught.exception.problems)


class UsageTests(unittest.TestCase):
    def test_the_usage_in_the_text_of_the_job_is_the_command_line_that_the_parser_accepts(self) -> None:
        lines = [line for line in (gold_dimensions.__doc__ or "").splitlines() if line.startswith("``python -m spark.jobs.gold_dimensions")]
        self.assertEqual(
            [
                "``python -m spark.jobs.gold_dimensions build --branch <name> [--base <name>] --valid-from <YYYY-MM-DD>``",
                "``python -m spark.jobs.gold_dimensions verify --branch <name>``",
            ],
            lines,
        )

    def test_the_text_of_the_job_describes_versions_and_not_one_row_for_each_member(self) -> None:
        text = " ".join((gold_dimensions.__doc__ or "").split())
        self.assertNotIn("one row for each member", text)
        for word in ("version", "valid_from", "is_current"):
            self.assertIn(word, text)


class CommandTests(unittest.TestCase):
    def call(self, argv: list[str], execute: Any) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = run(argv, execute)
            except SystemExit as stop:
                code = int(stop.code or 0)
        return code, out.getvalue(), err.getvalue()

    def test_a_result_is_one_line_that_starts_with_result(self) -> None:
        code, out, err = self.call(["verify", "--branch", "gold_a"], lambda arguments: {"mode": arguments.mode, "branch": arguments.branch})
        self.assertEqual((0, ""), (code, err))
        self.assertEqual('RESULT {"branch": "gold_a", "mode": "verify"}\n', out)

    def test_the_build_takes_a_base_that_defaults_to_main_and_the_verify_takes_none(self) -> None:
        seen: list[Namespace] = []

        def execute(arguments: Namespace) -> dict[str, Any]:
            seen.append(arguments)
            return {}

        self.call(["build", "--branch", "gold_a", "--valid-from", "2026-11-23"], execute)
        self.assertEqual(("main", "2026-11-23"), (seen[0].base, seen[0].valid_from))
        code, _, err = self.call(["verify", "--branch", "gold_a", "--base", "main"], lambda arguments: {})
        self.assertEqual((2, f"{JOB}: usage\n"), (code, err))

    def test_a_build_needs_a_real_version_date(self) -> None:
        for argv in (
            ["build", "--branch", "gold_a"],
            ["build", "--branch", "gold_a", "--valid-from", "yesterday"],
            ["build", "--branch", "g", "--valid-from", "2026-02-30"],
        ):
            with self.subTest(argv=argv):
                code, out, err = self.call(argv, lambda arguments: {})
                self.assertEqual((2, "", f"{JOB}: usage\n"), (code, out, err))

    def test_a_bad_call_prints_the_code_usage_and_never_the_argument(self) -> None:
        for argv in ([], ["build"], ["build", "--branch", f"bad name {MARKER}"], ["other", "--branch", "x"]):
            with self.subTest(argv=argv):
                code, out, err = self.call(argv, lambda arguments: {})
                self.assertEqual((2, "", f"{JOB}: usage\n"), (code, out, err))

    def test_each_failed_check_is_printed_as_its_code_and_exits_one(self) -> None:
        def execute(arguments: Namespace) -> dict[str, Any]:
            raise GoldDimensionsError("key_rule: dim_case, 1 member", "unknown_member: dim_track, 0 rows")

        code, out, err = self.call(["verify", "--branch", MARKER], execute)
        self.assertEqual((1, ""), (code, out))
        self.assertEqual(f"{JOB}: key_rule: dim_case, 1 member\n{JOB}: unknown_member: dim_track, 0 rows\n", err)

    def test_an_upstream_failure_prints_a_fixed_code_and_never_its_text(self) -> None:
        cases = {
            "branch_missing": NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND"),
            "catalog_error": NessieError(f"{MARKER} unavailable", status=503),
        }
        for code_name, error in cases.items():
            with self.subTest(code=code_name):

                def execute(arguments: Namespace, error: Exception = error) -> dict[str, Any]:
                    raise error

                code, out, err = self.call(["verify", "--branch", "x"], execute)
                self.assertEqual((1, "", f"{JOB}: {code_name}\n"), (code, out, err))

    def test_a_setting_error_is_two_and_any_other_error_is_seventy_and_neither_prints_its_text(self) -> None:
        for error, wanted in ((SettingsError(MARKER), (2, "settings")), (RuntimeError(MARKER), (70, "internal_error"))):
            with self.subTest(error=type(error).__name__):

                def execute(arguments: Namespace, error: Exception = error) -> dict[str, Any]:
                    raise error

                code, out, err = self.call(["verify", "--branch", "x"], execute)
                if wanted[1] == "internal_error":
                    self.assertEqual((wanted[0], ""), (code, out))
                    assert_internal(self, err, JOB, f"{JOB}: internal_error", ("builtins.RuntimeError",), MARKER)
                else:
                    self.assertEqual((wanted[0], "", f"{JOB}: {wanted[1]}\n"), (code, out, err))

    def test_an_error_whose_source_loader_raises_is_still_exit_70_with_both_lines(self) -> None:
        error = error_with_raising_loader(self)

        def execute(arguments: Namespace) -> dict[str, Any]:
            raise error

        code, out, err = self.call(["verify", "--branch", "x"], execute)
        self.assertEqual((70, ""), (code, out))
        self.assertEqual(ENTRY_FRAME, assert_internal(self, err, JOB, f"{JOB}: internal_error", ("builtins.KeyError",), "the loader failed")[-1])


if __name__ == "__main__":
    unittest.main()
