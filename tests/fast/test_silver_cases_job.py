"""The sequence and the command line of the Silver cases job, on a stand-in for its Spark steps.

The order of the steps is the safety of the job: every input check and the freeze come before the first write, every check of the written tables comes after
it, a rerun that changes nothing writes nothing, and no message or result names a case, a patient or an argument.

Failure modes covered here:

- A write before a failed input check or a changed freeze leaves a table behind that Gold could read.
- A written table that is not checked, or a check that is skipped on a rerun that writes nothing.
- A verify that writes, or that passes when the cutoffs were never saved.
- A message that echoes an argument, a branch name or an upstream error.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from argparse import Namespace
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.silver_cases import CutoffRow, MeasureRow, SilverCasesError, cutoff_rows, measure_rows
from spark.silver_cases_run import (
    CHECKS_BUILD,
    run,
    run_build,
    run_verify,
    start_deadline,
)

LENGTHS = [3600, 7200, 5400, 4800, 9000, 1800]
MARKER = "MARKER_4417"


def clean_input_facts(**changes: object) -> dict[str, object]:
    base: dict[str, object] = {
        "bronze_case_rows": 6388,
        "case_key_null": 0,
        "case_key_duplicates": 0,
        "subjectid_form": 0,
        "unparsed": {"height": 0},
        "domain": {"sex": 0},
        "operation_ends_before_it_starts": 0,
        "cases_without_one_split_row": 0,
        "split_patients_without_case": 0,
        "lab_source_empty": 0,
        "lab_key_null": 0,
        "lab_unparsed": {"result": 0},
        "lab_rows_without_case": 0,
        "sealed_outcome_form_bad": 0,
        "lab_pinned_mismatch": 0,
        "sealed_lab_form_bad": 0,
    }
    return base | changes


def clean_output_facts(**changes: object) -> dict[str, object]:
    base: dict[str, object] = {
        "case_rows": 6388,
        "bronze_case_rows": 6388,
        "case_value_differences": 0,
        "case_key_duplicates": 0,
        "lab_copies": 700000,
        "bronze_lab_rows": 700000,
        "lab_value_differences": 0,
        "lab_key_duplicates": 0,
        "lab_occurrence_gaps": 0,
        "never_feature_marking_wrong": 0,
        "lab_rows": 690000,
        "case_key_differences": 0,
        "sealed_outcome_differs": 0,
        "silver_lab_orphans": 0,
        "sealed_lab_differs": 0,
    }
    return base | changes


class FakeOps:
    """The Spark steps, recording their order. ``held`` is what the branch already holds: the measure and the cutoffs, or ``None`` for a missing table."""

    def __init__(self, **changes: Any) -> None:
        self.calls: list[str] = []
        self.input = clean_input_facts()
        self.output = clean_output_facts()
        self.lengths = list(LENGTHS)
        self.held: tuple[list[MeasureRow] | None, list[CutoffRow] | None] = (None, None)
        self.writes = {"cases": True, "lab": True, "rules": True}
        self.after_write_rules: tuple[list[MeasureRow] | None, list[CutoffRow] | None] | None = None
        self.split: list[str] = []
        self.owner: list[str] = []
        self.schema_wrong = 0
        for name, value in changes.items():
            setattr(self, name, value)

    def split_problems(self) -> list[str]:
        self.calls.append("split_problems")
        return self.split

    def ownership_problems(self) -> list[str]:
        self.calls.append("ownership_problems")
        return self.owner

    def output_schema_tables_wrong(self) -> int:
        self.calls.append("output_schema")
        return self.schema_wrong

    def input_facts(self) -> dict[str, object]:
        self.calls.append("input_facts")
        return self.input

    def development_lengths(self) -> list[int]:
        self.calls.append("development_lengths")
        return self.lengths

    def existing_rules(self) -> tuple[list[MeasureRow] | None, list[CutoffRow] | None]:
        self.calls.append("existing_rules")
        written = "write_rules" in self.calls
        return (
            self.after_write_rules
            if written and self.after_write_rules is not None
            else (self.held if not written else (measure_rows(LENGTHS), cutoff_rows(LENGTHS)))
        )

    def write_cases(self) -> bool:
        self.calls.append("write_cases")
        return self.writes["cases"]

    def write_lab(self) -> bool:
        self.calls.append("write_lab")
        return self.writes["lab"]

    def write_rules(self, measure: list[MeasureRow], cutoffs: list[CutoffRow]) -> bool:
        self.calls.append("write_rules")
        return self.writes["rules"]

    def output_facts(self) -> dict[str, object]:
        self.calls.append("output_facts")
        return self.output

    def snapshot_ids(self) -> dict[str, int | None]:
        self.calls.append("snapshot_ids")
        return {"silver.cases": 11, "silver.lab_results": 12, "silver.operation_length_measure": 13, "silver.score_cutoffs": 14}


def build(ops: FakeOps, *, pinned: bool = True) -> dict[str, Any]:
    return run_build(ops, pinned=pinned).facts()


def refused(call: Any) -> list[str]:
    try:
        call()
    except SilverCasesError as error:
        return error.problems
    raise AssertionError("the sequence accepted what it must refuse")


class BuildOrderTest(unittest.TestCase):
    def test_every_input_check_and_the_freeze_come_before_the_first_write_and_the_output_checks_after_the_last(self) -> None:
        ops = FakeOps()
        build(ops)
        self.assertEqual(
            [
                "split_problems",
                "ownership_problems",
                "input_facts",
                "development_lengths",
                "existing_rules",
                "write_cases",
                "write_lab",
                "write_rules",
                "output_schema",
                "existing_rules",
                "output_facts",
                "snapshot_ids",
            ],
            ops.calls,
        )

    def test_the_result_names_what_was_written_the_counts_the_cutoffs_and_every_check_that_ran(self) -> None:
        facts = build(FakeOps())
        self.assertEqual("written", facts["outcome"])
        self.assertEqual({"cases": True, "lab": True, "rules": True}, facts["written"])
        self.assertEqual(CHECKS_BUILD, facts["checks_run"])
        self.assertEqual(6388, facts["cases"])
        self.assertEqual(690000, facts["development_lab_rows"])
        self.assertEqual(700000, facts["development_lab_copies"])
        self.assertEqual(len(LENGTHS), facts["development_cases"])
        self.assertEqual(
            {"silver.cases": 11, "silver.lab_results": 12, "silver.operation_length_measure": 13, "silver.score_cutoffs": 14}, facts["snapshot_ids"]
        )
        self.assertEqual(
            [(row.cutoff, row.elapsed_seconds) for row in cutoff_rows(LENGTHS)], [(item["cutoff"], item["elapsed_seconds"]) for item in facts["cutoffs"]]
        )

    def test_the_result_is_only_counts_names_of_tables_and_cutoffs_and_holds_no_value_of_a_case(self) -> None:
        facts = json.dumps(build(FakeOps()))
        for word in ("subjectid", "caseid", "training", "validation", "test"):
            self.assertNotIn(word, facts)

    def test_a_rerun_that_changes_nothing_writes_nothing_and_says_unchanged(self) -> None:
        ops = FakeOps(writes={"cases": False, "lab": False, "rules": False}, held=(measure_rows(LENGTHS), cutoff_rows(LENGTHS)))
        facts = build(ops)
        self.assertEqual("unchanged", facts["outcome"])
        self.assertEqual({"cases": False, "lab": False, "rules": False}, facts["written"])
        self.assertIn("output_facts", ops.calls)

    def test_one_written_table_makes_the_outcome_written(self) -> None:
        ops = FakeOps(writes={"cases": False, "lab": True, "rules": False}, held=(measure_rows(LENGTHS), cutoff_rows(LENGTHS)))
        self.assertEqual("written", build(ops)["outcome"])


class InputFailureTest(unittest.TestCase):
    def test_a_failed_input_check_stops_before_any_write_and_names_every_cause_once(self) -> None:
        ops = FakeOps(input=clean_input_facts(case_key_duplicates=2, lab_rows_without_case=1, unparsed={"height": 1}))
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertEqual(["key_duplicate: 2 keys", "lab_case_missing: a laboratory row has no case", "unparsed: 1 value in 1 column (height)"], problems)
        self.assertFalse({"write_cases", "write_lab", "write_rules"} & set(ops.calls))

    def test_a_split_that_breaks_the_rule_stops_before_any_fact_that_depends_on_it_and_reports_only_the_split_codes(self) -> None:
        ops = FakeOps(split=["rule_mismatch: 1 patient"], input=clean_input_facts(case_key_duplicates=2, sealed_outcome_form_bad=1, sealed_lab_form_bad=1))
        self.assertEqual(["rule_mismatch: 1 patient"], refused(lambda: run_build(ops, pinned=True)))
        self.assertEqual(["split_problems"], ops.calls)

    def test_an_ambiguous_bronze_owner_stops_before_any_aggregate_and_reports_only_its_code(self) -> None:
        ops = FakeOps(
            owner=["ambiguous_case_owner: a Bronze case has more than one row or more than one patient"], input=clean_input_facts(case_key_duplicates=2)
        )
        self.assertEqual(["ambiguous_case_owner: a Bronze case has more than one row or more than one patient"], refused(lambda: run_build(ops, pinned=True)))
        self.assertEqual(["split_problems", "ownership_problems"], ops.calls)

    def test_verify_checks_the_owner_too_before_any_aggregate(self) -> None:
        ops = FakeOps(owner=["ambiguous_case_owner: a Bronze case has more than one row or more than one patient"])
        refused(lambda: run_verify(ops, pinned=True))
        self.assertEqual(["split_problems", "ownership_problems"], ops.calls)

    def test_verify_checks_the_split_first_too(self) -> None:
        ops = FakeOps(split=["rule_mismatch: 1 patient"])
        self.assertEqual(["rule_mismatch: 1 patient"], refused(lambda: run_verify(ops, pinned=True)))
        self.assertEqual(["split_problems"], ops.calls)

    def test_a_held_out_outcome_value_not_in_its_published_form_stops_before_any_write_and_names_no_count(self) -> None:
        ops = FakeOps(input=clean_input_facts(sealed_outcome_form_bad=1))
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertEqual(["sealed_outcome_form: a held-out outcome value is not in its published form"], problems)
        self.assertNotIn("write_cases", ops.calls)

    def test_a_held_out_laboratory_value_not_in_its_published_form_stops_before_any_write_and_names_no_count(self) -> None:
        ops = FakeOps(input=clean_input_facts(sealed_lab_form_bad=1))
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertEqual(["sealed_lab_form: a held-out laboratory value is not in its published form"], problems)
        self.assertNotIn("write_cases", ops.calls)

    def test_no_development_case_stops_before_any_write(self) -> None:
        ops = FakeOps(lengths=[])
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertEqual(["no_development_cases: 0 cases"], problems)
        self.assertNotIn("write_cases", ops.calls)

    def test_a_median_too_short_for_the_cutoffs_stops_before_any_write(self) -> None:
        ops = FakeOps(lengths=[200, 200, 200])
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertTrue(problems[0].startswith("cutoffs_not_increasing"))
        self.assertNotIn("write_cases", ops.calls)

    def test_the_pinned_release_demands_its_counts_before_a_write_and_a_sample_does_not(self) -> None:
        ops = FakeOps(input=clean_input_facts(bronze_case_rows=13, lab_pinned_mismatch=1))
        self.assertTrue(refused(lambda: run_build(ops, pinned=True))[0].startswith("pinned_counts: "))
        self.assertNotIn("write_cases", ops.calls)
        sample = FakeOps(
            input=clean_input_facts(bronze_case_rows=13),
            output=clean_output_facts(case_rows=13, bronze_case_rows=13, lab_rows=90, lab_copies=100, bronze_lab_rows=100),
        )
        self.assertEqual("written", build(sample, pinned=False)["outcome"])


class FreezeTest(unittest.TestCase):
    def test_cutoffs_that_would_change_stop_before_any_write(self) -> None:
        held = (measure_rows([3000, 3000, 3000, 3000]), cutoff_rows([3000, 3000, 3000, 3000]))
        ops = FakeOps(held=held)
        problems = refused(lambda: run_build(ops, pinned=True))
        self.assertTrue(any(problem.startswith("changed_cutoff: ") for problem in problems))
        self.assertTrue(any(problem.startswith("changed_measure: ") for problem in problems))
        self.assertNotIn("write_cases", ops.calls)
        self.assertNotIn("write_rules", ops.calls)

    def test_a_held_cutoff_table_with_a_repeated_key_stops_before_any_write(self) -> None:
        rows = cutoff_rows(LENGTHS)
        ops = FakeOps(held=(measure_rows(LENGTHS), [*rows, rows[1]]))
        self.assertEqual(["inherited_duplicates: 1 key"], refused(lambda: run_build(ops, pinned=True)))
        self.assertNotIn("write_cases", ops.calls)

    def test_held_rows_in_another_order_are_the_same_rows_and_the_rebuild_goes_on(self) -> None:
        ops = FakeOps(writes={"cases": False, "lab": False, "rules": False}, held=(measure_rows(LENGTHS)[::-1], cutoff_rows(LENGTHS)[::-1]))
        self.assertEqual("unchanged", build(ops)["outcome"])

    def test_a_held_table_that_is_empty_or_half_there_stops_before_any_write(self) -> None:
        for held, code in (((measure_rows(LENGTHS), None), "inherited_incomplete"), (([], []), "inherited_empty")):
            ops = FakeOps(held=held)
            self.assertTrue(refused(lambda ops=ops: run_build(ops, pinned=True))[0].startswith(code))
            self.assertNotIn("write_cases", ops.calls)


class OutputFailureTest(unittest.TestCase):
    def test_a_held_out_laboratory_difference_ends_the_job_with_one_code_and_no_count(self) -> None:
        ops = FakeOps(output=clean_output_facts(sealed_lab_differs=1))
        self.assertEqual(["sealed_lab_differs: a held-out laboratory row differs from Bronze or breaks its key"], refused(lambda: run_build(ops, pinned=True)))

    def test_the_result_names_development_laboratory_counts_only(self) -> None:
        facts = build(FakeOps())
        self.assertNotIn("lab_rows", facts)
        self.assertNotIn("lab_copies", facts)

    def test_a_replaced_case_key_or_a_held_out_outcome_difference_ends_the_job(self) -> None:
        ops = FakeOps(output=clean_output_facts(case_key_differences=2, sealed_outcome_differs=1))
        self.assertEqual(
            ["case_key_differences: 2 keys", "sealed_outcome_differs: a held-out outcome value differs from Bronze"],
            refused(lambda: run_build(ops, pinned=True)),
        )

    def test_a_dropped_or_retyped_column_stops_the_job_before_any_value_query_and_reports_only_the_schema(self) -> None:
        ops = FakeOps(schema_wrong=2, output=clean_output_facts(case_key_differences=2, sealed_outcome_differs=1))
        self.assertEqual(["output_schema: 2 tables"], refused(lambda: run_build(ops, pinned=True)))
        self.assertNotIn("output_facts", ops.calls)
        self.assertNotIn("existing_rules", ops.calls[ops.calls.index("write_rules") :])

    def test_verify_checks_the_stored_schema_before_it_reads_the_saved_rules_or_any_value(self) -> None:
        ops = FakeOps(schema_wrong=1, held=(measure_rows(LENGTHS), cutoff_rows(LENGTHS)))
        self.assertEqual(["output_schema: 1 table"], refused(lambda: run_verify(ops, pinned=True)))
        self.assertNotIn("output_facts", ops.calls)
        self.assertNotIn("existing_rules", ops.calls)

    def test_a_check_of_a_written_table_that_fails_ends_the_job_with_its_code(self) -> None:
        ops = FakeOps(output=clean_output_facts(lab_value_differences=3, case_key_duplicates=1))
        self.assertEqual(["case_key_duplicate: 1 key", "lab_value_differences: 3 rows"], refused(lambda: run_build(ops, pinned=True)))
        self.assertLess(ops.calls.index("write_rules"), ops.calls.index("output_facts"))

    def test_a_table_not_marked_with_its_never_feature_columns_fails(self) -> None:
        ops = FakeOps(output=clean_output_facts(never_feature_marking_wrong=1))
        self.assertEqual(["never_feature_marking: 1 table"], refused(lambda: run_build(ops, pinned=True)))

    def test_cutoffs_read_back_that_differ_from_the_ones_written_fail(self) -> None:
        other = (measure_rows([100, 200, 300]), cutoff_rows([6000, 6000, 6000]))
        ops = FakeOps(after_write_rules=other)
        self.assertTrue(any(problem.startswith("readback_differs") for problem in refused(lambda: run_build(ops, pinned=True))))

    def test_cutoffs_read_back_in_another_order_are_the_same_cutoffs(self) -> None:
        reordered = (measure_rows(LENGTHS)[::-1], cutoff_rows(LENGTHS)[::-1])
        self.assertEqual("written", build(FakeOps(after_write_rules=reordered))["outcome"])

    def test_cutoffs_read_back_with_a_repeated_row_fail_even_when_every_row_is_one_that_was_written(self) -> None:
        rows = cutoff_rows(LENGTHS)
        ops = FakeOps(after_write_rules=(measure_rows(LENGTHS), [*rows, rows[0]]))
        self.assertTrue(any(problem.startswith("readback_differs") for problem in refused(lambda: run_build(ops, pinned=True))))

    def test_cutoffs_missing_after_the_write_fail(self) -> None:
        ops = FakeOps(after_write_rules=(None, None))
        self.assertTrue(refused(lambda: run_build(ops, pinned=True))[0].startswith("readback_differs"))


class VerifyTest(unittest.TestCase):
    def test_verify_writes_nothing_and_runs_the_input_and_output_checks_and_the_rules_comparison(self) -> None:
        ops = FakeOps(held=(measure_rows(LENGTHS), cutoff_rows(LENGTHS)))
        facts = run_verify(ops, pinned=True).facts()
        self.assertEqual("verified", facts["outcome"])
        self.assertFalse({"write_cases", "write_lab", "write_rules"} & set(ops.calls))
        self.assertEqual(
            ["split_problems", "ownership_problems", "input_facts", "development_lengths", "output_schema", "existing_rules", "output_facts", "snapshot_ids"],
            ops.calls,
        )

    def test_verify_fails_when_the_cutoffs_were_never_saved(self) -> None:
        ops = FakeOps(held=(None, None))
        self.assertEqual(["table_missing: 2 tables"], refused(lambda: run_verify(ops, pinned=True)))

    def test_verify_fails_when_the_saved_cutoffs_are_not_the_ones_the_development_data_gives(self) -> None:
        ops = FakeOps(held=(measure_rows([9000, 9000]), cutoff_rows([9000, 9000])))
        self.assertTrue(any(problem.startswith("changed_cutoff") for problem in refused(lambda: run_verify(ops, pinned=True))))

    def test_verify_fails_on_a_changed_value_in_a_written_table(self) -> None:
        ops = FakeOps(held=(measure_rows(LENGTHS), cutoff_rows(LENGTHS)), output=clean_output_facts(case_value_differences=1))
        self.assertEqual(["case_value_differences: 1 row"], refused(lambda: run_verify(ops, pinned=True)))


def invoke(argv: list[str], execute: Any = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(argv, execute or (lambda arguments: {"mode": arguments.mode}))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def recording(seen: list[Namespace]) -> Any:
    def execute(arguments: Namespace) -> dict[str, Any]:
        seen.append(arguments)
        return {}

    return execute


def raising(error: Exception) -> Any:
    def execute(_arguments: Namespace) -> dict[str, Any]:
        raise error

    return execute


class CommandLineTest(unittest.TestCase):
    def test_a_good_call_prints_one_result_line_and_exits_0(self) -> None:
        code, out, err = invoke(["build", "--branch", "b", "--pinned-release"])
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({"mode": "build"}, json.loads(out.removeprefix("RESULT ")))

    def test_the_arguments_are_a_mode_a_branch_a_base_for_build_and_the_pinned_flag(self) -> None:
        seen: list[Namespace] = []
        invoke(["build", "--branch", "b", "--base", "p6_trunk", "--pinned-release"], recording(seen))
        invoke(["verify", "--branch", "b"], recording(seen))
        self.assertEqual(("b", "p6_trunk", True), (seen[0].branch, seen[0].base, seen[0].pinned_release))
        self.assertEqual(("b", False), (seen[1].branch, seen[1].pinned_release))
        self.assertEqual(2, invoke(["verify", "--branch", "b", "--base", "main"])[0])

    def test_a_bad_call_is_the_code_usage_and_echoes_nothing(self) -> None:
        for argv in (["build"], ["build", "--branch", f"bad name {MARKER}"], ["build", "--branch", "b", f"--{MARKER}"], [MARKER]):
            code, out, err = invoke(argv)
            self.assertEqual((2, "", "silver_cases: usage\n"), (code, out, err), argv)

    def test_a_failed_check_prints_each_code_and_a_count_and_exits_1(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SilverCasesError("key_duplicate: 2 keys", "unparsed: 1 value in 1 column (height)")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("silver_cases: key_duplicate: 2 keys\nsilver_cases: unparsed: 1 value in 1 column (height)\n", err)

    def test_nessie_is_a_fixed_code_whatever_its_message_holds(self) -> None:
        code, _, err = invoke(["build", "--branch", "b"], raising(NessieError(f"cannot work with {MARKER}", status=409)))
        self.assertEqual((1, "silver_cases: catalog_error\n"), (code, err))
        code, _, err = invoke(["verify", "--branch", "b"], raising(NessieError(f"missing {MARKER}", status=404, error_code="REFERENCE_NOT_FOUND")))
        self.assertEqual((1, "silver_cases: branch_missing\n"), (code, err))

    def test_a_setting_that_cannot_be_used_is_settings_and_exits_2_and_anything_else_is_internal_error_and_exits_70(self) -> None:
        code, _, err = invoke(["build", "--branch", "b"], raising(SettingsError(f"{MARKER} is not set")))
        self.assertEqual((2, "silver_cases: settings\n"), (code, err))
        code, _, err = invoke(["build", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, "silver_cases: internal_error\n"), (code, err))


class DeadlineTest(unittest.TestCase):
    def test_the_timer_prints_a_fixed_code_and_exits_124_without_the_duration(self) -> None:
        stopped: list[int] = []
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            timer = start_deadline(0.01, stopped.append)
            timer.join(2)
        self.assertEqual([124], stopped)
        self.assertEqual("silver_cases: timeout\n", err.getvalue())

    def test_a_job_that_ends_first_cancels_its_timer(self) -> None:
        stopped: list[int] = []
        timer = start_deadline(60, stopped.append)
        timer.cancel()
        self.assertEqual([], stopped)


if __name__ == "__main__":
    unittest.main()
