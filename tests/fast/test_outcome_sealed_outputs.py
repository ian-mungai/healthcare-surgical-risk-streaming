"""No count, value, row or digest that a held-out outcome can change leaves the jobs that read the clinical table (the owner's decision D). No Spark runs here.

The clinical table holds the outcome columns (``icu_days`` and ``death_inhosp``), and every table built from it (the cases, the patient split) is a table
whose rows can follow an outcome. A result or a refusal that holds a count, a digest or a changed-or-unchanged field over such a table changes when only a
held-out patient's outcome changes. The results hold hashes, snapshot IDs, the checks that ran, one pass flag for each table and measures of the development
patients. A refusal is ``code: failed``, and a count of tables is the only count left. Bronze signal tables keep their row counts: they hold no outcome.

Failure modes covered here:

- A result of the Silver cases job holds ``outcome`` or ``written`` (a held-out row changes whether a table was written) or the count of all cases.
- A result of the patient split holds ``outcome``, the count of patients or cases or the count of each set.
- A result of the Bronze clinical table holds its row counts, its digests or a check detail that holds a count.
- The table maintenance result holds the row count or the checksum of a tagged table that can hold an outcome, or a refusal that names them.
- A refusal holds a count of rows, cases, patients, keys, values or columns after its code.
- A changed held-out row changes the result in more than the hashes and the snapshot IDs.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.stack.tags import TableSnapshot
from spark import maintenance, silver_cases, split_table
from spark.bronze_clinical_lab_run import finish
from spark.jobs import table_maintenance as maintenance_job
from spark.jobs.patient_split import Lake as SplitLake
from spark.silver_cases import OWNERSHIP_PROBLEM, SilverCasesError, check_lengths, cutoff_rows, frozen_problems, input_problems, measure_rows, output_problems
from spark.silver_cases_run import RULES_READ_BACK, run_build, run_verify
from spark.split_table import SplitRow, build_assignments
from tests.fast import test_bronze_job_sequence as bronze
from tests.fast import test_patient_split as split
from tests.fast import test_silver_cases_job as cases
from tests.fast import test_table_maintenance as maintenance_tests
from tests.fast.test_silver_sealed_outputs import keys_of, numbers

ROOT = Path(__file__).resolve().parents[2]
BRANCH_MOVED_TEXT = re.escape(split_table.BRANCH_MOVED)
SEALED = re.compile(rf"^[a-z_]+: failed$|^[a-z_]+: [0-9]+ tables?$|^{BRANCH_MOVED_TEXT}$")
BANNED_KEYS = {"outcome", "written", "rows", "source_rows", "keys_digest", "rows_digest", "patients", "counts", "checksum", "digest", "count"}


def unsealed(messages: list[str]) -> list[str]:
    return [message for message in messages if not SEALED.match(message)]


class CasesResult(unittest.TestCase):
    """The Silver cases result: hashes, snapshot IDs, the checks that ran, one pass flag for each table and measures of the development patients."""

    ALLOWED_NUMBERS = {"snapshot_ids", "development_cases", "development_lab_rows", "development_lab_copies", "measure", "cutoffs"}

    def facts(self, ops: cases.FakeOps | None = None, *, pinned: bool = True) -> dict[str, Any]:
        return run_build(ops or cases.FakeOps(), pinned=pinned).facts()

    def test_the_result_holds_no_outcome_no_written_flag_and_no_count_of_all_cases(self) -> None:
        for facts in (self.facts(), run_verify(cases.FakeOps(held=(measure_rows(cases.LENGTHS), cutoff_rows(cases.LENGTHS))), pinned=True).facts()):
            self.assertEqual(set(), keys_of(facts) & {"outcome", "written", "rows"}, facts)
            self.assertNotIn("cases", facts)

    def test_the_only_numbers_are_snapshot_ids_and_measures_of_development_patients(self) -> None:
        facts = self.facts()
        self.assertEqual([], [path for path in numbers(facts) if path[0] not in self.ALLOWED_NUMBERS], facts)

    def test_each_table_has_one_pass_flag(self) -> None:
        self.assertEqual({"cases": True, "lab": True, "rules": True}, self.facts()["passed"])
        self.assertEqual(
            {"cases": True, "lab": True, "rules": True},
            run_verify(cases.FakeOps(held=(measure_rows(cases.LENGTHS), cutoff_rows(cases.LENGTHS))), pinned=True).facts()["passed"],
        )

    def test_a_changed_held_out_row_changes_only_the_snapshot_ids(self) -> None:
        class Other(cases.FakeOps):
            def snapshot_ids(self) -> dict[str, int | None]:
                return {"silver.cases": 21, "silver.lab_results": 22, "silver.operation_length_measure": 23, "silver.score_cutoffs": 24}

        other = Other(
            output=cases.clean_output_facts(case_rows=7000, bronze_case_rows=7000),
            input=cases.clean_input_facts(bronze_case_rows=7000),
            writes={"cases": False, "lab": True, "rules": False},
        )
        first, second = self.facts(pinned=False), self.facts(other, pinned=False)
        self.assertNotEqual(first["snapshot_ids"], second["snapshot_ids"])
        self.assertEqual(
            {key: value for key, value in first.items() if key != "snapshot_ids"}, {key: value for key, value in second.items() if key != "snapshot_ids"}
        )


def cases_messages() -> list[str]:
    """Every refusal of the Silver cases job, driven with a bad value for every fact and every step."""
    messages: list[str] = []
    clean_in, clean_out = cases.clean_input_facts(), cases.clean_output_facts()
    for name, value in clean_in.items():
        if name == "bronze_case_rows":
            continue
        bad: object = {"height": 3, "sex": 4} if isinstance(value, dict) else 5
        messages += input_problems(**{**clean_in, name: bad}, development_cases=3, pinned=True)  # type: ignore[arg-type]
    messages += input_problems(**{**clean_in, "bronze_case_rows": 0}, development_cases=0, pinned=True)  # type: ignore[arg-type]
    messages += input_problems(**{**clean_in, "bronze_case_rows": 6000}, development_cases=3, pinned=True)  # type: ignore[arg-type]
    for name in clean_out:
        messages += output_problems(**{**clean_out, name: 9_999_999 if name.endswith("rows") else 5})  # type: ignore[arg-type]
    messages += output_problems(**{**clean_out, "lab_rows": 700_001})  # type: ignore[arg-type]
    measure, cutoffs = measure_rows(cases.LENGTHS), cutoff_rows(cases.LENGTHS)
    changed_measure = [dataclasses.replace(row, value=row.value + 1) for row in measure]
    changed_cutoffs = [dataclasses.replace(row, development_cases=row.development_cases + 1) for row in cutoffs]
    messages += frozen_problems(changed_measure, changed_cutoffs, measure, cutoffs)
    messages += (
        frozen_problems(measure, None, measure, cutoffs)
        + frozen_problems([], [], measure, cutoffs)
        + frozen_problems([*measure, *measure], cutoffs, measure, cutoffs)
    )
    for call in (lambda: check_lengths([]), lambda: check_lengths([5, -1, -2]), lambda: cutoff_rows([60])):
        try:
            call()
        except SilverCasesError as error:
            messages += error.problems
    messages += silver_cases.source_schema_problems({"bronze.clinical_cases": None, "bronze.lab_results": [("x", "string")], "silver.patient_split": None})
    for ops in (
        cases.FakeOps(schema_wrong=2),
        cases.FakeOps(owner=[OWNERSHIP_PROBLEM]),
        cases.FakeOps(split=silver_cases.split_rule_problems([SplitRow("5", "test", "v9", 1), SplitRow("5", "training", "v9", 1)], pinned=False)),
        cases.FakeOps(held=(None, cutoffs)),
        cases.FakeOps(held=(measure, changed_cutoffs)),
        cases.FakeOps(after_write_rules=(None, None)),
        cases.FakeOps(output=cases.clean_output_facts(case_rows=1)),
        cases.FakeOps(lengths=[]),
    ):
        for run in (run_build, run_verify):
            try:
                run(ops, pinned=True)
            except SilverCasesError as error:
                messages += error.problems
    return messages


class CasesRefusals(unittest.TestCase):
    def test_every_refusal_is_a_code_and_a_flag_or_a_count_of_tables(self) -> None:
        messages = cases_messages()
        self.assertGreater(len(messages), 40)
        self.assertEqual([], unsealed(messages))

    def test_the_read_back_and_owner_problems_are_codes_with_a_flag(self) -> None:
        self.assertEqual("readback_differs: failed", RULES_READ_BACK)
        self.assertEqual("ambiguous_case_owner: failed", OWNERSHIP_PROBLEM)


class SplitResult(unittest.TestCase):
    ALLOWED_NUMBERS = {"snapshot_id", "source_snapshot_id", "fixed_sample_cases_checked"}

    def build(self, patients: dict[str, str] | None = None, snapshot: int = 11) -> dict[str, Any]:
        return split_table.run_build(
            split.Lake(patients or split.FULL, snapshot=snapshot).operations(), "h0", pinned=bool(patients is None), sample=None
        ).facts()

    def test_the_result_holds_no_outcome_no_count_of_patients_or_cases_and_no_count_of_a_set(self) -> None:
        facts = self.build()
        self.assertEqual(set(), keys_of(facts) & BANNED_KEYS, facts)
        verified = split_table.run_verify(
            split.Lake(split.FULL, existing=split.rows_of(split.FULL, snapshot=11)).operations(), "h0", pinned=True, sample=None
        ).facts()
        self.assertEqual(set(), keys_of(verified) & BANNED_KEYS, verified)

    def test_the_only_numbers_are_snapshot_ids_and_the_size_of_the_tracked_sample(self) -> None:
        facts = self.build()
        self.assertEqual([], [path for path in numbers(facts) if path[0] not in self.ALLOWED_NUMBERS], facts)

    def test_the_table_has_one_pass_flag(self) -> None:
        self.assertEqual({"patient_split": True}, self.build()["passed"])

    def test_a_larger_cohort_changes_only_hashes_and_snapshot_ids(self) -> None:
        small = {str(number): name for number, name in zip(range(1, 3000), build_assignments(str(n) for n in range(1, 3000)).values(), strict=True)}
        first, second = self.build(small, snapshot=11), self.build({**small, **build_assignments(["3001", "3002"])}, snapshot=12)
        volatile = {"snapshot_id", "source_snapshot_id", "checked_hash", "write_hash", "branch_hash"}
        self.assertEqual({k: v for k, v in first.items() if k not in volatile}, {k: v for k, v in second.items() if k not in volatile})


class SplitRefusals(unittest.TestCase):
    def test_every_refusal_of_the_split_is_a_code_and_a_flag_or_a_count_of_tables(self) -> None:
        messages = split.all_messages()
        self.assertGreater(len(messages), 30)
        self.assertEqual([], unsealed(messages))

    def test_a_schema_and_a_pinned_count_refusal_are_flags(self) -> None:
        rows = split.rows_of({**split.FULL, "424242": "test"})
        self.assertIn("pinned_counts: failed", split_table.table_problems(rows, pinned=True))
        self.assertEqual(["schema: failed"], split_table.schema_problems([("x", "string")], split_table.COLUMNS))


def clinical(result: dict[str, Any]) -> dict[str, Any]:
    table: dict[str, Any] = result["tables"]["clinical_cases"]
    return table


class BronzeClinicalResult(unittest.TestCase):
    def result(self, harness: bronze.Harness | None = None, command: str = "write") -> dict[str, Any]:
        return (harness or bronze.Harness()).run(command)

    def test_the_clinical_table_holds_no_count_no_digest_and_no_check_detail(self) -> None:
        table = clinical(self.result())
        self.assertEqual(set(), set(table) & BANNED_KEYS, table)
        self.assertTrue(table["passed"])
        for check in table["checks"]:
            self.assertEqual({"name", "passed"}, set(check), check)

    def test_the_clinical_table_keeps_its_snapshot_ids_and_the_laboratory_table_keeps_its_counts_and_digests(self) -> None:
        result = self.result()
        self.assertEqual(9, clinical(result)["bronze_snapshot_id"])
        lab = result["tables"]["lab_results"]
        self.assertEqual(6, lab["rows"])
        self.assertEqual("lab-keys", lab["keys_digest"])

    def test_the_clinical_digests_are_not_even_computed(self) -> None:
        harness = bronze.Harness()
        self.result(harness)
        self.assertNotIn("digests:clinical", harness.calls)
        self.assertIn("digests:lab", harness.calls)

    def test_a_clinical_table_with_other_row_counts_gives_the_same_result(self) -> None:
        other = bronze.Harness()
        other.spark.source_count_value = other.spark.bronze_count_value = 9
        self.assertEqual(clinical(self.result()), clinical(self.result(other)))

    def test_a_failed_clinical_check_is_a_name_and_a_flag_and_its_message_holds_no_digit(self) -> None:
        for plant in ("difference", "duplicates", "nulls", "rows"):
            harness = bronze.Harness()
            if plant == "difference":
                harness.spark.difference_counts = (3, 4)
            elif plant == "duplicates":
                harness.spark.duplicates = 7
            elif plant == "nulls":
                harness.spark.null_counts = (5, 6)
            else:
                harness.spark.bronze_count_value = 8
            result = self.result(harness, "verify")
            self.assertFalse(result["passed"], plant)
            err, out = io.StringIO(), io.StringIO()
            with contextlib.redirect_stderr(err):
                finish(result, "bronze_run", out, err)
            clinical_failures = [part for part in err.getvalue().split("; ") if "clinical_cases" in part]
            self.assertTrue(clinical_failures, plant)
            for part in clinical_failures:
                self.assertNotRegex(part.split("clinical_cases")[1], r"[0-9]", f"{plant}: {part}")

    def test_the_laboratory_table_keeps_its_check_details(self) -> None:
        harness = bronze.Harness()
        harness.spark.gaps = 2
        lab = self.result(harness, "verify")["tables"]["lab_results"]
        self.assertTrue(all("detail" in check for check in lab["checks"]))

    def test_the_verdict_of_a_publish_reads_names_and_flags_only(self) -> None:
        from spark.bronze_clinical_lab_publish import verdict_of

        harness = bronze.Harness()
        harness.spark.difference_counts = (1, 0)
        self.assertEqual(
            [("clinical_cases", "no value changed", "equality")], [(f.table, f.name, f.guard) for f in verdict_of(self.result(harness, "verify")).failed][:1]
        )


class MaintenanceResult(unittest.TestCase):
    SEALED_TABLE = "silver.cases"

    def read(self, table: str, rows: int = 10, checksum: int | None = 5) -> list[dict[str, Any]]:
        current = mock.Mock()
        current.snapshotId.return_value = 7
        handle = mock.Mock()
        handle.currentSnapshot.return_value = current
        session = mock.Mock()
        session.table.return_value.selectExpr.return_value.collect.return_value = [{"rows": rows, "checksum": checksum}]
        tagged: dict[str, tuple[TableSnapshot, ...]] = {"run-a": (TableSnapshot(table, 7, "file:/w/m.json"),)}
        with mock.patch.object(maintenance_job, "iceberg_table", return_value=handle):
            return maintenance_job.read_tagged(session, tagged, {"run-a": "pin0"})

    def test_a_table_that_can_hold_an_outcome_is_in_the_result_with_its_snapshot_only(self) -> None:
        for table in ("silver.cases", "bronze.clinical_cases", "raw_records.clinical_data", "silver.patient_split", "silver.labels", "some.new_table"):
            with self.subTest(table=table):
                self.assertEqual([{"tag": "run-a", "table": table, "snapshot_id": 7}], maintenance.public_reads(self.read(table)))

    def test_a_bronze_signal_table_keeps_its_row_count_and_checksum_in_the_result(self) -> None:
        for table in ("bronze.numeric_observations", "bronze.waveform_seconds", "bronze.tracks", "bronze.string_events", "catalog_proof.numbers"):
            with self.subTest(table=table):
                self.assertEqual([{"tag": "run-a", "table": table, "snapshot_id": 7, "rows": 10, "checksum": 5}], maintenance.public_reads(self.read(table)))

    def test_the_job_still_reads_the_row_count_and_the_checksum_of_every_table_for_its_own_comparison(self) -> None:
        self.assertEqual([{"tag": "run-a", "table": self.SEALED_TABLE, "snapshot_id": 7, "rows": 10, "checksum": 5}], self.read(self.SEALED_TABLE))

    def test_a_changed_row_count_of_a_sealed_table_is_a_difference_that_names_no_value(self) -> None:
        found = maintenance.compare_reads(self.read(self.SEALED_TABLE, 10, 5), self.read(self.SEALED_TABLE, 11, 6))
        self.assertEqual(2, len(found))
        for message in found:
            self.assertIn(self.SEALED_TABLE, message)
            self.assertNotRegex(message, r"\b(10|11|5|6)\b")

    def test_a_changed_snapshot_of_a_sealed_table_still_names_its_snapshots(self) -> None:
        after = [{**self.read(self.SEALED_TABLE)[0], "snapshot_id": 8}]
        self.assertIn("snapshot_id 7", maintenance.compare_reads(self.read(self.SEALED_TABLE), after)[0])

    def test_the_comparison_of_an_open_table_still_names_its_values(self) -> None:
        before = [{"tag": "t", "table": "bronze.tracks", "snapshot_id": 1, "rows": 10, "checksum": 5}]
        after = [{"tag": "t", "table": "bronze.tracks", "snapshot_id": 1, "rows": 11, "checksum": 5}]
        self.assertIn("11", maintenance.compare_reads(before, after)[0])

    def test_the_compaction_report_of_a_sealed_table_holds_no_count(self) -> None:
        self.assertEqual({"compacted": True}, maintenance.sealed_report(self.SEALED_TABLE, {"rewrite_data_files": {"rewritten_data_files_count": 3}}))
        report = {"rewrite_data_files": {"rewritten_data_files_count": 3}}
        self.assertEqual(report, maintenance.sealed_report("bronze.tracks", report))
        self.assertEqual({"compacted": True}, maintenance.sealed_report(self.SEALED_TABLE, {"would_rewrite": {"data_files": 4, "bytes": 9}}))


class MaintainResult(unittest.TestCase):
    """The result that ``maintain`` returns, with the reads of the tags replaced: a sealed table holds no row count and no checksum anywhere in it."""

    def result(self, tables: tuple[str, ...]) -> dict[str, Any]:
        reads = [{"tag": "run-a", "table": table, "snapshot_id": 7, "rows": 1234, "checksum": 5678} for table in tables]
        snapshots = tuple(TableSnapshot(table, 7, "file:/w/m.json") for table in tables)
        patches = (
            mock.patch.object(maintenance_job, "read_tagged", return_value=reads),
            mock.patch.object(maintenance_job, "list_tags", return_value={"run-a": "h"}),
            mock.patch.object(maintenance_job, "table_snapshots", return_value=snapshots),
            mock.patch.object(maintenance_job, "open_pins"),
        )
        with contextlib.ExitStack() as stack:
            for patched in patches:
                stack.enter_context(patched)
            return maintenance_tests.Rewrites(dict.fromkeys(tables, maintenance_tests.SAFE)).run([], maintenance_tests.stand_in(tables))

    def test_a_sealed_table_holds_no_row_count_and_no_checksum_in_the_result(self) -> None:
        result = self.result(("silver.cases", "silver.patient_split"))
        found = keys_of(result)
        self.assertFalse(found & {"rows", "checksum", "rewrite_data_files"}, result)
        self.assertEqual([7, 7], [item["snapshot_id"] for item in result["tagged_reads_before"]])
        self.assertEqual(result["tagged_reads_before"], result["tagged_reads_after"])

    def test_an_open_table_keeps_them_in_the_result(self) -> None:
        result = self.result(("bronze.tracks",))
        self.assertEqual([{"tag": "run-a", "table": "bronze.tracks", "snapshot_id": 7, "rows": 1234, "checksum": 5678}], result["tagged_reads_after"])


class SplitJobRefusals(unittest.TestCase):
    """The refusals of the patient split job's own reads (not its rules) are flags too."""

    def lake(self, session: Any) -> SplitLake:
        settings = {"nessie_uri": "http://127.0.0.1:1/api/v2", "warehouse": "file:///w/", "region": ""}
        return SplitLake(session, split.AnsweringNessie(split.BASE), "b", settings)

    def refusal(self, session: Any) -> list[str]:
        with self.assertRaises(split_table.PatientSplitError) as caught:
            self.lake(session).read_source(split.BASE)
        return caught.exception.problems

    def test_a_source_with_no_patient_is_a_flag(self) -> None:
        session = split.FakeSession()
        session.table = lambda name: split.FakeFrame(split_table.SOURCE_COLUMNS, [])  # type: ignore[method-assign]
        self.assertEqual(["source_empty: failed"], self.refusal(session))

    def test_a_source_with_no_snapshot_is_a_flag(self) -> None:
        session = split.FakeSession()
        session.load = lambda _session, name: SimpleNamespace(currentSnapshot=lambda: None)  # type: ignore[method-assign]
        session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable = session.load
        self.assertEqual(["source_no_snapshot: failed"], self.refusal(session))

    def test_a_source_with_other_columns_is_the_schema_flag_and_a_missing_source_is_a_count_of_tables(self) -> None:
        session = split.FakeSession()
        session.table = lambda name: split.FakeFrame([("caseid", "string"), ("subjectid", "bigint")], [])  # type: ignore[method-assign]
        self.assertEqual(["schema: failed"], self.refusal(session))
        self.assertEqual(["source_table: 0 tables"], self.refusal(split.FakeSession(missing=("clinical_cases",))))


class ClassScan(unittest.TestCase):
    """The sources of the sealed jobs hold no message that puts a count after a code, except a count of tables."""

    FILES = (
        "spark/silver_cases.py",
        "spark/silver_cases_run.py",
        "spark/jobs/silver_cases.py",
        "spark/split_table.py",
        "spark/jobs/patient_split.py",
        "spark/jobs/silver_signals.py",
        "spark/jobs/silver_hypotension.py",
        "spark/silver/run.py",
        "spark/silver/hypotension_run.py",
        "spark/silver/checks.py",
        "spark/silver/hypotension_checks.py",
        "spark/silver/cli.py",
    )

    def test_no_message_formats_a_count_with_a_noun_other_than_table(self) -> None:
        pattern = re.compile(r"unit\([^)]*,\s*'(?!table)[a-z ]+'\)")
        for path in self.FILES:
            self.assertEqual([], pattern.findall((ROOT / path).read_text()), path)

    def test_no_message_holds_a_code_and_a_written_count(self) -> None:
        pattern = re.compile(r"""["'f][a-z_]+: (\{[^}]+\}|[0-9]+) (?!tables?\b)[a-z]+""")
        for path in self.FILES:
            self.assertEqual([], pattern.findall((ROOT / path).read_text()), path)


if __name__ == "__main__":
    unittest.main()
