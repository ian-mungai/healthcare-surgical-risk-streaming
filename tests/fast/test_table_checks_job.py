"""The comparisons of the Spark job on the production path, with a stand-in for the frames, and the job's use of the recorded hash.

``ListFrame`` keeps rows in a list and has only the three calls the comparison makes: ``select``, ``exceptAll`` and ``count``. It has no ``subtract``, no
``distinct`` and no trimming, so a comparison that is changed to a set difference or to a different call fails here. The expressions that build the real
frames from a file and from a table are Spark's; the acceptance test ``tests/e2e/table_checks.py`` runs them in the scenarios it names.
"""

from __future__ import annotations

import importlib
import io
import json
import tempfile
import unittest
from collections import Counter
from collections.abc import Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from spark import table_checks as rules

job: Any = importlib.import_module("spark.jobs.table_checks")
COLUMNS = ["caseid", "subjectid", "age"]
MARKER = "MARKER_9d2"


class ListFrame:
    def __init__(self, columns: list[str], rows: list[tuple[str, ...]]) -> None:
        self.columns = columns
        self.rows = rows
        self.calls: list[str] = []

    def select(self, *names: str) -> ListFrame:
        wanted = [name.strip("`") for name in names]
        positions = [self.columns.index(name) for name in wanted]
        return ListFrame(wanted, [tuple(row[position] for position in positions) for row in self.rows])

    def exceptAll(self, other: ListFrame) -> ListFrame:  # noqa: N802
        left, right = Counter(self.rows), Counter(other.rows)
        return ListFrame(self.columns, list((left - right).elements()))

    def count(self) -> int:
        return len(self.rows)


ROWS = [("1", "07", "60"), ("2", "08", ""), ("3", "09", "71"), ("3", "09", "71")]


def frames(table_rows: Sequence[tuple[str, ...]] | None = None) -> tuple[ListFrame, ListFrame]:
    return ListFrame(COLUMNS, list(ROWS)), ListFrame(COLUMNS, list(ROWS if table_rows is None else table_rows))


def compare(spec_name: str, table_rows: Sequence[tuple[str, ...]] | None = None) -> rules.TableResult:
    result = rules.TableResult(spec_name, 1)
    source, table = frames(table_rows)
    job.compare_rows(result, rules.SPEC_BY_NAME[spec_name], COLUMNS, source, table)
    return result


def outcome(result: rules.TableResult) -> dict[str, bool]:
    return {check.name: check.passed for check in result.checks}


class CompareRowsTest(unittest.TestCase):
    def test_every_comparison_reports_one_fixed_code_and_no_count_whether_it_passed_or_not(self) -> None:
        for name in ("clinical_data", "lab_data"):
            result = compare(name)
            self.assertEqual(["table_compared;equal=1"] * 3, [check.detail for check in result.checks])
            failed = compare(name, ROWS[:3])
            self.assertEqual(["table_compared;equal=0"] * 3, [check.detail for check in failed.checks])

    def test_control_equal_rows_pass_every_comparison_in_the_required_order(self) -> None:
        result = compare("clinical_data")
        self.assertEqual({"row count": True, "key multiset": True, "row payload": True}, outcome(result))
        self.assertEqual(["row count", "key multiset", "row payload"], [check.name for check in result.checks])
        self.assertEqual((None, None), (result.rows_source, result.rows_table))

    def test_the_comparisons_are_the_tail_of_the_tables_required_checks(self) -> None:
        for name in ("clinical_data", "lab_data"):
            self.assertEqual(list(rules.REQUIRED_CHECKS[name][-3:]), [check.name for check in compare(name).checks])

    def test_a_key_substituted_with_the_same_count_fails_the_key_comparison(self) -> None:
        rows = [("1", "07", "60"), ("2", "08", ""), ("9", "09", "71"), ("3", "09", "71")]
        self.assertEqual({"row count": True, "key multiset": False, "row payload": False}, outcome(compare("clinical_data", rows)))

    def test_a_value_substituted_outside_the_key_fails_only_the_payload_comparison(self) -> None:
        rows = [("1", "07", "61"), ("2", "08", ""), ("3", "09", "71"), ("3", "09", "71")]
        self.assertEqual({"row count": True, "key multiset": True, "row payload": False}, outcome(compare("clinical_data", rows)))

    def test_one_of_two_identical_rows_missing_is_a_difference_because_rows_are_compared_as_multisets(self) -> None:
        result = compare("clinical_data", ROWS[:3])
        self.assertEqual({"row count": False, "key multiset": False, "row payload": False}, outcome(result))

    def test_an_extra_row_fails_the_row_count(self) -> None:
        self.assertFalse(outcome(compare("clinical_data", [*ROWS, ("4", "10", "50")]))["row count"])

    def test_a_row_that_only_the_table_holds_fails_the_key_and_the_payload_comparison_too(self) -> None:
        for name in ("clinical_data", "lab_data"):
            self.assertEqual({"row count": False, "key multiset": False, "row payload": False}, outcome(compare(name, [*ROWS, ("4", "10", "50")])))

    def test_a_trimmed_or_cased_value_is_a_different_value(self) -> None:
        rows = [("1", "07", "60"), ("2", "8", ""), ("3", "09", "71"), ("3", "09", "71")]
        self.assertFalse(outcome(compare("clinical_data", rows))["row payload"])

    def test_the_whole_row_is_the_key_of_lab_data_so_its_key_check_sees_every_column(self) -> None:
        rows = [("1", "07", "61"), ("2", "08", ""), ("3", "09", "71"), ("3", "09", "71")]
        self.assertEqual({"row count": True, "key multiset": False, "row payload": False}, outcome(compare("lab_data", rows)))

    def test_clinical_data_keys_only_the_case_number(self) -> None:
        source, table = frames()
        seen: list[list[str]] = []
        original = ListFrame.select

        def spy(frame: ListFrame, *names: str) -> ListFrame:
            seen.append([name.strip("`") for name in names])
            return original(frame, *names)

        with mock.patch.object(ListFrame, "select", spy):
            job.compare_rows(rules.TableResult("clinical_data", 1), rules.SPEC_BY_NAME["clinical_data"], COLUMNS, source, table)
        self.assertIn(["caseid"], seen)

    def test_no_detail_holds_a_value_of_a_row(self) -> None:
        rows = [("1", "07", "60"), ("2", "08", ""), (MARKER, "09", "71"), ("3", "09", MARKER)]
        result = compare("clinical_data", rows)
        self.assertFalse(result.passed)
        self.assertNotIn(MARKER, json.dumps([{"name": check.name, "passed": check.passed, "detail": check.detail} for check in result.checks]))
        for check in result.checks:
            rules.parse_detail(check.detail)


class MainTest(unittest.TestCase):
    """The job opens the catalog on the recorded hash, writes the result of the plan it ran and prints no value of the data on a failure."""

    def staging(self, branch: str = "load_run_one") -> Path:
        folder = tempfile.TemporaryDirectory(prefix="table_checks_job_")
        self.addCleanup(folder.cleanup)
        plan = {
            "format": rules.PLAN_FORMAT,
            "run_id": "a" * 32,
            "branch": branch,
            "branch_hash": "b" * 64,
            "empty_form": "empty",
            "tables": [
                {"name": "clinical_data", "snapshot_id": 11, "file": "clinical_data.csv", "file_sha256": "c" * 64, "file_size": 1},
                {"name": "lab_data", "snapshot_id": 12, "file": "lab_data.csv", "file_sha256": "d" * 64, "file_size": 1},
                {"name": "track_list", "snapshot_id": 13, "rows": 7, "rows_sha256": "e" * 64},
            ],
        }
        (Path(folder.name) / "plan.json").write_text(json.dumps(plan))
        return Path(folder.name)

    def run_main(self, staging: Path, checker: Any) -> tuple[int, str, str, mock.MagicMock]:
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(job, "build_session") as session,
            mock.patch.object(job, "start_deadline"),
            mock.patch.object(job, "check_table", checker),
            redirect_stdout(out),
            redirect_stderr(err),
        ):
            code = job.main(["--staging", str(staging)])
        return code, out.getvalue(), err.getvalue(), session

    @staticmethod
    def passing(session: Any, staging: Path, plan: rules.Plan, entry: rules.PlanTable) -> rules.TableResult:
        sealed = rules.SPEC_BY_NAME[entry.name].sealed
        rows = None if sealed else entry.rows
        result = rules.TableResult(entry.name, entry.snapshot_id, rows_table=rows, rows_source=rows, columns_source=3, columns_table=3)
        details = {
            "columns": rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0),
            "rows digest": rules.detail("digest_compared", equal=1),
            "row count": rules.detail("table_compared", equal=1) if sealed else rules.detail("row_count", source=entry.rows, table=entry.rows),
            "key multiset": rules.detail("table_compared", equal=1),
            "row payload": rules.detail("table_compared", equal=1),
        }
        for name in rules.REQUIRED_CHECKS[entry.name]:
            result.add(name, details.get(name, "ok"))
        return result

    def test_the_catalog_is_opened_on_the_branch_as_of_the_recorded_hash(self) -> None:
        code, out, _, session = self.run_main(self.staging(), self.passing)
        self.assertEqual(0, code)
        session.assert_called_once_with(job.JOB, "load_run_one", "b" * 64)
        self.assertTrue(out.startswith(job.RESULT_PREFIX))

    def test_the_result_repeats_the_plan_it_ran_and_parses_against_it(self) -> None:
        staging = self.staging()
        _, out, _, _ = self.run_main(staging, self.passing)
        plan = rules.parse_plan((staging / "plan.json").read_text())
        self.assertTrue(rules.parse_result(out.removeprefix(job.RESULT_PREFIX), plan).passed)

    def test_a_failed_check_ends_with_exit_code_1_and_a_result_that_names_the_check(self) -> None:
        def failing(session: Any, staging: Path, plan: rules.Plan, entry: rules.PlanTable) -> rules.TableResult:
            if entry.name != "lab_data":
                return self.passing(session, staging, plan, entry)
            result = rules.TableResult(entry.name, entry.snapshot_id)
            result.add(rules.REQUIRED_CHECKS[entry.name][0], "bytes_differ")
            return result

        code, out, _, _ = self.run_main(self.staging(), failing)
        self.assertEqual(1, code)
        self.assertIn("bytes_differ", out)

    def test_a_plan_that_cannot_be_used_ends_with_exit_code_2_and_a_message_with_no_value_of_the_plan(self) -> None:
        staging = self.staging(branch=MARKER)
        (staging / "plan.json").write_text(json.dumps({"format": MARKER}))
        code, out, err, session = self.run_main(staging, self.passing)
        self.assertEqual(2, code)
        self.assertNotIn(MARKER, out + err)
        session.assert_not_called()

    def test_a_missing_plan_file_names_no_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix=f"{MARKER}_") as folder:
            code, out, err, _ = self.run_main(Path(folder), self.passing)
        self.assertEqual(2, code)
        self.assertNotIn(MARKER, out + err)


class TrackListDigestTest(unittest.TestCase):
    """The job's track list path with stand-in frames and a stand-in for ``pyspark``: the digest check holds what the pure comparison gives for the rows
    that the job streams, so a job that reports ``equal=1`` whatever the rows are fails here."""

    LINES = [f"{number}\tname{number}\tvalue" for number in range(7)]

    class Rows:
        """The table as the job reads it: a count, a filter that finds no separator and the sorted lines that it streams."""

        def __init__(self, lines: list[str]) -> None:
            self.lines = lines

        def count(self) -> int:
            return len(self.lines)

        def filter(self, condition: Any) -> TrackListDigestTest.Rows:
            return TrackListDigestTest.Rows([])

        def select(self, *columns: Any) -> TrackListDigestTest.Rows:
            return self

        def orderBy(self, name: str) -> TrackListDigestTest.Rows:  # noqa: N802
            return self

        def toLocalIterator(self) -> Any:  # noqa: N802
            return iter(SimpleNamespace(line=line) for line in self.lines)

    def check(self, table_lines: list[str]) -> str:
        from types import ModuleType

        errors = ModuleType("pyspark.errors")
        errors.AnalysisException = type("AnalysisException", (Exception,), {})  # type: ignore[attr-defined]
        sql = ModuleType("pyspark.sql")
        sql.functions = mock.MagicMock()  # type: ignore[attr-defined]
        plan = rules.parse_plan(
            json.dumps(
                {
                    "format": rules.PLAN_FORMAT,
                    "run_id": "a" * 32,
                    "branch": "load_run_one",
                    "branch_hash": "b" * 64,
                    "empty_form": "empty",
                    "tables": [
                        {"name": "clinical_data", "snapshot_id": 11, "file": "clinical_data.csv", "file_sha256": "c" * 64, "file_size": 1},
                        {"name": "lab_data", "snapshot_id": 12, "file": "lab_data.csv", "file_sha256": "d" * 64, "file_size": 1},
                        {"name": "track_list", "snapshot_id": 13, "rows": 7, "rows_sha256": rules.rows_digest(self.LINES)},
                    ],
                }
            )
        )
        columns = [
            SimpleNamespace(name=name, dataType=SimpleNamespace(simpleString=lambda: "string")) for name in rules.SPEC_BY_NAME["track_list"].pinned_columns
        ]
        session = mock.MagicMock()
        session.table.return_value.schema.fields = columns
        with (
            mock.patch.dict("sys.modules", {"pyspark": ModuleType("pyspark"), "pyspark.errors": errors, "pyspark.sql": sql}),
            mock.patch.object(job, "snapshot_of", return_value=13),
            mock.patch.object(job, "table_frame", return_value=self.Rows(table_lines)),
        ):
            result = job.check_table(session, Path("."), plan, plan.tables[2])
        self.assertEqual(["snapshot", "columns", "row count", "rows digest"], [check.name for check in result.checks])
        detail: str = result.checks[-1].detail
        return detail

    def test_control_the_rows_of_the_pinned_digest_give_equal_1(self) -> None:
        self.assertEqual("digest_compared;equal=1", self.check(list(self.LINES)))

    def test_rows_that_are_not_the_pinned_rows_give_equal_0_whatever_their_count(self) -> None:
        self.assertEqual("digest_compared;equal=0", self.check([*self.LINES[:6], "6\tname6\tother"]))


if __name__ == "__main__":
    unittest.main()
