"""The sequence of the Bronze clinical and laboratory job: which steps run, in which order, against which reference, and what each refuses.

The job takes its Nessie and Spark operations as parameters, so these tests run the whole sequence on fakes that record every call. A step
that is removed or moved changes the recorded calls. The fakes also stand in for the ways a table, a hash or a numbering can be wrong. The
real runs are in ``tests/e2e/bronze_clinical_lab.py``.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieError
from spark.bronze_clinical_lab import (
    LAB_COLUMNS,
    REPEATED_ROWS_LIMIT,
    BronzeRulesError,
    TableContent,
    assign_occurrences,
)
from spark.bronze_clinical_lab_run import RESULT_PREFIX, finish, parser, run_job
from spark.settings import SettingsError

HASH = "a" * 64
HEAD_B = "b" * 64
TRUNK = "trunk"
BRANCH = "bronze_run"
STRING = "string"
CLINICAL_COLUMNS = ["caseid", "subjectid", "age"]
SOURCES = ("raw_records.clinical_data", "raw_records.lab_data")
BRONZE = {"clinical": "bronze.clinical_cases", "lab": "bronze.lab_results"}
LAB_ROWS = [
    ("1", "5", "k", "4.0"),
    ("1", "5", "k", "4.0"),
    ("1", "5", "k", "4.1"),
    ("2", "5", "k", "3.9"),
    ("2", "6", "na", "139"),
    ("2", "6", "na", None),
]


def content(name: str, snapshot: int = 7, location: str | None = None) -> TableContent:
    return TableContent(location or f"file:/warehouse/{name}/metadata/00001.metadata.json", snapshot)


class FakeNessie:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.default = TRUNK
        self.history = {TRUNK: {HASH}}
        self.contents: dict[tuple[str, str], TableContent] = {}
        for table in SOURCES:
            self.contents[(f"{TRUNK}@{HASH}", table)] = content(table)
            self.contents[(BRANCH, table)] = content(table)
        for table in BRONZE.values():
            self.contents[(BRANCH, table)] = content(table, 9)
        self.branches = {BRANCH}

    def default_branch(self) -> str:
        self.calls.append("default_branch")
        return self.default

    def history_contains(self, branch: str, source_hash: str) -> bool:
        self.calls.append(f"history_contains:{branch}:{source_hash[:4]}")
        return source_hash in self.history.get(branch, set())

    def content(self, reference: str, table: str) -> TableContent | None:
        self.calls.append(f"content:{reference.replace(HASH, 'H')}:{table}")
        found = self.contents.get((reference, table))
        return self.contents.get((BRANCH, table)) if found is None and reference == f"{BRANCH}@{HEAD_B}" else found

    def create_branch(self, name: str, from_branch: str, source_hash: str) -> None:
        self.calls.append(f"create_branch:{name}:{from_branch}:{source_hash[:4]}")
        self.branches.add(name)

    def is_branch(self, name: str) -> bool:
        self.calls.append(f"is_branch:{name}")
        return name in self.branches

    def head(self, branch: str) -> str:
        self.calls.append(f"head:{branch}")
        return HEAD_B


def correct_numbers(rows: Sequence[tuple[str | None, str | None, str | None, str | None]]) -> list[tuple[Any, ...]]:
    return list(assign_occurrences(rows))


def dense_numbers(rows: Sequence[tuple[str | None, str | None, str | None, str | None]]) -> list[tuple[Any, ...]]:
    """Dense rank: rows with the same result share a number, so two identical rows are both 1."""
    seen: dict[tuple[str | None, ...], list[str | None]] = {}
    numbered: list[tuple[Any, ...]] = []
    for row in sorted(rows, key=lambda item: (item[:3], item[3] is not None, item[3] or "")):
        results = seen.setdefault(row[:3], [])
        if row[3] not in results:
            results.append(row[3])
        numbered.append((*row, results.index(row[3]) + 1))
    return numbered


def reversed_numbers(rows: Sequence[tuple[str | None, str | None, str | None, str | None]]) -> list[tuple[Any, ...]]:
    """The right numbers in the reverse order within each group."""
    forward = correct_numbers(rows)
    top = Counter(row[:3] for row in forward)
    return [(*row[:4], top[row[:3]] + 1 - row[4]) for row in forward]


class FakeSpark:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.opened: tuple[str, str, str] | None = None
        self.pinned: str | None = None
        self.numbering: Callable[[Sequence[tuple[str | None, str | None, str | None, str | None]]], list[tuple[Any, ...]]] = correct_numbers
        self.difference_counts = (0, 0)
        self.duplicates = 0
        self.null_counts = (0, 0)
        self.collect_nothing = False
        self.numbering_forbidden = False
        self.gaps = 0
        self.extra_repeated: list[tuple[Any, ...]] = []
        self.bronze_count_value = 6
        self.source_count_value = 6

    def source_columns(self, kind: str) -> list[tuple[str, str]]:
        self.calls.append(f"source_columns:{kind}")
        return [(name, STRING) for name in (CLINICAL_COLUMNS if kind == "clinical" else LAB_COLUMNS)] + [("_airbyte_raw_id", STRING)]

    def prepare(self) -> None:
        self.calls.append("prepare")

    def write(self, kind: str, columns: list[str]) -> None:
        self.calls.append(f"write:{kind}")

    def bronze_columns(self, kind: str, columns: list[str]) -> list[tuple[str, str]] | None:
        self.calls.append(f"bronze_columns:{kind}")
        return [(name, STRING) for name in columns] + ([("source_occurrence", "int")] if kind == "lab" else [])

    def source_count(self, kind: str, columns: list[str]) -> int:
        self.calls.append(f"source_count:{kind}")
        return self.source_count_value

    def bronze_count(self, kind: str) -> int:
        self.calls.append(f"bronze_count:{kind}")
        return self.bronze_count_value

    def difference(self, kind: str, columns: list[str]) -> tuple[int, int]:
        self.calls.append(f"difference:{kind}")
        return self.difference_counts

    def null_keys(self, kind: str, columns: list[str]) -> tuple[int, int]:
        self.calls.append(f"null_keys:{kind}")
        return self.null_counts

    def key_duplicates(self, kind: str) -> int:
        self.calls.append(f"key_duplicates:{kind}")
        return self.duplicates

    def occurrence_gaps(self) -> int:
        self.calls.append("occurrence_gaps")
        return self.gaps

    def collected(self) -> list[tuple[Any, ...]]:
        groups = Counter(row[:3] for row in LAB_ROWS)
        repeated = [row for row in LAB_ROWS if groups[row[:3]] > 1]
        return [*self.numbering(repeated), *self.extra_repeated]

    def repeated_row_count(self) -> int:
        self.calls.append("repeated_row_count")
        return len(self.collected())

    def repeated_rows(self, limit: int) -> list[tuple[Any, ...]]:
        self.calls.append(f"repeated_rows:{limit}")
        if self.numbering_forbidden:
            raise AssertionError("no row may be numbered or sorted when a key is null")
        return [] if self.collect_nothing else self.collected()

    def digests(self, kind: str, columns: list[str]) -> tuple[str, str]:
        self.calls.append(f"digests:{kind}")
        return f"{kind}-keys", f"{kind}-rows"

    def close(self) -> None:
        self.calls.append("close")


class Harness:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.nessie = FakeNessie(self.calls)
        self.spark = FakeSpark(self.calls)

    def open_spark(self, branch: str, default_branch: str, source_hash: str, branch_hash: str | None = None) -> FakeSpark:
        self.calls.append(f"open:{branch}:{default_branch}:{source_hash[:4]}" + (f":{branch_hash[:4]}" if branch_hash else ""))
        self.spark.opened = (branch, default_branch, source_hash)
        self.spark.pinned = branch_hash
        return self.spark

    def run(self, command: str = "write", source_hash: str = HASH, branch_hash: str | None = None) -> dict[str, Any]:
        return run_job(command, BRANCH, source_hash, self.nessie, self.open_spark, branch_hash)

    def failed(self, result: dict[str, Any]) -> set[str]:
        return {check["name"] for table in result["tables"].values() for check in table["checks"] if not check["passed"]}


def verify_calls(kind: str, reference: str = BRANCH) -> list[str]:
    source = SOURCES[0] if kind == "clinical" else SOURCES[1]
    calls = [
        f"content:{reference}:{source}",
        f"content:{TRUNK}@H:{source}",
        f"content:{reference}:{BRONZE[kind]}",
        f"bronze_columns:{kind}",
        f"null_keys:{kind}",
        f"source_count:{kind}",
        f"bronze_count:{kind}",
        f"difference:{kind}",
        f"key_duplicates:{kind}",
    ]
    if kind == "lab":
        calls += ["occurrence_gaps", "repeated_row_count", f"repeated_rows:{REPEATED_ROWS_LIMIT}"]
    return calls if kind == "clinical" else [*calls, f"digests:{kind}"]  # the digests of the clinical table, which holds the outcome, never leave the job


def start_calls(command: str) -> list[str]:
    calls = ["default_branch", f"history_contains:{TRUNK}:aaaa"]
    if command == "write":
        calls += [f"content:{TRUNK}@H:{SOURCES[0]}", f"content:{TRUNK}@H:{SOURCES[1]}", f"create_branch:{BRANCH}:{TRUNK}:aaaa"]
        return [*calls, f"open:{BRANCH}:{TRUNK}:aaaa", "source_columns:clinical", "source_columns:lab"]
    calls += [f"is_branch:{BRANCH}", f"head:{BRANCH}"]
    return [*calls, f"open:{BRANCH}:{TRUNK}:aaaa:bbbb", "source_columns:clinical", "source_columns:lab"]


class SequenceTest(unittest.TestCase):
    def test_write_runs_every_step_in_this_order(self) -> None:
        harness = Harness()
        result = harness.run("write")
        expected = [
            *start_calls("write"),
            "prepare",
            "write:clinical",
            "write:lab",
            *verify_calls("clinical"),
            *verify_calls("lab"),
            "close",
            f"head:{BRANCH}",
        ]
        self.assertEqual(expected, harness.calls)
        self.assertTrue(result["passed"])

    def test_verify_runs_every_check_and_writes_nothing(self) -> None:
        harness = Harness()
        result = harness.run("verify")
        pinned = f"{BRANCH}@{HEAD_B}"
        expected = [*start_calls("verify"), *verify_calls("clinical", pinned), *verify_calls("lab", pinned), "close"]
        self.assertEqual(expected, harness.calls)
        self.assertTrue(result["passed"])
        self.assertFalse([call for call in harness.calls if call.startswith(("write", "prepare", "create_branch"))])

    def test_the_hash_is_checked_in_the_history_before_any_branch_or_session_exists(self) -> None:
        harness = Harness()
        harness.run("write")
        for later in ("create_branch", "open:"):
            self.assertLess(harness.calls.index(f"history_contains:{TRUNK}:aaaa"), next(i for i, call in enumerate(harness.calls) if call.startswith(later)))

    def test_the_source_is_read_in_the_session_at_the_default_branch_and_the_hash_never_through_the_run_branch(self) -> None:
        harness = Harness()
        harness.run("write")
        self.assertEqual((BRANCH, TRUNK, HASH), harness.spark.opened)


class PinnedBranchTest(unittest.TestCase):
    """A check at a given hash of the branch reads that hash for Nessie's metadata and for Spark, and never the branch by its name."""

    PINNED = "c" * 64

    def pinned_harness(self) -> Harness:
        harness = Harness()
        for table in (*SOURCES, *BRONZE.values()):
            harness.nessie.contents[(f"{BRANCH}@{self.PINNED}", table)] = harness.nessie.contents[(BRANCH, table)]
        return harness

    def test_the_metadata_of_each_table_is_read_at_the_hash_and_not_through_the_branch_name(self) -> None:
        harness = self.pinned_harness()
        result = harness.run("verify", branch_hash=self.PINNED)
        contents = [call for call in harness.calls if call.startswith("content:")]
        self.assertTrue(contents)
        self.assertEqual([], [call for call in contents if call.startswith(f"content:{BRANCH}:")])
        self.assertTrue(all(call.startswith((f"content:{BRANCH}@{self.PINNED}:", f"content:{TRUNK}@H:")) for call in contents), contents)
        self.assertTrue(result["passed"])

    def test_the_spark_session_is_opened_pinned_to_the_hash(self) -> None:
        harness = self.pinned_harness()
        harness.run("verify", branch_hash=self.PINNED)
        self.assertEqual(self.PINNED, harness.spark.pinned)
        self.assertIn(f"open:{BRANCH}:{TRUNK}:aaaa:cccc", harness.calls)

    def test_the_result_names_the_hash_that_was_checked_and_does_not_read_the_head_again(self) -> None:
        harness = self.pinned_harness()
        result = harness.run("verify", branch_hash=self.PINNED)
        self.assertEqual(self.PINNED, result["branch_hash"])
        self.assertNotIn(f"head:{BRANCH}", harness.calls)

    def test_a_table_that_is_not_there_at_the_hash_fails_even_when_the_branch_head_holds_it(self) -> None:
        harness = Harness()
        result = harness.run("verify", branch_hash=self.PINNED)
        self.assertFalse(result["passed"])
        self.assertIn("source_changed", harness.failed(result))

    def test_a_verify_with_no_hash_resolves_the_head_once_at_its_start_and_uses_it_for_every_read_and_the_result(self) -> None:
        harness = Harness()
        result = harness.run("verify")
        self.assertEqual(1, harness.calls.count(f"head:{BRANCH}"))
        self.assertLess(harness.calls.index(f"head:{BRANCH}"), next(i for i, call in enumerate(harness.calls) if call.startswith("open:")))
        self.assertEqual(HEAD_B, harness.spark.pinned)
        self.assertEqual(HEAD_B, result["branch_hash"])
        reads = [call for call in harness.calls if call.startswith(f"content:{BRANCH}")]
        self.assertTrue(reads)
        self.assertTrue(all(call.startswith(f"content:{BRANCH}@{HEAD_B}:") for call in reads), reads)

    def test_a_write_reads_the_head_by_the_branch_name_because_it_writes_there(self) -> None:
        harness = Harness()
        harness.run("write")
        self.assertIsNone(harness.spark.pinned)
        self.assertIn(f"content:{BRANCH}:{SOURCES[0]}", harness.calls)

    def test_a_write_cannot_be_pinned_because_it_writes_to_the_head(self) -> None:
        with self.assertRaises(SettingsError):
            Harness().run("write", branch_hash=self.PINNED)


class DefaultBranchTest(unittest.TestCase):
    def test_the_run_branch_is_made_from_the_hash_on_the_default_branch_that_nessie_names(self) -> None:
        harness = Harness()
        harness.nessie.default = "release"
        harness.nessie.history = {"release": {HASH}}
        harness.nessie.contents = {(f"release@{HASH}", table): content(table) for table in SOURCES} | {
            (BRANCH, table): content(table) for table in (*SOURCES, *BRONZE.values())
        }
        harness.run("write")
        self.assertIn(f"history_contains:release:{HASH[:4]}", harness.calls)
        self.assertIn(f"create_branch:{BRANCH}:release:{HASH[:4]}", harness.calls)
        self.assertEqual((BRANCH, "release", HASH), harness.spark.opened)

    def test_a_hash_that_is_only_on_an_unmerged_branch_is_refused_before_any_branch_is_created(self) -> None:
        for command in ("write", "verify"):
            harness = Harness()
            harness.nessie.history = {TRUNK: set(), "load-unmerged": {HASH}}
            with self.subTest(command), self.assertRaisesRegex(NessieError, "not in the history of trunk"):
                harness.run(command)
            self.assertEqual(["default_branch", f"history_contains:{TRUNK}:aaaa"], harness.calls)

    def test_the_command_line_has_no_base_argument_so_no_other_branch_can_be_named(self) -> None:
        for command in ("write", "verify"):
            with self.subTest(command), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as stop:
                parser().parse_args([command, "--branch", BRANCH, "--source-hash", HASH, "--base", "load-unmerged"])
            self.assertEqual(2, stop.exception.code)

    def test_a_malformed_hash_is_refused_before_nessie_is_asked_anything(self) -> None:
        harness = Harness()
        with self.assertRaisesRegex(SettingsError, "64 lowercase"):
            harness.run("write", "not-a-hash")
        self.assertEqual([], harness.calls)

    def test_a_loaded_table_that_is_not_there_at_the_hash_is_refused_before_any_branch_is_created(self) -> None:
        harness = Harness()
        del harness.nessie.contents[(f"{TRUNK}@{HASH}", SOURCES[1])]
        with self.assertRaisesRegex(BronzeRulesError, "raw_records.lab_data"):
            harness.run("write")
        self.assertFalse([call for call in harness.calls if call.startswith(("create_branch", "open:"))])

    def test_verify_refuses_a_name_that_is_not_a_branch(self) -> None:
        harness = Harness()
        harness.nessie.branches = set()
        with self.assertRaisesRegex(NessieError, "not a branch"):
            harness.run("verify")


class SourceLineageTest(unittest.TestCase):
    def test_the_report_holds_the_metadata_location_and_the_snapshot_of_each_loaded_table_at_the_hash(self) -> None:
        result = Harness().run("write")
        clinical = result["tables"]["clinical_cases"]
        self.assertEqual(content(SOURCES[0]).metadata_location, clinical["source_metadata_location"])
        self.assertEqual(7, clinical["source_snapshot_id"])
        self.assertEqual(9, clinical["bronze_snapshot_id"])

    def test_a_loaded_table_with_another_metadata_location_but_the_same_snapshot_fails_source_changed(self) -> None:
        harness = Harness()
        harness.nessie.contents[(BRANCH, SOURCES[0])] = content(SOURCES[0], 7, "file:/warehouse/other/metadata/00002.metadata.json")
        result = harness.run("verify")
        self.assertEqual({"source_changed"}, harness.failed(result))
        self.assertFalse(result["passed"])

    def test_a_loaded_table_with_another_snapshot_but_the_same_metadata_location_fails_source_changed(self) -> None:
        harness = Harness()
        harness.nessie.contents[(BRANCH, SOURCES[1])] = content(SOURCES[1], 8)
        self.assertEqual({"source_changed"}, harness.failed(harness.run("verify")))

    def test_a_loaded_table_that_is_gone_from_the_branch_fails_source_changed(self) -> None:
        harness = Harness()
        del harness.nessie.contents[(BRANCH, SOURCES[0])]
        self.assertEqual({"source_changed"}, harness.failed(harness.run("verify")))


class TableCheckTest(unittest.TestCase):
    def test_rows_missing_from_bronze_or_not_in_the_loaded_table_fail_value_equality(self) -> None:
        for counts in ((1, 0), (0, 1), (2, 3)):
            harness = Harness()
            harness.spark.difference_counts = counts
            with self.subTest(counts):
                self.assertIn("no value changed", harness.failed(harness.run("verify")))

    def test_a_row_count_that_differs_fails_the_row_count(self) -> None:
        harness = Harness()
        harness.spark.bronze_count_value = 5
        self.assertIn("row count", harness.failed(harness.run("verify")))

    def test_a_key_that_repeats_fails_the_unique_check(self) -> None:
        harness = Harness()
        harness.spark.duplicates = 2
        self.assertEqual({"key unique"}, harness.failed(harness.run("verify")))

    def test_a_gap_in_the_occurrence_numbers_fails_the_gap_check(self) -> None:
        harness = Harness()
        harness.spark.gaps = 2
        self.assertEqual({"occurrence numbers from 1 without a gap"}, harness.failed(harness.run("verify")))


class NullKeyTest(unittest.TestCase):
    PLANTED_CASE = "X-PLANTED-CASE"

    def verified(self, harness: Harness) -> tuple[dict[str, Any], int, str, str]:
        result = harness.run("verify")
        out, err = io.StringIO(), io.StringIO()
        return result, finish(result, BRANCH, out, err), out.getvalue(), err.getvalue()

    def test_a_null_key_in_bronze_or_in_the_loaded_table_fails_the_key_check_first_and_nothing_is_numbered_or_sorted(self) -> None:
        for nulls in ((0, 1), (2, 0), (1, 1)):
            harness = Harness()
            harness.spark.null_counts = nulls
            harness.spark.numbering_forbidden = True
            with self.subTest(nulls):
                result, code, out, _ = self.verified(harness)
                self.assertEqual(1, code)
                self.assertEqual({"key not null"}, harness.failed(result))
                self.assertTrue(out.startswith(RESULT_PREFIX))
                for later in ("occurrence_gaps", "repeated_row_count", "repeated_rows", "source_count", "difference", "digests", "key_duplicates"):
                    self.assertFalse([call for call in harness.calls if call.startswith(later)], later)

    def test_the_key_check_runs_before_every_check_that_reads_the_rows_and_after_the_column_check(self) -> None:
        harness = Harness()
        harness.run("verify")
        first = harness.calls.index("bronze_columns:clinical")
        self.assertEqual("null_keys:clinical", harness.calls[first + 1])

    def test_a_null_occurrence_in_a_repeated_group_gives_a_result_and_a_failed_exit_not_an_int_of_none(self) -> None:
        harness = Harness()
        harness.spark.null_counts = (0, 1)
        harness.spark.numbering = lambda rows: [(*row[:4], None) for row in rows]
        harness.spark.numbering_forbidden = True
        result, code, out, err = self.verified(harness)
        self.assertEqual((1, False), (code, result["passed"]))
        self.assertIn("1 in bronze", err)
        self.assertEqual(1, out.count("\n"))

    def test_a_null_case_and_a_text_case_in_one_group_give_a_result_and_no_row_value_in_any_output(self) -> None:
        harness = Harness()
        harness.spark.null_counts = (0, 1)
        harness.spark.extra_repeated = [(None, "5", "k", "4.0", 1), (self.PLANTED_CASE, "5", "k", "4.1", 2)]
        harness.spark.numbering_forbidden = True
        result, code, out, err = self.verified(harness)
        self.assertEqual(1, code)
        self.assertFalse(result["passed"])
        self.assertNotIn(self.PLANTED_CASE, out + err + json.dumps(result))

    def test_a_null_result_is_a_missing_value_and_not_a_null_key(self) -> None:
        harness = Harness()
        self.assertTrue(harness.run("verify")["passed"])
        self.assertIn(None, [row[3] for row in LAB_ROWS])


class OccurrenceRuleTest(unittest.TestCase):
    def test_the_numbers_that_spark_wrote_pass_when_they_equal_the_python_numbers(self) -> None:
        for command in ("write", "verify"):
            with self.subTest(command):
                harness = Harness()
                self.assertTrue(harness.run(command)["passed"])

    def test_a_dense_rank_a_reversed_rank_and_a_changed_number_each_fail_the_independent_check_in_write_and_in_verify(self) -> None:
        for name, numbering in (("dense rank", dense_numbers), ("reversed rank", reversed_numbers)):
            for command in ("write", "verify"):
                harness = Harness()
                harness.spark.numbering = numbering
                with self.subTest(f"{name} in {command}"):
                    result = harness.run(command)
                    self.assertEqual({"occurrence rule"}, harness.failed(result))
                    self.assertFalse(result["passed"])

    def test_a_number_that_is_off_by_one_in_one_row_fails_the_independent_check(self) -> None:
        harness = Harness()
        harness.spark.numbering = lambda rows: [(*row[:4], row[4] + (1 if index == 0 else 0)) for index, row in enumerate(correct_numbers(rows))]
        self.assertEqual({"occurrence rule"}, harness.failed(harness.run("verify")))

    def test_more_repeated_rows_than_the_limit_are_refused_with_the_limit_in_the_message(self) -> None:
        self.assertEqual(100_000, REPEATED_ROWS_LIMIT)
        harness = Harness()
        harness.spark.extra_repeated = [("9", "9", "k", "1", 1)] * (REPEATED_ROWS_LIMIT + 1)
        with self.assertRaisesRegex(BronzeRulesError, str(REPEATED_ROWS_LIMIT)):
            harness.run("verify")

    def test_no_row_collected_where_the_counts_say_repeated_groups_exist_fails_the_check(self) -> None:
        harness = Harness()
        harness.spark.collect_nothing = True
        result = harness.run("verify")
        self.assertEqual({"occurrence rule"}, harness.failed(result))
        self.assertFalse(result["passed"])

    def test_the_cap_is_applied_on_the_count_before_any_row_is_collected(self) -> None:
        harness = Harness()
        harness.spark.extra_repeated = [("9", "9", "k", "1", 1)] * (REPEATED_ROWS_LIMIT + 1)
        with self.assertRaises(BronzeRulesError):
            harness.run("verify")
        self.assertFalse([call for call in harness.calls if call.startswith("repeated_rows")])

    def test_the_session_is_closed_also_when_a_step_raises(self) -> None:
        harness = Harness()
        harness.spark.extra_repeated = [("9", "9", "k", "1", 1)] * (REPEATED_ROWS_LIMIT + 1)
        with self.assertRaises(BronzeRulesError):
            harness.run("verify")
        self.assertEqual("close", harness.calls[-1])


if __name__ == "__main__":
    unittest.main()
