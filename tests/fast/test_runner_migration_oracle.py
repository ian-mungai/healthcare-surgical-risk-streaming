"""The signal profile runner reads a branch's history with the type of each operation, and its oracle names a case by the number that Spark groups by.

Failure modes covered here (the runner itself is untracked, so these tests skip where it is absent):

- The history reader keeps only the keys of a commit, so two puts of ``silver.unscored_share`` pass as a drop and a create. The reader keeps ``(type, key)``
  for every operation, and the migration check needs exactly a put (newest) and then a delete (older) of that key, each in a commit that holds no other key,
  a build whose base is the planted branch's head and a planted branch that did not move.
- The oracle keys the clinical rows by the raw text of ``caseid``, so ``99`` and ``099`` become two cases while Spark groups them by the number.
"""

from __future__ import annotations

import ast
import importlib
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "signal_profile.py"
KEY = ("silver", "unscored_share")
OTHER = ("silver", "signal_profile")
PLANTED = "a" * 64
MOVED = "b" * 64
FIRST = "c" * 64


def operation(kind: str, key: tuple[str, ...] = KEY) -> dict[str, Any]:
    return {"type": kind, "key": {"elements": list(key)}}


def entry(commit: str, *operations: dict[str, Any]) -> dict[str, Any]:
    return {"commitMeta": {"hash": commit}, "operations": list(operations)}


def log_of(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"logEntries": list(entries)}


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class HistoryReader(unittest.TestCase):
    def setUp(self) -> None:
        self.module = importlib.import_module("tests.e2e.signal_profile")

    def test_the_reader_keeps_the_type_and_the_key_of_each_operation_newest_first(self) -> None:
        log = log_of(entry("2", operation("PUT")), entry("1", operation("DELETE"), operation("PUT", OTHER)), entry(PLANTED))
        self.assertEqual([[("PUT", KEY)], [("DELETE", KEY), ("PUT", OTHER)]], self.module.history_commits(log, PLANTED))

    def test_a_log_that_does_not_hold_the_base_reads_as_no_commits(self) -> None:
        self.assertEqual([], self.module.history_commits(log_of(entry("2", operation("PUT")), entry("1", operation("DELETE"))), PLANTED))
        self.assertEqual([], self.module.history_commits({}, PLANTED))

    def test_a_log_that_starts_at_the_base_reads_as_no_commits(self) -> None:
        self.assertEqual([], self.module.history_commits(log_of(entry(PLANTED), entry(FIRST)), PLANTED))

    def test_an_entry_with_no_operations_is_an_empty_commit(self) -> None:
        self.assertEqual([[]], self.module.history_commits(log_of({"commitMeta": {"hash": "2"}}, entry(PLANTED)), PLANTED))

    def test_the_keys_of_a_commit_are_its_keys_without_the_types(self) -> None:
        self.assertEqual([KEY, OTHER], self.module.keys_of([("PUT", KEY), ("DELETE", OTHER)]))


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class MigrationCheck(unittest.TestCase):
    def setUp(self) -> None:
        self.module = importlib.import_module("tests.e2e.signal_profile")

    def check(
        self, commits: list[list[tuple[str, tuple[str, ...]]]], *, planted_before: str = PLANTED, planted_now: str = PLANTED, built_base: str = PLANTED
    ) -> bool:
        return bool(self.module.rebuilt_by_drop_then_create(commits, KEY, planted_before, planted_now, built_base))

    def test_a_delete_and_then_a_put_of_the_table_pass(self) -> None:
        self.assertTrue(self.check([[("PUT", KEY)], [("DELETE", KEY)]]))

    def test_the_reader_and_the_check_pass_a_history_read_from_a_nessie_log(self) -> None:
        log = log_of(entry("2", operation("PUT")), entry("1", operation("DELETE")), entry(PLANTED))
        self.assertTrue(self.check(self.module.history_commits(log, PLANTED)))

    def test_two_puts_do_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)], [("PUT", KEY)]]))
        log = log_of(entry("2", operation("PUT")), entry("1", operation("PUT")), entry(PLANTED))
        self.assertFalse(self.check(self.module.history_commits(log, PLANTED)))

    def test_two_deletes_do_not_pass(self) -> None:
        self.assertFalse(self.check([[("DELETE", KEY)], [("DELETE", KEY)]]))

    def test_a_delete_alone_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("DELETE", KEY)]]))

    def test_a_put_alone_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)]]))

    def test_the_put_before_the_delete_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("DELETE", KEY)], [("PUT", KEY)]]))

    def test_no_commit_does_not_pass(self) -> None:
        self.assertFalse(self.check([]))

    def test_a_third_commit_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY)], [("PUT", OTHER)]]))
        self.assertFalse(self.check([[("PUT", OTHER)], [("PUT", KEY)], [("DELETE", KEY)]]))

    def test_another_key_in_either_commit_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY), ("PUT", OTHER)], [("DELETE", KEY)]]))
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY), ("DELETE", OTHER)]]))

    def test_a_second_operation_on_the_key_in_a_commit_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY), ("PUT", KEY)], [("DELETE", KEY)]]))
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY), ("PUT", KEY)]]))

    def test_a_planted_branch_that_moved_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY)]], planted_now=MOVED))

    def test_a_build_on_another_base_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY)]], built_base=MOVED))

    def test_a_missing_hash_does_not_pass(self) -> None:
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY)]], built_base=None))  # type: ignore[arg-type]
        self.assertFalse(self.check([[("PUT", KEY)], [("DELETE", KEY)]], planted_before="", planted_now="", built_base=""))

    def test_a_base_that_the_history_does_not_hold_reads_as_no_commits_and_does_not_pass(self) -> None:
        log = log_of(entry("2", operation("PUT")), entry("1", operation("DELETE")))
        self.assertFalse(self.check(self.module.history_commits(log, PLANTED)))


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class MigrationWiring(unittest.TestCase):
    """The live case ``schema_cases`` cannot run here (it needs the stack), so its use of the check is read from the source."""

    def setUp(self) -> None:
        tree = ast.parse(RUNNER.read_text())
        self.case = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "schema_cases")
        self.calls = [n for n in ast.walk(self.case) if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "rebuilt_by_drop_then_create"]

    def test_the_case_asks_the_check_with_the_planted_head_before_and_after_and_the_base_of_the_build(self) -> None:
        self.assertEqual(1, len(self.calls))
        commits, key, before, now, built = (ast.unparse(arg) for arg in self.calls[0].args)
        self.assertEqual(("commits", "unscored_key", "before"), (commits, key, before))
        self.assertEqual("self.head(planted)", now)
        self.assertIn("base_hash", built)

    def test_the_result_of_the_check_is_part_of_the_verdict_and_no_weaker_test_of_the_commits_is(self) -> None:
        record = next(n for n in ast.walk(self.case) if isinstance(n, ast.Call) and ast.unparse(n.func) == "self.record.expect")
        verdict = ast.unparse(record.args[-1])
        self.assertIn("migrated", verdict)
        self.assertNotIn("len(commits)", verdict)
        assign = next(n for n in ast.walk(self.case) if isinstance(n, ast.Assign) and ast.unparse(n.targets[0]) == "migrated")
        self.assertIn("rebuilt_by_drop_then_create", ast.unparse(assign.value))


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class OracleCaseNumber(unittest.TestCase):
    def setUp(self) -> None:
        self.module = importlib.import_module("tests.e2e.signal_profile")
        tree = ast.parse(self.module.STAND_IN)
        self.dump = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "dump")
        helper = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "grid_by_case")
        space: dict[str, Any] = {}
        exec(compile(ast.Module(body=[helper], type_ignores=[]), "stand_in", "exec"), space)  # noqa: S102
        self.grid_by_case = space["grid_by_case"]

    def test_the_same_case_written_with_leading_zeros_is_one_case_with_its_pairs_together(self) -> None:
        rows = [("99", "1000", "5000"), ("099", "1000", "5000"), ("0099", "1000", None)]
        self.assertEqual({"99": [["1000", "5000"], ["1000", "5000"], ["1000", None]]}, self.grid_by_case(rows))

    def test_two_cases_stay_two_cases(self) -> None:
        self.assertEqual({"99": [["1", "2"]], "100": [["3", "4"]]}, self.grid_by_case([("99", "1", "2"), ("100", "3", "4")]))

    def test_the_key_is_the_text_of_the_number_that_the_arterial_rows_use(self) -> None:
        self.assertEqual(["99"], list(self.grid_by_case([(" 099 ", "1", "2")])))

    def test_no_rows_is_no_cases(self) -> None:
        self.assertEqual({}, self.grid_by_case([]))

    def test_the_plant_that_moves_a_case_to_another_patient_finds_the_case_by_its_number(self) -> None:
        plant = next(node for node in ast.walk(ast.parse(self.module.STAND_IN)) if isinstance(node, ast.FunctionDef) and node.name == "reassign_case")
        compares = [ast.unparse(node) for node in ast.walk(plant) if isinstance(node, ast.Compare)]
        self.assertEqual(1, len(compares))
        self.assertIn("cast('bigint')", compares[0], "the case is found by the raw text of caseid")

    def test_the_dump_builds_its_grid_through_the_helper_and_not_by_the_raw_text(self) -> None:
        calls = [node for node in ast.walk(self.dump) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "grid_by_case"]
        self.assertEqual(1, len(calls))
        raw = [
            node
            for node in ast.walk(self.dump)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", "") == "str"
            and node.args
            and isinstance(node.args[0], ast.Subscript)
            and getattr(node.args[0].slice, "value", "") == "caseid"
        ]
        self.assertEqual([], raw, "a case is keyed by the raw text of caseid")


if __name__ == "__main__":
    unittest.main()
