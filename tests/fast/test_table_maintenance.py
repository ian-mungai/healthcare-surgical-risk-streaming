"""Table maintenance only compacts: it deletes nothing, sets no table property and refuses a table that Iceberg could delete files from.

The owner's decision (Nov 22 2026): maintenance deletes nothing before teardown. The job rewrites data files and manifests on the default branch. The
rewrites add files and a commit, and the files they replace stay, because older snapshots, and the tags that point to them, still need them. These
checks hold what a real run cannot show cheaply: the refusals that come before any rewrite, the comparison of what every tag reads before and after,
the argument limits, and a scan of the sources that fails if a deletion, a table-property write or a name of a deleting procedure comes back. The
acceptance run proves the rewrites on real tables, with Spark and Trino reading every tag afterwards.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

from spark.maintenance import RetentionError, compare_reads, compare_tags, in_flight_branches, parse, table_identifier
from spark.preconditions import PROPERTY_KEYS, precondition_problems

ROOT = Path(__file__).resolve().parents[2]
JOB = ROOT / "spark" / "jobs" / "table_maintenance.py"
MODULES = (
    JOB,
    ROOT / "spark" / "maintenance.py",
    ROOT / "spark" / "catalog.py",
    ROOT / "spark" / "session.py",
    ROOT / "spark" / "settings.py",
    ROOT / "scripts" / "stack" / "tags.py",
    ROOT / "scripts" / "stack" / "nessie.py",
)
PRECONDITIONS = ROOT / "spark" / "preconditions.py"
DELETING = (
    "expire_snapshots",
    "remove_orphan_files",
    "expireSnapshots",
    "deleteOrphanFiles",
    "deleteFile",
    "deleteFiles",
    "TBLPROPERTIES",
    "updateProperties",
    "setProperty",
    "removeProperty",
    "gc.enabled",
    "delete-after-commit",
)
GC = "gc.enabled"
AFTER_COMMIT = "write.metadata.delete-after-commit.enabled"


class ArgumentTest(unittest.TestCase):
    def test_the_defaults_are_the_trunk_every_table_a_real_run_and_a_finite_deadline(self) -> None:
        arguments = parse([])
        self.assertEqual(("main", [], False, False), (arguments.trunk, arguments.tables, arguments.rewrite_all, arguments.dry_run))
        self.assertGreater(arguments.deadline_seconds, 0)
        self.assertLess(arguments.deadline_seconds, float("inf"))

    def test_a_deadline_that_is_not_finite_or_not_above_zero_is_refused_before_anything_runs(self) -> None:
        for value in ("inf", "-inf", "nan", "0", "-5", "1e999", "soon", ""):
            with self.subTest(value=value), self.assertRaises(SystemExit) as caught:
                parse([f"--deadline-seconds={value}"])
            self.assertEqual(2, caught.exception.code)

    def test_a_table_name_that_is_not_plain_names_joined_by_dots_is_refused(self) -> None:
        self.assertEqual(["catalog_proof.numbers", "a.b"], parse(["--table", "catalog_proof.numbers", "--table", "a.b"]).tables)
        for name in ("numbers'; DROP TABLE x; --", "a b.c", "a..b", ".a", "a.", "numbers"):
            with self.subTest(name=name), self.assertRaises(SystemExit) as caught:
                parse(["--table", name])
            self.assertEqual(2, caught.exception.code)

    def test_the_table_identifier_check_names_what_it_refused(self) -> None:
        self.assertEqual("catalog_proof.numbers", table_identifier("catalog_proof.numbers"))
        with self.assertRaises(RetentionError):
            table_identifier("a b.c")


class BranchTest(unittest.TestCase):
    def test_a_branch_other_than_the_trunk_means_a_run_may_be_writing_and_maintenance_waits(self) -> None:
        self.assertEqual(["proof_a", "proof_b"], in_flight_branches(["proof_b", "main", "proof_a"], "main"))
        self.assertEqual([], in_flight_branches(["main"], "main"))


class PreconditionTest(unittest.TestCase):
    def test_a_table_that_nessie_made_has_no_problem(self) -> None:
        self.assertEqual([], precondition_problems("catalog_proof.numbers", {GC: "false", AFTER_COMMIT: "false"}))
        self.assertEqual([], precondition_problems("catalog_proof.numbers", {GC: "false", AFTER_COMMIT: None}))
        self.assertEqual([], precondition_problems("catalog_proof.numbers", {GC: "FALSE", AFTER_COMMIT: "False"}))

    def test_a_table_whose_gc_flag_is_not_false_is_refused_by_name(self) -> None:
        for value in ("true", "TRUE", None, "", "yes", "0"):
            with self.subTest(value=value):
                found = precondition_problems("catalog_proof.numbers", {GC: value, AFTER_COMMIT: None})
                self.assertEqual(1, len(found))
                self.assertIn("catalog_proof.numbers", found[0])
                self.assertIn(GC, found[0])

    def test_a_table_that_deletes_old_metadata_files_after_each_commit_is_refused_by_name(self) -> None:
        for value in ("true", "TRUE", " true "):
            with self.subTest(value=value):
                found = precondition_problems("catalog_proof.numbers", {GC: "false", AFTER_COMMIT: value})
                self.assertEqual(1, len(found))
                self.assertIn("catalog_proof.numbers", found[0])
                self.assertIn(AFTER_COMMIT, found[0])

    def test_both_problems_are_named_together(self) -> None:
        self.assertEqual(2, len(precondition_problems("a.b", {GC: "true", AFTER_COMMIT: "true"})))

    def test_the_two_keys_that_the_job_reads_are_the_two_that_are_checked(self) -> None:
        self.assertEqual({GC, AFTER_COMMIT}, set(PROPERTY_KEYS))


class ReadComparisonTest(unittest.TestCase):
    BEFORE = [
        {"tag": "run-a", "table": "x.t", "snapshot_id": 1, "rows": 10, "checksum": 111},
        {"tag": "run-b", "table": "x.t", "snapshot_id": 2, "rows": 20, "checksum": 222},
    ]

    def changed(self, **changes: int | None) -> list[dict[str, object]]:
        after: list[dict[str, object]] = [dict(item) for item in self.BEFORE]
        after[1].update(changes)
        return after

    def test_reads_that_equal_the_reads_before_are_no_difference_whatever_their_order(self) -> None:
        self.assertEqual([], compare_reads(self.BEFORE, self.BEFORE))
        self.assertEqual([], compare_reads(self.BEFORE, list(reversed(self.BEFORE))))

    def test_a_row_count_a_snapshot_or_a_checksum_that_differs_is_named_with_its_tag_and_table(self) -> None:
        for field, value in (("rows", 19), ("snapshot_id", 3), ("checksum", 999), ("checksum", None)):
            with self.subTest(field=field):
                found = compare_reads(self.BEFORE, self.changed(**{field: value}))
                self.assertEqual(1, len(found))
                for part in ("run-b", "x.t", field):
                    self.assertIn(part, found[0])

    def test_a_tag_and_table_read_before_and_not_after_is_a_difference(self) -> None:
        found = compare_reads(self.BEFORE, self.BEFORE[:1])
        self.assertEqual(1, len(found))
        self.assertIn("run-b", found[0])

    def test_a_read_of_a_tag_that_came_after_is_not_compared_with_anything(self) -> None:
        added = [*self.BEFORE, {"tag": "run-c", "table": "x.t", "snapshot_id": 3, "rows": 30, "checksum": 333}]
        self.assertEqual([], compare_reads(self.BEFORE, added))


class TagComparisonTest(unittest.TestCase):
    def test_tags_that_are_as_they_were_are_no_change(self) -> None:
        self.assertEqual(([], []), compare_tags({"run-a": "h1", "run-b": "h2"}, {"run-a": "h1", "run-b": "h2"}))

    def test_a_tag_that_vanished_or_moved_is_a_change_and_names_the_tag(self) -> None:
        changed, added = compare_tags({"run-a": "h1", "run-b": "h2"}, {"run-b": "h9"})
        self.assertEqual([], added)
        self.assertEqual(2, len(changed))
        self.assertIn("run-a", changed[0])
        self.assertIn("run-b", changed[1])

    def test_a_tag_made_during_the_run_is_reported_and_is_not_a_change(self) -> None:
        self.assertEqual(([], ["run-c"]), compare_tags({"run-a": "h1"}, {"run-a": "h1", "run-c": "h3"}))


class NoDeletionTest(unittest.TestCase):
    """The sources never delete, never write a table property and never name the procedures that delete."""

    def test_no_job_or_module_of_the_job_holds_a_deleting_call_a_property_write_or_the_gc_or_metadata_deletion_names(self) -> None:
        for path in MODULES:
            text = path.read_text()
            for word in DELETING:
                with self.subTest(file=path.name, word=word):
                    self.assertNotIn(word, text)

    def test_the_preconditions_module_names_the_two_properties_only_to_read_them(self) -> None:
        text = PRECONDITIONS.read_text()
        for word in DELETING:
            if word not in ("gc.enabled", "delete-after-commit"):
                with self.subTest(word=word):
                    self.assertNotIn(word, text)
        self.assertEqual(1, len(re.findall(r'"gc\.enabled"', text)))
        self.assertEqual(1, len(re.findall(r'"write\.metadata\.delete-after-commit\.enabled"', text)))

    def test_the_job_compacts_and_uses_the_preconditions_so_this_scan_is_not_empty(self) -> None:
        text = JOB.read_text()
        for word in ("rewrite_data_files", "rewrite_manifests", "precondition_problems", "compare_reads", "compare_tags"):
            with self.subTest(word=word):
                self.assertIn(word, text)


class JobOrderTest(unittest.TestCase):
    """The refusals come before any rewrite. A real run proves it on Spark; this keeps the order from being changed in the source."""

    @staticmethod
    def calls(function: ast.FunctionDef) -> list[tuple[int, str]]:
        found = []
        for node in ast.walk(function):
            if isinstance(node, ast.Call):
                name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
                found.append((node.lineno, name))
        return sorted(found)

    def functions(self) -> dict[str, ast.FunctionDef]:
        tree = ast.parse(JOB.read_text())
        return {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}

    def test_the_job_refuses_unsafe_tables_before_it_reads_or_rewrites_anything(self) -> None:
        names = [name for _, name in self.calls(self.functions()["maintain"])]
        self.assertIn("refuse_unsafe_tables", names)
        self.assertLess(names.index("refuse_unsafe_tables"), names.index("read_tagged"))
        self.assertLess(names.index("refuse_unsafe_tables"), names.index("compact"))

    def test_the_refusal_uses_the_precondition_check_and_the_job_compares_the_reads_and_the_tags_after_the_rewrite(self) -> None:
        functions = self.functions()
        self.assertIn("precondition_problems", [name for _, name in self.calls(functions["refuse_unsafe_tables"])])
        names = [name for _, name in self.calls(functions["maintain"])]
        for compare in ("compare_reads", "compare_tags"):
            self.assertGreater(names.index(compare), names.index("compact"))


if __name__ == "__main__":
    unittest.main()
