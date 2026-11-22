"""Table maintenance never removes what a tag points to: which snapshots it may expire and which orphan files it may delete.

The plan is a decision on plain facts (snapshot IDs and times, file paths), so the cases here prove each edge of it: a tagged snapshot that is old enough to
expire, the newest snapshots that are kept, the snapshot a table is at now, a boundary time and the spellings of one file path. The acceptance run
proves the same decisions on real tables: the maintenance job runs on Spark and Trino reads each tagged snapshot after it.
"""

from __future__ import annotations

import unittest

from scripts.stack.tags import TableSnapshot
from spark.maintenance import (
    RetentionError,
    SnapshotInfo,
    cut_off_ms,
    guard_expiry,
    in_flight_branches,
    normal_path,
    plan_expiry,
    protected_snapshots,
    removable_orphans,
    table_identifier,
)

TABLE = "catalog_proof.numbers"
HISTORY = [SnapshotInfo(snapshot_id, committed) for snapshot_id, committed in ((10, 1000), (20, 2000), (30, 3000), (40, 4000), (50, 5000))]


class ProtectedTest(unittest.TestCase):
    def test_each_tables_tagged_snapshots_are_named_by_their_tag(self) -> None:
        tagged = {
            "run-a": [TableSnapshot(TABLE, 20, "m1"), TableSnapshot("x.other", 7, "m2")],
            "run-b": [TableSnapshot(TABLE, 40, "m3")],
        }
        self.assertEqual({TABLE: {20: "run-a", 40: "run-b"}, "x.other": {7: "run-a"}}, protected_snapshots(tagged))

    def test_a_snapshot_under_two_tags_is_named_by_the_first_tag_in_name_order_and_a_table_with_no_snapshot_protects_nothing(self) -> None:
        tagged = {"run-b": [TableSnapshot(TABLE, 20, "m")], "run-a": [TableSnapshot(TABLE, 20, "m"), TableSnapshot("x.empty", None, "m")]}
        self.assertEqual({TABLE: {20: "run-a"}}, protected_snapshots(tagged))


class ExpiryPlanTest(unittest.TestCase):
    def test_older_untagged_snapshots_are_planned_oldest_first_and_the_newest_is_kept(self) -> None:
        self.assertEqual([10, 20, 30, 40], plan_expiry(HISTORY, 50, {}, 9999, 1))

    def test_a_tagged_snapshot_is_never_planned_however_old(self) -> None:
        self.assertEqual([10, 30, 40], plan_expiry(HISTORY, 50, {20: "run-a"}, 9999, 1))

    def test_the_snapshot_the_table_is_at_is_kept_when_it_is_not_the_newest(self) -> None:
        self.assertEqual([10, 20, 40], plan_expiry(HISTORY, 30, {}, 9999, 1))

    def test_the_newest_n_snapshots_are_kept_whatever_their_age(self) -> None:
        self.assertEqual([10, 20, 30], plan_expiry(HISTORY, 50, {}, 9999, 2))
        self.assertEqual([], plan_expiry(HISTORY, 50, {}, 9999, 5))

    def test_a_snapshot_committed_at_the_cut_off_time_or_later_is_kept(self) -> None:
        self.assertEqual([10, 20], plan_expiry(HISTORY, 50, {}, 3000, 1))
        self.assertEqual([10], plan_expiry(HISTORY, 50, {}, 2000, 1))
        self.assertEqual([], plan_expiry(HISTORY, 50, {}, 1000, 1))

    def test_fewer_than_one_kept_snapshot_is_refused(self) -> None:
        with self.assertRaises(RetentionError):
            plan_expiry(HISTORY, 50, {}, 9999, 0)

    def test_a_table_with_no_snapshot_has_nothing_to_expire(self) -> None:
        self.assertEqual([], plan_expiry([], None, {}, 9999, 1))


class GuardTest(unittest.TestCase):
    def test_a_plan_that_holds_a_tagged_snapshot_is_refused_naming_the_table_the_snapshot_and_the_tag(self) -> None:
        with self.assertRaises(RetentionError) as caught:
            guard_expiry(TABLE, [10, 20], {20: "run-a"})
        for part in (TABLE, "20", "run-a"):
            self.assertIn(part, str(caught.exception))

    def test_a_plan_of_untagged_snapshots_passes(self) -> None:
        guard_expiry(TABLE, [10, 30], {20: "run-a"})
        guard_expiry(TABLE, [], {20: "run-a"})


class PathTest(unittest.TestCase):
    def test_the_spellings_of_one_file_are_one_path(self) -> None:
        self.assertEqual("/warehouse/t/data/a.parquet", normal_path("file:/warehouse/t/data/a.parquet"))
        self.assertEqual("/warehouse/t/data/a.parquet", normal_path("file:///warehouse/t/data/a.parquet"))
        self.assertEqual("/warehouse/t/data/a.parquet", normal_path("/warehouse/t/data/a.parquet"))
        self.assertEqual("s3://bucket/warehouse/a", normal_path("s3a://bucket/warehouse/a"))
        self.assertEqual("s3://bucket/warehouse/a", normal_path("s3n://bucket/warehouse/a"))
        self.assertEqual("s3://bucket/warehouse/a", normal_path("s3://bucket/warehouse/a"))

    def test_two_different_files_stay_two_paths(self) -> None:
        self.assertNotEqual(normal_path("file:/warehouse/t/a"), normal_path("file:/warehouse/t/b"))
        self.assertNotEqual(normal_path("s3://one/a"), normal_path("s3://two/a"))


class OrphanTest(unittest.TestCase):
    def test_a_candidate_that_a_tag_needs_is_kept_whatever_its_spelling_and_the_others_are_removable_in_order(self) -> None:
        candidates = ["file:/warehouse/t/metadata/00001.metadata.json", "file:/warehouse/t/data/old.parquet", "s3a://b/w/t/m2.json"]
        protected = ["file:///warehouse/t/metadata/00001.metadata.json", "s3://b/w/t/m2.json"]
        removable, kept = removable_orphans(candidates, protected)
        self.assertEqual(["file:/warehouse/t/data/old.parquet"], removable)
        self.assertEqual(["file:/warehouse/t/metadata/00001.metadata.json", "s3a://b/w/t/m2.json"], kept)

    def test_with_no_protected_file_every_candidate_is_removable_and_with_no_candidate_nothing_is(self) -> None:
        self.assertEqual((["a", "b"], []), removable_orphans(["a", "b"], []))
        self.assertEqual(([], []), removable_orphans([], ["a"]))


class ScopeTest(unittest.TestCase):
    def test_a_branch_other_than_the_trunk_means_a_run_may_be_writing_and_maintenance_waits(self) -> None:
        self.assertEqual(["proof_a", "proof_b"], in_flight_branches(["proof_b", "main", "proof_a"], "main"))
        self.assertEqual([], in_flight_branches(["main"], "main"))

    def test_a_table_name_that_is_not_plain_names_joined_by_dots_is_refused_before_it_reaches_a_statement(self) -> None:
        self.assertEqual("catalog_proof.numbers", table_identifier("catalog_proof.numbers"))
        for name in ("", "numbers'; DROP TABLE x; --", "a b.c", "a..b", ".a", "a."):
            with self.subTest(name=name), self.assertRaises(RetentionError):
                table_identifier(name)

    def test_the_cut_off_is_the_given_minutes_before_now_and_a_negative_age_is_refused(self) -> None:
        self.assertEqual(1_000_000 - 90 * 60_000, cut_off_ms(1_000_000, 90))
        self.assertEqual(1_000_000, cut_off_ms(1_000_000, 0))
        with self.assertRaises(RetentionError):
            cut_off_ms(1_000_000, -1)


if __name__ == "__main__":
    unittest.main()
