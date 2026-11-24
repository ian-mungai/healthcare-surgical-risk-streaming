"""Publication of the clinical and laboratory Bronze branch: the same checks as ``verify``, run at the head of the branch, and a merge into Nessie's default
branch only when every one passed. The sequence runs on the fakes of the sequence tests, with a fake of the merge side.
"""

from __future__ import annotations

import unittest
from typing import Any

from scripts.stack.nessie import NessieError
from spark.bronze.publication import PublicationError
from spark.bronze_clinical_lab_publish import publish_job, verdict_of
from spark.bronze_clinical_lab_run import run_job
from tests.fast.test_bronze_job_sequence import BRANCH, BRONZE, HASH, SOURCES, TRUNK, Harness
from tests.fast.test_bronze_publish import BASE_HEAD, HEAD, FakeNessie, table

KEYS = list(BRONZE.values())


class Publisher(FakeNessie):
    def __init__(self) -> None:
        super().__init__(TRUNK, KEYS)


def publish_run(harness: Harness, publisher: Publisher) -> dict[str, Any]:
    for name in (*SOURCES, *BRONZE.values()):
        harness.nessie.contents[(f"{BRANCH}@{HEAD}", name)] = harness.nessie.contents[(BRANCH, name)]
    return publish_job(BRANCH, HASH, harness.nessie, publisher, harness.open_spark)


class PublishTest(unittest.TestCase):
    def test_a_branch_that_passes_every_check_is_published_to_the_default_branch_that_nessie_names(self) -> None:
        harness, publisher = Harness(), Publisher()
        result = publish_run(harness, publisher)
        self.assertTrue(result["passed"])
        self.assertEqual(("published", TRUNK, HEAD), (result["publication"]["status"], result["publication"]["base"], result["publication"]["published_hash"]))
        self.assertEqual([(TRUNK, BASE_HEAD, BRANCH, HEAD)], publisher.assign_calls)
        self.assertEqual({KEYS[0]: 10, KEYS[1]: 11}, result["publication"]["snapshots"])
        self.assertIn(f"open:{BRANCH}:{TRUNK}:aaaa:aaaa", harness.calls)
        self.assertEqual(HEAD, harness.spark.pinned)

    def test_a_failed_check_trips_its_guard_and_publishes_nothing(self) -> None:
        for plant, guard in (("null_counts", "key"), ("difference_counts", "equality"), ("duplicates", "key")):
            harness, publisher = Harness(), Publisher()
            if plant == "null_counts":
                harness.spark.null_counts = (1, 0)
            elif plant == "difference_counts":
                harness.spark.difference_counts = (1, 0)
            else:
                harness.spark.duplicates = 2
            result = publish_run(harness, publisher)
            self.assertFalse(result["passed"], plant)
            self.assertEqual(("blocked", [guard]), (result["publication"]["status"], result["publication"]["blocked_by"]), plant)
            self.assertEqual([], publisher.assign_calls, plant)
            self.assertEqual(BASE_HEAD, publisher.heads[TRUNK], plant)

    def test_a_loaded_table_that_changed_after_the_branch_was_made_trips_the_source_guard(self) -> None:
        harness, publisher = Harness(), Publisher()
        harness.nessie.contents[(BRANCH, "raw_records.lab_data")] = type(harness.nessie.contents[(BRANCH, "raw_records.lab_data")])("file:/other", 7)
        result = publish_run(harness, publisher)
        self.assertEqual(["source"], result["publication"]["blocked_by"])
        self.assertEqual([], publisher.assign_calls)

    def test_a_hash_that_is_not_in_the_history_of_the_default_branch_is_refused_before_any_check_or_merge(self) -> None:
        harness, publisher = Harness(), Publisher()
        with self.assertRaises(NessieError):
            publish_job(BRANCH, "c" * 64, harness.nessie, publisher, harness.open_spark)
        self.assertEqual([], publisher.assign_calls)
        self.assertFalse([call for call in harness.calls if call.startswith("open:")])

    def test_a_branch_that_also_carries_an_unchecked_table_is_refused_and_nothing_is_published(self) -> None:
        harness, publisher = Harness(), Publisher()
        publisher.entries_at[f"{BRANCH}@{HEAD}"]["bronze.waveform_seconds"] = table(77)
        with self.assertRaises(PublicationError) as raised:
            publish_run(harness, publisher)
        self.assertIn("bronze.waveform_seconds", str(raised.exception))
        self.assertEqual([], publisher.assign_calls)
        self.assertEqual(BASE_HEAD, publisher.heads[TRUNK])

    def test_a_name_that_is_not_a_branch_is_refused(self) -> None:
        harness, publisher = Harness(), Publisher()
        publisher.tags.add(BRANCH)
        with self.assertRaises(PublicationError):
            publish_run(harness, publisher)

    def test_the_verdict_of_a_verify_result_names_the_guard_of_each_failed_check(self) -> None:
        harness = Harness()
        harness.spark.bronze_count_value = 5
        document = run_job("verify", BRANCH, HASH, harness.nessie, harness.open_spark)
        result = verdict_of(document)
        self.assertEqual(
            [("clinical_cases", "row count", "equality"), ("lab_results", "row count", "equality")], [(f.table, f.name, f.guard) for f in result.failed]
        )
        self.assertEqual({"key": True, "structure": True, "source": True, "equality": False}, result.guards)

    def test_a_result_that_holds_fewer_tables_than_the_job_checks_is_not_a_pass(self) -> None:
        harness = Harness()
        document = run_job("verify", BRANCH, HASH, harness.nessie, harness.open_spark)
        document["tables"].pop("lab_results")
        self.assertFalse(verdict_of(document).passed)


if __name__ == "__main__":
    unittest.main()
