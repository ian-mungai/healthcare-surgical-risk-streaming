"""The record load: the gate, the exact-version copy, the load branch, the three syncs and the record of what was loaded, in that order.

Each planted fault has a passing control. A fault before the copy leaves Airbyte and the catalog untouched, a fault in a sync stops the later syncs and leaves
no record, and a second call of a finished run changes nothing. The stand-ins record every call, so each case proves what was and was not called.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from orchestration.surgical_risk.run_folder import SECRET_KEY
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import CopyError
from scripts.ingestion.record_definitions import Environment
from scripts.ingestion.record_load import Deadlines, LoadError, LoadResult, SyncDeadline, load_records, result_tags
from tests.fast.ingestion_support import list_text_for, pin_the_synthetic_list
from tests.fast.record_support import BASE_HASH, RELEASE_FILES, RUN, FakeAirbyte, FakeCatalog, FakeClock, landed_records

ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
BRANCH = f"load_{RUN}"
PREFIX = f"records_input/{RUN}/"
TABLES = ["clinical_data", "lab_data", "track_list"]
TAG_VALUE_LIMIT = 250  # the limit of orchestration/surgical_risk/groups.py, which needs Dagster to import


def setUpModule() -> None:
    pin_the_synthetic_list(list_text_for(RELEASE_FILES).encode())


class Setup:
    def __init__(self, test: unittest.TestCase) -> None:
        self.landed, self.reference = landed_records()
        self.catalog = FakeCatalog()
        self.airbyte = FakeAirbyte(self.catalog)
        self.clock = FakeClock()
        self.folder = Path(test.enterContext(tempfile.TemporaryDirectory(prefix="record_load_test_")))
        self.deadlines = Deadlines(sync_seconds=100.0, poll_seconds=10.0)

    @property
    def store(self) -> Any:
        return self.landed.store

    def load(self, reference: ManifestRef | None = None, run: str = RUN) -> LoadResult:
        return load_records(
            store=self.store,
            reference=reference or self.reference,
            run_id=run,
            environment=ENVIRONMENT,
            catalog=self.catalog,
            airbyte=self.airbyte,
            deadlines=self.deadlines,
            record_dir=self.folder,
            clock=self.clock.time,
            sleep=self.clock.sleep,
        )

    def untouched(self, test: unittest.TestCase) -> None:
        """Nothing was copied, no Airbyte call was made and the catalog was not asked for a branch or a head."""
        test.assertEqual([], self.store.copies)
        test.assertEqual([], self.airbyte.calls)
        test.assertEqual([], self.catalog.calls)
        test.assertEqual([], list(self.folder.iterdir()))


class OrderTest(unittest.TestCase):
    def test_control_a_passed_manifest_loads_three_tables_on_the_load_branch_in_the_fixed_order(self) -> None:
        setup = Setup(self)
        result = setup.load()
        self.assertEqual((RUN, BRANCH, BASE_HASH), (result.run_id, result.branch, result.base_hash))
        self.assertNotEqual(BASE_HASH, result.head_hash)
        self.assertEqual(setup.catalog.heads[BRANCH], result.head_hash)
        self.assertEqual(BASE_HASH, setup.catalog.heads["main"], "main did not move")
        self.assertEqual(TABLES, sorted(result.snapshots))
        self.assertEqual(TABLES, [call.removeprefix("start_sync ") for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertEqual(
            {"clinical_data": PREFIX + "clinical_data.csv", "lab_data": PREFIX + "lab_data.csv", "track_list": "https://api.vitaldb.net/trks"},
            setup.airbyte.read_from,
        )
        self.assertEqual(BRANCH, setup.airbyte.destination_branch)

    def test_the_copies_are_made_before_the_first_airbyte_call_and_the_branch_after_the_connections_exist(self) -> None:
        setup = Setup(self)
        seen: list[int] = []
        setup.airbyte.on_first_call = lambda: seen.append(len(setup.store.copies))
        setup.load()
        self.assertEqual([2], seen)
        self.assertLess(setup.airbyte.calls.index("ensure_connection track_list"), setup.airbyte.calls.index("start_sync clinical_data"))

    def test_each_sync_is_started_only_after_no_job_of_its_connection_is_running(self) -> None:
        setup = Setup(self)
        setup.load()
        for table in TABLES:
            self.assertLess(setup.airbyte.calls.index(f"running_jobs connection_{table}"), setup.airbyte.calls.index(f"start_sync {table}"))


class FaultBeforeTheCopyTest(unittest.TestCase):
    def test_a_manifest_that_does_not_exist_leaves_everything_untouched(self) -> None:
        setup = Setup(self)
        absent = ManifestRef(setup.reference.bucket, f"manifests/probe_set/{'0' * 64}.json", "version9999", "0" * 64)
        with self.assertRaises(GateRefused):
            setup.load(absent)
        setup.untouched(self)

    def test_a_substituted_version_leaves_everything_untouched(self) -> None:
        setup = Setup(self)
        setup.store.put_object(setup.landed.keys["lab_data.csv"], b"caseid\n9\n")
        with self.assertRaises(GateRefused):
            setup.load()
        setup.untouched(self)

    def test_a_missing_version_leaves_everything_untouched(self) -> None:
        setup = Setup(self)
        setup.store.put_delete_marker(setup.landed.keys["clinical_data.csv"])
        with self.assertRaises(GateRefused):
            setup.load()
        setup.untouched(self)

    def test_a_copy_whose_hash_differs_leaves_airbyte_and_the_catalog_untouched(self) -> None:
        setup = Setup(self)
        setup.store.corrupt.add(setup.landed.keys["clinical_data.csv"])
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], setup.airbyte.calls)
        self.assertEqual([], setup.catalog.calls)
        self.assertEqual([], list(setup.folder.iterdir()))

    def test_a_run_ID_that_is_not_a_plain_name_leaves_everything_untouched(self) -> None:
        setup = Setup(self)
        with self.assertRaises(ValueError):
            setup.load(run="../x")
        setup.untouched(self)


class FaultInTheLoadTest(unittest.TestCase):
    def test_a_copy_replaced_after_the_confirmation_stops_the_load_before_the_first_sync(self) -> None:
        setup = Setup(self)
        setup.airbyte.on_ensure_connection = lambda table: setup.store.put_object(PREFIX + "clinical_data.csv", b"caseid\n9\n")
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertNotIn(f"create_branch {BRANCH}", setup.catalog.calls)

    def test_a_copy_replaced_while_a_sync_runs_fails_the_load_and_writes_no_record(self) -> None:
        setup = Setup(self)
        setup.airbyte.after_start["lab_data"] = lambda: setup.store.put_object(PREFIX + "lab_data.csv", b"caseid\n9\n")
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], list(setup.folder.iterdir()))

    def test_a_destination_that_does_not_write_to_the_load_branch_stops_the_load_before_any_sync(self) -> None:
        setup = Setup(self)
        setup.airbyte.report_branch = "main"
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("branch", str(raised.exception))
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertEqual(BASE_HASH, setup.catalog.heads["main"])

    def test_a_job_that_is_already_running_stops_the_load_before_the_branch_is_made(self) -> None:
        setup = Setup(self)
        setup.airbyte.running = [7]
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertNotIn(f"create_branch {BRANCH}", setup.catalog.calls)

    def test_a_failed_sync_stops_the_later_syncs_names_the_table_and_writes_no_record(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("lab_data", str(raised.exception))
        self.assertIn("failed", str(raised.exception))
        self.assertNotIsInstance(raised.exception, SyncDeadline)
        self.assertEqual(["clinical_data", "lab_data"], [call.removeprefix("start_sync ") for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertEqual([], list(setup.folder.iterdir()))

    def test_a_cancelled_or_incomplete_sync_is_a_failure_too(self) -> None:
        for outcome in ("cancelled", "incomplete"):
            with self.subTest(outcome):
                setup = Setup(self)
                setup.airbyte.outcomes["clinical_data"] = outcome
                with self.assertRaises(LoadError):
                    setup.load()
                self.assertEqual(["start_sync clinical_data"], [call for call in setup.airbyte.calls if call.startswith("start_sync")])

    def test_a_sync_past_its_deadline_is_cancelled_and_stops_the_load(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "stuck"
        with self.assertRaises(SyncDeadline) as raised:
            setup.load()
        self.assertIn("lab_data", str(raised.exception))
        self.assertIn("100", str(raised.exception))
        self.assertEqual([2], setup.airbyte.cancelled, "the job that the load started is cancelled")
        self.assertGreaterEqual(setup.clock.now, 100.0)
        self.assertLess(setup.clock.now, 200.0, "the wait ended at the deadline")
        self.assertNotIn("start_sync track_list", setup.airbyte.calls)
        self.assertEqual([], list(setup.folder.iterdir()))

    def test_control_a_sync_that_ends_before_its_deadline_is_not_cancelled(self) -> None:
        setup = Setup(self)
        setup.airbyte.polls = 5
        setup.load()
        self.assertEqual([], setup.airbyte.cancelled)

    def test_a_sync_that_commits_nothing_is_a_failure(self) -> None:
        setup = Setup(self)
        setup.airbyte.commits = False
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("did not move", str(raised.exception))
        self.assertEqual([], list(setup.folder.iterdir()))

    def test_a_table_whose_snapshot_did_not_change_is_a_failure_even_when_main_already_held_one(self) -> None:
        setup = Setup(self)
        setup.catalog.tables["main"]["raw_records.lab_data"] = 555
        setup.airbyte.skip_tables = {"lab_data"}
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("lab_data", str(raised.exception))
        self.assertNotIn("clinical_data", str(raised.exception))

    def test_a_table_missing_on_the_branch_after_the_syncs_is_a_failure(self) -> None:
        setup = Setup(self)
        setup.airbyte.skip_tables = {"track_list"}
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("track_list", str(raised.exception))
        self.assertEqual([], list(setup.folder.iterdir()))


class SecondCallTest(unittest.TestCase):
    def test_a_second_call_of_a_finished_run_copies_syncs_and_branches_nothing(self) -> None:
        setup = Setup(self)
        first = setup.load()
        copies, writes = list(setup.store.copies), list(setup.store.writes)
        airbyte_calls, catalog_calls = list(setup.airbyte.calls), list(setup.catalog.calls)
        second = setup.load()
        self.assertEqual(first, second)
        self.assertEqual((copies, writes, airbyte_calls), (setup.store.copies, setup.store.writes, setup.airbyte.calls))
        self.assertEqual([], [call for call in setup.catalog.calls[len(catalog_calls) :] if call.startswith(("create_branch", "delete_branch"))])

    def test_a_second_call_still_runs_the_gate_first(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.store.put_object(setup.landed.keys["lab_data.csv"], b"caseid\n9\n")
        with self.assertRaises(GateRefused):
            setup.load()

    def test_a_finished_run_whose_branch_moved_is_refused_and_not_loaded_again(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.catalog.heads[BRANCH] = "f" * 64
        starts = len([call for call in setup.airbyte.calls if call.startswith("start_sync")])
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual(starts, len([call for call in setup.airbyte.calls if call.startswith("start_sync")]))

    def test_a_finished_run_whose_snapshot_changed_is_refused(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.catalog.tables[BRANCH]["raw_records.lab_data"] = 99999
        with self.assertRaises(LoadError):
            setup.load()

    def test_a_run_ID_that_was_loaded_from_another_manifest_is_refused(self) -> None:
        setup = Setup(self)
        setup.load()
        record = next(setup.folder.iterdir())
        document = json.loads(record.read_text())
        document["manifest_sha256"] = "0" * 64
        record.write_text(json.dumps(document))
        with self.assertRaises(LoadError):
            setup.load()

    def test_an_unfinished_try_leaves_a_branch_that_the_retry_replaces_and_then_loads(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        self.assertIn(BRANCH, setup.catalog.heads)
        setup.airbyte.outcomes.clear()
        result = setup.load()
        self.assertIn(f"delete_branch {BRANCH}", setup.catalog.calls)
        self.assertEqual(TABLES, sorted(result.snapshots))
        self.assertEqual(2, len([call for call in setup.store.copies]), "the copies of the first try were reused, not made again")


class RecordTest(unittest.TestCase):
    def test_the_record_names_the_branch_the_hashes_the_snapshots_the_manifest_and_the_pinned_connectors(self) -> None:
        setup = Setup(self)
        result = setup.load()
        document = json.loads(next(setup.folder.iterdir()).read_text())
        self.assertEqual(
            (RUN, BRANCH, BASE_HASH, result.head_hash, setup.reference.sha256),
            (document["run_id"], document["branch"], document["base_hash"], document["head_hash"], document["manifest_sha256"]),
        )
        self.assertEqual(TABLES, sorted(document["snapshots"]))
        self.assertTrue(all(isinstance(snapshot, int) for snapshot in document["snapshots"].values()))
        self.assertEqual(["clinical_data", "lab_data"], [copy["table"] for copy in document["copies"]])
        self.assertEqual({"airbyte/destination-s3-data-lake:0.4.2", "airbyte/source-s3:4.17.2", "airbyte/source-file:0.6.2"}, set(document["connectors"]))

    def test_the_record_holds_no_secret_and_no_object_key_outside_the_read_prefix(self) -> None:
        setup = Setup(self)
        setup.load()
        text = next(setup.folder.iterdir()).read_text()
        for word in ("access_key", "secret", "password", "token"):
            self.assertNotIn(word, text)

    def test_the_record_is_written_once_and_whole(self) -> None:
        setup = Setup(self)
        setup.load()
        self.assertEqual([f"{RUN}.json"], [path.name for path in setup.folder.iterdir()])


class TagTest(unittest.TestCase):
    def test_the_run_tags_state_the_branch_the_two_heads_the_manifest_and_each_snapshot_and_survive_the_run_folder_rules(self) -> None:
        setup = Setup(self)
        result = setup.load()
        tags = result_tags(result, "surgical_risk/records")
        self.assertEqual(
            {
                "surgical_risk/records/load_branch": BRANCH,
                "surgical_risk/records/base_hash": BASE_HASH,
                "surgical_risk/records/head_hash": result.head_hash,
                "surgical_risk/records/manifest_sha256": setup.reference.sha256,
                **{f"surgical_risk/records/snapshot_{table}": str(result.snapshots[table]) for table in TABLES},
            },
            tags,
        )
        self.assertTrue(all(len(value) <= TAG_VALUE_LIMIT for value in tags.values()))
        self.assertEqual([], [key for key in tags if SECRET_KEY.search(key)], "the run folder would replace the value of a tag whose key looks secret")


if __name__ == "__main__":
    unittest.main()
