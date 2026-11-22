"""The record load: the gate, the exact-version copy, the load branch, the three syncs and the record of what was loaded, in that order.

Each planted fault has a passing control. A fault before the copy leaves Airbyte and the catalog untouched, a fault in a sync stops the later syncs and leaves
no record, and a second call of a finished run changes nothing. The stand-ins record every call, so each case proves what was and was not called.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration.surgical_risk.run_folder import SECRET_KEY
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import CopyError
from scripts.ingestion.record_definitions import Environment
from scripts.ingestion.record_load import (
    Deadlines,
    LoadError,
    LoadResult,
    SyncDeadline,
    earlier_record,
    load_records,
    manifest_line,
    recorded_manifest,
    result_tags,
    write_record,
)
from tests.fast.ingestion_support import list_text_for, pin_the_synthetic_list
from tests.fast.record_support import BASE_HASH, RELEASE_FILES, RUN, FakeAirbyte, FakeCatalog, FakeClock, landed_records

ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
BRANCH = f"load_{RUN}"
PREFIX = f"records_input/{RUN}/"
TABLES = ["clinical_data", "lab_data", "track_list"]
RECORD_FILES = sorted([f"{RUN}.json", f"{RUN}.load_record.json"])
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
        self.assertEqual(["newest_recorded_manifest"], setup.catalog.calls, "the catalog was asked a question and made no branch")
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
        record = setup.folder / f"{RUN}.json"
        document = json.loads(record.read_text())
        document["manifest_sha256"] = "0" * 64
        document["manifest"]["sha256"] = "0" * 64
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
        document = json.loads((setup.folder / f"{RUN}.json").read_text())
        self.assertEqual(
            (RUN, BRANCH, BASE_HASH, result.head_hash, setup.reference.sha256),
            (document["run_id"], document["branch"], document["base_hash"], document["head_hash"], document["manifest_sha256"]),
        )
        self.assertEqual(TABLES, sorted(document["snapshots"]))
        self.assertTrue(all(isinstance(snapshot, int) for snapshot in document["snapshots"].values()))
        self.assertEqual(["clinical_data", "lab_data"], [copy["table"] for copy in document["copies"]])
        self.assertEqual({"airbyte/destination-s3-data-lake:0.4.2", "airbyte/source-s3:4.17.1", "airbyte/source-file:0.6.2"}, set(document["connectors"]))

    def test_the_record_holds_no_secret_and_no_object_key_outside_the_read_prefix(self) -> None:
        setup = Setup(self)
        setup.load()
        text = "".join(path.read_text() for path in setup.folder.iterdir())
        for word in ("access_key", "secret", "password", "token"):
            self.assertNotIn(word, text)

    def test_the_record_is_written_once_and_whole(self) -> None:
        setup = Setup(self)
        setup.load()
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))


LOAD_RECORD_FIELDS = {"format", "branch", "branch_hash", "manifest", "tables"}
HASH = re.compile(r"[0-9a-f]{64}")


def table_checks_accept(text: str) -> bool:
    """The rules of the table checks' parser for a load record, as read in ``parse_load_record`` of ``scripts/ingestion/table_checks.py`` on
    ``feat/p4-table-checks``: exactly the five fields, each of its type, the format ``load_record_1``, a branch that is not main, 64 lower case hex digits for
    the hashes and an integer for each of the three tables."""
    document = json.loads(text)
    manifest = document.get("manifest")
    tables = document.get("tables")
    return (
        type(document) is dict
        and set(document) == LOAD_RECORD_FIELDS
        and document["format"] == "load_record_1"
        and type(document["branch"]) is str
        and document["branch"] not in ("", "main")
        and HASH.fullmatch(str(document["branch_hash"])) is not None
        and type(manifest) is dict
        and set(manifest) == {"bucket", "key", "version_id", "sha256"}
        and all(type(value) is str for value in manifest.values())
        and HASH.fullmatch(manifest["sha256"]) is not None
        and type(tables) is dict
        and sorted(tables) == sorted(TABLES)
        and all(type(snapshot) is int for snapshot in tables.values())
    )


class LoadRecordShapeTest(unittest.TestCase):
    """The shape that the table checks read: ``load_record_1``."""

    EXTRA = {"format", "branch_hash", "manifest", "tables"}

    def test_the_run_record_holds_the_load_record_fields_beside_its_own(self) -> None:
        setup = Setup(self)
        result = setup.load()
        document = json.loads((setup.folder / f"{RUN}.json").read_text())
        self.assertEqual(
            {"run_id", "manifest_sha256", "branch", "base_hash", "head_hash", "snapshots", "jobs", "copies", "connectors"} | self.EXTRA, set(document)
        )
        self.assertEqual("load_record_1", document["format"])
        self.assertEqual((BRANCH, result.head_hash), (document["branch"], document["branch_hash"]))
        self.assertEqual(
            {"bucket": setup.reference.bucket, "key": setup.reference.key, "version_id": setup.reference.version_id, "sha256": setup.reference.sha256},
            document["manifest"],
        )
        self.assertEqual(document["snapshots"], document["tables"])
        self.assertEqual(TABLES, sorted(document["tables"]))
        self.assertTrue(all(type(snapshot) is int for snapshot in document["tables"].values()))

    def test_a_second_file_holds_exactly_the_five_fields_that_the_table_checks_accept_and_no_others(self) -> None:
        setup = Setup(self)
        result = setup.load()
        text = (setup.folder / f"{RUN}.load_record.json").read_text()
        document = json.loads(text)
        self.assertEqual(LOAD_RECORD_FIELDS, set(document))
        self.assertTrue(table_checks_accept(text))
        self.assertEqual((BRANCH, result.head_hash, result.snapshots), (document["branch"], document["branch_hash"], document["tables"]))
        self.assertEqual(setup.reference.sha256, document["manifest"]["sha256"])

    def test_the_run_record_alone_is_refused_by_the_table_checks_rules_because_of_its_other_fields_which_is_why_the_second_file_exists(self) -> None:
        setup = Setup(self)
        setup.load()
        self.assertFalse(table_checks_accept((setup.folder / f"{RUN}.json").read_text()))

    def test_the_load_record_file_is_not_read_as_a_run_record(self) -> None:
        setup = Setup(self)
        result = setup.load()
        other = ManifestRef(setup.reference.bucket, setup.reference.key, setup.reference.version_id, "0" * 64)
        self.assertIsNone(earlier_record(setup.folder, "0" * 64), "no record holds another manifest")
        self.assertEqual(result, earlier_record(setup.folder, setup.reference.sha256))
        self.assertEqual("0" * 64, other.sha256)

    def test_the_run_record_is_written_last_so_a_stop_between_the_two_files_is_an_unfinished_load_that_a_retry_completes(self) -> None:
        setup = Setup(self)
        with mock.patch("scripts.ingestion.record_load.os.link", side_effect=OSError("stopped")), self.assertRaises(OSError):
            setup.load()
        self.assertEqual([f"{RUN}.load_record.json"], sorted(path.name for path in setup.folder.iterdir() if not path.name.startswith(".")))
        result = setup.load()
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))
        self.assertEqual(RUN, result.run_id)
        self.assertTrue(table_checks_accept((setup.folder / f"{RUN}.load_record.json").read_text()))

    def test_a_run_record_whose_load_record_fields_disagree_with_its_own_is_refused(self) -> None:
        setup = Setup(self)
        setup.load()
        path = setup.folder / f"{RUN}.json"
        original = json.loads(path.read_text())
        edits: tuple[tuple[str, Callable[[dict[str, Any]], object]], ...] = (
            ("another format", lambda document: document.update(format="load_record_0")),
            ("another branch hash", lambda document: document.update(branch_hash="f" * 64)),
            ("another table snapshot", lambda document: document["tables"].update(lab_data=1)),
            ("another manifest", lambda document: document["manifest"].update(sha256="f" * 64)),
            ("manifest without a field", lambda document: document["manifest"].pop("key")),
            ("a field missing", lambda document: document.pop("tables")),
            ("a field added", lambda document: document.update(extra=1)),
        )
        for name, edit in edits:
            with self.subTest(name):
                document = json.loads(json.dumps(original))
                edit(document)
                path.write_text(json.dumps(document))
                with self.assertRaises(LoadError):
                    setup.load()
        path.write_text(json.dumps(original))
        self.assertEqual(RUN, setup.load().run_id)


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


class CleanupTest(unittest.TestCase):
    """The run's copies are removed once the record is written, and kept when the load fails."""

    def prefix_versions(self, setup: Setup) -> list[tuple[str, str]]:
        return [(version.key, version.version_id) for version in setup.store.list_versions(PREFIX)]

    def test_the_copies_are_removed_by_key_and_version_ID_after_the_record_is_written(self) -> None:
        setup = Setup(self)
        result = setup.load()
        self.assertEqual([], self.prefix_versions(setup))
        self.assertEqual(2, len(setup.store.deletes))
        self.assertEqual({PREFIX + "clinical_data.csv", PREFIX + "lab_data.csv"}, {key for key, _ in setup.store.deletes})
        self.assertEqual({copy.version_id for copy in result.copies}, {version for _, version in setup.store.deletes})
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))

    def test_a_failed_load_keeps_the_copies_for_inspection(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual([PREFIX + "clinical_data.csv", PREFIX + "lab_data.csv"], sorted(key for key, _ in self.prefix_versions(setup)))
        self.assertEqual([], setup.store.deletes)

    def test_a_late_sync_keeps_the_copies_too(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["track_list"] = "stuck"
        with self.assertRaises(SyncDeadline):
            setup.load()
        self.assertEqual(2, len(self.prefix_versions(setup)))

    def test_a_removal_that_fails_leaves_the_record_and_a_second_call_removes_the_copies_and_changes_nothing_else(self) -> None:
        setup = Setup(self)
        setup.store.fail_deletes = True
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("call again", str(raised.exception))
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))
        self.assertEqual(2, len(self.prefix_versions(setup)))
        setup.store.fail_deletes = False
        starts = [call for call in setup.airbyte.calls if call.startswith("start_sync")]
        result = setup.load()
        self.assertEqual([], self.prefix_versions(setup))
        self.assertEqual(starts, [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertEqual(RUN, result.run_id)

    def test_every_version_of_a_table_copy_is_removed_not_only_the_last_one(self) -> None:
        setup = Setup(self)
        setup.store.corrupt.add(setup.landed.keys["clinical_data.csv"])
        with self.assertRaises(CopyError):
            setup.load()
        setup.store.corrupt.clear()
        setup.load()
        self.assertEqual([], self.prefix_versions(setup))
        self.assertEqual(3, len(setup.store.deletes), "the corrupted first copy and the replacement of clinical_data, and the copy of lab_data")

    def test_a_second_call_of_a_finished_run_makes_no_copy_and_no_removal(self) -> None:
        setup = Setup(self)
        setup.load()
        copies, deletes, writes = list(setup.store.copies), list(setup.store.deletes), list(setup.store.writes)
        setup.load()
        self.assertEqual((copies, deletes, writes), (setup.store.copies, setup.store.deletes, setup.store.writes))

    def test_an_object_in_the_prefix_that_is_not_a_table_is_left_where_it_is(self) -> None:
        setup = Setup(self)
        setup.load()
        extra = setup.store.put_object(PREFIX + "other.csv", b"x")
        setup.load()
        self.assertEqual([(extra.key, extra.version_id)], self.prefix_versions(setup))


class ManifestConventionTest(unittest.TestCase):
    def test_the_merge_message_line_is_the_key_a_space_and_the_manifest_SHA_256(self) -> None:
        digest = "ab" * 32
        self.assertEqual(f"records-manifest: {digest}", manifest_line(digest))
        self.assertEqual(digest, recorded_manifest(f"load raw records\n\n{manifest_line(digest)}\n"))
        self.assertEqual(digest, recorded_manifest(manifest_line(digest)))

    def test_a_message_without_a_well_formed_line_records_nothing(self) -> None:
        digest = "ab" * 32
        for message in (
            "",
            "merge raw_records",
            f"records-manifest:{digest}",
            f"records-manifest: {digest.upper()}",
            f"records-manifest: {digest[:-1]}",
            f"x records-manifest: {digest}",
            f"records-manifest: {digest}0",
        ):
            with self.subTest(message=message):
                self.assertIsNone(recorded_manifest(message))

    def test_a_digest_that_is_not_64_lower_case_hex_digits_is_refused_when_a_line_is_made(self) -> None:
        for digest in ("", "ab", "AB" * 32, "zz" * 32):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                manifest_line(digest)


class SameManifestTest(unittest.TestCase):
    """A load whose manifest the default branch's newest recorded merge already holds returns that earlier run's record and starts nothing."""

    def earlier(self, setup: Setup, run: str = "run_earlier", age: float = 0.0) -> LoadResult:
        earlier = LoadResult(
            run,
            setup.reference,
            f"load_{run}",
            BASE_HASH,
            "c" * 64,
            {"clinical_data": 1, "lab_data": 2, "track_list": 3},
            {"clinical_data": 4, "lab_data": 5, "track_list": 6},
            (),
            ("x",),
        )
        path = write_record(setup.folder, earlier)
        os.utime(path, (1_000_000 + age, 1_000_000 + age))
        return earlier

    def test_the_earlier_runs_record_is_returned_and_nothing_is_copied_started_or_branched(self) -> None:
        setup = Setup(self)
        earlier = self.earlier(setup)
        setup.catalog.recorded = setup.reference.sha256
        self.assertEqual(earlier, setup.load())
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], setup.airbyte.calls)
        self.assertEqual(["newest_recorded_manifest"], setup.catalog.calls)
        self.assertEqual(["run_earlier.json", "run_earlier.load_record.json"], sorted(path.name for path in setup.folder.iterdir()))

    def test_control_another_manifest_on_main_or_none_loads_as_usual(self) -> None:
        for recorded in (None, "0" * 64):
            with self.subTest(recorded=recorded):
                setup = Setup(self)
                self.earlier(setup)
                setup.catalog.recorded = recorded
                result = setup.load()
                self.assertEqual(RUN, result.run_id)

    def test_the_gate_still_runs_first_so_a_substituted_version_is_refused_even_when_main_records_the_manifest(self) -> None:
        setup = Setup(self)
        self.earlier(setup)
        setup.catalog.recorded = setup.reference.sha256
        setup.store.put_object(setup.landed.keys["lab_data.csv"], b"caseid\n9\n")
        with self.assertRaises(GateRefused):
            setup.load()
        self.assertEqual([], setup.catalog.calls)

    def test_a_manifest_that_main_records_with_no_record_of_its_load_here_is_refused_and_starts_nothing(self) -> None:
        setup = Setup(self)
        setup.catalog.recorded = setup.reference.sha256
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("no record", str(raised.exception))
        self.assertEqual([], setup.airbyte.calls)
        self.assertEqual([], setup.store.copies)

    def test_when_several_records_hold_the_manifest_the_newest_file_is_returned(self) -> None:
        setup = Setup(self)
        self.earlier(setup, "run_old", age=0.0)
        newest = self.earlier(setup, "run_new", age=50.0)
        self.earlier(setup, "run_middle", age=10.0)
        setup.catalog.recorded = setup.reference.sha256
        self.assertEqual(newest, setup.load())

    def test_a_record_of_another_manifest_is_not_returned(self) -> None:
        setup = Setup(self)
        absent = ManifestRef("example-bucket", f"manifests/probe_set/{'0' * 64}.json", "v1", "0" * 64)
        other = LoadResult("run_other", absent, "load_run_other", BASE_HASH, "c" * 64, {"clinical_data": 1}, {"clinical_data": 4}, (), ("x",))
        write_record(setup.folder, other)
        setup.catalog.recorded = setup.reference.sha256
        with self.assertRaises(LoadError):
            setup.load()

    def test_the_run_that_has_its_own_record_is_returned_before_the_catalog_is_asked_about_the_manifest(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.catalog.recorded = setup.reference.sha256
        setup.catalog.calls.clear()
        self.assertEqual(RUN, setup.load().run_id)
        self.assertNotIn("newest_recorded_manifest", setup.catalog.calls)


if __name__ == "__main__":
    unittest.main()
