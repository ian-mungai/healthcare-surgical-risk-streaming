"""The record load: the gate, the exact-version copy, the load branch, the three syncs and the record of what was loaded, in that order.

Each planted fault has a passing control. A fault before the copy leaves Airbyte and the catalog untouched, a fault in a sync stops the later syncs and leaves
no record, and a second call of a finished run changes nothing. The stand-ins record every call, so each case proves what was and was not called.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
import unittest
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration.surgical_risk.run_folder import SECRET_KEY
from scripts.ingestion.airbyte_api import AirbyteError
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import CopyChanged, CopyError
from scripts.ingestion.record_definitions import Environment
from scripts.ingestion.record_load import (
    BranchMoved,
    Deadlines,
    JobStillActive,
    LoadError,
    LoadResult,
    SyncDeadline,
    current_load_record_text,
    earlier_record,
    load_records,
    manifest_line,
    read_record,
    recorded_manifest,
    result_tags,
    write_record,
)
from scripts.ingestion.record_state import RunState, StateError
from scripts.ingestion.table_checks import TableChecksError, parse_load_record
from tests.fast.ingestion_support import list_text_for, pin_the_synthetic_list
from tests.fast.record_support import BASE_HASH, RELEASE_FILES, RUN, FakeAirbyte, FakeCatalog, FakeClock, Stopped, landed_records

ENVIRONMENT = Environment("example-bucket", "us-west-2", "warehouse", "http://nessie.example:19120/api/v2")
BRANCH = f"load_{RUN}"
PREFIX = f"records_input/{RUN}/"
TABLES = ["clinical_data", "lab_data", "track_list"]
RECORD_FILES = sorted([f"{RUN}.json", f"{RUN}.load_record.json", f"{RUN}.state.json"])
CLINICAL_KEY = f"records_input/{RUN}/{hashlib.sha256(RELEASE_FILES['clinical_data.csv']).hexdigest()}/clinical_data.csv"
LAB_KEY = f"records_input/{RUN}/{hashlib.sha256(RELEASE_FILES['lab_data.csv']).hexdigest()}/lab_data.csv"
TAG_VALUE_LIMIT = 250  # the limit of orchestration/surgical_risk/groups.py, which needs Dagster to import


def setUpModule() -> None:
    pin_the_synthetic_list(list_text_for(RELEASE_FILES).encode())


class Setup:
    def __init__(self, test: unittest.TestCase) -> None:
        self.landed, self.reference = landed_records()
        self.catalog = FakeCatalog()
        self.airbyte = FakeAirbyte(self.catalog)
        self.clock = FakeClock()
        self.airbyte.clock = self.clock
        self.folder = Path(test.enterContext(tempfile.TemporaryDirectory(prefix="record_load_test_")))
        self.deadlines = Deadlines(sync_seconds=100.0, poll_seconds=10.0)
        self.catalog.state_dir = self.folder

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

    def records(self) -> list[str]:
        """The files of the run's records, not its state file: a load that did not finish has none."""
        return sorted(path.name for path in self.folder.iterdir() if not path.name.endswith(".state.json"))

    def untouched(self, test: unittest.TestCase) -> None:
        """Nothing was copied, no Airbyte call was made and the catalog was not asked for a branch or a head."""
        test.assertEqual([], self.store.copies)
        test.assertEqual([], self.airbyte.calls)
        test.assertEqual([], self.catalog.calls)
        test.assertEqual([], sorted(path.name for path in self.folder.iterdir()))


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
            {"clinical_data": CLINICAL_KEY, "lab_data": LAB_KEY, "track_list": "https://api.vitaldb.net/trks"},
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

    def test_every_connection_that_exists_is_checked_for_a_running_job_before_anything_is_configured_or_started(self) -> None:
        setup = Setup(self)
        setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
        setup.load()
        first_change = min(setup.airbyte.calls.index(call) for call in ("ensure_destination", "start_sync clinical_data"))
        for table in TABLES:
            self.assertLess(setup.airbyte.calls.index(f"running_jobs connection_{table}"), first_change)
        self.assertLess(setup.airbyte.calls.index("find_connection_ids"), first_change)


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
        self.assertEqual(["find_connection_ids"], setup.airbyte.calls, "Airbyte was only asked which connections exist")
        self.assertEqual(["has_recorded_manifest", f"reference {BRANCH}"], setup.catalog.calls, "the catalog was asked two questions and made no branch")
        self.assertEqual([], setup.records())

    def test_a_run_ID_that_is_not_a_plain_name_leaves_everything_untouched(self) -> None:
        setup = Setup(self)
        with self.assertRaises(ValueError):
            setup.load(run="../x")
        setup.untouched(self)


class FaultInTheLoadTest(unittest.TestCase):
    def test_a_copy_replaced_after_the_confirmation_stops_the_load_before_the_first_sync(self) -> None:
        setup = Setup(self)
        setup.airbyte.on_ensure_connection = lambda table: setup.store.put_object(CLINICAL_KEY, b"caseid\n9\n")
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertNotIn(f"create_branch {BRANCH}", setup.catalog.calls)

    def test_a_copy_replaced_while_a_sync_runs_fails_the_load_and_writes_no_record(self) -> None:
        setup = Setup(self)
        setup.airbyte.after_start["lab_data"] = lambda: setup.store.put_object(LAB_KEY, b"caseid\n9\n")
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], setup.records())

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
        setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
        setup.airbyte.running = [7]
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith(("start_sync", "ensure_"))], "no configuration was changed")
        self.assertEqual([], setup.store.copies)
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
        self.assertEqual([], setup.records())

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
        self.assertEqual([], setup.records())

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
        self.assertEqual([], setup.records())

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
        self.assertEqual([], setup.records())


def folder_state(folder: Path) -> dict[str, tuple[bytes, int, int]]:
    """Each file of the folder as its bytes, its change time and its inode, so a write, a replacement or a touch shows."""
    return {path.name: (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_ino) for path in sorted(folder.iterdir())}


def make_legacy(setup: Setup) -> LoadResult:
    """Rewrite the two files of a finished load as version 1 left them: the full record and the five-field file, both in the format ``load_record_1``."""
    result = setup.load()
    full = json.loads((setup.folder / f"{RUN}.json").read_text())
    full["format"] = "load_record_1"
    (setup.folder / f"{RUN}.json").write_text(json.dumps(full))
    five = {name: full[name] for name in ("format", "branch", "branch_hash", "manifest", "tables")}
    (setup.folder / f"{RUN}.load_record.json").write_text(json.dumps(five))
    return result


class LegacyRecordTest(unittest.TestCase):
    """A record that version 1 wrote is read as it is, and the file for the table checks is made again from its saved base head."""

    def test_the_full_record_of_version_1_is_read(self) -> None:
        setup = Setup(self)
        result = make_legacy(setup)
        self.assertEqual(result, LoadResult.from_json((setup.folder / f"{RUN}.json").read_text()))

    def test_a_second_call_of_a_run_that_version_1_finished_loads_nothing_and_leaves_both_files_as_they_were(self) -> None:
        setup = Setup(self)
        result = make_legacy(setup)
        airbyte_calls = list(setup.airbyte.calls)
        before = folder_state(setup.folder)
        self.assertEqual(result, setup.load())
        self.assertEqual(airbyte_calls, setup.airbyte.calls)
        self.assertEqual(before, folder_state(setup.folder), "no file was written, replaced or touched")
        text = current_load_record_text(setup.folder / f"{RUN}.load_record.json")
        self.assertEqual(LOAD_RECORD_FIELDS, set(json.loads(text)))
        self.assertEqual(BASE_HASH, parse_load_record(text).base_hash)

    def test_the_manifest_lookup_finds_a_record_that_version_1_wrote(self) -> None:
        setup = Setup(self)
        result = make_legacy(setup)
        self.assertEqual(result, earlier_record(setup.folder, setup.reference.sha256))

    def test_the_table_checks_file_of_version_1_is_converted_in_memory_and_nothing_is_written(self) -> None:
        setup = Setup(self)
        result = make_legacy(setup)
        other = setup.folder / "run_other.load_record.json"
        other.write_text("another run's file\n")
        path = setup.folder / f"{RUN}.load_record.json"
        before = folder_state(setup.folder)
        text = current_load_record_text(path)
        self.assertEqual(result.load_record_json(), text)
        self.assertEqual(BASE_HASH, parse_load_record(text).base_hash)
        self.assertEqual(before, folder_state(setup.folder), "no file of the folder was written, replaced, touched or added")
        self.assertEqual(text, current_load_record_text(path))

    def test_the_table_checks_file_of_version_1_is_read_from_a_folder_that_cannot_be_written(self) -> None:
        setup = Setup(self)
        result = make_legacy(setup)
        setup.folder.chmod(0o500)
        self.addCleanup(setup.folder.chmod, 0o700)
        if os.access(setup.folder, os.W_OK):
            self.skipTest("this process can write to a folder without write permission")
        self.assertEqual(result.load_record_json(), current_load_record_text(setup.folder / f"{RUN}.load_record.json"))

    def test_a_file_of_version_1_that_disagrees_with_the_full_record_in_any_of_its_five_fields_is_refused(self) -> None:
        changes: dict[str, Any] = {
            "branch": "load_other",
            "branch_hash": "f" * 64,
            "manifest": {"bucket": "other", "key": "k", "version_id": "v", "sha256": "0" * 64},
            "tables": {"clinical_data": 99, "lab_data": 2, "track_list": 3},
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                setup = Setup(self)
                make_legacy(setup)
                path = setup.folder / f"{RUN}.load_record.json"
                document = json.loads(path.read_text())
                document[field] = value
                path.write_text(json.dumps(document))
                before = folder_state(setup.folder)
                with self.assertRaises(LoadError):
                    current_load_record_text(path)
                self.assertEqual(before, folder_state(setup.folder))

    def test_a_file_of_version_1_with_a_missing_or_extra_field_is_refused(self) -> None:
        edits: list[Callable[[dict[str, Any]], object]] = [lambda document: document.pop("tables"), lambda document: document.update(extra=1)]
        for edit in edits:
            setup = Setup(self)
            make_legacy(setup)
            path = setup.folder / f"{RUN}.load_record.json"
            document = json.loads(path.read_text())
            edit(document)
            path.write_text(json.dumps(document))
            with self.assertRaises(LoadError):
                current_load_record_text(path)

    def test_the_file_of_the_current_format_is_returned_as_it_is_and_judged_by_the_table_checks_parser(self) -> None:
        setup = Setup(self)
        result = setup.load()
        path = setup.folder / f"{RUN}.load_record.json"
        self.assertEqual(result.load_record_json(), current_load_record_text(path))
        path.write_text("not json")
        self.assertEqual("not json", current_load_record_text(path))

    def test_a_file_of_version_1_with_no_full_record_beside_it_is_refused(self) -> None:
        setup = Setup(self)
        make_legacy(setup)
        (setup.folder / f"{RUN}.json").unlink()
        with self.assertRaises(LoadError):
            current_load_record_text(setup.folder / f"{RUN}.load_record.json")


class SidecarWriteTest(unittest.TestCase):
    """The file for the table checks belongs to its run: a try that stopped before the run record leaves one that the retry replaces whole, a link or a
    folder in its place is refused, and the run record is created and never replaced."""

    def test_a_file_that_a_stopped_try_left_is_replaced_whole_by_the_retry(self) -> None:
        setup = Setup(self)
        setup.load()
        (setup.folder / f"{RUN}.json").unlink()
        (setup.folder / f"{RUN}.load_record.json").write_text("left by a stopped try\n")
        again = setup.load()
        self.assertEqual(again.load_record_json(), (setup.folder / f"{RUN}.load_record.json").read_text())
        self.assertEqual(sorted(RECORD_FILES), sorted(path.name for path in setup.folder.iterdir()))

    def test_a_link_or_a_folder_in_place_of_the_file_is_refused_and_not_followed(self) -> None:
        for label in ("a link", "a dangling link", "a folder"):
            with self.subTest(case=label):
                setup = Setup(self)
                setup.load()
                (setup.folder / f"{RUN}.json").unlink()
                sidecar = setup.folder / f"{RUN}.load_record.json"
                sidecar.unlink()
                elsewhere = setup.folder.parent / f"elsewhere_{label.replace(' ', '_')}"
                if label == "a link":
                    elsewhere.write_text("kept\n")
                    sidecar.symlink_to(elsewhere)
                elif label == "a dangling link":
                    sidecar.symlink_to(elsewhere)
                else:
                    sidecar.mkdir()
                with self.assertRaises(LoadError):
                    setup.load()
                self.assertEqual("kept\n" if label == "a link" else None, elsewhere.read_text() if elsewhere.exists() else None)
                self.addCleanup(elsewhere.unlink, missing_ok=True)

    def test_the_second_call_of_a_finished_run_makes_a_missing_file_and_refuses_one_that_disagrees(self) -> None:
        setup = Setup(self)
        result = setup.load()
        sidecar = setup.folder / f"{RUN}.load_record.json"
        sidecar.unlink()
        self.assertEqual(result, setup.load())
        self.assertEqual(result.load_record_json(), sidecar.read_text())
        before = folder_state(setup.folder)
        self.assertEqual(result, setup.load())
        self.assertEqual(before, folder_state(setup.folder), "a file that agrees is left alone")
        sidecar.write_text("other\n")
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual("other\n", sidecar.read_text())

    def test_the_run_record_is_created_through_the_helper_and_a_held_one_is_never_replaced(self) -> None:
        setup = Setup(self)
        result = setup.load()
        (setup.folder / f"{RUN}.load_record.json").unlink()
        run_record = setup.folder / f"{RUN}.json"
        before = run_record.read_bytes(), run_record.stat().st_ino
        with self.assertRaises(LoadError):
            write_record(setup.folder, result)
        self.assertEqual(before, (run_record.read_bytes(), run_record.stat().st_ino))


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


LOAD_RECORD_FIELDS = {"format", "branch", "branch_hash", "base_hash", "manifest", "tables"}


def table_checks_accept(text: str) -> bool:
    """Whether the table checks' own parser (``parse_load_record``) accepts the text as a load record."""
    try:
        parse_load_record(text)
    except TableChecksError:
        return False
    return True


class LoadRecordShapeTest(unittest.TestCase):
    """The shape that the table checks read: ``load_record_2``."""

    EXTRA = {"format", "branch_hash", "manifest", "tables"}

    def test_the_run_record_holds_the_load_record_fields_beside_its_own(self) -> None:
        setup = Setup(self)
        result = setup.load()
        document = json.loads((setup.folder / f"{RUN}.json").read_text())
        self.assertEqual(
            {"run_id", "manifest_sha256", "branch", "base_hash", "head_hash", "snapshots", "jobs", "copies", "connectors"} | self.EXTRA, set(document)
        )
        self.assertEqual("load_record_2", document["format"])
        self.assertEqual((BRANCH, result.head_hash), (document["branch"], document["branch_hash"]))
        self.assertEqual(
            {"bucket": setup.reference.bucket, "key": setup.reference.key, "version_id": setup.reference.version_id, "sha256": setup.reference.sha256},
            document["manifest"],
        )
        self.assertEqual(document["snapshots"], document["tables"])
        self.assertEqual(TABLES, sorted(document["tables"]))
        self.assertTrue(all(type(snapshot) is int for snapshot in document["tables"].values()))

    def test_a_second_file_holds_exactly_the_six_fields_that_the_table_checks_accept_and_no_others(self) -> None:
        setup = Setup(self)
        result = setup.load()
        text = (setup.folder / f"{RUN}.load_record.json").read_text()
        document = json.loads(text)
        self.assertEqual(LOAD_RECORD_FIELDS, set(document))
        self.assertTrue(table_checks_accept(text))
        self.assertEqual(
            (BRANCH, result.head_hash, BASE_HASH, result.snapshots), (document["branch"], document["branch_hash"], document["base_hash"], document["tables"])
        )
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

    def test_a_record_file_of_invalid_utf_8_is_refused_by_name_when_read_and_is_not_a_record_when_searched(self) -> None:
        """Join round 2, class of finding 2: ``UnicodeDecodeError`` is not an ``OSError`` or a ``LoadError``, so such a file ended in a traceback."""
        setup = Setup(self)
        result = setup.load()
        (setup.folder / "zzz_not_text.json").write_bytes(b"\xff\xfe")
        self.assertEqual(result, earlier_record(setup.folder, setup.reference.sha256), "a file that is not text is not a record")
        unreadable = setup.folder / "zzz_unreadable.json"
        unreadable.write_text("{}")
        unreadable.chmod(0)
        try:
            with self.assertRaises(PermissionError, msg="a file that cannot be opened is an error and is not skipped as not a record"):
                earlier_record(setup.folder, setup.reference.sha256)
        finally:
            unreadable.chmod(0o644)
        unreadable.unlink()
        (setup.folder / f"{RUN}.json").write_bytes(b"\xff\xfe")
        with self.assertRaises(LoadError) as raised:
            read_record(setup.folder, RUN)
        self.assertEqual("the record of this run cannot be read", str(raised.exception))

    def test_the_run_record_is_written_last_so_a_stop_between_the_two_files_is_an_unfinished_load_that_a_retry_completes(self) -> None:
        setup = Setup(self)
        with mock.patch("scripts.ingestion.output_file.os.link", side_effect=OSError("stopped")), self.assertRaises(ClosedInputError):
            setup.load()
        self.assertEqual([f"{RUN}.load_record.json"], setup.records())
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
        self.assertEqual({CLINICAL_KEY, LAB_KEY}, {key for key, _ in setup.store.deletes})
        self.assertEqual({copy.version_id for copy in result.copies}, {version for _, version in setup.store.deletes})
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))

    def test_a_failed_load_keeps_the_copies_for_inspection(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual(sorted([CLINICAL_KEY, LAB_KEY]), sorted(key for key, _ in self.prefix_versions(setup)))
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


def write_earlier(setup: Setup, run: str = "run_earlier", age: float = 0.0) -> LoadResult:
    """The record of an earlier run of the same manifest, written to the record folder with a file time that is ``age`` seconds after the epoch's start."""
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


class SameManifestTest(unittest.TestCase):
    """A load whose manifest any commit of the default branch's history already records returns the record of the earlier run and starts nothing."""

    def earlier(self, setup: Setup, run: str = "run_earlier", age: float = 0.0) -> LoadResult:
        return write_earlier(setup, run, age)

    def test_the_earlier_runs_record_is_returned_and_nothing_is_copied_started_or_branched(self) -> None:
        setup = Setup(self)
        earlier = self.earlier(setup)
        setup.catalog.recorded = {setup.reference.sha256}
        self.assertEqual(earlier, setup.load())
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], setup.airbyte.calls)
        self.assertEqual(["has_recorded_manifest"], setup.catalog.calls)
        self.assertEqual(["run_earlier.json", "run_earlier.load_record.json"], sorted(path.name for path in setup.folder.iterdir()))

    def test_control_another_manifest_on_main_or_none_loads_as_usual(self) -> None:
        for recorded in (set(), {"0" * 64}):
            with self.subTest(recorded=recorded):
                setup = Setup(self)
                self.earlier(setup)
                setup.catalog.recorded = recorded
                result = setup.load()
                self.assertEqual(RUN, result.run_id)

    def test_the_gate_still_runs_first_so_a_substituted_version_is_refused_even_when_main_records_the_manifest(self) -> None:
        setup = Setup(self)
        self.earlier(setup)
        setup.catalog.recorded = {setup.reference.sha256}
        setup.store.put_object(setup.landed.keys["lab_data.csv"], b"caseid\n9\n")
        with self.assertRaises(GateRefused):
            setup.load()
        self.assertEqual([], setup.catalog.calls)

    def test_a_manifest_that_main_records_with_no_record_of_its_load_here_is_refused_and_starts_nothing(self) -> None:
        setup = Setup(self)
        setup.catalog.recorded = {setup.reference.sha256}
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
        setup.catalog.recorded = {setup.reference.sha256}
        self.assertEqual(newest, setup.load())

    def test_a_record_of_another_manifest_is_not_returned(self) -> None:
        setup = Setup(self)
        absent = ManifestRef("example-bucket", f"manifests/probe_set/{'0' * 64}.json", "v1", "0" * 64)
        other = LoadResult("run_other", absent, "load_run_other", BASE_HASH, "c" * 64, {"clinical_data": 1}, {"clinical_data": 4}, (), ("x",))
        write_record(setup.folder, other)
        setup.catalog.recorded = {setup.reference.sha256}
        with self.assertRaises(LoadError):
            setup.load()

    def test_the_run_that_has_its_own_record_is_returned_before_the_catalog_is_asked_about_the_manifest(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.catalog.recorded = {setup.reference.sha256}
        setup.catalog.calls.clear()
        self.assertEqual(RUN, setup.load().run_id)
        self.assertNotIn("has_recorded_manifest", setup.catalog.calls)


class ReadKeyVerificationTest(unittest.TestCase):
    """After the last sync every read key holds exactly the one version the run wrote, or the load fails as ``copy_changed`` and records nothing."""

    def finished_nothing(self, setup: Setup) -> None:
        self.assertEqual([], setup.records())
        self.assertIn(BRANCH, setup.catalog.heads, "the load branch is left, unmerged, for inspection")
        self.assertEqual(BASE_HASH, setup.catalog.heads["main"])
        self.assertEqual([], setup.store.deletes, "the copies are kept for inspection")

    def test_a_version_substituted_during_the_last_sync_and_still_current_fails_the_load_as_copy_changed(self) -> None:
        setup = Setup(self)
        setup.airbyte.after_start["track_list"] = lambda: setup.store.put_object(CLINICAL_KEY, b"substituted")
        with self.assertRaises(CopyChanged) as raised:
            setup.load()
        self.assertIn("copy_changed", str(raised.exception))
        self.finished_nothing(setup)

    def test_a_version_substituted_and_then_the_original_bytes_written_again_fails_the_load_although_the_current_bytes_are_right(self) -> None:
        setup = Setup(self)

        def substitute_then_restore() -> None:
            setup.store.put_object(LAB_KEY, b"substituted")
            setup.store.put_object(LAB_KEY, RELEASE_FILES["lab_data.csv"])

        setup.airbyte.after_start["track_list"] = substitute_then_restore
        with self.assertRaises(CopyChanged):
            setup.load()
        self.finished_nothing(setup)

    def test_a_delete_marker_after_the_last_sync_fails_the_load(self) -> None:
        setup = Setup(self)
        setup.airbyte.after_start["track_list"] = lambda: setup.store.put_delete_marker(CLINICAL_KEY)
        with self.assertRaises(CopyChanged):
            setup.load()
        self.finished_nothing(setup)

    def test_control_when_nothing_else_is_written_the_load_finishes_and_the_run_record_is_written(self) -> None:
        setup = Setup(self)
        setup.load()
        self.assertEqual(RECORD_FILES, sorted(path.name for path in setup.folder.iterdir()))

    def test_the_check_runs_before_the_head_and_the_snapshots_are_judged_and_before_the_record(self) -> None:
        setup = Setup(self)
        setup.airbyte.after_start["track_list"] = lambda: setup.store.put_object(CLINICAL_KEY, b"substituted")
        setup.airbyte.commits = False
        with self.assertRaises(CopyChanged):
            setup.load()


class RunStateTest(unittest.TestCase):
    """A retry deletes only what the run's state file names, and refuses a version or a branch it does not name."""

    def test_the_state_names_each_copy_the_branch_with_its_base_hash_and_each_job_the_run_started(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["track_list"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        state = RunState(setup.folder, RUN)
        self.assertEqual({CLINICAL_KEY, LAB_KEY}, set(state.keys()))
        self.assertEqual((BRANCH, BASE_HASH), state.branch)
        self.assertEqual({"clinical_data": 1, "lab_data": 2, "track_list": 3}, state.jobs)

    def test_a_version_at_a_read_key_that_the_state_does_not_name_is_refused_on_a_retry_and_never_deleted(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        stranger = setup.store.put_object(CLINICAL_KEY, b"stranger")
        setup.airbyte.outcomes.clear()
        with self.assertRaises(CopyError) as raised:
            setup.load()
        self.assertIn("not recorded", str(raised.exception))
        self.assertEqual([], setup.store.deletes)
        self.assertIn((stranger.key, stranger.version_id), [(version.key, version.version_id) for version in setup.store.list_versions(CLINICAL_KEY)])

    def test_a_branch_of_the_run_s_name_that_the_state_does_not_record_is_refused_and_never_deleted(self) -> None:
        setup = Setup(self)
        setup.catalog.make_branch(BRANCH)
        setup.catalog.calls.clear()
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("no record that this run created it", str(raised.exception))
        self.assertNotIn(f"delete_branch {BRANCH}", setup.catalog.calls)
        self.assertIn(BRANCH, setup.catalog.heads)
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith(("start_sync", "ensure_"))])
        self.assertEqual([], setup.store.copies, "the refusal came before the first copy")

    def test_a_branch_that_the_state_records_is_deleted_at_its_current_head_and_made_again(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        setup.airbyte.outcomes.clear()
        setup.catalog.calls.clear()
        result = setup.load()
        self.assertEqual(RUN, result.run_id)
        self.assertIn(f"delete_branch {BRANCH}", setup.catalog.calls)
        self.assertEqual((BRANCH, BASE_HASH), RunState(setup.folder, RUN).branch)

    def test_a_recorded_branch_that_is_gone_is_made_again_without_a_delete(self) -> None:
        setup = Setup(self)
        setup.airbyte.outcomes["lab_data"] = "failed"
        with self.assertRaises(LoadError):
            setup.load()
        del setup.catalog.heads[BRANCH], setup.catalog.tables[BRANCH]
        setup.airbyte.outcomes.clear()
        setup.catalog.calls.clear()
        setup.load()
        self.assertNotIn(f"delete_branch {BRANCH}", setup.catalog.calls)

    def test_a_state_file_that_is_not_of_its_form_stops_the_load_before_anything_is_touched(self) -> None:
        setup = Setup(self)
        (setup.folder / f"{RUN}.state.json").write_text("{}")
        with self.assertRaises(StateError):
            setup.load()
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], setup.airbyte.calls)

    def test_the_removal_after_a_finished_load_deletes_exactly_the_recorded_versions_and_leaves_a_stranger(self) -> None:
        setup = Setup(self)
        result = setup.load()
        stranger = setup.store.put_object(CLINICAL_KEY, b"stranger")
        setup.load()
        self.assertEqual({(copy.key, copy.version_id) for copy in result.copies}, set(setup.store.deletes))
        self.assertEqual([(stranger.key, stranger.version_id)], [(version.key, version.version_id) for version in setup.store.list_versions(CLINICAL_KEY)])


class JobWaitTest(unittest.TestCase):
    def test_a_sync_that_succeeds_after_its_deadline_is_not_accepted_and_nothing_is_recorded(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=5.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.latency = 10.0
        setup.airbyte.polls = 0
        with self.assertRaises(SyncDeadline) as raised:
            setup.load()
        self.assertIn("after its deadline", str(raised.exception))
        self.assertEqual([], setup.records())
        self.assertEqual(["start_sync clinical_data"], [call for call in setup.airbyte.calls if call.startswith("start_sync")])
        self.assertEqual([], setup.airbyte.cancelled, "a job that has ended is not cancelled")

    def test_control_a_sync_that_succeeds_inside_its_deadline_is_accepted(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=50.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.latency = 1.0
        setup.airbyte.polls = 0
        self.assertEqual(RUN, setup.load().run_id)

    def test_a_wait_never_sleeps_past_the_deadline(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=5.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        with self.assertRaises(SyncDeadline):
            setup.load()
        self.assertEqual(5.0, setup.clock.sleeps[0])

    def test_a_job_that_acknowledges_the_cancellation_and_keeps_running_fails_as_job_still_active_with_its_ID_recorded(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.outcomes["lab_data"] = "unkillable"
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertIn("job_still_active", str(raised.exception))
        self.assertIn("job 2", str(raised.exception))
        self.assertNotIn("was cancelled", str(raised.exception))
        self.assertEqual(2, RunState(setup.folder, RUN).jobs["lab_data"])
        self.assertNotIn("start_sync track_list", setup.airbyte.calls)
        self.assertLessEqual(setup.clock.now, 20.0 + 10.0 + 30.0 + 10.0, "the confirmation is bounded")
        self.assertEqual([], setup.records())

    def test_a_cancellation_that_cannot_be_asked_for_fails_as_job_still_active(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.cancel_fails = True
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertIn("job 1", str(raised.exception))

    def test_a_job_that_is_cancelled_is_a_sync_deadline_that_says_so(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        with self.assertRaises(SyncDeadline) as raised:
            setup.load()
        self.assertIn("was cancelled", str(raised.exception))
        self.assertNotIsInstance(raised.exception, JobStillActive)


@contextlib.contextmanager
def stopping(owner: Any, name: str, when: str) -> Iterator[None]:
    """The process stops right before or right after the call of ``owner.name``, which a test plants to interrupt a pair of steps."""
    original = getattr(owner, name)

    def stop(*arguments: Any, **keywords: Any) -> Any:
        if when == "before":
            raise Stopped(name)
        original(*arguments, **keywords)
        raise Stopped(name)

    with mock.patch.object(owner, name, stop):
        yield


def failed_first_try(setup: Setup) -> None:
    setup.airbyte.outcomes["lab_data"] = "failed"
    with contextlib.suppress(LoadError):
        setup.load()
    setup.airbyte.outcomes.clear()


class BranchOwnershipTest(unittest.TestCase):
    """A branch of a run is owned by the triple of its kind (a branch), its name and the hash that it was made at. The state file holds the name and the hash,
    written before the run uses the branch, and the catalog says the kind."""

    def test_a_branch_of_the_name_that_does_not_descend_from_the_recorded_creation_is_refused_and_not_deleted(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        del setup.catalog.heads[BRANCH], setup.catalog.tables[BRANCH]
        setup.catalog.make_branch(BRANCH, root="e" * 64)
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("not the branch that this run created", str(raised.exception))
        self.assertEqual([], setup.catalog.deleted)
        self.assertIn(BRANCH, setup.catalog.heads)

    def test_a_recorded_branch_that_moved_after_the_check_is_refused_by_the_hash_that_the_check_read_and_stays(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        setup.airbyte.on_ensure_connection = lambda table: setup.catalog.chains[BRANCH].append("f" * 64)
        with self.assertRaises(BranchMoved):
            setup.load()
        self.assertEqual([], setup.catalog.deleted)
        self.assertEqual("f" * 64, setup.catalog.heads[BRANCH])

    def test_the_ownership_of_the_branch_is_checked_before_the_first_copy_and_the_first_settings_change(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        del setup.catalog.heads[BRANCH], setup.catalog.tables[BRANCH]
        setup.catalog.make_branch(BRANCH, root="e" * 64)
        copies, calls = len(setup.store.copies), len(setup.airbyte.calls)
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual(copies, len(setup.store.copies))
        self.assertEqual([], [call for call in setup.airbyte.calls[calls:] if call.startswith(("ensure_", "start_sync"))])

    def test_a_branch_made_by_the_run_is_recorded_with_its_creation_hash_before_anything_else_uses_it(self) -> None:
        setup = Setup(self)
        setup.catalog.stop_after.add("create_branch")
        with self.assertRaises(Stopped):
            setup.load()
        self.assertEqual((BRANCH, BASE_HASH), RunState(setup.folder, RUN).branch)

    def test_a_recreation_records_the_new_pair_and_the_old_entry_is_gone_before_the_new_branch_exists(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        moved = setup.catalog.advance_main()
        setup.catalog.stop_before.add("create_branch")
        with self.assertRaises(Stopped):
            setup.load()
        self.assertNotIn(BRANCH, setup.catalog.heads)
        self.assertEqual((BRANCH, moved), RunState(setup.folder, RUN).branch, "the new pair is recorded before the branch is made")

    def test_a_creation_that_finds_the_name_taken_forgets_the_pair_it_had_just_recorded(self) -> None:
        setup = Setup(self)
        original = setup.catalog.create_branch

        def taken(branch: str, at: str) -> str:
            setup.catalog.make_branch(branch)
            return original(branch, at)

        with mock.patch.object(setup.catalog, "create_branch", taken), self.assertRaises(LoadError):
            setup.load()
        self.assertIsNone(RunState(setup.folder, RUN).branch, "a branch that this run did not make is not owned")
        self.assertEqual([], setup.catalog.deleted)

    def test_a_deletion_is_always_of_a_branch_that_the_state_file_named_at_the_hash_it_was_made_at(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        setup.catalog.advance_main()
        setup.load()
        self.assertEqual([BRANCH], setup.catalog.deleted)
        self.assertEqual([], setup.catalog.violations)


class InterruptionTest(unittest.TestCase):
    """A stop between two steps of the branch, the state file or the jobs leaves a run that a retry finishes or refuses, and nothing unowned is deleted."""

    STEPS = (
        ("after the branch delete", lambda setup: setup.catalog.stop_after.add("delete_branch")),
        ("before the entry is cleared", lambda setup: stopping(RunState, "forget_branch", "before")),
        ("after the entry is cleared", lambda setup: stopping(RunState, "forget_branch", "after")),
        ("before the new pair is recorded", lambda setup: stopping(RunState, "record_branch", "before")),
        ("after the new pair is recorded", lambda setup: stopping(RunState, "record_branch", "after")),
        ("before the branch is made", lambda setup: setup.catalog.stop_before.add("create_branch")),
        ("after the branch is made", lambda setup: setup.catalog.stop_after.add("create_branch")),
    )

    def interrupted(self, setup: Setup, install: Callable[[Setup], Any]) -> None:
        planted = install(setup)
        with planted if planted is not None else contextlib.nullcontext(), self.assertRaises(Stopped):
            setup.load()

    def test_a_retry_after_a_stop_at_any_step_of_the_branch_finishes_the_load_and_deletes_only_what_the_state_file_named(self) -> None:
        for name, install in self.STEPS:
            with self.subTest(name):
                setup = Setup(self)
                failed_first_try(setup)
                setup.catalog.advance_main()
                self.interrupted(setup, install)
                result = setup.load()
                self.assertEqual([], setup.catalog.violations)
                self.assertEqual((BRANCH, result.base_hash), RunState(setup.folder, RUN).branch)
                self.assertEqual(TABLES, sorted(result.snapshots))

    def test_two_stops_in_a_row_the_second_after_the_branch_is_made_do_not_let_the_third_try_delete_an_unrecorded_branch(self) -> None:
        for first, second in (("delete_branch", "create_branch"), ("create_branch", "delete_branch")):
            with self.subTest(f"{first} then {second}"):
                setup = Setup(self)
                failed_first_try(setup)
                setup.catalog.advance_main()
                setup.catalog.stop_after.add(first)
                with self.assertRaises(Stopped):
                    setup.load()
                setup.catalog.advance_main()
                setup.catalog.stop_after.add(second)
                with self.assertRaises(Stopped):
                    setup.load()
                result = setup.load()
                self.assertEqual([], setup.catalog.violations)
                self.assertEqual(TABLES, sorted(result.snapshots))

    def test_a_stop_between_the_start_of_a_sync_and_its_record_leaves_an_active_job_that_the_retry_refuses_until_it_ends(self) -> None:
        for name, install in (
            ("after the start", lambda setup: setup.airbyte.stop_after.add("start_sync clinical_data")),
            ("before the job is recorded", lambda setup: stopping(RunState, "record_job", "before")),
        ):
            with self.subTest(name):
                setup = Setup(self)
                self.interrupted(setup, install)
                setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
                setup.airbyte.running = [1]
                with self.assertRaises(LoadError) as raised:
                    setup.load()
                self.assertIn("already running", str(raised.exception))
                setup.airbyte.running = []
                self.assertEqual(TABLES, sorted(setup.load().snapshots))
                self.assertEqual([], setup.catalog.violations)

    def test_a_stop_after_the_last_sync_and_before_the_record_makes_the_retry_load_again_on_a_branch_it_owns(self) -> None:
        setup = Setup(self)
        with mock.patch("scripts.ingestion.record_load.write_record", side_effect=Stopped("write_record")), self.assertRaises(Stopped):
            setup.load()
        result = setup.load()
        self.assertEqual([BRANCH], setup.catalog.deleted)
        self.assertEqual([], setup.catalog.violations)
        self.assertEqual(TABLES, sorted(result.snapshots))

    def test_a_stop_between_the_record_and_the_removal_of_the_copies_leaves_a_record_and_the_retry_removes_the_copies(self) -> None:
        setup = Setup(self)
        with mock.patch("scripts.ingestion.record_load.remove_all", side_effect=Stopped("remove_all")), self.assertRaises(Stopped):
            setup.load()
        self.assertEqual(2, len(setup.store.list_versions(PREFIX)))
        setup.load()
        self.assertEqual([], setup.store.list_versions(PREFIX))


class OwnershipFirstTest(unittest.TestCase):
    """Whatever the run would touch and does not own is found before anything is copied, configured or synced."""

    def test_an_active_job_that_the_run_did_not_start_stops_the_load_before_any_copy_or_branch(self) -> None:
        setup = Setup(self)
        setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
        setup.airbyte.running = [7]
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], [call for call in setup.catalog.calls if call.startswith(("create_branch", "delete_branch"))])

    def test_an_active_job_that_the_run_did_start_is_refused_all_the_same(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
        setup.airbyte.running = [2]
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual([], setup.catalog.deleted)

    def test_a_stranger_version_at_a_read_key_stops_the_load_before_the_branch_is_deleted_or_a_setting_is_changed(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        setup.store.put_object(CLINICAL_KEY, b"stranger")
        calls = len(setup.airbyte.calls)
        with self.assertRaises(CopyError):
            setup.load()
        self.assertEqual([], setup.catalog.deleted)
        self.assertEqual([], [call for call in setup.airbyte.calls[calls:] if call.startswith(("ensure_", "start_sync"))])

    def test_when_both_a_read_key_and_the_branch_are_foreign_nothing_is_written(self) -> None:
        setup = Setup(self)
        setup.store.put_object(CLINICAL_KEY, b"stranger")
        setup.catalog.make_branch(BRANCH)
        with self.assertRaises((CopyError, LoadError)):
            setup.load()
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], setup.store.deletes)
        self.assertEqual([], setup.catalog.deleted)

    def test_a_job_list_that_is_not_in_the_documented_shape_refuses_the_load_before_any_write(self) -> None:
        setup = Setup(self)
        setup.airbyte.known = {table: f"connection_{table}" for table in TABLES}
        setup.airbyte.running_error = True
        with self.assertRaises(AirbyteError):
            setup.load()
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith(("ensure_", "start_sync"))])
        self.assertEqual([], [call for call in setup.catalog.calls if call.startswith(("create_branch", "delete_branch"))])


class ConfirmationBudgetTest(unittest.TestCase):
    """The wait for a job and the confirmation of its cancellation give each request the time that is left of their own budget."""

    def deadlines(self, setup: Setup) -> None:
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)

    def test_every_request_of_the_wait_and_of_the_confirmation_gets_what_is_left_of_its_own_budget(self) -> None:
        setup = Setup(self)
        self.deadlines(setup)
        setup.airbyte.outcomes["lab_data"] = "unkillable"
        setup.airbyte.latency = 3.0
        with self.assertRaises(JobStillActive):
            setup.load()
        calls = [entry for entry in setup.airbyte.timeouts if entry[0] in ("job_status", "cancel_job")]
        cancel_at = next(at for name, _, at in calls if name == "cancel_job")
        for name, timeout, at in calls:
            self.assertIsNotNone(timeout, name)
            if at < cancel_at:
                self.assertLessEqual(at + (timeout or 0), cancel_at + 1e-9, "a read of the wait ends by the sync's deadline")
            else:
                self.assertLessEqual(at + (timeout or 0), cancel_at + 30.0 + 1e-9, "a request of the confirmation ends by the confirmation's deadline")
        self.assertLessEqual(setup.clock.now, cancel_at + 30.0 + 3.0 + 1e-9, "the confirmation overshoots by at most one read")

    def test_a_confirmation_that_cannot_be_read_inside_its_budget_is_job_still_active_with_the_job_recorded(self) -> None:
        setup = Setup(self)
        self.deadlines(setup)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.unreadable_after_cancel = True
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertIn("job_still_active", str(raised.exception))
        self.assertIn("job 1", str(raised.exception))
        self.assertEqual(1, RunState(setup.folder, RUN).jobs["clinical_data"])
        self.assertNotIn("start_sync lab_data", setup.airbyte.calls)
        self.assertEqual([], setup.records())

    def test_a_status_that_cannot_be_read_in_the_wait_cancels_the_job_and_stops_the_load_without_a_deadline_message(self) -> None:
        setup = Setup(self)
        self.deadlines(setup)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.unreadable = 1
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertNotIsInstance(raised.exception, (SyncDeadline, JobStillActive))
        self.assertIn("could not be read", str(raised.exception))
        self.assertEqual([1], setup.airbyte.cancelled)
        self.assertNotIn("start_sync lab_data", setup.airbyte.calls)

    def test_a_status_that_can_never_be_read_is_job_still_active(self) -> None:
        setup = Setup(self)
        self.deadlines(setup)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.unreadable = 10**6
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertIn("job 1", str(raised.exception))
        self.assertLessEqual(setup.clock.now, 30.0 + 10.0 + 1e-9, "the wait for the confirmation is bounded")

    def test_a_cancellation_that_failed_while_the_job_ended_by_itself_is_not_said_to_have_been_asked_for(self) -> None:
        setup = Setup(self)
        self.deadlines(setup)
        setup.airbyte.outcomes["clinical_data"] = "failed"
        setup.airbyte.polls = 1
        setup.airbyte.cancel_fails = True
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("failed", str(raised.exception))
        self.assertEqual([], setup.airbyte.cancelled)


class StatusTest(unittest.TestCase):
    """A status that is not one of the documented ones is never copied into a message and never taken for a job that has ended."""

    MARKER = "marker_status_7e2d1a_never_in_a_message"

    def test_a_status_outside_the_documented_set_is_not_a_success_not_an_end_and_not_shown(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.status_text = self.MARKER
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertNotIn(self.MARKER, str(raised.exception))
        self.assertNotIn("start_sync lab_data", setup.airbyte.calls)
        self.assertEqual([], setup.records())
        cancelled_at = next(at for name, _, at in setup.airbyte.timeouts if name == "cancel_job")
        self.assertEqual(0.0, cancelled_at, "the job was cancelled at once, not after the sync's whole deadline")
        self.assertLessEqual(setup.clock.now, 30.0 + 10.0 + 1e-9)

    def test_a_job_that_is_pending_or_queued_is_waited_for_like_a_running_one(self) -> None:
        for status in ("pending", "queued", "running"):
            with self.subTest(status):
                setup = Setup(self)
                setup.airbyte.polls = 3
                setup.airbyte.polls_as = status
                self.assertEqual(TABLES, sorted(setup.load().snapshots))
                self.assertEqual([], setup.airbyte.cancelled)

    def test_the_four_ends_are_each_decided_as_the_documentation_says_and_only_success_goes_on(self) -> None:
        for status, goes_on in (("succeeded", True), ("failed", False), ("cancelled", False), ("incomplete", False)):
            with self.subTest(status):
                setup = Setup(self)
                setup.airbyte.outcomes["clinical_data"] = status
                if goes_on:
                    setup.load()
                else:
                    with self.assertRaises(LoadError):
                        setup.load()
                    self.assertNotIn("start_sync lab_data", setup.airbyte.calls)


class TagOfTheNameTest(unittest.TestCase):
    """Ownership is the triple of kind, name and creation hash: a tag of the run's branch name is never a leftover branch, whatever hash it is at."""

    def retry_over_a_tag(self, setup: Setup, *, recorded: bool = True) -> LoadError:
        failed_first_try(setup)
        creation = RunState(setup.folder, RUN).branch
        self.assertIsNotNone(creation)
        del setup.catalog.heads[BRANCH], setup.catalog.tables[BRANCH]
        setup.catalog.make_tag(BRANCH, at=creation[1] if creation else BASE_HASH)
        if not recorded:
            (setup.folder / f"{RUN}.state.json").unlink()
        copies, calls = len(setup.store.copies), len(setup.airbyte.calls)
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertEqual(copies, len(setup.store.copies), "the refusal came before any copy")
        self.assertEqual([], [call for call in setup.airbyte.calls[calls:] if call.startswith(("ensure_", "start_sync"))])
        return raised.exception

    def test_a_tag_of_the_name_at_the_recorded_creation_hash_is_refused_and_no_delete_is_sent(self) -> None:
        setup = Setup(self)
        error = self.retry_over_a_tag(setup)
        self.assertIn("not a branch", str(error))
        self.assertEqual([], setup.catalog.deleted)
        self.assertEqual([], setup.catalog.typed_misses, "not even a delete that the typed call would have missed was sent")
        self.assertNotIn(f"delete_branch {BRANCH}", setup.catalog.calls)
        self.assertEqual("TAG", setup.catalog.kinds[BRANCH])
        self.assertIn(BRANCH, setup.catalog.heads)

    def test_a_tag_of_the_name_with_no_state_record_is_refused_too(self) -> None:
        setup = Setup(self)
        self.retry_over_a_tag(setup, recorded=False)
        self.assertEqual([], setup.catalog.deleted)
        self.assertNotIn(f"delete_branch {BRANCH}", setup.catalog.calls)

    def test_a_finished_run_whose_branch_name_is_now_a_tag_at_the_recorded_head_is_refused(self) -> None:
        setup = Setup(self)
        setup.load()
        setup.catalog.kinds[BRANCH] = "TAG"
        with self.assertRaises(LoadError):
            setup.load()

    def test_control_a_branch_of_the_name_at_the_recorded_creation_is_still_the_run_s_leftover(self) -> None:
        setup = Setup(self)
        failed_first_try(setup)
        self.assertEqual(TABLES, sorted(setup.load().snapshots))
        self.assertEqual([BRANCH], setup.catalog.deleted)


class LateConfirmationTest(unittest.TestCase):
    """An answer that comes back after the confirmation's budget is not a confirmation, whatever it says."""

    def setup_with(self, confirm: float, latency: float) -> Setup:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=confirm)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.latency = latency
        return setup

    def test_a_status_that_says_cancelled_but_arrives_after_the_budget_is_job_still_active_with_the_job_recorded(self) -> None:
        setup = self.setup_with(1.0, 1.15)
        with self.assertRaises(JobStillActive) as raised:
            setup.load()
        self.assertNotIsInstance(raised.exception, SyncDeadline)
        self.assertIn("job 1", str(raised.exception))
        self.assertEqual(1, RunState(setup.folder, RUN).jobs["clinical_data"])
        self.assertNotIn("start_sync lab_data", setup.airbyte.calls)

    def test_control_the_same_answer_inside_the_budget_confirms_the_cancellation(self) -> None:
        setup = self.setup_with(1.0, 0.5)
        with self.assertRaises(SyncDeadline) as raised:
            setup.load()
        self.assertIn("was cancelled", str(raised.exception))

    def test_an_answer_that_ends_exactly_at_the_budget_is_inside_it(self) -> None:
        setup = self.setup_with(1.0, 1.0)
        with self.assertRaises(SyncDeadline):
            setup.load()


class StartedJobTest(unittest.TestCase):
    """The job ID is recorded before anything else is read about the job, and a start that gives no ID names what cannot be done."""

    def test_the_job_is_in_the_state_file_when_its_first_status_is_read(self) -> None:
        setup = Setup(self)
        seen: list[dict[str, int]] = []
        setup.airbyte.on_status = lambda: seen.append(dict(RunState(setup.folder, RUN).jobs))
        setup.load()
        self.assertEqual({"clinical_data": 1}, seen[0])

    def test_a_job_whose_status_cannot_be_read_is_recorded_and_goes_through_the_bounded_cancellation(self) -> None:
        setup = Setup(self)
        setup.deadlines = Deadlines(sync_seconds=20.0, poll_seconds=10.0, cancel_confirm_seconds=30.0)
        setup.airbyte.outcomes["clinical_data"] = "stuck"
        setup.airbyte.unreadable = 1
        with self.assertRaises(LoadError):
            setup.load()
        self.assertEqual({"clinical_data": 1}, RunState(setup.folder, RUN).jobs)
        self.assertEqual([1], setup.airbyte.cancelled)

    def test_a_start_with_no_job_ID_stops_the_load_with_nothing_recorded_and_nothing_to_cancel(self) -> None:
        setup = Setup(self)
        setup.airbyte.start_error = True
        with self.assertRaises(AirbyteError) as raised:
            setup.load()
        self.assertIn("cannot be named", str(raised.exception))
        self.assertEqual({}, RunState(setup.folder, RUN).jobs)
        self.assertEqual([], setup.airbyte.cancelled)
        self.assertEqual([], setup.records())


class ActiveJobAtStartTest(unittest.TestCase):
    def test_a_job_on_any_of_the_three_connections_stops_the_load_before_any_configuration_change(self) -> None:
        for table in TABLES:
            with self.subTest(table):
                setup = Setup(self)
                setup.airbyte.known = {name: f"connection_{name}" for name in TABLES}
                setup.airbyte.running_for = {f"connection_{table}": [9]}
                with self.assertRaises(LoadError):
                    setup.load()
                self.assertEqual([], [call for call in setup.airbyte.calls if call.startswith("ensure_")])
                self.assertEqual([], setup.store.copies)

    def test_the_check_runs_after_the_gate_and_the_manifest_skip_and_before_the_copy(self) -> None:
        setup = Setup(self)
        setup.airbyte.known = {name: f"connection_{name}" for name in TABLES}
        setup.airbyte.running = [9]
        setup.store.put_object(setup.landed.keys["lab_data.csv"], b"caseid\n9\n")
        with self.assertRaises(GateRefused):
            setup.load()
        self.assertEqual([], setup.airbyte.calls)


class AbsenceTest(unittest.TestCase):
    def test_an_older_merged_manifest_is_found_among_all_the_markers_and_returns_the_earlier_record(self) -> None:
        setup = Setup(self)
        earlier = write_earlier(setup)
        setup.catalog.recorded = {"0" * 64, setup.reference.sha256}
        self.assertEqual(earlier, setup.load())

    def test_a_history_that_cannot_prove_absence_refuses_the_load_before_anything_is_copied_or_started(self) -> None:
        setup = Setup(self)
        setup.catalog.cannot_establish_absence = True
        with self.assertRaises(LoadError) as raised:
            setup.load()
        self.assertIn("cannot establish absence", str(raised.exception))
        self.assertEqual([], setup.store.copies)
        self.assertEqual([], setup.airbyte.calls)

    def test_a_manifest_that_is_found_is_returned_even_when_the_rest_of_the_history_is_too_long(self) -> None:
        setup = Setup(self)
        earlier = write_earlier(setup)
        setup.catalog.cannot_establish_absence = True
        setup.catalog.recorded = {setup.reference.sha256}
        self.assertEqual(earlier, setup.load())


if __name__ == "__main__":
    unittest.main()
