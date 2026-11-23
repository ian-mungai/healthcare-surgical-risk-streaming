"""The copy of the object versions that the manifest binds, to the run's own read keys, for the record connections.

A read key holds the verified content hash and the run ID, ``records_input/<run ID>/<SHA-256>/<table>.csv``, so a key is unique to its content and its run. The
gate runs first, so a manifest that is missing or edited, a missing version or a substituted version is refused before any copy is made. Each copy is checked
against the manifest's SHA-256 and its version ID goes into the run's ledger before the check, so a retry deletes only a version that the run itself wrote. A
version at a read key that the ledger does not name is refused and never deleted. After the last sync the keys are checked for exactly the one recorded version.
"""

from __future__ import annotations

import hashlib
import unittest
from collections.abc import Callable
from typing import Any

from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.record_copy import (
    Copied,
    CopyChanged,
    CopyError,
    S3CopyStore,
    check_read_keys_owned,
    confirm_copies,
    copy_bound_versions,
    remove_copies,
    verify_read_keys,
)
from scripts.ingestion.store import ObjectVersion, StoreError
from tests.fast.ingestion_support import SOURCE_PREFIX, list_text_for, pin_the_synthetic_list, synthetic_pin
from tests.fast.memory_store import Stored
from tests.fast.record_support import CLINICAL, LAB, RELEASE_FILES, RUN, CopyingMemoryStore, MemoryLedger, RecordLanded, landed_records, source_key

CLINICAL_KEY = f"records_input/{RUN}/{hashlib.sha256(CLINICAL).hexdigest()}/clinical_data.csv"
LAB_KEY = f"records_input/{RUN}/{hashlib.sha256(LAB).hexdigest()}/lab_data.csv"
PREFIX = f"records_input/{RUN}/"


def setUpModule() -> None:
    pin_the_synthetic_list(list_text_for(RELEASE_FILES).encode())


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def current(store: CopyingMemoryStore, key: str) -> bytes:
    return next(item.data for item in store.history[key] if item.version.is_latest)


def insert_older(store: CopyingMemoryStore, key: str, version_id: str) -> None:
    """An older version of ``key`` that is not current, as a store holds one when a newer version was written after it."""
    store.history[key].insert(0, Stored(ObjectVersion(key, version_id, 3, "2025-01-01T00:00:00Z", False, False), b"old"))


def copy(landed: RecordLanded, reference: ManifestRef, ledger: MemoryLedger | None = None) -> tuple[Copied, ...]:
    return copy_bound_versions(landed.store, reference, RUN, ledger or MemoryLedger())


class CopyTest(unittest.TestCase):
    def test_control_each_bound_version_is_copied_by_its_version_ID_to_a_key_that_holds_its_hash_and_is_recorded(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copied = copy(landed, reference, ledger)
        self.assertEqual(["clinical_data", "lab_data"], [item.table for item in copied])
        self.assertEqual([CLINICAL_KEY, LAB_KEY], [item.key for item in copied])
        self.assertEqual({CLINICAL_KEY, LAB_KEY}, {key for key in landed.store.history if key.startswith("records_input/")})
        self.assertEqual(CLINICAL, current(landed.store, CLINICAL_KEY))
        self.assertEqual(LAB, current(landed.store, LAB_KEY))
        self.assertEqual(
            [
                (landed.keys["clinical_data.csv"], landed.versions["clinical_data.csv"].version_id),
                (landed.keys["lab_data.csv"], landed.versions["lab_data.csv"].version_id),
            ],
            [(source, version) for source, version, _ in landed.store.copies],
        )
        self.assertEqual([sha(CLINICAL), sha(LAB)], [item.sha256 for item in copied])
        self.assertEqual({item.key: {item.version_id} for item in copied}, {key: set(ledger.versions(key)) for key in (CLINICAL_KEY, LAB_KEY)})

    def test_only_the_two_tables_are_copied_and_no_other_landed_payload_is_read(self) -> None:
        landed, reference = landed_records()
        landed.store.reads.clear()
        copy(landed, reference)
        vital = landed.keys["vital_files/0001.vital"]
        self.assertEqual([], [read for read in landed.store.reads if read.key == vital])
        self.assertTrue(all(source != vital for source, _, _ in landed.store.copies))

    def test_a_version_substituted_after_the_manifest_is_refused_before_any_copy(self) -> None:
        landed, reference = landed_records()
        writes = len(landed.store.writes)
        landed.store.put_object(landed.keys["clinical_data.csv"], b"caseid,subjectid\n9,9\n")
        with self.assertRaises(GateRefused):
            copy(landed, reference)
        self.assertEqual([], landed.store.copies)
        self.assertEqual(writes + 1, len(landed.store.writes), "nothing was written but the substituted version")

    def test_a_missing_version_of_either_table_is_refused_before_any_copy(self) -> None:
        for name in ("clinical_data.csv", "lab_data.csv"):
            with self.subTest(name):
                landed, reference = landed_records()
                landed.store.put_delete_marker(landed.keys[name])
                with self.assertRaises(GateRefused):
                    copy(landed, reference)
                self.assertEqual([], landed.store.copies)

    def test_a_manifest_that_does_not_exist_is_refused_before_any_copy(self) -> None:
        """A failed check publishes no manifest, so the record load has no reference to read: a made-up reference stands for that."""
        landed, reference = landed_records()
        absent = ManifestRef(reference.bucket, f"manifests/probe_set/{'0' * 64}.json", "version9999", "0" * 64)
        with self.assertRaises(GateRefused):
            copy(landed, absent)
        self.assertEqual([], landed.store.copies)

    def test_a_manifest_that_does_not_bind_a_table_is_refused_before_any_copy(self) -> None:
        without_lab = {key: data for key, data in RELEASE_FILES.items() if key != "lab_data.csv"}
        with synthetic_pin(list_text_for(without_lab).encode()):
            landed, reference = landed_records(without_lab)
            with self.assertRaises(GateRefused):
                copy(landed, reference)
        self.assertEqual([], landed.store.copies)

    def test_a_copy_whose_hash_differs_from_the_manifest_is_refused_and_names_the_table_only(self) -> None:
        landed, reference = landed_records()
        landed.store.corrupt.add(landed.keys["lab_data.csv"])
        with self.assertRaises(CopyError) as raised:
            copy(landed, reference)
        self.assertIn("lab_data", str(raised.exception))
        self.assertNotIn(sha(LAB), str(raised.exception))

    def test_a_copy_with_the_same_length_and_other_bytes_is_refused_by_its_hash_alone(self) -> None:
        landed, reference = landed_records()
        landed.store.corrupt_same_length.add(landed.keys["clinical_data.csv"])
        with self.assertRaises(CopyError) as raised:
            copy(landed, reference)
        self.assertIn("clinical_data", str(raised.exception))
        written = current(landed.store, CLINICAL_KEY)
        self.assertEqual(len(CLINICAL), len(written), "the size matches, so only the hash can refuse it")
        self.assertNotEqual(CLINICAL, written)

    def test_a_second_call_copies_and_writes_nothing(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        first = copy(landed, reference, ledger)
        copies, writes = list(landed.store.copies), list(landed.store.writes)
        second = copy(landed, reference, ledger)
        self.assertEqual(first, second)
        self.assertEqual((copies, writes), (landed.store.copies, landed.store.writes))

    def test_a_copy_that_this_run_wrote_wrong_is_deleted_by_its_recorded_version_ID_and_replaced(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        landed.store.corrupt.add(landed.keys["lab_data.csv"])
        with self.assertRaises(CopyError):
            copy(landed, reference, ledger)
        wrong = next(iter(ledger.versions(LAB_KEY)))
        landed.store.corrupt.clear()
        copied = copy(landed, reference, ledger)
        self.assertEqual(LAB, current(landed.store, LAB_KEY))
        self.assertEqual(sha(LAB), copied[1].sha256)
        self.assertIn((LAB_KEY, wrong), landed.store.deletes)
        self.assertEqual(1, len(landed.store.list_versions(LAB_KEY)), "the wrong copy is gone, so exactly one version holds the key")
        self.assertEqual({copied[1].version_id}, set(ledger.versions(LAB_KEY)))

    def test_a_recorded_older_version_beside_a_good_current_copy_is_deleted_by_its_recorded_ID_and_the_good_copy_is_kept(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        first = copy(landed, reference, ledger)
        insert_older(landed.store, CLINICAL_KEY, "older1")
        ledger.add(CLINICAL_KEY, "older1")
        landed.store.copies.clear()
        again = copy(landed, reference, ledger)
        self.assertEqual(first, again)
        self.assertEqual([], landed.store.copies, "the good copy was kept, not copied again")
        self.assertIn((CLINICAL_KEY, "older1"), landed.store.deletes)
        self.assertEqual(1, len(landed.store.list_versions(CLINICAL_KEY)))
        self.assertEqual({first[0].version_id}, set(ledger.versions(CLINICAL_KEY)))

    def test_a_version_at_a_read_key_that_the_run_did_not_record_is_refused_and_never_deleted(self) -> None:
        makers: tuple[tuple[str, Callable[[CopyingMemoryStore], object]], ...] = (
            ("a new version with the size of the table", lambda store: store.put_object(CLINICAL_KEY, b"x" * len(CLINICAL))),
            ("a version with the right bytes", lambda store: store.put_object(CLINICAL_KEY, CLINICAL)),
            ("a delete marker", lambda store: store.put_delete_marker(CLINICAL_KEY)),
        )
        for name, make in makers:
            with self.subTest(name):
                landed, reference = landed_records()
                ledger = MemoryLedger()
                make(landed.store)
                with self.assertRaises(CopyError) as raised:
                    copy(landed, reference, ledger)
                self.assertIn("not recorded", str(raised.exception))
                self.assertEqual([], landed.store.copies)
                self.assertEqual([], landed.store.deletes)
                self.assertEqual(1, len(landed.store.list_versions(CLINICAL_KEY)))

    def test_an_unrecorded_version_beside_a_recorded_one_is_refused_on_a_retry_too(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copy(landed, reference, ledger)
        landed.store.put_object(CLINICAL_KEY, b"substituted")
        with self.assertRaises(CopyError):
            copy(landed, reference, ledger)
        self.assertEqual([], landed.store.deletes)

    def test_an_object_in_the_read_prefix_that_is_not_a_table_is_refused(self) -> None:
        landed, reference = landed_records()
        landed.store.put_object(PREFIX + "extra.csv", b"caseid\n7\n")
        with self.assertRaises(CopyError):
            copy(landed, reference)
        self.assertEqual([], landed.store.copies)

    def test_a_run_ID_that_is_not_a_plain_name_is_refused_before_the_gate_runs(self) -> None:
        landed, reference = landed_records()
        landed.store.reads.clear()
        with self.assertRaises(ValueError):
            copy_bound_versions(landed.store, reference, "../other", MemoryLedger())
        self.assertEqual([], landed.store.reads)


class ConfirmTest(unittest.TestCase):
    def copied(self) -> tuple[RecordLanded, list[Copied]]:
        landed, reference = landed_records()
        return landed, list(copy(landed, reference))

    def test_control_the_copies_as_recorded_are_confirmed_and_nothing_is_read(self) -> None:
        landed, copied = self.copied()
        landed.store.reads.clear()
        confirm_copies(landed.store, RUN, copied)
        self.assertEqual([], landed.store.reads)

    def test_a_new_version_of_a_copy_is_refused(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(CLINICAL_KEY, b"caseid,subjectid\n9,9\n")
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)

    def test_a_deleted_copy_and_an_added_object_are_each_refused(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(PREFIX + "added.csv", b"x")
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)
        landed, copied = self.copied()
        landed.store.put_delete_marker(LAB_KEY)
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)

    def test_an_empty_record_of_copies_is_refused(self) -> None:
        landed, _ = self.copied()
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, [])


class VerifyReadKeysTest(unittest.TestCase):
    """After the last sync each read key must hold exactly the one version that the run wrote: a version that was current while Airbyte read, or that was
    restored afterwards, leaves a second entry."""

    def copied(self) -> tuple[RecordLanded, list[Copied]]:
        landed, reference = landed_records()
        return landed, list(copy(landed, reference))

    def test_control_exactly_the_recorded_versions_pass(self) -> None:
        landed, copied = self.copied()
        verify_read_keys(landed.store, copied)

    def test_a_substituted_version_that_is_still_current_is_refused_as_copy_changed(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(CLINICAL_KEY, b"substituted")
        with self.assertRaises(CopyChanged) as raised:
            verify_read_keys(landed.store, copied)
        self.assertIn("copy_changed", str(raised.exception))
        self.assertIn("clinical_data", str(raised.exception))

    def test_a_substituted_version_followed_by_the_original_bytes_again_is_refused_although_the_bytes_are_right(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(LAB_KEY, b"substituted")
        landed.store.put_object(LAB_KEY, LAB)
        self.assertEqual(LAB, current(landed.store, LAB_KEY))
        with self.assertRaises(CopyChanged):
            verify_read_keys(landed.store, copied)

    def test_an_older_version_that_is_not_current_is_refused_too(self) -> None:
        landed, copied = self.copied()
        insert_older(landed.store, CLINICAL_KEY, "older1")
        self.assertEqual(copied[0].version_id, next(item.version.version_id for item in landed.store.history[CLINICAL_KEY] if item.version.is_latest))
        with self.assertRaises(CopyChanged):
            verify_read_keys(landed.store, copied)

    def test_a_delete_marker_and_a_missing_version_are_refused(self) -> None:
        landed, copied = self.copied()
        landed.store.put_delete_marker(CLINICAL_KEY)
        with self.assertRaises(CopyChanged):
            verify_read_keys(landed.store, copied)
        landed, copied = self.copied()
        landed.store.remove_version(LAB_KEY, copied[1].version_id)
        with self.assertRaises(CopyChanged):
            verify_read_keys(landed.store, copied)

    def test_an_empty_record_of_copies_is_refused(self) -> None:
        landed, _ = self.copied()
        with self.assertRaises(CopyChanged):
            verify_read_keys(landed.store, [])


class RemoveTest(unittest.TestCase):
    def test_only_the_versions_that_the_ledger_names_are_deleted_and_each_by_its_version_ID(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        landed.store.corrupt.add(landed.keys["lab_data.csv"])
        with self.assertRaises(CopyError):
            copy(landed, reference, ledger)
        landed.store.corrupt.clear()
        copied = copy(landed, reference, ledger)
        recorded = {(item.key, item.version_id) for item in copied}
        stranger = landed.store.put_object(CLINICAL_KEY, b"stranger")
        landed.store.deletes.clear()
        self.assertEqual(len(recorded), remove_copies(landed.store, RUN, ledger))
        self.assertEqual(recorded, set(landed.store.deletes))
        self.assertEqual([(stranger.key, stranger.version_id)], [(version.key, version.version_id) for version in landed.store.list_versions(CLINICAL_KEY)])
        self.assertTrue(all(not ledger.versions(key) for key in (CLINICAL_KEY, LAB_KEY)))

    def test_a_second_call_removes_nothing_and_a_run_that_recorded_nothing_removes_nothing(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copy(landed, reference, ledger)
        remove_copies(landed.store, RUN, ledger)
        deletes = list(landed.store.deletes)
        self.assertEqual(0, remove_copies(landed.store, RUN, ledger))
        self.assertEqual(0, remove_copies(landed.store, "run_never", MemoryLedger()))
        self.assertEqual(deletes, landed.store.deletes)

    def test_without_a_ledger_nothing_in_the_prefix_is_deleted_even_the_run_s_own_copies(self) -> None:
        landed, reference = landed_records()
        copy(landed, reference)
        self.assertEqual(0, remove_copies(landed.store, RUN, MemoryLedger()))
        self.assertEqual([], landed.store.deletes)

    def test_the_landed_payloads_and_the_manifest_are_never_touched(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copy(landed, reference, ledger)
        remove_copies(landed.store, RUN, ledger)
        for path, key in landed.keys.items():
            self.assertEqual(RELEASE_FILES[path], current(landed.store, key))
        self.assertEqual(1, len(landed.store.list_versions(reference.key)))

    def test_a_run_ID_that_is_not_a_plain_name_is_refused(self) -> None:
        landed, _ = landed_records()
        with self.assertRaises(ValueError):
            remove_copies(landed.store, "../x", MemoryLedger())


class HiddenListingStore(CopyingMemoryStore):
    """A store whose listing leaves out what it holds, as a page that stopped early or a listing that is behind does."""

    def __init__(self) -> None:
        super().__init__()
        self.hidden = False

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        return [] if self.hidden else super().list_versions(prefix)


class FailingOneStore(CopyingMemoryStore):
    """A store whose delete of one version fails, as a call that ran out of its time does."""

    def __init__(self) -> None:
        super().__init__()
        self.refuse: set[str] = set()

    def delete_version(self, key: str, version_id: str) -> None:
        if version_id in self.refuse:
            raise StoreError(Notice(Code.VERSION_MISSING))
        super().delete_version(key, version_id)


class ExactDeleteTest(unittest.TestCase):
    """A version is forgotten only after the delete of exactly that version succeeded. A listing is never the proof that it is gone."""

    def test_a_recorded_version_that_the_listing_does_not_show_is_deleted_by_its_exact_ID_and_then_forgotten(self) -> None:
        store = HiddenListingStore()
        _, reference = landed_records(store=store)
        ledger = MemoryLedger()
        copied = copy_bound_versions(store, reference, RUN, ledger)
        recorded = {(item.key, item.version_id) for item in copied}
        store.hidden = True
        store.deletes.clear()
        self.assertEqual(len(recorded), remove_copies(store, RUN, ledger))
        self.assertEqual(recorded, set(store.deletes), "each recorded version was deleted by its key and version ID though the listing showed nothing")
        self.assertEqual(set(), set(ledger.keys()))
        store.hidden = False
        self.assertEqual([], store.list_versions(PREFIX))

    def test_a_delete_that_fails_keeps_the_version_in_the_ledger_and_the_versions_that_were_deleted_are_forgotten(self) -> None:
        store = FailingOneStore()
        _, reference = landed_records(store=store)
        ledger = MemoryLedger()
        copied = copy_bound_versions(store, reference, RUN, ledger)
        failing, other = copied[1], copied[0]
        store.refuse = {failing.version_id}
        with self.assertRaises(StoreError):
            remove_copies(store, RUN, ledger)
        self.assertEqual(frozenset({failing.version_id}), ledger.versions(failing.key), "the version whose delete failed is still owned")
        self.assertEqual(frozenset(), ledger.versions(other.key), "the version whose delete succeeded is forgotten")
        store.refuse = set()
        self.assertEqual(1, remove_copies(store, RUN, ledger))
        self.assertEqual(set(), set(ledger.keys()))

    def test_a_version_that_is_already_gone_is_forgotten_because_its_exact_delete_succeeded(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copied = copy(landed, reference, ledger)
        landed.store.remove_version(copied[0].key, copied[0].version_id)
        landed.store.deletes.clear()
        self.assertEqual(2, remove_copies(landed.store, RUN, ledger))
        self.assertEqual({(item.key, item.version_id) for item in copied}, set(landed.store.deletes))
        self.assertEqual(set(), set(ledger.keys()))

    def test_a_recorded_version_that_is_outside_the_read_prefix_is_not_deleted_or_forgotten(self) -> None:
        landed, _ = landed_records()
        ledger = MemoryLedger()
        ledger.add("somewhere/else.csv", "v1")
        self.assertEqual(0, remove_copies(landed.store, RUN, ledger))
        self.assertEqual([], landed.store.deletes)
        self.assertEqual(frozenset({"v1"}), ledger.versions("somewhere/else.csv"))


class OwnedReadKeysTest(unittest.TestCase):
    """Before anything is copied the run finds a version in its read prefix that it did not record, and deletes nothing."""

    def test_control_a_prefix_that_holds_only_recorded_versions_or_nothing_passes(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        check_read_keys_owned(landed.store, RUN, ledger)
        copy(landed, reference, ledger)
        check_read_keys_owned(landed.store, RUN, ledger)

    def test_a_version_that_the_ledger_does_not_name_is_refused_with_a_message_that_names_no_key_and_nothing_is_deleted(self) -> None:
        landed, reference = landed_records()
        ledger = MemoryLedger()
        copy(landed, reference, ledger)
        landed.store.put_object(CLINICAL_KEY, b"stranger")
        with self.assertRaises(CopyError) as raised:
            check_read_keys_owned(landed.store, RUN, ledger)
        self.assertIn("not recorded", str(raised.exception))
        self.assertNotIn("clinical_data", str(raised.exception))
        self.assertEqual([], landed.store.deletes)

    def test_an_object_of_another_name_in_the_prefix_and_a_delete_marker_are_refused_too(self) -> None:
        landed, _ = landed_records()
        landed.store.put_object(PREFIX + "other.csv", b"x")
        with self.assertRaises(CopyError):
            check_read_keys_owned(landed.store, RUN, MemoryLedger())
        other, _ = landed_records()
        other.store.put_delete_marker(CLINICAL_KEY)
        with self.assertRaises(CopyError):
            check_read_keys_owned(other.store, RUN, MemoryLedger())


class FakeAws:
    def __init__(self, replies: list[Any]) -> None:
        self.replies = replies
        self.calls: list[tuple[str, ...]] = []

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = 0) -> Any:
        self.calls.append(arguments)
        return self.replies.pop(0)


class S3CopyTest(unittest.TestCase):
    """The AWS call that makes the copy: the version ID is in the copy source, and the reply must name that same source version."""

    def version(self, landed: RecordLanded) -> Any:
        return landed.versions["clinical_data.csv"]

    def store(self, replies: list[Any]) -> tuple[S3CopyStore, FakeAws]:
        aws = FakeAws(replies)
        return S3CopyStore(aws, "example-bucket"), aws

    def listing(self, key: str, version_id: str) -> dict[str, Any]:
        return {"Versions": [{"Key": key, "VersionId": version_id, "IsLatest": True, "LastModified": "2026-11-22T00:00:00+00:00", "Size": 5}]}

    def test_control_the_copy_names_the_source_version_and_returns_the_new_version(self) -> None:
        landed, _ = landed_records()
        version = self.version(landed)
        store, aws = self.store([{"VersionId": "new1", "CopySourceVersionId": version.version_id}, self.listing(PREFIX + "clinical_data.csv", "new1")])
        copy = store.copy_version(version, PREFIX + "clinical_data.csv")
        self.assertEqual(("new1", PREFIX + "clinical_data.csv"), (copy.version_id, copy.key))
        arguments = aws.calls[0]
        self.assertEqual(("s3api", "copy-object"), arguments[:2])
        self.assertIn(f"example-bucket/{version.key}?versionId={version.version_id}", arguments)
        self.assertEqual(PREFIX + "clinical_data.csv", arguments[arguments.index("--key") + 1])

    def test_a_reply_for_another_source_version_or_with_no_new_version_is_refused(self) -> None:
        landed, _ = landed_records()
        version = self.version(landed)
        for name, reply in (
            ("other source version", {"VersionId": "new1", "CopySourceVersionId": "other"}),
            ("no source version", {"VersionId": "new1"}),
            ("no new version", {"CopySourceVersionId": version.version_id}),
            ("null new version", {"VersionId": "null", "CopySourceVersionId": version.version_id}),
            ("not an object", ["x"]),
        ):
            with self.subTest(name), self.assertRaises(CopyError):
                self.store([reply])[0].copy_version(version, PREFIX + "clinical_data.csv")

    def test_a_missing_source_version_is_refused(self) -> None:
        landed, _ = landed_records()
        with self.assertRaises(CopyError):
            self.store([None])[0].copy_version(self.version(landed), PREFIX + "clinical_data.csv")

    def test_the_removal_names_the_version_and_the_reply_must_name_the_same_version(self) -> None:
        landed, _ = landed_records()
        version = self.version(landed)
        store, aws = self.store([{"VersionId": version.version_id}])
        store.delete_version(version.key, version.version_id)
        arguments = aws.calls[0]
        self.assertEqual(("s3api", "delete-object"), arguments[:2])
        self.assertEqual((version.key, version.version_id), (arguments[arguments.index("--key") + 1], arguments[arguments.index("--version-id") + 1]))
        for name, reply in (("other version", {"VersionId": "other"}), ("no version", {}), ("not an object", ["x"])):
            with self.subTest(name), self.assertRaises(CopyError):
                self.store([reply])[0].delete_version(version.key, version.version_id)

    def test_a_version_that_is_already_gone_is_not_an_error(self) -> None:
        landed, _ = landed_records()
        version = self.version(landed)
        self.store([None])[0].delete_version(version.key, version.version_id)


class ReleasePrefixTest(unittest.TestCase):
    def test_the_test_release_uses_the_pinned_release_path(self) -> None:
        self.assertEqual("vitaldb/1.0.0/clinical_data.csv", source_key("clinical_data.csv"))
        self.assertTrue(source_key("lab_data.csv").startswith(SOURCE_PREFIX))


if __name__ == "__main__":
    unittest.main()
