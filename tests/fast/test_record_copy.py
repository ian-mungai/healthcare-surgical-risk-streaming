"""The copy of the object versions that the manifest binds, to the run's own read prefix, for the record connections.

The gate runs first, so a manifest that is missing or edited, a missing version or a substituted version is refused before any copy is made. Each copy is
checked against the manifest's SHA-256, and a second call copies nothing. The stand-in store records each copy and each write.
"""

from __future__ import annotations

import hashlib
import unittest
from typing import Any

from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import Copied, CopyError, S3CopyStore, confirm_copies, copy_bound_versions
from tests.fast.ingestion_support import SOURCE_PREFIX, list_text_for, pin_the_synthetic_list, synthetic_pin
from tests.fast.record_support import CLINICAL, LAB, RELEASE_FILES, RUN, CopyingMemoryStore, RecordLanded, landed_records, source_key

PREFIX = f"records_input/{RUN}/"


def setUpModule() -> None:
    pin_the_synthetic_list(list_text_for(RELEASE_FILES).encode())


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def current(store: CopyingMemoryStore, key: str) -> bytes:
    return next(item.data for item in store.history[key] if item.version.is_latest)


class CopyTest(unittest.TestCase):
    def test_control_each_bound_version_is_copied_by_its_version_ID_and_its_hash_is_confirmed(self) -> None:
        landed, reference = landed_records()
        copied = copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual(["clinical_data", "lab_data"], [item.table for item in copied])
        self.assertEqual({PREFIX + "clinical_data.csv", PREFIX + "lab_data.csv"}, {key for key in landed.store.history if key.startswith("records_input/")})
        self.assertEqual(CLINICAL, current(landed.store, PREFIX + "clinical_data.csv"))
        self.assertEqual(LAB, current(landed.store, PREFIX + "lab_data.csv"))
        self.assertEqual(
            [
                (landed.keys["clinical_data.csv"], landed.versions["clinical_data.csv"].version_id),
                (landed.keys["lab_data.csv"], landed.versions["lab_data.csv"].version_id),
            ],
            [(source, version) for source, version, _ in landed.store.copies],
        )
        self.assertEqual([sha(CLINICAL), sha(LAB)], [item.sha256 for item in copied])

    def test_only_the_two_tables_are_copied_and_no_other_landed_payload_is_read(self) -> None:
        landed, reference = landed_records()
        landed.store.reads.clear()
        copy_bound_versions(landed.store, reference, RUN)
        vital = landed.keys["vital_files/0001.vital"]
        self.assertEqual([], [read for read in landed.store.reads if read.key == vital])
        self.assertTrue(all(source != vital for source, _, _ in landed.store.copies))

    def test_a_version_substituted_after_the_manifest_is_refused_before_any_copy(self) -> None:
        landed, reference = landed_records()
        writes = len(landed.store.writes)
        landed.store.put_object(landed.keys["clinical_data.csv"], b"caseid,subjectid\n9,9\n")
        with self.assertRaises(GateRefused):
            copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual([], landed.store.copies)
        self.assertEqual(writes + 1, len(landed.store.writes), "nothing was written but the substituted version")

    def test_a_missing_version_of_either_table_is_refused_before_any_copy(self) -> None:
        for name in ("clinical_data.csv", "lab_data.csv"):
            with self.subTest(name):
                landed, reference = landed_records()
                landed.store.put_delete_marker(landed.keys[name])
                with self.assertRaises(GateRefused):
                    copy_bound_versions(landed.store, reference, RUN)
                self.assertEqual([], landed.store.copies)

    def test_a_manifest_that_does_not_exist_is_refused_before_any_copy(self) -> None:
        """A failed check publishes no manifest, so the record load has no reference to read: a made-up reference stands for that."""
        landed, reference = landed_records()
        absent = ManifestRef(reference.bucket, f"manifests/probe_set/{'0' * 64}.json", "version9999", "0" * 64)
        with self.assertRaises(GateRefused):
            copy_bound_versions(landed.store, absent, RUN)
        self.assertEqual([], landed.store.copies)

    def test_a_manifest_that_does_not_bind_a_table_is_refused_before_any_copy(self) -> None:
        without_lab = {key: data for key, data in RELEASE_FILES.items() if key != "lab_data.csv"}
        with synthetic_pin(list_text_for(without_lab).encode()):
            landed, reference = landed_records(without_lab)
            with self.assertRaises(GateRefused):
                copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual([], landed.store.copies)

    def test_a_copy_whose_hash_differs_from_the_manifest_is_refused_and_names_the_table_only(self) -> None:
        landed, reference = landed_records()
        landed.store.corrupt.add(landed.keys["lab_data.csv"])
        with self.assertRaises(CopyError) as raised:
            copy_bound_versions(landed.store, reference, RUN)
        self.assertIn("lab_data", str(raised.exception))
        self.assertNotIn(sha(LAB), str(raised.exception))

    def test_a_second_call_copies_and_writes_nothing(self) -> None:
        landed, reference = landed_records()
        first = copy_bound_versions(landed.store, reference, RUN)
        copies, writes = list(landed.store.copies), list(landed.store.writes)
        second = copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual(first, second)
        self.assertEqual((copies, writes), (landed.store.copies, landed.store.writes))

    def test_a_copy_that_was_corrupted_by_an_earlier_try_is_replaced_and_then_confirmed(self) -> None:
        landed, reference = landed_records()
        landed.store.corrupt.add(landed.keys["lab_data.csv"])
        with self.assertRaises(CopyError):
            copy_bound_versions(landed.store, reference, RUN)
        landed.store.corrupt.clear()
        copied = copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual(LAB, current(landed.store, PREFIX + "lab_data.csv"))
        self.assertEqual(sha(LAB), copied[1].sha256)

    def test_a_copy_in_the_prefix_with_the_size_of_the_table_but_other_bytes_is_replaced_not_reused(self) -> None:
        landed, reference = landed_records()
        landed.store.put_object(PREFIX + "clinical_data.csv", b"x" * len(CLINICAL))
        copied = copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual(CLINICAL, current(landed.store, PREFIX + "clinical_data.csv"))
        self.assertEqual(sha(CLINICAL), copied[0].sha256)
        self.assertEqual(2, len(landed.store.copies))

    def test_an_object_in_the_read_prefix_that_is_not_a_table_is_refused(self) -> None:
        landed, reference = landed_records()
        landed.store.put_object(PREFIX + "extra.csv", b"caseid\n7\n")
        with self.assertRaises(CopyError):
            copy_bound_versions(landed.store, reference, RUN)
        self.assertEqual([], landed.store.copies)

    def test_a_run_ID_that_is_not_a_plain_name_is_refused_before_the_gate_runs(self) -> None:
        landed, reference = landed_records()
        landed.store.reads.clear()
        with self.assertRaises(ValueError):
            copy_bound_versions(landed.store, reference, "../other")
        self.assertEqual([], landed.store.reads)


class ConfirmTest(unittest.TestCase):
    def copied(self) -> tuple[RecordLanded, list[Copied]]:
        landed, reference = landed_records()
        return landed, list(copy_bound_versions(landed.store, reference, RUN))

    def test_control_the_copies_as_recorded_are_confirmed_and_nothing_is_read(self) -> None:
        landed, copied = self.copied()
        landed.store.reads.clear()
        confirm_copies(landed.store, RUN, copied)
        self.assertEqual([], landed.store.reads)

    def test_a_new_version_of_a_copy_is_refused(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(PREFIX + "clinical_data.csv", b"caseid,subjectid\n9,9\n")
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)

    def test_a_deleted_copy_and_an_added_object_are_each_refused(self) -> None:
        landed, copied = self.copied()
        landed.store.put_object(PREFIX + "added.csv", b"x")
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)
        landed, copied = self.copied()
        landed.store.put_delete_marker(PREFIX + "lab_data.csv")
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, copied)

    def test_an_empty_record_of_copies_is_refused(self) -> None:
        landed, _ = self.copied()
        with self.assertRaises(CopyError):
            confirm_copies(landed.store, RUN, [])


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


class ReleasePrefixTest(unittest.TestCase):
    def test_the_test_release_uses_the_pinned_release_path(self) -> None:
        self.assertEqual("vitaldb/1.0.0/clinical_data.csv", source_key("clinical_data.csv"))
        self.assertTrue(source_key("lab_data.csv").startswith(SOURCE_PREFIX))


if __name__ == "__main__":
    unittest.main()
