"""The manifest of passed files: published only from a complete passed check, for the versions that were checked and once for each content.

Each refusal has the publishing of a passed check as its control. The stand-in store is described in ``memory_store.py``.
"""

from __future__ import annotations

import hashlib
import json
import unittest
from dataclasses import replace
from typing import Any

from scripts.ingestion.check import CheckedFile, CheckResult, Finding, canonical
from scripts.ingestion.gate import GateRefused, verify_before_read
from scripts.ingestion.manifest import (
    MANIFEST_PREFIX,
    ManifestError,
    ManifestRef,
    PublishRefused,
    build_document,
    manifest_key,
    publish_manifest,
    read_manifest,
)
from scripts.ingestion.store import ObjectVersion, StoreError
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, digest, landed_set


def checked(landed: Landed) -> CheckResult:
    return landed.check()


def manifest_objects(landed: Landed) -> list[str]:
    return [version.key for version in landed.store.list_versions(MANIFEST_PREFIX)]


class PublishTest(unittest.TestCase):
    def test_control_a_passed_check_publishes_a_manifest_outside_the_payload_prefix(self) -> None:
        landed = landed_set()
        result = checked(landed)
        reference = publish_manifest(landed.store, landed.selection, result)
        self.assertTrue(reference.key.startswith(f"{MANIFEST_PREFIX}probe_set/"))
        self.assertFalse(reference.key.startswith("landing/"))
        self.assertNotIn(reference.version_id, ("", "null"))
        self.assertEqual([reference.key], manifest_objects(landed))
        stored = landed.store.get_version_bytes(landed.store.list_versions(reference.key)[0], 10**7)
        self.assertEqual(hashlib.sha256(stored).hexdigest(), reference.sha256)
        self.assertTrue(reference.key.endswith(f"{reference.sha256}.json"))

    def test_each_entry_binds_the_landed_version_and_the_set_records_the_rest_once(self) -> None:
        landed = landed_set()
        result = checked(landed)
        reference = publish_manifest(landed.store, landed.selection, result)
        document = json.loads(landed.store.get_version_bytes(landed.store.list_versions(reference.key)[0], 10**7))
        self.assertEqual("2016-08 to 2017-06", document["measurement_period"])
        self.assertEqual(
            {
                "name": "probe_set",
                "source_bucket": "example-publisher",
                "source_prefix": SOURCE_PREFIX,
                "file_count": len(SOURCE),
                "keys_sha256": landed.selection.keys_sha256,
                "selection_sha256": landed.selection.selection_sha256,
            },
            document["selection"],
        )
        self.assertEqual({"key": f"{SOURCE_PREFIX}SHA256SUMS.txt", "sha256": landed.selection.checksum_list_sha256}, document["checksum_list"])
        self.assertEqual(result.result_sha256, document["check_result_sha256"])
        entries = {entry["source_key"]: entry for entry in document["entries"]}
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE}, set(entries))
        for path, data in SOURCE.items():
            source_key = SOURCE_PREFIX + path
            version = landed.versions[path]
            self.assertEqual(
                {
                    "bucket": landed.store.bucket,
                    "key": landed.key_for(source_key),
                    "version_id": version.version_id,
                    "sha256": digest(data),
                    "source_key": source_key,
                    "size": len(data),
                    "release": "1.0.0",
                    "retrieved_utc": version.last_modified,
                },
                entries[source_key],
            )
        self.assertNotIn("measurement_period", entries[SOURCE_PREFIX + "clinical_data.csv"])
        self.assertNotIn("selection", entries[SOURCE_PREFIX + "clinical_data.csv"])

    def test_a_reader_confirms_the_manifest_is_the_checks_by_recomputing_the_check_digest(self) -> None:
        landed = landed_set()
        result = checked(landed)
        reference = publish_manifest(landed.store, landed.selection, result)
        manifest = read_manifest(landed.store, reference)
        self.assertEqual(result.result_sha256, manifest.check_result_sha256)
        self.assertEqual(result.result_sha256, manifest.recomputed_check_result_sha256())

    def test_a_failed_check_publishes_nothing(self) -> None:
        landed = landed_set(landed={"vital_files/0001.vital": b"changed"})
        result = checked(landed)
        with self.assertRaisesRegex(PublishRefused, "did not pass"):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))

    def test_a_missing_file_publishes_nothing(self) -> None:
        landed = landed_set(skip=("clinical_data.csv",))
        with self.assertRaisesRegex(PublishRefused, "did not pass"):
            publish_manifest(landed.store, landed.selection, checked(landed))
        self.assertEqual([], manifest_objects(landed))

    def test_a_partial_result_publishes_nothing(self) -> None:
        landed = landed_set()
        result = checked(landed)
        partial = replace(result, files=result.files[:-1], file_count=len(result.files) - 1)
        partial = replace(partial, claimed_sha256=partial.result_sha256)
        with self.assertRaisesRegex(PublishRefused, "every selected file"):
            publish_manifest(landed.store, landed.selection, partial)
        self.assertEqual([], manifest_objects(landed))

    def test_a_result_for_another_selection_publishes_nothing(self) -> None:
        landed = landed_set()
        result = replace(checked(landed), selection_sha256="0" * 64)
        with self.assertRaisesRegex(PublishRefused, "selection"):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))

    def test_a_selection_that_differs_from_the_checked_one_in_any_field_publishes_nothing(self) -> None:
        landed = landed_set()
        result = checked(landed)
        for field, other in (
            ("source_bucket", "another-publisher"),
            ("checksum_list_key", f"{SOURCE_PREFIX}OTHER_SUMS.txt"),
            ("measurement_period", "2020-01 to 2020-02"),
            ("release", "2.0.0"),
            ("source_prefix", "vitaldb/2.0.0/"),
        ):
            changed: dict[str, Any] = {field: other}
            with self.subTest(field=field), self.assertRaisesRegex(PublishRefused, "not for this selection"):
                publish_manifest(landed.store, replace(landed.selection, **changed), result)
            self.assertEqual([], manifest_objects(landed))
        self.assertTrue(publish_manifest(landed.store, landed.selection, result).key)

    def test_a_result_that_did_not_pass_publishes_nothing_whatever_else_in_it_holds(self) -> None:
        landed = landed_set()
        result = checked(landed)
        for name, changed in (
            ("a file count of zero", replace(result, file_count=0)),
            ("a file count above the files", replace(result, file_count=len(result.files) + 1)),
            ("a finding", replace(result, findings=(Finding("x", "missing"),), claimed_sha256=result.claimed_sha256)),
        ):
            with self.subTest(name), self.assertRaisesRegex(PublishRefused, "did not pass"):
                publish_manifest(landed.store, landed.selection, changed)
            self.assertEqual([], manifest_objects(landed))

    def test_a_result_that_names_another_selection_publishes_nothing_although_its_digest_is_consistent(self) -> None:
        landed = landed_set()
        with self.assertRaisesRegex(PublishRefused, "not for this selection"):
            publish_manifest(landed.store, landed.selection, replace(checked(landed), selection_name="another_set"))
        self.assertEqual([], manifest_objects(landed))

    def test_a_result_of_another_checksum_list_publishes_nothing_although_its_digest_is_consistent(self) -> None:
        landed = landed_set()
        result = checked(landed)
        other = replace(result, list_sha256="1" * 64)
        other = replace(other, claimed_sha256=other.result_sha256)
        with self.assertRaisesRegex(PublishRefused, "pinned checksum list"):
            publish_manifest(landed.store, landed.selection, other)
        self.assertEqual([], manifest_objects(landed))

    def test_a_result_file_edited_after_the_check_publishes_nothing(self) -> None:
        landed = landed_set()
        text = checked(landed).to_json()
        edited = json.loads(text)
        edited["files"][0]["version_id"] = "version9999"
        with self.assertRaisesRegex(PublishRefused, "edited"):
            publish_manifest(landed.store, landed.selection, CheckResult.from_json(json.dumps(edited)))
        self.assertEqual([], manifest_objects(landed))
        self.assertTrue(publish_manifest(landed.store, landed.selection, CheckResult.from_json(text)).key)

    def test_a_version_landed_after_the_check_publishes_nothing_for_the_versions_that_were_checked(self) -> None:
        landed = landed_set()
        result = checked(landed)
        landed.land("vital_files/0001.vital", SOURCE["vital_files/0001.vital"])
        with self.assertRaisesRegex(PublishRefused, "vital_files/0001.vital.*new version landed after the check"):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))
        self.assertTrue(publish_manifest(landed.store, landed.selection, checked(landed)).key)

    def test_a_version_removed_after_the_check_publishes_nothing(self) -> None:
        landed = landed_set()
        result = checked(landed)
        landed.store.put_delete_marker(landed.key_for(SOURCE_PREFIX + "clinical_data.csv"))
        with self.assertRaisesRegex(PublishRefused, "clinical_data.csv: no longer present after the check"):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))


class WriteTest(unittest.TestCase):
    def test_a_manifest_that_reads_back_with_other_content_is_refused(self) -> None:
        landed = landed_set()
        create = landed.store.create_object
        landed.store.create_object = lambda key, data: create(key, data + b" ")  # type: ignore[method-assign]
        with self.assertRaisesRegex(PublishRefused, "reads back with other content"):
            publish_manifest(landed.store, landed.selection, checked(landed))


class NullVersionTest(unittest.TestCase):
    def test_a_bucket_whose_versioning_is_off_is_refused_before_the_manifest_is_written(self) -> None:
        landed = landed_set()
        result = checked(landed)
        landed.store.versioned = False
        with self.assertRaises(StoreError):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))

    def null_manifest(self, landed: Landed) -> ManifestRef:
        """A manifest written while versioning was off: its only version is the null version. Versioning is then on again."""
        result = checked(landed)
        data = canonical(build_document(landed.selection, result))
        key = manifest_key(landed.selection.name, digest(data))
        landed.store.versioned = False
        version = landed.store.put_object(key, data)
        landed.store.versioned = True
        self.assertEqual("null", version.version_id)
        return ManifestRef(landed.store.bucket, key, version.version_id, digest(data))

    def test_a_manifest_that_exists_only_as_a_null_version_is_not_reused(self) -> None:
        landed = landed_set()
        self.null_manifest(landed)
        with self.assertRaisesRegex(PublishRefused, "null version"):
            publish_manifest(landed.store, landed.selection, checked(landed))

    def test_a_reference_to_a_null_version_is_not_read(self) -> None:
        landed = landed_set()
        reference = self.null_manifest(landed)
        with self.assertRaisesRegex(ManifestError, "null version"):
            read_manifest(landed.store, reference)
        with self.assertRaisesRegex(GateRefused, "null version"):
            verify_before_read(landed.store, reference)

    def test_a_result_that_binds_a_null_version_publishes_nothing_although_its_digest_is_consistent(self) -> None:
        landed = landed_set()
        landed.store.versioned = False
        landed.store.history.clear()
        for path, data in SOURCE.items():
            landed.land(path, data)
        landed.store.versioned = True
        files = tuple(
            CheckedFile(landed.store.bucket, version.key, version.version_id, digest(SOURCE[path]), SOURCE_PREFIX + path, version.size, "1.0.0", "t")
            for path, version in landed.versions.items()
        )
        self.assertEqual({"null"}, {file.version_id for file in files})
        result = CheckResult(landed.selection.name, landed.selection.selection_sha256, landed.selection.checksum_list_sha256, len(files), files, ())
        result = replace(result, claimed_sha256=result.result_sha256)
        with self.assertRaisesRegex(PublishRefused, "null version"):
            publish_manifest(landed.store, landed.selection, result)
        self.assertEqual([], manifest_objects(landed))

    def test_a_write_that_returns_a_null_version_publishes_no_reference(self) -> None:
        landed = landed_set()

        def null_create(key: str, data: bytes) -> ObjectVersion:
            landed.store.versioned = False
            version = landed.store.put_object(key, data)
            landed.store.versioned = True
            return version

        landed.store.create_object = null_create  # type: ignore[method-assign]
        with self.assertRaisesRegex(PublishRefused, "null version"):
            publish_manifest(landed.store, landed.selection, checked(landed))

    def test_control_the_same_manifest_written_with_versioning_on_is_read(self) -> None:
        landed = landed_set()
        reference = publish_manifest(landed.store, landed.selection, checked(landed))
        self.assertEqual(len(SOURCE), len(read_manifest(landed.store, reference).entries))
        self.assertEqual(len(SOURCE), len(verify_before_read(landed.store, reference)))


class RerunTest(unittest.TestCase):
    def test_a_rerun_with_the_same_landed_versions_gives_the_same_manifest_and_adds_nothing(self) -> None:
        landed = landed_set()
        first = publish_manifest(landed.store, landed.selection, checked(landed))
        writes = list(landed.store.writes)
        second = publish_manifest(landed.store, landed.selection, checked(landed))
        self.assertEqual(first, second)
        self.assertEqual(writes, landed.store.writes)
        self.assertEqual(1, len(landed.store.list_versions(first.key)))

    def test_a_new_landed_version_gives_a_new_manifest_and_leaves_the_first_in_place(self) -> None:
        landed = landed_set()
        first = publish_manifest(landed.store, landed.selection, checked(landed))
        landed.land("vital_files/0001.vital", SOURCE["vital_files/0001.vital"])
        second = publish_manifest(landed.store, landed.selection, checked(landed))
        self.assertNotEqual(first.key, second.key)
        self.assertEqual(sorted([first.key, second.key]), manifest_objects(landed))

    def test_a_manifest_key_that_holds_other_content_is_refused_and_left_alone(self) -> None:
        landed = landed_set()
        first = publish_manifest(landed.store, landed.selection, checked(landed))
        landed.store.put_object(first.key, b'{"substituted": true}')
        with self.assertRaisesRegex(PublishRefused, "holds other content"):
            publish_manifest(landed.store, landed.selection, checked(landed))
        self.assertEqual(2, len(landed.store.list_versions(first.key)))


if __name__ == "__main__":
    unittest.main()
