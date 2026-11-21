"""The verify-before-read gate: a reader gets only the versions its passed manifest binds, and a missing or substituted version is refused with no payload read.

Each refusal has a passing control. The stand-in store records every read, so each case proves that no payload under the landing prefix was read.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from scripts.ingestion.check import result_digest
from scripts.ingestion.gate import GateRefused, verify_before_read
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.manifest import MANIFEST_PREFIX, ManifestRef, publish_manifest
from tests.fast.ingestion_support import SOURCE, SOURCE_PREFIX, Landed, digest, landed_set

WANTED = [SOURCE_PREFIX + "vital_files/0001.vital", SOURCE_PREFIX + "clinical_data.csv"]


def published(landed: Landed) -> ManifestRef:
    return publish_manifest(landed.store, landed.selection, landed.check())


def forged(landed: Landed, reference: ManifestRef, change: Callable[[dict[str, Any]], None], *, consistent: bool) -> ManifestRef:
    """Publish an edited copy of the manifest under the key of its own SHA-256. With ``consistent`` the check digest is recomputed from the edited entries."""
    document = json.loads(landed.store.get_version_bytes(landed.store.list_versions(reference.key)[0], 10**7))
    change(document)
    if consistent:
        document["check_result_sha256"] = result_digest(document["selection"]["keys_sha256"], document["checksum_list"]["sha256"], document["entries"])
    data = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    key = f"{MANIFEST_PREFIX}probe_set/{digest(data)}.json"
    return ManifestRef(reference.bucket, key, landed.store.put_object(key, data).version_id, digest(data))


def problems(landed: Landed, reference: ManifestRef, wanted: list[str] | None = None) -> dict[str, str]:
    try:
        verify_before_read(landed.store, reference, wanted)
    except GateRefused as refused:
        return {problem.source_key: problem.cause for problem in refused.problems}
    raise AssertionError("the gate did not refuse")


class GateTest(unittest.TestCase):
    def test_control_the_versions_of_the_manifest_pass_and_no_payload_is_read(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.reads.clear()
        entries = verify_before_read(landed.store, reference, WANTED)
        self.assertEqual(WANTED, [entry.source_key for entry in entries])
        for entry in entries:
            path = entry.source_key.removeprefix(SOURCE_PREFIX)
            self.assertEqual(
                (landed.key_for(entry.source_key), landed.versions[path].version_id, landed.store.bucket), (entry.key, entry.version_id, entry.bucket)
            )
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))

    def test_no_keys_named_means_every_entry_of_the_manifest(self) -> None:
        landed = landed_set()
        entries = verify_before_read(landed.store, published(landed))
        self.assertEqual({SOURCE_PREFIX + path for path in SOURCE}, {entry.source_key for entry in entries})

    def test_a_version_substituted_after_the_manifest_is_refused_before_any_payload_is_read(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.reads.clear()
        landed.land("vital_files/0001.vital", b"a substituted copy of the recording")
        self.assertEqual({WANTED[0]: "version_substituted"}, problems(landed, reference, WANTED))
        self.assertEqual([], landed.store.payload_reads(LANDING_PREFIX))

    def test_a_substitute_of_a_key_the_reader_does_not_need_does_not_stop_the_reader(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.land("vital_files/0002.vital", b"substituted")
        self.assertEqual(WANTED, [entry.source_key for entry in verify_before_read(landed.store, reference, WANTED)])

    def test_an_object_deleted_after_the_manifest_is_refused_as_missing(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.remove_version(landed.key_for(WANTED[1]), landed.versions["clinical_data.csv"].version_id)
        self.assertEqual({WANTED[1]: "missing"}, problems(landed, reference, WANTED))

    def test_a_delete_marker_on_top_of_the_bound_version_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_delete_marker(landed.key_for(WANTED[1]))
        self.assertEqual({WANTED[1]: "missing"}, problems(landed, reference, WANTED))

    def test_a_key_that_the_manifest_does_not_bind_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.store.put_object(LANDING_PREFIX + "sync_99/vital_files/9999.vital", b"landed after the check")
        unbound = SOURCE_PREFIX + "vital_files/9999.vital"
        self.assertEqual({unbound: "not_in_manifest"}, problems(landed, reference, [WANTED[0], unbound]))

    def test_every_refusal_is_listed_with_its_key_and_cause(self) -> None:
        landed = landed_set()
        reference = published(landed)
        landed.land("vital_files/0001.vital", b"substituted")
        landed.store.put_delete_marker(landed.key_for(WANTED[1]))
        with self.assertRaises(GateRefused) as raised:
            verify_before_read(landed.store, reference, [*WANTED, SOURCE_PREFIX + "other.csv"])
        message = str(raised.exception)
        for key, cause in ((WANTED[0], "version_substituted"), (WANTED[1], "missing"), (SOURCE_PREFIX + "other.csv", "not_in_manifest")):
            self.assertIn(f"{key}: {cause}", message)

    def test_a_manifest_that_does_not_match_its_reference_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        with self.assertRaisesRegex(GateRefused, "manifest"):
            verify_before_read(landed.store, replace(reference, sha256="0" * 64), WANTED)
        with self.assertRaisesRegex(GateRefused, "manifest"):
            verify_before_read(landed.store, replace(reference, version_id="version9999"), WANTED)
        with self.assertRaisesRegex(GateRefused, "bucket"):
            verify_before_read(landed.store, replace(reference, bucket="another-bucket"), WANTED)
        self.assertTrue(verify_before_read(landed.store, reference, WANTED))

    def test_a_manifest_that_is_not_a_passed_one_or_binds_a_null_version_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        document = json.loads(landed.store.get_version_bytes(landed.store.list_versions(reference.key)[0], 10**7))
        for change, expected in (
            (("status", "failed"), "passed"),
            (("entries", [{**document["entries"][0], "version_id": "null"}, *document["entries"][1:]]), "null"),
        ):
            edited = landed_set()
            data = json.dumps({**document, change[0]: change[1]}, sort_keys=True, separators=(",", ":")).encode()
            key = f"{MANIFEST_PREFIX}probe_set/{digest(data)}.json"
            version = edited.store.put_object(key, data)
            with self.subTest(expected=expected), self.assertRaisesRegex(GateRefused, expected):
                verify_before_read(edited.store, ManifestRef(reference.bucket, key, version.version_id, digest(data)), WANTED)

    def test_a_manifest_whose_entries_were_edited_after_the_check_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)

        def other_time(document: dict[str, Any]) -> None:
            document["entries"][0]["retrieved_utc"] = "2030-01-01T00:00:00Z"

        with self.assertRaisesRegex(GateRefused, "do not match the digest of the check"):
            verify_before_read(landed.store, forged(landed, reference, other_time, consistent=False), WANTED)
        self.assertTrue(verify_before_read(landed.store, forged(landed, reference, other_time, consistent=True), None))

    def test_a_manifest_that_binds_another_bucket_is_refused_for_that_key(self) -> None:
        landed = landed_set()
        reference = published(landed)

        def other_bucket(document: dict[str, Any]) -> None:
            document["entries"][0]["bucket"] = "another-bucket"

        edited = forged(landed, reference, other_bucket, consistent=True)
        with self.assertRaises(GateRefused) as raised:
            verify_before_read(landed.store, edited)
        self.assertEqual(["bucket_differs"], [problem.cause for problem in raised.exception.problems])

    def test_a_manifest_object_that_does_not_hash_to_its_reference_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        substitute = landed.store.put_object(reference.key, b'{"status":"passed"}')
        with self.assertRaisesRegex(GateRefused, "SHA-256 differs from its reference"):
            verify_before_read(landed.store, replace(reference, version_id=substitute.version_id), WANTED)

    def test_a_reference_whose_key_does_not_end_with_its_sha256_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        data = landed.store.get_version_bytes(landed.store.list_versions(reference.key)[0], 10**7)
        key = f"{MANIFEST_PREFIX}probe_set/{'0' * 64}.json"
        copy = landed.store.put_object(key, data)
        with self.assertRaisesRegex(GateRefused, "does not hold the SHA-256"):
            verify_before_read(landed.store, replace(reference, key=key, version_id=copy.version_id), WANTED)

    def test_a_bound_version_removed_while_another_is_current_is_refused_as_missing(self) -> None:
        landed = landed_set()
        reference = published(landed)
        bound = landed.versions["clinical_data.csv"].version_id
        landed.land("clinical_data.csv", b"caseid,age\n1,61\n")
        landed.store.remove_version(landed.key_for(WANTED[1]), bound)
        self.assertEqual({WANTED[1]: "missing"}, problems(landed, reference, WANTED))

    def test_a_size_that_differs_from_the_bound_size_is_refused(self) -> None:
        landed = landed_set()
        reference = published(landed)
        version = landed.store.history[landed.key_for(WANTED[1])][0]
        landed.store.history[landed.key_for(WANTED[1])][0] = replace(version, version=replace(version.version, size=version.version.size + 1))
        self.assertEqual({WANTED[1]: "size_differs"}, problems(landed, reference, WANTED))


if __name__ == "__main__":
    unittest.main()
