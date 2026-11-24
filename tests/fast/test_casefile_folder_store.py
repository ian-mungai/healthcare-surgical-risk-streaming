"""The shared folder store (``casefile/folder_store.py``, used by the replay and the Bronze decoder) keeps the ingestion
store's contract: closed errors only, and one version for a created key.

The folder store stands in for the landed copy when the replay runs without a bucket, and the project's own check, manifest and gate run against it. A
plain-text error would stop them with a ``TypeError`` instead of the refusal they report, and a create that adds a second version would let a manifest key
change under its reference.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from casefile.folder_store import FolderStore
from scripts.ingestion.notice import Code
from scripts.ingestion.store import ObjectVersion, StoreError

KEY = "manifests/folder_probe/probe.json"


class FolderStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_folder_store_")
        self.addCleanup(folder.cleanup)
        self.store = FolderStore(Path(folder.name))

    def codes(self, error: StoreError) -> list[Code]:
        return [notice.code for notice in error.notices]

    def test_a_version_that_does_not_exist_is_a_closed_store_error(self) -> None:
        absent = ObjectVersion(KEY, "v0001", 4, "2026-11-21T00:00:00Z", True, False)
        with self.assertRaises(StoreError) as streamed:
            b"".join(self.store.iter_version(absent))
        with self.assertRaises(StoreError) as whole:
            self.store.get_version_bytes(absent, 10)
        self.assertEqual([[Code.VERSION_MISSING], [Code.VERSION_MISSING]], [self.codes(streamed.exception), self.codes(whole.exception)])

    def test_a_version_over_the_limit_is_a_closed_store_error(self) -> None:
        version = self.store.create_object(KEY, b"0123456789")
        with self.assertRaises(StoreError) as caught:
            self.store.get_version_bytes(version, 9)
        self.assertEqual([Code.TOO_LARGE], self.codes(caught.exception))
        self.assertEqual(b"0123456789", self.store.get_version_bytes(version, 10))

    def test_a_create_with_the_same_bytes_returns_the_one_version(self) -> None:
        first = self.store.create_object(KEY, b'{"a": 1}\n')
        again = self.store.create_object(KEY, b'{"a": 1}\n')
        self.assertEqual(first, again)
        self.assertEqual([first.version_id], [version.version_id for version in self.store.list_versions(KEY)])

    def test_a_create_with_other_bytes_is_refused_and_leaves_the_key_alone(self) -> None:
        first = self.store.create_object(KEY, b'{"a": 1}\n')
        with self.assertRaises(StoreError) as caught:
            self.store.create_object(KEY, b'{"a": 2}\n')
        self.assertEqual([Code.OTHER_CONTENT], self.codes(caught.exception))
        self.assertEqual([first.version_id], [version.version_id for version in self.store.list_versions(KEY)])
        self.assertEqual(b'{"a": 1}\n', self.store.get_version_bytes(first, 100))

    def test_a_landed_file_adds_a_version_as_the_copy_does(self) -> None:
        source = Path(self.store.root) / "source"
        source.write_bytes(b"first")
        first = self.store.land_file("landing/sync_00/a.vital", source)
        source.write_bytes(b"second")
        second = self.store.land_file("landing/sync_00/a.vital", source)
        listed = self.store.list_versions("landing/")
        self.assertEqual([first.version_id, second.version_id], [version.version_id for version in listed])
        self.assertEqual([False, True], [version.is_latest for version in listed])


if __name__ == "__main__":
    unittest.main()
