"""The replay's folder store keeps the ingestion store's contract: closed errors only, and one version for a created key.

The folder store stands in for the landed copy when the replay runs without a bucket, and the project's own check, manifest and gate run against it. A
plain-text error would stop them with a ``TypeError`` instead of the refusal they report, and a create that adds a second version would let a manifest key
change under its reference.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from replay.service.folder_store import FolderStore
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


class ReadLogTest(unittest.TestCase):
    """The read log is appended to only when it is a regular file: a named pipe, a folder or a link in its place is refused with a closed error, and
    nothing blocks."""

    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_read_log_")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.version = ObjectVersion(KEY, "v0001", 4, "2026-11-21T00:00:00Z", True, False)

    def note(self, log: Path) -> None:
        FolderStore(self.root / "store", read_log=log).note_read(self.version)

    def test_a_regular_file_gets_one_line_per_read_and_is_made_when_absent(self) -> None:
        log = self.root / "reads.log"
        self.note(log)
        self.note(log)
        self.assertEqual(f"{KEY} v0001\n" * 2, log.read_text())

    def test_a_named_pipe_with_a_reader_is_refused_after_open_and_nothing_is_written_to_it(self) -> None:
        log = self.root / "reads.log"
        os.mkfifo(log)
        reader = os.open(log, os.O_RDONLY | os.O_NONBLOCK)
        self.addCleanup(os.close, reader)
        with self.assertRaises(StoreError):
            self.note(log)
        self.assertEqual(b"", os.read(reader, 10))

    def test_a_named_pipe_without_a_reader_is_refused_and_does_not_block(self) -> None:
        log = self.root / "reads.log"
        os.mkfifo(log)
        with self.assertRaises(StoreError):
            self.note(log)

    def test_a_folder_and_a_link_are_refused_and_the_file_a_link_names_is_unchanged(self) -> None:
        target = self.root / "target.txt"
        target.write_text("kept\n")
        folder = self.root / "folder.log"
        folder.mkdir()
        link = self.root / "link.log"
        link.symlink_to(target)
        for log in (folder, link):
            with self.subTest(log=log.name), self.assertRaises(StoreError):
                self.note(log)
        self.assertEqual("kept\n", target.read_text())


if __name__ == "__main__":
    unittest.main()
