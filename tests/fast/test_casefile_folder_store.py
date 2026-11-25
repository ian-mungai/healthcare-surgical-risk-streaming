"""The shared folder store (``casefile/folder_store.py``, used by the replay and the Bronze decoder) keeps the ingestion
store's contract: closed errors only, and one version for a created key.

The folder store stands in for the landed copy when the replay runs without a bucket, and the project's own check, manifest and gate run against it. A
plain-text error would stop them with a ``TypeError`` instead of the refusal they report, and a create that adds a second version would let a manifest key
change under its reference.
"""

from __future__ import annotations

import os
import signal
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from casefile.folder_store import FolderStore, ReadLog
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
    """The read log is always a new file: ``ReadLog.create`` makes it exclusively when the run opens its store, an existing path of any kind is refused with
    ``CANNOT_WRITE`` before a read and stays as it was, and every read appends one whole line to the open file."""

    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_read_log_")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.version = ObjectVersion(KEY, "v0001", 4, "2026-11-21T00:00:00Z", True, False)

    def created(self, log: Path) -> ReadLog:
        opened = ReadLog.create(log)
        self.addCleanup(opened.close)
        return opened

    def codes(self, error: StoreError) -> list[Code]:
        return [notice.code for notice in error.notices]

    def test_a_new_file_is_made_when_the_log_is_created_and_gets_one_line_per_read(self) -> None:
        log = self.root / "reads.log"
        store = FolderStore(self.root / "store", read_log=self.created(log))
        self.assertEqual("", log.read_text())
        store.note_read(self.version)
        store.note_read(self.version)
        self.assertEqual(f"{KEY} v0001\n" * 2, log.read_text())

    def test_the_new_file_is_readable_by_others_and_writable_by_its_owner_alone_whatever_the_umask(self) -> None:
        previous = os.umask(0)
        self.addCleanup(os.umask, previous)
        log = self.root / "reads.log"
        self.created(log)
        self.assertEqual(0o644, log.stat().st_mode & 0o777)

    def test_a_read_is_appended_after_what_another_writer_added_and_never_over_it(self) -> None:
        log = self.root / "reads.log"
        store = FolderStore(self.root / "store", read_log=self.created(log))
        store.note_read(self.version)
        with log.open("ab") as other:
            other.write(b"added by another writer\n")
        store.note_read(self.version)
        self.assertEqual(f"{KEY} v0001\nadded by another writer\n{KEY} v0001\n", log.read_text())

    def test_a_path_that_exists_in_any_form_is_refused_before_any_read_and_what_it_names_is_unchanged(self) -> None:
        target = self.root / "target.txt"
        target.write_bytes(b"kept\n")
        regular = self.root / "regular.log"
        regular.write_bytes(b"a record\n")
        empty = self.root / "empty.log"
        empty.write_bytes(b"")
        hard = self.root / "hard.log"
        os.link(target, hard)
        link = self.root / "link.log"
        link.symlink_to(target)
        dangling = self.root / "dangling.log"
        dangling.symlink_to(self.root / "nowhere.txt")
        folder = self.root / "folder.log"
        folder.mkdir()
        pipe = self.root / "pipe.log"
        os.mkfifo(pipe)
        before = {path: path.read_bytes() for path in (target, regular, empty, hard)}
        for log in (target, regular, empty, hard, link, dangling, folder, pipe):
            with self.subTest(log=log.name), self.assertRaises(StoreError) as caught:
                ReadLog.create(log)
            self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))
        self.assertEqual(before, {path: path.read_bytes() for path in before})
        self.assertFalse((self.root / "nowhere.txt").exists())
        self.assertTrue(folder.is_dir())
        self.assertTrue(stat.S_ISFIFO(pipe.lstat().st_mode))

    def test_a_second_log_that_names_the_same_path_is_refused_and_the_first_keeps_its_lines(self) -> None:
        log = self.root / "reads.log"
        FolderStore(self.root / "store", read_log=self.created(log)).note_read(self.version)
        with self.assertRaises(StoreError) as caught:
            ReadLog.create(log)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))
        self.assertEqual(f"{KEY} v0001\n", log.read_text())

    def test_a_path_in_a_folder_that_does_not_exist_is_refused_and_makes_no_folder(self) -> None:
        with self.assertRaises(StoreError) as caught:
            ReadLog.create(self.root / "absent" / "reads.log")
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))
        self.assertFalse((self.root / "absent").exists())

    def test_a_short_write_is_a_closed_cannot_write_error(self) -> None:
        log = self.root / "reads.log"
        store = FolderStore(self.root / "store", read_log=self.created(log))
        real_write = os.write

        def short(descriptor: int, data: bytes) -> int:
            return real_write(descriptor, data[:-1])

        with mock.patch.object(os, "write", short), self.assertRaises(StoreError) as caught:
            store.note_read(self.version)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))

    def test_a_short_write_that_writes_nothing_is_the_same_error(self) -> None:
        store = FolderStore(self.root / "store", read_log=self.created(self.root / "reads.log"))
        with mock.patch.object(os, "write", lambda descriptor, data: 0), self.assertRaises(StoreError) as caught:
            store.note_read(self.version)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))

    def test_a_write_that_fails_is_a_closed_cannot_write_error(self) -> None:
        store = FolderStore(self.root / "store", read_log=self.created(self.root / "reads.log"))
        with mock.patch.object(os, "write", side_effect=OSError(28, "no space left")), self.assertRaises(StoreError) as caught:
            store.note_read(self.version)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))

    def test_a_job_worker_attaches_to_the_log_that_its_driver_created_and_only_to_a_regular_file(self) -> None:
        log = self.root / "reads.log"
        self.created(log)
        attached = ReadLog.attach(log)
        self.addCleanup(attached.close)
        FolderStore(self.root / "store", read_log=attached).note_read(self.version)
        self.assertEqual(f"{KEY} v0001\n", log.read_text())
        target = self.root / "target.txt"
        target.write_bytes(b"kept\n")
        link = self.root / "link.log"
        link.symlink_to(target)
        folder = self.root / "folder.log"
        folder.mkdir()
        pipe = self.root / "pipe.log"
        os.mkfifo(pipe)
        reader = os.open(pipe, os.O_RDONLY | os.O_NONBLOCK)
        self.addCleanup(os.close, reader)
        for refused in (link, folder, pipe, self.root / "absent.log"):
            with self.subTest(attach=refused.name), self.assertRaises(StoreError) as caught:
                ReadLog.attach(refused)
            self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))
        self.assertEqual(b"kept\n", target.read_bytes())
        self.assertFalse((self.root / "absent.log").exists())
        self.assertEqual(b"", os.read(reader, 10))

    def test_a_named_pipe_without_a_reader_is_refused_by_attach_and_does_not_block(self) -> None:
        pipe = self.root / "pipe.log"
        os.mkfifo(pipe)

        def blocked(signum: int, frame: object) -> None:
            raise AssertionError("attach waited for a reader")

        previous = signal.signal(signal.SIGALRM, blocked)
        self.addCleanup(signal.signal, signal.SIGALRM, previous)
        signal.alarm(5)
        self.addCleanup(signal.alarm, 0)
        with self.assertRaises(StoreError) as caught:
            ReadLog.attach(pipe)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))

    def test_a_closed_log_is_a_closed_error_on_the_next_read(self) -> None:
        opened = ReadLog.create(self.root / "reads.log")
        opened.close()
        opened.close()
        with self.assertRaises(StoreError) as caught:
            FolderStore(self.root / "store", read_log=opened).note_read(self.version)
        self.assertEqual([Code.CANNOT_WRITE], self.codes(caught.exception))


if __name__ == "__main__":
    unittest.main()
