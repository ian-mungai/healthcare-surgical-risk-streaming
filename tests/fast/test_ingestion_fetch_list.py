"""The publisher's checksum list is fetched as verification metadata: pinned by its release path, size and SHA-256, and kept apart from the payloads.

The tracked pin fixes the list's key, size and SHA-256, and a synthetic list stands in for the published one in these cases. A fetched file is verified before
it takes its place (it is created and never replaces a path), so no unverified byte is ever at the target, and a second fetch of a verified file reads nothing.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest import mock

from scripts.ingestion import fetch_list
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.fetch_list import ListPin, fetch_checksum_list, tracked_list_pin
from scripts.ingestion.notice import Code, Notice, Subject, render
from scripts.sample.publisher import PublisherError

PIN = json.loads(Path("tests/fixtures/vitaldb/release.json").read_text())
LIST_BYTES = b"0" * 64 + b" vital_files/0001.vital\n"
SYNTHETIC = ListPin(PIN["checksum_list"]["key"], len(LIST_BYTES), hashlib.sha256(LIST_BYTES).hexdigest())


class Download:
    """A stand-in for the unsigned read of the publisher's bucket. It records each key it is asked for."""

    def __init__(self, content: bytes = LIST_BYTES, fail: Exception | None = None) -> None:
        self.content, self.fail = content, fail
        self.keys: list[str] = []

    def __call__(self, key: str, target: Path) -> None:
        self.keys.append(key)
        if self.fail:
            raise self.fail
        target.write_bytes(self.content)


class FetchTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="ingestion_fetch_list_")
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.target = self.folder / "metadata" / "SHA256SUMS.txt"

    def leftovers(self) -> list[str]:
        return sorted(path.name for path in self.target.parent.glob("*") if path != self.target) if self.target.parent.exists() else []

    def test_control_the_pinned_list_is_read_by_its_release_path_and_kept_at_the_target(self) -> None:
        download = Download()
        self.assertTrue(fetch_checksum_list(self.target, SYNTHETIC, download))
        self.assertEqual([PIN["checksum_list"]["key"]], download.keys)
        self.assertEqual(LIST_BYTES, self.target.read_bytes())
        self.assertEqual([], self.leftovers())

    def test_a_list_of_another_size_is_refused_and_nothing_is_kept(self) -> None:
        with self.assertRaises(PublisherError) as raised:
            fetch_checksum_list(self.target, SYNTHETIC, Download(LIST_BYTES + b"x"))
        self.assertEqual("reading the checksum list does not have the pinned size", str(raised.exception))
        self.assertFalse(self.target.exists())
        self.assertEqual([], self.leftovers())

    def test_a_list_of_another_digest_is_refused_and_nothing_is_kept(self) -> None:
        with self.assertRaises(PublisherError) as raised:
            fetch_checksum_list(self.target, SYNTHETIC, Download(b"1" * len(LIST_BYTES)))
        self.assertEqual("reading the checksum list does not match the pinned SHA-256", str(raised.exception))
        self.assertFalse(self.target.exists())
        self.assertEqual([], self.leftovers())

    def test_a_failed_read_keeps_nothing(self) -> None:
        with self.assertRaises(PublisherError):
            fetch_checksum_list(self.target, SYNTHETIC, Download(fail=PublisherError(Notice(Code.EXIT_STATUS, subject=Subject.READING_LIST, count=1))))
        self.assertFalse(self.target.exists())
        self.assertEqual([], self.leftovers())

    def test_a_second_fetch_of_a_verified_list_reads_nothing(self) -> None:
        fetch_checksum_list(self.target, SYNTHETIC, Download())
        again = Download()
        self.assertFalse(fetch_checksum_list(self.target, SYNTHETIC, again))
        self.assertEqual([], again.keys)
        self.assertEqual(LIST_BYTES, self.target.read_bytes())

    def test_a_changed_file_at_the_target_is_refused_before_anything_is_read_and_stays(self) -> None:
        self.target.parent.mkdir(parents=True)
        self.target.write_bytes(b"edited")
        download = Download()
        with self.assertRaises(ClosedInputError) as raised:
            fetch_checksum_list(self.target, SYNTHETIC, download)
        self.assertEqual(render(Notice(Code.OUTPUT_EXISTS)), str(raised.exception))
        self.assertEqual([], download.keys)
        self.assertEqual(b"edited", self.target.read_bytes())
        self.assertEqual([], self.leftovers())

    def test_a_link_or_a_folder_at_the_target_is_refused_and_left_as_it_was(self) -> None:
        self.target.parent.mkdir(parents=True)
        elsewhere = self.folder / "elsewhere"
        plants: list[Callable[[], object]] = [lambda: self.target.symlink_to(elsewhere), lambda: self.target.mkdir()]
        for plant in plants:
            plant()
            with self.assertRaises(ClosedInputError):
                fetch_checksum_list(self.target, SYNTHETIC, Download())
            self.assertFalse(elsewhere.exists())
            self.assertTrue(self.target.is_symlink() or self.target.is_dir())
            if self.target.is_symlink():
                self.target.unlink()
            else:
                self.target.rmdir()

    def test_the_list_is_written_through_the_helper_and_not_over_a_path_that_appears_after_the_read(self) -> None:
        late = Download()

        def appears(key: str, target: Path) -> None:
            late(key, target)
            self.target.write_bytes(b"late")

        self.target.parent.mkdir(parents=True)
        with self.assertRaises(ClosedInputError):
            fetch_checksum_list(self.target, SYNTHETIC, appears)
        self.assertEqual(b"late", self.target.read_bytes())

    def test_a_read_that_does_not_verify_leaves_no_file_and_no_temporary_folder_beside_the_target(self) -> None:
        with self.assertRaises(PublisherError):
            fetch_checksum_list(self.target, SYNTHETIC, Download(b"1" * len(LIST_BYTES)))
        self.assertEqual([], self.leftovers())

    def test_the_tracked_pin_gives_the_key_size_and_digest_of_the_release_pin(self) -> None:
        pin = tracked_list_pin()
        self.assertEqual(
            (PIN["checksum_list"]["key"], PIN["checksum_list"]["size_bytes"], PIN["checksum_list"]["sha256"]), (pin.key, pin.size_bytes, pin.sha256)
        )


class CommandTest(unittest.TestCase):
    def run_main(self, *arguments: str, download: Download) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.object(fetch_list, "download_key", download), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = fetch_list.main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def test_the_command_reads_the_tracked_pins_key_and_refuses_a_list_that_is_not_the_pinned_one(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            target = Path(scratch) / "SHA256SUMS.txt"
            download = Download()
            code, output, errors = self.run_main("--output", str(target), download=download)
            self.assertEqual([PIN["checksum_list"]["key"]], download.keys)
            self.assertEqual((1, ""), (code, output))
            self.assertEqual("reading the checksum list does not have the pinned size\n", errors)
            self.assertFalse(target.exists())

    def test_a_usage_error_names_no_word_of_the_command_line(self) -> None:
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors), self.assertRaises(SystemExit):
            fetch_list.main(["--unknown-word-xyz"])
        self.assertNotIn("unknown-word-xyz", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
