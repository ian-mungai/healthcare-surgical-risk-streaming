"""The landed cache of the streaming runners fills an empty folder: it fetches the recordings of the planned cases and the two table files.

The runner stays untracked, so each test skips when it is not on this machine. The publisher is a stand-in for the AWS command line: it writes the key that
it was asked for into the target file, and the stand-in digest gives a file its pinned SHA-256 only when the file holds that key. Nothing is downloaded and
no AWS call is made.

Failure modes covered here:

- A recording is asked of ``download_key``, which reads only the two keys of ``KEY_READS``, so a cache without the recordings ends in a ``KeyError`` and
  the run never starts.
- A case set of a stream plan names a case that the tracked candidate file does not hold.
- A recording is read with the caller's AWS access, or with no deadline.
- A file that is already there is downloaded again.
- A failed or slow download, a computer with no AWS command line or a file that is not the pinned one passes, or ends in an error that does not name it.
- A folder or a file in the cache, or the cache folder itself, is a link: to a place outside or inside the cache, that is there or not. A recording or a
  table is written through it, a folder is made, a file is read through it, or the downloader runs before the link is seen. A check that only asks where a
  path leads lets a link to a missing file inside the cache pass.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.sample import publisher
from scripts.sample.candidates import CANDIDATES_FILE, RELEASE_FILE, read_candidates
from tests.fast.runner_import import import_runner

TABLES = ("SHA256SUMS.txt", "clinical_data.csv")


def pins() -> dict[str, str]:
    """The pinned SHA-256 of every file that a cache can hold, by its path under the release prefix."""
    release = json.loads(RELEASE_FILE.read_text())
    found = {item.key.removeprefix(publisher.RELEASE_PREFIX): item.sha256 for item in read_candidates(CANDIDATES_FILE)}
    found["clinical_data.csv"] = release["tables"][0]["sha256"]
    found["SHA256SUMS.txt"] = release["checksum_list"]["sha256"]
    return found


class Publisher:
    """The AWS command line in front of the publisher's bucket. It keeps every call and writes the asked key into the target."""

    def __init__(self, failure: Callable[[str], Any] | None = None) -> None:
        self.calls: list[tuple[str, list[str], dict[str, Any]]] = []
        self.failure = failure

    def __call__(self, program: str, arguments: Sequence[str], **options: Any) -> SimpleNamespace:
        self.calls.append((program, list(arguments), options))
        key = arguments[2].removeprefix(f"s3://{publisher.BUCKET}/")
        if self.failure is not None and key.endswith(".vital"):
            return self.failure(arguments[3])  # type: ignore[no-any-return]
        Path(arguments[3]).write_text(key)
        return SimpleNamespace(returncode=0, stdout="", stderr="")


class CacheCase(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = import_runner("stack_replay")
        scratch = tempfile.TemporaryDirectory(prefix="landed_cache_test_")
        self.addCleanup(scratch.cleanup)
        self.cache = Path(scratch.name) / "cache"
        self.digested: list[Path] = []
        self.cases = tuple(sorted(set(self.module.FULL.control_cases) | set(self.module.FULL.small_cases)))
        self.names = sorted(
            [item.key.removeprefix(publisher.RELEASE_PREFIX) for item in read_candidates(CANDIDATES_FILE) if item.case in self.cases] + list(TABLES)
        )

    def fill(self, published: Publisher) -> dict[str, Path]:
        expected = pins()

        def digest(path: Path) -> str:
            self.digested.append(path)
            name = str(path.relative_to(self.cache))
            return expected[name] if path.read_text() == publisher.RELEASE_PREFIX + name else "0" * 64

        with (
            mock.patch.object(publisher, "run_command", published),
            mock.patch.object(self.module, "run_command", published),
            mock.patch.object(self.module, "file_sha256", digest),
            mock.patch.dict(os.environ, {"AWS_PROFILE": "owner-profile", "AWS_SECRET_ACCESS_KEY": "placeholder"}),
        ):
            return dict(self.module.fill_landed_cache(self.cache, self.cases))


class LandedCache(CacheCase):
    def test_the_cases_of_both_stream_plans_are_candidates_and_each_plan_fills_an_empty_cache(self) -> None:
        stream = import_runner("stream_host")
        candidates = {item.case for item in read_candidates(CANDIDATES_FILE)}
        for plans in (stream.FULL, stream.STREAM_ITERATION):
            cases = set(plans.control_cases) | set(plans.small_cases) | set(stream.ROOMS_CASES)
            self.assertEqual((len(cases), cases <= candidates), (3, True))
            self.assertEqual(cases, set(self.cases))

    def test_an_empty_cache_gets_every_recording_and_both_tables(self) -> None:
        published = Publisher()
        paths = self.fill(published)
        self.assertEqual(sorted(paths), self.names)
        self.assertEqual(len([name for name in paths if name.startswith("vital_files/")]), 3)
        self.assertEqual(sorted(call[1][2] for call in published.calls), [f"s3://{publisher.BUCKET}/{publisher.RELEASE_PREFIX}{name}" for name in self.names])

    def test_every_read_is_unsigned_has_a_deadline_and_gets_no_aws_setting_of_the_caller(self) -> None:
        published = Publisher()
        self.fill(published)
        for program, arguments, options in published.calls:
            self.assertEqual(program, "aws")
            self.assertEqual(arguments[:2], ["s3", "cp"])
            self.assertIn("--no-sign-request", arguments)
            self.assertEqual(options["timeout"], publisher.READ_TIMEOUT_SECONDS)
            environment = options["env"]
            self.assertNotIn("AWS_PROFILE", environment)
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
            self.assertEqual((environment["AWS_CONFIG_FILE"], environment["AWS_SHARED_CREDENTIALS_FILE"]), (os.devnull, os.devnull))

    def test_a_full_cache_is_checked_and_nothing_is_downloaded(self) -> None:
        self.fill(Publisher())
        again = Publisher()
        self.assertEqual(sorted(self.fill(again)), self.names)
        self.assertEqual(again.calls, [])

    def test_a_cache_with_some_of_the_files_gets_only_the_missing_ones(self) -> None:
        self.fill(Publisher())
        (self.cache / "vital_files" / "1147.vital").unlink()
        (self.cache / "SHA256SUMS.txt").unlink()
        again = Publisher()
        self.assertEqual(sorted(self.fill(again)), self.names)
        self.assertEqual(sorted(call[1][2].rsplit("/", 1)[1] for call in again.calls), ["1147.vital", "SHA256SUMS.txt"])

    def test_a_download_that_fails_is_slow_or_has_no_program_blocks_and_names_the_file(self) -> None:
        def exits(target: str) -> SimpleNamespace:
            return SimpleNamespace(returncode=1, stdout="", stderr="no such key")

        def slow(target: str) -> SimpleNamespace:
            raise TimeoutExpired("aws", publisher.READ_TIMEOUT_SECONDS)

        def absent(target: str) -> SimpleNamespace:
            raise FileNotFoundError("aws")

        for failure, word in ((exits, "exit status 1"), (slow, "timed out"), (absent, "AWS command line")):
            with self.subTest(word=word), self.assertRaises(self.module.Blocked) as caught:
                self.fill(Publisher(failure))
            self.assertIn(word, str(caught.exception))
            self.assertIn("vital_files/0908.vital", str(caught.exception))

    def test_a_recording_that_is_not_the_pinned_one_blocks(self) -> None:
        def other(target: str) -> SimpleNamespace:
            Path(target).write_text("another recording")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with self.assertRaises(self.module.Blocked) as caught:
            self.fill(Publisher(other))
        self.assertIn("does not match its pinned SHA-256", str(caught.exception))


class LinksInTheCache(CacheCase):
    """The same cache, with links in it. ``outside`` is a folder beside the cache."""

    def setUp(self) -> None:
        super().setUp()
        self.outside = self.cache.parent / "outside"
        self.outside.mkdir()
        self.cache.mkdir()

    def entries(self) -> list[str]:
        """Everything under the test's folder, with no link followed."""
        return sorted(str(item) for item in self.cache.parent.rglob("*"))

    def refused(self, words: str) -> None:
        """The fill is blocked with these words, and nothing was made, read or downloaded."""
        published = Publisher()
        before = self.entries()
        self.digested.clear()
        with self.assertRaises(self.module.Blocked) as caught:
            self.fill(published)
        self.assertIn(words, str(caught.exception))
        self.assertEqual((self.entries(), self.digested, published.calls), (before, [], []))

    def test_a_link_to_a_missing_file_inside_the_cache_is_refused_before_anything_happens(self) -> None:
        (self.cache / "SHA256SUMS.txt").symlink_to("not_there.txt")
        self.refused("SHA256SUMS.txt goes through a link")

    def test_a_folder_link_is_refused_wherever_it_leads(self) -> None:
        (self.cache / "recordings").mkdir()
        for where, target in (
            ("inside", self.cache / "recordings"),
            ("missing inside", self.cache / "later"),
            ("outside", self.outside),
            ("missing outside", self.outside / "later"),
        ):
            with self.subTest(where=where):
                link = self.cache / "vital_files"
                link.unlink(missing_ok=True)
                link.symlink_to(target, target_is_directory=True)
                self.refused("vital_files/0908.vital goes through a link")

    def test_a_file_link_is_refused_wherever_it_leads_even_when_it_holds_the_pinned_file(self) -> None:
        for name in ("vital_files/1147.vital", "clinical_data.csv", "SHA256SUMS.txt"):
            for where in ("inside", "missing inside", "outside", "missing outside"):
                with self.subTest(name=name, where=where):
                    shutil.rmtree(self.cache)
                    link = self.cache / name
                    link.parent.mkdir(parents=True)
                    held = (self.cache if "inside" in where else self.outside) / f"held_{Path(name).name}"
                    if not where.startswith("missing"):
                        held.write_text(publisher.RELEASE_PREFIX + name)
                    link.symlink_to(held)
                    self.refused(f"{name} goes through a link")

    def test_a_cache_folder_that_is_itself_a_link_is_refused_whether_its_target_is_there_or_not(self) -> None:
        for target in (self.cache.parent / "real_cache", self.cache.parent / "later_cache"):
            with self.subTest(target=target.name):
                if self.cache.is_symlink():
                    self.cache.unlink()
                else:
                    self.cache.rmdir()
                    (self.cache.parent / "real_cache").mkdir()
                self.cache.symlink_to(target, target_is_directory=True)
                self.refused("the cache folder is a link")


if __name__ == "__main__":
    unittest.main()
