"""A folder that cannot be read is an error, never an empty listing.

``pathlib``'s ``glob``, ``rglob`` and ``iterdir`` on a folder that is not readable return nothing, so an unreadable runs folder looked empty and a new
run ID was chosen twice. ``list_folder`` raises instead, and the scan below holds every other reader of a run, inbox or input folder to it.
"""

from __future__ import annotations

import contextlib
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.ingestion.folder import list_folder
from scripts.process import run_command
from scripts.stack.lifecycle import declared_problems
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

LISTING_CALL = re.compile(r"\.(glob|rglob|iterdir)\(")
# Files that list a folder of the repository's own tracked templates or sources, or that run alone on the host, where an unreadable folder is not a run input.
OWN_FILES = frozenset(
    {
        "ansible/roles/secret_files/module_utils/secret_files.py",
        "infra/iam/scripts/render_policy.py",
        "scripts/infra/terraform_wrapper.py",
        "scripts/ingestion/folder.py",
    }
)


def tracked_sources() -> list[str]:
    listing = run_command("git", ["ls-files", "-z", "--", "*.py"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout
    return [name for name in listing.split("\0") if name and (ROOT / name).is_file() and not name.startswith("tests/")]


class ListFolderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def test_the_matching_names_are_listed_sorted(self) -> None:
        for name in ("b_run", "a_run", "other.txt"):
            (self.root / name).touch()
        self.assertEqual([self.root / "a_run", self.root / "b_run"], list_folder(self.root, "*_run"))

    def test_the_listing_is_sorted_whatever_order_the_folder_gives(self) -> None:
        for name in ("b_run", "a_run", "c_run"):
            (self.root / name).touch()
        real = os.scandir

        def reversed_scan(folder: Path) -> contextlib.AbstractContextManager[list[os.DirEntry[str]]]:
            with real(folder) as entries:
                return contextlib.nullcontext(sorted(entries, key=lambda entry: entry.name, reverse=True))

        with mock.patch("scripts.ingestion.folder.os.scandir", reversed_scan):
            wanted = [self.root / "a_run", self.root / "b_run", self.root / "c_run"]
            self.assertEqual(wanted, list_folder(self.root, "*_run"))
            self.assertEqual(wanted, list_folder(self.root, "*_run", recursive=True))

    def test_a_folder_that_does_not_exist_holds_nothing(self) -> None:
        self.assertEqual([], list_folder(self.root / "absent"))

    def test_the_recursive_listing_reaches_each_depth(self) -> None:
        (self.root / "v1" / "deep").mkdir(parents=True)
        (self.root / "v1" / "deep" / "v0001").touch()
        (self.root / "v0002").touch()
        self.assertEqual([self.root / "v0002", self.root / "v1" / "deep" / "v0001"], list_folder(self.root, "v????", recursive=True))

    def test_a_folder_that_cannot_be_read_is_an_error_and_not_an_empty_listing(self) -> None:
        (self.root / "20260101_job_run1").mkdir()
        self.root.chmod(0)
        self.addCleanup(self.root.chmod, 0o700)
        if os.access(self.root, os.R_OK):
            self.skipTest("this account reads every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                list_folder(self.root, "*_*", recursive=recursive)

    def test_a_subfolder_that_cannot_be_read_is_an_error_in_the_recursive_listing(self) -> None:
        inner = self.root / "inner"
        inner.mkdir()
        (inner / "v0001").touch()
        inner.chmod(0)
        self.addCleanup(inner.chmod, 0o700)
        if os.access(inner, os.R_OK):
            self.skipTest("this account reads every folder")
        with self.assertRaises(PermissionError):
            list_folder(self.root, "v????", recursive=True)

    def test_a_file_in_place_of_the_folder_is_an_error(self) -> None:
        (self.root / "file").touch()
        with self.assertRaises(NotADirectoryError):
            list_folder(self.root / "file")

    def test_the_lifecycle_walk_of_the_infrastructure_folder_fails_on_a_folder_it_cannot_read(self) -> None:
        inner = self.root / "inner"
        inner.mkdir()
        (inner / "bucket.tf").write_text('lifecycle_configuration { rule { id = "x" } }\n')
        inner.chmod(0)
        self.addCleanup(inner.chmod, 0o700)
        if os.access(inner, os.R_OK):
            self.skipTest("this account reads every folder")
        with self.assertRaises(PermissionError):
            declared_problems(self.root)


class ScanTests(unittest.TestCase):
    def test_no_module_lists_a_folder_with_pathlib_outside_the_helper(self) -> None:
        found = {}
        for name in tracked_sources():
            if name in OWN_FILES:
                continue
            lines = [number for number, line in enumerate((ROOT / name).read_text().splitlines(), 1) if LISTING_CALL.search(line)]
            if lines:
                found[name] = lines
        self.assertEqual({}, found)

    def test_every_folder_walk_raises_on_an_unreadable_folder(self) -> None:
        found = {}
        for name in tracked_sources():
            lines = [number for number, line in enumerate((ROOT / name).read_text().splitlines(), 1) if "os.walk(" in line and "onerror=" not in line]
            if lines:
                found[name] = lines
        self.assertEqual({}, found)


if __name__ == "__main__":
    unittest.main()
