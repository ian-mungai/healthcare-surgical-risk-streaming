"""A folder that cannot be read is an error, never an empty listing.

``pathlib``'s ``glob``, ``rglob`` and ``iterdir`` on a folder that is not readable return nothing, so an unreadable runs folder looked empty and a new
run ID was chosen twice. ``list_folder`` raises instead, and the scan below holds every other reader of a run, inbox or input folder to it.
"""

from __future__ import annotations

import contextlib
import errno
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from replay.service.serve import next_request
from scripts.ingestion.folder import MAX_LINKS, list_files, list_folder, searched_folders
from scripts.ingestion.record_load import earlier_record
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

    def test_a_folder_behind_an_ancestor_that_cannot_be_searched_is_an_error_in_both_modes(self) -> None:
        ancestor = self.root / "ancestor"
        (ancestor / "runs").mkdir(parents=True)
        ancestor.chmod(0)
        self.addCleanup(ancestor.chmod, 0o700)
        if os.access(ancestor, os.X_OK):
            self.skipTest("this account searches every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                list_folder(ancestor / "runs", "*", recursive=recursive)

    def test_a_missing_folder_under_a_parent_that_can_be_searched_but_not_read_is_an_error_in_both_modes(self) -> None:
        parent = self.root / "parent"
        parent.mkdir()
        parent.chmod(0o100)
        self.addCleanup(parent.chmod, 0o700)
        if os.access(parent, os.R_OK):
            self.skipTest("this account reads every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                list_folder(parent / "absent", recursive=recursive)

    def test_a_missing_folder_under_a_higher_ancestor_that_can_be_searched_but_not_read_is_an_error_in_both_modes(self) -> None:
        higher = self.root / "higher"
        (higher / "middle").mkdir(parents=True)
        higher.chmod(0o100)
        self.addCleanup(higher.chmod, 0o700)
        if os.access(higher, os.R_OK):
            self.skipTest("this account reads every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                list_folder(higher / "middle" / "absent", recursive=recursive)

    def test_a_missing_folder_reached_through_a_link_is_an_error_when_an_ancestor_of_the_real_path_cannot_be_read(self) -> None:
        """Join round 2b, finding 2: the ancestors were checked as written, so a link that skips an unreadable folder made the real path look empty."""
        hidden = self.root / "hidden"
        (hidden / "inner").mkdir(parents=True)
        (self.root / "alias").symlink_to(hidden / "inner")
        hidden.chmod(0o100)
        self.addCleanup(hidden.chmod, 0o700)
        if os.access(hidden, os.R_OK):
            self.skipTest("this account reads every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive, route="real path"), self.assertRaises(PermissionError):
                list_folder(hidden / "inner" / "absent", recursive=recursive)
            with self.subTest(recursive=recursive, route="alias"), self.assertRaises(PermissionError):
                list_folder(self.root / "alias" / "absent", recursive=recursive)

    def test_a_missing_folder_reached_through_a_link_is_an_error_when_an_ancestor_as_written_cannot_be_read(self) -> None:
        (self.root / "real" / "inner").mkdir(parents=True)
        locked = self.root / "locked"
        locked.mkdir()
        (locked / "alias").symlink_to(self.root / "real" / "inner")
        locked.chmod(0o100)
        self.addCleanup(locked.chmod, 0o700)
        if os.access(locked, os.R_OK):
            self.skipTest("this account reads every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                list_folder(locked / "alias" / "absent", recursive=recursive)

    def two_hop_alias(self, *, relative: bool) -> Path:
        """``alias`` links to ``hidden/hop`` and ``hidden/hop`` links to ``real/inner``: ``hidden`` is on neither the path as written nor the real path."""
        base = self.root / ("relative" if relative else "absolute")
        (base / "real" / "inner").mkdir(parents=True)
        (base / "hidden").mkdir()
        (base / "hidden" / "hop").symlink_to("../real/inner" if relative else base / "real" / "inner")
        (base / "alias").symlink_to("hidden/hop" if relative else base / "hidden" / "hop")
        return base / "alias" / "absent"

    def test_a_missing_folder_reached_through_two_links_is_an_error_when_the_folder_of_a_link_can_be_searched_but_not_read(self) -> None:
        for relative in (False, True):
            with self.subTest(relative=relative):
                missing = self.two_hop_alias(relative=relative)
                hidden = missing.parent.parent / "hidden"
                hidden.chmod(0o100)
                self.addCleanup(hidden.chmod, 0o700)
                if os.access(hidden, os.R_OK):
                    self.skipTest("this account reads every folder")
                for recursive in (False, True):
                    with self.subTest(recursive=recursive), self.assertRaises(PermissionError):
                        list_folder(missing, recursive=recursive)
                    with self.subTest(recursive=recursive, files=True), self.assertRaises(PermissionError):
                        list_files(missing, recursive=recursive)
                hidden.chmod(0o700)

    def test_a_missing_folder_reached_through_two_links_under_readable_folders_stays_empty(self) -> None:
        for relative in (False, True):
            with self.subTest(relative=relative):
                missing = self.two_hop_alias(relative=relative)
                for recursive in (False, True):
                    self.assertEqual([], list_folder(missing, recursive=recursive))
                    self.assertEqual([], list_files(missing, recursive=recursive))

    def test_a_chain_of_more_than_forty_links_is_an_error_and_not_an_empty_listing(self) -> None:
        (self.root / "real").mkdir()
        previous = self.root / "real"
        for number in range(41):
            link = self.root / f"link{number}"
            link.symlink_to(previous)
            previous = link
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(OSError):
                list_folder(previous / "absent", recursive=recursive)
        shorter = self.root / "link5" / "absent"
        self.assertEqual([], list_folder(shorter))

    def test_the_hop_by_hop_search_names_every_folder_and_stops_after_the_limit_of_links(self) -> None:
        base = self.root.resolve()
        (base / "real").mkdir()
        previous = base / "real"
        chain: list[Path] = []
        for number in range(MAX_LINKS + 1):
            link = base / f"hop{number}"
            link.symlink_to(previous)
            chain.append(link)
            previous = link
        self.assertEqual(40, MAX_LINKS)
        self.assertIn(base, searched_folders(chain[MAX_LINKS - 1] / "absent"))
        with self.assertRaises(OSError) as caught:
            searched_folders(chain[MAX_LINKS] / "absent")
        self.assertEqual(errno.ELOOP, caught.exception.errno)

    def test_a_link_that_loops_is_an_error_and_not_an_empty_listing(self) -> None:
        (self.root / "loop").symlink_to("loop")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(OSError):
                list_folder(self.root / "loop" / "absent", recursive=recursive)

    def test_a_missing_folder_reached_through_a_link_under_readable_ancestors_on_both_routes_stays_empty(self) -> None:
        (self.root / "real" / "inner").mkdir(parents=True)
        (self.root / "alias").symlink_to(self.root / "real" / "inner")
        for recursive in (False, True):
            self.assertEqual([], list_folder(self.root / "alias" / "absent", recursive=recursive))

    def test_a_missing_folder_under_a_parent_that_can_be_read_but_not_searched_is_an_error_in_both_modes(self) -> None:
        parent = self.root / "readable"
        parent.mkdir()
        parent.chmod(0o400)
        self.addCleanup(parent.chmod, 0o700)
        if os.access(parent, os.X_OK):
            self.skipTest("this account searches every folder")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(OSError):
                list_folder(parent / "absent", recursive=recursive)

    def test_the_files_listing_and_a_missing_folder_under_a_readable_chain_stay_empty(self) -> None:
        (self.root / "chain" / "inner").mkdir(parents=True)
        for recursive in (False, True):
            self.assertEqual([], list_folder(self.root / "chain" / "inner" / "absent", recursive=recursive))
            self.assertEqual([], list_files(self.root / "chain" / "inner" / "absent", recursive=recursive))

    def test_only_a_missing_final_component_holds_nothing_in_both_modes(self) -> None:
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                self.assertEqual([], list_folder(self.root / "absent", recursive=recursive))

    def test_a_missing_ancestor_is_an_error_in_both_modes(self) -> None:
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(FileNotFoundError):
                list_folder(self.root / "absent" / "deeper", recursive=recursive)

    def test_a_link_to_nothing_as_an_ancestor_is_an_error_in_both_modes(self) -> None:
        (self.root / "dangling").symlink_to(self.root / "nowhere")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(OSError):
                list_folder(self.root / "dangling" / "runs", recursive=recursive)

    def test_a_link_to_a_folder_as_an_ancestor_or_as_the_folder_is_read(self) -> None:
        (self.root / "real" / "runs").mkdir(parents=True)
        (self.root / "real" / "runs" / "a_run").touch()
        (self.root / "link").symlink_to(self.root / "real")
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                self.assertEqual([self.root / "link" / "runs" / "a_run"], list_folder(self.root / "link" / "runs", recursive=recursive))
                self.assertEqual([self.root / "link" / "runs"], list_folder(self.root / "link", "runs", recursive=False))

    def test_a_file_as_an_ancestor_is_an_error_in_both_modes(self) -> None:
        (self.root / "file").touch()
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(NotADirectoryError):
                list_folder(self.root / "file" / "runs", recursive=recursive)

    def test_a_link_to_nothing_in_place_of_the_folder_is_an_error_in_both_modes(self) -> None:
        (self.root / "dangling").symlink_to(self.root / "nowhere")
        for recursive in (False, True):
            with self.subTest(recursive=recursive), self.assertRaises(OSError):
                list_folder(self.root / "dangling", recursive=recursive)

    def test_a_listed_file_that_cannot_be_examined_is_an_error_and_is_not_skipped(self) -> None:
        inbox = self.root / "inbox"
        inbox.mkdir()
        (inbox / "request.json").write_text("{}")
        inbox.chmod(0o400)
        self.addCleanup(inbox.chmod, 0o700)
        if os.access(inbox / "request.json", os.R_OK):
            self.skipTest("this account examines every file")
        with self.assertRaises(PermissionError):
            list_files(inbox, "*.json")

    def test_the_inbox_of_the_replay_raises_for_a_request_that_it_cannot_examine(self) -> None:
        inbox = self.root / "inbox"
        inbox.mkdir()
        (inbox / "request.json").write_text("{}")
        inbox.chmod(0o400)
        self.addCleanup(inbox.chmod, 0o700)
        if os.access(inbox / "request.json", os.R_OK):
            self.skipTest("this account examines every file")
        with self.assertRaises(PermissionError):
            next_request(inbox)

    def test_a_record_file_that_cannot_be_read_is_an_error_and_not_a_record_that_is_not_there(self) -> None:
        record = self.root / "run_one.json"
        record.write_text("{}")
        record.chmod(0)
        self.addCleanup(record.chmod, 0o600)
        if os.access(record, os.R_OK):
            self.skipTest("this account reads every file")
        with self.assertRaises(PermissionError):
            earlier_record(self.root, "a" * 64)

    def test_only_files_are_listed_as_files(self) -> None:
        (self.root / "a.json").write_text("{}")
        (self.root / "b.json").mkdir()
        (self.root / "c.json").symlink_to(self.root / "a.json")
        self.assertEqual([self.root / "a.json", self.root / "c.json"], list_files(self.root, "*.json"))

    def test_a_file_that_vanishes_between_the_listing_and_the_examination_is_not_listed(self) -> None:
        (self.root / "a.json").write_text("{}")
        (self.root / "b.json").write_text("{}")
        real = os.lstat

        def vanishing(path: str | Path) -> os.stat_result:
            if Path(path).name == "a.json":
                raise FileNotFoundError(path)
            return real(path)

        with mock.patch("scripts.ingestion.folder.os.lstat", vanishing):
            self.assertEqual([self.root / "b.json"], list_files(self.root, "*.json"))

    def test_a_link_to_nothing_among_the_files_is_an_error(self) -> None:
        (self.root / "x.json").symlink_to(self.root / "nowhere")
        with self.assertRaises(OSError):
            list_files(self.root, "*.json")

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

    def test_no_module_examines_a_listed_entry_with_a_call_that_says_no_for_an_error(self) -> None:
        found = {}
        for name in tracked_sources():
            if name == "scripts/ingestion/folder.py":
                continue
            lines = [number for number, line in enumerate((ROOT / name).read_text().splitlines(), 1) if "list_folder(" in line and ".is_file()" in line]
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
