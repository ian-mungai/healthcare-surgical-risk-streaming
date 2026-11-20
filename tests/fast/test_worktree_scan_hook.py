"""The Git hook that scans the working tree before pre-commit hides unstaged edits of tracked files.

The planted-fault run in ``tests/e2e/commit_guards.py`` proves the hook through a real commit. Failure modes covered here:

- The installer writes nothing, or a hook that is not executable, so Git skips it.
- A rerun rewrites an installed hook, or does not repair a changed one.
- The check reports a missing or changed hook as installed.
- The hook is written to the wrong repository, when run from a linked worktree or from inside another Git command's hook
  environment (``GIT_DIR`` is set).
- A write that stops before the hook is complete leaves the earlier hook in place, and the next setup succeeds. It never treats the
  half-written file as the owner's own hook.
"""

from __future__ import annotations

import os
import stat
import unittest
from pathlib import Path
from unittest import mock

from scripts.quality import worktree_scan_hook
from tests.fast.git_fixture import TemporaryRepository


class WorktreeScanHookTest(TemporaryRepository):
    def hook(self) -> Path:
        return self.repo / ".git" / "hooks" / "pre-commit.legacy"

    def test_install_writes_an_executable_hook_that_runs_the_scan(self) -> None:
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertTrue(self.hook().stat().st_mode & stat.S_IXUSR)
        self.assertIn("-m scripts.quality.repo_checks privacy-scan --all", self.hook().read_text())

    def test_a_rerun_changes_nothing(self) -> None:
        worktree_scan_hook.install(self.repo)
        before = self.hook().stat().st_mtime_ns
        self.assertFalse(worktree_scan_hook.install(self.repo))
        self.assertEqual(before, self.hook().stat().st_mtime_ns)

    def test_a_changed_or_unexecutable_hook_is_not_installed_and_is_repaired(self) -> None:
        worktree_scan_hook.install(self.repo)
        self.hook().write_text("#!/bin/sh\nexit 0\n")
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.hook().chmod(0o644)
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))

    def test_a_different_hook_at_the_path_is_kept_beside_the_scanner_and_not_overwritten(self) -> None:
        worktree_scan_hook.install(self.repo)
        self.hook().write_text("#!/bin/sh\nexit 0\n")
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertEqual("#!/bin/sh\nexit 0\n", (self.hook().parent / "pre-commit.legacy.previous").read_text())
        self.assertTrue(worktree_scan_hook.installed(self.repo))

    def test_a_dangling_link_at_the_path_is_kept_and_nothing_is_written_through_it(self) -> None:
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        gone = self.repo / "gone_hook"
        self.hook().symlink_to(gone)
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertFalse(gone.exists())
        self.assertTrue((self.hook().parent / "pre-commit.legacy.previous").is_symlink())
        self.assertTrue(worktree_scan_hook.installed(self.repo))

    def stop_half_way(self, path: Path, data: str, *arguments: object, **keywords: object) -> int:
        with open(path, "w") as handle:
            handle.write(data[:25])
        raise OSError("the write stopped")

    def test_a_write_that_stops_half_way_never_reaches_the_hook_path_and_the_next_setup_succeeds(self) -> None:
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        self.hook().write_text("#!/bin/sh\nexit 0\n")
        with mock.patch.object(Path, "write_text", autospec=True, side_effect=self.stop_half_way), self.assertRaises(OSError):
            worktree_scan_hook.install(self.repo)
        self.assertFalse(self.hook().exists())
        self.assertEqual("#!/bin/sh\nexit 0\n", (self.hook().parent / "pre-commit.legacy.previous").read_text())
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertEqual("#!/bin/sh\nexit 0\n", (self.hook().parent / "pre-commit.legacy.previous").read_text())

    def test_a_scanner_of_an_earlier_version_stays_whole_when_the_write_stops_and_the_next_setup_replaces_it(self) -> None:
        earlier = worktree_scan_hook.EARLIER_HOOK_TEXTS[0]
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        self.hook().write_text(earlier)
        with mock.patch.object(Path, "write_text", autospec=True, side_effect=self.stop_half_way), self.assertRaises(OSError):
            worktree_scan_hook.install(self.repo)
        self.assertEqual(earlier, self.hook().read_text())
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertFalse((self.hook().parent / "pre-commit.legacy.previous").exists())

    def test_a_stop_before_the_rename_leaves_the_earlier_state_and_the_next_setup_overwrites_the_leftover(self) -> None:
        earlier = worktree_scan_hook.EARLIER_HOOK_TEXTS[0]
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        self.hook().write_text(earlier)
        with mock.patch.object(os, "replace", side_effect=OSError("the rename stopped")), self.assertRaises(OSError):
            worktree_scan_hook.install(self.repo)
        self.assertEqual(earlier, self.hook().read_text())
        leftovers = [path for path in self.hook().parent.iterdir() if path.name.startswith("pre-commit.legacy") and path != self.hook()]
        self.assertEqual(1, len(leftovers))
        leftovers[0].write_text("a leftover of a stopped write")
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertEqual([self.hook()], [path for path in self.hook().parent.iterdir() if path.name.startswith("pre-commit.legacy")])

    def test_a_link_left_at_the_pending_name_is_removed_and_the_file_it_points_to_is_untouched(self) -> None:
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        other = self.repo / "other_file.txt"
        other.write_text("kept")
        pending = self.hook().parent / worktree_scan_hook.PENDING_NAME
        pending.symlink_to(other)
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertEqual("kept", other.read_text())
        self.assertFalse(self.hook().is_symlink())
        self.assertFalse(os.path.lexists(pending))

    def test_the_file_is_whole_and_executable_when_it_is_renamed_into_place(self) -> None:
        real_replace = os.replace
        seen: list[tuple[int, str]] = []

        def look_then_replace(source: Path, target: Path) -> None:
            seen.append((stat.S_IMODE(Path(source).stat().st_mode), Path(source).read_text()))
            real_replace(source, target)

        with mock.patch.object(os, "replace", side_effect=look_then_replace):
            worktree_scan_hook.install(self.repo)
        self.assertEqual([(worktree_scan_hook.HOOK_MODE, worktree_scan_hook.HOOK_TEXT)], seen)

    def test_the_callers_git_environment_does_not_redirect_the_hook(self) -> None:
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.other_repository())}):
            self.assertTrue(worktree_scan_hook.install(self.repo))
            self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertTrue(self.hook().is_file())

    def test_a_linked_worktree_uses_the_shared_hooks_folder(self) -> None:
        self.git("commit", "--quiet", "--allow-empty", "-m", "chore: start")
        linked = self.repo.parent / "linked"
        self.git("worktree", "add", "--quiet", str(linked))
        worktree_scan_hook.install(linked)
        self.assertTrue(self.hook().is_file())
        self.assertFalse((linked / ".git" / "hooks").exists())


if __name__ == "__main__":
    unittest.main()
