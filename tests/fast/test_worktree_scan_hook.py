"""The Git hook that scans the working tree before pre-commit hides unstaged edits of tracked files.

The planted-fault run in ``tests/e2e/commit_guards.py`` proves the hook through a real commit. Failure modes covered here:

- The installer writes nothing, or a hook that is not executable, so Git skips it.
- A rerun rewrites an installed hook, or does not repair a changed one.
- The check reports a missing or changed hook as installed.
- The hook is written to the wrong repository, when run from a linked worktree or from inside another Git command's hook
  environment (``GIT_DIR`` is set).
- A repository with its own Git environment (``env``) writes its hook where that environment says, not where the caller's global ``core.hooksPath`` says.
- A write that stops before the hook is complete leaves the earlier hook in place, and the next setup succeeds. It never treats the
  half-written file as the owner's own hook.
- The hooks folder is a link (to a folder, to nothing or to a file). A link to a folder is followed, so the owner's hook is kept beside the
  scanner in the real folder. A link that leads to no folder is refused before the owner's hook moves, and nothing changes.
"""

from __future__ import annotations

import os
import shutil
import stat
import unittest
from pathlib import Path
from typing import Any
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

    def global_hooks_path(self) -> Path:
        """Point the caller's global Git config at a shared hook folder, as a developer's does, and return the folder."""
        shared = self.repo.parent / "shared_hooks"
        shared.mkdir()
        home = self.repo.parent / "caller_home"
        home.mkdir()
        (home / ".gitconfig").write_text(f"[core]\n\thooksPath = {shared}\n")
        patcher = mock.patch.dict(os.environ, {"HOME": str(home)})
        patcher.start()
        self.addCleanup(patcher.stop)
        return shared

    def test_a_repository_with_its_own_environment_ignores_the_callers_global_hooks_path(self) -> None:
        shared = self.global_hooks_path()
        self.assertTrue(worktree_scan_hook.install(self.repo, env=self.env))
        self.assertTrue(self.hook().is_file())
        self.assertTrue(worktree_scan_hook.installed(self.repo, env=self.env))
        self.assertEqual(self.hook().resolve(), worktree_scan_hook.hook_path(self.repo, env=self.env))  # the hooks folder is taken by its real path
        self.assertEqual([], list(shared.iterdir()))

    def test_without_an_environment_the_callers_global_hooks_path_is_followed(self) -> None:
        shared = self.global_hooks_path()
        self.assertEqual(shared.resolve() / "pre-commit.legacy", worktree_scan_hook.hook_path(self.repo))

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

    def stop_half_way(self) -> Any:
        """Patch ``os.fdopen`` so the handle of the helper's temporary file takes the first 25 characters and then stops."""
        real = os.fdopen

        class HalfWay:
            def __init__(self, descriptor: int, mode: str) -> None:
                self.handle = real(descriptor, mode)

            def __enter__(self) -> HalfWay:
                return self

            def __exit__(self, *details: object) -> None:
                self.handle.close()

            def fileno(self) -> int:
                return self.handle.fileno()

            def write(self, data: bytes) -> int:
                self.handle.write(data[:25])
                self.handle.flush()
                raise OSError("the write stopped")

            def flush(self) -> None:
                self.handle.flush()

        return mock.patch("scripts.ingestion.output_file.os.fdopen", HalfWay)

    def test_a_write_that_stops_half_way_never_reaches_the_hook_path_and_the_next_setup_succeeds(self) -> None:
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        self.hook().write_text("#!/bin/sh\nexit 0\n")
        with self.stop_half_way(), self.assertRaises(OSError):
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
        with self.stop_half_way(), self.assertRaises(OSError):
            worktree_scan_hook.install(self.repo)
        self.assertEqual(earlier, self.hook().read_text())
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertFalse((self.hook().parent / "pre-commit.legacy.previous").exists())

    def hook_folder_names(self) -> list[str]:
        return sorted(path.name for path in self.hook().parent.iterdir())

    def test_a_stop_before_the_rename_leaves_the_earlier_state_and_no_other_file(self) -> None:
        earlier = worktree_scan_hook.EARLIER_HOOK_TEXTS[0]
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        self.hook().write_text(earlier)
        before = self.hook_folder_names()
        with mock.patch.object(os, "replace", side_effect=OSError("the rename stopped")), self.assertRaises(OSError):
            worktree_scan_hook.install(self.repo)
        self.assertEqual(earlier, self.hook().read_text())
        self.assertEqual(before, self.hook_folder_names())
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertEqual(before, self.hook_folder_names())

    def test_every_file_and_link_already_in_the_hook_folder_is_left_as_it_is(self) -> None:
        self.hook().parent.mkdir(parents=True, exist_ok=True)
        other = self.repo / "other_file.txt"
        other.write_text("kept")
        folder = self.hook().parent
        (folder / "pre-commit.legacy.pending").write_text("the owner's file")
        (folder / "pre-commit.pending").symlink_to(other)
        (folder / "pre-commit.sample").write_text("a sample")
        before = self.hook_folder_names()
        for stopped in (True, False):
            if stopped:
                with mock.patch.object(os, "replace", side_effect=OSError("the rename stopped")), self.assertRaises(OSError):
                    worktree_scan_hook.install(self.repo)
            else:
                self.assertTrue(worktree_scan_hook.install(self.repo))
            self.assertEqual("the owner's file", (folder / "pre-commit.legacy.pending").read_text())
            self.assertTrue((folder / "pre-commit.pending").is_symlink())
            self.assertEqual("a sample", (folder / "pre-commit.sample").read_text())
            self.assertEqual("kept", other.read_text())
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertFalse(self.hook().is_symlink())
        self.assertEqual(sorted([*before, self.hook().name]), self.hook_folder_names())

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

    def link_hooks_folder_to(self, target: Path) -> Path:
        """Replace the repository's hooks folder with a link to ``target`` and return the link."""
        link = self.hook().parent
        shutil.rmtree(link)
        link.symlink_to(target)
        return link

    def test_a_hooks_folder_that_is_a_link_to_a_folder_keeps_the_owners_hook_and_writes_the_scanner_in_the_real_folder(self) -> None:
        real = self.repo.parent / "shared_hooks"
        real.mkdir()
        (real / "pre-commit.legacy").write_text("#!/bin/sh\nexit 0\n")
        link = self.link_hooks_folder_to(real)
        self.assertTrue(worktree_scan_hook.install(self.repo))
        self.assertTrue(link.is_symlink())
        self.assertEqual("#!/bin/sh\nexit 0\n", (real / "pre-commit.legacy.previous").read_text())
        self.assertEqual(worktree_scan_hook.HOOK_TEXT, (real / "pre-commit.legacy").read_text())
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertEqual(["pre-commit.legacy", "pre-commit.legacy.previous"], sorted(path.name for path in real.iterdir()))
        self.assertFalse(worktree_scan_hook.install(self.repo))

    def test_a_hooks_folder_that_leads_to_no_folder_is_refused_before_anything_moves(self) -> None:
        for name, is_file in (("gone", False), ("a_file", True)):
            with self.subTest(target=name):
                target = self.repo.parent / name
                if is_file:
                    target.write_text("not a folder")
                link = self.link_hooks_folder_to(target)
                with self.assertRaises(OSError):
                    worktree_scan_hook.install(self.repo)
                self.assertEqual(str(target), os.readlink(link))
                self.assertEqual(is_file, target.exists())
                if is_file:
                    self.assertEqual("not a folder", target.read_text())
                link.unlink()
                link.mkdir()


if __name__ == "__main__":
    unittest.main()
