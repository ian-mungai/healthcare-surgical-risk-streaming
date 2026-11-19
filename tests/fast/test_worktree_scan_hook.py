"""The Git hook that scans the working tree before pre-commit hides unstaged edits of tracked files.

The planted-fault run in ``tests/e2e/commit_guards.py`` proves the hook through a real commit. Failure modes covered here:

- The installer writes nothing, or a hook that is not executable, so Git skips it.
- A rerun rewrites an installed hook, or does not repair a changed one.
- The check reports a missing or changed hook as installed.
- The hook is written to the wrong repository, when run from a linked worktree or from inside another Git command's hook
  environment (``GIT_DIR`` is set).
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
