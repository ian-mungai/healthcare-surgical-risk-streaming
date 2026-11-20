"""``make setup`` keeps the working-tree privacy hook when a hook that pre-commit does not own already exists.

``pre-commit install`` moves such a hook to ``pre-commit.legacy``, over the scanner that ``pre-commit run`` calls first.
Failure modes covered here:

- The setup target installs the scanner before it runs ``pre-commit install``, so an older hook replaces it.
- The check passes while the legacy hook is something other than the scanner.
- The real ``pre-commit install`` in a repository with an older hook, then the installer's hook step, leaves the scanner in place.
"""

from __future__ import annotations

import sys
import unittest
from unittest import mock

from scripts.process import run_command
from scripts.quality import install_tools, worktree_scan_hook
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository

OLDER_HOOK = "#!/bin/sh\necho older hook\n"


class SetupOrderTest(unittest.TestCase):
    def test_the_setup_target_runs_pre_commit_install_before_the_installer(self) -> None:
        lines = run_command("make", ["--dry-run", "--no-print-directory", "setup"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout.splitlines()
        install = [number for number, line in enumerate(lines) if "pre-commit install" in line]
        installer = [number for number, line in enumerate(lines) if "scripts.quality.install_tools" in line]
        self.assertEqual(1, len(install), lines)
        self.assertEqual(1, len(installer), lines)
        self.assertLess(install[0], installer[0])


class HookMigrationTest(TemporaryRepository):
    def hooks(self) -> str:
        return str(self.repo / ".git" / "hooks")

    def pre_commit_install(self) -> None:
        run_command(sys.executable, ["-m", "pre_commit", "install"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)

    def write_older_hook(self) -> None:
        path = self.repo / ".git" / "hooks" / "pre-commit"
        path.write_text(OLDER_HOOK)
        path.chmod(0o755)

    def settle(self, *, check: bool) -> bool:
        with mock.patch.object(install_tools, "ROOT", self.repo):
            return install_tools.settle_hook(check=check)

    def test_the_scanner_installed_after_pre_commit_install_survives_an_older_hook(self) -> None:
        self.write_older_hook()
        self.pre_commit_install()
        self.assertEqual(OLDER_HOOK, (self.repo / ".git" / "hooks" / "pre-commit.legacy").read_text())
        self.assertTrue(self.settle(check=False))
        self.assertTrue(worktree_scan_hook.installed(self.repo))
        self.assertTrue(self.settle(check=True))
        self.assertIn("hook-impl", (self.repo / ".git" / "hooks" / "pre-commit").read_text())

    def test_the_check_fails_when_the_legacy_hook_is_an_older_hook_and_not_the_scanner(self) -> None:
        worktree_scan_hook.install(self.repo)
        self.write_older_hook()
        self.pre_commit_install()
        self.assertEqual(OLDER_HOOK, (self.repo / ".git" / "hooks" / "pre-commit.legacy").read_text())
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        self.assertFalse(self.settle(check=True))


if __name__ == "__main__":
    unittest.main()
