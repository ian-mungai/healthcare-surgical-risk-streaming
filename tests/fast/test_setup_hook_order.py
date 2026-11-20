"""``make setup`` keeps the working-tree privacy hook when a hook that pre-commit does not own already exists.

``pre-commit install`` moves such a hook to ``pre-commit.legacy``, over the scanner that ``pre-commit run`` calls first.
Failure modes covered here:

- The setup target installs the scanner before it runs ``pre-commit install``, so an older hook replaces it.
- The check passes while the legacy hook is something other than the scanner.
- The real ``pre-commit install`` in a repository with an older hook, then the installer's hook step, leaves the scanner in place.
- The older hook that pre-commit moved to ``pre-commit.legacy`` stops running when the scanner replaces it. A real commit runs the scan, then the older hook.
- A second setup chains the scanner to itself, loops, or drops the older hook. A scan that fails or an older hook that fails lets the commit through.
- Setup cannot keep the older hook, because a kept one exists, and does not say so.
"""

from __future__ import annotations

import contextlib
import io
import stat
import sys
import unittest
from unittest import mock

from scripts.process import run_command
from scripts.quality import install_tools, worktree_scan_hook
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository

OLDER_HOOK = "#!/bin/sh\necho older hook\n"
MARKING_HOOK = '#!/bin/sh\necho older >> "$HOOK_LOG"\nexit "${OLDER_EXIT:-0}"\n'
SCAN_STUB = '#!/bin/sh\necho scan >> "$HOOK_LOG"\nexit "${SCAN_EXIT:-0}"\n'
OLD_SCANNER = "#!/bin/sh\nexec .venv/bin/python -m scripts.quality.repo_checks privacy-scan --all\n"


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


class OlderHookChainTest(TemporaryRepository):
    """The setup steps in a repository whose older hook leaves a marker when it runs, through real ``pre-commit install`` and real commits."""

    def setUp(self) -> None:
        super().setUp()
        self.log = self.repo.parent / "hook.log"
        self.env |= {"HOOK_LOG": str(self.log)}
        stub = self.repo / ".venv" / "bin" / "python"
        stub.parent.mkdir(parents=True)
        stub.write_text(SCAN_STUB)
        stub.chmod(0o755)
        (self.repo / ".pre-commit-config.yaml").write_text("repos: []\n")
        self.hooks = self.repo / ".git" / "hooks"

    def write_hook(self, name: str, text: str) -> None:
        (self.hooks / name).write_text(text)
        (self.hooks / name).chmod(0o755)

    def setup_steps(self) -> bool:
        run_command(sys.executable, ["-m", "pre_commit", "install"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(io.StringIO()):
            return install_tools.settle_hook(check=False)

    def commit(self, **environment: str) -> int:
        self.log.touch()
        result = run_command(
            "git", ["commit", "--quiet", "--allow-empty", "-m", "chore: x"], cwd=self.repo, env=self.env | environment, timeout=TIMEOUT_SECONDS
        )
        return result.returncode

    def ran(self) -> list[str]:
        text = self.log.read_text() if self.log.exists() else ""
        self.log.write_text("")
        return text.split()

    def test_a_commit_runs_the_scan_then_the_older_hook_that_pre_commit_moved_aside(self) -> None:
        self.write_hook("pre-commit", MARKING_HOOK)
        self.assertTrue(self.setup_steps())
        self.assertEqual(0, self.commit())
        self.assertEqual(["scan", "older"], self.ran())
        self.assertEqual(MARKING_HOOK, (self.hooks / "pre-commit.legacy.previous").read_text())
        self.assertEqual(0o755, stat.S_IMODE((self.hooks / "pre-commit.legacy.previous").stat().st_mode))

    def test_a_second_setup_keeps_the_older_hook_once_and_does_not_chain_the_scanner_to_itself(self) -> None:
        self.write_hook("pre-commit", MARKING_HOOK)
        self.setup_steps()
        self.commit()
        self.ran()
        self.setup_steps()
        self.assertEqual(MARKING_HOOK, (self.hooks / "pre-commit.legacy.previous").read_text())
        self.assertEqual(0, self.commit())
        self.assertEqual(["scan", "older"], self.ran())
        self.assertEqual(MARKING_HOOK, (self.hooks / "pre-commit.legacy.previous").read_text())
        self.assertNotIn("pre-commit.legacy ", (self.hooks / "pre-commit.legacy").read_text().replace("pre-commit.legacy.previous", ""))

    def test_a_failing_scan_blocks_the_commit_before_the_older_hook(self) -> None:
        self.write_hook("pre-commit", MARKING_HOOK)
        self.setup_steps()
        self.assertNotEqual(0, self.commit(SCAN_EXIT="1"))
        self.assertEqual(["scan"], self.ran())

    def test_a_failing_older_hook_blocks_the_commit(self) -> None:
        self.write_hook("pre-commit", MARKING_HOOK)
        self.setup_steps()
        self.assertNotEqual(0, self.commit(OLDER_EXIT="1"))
        self.assertEqual(["scan", "older"], self.ran())

    def test_with_no_older_hook_the_scan_runs_alone_and_nothing_is_kept(self) -> None:
        self.setup_steps()
        self.assertEqual(0, self.commit())
        self.assertEqual(["scan"], self.ran())
        self.assertFalse((self.hooks / "pre-commit.legacy.previous").exists())

    def test_the_scanner_that_an_earlier_setup_wrote_is_replaced_in_place_and_not_kept_as_an_older_hook(self) -> None:
        self.write_hook("pre-commit.legacy", OLD_SCANNER)
        self.assertTrue(self.setup_steps())
        self.assertEqual(worktree_scan_hook.HOOK_TEXT, (self.hooks / "pre-commit.legacy").read_text())
        self.assertFalse((self.hooks / "pre-commit.legacy.previous").exists())
        self.assertEqual(0, self.commit())
        self.assertEqual(["scan"], self.ran())

    def test_setup_fails_visibly_and_overwrites_nothing_when_an_older_hook_is_already_kept(self) -> None:
        self.write_hook("pre-commit.legacy.previous", "#!/bin/sh\necho kept first\n")
        self.write_hook("pre-commit", MARKING_HOOK)
        error = io.StringIO()
        run_command(sys.executable, ["-m", "pre_commit", "install"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(error):
            self.assertFalse(install_tools.settle_hook(check=False))
        self.assertIn("pre-commit.legacy.previous", error.getvalue())
        self.assertEqual("#!/bin/sh\necho kept first\n", (self.hooks / "pre-commit.legacy.previous").read_text())
        self.assertEqual(MARKING_HOOK, (self.hooks / "pre-commit.legacy").read_text())
        with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(io.StringIO()):
            self.assertFalse(install_tools.settle_hook(check=True))
        with (
            mock.patch.object(install_tools, "ROOT", self.repo),
            mock.patch.object(install_tools, "settle", return_value=True),
            mock.patch.object(sys, "argv", ["install_tools"]),
            contextlib.redirect_stderr(io.StringIO()),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(1, install_tools.main())


if __name__ == "__main__":
    unittest.main()
