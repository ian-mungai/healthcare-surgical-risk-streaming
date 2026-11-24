"""A temporary Git repository for fast tests that run the repository checks as a hook or CI would."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.process import CompletedProcess, run_command

ROOT = Path(__file__).resolve().parents[2]
TIMEOUT_SECONDS = 60


def clean_git_env() -> dict[str, str]:
    """The caller's environment without any ``GIT_*`` variable. A hook runs with ``GIT_DIR`` and ``GIT_INDEX_FILE`` set to the real repository, and a
    ``git init`` or ``git add`` that inherits them changes that repository (``git init`` makes it bare)."""
    return {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}


def git_in(folder: Path, *arguments: str, keep_newlines: bool = False) -> CompletedProcess[str]:
    """Run Git in ``folder`` with a clean environment. Every test that makes or changes a repository outside ``TemporaryRepository`` goes through here."""
    return run_command("git", arguments, cwd=folder, env=clean_git_env(), timeout=TIMEOUT_SECONDS, check=True, keep_newlines=keep_newlines)


def shared_core_settings() -> str:
    """The ``core.*`` settings of the shared Git config that every worktree reads: ``core.bare`` there makes the main checkout lose its work tree."""
    common = Path(git_in(ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip())
    return git_in(ROOT, "config", "--file", str(common / "config"), "--get-regexp", r"^core\.").stdout


# Taken when the first test module imports this one, which is before any test of the run starts.
SHARED_CORE_AT_START = shared_core_settings()


class TemporaryRepository(unittest.TestCase):
    def setUp(self) -> None:
        home = tempfile.TemporaryDirectory(prefix="repo_check_cli_")
        self.addCleanup(home.cleanup)
        self.repo = Path(home.name) / "repo"
        self.repo.mkdir()
        self.env = {
            "PATH": os.environ["PATH"],
            "HOME": home.name,
            "PYTHONPATH": str(ROOT),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        }
        self.git("init", "--quiet", "--initial-branch", "main")

    def other_repository(self) -> Path:
        """Make a second repository and return its Git folder, for a test that sets the caller's Git environment to it."""
        other = self.repo.parent / "other"
        other.mkdir()
        run_command("git", ["init", "--quiet"], cwd=other, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        return other / ".git"

    def git(self, *args: str) -> CompletedProcess[str]:
        return run_command("git", args, cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)

    def check(self, *args: str) -> CompletedProcess[str]:
        return run_command(sys.executable, ["-m", "scripts.quality.repo_checks", *args], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
