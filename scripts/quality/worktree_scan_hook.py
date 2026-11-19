"""The Git hook that scans the whole working tree for private values before pre-commit runs.

pre-commit hides every unstaged edit of a tracked file while its hooks run, so no hook sees that content. pre-commit runs
a file named ``pre-commit.legacy`` in the Git hooks folder first, before it hides anything. That file is this hook.
"""

from __future__ import annotations

import stat
from pathlib import Path

from scripts.quality.repo_checks import git

HOOK_NAME = "pre-commit.legacy"
HOOK_TEXT = "#!/bin/sh\nexec .venv/bin/python -m scripts.quality.repo_checks privacy-scan --all\n"
HOOK_MODE = 0o755


def hook_path(root: Path) -> Path:
    return root / git("rev-parse", "--git-path", "hooks", cwd=root).strip() / HOOK_NAME


def installed(root: Path) -> bool:
    path = hook_path(root)
    return path.is_file() and path.read_text() == HOOK_TEXT and stat.S_IMODE(path.stat().st_mode) == HOOK_MODE


def install(root: Path) -> bool:
    """Write the hook and return True, or return False when it is already installed."""
    if installed(root):
        return False
    path = hook_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HOOK_TEXT)
    path.chmod(HOOK_MODE)
    return True
