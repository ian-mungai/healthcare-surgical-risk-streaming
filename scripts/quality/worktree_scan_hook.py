"""The Git hook that scans the whole working tree for private values before pre-commit runs.

pre-commit hides every unstaged edit of a tracked file while its hooks run, so no hook sees that content. pre-commit runs
a file named ``pre-commit.legacy`` in the Git hooks folder first, before it hides anything. That file is this hook.

``pre-commit install`` moves an older hook that it does not own to that same name. The installer keeps such a hook as
``pre-commit.legacy.previous``, and this hook runs the scan and then that kept hook. It refuses, and overwrites nothing,
when a kept hook already exists.

The installer writes the hook to a newly made temporary file beside the hook's path and renames it into place, so a write that stops
leaves no half-written hook at the hook's path. It removes only that temporary file and touches no other file in the folder.
"""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

from scripts.quality.repo_checks import git

HOOK_NAME = "pre-commit.legacy"
KEPT_NAME = "pre-commit.legacy.previous"
SCAN_COMMAND = ".venv/bin/python -m scripts.quality.repo_checks privacy-scan --all"
HOOK_TEXT = f'#!/bin/sh\n{SCAN_COMMAND} || exit $?\nprevious="$(dirname "$0")/{KEPT_NAME}"\nif [ -x "$previous" ]; then exec "$previous" "$@"; fi\n'
EARLIER_HOOK_TEXTS = ("#!/bin/sh\nexec .venv/bin/python -m scripts.quality.repo_checks privacy-scan --all\n",)
HOOK_MODE = 0o755


class KeptHookError(RuntimeError):
    """The older hook that pre-commit moved aside cannot be kept."""


def hook_path(root: Path) -> Path:
    return root / git("rev-parse", "--git-path", "hooks", cwd=root).strip() / HOOK_NAME


def installed(root: Path) -> bool:
    path = hook_path(root)
    return path.is_file() and path.read_bytes() == HOOK_TEXT.encode() and stat.S_IMODE(path.stat().st_mode) == HOOK_MODE


def is_scanner(path: Path) -> bool:
    """Whether the file holds exactly a scan hook text that this repository has shipped, and so is not the owner's hook."""
    return path.is_file() and path.read_bytes() in (HOOK_TEXT.encode(), *(text.encode() for text in EARLIER_HOOK_TEXTS))


def install(root: Path) -> bool:
    """Write the hook and return True, or return False when it is already installed.

    An older hook at the hook's path is moved to ``pre-commit.legacy.previous`` first, and ``KeptHookError`` is raised when that name is taken.
    """
    if installed(root):
        return False
    path = hook_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(path) and not is_scanner(path):
        kept = path.with_name(KEPT_NAME)
        if os.path.lexists(kept):
            raise KeptHookError(
                f"{path.name} holds an older hook and {kept.name} already holds another: compare the two files in {path.parent}, "
                f"move one away as the owner, then run setup again; nothing was overwritten"
            )
        os.rename(path, kept)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f"{HOOK_NAME}.")
    os.close(descriptor)
    pending = Path(name)
    try:
        pending.write_text(HOOK_TEXT)
        pending.chmod(HOOK_MODE)
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)
    return True
