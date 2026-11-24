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
from collections.abc import Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True)
class LegacyHook:
    """A hook that runs from the ``<stage>.legacy`` file that pre-commit calls before it hides anything, with the older hook kept beside it."""

    name: str
    kept_name: str
    text: str
    earlier_texts: tuple[str, ...] = ()


SCANNER = LegacyHook(HOOK_NAME, KEPT_NAME, HOOK_TEXT, EARLIER_HOOK_TEXTS)


def hook_path(root: Path, hook: LegacyHook = SCANNER, env: Mapping[str, str] | None = None) -> Path:
    """The hook's file. With ``env``, Git resolves the hook folder in that environment alone, so a repository with its own Git environment, such as a scratch
    repository, does not read the caller's global ``core.hooksPath``."""
    return root / git("rev-parse", "--git-path", "hooks", cwd=root, env=env).strip() / hook.name


def installed(root: Path, hook: LegacyHook = SCANNER, env: Mapping[str, str] | None = None) -> bool:
    path = hook_path(root, hook, env)
    return path.is_file() and path.read_bytes() == hook.text.encode() and stat.S_IMODE(path.stat().st_mode) == HOOK_MODE


def is_scanner(path: Path, hook: LegacyHook = SCANNER) -> bool:
    """Whether the file holds exactly a hook text that this repository has shipped, and so is not the owner's hook."""
    return path.is_file() and path.read_bytes() in (hook.text.encode(), *(text.encode() for text in hook.earlier_texts))


def install(root: Path, hook: LegacyHook = SCANNER, env: Mapping[str, str] | None = None) -> bool:
    """Write the hook and return True, or return False when it is already installed.

    An older hook at the hook's path is moved to ``<name>.previous`` first, and ``KeptHookError`` is raised when that name is taken.
    """
    if installed(root, hook, env):
        return False
    path = hook_path(root, hook, env)
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(path) and not is_scanner(path, hook):
        kept = path.with_name(hook.kept_name)
        if os.path.lexists(kept):
            raise KeptHookError(
                f"{path.name} holds an older hook and {kept.name} already holds another: compare the two files in {path.parent}, "
                f"move one away as the owner, then run setup again; nothing was overwritten"
            )
        os.rename(path, kept)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f"{hook.name}.")
    os.close(descriptor)
    pending = Path(name)
    try:
        pending.write_text(hook.text)
        pending.chmod(HOOK_MODE)
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)
    return True
