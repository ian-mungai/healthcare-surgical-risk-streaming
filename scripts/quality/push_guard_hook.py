"""The Git hook that refuses a push while tracked files have uncommitted changes, before pre-commit hides them.

Pre-commit stashes every unstaged change while its pre-push hooks run, so the push gate cannot see one. Pre-commit runs a file named ``pre-push.legacy`` in the
Git hooks folder first, before it stashes anything. That file is this hook. It refuses the push when ``git status`` lists a tracked file as changed, in the
index or in the working tree, because the checks of the gate read the working tree and the pushed commit must be the one that they read.

``pre-commit install`` moves an older pre-push hook that it does not own to that same name. The installer keeps such a hook as ``pre-push.legacy.previous``
and this hook runs it after its own check, with the arguments and the standard input that Git gave. It refuses, and overwrites nothing, when a kept hook already
exists. The file is written the way ``worktree_scan_hook`` writes the scan hook.
"""

from __future__ import annotations

from pathlib import Path

from scripts.quality import worktree_scan_hook
from scripts.quality.push_gate import TRACKED

HOOK_NAME = "pre-push.legacy"
KEPT_NAME = "pre-push.legacy.previous"
MESSAGE = "tracked files have uncommitted changes, so the checks would not read the pushed commit: commit or restore them"
HOOK_TEXT = (
    "#!/bin/sh\n"
    'changed="$(git status --porcelain --untracked-files=no)"\n'
    'if [ -n "$changed" ]; then\n'
    f'  echo "push gate stopped by {TRACKED}: {MESSAGE}" >&2\n'
    '  echo "$changed" >&2\n'
    "  exit 1\n"
    "fi\n"
    f'previous="$(dirname "$0")/{KEPT_NAME}"\n'
    'if [ -x "$previous" ]; then exec "$previous" "$@"; fi\n'
)
PUSH_GUARD = worktree_scan_hook.LegacyHook(HOOK_NAME, KEPT_NAME, HOOK_TEXT)


def installed(root: Path) -> bool:
    return worktree_scan_hook.installed(root, PUSH_GUARD)


def install(root: Path) -> bool:
    """Write the hook and return True, or return False when it is already installed. An older hook that cannot be kept raises ``KeptHookError``."""
    return worktree_scan_hook.install(root, PUSH_GUARD)
