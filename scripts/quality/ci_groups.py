"""The job groups of the CI workflow and the changed paths that select them, from the map in ``scripts/quality/ci_groups.json``.

The map lists, for each group, the path patterns whose files decide the group's outcome, each with its reason. ``every_group`` lists the
paths that change what the plan runs, and ``none`` the paths that only the always-running hooks check. A pattern is a ``fnmatch`` pattern
matched against the whole path, and its ``*`` also matches ``/``. A tracked file on no list is an error that a fast test reports, and a
changed path on no list selects every group.

The changed paths are the two-dot difference of the trees of the start commit and the checkout, without rename detection, so a rename
counts with both names, a deleted file counts and a rollback lists the files it restores. A range that cannot be verified selects every
group: an event without a range, a start commit that is missing or all zeros, a checkout that is not the commit of the run, and for a push a
start commit that is not an ancestor of the checkout (a force push).
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality.repo_checks import git

CONFIG = Path("scripts/quality/ci_groups.json")
COMMIT = re.compile(r"[0-9a-f]{40}")
EVENTS = ("push", "pull_request")
GIT_TIMEOUT_SECONDS = 60


class Unverified(Exception):
    """The range cannot be verified, so every group runs; the message is the reason."""


@dataclass(frozen=True)
class Decision:
    run: bool
    reason: str


def load(root: Path) -> dict[str, Any]:
    """The map of the checkout at ``root``."""
    config: dict[str, Any] = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    return config


def entry_for(path: str, entries: list[dict[str, str]]) -> dict[str, str] | None:
    return next((entry for entry in entries if fnmatch.fnmatchcase(path, entry["pattern"])), None)


def listed(config: dict[str, Any]) -> list[dict[str, str]]:
    return [*config["every_group"], *config["none"], *(entry for group in config["groups"].values() for entry in group["paths"])]


def unmapped(paths: list[str], config: dict[str, Any]) -> list[str]:
    """The paths that no list of the map names."""
    entries = listed(config)
    return [path for path in paths if entry_for(path, entries) is None]


def exit_code(root: Path, *args: str) -> int:
    env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    return run_command("git", args, cwd=root, env=env, timeout=GIT_TIMEOUT_SECONDS).returncode


def verify_range(root: Path, event: str, start: str, run_commit: str) -> None:
    """Raise ``Unverified`` unless ``start`` to the checkout is a range the plan can compare for ``event``."""
    if event not in EVENTS:
        raise Unverified(f"the event {event} has no range to compare")
    if not COMMIT.fullmatch(start) or set(start) == {"0"}:
        raise Unverified("the push has no earlier commit (a first push) or the event names no start commit")
    if exit_code(root, "cat-file", "-e", f"{start}^{{commit}}") != 0:
        raise Unverified(f"the start commit {start} is not in this checkout")
    head = git("rev-parse", "HEAD", cwd=root).strip()
    if head != run_commit:
        raise Unverified(f"the checkout {head} is not the commit of the run {run_commit or 'none'}")
    if event == "push":
        ancestor = exit_code(root, "merge-base", "--is-ancestor", start, head)
        if ancestor == 1:
            raise Unverified(f"the start commit {start} is not an ancestor of {head} (a force push)")
        if ancestor != 0:
            raise Unverified(f"Git could not compare {start} with {head}")


def changed_paths(root: Path, start: str, end: str) -> list[str]:
    """Every path whose content differs between the trees of ``start`` and ``end``: both names of a rename and every deleted file."""
    listed_paths = git("diff", "--name-only", "--no-renames", "-z", start, end, "--", cwd=root, keep_newlines=True)
    return sorted(set(listed_paths.split("\0")) - {""})


def widening(config: dict[str, Any], changed: list[str]) -> str | None:
    """The reason that ``changed`` selects every group, or None."""
    for path in changed:
        entry = entry_for(path, config["every_group"])
        if entry:
            return f"{path} changes what the plan runs ({entry['pattern']}: {entry['reason']})"
    missing = unmapped(changed, config)
    return f"{missing[0]} is on no list of {CONFIG}" if missing else None


def select(config: dict[str, Any], changed: list[str] | None, everything: str) -> dict[str, Decision]:
    """The decision for each group. ``changed`` None means every group runs for the reason ``everything``."""
    decisions = {}
    for name, group in config["groups"].items():
        if group.get("always"):
            decisions[name] = Decision(True, group["reason"])
        elif changed is None:
            decisions[name] = Decision(True, everything)
        else:
            hit = next(((path, entry) for path in changed if (entry := entry_for(path, group["paths"]))), None)
            if hit:
                decisions[name] = Decision(True, f"selected: {hit[0]} is on its list ({hit[1]['pattern']}: {hit[1]['reason']})")
            else:
                decisions[name] = Decision(False, f"skipped: none of the {len(changed)} changed paths is on its list")
    return decisions
