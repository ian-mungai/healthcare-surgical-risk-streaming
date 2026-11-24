"""Cleanup helpers for the signal profile acceptance runner: every cleanup step runs, and the scratch folder is always scrubbed.

``run_cleanup`` runs every step of a cleanup and then the scrub, whatever a step raises (a timeout of ``docker ps`` included), and raises the first error after
the scrub. ``scrub_tree`` removes a folder, also one with read-only parts, and says whether it is gone.
"""

from __future__ import annotations

import contextlib
import os
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path


def run_cleanup(steps: Sequence[Callable[[], object]], scrub: Callable[[], object]) -> None:
    """Run every step, then ``scrub``, whatever a step raises. The first error is raised after the scrub."""
    first: BaseException | None = None
    try:
        for step in steps:
            try:
                step()
            except BaseException as error:
                first = first or error
    finally:
        scrub()
    if first is not None:
        raise first


def scrub_tree(path: Path) -> bool:
    """Remove ``path`` and everything under it, also a read-only part, and return whether it is gone."""

    def retry(function: Callable[[str], object], target: str, _error: BaseException) -> None:
        for item in (os.path.dirname(target), target):
            with contextlib.suppress(OSError):
                os.chmod(item, 0o700)
        with contextlib.suppress(OSError):
            function(target)

    shutil.rmtree(path, onexc=retry)
    return not path.exists()
