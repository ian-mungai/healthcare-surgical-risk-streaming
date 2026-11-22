"""The replay's files on its named volume: the frozen schedule and the saved state.

A file is written to a temporary name of its own in the same folder (so two writers never share one), flushed to disk, renamed over the target and the
folder is flushed, so a stop at any instant leaves
either the whole old file or the whole new file. The frozen schedule is created once and never overwritten.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from replay.events.run import RunState
from replay.events.schedule import FrozenSchedule


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{secrets.token_hex(4)}.writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o640)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    folder = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


class StateFile:
    """The saved state of a run: positions, the first send, the admissions and whether the run finished."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> RunState | None:
        return RunState.from_json(self.path.read_text()) if self.path.is_file() else None

    def save(self, state: RunState) -> None:
        atomic_write(self.path, state.to_json())


class ScheduleFile:
    """The frozen schedule of a run. It is written once, before any admission."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> FrozenSchedule | None:
        return FrozenSchedule.from_json(self.path.read_text()) if self.path.is_file() else None

    def freeze(self, frozen: FrozenSchedule) -> None:
        """Write the schedule. A schedule that already exists is never replaced: the caller compares it with the new one first."""
        if self.path.exists():
            raise FileExistsError(f"{self.path.name} is already frozen")
        atomic_write(self.path, frozen.to_json())
