"""The replay's files on its named volume: the frozen schedule and the saved state.

The saved state is the fixed name ``state.json`` in the state folder of the run. It is replaced through ``replace_owned_file`` with that folder as its root,
so a stop at any instant leaves either the whole old file or the whole new file, and a link or a folder in its place is refused.
The frozen schedule is created through ``write_new_file`` once and never overwritten.
"""

from __future__ import annotations

from pathlib import Path

from replay.events.run import RunState
from replay.events.schedule import FrozenSchedule
from replay.service.run_record import STATE_NAME
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.notice import Code
from scripts.ingestion.output_file import replace_owned_file, write_new_file

FILE_MODE = 0o640


class StateFile:
    """The saved state of a run: positions, the first send, the admissions and whether the run finished."""

    def __init__(self, state: Path) -> None:
        self.state = state
        self.path = state / STATE_NAME

    def load(self) -> RunState | None:
        return RunState.from_json(self.path.read_text()) if self.path.is_file() else None

    def save(self, state: RunState) -> None:
        self.state.mkdir(parents=True, exist_ok=True)
        replace_owned_file(self.path, state.to_json(), state_root=self.state, mode=FILE_MODE)


class ScheduleFile:
    """The frozen schedule of a run. It is written once, before any admission."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> FrozenSchedule | None:
        return FrozenSchedule.from_json(self.path.read_text()) if self.path.is_file() else None

    def freeze(self, frozen: FrozenSchedule) -> None:
        """Write the schedule. A schedule that already exists is never replaced: the caller compares it with the new one first."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            write_new_file(self.path, frozen.to_json(), mode=FILE_MODE)
        except ClosedInputError as error:
            if any(notice.code is Code.OUTPUT_EXISTS for notice in error.notices):
                raise FileExistsError("the schedule is already frozen") from None
            raise
