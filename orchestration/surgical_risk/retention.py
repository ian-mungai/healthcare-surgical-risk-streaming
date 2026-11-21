"""The retention rule for Dagster's run history: a run that ended more than ``RUN_RETENTION_DAYS`` ago is removed with its event records and compute logs.

Dagster's free edition keeps runs, their events and their compute logs until someone removes them. ``prune`` removes the runs that are over the age and no
other run: a queued or started run is never removed, and no run folder is touched. A run's folder is written before the run goes, so the record outlives it.
The decision has no Dagster import. ``housekeeping.py`` gives it Dagster's runs and its deletion calls.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

# The age in days after which an ended run is removed. It is the same 30 days that the log store keeps its logs.
RUN_RETENTION_DAYS = 30
SECONDS_PER_DAY = 86400
PRUNE_JOB = "prune_old_runs_job"
OVER = frozenset({"SUCCESS", "FAILURE", "CANCELED"})
REMOVAL = re.compile(r"removed run (\S+) ended (\d+(?:\.\d+)?)")


@dataclass(frozen=True)
class EndedRun:
    run_id: str
    status: str
    job_name: str
    ended: float | None


class RunStore(Protocol):
    def ensure_folder(self, run_id: str) -> None: ...

    def delete_compute_logs(self, run_id: str) -> None: ...

    def delete_run(self, run_id: str) -> None: ...


def expired(runs: Sequence[EndedRun], now: float, days: int = RUN_RETENTION_DAYS) -> list[EndedRun]:
    """The runs that are over and ended more than ``days`` days before ``now``."""
    return [run for run in runs if run.status in OVER and run.ended is not None and now - run.ended > days * SECONDS_PER_DAY]


def prune(runs: Sequence[EndedRun], now: float, store: RunStore, days: int = RUN_RETENTION_DAYS) -> list[EndedRun]:
    """Remove each expired run: its folder is written first, then its compute logs and the run with its events go. Return the runs removed."""
    removed = expired(runs, now, days)
    for run in removed:
        store.ensure_folder(run.run_id)
        store.delete_compute_logs(run.run_id)
        store.delete_run(run.run_id)
    return removed


def removal_message(run: EndedRun) -> str:
    return f"removed run {run.run_id} ended {run.ended}"


def parse_removal(message: str) -> tuple[str, float] | None:
    """The run ID and end time of a removal message, or None for any other message."""
    found = REMOVAL.fullmatch(message.strip())
    return None if found is None else (found.group(1), float(found.group(2)))
