"""The replay as an always-on service: wait for a run record in the inbox, run it, keep it with its result and return to waiting.

The inbox is a folder on the replay's state volume. ``submit`` writes a run record there under ``<run ID>.json``, whole or not at all, and refuses a
different record under a run ID that it already holds. ``serve`` takes the requests one at a time in name order. A finished run's record moves to
``done/`` and a failed run's to ``failed/``, each with ``results/<name>.json``. A run that a stop signal ends stays in the inbox, so the restarted service
runs it again and the replay resumes from its saved position. The health file says whether the service waits, runs or stopped and when it last made
progress. ``healthy`` is the container's health check: a waiting or running service whose file is fresh.
"""

from __future__ import annotations

import functools
import json
import os
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from replay.service.run_record import RunRecordError, parse_run_record
from replay.service.state_file import atomic_write

BEAT_SECONDS = 5.0
RUN_ID = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
LIVE_STATUSES = ("waiting", "running")


class SubmitError(ValueError):
    """The run record cannot be submitted: it is not a valid run record, or its run ID already holds another record."""


@dataclass(frozen=True, slots=True)
class Outcome:
    """What one run gave: ``finished``, ``failed`` with its exit code and closed error text, or ``stopped`` by a signal before it finished."""

    kind: str
    exit_code: int
    error: str = ""


class Health:
    """The health file: the status, the request that runs, the last result and the wall time of the last progress, written at most every few seconds."""

    def __init__(self, path: Path, clock: Callable[[], float]) -> None:
        self.path = path
        self.clock = clock
        self.written = float("-inf")
        self.last: dict[str, Any] | None = None

    def beat(self, status: str, request: str = "", *, force: bool = False) -> None:
        now = self.clock()
        if force or now - self.written >= BEAT_SECONDS:
            atomic_write(self.path, json.dumps({"status": status, "updated": now, "request": request, "last": self.last}, sort_keys=True) + "\n")
            self.written = now


def healthy(path: Path, now: float, max_age_seconds: float) -> bool:
    """Whether the health file says the service waits or runs and was written at most ``max_age_seconds`` ago."""
    try:
        document = json.loads(path.read_text())
    except OSError, ValueError:
        return False
    if not isinstance(document, dict) or document.get("status") not in LIVE_STATUSES:
        return False
    updated = document.get("updated")
    return isinstance(updated, (int, float)) and now - updated <= max_age_seconds


def next_request(inbox: Path) -> Path | None:
    requests = sorted(path for path in inbox.glob("*.json") if path.is_file())
    return requests[0] if requests else None


def keep(inbox: Path, request: Path, outcome: Outcome) -> dict[str, Any]:
    """Write the request's result, then move the request to ``done/`` or ``failed/``. A stop between the two leaves the request to run again."""
    result = {"request": request.stem, **asdict(outcome)}
    result["outcome"] = result.pop("kind")
    atomic_write(inbox / "results" / request.name, json.dumps(result, sort_keys=True) + "\n")
    target = inbox / ("done" if outcome.kind == "finished" else "failed")
    target.mkdir(parents=True, exist_ok=True)
    os.replace(request, target / request.name)
    return result


def serve(
    inbox: Path,
    health_path: Path,
    run_request: Callable[[Path, Callable[[], None]], Outcome],
    stopped: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    poll_seconds: float,
) -> int:
    """Serve the inbox until ``stopped`` says so. ``run_request`` runs one record and calls the beat it is given as the run makes progress."""
    inbox.mkdir(parents=True, exist_ok=True)
    health = Health(health_path, clock)
    health.beat("waiting", force=True)
    while not stopped():
        request = next_request(inbox)
        if request is None:
            health.beat("waiting")
            sleep(poll_seconds)
            continue
        health.beat("running", request.stem, force=True)
        outcome = run_request(request, functools.partial(health.beat, "running", request.stem))
        if outcome.kind == "stopped":
            break
        health.last = keep(inbox, request, outcome)
        health.beat("waiting", force=True)
    health.beat("stopped", force=True)
    return 0


def submit(inbox: Path, text: str) -> str:
    """Write a valid run record to the inbox under its run ID and say so. The same record again is a no-op; another record under that run ID is refused."""
    try:
        record = parse_run_record(text)
    except RunRecordError as error:
        raise SubmitError(str(error)) from None
    if not RUN_ID.fullmatch(record.run_id):
        raise SubmitError("the run ID must be lower-case letters, digits and underscores, at most 64")
    name = f"{record.run_id}.json"
    for held in (inbox / name, inbox / "done" / name, inbox / "failed" / name):
        if held.is_file():
            if held.read_text() != text:
                raise SubmitError(f"the run ID {record.run_id} already holds another run record; use a new run ID")
            return "already submitted"
    atomic_write(inbox / name, text)
    return "submitted"
