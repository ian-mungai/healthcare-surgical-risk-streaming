"""The replay as an always-on service: wait for a run record in the inbox, run it, keep it with its result and return to waiting.

The inbox is a folder on the replay's state volume. ``submit`` first reserves the run ID: it creates ``reserved/<run ID>`` with the exact bytes of the run
record, and ``reserved/<run ID>.reference`` with the bytes of the manifest reference, each by a create that fails when the file exists. Nothing moves or
deletes ``reserved/``. A submit whose bytes differ from the reservation is refused, so of two different records under one run ID exactly one is accepted,
even when they arrive at once. A run ID that already holds a record in the inbox, in ``done/`` or in ``failed/`` (from before reservations existed) is the
same submission only when the record and its reference have the very same bytes, and then gets its reservation from them; any other bytes are refused. Only
after the reservation does ``submit`` write the queue entry ``<run ID>.json`` with ``<run ID>.reference`` beside it, each whole or not at all, and only when
no record of that run ID is held anywhere. The check and the writes, and the archival of a request, each hold the exclusive lock of the reservation file, so
a delayed submit can never put a request back that was archived meanwhile.

``serve`` takes the requests one at a time in name order and reads the manifest reference from beside the record. A finished run's record moves to ``done/``
and a failed run's to ``failed/``, with its reference and ``results/<name>.json``. A record that cannot be read, is not a valid run record or has no
reference fails at once with its reason. A request that already has a finished or failed result is only moved: it is never run again and its result is never
written again. A run that a stop signal ends stays in the inbox, so the restarted service runs it again and the replay resumes from its saved position.

Before it starts a request, ``serve`` adds one to the count in ``<run ID>.attempts`` beside it. A request that is found with the count at 3 has stopped the
service three times, whatever the error was, so it fails with that reason and is archived without a replay and the next request runs. A stop by signal puts the
count back. A planned stop ends every wait of the replay at once (``stopping.py``), so the service stops within the grace period of its container, the
position is saved and the stop is never counted. An arithmetic or value error of a replay fails that request at once.

The health file says whether the service waits, runs or stopped and when it last made progress. ``healthy`` is the container's health check: a waiting or
running service whose file is fresh and was not written in the future.
"""

from __future__ import annotations

import contextlib
import fcntl
import functools
import json
import os
import re
import secrets
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, TextIO

from replay.service.run_record import RunRecord, RunRecordError, parse_run_record
from replay.service.state_file import atomic_write
from scripts.ingestion.manifest import ManifestError, ManifestRef

BEAT_SECONDS = 5.0
RUN_ID = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
LIVE_STATUSES = ("waiting", "running")
TERMINAL_OUTCOMES = {"finished": "done", "failed": "failed"}
RESERVED = "reserved"
REFERENCE_SUFFIX = ".reference"
ATTEMPTS_SUFFIX = ".attempts"
MAX_ATTEMPTS = 3
LOCK_SECONDS = 30.0
LOCK_POLL_SECONDS = 0.01
FILE_MODE = 0o640


class SubmitError(ValueError):
    """The run record cannot be submitted: it is not a valid run record, its manifest reference is not valid, or its run ID already holds another submission."""


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
    return isinstance(updated, (int, float)) and not isinstance(updated, bool) and 0 <= now - updated <= max_age_seconds


def next_request(inbox: Path) -> Path | None:
    requests = sorted(path for path in inbox.glob("*.json") if path.is_file())
    return requests[0] if requests else None


def reference_of(request: Path) -> Path:
    """The manifest reference that ``submit`` wrote beside the record of this request."""
    return request.with_suffix(REFERENCE_SUFFIX)


@contextlib.contextmanager
def held(inbox: Path, run_id: str) -> Iterator[None]:
    """Hold the exclusive lock of the reservation file of this run ID, and fail with a clear message when another holder keeps it past ``LOCK_SECONDS``. A
    run ID without a reservation has nobody to wait for."""
    try:
        descriptor = os.open(inbox / RESERVED / run_id, os.O_RDONLY)
    except FileNotFoundError:
        yield
        return
    try:
        deadline = time.monotonic() + LOCK_SECONDS
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"the lock of the run ID {run_id} was not free within {LOCK_SECONDS:g} seconds") from None
                time.sleep(LOCK_POLL_SECONDS)
        yield
    finally:
        os.close(descriptor)


def attempts_of(request: Path) -> Path:
    """The file beside the request that counts how often the service started it."""
    return request.with_suffix(ATTEMPTS_SUFFIX)


def started_count(request: Path) -> int:
    try:
        return max(int(attempts_of(request).read_text()), 0)
    except OSError, ValueError:
        return 0


def archive(inbox: Path, request: Path, kind: str) -> None:
    """Move the request and its reference to ``done/`` or ``failed/`` and drop its start count. The reference moves first, so a request in the inbox always
    has its reference."""
    target = inbox / TERMINAL_OUTCOMES[kind]
    target.mkdir(parents=True, exist_ok=True)
    reference = reference_of(request)
    with held(inbox, request.stem):
        if reference.is_file():
            os.replace(reference, target / reference.name)
        os.replace(request, target / request.name)
        attempts_of(request).unlink(missing_ok=True)


def kept_outcome(inbox: Path, request: Path) -> str | None:
    """``finished`` or ``failed`` when a terminal result of this request is already written, else None."""
    try:
        document = json.loads((inbox / "results" / request.name).read_text())
    except OSError, ValueError:
        return None
    outcome = document.get("outcome") if isinstance(document, dict) else None
    return outcome if outcome in TERMINAL_OUTCOMES else None


def keep(inbox: Path, request: Path, outcome: Outcome) -> dict[str, Any]:
    """Write the request's result, then move the request to ``done/`` or ``failed/``. A stop between the two leaves the result, and the restart only moves."""
    result = {"request": request.stem, **asdict(outcome)}
    result["outcome"] = result.pop("kind")
    atomic_write(inbox / "results" / request.name, json.dumps(result, sort_keys=True) + "\n")
    archive(inbox, request, outcome.kind)
    return result


def queued_record(request: Path) -> RunRecord:
    """The run record of a request, with its manifest reference taken from the file that ``submit`` wrote beside it. A request without it is refused."""
    record = parse_run_record(request.read_text())
    reference = reference_of(request)
    if not reference.is_file():
        raise RunRecordError("the manifest reference that was submitted with this record is missing")
    return replace(record, manifest_reference=str(reference))


def unusable(request: Path) -> Outcome | None:
    """A failed outcome with its reason when the request cannot be read or is not a valid run record with its reference, else None."""
    try:
        queued_record(request)
    except (OSError, ValueError) as error:
        return Outcome("failed", 2, f"{type(error).__name__}: {error}")
    return None


def serve(
    inbox: Path,
    health_path: Path,
    run_request: Callable[[Path, Callable[[], None]], Outcome],
    stopped: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], object],
    poll_seconds: float,
) -> int:
    """Serve the inbox until ``stopped`` says so. ``run_request`` runs one record and calls the beat it is given as the run makes progress. A record that is
    not a valid run record never reaches it."""
    inbox.mkdir(parents=True, exist_ok=True)
    health = Health(health_path, clock)
    health.beat("waiting", force=True)
    while not stopped():
        request = next_request(inbox)
        if request is None:
            health.beat("waiting")
            sleep(poll_seconds)
            continue
        if (earlier := kept_outcome(inbox, request)) is not None:
            archive(inbox, request, earlier)
            continue
        started = started_count(request)
        if started >= MAX_ATTEMPTS:
            health.last = keep(inbox, request, Outcome("failed", 2, f"the service stopped during this request {MAX_ATTEMPTS} times"))
            health.beat("waiting", force=True)
            continue
        atomic_write(attempts_of(request), f"{started + 1}\n")
        if (refused := unusable(request)) is not None:
            health.last = keep(inbox, request, refused)
            health.beat("waiting", force=True)
            continue
        health.beat("running", request.stem, force=True)
        try:
            outcome = run_request(request, functools.partial(health.beat, "running", request.stem))
        except (ArithmeticError, ValueError) as error:
            outcome = Outcome("failed", 2, f"{type(error).__name__}: {error}")
        if outcome.kind == "stopped":
            atomic_write(attempts_of(request), f"{started}\n")
            break
        health.last = keep(inbox, request, outcome)
        health.beat("waiting", force=True)
    health.beat("stopped", force=True)
    return 0


def claim(path: Path, data: bytes) -> bool:
    """Create ``path`` with exactly ``data`` when it does not exist and say so. The bytes are complete when the file appears, and no second claim succeeds."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.claiming")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, FILE_MODE)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        return True
    finally:
        temporary.unlink(missing_ok=True)


def reserve(inbox: Path, name: str, data: bytes, what: str, run_id: str) -> None:
    """Reserve ``reserved/<name>`` with ``data``. Bytes that equal the reservation already held pass; other bytes are refused."""
    path = inbox / RESERVED / name
    if not claim(path, data) and path.read_bytes() != data:
        raise SubmitError(f"the run ID {run_id} already holds another {what}; use a new run ID")


def record_held(inbox: Path, name: str) -> Path | None:
    """The file of the record ``name`` in the inbox, ``done/`` or ``failed/``, else None."""
    return next((path for path in (inbox / name, inbox / "done" / name, inbox / "failed" / name) if path.is_file()), None)


def bytes_of(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except OSError:
        return None


def read_submission(reference_file: Path, stream: TextIO) -> tuple[str, str]:
    """The run record from ``stream`` and the manifest reference from its file. A text that is not valid UTF-8 or a file that cannot be read is refused."""
    try:
        reference_text = reference_file.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raise SubmitError("the manifest reference file is not valid UTF-8 text") from None
    except OSError:
        raise SubmitError("the manifest reference file cannot be read") from None
    try:
        return stream.read(), reference_text
    except UnicodeDecodeError:
        raise SubmitError("the run record on standard input is not valid UTF-8 text") from None


def submit(inbox: Path, text: str, reference_text: str) -> str:
    """Reserve the run ID of a valid run record and queue the record with its manifest reference, and say so.

    The same submission again changes nothing, except that it completes a queue entry that a stop left unwritten. Another record or another reference under
    that run ID is refused, also when it arrives at the same moment and also when a record of that ID from before reservations is already held.
    """
    try:
        record = parse_run_record(text)
        ManifestRef.from_json(reference_text)
    except (RunRecordError, ManifestError) as error:
        raise SubmitError(str(error)) from None
    if not RUN_ID.fullmatch(record.run_id):
        raise SubmitError("the run ID must be lower-case letters, digits and underscores, at most 64")
    name = f"{record.run_id}.json"
    earlier = record_held(inbox, name)
    if earlier is not None and (bytes_of(earlier), bytes_of(reference_of(earlier))) != (text.encode(), reference_text.encode()):
        raise SubmitError(f"the run ID {record.run_id} already holds another run record or manifest reference; use a new run ID")
    reserve(inbox, record.run_id, text.encode(), "run record", record.run_id)
    reserve(inbox, record.run_id + REFERENCE_SUFFIX, reference_text.encode(), "manifest reference", record.run_id)
    if earlier is not None:
        return "already submitted"
    with held(inbox, record.run_id):
        if record_held(inbox, name) is not None:
            return "already submitted"
        atomic_write(reference_of(inbox / name), reference_text)
        atomic_write(inbox / name, text)
    return "submitted"
