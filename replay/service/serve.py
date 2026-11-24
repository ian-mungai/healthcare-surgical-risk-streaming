"""The replay as an always-on service: wait for a run record in the inbox, run it, keep it with its result and return to waiting.

The inbox is a folder on the replay's state volume. ``submit`` reads the record and the manifest reference as bytes, decodes them as strict UTF-8 only to
parse them, and stores and compares the exact bytes. It first reserves the run ID: it creates ``reserved/<run ID>`` with the bytes of the run record, and
``reserved/<run ID>.reference`` with the bytes of the manifest reference, each by a create that fails when the file exists. Nothing moves or deletes
``reserved/``. A submit whose bytes differ from the reservation is refused, so of two different records under one run ID exactly one is accepted, even when
they arrive at once. An ID that has a reservation is compared with the reservation alone, never with the files that archival moves. A run ID that holds a
record in the inbox, in ``done/`` or in ``failed/`` and has no reservation (from before reservations existed) is the same submission only when the record
and its reference have the very same bytes, and then gets its reservation from them; any other bytes are refused. Only after the reservation does ``submit``
write the queue entry ``<run ID>.json`` with ``<run ID>.reference`` beside it, each whole or not at all, and only when no record of that run ID is held
anywhere. The whole of a submit and the archival of a request each hold the exclusive lock ``locks/<run ID>``, so a delayed submit can never put back a request
that was archived meanwhile. An archival that a stop or a crash cut off between its two moves leaves the reference in ``done/`` or ``failed/`` and the record in
the inbox until the restart finishes it: a submit then finds the reference in the folder that the written result names. Cut off before the first move, both
are still in the inbox.

``serve`` takes the requests one at a time in name order and reads the manifest reference from beside the record. A finished run's record moves to ``done/``
and a failed run's to ``failed/``, with its reference and ``results/<name>.json``. A record that cannot be read, is not a valid run record or has no
reference fails at once with its reason. A request that already has a finished or failed result is only moved: it is never run again and its result is never
written again. A result file that is not a valid terminal result (larger than 64 KiB, not a file that can be read, not UTF-8 or JSON, too deeply nested, not an
object, no ``finished`` or ``failed`` outcome) is logged, kept beside its place as ``<name>.json.invalid`` for inspection and treated as no result; judging it
can never raise. A run that a stop signal ends stays in the inbox, so the restarted service runs it
again and the replay resumes from its saved position.

Before it starts a request, ``serve`` adds one to the count in ``<run ID>.attempts`` beside it. A request that is found with the count at 3 has stopped the
service three times, whatever the error was, so it fails with that reason and is archived without a replay and the next request runs. A planned stop ends
every wait of the replay at once (``stopping.py``), so the service stops within the grace period of its container, the position is saved and the count is put
back: only a start that did not end cleanly counts. A result, finished or failed, is never written once the stop flag was seen: a request that ends in a
refusal, an error or a finish after the stop arrived stays queued with its count put back, and the next start repeats it. An arithmetic or value error of a
replay fails that request at once. Recorded limit: one operation already running when the stop arrives (a verification, one case's decode) is not
interrupted; if it outlasts the grace period the container is killed and that start counts as an attempt.

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
import stat
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, BinaryIO, TextIO

from replay.service.run_record import RunRecord, RunRecordError, parse_run_record
from scripts.ingestion.closed import ClosedInputError
from scripts.ingestion.folder import list_files
from scripts.ingestion.manifest import ManifestError, ManifestRef
from scripts.ingestion.notice import Code, describe_error
from scripts.ingestion.output_file import replace_owned_file, write_new_file

BEAT_SECONDS = 5.0
RUN_ID = re.compile(r"[a-z0-9][a-z0-9_]{0,63}")
LIVE_STATUSES = ("waiting", "running")
TERMINAL_OUTCOMES = {"finished": "done", "failed": "failed"}
RESERVED = "reserved"
LOCKS = "locks"
INVALID_SUFFIX = ".invalid"
# The largest result file, for the writer and the reader alike. No result has been written on any host yet, so no older result is larger than this.
RESULT_LIMIT_BYTES = 64 * 1024
ERROR_LIMIT_CHARS = 5_000  # a character outside the basic plane takes 12 bytes as an ASCII escape: 60,000 bytes at most
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
            text = json.dumps({"status": status, "updated": now, "request": request, "last": self.last}, sort_keys=True) + "\n"
            replace_owned_file(self.path, text, state_root=self.path.parent, mode=FILE_MODE)
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
    requests = list_files(inbox, "*.json")
    return requests[0] if requests else None


def reference_of(request: Path) -> Path:
    """The manifest reference that ``submit`` wrote beside the record of this request."""
    return request.with_suffix(REFERENCE_SUFFIX)


@contextlib.contextmanager
def held(inbox: Path, run_id: str) -> Iterator[None]:
    """Hold the exclusive lock of this run ID (the file ``locks/<run ID>``, created when it is missing and never removed), and fail with a clear message when
    another holder keeps it past ``LOCK_SECONDS``."""
    (inbox / LOCKS).mkdir(parents=True, exist_ok=True)
    descriptor = os.open(inbox / LOCKS / run_id, os.O_RDWR | os.O_CREAT, FILE_MODE)
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
    """Move the request and its reference to ``done/`` or ``failed/`` and drop its start count. The reference moves first, so an archival cut off between the
    two moves leaves the reference in the folder and the request in the inbox, and the restart finishes it."""
    target = inbox / TERMINAL_OUTCOMES[kind]
    target.mkdir(parents=True, exist_ok=True)
    reference = reference_of(request)
    with held(inbox, request.stem):
        if reference.is_file():
            os.replace(reference, target / reference.name)
        os.replace(request, target / request.name)
        attempts_of(request).unlink(missing_ok=True)


def read_result(path: Path) -> tuple[str | None, str]:
    """Judge a result file without ever raising: ``(outcome, "")`` for a valid terminal result, ``(None, "")`` when there is no file and ``(None, reason)``
    when there is one that is not valid. It is read through a handle opened without blocking (a pipe in its place cannot hold the service) and at most
    ``RESULT_LIMIT_BYTES`` of it; a larger one is invalid. Any error of reading, decoding or parsing (a deeply nested document included) is a reason."""
    try:
        with open(path, "rb", opener=lambda name, flags: os.open(name, flags | os.O_NONBLOCK)) as handle:
            data = handle.read(RESULT_LIMIT_BYTES + 1)
        if len(data) > RESULT_LIMIT_BYTES:
            return None, f"it is larger than {RESULT_LIMIT_BYTES} bytes"
        document = json.loads(data.decode("utf-8"))
    except FileNotFoundError:
        return None, ""
    except (OSError, ValueError, RecursionError) as error:
        return None, f"it cannot be read as JSON text: {type(error).__name__}"
    outcome = document.get("outcome") if isinstance(document, dict) else None
    if isinstance(outcome, str) and outcome in TERMINAL_OUTCOMES:
        return outcome, ""
    return None, "it is not an object with the outcome finished or failed"


def kept_outcome(inbox: Path, request: Path, log: Callable[..., None]) -> str | None:
    """``finished`` or ``failed`` when a terminal result of this request is already written, else None. A result file that is not a valid terminal result is
    logged with its reason and renamed with the suffix ``.invalid``, and counts as no result. Nothing in the file can raise out of here."""
    path = inbox / "results" / request.name
    outcome, reason = read_result(path)
    if outcome is not None or not reason:
        return outcome
    log("result_invalid", request=request.stem, path=str(path), reason=reason)
    try:
        os.replace(path, path.with_name(path.name + INVALID_SUFFIX))
    except OSError as error:
        log("result_invalid_not_kept", request=request.stem, path=str(path), reason=f"it cannot be renamed: {type(error).__name__}")
    return None


def result_text(request: Path, outcome: Outcome) -> str:
    """The text of a result file: JSON of at most ``RESULT_LIMIT_BYTES``, so the reader always accepts what the writer wrote. The only text of unbounded
    length is ``error`` (the refusal of a gate names every refused key): it is cut to ``ERROR_LIMIT_CHARS`` with the count of the characters cut. A result that
    is still too large, which no kind of failure gives, is replaced by a short failure with a reason code."""
    error = outcome.error
    if len(error) > ERROR_LIMIT_CHARS:
        error = f"{error[:ERROR_LIMIT_CHARS]} ... ({len(error) - ERROR_LIMIT_CHARS} more characters cut)"
    text = json.dumps({"request": request.stem, "outcome": outcome.kind, "exit_code": outcome.exit_code, "error": error}, sort_keys=True) + "\n"
    if len(text) > RESULT_LIMIT_BYTES:  # the JSON is ASCII, so a character is a byte
        text = (
            json.dumps({"outcome": "failed", "exit_code": 2, "error": "the result was too large to keep", "reason": "result_too_large"}, sort_keys=True) + "\n"
        )
    return text


def keep(inbox: Path, request: Path, outcome: Outcome) -> dict[str, Any]:
    """Write the request's result, then move the request to ``done/`` or ``failed/``. A stop between the two leaves the result, and the restart only moves.
    The result is a new file: a path that is held in any form stops the service with a fixed error, never replaces it."""
    text = result_text(request, outcome)
    (inbox / "results").mkdir(parents=True, exist_ok=True)
    write_new_file(inbox / "results" / request.name, text, mode=FILE_MODE)
    archive(inbox, request, outcome.kind)
    result: dict[str, Any] = json.loads(text)
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
        return Outcome("failed", 2, describe_error(error))
    return None


def serve(
    inbox: Path,
    health_path: Path,
    run_request: Callable[[Path, Callable[[], None]], Outcome],
    stopped: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], object],
    poll_seconds: float,
    log: Callable[..., None] = lambda event, **fields: None,
) -> int:
    """Serve the inbox until ``stopped`` says so. ``run_request`` runs one record and calls the beat it is given as the run makes progress. A record that is
    not a valid run record never reaches it."""
    inbox.mkdir(parents=True, exist_ok=True)
    health_path.parent.mkdir(parents=True, exist_ok=True)
    health = Health(health_path, clock)
    health.beat("waiting", force=True)
    while not stopped():
        request = next_request(inbox)
        if request is None:
            health.beat("waiting")
            sleep(poll_seconds)
            continue
        if (earlier := kept_outcome(inbox, request, log)) is not None:
            archive(inbox, request, earlier)
            continue
        started = started_count(request)
        if started >= MAX_ATTEMPTS:
            health.last = keep(inbox, request, Outcome("failed", 2, f"the service stopped during this request {MAX_ATTEMPTS} times"))
            health.beat("waiting", force=True)
            continue
        replace_owned_file(attempts_of(request), f"{started + 1}\n", state_root=inbox, mode=FILE_MODE)
        if (refused := unusable(request)) is not None:
            health.last = keep(inbox, request, refused)
            health.beat("waiting", force=True)
            continue
        health.beat("running", request.stem, force=True)
        try:
            outcome = run_request(request, functools.partial(health.beat, "running", request.stem))
        except (ArithmeticError, ValueError) as error:
            outcome = Outcome("failed", 2, describe_error(error))
        if outcome.kind == "stopped" or stopped():
            if outcome.kind != "stopped":
                log("request_left_queued", request=request.stem, outcome=outcome.kind, error=outcome.error)
            replace_owned_file(attempts_of(request), f"{started}\n", state_root=inbox, mode=FILE_MODE)
            break
        health.last = keep(inbox, request, outcome)
        health.beat("waiting", force=True)
    health.beat("stopped", force=True)
    return 0


def claim(path: Path, data: bytes) -> bool:
    """Create ``path`` with exactly ``data`` when it does not exist and say so. The bytes are complete when the file appears, and no second claim succeeds.
    A path that is held in any form (a link too) is not claimed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        write_new_file(path, data, mode=FILE_MODE)
    except ClosedInputError as error:
        if any(notice.code is Code.OUTPUT_EXISTS for notice in error.notices):
            return False
        raise
    return True


def queue_file(path: Path, data: bytes) -> None:
    """Put one file of the queue entry in the inbox. A regular file that holds the very same bytes (a stop left it) stays; any other path that is held, a link
    too, is refused and never replaced."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        write_new_file(path, data, mode=FILE_MODE)
        return
    if regular_bytes(path) != data:
        raise SubmitError("the queue already holds another file under this run ID")


def reserve(inbox: Path, name: str, data: bytes, what: str, run_id: str) -> None:
    """Reserve ``reserved/<name>`` with ``data``. Bytes that equal the reservation already held pass; other bytes are refused."""
    path = inbox / RESERVED / name
    if not claim(path, data) and regular_bytes(path) != data:
        raise SubmitError(f"the run ID {run_id} already holds another {what}; use a new run ID")


def record_held(inbox: Path, name: str) -> Path | None:
    """The file of the record ``name`` in the inbox, ``done/`` or ``failed/``, else None."""
    return next((path for path in (inbox / name, inbox / "done" / name, inbox / "failed" / name) if path.is_file()), None)


def regular_bytes(path: Path) -> bytes | None:
    """The bytes of a regular file, or None for a path that is missing, unreadable or a link, a folder or another kind of entry."""
    try:
        return path.read_bytes() if stat.S_ISREG(os.lstat(path).st_mode) else None
    except OSError:
        return None


def bytes_of(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except OSError:
        return None


def conflicts_with_held(inbox: Path, name: str, record_bytes: bytes, reference_bytes: bytes) -> bool:
    """Whether a record from before reservations is held under this name with other bytes. An ID whose reference is reserved is judged by its reservation alone,
    never by the files that archival moves. The caller holds the lock of the ID, so no archival runs, but one that was cut off can have moved the reference and
    not yet the record: the reference is then looked for beside the record, in the inbox and in the folder that the written result names."""
    if (inbox / RESERVED / (Path(name).stem + REFERENCE_SUFFIX)).exists() or (earlier := record_held(inbox, name)) is None:
        return False
    outcome, _ = read_result(inbox / "results" / name)
    reference_name = reference_of(Path(name)).name
    sides = [earlier.parent, inbox, *([inbox / TERMINAL_OUTCOMES[outcome]] if outcome else [])]
    reference = next((side / reference_name for side in sides if (side / reference_name).is_file()), None)
    return (bytes_of(earlier), None if reference is None else bytes_of(reference)) != (record_bytes, reference_bytes)


def read_submission(reference_file: Path, stream: BinaryIO) -> tuple[bytes, bytes]:
    """The run record from ``stream`` and the manifest reference from its file, each as the exact bytes. A file that cannot be read is refused."""
    try:
        reference = reference_file.read_bytes()
    except OSError:
        raise SubmitError("the manifest reference file cannot be read") from None
    return stream.read(), reference


def text_of(data: bytes, what: str) -> str:
    """The strict UTF-8 text of ``data``, for parsing only. The bytes stay what is stored and compared."""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        raise SubmitError(f"the {what} is not valid UTF-8 text") from None


def submit(inbox: Path, record_data: bytes | str, reference_data: bytes | str) -> str:
    """Reserve the run ID of a valid run record and queue the record with its manifest reference, and say so.

    The same submission again changes nothing, except that it completes a queue entry that a stop left unwritten. Another record or another reference under
    that run ID is refused, also when it arrives at the same moment and also when a record of that ID from before reservations is already held. Bytes that
    differ in a line end only are other bytes. A text is taken as its UTF-8 bytes.
    """
    record_bytes = record_data.encode() if isinstance(record_data, str) else record_data
    reference_bytes = reference_data.encode() if isinstance(reference_data, str) else reference_data
    try:
        record = parse_run_record(text_of(record_bytes, "run record"))
        ManifestRef.from_json(text_of(reference_bytes, "manifest reference"))
    except (RunRecordError, ManifestError) as error:
        raise SubmitError(str(error)) from None
    if not RUN_ID.fullmatch(record.run_id):
        raise SubmitError("the run ID must be lower-case letters, digits and underscores, at most 64")
    name = f"{record.run_id}.json"
    with held(inbox, record.run_id):
        if conflicts_with_held(inbox, name, record_bytes, reference_bytes):
            raise SubmitError(f"the run ID {record.run_id} already holds another run record or manifest reference; use a new run ID")
        reserve(inbox, record.run_id, record_bytes, "run record", record.run_id)
        reserve(inbox, record.run_id + REFERENCE_SUFFIX, reference_bytes, "manifest reference", record.run_id)
        if record_held(inbox, name) is not None:
            return "already submitted"
        inbox.mkdir(parents=True, exist_ok=True)
        queue_file(reference_of(inbox / name), reference_bytes)
        queue_file(inbox / name, record_bytes)
    return "submitted"


def submit_command(inbox: Path, reference_file: Path, stream: BinaryIO, out: TextIO, log: Callable[..., None]) -> int:
    """The ``submit`` command: the exit code 0 and what ``submit`` says on ``out``, or 2 with the reason logged when the submission is refused."""
    try:
        record, reference = read_submission(reference_file, stream)
        out.write(submit(inbox, record, reference) + "\n")
    except (SubmitError, ClosedInputError) as error:
        log("submit_refused", message=describe_error(error))
        return 2
    return 0
