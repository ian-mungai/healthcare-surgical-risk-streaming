"""The sync of the raw copy, driven from the code location container on the host: start it only on the fixed sample, watch it, cancel it, read its record.

The raw connection (``scripts.ingestion.connection``) is made by the host's connection step with the selection that the host's environment names: the fixed
sample on a dev host and the full release on a prod host. Nothing else starts its sync. This module is that start, for the live batch of the ingestion phase.
It runs where Airbyte's client credentials are, which is the code location container, and it prints closed facts only (a job number, a status, two counts and
a duration): no glob, no ID of Airbyte, no credential and no text of an exception.

``start`` refuses, before any job call, a connection whose source selects any other set of files than the fixed sample's (so a dev host that still holds the
full release's selection copies nothing), a connection whose destination is not the project's bucket under the landing prefix (``destination_differs``) and a
connection that has an active job (so no second copy runs beside the first). It reads the IDs of the connection's sync jobs, and keeps the attempt (the
connection, the time and those IDs) in a ledger, in the state folder, before it sends the start (a start whose IDs cannot be read is ``jobs_unreadable`` and
sends nothing), and the job from the start answer beside it before it returns the job. A start whose reply is lost is reconciled by a confirmed identity: the
connection's latest job whose ID is not among the IDs that were read before the start and that the ledger does not hold yet. The creation time of a job is
never used, because Airbyte reports it in whole seconds
(https://github.com/airbytehq/airbyte-platform/blob/main/airbyte-server/src/main/kotlin/io/airbyte/server/apis/publicapi/mappers/JobResponseMapper.kt#L55-L56),
so a job that is made in the fractional second of the attempt reads as older than the attempt. A start that cannot be reconciled is ``start_unconfirmed``, and
its attempt stays in the ledger as unresolved: ``cancel-owned`` reconciles it again, counts it in ``unresolved`` while it is open and drops it only when no
job is found and ``ATTEMPT_SETTLE_SECONDS`` have passed since the attempt (a request that was never answered cannot land after that).

RECORDED LIMIT (owner-accepted, Nov 25 2026): a sync that an operator starts on the connection between the read of its job IDs and the start of this tool is
not in the read IDs, so a lost reply can adopt it as this tool's own, and ``cancel-owned`` can then cancel it. One operator uses the development host, so the
limit is accepted there and no other fix is made. A ledger of an earlier build (``raw_copy_jobs_1``) is read as resolved jobs and its jobs are kept when the
ledger is written again. A read of Airbyte spends only the time that is left of its operation's budget, through its pages and its ownership reads. ``wait``
and ``cancel`` act only on a job of that ledger whose connection Airbyte confirms (``job_not_owned``
otherwise), so a job that a person or another tool started is never waited for, and never cancelled. ``wait`` ends at its deadline, which starts before its
ownership reads: a job that is still running then is cancelled and the cancellation is confirmed, a job whose success is read after the deadline is a
``deadline`` and not a success, and a job that cannot be confirmed stopped is named ``job_still_active``. ``cancel-owned`` cancels every job of the ledger that
is still active, goes on after a job or an attempt that cannot be looked up (it counts it) and prints ``jobs``, ``still_active`` and ``unresolved``.

``docker compose exec -T dagster_code_location python -m scripts.ingestion.raw_copy status --bucket B``

``... raw_copy start --bucket B``, ``... raw_copy wait --job N --deadline-seconds S``, ``... raw_copy cancel --job N``, ``... raw_copy cancel-owned``,
``... raw_copy record --job N``
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import secrets
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

from scripts.ingestion.airbyte_api import JOB_STATUSES, SYNC, TERMINAL_STATUSES, AirbyteError, HttpAirbyteApi, fields, job_entry, named, text
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.connection import CONNECTION_NAME, fixed_sample_selection
from scripts.ingestion.landing import LANDING_PREFIX
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import replace_owned_file
from scripts.ingestion.record_run import settings_from_environment

SUCCEEDED = "succeeded"
DEFAULT_DEADLINE_SECONDS = 3600.0
DEFAULT_POLL_SECONDS = 10.0
DEFAULT_CONFIRM_SECONDS = 120.0
STATUS_READ_SECONDS = 30.0
RECORD_COUNTS = ("rowsSynced", "bytesSynced")
LEDGER_FORMAT = "raw_copy_jobs_2"
FIRST_LEDGER_FORMAT = "raw_copy_jobs_1"
LEDGER_FILE = "owned_jobs.json"
LEDGER_FOLDER_DEFAULT = "/opt/dagster/data/raw_copy"
LEDGER_FOLDER_VARIABLE = "RAW_COPY_STATE_DIR"
LEDGER_KEEPS = 20
ATTEMPT_SETTLE_SECONDS = 300.0


class RawCopyError(RuntimeError):
    """A refusal of this module. ``error_code`` is one fixed word, and it is the only thing that a command shows of the refusal."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RawConnection:
    """The raw connection's ID, the globs that its source selects, in the order Airbyte keeps them, and the bucket and the path that its destination writes."""

    connection_id: str
    globs: tuple[str, ...]
    bucket: str
    landing_path: str


@dataclass(frozen=True, slots=True)
class Attempt:
    """A start that was sent and whose job is not yet known: its token, the connection, the time of the attempt (epoch seconds) and the IDs of the connection's
    sync jobs that were read before the start (None for an attempt that an earlier build kept without them)."""

    token: str
    connection_id: str
    started: float
    before: tuple[int, ...] | None = None


class JobLedger:
    """The attempts and the jobs that this tool started, kept in a file of the state folder (the latest ``LEDGER_KEEPS`` jobs and every open attempt).

    An attempt is written before its start is sent and carries no job until the start answer, or a reconciliation, names it. A job that is not in the ledger
    was not started by this tool, so ``wait`` and ``cancel`` refuse it. The file is replaced whole, never edited in place.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)

    @property
    def path(self) -> Path:
        return self.directory / LEDGER_FILE

    def rows(self) -> list[dict[str, Any]]:
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except OSError, ValueError:
            raise RawCopyError("state_unreadable") from None
        rows = document.get("jobs") if isinstance(document, dict) else None
        if isinstance(document, dict) and document.get("format") == FIRST_LEDGER_FORMAT:
            if type(rows) is not list or any(not first_format_row(row) for row in rows):
                raise RawCopyError("state_unreadable")
            return [{"token": f"v1_{number}", "job": row["job"], "connection_id": row["connection_id"], "started": 0.0} for number, row in enumerate(rows)]
        if type(rows) is not list or document.get("format") != LEDGER_FORMAT or any(not well_formed(row) for row in rows):
            raise RawCopyError("state_unreadable")
        return rows

    def entries(self) -> list[tuple[int, str]]:
        return [(row["job"], row["connection_id"]) for row in self.rows() if row["job"] is not None]

    def jobs(self) -> list[int]:
        return [job for job, _ in self.entries()]

    def attempts(self) -> list[Attempt]:
        return [
            Attempt(row["token"], row["connection_id"], row["started"], tuple(row["before"]) if "before" in row else None)
            for row in self.rows()
            if row["job"] is None
        ]

    def connection_of(self, job: int) -> str | None:
        found = [connection for kept, connection in self.entries() if kept == job]
        return found[-1] if found else None

    def save(self, rows: list[dict[str, Any]]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        resolved = [row for row in rows if row["job"] is not None][-LEDGER_KEEPS:]
        kept = [row for row in rows if row["job"] is None or row in resolved]
        replace_owned_file(self.path, json.dumps({"format": LEDGER_FORMAT, "jobs": kept}, sort_keys=True) + "\n", state_root=self.directory, mode=0o600)

    def begin(self, connection_id: str, started: float, before: Sequence[int] = ()) -> str:
        """Keep an attempt that is about to be sent, with the IDs of the connection's jobs that were read before it, and return its token."""
        token = secrets.token_hex(8)
        self.save([*self.rows(), {"token": token, "job": None, "connection_id": connection_id, "started": started, "before": list(before)}])
        return token

    def resolve(self, token: str, job: int) -> None:
        """Name the job of an attempt."""
        self.save([{**row, "job": job} if row["token"] == token else row for row in self.rows()])

    def settle(self, token: str) -> None:
        """Drop an attempt that is known to have made no job."""
        self.save([row for row in self.rows() if row["token"] != token])


def well_formed(row: object) -> bool:
    return (
        type(row) is dict
        and type(row.get("token")) is str
        and type(row.get("connection_id")) is str
        and (row.get("job") is None or type(row.get("job")) is int)
        and type(row.get("started")) in (int, float)
        and ("before" not in row or (type(row["before"]) is list and all(type(job) is int for job in row["before"])))
    )


def first_format_row(row: object) -> bool:
    """A row of the ledger of the first format: a job and its connection, and nothing about an attempt."""
    return type(row) is dict and type(row.get("job")) is int and type(row.get("connection_id")) is str


class RawCopyApi(Protocol):
    def raw_connection(self, *, timeout: float | None = None) -> RawConnection: ...

    def job_connection(self, job: int, *, timeout: float | None = None) -> str: ...

    def running_jobs(self, connection_id: str) -> list[int]: ...

    def job_ids(self, connection_id: str, *, timeout: float | None = None) -> list[int]: ...

    def start_sync(self, connection_id: str) -> int: ...

    def job_status(self, job: int, *, timeout: float | None = None) -> str: ...

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None: ...

    def job_document(self, job: int, *, timeout: float | None = None) -> Mapping[str, Any]: ...


class HttpRawCopyApi(HttpAirbyteApi):
    """The client of the record load, with the two reads that only the raw copy needs."""

    def raw_connection(self, *, timeout: float | None = None) -> RawConnection:
        """The connection that the raw copy runs and the globs of its source. Nothing is created or changed. No connection of the name, two of the name,
        a source with other than one stream and a glob list of any other form are refused. ``timeout`` is the budget of all its reads together: each read gets
        only the time that is left."""
        ends = None if timeout is None else time.monotonic() + timeout

        def left() -> float | None:
            return None if ends is None else ends - time.monotonic()

        listed = self.listing(f"/connections?workspaceIds={quote(self.workspace_id(timeout=left()), safe='')}", timeout=left())
        connection = named(listed, CONNECTION_NAME, "connection")
        if connection is None:
            raise AirbyteError("Airbyte holds no connection of the raw copy")
        what = "GET connections"
        source = self.call("GET", f"/sources/{quote(text(connection, 'sourceId', what), safe='')}", timeout=left())
        destination = self.call("GET", f"/destinations/{quote(text(connection, 'destinationId', what), safe='')}", timeout=left())
        written = fields(destination, "configuration", "GET destinations")
        bucket, landing_path = written.get("s3_bucket_name"), written.get("s3_bucket_path")
        if type(bucket) is not str or type(landing_path) is not str:
            raise AirbyteError("the destination of the raw copy names no bucket and no path as text")
        streams = fields(source, "configuration", "GET sources").get("streams")
        if type(streams) is not list or len(streams) != 1 or type(streams[0]) is not dict:
            raise AirbyteError("the source of the raw copy does not have exactly one stream")
        globs = streams[0].get("globs")
        if type(globs) is not list or any(type(item) is not str or not item for item in globs):
            raise AirbyteError("the stream of the raw copy has no list of globs")
        return RawConnection(text(connection, "connectionId", what), tuple(globs), bucket, landing_path)

    def job_connection(self, job: int, *, timeout: float | None = None) -> str:
        """The connection that the sync job belongs to, read from the job's own document. A job of another type, another number or no connection is refused."""
        what = f"GET jobs/{job}"
        document = self.call("GET", f"/jobs/{job}", timeout=timeout)
        found, _ = job_entry(document, what, text(document, "connectionId", what))
        if found != job:
            raise AirbyteError(f"{what} gave another job than the one that was asked for")
        return text(document, "connectionId", what)

    def job_document(self, job: int, *, timeout: float | None = None) -> dict[str, Any]:
        return self.call("GET", f"/jobs/{job}", timeout=timeout)

    def job_ids(self, connection_id: str, *, timeout: float | None = None) -> list[int]:
        """The IDs of every sync job of the connection, whatever its status. Every entry must be a sync job of the connection in the documented shape, or the
        lookup is refused. ``timeout`` is the budget of all its pages together."""
        return [
            job_entry(entry, "GET jobs", connection_id)[0]
            for entry in self.listing(f"/jobs?connectionId={quote(connection_id, safe='')}&jobType={SYNC}", timeout=timeout)
        ]


def check_selection(connection: RawConnection, wanted: Sequence[str]) -> None:
    """Refuse unless the source selects exactly the wanted files, each once. The order of the globs is not a difference."""
    if sorted(connection.globs) != sorted(wanted):
        raise RawCopyError("selection_differs")


def check_destination(connection: RawConnection, bucket: str) -> None:
    """Refuse unless the destination writes the project's bucket under exactly the landing prefix."""
    if connection.bucket != bucket or connection.landing_path != LANDING_PREFIX.rstrip("/"):
        raise RawCopyError("destination_differs")


def find_attempt_job(api: RawCopyApi, ledger: JobLedger, attempt: Attempt) -> int | None:
    """The job that the attempt made, by a confirmed identity: the connection's latest job whose ID was not among the IDs read before the attempt and that the
    ledger does not hold yet. The creation time of a job is not used: Airbyte reports whole seconds, so a job of the attempt's own second reads as older. None
    when there is none, and for an attempt that has no IDs from before it. A lookup that fails raises ``AirbyteError``: an unreadable list is no empty one."""
    if attempt.before is None:
        return None
    excluded = set(ledger.jobs()) | set(attempt.before)
    found = [job for job in api.job_ids(attempt.connection_id, timeout=DEFAULT_CONFIRM_SECONDS) if job not in excluded]
    return max(found) if found else None


def start(api: RawCopyApi, wanted: Sequence[str], ledger: JobLedger, bucket: str, *, now: Callable[[], float] = time.time) -> int:
    """Start one sync of the raw connection and return its job. The selection and the destination are checked first, and no job call is made when either
    differs. The attempt is kept in the ledger before the start is sent (nothing is sent when it cannot be kept), and the job is kept beside it before the job
    is returned; a job that cannot be kept is cancelled at once. A start whose reply is lost is reconciled by ``find_attempt_job``; one that cannot be is
    ``start_unconfirmed`` and its attempt stays in the ledger."""
    connection = api.raw_connection()
    check_selection(connection, wanted)
    check_destination(connection, bucket)
    if api.running_jobs(connection.connection_id):
        raise RawCopyError("job_active")
    try:
        before = tuple(sorted(api.job_ids(connection.connection_id, timeout=DEFAULT_CONFIRM_SECONDS)))
    except AirbyteError:
        raise RawCopyError("jobs_unreadable") from None
    started = now()
    try:
        attempt = Attempt(ledger.begin(connection.connection_id, started, before), connection.connection_id, started, before)
    except RawCopyError:
        raise
    except Exception:
        raise RawCopyError("state_unwritable") from None
    try:
        job = api.start_sync(connection.connection_id)
    except AirbyteError:
        try:
            found = find_attempt_job(api, ledger, attempt)
        except AirbyteError, RawCopyError:
            found = None
        if found is None:
            raise RawCopyError("start_unconfirmed") from None
        job = found
    try:
        ledger.resolve(attempt.token, job)
    except Exception:
        with contextlib.suppress(AirbyteError):
            api.cancel_job(job, timeout=DEFAULT_CONFIRM_SECONDS)
        raise RawCopyError("state_unwritable") from None
    return job


def read_budget(ends: float, clock: Callable[[], float]) -> float:
    """The time that a read may take: what is left until ``ends``, at most ``STATUS_READ_SECONDS``. ``AirbyteError`` when nothing is left, so that no read is
    sent after the budget."""
    left = ends - clock()
    if left <= 0:
        raise AirbyteError("a read had no time left in the budget of its operation")
    return min(left, STATUS_READ_SECONDS)


def require_owned(api: RawCopyApi, ledger: JobLedger, job: int, budget: float, clock: Callable[[], float] = time.monotonic) -> None:
    """Refuse (``job_not_owned``) unless the ledger holds the job with the raw connection's ID and Airbyte says the job belongs to that same connection.
    Both reads spend ``budget`` together: each gets the time that is left, and the second is not sent when the first used it up (``ownership_unconfirmed``)."""
    kept = ledger.connection_of(job)
    ends = clock() + budget
    try:
        current = api.raw_connection(timeout=read_budget(ends, clock)).connection_id
        belongs = api.job_connection(job, timeout=read_budget(ends, clock))
    except AirbyteError:
        raise RawCopyError("ownership_unconfirmed") from None
    if kept is None or kept != current or belongs != kept:
        raise RawCopyError("job_not_owned")


def read_status(api: RawCopyApi, job: int, budget: float) -> str | None:
    """The status of the job, or None when it cannot be read inside the budget."""
    try:
        return api.job_status(job, timeout=max(min(budget, STATUS_READ_SECONDS), 0.001))
    except AirbyteError:
        return None


def confirm_stopped(api: RawCopyApi, job: int, confirm_seconds: float, poll_seconds: float, clock: Callable[[], float], sleep: Callable[[float], None]) -> str:
    """Read the job until it is terminal, inside ``confirm_seconds``. Raise ``job_still_active`` when that is not confirmed: an unreadable status is no stop."""
    ends = clock() + confirm_seconds
    while True:
        status = read_status(api, job, ends - clock())
        if status in TERMINAL_STATUSES:
            return status
        if clock() >= ends:
            raise RawCopyError("job_still_active")
        sleep(min(poll_seconds, max(ends - clock(), 0.0)))


def cancel(
    api: RawCopyApi,
    job: int,
    ledger: JobLedger,
    *,
    confirm_seconds: float = DEFAULT_CONFIRM_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Cancel the job when it is active and return its terminal status. A job that has ended keeps its own status and is not cancelled. A job that this tool
    did not start is refused (``job_not_owned``) before any status read or cancel."""
    require_owned(api, ledger, job, confirm_seconds, clock)
    status = read_status(api, job, confirm_seconds)
    if status in TERMINAL_STATUSES:
        return status
    with contextlib.suppress(AirbyteError):
        api.cancel_job(job, timeout=confirm_seconds)
    return confirm_stopped(api, job, confirm_seconds, poll_seconds, clock, sleep)


def wait(
    api: RawCopyApi,
    job: int,
    ledger: JobLedger,
    *,
    deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    cancel_confirm_seconds: float = DEFAULT_CONFIRM_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """The terminal status of the job. At the deadline the job is cancelled and confirmed stopped, and the call raises ``deadline``; a stop that cannot be
    confirmed raises ``job_still_active`` instead. A status that cannot be read is treated as the deadline of that read: the job is cancelled too."""
    ends = clock() + deadline_seconds
    require_owned(api, ledger, job, deadline_seconds, clock)
    while True:
        status = read_status(api, job, ends - clock())
        late = clock() > ends
        if status in TERMINAL_STATUSES and not late:
            return status
        if status is None or late or clock() >= ends:
            cancel(api, job, ledger, confirm_seconds=cancel_confirm_seconds, poll_seconds=poll_seconds, clock=clock, sleep=sleep)
            raise RawCopyError("deadline")
        sleep(min(poll_seconds, max(ends - clock(), 0.0)))


def cancel_owned(
    api: RawCopyApi,
    ledger: JobLedger,
    *,
    confirm_seconds: float = DEFAULT_CONFIRM_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.time,
) -> dict[str, int]:
    """Reconcile each open attempt, cancel each job of the ledger that is still active and count the jobs, those that are not confirmed stopped and the
    attempts that stay unresolved. A job that has ended is not cancelled. A lookup that fails for one attempt or one job is counted and the cleanup goes on
    with the others, so one failure never leaves another job running."""
    unresolved = 0
    for attempt in ledger.attempts():
        try:
            job = find_attempt_job(api, ledger, attempt)
            if job is not None:
                ledger.resolve(attempt.token, job)
            elif now() - attempt.started >= ATTEMPT_SETTLE_SECONDS:
                ledger.settle(attempt.token)
            else:
                unresolved += 1
        except AirbyteError, RawCopyError, OSError:
            unresolved += 1
    still = 0
    jobs = ledger.jobs()
    for job in jobs:
        try:
            cancel(api, job, ledger, confirm_seconds=confirm_seconds, poll_seconds=poll_seconds, clock=clock, sleep=sleep)
        except RawCopyError:
            still += 1
    return {"jobs": len(jobs), "still_active": still, "unresolved": unresolved}


def job_record(api: RawCopyApi, job: int) -> dict[str, Any]:
    """The closed facts of the job's record: its number, its status, the two counts and its duration when Airbyte gives one as text. Nothing else is kept."""
    document = api.job_document(job)
    if document.get("jobId") != job or type(document.get("jobId")) is not int or document.get("status") not in JOB_STATUSES:
        raise RawCopyError("bad_job_record")
    record: dict[str, Any] = {"jobId": job, "status": document["status"]}
    for name in RECORD_COUNTS:
        value = document.get(name)
        if type(value) is not int or value < 0:
            raise RawCopyError("bad_job_record")
        record[name] = value
    if type(document.get("duration")) is str:
        record["duration"] = document["duration"]
    return record


def open_ledger() -> JobLedger:
    """The ledger in the state folder of the code location container (``RAW_COPY_STATE_DIR`` when it is set)."""
    return JobLedger(Path(os.environ.get(LEDGER_FOLDER_VARIABLE) or LEDGER_FOLDER_DEFAULT))


def open_api() -> HttpRawCopyApi:
    """The client for the host, from the settings of the code location container (the same ones as the record load)."""
    settings = settings_from_environment(os.environ)
    return HttpRawCopyApi(settings.airbyte_url, settings.client_id, settings.client_secret, settings.aws_access_key_id, settings.aws_secret_access_key)


def positive(value: str) -> float:
    number = float(value)
    if not 0 < number < 86400:
        raise ValueError
    return number


def show(document: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(document, sort_keys=True) + "\n")


def command(arguments: argparse.Namespace, api: RawCopyApi, ledger: JobLedger) -> int:
    wanted = fixed_sample_selection().source_keys
    if arguments.command == "status":
        connection = api.raw_connection()
        show(
            {
                "selection_is_fixed_sample": sorted(connection.globs) == sorted(wanted),
                "destination_is_project_landing": connection.bucket == arguments.bucket and connection.landing_path == LANDING_PREFIX.rstrip("/"),
                "files": len(connection.globs),
                "active_jobs": len(api.running_jobs(connection.connection_id)),
            }
        )
        return 0
    if arguments.command == "start":
        show({"job": start(api, wanted, ledger, arguments.bucket)})
        return 0
    if arguments.command == "wait":
        status = wait(api, arguments.job, ledger, deadline_seconds=arguments.deadline_seconds, poll_seconds=arguments.poll_seconds)
        show({"job": arguments.job, "status": status})
        return 0 if status == SUCCEEDED else 1
    if arguments.command == "cancel":
        show(
            {"job": arguments.job, "status": cancel(api, arguments.job, ledger, confirm_seconds=arguments.confirm_seconds, poll_seconds=arguments.poll_seconds)}
        )
        return 0
    if arguments.command == "cancel-owned":
        counts = cancel_owned(api, ledger, confirm_seconds=arguments.confirm_seconds, poll_seconds=arguments.poll_seconds)
        show(counts)
        return 0 if counts["still_active"] == 0 and counts["unresolved"] == 0 else 1
    show(job_record(api, arguments.job))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(prog="scripts.ingestion.raw_copy", description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "start"):
        commands.add_parser(name).add_argument("--bucket", required=True)
    waiter = commands.add_parser("wait")
    waiter.add_argument("--job", type=int, required=True)
    waiter.add_argument("--deadline-seconds", type=positive, default=DEFAULT_DEADLINE_SECONDS)
    waiter.add_argument("--poll-seconds", type=positive, default=DEFAULT_POLL_SECONDS)
    canceller = commands.add_parser("cancel")
    canceller.add_argument("--job", type=int, required=True)
    canceller.add_argument("--confirm-seconds", type=positive, default=DEFAULT_CONFIRM_SECONDS)
    canceller.add_argument("--poll-seconds", type=positive, default=DEFAULT_POLL_SECONDS)
    owned = commands.add_parser("cancel-owned")
    owned.add_argument("--confirm-seconds", type=positive, default=DEFAULT_CONFIRM_SECONDS)
    owned.add_argument("--poll-seconds", type=positive, default=DEFAULT_POLL_SECONDS)
    recorder = commands.add_parser("record")
    recorder.add_argument("--job", type=int, required=True)
    arguments = parser.parse_args(argv)
    try:
        return command(arguments, open_api(), open_ledger())
    except RawCopyError as error:
        sys.stderr.write(f"raw_copy refused: {error.error_code}\n")
        return 1
    except Exception as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
