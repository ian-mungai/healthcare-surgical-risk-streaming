"""The sync of the raw copy, driven from the code location container on the host: start it only on the fixed sample, watch it, cancel it, read its record.

The raw connection (``scripts.ingestion.connection``) is made by the host's connection step with the selection that the host's environment names: the fixed
sample on a dev host and the full release on a prod host. Nothing else starts its sync. This module is that start, for the live batch of the ingestion phase.
It runs where Airbyte's client credentials are, which is the code location container, and it prints closed facts only (a job number, a status, two counts and
a duration): no glob, no ID of Airbyte, no credential and no text of an exception.

``start`` refuses, before any job call, a connection whose source selects any other set of files than the fixed sample's (so a dev host that still holds the
full release's selection copies nothing), and a connection that has an active job (so no second copy runs beside the first). ``wait`` ends at its deadline:
a job that is still running then is cancelled and the cancellation is confirmed, and a job that cannot be confirmed stopped is named ``job_still_active``.

``docker compose exec -T dagster_code_location python -m scripts.ingestion.raw_copy status``

``... raw_copy start``, ``... raw_copy wait --job N --deadline-seconds S``, ``... raw_copy cancel --job N``, ``... raw_copy record --job N``
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

from scripts.ingestion.airbyte_api import JOB_STATUSES, TERMINAL_STATUSES, AirbyteError, HttpAirbyteApi, fields, named, text
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.connection import CONNECTION_NAME, fixed_sample_selection
from scripts.ingestion.notice import describe_error
from scripts.ingestion.record_run import settings_from_environment

SUCCEEDED = "succeeded"
DEFAULT_DEADLINE_SECONDS = 3600.0
DEFAULT_POLL_SECONDS = 10.0
DEFAULT_CONFIRM_SECONDS = 120.0
STATUS_READ_SECONDS = 30.0
RECORD_COUNTS = ("rowsSynced", "bytesSynced")


class RawCopyError(RuntimeError):
    """A refusal of this module. ``error_code`` is one fixed word, and it is the only thing that a command shows of the refusal."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RawConnection:
    """The raw connection's ID and the globs that its source selects, in the order Airbyte keeps them."""

    connection_id: str
    globs: tuple[str, ...]


class RawCopyApi(Protocol):
    def raw_connection(self) -> RawConnection: ...

    def running_jobs(self, connection_id: str) -> list[int]: ...

    def start_sync(self, connection_id: str) -> int: ...

    def job_status(self, job: int, *, timeout: float | None = None) -> str: ...

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None: ...

    def job_document(self, job: int, *, timeout: float | None = None) -> Mapping[str, Any]: ...


class HttpRawCopyApi(HttpAirbyteApi):
    """The client of the record load, with the two reads that only the raw copy needs."""

    def raw_connection(self) -> RawConnection:
        """The connection that the raw copy runs and the globs of its source. Nothing is created or changed. No connection of the name, two of the name,
        a source with other than one stream and a glob list of any other form are refused."""
        listed = self.listing(f"/connections?workspaceIds={quote(self.workspace_id(), safe='')}")
        connection = named(listed, CONNECTION_NAME, "connection")
        if connection is None:
            raise AirbyteError("Airbyte holds no connection of the raw copy")
        what = "GET connections"
        source = self.call("GET", f"/sources/{quote(text(connection, 'sourceId', what), safe='')}")
        streams = fields(source, "configuration", "GET sources").get("streams")
        if type(streams) is not list or len(streams) != 1 or type(streams[0]) is not dict:
            raise AirbyteError("the source of the raw copy does not have exactly one stream")
        globs = streams[0].get("globs")
        if type(globs) is not list or any(type(item) is not str or not item for item in globs):
            raise AirbyteError("the stream of the raw copy has no list of globs")
        return RawConnection(text(connection, "connectionId", what), tuple(globs))

    def job_document(self, job: int, *, timeout: float | None = None) -> dict[str, Any]:
        return self.call("GET", f"/jobs/{job}", timeout=timeout)


def check_selection(connection: RawConnection, wanted: Sequence[str]) -> None:
    """Refuse unless the source selects exactly the wanted files, each once. The order of the globs is not a difference."""
    if sorted(connection.globs) != sorted(wanted):
        raise RawCopyError("selection_differs")


def start(api: RawCopyApi, wanted: Sequence[str]) -> int:
    """Start one sync of the raw connection and return its job. The selection is checked first, and no job call is made when it differs."""
    connection = api.raw_connection()
    check_selection(connection, wanted)
    if api.running_jobs(connection.connection_id):
        raise RawCopyError("job_active")
    return api.start_sync(connection.connection_id)


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
    *,
    confirm_seconds: float = DEFAULT_CONFIRM_SECONDS,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Cancel the job when it is active and return its terminal status. A job that has ended keeps its own status and is not cancelled."""
    status = read_status(api, job, confirm_seconds)
    if status in TERMINAL_STATUSES:
        return status
    with contextlib.suppress(AirbyteError):
        api.cancel_job(job, timeout=confirm_seconds)
    return confirm_stopped(api, job, confirm_seconds, poll_seconds, clock, sleep)


def wait(
    api: RawCopyApi,
    job: int,
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
    while True:
        status = read_status(api, job, ends - clock())
        if status in TERMINAL_STATUSES:
            return status
        if status is None or clock() >= ends:
            cancel(api, job, confirm_seconds=cancel_confirm_seconds, poll_seconds=poll_seconds, clock=clock, sleep=sleep)
            raise RawCopyError("deadline")
        sleep(min(poll_seconds, max(ends - clock(), 0.0)))


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


def command(arguments: argparse.Namespace, api: RawCopyApi) -> int:
    wanted = fixed_sample_selection().source_keys
    if arguments.command == "status":
        connection = api.raw_connection()
        show(
            {
                "selection_is_fixed_sample": sorted(connection.globs) == sorted(wanted),
                "files": len(connection.globs),
                "active_jobs": len(api.running_jobs(connection.connection_id)),
            }
        )
        return 0
    if arguments.command == "start":
        show({"job": start(api, wanted)})
        return 0
    if arguments.command == "wait":
        status = wait(api, arguments.job, deadline_seconds=arguments.deadline_seconds, poll_seconds=arguments.poll_seconds)
        show({"job": arguments.job, "status": status})
        return 0 if status == SUCCEEDED else 1
    if arguments.command == "cancel":
        show({"job": arguments.job, "status": cancel(api, arguments.job, confirm_seconds=arguments.confirm_seconds, poll_seconds=arguments.poll_seconds)})
        return 0
    show(job_record(api, arguments.job))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = QuietParser(prog="scripts.ingestion.raw_copy", description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    commands.add_parser("start")
    waiter = commands.add_parser("wait")
    waiter.add_argument("--job", type=int, required=True)
    waiter.add_argument("--deadline-seconds", type=positive, default=DEFAULT_DEADLINE_SECONDS)
    waiter.add_argument("--poll-seconds", type=positive, default=DEFAULT_POLL_SECONDS)
    canceller = commands.add_parser("cancel")
    canceller.add_argument("--job", type=int, required=True)
    canceller.add_argument("--confirm-seconds", type=positive, default=DEFAULT_CONFIRM_SECONDS)
    canceller.add_argument("--poll-seconds", type=positive, default=DEFAULT_POLL_SECONDS)
    recorder = commands.add_parser("record")
    recorder.add_argument("--job", type=int, required=True)
    arguments = parser.parse_args(argv)
    try:
        return command(arguments, open_api())
    except RawCopyError as error:
        sys.stderr.write(f"raw_copy refused: {error.error_code}\n")
        return 1
    except Exception as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
