"""The calls of the record connections to Airbyte's public API, with the standard library.

``HttpAirbyteApi`` makes the custom definitions, the destination, the sources and the connections match what ``record_definitions`` says, and runs, watches and
cancels a sync. It speaks the public API under ``/api/public/v1`` of Airbyte's server, with the access token that the instance's client credentials give: the
same reading of the API that the raw connection's module makes (``ansible/roles/airbyte/library/airbyte_connection.py``). The paths and bodies are proven on the
host, not here.

Idempotent: each ``ensure_*`` lists what exists and creates only what is missing, and updates only what lacks a value of the document, so a second call with the
same arguments makes no create and no update. Airbyte adds fields of its own to an answer, so only the document's values are compared. A connection is created
without a stream list of its own: Airbyte reads the source's schema when it creates the connection, so each source must read a file that exists. Every later run
changes only the source's read prefix and the destination's branch. An object that belongs to another definition or joins other objects, and a name that two
objects hold, are refused and changed nowhere: deleting a connection would delete its sync state.

Every answer that the load reads is read through a closed shape: a field that is missing, of another type or outside its closed set refuses the call with an
error that names the call and the field, never the text of the answer. A job is read through the documented shape of ``JobResponse`` (``jobId`` whole number,
``status`` one of ``JOB_STATUSES``, ``jobType`` and ``connectionId``), and a status outside the set, or a job entry outside the shape, refuses the load.
The statuses are the ``JobStatusEnum`` of the public API in ``airbyte-api/server-api/src/main/openapi/api_documentation_jobs.yaml`` of
``airbytehq/airbyte-platform`` (main branch, read on Nov 22 2026): pending, queued, running, incomplete, failed, succeeded and cancelled. A job is active
while it is pending, queued or running.

Every exchange has one elapsed deadline: the smaller of the client's own limit and the time that the caller's operation has left (``timeout``). The connection
carries the deadline as its socket timeout, which bounds the connect and each wait for a byte, and a watchdog timer shuts the socket at the deadline, which
bounds a server that trickles its headers or its body. The standard library offers no other bound on a whole exchange without socket machinery of its own, and
this is not a second transport: it is the same connection. Remaining overshoot: an exchange ends within about one second after its deadline (the watchdog's
wake-up and the unwinding of a blocked read), and the test holds it under 1.5 seconds.

The destination's AWS access key is sent only in the body of a create or update of the destination, and no message holds a request body, a response body or a
secret.

``.venv/bin/python -m scripts.ingestion.airbyte_api check --address http://10.42.0.1:8001`` says whether an address may receive the credentials (exit 0 or 1);
the deploy runs it, so that the client, the settings and the deploy share one definition of the allowed ranges.
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import ipaddress
import json
import re
import socket
import sys
import threading
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.record_definitions import Connector, Table

DEFAULT_TIMEOUT_SECONDS = 60.0
BODY_LIMIT_BYTES = 8_000_000
API_PREFIX = "/api/public/v1"
UNAUTHORIZED = 401
REDIRECT_FIRST = 300
REDIRECT_END = 400
PAGE_SIZE = 100
MAX_PAGES = 1000
SECRET_FIELDS = ("access_key_id", "secret_access_key")
JOB_STATUSES = ("pending", "queued", "running", "incomplete", "failed", "succeeded", "cancelled")
ACTIVE_STATUSES = ("pending", "queued", "running")
TERMINAL_STATUSES = ("incomplete", "failed", "succeeded", "cancelled")
SYNC = "sync"
# The addresses that the deploy can write: the bridge address that k3s gives the node is a private address, and the code location reaches it through the host.
PRIVATE_NETWORKS = tuple(ipaddress.IPv4Network(network) for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16"))
ORIGIN = re.compile(r"http://([0-9]{1,3}(?:\.[0-9]{1,3}){3}):([0-9]{1,5})")
DEFINITION_FIELDS = ("name", "dockerRepository", "dockerImageTag")
DEFINITION_NAMES = {
    definitions.SOURCE_S3.repository: "vitaldb S3 source (pinned)",
    definitions.SOURCE_FILE.repository: "vitaldb File source (pinned)",
    definitions.DESTINATION.repository: "vitaldb S3 Data Lake destination (pinned)",
}


class AirbyteError(RuntimeError):
    """A call did not complete, was refused or answered with something else than was asked. The message names the call or a field and never an answer."""


def check_api_origin(address: str) -> tuple[str, int]:
    """The host and the port of an address that may receive the client's credentials, or ``AirbyteError``.

    Both credentials and the destination's AWS access key go to this address, so it is exactly ``http://<IPv4 literal>:<port>`` and nothing after the port (no
    slash, no query delimiter, no fragment, no user information, no host name that could resolve elsewhere), with an address in ``PRIVATE_NETWORKS``.
    """
    found = ORIGIN.fullmatch(address)
    try:
        ip = ipaddress.IPv4Address(found.group(1)) if found else None
        port = int(found.group(2)) if found else 0
    except ValueError:
        ip, port = None, 0
    if found is None or ip is None or not 1 <= port <= 65535 or str(port) != found.group(2) or not any(ip in network for network in PRIVATE_NETWORKS):
        raise AirbyteError("the Airbyte address must be exactly http://<a private IPv4 address>:<port>, with nothing after the port")
    return str(ip), port


def text(entry: Mapping[str, Any], name: str, what: str) -> str:
    value = entry.get(name)
    if type(value) is not str or not value:
        raise AirbyteError(f"{what} has no text field {name}")
    return value


def whole(entry: Mapping[str, Any], name: str, what: str) -> int:
    value = entry.get(name)
    if type(value) is not int:
        raise AirbyteError(f"{what} has no whole number field {name}")
    return value


def fields(entry: Mapping[str, Any], name: str, what: str) -> dict[str, Any]:
    value = entry.get(name)
    if type(value) is not dict:
        raise AirbyteError(f"{what} has no object field {name}")
    return value


def job_entry(entry: object, what: str, connection_id: str | None = None) -> tuple[int, str]:
    """The ID and the status of a job read in the documented shape of ``JobResponse``, or ``AirbyteError``."""
    if type(entry) is not dict:
        raise AirbyteError(f"{what} gave a job entry that is not an object")
    job = whole(entry, "jobId", what)
    status = text(entry, "status", what)
    if status not in JOB_STATUSES:
        raise AirbyteError(f"{what} gave a job status outside the statuses of the API")
    if connection_id is not None and (text(entry, "connectionId", what) != connection_id or text(entry, "jobType", what) != SYNC):
        raise AirbyteError(f"{what} gave a job that is not a sync of the connection that was asked for")
    return job, status


def table_named(name: str) -> Table:
    for table in definitions.TABLES:
        if table.name == name:
            return table
    raise AirbyteError(f"{name} is not a record table")


def covers(actual: Any, wanted: Any) -> bool:
    """Whether ``actual`` holds every value of ``wanted``. An object may hold more fields. A list must match item by item, in order, at its length."""
    if isinstance(wanted, dict):
        return isinstance(actual, dict) and all(name in actual and covers(actual[name], value) for name, value in wanted.items())
    if isinstance(wanted, list):
        return isinstance(actual, list) and len(actual) == len(wanted) and all(covers(left, right) for left, right in zip(actual, wanted, strict=True))
    return type(actual) is type(wanted) and actual == wanted


def named(objects: list[dict[str, Any]], name: str, what: str) -> dict[str, Any] | None:
    matches = [item for item in objects if item.get("name") == name]
    if len(matches) > 1:
        raise AirbyteError(f"{len(matches)} {what} objects are named like this tool's own; Airbyte must hold one")
    return matches[0] if matches else None


class HttpAirbyteApi:
    """One Airbyte server at ``base_url``, which ``check_api_origin`` accepts. The two AWS values are the destination's access key."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.host, self.port = check_api_origin(base_url)
        self.timeout_seconds = timeout_seconds
        self.credentials = {"clientId": client_id, "clientSecret": client_secret}
        self.aws = {"access_key_id": aws_access_key_id, "secret_access_key": aws_secret_access_key}
        self.token: str | None = None
        self.workspace: str | None = None
        self.destination_id: str | None = None
        self.source_ids: dict[str, str] = {}
        self.connection_ids: dict[str, str] = {}

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str], budget: float) -> tuple[int, bytes]:
        """One HTTP exchange on the wire that ends within ``budget`` seconds: the status and the body. A caller with no direct route overrides this one method.

        The budget is the socket's timeout, and a watchdog shuts the socket when it passes, so a trickled header or body cannot hold the exchange.
        """
        connection = http.client.HTTPConnection(self.host, self.port, timeout=budget)
        expired = threading.Event()

        def shut() -> None:
            expired.set()
            sock = connection.sock
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
                sock.close()

        watchdog = threading.Timer(budget, shut)
        watchdog.daemon = True
        watchdog.start()
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            body = response.read(BODY_LIMIT_BYTES)
        except OSError, http.client.HTTPException:
            if expired.is_set():
                raise TimeoutError from None
            raise
        finally:
            watchdog.cancel()
            connection.close()
        if expired.is_set():
            raise TimeoutError
        return response.status, body

    def exchange(self, method: str, path: str, body: Mapping[str, Any] | None, budget: float, *, authorized: bool = True) -> tuple[int, Any]:
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if authorized:
            headers["Authorization"] = f"Bearer {self.token}"
        label = f"{method} {path.partition('?')[0].strip('/')}"
        try:
            status, raw = self.send(method, f"{API_PREFIX}{path}", None if body is None else json.dumps(body).encode(), headers, budget)
        except TimeoutError:
            raise AirbyteError(f"{label} did not finish within its deadline") from None
        except OSError, http.client.HTTPException:
            raise AirbyteError(f"{label} could not reach Airbyte") from None
        if REDIRECT_FIRST <= status < REDIRECT_END:
            raise AirbyteError(f"{label} answered with a redirect, which is refused: the credentials go to one address only")
        try:
            return status, (json.loads(raw) if raw.strip() else {})
        except ValueError:
            return status, None

    def renew_token(self, budget: float) -> None:
        status, document = self.exchange("POST", "/applications/token", self.credentials, budget, authorized=False)
        token = document.get("access_token") if isinstance(document, dict) else None
        if status >= 400 or type(token) is not str or not token:
            raise AirbyteError(f"POST applications/token was refused or gave no token (status {status})")
        self.token = token

    def call(self, method: str, path: str, body: Mapping[str, Any] | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        """One call with the bearer token, which is fetched on first use and renewed once when the server refuses it. Returns the answer, an object.

        ``timeout`` is the time that the caller's operation has left. The call, with its token renewal and its repeat, ends inside the smaller of that and the
        client's own limit, and a call with no time left makes no request.
        """
        label = f"{method} {path.partition('?')[0].strip('/')}"
        ends = time.monotonic() + (self.timeout_seconds if timeout is None else min(self.timeout_seconds, timeout))

        def left() -> float:
            remaining = ends - time.monotonic()
            if remaining <= 0:
                raise AirbyteError(f"{label} had no time left in the budget of its operation")
            return remaining

        if self.token is None:
            self.renew_token(left())
        status, document = self.exchange(method, path, body, left())
        if status == UNAUTHORIZED:
            self.renew_token(left())
            status, document = self.exchange(method, path, body, left())
        if status >= 400:
            raise AirbyteError(f"{label} was refused with status {status}")
        if not isinstance(document, dict):
            raise AirbyteError(f"{label} gave an answer that is not an object")
        return document

    def listing(self, path: str) -> list[dict[str, Any]]:
        """Every object of a list call, page by page. An entry that is not an object, or a ``next`` that is not text, refuses the call."""
        label = f"GET {path.partition('?')[0].strip('/')}"
        found: list[dict[str, Any]] = []
        separator = "&" if "?" in path else "?"
        for _ in range(MAX_PAGES):
            reply = self.call("GET", f"{path}{separator}limit={PAGE_SIZE}&offset={len(found)}")
            data = reply.get("data")
            following = reply.get("next")
            if not isinstance(data, list) or any(type(item) is not dict for item in data) or not (following is None or type(following) is str):
                raise AirbyteError(f"{label} gave a page that is not of the documented form")
            found += data
            if not following or not data:
                return found
        raise AirbyteError(f"{label} has more pages than {MAX_PAGES}")

    def workspace_id(self) -> str:
        if self.workspace is None:
            workspaces = self.listing("/workspaces")
            if len(workspaces) != 1:
                raise AirbyteError("GET workspaces does not give exactly one workspace")
            self.workspace = text(workspaces[0], "workspaceId", "GET workspaces")
        return self.workspace

    def definition_id(self, kind: str, connector: Connector) -> str:
        """The id of this tool's own definition of the connector at the pinned version: created when missing, moved to the version when it differs."""
        name = DEFINITION_NAMES[connector.repository]
        path = f"/workspaces/{quote(self.workspace_id(), safe='')}/definitions/{kind}s"
        current = named(self.listing(path), name, f"{kind} definition")
        if current is None:
            created = self.call("POST", path, {"name": name, "dockerRepository": connector.repository, "dockerImageTag": connector.tag})
            return text(created, "id", f"POST {path.rsplit('/', 1)[-1]}")
        what = f"GET {kind} definitions"
        if text(current, "dockerRepository", what) != connector.repository:
            raise AirbyteError(f"the {kind} definition of this tool runs another image than the pinned one")
        identity = text(current, "id", what)
        if text(current, "dockerImageTag", what) != connector.tag:
            self.call("PUT", f"{path}/{quote(identity, safe='')}", {"name": name, "dockerImageTag": connector.tag})
        return identity

    def actors(self, kind: str) -> list[dict[str, Any]]:
        return self.listing(f"/{kind}s?workspaceIds={quote(self.workspace_id(), safe='')}")

    def ensure_actor(self, kind: str, name: str, definition: str, configuration: Mapping[str, Any]) -> str:
        """The id of the source or destination of this name: created when missing, updated when its configuration lacks a value of the document."""
        id_field = f"{kind}Id"
        what = f"GET {kind}s"
        wanted = {key: value for key, value in configuration.items() if key not in SECRET_FIELDS}
        current = named(self.actors(kind), name, kind)
        if current is None:
            body = {"name": name, "workspaceId": self.workspace_id(), "definitionId": definition, "configuration": dict(configuration)}
            return text(self.call("POST", f"/{kind}s", body), id_field, f"POST {kind}s")
        identity = text(current, id_field, what)
        if text(current, "definitionId", what) != definition:
            raise AirbyteError(f"the {kind} of this tool belongs to another definition than the pinned one")
        if not covers(fields(current, "configuration", what), wanted):
            self.call("PATCH", f"/{kind}s/{quote(identity, safe='')}", {"configuration": dict(configuration)})
        return identity

    def ensure_destination(self, configuration: Mapping[str, Any]) -> None:
        definition = self.definition_id("destination", definitions.DESTINATION)
        self.destination_id = self.ensure_actor("destination", definitions.DESTINATION_NAME, definition, {**configuration, **self.aws})

    def destination_branch_now(self) -> str:
        """The branch that the destination writes to, read from Airbyte now."""
        if self.destination_id is None:
            raise AirbyteError("the destination was not ensured by this client")
        for entry in self.actors("destination"):
            if entry.get("destinationId") == self.destination_id:
                return text(fields(entry, "configuration", "GET destinations"), "main_branch_name", "GET destinations")
        raise AirbyteError("the destination is not in the list")

    def ensure_source(self, table: str, configuration: Mapping[str, Any]) -> None:
        record = table_named(table)
        definition = self.definition_id("source", definitions.source_definition(record))
        self.source_ids[table] = self.ensure_actor("source", definitions.source_name(record), definition, configuration)

    def ensure_connection(self, table: str, connection: Mapping[str, Any]) -> str:
        record = table_named(table)
        if table not in self.source_ids or self.destination_id is None:
            raise AirbyteError(f"the source of {table} or the destination was not ensured by this client")
        name = definitions.connection_name(record)
        listed = self.listing(f"/connections?workspaceIds={quote(self.workspace_id(), safe='')}")
        current = named(listed, name, "connection")
        if current is None:
            body = {**connection, "sourceId": self.source_ids[table], "destinationId": self.destination_id}
            self.connection_ids[table] = text(self.call("POST", "/connections", body), "connectionId", "POST connections")
            return self.connection_ids[table]
        what = "GET connections"
        identity = text(current, "connectionId", what)
        if (text(current, "sourceId", what), text(current, "destinationId", what)) != (self.source_ids[table], self.destination_id):
            raise AirbyteError(f"the connection of {table} does not join this source and this destination; Airbyte cannot move it")
        if not covers(current, dict(connection)):
            self.call("PATCH", f"/connections/{quote(identity, safe='')}", connection)
        self.connection_ids[table] = identity
        return identity

    def find_connection_ids(self) -> dict[str, str]:
        """The connections of the three tables that exist now, by table name. Nothing is created or changed. A name that two connections hold is refused."""
        listed = self.listing(f"/connections?workspaceIds={quote(self.workspace_id(), safe='')}")
        found: dict[str, str] = {}
        for table in definitions.TABLES:
            current = named(listed, definitions.connection_name(table), "connection")
            if current is not None:
                found[table.name] = text(current, "connectionId", "GET connections")
        return found

    def running_jobs(self, connection_id: str) -> list[int]:
        """The IDs of the sync jobs of the connection that are active (pending, queued or running). Every entry must be a job in the documented shape."""
        what = "GET jobs"
        jobs = self.listing(f"/jobs?connectionId={quote(connection_id, safe='')}&jobType={SYNC}")
        read = [job_entry(entry, what, connection_id) for entry in jobs]
        return [job for job, status in read if status in ACTIVE_STATUSES]

    def start_sync(self, connection_id: str) -> int:
        answer = self.call("POST", "/jobs", {"connectionId": connection_id, "jobType": SYNC})
        job, _ = job_entry(answer, "POST jobs")
        return job

    def job_status(self, job: int, *, timeout: float | None = None) -> str:
        """The status of the job, one of ``JOB_STATUSES``, read inside ``timeout`` seconds when it is given."""
        what = f"GET jobs/{job}"
        found, status = job_entry(self.call("GET", f"/jobs/{job}", timeout=timeout), what)
        if found != job:
            raise AirbyteError(f"{what} gave another job than the one that was asked for")
        return status

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None:
        self.call("DELETE", f"/jobs/{job}", timeout=timeout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.ingestion.airbyte_api", description="Check an address that may receive the Airbyte credentials.")
    commands = parser.add_subparsers(dest="command", required=True)
    checker = commands.add_parser("check")
    checker.add_argument("--address", required=True)
    arguments = parser.parse_args(argv)
    try:
        check_api_origin(arguments.address)
    except AirbyteError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
