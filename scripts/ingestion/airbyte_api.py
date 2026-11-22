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

Every call has a deadline. A failed, refused or late call is one ``AirbyteError`` that names the call and holds no request body, no response body and no secret.
The destination's AWS access key is sent only in the body of a create or update of the destination.
"""

from __future__ import annotations

import http.client
import json
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.record_definitions import Connector, Table

DEFAULT_TIMEOUT_SECONDS = 60.0
BODY_LIMIT_BYTES = 8_000_000
API_PREFIX = "/api/public/v1"
UNAUTHORIZED = 401
PAGE_SIZE = 100
MAX_PAGES = 1000
SECRET_FIELDS = ("access_key_id", "secret_access_key")
RUNNING_STATUSES = ("pending", "running", "incomplete")
DEFINITION_FIELDS = ("name", "dockerRepository", "dockerImageTag")
DEFINITION_NAMES = {
    definitions.SOURCE_S3.repository: "vitaldb S3 source (pinned)",
    definitions.SOURCE_FILE.repository: "vitaldb File source (pinned)",
    definitions.DESTINATION.repository: "vitaldb S3 Data Lake destination (pinned)",
}


class AirbyteError(RuntimeError):
    """A call did not complete, was refused or answered with something other than what was asked for. The message names the call only."""


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
    """One Airbyte server. ``base_url`` is a plain http URL with a host and no path. The two AWS values are the destination's access key."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        parts = urlsplit(base_url)
        if parts.scheme != "http" or not parts.hostname or parts.path not in ("", "/") or parts.query or parts.fragment:
            raise AirbyteError("the Airbyte address must be a plain http URL with a host and no path, such as http://127.0.0.1:8001")
        self.host = parts.hostname
        self.port = parts.port or 80
        self.timeout_seconds = timeout_seconds
        self.credentials = {"clientId": client_id, "clientSecret": client_secret}
        self.aws = {"access_key_id": aws_access_key_id, "secret_access_key": aws_secret_access_key}
        self.token: str | None = None
        self.workspace: str | None = None
        self.destination_id: str | None = None
        self.source_ids: dict[str, str] = {}
        self.connection_ids: dict[str, str] = {}

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
        """One HTTP exchange on the wire: the status and the body. A caller that cannot reach the server directly overrides this one method."""
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_seconds)
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(BODY_LIMIT_BYTES)
        finally:
            connection.close()

    def exchange(self, method: str, path: str, body: Mapping[str, Any] | None, *, authorized: bool = True) -> tuple[int, Any]:
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if authorized:
            headers["Authorization"] = f"Bearer {self.token}"
        label = f"{method} {path.partition('?')[0].strip('/')}"
        try:
            status, raw = self.send(method, f"{API_PREFIX}{path}", None if body is None else json.dumps(body).encode(), headers)
        except TimeoutError:
            raise AirbyteError(f"{label} did not finish within {self.timeout_seconds:g} seconds") from None
        except OSError, http.client.HTTPException:
            raise AirbyteError(f"{label} could not reach Airbyte") from None
        try:
            return status, (json.loads(raw) if raw.strip() else {})
        except ValueError:
            return status, None

    def renew_token(self) -> None:
        status, document = self.exchange("POST", "/applications/token", self.credentials, authorized=False)
        token = document.get("access_token") if isinstance(document, dict) else None
        if status >= 400 or type(token) is not str or not token:
            raise AirbyteError(f"POST applications/token was refused or gave no token (status {status})")
        self.token = token

    def call(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """One call with the bearer token, which is fetched on first use and renewed once when the server refuses it. Returns the answer, an object."""
        if self.token is None:
            self.renew_token()
        status, document = self.exchange(method, path, body)
        if status == UNAUTHORIZED:
            self.renew_token()
            status, document = self.exchange(method, path, body)
        label = f"{method} {path.partition('?')[0].strip('/')}"
        if status >= 400:
            raise AirbyteError(f"{label} was refused with status {status}")
        if not isinstance(document, dict):
            raise AirbyteError(f"{label} gave an answer that is not an object")
        return document

    def listing(self, path: str) -> list[dict[str, Any]]:
        """Every object of a list call, page by page."""
        found: list[dict[str, Any]] = []
        separator = "&" if "?" in path else "?"
        for _ in range(MAX_PAGES):
            reply = self.call("GET", f"{path}{separator}limit={PAGE_SIZE}&offset={len(found)}")
            data = reply.get("data")
            if not isinstance(data, list):
                raise AirbyteError(f"GET {path.partition('?')[0].strip('/')} gave no list of objects")
            found += [item for item in data if isinstance(item, dict)]
            if not reply.get("next") or not data:
                return found
        raise AirbyteError(f"GET {path.partition('?')[0].strip('/')} has more pages than {MAX_PAGES}")

    def workspace_id(self) -> str:
        if self.workspace is None:
            workspaces = self.listing("/workspaces")
            if len(workspaces) != 1 or not isinstance(workspaces[0].get("workspaceId"), str):
                raise AirbyteError("GET workspaces does not give exactly one workspace")
            self.workspace = workspaces[0]["workspaceId"]
        return self.workspace

    def definition_id(self, kind: str, connector: Connector) -> str:
        """The id of this tool's own definition of the connector at the pinned version: created when missing, moved to the version when it differs."""
        name = DEFINITION_NAMES[connector.repository]
        path = f"/workspaces/{quote(self.workspace_id(), safe='')}/definitions/{kind}s"
        current = named(self.listing(path), name, f"{kind} definition")
        if current is None:
            created = self.call("POST", path, {"name": name, "dockerRepository": connector.repository, "dockerImageTag": connector.tag})
            return str(created["id"])
        if current.get("dockerRepository") != connector.repository:
            raise AirbyteError(f"the {kind} definition of this tool runs another image than the pinned one")
        if current.get("dockerImageTag") != connector.tag:
            self.call("PUT", f"{path}/{quote(str(current['id']), safe='')}", {"name": name, "dockerImageTag": connector.tag})
        return str(current["id"])

    def actors(self, kind: str) -> list[dict[str, Any]]:
        return self.listing(f"/{kind}s?workspaceIds={quote(self.workspace_id(), safe='')}")

    def ensure_actor(self, kind: str, name: str, definition: str, configuration: Mapping[str, Any]) -> str:
        """The id of the source or destination of this name: created when missing, updated when its configuration lacks a value of the document."""
        id_field = f"{kind}Id"
        wanted = {key: value for key, value in configuration.items() if key not in SECRET_FIELDS}
        current = named(self.actors(kind), name, kind)
        if current is None:
            body = {"name": name, "workspaceId": self.workspace_id(), "definitionId": definition, "configuration": dict(configuration)}
            return str(self.call("POST", f"/{kind}s", body)[id_field])
        if current.get("definitionId") != definition:
            raise AirbyteError(f"the {kind} of this tool belongs to another definition than the pinned one")
        if not covers(current.get("configuration"), wanted):
            self.call("PATCH", f"/{kind}s/{quote(str(current[id_field]), safe='')}", {"configuration": dict(configuration)})
        return str(current[id_field])

    def ensure_destination(self, configuration: Mapping[str, Any]) -> None:
        definition = self.definition_id("destination", definitions.DESTINATION)
        self.destination_id = self.ensure_actor("destination", definitions.DESTINATION_NAME, definition, {**configuration, **self.aws})

    def destination_branch_now(self) -> str:
        """The branch that the destination writes to, read from Airbyte now."""
        if self.destination_id is None:
            raise AirbyteError("the destination was not ensured by this client")
        for entry in self.actors("destination"):
            if entry.get("destinationId") == self.destination_id:
                branch = (entry.get("configuration") or {}).get("main_branch_name")
                if type(branch) is str:
                    return branch
        raise AirbyteError("the destination is not in the list or names no branch")

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
            self.connection_ids[table] = str(self.call("POST", "/connections", body)["connectionId"])
            return self.connection_ids[table]
        if (current.get("sourceId"), current.get("destinationId")) != (self.source_ids[table], self.destination_id):
            raise AirbyteError(f"the connection of {table} does not join this source and this destination; Airbyte cannot move it")
        if not covers(current, dict(connection)):
            self.call("PATCH", f"/connections/{quote(str(current['connectionId']), safe='')}", connection)
        self.connection_ids[table] = str(current["connectionId"])
        return self.connection_ids[table]

    def running_jobs(self, connection_id: str) -> list[int]:
        """The IDs of the sync jobs of the connection that have not ended."""
        jobs = self.listing(f"/jobs?connectionId={quote(connection_id, safe='')}&jobType=sync")
        return [int(job["jobId"]) for job in jobs if job.get("status") in RUNNING_STATUSES]

    def start_sync(self, connection_id: str) -> int:
        job = self.call("POST", "/jobs", {"connectionId": connection_id, "jobType": "sync"}).get("jobId")
        if type(job) is not int:
            raise AirbyteError("POST jobs gave no job")
        return job

    def job_status(self, job: int) -> str:
        status = self.call("GET", f"/jobs/{job}").get("status")
        if type(status) is not str:
            raise AirbyteError("GET jobs gave no status")
        return status

    def cancel_job(self, job: int) -> None:
        self.call("DELETE", f"/jobs/{job}")
