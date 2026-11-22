"""The calls of the record connections to Airbyte's configuration API, with the standard library.

``HttpAirbyteApi`` makes the definitions, the destination, the sources and the connections match what ``record_definitions`` says, and runs, watches and
cancels a sync. It speaks the configuration API under ``/api/v1/`` of Airbyte's server (``airbyte-airbyte-server-svc``, port 8001 in the cluster), with the
access token that the instance's client credentials give. The API's paths and bodies are this code's reading of the API of Airbyte 2.x and are proven on the
host, not here.

Idempotent: each ``ensure_*`` lists what exists and creates only what is missing, and updates only what differs, so a second call with the same arguments makes
no create, no update and no schema discovery. A connection is created with the stream list that Airbyte discovers from the source, so its source must read a
file that exists, and every later run changes only the source's read prefix and the destination's branch, which leave the stream list as it is.

Every call has a deadline. A failed, refused or late call is one ``AirbyteError`` that names the call and holds no request body, no response body and no
secret. The destination's AWS access key is sent only in the body of a create or update of the destination.
"""

from __future__ import annotations

import http.client
import json
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.record_definitions import Connector, Table

DEFAULT_TIMEOUT_SECONDS = 60.0
BODY_LIMIT_BYTES = 8_000_000
API_PREFIX = "/api/v1/"
UNAUTHORIZED = 401
SECRET_FIELDS = ("access_key_id", "secret_access_key")
RUNNING_STATUSES = ("pending", "running", "incomplete")
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
        self.credentials = {"client_id": client_id, "client_secret": client_secret, "grant-type": "client_credentials"}
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

    def exchange(self, path: str, body: Mapping[str, Any], *, authorized: bool = True) -> tuple[int, Any]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if authorized:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            status, raw = self.send("POST", f"{API_PREFIX}{path}", json.dumps(body).encode(), headers)
        except TimeoutError:
            raise AirbyteError(f"{path} did not finish within {self.timeout_seconds:g} seconds") from None
        except (OSError, http.client.HTTPException) as error:
            raise AirbyteError(f"{path} failed: {type(error).__name__}") from None
        try:
            return status, (json.loads(raw) if raw.strip() else {})
        except ValueError:
            return status, None

    def renew_token(self) -> None:
        status, document = self.exchange("applications/token", self.credentials, authorized=False)
        token = document.get("access_token") if isinstance(document, dict) else None
        if status >= 400 or type(token) is not str or not token:
            raise AirbyteError(f"applications/token was refused or gave no token (status {status})")
        self.token = token

    def call(self, path: str, body: Mapping[str, Any]) -> dict[str, Any]:
        """One call with the bearer token, which is fetched on first use and renewed once when the server refuses it. Returns the answer, an object."""
        if self.token is None:
            self.renew_token()
        status, document = self.exchange(path, body)
        if status == UNAUTHORIZED:
            self.renew_token()
            status, document = self.exchange(path, body)
        if status >= 400:
            raise AirbyteError(f"{path} was refused with status {status}")
        if not isinstance(document, dict):
            raise AirbyteError(f"{path} gave an answer that is not an object")
        return document

    def workspace_id(self) -> str:
        if self.workspace is None:
            workspaces = self.call("workspaces/list", {}).get("workspaces")
            if not isinstance(workspaces, list) or len(workspaces) != 1 or not isinstance(workspaces[0].get("workspaceId"), str):
                raise AirbyteError("workspaces/list does not give exactly one workspace")
            self.workspace = workspaces[0]["workspaceId"]
        return self.workspace

    def definition_id(self, kind: str, connector: Connector) -> str:
        """The id of this tool's own definition of the connector at the pinned version: created when missing, moved to the version when it differs."""
        name = DEFINITION_NAMES[connector.repository]
        workspace = self.workspace_id()
        listed = self.call(f"{kind}_definitions/list_for_workspace", {"workspaceId": workspace}).get(f"{kind}Definitions", [])
        for entry in listed if isinstance(listed, list) else []:
            if entry.get("name") == name and entry.get("dockerRepository") == connector.repository:
                identity = entry[f"{kind}DefinitionId"]
                if entry.get("dockerImageTag") != connector.tag:
                    self.call(f"{kind}_definitions/update", {f"{kind}DefinitionId": identity, "dockerImageTag": connector.tag})
                return str(identity)
        created = self.call(
            f"{kind}_definitions/create_custom",
            {
                "workspaceId": workspace,
                f"{kind}Definition": {
                    "name": name,
                    "dockerRepository": connector.repository,
                    "dockerImageTag": connector.tag,
                    "documentationUrl": "https://docs.airbyte.com",
                },
            },
        )
        return str(created[f"{kind}DefinitionId"])

    def listed(self, path: str, field: str) -> list[dict[str, Any]]:
        entries = self.call(path, {"workspaceId": self.workspace_id()}).get(field, [])
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise AirbyteError(f"{path} gave a list of the wrong form")
        return entries

    def ensure_destination(self, configuration: Mapping[str, Any]) -> None:
        identity = self.definition_id("destination", definitions.DESTINATION)
        wanted = dict(configuration)
        for entry in self.listed("destinations/list", "destinations"):
            if entry.get("name") == definitions.DESTINATION_NAME:
                self.destination_id = str(entry["destinationId"])
                stored = {name: value for name, value in (entry.get("connectionConfiguration") or {}).items() if name not in SECRET_FIELDS}
                if stored != wanted or entry.get("destinationDefinitionId") != identity:
                    self.call(
                        "destinations/update",
                        {"destinationId": self.destination_id, "name": definitions.DESTINATION_NAME, "connectionConfiguration": {**wanted, **self.aws}},
                    )
                return
        created = self.call(
            "destinations/create",
            {
                "workspaceId": self.workspace_id(),
                "name": definitions.DESTINATION_NAME,
                "destinationDefinitionId": identity,
                "connectionConfiguration": {**wanted, **self.aws},
            },
        )
        self.destination_id = str(created["destinationId"])

    def destination_branch_now(self) -> str:
        """The branch that the destination writes to, read from Airbyte now."""
        if self.destination_id is None:
            raise AirbyteError("the destination was not ensured by this client")
        for entry in self.listed("destinations/list", "destinations"):
            if entry.get("destinationId") == self.destination_id:
                branch = (entry.get("connectionConfiguration") or {}).get("main_branch_name")
                if type(branch) is str:
                    return branch
        raise AirbyteError("the destination is not in the list or names no branch")

    def ensure_source(self, table: str, configuration: Mapping[str, Any]) -> None:
        record = table_named(table)
        identity = self.definition_id("source", definitions.source_definition(record))
        name = definitions.source_name(record)
        for entry in self.listed("sources/list", "sources"):
            if entry.get("name") == name:
                self.source_ids[table] = str(entry["sourceId"])
                if entry.get("connectionConfiguration") != dict(configuration) or entry.get("sourceDefinitionId") != identity:
                    self.call("sources/update", {"sourceId": self.source_ids[table], "name": name, "connectionConfiguration": dict(configuration)})
                return
        created = self.call(
            "sources/create", {"workspaceId": self.workspace_id(), "name": name, "sourceDefinitionId": identity, "connectionConfiguration": dict(configuration)}
        )
        self.source_ids[table] = str(created["sourceId"])

    def stream_config(self, catalog: Mapping[str, Any], table: str, wanted: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        """The catalog with only this table's stream selected and set to the wanted sync modes, and whether that differs from ``catalog``."""
        streams = [dict(stream) for stream in catalog.get("streams", [])]
        changed = False
        found = False
        for stream in streams:
            if (stream.get("stream") or {}).get("name") == table:
                found = True
                before = dict(stream.get("config") or {})
                after = {**before, "syncMode": wanted["syncMode"], "destinationSyncMode": wanted["destinationSyncMode"], "selected": True}
                changed = changed or after != before
                stream["config"] = after
        if not found:
            raise AirbyteError(f"the source of {table} does not list the stream {table}")
        return {**catalog, "streams": streams}, changed

    def ensure_connection(self, table: str, connection: Mapping[str, Any]) -> str:
        record = table_named(table)
        if table not in self.source_ids or self.destination_id is None:
            raise AirbyteError(f"the source of {table} or the destination was not ensured by this client")
        stream = connection["streams"][0]
        fixed = {key: connection[key] for key in ("status", "scheduleType", "namespaceDefinition", "namespaceFormat")}
        for entry in self.listed("connections/list", "connections"):
            if entry.get("name") == definitions.connection_name(record):
                if entry.get("sourceId") != self.source_ids[table] or entry.get("destinationId") != self.destination_id:
                    raise AirbyteError(f"the connection of {table} does not join this source and this destination")
                catalog, changed = self.stream_config(entry.get("syncCatalog") or {}, table, stream)
                if changed or any(entry.get(key) != value for key, value in fixed.items()):
                    self.call("connections/update", {"connectionId": entry["connectionId"], **fixed, "syncCatalog": catalog})
                self.connection_ids[table] = str(entry["connectionId"])
                return self.connection_ids[table]
        discovered = self.call("sources/discover_schema", {"sourceId": self.source_ids[table], "disable_cache": True})
        catalog, _ = self.stream_config(discovered.get("catalog") or {}, table, stream)
        created = self.call(
            "connections/create",
            {
                "name": definitions.connection_name(record),
                "sourceId": self.source_ids[table],
                "destinationId": self.destination_id,
                **fixed,
                "syncCatalog": catalog,
                "sourceCatalogId": discovered.get("catalogId"),
            },
        )
        self.connection_ids[table] = str(created["connectionId"])
        return self.connection_ids[table]

    def running_jobs(self, connection_id: str) -> list[int]:
        """The IDs of the sync jobs of the connection that have not ended."""
        jobs = self.call("jobs/list", {"configTypes": ["sync"], "configId": connection_id}).get("jobs", [])
        found = [item.get("job", {}) for item in jobs] if isinstance(jobs, list) else []
        return [int(job["id"]) for job in found if job.get("status") in RUNNING_STATUSES]

    def start_sync(self, connection_id: str) -> int:
        job = self.call("connections/sync", {"connectionId": connection_id}).get("job")
        if not isinstance(job, dict) or type(job.get("id")) is not int:
            raise AirbyteError("connections/sync gave no job")
        return int(job["id"])

    def job_status(self, job: int) -> str:
        document = self.call("jobs/get", {"id": job}).get("job")
        status = document.get("status") if isinstance(document, dict) else None
        if type(status) is not str:
            raise AirbyteError("jobs/get gave no status")
        return status

    def cancel_job(self, job: int) -> None:
        self.call("jobs/cancel", {"id": job})
