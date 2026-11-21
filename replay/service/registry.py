"""The calls to Apicurio Registry's core API version 3 that the replay needs, with the standard library.

``register`` puts a format in the registry under its subject and sets the artifact's compatibility rule. A format that is already registered with the same bytes
gives the same version and the same content ID, so a rerun adds nothing. ``new_version`` adds a version and is refused by the registry's rule when the change is
not compatible. Every call has a deadline. The registry in the project has no sign-in, so no credential is sent.
"""

from __future__ import annotations

import http.client
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

DEFAULT_TIMEOUT_SECONDS = 30.0
BODY_LIMIT_BYTES = 4_000_000
RULE_VIOLATION = "RuleViolationException"


class RegistryError(RuntimeError):
    """A call did not complete, or the registry refused it. The message names the call and the registry's own error name, never a schema."""

    def __init__(self, message: str, *, status: int | None = None, name: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.name = name


class RegistryRefused(RegistryError):
    """The registry's compatibility rule refused a new version."""


@dataclass(frozen=True, slots=True)
class Registered:
    group: str
    artifact_id: str
    version: str
    content_id: int
    global_id: int


class RegistryClient:
    """Calls to one registry. ``base_url`` ends in ``/apis/registry/v3``."""

    def __init__(self, base_url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        parts = urlsplit(base_url)
        if parts.scheme != "http" or not parts.hostname or parts.query or parts.fragment:
            raise RegistryError("the registry address must be a plain http URL with a host, such as http://127.0.0.1:8080/apis/registry/v3")
        self.host = parts.hostname
        self.port = parts.port or 80
        self.prefix = parts.path.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        headers = {"Accept": "application/json", **({"Content-Type": "application/json"} if payload is not None else {})}
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_seconds)
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(BODY_LIMIT_BYTES)
        finally:
            connection.close()

    def request(self, method: str, path: str, body: dict[str, Any] | None = None, *, allowed: tuple[int, ...] = ()) -> tuple[int, Any]:
        """One call. A status of 400 or more is an error, except a status in ``allowed``. Returns the status and the decoded answer."""
        label = f"{method} {self.prefix}{path.split('?')[0]}"
        try:
            status, raw = self.send(method, f"{self.prefix}{path}", None if body is None else json.dumps(body).encode())
        except TimeoutError as error:
            raise RegistryError(f"{label} did not finish within {self.timeout_seconds:g} seconds") from error
        except (OSError, http.client.HTTPException) as error:
            raise RegistryError(f"{label} failed: {type(error).__name__}") from error
        try:
            document: Any = json.loads(raw) if raw.strip() else None
        except ValueError:
            document = {"detail": raw[:200].decode(errors="replace")}
        if status >= 400 and status not in allowed:
            name = document.get("name") if isinstance(document, dict) else None
            detail = document.get("title") if isinstance(document, dict) else None
            kind = RegistryRefused if name == RULE_VIOLATION else RegistryError
            raise kind(f"{label} was refused with status {status}, {name}: {detail}", status=status, name=name)
        return status, document

    def artifact_path(self, group: str, artifact_id: str) -> str:
        return f"/groups/{quote(group, safe='')}/artifacts/{quote(artifact_id, safe='')}"

    @staticmethod
    def registered(group: str, artifact_id: str, version: Any) -> Registered:
        if not isinstance(version, dict) or not isinstance(version.get("contentId"), int) or not isinstance(version.get("globalId"), int):
            raise RegistryError("the registry's answer holds no version with a content ID and a global ID")
        return Registered(group, artifact_id, str(version["version"]), version["contentId"], version["globalId"])

    def register(self, group: str, artifact_id: str, schema_text: str, rule: str) -> Registered:
        """Create the artifact with this schema as its first version, or find the version that holds exactly these bytes, then set the compatibility rule."""
        body = {
            "artifactId": artifact_id,
            "artifactType": "AVRO",
            "firstVersion": {"version": "1", "content": {"content": schema_text, "contentType": "application/json"}},
        }
        _, document = self.request("POST", f"/groups/{quote(group, safe='')}/artifacts?ifExists=FIND_OR_CREATE_VERSION", body)
        registered = self.registered(group, artifact_id, (document or {}).get("version"))
        self.set_rule(group, artifact_id, rule)
        return registered

    def set_rule(self, group: str, artifact_id: str, rule: str) -> None:
        path = self.artifact_path(group, artifact_id) + "/rules"
        status, _ = self.request("POST", path, {"ruleType": "COMPATIBILITY", "config": rule}, allowed=(409,))
        if status == 409:
            self.request("PUT", f"{path}/COMPATIBILITY", {"ruleType": "COMPATIBILITY", "config": rule})
        _, document = self.request("GET", f"{path}/COMPATIBILITY")
        if not isinstance(document, dict) or document.get("config") != rule:
            raise RegistryError(f"the compatibility rule of {artifact_id} is not {rule} after it was set")

    def new_version(self, group: str, artifact_id: str, version: str, schema_text: str) -> Registered:
        """Add a version. A change that the compatibility rule refuses raises ``RegistryRefused``."""
        body = {"version": version, "content": {"content": schema_text, "contentType": "application/json"}}
        _, document = self.request("POST", self.artifact_path(group, artifact_id) + "/versions", body)
        return self.registered(group, artifact_id, document)

    def latest(self, group: str, artifact_id: str) -> Registered | None:
        """The latest version of an artifact, or None when the artifact does not exist."""
        status, document = self.request("GET", self.artifact_path(group, artifact_id) + "/versions/branch=latest", allowed=(404,))
        return None if status == 404 else self.registered(group, artifact_id, document)

    def schema_by_content_id(self, content_id: int) -> str:
        """The schema text that the registry holds under a content ID."""
        label = f"GET {self.prefix}/ids/contentIds/{content_id}"
        try:
            status, raw = self.send("GET", f"{self.prefix}/ids/contentIds/{content_id}", None)
        except TimeoutError as error:
            raise RegistryError(f"{label} did not finish within {self.timeout_seconds:g} seconds") from error
        except (OSError, http.client.HTTPException) as error:
            raise RegistryError(f"{label} failed: {type(error).__name__}") from error
        if status != 200:
            raise RegistryError(f"{label} was refused with status {status}", status=status)
        return raw.decode()

    def delete_artifact(self, group: str, artifact_id: str) -> bool:
        """Delete an artifact where the registry allows it. A registry with deletion off answers 405, and the call returns False."""
        status, _ = self.request("DELETE", self.artifact_path(group, artifact_id), allowed=(405, 404))
        return status in (200, 204)
