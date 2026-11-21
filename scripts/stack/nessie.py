"""The Nessie operations the project needs, over Nessie's HTTP API version 2 with the standard library.

A batch run writes to a private branch and merges it after its checks pass. A reported run tags the hash of the merged data. A training run reads a
reference's hash. Every call has a deadline. A refusal is an error that carries Nessie's own error code, and a refusal caused by a hash that no longer
matches is a ``NessieConflict``. Nessie's free edition as deployed here has no sign-in, so the client sends no credential.
"""

from __future__ import annotations

import http.client
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

DEFAULT_TIMEOUT_SECONDS = 30.0
CONFLICT_STATUS = 409
BODY_LIMIT_BYTES = 1_000_000


class NessieError(RuntimeError):
    """A call did not complete or Nessie refused it. The message names the call and never holds a request body."""

    def __init__(self, message: str, *, status: int | None = None, error_code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.error_code = error_code


class NessieConflict(NessieError):
    """Nessie refused the call because the reference is no longer at the hash the caller expected, or the content changed under it."""


@dataclass(frozen=True)
class Reference:
    kind: str
    name: str
    hash: str


def reference_from(document: Any) -> Reference:
    if not isinstance(document, dict) or not all(isinstance(document.get(key), str) for key in ("type", "name", "hash")):
        raise NessieError("Nessie's answer holds no reference with a type, a name and a hash")
    return Reference(document["type"], document["name"], document["hash"])


def put_namespace(*elements: str) -> dict[str, Any]:
    """The commit operation that creates the namespace with these name elements."""
    return {"type": "PUT", "key": {"elements": list(elements)}, "content": {"type": "NAMESPACE", "elements": list(elements)}}


class NessieClient:
    """Calls to one Nessie server. ``base_url`` ends in ``/api/v2``. Every call ends within ``timeout_seconds``."""

    def __init__(self, base_url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        parts = urlsplit(base_url)
        if parts.scheme != "http" or not parts.hostname or parts.query or parts.fragment:
            raise NessieError("the Nessie address must be a plain http URL with a host, such as http://127.0.0.1:19120/api/v2")
        self.host = parts.hostname
        self.port = parts.port or 80
        self.prefix = parts.path.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        """One HTTP exchange on the wire: the status and the body. A caller that cannot reach the server directly overrides this one method."""
        headers = {"Accept": "application/json", **({"Content-Type": "application/json"} if payload is not None else {})}
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_seconds)
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(BODY_LIMIT_BYTES)
        finally:
            connection.close()

    def request(self, method: str, path: str, body: dict[str, Any] | None = None, query: dict[str, str] | None = None) -> Any:
        """One call. ``path`` follows the API prefix. Returns the decoded answer, or None for an answer with no body."""
        target = f"{self.prefix}{path}" + ("?" + "&".join(f"{quote(key)}={quote(value, safe='')}" for key, value in query.items()) if query else "")
        label = f"{method} {self.prefix}{path}"
        try:
            status, raw = self.send(method, target, None if body is None else json.dumps(body).encode())
        except TimeoutError as error:
            raise NessieError(f"{label} did not finish within {self.timeout_seconds:g} seconds") from error
        except (OSError, http.client.HTTPException) as error:
            raise NessieError(f"{label} failed: {type(error).__name__}") from error
        document = self.decode(raw)
        if status >= 400:
            code = document.get("errorCode") if isinstance(document, dict) else None
            reason = document.get("message") if isinstance(document, dict) else None
            kind = NessieConflict if status == CONFLICT_STATUS else NessieError
            raise kind(f"{label} was refused with status {status}, error code {code}: {reason}", status=status, error_code=code)
        return document

    @staticmethod
    def decode(raw: bytes) -> Any:
        if not raw.strip():
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return {"message": raw[:200].decode(errors="replace")}

    def reference(self, name: str) -> Reference:
        """The branch or tag ``name`` with the hash it points to now."""
        return reference_from((self.request("GET", f"/trees/{quote(name, safe='')}") or {}).get("reference"))

    def config(self) -> dict[str, Any]:
        """The server's configuration: the API versions it serves, its default branch and the hash of an empty history."""
        document = self.request("GET", "/config")
        if not isinstance(document, dict):
            raise NessieError("Nessie's configuration answer is not an object")
        return document

    def entries(self, name: str) -> list[str]:
        """The content keys of the branch or tag ``name``, as dotted names in order."""
        document = self.request("GET", f"/trees/{quote(name, safe='')}/entries") or {}
        return sorted(".".join(entry["name"]["elements"]) for entry in document.get("entries", []))

    def commit(self, branch: str, expected_hash: str, message: str, operations: Sequence[dict[str, Any]]) -> Reference:
        """Commit ``operations`` to ``branch``, which must still be at ``expected_hash``. Returns the branch at its new hash."""
        document = self.request(
            "POST",
            f"/trees/{quote(f'{branch}@{expected_hash}', safe='@')}/history/commit",
            {"commitMeta": {"message": message}, "operations": list(operations)},
        )
        return reference_from((document or {}).get("targetBranch"))

    def initialise(self, branch: str, namespace: str) -> bool:
        """Give ``branch`` its first commit, a namespace, when its history is empty, and return whether it did.

        Nessie cannot merge into a branch that has no commit, and an empty commit is refused, so a repository needs one commit before the first merge.
        """
        head = self.reference(branch)
        if head.hash != self.config().get("noAncestorHash"):
            return False
        self.commit(branch, head.hash, f"create namespace {namespace}", [put_namespace(namespace)])
        return True

    def create_branch(self, name: str, from_name: str = "main") -> Reference:
        """Create the branch ``name`` at the hash that ``from_name`` has now. A branch of that name that exists is a conflict."""
        source = self.reference(from_name)
        return self.create_reference("BRANCH", name, source)

    def create_tag(self, name: str, hash_value: str, from_name: str = "main") -> Reference:
        """Create the tag ``name`` on the exact hash ``hash_value``, which must be in the history of ``from_name``."""
        return self.create_reference("TAG", name, Reference("BRANCH", from_name, hash_value))

    def create_reference(self, kind: str, name: str, source: Reference) -> Reference:
        document = self.request("POST", "/trees", {"type": source.kind, "name": source.name, "hash": source.hash}, {"name": name, "type": kind})
        return reference_from((document or {}).get("reference", document))

    def delete_reference(self, name: str, hash_value: str) -> None:
        """Delete the branch or tag ``name`` when it is still at ``hash_value``."""
        self.request("DELETE", f"/trees/{quote(f'{name}@{hash_value}', safe='@')}")

    def merge(self, source: str, target: str, expected_target_hash: str, message: str) -> str:
        """Merge the branch ``source`` as it is now into ``target``, which must still be at ``expected_target_hash``. Returns the new hash of ``target``."""
        source_hash = self.reference(source).hash
        document = self.request(
            "POST",
            f"/trees/{quote(f'{target}@{expected_target_hash}', safe='@')}/history/merge",
            {"fromRefName": source, "fromHash": source_hash, "message": message},
        )
        result = document if isinstance(document, dict) else {}
        if result.get("wasApplied") is not True or not isinstance(result.get("resultantTargetHash"), str):
            raise NessieError(
                f"merge of {source} into {target} was not applied (wasApplied {result.get('wasApplied')}, wasSuccessful {result.get('wasSuccessful')})"
            )
        return str(result["resultantTargetHash"])
