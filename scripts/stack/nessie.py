"""The Nessie operations the project needs, over Nessie's HTTP API version 2 with the standard library.

A batch run writes to a private branch and merges it after its checks pass. A reported run tags the hash of the merged data. A training run reads a
reference's hash. Every call has a deadline. A refusal is an error that carries Nessie's own error code, and a refusal caused by a hash that no longer
matches is a ``NessieConflict``. Nessie's free edition as deployed here has no sign-in, so the client sends no credential.

The command line is the deploy's step that gives main its first commit, which every later merge needs. It runs in the code location container, which
reaches Nessie on the Compose network at ``NESSIE_URI``:

``python -m scripts.stack.nessie initialise --namespace surgical_risk``

It checks that the configuration answer holds a default branch and a hash of an empty history, and refuses anything else. It commits the namespace when
main's history is empty or main's head does not hold the namespace, checks that the namespace is there, and says ``initialised``. It says ``unchanged``
only when the namespace is already at main's head, so a second run changes nothing.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

DEFAULT_TIMEOUT_SECONDS = 30.0
NAMESPACE = re.compile(r"[a-z][a-z0-9_]{0,63}")
HASH = re.compile(r"[0-9a-f]{64}")
CONFLICT_STATUS = 409
BODY_LIMIT_BYTES = 1_000_000


class PlainText(str):
    """The start of an answer body that is not JSON."""


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
        if status < 400 and isinstance(document, PlainText):
            raise NessieError(f"{label} answered with status {status} and a body that is not JSON")
        if status >= 400:
            code = document.get("errorCode") if isinstance(document, dict) else None
            reason = document.get("message") if isinstance(document, dict) else (str(document) if isinstance(document, PlainText) else None)
            kind = NessieConflict if status == CONFLICT_STATUS else NessieError
            raise kind(f"{label} was refused with status {status}, error code {code}: {reason}", status=status, error_code=code)
        return document

    @staticmethod
    def decode(raw: bytes) -> Any:
        """The JSON of an answer, None for an empty one, or ``PlainText`` with the start of a body that is not JSON.

        Only a refusal may carry ``PlainText``: ``request`` raises for a success answer that does, so a truncated or plain-text success answer is never read
        as an empty document.
        """
        if not raw.strip():
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return PlainText(raw[:200].decode(errors="replace"))

    def reference(self, name: str) -> Reference:
        """The branch or tag ``name`` with the hash it points to now."""
        return reference_from((self.request("GET", f"/trees/{quote(name, safe='')}") or {}).get("reference"))

    def config(self) -> dict[str, Any]:
        """The server's configuration: the API versions it serves, its default branch and the hash of an empty history."""
        document = self.request("GET", "/config")
        if not isinstance(document, dict):
            raise NessieError("Nessie's configuration answer is not an object")
        return document

    def has_namespace(self, branch: str, namespace: str) -> bool:
        """Whether the head of ``branch`` holds a content of the type namespace under this one name."""
        document = self.request("GET", f"/trees/{quote(branch, safe='')}/entries") or {}
        entries = document.get("entries", []) if isinstance(document, dict) else []
        return any(
            isinstance(entry, dict) and entry.get("type") == "NAMESPACE" and (entry.get("name") or {}).get("elements") == [namespace] for entry in entries
        )

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
        """Make sure the head of ``branch`` holds ``namespace`` and return whether a commit was needed.

        Nessie cannot merge into a branch that has no commit, and an empty commit is refused, so a repository needs one commit before the first merge. The
        configuration answer must name a default branch and give the hash of an empty history, else the call is refused. A commit is checked by reading the
        namespace back at the new head.
        """
        config = self.config()
        default_branch = config.get("defaultBranch")
        empty = config.get("noAncestorHash")
        if not isinstance(default_branch, str) or not default_branch or not isinstance(empty, str) or not HASH.fullmatch(empty):
            raise NessieError("Nessie's configuration answer holds no default branch and no hash of an empty history")
        head = self.reference(branch)
        if not HASH.fullmatch(head.hash):
            raise NessieError(f"Nessie's hash for {branch} is not of the expected form")
        if head.hash != empty and self.has_namespace(branch, namespace):
            return False
        self.commit(branch, head.hash, f"create namespace {namespace}", [put_namespace(namespace)])
        if not self.has_namespace(branch, namespace):
            raise NessieError(f"the namespace {namespace} is not at the head of {branch} after its commit")
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

    def merge(self, source: str, target: str, expected_target_hash: str, message: str, *, source_hash: str) -> str:
        """Merge ``source`` as it was at ``source_hash`` into ``target``, which the caller expects at ``expected_target_hash``. Returns ``target``'s new hash.

        The caller names the source hash, usually the one it checked, and the client never looks the head of ``source`` up. Nessie merges the commits of
        ``source`` up to ``source_hash`` and no later one, so a branch that has moved since is merged at the named hash only. The expected target hash is a
        guard and not a guarantee: Nessie 0.110.1 can accept an older expected hash after a compatible commit on ``target``, so a caller that must know what
        happened reads the history of ``target``.
        """
        if not HASH.fullmatch(source_hash):
            raise ValueError("the source hash must be 64 lower-case hex digits")
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


def main(argv: list[str] | None = None) -> int:
    """Give main the project namespace when its head lacks it. Exit 0 when main holds the namespace, 1 when Nessie fails or refuses, 2 for bad input."""
    parser = argparse.ArgumentParser(prog="scripts.stack.nessie", description="Give Nessie's main branch its first commit, a namespace, once.")
    commands = parser.add_subparsers(dest="command", required=True)
    initialise = commands.add_parser("initialise")
    initialise.add_argument("--namespace", required=True, help="the namespace of the first commit: lower-case letters, digits and underscores")
    initialise.add_argument("--branch", default="main")
    arguments = parser.parse_args(argv)
    uri = os.environ.get("NESSIE_URI", "")
    if not uri:
        sys.stderr.write("NESSIE_URI is not set: it names Nessie's API, such as http://nessie:19120/api/v2\n")
        return 2
    if not NAMESPACE.fullmatch(arguments.namespace):
        sys.stderr.write("the namespace must be lower-case letters, digits and underscores, starting with a letter\n")
        return 2
    try:
        created = NessieClient(uri).initialise(arguments.branch, arguments.namespace)
    except NessieError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    if created:
        sys.stdout.write(f"initialised: {arguments.branch} holds the namespace {arguments.namespace}, committed now\n")
    else:
        sys.stdout.write(f"unchanged: {arguments.branch} already holds the namespace {arguments.namespace}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
