"""The one way the project asks Trino a question: its statement protocol over HTTP with the standard library.

Trino as deployed here has no sign-in and its access control is ``read-only``, so a statement that would write is refused by the server with the error
name ``PERMISSION_DENIED``. Every query ends within its deadline. A caller that cannot reach the server directly overrides ``send``, the one method that
touches the wire.
"""

from __future__ import annotations

import http.client
import json
import time
from typing import Any
from urllib.parse import urlsplit

DEFAULT_DEADLINE_SECONDS = 120.0
CALL_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.2
BODY_LIMIT_BYTES = 50_000_000
DEFAULT_USER = "catalog_proof"


class TrinoError(RuntimeError):
    """A query did not finish or Trino refused it. The message names the call and holds Trino's own message, never a request body."""

    def __init__(self, message: str, *, error_name: str | None = None) -> None:
        super().__init__(message)
        self.error_name = error_name


class TrinoClient:
    """Queries to one Trino server whose ``base_url`` is a plain http address such as ``http://127.0.0.1:8080``."""

    def __init__(self, base_url: str, user: str = DEFAULT_USER, deadline_seconds: float = DEFAULT_DEADLINE_SECONDS) -> None:
        parts = urlsplit(base_url)
        if parts.scheme != "http" or not parts.hostname or parts.path not in ("", "/") or parts.query or parts.fragment:
            raise TrinoError("the Trino address must be a plain http URL with a host and no path, such as http://127.0.0.1:8080")
        try:
            port = parts.port
        except ValueError:
            raise TrinoError("the Trino address has a port that is not a number from 0 to 65535") from None
        self.host = parts.hostname
        self.port = port or 80
        self.user = user
        self.deadline_seconds = deadline_seconds

    def send(self, method: str, target: str, payload: bytes | None, headers: dict[str, str], timeout_seconds: float) -> tuple[int, bytes]:
        """One HTTP exchange on the wire: the status and the body."""
        connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout_seconds)
        try:
            connection.request(method, target, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, response.read(BODY_LIMIT_BYTES)
        finally:
            connection.close()

    def call(self, method: str, target: str, payload: bytes | None, deadline: float) -> dict[str, Any]:
        label = f"{method} {urlsplit(target).path.rsplit('/', 2)[0]} on {self.host}:{self.port}"
        headers = {"X-Trino-User": self.user, "X-Trino-Source": "catalog_proof", "Accept": "application/json"}
        timeout = max(0.1, min(CALL_TIMEOUT_SECONDS, deadline - time.monotonic()))
        try:
            status, raw = self.send(method, target, payload, headers, timeout)
        except TimeoutError as error:
            raise TrinoError(f"{label} did not answer within {timeout:g} seconds") from error
        except (OSError, http.client.HTTPException) as error:
            raise TrinoError(f"{label} failed: {type(error).__name__}") from error
        if status >= 400 and not raw.strip().startswith(b"{"):
            raise TrinoError(f"{label} was refused with status {status}")
        try:
            document = json.loads(raw) if raw.strip() else {}
        except ValueError as error:
            raise TrinoError(f"{label} answered with something that is not JSON (status {status})") from error
        if not isinstance(document, dict):
            raise TrinoError(f"{label} answered with something that is not an object")
        return document

    def query(self, sql: str) -> list[list[Any]]:
        """Run one statement and return its rows. The statement follows the server's access control: Trino here refuses a write."""
        deadline = time.monotonic() + self.deadline_seconds
        document = self.call("POST", "/v1/statement", sql.encode(), deadline)
        rows: list[list[Any]] = []
        while True:
            error = document.get("error")
            if isinstance(error, dict):
                raise TrinoError(f"Trino refused the statement: {error.get('message')}", error_name=str(error.get("errorName")))
            rows.extend(document.get("data") or [])
            next_uri = document.get("nextUri")
            if not isinstance(next_uri, str):
                return rows
            target = urlsplit(next_uri)
            path = target.path + (f"?{target.query}" if target.query else "")
            if time.monotonic() >= deadline:
                self.cancel(path)
                raise TrinoError(f"the query did not finish within {self.deadline_seconds:g} seconds and was cancelled")
            time.sleep(POLL_SECONDS)
            document = self.call("GET", path, None, deadline)

    def cancel(self, path: str) -> None:
        """Ask Trino to stop the query behind ``path``. A failure to cancel is not raised: the query already failed its deadline."""
        try:
            self.call("DELETE", path, None, time.monotonic() + CALL_TIMEOUT_SECONDS)
        except TrinoError:
            return
