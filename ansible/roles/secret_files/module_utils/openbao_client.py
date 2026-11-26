"""A small OpenBao HTTP client for a module that reads or writes one secret: fixed diagnostics only.

Every request of this client carries a secret (a secret ID in a login, a token in a header or a password in a write body), and an answer can repeat what it was
sent. So no failure holds any part of an answer or of the HTTP library's text: each is a fixed message with the name of the call and, where there is one, the
status. A token goes into a header only when it has the form of a token, so a line break cannot add a header or put the token into the library's error.

It uses only the standard library and runs on the host's Python. The address is plain HTTP, as the host's OpenBao listens on its own loopback address.
"""

from __future__ import annotations

import http.client
import json
import re
from typing import Any
from urllib.parse import urlsplit

HTTP_SECONDS = 10
# The characters of OpenBao's tokens and of a bearer token (RFC 6750, b64token), and nothing else.
TOKEN_FORM = re.compile(r"[A-Za-z0-9\-._~+/]+=*")


class OpenBaoError(RuntimeError):
    """A call failed or an answer was not usable. The message never holds a secret, an answer's text or the library's text."""


class OpenBaoClient:
    def __init__(self, address: str) -> None:
        parts = urlsplit(address)
        if parts.scheme != "http" or not parts.hostname:
            raise OpenBaoError("the OpenBao address must be an http address")
        self.host = parts.hostname
        self.port = parts.port or 80

    def call(self, name: str, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None) -> tuple[int, bytes]:
        """One request. ``name`` is what the caller says in a failure: the call's own name, never a value."""
        if token is not None and TOKEN_FORM.fullmatch(token) is None:
            raise OpenBaoError("the token that OpenBao gave is not a valid token")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=HTTP_SECONDS)
        answer = None
        try:
            connection.request(method, f"/v1/{path}", json.dumps(body) if body is not None else None, {"X-Vault-Token": token} if token else {})
            response = connection.getresponse()
            answer = (response.status, response.read())
        except (OSError, http.client.HTTPException):
            answer = None
        finally:
            connection.close()
        if answer is None:
            # Raised outside the handler, so the library's text (which can repeat the request) is not chained to the message.
            raise OpenBaoError(f"OpenBao gave no answer to the {name}")
        return answer

    @staticmethod
    def document(raw: bytes) -> dict[str, Any]:
        try:
            found = json.loads(raw.decode(errors="replace")) if raw else {}
        except ValueError:
            return {}
        return found if isinstance(found, dict) else {}

    def login(self, role: str, secret_id: str) -> str:
        status, raw = self.call("login", "POST", "auth/approle/login", body={"role_id": role, "secret_id": secret_id})
        token = (self.document(raw).get("auth") or {}).get("client_token") if status == 200 else None
        if status != 200 or not isinstance(token, str) or not token or TOKEN_FORM.fullmatch(token) is None:
            raise OpenBaoError(f"OpenBao refused the login of the role {role} (HTTP {status})")
        return token

    def read(self, token: str, path: str) -> dict[str, Any] | None:
        """The data of a version 2 secret, or None when OpenBao says by its 404 that the secret does not exist. Any other status is a failure."""
        status, raw = self.call("read of a secret", "GET", path, token)
        if status == 404:
            return None
        if status != 200:
            raise OpenBaoError(f"OpenBao refused the read of a secret (HTTP {status})")
        outer = self.document(raw).get("data")
        stored = outer.get("data") if isinstance(outer, dict) else None
        if not isinstance(stored, dict):
            raise OpenBaoError("OpenBao answered the read of a secret in a form that is not a secret")
        return stored

    def write(self, token: str, path: str, data: dict[str, Any]) -> None:
        status, _ = self.call("write of a secret", "POST", path, token, {"data": data})
        if status not in (200, 204):
            raise OpenBaoError(f"OpenBao refused the write of a secret (HTTP {status})")
