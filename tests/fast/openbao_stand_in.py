"""An in-memory stand-in for the parts of OpenBao's HTTP API that the intake, the read of an external secret and the generated secrets use.

It serves AppRole login and the KV version 2 data and metadata paths on a loopback port, with the check-and-set rule, soft deletes and a policy of readable and
writable paths for each role. A test can make the next call that matches a method and a path prefix drop its connection (``drop_next``) or store another text
than the one it was sent (``tamper_next_write``). It holds only the markers that a test gives it.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

ADMIN_ROLE = "bootstrap"


def matches(items: tuple[str, ...], full: str) -> bool:
    """Whether a path is one of the items or lies under an item that ends with ``*``."""
    return any(full == item or (item.endswith("*") and full.startswith(item[:-1])) for item in items)


class Role:
    def __init__(self, secret_id: str, token: str, readable: tuple[str, ...], writable: tuple[str, ...] = ()) -> None:
        self.secret_id, self.token, self.readable, self.writable = secret_id, token, readable, writable


class StandIn:
    def __init__(self, roles: dict[str, Role]) -> None:
        self.roles = roles
        self.versions: dict[str, list[str | None]] = {}
        self.calls: list[tuple[str, str]] = []
        self.drops: list[list[Any]] = []
        self.tamper: list[str] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self.handler())
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)

    @property
    def address(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> StandIn:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def drop_next(self, method: str, prefix: str, after: int = 0) -> None:
        """Drop the connection of the first call that matches, after ``after`` matching calls were answered."""
        self.drops.append([method, prefix, after])

    def tamper_next_write(self, stored: str) -> None:
        self.tamper.append(stored)

    def stored(self, name: str) -> str | None:
        versions = self.versions.get(name) or []
        return versions[-1] if versions else None

    def writes(self) -> int:
        return len([call for call in self.calls if call[0] == "POST" and "/secret/data/" in call[1]])

    def handler(self) -> type[BaseHTTPRequestHandler]:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_: object) -> None:
                return

            def answer(self, status: int, document: dict[str, Any] | None = None) -> None:
                body = json.dumps(document or {}).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def serve(self, method: str) -> None:
                path = self.path.split("?", 1)[0]
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                outer.calls.append((method, path))
                for drop in list(outer.drops):
                    if drop[0] == method and path.startswith(drop[1]):
                        if drop[2] > 0:
                            drop[2] -= 1
                            continue
                        outer.drops.remove(drop)
                        self.connection.close()
                        return
                token = self.headers.get("X-Vault-Token", "")
                if path == "/v1/auth/approle/login":
                    role = outer.roles.get(body.get("role_id", ""))
                    if role is None or role.secret_id != body.get("secret_id"):
                        return self.answer(400, {"errors": ["invalid role or secret ID"]})
                    return self.answer(200, {"auth": {"client_token": role.token}})
                holder = next((role for role in outer.roles.values() if role.token == token), None)
                if holder is None:
                    return self.answer(403, {"errors": ["permission denied"]})
                if path.startswith("/v1/secret/metadata/"):
                    name = path.removeprefix("/v1/secret/metadata/")
                    if not matches(holder.readable, f"secret/data/{name}"):
                        return self.answer(403, {"errors": ["permission denied"]})
                    versions = outer.versions.get(name)
                    return self.answer(200, {"data": {"current_version": len(versions)}}) if versions else self.answer(404)
                if path.startswith("/v1/secret/data/"):
                    name = path.removeprefix("/v1/secret/data/")
                    full = f"secret/data/{name}"
                    if method == "GET":
                        if not matches(holder.readable, full):
                            return self.answer(403, {"errors": ["permission denied"]})
                        value = outer.stored(name)
                        return self.answer(200, {"data": {"data": {"value": value}}}) if value is not None else self.answer(404)
                    if not matches(holder.writable, full):
                        return self.answer(403, {"errors": ["permission denied"]})
                    if method == "DELETE":
                        if outer.versions.get(name):
                            outer.versions[name].append(None)
                        return self.answer(204)
                    versions = outer.versions.setdefault(name, [])
                    if body.get("options", {}).get("cas") != len(versions):
                        return self.answer(400, {"errors": ["check-and-set parameter did not match the current version"]})
                    versions.append(outer.tamper.pop(0) if outer.tamper else body["data"]["value"])
                    return self.answer(200, {"data": {"version": len(versions)}})
                return self.answer(404)

            def do_GET(self) -> None:
                self.serve("GET")

            def do_POST(self) -> None:
                self.serve("POST")

            def do_PUT(self) -> None:
                self.serve("POST")

            def do_DELETE(self) -> None:
                self.serve("DELETE")

        return Handler
