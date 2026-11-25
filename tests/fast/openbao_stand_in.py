"""An in-memory stand-in for the parts of OpenBao's HTTP API that the intake, the read of an external secret and the generated secrets use.

It serves AppRole login and the KV version 2 data and metadata paths on a loopback port, with the check-and-set rule, soft deletes and a policy of readable and
writable paths for each role. A role made with ``policy_names`` is judged by the real policy texts in ``StandIn.policies`` instead (the rules of the form
``path "<glob>" { capabilities = [...] }``), and the server then also keeps policies and AppRoles that the reconcile step writes through the API, so a test
can run it against a host whose first start wrote an older admin policy. A test can make the next call that matches a method and a path prefix drop its
connection (``drop_next``) or store another text than the one it was sent (``tamper_next_write``). It holds only the markers that a test gives it.
Each KV write keeps the ``sha256`` field that it was sent beside the value; a stored version that was seeded by a test without one reads back with the
digest of its text, unless its name is in ``bare`` (then the read has no digest, like a version that an older writer made).
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

ADMIN_ROLE = "bootstrap"


def matches(items: tuple[str, ...], full: str) -> bool:
    """Whether a path is one of the items or lies under an item that ends with ``*``."""
    return any(full == item or (item.endswith("*") and full.startswith(item[:-1])) for item in items)


RULE = re.compile(r'path\s+"([^"]+)"\s*\{\s*capabilities\s*=\s*\[([^\]]*)\]')


def policy_grants(text: str, path: str, capability: str) -> bool:
    """Whether a policy text grants the capability on the path, by exact path or by a trailing ``*`` (OpenBao's prefix glob)."""
    for glob, listed in RULE.findall(text):
        hit = path.startswith(glob[:-1]) if glob.endswith("*") else path == glob
        if hit and capability in re.findall(r'"(\w+)"', listed):
            return True
    return False


def field_of(name: str) -> str:
    """The field that a stored secret keeps its text in: ``password`` for a PostgreSQL account's path, ``value`` for any other."""
    return "password" if name.endswith("/postgres") else "value"


class Role:
    def __init__(
        self,
        secret_id: str,
        token: str,
        readable: tuple[str, ...],
        writable: tuple[str, ...] = (),
        policy_names: tuple[str, ...] | None = None,
    ) -> None:
        self.secret_id, self.token, self.readable, self.writable, self.policy_names = secret_id, token, readable, writable, policy_names


class StandIn:
    def __init__(self, roles: dict[str, Role]) -> None:
        self.roles = roles
        self.policies: dict[str, str] = {}
        self.approle: dict[str, dict[str, Any]] = {}
        self.secret_ids: dict[str, set[str]] = {}
        self.tokens: dict[str, Role] = {}
        self.versions: dict[str, list[str | None]] = {}
        self.digests: dict[str, dict[int, str]] = {}
        self.bare: set[str] = set()
        self.calls: list[tuple[str, str]] = []
        self.drops: list[list[Any]] = []
        self.tamper: list[str] = []
        self.failures: list[list[Any]] = []
        self.ignore_deletes = False
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

    def fail_next(self, method: str, prefix: str, status: int, text: str) -> None:
        """Answer the first call that matches with this status and an error list that holds ``text`` (a real server can repeat a rejected value there)."""
        self.failures.append([method, prefix, status, text])

    def tamper_next_write(self, stored: str) -> None:
        self.tamper.append(stored)

    def stored(self, name: str) -> str | None:
        versions = self.versions.get(name) or []
        return versions[-1] if versions else None

    def can(self, holder: Role, full: str, capability: str) -> bool:
        """Whether a role may do ``capability`` on the full path: by its policies when it has them, else by its readable and writable paths."""
        if holder.policy_names is not None:
            return policy_grants("\n".join(self.policies.get(name, "") for name in holder.policy_names), full, capability)
        data_path = full.replace("secret/metadata/", "secret/data/", 1)
        return matches(holder.readable if capability in ("read", "list") else holder.writable, data_path)

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
                for failure in list(outer.failures):
                    if failure[0] == method and path.startswith(failure[1]):
                        outer.failures.remove(failure)
                        return self.answer(failure[2], {"errors": [failure[3]]})
                token = self.headers.get("X-Vault-Token", "")
                if path == "/v1/sys/seal-status":
                    return self.answer(200, {"initialized": True, "sealed": False})
                if path == "/v1/auth/approle/login":
                    return self.login(body)
                holder = next((role for role in outer.roles.values() if role.token == token), None) or outer.tokens.get(token)
                if holder is None:
                    return self.answer(403, {"errors": ["permission denied"]})
                listing = "list=true" in self.path
                full = path.removeprefix("/v1/")
                if full.startswith(("sys/policies/acl", "auth/approle/role")):
                    return self.management(method, full, listing, holder, body)
                if full.startswith("secret/metadata/"):
                    name = full.removeprefix("secret/metadata/")
                    if not outer.can(holder, full, "read"):
                        return self.answer(403, {"errors": ["permission denied"]})
                    versions = outer.versions.get(name)
                    return self.answer(200, {"data": {"current_version": len(versions)}}) if versions else self.answer(404)
                if full.startswith("secret/data/"):
                    name = full.removeprefix("secret/data/")
                    if method == "GET":
                        if not outer.can(holder, full, "read"):
                            return self.answer(403, {"errors": ["permission denied"]})
                        value = outer.stored(name)
                        if value is None:
                            return self.answer(404)
                        document: dict[str, Any] = {field_of(name): value}
                        if name not in outer.bare:
                            document["sha256"] = outer.digests.get(name, {}).get(len(outer.versions[name]) - 1) or hashlib.sha256(value.encode()).hexdigest()
                        return self.answer(200, {"data": {"data": document}})
                    if method == "DELETE":
                        if not outer.can(holder, full, "delete"):
                            return self.answer(403, {"errors": ["permission denied"]})
                        if outer.versions.get(name) and not outer.ignore_deletes:
                            outer.versions[name].append(None)
                        return self.answer(204)
                    versions = outer.versions.setdefault(name, [])
                    if not outer.can(holder, full, "update" if versions else "create"):
                        return self.answer(403, {"errors": ["permission denied"]})
                    cas = body.get("options", {}).get("cas")
                    if cas is not None and cas != len(versions):
                        return self.answer(400, {"errors": ["check-and-set parameter did not match the current version"]})
                    versions.append(outer.tamper.pop(0) if outer.tamper else body["data"][field_of(name)])
                    if "sha256" in body["data"]:
                        outer.digests.setdefault(name, {})[len(versions) - 1] = body["data"]["sha256"]
                    return self.answer(200, {"data": {"version": len(versions)}})
                return self.answer(404)

            def login(self, body: dict[str, Any]) -> None:
                role = outer.roles.get(body.get("role_id", ""))
                if role is not None and role.secret_id == body.get("secret_id"):
                    return self.answer(200, {"auth": {"client_token": role.token}})
                name = body.get("role_id", "")
                if name in outer.approle and body.get("secret_id") in outer.secret_ids.get(name, set()):
                    token = f"token-of-{name}"
                    outer.tokens[token] = Role("", token, (), (), tuple(outer.approle[name].get("token_policies") or ()))
                    return self.answer(200, {"auth": {"client_token": token}})
                return self.answer(400, {"errors": ["invalid role or secret ID"]})

            def management(self, method: str, full: str, listing: bool, holder: Role, body: dict[str, Any]) -> None:
                """The policy and AppRole paths that the reconcile step uses, judged by the holder's policies."""
                if listing:
                    if not outer.can(holder, full, "list"):
                        return self.answer(403, {"errors": ["permission denied"]})
                    names = outer.policies if full == "sys/policies/acl" else outer.approle
                    return self.answer(200, {"data": {"keys": sorted(names)}})
                if full.startswith("sys/policies/acl/"):
                    name = full.removeprefix("sys/policies/acl/")
                    if method == "GET":
                        if not outer.can(holder, full, "read"):
                            return self.answer(403, {"errors": ["permission denied"]})
                        return self.answer(200, {"data": {"policy": outer.policies[name]}}) if name in outer.policies else self.answer(404)
                    if not outer.can(holder, full, "update" if name in outer.policies else "create"):
                        return self.answer(403, {"errors": ["permission denied"]})
                    outer.policies[name] = body["policy"]
                    return self.answer(204)
                parts = full.removeprefix("auth/approle/role/").split("/")
                name = parts[0]
                if not outer.can(holder, full, "read" if method == "GET" else "update" if name in outer.approle else "create"):
                    return self.answer(403, {"errors": ["permission denied"]})
                if len(parts) == 1 and method == "GET":
                    return self.answer(200, {"data": outer.approle[name]}) if name in outer.approle else self.answer(404)
                if len(parts) == 1:
                    outer.approle[name] = body
                    return self.answer(204)
                if parts[1:] == ["role-id"] and name in outer.approle:
                    return self.answer(200, {"data": {"role_id": outer.approle[name]["role_id"]}})
                if parts[1:] == ["secret-id"] and name in outer.approle:
                    issued = secrets.token_urlsafe(16)
                    outer.secret_ids.setdefault(name, set()).add(issued)
                    return self.answer(200, {"data": {"secret_id": issued}})
                if parts[1:] == ["secret-id", "lookup"] and name in outer.approle:
                    return self.answer(200, {"data": {"valid": True}}) if body.get("secret_id") in outer.secret_ids.get(name, set()) else self.answer(204)
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
