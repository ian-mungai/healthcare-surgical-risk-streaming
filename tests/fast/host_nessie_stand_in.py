"""An in-memory stand-in for the live host's Nessie 0.110.1 as it answered on 2026-11-24: ``main`` is the default branch and has no commit.

The rules that the host's runs met are modelled, and no others: a branch made from a branch with no commit starts at the hash of an empty history, a commit
moves a branch to a new hash and keeps the entries of its parent, a merge into a branch with no commit is refused with status 404 and "No common ancestor",
and a call that names a branch at a hash it is not at is refused with status 409. It starts no server.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote, urlsplit

from tests.fast.nessie_stand_in import StandIn

EMPTY = "0" * 64


class HostNessie(StandIn):
    def __init__(self) -> None:
        super().__init__()
        self.references = {"main": ("BRANCH", EMPTY)}
        self.logs = {"main": []}
        self.history = {EMPTY}
        self.entries_at = {EMPTY: []}
        self.counter = 0

    def split(self, path: str, suffix: str) -> tuple[str, str]:
        name, _, expected = path[len("/trees/") : -len(suffix)].partition("@")
        return name, expected

    def serve_commit(self, path: str, payload: bytes | None) -> tuple[int, bytes]:
        name, expected = self.split(path, "/history/commit")
        if name not in self.references:
            return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"{name} not found"})
        kind, head = self.references[name]
        if kind != "BRANCH" or head != expected:
            return self.answer(409, {"errorCode": "REFERENCE_CONFLICT", "message": f"{name} is not at {expected}"})
        entries = list(self.entries_at[head])
        for operation in json.loads(payload or b"{}")["operations"]:
            elements = operation["key"]["elements"]
            entries = [item for item in entries if item["name"]["elements"] != elements]
            if operation["type"] == "PUT":
                content = {**operation["content"], "id": f"id-{'.'.join(elements)}"}
                entries.append({"type": content["type"], "name": {"elements": elements}, "contentId": content["id"], "content": content})
        return self.move(name, entries, "targetBranch")

    def move(self, name: str, entries: list[dict[str, Any]], key: str) -> tuple[int, bytes]:
        self.counter += 1
        new = f"{self.counter:064x}"
        self.history.add(new)
        self.entries_at[new] = entries
        self.references[name] = ("BRANCH", new)
        self.logs.setdefault(name, []).insert(0, new)
        return self.answer(200, {key: {"type": "BRANCH", "name": name, "hash": new}, "wasApplied": True, "resultantTargetHash": new})

    def serve_merge(self, path: str, payload: bytes | None) -> tuple[int, bytes]:
        name, expected = self.split(path, "/history/merge")
        body = json.loads(payload or b"{}")
        if name not in self.references or body["fromRefName"] not in self.references or self.references[name][1] != expected:
            return self.answer(409, {"errorCode": "REFERENCE_CONFLICT", "message": f"{name} is not at {expected}"})
        if self.references[name][1] == EMPTY:
            return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"No common ancestor in parents of {body['fromHash']} and {expected}"})
        return self.move(name, list(self.entries_at[body["fromHash"]]), "unused")

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        path = unquote(urlsplit(target).path).removeprefix("/api/v2")
        if method == "POST" and path.endswith("/history/commit"):
            self.calls.append((method, target))
            return self.serve_commit(path, payload)
        if method == "POST" and path.endswith("/history/merge"):
            self.calls.append((method, target))
            return self.serve_merge(path, payload)
        if method == "GET" and path == "/config":
            return self.answer(200, {"defaultBranch": self.default_branch, "noAncestorHash": EMPTY, "actualApiVersion": 2, "maxSupportedApiVersion": 2})
        status, raw = super().send(method, target, payload)
        if method == "GET" and path.startswith("/trees/") and path.endswith("/entries") and status == 200:
            name = path[len("/trees/") : -len("/entries")]
            at_hash = name.partition("@")[2] or self.references[name][1]
            document = json.loads(raw)
            return status, json.dumps({**document, "effectiveReference": {"type": "BRANCH", "name": name.partition("@")[0], "hash": at_hash}}).encode()
        return status, raw

    def head(self, name: str) -> str:
        return self.references[name][1]
