"""An in-memory stand-in for Nessie 0.110.1's API version 2 and the fixed values the tag and table maintenance tests share. It starts no server."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.stack.nessie import NessieClient

HASH_ONE = "1" * 64
HASH_TWO = "2" * 64
HASH_UNKNOWN = "9" * 64
HASH_SIDE = "5" * 64
RUN_ID = "20261122t0925-4f2a9c"
TABLE = "catalog_proof.numbers"
METADATA = "file:/warehouse/catalog_proof/numbers_x/metadata/00002-y.metadata.json"
PAGE = 2


def table_entry(key: Sequence[str], snapshot_id: int | None, location: str) -> dict[str, Any]:
    content: dict[str, Any] = {"type": "ICEBERG_TABLE", "id": "c1", "metadataLocation": location, "schemaId": 0}
    if snapshot_id is not None:
        content["snapshotId"] = snapshot_id
    return {"type": "ICEBERG_TABLE", "name": {"elements": list(key)}, "contentId": "c1", "content": content}


NAMESPACE_ENTRY = {"type": "NAMESPACE", "name": {"elements": ["catalog_proof"]}, "content": {"type": "NAMESPACE", "id": "n1", "elements": ["catalog_proof"]}}


ALLOWED_QUERY = {
    "/trees": {"max-records", "page-token", "filter"},
    "entries": {"content", "max-records", "page-token", "filter"},
    "history": {"filter", "max-records", "page-token", "limit-hash", "fetch"},
}
HISTORY_FILTER = re.compile(r"commit\.hash == '([0-9a-f]{64})'")


class StandIn(NessieClient):
    """Nessie 0.110.1's API version 2: the references, the entries at each hash and the commit log of each branch.

    Lists are paged two at a time with the parameters that the server's OpenAPI file names (``max-records`` and ``page-token``), and a parameter that the server
    does not name is refused as it is there. Like the real server, a tag is made at any commit that exists, on any branch: the log of ``main`` holds only the
    commits that were merged into it.
    """

    def __init__(self) -> None:
        super().__init__("http://nessie.invalid:19120/api/v2", 5)
        self.references: dict[str, tuple[str, str]] = {"main": ("BRANCH", HASH_TWO), "side": ("BRANCH", HASH_SIDE)}
        self.logs: dict[str, list[str]] = {"main": [HASH_TWO, HASH_ONE], "side": [HASH_SIDE, HASH_ONE]}
        self.history = {HASH_ONE, HASH_TWO, HASH_SIDE}
        self.entries_at: dict[str, list[dict[str, Any]]] = {
            HASH_ONE: [NAMESPACE_ENTRY, table_entry(["catalog_proof", "numbers"], 111, METADATA)],
            HASH_TWO: [NAMESPACE_ENTRY, table_entry(["catalog_proof", "numbers"], 222, METADATA)],
            HASH_SIDE: [NAMESPACE_ENTRY, table_entry(["catalog_proof", "numbers"], 333, METADATA)],
        }
        self.blank_history_pages = 0
        self.endless_history = False
        self.ignore_filter = False
        self.default_branch: Any = "main"
        self.calls: list[tuple[str, str]] = []

    def answer(self, status: int, document: dict[str, Any]) -> tuple[int, bytes]:
        return status, json.dumps(document).encode()

    def page(self, items: list[Any], query: dict[str, list[str]], name: str) -> dict[str, Any]:
        start = int(query.get("page-token", ["0"])[0])
        size = min(PAGE, int(query.get("max-records", [str(PAGE)])[0]))
        more = start + size < len(items)
        return {name: items[start : start + size], "hasMore": more, "token": str(start + size) if more else None}

    def history_page(self, ref: str, query: dict[str, list[str]]) -> tuple[int, bytes]:
        if ref not in self.logs:
            return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"{ref} not found"})
        number = int(query.get("page-token", ["0"])[0])
        if self.endless_history or number < self.blank_history_pages:
            return self.answer(200, {"logEntries": [], "hasMore": True, "token": str(number + 1)})
        wanted = None if self.ignore_filter else HISTORY_FILTER.fullmatch(query.get("filter", [""])[0])
        commits = [item for item in self.logs[ref] if wanted is None or item == wanted.group(1)]
        entries = [{"commitMeta": {"hash": item}, "parentCommitHash": None} for item in commits]
        return self.answer(200, {"logEntries": entries, "hasMore": False})

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.calls.append((method, target))
        parts = urlsplit(target)
        query = parse_qs(parts.query)
        path = unquote(parts.path).removeprefix("/api/v2")
        kind = "/trees" if path == "/trees" else "entries" if path.endswith("/entries") else "history" if path.endswith("/history") else ""
        if method == "GET" and kind and not set(query) <= ALLOWED_QUERY[kind]:
            return self.answer(400, {"errorCode": "BAD_REQUEST", "message": f"unknown query parameters {sorted(set(query) - ALLOWED_QUERY[kind])}"})
        if method == "GET" and path == "/config":
            return self.answer(200, {"defaultBranch": self.default_branch, "noAncestorHash": "0" * 64})
        if method == "GET" and path == "/trees":
            refs = [{"type": kind, "name": name, "hash": value} for name, (kind, value) in sorted(self.references.items())]
            return self.answer(200, self.page(refs, query, "references"))
        if method == "GET" and path.startswith("/trees/") and path.endswith("/history"):
            return self.history_page(path[len("/trees/") : -len("/history")], query)
        if method == "GET" and path.startswith("/trees/") and path.endswith("/entries"):
            name = path[len("/trees/") : -len("/entries")]
            if "@" in name:
                reference, _, at_hash = name.partition("@")
                if reference not in self.references or at_hash not in self.entries_at:
                    return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"{name} not found"})
                return self.answer(200, self.page(self.entries_at[at_hash], query, "entries"))
            if name not in self.references:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"{name} not found"})
            return self.answer(200, self.page(self.entries_at[self.references[name][1]], query, "entries"))
        if method == "GET" and path.startswith("/trees/"):
            name = path[len("/trees/") :]
            if name not in self.references:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"Named reference '{name}' not found"})
            reference_kind, value = self.references[name]
            return self.answer(200, {"reference": {"type": reference_kind, "name": name, "hash": value}})
        if method == "POST" and path == "/trees":
            body = json.loads(payload or b"{}")
            name = query["name"][0]
            if body["hash"] not in self.history:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": "Could not find commit"})
            if name in self.references:
                return self.answer(409, {"errorCode": "REFERENCE_ALREADY_EXISTS", "message": f"A reference of name '{name}' already exists"})
            self.references[name] = (query["type"][0], body["hash"])
            return self.answer(200, {"reference": {"type": query["type"][0], "name": name, "hash": body["hash"]}})
        return self.answer(500, {"errorCode": "UNKNOWN", "message": f"{method} {path} is not served by the stand-in"})

    def writes(self) -> list[tuple[str, str]]:
        return [call for call in self.calls if call[0] != "GET"]
