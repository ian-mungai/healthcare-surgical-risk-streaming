"""The catalog calls of the record load over the Nessie client: the load branch from main's head, the head, and each raw table's snapshot on the branch.

A stand-in answers the Nessie API version 2 calls that the adapter makes. The calls themselves are proven against a real Nessie by the acceptance test.
"""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.ingestion import record_catalog
from scripts.ingestion.record_catalog import NessieCatalog
from scripts.ingestion.record_load import LoadError
from scripts.stack.nessie import NessieClient, NessieError

MAIN = "a" * 64


class Server:
    def __init__(self) -> None:
        self.refs: dict[str, str] = {"main": MAIN}
        self.contents: dict[str, dict[str, dict[str, Any]]] = {"main": {}}
        self.calls: list[str] = []
        self.fail: tuple[str, int] | None = None
        self.history: list[str] = []
        self.page = 100

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        parts = urlsplit(target)
        path = unquote(parts.path).removeprefix("/api/v2")
        self.calls.append(f"{method} {path}")
        if self.fail and self.fail[0] == path:
            return self.fail[1], b'{"errorCode":"REFERENCE_NOT_FOUND","message":"x"}'
        if method == "POST" and path == "/trees":
            name = parse_qs(parts.query)["name"][0]
            self.refs[name] = json.loads(payload or b"{}")["hash"]
            self.contents[name] = dict(self.contents["main"])
            return 200, json.dumps({"reference": {"type": "BRANCH", "name": name, "hash": self.refs[name]}}).encode()
        if method == "DELETE":
            name, _, expected = path.removeprefix("/trees/").partition("@")
            if self.refs.get(name) != expected:
                return 409, b'{"errorCode":"REFERENCE_CONFLICT","message":"x"}'
            del self.refs[name]
            return 200, b"{}"
        if method == "GET" and path == "/trees/main/history":
            query = parse_qs(parts.query)
            start = int(query.get("page-token", ["0"])[0])
            asked = int(query["max-records"][0])
            if not 1 <= asked <= 100:
                return 400, b'{"message":"max-records out of range"}'
            size = min(asked, self.page)
            chunk = self.history[start : start + size]
            more = start + size < len(self.history)
            document = {
                "hasMore": more,
                "token": str(start + size) if more else None,
                "logEntries": [{"commitMeta": {"message": message}} for message in chunk],
            }
            return 200, json.dumps(document).encode()
        if path.endswith("/entries"):
            name = path.removeprefix("/trees/").removesuffix("/entries")
            return 200, json.dumps(
                {"entries": [{"type": item["type"], "name": {"elements": key.split(".")}} for key, item in self.contents[name].items()]}
            ).encode()
        if "/contents/" in path:
            name, _, key = path.removeprefix("/trees/").partition("/contents/")
            return 200, json.dumps({"content": self.contents[name][key]}).encode()
        name = path.removeprefix("/trees/")
        if name not in self.refs:
            return 404, b'{"errorCode":"REFERENCE_NOT_FOUND","message":"x"}'
        return 200, json.dumps({"reference": {"type": "BRANCH", "name": name, "hash": self.refs[name]}}).encode()


class Client(NessieClient):
    def __init__(self, server: Server) -> None:
        super().__init__("http://nessie.example:19120/api/v2")
        self.server = server

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        return self.server.send(method, target, payload)


def table(snapshot: int | None) -> dict[str, Any]:
    return {"type": "ICEBERG_TABLE", **({"snapshotId": snapshot} if snapshot is not None else {})}


class CatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = Server()
        self.catalog = NessieCatalog(Client(self.server))

    def test_the_branch_is_created_at_the_head_of_main_and_that_head_is_returned(self) -> None:
        self.assertEqual(MAIN, self.catalog.create_branch("load_run1"))
        self.assertEqual(MAIN, self.server.refs["load_run1"])
        self.assertEqual(MAIN, self.catalog.head("load_run1"))

    def test_a_branch_that_exists_is_found_and_one_that_does_not_is_not(self) -> None:
        self.assertTrue(self.catalog.has_branch("main"))
        self.assertFalse(self.catalog.has_branch("load_run1"))

    def test_any_failure_but_a_missing_branch_is_an_error_not_a_no(self) -> None:
        self.server.fail = ("/trees/load_run1", 500)
        with self.assertRaises(NessieError):
            self.catalog.has_branch("load_run1")

    def test_a_branch_is_deleted_only_at_the_head_that_was_read(self) -> None:
        self.catalog.create_branch("load_run1")
        with self.assertRaises(NessieError):
            self.catalog.delete_branch("load_run1", "b" * 64)
        self.assertIn("load_run1", self.server.refs)
        self.catalog.delete_branch("load_run1", MAIN)
        self.assertNotIn("load_run1", self.server.refs)

    def test_the_snapshot_of_each_raw_table_is_read_and_other_entries_are_ignored(self) -> None:
        self.server.contents["main"] = {
            "raw_records": {"type": "NAMESPACE"},
            "raw_records.clinical_data": table(11),
            "raw_records.lab_data": table(22),
            "raw_records.no_snapshot": table(None),
            "bronze.cases": table(33),
        }
        self.assertEqual({"clinical_data": 11, "lab_data": 22}, self.catalog.snapshot_ids("main"))

    def test_a_branch_without_tables_has_no_snapshots(self) -> None:
        self.assertEqual({}, self.catalog.snapshot_ids("main"))


class RecordedManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = Server()
        self.catalog = NessieCatalog(Client(self.server))

    def line(self, digit: str) -> str:
        return f"merge raw\n\nrecords-manifest: {digit * 64}"

    def test_a_manifest_is_found_among_all_the_markers_not_only_the_newest(self) -> None:
        self.server.history = ["merge something else", self.line("b"), "later merge", self.line("a")]
        self.assertTrue(self.catalog.has_recorded_manifest("a" * 64))
        self.assertTrue(self.catalog.has_recorded_manifest("b" * 64))

    def test_a_manifest_that_no_marker_names_is_absent_when_the_whole_history_was_read(self) -> None:
        self.server.history = ["initial", self.line("b"), f"records-manifest:{'a' * 64}", f"x records-manifest: {'a' * 64}"]
        self.assertFalse(self.catalog.has_recorded_manifest("a" * 64))

    def test_pages_are_followed_until_the_manifest_is_found_or_the_history_ends(self) -> None:
        self.server.page = 2
        self.server.history = ["one", "two", "three", "four", self.line("c"), "six"]
        self.assertTrue(self.catalog.has_recorded_manifest("c" * 64))
        self.assertEqual(3, len([call for call in self.server.calls if call.endswith("/history")]))
        self.server.calls.clear()
        self.assertFalse(self.catalog.has_recorded_manifest("d" * 64))
        self.assertEqual(3, len([call for call in self.server.calls if call.endswith("/history")]), "the history ends after three pages of two")

    def test_reaching_the_cap_without_finding_the_manifest_is_a_refusal_not_an_absence(self) -> None:
        self.server.history = ["x"] * 600 + [self.line("d")]
        with mock.patch.object(record_catalog, "HISTORY_LIMIT", 500), self.assertRaises(LoadError) as raised:
            self.catalog.has_recorded_manifest("e" * 64)
        self.assertIn("cannot establish absence", str(raised.exception))
        self.assertEqual(5, len([call for call in self.server.calls if call.endswith("/history")]), "500 commits in pages of 100, and no more")

    def test_a_manifest_found_inside_the_cap_is_found_although_more_history_follows(self) -> None:
        self.server.history = ["x"] * 10 + [self.line("d")] + ["y"] * 600
        with mock.patch.object(record_catalog, "HISTORY_LIMIT", 500):
            self.assertTrue(self.catalog.has_recorded_manifest("d" * 64))

    def test_a_history_that_is_exactly_as_long_as_the_cap_and_ends_is_an_absence(self) -> None:
        self.server.history = ["x"] * 200
        with mock.patch.object(record_catalog, "HISTORY_LIMIT", 200):
            self.assertFalse(self.catalog.has_recorded_manifest("e" * 64))

    def test_a_failing_history_call_is_an_error_not_an_absence(self) -> None:
        self.server.fail = ("/trees/main/history", 500)
        with self.assertRaises(NessieError):
            self.catalog.has_recorded_manifest("a" * 64)

    def test_a_digest_that_is_not_64_lower_case_hex_digits_is_refused_before_any_call(self) -> None:
        for digest in ("", "ab", "A" * 64):
            with self.subTest(digest), self.assertRaises(ValueError):
                self.catalog.has_recorded_manifest(digest)
        self.assertEqual([], self.server.calls)


if __name__ == "__main__":
    unittest.main()
