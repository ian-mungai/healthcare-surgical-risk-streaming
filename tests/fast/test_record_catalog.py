"""The catalog calls of the record load over the Nessie client: the load branch from main's head, the head, and each raw table's snapshot on the branch.

A stand-in answers the Nessie API version 2 calls that the adapter makes. The calls themselves are proven against a real Nessie by the acceptance test.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Callable
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.ingestion import record_catalog
from scripts.ingestion.record_catalog import NessieCatalog
from scripts.ingestion.record_load import BranchExists, BranchMoved, LoadError
from scripts.stack.nessie import NessieClient, NessieError

MAIN = "a" * 64


class Server:
    def __init__(self) -> None:
        self.refs: dict[str, str] = {"main": MAIN}
        self.contents: dict[str, dict[str, dict[str, Any]]] = {"main": {}}
        self.calls: list[str] = []
        self.fail: tuple[str, int] | None = None
        self.history: list[str] = []
        self.branch_history: dict[str, list[str]] = {}
        self.page = 100
        self.fault: Callable[[dict[str, Any]], None] | None = None
        self.bodies: list[dict[str, Any]] = []
        self.entry_fault: Callable[[dict[str, Any]], None] | None = None
        self.content_fault: Callable[[dict[str, Any]], None] | None = None
        self.created_hash: str | None = None

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        parts = urlsplit(target)
        path = unquote(parts.path).removeprefix("/api/v2")
        self.calls.append(f"{method} {path}")
        if self.fail and self.fail[0] == path:
            return self.fail[1], b'{"errorCode":"REFERENCE_NOT_FOUND","message":"x"}'
        if method == "POST" and path == "/trees":
            name = parse_qs(parts.query)["name"][0]
            body = json.loads(payload or b"{}")
            self.bodies.append(body)
            if name in self.refs:
                return 409, b'{"errorCode":"REFERENCE_ALREADY_EXISTS","message":"x"}'
            self.refs[name] = body["hash"]
            self.contents[name] = dict(self.contents["main"])
            return 200, json.dumps({"reference": {"type": "BRANCH", "name": name, "hash": self.created_hash or self.refs[name]}}).encode()
        if method == "DELETE":
            name, _, expected = path.removeprefix("/trees/").partition("@")
            if name not in self.refs:
                return 404, b'{"errorCode":"REFERENCE_NOT_FOUND","message":"x"}'
            if self.refs.get(name) != expected:
                return 409, b'{"errorCode":"REFERENCE_CONFLICT","message":"x"}'
            del self.refs[name]
            return 200, b"{}"
        if method == "GET" and path.endswith("/history"):
            name = path.removeprefix("/trees/").removesuffix("/history")
            query = parse_qs(parts.query)
            start = int(query.get("page-token", ["0"])[0])
            asked = int(query["max-records"][0])
            if not 1 <= asked <= 100:
                return 400, b'{"message":"max-records out of range"}'
            size = min(asked, self.page)
            commits = (
                [(f"{1000 + number:064x}", message) for number, message in enumerate(self.history)]
                if name == "main"
                else [(commit, "a commit") for commit in self.branch_history[name]]
            )
            chunk = commits[start : start + size]
            more = start + size < len(commits)
            document: dict[str, Any] = {
                "hasMore": more,
                "token": str(start + size) if more else None,
                "logEntries": [{"commitMeta": {"hash": commit, "message": message}} for commit, message in chunk],
            }
            if self.fault is not None:
                self.fault(document)
            return 200, json.dumps(document).encode()
        if path.endswith("/entries"):
            name = path.removeprefix("/trees/").removesuffix("/entries")
            document = {
                "hasMore": False,
                "entries": [{"type": item["type"], "name": {"elements": key.split(".")}} for key, item in self.contents[name].items()],
            }
            if self.entry_fault is not None:
                self.entry_fault(document)
            return 200, json.dumps(document).encode()
        if "/contents/" in path:
            name, _, key = path.removeprefix("/trees/").partition("/contents/")
            document = {"content": dict(self.contents[name][key])}
            if self.content_fault is not None:
                self.content_fault(document)
            return 200, json.dumps(document).encode()
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

    def test_the_branch_is_created_at_exactly_the_hash_that_is_asked_for_and_that_hash_is_returned(self) -> None:
        older = "c" * 64
        self.assertEqual(older, self.catalog.create_branch("load_run1", older))
        self.assertEqual(older, self.server.refs["load_run1"])
        self.assertEqual({"type": "BRANCH", "name": "main", "hash": older}, self.server.bodies[0])
        self.assertEqual(older, self.catalog.head("load_run1"))

    def test_a_creation_that_answers_with_another_hash_than_the_one_asked_for_is_refused(self) -> None:
        self.server.created_hash = "d" * 64
        with self.assertRaises(LoadError):
            self.catalog.create_branch("load_run1", MAIN)

    def test_a_branch_of_that_name_that_exists_is_a_branch_exists_error_and_is_not_touched(self) -> None:
        self.catalog.create_branch("load_run1", MAIN)
        with self.assertRaises(BranchExists):
            self.catalog.create_branch("load_run1", MAIN)
        self.assertIn("load_run1", self.server.refs)

    def test_a_branch_that_exists_is_found_and_one_that_does_not_is_not(self) -> None:
        self.assertTrue(self.catalog.has_branch("main"))
        self.assertFalse(self.catalog.has_branch("load_run1"))

    def test_any_failure_but_a_missing_branch_is_an_error_not_a_no(self) -> None:
        self.server.fail = ("/trees/load_run1", 500)
        with self.assertRaises(NessieError):
            self.catalog.has_branch("load_run1")

    def test_a_branch_is_deleted_only_at_the_head_that_was_read_and_another_head_is_a_branch_moved_error(self) -> None:
        self.catalog.create_branch("load_run1", MAIN)
        with self.assertRaises(BranchMoved):
            self.catalog.delete_branch("load_run1", "b" * 64)
        self.assertIn("load_run1", self.server.refs)
        self.catalog.delete_branch("load_run1", MAIN)
        self.assertNotIn("load_run1", self.server.refs)

    def test_a_branch_that_is_already_gone_is_not_an_error_to_delete_and_any_other_refusal_is(self) -> None:
        self.catalog.delete_branch("load_run1", MAIN)
        self.server.fail = ("/trees/load_run1@" + MAIN, 500)
        with self.assertRaises(NessieError):
            self.catalog.delete_branch("load_run1", MAIN)

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


class DescentTest(unittest.TestCase):
    """A branch descends from a hash when that hash is among the commits of the branch's history, read whole or until it is found."""

    def setUp(self) -> None:
        self.server = Server()
        self.catalog = NessieCatalog(Client(self.server))
        self.server.refs["load_run1"] = "3" * 64
        self.server.branch_history["load_run1"] = ["3" * 64, "2" * 64, "1" * 64]

    def test_a_hash_in_the_history_of_the_branch_is_an_ancestor_and_one_that_is_not_there_is_not(self) -> None:
        self.assertTrue(self.catalog.descends_from("load_run1", "1" * 64))
        self.assertTrue(self.catalog.descends_from("load_run1", "3" * 64))
        self.assertFalse(self.catalog.descends_from("load_run1", "9" * 64))

    def test_the_head_itself_needs_no_history_call(self) -> None:
        self.catalog.descends_from("load_run1", "3" * 64)
        self.assertEqual([], [call for call in self.server.calls if call.endswith("/history")])

    def test_pages_are_followed_to_the_ancestor(self) -> None:
        self.server.page = 1
        self.assertTrue(self.catalog.descends_from("load_run1", "1" * 64))
        self.assertEqual(3, len([call for call in self.server.calls if call.endswith("/history")]))

    def test_a_history_longer_than_the_scan_that_does_not_hold_the_hash_is_a_refusal_not_a_no(self) -> None:
        self.server.branch_history["load_run1"] = [f"{number:064x}" for number in range(2000, 1000, -1)]
        with mock.patch.object(record_catalog, "HISTORY_LIMIT", 200), self.assertRaises(LoadError) as raised:
            self.catalog.descends_from("load_run1", "1" * 64)
        self.assertIn("cannot establish", str(raised.exception))

    def test_a_page_that_is_not_well_formed_is_a_refusal_not_a_no(self) -> None:
        self.server.page = 1
        for name, fault in MALFORMED_PAGES.items():
            with self.subTest(name):
                self.server.fault = fault
                with self.assertRaises(LoadError):
                    self.catalog.descends_from("load_run1", "9" * 64)

    def test_a_ancestor_that_is_not_64_lower_case_hex_digits_is_refused_before_any_call(self) -> None:
        for value in ("", "ab", "A" * 64):
            with self.subTest(value), self.assertRaises(ValueError):
                self.catalog.descends_from("load_run1", value)
        self.assertEqual([], self.server.calls)


MARKER = "marker_c41f9a_never_in_a_message"


def drop(field: str) -> Callable[[dict[str, Any]], None]:
    return lambda document: document.pop(field)


def put(field: str, value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda document: document.__setitem__(field, value)


def entries_of(change: Callable[[dict[str, Any]], None]) -> Callable[[dict[str, Any]], None]:
    def apply(document: dict[str, Any]) -> None:
        for entry in document["logEntries"]:
            change(entry)

    return apply


def meta_of(change: Callable[[dict[str, Any]], None]) -> Callable[[dict[str, Any]], None]:
    return entries_of(lambda entry: change(entry["commitMeta"]))


DROP = object()


def more_and(fields: dict[str, Any]) -> Callable[[dict[str, Any]], None]:
    """A page that says there are more pages, with these fields changed or, for ``DROP``, removed."""

    def apply(document: dict[str, Any]) -> None:
        document["hasMore"] = True
        for name, value in fields.items():
            if value is DROP:
                document.pop(name, None)
            else:
                document[name] = value

    return apply


# Each is a page that must not be taken for the page that ends a history. It is applied to every page, so the first page is the wrong one.
MALFORMED_PAGES: dict[str, Callable[[dict[str, Any]], None]] = {
    "more pages and no token": more_and({"token": DROP}),
    "more pages and a null token": more_and({"token": None}),
    "more pages and an empty token": more_and({"token": ""}),
    "more pages and a token that is a number": more_and({"token": 7}),
    "more pages and a token that is a list": more_and({"token": [MARKER]}),
    "more pages and a token that this scan already used": more_and({"token": "0"}),
    "more pages and no entries": more_and({"logEntries": []}),
    "has-more that is not a boolean": put("hasMore", "yes"),
    "has-more that is missing": drop("hasMore"),
    "no list of entries": drop("logEntries"),
    "entries that are not a list": put("logEntries", {MARKER: MARKER}),
    "an entry that is not an object": lambda document: document["logEntries"].append(MARKER),
    "an entry without commit meta": entries_of(lambda entry: entry.pop("commitMeta")),
    "commit meta that is not an object": entries_of(lambda entry: entry.__setitem__("commitMeta", [MARKER])),
    "commit meta without a message": meta_of(lambda meta: meta.pop("message")),
    "a message that is not text": meta_of(lambda meta: meta.__setitem__("message", {MARKER: MARKER})),
    "commit meta without a hash": meta_of(lambda meta: meta.pop("hash")),
    "a hash that is not 64 hex digits": meta_of(lambda meta: meta.__setitem__("hash", MARKER)),
    "a hash that is a number": meta_of(lambda meta: meta.__setitem__("hash", 5)),
}


class IncompletePageTest(unittest.TestCase):
    """A page of the history that is not well formed never proves that a manifest is absent."""

    def setUp(self) -> None:
        self.server = Server()
        self.catalog = NessieCatalog(Client(self.server))
        self.server.page = 2
        self.server.history = ["one", "two", "three", "four", "five"]

    def test_each_malformed_page_is_refused_as_not_able_to_establish_absence_and_no_message_holds_the_answer(self) -> None:
        for name, fault in MALFORMED_PAGES.items():
            with self.subTest(name):
                self.server.fault = fault
                with self.assertRaises(LoadError) as raised:
                    self.catalog.has_recorded_manifest("a" * 64)
                self.assertIn("cannot establish absence", str(raised.exception))
                self.assertNotIn(MARKER, str(raised.exception))

    def test_a_repeated_token_on_a_later_page_is_refused_too(self) -> None:
        self.server.fault = lambda document: document.__setitem__("token", "2") if document["hasMore"] else None
        with self.assertRaises(LoadError) as raised:
            self.catalog.has_recorded_manifest("a" * 64)
        self.assertIn("cannot establish absence", str(raised.exception))

    def test_the_last_page_may_carry_no_token_when_it_says_there_is_no_more(self) -> None:
        self.assertFalse(self.catalog.has_recorded_manifest("a" * 64))

    def test_a_manifest_on_a_well_formed_page_is_found_even_though_a_later_page_would_be_malformed(self) -> None:
        self.server.history = [f"merge raw\n\nrecords-manifest: {'a' * 64}", "two", "three"]
        pages: list[int] = []

        def later_pages_lose_their_token(document: dict[str, Any]) -> None:
            pages.append(1)
            if len(pages) > 1:
                document.pop("token", None)

        self.server.fault = later_pages_lose_their_token
        self.assertTrue(self.catalog.has_recorded_manifest("a" * 64))
        self.assertEqual(1, len(pages), "the scan stopped at the page that held the line")

    def test_an_entry_without_the_fields_that_the_scan_reads_is_refused_even_before_the_line_it_would_have_found(self) -> None:
        self.server.history = [f"merge raw\n\nrecords-manifest: {'a' * 64}", "two"]
        self.server.fault = lambda document: document["logEntries"].insert(0, {"commitMeta": {"hash": "b" * 64}})
        with self.assertRaises(LoadError):
            self.catalog.has_recorded_manifest("a" * 64)


class AnswerShapeTest(unittest.TestCase):
    """The entries and the contents that the snapshots are read from are checked in the same way: a malformed answer is refused, never read as no table."""

    def setUp(self) -> None:
        self.server = Server()
        self.catalog = NessieCatalog(Client(self.server))
        self.server.contents["main"] = {"raw_records.clinical_data": table(11), "raw_records.lab_data": table(22)}

    def test_each_malformed_answer_of_the_entries_is_refused_and_no_message_holds_it(self) -> None:
        faults: dict[str, Callable[[dict[str, Any]], None]] = {
            "more entries": put("hasMore", True),
            "no entries": drop("entries"),
            "entries that are not a list": put("entries", {MARKER: MARKER}),
            "an entry that is not an object": lambda document: document["entries"].append(MARKER),
            "an entry without a name": lambda document: document["entries"][0].pop("name"),
            "a name that is not an object": lambda document: document["entries"][0].__setitem__("name", MARKER),
            "elements that are not a list": lambda document: document["entries"][0]["name"].__setitem__("elements", MARKER),
            "an element that is not text": lambda document: document["entries"][0]["name"]["elements"].append(5),
        }
        for name, fault in faults.items():
            with self.subTest(name):
                self.server.entry_fault = fault
                with self.assertRaises(LoadError) as raised:
                    self.catalog.snapshot_ids("main")
                self.assertNotIn(MARKER, str(raised.exception))

    def test_each_malformed_content_of_a_table_is_refused_and_a_table_without_a_snapshot_is_still_not_listed(self) -> None:
        faults: dict[str, Callable[[dict[str, Any]], None]] = {
            "no content": drop("content"),
            "a content that is not an object": put("content", [MARKER]),
            "a snapshot that is text": lambda document: document["content"].__setitem__("snapshotId", MARKER),
            "a snapshot that is a boolean": lambda document: document["content"].__setitem__("snapshotId", True),
            "a snapshot that is a fraction": lambda document: document["content"].__setitem__("snapshotId", 1.5),
            "a snapshot that is null": lambda document: document["content"].__setitem__("snapshotId", None),
        }
        for name, fault in faults.items():
            with self.subTest(name):
                self.server.content_fault = fault
                with self.assertRaises(LoadError) as raised:
                    self.catalog.snapshot_ids("main")
                self.assertNotIn(MARKER, str(raised.exception))
        self.server.content_fault = None
        self.server.contents["main"]["raw_records.no_snapshot"] = table(None)
        self.assertEqual({"clinical_data": 11, "lab_data": 22}, self.catalog.snapshot_ids("main"))


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
