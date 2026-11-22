"""The tag step of a reported run: the merged hash gets a tag with the run ID in its name, and the tag, the hash and each table's snapshot go in the run folder.

A stand-in for Nessie's API version 2 keeps the references and the contents at each hash, in the shapes that a real Nessie 0.110.1 gave in the acceptance run
of this slice, so the cases prove the step's decisions: a rerun with the same hash changes nothing, the same tag name on another hash is refused and the
folder's record is never overwritten by a different one. The calls against a real Nessie are proved by that acceptance run.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import signal
import tempfile
import threading
import time
import unittest
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.stack import tags
from scripts.stack.nessie import BODY_LIMIT_BYTES, NessieClient, NessieError

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


class TagNameTest(unittest.TestCase):
    def test_the_tag_name_holds_the_run_id(self) -> None:
        self.assertEqual(f"run-{RUN_ID}", tags.tag_name(RUN_ID))
        self.assertEqual("run-6b1d2c3e-0000-4000-8000-000000000001", tags.tag_name("6b1d2c3e-0000-4000-8000-000000000001"))

    def test_a_run_id_that_is_not_a_plain_name_is_refused(self) -> None:
        for run_id in ("", " ", "a b", "../main", "run/1", "-leading-dash", "x" * 97, "run.1", "ü"):
            with self.subTest(run_id=run_id), self.assertRaises(tags.TagError):
                tags.tag_name(run_id)


class ReadTest(unittest.TestCase):
    def test_every_tag_is_listed_across_pages_and_branches_are_left_out(self) -> None:
        nessie = StandIn()
        nessie.references.update({"run-a": ("TAG", HASH_ONE), "run-b": ("TAG", HASH_TWO), "run-c": ("TAG", HASH_ONE), "side": ("BRANCH", HASH_ONE)})
        self.assertEqual({"run-a": HASH_ONE, "run-b": HASH_TWO, "run-c": HASH_ONE}, tags.list_tags(nessie))

    def test_the_branches_are_listed_the_same_way(self) -> None:
        nessie = StandIn()
        nessie.references.update({"run-a": ("TAG", HASH_ONE), "side": ("BRANCH", HASH_ONE), "other": ("BRANCH", HASH_TWO)})
        self.assertEqual({"main": HASH_TWO, "side": HASH_ONE, "other": HASH_TWO}, tags.list_references(nessie, "BRANCH"))

    def test_the_tables_at_a_reference_are_listed_across_pages_with_their_snapshots_and_the_namespace_is_left_out(self) -> None:
        nessie = StandIn()
        nessie.entries_at[HASH_TWO] = [
            NAMESPACE_ENTRY,
            table_entry(["catalog_proof", "numbers"], 222, METADATA),
            table_entry(["catalog_proof", "other"], 333, "file:/warehouse/o/metadata/1.metadata.json"),
            table_entry(["catalog_proof", "empty"], -1, "file:/warehouse/e/metadata/1.metadata.json"),
        ]
        found = tags.table_snapshots(nessie, "main")
        self.assertEqual(
            (
                tags.TableSnapshot("catalog_proof.empty", None, "file:/warehouse/e/metadata/1.metadata.json"),
                tags.TableSnapshot("catalog_proof.numbers", 222, METADATA),
                tags.TableSnapshot("catalog_proof.other", 333, "file:/warehouse/o/metadata/1.metadata.json"),
            ),
            found,
        )

    def test_a_table_with_no_snapshot_is_recorded_with_none_and_a_negative_id_counts_as_none(self) -> None:
        nessie = StandIn()
        nessie.entries_at[HASH_TWO] = [table_entry(["a", "t"], -1, "file:/w/t/metadata/1.metadata.json")]
        self.assertEqual(None, tags.table_snapshots(nessie, "main")[0].snapshot_id)


class TagRunTest(unittest.TestCase):
    def test_the_merged_hash_is_tagged_and_each_table_snapshot_is_read_at_the_tag(self) -> None:
        nessie = StandIn()
        result = tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertEqual(("TAG", HASH_ONE), nessie.references[f"run-{RUN_ID}"])
        self.assertEqual((f"run-{RUN_ID}", HASH_ONE, True), (result.tag, result.hash, result.created))
        self.assertEqual((tags.TableSnapshot(TABLE, 111, METADATA),), result.tables)

    def test_the_snapshots_are_the_ones_at_the_tag_not_those_at_the_trunk_head(self) -> None:
        nessie = StandIn()
        result = tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertNotEqual(222, result.tables[0].snapshot_id)

    def test_a_rerun_with_the_same_run_and_hash_changes_nothing(self) -> None:
        nessie = StandIn()
        first = tags.tag_run(nessie, RUN_ID, HASH_ONE)
        writes = len(nessie.writes())
        second = tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertEqual((True, False), (first.created, second.created))
        self.assertEqual(first.tables, second.tables)
        self.assertEqual(writes, len(nessie.writes()))

    def test_the_same_tag_name_on_another_hash_is_refused_and_the_tag_stays_where_it_was(self) -> None:
        nessie = StandIn()
        tags.tag_run(nessie, RUN_ID, HASH_ONE)
        writes = len(nessie.writes())
        with self.assertRaises(tags.TagConflict) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_TWO)
        self.assertIn(f"run-{RUN_ID}", str(caught.exception))
        self.assertIn(HASH_ONE, str(caught.exception))
        self.assertEqual(("TAG", HASH_ONE), nessie.references[f"run-{RUN_ID}"])
        self.assertEqual(writes, len(nessie.writes()))

    def test_a_branch_of_the_tag_name_is_refused_even_at_the_same_hash(self) -> None:
        nessie = StandIn()
        nessie.references[f"run-{RUN_ID}"] = ("BRANCH", HASH_ONE)
        with self.assertRaises(tags.TagConflict):
            tags.tag_run(nessie, RUN_ID, HASH_ONE)

    def test_a_tag_that_appears_between_the_read_and_the_create_is_compared_not_trusted(self) -> None:
        nessie = StandIn()
        real_send = nessie.send
        raced = {"done": False}

        def racing(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            if method == "POST" and not raced["done"]:
                raced["done"] = True
                nessie.references[f"run-{RUN_ID}"] = ("TAG", HASH_TWO)
            return real_send(method, target, payload)

        with mock.patch.object(nessie, "send", racing), self.assertRaises(tags.TagConflict):
            tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertEqual(("TAG", HASH_TWO), nessie.references[f"run-{RUN_ID}"])

    def test_a_refusal_other_than_not_found_when_the_tag_is_read_is_an_error_and_no_tag_is_made(self) -> None:
        nessie = StandIn()
        real_send = nessie.send

        def refusing(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            if method == "GET" and target.endswith(f"/trees/run-{RUN_ID}"):
                return 500, b'{"errorCode": "UNKNOWN", "message": "refused"}'
            return real_send(method, target, payload)

        with mock.patch.object(nessie, "send", refusing), self.assertRaises(NessieError) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertEqual(500, caught.exception.status)
        self.assertEqual([], nessie.writes())

    def test_a_hash_that_is_not_a_hash_or_not_in_the_history_is_refused_and_no_tag_is_made(self) -> None:
        for value in ("main", "abc", "A" * 64, ""):
            with self.subTest(value=value):
                nessie = StandIn()
                with self.assertRaises(tags.TagError):
                    tags.tag_run(nessie, RUN_ID, value)
                self.assertEqual([], nessie.writes())
        nessie = StandIn()
        with self.assertRaises(NessieError):
            tags.tag_run(nessie, RUN_ID, HASH_UNKNOWN)
        self.assertNotIn(f"run-{RUN_ID}", nessie.references)

    def test_a_tag_that_does_not_read_back_at_the_hash_is_an_error(self) -> None:
        nessie = StandIn()
        real_send = nessie.send

        def lying(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            status, body = real_send(method, target, payload)
            if method == "POST":
                nessie.references[f"run-{RUN_ID}"] = ("TAG", HASH_TWO)
            return status, body

        with mock.patch.object(nessie, "send", lying), self.assertRaises(NessieError) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertIn(HASH_ONE, str(caught.exception))


class TrunkTest(unittest.TestCase):
    def test_a_commit_that_is_only_on_another_branch_is_refused_and_no_tag_is_made(self) -> None:
        nessie = StandIn()
        with self.assertRaises(tags.NotOnTrunk) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_SIDE)
        self.assertIn(HASH_SIDE, str(caught.exception))
        self.assertIn("main", str(caught.exception))
        self.assertNotIn(f"run-{RUN_ID}", nessie.references)
        self.assertEqual([], nessie.writes())

    def test_a_tag_that_already_sits_on_such_a_commit_is_refused_and_not_called_unchanged(self) -> None:
        nessie = StandIn()
        nessie.references[f"run-{RUN_ID}"] = ("TAG", HASH_SIDE)
        with self.assertRaises(tags.NotOnTrunk):
            tags.tag_run(nessie, RUN_ID, HASH_SIDE)

    def test_a_commit_that_the_log_of_main_reaches_on_a_later_page_is_accepted(self) -> None:
        nessie = StandIn()
        nessie.blank_history_pages = 3
        self.assertTrue(tags.tag_run(nessie, RUN_ID, HASH_ONE).created)

    def test_a_log_that_never_ends_is_an_error_after_the_page_limit_and_no_tag_is_made(self) -> None:
        nessie = StandIn()
        nessie.endless_history = True
        with mock.patch.object(tags, "MAX_PAGES", 5), self.assertRaises(NessieError) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertIn("pages", str(caught.exception))
        self.assertEqual(5, len([call for call in nessie.calls if "/history" in call[1]]))
        self.assertEqual([], nessie.writes())

    def test_the_log_is_asked_in_the_form_of_the_pinned_server_for_this_hash_in_pages_of_bounded_size(self) -> None:
        nessie = StandIn()
        tags.tag_run(nessie, RUN_ID, HASH_ONE)
        asked = [parse_qs(urlsplit(target).query) for _, target in nessie.calls if "/trees/main/history" in target]
        self.assertEqual(1, len(asked))
        self.assertEqual([f"commit.hash == '{HASH_ONE}'"], asked[0]["filter"])
        self.assertEqual([str(tags.PAGE_SIZE)], asked[0]["max-records"])

    def test_a_log_that_ignores_the_filter_is_read_for_the_hash_itself_and_other_commits_do_not_count(self) -> None:
        nessie = StandIn()
        nessie.ignore_filter = True
        with self.assertRaises(tags.NotOnTrunk):
            tags.tag_run(nessie, RUN_ID, HASH_SIDE)
        self.assertTrue(tags.tag_run(nessie, RUN_ID, HASH_ONE).created)

    def test_a_trunk_that_is_not_a_branch_is_refused(self) -> None:
        nessie = StandIn()
        nessie.references["fake_trunk"] = ("TAG", HASH_TWO)
        nessie.logs["fake_trunk"] = [HASH_TWO, HASH_ONE]
        nessie.default_branch = "fake_trunk"
        with self.assertRaises(tags.NotOnTrunk):
            tags.tag_run(nessie, RUN_ID, HASH_ONE)


class DefaultBranchTest(unittest.TestCase):
    """The trunk is the default branch that Nessie names. ``--trunk`` only asserts it."""

    def test_a_commit_that_exists_only_on_a_side_branch_is_refused_when_that_branch_is_named_as_the_trunk(self) -> None:
        nessie = StandIn()
        with self.assertRaises(tags.NotOnTrunk) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_SIDE, "side")
        self.assertIn("default branch", str(caught.exception))
        self.assertIn("main", str(caught.exception))
        self.assertEqual([], nessie.writes())
        self.assertEqual(["/api/v2/config"], [target for _, target in nessie.calls])

    def test_the_trunk_is_the_default_branch_that_nessie_names_when_none_is_given(self) -> None:
        nessie = StandIn()
        nessie.default_branch = "side"
        self.assertTrue(tags.tag_run(nessie, RUN_ID, HASH_SIDE).created)
        with self.assertRaises(tags.NotOnTrunk):
            tags.tag_run(nessie, f"{RUN_ID}x", HASH_TWO)

    def test_a_trunk_that_is_the_default_branch_is_accepted_as_an_assertion(self) -> None:
        self.assertEqual("main", tags.tag_run(StandIn(), RUN_ID, HASH_ONE, "main").trunk)
        self.assertEqual("main", tags.tag_run(StandIn(), RUN_ID, HASH_ONE).trunk)

    def test_a_configuration_without_a_default_branch_is_an_error_and_nothing_is_written(self) -> None:
        for name in (None, "", 7, ["main"]):
            with self.subTest(name=name):
                nessie = StandIn()
                nessie.default_branch = name
                with self.assertRaises(NessieError):
                    tags.tag_run(nessie, RUN_ID, HASH_ONE)
                self.assertEqual([], nessie.writes())

    def test_the_command_refuses_a_trunk_that_is_not_the_default_branch_before_any_other_call(self) -> None:
        nessie = StandIn()
        with tempfile.TemporaryDirectory() as directory:
            folder = str(Path(directory) / "run")
            code, out, err = run_command(["tag", "--run-id", RUN_ID, "--hash", HASH_SIDE, "--run-folder", folder, "--trunk", "side"], nessie)
            written = Path(folder).exists()
        self.assertEqual(1, code)
        self.assertIn("default branch", err)
        self.assertEqual([], nessie.writes())
        self.assertNotIn(f"run-{RUN_ID}", nessie.references)
        self.assertFalse(written)
        self.assertEqual(1, len(nessie.calls))


class EntriesTest(unittest.TestCase):
    """Every table entry carries its content: a metadata location and a snapshot ID. Nothing incomplete is recorded as verified."""

    def entries(self, *entries: dict[str, Any]) -> StandIn:
        nessie = StandIn()
        nessie.entries_at[HASH_ONE] = [NAMESPACE_ENTRY, *entries]
        return nessie

    def test_an_entry_without_content_is_an_error_naming_the_table_and_the_tag_step_exits_1_with_no_record(self) -> None:
        bare = {"type": "ICEBERG_TABLE", "name": {"elements": ["catalog_proof", "numbers"]}}
        nessie = self.entries(bare)
        with self.assertRaises(NessieError) as caught:
            tags.tag_run(nessie, RUN_ID, HASH_ONE)
        self.assertIn("catalog_proof.numbers", str(caught.exception))
        with tempfile.TemporaryDirectory() as directory:
            code, _, err = run_command(["tag", "--run-id", RUN_ID, "--hash", HASH_ONE, "--run-folder", str(Path(directory) / "run")], nessie)
            self.assertFalse((Path(directory) / "run").exists())
        self.assertEqual(1, code)
        self.assertIn("catalog_proof.numbers", err)

    def test_content_with_no_metadata_location_or_an_empty_one_or_one_that_is_not_text_is_an_error(self) -> None:
        for location in (None, "", 5, ["x"]):
            with self.subTest(location=location):
                entry = table_entry(["catalog_proof", "numbers"], 111, "x")
                entry["content"]["metadataLocation"] = location
                with self.assertRaises(NessieError) as caught:
                    tags.tag_run(self.entries(entry), RUN_ID, HASH_ONE)
                self.assertIn("catalog_proof.numbers", str(caught.exception))
        entry = table_entry(["catalog_proof", "numbers"], 111, "x")
        del entry["content"]["metadataLocation"]
        with self.assertRaises(NessieError):
            tags.tag_run(self.entries(entry), RUN_ID, HASH_ONE)

    def test_a_snapshot_id_that_is_missing_or_not_an_integer_or_below_the_no_snapshot_value_is_an_error(self) -> None:
        for snapshot in ("5", 5.0, True, None, -2, [1]):
            with self.subTest(snapshot=snapshot):
                entry = table_entry(["catalog_proof", "numbers"], 111, METADATA)
                entry["content"]["snapshotId"] = snapshot
                with self.assertRaises(NessieError) as caught:
                    tags.tag_run(self.entries(entry), RUN_ID, HASH_ONE)
                self.assertIn("catalog_proof.numbers", str(caught.exception))
        entry = table_entry(["catalog_proof", "numbers"], None, METADATA)
        with self.assertRaises(NessieError):
            tags.tag_run(self.entries(entry), RUN_ID, HASH_ONE)

    def test_the_explicit_no_snapshot_value_is_recorded_as_none_and_a_table_with_a_snapshot_is_recorded_with_it(self) -> None:
        empty = table_entry(["catalog_proof", "empty"], -1, "file:/w/e/metadata/1.metadata.json")
        full = table_entry(["catalog_proof", "numbers"], 111, METADATA)
        found = tags.tag_run(self.entries(empty, full), RUN_ID, HASH_ONE).tables
        self.assertEqual([None, 111], [item.snapshot_id for item in found])

    def test_an_entry_with_no_name_or_a_name_that_is_not_a_list_of_text_is_an_error(self) -> None:
        names: list[Any] = [None, {}, {"elements": []}, {"elements": "x"}, {"elements": [1]}]
        for name in names:
            with self.subTest(name=name):
                entry = table_entry(["catalog_proof", "numbers"], 111, METADATA)
                entry["name"] = name
                with self.assertRaises(NessieError):
                    tags.tag_run(self.entries(entry), RUN_ID, HASH_ONE)

    def test_entries_that_are_not_tables_are_still_left_out(self) -> None:
        view = {"type": "ICEBERG_VIEW", "name": {"elements": ["catalog_proof", "v"]}, "content": {"type": "ICEBERG_VIEW"}}
        found = tags.tag_run(self.entries(view, table_entry(["catalog_proof", "numbers"], 111, METADATA)), RUN_ID, HASH_ONE).tables
        self.assertEqual(["catalog_proof.numbers"], [item.table for item in found])


class RecordFirstTest(unittest.TestCase):
    """A record of another hash in the run folder refuses before any write to Nessie."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = Path(self.directory.name) / "run"

    def restore_record(self, value: str) -> None:
        self.folder.mkdir()
        record = tags.version_record(tags.tag_run(StandIn(), RUN_ID, value))
        (self.folder / "data_version.json").write_text(json.dumps(record, indent=2) + "\n")

    def arguments(self, value: str) -> list[str]:
        return ["tag", "--run-id", RUN_ID, "--hash", value, "--run-folder", str(self.folder)]

    def test_a_restored_record_at_one_hash_and_no_tag_refuses_a_request_for_another_hash_and_creates_no_tag(self) -> None:
        self.restore_record(HASH_ONE)
        nessie = StandIn()
        code, _, err = run_command(self.arguments(HASH_TWO), nessie)
        self.assertEqual(1, code)
        self.assertIn(HASH_ONE, err)
        self.assertEqual([], nessie.writes())
        self.assertNotIn(f"run-{RUN_ID}", nessie.references)
        self.assertEqual(HASH_ONE, json.loads((self.folder / "data_version.json").read_text())["hash"])

    def test_the_refusal_comes_before_any_call_to_nessie(self) -> None:
        self.restore_record(HASH_ONE)
        nessie = StandIn()
        run_command(self.arguments(HASH_TWO), nessie)
        self.assertEqual([], nessie.calls)

    def test_a_record_that_cannot_be_read_or_is_not_a_record_of_this_run_refuses_before_any_write(self) -> None:
        by_one_field = [
            json.dumps({"hash": HASH_ONE, "tag": f"run-{RUN_ID}", "run_id": "other"}),
            json.dumps({"hash": HASH_ONE, "tag": "run-other", "run_id": RUN_ID}),
            json.dumps({"hash": HASH_TWO, "tag": f"run-{RUN_ID}", "run_id": RUN_ID}),
        ]
        for text in (
            "not json",
            "[]",
            "{}",
            json.dumps({"hash": HASH_ONE}),
            json.dumps({"hash": HASH_ONE, "tag": "run-other", "run_id": "other"}),
            *by_one_field,
        ):
            with self.subTest(text=text[:30]):
                self.folder.mkdir(exist_ok=True)
                (self.folder / "data_version.json").write_text(text)
                nessie = StandIn()
                code, _, _ = run_command(self.arguments(HASH_ONE), nessie)
                self.assertEqual(1, code)
                self.assertEqual([], nessie.writes())
                self.assertNotIn(f"run-{RUN_ID}", nessie.references)

    def test_a_restored_record_at_the_same_hash_and_no_tag_makes_the_tag_and_leaves_the_record(self) -> None:
        self.restore_record(HASH_ONE)
        before = (self.folder / "data_version.json").read_bytes()
        nessie = StandIn()
        code, out, _ = run_command(self.arguments(HASH_ONE), nessie)
        self.assertEqual(0, code)
        self.assertIn("tagged", out)
        self.assertEqual(("TAG", HASH_ONE), nessie.references[f"run-{RUN_ID}"])
        self.assertEqual(before, (self.folder / "data_version.json").read_bytes())

    def test_the_library_call_judges_the_record_first_too(self) -> None:
        self.restore_record(HASH_ONE)
        nessie = StandIn()
        with self.assertRaises(tags.TagConflict):
            tags.tag_and_record(nessie, RUN_ID, HASH_TWO, self.folder)
        self.assertEqual([], nessie.calls)


class ForgedPages(StandIn):
    """A server that answers the list of references with pages that a client must not trust."""

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        super().__init__()
        self.pages = pages

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.calls.append((method, target))
        number = len([call for call in self.calls if call[1].startswith("/api/v2/trees?")]) - 1
        return self.answer(200, self.pages[min(number, len(self.pages) - 1)])


class PagingTest(unittest.TestCase):
    def test_a_token_that_comes_back_is_an_error_and_not_a_loop(self) -> None:
        forged = ForgedPages(
            [
                {"references": [], "hasMore": True, "token": "t1"},
                {"references": [], "hasMore": True, "token": "t2"},
                {"references": [], "hasMore": True, "token": "t1"},
            ]
        )
        with self.assertRaises(NessieError) as caught:
            tags.list_tags(forged)
        self.assertIn("token", str(caught.exception))
        self.assertLessEqual(len(forged.calls), 4)

    def test_a_token_that_does_not_move_on_is_an_error(self) -> None:
        forged = ForgedPages([{"references": [], "hasMore": True, "token": "t1"}, {"references": [], "hasMore": True, "token": "t1"}])
        with self.assertRaises(NessieError):
            tags.list_tags(forged)

    def test_more_pages_with_no_token_is_an_error_and_not_the_end_of_the_list(self) -> None:
        for page in ({"references": [], "hasMore": True}, {"references": [], "hasMore": True, "token": ""}, {"references": [], "hasMore": True, "token": 7}):
            with self.subTest(page=page), self.assertRaises(NessieError):
                tags.list_tags(ForgedPages([page]))

    def test_a_page_without_its_list_or_with_items_that_are_not_objects_is_an_error_and_not_an_empty_list(self) -> None:
        forged: list[Any] = [{}, {"hasMore": False}, {"references": "x"}, {"references": [1, 2]}, {"references": None}, []]
        for page in forged:
            with self.subTest(page=page), self.assertRaises(NessieError):
                tags.list_tags(ForgedPages([page]))

    def test_a_has_more_that_is_not_true_or_false_is_an_error_and_not_a_page_to_follow(self) -> None:
        last = {"references": [], "hasMore": False}
        forged: list[Any] = [{"references": []}, {"references": [], "hasMore": "yes", "token": "t1"}, {"references": [], "hasMore": 1, "token": "t1"}]
        for page in forged:
            with self.subTest(page=page), self.assertRaises(NessieError):
                tags.list_tags(ForgedPages([page, last]))

    def test_pages_are_asked_with_a_bounded_size_and_the_names_of_the_pinned_server(self) -> None:
        nessie = StandIn()
        tags.list_tags(nessie)
        tags.table_snapshots(nessie, "main")
        for _, target in nessie.calls:
            self.assertEqual([str(tags.PAGE_SIZE)], parse_qs(urlsplit(target).query)["max-records"])

    def test_a_chain_of_pages_that_passes_the_page_limit_is_an_error(self) -> None:
        nessie = StandIn()
        nessie.references.update({f"run-{number}": ("TAG", HASH_ONE) for number in range(10)})
        with mock.patch.object(tags, "MAX_PAGES", 3), self.assertRaises(NessieError) as caught:
            tags.list_tags(nessie)
        self.assertIn("pages", str(caught.exception))


@contextmanager
def serving(body: bytes, delay_seconds: float = 0.0) -> Iterator[tuple[str, list[str]]]:
    seen: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            seen.append(self.path)
            time.sleep(delay_seconds)
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                return

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/api/v2", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


class TagClientTest(unittest.TestCase):
    def test_an_answer_over_the_byte_cap_is_an_error_and_not_an_empty_inventory(self) -> None:
        padding = "x" * (BODY_LIMIT_BYTES + 100)
        body = json.dumps({"references": [{"type": "TAG", "name": "run-a", "hash": HASH_ONE, "note": padding}], "hasMore": False}).encode()
        with serving(body) as (url, _), self.assertRaises(NessieError) as caught:
            tags.list_tags(tags.TagClient(url, 30))
        self.assertIn("larger", str(caught.exception))

    def test_an_answer_just_inside_the_cap_is_read_whole(self) -> None:
        body = json.dumps({"references": [{"type": "TAG", "name": "run-a", "hash": HASH_ONE}], "hasMore": False}).encode()
        with serving(body) as (url, _):
            self.assertEqual({"run-a": HASH_ONE}, tags.list_tags(tags.TagClient(url, 30)))

    def test_a_deadline_that_has_passed_stops_a_call_before_it_is_sent(self) -> None:
        with serving(b"{}") as (url, seen):
            client = tags.TagClient(url, 0.5)
            with mock.patch("scripts.stack.tags.time.monotonic", return_value=time.monotonic() + 10), self.assertRaises(NessieError) as caught:
                client.reference("main")
        self.assertIn("deadline", str(caught.exception))
        self.assertEqual([], seen)

    def test_a_call_gets_only_the_time_that_is_left_of_the_deadline(self) -> None:
        with serving(b"{}", delay_seconds=3) as (url, _):
            started = time.monotonic()
            with self.assertRaises(NessieError):
                tags.TagClient(url, 0.5).reference("main")
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.5)


class SharedDeadlineTest(unittest.TestCase):
    def test_the_calls_of_one_run_share_the_deadline_and_the_second_gets_only_what_the_first_left(self) -> None:
        with serving(b"{}", delay_seconds=0.7) as (url, seen):
            client = tags.TagClient(url, 1.0)
            started = time.monotonic()
            client.config()
            with self.assertRaises(NessieError):
                client.config()
            elapsed = time.monotonic() - started
        self.assertEqual(2, len(seen))
        self.assertLess(elapsed, 1.35)


class DeadlineCommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = str(Path(self.directory.name) / "run")

    def test_a_deadline_that_is_not_finite_or_not_positive_exits_2_before_any_call(self) -> None:
        for value in ("inf", "-inf", "nan", "0", "-5", "1e999", "1e20", "86400.5", "86401"):
            nessie = StandIn()
            with self.subTest(value=value):
                code, _, err = run_command(["tag", "--run-id", RUN_ID, "--hash", HASH_ONE, "--run-folder", self.folder, f"--deadline-seconds={value}"], nessie)
                self.assertEqual(2, code)
                self.assertIn("deadline", err)
                self.assertEqual([], nessie.calls)

    def test_a_deadline_of_exactly_one_day_is_accepted_and_the_timer_is_cleared_afterwards(self) -> None:
        nessie = StandIn()
        code, _, _ = run_command(["tag", "--run-id", RUN_ID, "--hash", HASH_ONE, "--run-folder", self.folder, "--deadline-seconds=86400"], nessie)
        self.assertEqual(0, code)
        self.assertEqual(0.0, signal.getitimer(signal.ITIMER_REAL)[0])

    def test_a_run_that_outlasts_its_deadline_exits_1_and_says_so_and_leaves_nothing(self) -> None:
        nessie = StandIn()
        real_send = nessie.send

        def slow(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            time.sleep(4)
            return real_send(method, target, payload)

        started = time.monotonic()
        with mock.patch.object(nessie, "send", slow):
            code, _, err = run_command(["tag", "--run-id", RUN_ID, "--hash", HASH_ONE, "--run-folder", self.folder, "--deadline-seconds", "0.5"], nessie)
        self.assertEqual(1, code)
        self.assertIn("deadline", err)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual([], nessie.writes())
        self.assertFalse(Path(self.folder).exists())


class RunFolderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = Path(self.directory.name) / "run"
        self.result = tags.tag_run(StandIn(), RUN_ID, HASH_ONE)

    def test_the_record_holds_the_tag_the_hash_and_each_tables_snapshot(self) -> None:
        self.assertTrue(tags.write_version_record(self.folder, self.result))
        record = json.loads((self.folder / "data_version.json").read_text())
        self.assertEqual(
            {
                "run_id": RUN_ID,
                "tag": f"run-{RUN_ID}",
                "hash": HASH_ONE,
                "trunk": "main",
                "tables": [{"table": TABLE, "snapshot_id": 111, "metadata_location": METADATA}],
            },
            record,
        )

    def test_a_second_write_of_the_same_record_changes_nothing(self) -> None:
        tags.write_version_record(self.folder, self.result)
        before = (self.folder / "data_version.json").stat().st_mtime_ns
        self.assertFalse(tags.write_version_record(self.folder, self.result))
        self.assertEqual(before, (self.folder / "data_version.json").stat().st_mtime_ns)

    def test_a_record_of_another_hash_in_the_folder_is_refused_and_kept(self) -> None:
        tags.write_version_record(self.folder, self.result)
        other = tags.tag_run(StandIn(), RUN_ID, HASH_TWO)
        with self.assertRaises(tags.TagConflict) as caught:
            tags.write_version_record(self.folder, other)
        self.assertIn(HASH_ONE, str(caught.exception))
        self.assertEqual(HASH_ONE, json.loads((self.folder / "data_version.json").read_text())["hash"])

    def test_two_writers_that_both_passed_their_checks_leave_the_record_of_the_first_and_refuse_the_second(self) -> None:
        other = tags.tag_run(StandIn(), RUN_ID, HASH_TWO)
        real_link = os.link
        raced = {"done": False}

        def second_writer_first(source: Any, target: Any) -> None:
            if not raced["done"]:
                raced["done"] = True
                tags.write_version_record(self.folder, other)
            real_link(source, target)

        with mock.patch("scripts.stack.tags.os.link", second_writer_first), self.assertRaises(tags.TagConflict):
            tags.write_version_record(self.folder, self.result)
        self.assertEqual(HASH_TWO, json.loads((self.folder / "data_version.json").read_text())["hash"])
        self.assertEqual(["data_version.json"], sorted(path.name for path in self.folder.iterdir()))

    def test_two_writers_of_the_same_record_both_succeed_and_the_second_changes_nothing(self) -> None:
        real_link = os.link
        results: list[bool] = []

        def second_writer_first(source: Any, target: Any) -> None:
            if not results:
                results.append(False)
                results[0] = tags.write_version_record(self.folder, self.result)
            real_link(source, target)

        with mock.patch("scripts.stack.tags.os.link", second_writer_first):
            results.append(tags.write_version_record(self.folder, self.result))
        self.assertEqual([True, False], [results[0], results[1]])
        self.assertEqual(["data_version.json"], sorted(path.name for path in self.folder.iterdir()))

    def test_no_partial_file_is_left_beside_the_record(self) -> None:
        tags.write_version_record(self.folder, self.result)
        self.assertEqual(["data_version.json"], sorted(path.name for path in self.folder.iterdir()))


def run_command(arguments: list[str], nessie: StandIn | None, environment: bool = True) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    patched = mock.patch.object(tags, "TagClient", lambda *_: nessie)
    variables = {"NESSIE_URI": "http://nessie.invalid:19120/api/v2"} if environment else {}
    with patched, mock.patch.dict("os.environ", variables, clear=True), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = tags.main(arguments)
    return code, out.getvalue(), err.getvalue()


class CommandTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.folder = str(Path(self.directory.name) / "run")

    def arguments(self, value: str = HASH_ONE) -> list[str]:
        return ["tag", "--run-id", RUN_ID, "--hash", value, "--run-folder", self.folder]

    def test_a_first_run_tags_and_records_and_a_second_run_says_unchanged(self) -> None:
        nessie = StandIn()
        first = run_command(self.arguments(), nessie)
        writes = len(nessie.writes())
        second = run_command(self.arguments(), nessie)
        self.assertEqual((0, 0), (first[0], second[0]))
        self.assertIn("tagged", first[1])
        self.assertIn(f"run-{RUN_ID}", first[1])
        self.assertIn("unchanged", second[1])
        self.assertNotIn("tagged:", second[1])
        self.assertEqual(writes, len(nessie.writes()))
        self.assertEqual(HASH_ONE, json.loads((Path(self.folder) / "data_version.json").read_text())["hash"])

    def test_the_same_run_on_another_hash_exits_1_names_the_tag_and_changes_nothing(self) -> None:
        nessie = StandIn()
        run_command(self.arguments(), nessie)
        code, out, err = run_command(self.arguments(HASH_TWO), nessie)
        self.assertEqual(1, code)
        self.assertIn(f"run-{RUN_ID}", err)
        self.assertEqual(("TAG", HASH_ONE), nessie.references[f"run-{RUN_ID}"])
        self.assertEqual(HASH_ONE, json.loads((Path(self.folder) / "data_version.json").read_text())["hash"])

    def test_an_unset_address_or_a_bad_run_id_or_hash_exits_2_before_any_call(self) -> None:
        nessie = StandIn()
        unset = run_command(self.arguments(), nessie, environment=False)
        bad_run = run_command(["tag", "--run-id", "a b", "--hash", HASH_ONE, "--run-folder", self.folder], nessie)
        bad_hash = run_command(self.arguments("main"), nessie)
        self.assertEqual((2, 2, 2), (unset[0], bad_run[0], bad_hash[0]))
        self.assertIn("NESSIE_URI", unset[2])
        self.assertEqual([], nessie.calls)
        self.assertFalse(Path(self.folder).exists())


if __name__ == "__main__":
    unittest.main()
