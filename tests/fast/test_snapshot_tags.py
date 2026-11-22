"""The tag step of a reported run: the merged hash gets a tag with the run ID in its name, and the tag, the hash and each table's snapshot go in the run folder.

A stand-in for Nessie's API version 2 keeps the references and the contents at each hash, in the shapes that a real Nessie 0.110.1 gave in the acceptance run
of this slice, so the cases prove the step's decisions: a rerun with the same hash changes nothing, the same tag name on another hash is refused and the
folder's record is never overwritten by a different one. The calls against a real Nessie are proved by that acceptance run.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.stack import tags
from scripts.stack.nessie import NessieClient, NessieError

HASH_ONE = "1" * 64
HASH_TWO = "2" * 64
HASH_UNKNOWN = "9" * 64
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


class StandIn(NessieClient):
    """Nessie's references and the entries at each hash. Lists are paged two at a time, as a long list would be."""

    def __init__(self) -> None:
        super().__init__("http://nessie.invalid:19120/api/v2", 5)
        self.references: dict[str, tuple[str, str]] = {"main": ("BRANCH", HASH_TWO)}
        self.history = {HASH_ONE, HASH_TWO}
        self.entries_at: dict[str, list[dict[str, Any]]] = {
            HASH_ONE: [NAMESPACE_ENTRY, table_entry(["catalog_proof", "numbers"], 111, METADATA)],
            HASH_TWO: [NAMESPACE_ENTRY, table_entry(["catalog_proof", "numbers"], 222, METADATA)],
        }
        self.calls: list[tuple[str, str]] = []

    def answer(self, status: int, document: dict[str, Any]) -> tuple[int, bytes]:
        return status, json.dumps(document).encode()

    def page(self, items: list[Any], query: dict[str, list[str]], name: str) -> dict[str, Any]:
        start = int(query.get("pageToken", ["0"])[0])
        more = start + PAGE < len(items)
        return {name: items[start : start + PAGE], "hasMore": more, "token": str(start + PAGE) if more else None}

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.calls.append((method, target))
        parts = urlsplit(target)
        query = parse_qs(parts.query)
        path = unquote(parts.path).removeprefix("/api/v2")
        if method == "GET" and path == "/trees":
            refs = [{"type": kind, "name": name, "hash": value} for name, (kind, value) in sorted(self.references.items())]
            return self.answer(200, self.page(refs, query, "references"))
        if method == "GET" and path.startswith("/trees/") and path.endswith("/entries"):
            name = path[len("/trees/") : -len("/entries")]
            if name not in self.references:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"{name} not found"})
            return self.answer(200, self.page(self.entries_at[self.references[name][1]], query, "entries"))
        if method == "GET" and path.startswith("/trees/"):
            name = path[len("/trees/") :]
            if name not in self.references:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": f"Named reference '{name}' not found"})
            kind, value = self.references[name]
            return self.answer(200, {"reference": {"type": kind, "name": name, "hash": value}})
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
            table_entry(["catalog_proof", "empty"], None, "file:/warehouse/e/metadata/1.metadata.json"),
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

    def test_no_partial_file_is_left_beside_the_record(self) -> None:
        tags.write_version_record(self.folder, self.result)
        self.assertEqual(["data_version.json"], sorted(path.name for path in self.folder.iterdir()))


def run_command(arguments: list[str], nessie: StandIn | None, environment: bool = True) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    patched = mock.patch.object(tags, "NessieClient", lambda *_: nessie)
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
