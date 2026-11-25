"""The host batch keeps its evidence in one folder of its own and removes only the Nessie references that it recorded itself.

The runners are untracked, so the tests run only where they are on disk. Nothing starts: stand-ins answer every Nessie and Docker call.

Failure modes covered here:

- The teardown lists the server's references and deletes each one whose name looks like the run's (a reference of another run or a person, with the same ending,
  goes too), or deletes a reference that was moved or re-made since the run (the name matches and the hash or the type does not).
- The inventory of the references is written from what a case planned and not from what the server holds, so a branch that was never made is recorded, or a
  branch that a failed job left is missed.
- The evidence of one run is spread over folders (the record, the manifest, the lake file) and cannot be read together, a file is half written when the run is
  stopped, or a name that leaves the folder is accepted.
- The snapshots are verified by the branch that made them and not by the saved tag, so a tag that points somewhere else is not noticed.
- A run whose evidence folder exists already (a second run with the same token, or a folder that a person made) writes its record, its files or its inventory
  into that folder, over what is there.
- A tag that the server already held (the creation answers 409) is recorded as the run's own, so the teardown deletes a reference that another run or a person
  made, or a creation with no clear answer is taken as not made.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieConflict, NessieError, Reference
from tests.e2e.run_record import Record

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")
SUFFIX = "0123abcd"
HASH_A, HASH_B = "a" * 64, "b" * 64


class FakeNessie:
    """References by name. Any call that lists them or reads a name that is not asked for is a failure of the code under test."""

    def __init__(self, references: dict[str, Reference]) -> None:
        self.references = dict(references)
        self.deleted: list[tuple[str, str, str | None]] = []
        self.created: list[tuple[str, str, str]] = []

    def request(self, *arguments: Any, **options: Any) -> Any:
        raise AssertionError("the server's references were listed")

    def reference(self, name: str) -> Reference:
        if name not in self.references:
            raise NessieError("not found", status=404)
        return self.references[name]

    def delete_reference(self, name: str, hash_value: str, kind: str | None = None) -> None:
        self.deleted.append((name, hash_value, kind))
        self.references.pop(name, None)

    def create_tag(self, name: str, hash_value: str, from_name: str = "main") -> Reference:
        self.created.append((name, hash_value, from_name))
        self.references[name] = Reference("TAG", name, hash_value)
        return self.references[name]


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class EvidenceCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        self.features: ModuleType = importlib.import_module("tests.e2e.stream_features")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)


class Inventory(EvidenceCase):
    def test_the_inventory_holds_name_type_and_hash_of_each_planned_reference_that_exists(self) -> None:
        client = FakeNessie({f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_A)})
        found = self.host.inventory_references(client, [f"stream_control_{SUFFIX}", f"stream_stop_{SUFFIX}"])
        self.assertEqual([{"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": HASH_A}], found)

    def test_a_server_error_other_than_not_found_is_not_taken_for_a_missing_reference(self) -> None:
        client = FakeNessie({})
        client.reference = lambda name: (_ for _ in ()).throw(NessieError("down", status=500))  # type: ignore[method-assign]
        with self.assertRaises(NessieError):
            self.host.inventory_references(client, [f"stream_control_{SUFFIX}"])

    def test_the_stack_records_each_reference_it_plans_once(self) -> None:
        stack = self.stack()
        stack.declare_reference(f"stream_control_{SUFFIX}")
        stack.declare_reference(f"stream_control_{SUFFIX}")
        self.assertEqual([f"stream_control_{SUFFIX}"], stack.references)

    def stack(self) -> Any:
        work = self.scratch / "work"
        (work / "run").mkdir(parents=True)
        return self.host.HostStack(
            Record(feature="stand-in", slug="stand_in", rerun="none"), [], "replay:test", "spark:test", work / "landed", work / "run", work
        )


class TeardownOfReferences(EvidenceCase):
    def entries(self) -> list[dict[str, str]]:
        return [
            {"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": HASH_A},
            {"name": f"stream_features_{SUFFIX}", "type": "TAG", "hash": HASH_A},
        ]

    def test_exactly_the_recorded_references_are_deleted_and_nothing_else(self) -> None:
        references = {
            f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_A),
            f"stream_features_{SUFFIX}": Reference("TAG", f"stream_features_{SUFFIX}", HASH_A),
            f"stream_other_{SUFFIX}": Reference("BRANCH", f"stream_other_{SUFFIX}", HASH_B),
            "main": Reference("BRANCH", "main", HASH_B),
        }
        client = FakeNessie(references)
        removed, failures = self.host.teardown_references(client, self.entries())
        self.assertEqual((2, []), (removed, failures))
        self.assertEqual({(f"stream_control_{SUFFIX}", HASH_A, "BRANCH"), (f"stream_features_{SUFFIX}", HASH_A, "TAG")}, set(client.deleted))
        self.assertEqual({f"stream_other_{SUFFIX}", "main"}, set(client.references))

    def test_a_reference_that_moved_since_the_run_is_not_deleted(self) -> None:
        client = FakeNessie({f"stream_control_{SUFFIX}": Reference("BRANCH", f"stream_control_{SUFFIX}", HASH_B)})
        removed, failures = self.host.teardown_references(client, self.entries()[:1])
        self.assertEqual(0, removed)
        self.assertEqual([], client.deleted)
        self.assertTrue(failures)

    def test_a_reference_of_another_type_is_not_deleted(self) -> None:
        client = FakeNessie({f"stream_control_{SUFFIX}": Reference("TAG", f"stream_control_{SUFFIX}", HASH_A)})
        removed, failures = self.host.teardown_references(client, self.entries()[:1])
        self.assertEqual((0, []), (removed, []))
        self.assertEqual([], client.deleted)
        self.assertTrue(failures)

    def test_a_reference_that_is_already_gone_is_not_a_failure(self) -> None:
        removed, failures = self.host.teardown_references(FakeNessie({}), self.entries())
        self.assertEqual((0, []), (removed, failures))

    def test_a_server_that_cannot_be_asked_is_a_failure_and_deletes_nothing(self) -> None:
        client = FakeNessie({})
        client.reference = lambda name: (_ for _ in ()).throw(NessieError("down", status=500))  # type: ignore[method-assign]
        removed, failures = self.host.teardown_references(client, self.entries())
        self.assertEqual(0, removed)
        self.assertEqual(2, len(failures))


class InventoryFile(EvidenceCase):
    def folder(self) -> Path:
        return self.scratch / f"stream_host_{SUFFIX}"

    def write(self, document: dict[str, Any]) -> None:
        self.folder().mkdir(exist_ok=True)
        (self.folder() / "inventory.json").write_text(json.dumps(document))

    def good(self) -> dict[str, Any]:
        return {
            "project": f"p8-host-{SUFFIX}",
            "references": [{"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": HASH_A}],
            "topics": [f"replay_events_p8h_{SUFFIX}_control"],
            "references_error": "",
            "unconfirmed": [],
        }

    def test_a_good_inventory_is_read(self) -> None:
        self.write(self.good())
        found = self.host.read_inventory(f"p8-host-{SUFFIX}", self.scratch)
        self.assertEqual(self.good()["references"], found.references)
        self.assertEqual(([f"replay_events_p8h_{SUFFIX}_control"], [], ""), (found.topics, found.unconfirmed, found.references_error))

    def test_a_missing_inventory_is_refused(self) -> None:
        with self.assertRaises(self.host.PlatformRefusal):
            self.host.read_inventory(f"p8-host-{SUFFIX}", self.scratch)

    def test_an_inventory_of_another_project_is_refused(self) -> None:
        document = self.good()
        document["project"] = "p8-host-ffffffff"
        self.write(document)
        with self.assertRaises(self.host.PlatformRefusal):
            self.host.read_inventory(f"p8-host-{SUFFIX}", self.scratch)

    def test_an_entry_that_is_not_a_name_of_this_run_a_known_type_and_a_hash_is_refused(self) -> None:
        for entry in (
            {"name": "main", "type": "BRANCH", "hash": HASH_A},
            {"name": f"stream_control_{SUFFIX}", "type": "OTHER", "hash": HASH_A},
            {"name": f"stream_control_{SUFFIX}", "type": "BRANCH", "hash": "not a hash"},
            {"name": "stream_control_ffffffff", "type": "BRANCH", "hash": HASH_A},
            {"name": f"stream_control_{SUFFIX}", "type": "BRANCH"},
        ):
            with self.subTest(entry=entry):
                document = self.good()
                document["references"] = [entry]
                self.write(document)
                with self.assertRaises(self.host.PlatformRefusal):
                    self.host.read_inventory(f"p8-host-{SUFFIX}", self.scratch)


class EvidenceFolder(EvidenceCase):
    def test_every_file_of_a_run_is_written_in_the_runs_own_folder(self) -> None:
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        evidence.create()
        evidence.write_json("lake.json", {"tag": "t"})
        evidence.write_text("manifest_control.json", "{}\n")
        self.assertEqual(self.scratch / f"stream_host_{SUFFIX}", evidence.folder)
        self.assertEqual({"lake.json", "manifest_control.json"}, {path.name for path in evidence.folder.iterdir()})
        self.assertEqual({"tag": "t"}, json.loads((evidence.folder / "lake.json").read_text()))

    def test_a_name_that_leaves_the_folder_is_refused(self) -> None:
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        evidence.create()
        for name in ("../x.json", "a/b.json", "/etc/x.json", ".hidden", "X.json", ""):
            with self.subTest(name=name), self.assertRaises(ValueError):
                evidence.write_text(name, "{}")

    def test_a_second_run_with_the_same_token_is_refused(self) -> None:
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        evidence.create()
        with self.assertRaises(FileExistsError):
            self.host.Evidence(SUFFIX, self.scratch).create()

    def test_a_folder_that_this_run_made_is_marked_as_made(self) -> None:
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        self.assertFalse(evidence.created)
        evidence.create()
        self.assertTrue(evidence.created)

    def test_a_folder_that_exists_already_is_not_this_runs_and_nothing_is_written_in_it(self) -> None:
        folder = self.scratch / f"stream_host_{SUFFIX}"
        folder.mkdir()
        (folder / "record.json").write_text("someone else's")
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        with self.assertRaises(FileExistsError):
            evidence.create()
        self.assertFalse(evidence.created)
        with self.assertRaises(RuntimeError):
            evidence.write_text("record.json", "mine")
        with self.assertRaises(RuntimeError):
            evidence.write_json("inventory.json", {})
        self.assertEqual(["record.json"], [path.name for path in folder.iterdir()])
        self.assertEqual("someone else's", (folder / "record.json").read_text())

    def test_a_run_whose_folder_exists_already_writes_nothing_there_and_cleans_its_temporary_folder(self) -> None:
        runs = ROOT / "runs"
        runs.mkdir(exist_ok=True)
        foreign = runs / "stream_host_feedbeef"
        foreign.mkdir()
        self.addCleanup(shutil.rmtree, foreign, True)
        (foreign / "record.json").write_text("someone else's")
        work = self.scratch / "p8_host_work_x"
        work.mkdir()
        arguments = argparse.Namespace(landed_cache=self.scratch / "landed", iteration=False, only=None)
        errors = io.StringIO()
        with (
            mock.patch.object(self.host.secrets, "token_hex", lambda size: "feedbeef"),
            mock.patch.object(self.host.tempfile, "mkdtemp", lambda prefix: str(work)),
            mock.patch.object(self.host, "fill_landed_cache", side_effect=AssertionError("the run went on after its evidence folder was refused")),
            contextlib.redirect_stderr(errors),
        ):
            code = self.host.run_batch(arguments, self.host.FULL)
        self.assertEqual(1, code)
        self.assertEqual(["record.json"], [path.name for path in foreign.iterdir()])
        self.assertEqual("someone else's", (foreign / "record.json").read_text())
        self.assertFalse(work.exists())
        self.assertIn("evidence folder", errors.getvalue())

    def test_a_write_leaves_no_part_file_behind(self) -> None:
        evidence = self.host.Evidence(SUFFIX, self.scratch)
        evidence.create()
        evidence.write_json("a.json", {})
        self.assertEqual([], [path.name for path in evidence.folder.iterdir() if path.name.endswith(".part")])

    def test_the_candidate_file_hash_and_each_case_hash_are_kept_together(self) -> None:
        document = self.host.candidate_hashes((908,))
        self.assertEqual({"file_sha256", "cases"}, set(document))
        self.assertEqual(64, len(document["file_sha256"]))
        self.assertEqual(["908"], list(document["cases"]))

    def test_the_record_is_closed_in_the_evidence_folder(self) -> None:
        runs = ROOT / "runs"
        runs.mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="evidence_test_", dir=runs))
        self.addCleanup(shutil.rmtree, root, True)
        evidence = self.host.Evidence(SUFFIX, root)
        evidence.create()
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        record.expect("c", "s", "e", "o", True)
        with contextlib.redirect_stdout(io.StringIO()):
            code = self.host.close_in_folder(record, ["log"], evidence.folder, blocked="")
        self.assertEqual(0, code)
        self.assertTrue({"record.json", "retention.json", "log.txt"} <= {path.name for path in evidence.folder.iterdir()})


class TagAndSnapshots(EvidenceCase):
    def lake(self) -> dict[str, Any]:
        return {name: {"rows": [1], "snapshots": [11, 12]} for name in ("features", "decisions", "ledgers", "errors")}

    def test_the_snapshots_are_read_again_through_the_saved_tag_and_must_equal_the_branchs(self) -> None:
        nessie = FakeNessie({"b": Reference("BRANCH", "b", HASH_A)})
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        read: list[str] = []

        def through_tag(name: str) -> dict[str, Any]:
            read.append(name)
            return self.lake()

        self.features.tag_run(record, nessie, "b", self.lake(), SUFFIX, folder=self.scratch, read_tag=through_tag)
        self.assertEqual([f"stream_features_{SUFFIX}"], read)
        self.assertTrue(all(item.passed for item in record.observations), [item.observed for item in record.observations])
        saved = json.loads((self.scratch / "lake.json").read_text())
        self.assertEqual({"tag", "branch", "hash", "snapshots"}, set(saved))

    def test_a_tag_that_shows_other_snapshots_fails_the_check(self) -> None:
        nessie = FakeNessie({"b": Reference("BRANCH", "b", HASH_A)})
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        other = self.lake()
        other["features"]["snapshots"] = [99]
        self.features.tag_run(record, nessie, "b", self.lake(), SUFFIX, folder=self.scratch, read_tag=lambda name: other)
        self.assertFalse(all(item.passed for item in record.observations))

    def run_tag(self, nessie: Any) -> tuple[list[str], list[str], Any]:
        declared: list[str] = []
        unsure: list[str] = []
        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        outcome = None
        try:
            self.features.tag_run(
                record, nessie, "b", self.lake(), SUFFIX, folder=self.scratch, read_tag=lambda name: self.lake(), declare=declared.append, unsure=unsure.append
            )
        except Exception as error:
            outcome = error
        return declared, unsure, outcome

    def test_a_tag_is_the_runs_only_after_its_creation_is_confirmed(self) -> None:
        declared, unsure, outcome = self.run_tag(FakeNessie({"b": Reference("BRANCH", "b", HASH_A)}))
        self.assertEqual(([f"stream_features_{SUFFIX}"], [], None), (declared, unsure, outcome))

    def test_a_tag_that_exists_already_is_not_the_runs_and_is_refused_unrecorded(self) -> None:
        nessie = FakeNessie({"b": Reference("BRANCH", "b", HASH_A), f"stream_features_{SUFFIX}": Reference("TAG", f"stream_features_{SUFFIX}", HASH_B)})

        def conflict(name: str, hash_value: str, from_name: str = "main") -> Reference:
            raise NessieConflict("exists", status=409)

        nessie.create_tag = conflict  # type: ignore[method-assign]
        declared, unsure, outcome = self.run_tag(nessie)
        self.assertEqual(([], []), (declared, unsure))
        self.assertIsInstance(outcome, self.features.Blocked)
        self.assertIn("not this run's", str(outcome))
        self.assertEqual([], nessie.deleted)
        self.assertFalse((self.scratch / "lake.json").exists())

    def test_a_creation_with_no_clear_answer_is_kept_for_inspection_and_not_recorded(self) -> None:
        for failure in (NessieError("down", status=503), NessieError("no answer"), TimeoutError("slow"), OSError("reset")):
            with self.subTest(failure=failure):
                nessie = FakeNessie({"b": Reference("BRANCH", "b", HASH_A)})

                def unclear(name: str, hash_value: str, from_name: str = "main", failure: Exception = failure) -> Reference:
                    raise failure

                nessie.create_tag = unclear  # type: ignore[method-assign]
                declared, unsure, outcome = self.run_tag(nessie)
                self.assertEqual(([], [f"tag stream_features_{SUFFIX}"]), (declared, unsure))
                self.assertIsInstance(outcome, self.features.Blocked)

    def test_a_creation_the_server_refused_is_not_ambiguous(self) -> None:
        nessie = FakeNessie({"b": Reference("BRANCH", "b", HASH_A)})

        def refused(name: str, hash_value: str, from_name: str = "main") -> Reference:
            raise NessieError("bad name", status=400)

        nessie.create_tag = refused  # type: ignore[method-assign]
        declared, unsure, outcome = self.run_tag(nessie)
        self.assertEqual(([], []), (declared, unsure))
        self.assertIsInstance(outcome, self.features.Blocked)

    def test_the_mismatches_name_each_table(self) -> None:
        other = self.lake()
        other["ledgers"]["snapshots"] = []
        self.assertEqual(["ledgers: the tag shows [] and the branch showed [11, 12]"], self.features.snapshot_mismatches(self.lake(), other))
        self.assertEqual([], self.features.snapshot_mismatches(self.lake(), self.lake()))


if __name__ == "__main__":
    unittest.main()
