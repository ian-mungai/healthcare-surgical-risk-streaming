"""The steps of the phase 5 host runner (``tests/e2e/bronze_host.py``) that change something: what the run made and may delete, what the cleanup proves, how the
private trunk is tagged, what is saved before a pass is printed, and the order of the stages.

The runner stays untracked, so each test skips when it is not on this machine. No host, no Docker and no AWS access are used: a stand-in for Nessie's API
(``tests/fast/nessie_stand_in.py``, with the delete call and the page forms of the real server) and a stand-in for the host's command line replace them.

Failure modes covered here:

- The cleanup deletes a branch or a folder that the run did not make: a branch of the run's name that was there before the job, or a folder that existed.
- The cleanup says a run left nothing behind when it could not look: a failed ``docker ps`` is taken for an empty list, a failed check of the folder is taken
  for a folder that is gone, a list of references that Nessie pages is read in its first page only (the parameter is ``page-token``), or a page that says there
  are more pages and gives no token is taken for the end.
- The containers are still running while branches and the folder are removed.
- The private trunk is tagged without a check of the checked hash and of the four snapshot IDs, the default-branch rule of the shared tag step refuses it, or a
  pass is printed with no tag evidence in the run's folder.
- A case file is read before the whole run's object versions are confirmed, or before the Spark workload's own access is proved.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import re
import runpy
import shlex
import shutil
import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock
from urllib.parse import unquote, urlsplit

from scripts.ingestion.manifest import ManifestRef
from scripts.stack.nessie import NessieConflict, NessieError
from scripts.stack.tags import NotOnTrunk, TagConflict, tag_and_record
from tests.e2e import run_record
from tests.e2e.run_record import Record
from tests.fast.nessie_stand_in import HASH_ONE, HASH_TWO, METADATA, NAMESPACE_ENTRY, StandIn, table_entry

E2E = Path(__file__).resolve().parents[1] / "e2e"
HASH = "a" * 64
CHECKED = "c" * 64
REFERENCE = json.dumps({"bucket": "b", "key": "manifests/x/" + HASH + ".json", "sha256": HASH, "version_id": "v1"})
TABLES = ("numeric_observations", "waveform_seconds", "tracks", "string_events")
SNAPSHOTS = {f"bronze.{name}": 100 + number for number, name in enumerate(TABLES)}
SUFFIX = "ab12cd"
PREFIX = f"p5h_{SUFFIX}"
TRUNK = f"{PREFIX}_trunk"


def runner() -> ModuleType:
    if not (E2E / "bronze_host.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.bronze_host")


class HostNessie(StandIn):
    """The stand-in with the delete call, and with an option to take an unknown page parameter as the real server does: it ignores it."""

    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events
        self.ignores_unknown = False
        self.no_token = False

    def page(self, items: list[Any], query: dict[str, list[str]], name: str) -> dict[str, Any]:
        page = super().page(items, query, name)
        if self.no_token and page["hasMore"]:
            page["token"] = None
        return page

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.events.append(f"nessie {method}")
        if self.ignores_unknown:
            target = re.sub(r"([?&])pageToken=[^&]*&?", r"\1", target).rstrip("?&")
        if method == "DELETE":
            self.calls.append((method, target))
            name, _, at = unquote(urlsplit(target).path).removeprefix("/api/v2/trees/").partition("@")
            if name not in self.references:
                return self.answer(404, {"errorCode": "REFERENCE_NOT_FOUND", "message": "not found"})
            if self.references[name][1] != at:
                return self.answer(409, {"errorCode": "REFERENCE_CONFLICT", "message": "moved"})
            del self.references[name]
            return self.answer(200, {})
        return super().send(method, target, payload)

    def add_trunk(self, snapshots: dict[str, int | None] | None = None) -> None:
        """A private trunk at ``CHECKED`` that holds the four Bronze tables at the given snapshot IDs."""
        held = SNAPSHOTS if snapshots is None else snapshots
        self.history.add(CHECKED)
        self.references[TRUNK] = ("BRANCH", CHECKED)
        self.logs[TRUNK] = [CHECKED, HASH_TWO, HASH_ONE]
        self.entries_at[CHECKED] = [
            {"type": "NAMESPACE", "name": {"elements": ["bronze"]}, "content": {"type": "NAMESPACE", "id": "n2", "elements": ["bronze"]}},
            *[table_entry(name.split("."), snapshot, METADATA) for name, snapshot in held.items()],
        ]


class FakeTarget:
    """The host's command line as the runner sees it: it answers each command that the cleanup, the folder and the listing use."""

    compose_directory = "/opt/platform/compose"
    secrets_directory = "/opt/platform/secrets"
    project = "platform"

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[list[str]] = []
        self.containers = ["0123456789ab"]
        self.folders: set[str] = set()
        self.files: dict[str, str] = {}
        self.replies: dict[str, str] = {}
        self.ps_code = 0
        self.rm_code = 0
        self.presence_code = 0
        self.remove_code = 0
        self.mkdir_code = 0

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 180.0) -> tuple[int, str, str]:
        self.calls.append(list(args))
        self.events.append("target " + " ".join(args[:3]))
        head = args[0]
        if head == "mkdir":
            path = args[-1]
            if self.mkdir_code:
                return self.mkdir_code, "", ""
            if "-p" not in args and path in self.folders:
                return 1, "", "File exists"
            self.folders.add(path)
            return 0, "", ""
        if head == "sh" and args[1] == "-c":
            if self.presence_code:
                return self.presence_code, "", ""
            path = shlex.split(args[2])[3]
            return 0, "present\n" if path in self.folders else "absent\n", ""
        if head == "rm":
            if self.remove_code == 0:
                self.folders.discard(args[-1])
            return self.remove_code, "", ""
        if args[:3] == ["docker", "ps", "-aq"]:
            return (self.ps_code, "\n".join(self.containers), "") if self.ps_code == 0 else (self.ps_code, "", "ssh: timeout")
        if args[:3] == ["docker", "rm", "-f"]:
            if self.rm_code == 0:
                self.containers = [item for item in self.containers if item not in args]
            return self.rm_code, "", ""
        if head == "docker" and args[1] == "run":
            for script, reply in self.replies.items():
                if script in args:
                    return 0, reply, ""
            return 0, 'LISTING {"digest": "' + "d" * 64 + '", "listed": true}', ""
        if head == "cat":
            return 0, self.files.get(args[-1], ""), ""
        return 0, "", ""

    def write_text(self, path: str, text: str) -> None:
        self.run(["tee", path], stdin=text)


class Hosted(unittest.TestCase):
    """A run on the stand-ins, with the folder made, the start of main read and the listing of the bucket kept."""

    def setUp(self) -> None:
        self.module = runner()
        patcher = mock.patch.object(self.module.HostStack, "health", return_value="healthy")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.events: list[str] = []
        self.nessie = HostNessie(self.events)
        self.target = FakeTarget(self.events)
        record = Record(feature="test", slug="bronze_host_test", rerun="test")
        stack = self.module.HostStack(self.target, record, [], SUFFIX)
        stack.region = "eu-west-1"
        self.host = self.module.HostRun(stack, self.nessie, record, ManifestRef.from_json(REFERENCE), None, {})
        self.addCleanup(shutil.rmtree, self.host.tag_folder, ignore_errors=True)
        self.host.main_before = HASH_TWO
        self.host.main_entries_before = ["catalog_proof", "catalog_proof.numbers"]
        self.host.listing_before = "d" * 64

    def made_folder(self) -> None:
        self.assertTrue(self.host.stack.prepare())

    def references(self) -> list[str]:
        return sorted(self.nessie.references)


class Ownership(Hosted):
    def test_a_name_that_exists_before_the_job_is_refused(self) -> None:
        self.nessie.references[f"{PREFIX}_a"] = ("BRANCH", HASH_ONE)
        with self.assertRaises(self.module.Refusal) as caught:
            self.host.fresh("a")
        self.assertEqual("name_taken", caught.exception.code)

    def test_a_fresh_name_is_not_the_runs_until_the_job_names_the_hash_that_nessie_holds(self) -> None:
        name = self.host.fresh("a")
        self.assertEqual(f"{PREFIX}_a", name)
        self.assertEqual([], self.host.made)
        self.assertFalse(self.host.adopt(name, CHECKED))
        self.nessie.references[name] = ("BRANCH", HASH_ONE)
        self.assertFalse(self.host.adopt(name, CHECKED))
        self.assertFalse(self.host.adopt(name, None))
        self.assertEqual([], self.host.made)
        self.assertTrue(self.host.adopt(name, HASH_ONE))
        self.assertEqual([name], self.host.made)

    def test_a_name_that_was_not_checked_as_free_is_never_adopted(self) -> None:
        self.nessie.references[f"{PREFIX}_a"] = ("BRANCH", HASH_ONE)
        self.assertFalse(self.host.adopt(f"{PREFIX}_a", HASH_ONE))
        self.assertEqual([], self.host.made)

    def test_a_branch_that_a_job_did_not_make_is_not_deleted_and_the_cleanup_reports_it(self) -> None:
        self.made_folder()
        planted = f"{PREFIX}_a"
        self.nessie.references[planted] = ("BRANCH", HASH_ONE)
        self.host.fresh("b")
        passed, text = self.module.remove_everything(self.host)
        self.assertIn(planted, self.references())
        self.assertFalse(passed)
        self.assertIn(planted, text)

    def test_a_branch_that_exists_when_the_run_creates_it_is_a_conflict_and_not_the_runs(self) -> None:
        self.nessie.references[f"{PREFIX}_x"] = ("BRANCH", HASH_ONE)
        with self.assertRaises(NessieConflict):
            self.host.branch("x", "main")
        self.assertEqual([], self.host.made)

    def test_a_branch_that_the_run_creates_is_the_runs(self) -> None:
        name = self.host.branch("x", "main")
        self.assertEqual([name], self.host.made)
        self.assertIn(name, self.references())

    def test_the_folder_is_made_without_parents_and_an_existing_one_is_refused(self) -> None:
        self.target.folders.add(self.host.stack.folder)
        self.assertFalse(self.host.stack.prepare())
        self.assertFalse(self.host.stack.folder_made)
        made = [call for call in self.target.calls if call[0] == "mkdir"]
        self.assertTrue(made)
        self.assertTrue(all("-p" not in call for call in made))

    def test_a_folder_that_the_run_did_not_make_is_not_removed(self) -> None:
        self.target.folders.add(self.host.stack.folder)
        self.host.stack.prepare()
        self.module.remove_everything(self.host)
        self.assertIn(self.host.stack.folder, self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "rm"])

    def test_a_failed_make_is_not_a_folder_of_the_run(self) -> None:
        self.target.mkdir_code = 255
        self.assertFalse(self.host.stack.prepare())
        self.assertFalse(self.host.stack.folder_made)


class CleanupProof(Hosted):
    def setUp(self) -> None:
        super().setUp()
        self.made_folder()
        self.host.branch("trunk", "main")
        self.host.branch("b", "main")

    def test_everything_that_the_run_made_is_removed_and_proved(self) -> None:
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual([], [name for name in self.references() if name.startswith(PREFIX)])
        self.assertEqual([], self.target.containers)
        self.assertNotIn(self.host.stack.folder, self.target.folders)

    def test_the_containers_are_stopped_and_checked_before_a_branch_or_the_folder_is_removed(self) -> None:
        self.module.remove_everything(self.host)
        removed = next(number for number, event in enumerate(self.events) if event.startswith("target docker rm"))
        deleted = next(number for number, event in enumerate(self.events) if event == "nessie DELETE")
        folder = next(number for number, event in enumerate(self.events) if event.startswith("target rm"))
        listed_after = [number for number, event in enumerate(self.events) if event.startswith("target docker ps") and number > removed]
        self.assertTrue(listed_after)
        self.assertLess(listed_after[0], deleted)
        self.assertLess(removed, deleted)
        self.assertLess(removed, folder)

    def test_a_failed_container_listing_is_a_failure_and_not_an_empty_list(self) -> None:
        self.target.ps_code = 255
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("unknown", text)
        self.assertEqual([], [name for name in self.references() if name.startswith(PREFIX)])

    def test_a_container_that_cannot_be_removed_is_a_failure(self) -> None:
        self.target.rm_code = 1
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("0123456789ab", text)

    def test_a_failed_check_of_the_folder_is_a_failure_and_not_a_folder_that_is_gone(self) -> None:
        self.target.presence_code = 255
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("unknown", text)

    def test_a_folder_that_cannot_be_removed_is_a_failure(self) -> None:
        self.target.remove_code = 1
        passed, _ = self.module.remove_everything(self.host)
        self.assertFalse(passed)

    def test_a_reference_of_the_run_on_a_later_page_is_found(self) -> None:
        self.nessie.ignores_unknown = True
        for name in ("a1", "a2"):
            self.nessie.references[name] = ("BRANCH", HASH_ONE)
        stray = f"{PREFIX}_zz"
        self.nessie.references[stray] = ("BRANCH", HASH_ONE)
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn(stray, text)

    def test_every_page_of_the_references_is_read_with_the_servers_page_parameter(self) -> None:
        self.nessie.ignores_unknown = True
        for name in ("a1", "a2", "a3"):
            self.nessie.references[name] = ("BRANCH", HASH_ONE)
        self.assertEqual(sorted(self.nessie.references), sorted(self.host.references()))

    def test_a_page_that_says_there_are_more_and_gives_no_token_is_a_failure(self) -> None:
        self.nessie.no_token = True
        for name in ("a1", "a2", "a3"):
            self.nessie.references[name] = ("BRANCH", HASH_ONE)
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("references", text)

    def test_a_branch_that_cannot_be_deleted_is_a_failure_that_names_it(self) -> None:
        name = self.host.made[0]
        with mock.patch.object(self.nessie, "delete_reference", side_effect=NessieError("refused", status=500)):
            passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn(name, text)

    def test_a_bucket_listing_that_cannot_be_read_is_a_failure(self) -> None:
        self.host.listing_before = ""
        passed, _ = self.module.remove_everything(self.host)
        self.assertFalse(passed)

    def test_a_bucket_listing_that_changed_is_a_failure(self) -> None:
        self.host.listing_before = "e" * 64
        passed, _ = self.module.remove_everything(self.host)
        self.assertFalse(passed)

    def test_a_cleanup_step_that_stops_does_not_skip_the_steps_after_it(self) -> None:
        self.target.ps_code = 255
        self.module.remove_everything(self.host)
        self.assertNotIn(self.host.stack.folder, self.target.folders)

    def test_a_run_tag_that_is_required_and_missing_is_a_failure(self) -> None:
        self.host.notes["tag"] = self.host.tag
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn(self.host.tag, text)


class Entries(Hosted):
    def setUp(self) -> None:
        super().setUp()
        self.nessie.entries_at[HASH_TWO] = [
            NAMESPACE_ENTRY,
            table_entry(["catalog_proof", "numbers"], 222, METADATA),
            table_entry(["catalog_proof", "more"], 5, METADATA),
        ]

    def test_the_entry_names_of_every_page_are_read(self) -> None:
        self.assertEqual(["catalog_proof", "catalog_proof.more", "catalog_proof.numbers"], self.module.entry_names(self.nessie, "main"))

    def test_a_bronze_table_on_a_later_page_of_a_base_is_found(self) -> None:
        name = self.host.branch("base", "main")
        at = self.nessie.reference(name).hash
        self.nessie.entries_at[HASH_TWO] = [
            NAMESPACE_ENTRY,
            table_entry(["a", "a"], 1, METADATA),
            table_entry(["b", "b"], 2, METADATA),
            table_entry(["bronze", "tracks"], 3, METADATA),
        ]
        self.assertFalse(self.host.unchanged(name, at))

    def test_a_base_without_a_bronze_table_at_its_hash_is_unchanged(self) -> None:
        name = self.host.branch("base", "main")
        self.assertTrue(self.host.unchanged(name, self.nessie.reference(name).hash))


class PrivateTrunk(Hosted):
    def setUp(self) -> None:
        super().setUp()
        self.nessie.add_trunk()
        self.folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_tag_"))
        self.addCleanup(shutil.rmtree, self.folder, ignore_errors=True)

    def tag(self, **changes: Any) -> Any:
        asked = {"run_id": PREFIX, "trunk": TRUNK, "checked_hash": CHECKED, "snapshots": SNAPSHOTS, "folder": self.folder}
        return self.module.tag_private_trunk(self.nessie, **{**asked, **changes})

    def test_the_shared_step_refuses_a_trunk_that_is_not_the_default_branch(self) -> None:
        with self.assertRaises(NotOnTrunk):
            tag_and_record(self.nessie, PREFIX, CHECKED, self.folder, trunk=TRUNK)
        self.assertEqual([], self.nessie.writes())

    def test_the_private_trunk_is_tagged_at_the_checked_hash_with_the_four_snapshot_ids(self) -> None:
        result, record = self.tag()
        self.assertEqual(f"run-{PREFIX}", result.tag)
        self.assertEqual(("TAG", CHECKED), (self.nessie.reference(result.tag).kind, self.nessie.reference(result.tag).hash))
        self.assertEqual(CHECKED, record["hash"])
        self.assertEqual(SNAPSHOTS, {item["table"]: item["snapshot_id"] for item in record["tables"]})
        self.assertEqual(TRUNK, record["trunk"])

    def test_the_record_is_saved_in_the_folder_and_equals_what_was_verified(self) -> None:
        _, record = self.tag()
        self.assertEqual(record, json.loads((self.folder / "data_version.json").read_text()))

    def test_a_trunk_that_is_not_at_the_checked_hash_is_refused_before_any_write(self) -> None:
        self.nessie.references[TRUNK] = ("BRANCH", HASH_TWO)
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("trunk_hash", caught.exception.code)
        self.assertEqual([], self.nessie.writes())

    def test_a_snapshot_id_that_differs_from_the_jobs_is_refused_before_any_write(self) -> None:
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag(snapshots={**SNAPSHOTS, "bronze.tracks": 999})
        self.assertEqual("snapshots", caught.exception.code)
        self.assertEqual([], self.nessie.writes())
        self.assertFalse((self.folder / "data_version.json").exists())

    def test_a_table_that_the_trunk_lacks_is_refused_before_any_write(self) -> None:
        self.nessie.add_trunk({name: snapshot for name, snapshot in SNAPSHOTS.items() if name != "bronze.tracks"})
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("snapshots", caught.exception.code)
        self.assertEqual([], self.nessie.writes())

    def test_a_table_with_no_snapshot_is_refused(self) -> None:
        self.nessie.add_trunk({**SNAPSHOTS, "bronze.tracks": None})
        with self.assertRaises(self.module.TagProblem):
            self.tag(snapshots={**SNAPSHOTS, "bronze.tracks": None})
        self.assertEqual([], self.nessie.writes())

    def test_a_job_that_names_fewer_than_four_tables_is_refused(self) -> None:
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag(snapshots={name: snapshot for name, snapshot in SNAPSHOTS.items() if name != "bronze.tracks"})
        self.assertEqual("snapshots", caught.exception.code)

    def test_a_hash_that_is_not_in_the_history_of_the_trunk_is_refused(self) -> None:
        self.nessie.logs[TRUNK] = [HASH_TWO, HASH_ONE]
        with self.assertRaises(NotOnTrunk):
            self.tag()
        self.assertEqual([], self.nessie.writes())

    def test_a_tag_of_the_run_at_another_hash_is_refused_and_never_moved(self) -> None:
        self.nessie.references[f"run-{PREFIX}"] = ("TAG", HASH_ONE)
        with self.assertRaises(TagConflict):
            self.tag()
        self.assertEqual(("TAG", HASH_ONE), self.nessie.references[f"run-{PREFIX}"])

    def test_a_second_tag_step_with_the_same_facts_changes_nothing(self) -> None:
        first = self.tag()[1]
        again = self.tag()[1]
        self.assertEqual(first, again)
        self.assertEqual(1, len([call for call in self.nessie.writes() if call[0] == "POST"]))

    def test_a_record_in_the_folder_of_another_hash_is_refused(self) -> None:
        (self.folder / "data_version.json").write_text(json.dumps({"run_id": PREFIX, "tag": f"run-{PREFIX}", "hash": HASH}))
        with self.assertRaises(TagConflict):
            self.tag()
        self.assertEqual([], self.nessie.writes())


class Evidence(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()
        scratch = Path(tempfile.mkdtemp(prefix="bronze_host_test_evidence_"))
        self.addCleanup(shutil.rmtree, scratch, ignore_errors=True)
        self.scratch = scratch
        patcher = mock.patch.object(run_record, "ROOT", scratch)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.script = scratch / "bronze_host.py"
        self.script.write_text("# the runner\n")
        self.record = Record(feature="test", slug="bronze_host_test", rerun="test")
        self.record.expect("a control", "control", "passes", "passed", True)
        self.version: dict[str, Any] = {
            "run_id": PREFIX,
            "tag": f"run-{PREFIX}",
            "hash": CHECKED,
            "trunk": TRUNK,
            "tables": [{"table": name, "snapshot_id": snapshot, "metadata_location": METADATA} for name, snapshot in SNAPSHOTS.items()],
        }
        self.notes: dict[str, Any] = {"merged_hash": CHECKED, "tag": f"run-{PREFIX}", "snapshots": SNAPSHOTS, "data_version": self.version}

    def finish(self, blocked: str = "") -> tuple[int, Path]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.module.finish(self.record, ["log"], self.notes, blocked, self.script)
        folders = sorted((self.scratch / "runs").iterdir())
        self.assertEqual(1, len(folders))
        return code, folders[0]

    def outcome(self, folder: Path) -> str:
        return str(json.loads((folder / "record.json").read_text())["outcome"])

    def test_a_pass_has_the_tag_evidence_saved_in_the_run_folder(self) -> None:
        code, folder = self.finish()
        self.assertEqual((0, "pass"), (code, self.outcome(folder)))
        self.assertEqual(self.version, json.loads((folder / "data_version.json").read_text()))
        self.assertEqual("pass", self.outcome(folder))
        self.assertEqual(CHECKED, json.loads((folder / "run_notes.json").read_text())["merged_hash"])
        self.assertTrue((folder / "bronze_host.py").is_file())

    def test_a_run_that_published_but_has_no_tag_evidence_is_not_a_pass(self) -> None:
        del self.notes["data_version"]
        code, folder = self.finish()
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))
        self.assertFalse((folder / "data_version.json").exists())
        self.assertTrue(any(not item.passed and "tag evidence" in item.case for item in self.record.observations))

    def test_evidence_that_does_not_name_the_merged_hash_is_not_a_pass(self) -> None:
        self.notes["data_version"] = {**self.version, "hash": HASH}
        code, folder = self.finish()
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))

    def test_evidence_with_other_snapshot_ids_than_the_runs_is_not_a_pass(self) -> None:
        tables = [dict(item) for item in self.version["tables"]]
        tables[0]["snapshot_id"] = 999
        self.notes["data_version"] = {**self.version, "tables": tables}
        self.assertEqual(1, self.finish()[0])

    def test_evidence_with_fewer_than_four_tables_is_not_a_pass(self) -> None:
        self.notes["data_version"] = {**self.version, "tables": self.version["tables"][:3]}
        self.assertEqual(1, self.finish()[0])

    def test_a_run_blocked_before_the_control_needs_no_tag_evidence(self) -> None:
        self.notes = {}
        code, folder = self.finish(blocked="no usable passed manifest")
        self.assertEqual(1, code)
        self.assertTrue(self.outcome(folder).startswith("blocked"))
        self.assertFalse((folder / "data_version.json").exists())

    def test_a_failed_observation_is_never_turned_into_a_pass_by_the_evidence(self) -> None:
        self.record.expect("cleanup", "cleanup", "nothing left", "a branch is left", False)
        code, folder = self.finish()
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))
        self.assertTrue((folder / "data_version.json").is_file())


class Stages(Hosted):
    def stage_names(self) -> Iterator[str]:
        yield from ("stage_stack", "stage_admission", "stage_access", "stage_download", "stage_trunk", "stage_decode", "stage_clinical")

    def run_with(self, results: dict[str, Any]) -> tuple[list[str], str]:
        order: list[str] = []
        patches = []
        for name in self.stage_names():

            def record(run: Any, name: str = name) -> Any:
                order.append(name)
                return results.get(name)

            patches.append(mock.patch.object(self.module, name, side_effect=record))
        patches.append(mock.patch.object(self.module, "record_end", side_effect=lambda run: shutil.rmtree(run.tag_folder, ignore_errors=True)))
        patches.append(mock.patch.object(self.module.proof, "Remote", return_value=self.target))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            blocked = self.module.run_host(self.host.record, [], self.host.reference, HASH, {})
        return order, blocked

    def test_the_stages_run_in_the_order_that_proves_each_access_before_the_next_read(self) -> None:
        order, blocked = self.run_with({"stage_stack": True, "stage_admission": "", "stage_access": True, "stage_download": ""})
        self.assertEqual("", blocked)
        self.assertEqual(["stage_stack", "stage_admission", "stage_access", "stage_download", "stage_trunk", "stage_decode", "stage_clinical"], order)

    def test_the_spark_access_is_proved_before_any_case_file_is_downloaded(self) -> None:
        order, blocked = self.run_with({"stage_stack": True, "stage_admission": "", "stage_access": False})
        self.assertIn("cannot read the manifest", blocked)
        self.assertEqual(["stage_stack", "stage_admission", "stage_access"], order)

    def test_an_admission_refusal_stops_before_the_access_probe_and_the_download(self) -> None:
        order, blocked = self.run_with({"stage_stack": True, "stage_admission": "no usable passed manifest (case_set)"})
        self.assertIn("case_set", blocked)
        self.assertEqual(["stage_stack", "stage_admission"], order)

    def test_the_admission_reads_names_and_versions_only_and_never_downloads(self) -> None:
        self.target.replies["admit"] = 'FETCH {"problem": null, "cases": [908, 1147]}'
        self.made_folder()
        self.assertEqual("", self.module.stage_admission(self.host))
        self.assertEqual([908, 1147], self.host.cases)
        asked = [call for call in self.target.calls if call[:2] == ["docker", "run"]]
        self.assertTrue(asked)
        self.assertTrue(all("download" not in call for call in asked))
        self.assertTrue(all("admit" in call for call in asked))

    def test_the_admission_names_the_refusal_when_the_gate_or_the_sample_refuses(self) -> None:
        self.target.replies["admit"] = 'FETCH {"problem": "gate_refused"}'
        self.made_folder()
        self.assertIn("gate_refused", self.module.stage_admission(self.host))
        self.assertEqual([], self.host.cases)

    def test_the_download_reads_the_bound_versions_and_checks_the_cases_again(self) -> None:
        self.host.cases = [908, 1147]
        self.target.replies["download"] = 'FETCH {"problem": null, "cases": [908, 1147]}'
        self.target.files[f"{self.host.stack.out}/versions.json"] = json.dumps({"0908": ["v1", HASH], "1147": ["v2", HASH]})
        self.made_folder()
        self.assertEqual("", self.module.stage_download(self.host))
        self.assertEqual(["0908", "1147"], sorted(self.host.versions))

    def test_a_download_that_binds_other_cases_than_the_admitted_ones_is_refused(self) -> None:
        self.host.cases = [908, 1147]
        self.target.replies["download"] = 'FETCH {"problem": null, "cases": [908]}'
        self.made_folder()
        self.assertNotEqual("", self.module.stage_download(self.host))


class Container(unittest.TestCase):
    """The code that runs in the replay container, run here against stand-ins of the store and the gate."""

    DEVELOPMENT = {908, 1147}
    KEYS = ["vitaldb/1.0.0/clinical_data.csv", "vitaldb/1.0.0/vital_files/0908.vital", "vitaldb/1.0.0/vital_files/1147.vital"]

    def setUp(self) -> None:
        self.module = runner()
        self.entries = [SimpleNamespace(source_key=key, key="landing/" + key, version_id=f"v{number}", sha256="0" * 64) for number, key in enumerate(self.KEYS)]
        self.manifest = SimpleNamespace(status="passed", entries=self.entries)
        self.reads: list[str] = []
        self.store = SimpleNamespace(bucket="b")
        reading = mock.patch("scripts.ingestion.manifest.read_manifest", side_effect=lambda store, reference: self.manifest)
        reading.start()
        self.addCleanup(reading.stop)
        self.gate = mock.patch("scripts.ingestion.gate.verify_before_read", side_effect=self.verified)
        self.gate_called = self.gate.start()
        self.addCleanup(self.gate.stop)
        self.refuse = False

    def verified(self, store: Any, reference: Any, keys: Any) -> tuple[Any, ...]:
        from scripts.ingestion.gate import GateRefused
        from scripts.ingestion.notice import Code, Notice

        self.assertEqual([], self.reads)
        if self.refuse:
            raise GateRefused(Notice(Code.GATE_REFUSED))
        return tuple(entry for entry in self.entries if entry.source_key in keys)

    def test_the_gate_is_asked_for_every_required_object_version_before_any_read(self) -> None:
        problem, entries = self.module.admit(self.store, "reference", self.DEVELOPMENT)
        self.assertIsNone(problem)
        self.assertEqual(1, self.gate_called.call_count)
        self.assertEqual(sorted(self.KEYS), sorted(self.gate_called.call_args.args[2]))
        self.assertEqual(3, len(entries))

    def test_a_gate_refusal_is_a_problem_and_nothing_is_read(self) -> None:
        self.refuse = True
        self.assertEqual(("gate_refused", []), self.module.admit(self.store, "reference", self.DEVELOPMENT))
        self.assertEqual([], self.reads)

    def test_a_manifest_that_is_not_the_fixed_sample_is_refused_before_the_gate(self) -> None:
        self.manifest.entries = self.entries[:2]
        self.assertEqual(("case_set", []), self.module.admit(self.store, "reference", self.DEVELOPMENT))
        self.assertEqual(0, self.gate_called.call_count)

    def test_a_manifest_that_did_not_pass_is_refused_before_the_gate(self) -> None:
        self.manifest.status = "failed"
        self.assertEqual(("manifest_not_passed", []), self.module.admit(self.store, "reference", self.DEVELOPMENT))
        self.assertEqual(0, self.gate_called.call_count)

    def test_the_download_reads_each_bound_version_and_checks_its_digest(self) -> None:
        import hashlib

        payload = {"v1": b"one", "v2": b"two"}
        for entry in self.entries[1:]:
            entry.sha256 = hashlib.sha256(payload[entry.version_id]).hexdigest()
        reads = self.reads

        class Store:
            def list_versions(self, key: str) -> list[Any]:
                return [SimpleNamespace(key=entry.key, version_id=entry.version_id) for entry in entries if entry.key == key]

            def iter_version(self, version: Any) -> Iterator[bytes]:
                reads.append(version.version_id)
                yield payload[version.version_id]

        entries = self.entries
        folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_cases_"))
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        problem, versions = self.module.download(Store(), self.entries, str(folder))
        self.assertIsNone(problem)
        self.assertEqual(["v1", "v2"], reads)
        self.assertEqual({"0908": ["v1", self.entries[1].sha256], "1147": ["v2", self.entries[2].sha256]}, versions)
        self.assertEqual(b"one", (folder / "cases" / "0908.vital").read_bytes())

    def test_a_download_with_another_digest_is_refused(self) -> None:
        class Store:
            def list_versions(self, key: str) -> list[Any]:
                return [SimpleNamespace(key=entry.key, version_id=entry.version_id) for entry in entries if entry.key == key]

            def iter_version(self, version: Any) -> Iterator[bytes]:
                yield b"other"

        entries = self.entries
        folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_cases_"))
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        self.assertEqual("case_sha256", self.module.download(Store(), self.entries, str(folder))[0])

    @staticmethod
    def script(folder: Path, name: str, text: str) -> str:
        (folder / name).write_text(text)
        return str(folder / name)

    def run_fetch(self, mode: str, store: Any) -> tuple[str, Path]:
        """The fetch script as the replay container runs it, with the container's folder replaced by a scratch folder and the bucket by ``store``."""
        folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_fetch_"))
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        (folder / "reference.json").write_text(REFERENCE)
        (folder / "development.json").write_text(json.dumps(sorted(self.DEVELOPMENT)))
        stub = SimpleNamespace(S3ReadStore=lambda bucket, region: store)
        out = io.StringIO()
        with (
            mock.patch.dict("sys.modules", {"casefile.s3_store": stub}),
            mock.patch.dict("os.environ", {"AWS_REGION": "eu-west-1"}),
            mock.patch("sys.argv", ["fetch.py", mode]),
            contextlib.redirect_stdout(out),
            contextlib.suppress(SystemExit),
        ):
            runpy.run_path(self.script(folder, "fetch.py", self.module.FETCH.replace("/out", str(folder))))
        return out.getvalue(), folder

    def test_the_fetch_script_in_admit_mode_names_the_cases_and_downloads_nothing(self) -> None:
        reads = self.reads

        class Store:
            def iter_version(self, version: Any) -> Iterator[bytes]:
                reads.append(version.version_id)
                yield b""

        said, folder = self.run_fetch("admit", Store())
        self.assertEqual('FETCH {"problem": null, "cases": [908, 1147]}', said.strip())
        self.assertEqual([], self.reads)
        self.assertFalse((folder / "cases").exists())

    def test_the_fetch_script_in_download_mode_reads_the_bound_versions_and_binds_them(self) -> None:
        import hashlib

        entries = self.entries
        for entry in entries[1:]:
            entry.sha256 = hashlib.sha256(b"x" + entry.version_id.encode()).hexdigest()

        class Store:
            def list_versions(self, key: str) -> list[Any]:
                return [SimpleNamespace(key=entry.key, version_id=entry.version_id) for entry in entries if entry.key == key]

            def iter_version(self, version: Any) -> Iterator[bytes]:
                yield b"x" + version.version_id.encode()

        said, folder = self.run_fetch("download", Store())
        self.assertEqual('FETCH {"problem": null, "cases": [908, 1147]}', said.strip())
        self.assertEqual({"0908": ["v1", entries[1].sha256], "1147": ["v2", entries[2].sha256]}, json.loads((folder / "versions.json").read_text()))

    def test_the_fetch_script_refuses_a_gate_refusal_in_both_modes_before_any_read(self) -> None:
        self.refuse = True
        for mode in ("admit", "download"):
            with self.subTest(mode=mode):
                said, folder = self.run_fetch(mode, SimpleNamespace())
                self.assertEqual('FETCH {"problem": "gate_refused"}', said.strip())
                self.assertFalse((folder / "versions.json").exists())

    def test_the_container_copy_of_the_comparison_finds_what_the_runner_finds(self) -> None:
        folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_shared_"))
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        namespace = runpy.run_path(self.script(folder, "shared.py", "import hashlib, json, os, re, sys\n" + self.module.SHARED))
        left = {"tracks": '["k1"]\tA\n["k1"]\tB\n'}
        right = {"tracks": '["k1"]\tB\n["k1"]\tB\n'}
        self.assertEqual(self.module.compare_tables(left, right), namespace["compare_tables"](left, right))
        self.assertEqual("case_set", namespace["sample_problem"]("passed", self.KEYS[:2], self.DEVELOPMENT))

    def test_the_scripts_that_run_in_the_containers_compile(self) -> None:
        for name in ("FETCH", "LISTING", "ACCESS", "COMPARE", "PLANT", "KEPT_COUNT"):
            with self.subTest(name=name):
                compile(getattr(self.module, name), name, "exec")


if __name__ == "__main__":
    unittest.main()
