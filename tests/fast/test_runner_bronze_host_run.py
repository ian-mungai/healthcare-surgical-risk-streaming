"""The steps of the phase 5 host runner (``tests/e2e/bronze_host.py``) that change something: what the run made and may delete, what the cleanup proves, how the
private trunk is tagged, what is saved before a pass is printed, and the order of the stages.

The runner stays untracked, so each test skips when it is not on this machine. No host, no Docker and no AWS access are used: a stand-in for Nessie's API
(``tests/fast/nessie_stand_in.py``, with the delete call and the page forms of the real server) and a stand-in for the host's command line replace them.

Failure modes covered here:

- The cleanup deletes a branch or a folder that the run did not make: a branch of the run's name that was there before the job, a folder that existed, a
  branch that was replaced or moved after the run last confirmed it, or a container of another run that carries the same label.
- The run's token is taken while a container, folder, branch or tag already carries it, or a container of the run is not recorded at launch.
- The cleanup says a run left nothing behind when it could not look: a failed ``docker ps`` is taken for an empty list, a failed check of the folder is taken
  for a folder that is gone, a list of references that Nessie pages is read in its first page only (the parameter is ``page-token``), or a page that says there
  are more pages and gives no token is taken for the end.
- The containers are still running, or their state is unknown, while branches and the folder are removed, or a bucket check starts one after the last
  inventory.
- The private trunk is tagged without a check of the checked hash and of the four snapshot IDs (an extra Bronze table included), the default-branch rule of the
  shared tag step refuses it, a tag that existed before the run is adopted, the record is saved late (an interrupt leaves a tag without it), the retained tag is
  not checked against the saved record, or a pass is printed with no tag evidence in the run's folder.
- An interrupt right after Nessie accepted the tag post leaves a tag that no file of the run's folder names: the pending tag record is not saved before
  the post, is not kept on every outcome, is marked complete before the tag is verified, or a pass is printed while it is not complete.
- A launch whose SSH call timed out or failed is not pending, so both inventories read empty while its container is still starting and the cleanup deletes the
  branches and the folder under it; the wait for it has no fixed deadline, ends before the last attempt's deadline, or ends with a launch unresolved and the
  record still passes; an interrupt that cuts the stamp of the end of the launch's call leaves the old start time, so the wait ends at once; an SSH that cannot
  start in the final inventory, or an unreadable pending tag record, fails with the exception type only, naming no launch or record.
- A case file is read before the whole run's object versions are confirmed, or before the Spark workload's own access is proved.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import runpy
import shlex
import shutil
import tempfile
import unittest
from collections.abc import Collection, Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.ingestion.manifest import ManifestRef
from scripts.stack.nessie import NessieConflict, NessieError, Reference
from scripts.stack.tags import NotOnTrunk, TagConflict, tag_and_record
from tests.e2e import run_record
from tests.e2e.run_record import Record
from tests.fast.nessie_stand_in import HASH_ONE, HASH_TWO, METADATA, NAMESPACE_ENTRY, StandIn, table_entry
from tests.fast.runner_import import import_runner

HASH = "a" * 64
CHECKED = "c" * 64
REFERENCE = json.dumps({"bucket": "b", "key": "manifests/x/" + HASH + ".json", "sha256": HASH, "version_id": "v1"})
TABLES = ("numeric_observations", "waveform_seconds", "tracks", "string_events")
SNAPSHOTS = {f"bronze.{name}": 100 + number for number, name in enumerate(TABLES)}
SUFFIX = "ab12cd"
PREFIX = f"p5h_{SUFFIX}"
TRUNK = f"{PREFIX}_trunk"


def runner() -> ModuleType:
    return import_runner("bronze_host")


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
            wanted = parse_qs(urlsplit(target).query).get("type", [""])[0]
            if wanted and self.references[name][0] != wanted:
                return self.answer(400, {"errorCode": "BAD_REQUEST", "message": "wrong kind"})
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
        self.containers: dict[str, str] = {}
        self.launches: list[str] = []
        self.hang = False
        self.timeouts = 0
        self.start_after = 0
        self.starting: list[list[Any]] = []
        self.folders: set[str] = set()
        self.files: dict[str, str] = {}
        self.replies: dict[str, str] = {}
        self.ps_code = 0
        self.ps_codes: list[int] = []
        self.rm_code = 0
        self.presence_code = 0
        self.remove_code = 0
        self.mkdir_code = 0

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 180.0) -> tuple[int, str, str]:
        self.calls.append(list(args))
        self.events.append("target " + " ".join(args[:3]))
        self.advance()
        head = args[0]
        if head == "docker" and "run" in args and "--name" in args:
            name = args[args.index("--name") + 1]
            self.launches.append(name)
            if self.timeouts:
                self.timeouts -= 1
                self.starting.append([name, self.start_after])
                return 255, "", "TimeoutExpired"
            if self.hang:
                self.containers[f"{len(self.launches):012x}"] = name
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
        if args[:3] == ["docker", "ps", "-a"]:
            self.ps_code = self.ps_codes.pop(0) if self.ps_codes else self.ps_code
            listing = "\n".join(f"{identifier} {name}" for identifier, name in self.containers.items())
            return (self.ps_code, listing, "") if self.ps_code == 0 else (self.ps_code, "", "ssh: timeout")
        if args[:3] == ["docker", "rm", "-f"]:
            if self.rm_code == 0:
                self.containers = {identifier: name for identifier, name in self.containers.items() if identifier not in args}
            return self.rm_code, "", ""
        if head == "docker" and args[1] == "run":
            for script, reply in self.replies.items():
                if script in args:
                    return 0, reply, ""
            return 0, 'LISTING {"digest": "' + "d" * 64 + '", "listed": true}', ""
        if head == "cat":
            return 0, self.files.get(args[-1], ""), ""
        return 0, "", ""

    def advance(self) -> None:
        """A launch whose call timed out starts its container some commands later, as a slow host does."""
        for item in list(self.starting):
            item[1] -= 1
            if item[1] <= 0:
                self.starting.remove(item)
                self.containers[f"{len(self.launches) + 100 + len(self.containers):012x}"] = item[0]
                self.events.append("container started late")

    def finish_starting(self) -> None:
        """The host goes on after the run ended: every container that was still starting is there now."""
        for item in list(self.starting):
            item[1] = 1
        self.advance()

    def write_text(self, path: str, text: str) -> None:
        self.run(["tee", path], stdin=text)


class FakeClock:
    """The clock and the sleep of the stack's wait: a sleep only moves the clock, so no test waits."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


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
        self.time = FakeClock()
        stack.clock, stack.sleep = self.time.clock, self.time.sleep
        self.run_folder = Path(tempfile.mkdtemp(prefix="bronze_host_test_run_"))
        self.addCleanup(shutil.rmtree, self.run_folder, ignore_errors=True)
        self.host = self.module.HostRun(stack, self.nessie, record, ManifestRef.from_json(REFERENCE), None, {}, self.run_folder)
        self.host.main_before = HASH_TWO
        self.host.main_entries_before = ["catalog_proof", "catalog_proof.numbers"]
        self.host.listing_before = "d" * 64

    def made_folder(self) -> None:
        self.assertEqual("", self.host.stack.reserve())

    def launched(self, name: str | None = None, *, identifier: str = "0123456789ab") -> str:
        """A container that this run launched and that is still there (a job that hung): its name is recorded in the stack, and the host lists it."""
        name = name or self.host.stack.launch_name()
        if name not in self.host.stack.launched:
            self.host.stack.launched.append(name)
        self.target.containers[identifier] = name
        return name

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

    def test_the_folder_is_made_without_parents_and_an_existing_one_is_refused_before_any_make(self) -> None:
        self.target.folders.add(self.host.stack.folder)
        self.assertEqual("folder_exists", self.host.stack.reserve())
        self.assertFalse(self.host.stack.folder_made)
        self.assertFalse([call for call in self.target.calls if call[0] == "mkdir"])

    def test_the_folder_of_a_free_token_is_made_without_parents(self) -> None:
        self.assertEqual("", self.host.stack.reserve())
        made = [call for call in self.target.calls if call[0] == "mkdir"]
        self.assertTrue(made)
        self.assertTrue(all("-p" not in call for call in made))

    def test_a_folder_that_appears_after_the_check_is_refused_by_the_make_and_is_not_the_runs(self) -> None:
        original = self.target.run

        def racing(args: list[str], **options: Any) -> tuple[int, str, str]:
            if args[0] == "mkdir":
                self.target.folders.add(args[-1])
            return original(args, **options)

        self.target.run = racing  # type: ignore[method-assign]
        self.assertEqual("folder_not_made", self.host.stack.reserve())
        self.assertFalse(self.host.stack.folder_made)

    def test_a_folder_that_the_run_did_not_make_is_not_removed(self) -> None:
        self.target.folders.add(self.host.stack.folder)
        self.host.stack.reserve()
        self.module.remove_everything(self.host)
        self.assertIn(self.host.stack.folder, self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "rm"])

    def test_a_failed_make_is_not_a_folder_of_the_run(self) -> None:
        self.target.mkdir_code = 255
        self.assertEqual("folder_not_made", self.host.stack.reserve())
        self.assertFalse(self.host.stack.folder_made)


class CleanupProof(Hosted):
    def setUp(self) -> None:
        super().setUp()
        self.made_folder()
        self.host.branch("trunk", "main")
        self.host.branch("b", "main")
        self.launched()

    def test_everything_that_the_run_made_is_removed_and_proved(self) -> None:
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual([], [name for name in self.references() if name.startswith(PREFIX)])
        self.assertEqual({}, self.target.containers)
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

    def test_a_cleanup_step_that_stops_does_not_skip_the_proofs_after_it(self) -> None:
        self.target.ps_code = 255
        _, text = self.module.remove_everything(self.host)
        self.assertIn("main unchanged", text)
        self.assertIn("services healthy", text)

    def test_a_run_tag_that_is_required_and_missing_is_a_failure(self) -> None:
        (self.run_folder / "data_version.json").write_text(json.dumps({"run_id": PREFIX, "tag": self.host.tag, "hash": CHECKED}))
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn(self.host.tag, text)


class Reservation(Hosted):
    """The run's token is reserved exclusively before anything starts, and the run removes only what it recorded."""

    def test_a_container_that_carries_the_token_refuses_the_reservation_and_nothing_is_made(self) -> None:
        self.target.containers["aaaaaaaaaaaa"] = "someone_else"
        self.assertEqual("containers_in_use", self.host.reserve())
        self.assertEqual(set(), self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "mkdir"])

    def test_a_failed_container_listing_refuses_the_reservation(self) -> None:
        self.target.ps_code = 255
        self.assertEqual("containers_unknown", self.host.reserve())
        self.assertFalse([call for call in self.target.calls if call[0] == "mkdir"])

    def test_a_folder_that_exists_refuses_the_reservation(self) -> None:
        self.target.folders.add(self.host.stack.folder)
        self.assertEqual("folder_exists", self.host.reserve())
        self.assertFalse([call for call in self.target.calls if call[0] == "mkdir"])

    def test_a_folder_that_cannot_be_checked_refuses_the_reservation(self) -> None:
        self.target.presence_code = 255
        self.assertEqual("folder_unknown", self.host.reserve())

    def test_a_branch_or_tag_that_carries_the_token_refuses_the_reservation_before_the_folder(self) -> None:
        for name in (f"{PREFIX}_x", f"run-{PREFIX}"):
            with self.subTest(name=name):
                self.nessie.references[name] = ("BRANCH", HASH_ONE)
                self.assertEqual("references_in_use", self.host.reserve())
                self.assertEqual(set(), self.target.folders)
                del self.nessie.references[name]

    def test_the_folder_is_made_last_after_the_containers_the_folders_and_the_references_were_checked(self) -> None:
        self.assertEqual("", self.host.reserve())
        self.assertIn(self.host.stack.folder, self.target.folders)
        checked = next(number for number, event in enumerate(self.events) if event.startswith("target docker ps"))
        listed = next(number for number, event in enumerate(self.events) if event == "nessie GET")
        made = next(number for number, event in enumerate(self.events) if event.startswith("target mkdir"))
        self.assertLess(listed, made)
        self.assertLess(checked, made)

    def test_a_run_refused_for_its_folder_removes_no_container_of_another_run(self) -> None:
        self.target.containers["bbbbbbbbbbbb"] = "other_runs_job"
        self.target.folders.add(self.host.stack.folder)
        self.assertEqual("containers_in_use", self.host.reserve())
        passed, text = self.module.remove_everything(self.host)
        self.assertIn("bbbbbbbbbbbb", self.target.containers)
        self.assertFalse([call for call in self.target.calls if call[:3] == ["docker", "rm", "-f"]])
        self.assertIn(self.host.stack.folder, self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "tee" or call[:2] == ["docker", "run"]])
        self.assertFalse(passed)
        self.assertIn("other_runs_job", text)

    def test_only_a_container_that_the_run_launched_is_removed(self) -> None:
        self.made_folder()
        mine = self.launched(identifier="aaaaaaaaaaaa")
        self.target.containers["bbbbbbbbbbbb"] = "other_runs_job"
        passed, text = self.module.remove_everything(self.host)
        self.assertNotIn("aaaaaaaaaaaa", self.target.containers)
        self.assertEqual({"bbbbbbbbbbbb": "other_runs_job"}, self.target.containers)
        removed = [call for call in self.target.calls if call[:3] == ["docker", "rm", "-f"]]
        self.assertEqual([["docker", "rm", "-f", "aaaaaaaaaaaa"]], removed)
        self.assertFalse(passed)
        self.assertIn("other_runs_job", text)
        self.assertTrue(mine)

    def test_the_name_of_a_launch_is_recorded_before_the_command_is_sent_and_is_in_it(self) -> None:
        seen: list[tuple[list[str], list[str]]] = []
        original = self.target.run

        def spy(args: list[str], **options: Any) -> tuple[int, str, str]:
            if "--name" in args:
                seen.append((list(self.host.stack.launched), args))
            return original(args, **options)

        self.target.run = spy  # type: ignore[method-assign]
        self.host.stack.spark(["/out/x.py"])
        self.host.stack.replay(["/out/y.py"], replay_access=False)
        self.assertEqual(2, len(seen))
        for number, (recorded, args) in enumerate(seen, start=1):
            name = args[args.index("--name") + 1]
            self.assertEqual(number, len(recorded))
            self.assertEqual(name, recorded[-1])
            self.assertTrue(name.startswith(f"{PREFIX}_"))
        self.assertEqual(2, len(set(self.host.stack.launched)))


class CompareAndDelete(Hosted):
    """A branch is deleted only at the head that the run last confirmed, so a replacement of that name is never removed."""

    def setUp(self) -> None:
        super().setUp()
        self.made_folder()

    def deletes(self) -> list[str]:
        return [target for method, target in self.nessie.calls if method == "DELETE"]

    def test_a_branch_recreated_at_another_hash_is_kept_and_reported(self) -> None:
        name = self.host.branch("x", "main")
        self.nessie.references[name] = ("BRANCH", HASH_ONE)
        passed, text = self.module.remove_everything(self.host)
        self.assertEqual(("BRANCH", HASH_ONE), self.nessie.references[name])
        self.assertFalse(passed)
        self.assertIn(name, text)
        self.assertIn("moved", text)

    def test_the_delete_names_the_hash_that_the_run_last_confirmed(self) -> None:
        name = self.host.branch("x", "main")
        self.nessie.references[name] = ("BRANCH", HASH_ONE)
        self.module.remove_everything(self.host)
        self.assertTrue(any(f"{name}@{HASH_TWO}" in target for target in self.deletes()))
        self.assertFalse(any(f"{name}@{HASH_ONE}" in target for target in self.deletes()))

    def test_a_branch_that_the_run_moved_and_confirmed_is_deleted(self) -> None:
        name = self.host.branch("x", "main")
        self.nessie.references[name] = ("BRANCH", HASH_ONE)
        self.host.confirm(name)
        passed, text = self.module.remove_everything(self.host)
        self.assertNotIn(name, self.nessie.references)
        self.assertTrue(passed, text)

    def test_a_branch_that_a_job_made_is_confirmed_at_the_hash_that_nessie_holds(self) -> None:
        name = self.host.fresh("a")
        self.nessie.references[name] = ("BRANCH", HASH_ONE)
        self.assertTrue(self.host.adopt(name, HASH_ONE))
        self.assertEqual(HASH_ONE, self.host.confirmed[name])

    def test_a_branch_that_the_run_did_not_make_cannot_be_confirmed(self) -> None:
        with self.assertRaises(ValueError):
            self.host.confirm("main")

    def test_a_head_other_than_the_expected_one_is_not_confirmed(self) -> None:
        name = self.host.branch("x", "main")
        with self.assertRaises(NessieError):
            self.host.confirm(name, HASH_ONE)
        self.assertEqual(HASH_TWO, self.host.confirmed[name])

    def test_a_tag_that_took_the_place_of_a_branch_is_not_deleted(self) -> None:
        name = self.host.branch("x", "main")
        self.nessie.references[name] = ("TAG", HASH_TWO)
        passed, _ = self.module.remove_everything(self.host)
        self.assertEqual(("TAG", HASH_TWO), self.nessie.references[name])
        self.assertFalse(passed)

    def test_a_branch_that_is_already_gone_is_not_a_failure(self) -> None:
        name = self.host.branch("x", "main")
        del self.nessie.references[name]
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)

    def moving_spark(self, moved: Collection[str]) -> Any:
        """A stand-in for the Spark container: the plant program and each publish move the branches that they write to, as the real jobs do."""
        new = "7" * 64
        self.nessie.history.add(new)
        self.nessie.entries_at[new] = [NAMESPACE_ENTRY]

        def spark(arguments: list[str], **options: Any) -> tuple[int, str, str, float]:
            if arguments[0] == "/out/plant.py":
                for name in json.loads(arguments[1]):
                    self.nessie.references[name] = ("BRANCH", new)
                return 0, "PLANT []", "", 0.0
            if arguments[0] == "publish":
                base = arguments[arguments.index("--base") + 1]
                if base in moved:
                    self.nessie.references[base] = ("BRANCH", new)
            if arguments[0] == "/out/helper.py":
                self.nessie.references[arguments[arguments.index("--branch") + 1]] = ("BRANCH", new)
            return 0, "", "", 0.0

        return spark

    def test_every_branch_that_the_planted_rows_move_is_confirmed_and_deleted(self) -> None:
        self.host.trunk = self.host.branch("trunk", "main")
        self.host.start_hash = HASH_TWO
        source = self.host.branch("a", "main")
        with mock.patch.object(self.host.stack, "spark", side_effect=self.moving_spark({f"{PREFIX}_base_kept", *(f"{PREFIX}_base_p{n}" for n in range(7))})):
            self.module.plant_rows(self.host, source, HASH_TWO)
        self.assertGreater(len(self.host.made), 8)
        passed, text = self.module.remove_everything(self.host)
        self.assertEqual([], [name for name in self.nessie.references if name.startswith(PREFIX)], text)
        self.assertTrue(passed, text)

    def test_the_control_and_the_second_run_confirm_the_heads_that_their_jobs_move(self) -> None:
        """The decode stage on stand-ins of the jobs: the first publishes to the trunk, the second makes a branch and publishes to a base."""
        from spark.bronze.checks import plan

        self.nessie.add_trunk()
        self.nessie.references[TRUNK] = ("BRANCH", HASH_TWO)
        self.host.take(TRUNK, HASH_TWO)
        self.host.trunk, self.host.start_hash = TRUNK, HASH_TWO
        self.host.cases = [908, 1147]
        made_by_job = "7" * 64
        self.nessie.history.add(made_by_job)
        self.nessie.entries_at[made_by_job] = [NAMESPACE_ENTRY]
        tables = {name: {"snapshot_id": SNAPSHOTS[f"bronze.{name}"], "rows": 1, "checksum": "x"} for name in TABLES}

        def decode_job(arguments: list[str], environment: dict[str, str] | None = None) -> tuple[int, dict[str, Any], str, float]:
            branch, base = (arguments[arguments.index(flag) + 1] for flag in ("--branch", "--base"))
            self.nessie.references[branch] = ("BRANCH", made_by_job)
            self.nessie.references[base] = ("BRANCH", CHECKED)
            asked = int(arguments[arguments.index("--parallelism") + 1])
            result = {
                "passed": True,
                "checked_hash": CHECKED,
                "branch_hash": made_by_job,
                "publication": {"status": "published", "published_hash": CHECKED, "snapshots": SNAPSHOTS},
                "checks": {"failed": [], "checks": [0] * len(plan())},
                "tables": tables,
                "parallelism": asked,
            }
            return 0, result, "", 0.0

        moved = {f"{PREFIX}_base_b", f"{PREFIX}_base_kept", *(f"{PREFIX}_base_p{n}" for n in range(7))}
        with (
            mock.patch.object(self.host.stack, "decode_job", side_effect=decode_job),
            mock.patch.object(self.host.stack, "spark", side_effect=self.moving_spark(moved)),
            mock.patch.object(self.host, "snapshot", side_effect=lambda reference, table: SNAPSHOTS[table]),
        ):
            self.module.stage_decode(self.host)
        self.assertEqual(f"run-{PREFIX}", self.host.notes["tag"])
        self.assertEqual(CHECKED, self.nessie.references[TRUNK][1])
        passed, text = self.module.remove_everything(self.host)
        self.assertEqual([], [name for name in self.nessie.references if name.startswith(PREFIX)], text)
        self.assertTrue(passed, text)

    def test_a_planted_clinical_branch_that_the_edit_moved_is_confirmed_and_deleted(self) -> None:
        source = self.host.branch("cl_a", "main")
        with mock.patch.object(self.host.stack, "spark", side_effect=self.moving_spark(set())):
            planted, code = self.module.plant_clinical(self.host, "null_key_clinical", source, "delete from t")
        self.assertEqual((f"{PREFIX}_cl_null_key_clinical", 0), (planted, code))
        passed, text = self.module.remove_everything(self.host)
        self.assertEqual([], [name for name in self.nessie.references if name.startswith(PREFIX)], text)
        self.assertTrue(passed, text)


class CleanupOrder(Hosted):
    """Containers first and proved gone, after every step that starts one; branches and the folder only then."""

    def setUp(self) -> None:
        super().setUp()
        self.made_folder()
        self.host.branch("trunk", "main")
        self.launched()

    def kept(self) -> None:
        self.assertEqual([f"{PREFIX}_trunk"], [name for name in self.references() if name.startswith(PREFIX)])
        self.assertFalse([target for method, target in self.nessie.calls if method == "DELETE"])
        self.assertIn(self.host.stack.folder, self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "rm"])

    def test_the_bucket_check_runs_before_the_last_inventory_and_its_container_is_removed(self) -> None:
        self.target.hang = True
        passed, text = self.module.remove_everything(self.host)
        last_launch = max(number for number, event in enumerate(self.events) if event.startswith("target docker run"))
        last_inventory = max(number for number, event in enumerate(self.events) if event.startswith("target docker ps"))
        self.assertLess(last_launch, last_inventory)
        self.assertEqual({}, self.target.containers)
        self.assertTrue(passed, text)

    def test_a_container_started_after_the_last_inventory_is_removed_and_the_cleanup_fails(self) -> None:
        self.target.hang = True

        def late(run: Any) -> tuple[bool, str]:
            run.stack.replay(["/out/x.py"], replay_access=False)
            return True, "services healthy: True"

        with mock.patch.object(self.module, "prove_services", late):
            passed, text = self.module.remove_everything(self.host)
        self.assertEqual({}, self.target.containers)
        self.assertFalse(passed)
        self.assertIn("after the last inventory", text)

    def test_a_late_container_keeps_the_branches_and_the_folder_when_it_cannot_be_removed(self) -> None:
        self.target.hang = True

        def late(run: Any) -> tuple[bool, str]:
            run.stack.replay(["/out/x.py"], replay_access=False)
            self.target.rm_code = 1
            return True, "services healthy: True"

        with mock.patch.object(self.module, "prove_services", late):
            passed, _ = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertTrue(self.target.containers)

    def test_an_unknown_container_state_keeps_the_branches_and_the_folder_and_says_so(self) -> None:
        self.target.ps_code = 255
        passed, text = self.module.remove_everything(self.host)
        self.kept()
        self.assertFalse(passed)
        self.assertIn(f"{PREFIX}_trunk", text)
        self.assertIn(self.host.stack.folder, text)

    def test_a_container_that_cannot_be_removed_keeps_the_branches_and_the_folder(self) -> None:
        self.target.rm_code = 1
        passed, text = self.module.remove_everything(self.host)
        self.kept()
        self.assertFalse(passed)
        self.assertIn(self.host.stack.folder, text)

    def test_a_failed_listing_after_the_removal_keeps_the_branches_and_the_folder(self) -> None:
        self.target.ps_codes = [0, 255]
        passed, text = self.module.remove_everything(self.host)
        self.kept()
        self.assertFalse(passed)
        self.assertIn("unknown", text)

    def test_a_container_of_another_run_with_the_label_fails_the_cleanup_and_is_left(self) -> None:
        self.target.containers["bbbbbbbbbbbb"] = "other_runs_job"
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("other_runs_job", text)
        self.assertEqual({"bbbbbbbbbbbb": "other_runs_job"}, self.target.containers)
        self.assertEqual([], [name for name in self.references() if name.startswith(PREFIX)])


class PendingLaunch(Hosted):
    """A launch whose SSH call timed out or failed is pending: the container may still be starting, so the cleanup waits, looks again by the run's label and
    removes it before the branches and the folder go."""

    def setUp(self) -> None:
        super().setUp()
        self.made_folder()
        self.host.branch("trunk", "main")
        self.kept_names = [f"{PREFIX}_trunk"]

    def timed_out(self, *, start_after: int = 10**6) -> str:
        self.target.timeouts, self.target.start_after = 1, start_after
        code = self.host.stack.replay(["/out/x.py"], replay_access=False)[0]
        self.assertEqual(255, code)
        return str(self.host.stack.launched[-1])

    def assert_kept(self, text: str) -> None:
        self.assertEqual(self.kept_names, [name for name in self.references() if name.startswith(PREFIX)])
        self.assertFalse([target for method, target in self.nessie.calls if method == "DELETE"])
        self.assertIn(self.host.stack.folder, self.target.folders)
        self.assertFalse([call for call in self.target.calls if call[0] == "rm"])
        self.assertIn(self.host.stack.folder, text)
        self.assertIn(f"{PREFIX}_trunk", text)

    def test_a_container_that_starts_after_both_inventories_is_removed_before_the_branches_and_the_folder(self) -> None:
        self.timed_out(start_after=8)
        passed, text = self.module.remove_everything(self.host)
        self.assertIn("container started late", self.events, "the cleanup ended before the container started: it did not wait for the pending launch")
        started = self.events.index("container started late")
        removed = next(number for number, event in enumerate(self.events) if event.startswith("target docker rm") and number > started)
        deleted = next(number for number, event in enumerate(self.events) if event == "nessie DELETE")
        folder = next(number for number, event in enumerate(self.events) if event.startswith("target rm"))
        self.assertLess(removed, deleted)
        self.assertLess(removed, folder)
        self.target.finish_starting()
        self.assertEqual({}, self.target.containers)
        self.assertTrue(passed, text)
        self.assertEqual({}, self.host.stack.pending)
        self.assertEqual([], [name for name in self.references() if name.startswith(PREFIX)])
        self.assertNotIn(self.host.stack.folder, self.target.folders)

    def test_the_wait_ends_at_a_fixed_deadline_after_the_last_launch_attempt(self) -> None:
        self.timed_out()
        self.time.now += 50
        last = self.timed_out()
        attempted = self.time.now
        with mock.patch.object(self.module, "PENDING_POLL_SECONDS", 7):
            passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual(attempted + self.module.PENDING_SETTLE_SECONDS, self.time.now)
        self.assertNotIn(last, self.host.stack.pending)

    def test_a_pending_launch_that_cannot_be_resolved_keeps_the_branches_and_the_folder_and_names_them(self) -> None:
        name = self.timed_out()
        self.target.ps_code = 255
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assert_kept(text)
        self.assertIn(name, text)
        self.assertIn(name, self.host.stack.pending)

    def test_a_late_container_that_cannot_be_removed_keeps_the_branches_and_the_folder_and_names_them(self) -> None:
        name = self.timed_out(start_after=2)
        self.target.rm_code = 1
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assert_kept(text)
        self.assertIn(name, text)

    def test_a_late_container_that_is_seen_and_removed_ends_the_wait_early(self) -> None:
        self.timed_out(start_after=1)
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual({}, self.target.containers)
        self.assertLess(sum(self.time.slept), self.module.PENDING_SETTLE_SECONDS)

    def test_a_launch_that_exits_by_itself_is_not_pending_and_the_cleanup_does_not_wait(self) -> None:
        original = self.target.run
        failed: list[str] = []

        def failing(args: list[str], **options: Any) -> tuple[int, str, str]:
            code, out, err = original(args, **options)
            if "--name" in args and not failed:
                failed.append(args[args.index("--name") + 1])
                return 1, out, err
            return code, out, err

        self.target.run = failing  # type: ignore[method-assign]
        self.assertEqual(1, self.host.stack.replay(["/out/x.py"], replay_access=False)[0])
        self.assertEqual({}, self.host.stack.pending)
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual([], self.time.slept)

    def test_a_launch_that_an_interrupt_cut_short_is_pending(self) -> None:
        def interrupted(args: list[str], **options: Any) -> tuple[int, str, str]:
            raise KeyboardInterrupt

        self.target.run = interrupted  # type: ignore[method-assign]
        with self.assertRaises(KeyboardInterrupt):
            self.host.stack.replay(["/out/x.py"], replay_access=False)
        self.assertEqual([self.host.stack.launched[-1]], list(self.host.stack.pending))

    def test_a_launch_is_pending_before_its_command_is_sent_and_a_confirmed_exit_clears_it(self) -> None:
        original = self.target.run
        seen: list[bool] = []

        def watching(args: list[str], **options: Any) -> tuple[int, str, str]:
            if "--name" in args and "run" in args:
                seen.append(args[args.index("--name") + 1] in self.host.stack.pending)
            return original(args, **options)

        self.target.run = watching  # type: ignore[method-assign]
        self.assertEqual(0, self.host.stack.replay(["/out/x.py"], replay_access=False)[0])
        self.assertEqual([True], seen, "the launch was not registered as pending before the command was sent")
        self.assertEqual({}, self.host.stack.pending, "a confirmed exit did not clear the pending launch")

    def test_an_interrupt_right_after_the_ssh_call_returned_255_leaves_the_launch_pending(self) -> None:
        original = self.target.run
        returned: list[bool] = []

        def after_return(args: list[str], **options: Any) -> tuple[int, str, str]:
            result = original(args, **options)
            if "--name" in args and "run" in args:
                returned.append(True)
            return result

        def interrupted() -> float:
            if returned:
                raise KeyboardInterrupt
            return self.time.clock()

        self.target.timeouts, self.target.start_after = 1, 10**6
        self.target.run = after_return  # type: ignore[method-assign]
        self.host.stack.clock = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.host.stack.replay(["/out/x.py"], replay_access=False)
        self.assertEqual([self.host.stack.launched[-1]], list(self.host.stack.pending), "an interrupt after the 255 left no pending launch")

    def interrupted_at_the_end_stamp(self, *, start_after: int) -> str:
        """A job that ran 2400 seconds and whose SSH call timed out (255); one interrupt then hits the clock read that stamps the end of the call."""
        original = self.target.run
        returned: list[bool] = []

        def slow(args: list[str], **options: Any) -> tuple[int, str, str]:
            if "--name" in args and "run" in args and not returned:
                self.time.now += 2400
                result = original(args, **options)
                returned.append(True)
                return result
            return original(args, **options)

        def interrupted() -> float:
            if returned and not self.stamp_works:
                raise KeyboardInterrupt
            return self.time.clock()

        self.stamp_works = False
        self.target.timeouts, self.target.start_after = 1, start_after
        self.target.run = slow  # type: ignore[method-assign]
        self.host.stack.clock = interrupted
        with self.assertRaises(KeyboardInterrupt):
            self.host.stack.replay(["/out/x.py"], replay_access=False)
        self.stamp_works = True
        return str(self.host.stack.launched[-1])

    def test_an_interrupt_at_the_end_stamp_of_a_launch_still_gets_a_full_settlement_window_from_the_start_of_the_cleanup(self) -> None:
        name = self.interrupted_at_the_end_stamp(start_after=8)
        self.assertEqual([name], list(self.host.stack.pending))
        passed, text = self.module.remove_everything(self.host)
        self.assertIn("container started late", self.events, "the cleanup did not wait: the unknown end of the call was taken as the start of the call")
        started = self.events.index("container started late")
        deleted = next(number for number, event in enumerate(self.events) if event == "nessie DELETE")
        self.assertTrue(
            any(event.startswith("target docker rm") and number > started and number < deleted for number, event in enumerate(self.events)),
            "the late container was not removed before the branches were deleted",
        )
        self.target.finish_starting()
        self.assertEqual({}, self.target.containers)
        self.assertTrue(passed, text)

    def test_a_launch_with_no_end_stamp_is_waited_for_until_the_start_of_the_cleanup_plus_the_settlement_time(self) -> None:
        self.interrupted_at_the_end_stamp(start_after=10**6)
        cleanup_started = self.time.now
        with mock.patch.object(self.module, "PENDING_POLL_SECONDS", 7):
            passed, text = self.module.remove_everything(self.host)
        self.assertEqual(cleanup_started + self.module.PENDING_SETTLE_SECONDS, self.time.now)
        self.assertTrue(passed, text)

    def test_a_launch_with_an_end_stamp_keeps_its_deadline_from_that_stamp_while_another_has_none(self) -> None:
        self.timed_out()
        stamped = self.time.now
        self.interrupted_at_the_end_stamp(start_after=10**6)
        cleanup_started = self.time.now
        self.assertGreater(cleanup_started, stamped + self.module.PENDING_SETTLE_SECONDS)
        self.module.remove_everything(self.host)
        self.assertEqual(cleanup_started + self.module.PENDING_SETTLE_SECONDS, self.time.now)

    def past_the_deadline_inventory_fails(self, *, calls_that_work: int) -> tuple[bool, str, str]:
        """A pending launch that never appears; each ``docker ps`` after the settlement deadline, beyond ``calls_that_work`` of them, cannot start ``ssh``."""
        name = self.timed_out()
        deadline = max(self.host.stack.ended.values()) + self.module.PENDING_SETTLE_SECONDS
        original = self.target.run
        late: list[int] = []

        def broken(args: list[str], **options: Any) -> tuple[int, str, str]:
            if args[:3] == ["docker", "ps", "-a"] and self.time.now >= deadline:
                late.append(1)
                if len(late) > calls_that_work:
                    raise FileNotFoundError(2, "No such file or directory", "ssh")
            return original(args, **options)

        self.target.run = broken  # type: ignore[method-assign]
        passed, text = self.module.remove_everything(self.host)
        return passed, text, name

    def test_an_ssh_that_cannot_start_in_the_final_inventory_keeps_everything_and_names_the_launch(self) -> None:
        passed, text, name = self.past_the_deadline_inventory_fails(calls_that_work=0)
        self.assertFalse(passed)
        self.assert_kept(text)
        self.assertIn(name, text)
        self.assertNotIn("stopped by", text, "the error was left to the generic step handler, which names no launch")
        self.assertIn(name, self.host.stack.pending)

    def test_an_ssh_that_cannot_start_in_the_inventory_after_the_removal_keeps_everything_and_names_the_launch(self) -> None:
        passed, text, name = self.past_the_deadline_inventory_fails(calls_that_work=1)
        self.assertFalse(passed)
        self.assert_kept(text)
        self.assertIn(name, text)
        self.assertNotIn("stopped by", text, "the error was left to the generic step handler, which names no launch")
        self.assertIn(name, self.host.stack.pending)

    def test_the_time_of_a_pending_launch_is_the_end_of_its_call_not_its_start(self) -> None:
        original = self.target.run

        def slow(args: list[str], **options: Any) -> tuple[int, str, str]:
            if "--name" in args and "run" in args:
                self.time.now += 2400
            return original(args, **options)

        self.target.timeouts, self.target.start_after = 1, 10**6
        self.target.run = slow  # type: ignore[method-assign]
        self.host.stack.replay(["/out/x.py"], replay_access=False)
        self.assertEqual({self.host.stack.launched[-1]: self.time.now}, self.host.stack.ended)

    def test_every_call_of_the_settlement_is_capped_by_the_time_that_remains(self) -> None:
        self.timed_out(start_after=3)
        original = self.target.run
        deadline = max(self.host.stack.ended.values()) + self.module.PENDING_SETTLE_SECONDS
        calls: list[tuple[str, float, float]] = []

        def timed(args: list[str], **options: Any) -> tuple[int, str, str]:
            calls.append((" ".join(args[:3]), options.get("timeout", 180.0), deadline - self.time.now))
            return original(args, **options)

        self.target.run = timed  # type: ignore[method-assign]
        self.module.settle_pending(self.host)
        self.assertTrue([call for call in calls if call[0].startswith("docker ps")])
        self.assertTrue([call for call in calls if call[0].startswith("docker rm")], "no removal was made, so the cap of a removal is not tested")
        for call, timeout, remaining in calls:
            self.assertGreater(remaining, 0, f"{call} was sent after the settlement time ended")
            self.assertLessEqual(timeout, remaining, f"{call} may run for {timeout} seconds with {remaining} seconds left")

    def test_the_last_poll_of_the_settlement_does_not_run_past_the_deadline(self) -> None:
        self.timed_out()
        original = self.target.run
        deadline = max(self.host.stack.ended.values()) + self.module.PENDING_SETTLE_SECONDS
        ends: list[float] = []

        def timed(args: list[str], **options: Any) -> tuple[int, str, str]:
            ends.append(self.time.now + options.get("timeout", 180.0))
            return original(args, **options)

        self.target.run = timed  # type: ignore[method-assign]
        with mock.patch.object(self.module, "PENDING_POLL_SECONDS", 7):
            self.module.settle_pending(self.host)
        self.assertLessEqual(max(ends), deadline)

    def test_a_final_inventory_that_cannot_confirm_the_cleanup_keeps_everything_and_names_it(self) -> None:
        name = self.timed_out(start_after=1)
        self.target.ps_codes = [0, 0, 255, 255, 255]
        self.target.ps_code = 255
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assert_kept(text)
        self.assertIn(name, text)
        self.assertIn(name, self.host.stack.pending)

    def test_a_timed_out_spark_launch_is_recorded_under_its_container_name_not_an_environment_key(self) -> None:
        self.target.timeouts, self.target.start_after = 1, 10**6
        code = self.host.stack.spark(["/out/x.py"], environment={"JOB_DEADLINE_SECONDS": "2100", "OTHER": "1"})[0]
        self.assertEqual(255, code)
        real = self.host.stack.launched[0]
        self.assertEqual(1, len(self.host.stack.launched))
        self.assertTrue(real.endswith("_c1"), real)
        self.assertEqual([real], list(self.host.stack.pending))
        self.assertEqual([real], self.target.launches)

    def test_a_timed_out_spark_launch_that_starts_late_is_found_and_removed_by_its_name(self) -> None:
        self.target.timeouts, self.target.start_after = 1, 8
        self.assertEqual(255, self.host.stack.spark(["/out/x.py"], environment={"JOB_DEADLINE_SECONDS": "2100"})[0])
        passed, text = self.module.remove_everything(self.host)
        self.assertIn("container started late", self.events)
        self.assertTrue(passed, text)
        self.assertEqual({}, self.target.containers)
        self.assertEqual({}, self.host.stack.pending)
        self.assertLess(sum(self.time.slept), self.module.PENDING_SETTLE_SECONDS, "the wait did not end when the launch's own container was seen and removed")


class AcceptedLimits(unittest.TestCase):
    """The limits that the owner accepted are in the runner's docstring and in every record that the runner writes."""

    def setUp(self) -> None:
        self.module = runner()

    def recorded(self) -> list[str]:
        scratch = Path(tempfile.mkdtemp(prefix="bronze_host_test_limits_"))
        self.addCleanup(shutil.rmtree, scratch, ignore_errors=True)
        reference = scratch / "reference.json"
        reference.write_text(REFERENCE)
        seen: list[Record] = []

        def finish(record: Record, *_: Any, **__: Any) -> int:
            seen.append(record)
            return 0

        with (
            mock.patch("sys.argv", ["bronze_host", "--manifest-reference", str(reference)]),
            mock.patch.object(run_record, "ROOT", scratch),
            mock.patch.object(self.module, "run_host", return_value=""),
            mock.patch.object(self.module, "finish", side_effect=finish),
        ):
            self.assertEqual(0, self.module.main())
        return list(seen[0].limits)

    def test_the_record_names_the_two_accepted_limits_and_the_wait_of_the_pending_launches(self) -> None:
        limits = " ".join(self.recorded())
        self.assertIn("replaced by another actor during a failed clinical edit", limits)
        self.assertIn("reuses the same token after the first run released it", limits)
        self.assertIn("delayed container", limits)
        self.assertIn(f"{self.module.PENDING_SETTLE_SECONDS} seconds", limits)

    def test_the_docstring_names_the_accepted_limits_and_the_pending_launches(self) -> None:
        text = " ".join((self.module.__doc__ or "").split())
        self.assertIn("replaced by another actor during a failed clinical edit", text)
        self.assertIn("reuses the same token after the first run released it", text)
        self.assertIn("pending", text)
        self.assertIn(self.module.PENDING_NAME, text)


class TagProof(Hosted):
    """The cleanup checks the retained tag against the record that the run saved."""

    def setUp(self) -> None:
        super().setUp()
        self.made_folder()
        self.nessie.add_trunk()
        self.host.take(TRUNK, CHECKED)
        self.host.trunk = TRUNK
        self.tag = f"run-{PREFIX}"
        self.module.tag_private_trunk(self.nessie, PREFIX, TRUNK, CHECKED, SNAPSHOTS, self.run_folder)

    def test_a_tag_at_the_recorded_hash_is_proved(self) -> None:
        passed, text = self.module.prove_tag(self.host)
        self.assertTrue(passed, text)

    def test_a_tag_at_another_hash_than_the_record_is_a_failure(self) -> None:
        self.nessie.references[self.tag] = ("TAG", HASH_ONE)
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn(self.tag, text)

    def test_a_branch_of_the_tags_name_is_a_failure(self) -> None:
        self.nessie.references[self.tag] = ("BRANCH", CHECKED)
        passed, _ = self.module.prove_tag(self.host)
        self.assertFalse(passed)

    def test_a_tag_with_no_saved_record_is_a_failure(self) -> None:
        (self.run_folder / "data_version.json").unlink()
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("no saved record", text)

    def test_a_tag_whose_pending_record_is_not_complete_is_a_failure_that_names_the_record(self) -> None:
        (self.run_folder / "data_version.json").unlink()
        pending = json.loads((self.run_folder / "tag_pending.json").read_text())
        (self.run_folder / "tag_pending.json").write_text(json.dumps({**pending, "state": "pending"}))
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)
        self.assertIn(self.tag, text)

    def test_a_tag_whose_pending_record_is_not_complete_is_a_failure_even_with_a_saved_record(self) -> None:
        pending = json.loads((self.run_folder / "tag_pending.json").read_text())
        (self.run_folder / "tag_pending.json").write_text(json.dumps({**pending, "state": "pending"}))
        self.assertTrue((self.run_folder / "data_version.json").exists())
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)
        self.assertIn(self.tag, text)

    def test_a_pending_record_that_cannot_be_read_is_a_failure_that_names_it_even_with_a_saved_record(self) -> None:
        (self.run_folder / "tag_pending.json").write_text("{not json")
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)

    def test_a_pending_record_that_is_a_folder_is_a_failure_that_names_it(self) -> None:
        (self.run_folder / "tag_pending.json").unlink()
        (self.run_folder / "tag_pending.json").mkdir()
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)

    def test_a_pending_record_that_cannot_be_opened_is_a_failure_that_names_it(self) -> None:
        original = Path.read_text

        def guarded(path: Path, *args: Any, **options: Any) -> str:
            if path.name == "tag_pending.json":
                raise PermissionError(13, "Permission denied")
            return original(path, *args, **options)

        with mock.patch.object(Path, "read_text", guarded):
            passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)

    def test_a_complete_pending_record_with_a_saved_record_is_proved(self) -> None:
        pending = json.loads((self.run_folder / "tag_pending.json").read_text())
        self.assertEqual("complete", pending["state"])
        passed, text = self.module.prove_tag(self.host)
        self.assertTrue(passed, text)

    def test_the_cleanup_fails_naming_an_incomplete_pending_record_even_with_a_saved_record(self) -> None:
        pending = json.loads((self.run_folder / "tag_pending.json").read_text())
        (self.run_folder / "tag_pending.json").write_text(json.dumps({**pending, "state": "pending"}))
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn("tag_pending.json", text)

    def test_a_saved_record_with_no_tag_is_a_failure(self) -> None:
        del self.nessie.references[self.tag]
        passed, text = self.module.prove_tag(self.host)
        self.assertFalse(passed)
        self.assertIn(self.tag, text)

    def test_a_record_of_another_tag_name_is_a_failure(self) -> None:
        record = json.loads((self.run_folder / "data_version.json").read_text())
        (self.run_folder / "data_version.json").write_text(json.dumps({**record, "tag": "run-other"}))
        passed, _ = self.module.prove_tag(self.host)
        self.assertFalse(passed)

    def test_a_record_of_another_run_is_a_failure(self) -> None:
        record = json.loads((self.run_folder / "data_version.json").read_text())
        (self.run_folder / "data_version.json").write_text(json.dumps({**record, "run_id": "p5h_other"}))
        passed, _ = self.module.prove_tag(self.host)
        self.assertFalse(passed)

    def test_no_record_and_no_tag_is_not_a_failure(self) -> None:
        (self.run_folder / "data_version.json").unlink()
        del self.nessie.references[self.tag]
        passed, text = self.module.prove_tag(self.host)
        self.assertTrue(passed, text)

    def test_the_cleanup_fails_when_the_tag_is_not_the_recorded_one_and_still_deletes_the_branches(self) -> None:
        self.nessie.references[self.tag] = ("TAG", HASH_ONE)
        passed, text = self.module.remove_everything(self.host)
        self.assertFalse(passed)
        self.assertIn(self.tag, text)
        self.assertNotIn(TRUNK, self.nessie.references)

    def test_the_cleanup_passes_with_the_recorded_tag_left_in_place(self) -> None:
        passed, text = self.module.remove_everything(self.host)
        self.assertTrue(passed, text)
        self.assertEqual(("TAG", CHECKED), self.nessie.references[self.tag])


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
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("tag_not_the_runs", caught.exception.code)
        self.assertEqual(("TAG", HASH_ONE), self.nessie.references[f"run-{PREFIX}"])
        self.assertEqual([], self.nessie.writes())

    def test_a_tag_at_the_same_hash_is_not_adopted_by_a_run_that_has_no_saved_record(self) -> None:
        self.nessie.references[f"run-{PREFIX}"] = ("TAG", CHECKED)
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("tag_not_the_runs", caught.exception.code)
        self.assertEqual([], self.nessie.writes())
        self.assertFalse((self.folder / "data_version.json").exists())

    def test_a_tag_that_appears_while_the_step_makes_it_is_not_adopted(self) -> None:
        original = self.nessie.create_reference

        def raced(kind: str, name: str, source: Any) -> Any:
            self.nessie.references[name] = (kind, source.hash)
            return original(kind, name, source)

        with mock.patch.object(self.nessie, "create_reference", side_effect=raced), self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("tag_not_the_runs", caught.exception.code)
        self.assertFalse((self.folder / "data_version.json").exists())

    def test_a_tag_that_the_saved_record_names_is_found_again_and_one_that_moved_is_refused(self) -> None:
        self.tag()
        self.nessie.references[f"run-{PREFIX}"] = ("TAG", HASH_ONE)
        with self.assertRaises(TagConflict):
            self.tag()
        self.assertEqual(("TAG", HASH_ONE), self.nessie.references[f"run-{PREFIX}"])

    def test_an_extra_bronze_table_on_the_trunk_is_refused_before_any_write(self) -> None:
        self.nessie.entries_at[CHECKED].append(table_entry(["bronze", "extra"], 7, METADATA))
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("bronze_tables", caught.exception.code)
        self.assertEqual([], self.nessie.writes())
        self.assertFalse((self.folder / "data_version.json").exists())

    def test_a_table_of_another_namespace_on_the_trunk_does_not_stop_the_tag(self) -> None:
        self.nessie.entries_at[CHECKED].append(table_entry(["raw_records", "clinical_data"], 8, METADATA))
        result, record = self.tag()
        self.assertEqual(f"run-{PREFIX}", result.tag)
        self.assertIn("raw_records.clinical_data", [item["table"] for item in record["tables"]])

    def test_the_record_is_saved_before_any_step_that_follows_the_tag(self) -> None:
        tag = f"run-{PREFIX}"
        original = self.nessie.reference

        def watched(name: str) -> Any:
            if name == tag and name in self.nessie.references:
                raise KeyboardInterrupt
            return original(name)

        with mock.patch.object(self.nessie, "reference", side_effect=watched), self.assertRaises(KeyboardInterrupt):
            self.tag()
        self.assertEqual(("TAG", CHECKED), self.nessie.references[tag])
        saved = json.loads((self.folder / "data_version.json").read_text())
        self.assertEqual((tag, CHECKED), (saved["tag"], saved["hash"]))
        self.assertEqual(SNAPSHOTS, {item["table"]: item["snapshot_id"] for item in saved["tables"]})
        self.assertEqual([], [item.name for item in self.folder.iterdir() if item.name.startswith(".writing_")])

    def pending_file(self) -> dict[str, Any]:
        return json.loads((self.folder / "tag_pending.json").read_text())  # type: ignore[no-any-return]

    def test_a_pending_record_is_saved_before_the_tag_is_posted(self) -> None:
        seen: list[dict[str, Any]] = []
        original = self.nessie.create_reference

        def watched(kind: str, name: str, source: Any) -> Any:
            if kind == "TAG":
                seen.append(self.pending_file() if (self.folder / "tag_pending.json").exists() else {})
            return original(kind, name, source)

        with mock.patch.object(self.nessie, "create_reference", side_effect=watched):
            self.tag()
        self.assertEqual(1, len(seen))
        self.assertEqual(("pending", f"run-{PREFIX}", CHECKED), (seen[0].get("state"), seen[0].get("tag"), seen[0].get("hash")))
        self.assertEqual(SNAPSHOTS, {item["table"]: item["snapshot_id"] for item in seen[0].get("tables", [])})

    def test_an_interrupt_right_after_the_post_is_accepted_keeps_a_pending_record_that_names_the_tag(self) -> None:
        tag = f"run-{PREFIX}"
        original = self.nessie.create_reference

        def accepted_then_interrupted(kind: str, name: str, source: Any) -> Any:
            original(kind, name, source)
            raise KeyboardInterrupt

        with mock.patch.object(self.nessie, "create_reference", side_effect=accepted_then_interrupted), self.assertRaises(KeyboardInterrupt):
            self.tag()
        self.assertEqual(("TAG", CHECKED), self.nessie.references[tag])
        held = self.pending_file()
        self.assertEqual(("pending", tag, CHECKED, PREFIX), (held["state"], held["tag"], held["hash"], held["run_id"]))
        self.assertEqual(SNAPSHOTS, {item["table"]: item["snapshot_id"] for item in held["tables"]})
        self.assertEqual([], [item.name for item in self.folder.iterdir() if item.name.startswith((".writing_", ".pending_"))])

    def test_the_pending_record_is_marked_complete_after_the_tag_is_verified(self) -> None:
        _, record = self.tag()
        held = self.pending_file()
        self.assertEqual("complete", held["state"])
        self.assertEqual(record, {key: value for key, value in held.items() if key != "state"})

    def test_a_failed_check_after_the_post_leaves_the_pending_record_not_complete(self) -> None:
        tag = f"run-{PREFIX}"
        original = self.nessie.reference

        def moved(name: str) -> Any:
            result = original(name)
            return Reference("TAG", name, HASH_ONE) if name == tag and result.kind == "TAG" else result

        with mock.patch.object(self.nessie, "reference", side_effect=moved), self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("tag_not_at_hash", caught.exception.code)
        self.assertEqual(("pending", tag, CHECKED), (self.pending_file()["state"], self.pending_file()["tag"], self.pending_file()["hash"]))

    def test_a_refusal_before_the_post_writes_no_pending_record(self) -> None:
        self.nessie.references[TRUNK] = ("BRANCH", HASH_TWO)
        with self.assertRaises(self.module.TagProblem):
            self.tag()
        self.assertFalse((self.folder / "tag_pending.json").exists())

    def test_a_pending_record_that_cannot_be_saved_stops_the_step_before_the_post(self) -> None:
        (self.folder / "tag_pending.json").mkdir()
        with self.assertRaises(self.module.TagProblem) as caught:
            self.tag()
        self.assertEqual("pending_not_saved", caught.exception.code)
        self.assertEqual([], self.nessie.writes())
        self.assertFalse((self.folder / "data_version.json").exists())
        self.assertEqual([], [item.name for item in self.folder.iterdir() if item.name.startswith((".writing_", ".pending_"))])

    def test_a_second_tag_step_leaves_the_pending_record_complete(self) -> None:
        self.tag()
        self.tag()
        self.assertEqual("complete", self.pending_file()["state"])

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

    def finish(self, blocked: str = "", *, saved: bool = True) -> tuple[int, Path]:
        """The run's folder is made first, as ``main`` does, and the tag step saves its record in it; ``finish`` fills the same folder."""
        folder = self.record.write(["log"])
        if saved:
            (folder / "data_version.json").write_text(json.dumps(self.version, indent=2) + "\n")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.module.finish(self.record, ["log"], self.notes, blocked, folder, self.script)
        self.assertEqual([folder], sorted((self.scratch / "runs").iterdir()))
        return code, folder

    def outcome(self, folder: Path) -> str:
        return str(json.loads((folder / "record.json").read_text())["outcome"])

    def test_a_pass_has_the_tag_evidence_saved_in_the_run_folder(self) -> None:
        code, folder = self.finish()
        self.assertEqual((0, "pass"), (code, self.outcome(folder)))
        self.assertEqual(self.version, json.loads((folder / "data_version.json").read_text()))
        self.assertEqual("pass", self.outcome(folder))
        self.assertEqual(CHECKED, json.loads((folder / "run_notes.json").read_text())["merged_hash"])
        self.assertTrue((folder / "bronze_host.py").is_file())

    def test_a_pending_tag_record_that_is_not_complete_is_not_a_pass(self) -> None:
        folder = self.record.write(["log"])
        (folder / "data_version.json").write_text(json.dumps(self.version, indent=2) + "\n")
        (folder / "tag_pending.json").write_text(json.dumps({**self.version, "state": "pending"}))
        with contextlib.redirect_stdout(io.StringIO()):
            code = self.module.finish(self.record, ["log"], self.notes, "", folder, self.script)
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))

    def test_a_complete_pending_tag_record_is_a_pass(self) -> None:
        folder = self.record.write(["log"])
        (folder / "data_version.json").write_text(json.dumps(self.version, indent=2) + "\n")
        (folder / "tag_pending.json").write_text(json.dumps({**self.version, "state": "complete"}))
        with contextlib.redirect_stdout(io.StringIO()):
            code = self.module.finish(self.record, ["log"], self.notes, "", folder, self.script)
        self.assertEqual((0, "pass"), (code, self.outcome(folder)))

    def test_a_record_that_the_tag_step_did_not_save_in_the_folder_is_not_a_pass_and_is_not_written_by_the_finish(self) -> None:
        code, folder = self.finish(saved=False)
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))
        self.assertFalse((folder / "data_version.json").exists())

    def test_a_saved_record_that_differs_from_the_verified_one_is_not_a_pass_and_is_kept_as_it_is(self) -> None:
        folder = self.record.write(["log"])
        other = {**self.version, "hash": HASH}
        (folder / "data_version.json").write_text(json.dumps(other))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.module.finish(self.record, ["log"], self.notes, "", folder, self.script)
        self.assertEqual((1, "fail"), (code, self.outcome(folder)))
        self.assertEqual(other, json.loads((folder / "data_version.json").read_text()))

    def test_a_run_that_published_but_has_no_tag_evidence_is_not_a_pass(self) -> None:
        del self.notes["data_version"]
        code, folder = self.finish(saved=False)
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
        code, folder = self.finish(blocked="no usable passed manifest", saved=False)
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
        yield from ("stage_reserve", "stage_stack", "stage_admission", "stage_access", "stage_download", "stage_trunk", "stage_decode", "stage_clinical")

    def run_with(self, results: dict[str, Any]) -> tuple[list[str], str]:
        order: list[str] = []
        patches = []
        for name in self.stage_names():

            def record(run: Any, name: str = name) -> Any:
                order.append(name)
                return results.get(name)

            patches.append(mock.patch.object(self.module, name, side_effect=record))
        patches.append(mock.patch.object(self.module, "record_end"))
        patches.append(mock.patch.object(self.module.proof, "Remote", return_value=self.target))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            blocked = self.module.run_host(self.host.record, [], self.host.reference, HASH, {}, self.run_folder)
        return order, blocked

    def test_the_stages_run_in_the_order_that_proves_each_access_before_the_next_read(self) -> None:
        order, blocked = self.run_with({"stage_reserve": True, "stage_stack": True, "stage_admission": "", "stage_access": True, "stage_download": ""})
        self.assertEqual("", blocked)
        self.assertEqual(
            ["stage_reserve", "stage_stack", "stage_admission", "stage_access", "stage_download", "stage_trunk", "stage_decode", "stage_clinical"], order
        )

    def test_nothing_else_runs_when_the_token_cannot_be_reserved(self) -> None:
        order, blocked = self.run_with({"stage_reserve": False})
        self.assertEqual("", blocked)
        self.assertEqual(["stage_reserve"], order)

    def test_the_reservation_is_recorded_as_a_failed_observation_with_its_code(self) -> None:
        self.target.containers["aaaaaaaaaaaa"] = "someone_else"
        self.assertFalse(self.module.stage_reserve(self.host))
        failed = [item for item in self.host.record.observations if not item.passed]
        self.assertEqual(1, len(failed))
        self.assertIn("containers_in_use", failed[0].observed)

    def test_a_reserved_token_is_recorded_as_a_passed_observation(self) -> None:
        self.assertTrue(self.module.stage_reserve(self.host))
        self.assertTrue(all(item.passed for item in self.host.record.observations))
        self.assertIn(self.host.stack.folder, self.target.folders)

    def test_the_spark_access_is_proved_before_any_case_file_is_downloaded(self) -> None:
        order, blocked = self.run_with({"stage_reserve": True, "stage_stack": True, "stage_admission": "", "stage_access": False})
        self.assertIn("cannot read the manifest", blocked)
        self.assertEqual(["stage_reserve", "stage_stack", "stage_admission", "stage_access"], order)

    def test_an_admission_refusal_stops_before_the_access_probe_and_the_download(self) -> None:
        order, blocked = self.run_with({"stage_reserve": True, "stage_stack": True, "stage_admission": "no usable passed manifest (case_set)"})
        self.assertIn("case_set", blocked)
        self.assertEqual(["stage_reserve", "stage_stack", "stage_admission"], order)

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


class Main(unittest.TestCase):
    """``main`` makes the run's folder under ``runs/`` before the host is touched, so that the tag step can save its record in it. ``finish`` fills it."""

    def test_the_runs_folder_exists_with_an_unsettled_record_before_the_host_is_touched_and_is_the_one_that_is_filled(self) -> None:
        module = runner()
        scratch = Path(tempfile.mkdtemp(prefix="bronze_host_test_main_"))
        self.addCleanup(shutil.rmtree, scratch, ignore_errors=True)
        reference = scratch / "reference.json"
        reference.write_text(REFERENCE)
        seen: dict[str, Any] = {}

        def host(record: Any, log: list[str], reference: Any, source_hash: Any, notes: dict[str, Any], folder: Path) -> str:
            seen["folder"] = folder
            seen["outcome"] = json.loads((folder / "record.json").read_text())["outcome"]
            return ""

        out = io.StringIO()
        with (
            mock.patch.object(run_record, "ROOT", scratch),
            mock.patch.object(module, "recorded_state", return_value=("rev", "tree")),
            mock.patch.object(module, "run_host", side_effect=host),
            mock.patch("sys.argv", ["bronze_host", "--manifest-reference", str(reference)]),
            contextlib.redirect_stdout(out),
        ):
            module.main()
        self.assertEqual([seen["folder"]], sorted((scratch / "runs").iterdir()))
        self.assertEqual("blocked", seen["outcome"])
        self.assertTrue((seen["folder"] / "run_notes.json").is_file())


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
