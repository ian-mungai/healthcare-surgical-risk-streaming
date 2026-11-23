"""The order of the table checks: the gate, the staged verified bytes, the load record, the Spark job, then the merge, and the saved result.

A stand-in Nessie keeps references and the snapshot IDs of tables at each commit, and a stand-in runner plays the Spark job by printing the result line it
is told to print. The real job and the real Nessie are in ``tests/e2e/table_checks.py``. Each refusal has a passing control, and each refusal proves that the
job did not run and that nothing was merged.
"""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import fcntl
import hashlib
import inspect
import json
import os
import stat
import tempfile
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.ingestion import table_checks as run
from scripts.ingestion.gate import GateRefused
from scripts.ingestion.manifest import ManifestRef, publish_manifest
from scripts.process import CompletedProcess, TimeoutExpired
from scripts.stack.groups import Settings
from scripts.stack.nessie import NessieClient, NessieError
from spark import table_checks as rules
from tests.fast.ingestion_support import Landed, landed_set, pin_the_synthetic_list

CLINICAL = b"caseid,age\r\n1,60\r\n2,\r\n"
LAB = b"caseid,dt,name,result\r\n1,5,alb,2.9\r\n1,5,alb,2.9\r\n"
SOURCE = {"vital_files/0001.vital": b"a recording", "clinical_data.csv": CLINICAL, "lab_data.csv": LAB}
SNAPSHOTS = {"clinical_data": 111, "lab_data": 222, "track_list": 333}
PIN = rules.TrackPin(7, "a" * 64)
BRANCH = "load_run_one"
ROOT = Path(__file__).resolve().parents[2]
MARKER = "MARKER_9d2"


def setUpModule() -> None:
    pin_the_synthetic_list(b"".join(f"{hashlib.sha256(data).hexdigest()} {path}\n".encode() for path, data in sorted(SOURCE.items())))


def fake_hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


BASE = fake_hash("main", "0")


class FakeNessie(NessieClient):
    """References and the tables of each commit, answering the calls of the checks in the forms of Nessie's API version 2."""

    def __init__(self) -> None:
        super().__init__("http://nessie:19120/api/v2")
        self.heads = {"main": fake_hash("main", "0")}
        self.tables: dict[tuple[str, str], dict[str, int]] = {("main", self.heads["main"]): {}}
        self.calls: list[tuple[str, str]] = []
        self.merges: list[tuple[str, str, str]] = []
        self.merge_messages: list[str] = []
        self.commits: list[tuple[str, str, str]] = []
        self.accept_stale_expected = False
        self.noop_when_merged = False
        self.reply_without_commit = False
        self.crash_before_merge = False
        self.reply_hash = ""
        self.lose_next_merge_reply = False
        self.on_branch_read: Callable[[], None] | None = None
        self.history_page_size = 100
        self.merge_requests: list[dict[str, Any]] = []
        self.pending_merge: dict[str, Any] | None = None
        self.defer_next_merge = False
        self.defer_main_commit = ""
        self.history_answer: tuple[int, bytes] | None = None
        self.history_reads: list[str] = []
        self.on_history_read: Callable[[], None] | None = None
        self.main_reads = 0
        self.other: dict[tuple[str, str], dict[tuple[str, ...], dict[str, Any]]] = {}
        self.hide_content = False

    def entry_list(self, name: str, hash_value: str) -> list[dict[str, Any]]:
        """The contents of a commit: the namespace of the tables when it holds any, each table, and every other content that a test put on it."""
        held = self.tables.get((name, hash_value), {})
        found: list[dict[str, Any]] = []
        if held:
            found.append({"name": {"elements": [rules.NAMESPACE]}, "type": "NAMESPACE", "content": {"type": "NAMESPACE", "elements": [rules.NAMESPACE]}})
        for table, snapshot in sorted(held.items()):
            content = {"type": "ICEBERG_TABLE", "snapshotId": snapshot, "metadataLocation": f"s3://warehouse/{table}/{snapshot}.json"}
            found.append({"name": {"elements": [rules.NAMESPACE, table]}, "type": "ICEBERG_TABLE", "content": content})
        for key, content in sorted(self.other.get((name, hash_value), {}).items()):
            found.append({"name": {"elements": list(key)}, "type": content["type"], "content": content})
        if self.hide_content:
            found = [{key: value for key, value in entry.items() if key != "content"} for entry in found]
        return found

    def load_branch(self, name: str, snapshots: dict[str, int]) -> str:
        self.heads[name] = fake_hash(name, "load")
        self.tables[(name, self.heads[name])] = dict(snapshots)
        return self.heads[name]

    def add_main_commit(self, message: str, tables: dict[str, int] | None = None) -> str:
        """A commit on main that is not a merge of this step, as another tool would write it."""
        parent = self.heads["main"]
        self.heads["main"] = fake_hash("main", "other", message, str(len(self.commits)))
        self.tables[("main", self.heads["main"])] = dict(tables or {})
        self.commits.append((self.heads["main"], message, parent))
        return self.heads["main"]

    def apply_pending(self) -> None:
        """The first request, which the client gave up on, reaches Nessie now."""
        if self.pending_merge is not None:
            late, self.pending_merge = self.pending_merge, None
            self.apply_merge(late["target"], late["body"])

    def apply_merge(self, name: str, body: dict[str, Any]) -> None:
        self.merges.append((body["fromRefName"], body["fromHash"], name))
        self.merge_messages.append(body["message"])
        merged = {**self.tables[(name, self.heads[name])], **self.tables[(body["fromRefName"], body["fromHash"])]}
        parent = self.heads[name]
        self.heads[name] = fake_hash(name, "merged", body["fromHash"], str(len(self.commits)))
        self.tables[(name, self.heads[name])] = merged
        self.commits.append((self.heads[name], body["message"], parent))

    def move_branch(self, name: str) -> str:
        self.heads[name] = fake_hash(name, "moved", self.heads[name])
        self.tables[(name, self.heads[name])] = dict(self.tables[(name, [h for (n, h) in self.tables if n == name][0])])
        return self.heads[name]

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        path = unquote(urlsplit(target).path).removeprefix("/api/v2")
        self.calls.append((method, path))
        if method == "GET" and path == "/config":
            return 200, json.dumps({"defaultBranch": "main"}).encode()
        parts = path.removeprefix("/trees/").split("/")
        name, _, pinned = parts[0].partition("@")
        if name not in self.heads:
            return 404, json.dumps({"errorCode": "REFERENCE_NOT_FOUND", "message": "no such reference"}).encode()
        if method == "GET" and len(parts) == 1:
            if name == BRANCH and self.on_branch_read is not None:
                self.on_branch_read()
            if name == "main":
                self.main_reads += 1
            return 200, json.dumps({"reference": {"type": "BRANCH", "name": name, "hash": self.heads[name]}}).encode()
        if method == "GET" and parts[1] == "history":
            self.history_reads.append(pinned)
            if self.on_history_read is not None:
                self.on_history_read()
            if self.history_answer is not None:
                return self.history_answer
            start = int(parse_qs(urlsplit(target).query).get("page-token", ["0"])[0])
            known = [commit[0] for commit in self.commits]
            upto = self.commits[: known.index(pinned) + 1] if pinned in known else ([] if pinned in ("", fake_hash("main", "0")) else None)
            if upto is None:
                return 404, json.dumps({"errorCode": "REFERENCE_NOT_FOUND", "message": "no such commit"}).encode()
            entries = list(reversed(upto))[start : start + self.history_page_size]
            more = start + self.history_page_size < len(upto)
            document = {
                "logEntries": [{"commitMeta": {"hash": hash_value, "message": message}, "parentCommitHash": parent} for hash_value, message, parent in entries],
                "hasMore": more,
                "token": str(start + len(entries)),
            }
            return 200, json.dumps(document).encode()
        if method == "GET" and parts[1] == "entries":
            return 200, json.dumps({"entries": self.entry_list(name, pinned or self.heads[name]), "hasMore": False}).encode()
        if method == "GET" and parts[1] == "contents":
            table = parts[2].removeprefix(f"{rules.NAMESPACE}.")
            held = self.tables.get((name, pinned or self.heads[name]), {})
            if table not in held:
                return 404, json.dumps({"errorCode": "CONTENT_NOT_FOUND", "message": "no such table"}).encode()
            return 200, json.dumps({"content": {"type": "ICEBERG_TABLE", "snapshotId": held[table]}}).encode()
        if method == "POST" and parts[1:] == ["history", "merge"]:
            body = json.loads(payload or b"{}")
            if self.crash_before_merge:
                self.crash_before_merge = False
                raise RuntimeError("the process stopped before the request was sent")
            self.apply_pending()
            if self.defer_main_commit:
                text, self.defer_main_commit = self.defer_main_commit, ""
                self.add_main_commit(text)
            self.merge_requests.append({"expected": pinned, **body})
            if pinned != self.heads[name] and not self.accept_stale_expected:
                return 409, json.dumps({"errorCode": "REFERENCE_CONFLICT", "message": "the target moved"}).encode()
            if self.noop_when_merged and any(message == body["message"] for _, message, _ in self.commits):
                return 200, json.dumps({"wasApplied": True, "wasSuccessful": True, "resultantTargetHash": self.heads[name]}).encode()
            if self.reply_without_commit:
                return 200, json.dumps({"wasApplied": True, "wasSuccessful": True, "resultantTargetHash": self.heads[name]}).encode()
            if self.defer_next_merge:
                self.defer_next_merge = False
                self.pending_merge = {"target": name, "body": body}
                raise TimeoutError
            self.apply_merge(name, body)
            if self.lose_next_merge_reply:
                self.lose_next_merge_reply = False
                raise TimeoutError
            return 200, json.dumps({"wasApplied": True, "wasSuccessful": True, "resultantTargetHash": self.reply_hash or self.heads[name]}).encode()
        raise AssertionError(f"the stand-in has no answer for {method} {path}")


PASSING_DETAILS = {
    "columns": rules.detail("columns_differ", missing=0, extra=0, repeated=0, order=0, not_string=0),
    "rows digest": rules.detail("digest_compared", equal=1),
    "key multiset": rules.detail("rows_differ", only_file=0, only_table=0),
    "row payload": rules.detail("rows_differ", only_file=0, only_table=0),
}


def result_line(plan: rules.Plan, *, passed: bool = True, change: Callable[[dict[str, Any]], None] | None = None) -> str:
    """The result line of a job that ran ``plan``: every required check passed, or ``lab_data`` stopped at a failed row count. ``change`` then edits it."""
    results = []
    for entry in plan.tables:
        failing_table = not passed and entry.name == "lab_data"
        rows = entry.rows if entry.rows else 5
        table = rules.TableResult(entry.name, entry.snapshot_id, columns_source=3, columns_table=3)
        table.rows_source, table.rows_table = (2, 3) if failing_table else (rows, rows)
        for name in rules.REQUIRED_CHECKS[entry.name][: 4 if failing_table else 99]:
            counts = rules.detail("row_count", source=table.rows_source, table=table.rows_table)
            table.add(name, counts if name == "row count" else PASSING_DETAILS.get(name, "ok"))
        results.append(table)
    document = json.loads(json.dumps(rules.job_document(plan, results)))
    if change is not None:
        change(document)
    return "RESULT " + json.dumps(document) + "\n"


class Runner:
    """The stand-in for the Spark job. It prints what it is told to, after recording the staged files and the plan that it found."""

    def __init__(self, line: Callable[[rules.Plan], str] | None = None, exit_code: int | None = None, error_text: str = "") -> None:
        self.line = line
        self.exit_code = exit_code
        self.error_text = error_text
        self.calls = 0
        self.seen_files: dict[str, str] = {}
        self.plan: dict[str, Any] = {}

    def __call__(self, staging: Path) -> run.RunnerReply:
        self.calls += 1
        plan_text = (staging / "plan.json").read_text()
        self.plan = json.loads(plan_text)
        self.seen_files = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in staging.iterdir() if path.name != "plan.json"}
        text = (self.line or result_line)(rules.parse_plan(plan_text))
        code = self.exit_code
        if code is None:
            code = 0 if text.startswith("RESULT ") and rules.parse_result(text.removeprefix("RESULT "), rules.parse_plan(plan_text)).passed else 1
        return run.RunnerReply(code, text, self.error_text)


class Case:
    """One load of the two tables on a branch, a published manifest of the landed files, and a folder for the staged files and for the results."""

    def __init__(self, test: unittest.TestCase) -> None:
        folder = tempfile.TemporaryDirectory(prefix="table_checks_")
        test.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.staging = self.root / "staging"
        self.run_folder = self.root / "run"
        self.run_folder.mkdir()
        self.state = self.root / "state"
        self.now = 1_000_000.0
        self.wall = 1_700_000_000.0
        self.slept: list[float] = []
        self.sleep_jump = 0.0
        self.timing = run.Timing(clock=lambda: self.now, sleep=self.sleep, wall=lambda: self.wall)
        self.landed: Landed = landed_set(SOURCE)
        self.reference: ManifestRef = publish_manifest(self.landed.store, self.landed.selection, self.landed.check())
        self.nessie = FakeNessie()
        self.branch_hash = self.nessie.load_branch(BRANCH, SNAPSHOTS)
        self.record = run.LoadRecord(BRANCH, self.branch_hash, self.reference, dict(SNAPSHOTS), self.nessie.heads["main"])

    def rebase(self) -> None:
        """The load branch is taken to have been made from the head that the default branch has now."""
        self.record = dataclasses.replace(self.record, base_hash=self.nessie.heads["main"])

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        if len(self.slept) > 100:
            raise AssertionError("a search did not end at its deadline")
        self.now += seconds + self.sleep_jump

    @property
    def pending_path(self) -> Path:
        return run.pending_path(self.reference.sha256, self.state)

    def pending(self) -> dict[str, Any]:
        pending: dict[str, Any] = json.loads(self.pending_path.read_text())
        return pending

    def go(self, runner: Runner, *, record: run.LoadRecord | None = None, empty_form: str | None = None) -> run.Outcome:
        options = {} if empty_form is None else {"empty_form": empty_form}
        return run.run_table_checks(
            store=self.landed.store,
            nessie=self.nessie,
            record=record or self.record,
            track_pin=PIN,
            staging=self.staging,
            run_folder=self.run_folder,
            runner=runner,
            staging_gid=os.getgid(),
            state_directory=self.state,
            timing=self.timing,
            **options,
        )

    def saved(self) -> dict[str, Any]:
        saved: dict[str, Any] = json.loads((self.run_folder / run.RESULT_FILE).read_text())
        return saved

    def main_untouched(self) -> bool:
        return not self.nessie.merges and self.nessie.heads["main"] == fake_hash("main", "0")


class PassingRunTest(unittest.TestCase):
    def test_control_every_check_passes_then_the_branch_merges_and_the_result_is_saved_against_the_manifest(self) -> None:
        case, runner = Case(self), Runner()
        outcome = case.go(runner)
        self.assertTrue(outcome.passed and outcome.merged)
        self.assertEqual([(BRANCH, case.branch_hash, "main")], case.nessie.merges)
        saved = case.saved()
        self.assertEqual(case.reference.sha256, saved["manifest"]["sha256"])
        self.assertEqual(
            {"bucket": case.reference.bucket, "key": case.reference.key, "version_id": case.reference.version_id, "sha256": case.reference.sha256},
            saved["manifest"],
        )
        self.assertEqual(
            (True, True, case.nessie.heads["main"], fake_hash("main", "0")), (saved["passed"], saved["merged"], saved["merged_hash"], saved["main_hash_before"])
        )
        self.assertEqual({"branch": BRANCH, "branch_hash": case.branch_hash, "tables": SNAPSHOTS}, saved["load"])
        self.assertEqual(SNAPSHOTS, case.nessie.tables[("main", case.nessie.heads["main"])])

    def test_the_job_finds_the_verified_bytes_and_the_plan_that_binds_them_to_the_manifest(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner)
        self.assertEqual({"clinical_data.csv": hashlib.sha256(CLINICAL).hexdigest(), "lab_data.csv": hashlib.sha256(LAB).hexdigest()}, runner.seen_files)
        plan = rules.parse_plan(json.dumps(runner.plan))
        self.assertEqual((BRANCH, case.branch_hash, "empty"), (plan.branch, plan.branch_hash, plan.empty_form))
        clinical, lab, track = plan.tables
        self.assertEqual(
            (111, "clinical_data.csv", hashlib.sha256(CLINICAL).hexdigest(), len(CLINICAL)),
            (clinical.snapshot_id, clinical.file, clinical.file_sha256, clinical.file_size),
        )
        self.assertEqual((222, "lab_data.csv", hashlib.sha256(LAB).hexdigest(), len(LAB)), (lab.snapshot_id, lab.file, lab.file_sha256, lab.file_size))
        self.assertEqual((333, 7, "a" * 64), (track.snapshot_id, track.rows, track.rows_sha256))

    def test_the_empty_form_reaches_the_job(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner, empty_form="null")
        self.assertEqual("null", runner.plan["empty_form"])
        with self.assertRaises(run.TableChecksError):
            Case(self).go(Runner(), empty_form="blank")

    def test_each_verified_file_is_read_once_in_blocks_and_nothing_else_of_the_landing_prefix_is_read(self) -> None:
        case = Case(self)
        case.landed.store.reads.clear()
        case.go(Runner())
        reads = case.landed.store.payload_reads("landing/")
        self.assertEqual(2, len(reads))
        self.assertEqual({"iter_version"}, {read.method for read in reads})
        self.assertEqual({case.landed.keys["clinical_data.csv"], case.landed.keys["lab_data.csv"]}, {read.key for read in reads})
        self.assertLessEqual(case.landed.store.largest_block, 8)


NAMESPACE_CONTENT = {"type": "NAMESPACE", "elements": ["other_space"]}
OTHER_TABLE = {"type": "ICEBERG_TABLE", "snapshotId": 9, "metadataLocation": "s3://warehouse/other/9.json"}


class OutputBindingTest(unittest.TestCase):
    """The branch is merged only when everything that it changes against the default branch is the output of this load."""

    def refused(self, case: Case, text: str) -> None:
        with self.assertRaisesRegex(run.TableChecksError, text):
            case.go(Runner())
        self.assertEqual([], case.nessie.merges)

    def test_another_table_on_the_branch_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.other[(BRANCH, case.branch_hash)] = {("other_space", "stray"): OTHER_TABLE}
        self.refused(case, "other_space.stray")

    def test_a_content_that_the_default_branch_holds_and_the_branch_lacks_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.other[("main", case.nessie.heads["main"])] = {("other_space", "kept"): OTHER_TABLE}
        self.refused(case, "other_space.kept")

    def test_an_edit_of_an_existing_namespace_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.other[("main", case.nessie.heads["main"])] = {("other_space",): NAMESPACE_CONTENT}
        case.nessie.other[(BRANCH, case.branch_hash)] = {("other_space",): {**NAMESPACE_CONTENT, "properties": {"owner": "x"}}}
        self.refused(case, "other_space")

    def test_an_edit_of_the_namespace_that_holds_the_tables_of_the_load_is_not_merged(self) -> None:
        case = Case(self)
        held = {"type": "NAMESPACE", "elements": [rules.NAMESPACE], "properties": {"owner": "x"}}
        case.nessie.other[("main", case.nessie.heads["main"])] = {(rules.NAMESPACE,): held}
        self.refused(case, rules.NAMESPACE)

    def test_a_removal_of_an_existing_namespace_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.other[("main", case.nessie.heads["main"])] = {("other_space",): NAMESPACE_CONTENT}
        self.refused(case, "other_space")

    def test_a_table_of_the_load_that_the_default_branch_already_holds_as_it_is_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.tables[("main", case.nessie.heads["main"])] = {"clinical_data": SNAPSHOTS["clinical_data"]}
        self.refused(case, "clinical_data")

    def test_a_default_branch_that_moved_with_another_change_since_the_branch_was_made_is_not_merged(self) -> None:
        case = Case(self)
        case.nessie.add_main_commit("another tool", {"other_table": 5})
        self.refused(case, "recorded base")

    def test_a_default_branch_that_changed_and_came_back_is_refused_because_its_head_is_not_the_recorded_base(self) -> None:
        case = Case(self)
        case.nessie.add_main_commit("another tool", {})
        self.assertNotEqual(case.record.base_hash, case.nessie.heads["main"])
        self.refused(case, "recorded base")

    def test_a_default_branch_at_another_head_with_the_same_contents_is_refused(self) -> None:
        case = Case(self)
        record = run.LoadRecord(BRANCH, case.branch_hash, case.reference, dict(SNAPSHOTS), fake_hash("main", "older"))
        with self.assertRaisesRegex(run.TableChecksError, "recorded base"):
            case.go(Runner(), record=record)
        self.assertEqual([], case.nessie.merges)

    def test_a_listing_that_leaves_out_the_content_of_its_entries_is_refused(self) -> None:
        case = Case(self)
        case.nessie.hide_content = True
        case.nessie.other[("main", case.nessie.heads["main"])] = {("other_space",): NAMESPACE_CONTENT}
        case.nessie.other[(BRANCH, case.branch_hash)] = {("other_space",): {**NAMESPACE_CONTENT, "properties": {"owner": "x"}}}
        self.refused(case, "expected form")

    def test_two_keys_that_differ_in_their_name_parts_are_two_keys(self) -> None:
        case = Case(self)
        case.nessie.other[("main", case.nessie.heads["main"])] = {("other_space.stray",): OTHER_TABLE}
        case.nessie.other[(BRANCH, case.branch_hash)] = {("other_space", "stray"): OTHER_TABLE}
        self.refused(case, "other_space")

    def test_a_base_hash_that_is_not_a_hash_is_refused(self) -> None:
        document = json.loads(Case(self).reference.to_json())
        record = {"format": run.LOAD_RECORD_FORMAT, "branch": BRANCH, "branch_hash": "a" * 64, "base_hash": "main", "manifest": document, "tables": SNAPSHOTS}
        with self.assertRaises(run.TableChecksError):
            run.parse_load_record(json.dumps(record))

    def test_the_load_record_names_the_base_hash_and_refuses_a_record_without_it(self) -> None:
        document = json.loads(Case(self).reference.to_json())
        record = {"format": run.LOAD_RECORD_FORMAT, "branch": BRANCH, "branch_hash": "a" * 64, "base_hash": "b" * 64, "manifest": document, "tables": SNAPSHOTS}
        self.assertEqual("b" * 64, run.parse_load_record(json.dumps(record)).base_hash)
        del record["base_hash"]
        with self.assertRaises(run.TableChecksError):
            run.parse_load_record(json.dumps(record))

    def test_the_namespace_that_the_default_branch_lacks_is_merged_with_the_tables(self) -> None:
        case = Case(self)
        self.assertTrue(case.go(Runner()).merged)
        self.assertEqual([(BRANCH, case.branch_hash, "main")], case.nessie.merges)


class MergeRecordTest(unittest.TestCase):
    def test_the_merge_message_ends_with_the_manifest_line_and_names_the_checked_branch_and_source_hash_on_lines_of_their_own(self) -> None:
        case = Case(self)
        case.go(Runner())
        (message,) = case.nessie.merge_messages
        lines = message.split("\n")
        self.assertEqual(run.manifest_line(case.reference.sha256), lines[-1])
        self.assertIn(f"records-branch: {BRANCH}", lines)
        self.assertIn(f"records-source: {case.branch_hash}", lines)
        self.assertEqual(1, sum(line.startswith("records-manifest:") for line in lines))

    def test_the_lines_have_exactly_the_forms_the_recovery_reads(self) -> None:
        self.assertEqual("records-manifest: " + "ab" * 32, run.manifest_line("ab" * 32))
        self.assertEqual("records-source: " + "ab" * 32, run.source_line("ab" * 32))
        self.assertEqual("records-branch: load_run_one", run.branch_line("load_run_one"))
        for value in ("", "ab" * 31, "AB" * 32, "ab" * 33, "gg" * 32, "ab" * 32 + "\n"):
            for make in (run.manifest_line, run.source_line):
                with self.subTest(value=value, make=make.__name__), self.assertRaises(run.TableChecksError):
                    make(value)
        for branch in ("", "a b", "a\nb", "load_x\nrecords-manifest: " + "a" * 64):
            with self.subTest(branch=branch), self.assertRaises(run.TableChecksError):
                run.branch_line(branch)

    def test_a_failed_run_writes_no_commit_and_so_no_line(self) -> None:
        case = Case(self)
        case.go(Runner(lambda plan: result_line(plan, passed=False)))
        self.assertEqual([], case.nessie.merge_messages)


class GrainNoteTest(unittest.TestCase):
    @staticmethod
    def line_for(passed: bool) -> Callable[[rules.Plan], str]:
        return lambda plan: result_line(plan, passed=passed)

    def test_the_saved_result_says_that_the_lab_key_check_and_payload_check_are_the_same_comparison(self) -> None:
        for passed in (True, False):
            with self.subTest(passed=passed):
                case = Case(self)
                case.go(Runner(self.line_for(passed)))
                notes = case.saved()["notes"]
                self.assertEqual(1, len([note for note in notes if "lab_data" in note and "same comparison" in note]))


class MergeOnceTest(unittest.TestCase):
    """A merge of one load happens once: the request carries the checked hash, a lost reply is found in main's log, and two runs of one load cannot overlap."""

    def merge_commit(self, case: Case) -> str:
        return case.nessie.heads["main"]

    @staticmethod
    def message(case: Case, *, branch: str = BRANCH, source: str | None = None, title: str = "merge of this load") -> str:
        return "\n".join([title, "", run.branch_line(branch), run.source_line(source or case.branch_hash), run.manifest_line(case.reference.sha256)])

    def test_a_branch_that_moves_while_the_merge_is_sent_is_merged_at_the_checked_hash_only(self) -> None:
        case = Case(self)
        reads: list[int] = []

        def count_and_move() -> None:
            reads.append(1)
            if len(reads) > 2:
                case.nessie.move_branch(BRANCH)

        case.nessie.on_branch_read = count_and_move
        outcome = case.go(Runner())
        self.assertTrue(outcome.merged)
        self.assertEqual([(BRANCH, case.branch_hash, "main")], case.nessie.merges)
        self.assertEqual(2, len(reads))

    def test_a_merge_that_appears_in_the_log_after_the_early_look_is_found_again_just_before_the_merge_is_sent(self) -> None:
        case, runner = Case(self), Runner()
        reads: list[int] = []
        added: list[str] = []

        def another_tool_merges_it() -> None:
            reads.append(1)
            if len(reads) == 2:
                added.append(case.nessie.add_main_commit(self.message(case, title="merged elsewhere"), SNAPSHOTS))

        case.nessie.on_branch_read = another_tool_merges_it
        outcome = case.go(runner)
        self.assertEqual([], case.nessie.merges)
        self.assertTrue(outcome.merged)
        self.assertEqual(added[0], case.saved()["merged_hash"])

    def test_a_lost_reply_is_found_in_the_log_of_main_in_the_same_run_and_a_rerun_sends_nothing(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.lose_next_merge_reply = True
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertEqual((1, 1), (len(case.nessie.merges), len(case.nessie.merge_requests)))
        again = case.go(runner)
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merge_requests)))
        self.assertTrue(again.passed and again.merged)
        saved = case.saved()
        self.assertEqual((True, True, self.merge_commit(case)), (saved["passed"], saved["merged"], saved["merged_hash"]))
        self.assertFalse(case.pending_path.exists())

    def test_a_receipt_that_was_lost_is_rebuilt_from_the_log_and_nothing_is_run_or_merged(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner)
        (case.run_folder / run.RESULT_FILE).unlink()
        again = case.go(runner)
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merges)))
        self.assertTrue(again.merged)
        self.assertEqual(self.merge_commit(case), case.saved()["merged_hash"])

    def test_the_log_is_searched_page_by_page_and_only_a_last_line_that_is_exactly_the_manifest_line_counts(self) -> None:
        case = Case(self)
        line = run.manifest_line(case.reference.sha256)
        case.nessie.history_page_size = 1
        head = case.nessie.heads["main"]
        self.assertIsNone(run.merged_commit(case.nessie, case.record, head))
        case.nessie.add_main_commit(f"note: {line} is mentioned\n\nin the middle")
        case.nessie.add_main_commit(f"{line}\n\nand then more text")
        case.nessie.add_main_commit(f"merge of another load\n\n{run.manifest_line('b' * 64)}")
        case.nessie.add_main_commit(f"{self.message(case)} ")
        case.nessie.add_main_commit(f"{self.message(case)}\n")
        case.nessie.add_main_commit(f"{self.message(case)}\n\n")
        self.assertIsNone(run.merged_commit(case.nessie, case.record, case.nessie.heads["main"]))
        found = case.nessie.add_main_commit(self.message(case))
        for number in range(3):
            case.nessie.add_main_commit(f"later commit {number}")
        merged = run.merged_commit(case.nessie, case.record, case.nessie.heads["main"])
        self.assertEqual(found, merged.hash if merged else None)

    def test_a_log_too_long_to_search_is_refused_not_taken_for_unmerged(self) -> None:
        case = Case(self)
        case.nessie.history_page_size = 1
        for number in range(run.HISTORY_PAGE_LIMIT + 1):
            case.nessie.add_main_commit(f"commit {number}")
        with self.assertRaisesRegex(run.TableChecksError, "log"):
            run.merged_commit(case.nessie, case.record, case.nessie.heads["main"])

    def test_a_second_run_of_one_load_while_the_first_holds_the_lock_fails_with_a_clear_message_and_does_nothing(self) -> None:
        case, runner = Case(self), Runner()
        case.state.mkdir(parents=True)
        with run.lock_path(case.reference.sha256, case.state).open("w") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(run.TableChecksError, "another run of this load"):
                case.go(runner)
        self.assertEqual(0, runner.calls)
        self.assertTrue(case.main_untouched())

    def test_the_lock_and_the_pending_record_of_a_load_are_in_one_fixed_directory_keyed_by_the_manifest(self) -> None:
        digest = "a" * 64
        self.assertTrue(run.STATE_DIRECTORY.is_absolute())
        self.assertEqual(run.STATE_DIRECTORY / f"{digest}.lock", run.lock_path(digest, run.STATE_DIRECTORY))
        self.assertEqual(run.STATE_DIRECTORY / f"{digest}.pending.json", run.pending_path(digest, run.STATE_DIRECTORY))
        case, runner = Case(self), Runner()
        case.state.mkdir(parents=True)
        with run.lock_path(case.reference.sha256, case.state).open("w") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            other = Case(self)
            other.state = case.state
            other.landed, other.reference, other.record, other.nessie = case.landed, case.reference, case.record, case.nessie
            with self.assertRaisesRegex(run.TableChecksError, "another run of this load"):
                other.go(runner)
        self.assertEqual([], [path.name for path in case.run_folder.iterdir()])

    def test_the_lock_is_released_after_a_run_and_after_a_failed_run(self) -> None:
        case = Case(self)
        with self.assertRaises(run.TableChecksError):
            case.go(Runner(lambda plan: "no result here\n", exit_code=1))
        runner = Runner()
        case.go(runner)
        self.assertEqual(1, runner.calls)

    def test_a_receipt_that_says_merged_is_never_replaced_by_one_that_says_not_merged(self) -> None:
        case = Case(self)
        outcome = case.go(Runner())
        with self.assertRaisesRegex(run.TableChecksError, "merged"):
            run.write_result(case.run_folder, {**outcome.document, "merged": False, "merged_hash": ""})
        self.assertTrue(case.saved()["merged"])
        run.write_result(case.run_folder, {**outcome.document, "notes": ["still merged"]})
        self.assertEqual(["still merged"], case.saved()["notes"])


class OneTargetHashTest(unittest.TestCase):
    """The search of the log and the merge are bound to one hash of main: the catalog refuses a merge when main moved after the search."""

    def test_the_log_is_searched_from_the_hash_of_main_that_the_merge_names_as_its_expected_hash(self) -> None:
        case = Case(self)
        case.nessie.add_main_commit("an unrelated commit")
        case.rebase()
        head = case.nessie.heads["main"]
        case.go(Runner())
        merged = case.nessie.heads["main"]
        self.assertEqual([head, head, merged], case.nessie.history_reads)
        self.assertEqual([head], [request["expected"] for request in case.nessie.merge_requests])
        self.assertEqual(3, case.nessie.main_reads)

    def test_a_merge_refused_because_main_moved_is_an_open_outcome_that_is_not_sent_again(self) -> None:
        case = Case(self)
        reads: list[int] = []

        def main_moves_after_the_search() -> None:
            reads.append(1)
            if len(reads) == 2:
                case.nessie.defer_main_commit = "another tool writes to main"

        case.nessie.on_history_read = main_moves_after_the_search
        with self.assertRaisesRegex(run.MergeOutcomeOpen, "merge_outcome_open"):
            case.go(Runner())
        self.assertEqual(([], 1), (case.nessie.merges, len(case.nessie.merge_requests)))
        self.assertEqual((True, False), (case.saved()["passed"], case.saved()["merged"]))

    def test_a_truncated_success_answer_of_the_log_is_an_error_and_no_merge_is_sent(self) -> None:
        case = Case(self)
        case.nessie.history_answer = (200, b'{"logEntries": [{"commitMeta": {"hash": "a')
        with self.assertRaises(NessieError):
            case.go(Runner())
        self.assertEqual([], case.nessie.merge_requests)

    def test_an_answer_of_the_log_that_is_not_in_the_expected_form_is_an_error_and_no_merge_is_sent(self) -> None:
        good = {"commitMeta": {"hash": "a" * 64, "message": "m"}}
        for label, document in (
            ("not an object", []),
            ("no entries", {"hasMore": False}),
            ("entries not a list", {"logEntries": "x", "hasMore": False}),
            ("no hasMore", {"logEntries": []}),
            ("hasMore not a boolean", {"logEntries": [], "hasMore": "no"}),
            ("an entry that is not an object", {"logEntries": ["x"], "hasMore": False}),
            ("an entry without commitMeta", {"logEntries": [{}], "hasMore": False}),
            ("a message that is not text", {"logEntries": [{"commitMeta": {"hash": "a" * 64, "message": 5}}], "hasMore": False}),
            ("a hash that is not a hash", {"logEntries": [{"commitMeta": {"hash": "zz", "message": "m"}}], "hasMore": False}),
            ("more pages and no token", {"logEntries": [good], "hasMore": True}),
        ):
            with self.subTest(label):
                case = Case(self)
                case.nessie.history_answer = (200, json.dumps(document).encode())
                with self.assertRaises(run.TableChecksError):
                    case.go(Runner())
                self.assertEqual([], case.nessie.merge_requests)

    def test_an_answer_for_a_table_that_is_not_in_the_expected_form_is_an_error_and_not_a_missing_table(self) -> None:
        case = Case(self)
        original = case.nessie.send

        def odd(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            if "/contents/" in target:
                return 200, json.dumps({"content": {"type": "ICEBERG_TABLE", "snapshotId": "11"}}).encode()
            return original(method, target, payload)

        with mock.patch.object(case.nessie, "send", odd), self.assertRaises(run.TableChecksError):
            run.table_snapshot(case.nessie, BRANCH, case.branch_hash, "clinical_data")
        self.assertIsNone(run.table_snapshot(case.nessie, BRANCH, case.branch_hash, "no_such_table"))


class MergeOutcomeTest(unittest.TestCase):
    """The catalog's history is the one source of the merge, and a merge request is never sent again by this step: a pending record in one fixed directory keeps
    every later run from sending while the load is not in the history, until the operator removes it by hand."""

    def open_outcome(self, case: Case, runner: Runner) -> None:
        case.nessie.defer_next_merge = True
        with self.assertRaisesRegex(run.MergeOutcomeOpen, "merge_outcome_open"):
            case.go(runner)

    def test_the_receipt_is_built_from_the_commit_in_the_history_and_its_parent_and_not_from_what_was_sent_or_answered(self) -> None:
        case = Case(self)
        case.nessie.add_main_commit("an unrelated commit")
        case.rebase()
        case.nessie.reply_hash = "f" * 64
        case.go(Runner())
        commit, _, parent = case.nessie.commits[-1]
        saved = case.saved()
        self.assertEqual((commit, parent), (saved["merged_hash"], saved["main_hash_before"]))
        self.assertNotEqual("f" * 64, saved["merged_hash"])
        self.assertFalse(case.pending_path.exists())

    def test_the_pending_record_is_written_before_the_request_in_the_fixed_directory_and_not_under_the_results_folder(self) -> None:
        case, runner = Case(self), Runner()
        seen: list[dict[str, Any]] = []
        original = case.nessie.send

        def look(method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
            if method == "POST":
                seen.append(case.pending())
                seen.append({"in_results": sorted(path.name for path in case.run_folder.iterdir() if "pending" in path.name)})
            return original(method, target, payload)

        with mock.patch.object(case.nessie, "send", look):
            case.go(runner)
        pending, results = seen
        self.assertEqual(case.state / f"{case.reference.sha256}.pending.json", case.pending_path)
        self.assertEqual(
            (case.reference.sha256, BRANCH, case.branch_hash, case.wall, runner.plan["run_id"]),
            (pending["manifest"], pending["branch"], pending["source"], pending["started"], pending["run_id"]),
        )
        self.assertEqual({"format", "manifest", "branch", "source", "started", "run_id"}, set(pending))
        self.assertEqual({"in_results": []}, results)

    def test_a_timeout_in_the_run_is_not_sent_again_it_waits_until_one_monotonic_deadline_and_ends_open_with_the_pending_record_kept(self) -> None:
        case = Case(self)
        self.open_outcome(case, Runner())
        self.assertEqual(1, len(case.nessie.merge_requests))
        self.assertTrue(case.pending_path.exists())
        self.assertLessEqual(sum(case.slept), case.nessie.timeout_seconds + case.timing.wait_seconds + case.timing.poll_seconds)

    def test_every_deadline_and_wait_is_read_from_a_monotonic_clock_by_default_and_the_wall_clock_only_dates_the_record(self) -> None:
        timing = run.Timing()
        self.assertIs(time.monotonic, timing.clock)
        self.assertIs(time.time, timing.wall)
        source = (ROOT / "scripts" / "ingestion" / "table_checks.py").read_text()
        self.assertNotIn("time.time()", source)
        self.assertEqual(1, source.count("timing.wall()"))

    def test_the_open_outcome_tells_the_operator_what_to_check_and_which_file_to_remove(self) -> None:
        case = Case(self)
        case.nessie.defer_next_merge = True
        with self.assertRaises(run.MergeOutcomeOpen) as raised:
            case.go(Runner())
        text = str(raised.exception)
        self.assertIn(str(case.pending_path), text)
        self.assertIn("history of main", text)
        self.assertIn("remove", text)

    def test_every_later_run_with_the_pending_record_ends_open_and_sends_nothing_whatever_the_clock_does(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        for jump in (10, 86_400 * 30, -86_400 * 90, 0):
            case.now += jump
            case.wall += jump
            with self.subTest(jump=jump), self.assertRaisesRegex(run.MergeOutcomeOpen, "merge_outcome_open"):
                case.go(runner)
        self.assertEqual(1, len(case.nessie.merge_requests))

    def test_a_forward_jump_of_the_clock_during_the_wait_ends_the_search_at_its_deadline_without_another_request(self) -> None:
        case = Case(self)
        case.sleep_jump = 86_400.0
        self.open_outcome(case, Runner())
        self.assertEqual(1, len(case.nessie.merge_requests))

    def test_a_different_results_folder_does_not_hide_the_pending_record(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        case.run_folder = case.root / "another_results_root" / case.reference.sha256
        with self.assertRaisesRegex(run.MergeOutcomeOpen, "merge_outcome_open"):
            case.go(runner)
        self.assertEqual(1, len(case.nessie.merge_requests))
        case.nessie.apply_pending()
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertEqual(1, len(case.nessie.merge_requests))
        self.assertFalse(case.pending_path.exists())

    def test_the_reviewers_sequence_exactly_one_request_across_all_runs_when_the_first_lands_after_a_retry_read_the_log(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        case.nessie.on_history_read = case.nessie.apply_pending
        with self.assertRaises(run.MergeOutcomeOpen):
            case.go(runner)
        case.nessie.on_history_read = None
        outcome = case.go(runner)
        self.assertEqual((1, 1), (len(case.nessie.merges), len(case.nessie.merge_requests)))
        self.assertTrue(outcome.merged)
        commit, _, parent = case.nessie.commits[-1]
        self.assertEqual((commit, parent), (case.saved()["merged_hash"], case.saved()["main_hash_before"]))
        self.assertFalse(case.pending_path.exists())

    def test_slow_history_pages_are_held_to_one_deadline_and_send_one_request_in_total(self) -> None:
        case, runner = Case(self), Runner()
        for number in range(30):
            case.nessie.add_main_commit(f"unrelated commit {number}")
        case.rebase()
        case.nessie.history_page_size = 1
        case.nessie.on_history_read = lambda: setattr(case, "now", case.now + 4.0)
        with self.assertRaisesRegex(run.TableChecksError, "deadline"):
            case.go(runner)
        self.assertEqual((0, 0), (len(case.nessie.merge_requests), runner.calls))
        case.nessie.history_page_size = 100
        case.nessie.on_history_read = None
        case.nessie.defer_next_merge = True
        with self.assertRaises(run.MergeOutcomeOpen):
            case.go(runner)
        case.nessie.history_page_size = 1
        case.nessie.on_history_read = lambda: setattr(case, "now", case.now + 4.0)
        with self.assertRaises(run.TableChecksError):
            case.go(runner)
        self.assertEqual(1, len(case.nessie.merge_requests))

    def test_a_crash_between_the_pending_write_and_the_request_ends_open_until_the_operator_removes_the_record_and_then_sends_one_request(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.crash_before_merge = True
        with self.assertRaises(RuntimeError):
            case.go(runner)
        self.assertEqual(0, len(case.nessie.merge_requests))
        for _ in range(2):
            with self.assertRaises(run.MergeOutcomeOpen):
                case.go(runner)
        self.assertEqual(0, len(case.nessie.merge_requests))
        case.pending_path.unlink()
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertEqual((1, 1), (len(case.nessie.merge_requests), len(case.nessie.merges)))

    def test_a_pending_record_survives_a_restart_because_it_is_a_file_of_the_state_directory(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        before = case.pending_path.read_text()
        later = Case(self)
        later.state, later.run_folder = case.state, case.root / "run_after_the_restart"
        later.landed, later.reference, later.record, later.nessie = case.landed, case.reference, case.record, case.nessie
        with self.assertRaises(run.MergeOutcomeOpen):
            later.go(runner)
        self.assertEqual(before, case.pending_path.read_text())
        self.assertEqual(1, len(case.nessie.merge_requests))

    def test_a_found_merge_removes_the_pending_record_and_sends_nothing(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        case.nessie.apply_pending()
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertEqual(1, len(case.nessie.merge_requests))
        self.assertFalse(case.pending_path.exists())

    def test_a_cached_rerun_after_a_successful_merge_removes_a_leftover_pending_record_of_the_same_load(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner)
        run.write_pending(case.state, case.record, case.wall, "a" * 32)
        again = case.go(runner)
        self.assertTrue(again.merged)
        self.assertFalse(case.pending_path.exists())
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merge_requests)))

    def test_a_cached_rerun_never_removes_a_pending_record_that_is_not_shown_to_be_this_loads(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner)
        case.pending_path.write_text("left over")
        with self.assertRaises(run.TableChecksError):
            case.go(runner)
        self.assertEqual("left over", case.pending_path.read_text())

    def test_a_cached_receipt_of_one_load_never_removes_the_pending_record_of_another_load_and_the_one_request_is_never_resent(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        before = case.pending_path.read_text()
        other = run.LoadRecord("load_run_two", case.nessie.load_branch("load_run_two", SNAPSHOTS), case.reference, dict(SNAPSHOTS), BASE)
        cached = run.receipt(other, "empty", None, passed=True)
        cached.update(merged=True, merged_into="main", main_hash_before="1" * 64, merged_hash="2" * 64)
        run.write_result(case.root / "other_results", cached)
        case.run_folder = case.root / "other_results"
        with self.assertRaisesRegex(run.MergeOutcomeOpen, "another load"):
            case.go(runner, record=other)
        self.assertEqual(before, case.pending_path.read_text())
        case.run_folder = case.root / "run"
        with self.assertRaises(run.MergeOutcomeOpen):
            case.go(runner)
        self.assertEqual(1, len(case.nessie.merge_requests))
        case.nessie.apply_pending()
        self.assertTrue(case.go(runner).merged)
        self.assertEqual((1, 1), (len(case.nessie.merge_requests), len(case.nessie.merges)))
        self.assertFalse(case.pending_path.exists())

    def test_a_success_answer_with_no_commit_in_the_history_is_an_error_and_there_is_no_merged_receipt(self) -> None:
        case = Case(self)
        case.nessie.reply_without_commit = True
        with self.assertRaisesRegex(run.MergeNotFound, "merge_not_found"):
            case.go(Runner())
        self.assertEqual((True, False), (case.saved()["passed"], case.saved()["merged"]))
        self.assertTrue(case.pending_path.exists())

    def test_a_pending_record_of_another_load_is_refused_whatever_receipts_there_are(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.crash_before_merge = True
        with self.assertRaises(RuntimeError):
            case.go(runner)
        other_hash = case.nessie.load_branch("load_run_two", SNAPSHOTS)
        case.run_folder = case.root / "other_results"
        with self.assertRaisesRegex(run.TableChecksError, "pending merge record of another load"):
            case.go(runner, record=run.LoadRecord("load_run_two", other_hash, case.reference, dict(SNAPSHOTS), BASE))

    def test_a_pending_record_that_is_not_in_its_form_is_refused(self) -> None:
        for label in (
            "empty",
            "not an object",
            "not JSON",
            "another format",
            "infinite time",
            "NaN time",
            "text time",
            "extra field",
            "no run id",
            "bad run id",
            "control",
        ):
            case = Case(self)
            good = {
                "format": run.PENDING_FORMAT,
                "manifest": case.reference.sha256,
                "branch": BRANCH,
                "source": case.branch_hash,
                "started": 1.5,
                "run_id": "a" * 32,
            }
            text = {
                "empty": "",
                "not an object": "[]",
                "not JSON": "{",
                "another format": json.dumps({**good, "format": "x"}),
                "infinite time": json.dumps({**good, "started": float("inf")}),
                "NaN time": json.dumps({**good, "started": float("nan")}),
                "text time": json.dumps({**good, "started": "later"}),
                "extra field": json.dumps({**good, "extra": 1}),
                "no run id": json.dumps({key: value for key, value in good.items() if key != "run_id"}),
                "bad run id": json.dumps({**good, "run_id": "not a run id"}),
                "control": json.dumps(good),
            }[label]
            case.state.mkdir(parents=True)
            case.pending_path.write_text(text)
            with self.subTest(label):
                if label == "control":
                    with self.assertRaises(run.MergeOutcomeOpen):
                        case.go(Runner())
                else:
                    with self.assertRaises(run.TableChecksError) as raised:
                        case.go(Runner())
                    self.assertNotIsInstance(raised.exception, run.MergeOutcomeOpen)

    def test_a_receipt_rebuilt_from_the_history_keeps_only_the_evidence_of_the_attempt_that_merged(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        case.nessie.apply_pending()
        outcome = case.go(runner)
        self.assertTrue(outcome.document["checked"]["passed"])
        other = Case(self)
        other.state, other.run_folder = case.state, other.root / "a_folder_with_no_receipt"
        other.landed, other.reference, other.record, other.nessie = case.landed, case.reference, case.record, case.nessie
        rebuilt = other.go(runner)
        self.assertIsNone(rebuilt.document["checked"])
        self.assertTrue(rebuilt.merged)
        failed = Case(self)
        failed.state, failed.landed, failed.reference, failed.record, failed.nessie = case.state, case.landed, case.reference, case.record, case.nessie
        failed.run_folder = failed.root / "a_folder_with_a_failed_attempt"
        run.write_result(failed.run_folder, {**run.receipt(case.record, "empty", {"passed": False}, passed=False)})
        again = failed.go(runner)
        self.assertIsNone(again.document["checked"])

    def test_a_receipt_rebuilt_after_the_operator_removed_the_pending_record_cites_no_checks_and_says_so(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        self.assertTrue(case.saved()["passed"])
        case.pending_path.unlink()
        case.nessie.apply_pending()
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertIsNone(outcome.document["checked"])
        self.assertIn("rebuilt from the log", " ".join(outcome.document["notes"]))
        self.assertEqual(1, len(case.nessie.merge_requests))

    def test_a_receipt_rebuilt_while_the_pending_record_names_another_attempt_cites_no_checks(self) -> None:
        case, runner = Case(self), Runner()
        self.open_outcome(case, runner)
        case.nessie.apply_pending()
        case.pending_path.write_text(json.dumps({**case.pending(), "run_id": "0" * 32}))
        outcome = case.go(runner)
        self.assertTrue(outcome.merged)
        self.assertIsNone(outcome.document["checked"])


class OtherLoadTest(unittest.TestCase):
    """A receipt is rebuilt from the log only when the merge commit names this run's checked branch and source hash."""

    def lost_receipt(self, case: Case) -> None:
        case.go(Runner())
        (case.run_folder / run.RESULT_FILE).unlink()

    def test_a_retry_with_the_same_manifest_and_snapshots_but_another_branch_and_hash_is_not_given_the_receipt(self) -> None:
        case = Case(self)
        self.lost_receipt(case)
        other_hash = case.nessie.load_branch("load_run_two", SNAPSHOTS)
        retry = run.LoadRecord("load_run_two", other_hash, case.reference, dict(SNAPSHOTS), BASE)
        runner = Runner()
        with self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
            case.go(runner, record=retry)
        self.assertEqual(0, runner.calls)
        self.assertFalse((case.run_folder / run.RESULT_FILE).exists())

    def test_the_same_branch_with_another_source_hash_is_not_given_the_receipt_either(self) -> None:
        case = Case(self)
        self.lost_receipt(case)
        moved = fake_hash("elsewhere")
        case.nessie.heads[BRANCH] = moved
        case.nessie.tables[(BRANCH, moved)] = dict(SNAPSHOTS)
        with self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
            case.go(Runner(), record=run.LoadRecord(BRANCH, moved, case.reference, dict(SNAPSHOTS), BASE))

    def test_a_matching_recovery_rebuilds_the_receipt_and_runs_and_sends_nothing(self) -> None:
        case, runner = Case(self), Runner()
        self.lost_receipt(case)
        runner.calls = 0
        again = case.go(runner)
        self.assertEqual((0, 1), (runner.calls, len(case.nessie.merges)))
        self.assertTrue(again.merged and case.saved()["merged"])

    def test_a_merge_commit_in_the_older_form_without_the_new_lines_refuses_recovery(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.add_main_commit(f"merge {BRANCH}: table checks passed\n\n{run.manifest_line(case.reference.sha256)}", SNAPSHOTS)
        with self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
            case.go(runner)
        self.assertEqual(0, runner.calls)
        self.assertEqual([], case.nessie.merge_requests)

    def test_the_requested_lines_in_the_body_and_another_load_in_the_final_three_lines_refuses_recovery(self) -> None:
        case = Case(self)
        message = "\n".join(
            [
                run.branch_line(BRANCH),
                run.source_line(case.branch_hash),
                "",
                "merge",
                run.branch_line("load_other"),
                run.source_line("c" * 64),
                run.manifest_line(case.reference.sha256),
            ]
        )
        case.nessie.add_main_commit(message, SNAPSHOTS)
        with self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
            case.go(Runner())

    def test_the_final_three_lines_are_one_ordered_unit_so_another_order_a_gap_or_a_line_between_refuses_recovery(self) -> None:
        for label in ("order swapped", "a blank line between", "another line between"):
            case = Case(self)
            branch, source, manifest = run.branch_line(BRANCH), run.source_line(case.branch_hash), run.manifest_line(case.reference.sha256)
            lines = {
                "order swapped": ["merge", "", source, branch, manifest],
                "a blank line between": ["merge", branch, "", source, manifest],
                "another line between": ["merge", branch, source, "another line", manifest],
            }[label]
            case.nessie.add_main_commit("\n".join(lines), SNAPSHOTS)
            with self.subTest(label), self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
                case.go(Runner())

    def test_a_commit_with_only_one_of_the_two_lines_refuses_recovery(self) -> None:
        for lines in ([run.branch_line(BRANCH)], ["records-source: " + "b" * 64]):
            case = Case(self)
            case.nessie.add_main_commit("\n".join(["merge", "", *lines, run.manifest_line(case.reference.sha256)]), SNAPSHOTS)
            with self.assertRaisesRegex(run.TableChecksError, "another load of this manifest"):
                case.go(Runner())


class StagedModesTest(unittest.TestCase):
    """The staged inputs are readable by the Spark user through the shared group and by nobody else: the folder 0750, every file 0640, set by the launcher."""

    def seen_during_run(self, case: Case, umask: int) -> dict[str, tuple[int, int]]:
        seen: dict[str, tuple[int, int]] = {}

        def look(plan: rules.Plan) -> str:
            folder = case.staging
            seen["."] = (stat.S_IMODE(folder.stat().st_mode), folder.stat().st_gid)
            for path in folder.iterdir():
                seen[path.name] = (stat.S_IMODE(path.stat().st_mode), path.stat().st_gid)
            return result_line(plan)

        previous = os.umask(umask)
        try:
            case.go(Runner(look))
        finally:
            os.umask(previous)
        return seen

    def test_the_folder_and_every_file_have_exactly_the_stated_modes_and_group_whatever_the_umask(self) -> None:
        for umask in (0o077, 0o022, 0o000):
            with self.subTest(umask=oct(umask)):
                case = Case(self)
                seen = self.seen_during_run(case, umask)
                self.assertEqual({".", "plan.json", "clinical_data.csv", "lab_data.csv"}, set(seen))
                self.assertEqual({"."}, {name for name, (mode, _) in seen.items() if mode == 0o750})
                self.assertEqual({"plan.json", "clinical_data.csv", "lab_data.csv"}, {name for name, (mode, _) in seen.items() if mode == 0o640})
                self.assertEqual({os.getgid()}, {group for _, group in seen.values()})

    def test_the_constants_name_the_user_the_job_runs_as_and_the_group_it_shares_with_the_launcher(self) -> None:
        self.assertEqual((10100, 10001), (run.SPARK_UID, run.STAGING_GID))
        self.assertEqual(0o750, run.STAGING_FOLDER_MODE)
        self.assertEqual(0o640, run.STAGING_FILE_MODE)

    def test_a_group_the_launcher_cannot_set_stops_the_run_before_the_job_and_leaves_nothing_staged(self) -> None:
        case, runner = Case(self), Runner()
        with (
            mock.patch.object(os, "chown", side_effect=PermissionError),
            self.assertRaisesRegex(run.TableChecksError, "group"),
        ):
            run.run_table_checks(
                store=case.landed.store,
                nessie=case.nessie,
                record=case.record,
                track_pin=PIN,
                staging=case.staging,
                run_folder=case.run_folder,
                runner=runner,
                staging_gid=99998,
                state_directory=case.state,
            )
        self.assertEqual(0, runner.calls)
        self.assertEqual([], list(case.staging.iterdir()) if case.staging.exists() else [])


class StagingCleanupTest(unittest.TestCase):
    """Nothing stays in the staging folder when the step ends, whichever step ends it."""

    def nothing_staged(self, case: Case) -> None:
        self.assertEqual([], list(case.staging.iterdir()) if case.staging.exists() else [])

    def test_an_invalid_pin_is_refused_before_anything_is_staged(self) -> None:
        for pin in (rules.TrackPin(0, "a" * 64), rules.TrackPin(-1, "a" * 64), rules.TrackPin(7, "A" * 64), rules.TrackPin(7, "a" * 63)):
            with self.subTest(pin=pin):
                case = Case(self)
                case.landed.store.reads.clear()
                with self.assertRaises(run.TableChecksError):
                    run.run_table_checks(
                        store=case.landed.store,
                        nessie=case.nessie,
                        record=case.record,
                        track_pin=pin,
                        staging=case.staging,
                        run_folder=case.run_folder,
                        runner=Runner(),
                        staging_gid=os.getgid(),
                        state_directory=case.state,
                    )
                self.nothing_staged(case)
                self.assertEqual([], case.landed.store.payload_reads("landing/"))

    def test_a_runner_that_raises_a_result_that_is_refused_and_a_job_that_fails_leave_nothing_staged(self) -> None:
        def raising(staging: Path) -> run.RunnerReply:
            raise RuntimeError("the runner broke")

        for label, runner in (
            ("raises", raising),
            ("no result", Runner(lambda plan: "nothing\n", exit_code=1)),
            ("failed check", Runner(lambda plan: result_line(plan, passed=False))),
            ("refused result", Runner(lambda plan: "RESULT {}\n", exit_code=0)),
        ):
            with self.subTest(label):
                case = Case(self)
                with contextlib.suppress(run.TableChecksError, RuntimeError):
                    case.go(runner)  # type: ignore[arg-type]
                self.nothing_staged(case)

    def test_a_bytes_mismatch_a_gate_refusal_and_a_moved_branch_leave_nothing_staged(self) -> None:
        case = Case(self)
        case.landed.store.history[case.landed.versions["lab_data.csv"].key][-1].data = LAB.replace(b"2.9", b"2.8")
        with self.assertRaises(run.TableChecksError):
            case.go(Runner())
        self.nothing_staged(case)
        case = Case(self)
        case.landed.land("clinical_data.csv", b"caseid,age\r\n9,99\r\n")
        with self.assertRaises(GateRefused):
            case.go(Runner())
        self.nothing_staged(case)
        case = Case(self)
        case.nessie.move_branch(BRANCH)
        with self.assertRaises(run.TableChecksError):
            case.go(Runner())
        self.nothing_staged(case)


class SingleResultTest(unittest.TestCase):
    """Exactly one result line, from the closed table of details, and only validated fields are kept."""

    def test_two_result_lines_one_failed_and_one_passed_authorise_nothing(self) -> None:
        for order in ((False, True), (True, False), (True, True)):
            with self.subTest(order=order):
                case = Case(self)

                def two(plan: rules.Plan, order: tuple[bool, ...] = order) -> str:
                    return "".join(result_line(plan, passed=passed) for passed in order)

                with self.assertRaisesRegex(run.TableChecksError, "more than one"):
                    case.go(Runner(two, exit_code=0))
                self.assertTrue(case.main_untouched())
                self.assertIn("RESULT", (case.run_folder / run.JOB_OUTPUT_FILE).read_text())

    @staticmethod
    def with_detail(detail: str) -> Callable[[rules.Plan], str]:
        def change(document: dict[str, Any]) -> None:
            document["tables"][0]["checks"][4]["detail"] = detail

        return lambda plan: result_line(plan, change=change)

    def test_a_marker_in_a_detail_as_a_word_as_digits_and_as_a_code_like_token_reaches_no_file_and_no_message(self) -> None:
        variants = {
            "a lower-case word": "marker_9d2",
            "a code-like token": "rows_differ_marker_9d2;only_file=1;only_table=0",
            "digits in a count": "row_count;source=4719238;table=4719238",
            "a word where a count goes": "rows_differ;only_file=marker;only_table=0",
            "a known code with an extra field": "ok;marker=9",
        }
        for label, detail in variants.items():
            with self.subTest(label):
                case = Case(self)
                line = Runner(self.with_detail(detail), exit_code=0)
                with self.assertRaises(run.TableChecksError) as raised:
                    case.go(line)
                self.assertNotIn("marker", str(raised.exception))
                self.assertNotIn("4719238", str(raised.exception))
                self.assertFalse((case.run_folder / run.RESULT_FILE).exists())
                self.assertTrue(case.main_untouched())

    def test_the_saved_result_holds_the_validated_fields_and_a_verdict_derived_by_the_controller_from_the_checks(self) -> None:
        case = Case(self)
        outcome = case.go(Runner())
        saved = case.saved()["checked"]
        self.assertEqual({"format", "run_id", "branch", "branch_hash", "empty_form", "passed", "tables"}, set(saved))
        self.assertTrue(saved["passed"] and outcome.merged)

    def test_a_result_with_exit_code_0_and_false_summary_flags_is_refused_and_no_merge_request_is_sent(self) -> None:
        for label, top, tables in (("the result only", True, False), ("the tables only", False, True), ("both", True, True)):

            def flags(plan: rules.Plan, top: bool = top, tables: bool = tables) -> str:
                def change(document: dict[str, Any]) -> None:
                    if top:
                        document["passed"] = False
                    for table in document["tables"] if tables else []:
                        table["passed"] = False

                return result_line(plan, change=change)

            with self.subTest(label):
                case = Case(self)
                with self.assertRaises(run.TableChecksError):
                    case.go(Runner(flags, exit_code=0))
                self.assertEqual([], case.nessie.merge_requests)
                self.assertTrue(case.main_untouched())
                self.assertFalse((case.run_folder / run.RESULT_FILE).exists())

    def test_a_columns_check_with_zero_columns_on_both_sides_is_refused_and_no_merge_request_is_sent(self) -> None:
        for index in (0, 1):

            def zero(plan: rules.Plan, index: int = index) -> str:
                def change(document: dict[str, Any]) -> None:
                    document["tables"][index].update(columns_source=0, columns_table=0)

                return result_line(plan, change=change)

            with self.subTest(table=index):
                case = Case(self)
                with self.assertRaises(run.TableChecksError):
                    case.go(Runner(zero, exit_code=0))
                self.assertEqual([], case.nessie.merge_requests)
                self.assertTrue(case.main_untouched())


class RefusalTest(unittest.TestCase):
    def assert_nothing_ran(self, case: Case, runner: Runner) -> None:
        self.assertEqual(0, runner.calls)
        self.assertTrue(case.main_untouched())
        self.assertFalse((case.run_folder / run.RESULT_FILE).exists())

    def test_a_version_substituted_after_the_manifest_is_refused_before_any_payload_is_read(self) -> None:
        case, runner = Case(self), Runner()
        case.landed.land("clinical_data.csv", b"caseid,age\r\n9,99\r\n")
        case.landed.store.reads.clear()
        with self.assertRaises(GateRefused):
            case.go(runner)
        self.assertEqual([], case.landed.store.payload_reads("landing/"))
        self.assert_nothing_ran(case, runner)
        self.assertFalse(case.staging.exists() and any(case.staging.iterdir()))

    def test_bytes_that_differ_from_the_bound_digest_are_refused_and_removed(self) -> None:
        case, runner = Case(self), Runner()
        version = case.landed.versions["lab_data.csv"]
        stored = case.landed.store.history[version.key][-1]
        stored.data = LAB.replace(b"2.9", b"2.8")
        with self.assertRaisesRegex(run.TableChecksError, "verified"):
            case.go(runner)
        self.assert_nothing_ran(case, runner)
        self.assertEqual([], [path for path in case.staging.glob("*")] if case.staging.exists() else [])

    def test_a_load_record_that_binds_another_manifest_is_refused(self) -> None:
        case, runner = Case(self), Runner()
        other = ManifestRef(case.reference.bucket, case.reference.key, case.reference.version_id, "b" * 64)
        with self.assertRaises(Exception):  # noqa: B017
            case.go(runner, record=run.LoadRecord(BRANCH, case.branch_hash, other, dict(SNAPSHOTS), BASE))
        self.assert_nothing_ran(case, runner)

    def test_a_branch_that_moved_since_the_load_is_refused_before_the_job_runs(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.move_branch(BRANCH)
        with self.assertRaisesRegex(run.TableChecksError, "moved"):
            case.go(runner)
        self.assert_nothing_ran(case, runner)

    def test_a_branch_that_does_not_exist_is_refused(self) -> None:
        case, runner = Case(self), Runner()
        with self.assertRaises(NessieError):
            case.go(runner, record=run.LoadRecord("load_other", case.branch_hash, case.reference, dict(SNAPSHOTS), BASE))
        self.assert_nothing_ran(case, runner)

    def test_a_snapshot_that_differs_from_the_load_record_is_refused(self) -> None:
        case, runner = Case(self), Runner()
        with self.assertRaisesRegex(run.TableChecksError, "snapshot"):
            case.go(runner, record=run.LoadRecord(BRANCH, case.branch_hash, case.reference, {**SNAPSHOTS, "lab_data": 999}, BASE))
        self.assert_nothing_ran(case, runner)

    def test_a_table_the_branch_does_not_hold_at_the_hash_is_refused(self) -> None:
        case, runner = Case(self), Runner()
        del case.nessie.tables[(BRANCH, case.branch_hash)]["track_list"]
        with self.assertRaises(run.TableChecksError):
            case.go(runner)
        self.assert_nothing_ran(case, runner)


class FailedCheckTest(unittest.TestCase):
    def test_a_check_that_fails_merges_nothing_and_the_branch_stays(self) -> None:
        case, runner = Case(self), Runner(lambda plan: result_line(plan, passed=False))
        outcome = case.go(runner)
        self.assertFalse(outcome.passed or outcome.merged)
        self.assertTrue(case.main_untouched())
        self.assertIn(BRANCH, case.nessie.heads)
        saved = case.saved()
        self.assertEqual((False, False, "", case.reference.sha256), (saved["passed"], saved["merged"], saved["merged_hash"], saved["manifest"]["sha256"]))

    def test_a_job_that_ends_with_a_failure_code_but_a_passing_line_merges_nothing(self) -> None:
        case = Case(self)
        with self.assertRaisesRegex(run.TableChecksError, "exit code"):
            case.go(Runner(exit_code=1))
        self.assertTrue(case.main_untouched())

    def test_a_result_that_is_not_for_this_plan_or_this_form_merges_nothing(self) -> None:
        def changed(change: Callable[[dict[str, Any]], None]) -> Callable[[rules.Plan], str]:
            return lambda plan: result_line(plan, change=change)

        for label, line in (
            ("another hash", changed(lambda document: document.update(branch_hash="c" * 64))),
            ("another branch", changed(lambda document: document.update(branch="load_other"))),
            ("another run", changed(lambda document: document.update(run_id="a" * 32))),
            ("the other empty form", changed(lambda document: document.update(empty_form="null"))),
            ("a missing table", changed(lambda document: document["tables"].pop())),
            ("another snapshot", changed(lambda document: document["tables"][2].update(snapshot_id=1))),
            ("a check missing", changed(lambda document: document["tables"][0]["checks"].pop(3))),
            ("a check missing at the end", changed(lambda document: document["tables"][2]["checks"].pop())),
            ("an extra check", changed(lambda document: document["tables"][1]["checks"].append({"name": "extra", "passed": True, "detail": "ok"}))),
            ("a free-text detail", changed(lambda document: document["tables"][1]["checks"][0].update(detail="value 17 differs"))),
            ("no result line", lambda plan: "the job printed nothing useful\n"),
            ("a result that is not JSON", lambda plan: "RESULT {not json\n"),
        ):
            with self.subTest(label):
                case = Case(self)
                with self.assertRaises(run.TableChecksError):
                    case.go(Runner(line, exit_code=0))
                self.assertTrue(case.main_untouched())

    def test_a_failed_check_is_a_failed_run_and_merges_nothing(self) -> None:
        def lying(plan: rules.Plan) -> str:
            def flags(document: dict[str, Any]) -> None:
                document["tables"][1]["checks"][3].update(passed=False, detail="row_count;source=2;table=3")
                document["tables"][1].update(rows_source=2, rows_table=3)
                del document["tables"][1]["checks"][4:]

            return result_line(plan, change=flags)

        case = Case(self)
        outcome = case.go(Runner(lying, exit_code=1))
        self.assertFalse(outcome.passed or outcome.merged)
        self.assertTrue(case.main_untouched())
        with self.assertRaisesRegex(run.TableChecksError, "exit code"):
            Case(self).go(Runner(lying, exit_code=0))

    def test_a_job_that_cannot_run_names_its_exit_code_and_merges_nothing(self) -> None:
        case = Case(self)
        with self.assertRaisesRegex(run.TableChecksError, "exit code 2"):
            case.go(Runner(lambda plan: "", exit_code=2))
        self.assertTrue(case.main_untouched())

    def test_a_branch_that_moves_after_the_checks_is_not_merged(self) -> None:
        case = Case(self)

        def moving(plan: rules.Plan) -> str:
            case.nessie.move_branch(BRANCH)
            return result_line(plan)

        with self.assertRaisesRegex(run.TableChecksError, "moved"):
            case.go(Runner(moving))
        self.assertTrue(case.main_untouched())


class FailureOutputTest(unittest.TestCase):
    """The tables hold per-patient rows: no value of the data reaches a result, a message or an exception. The job's raw output goes to a private file."""

    def test_a_job_that_prints_no_result_gives_a_fixed_message_and_its_output_goes_to_a_file_in_the_run_folder(self) -> None:
        case = Case(self)
        runner = Runner(lambda plan: f"stdout {MARKER}\n", exit_code=1, error_text=f"Malformed record [{MARKER}]")
        with self.assertRaises(run.TableChecksError) as raised:
            case.go(runner)
        self.assertNotIn(MARKER, str(raised.exception))
        self.assertRegex(str(raised.exception), "exit code 1")
        kept = case.run_folder / run.JOB_OUTPUT_FILE
        self.assertIn(MARKER, kept.read_text())
        self.assertEqual(0o600, kept.stat().st_mode & 0o777)
        self.assertTrue(case.main_untouched())

    def test_a_result_that_is_refused_keeps_the_raw_output_in_the_file_too(self) -> None:
        case = Case(self)
        bad = Runner(lambda plan: result_line(plan, change=lambda document: document["tables"][0]["checks"][0].update(detail=f"value {MARKER}")), exit_code=0)
        with self.assertRaises(run.TableChecksError) as raised:
            case.go(bad)
        self.assertNotIn(MARKER, str(raised.exception))
        self.assertIn(MARKER, (case.run_folder / run.JOB_OUTPUT_FILE).read_text())

    def test_no_marker_reaches_the_saved_result_of_a_failed_run(self) -> None:
        case = Case(self)
        case.go(Runner(lambda plan: result_line(plan, passed=False), error_text=f"warning {MARKER}"))
        self.assertNotIn(MARKER, (case.run_folder / run.RESULT_FILE).read_text())


class RerunTest(unittest.TestCase):
    def test_a_rerun_after_the_merge_runs_nothing_and_merges_nothing_more(self) -> None:
        case, runner = Case(self), Runner()
        first = case.go(runner)
        again = case.go(runner)
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merges)))
        self.assertEqual(first.document, again.document)
        self.assertTrue(again.merged)

    def test_a_rerun_after_a_failed_check_runs_the_checks_again_and_replaces_the_result(self) -> None:
        case = Case(self)
        case.go(Runner(lambda plan: result_line(plan, passed=False)))
        self.assertFalse(case.saved()["passed"])
        runner = Runner()
        outcome = case.go(runner)
        self.assertEqual(1, runner.calls)
        self.assertTrue(outcome.merged and case.saved()["merged"])

    def test_results_of_another_load_in_the_run_folder_are_not_replaced(self) -> None:
        case = Case(self)
        case.go(Runner())
        other = Case(self)
        other.run_folder = case.run_folder
        other.nessie.load_branch("load_run_two", SNAPSHOTS)
        with self.assertRaisesRegex(run.TableChecksError, "another load"):
            other.go(Runner(), record=run.LoadRecord("load_run_two", other.nessie.heads["load_run_two"], other.reference, dict(SNAPSHOTS), BASE))


class ComposeRunnerTest(unittest.TestCase):
    """The command that starts the job through the stack's Compose files: the batch profile, no dependency started, the staging volume read-only."""

    def settings(self) -> Settings:
        return Settings(compose_directory=ROOT / "compose", project="stack_example", host_env=Path("/opt/stack/compose/host.env"))

    def runner(self) -> run.Runner:
        return run.compose_runner(self.settings(), volume="stack_example_table_check_staging", directory="/opt/dagster/staging", deadline_seconds=77)

    def test_control_the_command_names_the_job_the_volume_and_the_folder_of_this_run(self) -> None:
        with mock.patch.object(run, "run_command", return_value=CompletedProcess([], 0, "RESULT {}\n", "")) as started:
            reply = self.runner()(Path("/opt/dagster/staging/run_one"))
        program, arguments = started.call_args.args
        self.assertEqual("docker", program)
        self.assertEqual(77, started.call_args.kwargs["timeout"])
        text = " ".join(arguments)
        self.assertIn(f"compose --project-directory {ROOT / 'compose'}", text)
        self.assertIn("--project-name stack_example", text)
        self.assertIn("--profile batch run --rm --no-TTY --no-deps", text)
        self.assertIn("--volume stack_example_table_check_staging:/staging:ro", text)
        self.assertTrue(text.endswith("--entrypoint python spark -m spark.jobs.table_checks --staging /staging/run_one"), text)
        self.assertEqual((0, "RESULT {}\n"), (reply.exit_code, reply.stdout))

    def test_the_command_runs_the_job_as_the_spark_user_with_the_launchers_group_and_the_environment_carries_the_memory_limit(self) -> None:
        from scripts.stack.groups import rendered_environment
        from scripts.stack.limits import load_limits

        with mock.patch.object(run, "run_command", return_value=CompletedProcess([], 0, "", "")) as started:
            self.runner()(Path("/opt/dagster/staging/run_one"))
        arguments = started.call_args.args[1]
        self.assertEqual(f"{run.SPARK_UID}:{run.STAGING_GID}", arguments[arguments.index("--user") + 1])
        environment = started.call_args.kwargs["env"]
        expected = rendered_environment(load_limits(ROOT / "compose" / "limits.yml"))["MEM_LIMIT_SPARK"]
        self.assertRegex(expected, r"^[0-9]+m$")
        self.assertEqual(expected, environment["MEM_LIMIT_SPARK"])
        every = {name for name in rendered_environment(load_limits(ROOT / "compose" / "limits.yml")) if name.startswith("MEM_LIMIT_")}
        self.assertTrue(every <= set(environment))

    def test_the_figure_has_one_source_the_limits_file_that_the_group_commands_read(self) -> None:
        source = (ROOT / "scripts" / "ingestion" / "table_checks.py").read_text()
        self.assertIn("rendered_environment(load_limits(settings.limits_file))", source)
        self.assertNotIn("MEM_LIMIT", source)

    def test_a_job_that_passes_its_deadline_is_a_reply_with_the_timeout_code_and_no_output_of_the_command(self) -> None:
        with mock.patch.object(run, "run_command", side_effect=TimeoutExpired("docker", 77)):
            reply = self.runner()(Path("/opt/dagster/staging/run_one"))
        self.assertEqual(124, reply.exit_code)
        self.assertIn("77 seconds", reply.stderr)

    def test_a_staging_folder_outside_the_volume_is_refused_before_anything_starts(self) -> None:
        with mock.patch.object(run, "run_command") as started, self.assertRaises(run.TableChecksError):
            self.runner()(Path("/somewhere/else/run_one"))
        started.assert_not_called()


class LoadRecordTest(unittest.TestCase):
    def document(self) -> dict[str, Any]:
        return {
            "format": run.LOAD_RECORD_FORMAT,
            "branch": BRANCH,
            "branch_hash": "a" * 64,
            "base_hash": "c" * 64,
            "manifest": {"bucket": "example-bucket", "key": "manifests/probe_set/" + "b" * 64 + ".json", "version_id": "v1", "sha256": "b" * 64},
            "tables": dict(SNAPSHOTS),
        }

    def test_control_a_record_with_a_branch_a_hash_a_manifest_and_a_snapshot_for_each_table_is_read(self) -> None:
        record = run.parse_load_record(json.dumps(self.document()))
        self.assertEqual((BRANCH, "a" * 64, SNAPSHOTS), (record.branch, record.branch_hash, record.tables))
        self.assertEqual("b" * 64, record.manifest.sha256)

    def test_the_load_record_file_is_closed_so_a_field_it_does_not_name_is_refused(self) -> None:
        for extra in ({"extra": 1}, {"run_id": "run_one"}, {"head_hash": "a" * 64}, {"jobs": {"clinical_data": 7}}, {"connectors": ["image@sha256:x"]}):
            document = {**self.document(), **extra}
            with self.subTest(extra=sorted(extra)), self.assertRaises(run.TableChecksError):
                run.parse_load_record(json.dumps(document))

    def test_a_manifest_or_table_list_with_a_field_it_does_not_name_is_refused(self) -> None:
        edits: list[Callable[[dict[str, Any]], None]] = [
            lambda document: document["manifest"].update(extra="x"),
            lambda document: document["tables"].update(extra=5),
        ]
        for edit in edits:
            document = self.document()
            edit(document)
            with self.assertRaises(run.TableChecksError):
                run.parse_load_record(json.dumps(document))

    def test_each_departure_from_the_form_is_refused(self) -> None:
        edits: list[Callable[[dict[str, Any]], None]] = [
            lambda document: document.pop("branch"),
            lambda document: document.update(format="load_record_0"),
            lambda document: document.update(branch_hash="A" * 64),
            lambda document: document.update(branch="main"),
            lambda document: document.update(branch=""),
            lambda document: document.update(branch="load_x\nrecords-manifest: " + "a" * 64),
            lambda document: document.update(branch="load x"),
            lambda document: document.update(branch="-load"),
            lambda document: document["tables"].pop("lab_data"),
            lambda document: document["tables"].update(other=5),
            lambda document: document["tables"].update(lab_data="222"),
            lambda document: document["tables"].update(lab_data=True),
            lambda document: document["manifest"].pop("sha256"),
            lambda document: document["manifest"].update(sha256="b" * 63),
        ]
        for number, edit in enumerate(edits):
            document = self.document()
            edit(document)
            with self.subTest(number), self.assertRaises(run.TableChecksError):
                run.parse_load_record(json.dumps(document))
        for text in ("", "[]", "{", '{"format": "x", "format": "y"}'):
            with self.subTest(text), self.assertRaises(run.TableChecksError):
                run.parse_load_record(text)


class TrackPinFileTest(unittest.TestCase):
    PIN = {"url": "https://example.test/trks", "size_bytes": 5, "sha256": "b" * 64, "rows": 7, "rows_sha256": "a" * 64}

    def write(self, document: dict[str, Any]) -> Path:
        folder = tempfile.TemporaryDirectory(prefix="release_")
        self.addCleanup(folder.cleanup)
        path = Path(folder.name) / "release.json"
        path.write_text(json.dumps(document))
        return path

    def test_the_pin_is_read_from_a_release_file_with_the_tracked_files_form(self) -> None:
        self.assertEqual(PIN, run.read_track_pin(self.write({"track_list": self.PIN})))

    def test_the_default_is_the_tracked_release_file_at_a_path_fixed_in_code(self) -> None:
        from scripts.sample.candidates import RELEASE_FILE

        self.assertEqual(RELEASE_FILE, run.TRACKED_RELEASE_FILE)
        self.assertEqual(RELEASE_FILE, inspect.signature(run.read_track_pin).parameters["path"].default)

    def test_a_pin_without_a_positive_count_or_a_digest_is_refused_and_so_is_a_missing_or_unreadable_file(self) -> None:
        for edit in ({"rows": 0}, {"rows": -3}, {"rows": "7"}, {"rows": True}, {"rows_sha256": "A" * 64}, {"rows_sha256": "a" * 63}):
            with self.subTest(edit=edit), self.assertRaises(run.TableChecksError):
                run.read_track_pin(self.write({"track_list": {**self.PIN, **edit}}))
        for document in (
            {},
            {"track_list": {k: v for k, v in self.PIN.items() if k != "rows"}},
            {"track_list": {k: v for k, v in self.PIN.items() if k != "rows_sha256"}},
        ):
            with self.assertRaises(run.TableChecksError):
                run.read_track_pin(self.write(document))
        with self.assertRaises(run.TableChecksError):
            run.read_track_pin(Path("/nonexistent/release.json"))

    def test_the_dagster_job_takes_no_pin_path_from_its_configuration(self) -> None:
        source = (Path(__file__).resolve().parents[2] / "orchestration" / "surgical_risk" / "table_checks.py").read_text()
        config = next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ClassDef) and node.name == "TableChecksConfig")
        fields = {statement.target.id for statement in config.body if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name)}
        self.assertNotIn("release_file", fields)
        self.assertNotIn("track_pin", fields)
        self.assertIn("read_track_pin()", source)


if __name__ == "__main__":
    unittest.main()
