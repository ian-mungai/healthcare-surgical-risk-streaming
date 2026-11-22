"""The order of the table checks: the gate, the staged verified bytes, the load record, the Spark job, then the merge, and the saved result.

A stand-in Nessie keeps references and the snapshot IDs of tables at each commit, and a stand-in runner plays the Spark job by printing the result line it
is told to print. The real job and the real Nessie are in ``tests/e2e/table_checks.py``. Each refusal has a passing control, and each refusal proves that the
job did not run and that nothing was merged.
"""

from __future__ import annotations

import ast
import fcntl
import hashlib
import inspect
import json
import tempfile
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
MARKER = "MARKER_9d2"


def setUpModule() -> None:
    pin_the_synthetic_list(b"".join(f"{hashlib.sha256(data).hexdigest()} {path}\n".encode() for path, data in sorted(SOURCE.items())))


def fake_hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


class FakeNessie(NessieClient):
    """References and the tables of each commit, answering the calls of the checks in the forms of Nessie's API version 2."""

    def __init__(self) -> None:
        super().__init__("http://nessie:19120/api/v2")
        self.heads = {"main": fake_hash("main", "0")}
        self.tables: dict[tuple[str, str], dict[str, int]] = {("main", self.heads["main"]): {}}
        self.calls: list[tuple[str, str]] = []
        self.merges: list[tuple[str, str, str]] = []
        self.merge_messages: list[str] = []
        self.commits: list[tuple[str, str]] = []
        self.lose_next_merge_reply = False
        self.on_branch_read: Callable[[], None] | None = None
        self.history_page_size = 100

    def load_branch(self, name: str, snapshots: dict[str, int]) -> str:
        self.heads[name] = fake_hash(name, "load")
        self.tables[(name, self.heads[name])] = dict(snapshots)
        return self.heads[name]

    def add_main_commit(self, message: str, tables: dict[str, int] | None = None) -> str:
        """A commit on main that is not a merge of this step, as another tool would write it."""
        self.heads["main"] = fake_hash("main", "other", message, str(len(self.commits)))
        self.tables[("main", self.heads["main"])] = dict(tables or {})
        self.commits.append((self.heads["main"], message))
        return self.heads["main"]

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
            return 200, json.dumps({"reference": {"type": "BRANCH", "name": name, "hash": self.heads[name]}}).encode()
        if method == "GET" and parts[1] == "history":
            start = int(parse_qs(urlsplit(target).query).get("page-token", ["0"])[0])
            entries = list(reversed(self.commits))[start : start + self.history_page_size]
            more = start + self.history_page_size < len(self.commits)
            document = {
                "logEntries": [{"commitMeta": {"hash": hash_value, "message": message}} for hash_value, message in entries],
                "hasMore": more,
                "token": str(start + len(entries)),
            }
            return 200, json.dumps(document).encode()
        if method == "GET" and parts[1] == "contents":
            table = parts[2].removeprefix(f"{rules.NAMESPACE}.")
            held = self.tables.get((name, pinned or self.heads[name]), {})
            if table not in held:
                return 404, json.dumps({"errorCode": "CONTENT_NOT_FOUND", "message": "no such table"}).encode()
            return 200, json.dumps({"content": {"type": "ICEBERG_TABLE", "snapshotId": held[table]}}).encode()
        if method == "POST" and parts[1:] == ["history", "merge"]:
            body = json.loads(payload or b"{}")
            if pinned != self.heads[name]:
                return 409, json.dumps({"errorCode": "REFERENCE_CONFLICT", "message": "the target moved"}).encode()
            self.merges.append((body["fromRefName"], body["fromHash"], name))
            self.merge_messages.append(body["message"])
            merged = {**self.tables[(name, self.heads[name])], **self.tables[(body["fromRefName"], body["fromHash"])]}
            self.heads[name] = fake_hash(name, "merged", body["fromHash"])
            self.tables[(name, self.heads[name])] = merged
            self.commits.append((self.heads[name], body["message"]))
            if self.lose_next_merge_reply:
                self.lose_next_merge_reply = False
                raise TimeoutError
            return 200, json.dumps({"wasApplied": True, "wasSuccessful": True, "resultantTargetHash": self.heads[name]}).encode()
        raise AssertionError(f"the stand-in has no answer for {method} {path}")


def result_line(plan: rules.Plan, *, passed: bool = True, change: Callable[[dict[str, Any]], None] | None = None) -> str:
    """The result line of a job that ran ``plan``: every required check passed, or ``lab_data`` stopped at a failed row count. ``change`` then edits it."""
    results = []
    for entry in plan.tables:
        table = rules.TableResult(entry.name, entry.snapshot_id)
        stop = 4 if not passed and entry.name == "lab_data" else 99
        for name in rules.REQUIRED_CHECKS[entry.name][:stop]:
            table.add(name, passed or entry.name != "lab_data" or name != "row count", "ok" if name != "row count" or passed else "row_count;source=2;table=3")
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
            code = 0 if text.startswith("RESULT ") and json.loads(text.removeprefix("RESULT "))["passed"] else 1
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
        self.landed: Landed = landed_set(SOURCE)
        self.reference: ManifestRef = publish_manifest(self.landed.store, self.landed.selection, self.landed.check())
        self.nessie = FakeNessie()
        self.branch_hash = self.nessie.load_branch(BRANCH, SNAPSHOTS)
        self.record = run.LoadRecord(BRANCH, self.branch_hash, self.reference, dict(SNAPSHOTS))

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


class MergeRecordTest(unittest.TestCase):
    def test_the_merge_message_holds_the_manifest_line_on_a_line_of_its_own(self) -> None:
        case = Case(self)
        case.go(Runner())
        (message,) = case.nessie.merge_messages
        self.assertIn(f"records-manifest: {case.reference.sha256}", message.splitlines())
        self.assertEqual(1, sum(line.startswith("records-manifest:") for line in message.splitlines()))

    def test_the_line_has_exactly_the_form_the_load_step_reads(self) -> None:
        self.assertEqual("records-manifest: " + "ab" * 32, run.manifest_line("ab" * 32))
        for value in ("", "ab" * 31, "AB" * 32, "ab" * 33, "gg" * 32, "ab" * 32 + "\n"):
            with self.subTest(value=value), self.assertRaises(run.TableChecksError):
                run.manifest_line(value)

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
                added.append(case.nessie.add_main_commit(f"merged elsewhere\n\n{run.manifest_line(case.reference.sha256)}", SNAPSHOTS))

        case.nessie.on_branch_read = another_tool_merges_it
        outcome = case.go(runner)
        self.assertEqual([], case.nessie.merges)
        self.assertTrue(outcome.merged)
        self.assertEqual(added[0], case.saved()["merged_hash"])

    def test_a_lost_reply_is_found_in_the_log_of_main_so_the_retry_sends_no_second_merge(self) -> None:
        case, runner = Case(self), Runner()
        case.nessie.lose_next_merge_reply = True
        with self.assertRaises(NessieError):
            case.go(runner)
        self.assertEqual(1, len(case.nessie.merges))
        self.assertEqual((True, False), (case.saved()["passed"], case.saved()["merged"]))
        again = case.go(runner)
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merges)))
        self.assertTrue(again.passed and again.merged)
        saved = case.saved()
        self.assertEqual((True, True, self.merge_commit(case)), (saved["passed"], saved["merged"], saved["merged_hash"]))
        self.assertEqual(1, len([message for message in case.nessie.merge_messages if run.manifest_line(case.reference.sha256) in message]))

    def test_a_receipt_that_was_lost_is_rebuilt_from_the_log_and_nothing_is_run_or_merged(self) -> None:
        case, runner = Case(self), Runner()
        case.go(runner)
        (case.run_folder / run.RESULT_FILE).unlink()
        again = case.go(runner)
        self.assertEqual((1, 1), (runner.calls, len(case.nessie.merges)))
        self.assertTrue(again.merged)
        self.assertEqual(self.merge_commit(case), case.saved()["merged_hash"])

    def test_the_log_is_searched_page_by_page_and_only_a_last_line_that_is_the_manifest_line_counts(self) -> None:
        case = Case(self)
        line = run.manifest_line(case.reference.sha256)
        case.nessie.history_page_size = 1
        self.assertIsNone(run.merged_commit(case.nessie, case.record))
        case.nessie.add_main_commit(f"note: {line} is mentioned\n\nin the middle")
        case.nessie.add_main_commit(f"{line}\n\nand then more text")
        case.nessie.add_main_commit(f"merge of another load\n\n{run.manifest_line('b' * 64)}")
        self.assertIsNone(run.merged_commit(case.nessie, case.record))
        found = case.nessie.add_main_commit(f"merge of this load\n\n{line}\n")
        for number in range(3):
            case.nessie.add_main_commit(f"later commit {number}")
        self.assertEqual(found, run.merged_commit(case.nessie, case.record))

    def test_a_log_too_long_to_search_is_refused_not_taken_for_unmerged(self) -> None:
        case = Case(self)
        case.nessie.history_page_size = 1
        for number in range(run.HISTORY_PAGE_LIMIT + 1):
            case.nessie.add_main_commit(f"commit {number}")
        with self.assertRaisesRegex(run.TableChecksError, "log"):
            run.merged_commit(case.nessie, case.record)

    def test_a_second_run_of_one_load_while_the_first_holds_the_lock_fails_with_a_clear_message_and_does_nothing(self) -> None:
        case, runner = Case(self), Runner()
        case.run_folder.mkdir(exist_ok=True)
        with (case.run_folder / run.LOCK_FILE).open("w") as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(run.TableChecksError, "another run of this load"):
                case.go(runner)
        self.assertEqual(0, runner.calls)
        self.assertTrue(case.main_untouched())

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
            case.go(runner, record=run.LoadRecord(BRANCH, case.branch_hash, other, dict(SNAPSHOTS)))
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
            case.go(runner, record=run.LoadRecord("load_other", case.branch_hash, case.reference, dict(SNAPSHOTS)))
        self.assert_nothing_ran(case, runner)

    def test_a_snapshot_that_differs_from_the_load_record_is_refused(self) -> None:
        case, runner = Case(self), Runner()
        with self.assertRaisesRegex(run.TableChecksError, "snapshot"):
            case.go(runner, record=run.LoadRecord(BRANCH, case.branch_hash, case.reference, {**SNAPSHOTS, "lab_data": 999}))
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

    def test_a_failed_check_with_summary_flags_that_say_passed_is_a_failed_run_and_merges_nothing(self) -> None:
        def lying(plan: rules.Plan) -> str:
            def flags(document: dict[str, Any]) -> None:
                document["tables"][1]["checks"][3].update(passed=False, detail="row_count;source=2;table=3")
                document["passed"] = True
                for table in document["tables"]:
                    table["passed"] = True

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
            other.go(Runner(), record=run.LoadRecord("load_run_two", other.nessie.heads["load_run_two"], other.reference, dict(SNAPSHOTS)))


class ComposeRunnerTest(unittest.TestCase):
    """The command that starts the job through the stack's Compose files: the batch profile, no dependency started, the staging volume read-only."""

    def settings(self) -> Settings:
        return Settings(compose_directory=Path("/opt/stack/compose"), project="stack_example", host_env=Path("/opt/stack/compose/host.env"))

    def runner(self) -> run.Runner:
        return run.compose_runner(self.settings(), volume="stack_example_table_check_staging", directory="/opt/dagster/staging", deadline_seconds=77)

    def test_control_the_command_names_the_job_the_volume_and_the_folder_of_this_run(self) -> None:
        with mock.patch.object(run, "run_command", return_value=CompletedProcess([], 0, "RESULT {}\n", "")) as started:
            reply = self.runner()(Path("/opt/dagster/staging/run_one"))
        program, arguments = started.call_args.args
        self.assertEqual("docker", program)
        self.assertEqual(77, started.call_args.kwargs["timeout"])
        text = " ".join(arguments)
        self.assertIn("compose --project-directory /opt/stack/compose", text)
        self.assertIn("--project-name stack_example", text)
        self.assertIn("--profile batch run --rm --no-TTY --no-deps", text)
        self.assertIn("--volume stack_example_table_check_staging:/staging:ro", text)
        self.assertTrue(text.endswith("--entrypoint python spark -m spark.jobs.table_checks --staging /staging/run_one"), text)
        self.assertEqual((0, "RESULT {}\n"), (reply.exit_code, reply.stdout))

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
