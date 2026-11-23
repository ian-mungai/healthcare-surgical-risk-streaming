"""Publication of a Bronze branch: the checks run at one hash of the branch, and only a branch that passed them at that hash is merged, once.

Each case plants one fault and names the guard that must stop the merge and leave the published reference where it was. The Nessie side is a fake that records
every merge it is asked for, so "nothing was published" is a count of zero merges and an unchanged head of the base.
"""

from __future__ import annotations

import json
import unittest
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.bronze.checks import GUARD_KEY, GUARD_STRUCTURE, CheckResult, Verdict, plan, verdict
from spark.bronze.publication import (
    GUARD_BRANCH_MOVED,
    MergeOutcomeOpen,
    NessiePublisher,
    PublicationError,
    merge_message,
    publish,
    verify,
)
from spark.bronze_clinical_lab import TableContent

BRANCH, BASE = "bronze_run", "main"
HEAD, MOVED = "a" * 64, "b" * 64
BASE_HEAD, MERGED = "c" * 64, "d" * 64
KEYS = ["bronze.numeric_observations", "bronze.tracks"]


def content(snapshot: int) -> TableContent:
    return TableContent(f"s3://warehouse/metadata/{snapshot}.json", snapshot)


class FakeNessie:
    """The Nessie operations that publication needs, over dictionaries.

    ``merge_error`` is raised by the merge, and ``applies`` says whether it merged anyway.
    """

    def __init__(self, base: str = BASE, keys: list[str] | None = None) -> None:
        self.base = base
        self.heads = {BRANCH: HEAD, base: BASE_HEAD}
        self.tags: set[str] = set()
        self.contents = {f"{BRANCH}@{HEAD}": {key: content(10 + number) for number, key in enumerate(keys or KEYS)}, f"{base}@{BASE_HEAD}": {}}
        self.merged: dict[tuple[str, str], str] = {}
        self.merge_calls: list[tuple[str, str, str, str, str]] = []
        self.merge_error: Exception | None = None
        self.applies = False
        self.merge_changes_content = False
        self.head_reads = 0
        self.move_after_reads: int | None = None

    def is_branch(self, name: str) -> bool:
        return name in self.heads and name not in self.tags

    def head(self, ref: str) -> str:
        self.head_reads += 1
        if ref == BRANCH and self.move_after_reads is not None and self.head_reads > self.move_after_reads:
            return MOVED
        return self.heads[ref]

    def content(self, reference: str, key: str) -> TableContent | None:
        return self.contents.get(reference, {}).get(key)

    def merge_commit(self, base: str, branch: str, branch_hash: str) -> str | None:
        return self.merged.get((branch, branch_hash))

    def merge(self, branch: str, base: str, expected_base_hash: str, branch_hash: str, message: str) -> str:
        self.merge_calls.append((branch, base, expected_base_hash, branch_hash, message))
        if self.applies:
            self.land(branch, branch_hash)
        if self.merge_error is not None:
            raise self.merge_error
        return self.land(branch, branch_hash)

    def land(self, branch: str, branch_hash: str) -> str:
        self.merged[(branch, branch_hash)] = MERGED
        self.heads[self.base] = MERGED
        source = self.contents[f"{branch}@{branch_hash}"]
        self.contents[f"{self.base}@{MERGED}"] = {key: content(99) if self.merge_changes_content else value for key, value in source.items()}
        return MERGED


class Checker:
    """The checks of the Spark side: it records the hash it was asked to check and answers with the results it is given."""

    def __init__(self, results: list[CheckResult] | None = None) -> None:
        self.results = results
        self.hashes: list[str] = []

    def __call__(self, branch_hash: str) -> Verdict:
        self.hashes.append(branch_hash)
        return verdict(plan(), self.results if self.results is not None else [CheckResult(check, True, 0) for check in plan()])


def failing(table: str, name: str) -> list[CheckResult]:
    return [CheckResult(check, (check.table, check.name) != (table, name), 0 if (check.table, check.name) != (table, name) else 2) for check in plan()]


class PassingRunTest(unittest.TestCase):
    def test_a_branch_that_passes_its_checks_is_merged_once_at_the_hash_that_was_checked_into_the_head_that_was_read(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        result = publish(nessie, checker, BRANCH, BASE, KEYS)
        self.assertEqual("merged", result.status)
        self.assertEqual([HEAD], checker.hashes)
        self.assertEqual([(BRANCH, BASE, BASE_HEAD, HEAD, merge_message(BRANCH, HEAD))], nessie.merge_calls)
        self.assertEqual((HEAD, BASE_HEAD, MERGED), (result.branch_hash, result.base_hash_before, result.merged_hash))
        self.assertEqual({"bronze.numeric_observations": 10, "bronze.tracks": 11}, result.snapshots)
        self.assertEqual([], result.blocked_by)

    def test_the_merge_message_names_the_branch_and_the_checked_hash_in_its_last_two_lines(self) -> None:
        lines = merge_message(BRANCH, HEAD).split("\n")
        self.assertEqual([f"bronze-branch: {BRANCH}", f"bronze-source: {HEAD}"], lines[-2:])

    def test_a_second_run_on_a_merged_branch_merges_nothing_and_reports_the_merge_it_finds(self) -> None:
        nessie = FakeNessie()
        publish(nessie, Checker(), BRANCH, BASE, KEYS)
        again = publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertEqual("unchanged", again.status)
        self.assertEqual(1, len(nessie.merge_calls))
        self.assertEqual(MERGED, again.merged_hash)
        self.assertEqual({"bronze.numeric_observations": 10, "bronze.tracks": 11}, again.snapshots)

    def test_verify_runs_the_checks_at_the_head_and_merges_nothing(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        branch_hash, result = verify(nessie, checker, BRANCH)
        self.assertEqual((HEAD, True), (branch_hash, result.passed))
        self.assertEqual([], nessie.merge_calls)


class PlantedFaultTest(unittest.TestCase):
    def blocked(self, results: list[CheckResult], guard: str) -> None:
        nessie = FakeNessie()
        result = publish(nessie, Checker(results), BRANCH, BASE, KEYS)
        self.assertEqual("blocked", result.status)
        self.assertEqual([guard], result.blocked_by)
        self.assertEqual([], nessie.merge_calls)
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])
        self.assertIsNone(result.merged_hash)
        self.assertEqual({}, result.snapshots)

    def test_a_missing_key_trips_the_key_guard_and_nothing_is_merged(self) -> None:
        self.blocked(failing("numeric_observations", "case_id not null"), GUARD_KEY)

    def test_a_repeated_key_trips_the_key_guard_and_nothing_is_merged(self) -> None:
        self.blocked(failing("tracks", "key unique"), GUARD_KEY)

    def test_a_structurally_corrupt_value_trips_the_structure_guard_and_nothing_is_merged(self) -> None:
        self.blocked(failing("waveform_seconds", "samples hold a sample"), GUARD_STRUCTURE)

    def test_a_check_with_no_result_blocks_the_merge(self) -> None:
        self.blocked([CheckResult(check, True, 0) for check in plan() if check.name != "key unique"], GUARD_KEY)

    def test_a_branch_that_moves_while_it_is_checked_is_not_merged_at_all(self) -> None:
        nessie = FakeNessie()
        nessie.move_after_reads = 1
        result = publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertEqual(("blocked", [GUARD_BRANCH_MOVED]), (result.status, result.blocked_by))
        self.assertEqual([], nessie.merge_calls)
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])

    def test_a_name_that_is_not_a_branch_is_refused_before_any_check_runs(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        nessie.tags.add(BRANCH)
        with self.assertRaises(PublicationError):
            publish(nessie, checker, BRANCH, BASE, KEYS)
        self.assertEqual([], checker.hashes)

    def test_the_base_cannot_be_the_branch(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        with self.assertRaises(PublicationError):
            publish(nessie, checker, BRANCH, BRANCH, KEYS)
        self.assertEqual(([], []), (checker.hashes, nessie.merge_calls))

    def test_a_table_that_the_branch_does_not_hold_is_refused_before_any_merge(self) -> None:
        nessie = FakeNessie()
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, [*KEYS, "bronze.waveform_seconds"])
        self.assertIn("bronze.waveform_seconds", str(raised.exception))
        self.assertEqual([], nessie.merge_calls)


class MergeFailureTest(unittest.TestCase):
    def test_a_base_that_moved_is_a_refusal_with_the_reference_unchanged_and_no_second_request(self) -> None:
        nessie = FakeNessie()
        nessie.merge_error = NessieConflict("the base moved", status=409)
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertNotIsInstance(raised.exception, MergeOutcomeOpen)
        self.assertEqual(1, len(nessie.merge_calls))
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])

    def test_a_merge_that_timed_out_and_was_applied_is_found_in_the_history_and_not_sent_again(self) -> None:
        nessie = FakeNessie()
        nessie.merge_error, nessie.applies = NessieError("the merge did not finish within 30 seconds"), True
        result = publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertEqual(("merged", MERGED), (result.status, result.merged_hash))
        self.assertEqual(1, len(nessie.merge_calls))

    def test_a_merge_that_timed_out_and_is_not_in_the_history_is_an_open_outcome_and_is_not_sent_again(self) -> None:
        nessie = FakeNessie()
        nessie.merge_error = NessieError("the merge did not finish within 30 seconds")
        with self.assertRaises(MergeOutcomeOpen):
            publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertEqual(1, len(nessie.merge_calls))

    def test_a_merge_whose_tables_are_not_the_checked_tables_is_refused(self) -> None:
        nessie = FakeNessie()
        nessie.merge_changes_content = True
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, KEYS)
        self.assertIn("not the tables that were checked", str(raised.exception))


class LogClient(NessieClient):
    """A Nessie client that answers the log of the base with prepared pages and records each request. It starts no server."""

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        super().__init__("http://nessie.invalid:19120/api/v2", 5)
        self.pages = pages
        self.requests: list[tuple[str, str]] = []

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.requests.append((method, target))
        return 200, json.dumps(self.pages[min(len(self.requests), len(self.pages)) - 1]).encode()


def entry(commit: str, message: str) -> dict[str, Any]:
    return {"commitMeta": {"hash": commit, "message": message}}


def page(entries: list[dict[str, Any]], token: str | None = None) -> dict[str, Any]:
    return {"logEntries": entries, "hasMore": token is not None, **({"token": token} if token else {})}


class HistorySearchTest(unittest.TestCase):
    def find(self, pages: list[dict[str, Any]], branch: str = BRANCH, branch_hash: str = HEAD) -> tuple[str | None, LogClient]:
        client = LogClient(pages)
        return NessiePublisher(client).merge_commit(BASE, branch, branch_hash), client

    def test_the_merge_is_the_commit_whose_last_two_lines_name_the_branch_and_the_checked_hash(self) -> None:
        other = merge_message("other_run", HEAD)
        found, client = self.find([page([entry("1" * 64, other), entry(MERGED, merge_message(BRANCH, HEAD)), entry("2" * 64, "plain")])])
        self.assertEqual(MERGED, found)
        self.assertEqual([("GET", "/api/v2/trees/main/history?max-records=500")], client.requests)

    def test_a_merge_of_the_same_branch_at_another_hash_is_not_this_merge(self) -> None:
        self.assertIsNone(self.find([page([entry(MERGED, merge_message(BRANCH, MOVED))])])[0])

    def test_the_search_follows_the_pages_until_it_finds_the_merge_or_the_log_ends(self) -> None:
        pages = [page([entry("1" * 64, "x")], "t1"), page([entry(MERGED, merge_message(BRANCH, HEAD))], "t2")]
        found, client = self.find(pages)
        self.assertEqual(MERGED, found)
        self.assertEqual(2, len(client.requests))
        self.assertIn("page-token=t1", client.requests[1][1])
        self.assertIsNone(self.find([page([entry("1" * 64, "x")], "t1"), page([entry("2" * 64, "y")])])[0])

    def test_a_log_that_is_not_in_the_expected_form_is_an_error_and_not_an_empty_log(self) -> None:
        bad: dict[str, Any]
        for bad in (
            {"logEntries": "x", "hasMore": False},
            {"logEntries": [], "hasMore": "no"},
            {"logEntries": [{"commitMeta": {"hash": 1}}], "hasMore": False},
            {"logEntries": []},
        ):
            with self.assertRaises(NessieError):
                self.find([bad])

    def test_more_pages_with_no_token_and_a_log_with_no_end_are_errors(self) -> None:
        with self.assertRaises(NessieError):
            self.find([{"logEntries": [], "hasMore": True}])
        with self.assertRaises(NessieError):
            self.find([page([entry("1" * 64, "x")], "again")])


if __name__ == "__main__":
    unittest.main()
