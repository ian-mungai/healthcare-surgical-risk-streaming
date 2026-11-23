"""Publication of a Bronze branch: the checks run at one hash of the branch, and only a branch that passed them at that hash is published, by assigning the
base to it.

The base reference is assigned to the checked commit itself, expecting the head that it had when the branch was made (Nessie 0.110.1, API v2, ``PUT
/trees/{name}@{hash}``). Each case plants one fault and names the guard that must stop the publication and leave the published reference where it was. The
Nessie side is a fake that records every assignment it is asked for and refuses one whose expected hash is not the base's head, as Nessie does.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Callable
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError, Reference
from spark.bronze.checks import GUARD_KEY, GUARD_STRUCTURE, CheckResult, Verdict, plan, verdict
from spark.bronze.publication import (
    GUARD_BASE_MOVED,
    GUARD_BRANCH_MOVED,
    Entry,
    NessiePublisher,
    PublicationError,
    publish,
    verify,
)

BRANCH, BASE = "bronze_run", "main"
HEAD, MOVED = "a" * 64, "b" * 64
BASE_HEAD = "c" * 64
KEYS = ["bronze.numeric_observations", "bronze.tracks"]
EXPECTED: dict[str, int | None] = {KEYS[0]: 10, KEYS[1]: 11}
NAMESPACE = Entry("NAMESPACE")


def table(snapshot: int) -> Entry:
    return Entry("ICEBERG_TABLE", f"s3://warehouse/metadata/{snapshot}.json", snapshot)


class FakeNessie:
    """The Nessie operations that publication needs, over dictionaries.

    ``assign`` refuses, as Nessie does, an expected hash that is not the head of the base. ``before_assign`` runs just before that comparison, so a test can
    move the base between the checks and the assignment. ``assign_error`` is raised after the assignment, and ``assign_applies`` says whether the assignment
    happened before it.
    """

    def __init__(self, base: str = BASE, keys: list[str] | None = None) -> None:
        self.base = base
        self.heads = {BRANCH: HEAD, base: BASE_HEAD}
        self.tags: set[str] = set()
        written = {key: table(10 + number) for number, key in enumerate(keys or KEYS)}
        self.entries_at = {f"{BRANCH}@{HEAD}": {"bronze": NAMESPACE, **written}, f"{base}@{BASE_HEAD}": {}}
        self.assign_calls: list[tuple[str, str, str, str]] = []
        self.assign_error: Exception | None = None
        self.assign_applies = False
        self.assign_answers: str | None = None
        self.before_assign: Callable[[], None] | None = None
        self.base_moved = False
        self.descends_calls: list[tuple[str, str, str]] = []
        self.head_reads = 0
        self.move_after_reads: int | None = None

    def is_branch(self, name: str) -> bool:
        return name in self.heads and name not in self.tags

    def head(self, ref: str) -> str:
        self.head_reads += 1
        if ref == BRANCH and self.move_after_reads is not None and self.head_reads > self.move_after_reads:
            return MOVED
        return self.heads[ref]

    def entries(self, reference: str) -> dict[str, Entry]:
        return dict(self.entries_at.get(reference, {}))

    def descends(self, branch: str, branch_hash: str, base_hash: str) -> bool:
        self.descends_calls.append((branch, branch_hash, base_hash))
        return not self.base_moved

    def assign(self, base: str, expected_base_hash: str, branch: str, branch_hash: str) -> str:
        self.assign_calls.append((base, expected_base_hash, branch, branch_hash))
        if self.before_assign is not None:
            self.before_assign()
        if self.heads[base] != expected_base_hash:
            raise NessieConflict("the base is not at the expected hash", status=409)
        if self.assign_error is not None and not self.assign_applies:
            raise self.assign_error
        self.heads[base] = branch_hash
        if self.assign_error is not None:
            raise self.assign_error
        return self.assign_answers or branch_hash


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
    def test_a_branch_that_passes_its_checks_is_published_by_assigning_the_base_to_the_checked_commit_expecting_the_head_that_was_read(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        result = publish(nessie, checker, BRANCH, BASE, EXPECTED)
        self.assertEqual("published", result.status)
        self.assertEqual([HEAD], checker.hashes)
        self.assertEqual([(BASE, BASE_HEAD, BRANCH, HEAD)], nessie.assign_calls)
        self.assertEqual((HEAD, BASE_HEAD, HEAD), (result.branch_hash, result.base_hash_before, result.published_hash))
        self.assertEqual(HEAD, nessie.heads[BASE])
        self.assertEqual({"bronze.numeric_observations": 10, "bronze.tracks": 11}, result.snapshots)
        self.assertEqual([], result.blocked_by)

    def test_the_branch_is_asked_at_the_checked_hash_whether_it_descends_from_the_base_head_that_was_read(self) -> None:
        nessie = FakeNessie()
        publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual([(BRANCH, HEAD, BASE_HEAD)], nessie.descends_calls)

    def test_a_second_run_on_a_published_branch_assigns_nothing_and_reports_unchanged_when_the_base_head_is_the_checked_hash(self) -> None:
        nessie = FakeNessie()
        publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        again = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("unchanged", HEAD), (again.status, again.published_hash))
        self.assertEqual(1, len(nessie.assign_calls))
        self.assertEqual({"bronze.numeric_observations": 10, "bronze.tracks": 11}, again.snapshots)

    def test_a_rerun_still_runs_the_checks_first(self) -> None:
        nessie = FakeNessie()
        publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        again = publish(nessie, Checker(failing("tracks", "key unique")), BRANCH, BASE, EXPECTED)
        self.assertEqual(("blocked", [GUARD_KEY]), (again.status, again.blocked_by))

    def test_verify_runs_the_checks_at_the_head_and_assigns_nothing(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        branch_hash, result = verify(nessie, checker, BRANCH)
        self.assertEqual((HEAD, True), (branch_hash, result.passed))
        self.assertEqual([], nessie.assign_calls)


class PlantedFaultTest(unittest.TestCase):
    def blocked(self, results: list[CheckResult], guard: str) -> None:
        nessie = FakeNessie()
        result = publish(nessie, Checker(results), BRANCH, BASE, EXPECTED)
        self.assertEqual("blocked", result.status)
        self.assertEqual([guard], result.blocked_by)
        self.assertEqual([], nessie.assign_calls)
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])
        self.assertIsNone(result.published_hash)
        self.assertEqual({}, result.snapshots)

    def test_a_missing_key_trips_the_key_guard_and_nothing_is_published(self) -> None:
        self.blocked(failing("numeric_observations", "case_id not null"), GUARD_KEY)

    def test_a_repeated_key_trips_the_key_guard_and_nothing_is_published(self) -> None:
        self.blocked(failing("tracks", "key unique"), GUARD_KEY)

    def test_a_structurally_corrupt_value_trips_the_structure_guard_and_nothing_is_published(self) -> None:
        self.blocked(failing("waveform_seconds", "samples hold a sample"), GUARD_STRUCTURE)

    def test_a_check_with_no_result_blocks_the_publication(self) -> None:
        self.blocked([CheckResult(check, True, 0) for check in plan() if check.name != "key unique"], GUARD_KEY)

    def test_a_branch_that_moves_while_it_is_checked_is_not_published_at_all(self) -> None:
        nessie = FakeNessie()
        nessie.move_after_reads = 1
        result = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("blocked", [GUARD_BRANCH_MOVED]), (result.status, result.blocked_by))
        self.assertEqual([], nessie.assign_calls)
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])

    def test_a_name_that_is_not_a_branch_is_refused_before_any_check_runs(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        nessie.tags.add(BRANCH)
        with self.assertRaises(PublicationError):
            publish(nessie, checker, BRANCH, BASE, EXPECTED)
        self.assertEqual([], checker.hashes)

    def test_the_base_cannot_be_the_branch(self) -> None:
        nessie, checker = FakeNessie(), Checker()
        with self.assertRaises(PublicationError):
            publish(nessie, checker, BRANCH, BRANCH, EXPECTED)
        self.assertEqual(([], []), (checker.hashes, nessie.assign_calls))

    def test_a_table_that_the_branch_does_not_hold_is_refused_before_any_assignment(self) -> None:
        nessie = FakeNessie()
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, {**EXPECTED, "bronze.waveform_seconds": None})
        self.assertIn("bronze.waveform_seconds", str(raised.exception))
        self.assertEqual([], nessie.assign_calls)


class BaseMovedTest(unittest.TestCase):
    """A base that is not the commit the branch was made on is never overwritten: the publication is blocked and the base stays as it is."""

    def test_a_base_that_is_not_an_ancestor_of_the_checked_hash_blocks_the_publication_before_the_assignment(self) -> None:
        nessie = FakeNessie()
        nessie.base_moved = True
        result = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("blocked", [GUARD_BASE_MOVED]), (result.status, result.blocked_by))
        self.assertEqual(([], BASE_HEAD), (nessie.assign_calls, nessie.heads[BASE]))
        self.assertEqual({}, result.snapshots)

    def test_a_base_that_moves_between_the_ancestry_check_and_the_assignment_is_refused_by_the_expected_hash_and_keeps_its_new_head(self) -> None:
        nessie = FakeNessie()
        nessie.before_assign = lambda: nessie.heads.update({BASE: MOVED})
        result = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("blocked", [GUARD_BASE_MOVED]), (result.status, result.blocked_by))
        self.assertEqual([(BASE, BASE_HEAD, BRANCH, HEAD)], nessie.assign_calls)
        self.assertEqual(MOVED, nessie.heads[BASE])
        self.assertIsNone(result.published_hash)

    def test_a_base_that_already_holds_the_checked_commit_is_unchanged_even_though_it_has_no_ancestor_relation_to_report(self) -> None:
        nessie = FakeNessie()
        nessie.heads[BASE] = HEAD
        nessie.base_moved = True
        self.assertEqual("unchanged", publish(nessie, Checker(), BRANCH, BASE, EXPECTED).status)
        self.assertEqual([], nessie.assign_calls)


class AssignOutcomeTest(unittest.TestCase):
    """An assignment that failed or timed out may have been applied. The base head says which, and the assignment is never sent twice."""

    def test_a_timeout_after_which_the_base_holds_the_checked_commit_is_a_publication(self) -> None:
        nessie = FakeNessie()
        nessie.assign_error, nessie.assign_applies = NessieError("the assignment did not finish within 30 seconds"), True
        result = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("published", HEAD), (result.status, result.published_hash))
        self.assertEqual(1, len(nessie.assign_calls))

    def test_a_timeout_after_which_the_base_is_as_it_was_is_an_error_that_changes_nothing_and_a_rerun_may_send_again(self) -> None:
        nessie = FakeNessie()
        nessie.assign_error = NessieError("the assignment did not finish within 30 seconds")
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertIn("not applied", str(raised.exception))
        self.assertEqual((1, BASE_HEAD), (len(nessie.assign_calls), nessie.heads[BASE]))
        nessie.assign_error = None
        self.assertEqual("published", publish(nessie, Checker(), BRANCH, BASE, EXPECTED).status)

    def test_a_timeout_after_which_the_base_holds_another_commit_is_a_moved_base(self) -> None:
        nessie = FakeNessie()
        nessie.assign_error = NessieError("the assignment did not finish within 30 seconds")
        original = nessie.assign

        def assign_and_move(base: str, expected: str, branch: str, branch_hash: str) -> str:
            try:
                return original(base, expected, branch, branch_hash)
            finally:
                nessie.heads[base] = MOVED

        nessie.assign = assign_and_move  # type: ignore[method-assign, assignment]
        result = publish(nessie, Checker(), BRANCH, BASE, EXPECTED)
        self.assertEqual(("blocked", [GUARD_BASE_MOVED]), (result.status, result.blocked_by))

    def test_an_answer_that_names_another_hash_than_the_checked_one_is_an_error(self) -> None:
        nessie = FakeNessie()
        nessie.assign_answers = MOVED
        with self.assertRaises(PublicationError):
            publish(nessie, Checker(), BRANCH, BASE, EXPECTED)


class OutputBindingTest(unittest.TestCase):
    """The branch differs from its base by exactly the tables that the job wrote, at the snapshots it wrote, and the namespaces that hold them if they are
    new."""

    def refused(self, nessie: FakeNessie, expected: dict[str, int | None] | None = None) -> str:
        with self.assertRaises(PublicationError) as raised:
            publish(nessie, Checker(), BRANCH, BASE, EXPECTED if expected is None else expected)
        self.assertEqual([], nessie.assign_calls)
        self.assertEqual(BASE_HEAD, nessie.heads[BASE])
        return str(raised.exception)

    def test_a_table_that_the_branch_changes_and_the_job_did_not_write_is_refused_before_any_assignment(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["bronze.waveform_seconds"] = table(77)
        self.assertIn("bronze.waveform_seconds", self.refused(nessie))

    def test_a_table_of_the_job_that_the_branch_does_not_change_is_refused_because_it_was_inherited(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BASE}@{BASE_HEAD}"] = {KEYS[1]: table(11)}
        self.assertIn(KEYS[1], self.refused(nessie))

    def test_a_namespace_that_is_not_a_parent_of_a_written_table_is_refused(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["other"] = NAMESPACE
        self.assertIn("other", self.refused(nessie))

    def test_a_namespace_that_holds_the_tables_and_is_new_on_the_branch_is_the_only_namespace_change_that_is_allowed(self) -> None:
        self.assertEqual("published", publish(FakeNessie(), Checker(), BRANCH, BASE, EXPECTED).status)

    def test_an_edit_of_an_existing_namespace_that_holds_the_tables_is_refused(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BASE}@{BASE_HEAD}"] = {"bronze": Entry("NAMESPACE", content='{"properties": {"a": "1"}}')}
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["bronze"] = Entry("NAMESPACE", content='{"properties": {"a": "2"}}')
        self.assertIn("bronze", self.refused(nessie))

    def test_an_existing_namespace_that_the_branch_leaves_as_it_is_may_hold_the_new_tables(self) -> None:
        nessie = FakeNessie()
        same = Entry("NAMESPACE", content='{"properties": {"a": "1"}}')
        nessie.entries_at[f"{BASE}@{BASE_HEAD}"] = {"bronze": same}
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["bronze"] = same
        self.assertEqual("published", publish(nessie, Checker(), BRANCH, BASE, EXPECTED).status)

    def test_a_namespace_that_the_branch_removes_is_refused(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BASE}@{BASE_HEAD}"] = {"gold": NAMESPACE}
        self.assertIn("gold", self.refused(nessie))

    def test_a_snapshot_other_than_the_one_the_job_wrote_is_refused(self) -> None:
        self.assertIn(KEYS[0], self.refused(FakeNessie(), {KEYS[0]: 12, KEYS[1]: 11}))

    def test_a_table_that_the_job_wrote_and_the_branch_does_not_hold_is_refused(self) -> None:
        self.assertIn("bronze.waveform_seconds", self.refused(FakeNessie(), {**EXPECTED, "bronze.waveform_seconds": 5}))

    def test_a_standalone_publication_that_knows_no_snapshot_publishes_the_exact_set_of_tables(self) -> None:
        self.assertEqual("published", publish(FakeNessie(), Checker(), BRANCH, BASE, {KEYS[0]: None, KEYS[1]: None}).status)

    def test_a_standalone_publication_that_knows_no_snapshot_still_refuses_another_table(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["bronze.waveform_seconds"] = table(77)
        self.assertIn("bronze.waveform_seconds", self.refused(nessie, {KEYS[0]: None, KEYS[1]: None}))

    def test_an_entry_of_another_table_that_differs_only_in_its_content_is_refused(self) -> None:
        nessie = FakeNessie()
        nessie.entries_at[f"{BASE}@{BASE_HEAD}"] = {"bronze.other": Entry("ICEBERG_TABLE", "s3://w/o.json", 3, '{"schemaId": 0}')}
        nessie.entries_at[f"{BRANCH}@{HEAD}"]["bronze.other"] = Entry("ICEBERG_TABLE", "s3://w/o.json", 3, '{"schemaId": 1}')
        self.assertIn("bronze.other", self.refused(nessie))


class AssignTest(unittest.TestCase):
    """The client's conditional assignment: Nessie 0.110.1 API v2, ``PUT /trees/{name}@{expected hash}?type=BRANCH`` with the reference to assign to as the
    body."""

    def assign(self, status: int, answer: dict[str, Any]) -> tuple[Any, ScriptedClient]:
        client = ScriptedClient({f"PUT /api/v2/trees/{BASE}@{BASE_HEAD}?type=BRANCH": (status, answer)})
        return NessiePublisher(client).assign(BASE, BASE_HEAD, BRANCH, HEAD), client

    def test_the_request_names_the_base_and_its_expected_head_in_the_path_and_the_branch_at_the_checked_hash_in_the_body(self) -> None:
        found, client = self.assign(200, {"reference": {"type": "BRANCH", "name": BASE, "hash": HEAD}})
        self.assertEqual(HEAD, found)
        self.assertEqual([("PUT", f"/api/v2/trees/{BASE}@{BASE_HEAD}?type=BRANCH", {"type": "BRANCH", "name": BRANCH, "hash": HEAD})], client.requests)

    def test_a_refusal_because_the_expected_hash_is_not_the_head_is_a_conflict(self) -> None:
        with self.assertRaises(NessieConflict):
            self.assign(409, {"errorCode": "REFERENCE_CONFLICT", "message": "x"})

    def test_an_answer_that_names_no_reference_is_an_error(self) -> None:
        with self.assertRaises(NessieError):
            self.assign(200, {"unexpected": True})

    def test_the_client_refuses_a_hash_that_is_not_sixty_four_hex_digits_before_it_sends_anything(self) -> None:
        client = ScriptedClient({})
        with self.assertRaises(ValueError):
            NessieClient.assign_branch(client, BASE, "x", Reference("BRANCH", BRANCH, HEAD))
        with self.assertRaises(ValueError):
            NessieClient.assign_branch(client, BASE, BASE_HEAD, Reference("BRANCH", BRANCH, "y"))
        self.assertEqual([], client.requests)


class ScriptedClient(NessieClient):
    """A Nessie client that answers each request from a script keyed by method and target, and records it. It starts no server."""

    def __init__(self, script: dict[str, tuple[int, Any]]) -> None:
        super().__init__("http://nessie.invalid:19120/api/v2", 5)
        self.script = script
        self.requests: list[tuple[str, str, Any]] = []

    def send(self, method: str, target: str, payload: bytes | None) -> tuple[int, bytes]:
        self.requests.append((method, target, json.loads(payload) if payload else None))
        status, document = self.script[f"{method} {target}"]
        return status, json.dumps(document).encode()


def table_item(name: list[str], location: Any = "s3://w/m.json", snapshot: Any = 5) -> dict[str, Any]:
    return {"type": "ICEBERG_TABLE", "name": {"elements": name}, "content": {"type": "ICEBERG_TABLE", "metadataLocation": location, "snapshotId": snapshot}}


class EntriesTest(unittest.TestCase):
    def read(self, items: list[dict[str, Any]]) -> dict[str, Entry]:
        target = f"/api/v2/trees/main@{BASE_HEAD}/entries?content=true&max-records=100"
        client = ScriptedClient({f"GET {target}": (200, {"entries": items, "hasMore": False})})
        found = NessiePublisher(client).entries(f"main@{BASE_HEAD}")
        self.assertEqual([("GET", target, None)], client.requests)
        return found

    def test_a_table_is_its_dotted_name_with_its_metadata_location_and_snapshot_and_a_namespace_is_its_kind_alone(self) -> None:
        found = self.read(
            [
                table_item(["bronze", "tracks"]),
                {"type": "NAMESPACE", "name": {"elements": ["bronze"]}, "content": {"type": "NAMESPACE", "properties": {"a": "1"}}},
            ]
        )
        self.assertEqual(
            {
                "bronze.tracks": Entry("ICEBERG_TABLE", "s3://w/m.json", 5, json.dumps(table_item(["bronze", "tracks"])["content"], sort_keys=True)),
                "bronze": Entry("NAMESPACE", content=json.dumps({"properties": {"a": "1"}, "type": "NAMESPACE"}, sort_keys=True)),
            },
            found,
        )

    def test_two_namespaces_that_differ_only_in_a_property_are_two_entries(self) -> None:
        first = self.read([{"type": "NAMESPACE", "name": {"elements": ["a"]}, "content": {"type": "NAMESPACE", "properties": {"k": "1"}}}])
        second = self.read([{"type": "NAMESPACE", "name": {"elements": ["a"]}, "content": {"type": "NAMESPACE", "properties": {"k": "2"}}}])
        self.assertNotEqual(first["a"], second["a"])

    def test_a_table_with_no_snapshot_is_none_and_not_a_number(self) -> None:
        self.assertEqual(None, self.read([table_item(["bronze", "tracks"], snapshot=-1)])["bronze.tracks"].snapshot_id)

    def test_an_entry_that_is_incomplete_is_an_error_and_not_a_missing_table(self) -> None:
        for item in (
            table_item(["bronze", "tracks"], location=None),
            table_item(["bronze", "tracks"], snapshot="5"),
            table_item(["bronze", "tracks"], snapshot=True),
            {"type": "ICEBERG_TABLE", "name": {"elements": []}},
            {"name": {"elements": ["a"]}},
        ):
            with self.assertRaises(NessieError):
                self.read([item])


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


class DescendsTest(unittest.TestCase):
    def ask(self, pages: list[dict[str, Any]]) -> tuple[bool, LogClient]:
        client = LogClient(pages)
        return NessiePublisher(client).descends(BRANCH, HEAD, BASE_HEAD), client

    def test_the_branch_descends_from_a_base_head_that_is_a_commit_in_its_log_at_the_checked_hash(self) -> None:
        found, client = self.ask([page([entry(HEAD, "tip"), entry(BASE_HEAD, "base")])])
        self.assertTrue(found)
        self.assertEqual([("GET", f"/api/v2/trees/{BRANCH}@{HEAD}/history?max-records=500")], client.requests)

    def test_a_branch_whose_log_does_not_hold_the_base_head_does_not_descend_from_it(self) -> None:
        self.assertFalse(self.ask([page([entry(HEAD, "tip"), entry(MOVED, "other")])])[0])

    def test_the_log_is_followed_through_its_pages(self) -> None:
        found, client = self.ask([page([entry(HEAD, "tip")], "t1"), page([entry(BASE_HEAD, "base")])])
        self.assertTrue(found)
        self.assertIn("page-token=t1", client.requests[1][1])

    def test_a_log_that_is_not_in_the_expected_form_is_an_error_and_not_a_branch_that_does_not_descend(self) -> None:
        with self.assertRaises(NessieError):
            self.ask([{"logEntries": "x", "hasMore": False}])
