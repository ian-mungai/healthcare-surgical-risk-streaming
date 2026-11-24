"""The patient split, Silver signals and Silver hypotension runners merge the hash that the verify checked, and only when the branch is still at it.

The runners stay untracked, so each test skips when its runner is not on this machine. Nessie, the stack and the stand-in answers are fakes. No Docker or Spark
runs here.

Failure modes covered here:

- A runner merged ``nessie.reference(branch).hash`` after the verify, so a branch that moved between the verify and the merge had an unverified commit merged.
- A runner went on to the merge after a verify that failed, or after a branch that moved, and the merge case still passed.
- A reply of a verify call that names another mode or another branch than the one that was asked for was accepted by a launcher.
- The Silver cases runner merged the sample-sized branch (and the full branch) after its head had moved since the verify, the merge taking the verified hash
  only by name.
- A verify that exited 0 with a false pass flag was a pass, so the merge that follows took an unverified table (Silver signals and Silver hypotension).
"""

from __future__ import annotations

import contextlib
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e.run_record import Record
from tests.fast.silver_accept import Catalog, accept_keep
from tests.fast.test_outcome_acceptance import split_run
from tests.fast.test_runner_gold_sealed import runner
from tests.fast.test_runner_setup_builds import stand_in_stack
from tests.fast.test_silver_acceptance import HEAD, HYPO_SNAPSHOTS, HYPO_SOURCES, hypotension, verified
from tests.fast.test_silver_acceptance import VERIFIED as VERIFY_CATALOG

VERIFIED, MOVED, BASE = "b" * 64, "c" * 64, "a" * 64
BRANCH = "p6_run_branch_x1"
RUNNERS = (
    ("silver_patient_split", "merge_and_rebuild", "verify_control"),
    ("silver_signals", "merge_tag_rebuild", "verify_control"),
    ("silver_hypotension", "merge_tag_rebuild", "verify"),
)


class Stop(Exception):
    """Raised by the fake Nessie once it has seen the merge call, so that a test reads the call and nothing after it."""


class Nessie:
    """A Nessie whose branch holds the verified hash, or the hash of a later commit (``moved``)."""

    def __init__(self, moved: bool = False) -> None:
        self.hash = MOVED if moved else VERIFIED
        self.merges: list[dict[str, Any]] = []

    def reference(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(hash=self.hash, kind="BRANCH")

    def merge(self, *arguments: Any, **keywords: Any) -> str:
        self.merges.append({"arguments": arguments, **keywords})
        raise Stop


def flow(module: Any, nessie: Nessie, branch_head: str) -> Any:
    """A run of the runner with the fakes that its merge step reads. ``branch_head`` is what ``head`` answers for the branch that was verified."""
    run = module.Run.__new__(module.Run)
    run.record, run.nessie, run.trunk, run.suffix, run.facts = Record(feature="test", slug="test", rerun="none"), nessie, "trunk_x1", "x1", {}
    run.tags, run.made, run.counter = [], [], 0
    run.head = lambda name: branch_head if name == BRANCH else BASE
    return run


class MergeTheVerifiedHash(unittest.TestCase):
    def test_the_merge_names_the_hash_that_the_verify_checked_and_not_the_hash_that_the_branch_holds_when_the_merge_is_made(self) -> None:
        for name, merge, _ in RUNNERS:
            with self.subTest(name):
                module = runner(name)
                nessie = Nessie(moved=True)
                run = flow(module, nessie, VERIFIED)
                with self.assertRaises(Stop):
                    getattr(run, merge)({"branch": BRANCH, "branch_hash": VERIFIED})
                self.assertEqual(1, len(nessie.merges))
                self.assertEqual(VERIFIED, nessie.merges[0]["source_hash"])
                self.assertNotIn(MOVED, json.dumps(nessie.merges, default=str))

    def test_a_branch_that_moved_after_the_verify_fails_the_merge_case_and_nothing_is_merged(self) -> None:
        for name, merge, _ in RUNNERS:
            with self.subTest(name):
                module = runner(name)
                nessie = Nessie(moved=True)
                run = flow(module, nessie, MOVED)
                getattr(run, merge)({"branch": BRANCH, "branch_hash": VERIFIED})
                self.assertEqual([], nessie.merges)
                failed = [item for item in run.record.observations if not item.passed]
                self.assertEqual(1, len(failed), [item.observed for item in run.record.observations])
                self.assertIn("still at the verified hash False", failed[0].observed)
                self.assertNotIn(MOVED, json.dumps([item.observed for item in run.record.observations]))
                self.assertEqual({}, run.facts)


class GateOnTheVerify(unittest.TestCase):
    """``cases`` goes on to the merge only when the verify passed (the class of the Gold gate that the profile runner already has)."""

    def calls(self, name: str, verified: bool) -> list[str]:
        module = runner(name)
        calls: list[str] = []

        def step(label: str, answer: Any = None) -> Any:
            def call(*_: Any, **__: Any) -> Any:
                calls.append(label)
                return answer

            return call

        built = {"branch": BRANCH, "branch_hash": VERIFIED}
        steps = {
            "setup": step("setup", True),
            "first_build": step("first_build", built),
            "control": step("control", built),
            "independent": step("independent"),
            "independent_read": step("independent_read"),
            "control_events": step("control_events"),
            "verify_control": step("verify", verified),
            "verify": step("verify", verified),
            "merge_and_rebuild": step("merge"),
            "merge_tag_rebuild": step("merge"),
        }
        run = SimpleNamespace(facts={}, **steps)
        for later in ("plant", "plant_numeric", "plant_waveform", "bronze_faults", "empty_rebuild", "tampers", "moved_null", "bounded_failure"):
            setattr(run, later, step("later " + later))
        for later in ("planted", "sample_sized", "faults"):
            setattr(run, later, step("later " + later))
        run.wave_built = {}
        module.cases(run)
        return calls

    def test_a_verify_that_failed_is_not_followed_by_a_merge_or_a_later_case(self) -> None:
        for name, _, _ in RUNNERS:
            with self.subTest(name):
                calls = self.calls(name, False)
                self.assertIn("verify", calls)
                self.assertNotIn("merge", calls)
                self.assertEqual([], [call for call in calls if call.startswith("later")])

    def test_a_verify_that_passed_is_followed_by_the_merge(self) -> None:
        for name, _, _ in RUNNERS:
            with self.subTest(name):
                calls = self.calls(name, True)
                self.assertLess(calls.index("verify"), calls.index("merge"))


class VerifyVerdict(unittest.TestCase):
    """The verify of each runner gives its verdict as a Boolean, which ``cases`` reads: true for the reply to the request and false for any other."""

    def run_of(self, name: str, answer: dict[str, Any] | None) -> Any:
        module = runner(name)
        run = flow(module, Nessie(), VERIFIED)
        if name == "silver_patient_split":
            run.stack = SimpleNamespace(split=lambda *_, **__: (0 if answer else 1, answer or {}, "", 0.1))
            return run
        done = SimpleNamespace(
            keep=lambda: SimpleNamespace(ok=answer is not None, result=answer or {}, reason=None if answer else "the exit code is not the expected one"),
            code=0 if answer else 1,
            seconds=0.1,
            catalog=None,
        )
        run.silver = lambda *_, **__: done
        return run

    def verdict(self, name: str, answer: dict[str, Any] | None) -> bool:
        run = self.run_of(name, answer)
        built = {"branch": BRANCH, "branch_hash": VERIFIED}
        if name == "silver_hypotension":
            return bool(run.verify(BRANCH, "the fixed sample", VERIFIED))
        return bool(run.verify_control(built))

    def healthy(self, name: str) -> dict[str, Any]:
        if name == "silver_patient_split":
            return {**split_run("verify")[1], "branch": BRANCH, "branch_hash": VERIFIED, "write_hash": None}
        return {"branch_hash": VERIFIED}

    def test_a_verify_that_answers_as_asked_gives_true(self) -> None:
        for name, _, _ in RUNNERS:
            with self.subTest(name):
                self.assertIs(True, self.verdict(name, self.healthy(name)))

    def test_a_verify_that_fails_or_names_another_hash_gives_false(self) -> None:
        for name, _, _ in RUNNERS:
            with self.subTest(name, case="no accepted reply"):
                self.assertIs(False, self.verdict(name, None))
            with self.subTest(name, case="another hash"):
                self.assertIs(False, self.verdict(name, {**self.healthy(name), "branch_hash": MOVED}))


class VerifyReplies(unittest.TestCase):
    """A reply of the verify launcher that is not the answer to the request is discarded."""

    def reply(self, **edits: Any) -> dict[str, Any]:
        code, document = split_run("verify")
        self.assertEqual(0, code)
        return {**document, "branch": BRANCH, **edits}

    def test_the_patient_split_launcher_accepts_the_reply_to_its_request(self) -> None:
        module = runner("silver_patient_split")
        stack, _, _ = stand_in_stack(self, module, self.reply(), 0)
        code, result, _, _ = stack.split("verify", BRANCH)
        self.assertEqual((0, "verify", BRANCH), (code, result["mode"], result["branch"]))

    def test_the_patient_split_launcher_refuses_a_reply_of_another_mode_or_another_branch(self) -> None:
        module = runner("silver_patient_split")
        for label, edits in (("mode", {"mode": "build"}), ("branch", {"branch": "other_branch"})):
            with self.subTest(label):
                stack, _, _ = stand_in_stack(self, module, self.reply(**edits), 0)
                with self.assertRaises(RuntimeError) as caught:
                    stack.split("verify", BRANCH)
                self.assertNotIn("other_branch", str(caught.exception))


class VerifyFlags(unittest.TestCase):
    """The verify of Silver signals and Silver hypotension gives true only when every pass flag of the reply is true, judged by the real accept function."""

    def verdict(self, name: str, flags: dict[str, bool] | None) -> bool:
        module = runner(name)
        run = flow(module, Nessie(), HEAD)
        run.head = lambda _: HEAD
        if name == "silver_signals":
            job, catalog = module.SIGNALS_JOB, VERIFY_CATALOG
            document = verified() if flags is None else verified(passed=flags)
        else:
            job, catalog = module.HYPOTENSION_JOB, Catalog("b", HEAD, None, HEAD, HYPO_SOURCES, HYPO_SNAPSHOTS)
            document = {**hypotension(), "mode": "verify", "base_hash": None, "write_hash": None, "branch_hash": HEAD}
            document = document if flags is None else {**document, "passed": flags}
        done = SimpleNamespace(keep=lambda: accept_keep(0, document, job, catalog), code=0, seconds=0.1, catalog=catalog)
        run.silver = lambda *_, **__: done
        if name == "silver_signals":
            return bool(run.verify_control({"branch": "b", "branch_hash": HEAD}))
        return bool(run.verify("b", "the fixed sample", HEAD))

    def test_a_verify_with_every_flag_true_gives_true(self) -> None:
        for name in ("silver_signals", "silver_hypotension"):
            with self.subTest(name):
                self.assertIs(True, self.verdict(name, None))

    def test_a_verify_with_one_false_flag_gives_false_and_the_cases_do_not_go_on_to_the_merge(self) -> None:
        wrong = {
            "silver_signals": {"numeric_observations": True, "waveform_seconds": False},
            "silver_hypotension": {"art_mbp_1s": False, "hypotension_events": True},
        }
        for name, flags in wrong.items():
            with self.subTest(name):
                self.assertIs(False, self.verdict(name, flags))


class CasesMergeOnlyWhileTheBranchIsAtTheVerifiedHash(unittest.TestCase):
    """The Silver cases runner checks the head of the branch right before each merge of a verified hash, and a branch that moved is not merged."""

    def cases_run(self, module: Any, head_of_branch: str) -> tuple[Any, list[dict[str, Any]]]:
        merges: list[dict[str, Any]] = []
        run = module.Run.__new__(module.Run)
        run.record, run.made, run.trunk, run.facts, run.labels = Record(feature="test", slug="test", rerun="none"), [], "trunk", {}, {}
        run.nessie = SimpleNamespace(create_branch=lambda *_: None, entries=lambda *_: {})
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.recreate = lambda *_: None
        run.fork = lambda base, label: label
        run.tag = lambda *_: None
        run.candidates = [SimpleNamespace(case=1, slot="a_development")]
        run.sources = (Path("clinical.csv"), Path("lab.csv"))

        def merge(branch: str, into: str, message: str, *, source_hash: str | None = None) -> str:
            merges.append({"branch": branch, "message": message, "source_hash": source_hash})
            return "merged"

        run.merge = merge
        run.head = lambda branch: head_of_branch if branch in ("samplesilver", BRANCH) else BASE
        return run, merges

    def sample(self, head_of_branch: str) -> tuple[Any, list[dict[str, Any]]]:
        module = runner("silver_cases")
        run, merges = self.cases_run(module, head_of_branch)
        built = {"checks_run": module.CHECKS_SAMPLE, "branch_hash": VERIFIED}
        answers = {"build": (0, built, "", 0.1), "verify": (0, {"branch_hash": VERIFIED, "snapshot_ids": {}}, "", 0.1)}
        run.stack = SimpleNamespace(
            write_spec=lambda _: None,
            stand_in=lambda *_, **__: (0, {}, "", 0.1),
            split=lambda *_, **__: (0, {}, "", 0.1),
            silver=lambda mode, *_, **__: answers.get(mode, (1, {}, "", 0.1)),
        )
        run.lab_rows_of = lambda *_: 0
        names = ("loader_answered", "split_answered", "wrote", "all_passed", "no_removed_fields")
        with contextlib.ExitStack() as patches:
            for name in names:
                patches.enter_context(mock.patch.object(module, name, lambda *_, **__: True))
            patches.enter_context(mock.patch.object(module, "lab_rows_of", lambda *_: 0))
            run.sample_sized()
        return run, merges

    def test_the_sample_sized_branch_is_merged_by_the_verified_hash_while_it_is_still_at_it(self) -> None:
        run, merges = self.sample(VERIFIED)
        silver = [item for item in merges if item["message"] == "sample-sized silver"]
        self.assertEqual([VERIFIED], [item["source_hash"] for item in silver])
        self.assertTrue(all(item.passed for item in run.record.observations if "merge" in item.case), [item.observed for item in run.record.observations])

    def test_the_sample_sized_branch_that_moved_after_its_verify_is_not_merged_and_the_case_fails(self) -> None:
        run, merges = self.sample(MOVED)
        self.assertEqual([], [item for item in merges if item["message"] == "sample-sized silver"])
        failed = [item for item in run.record.observations if "merge of the verified hash" in item.case]
        self.assertEqual([False], [item.passed for item in failed])
        self.assertIn("still at the verified hash False", failed[0].observed)
        self.assertNotIn(MOVED, json.dumps([item.observed for item in run.record.observations]))

    def test_the_full_branch_that_moved_after_its_verify_is_not_merged_and_not_tagged(self) -> None:
        module = runner("silver_cases")
        tagged: list[str] = []
        for head, merged in ((VERIFIED, True), (MOVED, False)):
            with self.subTest(head=head[:1]):
                tagged.clear()
                run, merges = self.cases_run(module, head)
                run.tag = lambda *_: tagged.append("tag")
                run.suffix = "x1"
                run.stack = SimpleNamespace(silver=lambda *_, **__: (1, {}, "", 0.1))
                run.trunk_snapshots = {}
                run.merge_and_rebuild({"branch": BRANCH, "branch_hash": VERIFIED}, {"branch_hash": VERIFIED, "snapshot_ids": {}})
                self.assertEqual(merged, bool(merges))
                self.assertEqual(merged, bool(tagged))
                if merged:
                    self.assertEqual([VERIFIED], [item["source_hash"] for item in merges])
                else:
                    failed = [item for item in run.record.observations if not item.passed]
                    self.assertIn("still at the verified hash False", failed[0].observed)
