"""The run of the commit-guard program and its record, with faults that real commits cannot produce cheaply.

The planted-fault run in ``tests/e2e/commit_guards.py`` makes the real commits. Failure modes covered here:

- A planted fault passes because its hook crashed with another message, since only the hook id and the exit code were read.
- A timeout, an OS error or an interrupt before or inside the protected run leaves no record, or leaves the scratch folder.
- Two runs or two shards that start in the same second write into one record folder.
- The source list reads a path that Git quotes, or one that holds a carriage return, as a missing file and drops it from the copy.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import TimeoutExpired
from unittest import mock

from tests.e2e import commit_guards, run_record
from tests.fast.git_fixture import TemporaryRepository

HOOK_FAILURE = "Failing hook\n.....Failed\n- hook id: {hook}\n- exit code: 1\n\n{detail}\n"


def planted_case() -> commit_guards.Case:
    change = commit_guards.Change({}, ())
    return commit_guards.Case("planted", "privacy-scan", ("privacy-scan",), ("notes/planted.txt:1: AWS account ID",), change, change)


class FaultJudgementTest(unittest.TestCase):
    def judge(self, code: int, output: str, *, advanced: bool = False) -> bool:
        return commit_guards.judge_fault(planted_case(), code, output, advanced).passed

    def test_the_named_hook_with_its_message_passes(self) -> None:
        output = HOOK_FAILURE.format(hook="privacy-scan", detail="notes/planted.txt:1: AWS account ID")
        self.assertTrue(self.judge(1, output))

    def test_the_named_hook_with_another_message_fails(self) -> None:
        output = HOOK_FAILURE.format(hook="privacy-scan", detail="Traceback (most recent call last):\nKeyError: 'planted'")
        self.assertFalse(self.judge(1, output))

    def test_the_message_without_a_failed_hook_fails(self) -> None:
        self.assertFalse(self.judge(1, "notes/planted.txt:1: AWS account ID"))

    def test_another_failed_hook_fails(self) -> None:
        output = HOOK_FAILURE.format(hook="privacy-scan", detail="notes/planted.txt:1: AWS account ID") + HOOK_FAILURE.format(hook="mypy", detail="error")
        self.assertFalse(self.judge(1, output))

    def test_an_advanced_head_or_a_zero_exit_fails(self) -> None:
        output = HOOK_FAILURE.format(hook="privacy-scan", detail="notes/planted.txt:1: AWS account ID")
        self.assertFalse(self.judge(1, output, advanced=True))
        self.assertFalse(self.judge(0, output))

    def test_a_case_that_names_no_hook_passes_only_when_no_hook_failed(self) -> None:
        change = commit_guards.Change({}, ())
        worktree = commit_guards.Case("worktree", "worktree privacy scan", (), ("AWS account ID",), change, change)
        self.assertTrue(commit_guards.judge_fault(worktree, 1, "notes/x.txt:1: AWS account ID\n", False).passed)
        output = HOOK_FAILURE.format(hook="privacy-scan", detail="notes/x.txt:1: AWS account ID")
        self.assertFalse(commit_guards.judge_fault(worktree, 1, output, False).passed)


PLANTED_TEST = "tests.fast.test_planted.PlantedTest.test_planted"
PLANTED_FAILURE = (
    'FAIL: test_planted ({name})\n----------\nTraceback (most recent call last):\n  File "x.py", line 6, in test_planted\nAssertionError: {detail}\n'
)
FAST_SUMMARY = "Ran 312 tests in 60.0s\n\nFAILED (failures={count})\n"


def fast_test_output(*failures: str, count: int = 1) -> str:
    return HOOK_FAILURE.format(hook="fast-tests", detail="\n".join(failures) + "\n" + FAST_SUMMARY.format(count=count))


class FastTestFaultJudgementTest(unittest.TestCase):
    """The planted fast-test fault is proven by the planted test's full name and its assertion message, and by no other failure."""

    def judge(self, output: str) -> bool:
        case = next(case for case in commit_guards.planted_cases() if case.hooks == ("fast-tests",))
        return commit_guards.judge_fault(case, 1, output, False).passed

    def planted(self, detail: str = "1 != 2 : planted fast-test fault", name: str = PLANTED_TEST) -> str:
        return PLANTED_FAILURE.format(name=name, detail=detail)

    def test_the_planted_test_failing_with_its_assertion_message_passes(self) -> None:
        self.assertTrue(self.judge(fast_test_output(self.planted())))

    def test_a_failure_of_another_test_is_not_proof_of_the_fault(self) -> None:
        other = PLANTED_FAILURE.format(name="tests.fast.test_other.OtherTest.test_other", detail="3 != 4")
        self.assertFalse(self.judge(fast_test_output(other)))

    def test_the_planted_test_failing_with_another_message_is_not_proof(self) -> None:
        self.assertFalse(self.judge(fast_test_output(self.planted("KeyError: 'planted'"))))

    def test_the_planted_message_from_another_test_is_not_proof(self) -> None:
        self.assertFalse(self.judge(fast_test_output(self.planted(name="tests.fast.test_other.OtherTest.test_other"))))

    def test_a_second_failure_beside_the_planted_one_is_not_proof(self) -> None:
        other = PLANTED_FAILURE.format(name="tests.fast.test_other.OtherTest.test_other", detail="3 != 4")
        self.assertFalse(self.judge(fast_test_output(self.planted(), other, count=2)))


class ProtectedRunTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="commit_guard_run_")
        self.addCleanup(scratch.cleanup)
        self.runs = Path(scratch.name) / "root"
        self.temp = Path(scratch.name) / "temp"
        self.temp.mkdir()
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.runs),
            mock.patch.object(tempfile, "tempdir", str(self.temp)),
            mock.patch.object(sys, "argv", ["commit_guards"]),
            mock.patch.object(sys, "stdout", io.StringIO()),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def record(self) -> dict[str, object]:
        records = sorted((self.runs / "runs").glob("*/record.json"))
        self.assertEqual(1, len(records), "the run must write exactly one record")
        loaded: dict[str, object] = json.loads(records[0].read_text())
        return loaded

    def test_a_timeout_before_the_fixture_still_writes_the_record(self) -> None:
        with mock.patch.object(commit_guards, "tool_version", side_effect=TimeoutExpired("tool", 1)):
            self.assertEqual(1, commit_guards.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertIn("TimeoutExpired", json.dumps(record["observations"]))
        self.assertEqual([], list(self.temp.iterdir()))

    def test_a_failed_copy_removes_the_scratch_folder_and_writes_the_record(self) -> None:
        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(shutil, "copy2", side_effect=OSError("disk full")),
        ):
            self.assertEqual(1, commit_guards.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertIn("OSError", json.dumps(record["observations"]))
        self.assertEqual("temporary repository removed", record["cleanup"])
        self.assertEqual([], list(self.temp.iterdir()))

    def test_an_interrupt_writes_the_record_removes_the_scratch_folder_and_propagates(self) -> None:
        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards.Fixture, "must", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            commit_guards.main()
        record = self.record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual("temporary repository removed", record["cleanup"])
        self.assertEqual([], list(self.temp.iterdir()))


class RecordFolderTest(unittest.TestCase):
    def test_runs_that_start_in_the_same_second_use_different_folders(self) -> None:
        with (
            tempfile.TemporaryDirectory(prefix="run_record_") as root,
            mock.patch.object(run_record, "ROOT", Path(root)),
            mock.patch.object(sys, "stdout", io.StringIO()),
        ):
            for mode in ("shard 1 of 2", "shard 2 of 2", "shard 1 of 2"):
                record = run_record.Record(feature="feature", slug="slug", rerun="rerun", mode=mode, started_utc="2026-01-01T00:00:00Z")
                record.expect("case", "step", "expected", "observed", True)
                record.close([])
            folders = sorted(path.name for path in (Path(root) / "runs").iterdir())
        self.assertEqual(3, len(set(folders)), folders)
        self.assertEqual(2, sum("shard_1_of_2" in name for name in folders))
        self.assertEqual(1, sum("shard_2_of_2" in name for name in folders))


class SourceListTest(TemporaryRepository):
    def test_a_path_that_git_quotes_is_listed(self) -> None:
        names = ["café.txt", "tab\tname.txt", "carriage\rreturn.txt", "plain.txt"]
        for name in names:
            (self.repo / name).write_text("note\n")
        self.git("add", "--", names[0])
        self.assertEqual(sorted(names), commit_guards.source_files(self.repo))

    def test_the_callers_git_environment_does_not_redirect_the_listing(self) -> None:
        (self.repo / "plain.txt").write_text("note\n")
        (self.repo / "excluded.txt").write_text("note\n")
        (self.repo / ".git" / "info" / "exclude").write_text("excluded.txt\n")
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.other_repository())}):
            self.assertEqual(["plain.txt"], commit_guards.source_files(self.repo))


if __name__ == "__main__":
    unittest.main()
