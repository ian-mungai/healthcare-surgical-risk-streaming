"""The run of the commit-guard program and its record, with faults that real commits cannot produce cheaply.

The planted-fault run in ``tests/e2e/commit_guards.py`` makes the real commits. Failure modes covered here:

- A planted fault passes because its hook crashed with another message, since only the hook id and the exit code were read.
- A timeout, an OS error or an interrupt before or inside the protected run leaves no record, or leaves the scratch folder.
- A real interrupt (SIGINT) or an unexpected error while the temporary repository is removed leaves no record.
- Two runs or two shards that start in the same second write into one record folder.
- The source list reads a path that Git quotes, or one that holds a carriage return, as a missing file and drops it from the copy.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired
from unittest import mock

from scripts.process import run_command
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
FAST_SUMMARY = "Ran 312 tests in 60.0s\n\nFAILED ({counts})\n"


def fast_test_output(*failures: str, count: int = 1, rest: str = "") -> str:
    summary = FAST_SUMMARY.format(counts=f"failures={count}{rest}")
    return HOOK_FAILURE.format(hook="fast-tests", detail="\n".join(failures) + "\n" + summary)


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

    def test_skipped_tests_in_the_summary_do_not_hide_the_proof(self) -> None:
        """The suite has skipped tests, so unittest's summary reads ``FAILED (failures=1, skipped=2)``."""
        self.assertTrue(self.judge(fast_test_output(self.planted(), rest=", skipped=2")))

    def test_a_summary_quoted_inside_a_failure_message_does_not_stand_for_the_run(self) -> None:
        """Only the run's last summary line counts: a one-failure summary quoted in a message must not hide a second failure."""
        quoted = PLANTED_FAILURE.format(
            name="tests.fast.test_other.OtherTest.test_other", detail="'FAILED (failures=1, skipped=2)'\nFAILED (failures=1, skipped=2)"
        )
        self.assertFalse(self.judge(fast_test_output(self.planted(), quoted, count=2, rest=", skipped=2")))

    def test_a_buffered_summary_printed_after_the_real_footer_does_not_stand_for_the_run(self) -> None:
        """Pre-commit can emit a test's printed text after unittest's footer; only the status paired with the `Ran N tests` line counts."""
        other = PLANTED_FAILURE.format(name="tests.fast.test_other.OtherTest.test_other", detail="3 != 4")
        output = fast_test_output(self.planted(), other, count=2, rest=", skipped=2") + "FAILED (failures=1, skipped=2)\n"
        self.assertFalse(self.judge(output))
        errored = fast_test_output(self.planted(), rest=", errors=1, skipped=2") + "FAILED (failures=1, skipped=2)\n"
        self.assertFalse(self.judge(errored))

    def test_an_error_beside_the_planted_failure_is_not_proof(self) -> None:
        self.assertFalse(self.judge(fast_test_output(self.planted(), rest=", errors=1")))
        self.assertFalse(self.judge(fast_test_output(self.planted(), rest=", errors=1, skipped=2")))

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

    def interrupt(self) -> None:
        os.kill(os.getpid(), signal.SIGINT)

    def test_a_real_interrupt_while_the_temporary_repository_is_removed_writes_a_blocked_record_and_propagates(self) -> None:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)
        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(commit_guards.Fixture, "remove", side_effect=self.interrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            commit_guards.main()
        record = self.record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual("cleanup interrupted", record["cleanup"])
        self.assertIn("cleanup", [item["case"] for item in record["observations"] if not item["passed"]])  # type: ignore[attr-defined]

    def test_an_unexpected_error_while_the_temporary_repository_is_removed_writes_a_failed_record(self) -> None:
        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(commit_guards.Fixture, "remove", side_effect=ValueError("planted")),
        ):
            self.assertEqual(1, commit_guards.main())
        record = self.record()
        self.assertEqual("fail", record["outcome"])
        self.assertTrue(str(record["cleanup"]).startswith("cleanup failed: ValueError"), record["cleanup"])

    def run_without_cases(self, listed: list[list[str]]) -> int:
        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "Fixture"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(commit_guards, "remove_fixtures"),
            mock.patch.object(commit_guards, "source_files", side_effect=listed),
        ):
            return commit_guards.main()

    def test_the_run_writes_the_recorded_state_and_passes_an_unchanged_snapshot(self) -> None:
        paths = commit_guards.source_files()
        self.assertEqual(0, self.run_without_cases([paths, paths]))
        record = self.record()
        self.assertEqual(commit_guards.recorded_state(commit_guards.ROOT), (record["revision"], record["tree_sha256"]))
        self.assertIn('"observed": "unchanged: True"', json.dumps(record["observations"]))

    def test_a_file_that_appears_during_the_run_fails_the_snapshot(self) -> None:
        paths = commit_guards.source_files()
        self.assertEqual(1, self.run_without_cases([paths[:-1], paths]))
        self.assertIn('"observed": "unchanged: False"', json.dumps(self.record()["observations"]))


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


class RecordedStateTest(TemporaryRepository):
    def setUp(self) -> None:
        super().setUp()
        (self.repo / "tracked.txt").write_text("one\n")
        self.git("add", "--", "tracked.txt")
        self.git("commit", "--quiet", "--message", "first")
        self.head = self.git("rev-parse", "HEAD").stdout.strip()
        self.clean = commit_guards.recorded_state(self.repo)

    def test_a_clean_checkout_records_its_commit_and_the_digest_of_that_commits_tree(self) -> None:
        tree = self.git("rev-parse", "HEAD^{tree}").stdout.strip()
        self.assertEqual((self.head, hashlib.sha256(tree.encode()).hexdigest()), self.clean)

    def test_a_second_clone_of_the_commit_records_the_same_state(self) -> None:
        clone = self.repo.parent / "clone"
        run_command("git", ["clone", "--quiet", str(self.repo), str(clone)], cwd=self.repo.parent, env=self.env, timeout=60, check=True)
        self.assertEqual(self.clean, commit_guards.recorded_state(clone))

    def test_an_untracked_file_changes_nothing_in_the_recorded_state(self) -> None:
        (self.repo / "untracked.txt").write_text("note\n")
        self.assertIn("untracked.txt", commit_guards.source_files(self.repo))
        self.assertEqual(self.clean, commit_guards.recorded_state(self.repo))

    def test_a_new_commit_changes_the_recorded_digest(self) -> None:
        (self.repo / "tracked.txt").write_text("two\n")
        self.git("commit", "--quiet", "--all", "--message", "second")
        revision, digest = commit_guards.recorded_state(self.repo)
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), revision)
        self.assertNotEqual(self.clean[1], digest)

    def test_a_tracked_change_that_is_not_committed_is_named_in_the_revision(self) -> None:
        (self.repo / "tracked.txt").write_text("two\n")
        marked = (f"{self.head} with uncommitted changes", self.clean[1])
        self.assertEqual(marked, commit_guards.recorded_state(self.repo))
        self.git("add", "--", "tracked.txt")
        self.assertEqual(marked, commit_guards.recorded_state(self.repo))

    def test_a_removed_tracked_file_is_named_in_the_revision(self) -> None:
        (self.repo / "tracked.txt").unlink()
        self.assertEqual((f"{self.head} with uncommitted changes", self.clean[1]), commit_guards.recorded_state(self.repo))

    def test_reading_the_recorded_state_leaves_the_repository_files_as_they_were(self) -> None:
        os.utime(self.repo / "tracked.txt", (1, 1))
        store = self.repo / ".git"

        def kept() -> dict[str, tuple[int, int]]:
            return {str(path.relative_to(store)): (path.stat().st_mtime_ns, path.stat().st_size) for path in store.rglob("*")}

        before = kept()
        commit_guards.recorded_state(self.repo)
        self.assertEqual(before, kept())

    def test_the_recorded_state_ignores_the_git_environment_of_the_caller(self) -> None:
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.other_repository()), "GIT_INDEX_FILE": str(self.repo.parent / "absent_index")}):
            self.assertEqual(self.clean, commit_guards.recorded_state(self.repo))

    def test_a_repository_without_a_commit_says_so(self) -> None:
        empty = self.other_repository().parent
        self.assertEqual(("no commit", hashlib.sha256(b"no commit").hexdigest()), commit_guards.recorded_state(empty))
        self.git("switch", "--quiet", "--orphan", "unborn")
        self.assertEqual(("no commit", hashlib.sha256(b"no commit").hexdigest()), commit_guards.recorded_state(self.repo))

    def test_a_branch_whose_commit_cannot_be_read_is_an_error(self) -> None:
        ref = self.repo / ".git" / "refs" / "heads" / "main"
        ref.chmod(0)
        self.addCleanup(ref.chmod, 0o644)
        with self.assertRaises(CalledProcessError):
            commit_guards.recorded_state(self.repo)

    def test_a_folder_that_is_not_a_repository_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as folder, self.assertRaises(CalledProcessError):
            commit_guards.recorded_state(Path(folder))

    def test_the_run_record_holds_the_recorded_state(self) -> None:
        (self.repo / "tracked.txt").write_text("two\n")
        record = run_record.Record(feature="feature", slug="slug", rerun="rerun")
        with mock.patch.object(commit_guards, "ROOT", self.repo), mock.patch.object(commit_guards, "tool_version", return_value="1"):
            commit_guards.describe_run(record)
        self.assertEqual((f"{self.head} with uncommitted changes", self.clean[1]), (record.revision, record.tree_sha256))


if __name__ == "__main__":
    unittest.main()
