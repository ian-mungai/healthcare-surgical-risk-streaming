"""The run of the commit-guard program and its record, with faults that real commits cannot produce cheaply.

The planted-fault run in ``tests/e2e/commit_guards.py`` makes the real commits. Failure modes covered here:

- A planted fault passes because its hook crashed with another message, since only the hook id and the exit code were read.
- A timeout, an OS error or an interrupt before or inside the protected run leaves no record, or leaves the scratch folder.
- A real interrupt (SIGINT) or an unexpected error while the temporary repository is removed leaves no record.
- Two runs or two shards that start in the same second write into one record folder.
- The source list reads a path that Git quotes, or one that holds a carriage return, as a missing file and drops it from the copy.
- A global ``core.hooksPath`` (or a global Git config at all) sends a fixture's hook install outside its scratch repository, where workers overwrite one
  shared hook.
- A SIGINT during the removal of the temporary repository leaves it behind.
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
import threading
import unittest
from collections.abc import Callable
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired
from unittest import mock

from scripts.process import run_command
from scripts.quality import worktree_scan_hook
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
        self.printed = io.StringIO()
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.runs),
            mock.patch.object(tempfile, "tempdir", str(self.temp)),
            mock.patch.object(sys, "argv", ["commit_guards"]),
            mock.patch.object(sys, "stdout", self.printed),
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

    def test_a_real_interrupt_while_the_temporary_repository_is_removed_lets_the_removal_end_then_writes_a_blocked_record_and_propagates(self) -> None:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)
        real_remove = commit_guards.Fixture.remove

        def remove(fixture: commit_guards.Fixture) -> None:
            self.interrupt()
            real_remove(fixture)

        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(commit_guards.Fixture, "remove", autospec=True, side_effect=remove),
            self.assertRaises(KeyboardInterrupt),
        ):
            commit_guards.main()
        record = self.record()
        self.assertEqual("blocked: interrupted", record["outcome"])
        self.assertEqual("temporary repository removed", record["cleanup"])
        self.assertEqual([], list(self.temp.iterdir()), "the interrupted removal must not leave the repository")
        self.assertIs(signal.default_int_handler, signal.getsignal(signal.SIGINT))

    def test_a_real_interrupt_while_the_record_is_written_is_deferred_until_the_record_is_complete(self) -> None:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)
        real_write = run_record.Record.write

        def write(record: run_record.Record, log: list[str], folder: Path | None = None) -> Path:
            self.interrupt()
            return real_write(record, log, folder)

        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(run_record.Record, "write", autospec=True, side_effect=write),
            self.assertRaises(KeyboardInterrupt),
        ):
            commit_guards.main()
        record = self.record()
        folder = next((self.runs / "runs").iterdir())
        self.assertTrue((folder / "retention.json").is_file(), "the record's retention label is written too")
        self.assertEqual("temporary repository removed", record["cleanup"])
        self.assertEqual([], list(self.temp.iterdir()))
        self.assertEqual("blocked: interrupted", record["outcome"], "the interrupt that arrived during the write is in the saved record")
        self.assertEqual(run_record.FAILED_END, json.loads((folder / "retention.json").read_text())["end_condition"])

    def run_interrupted_after_the_record_write(self, *, which: int) -> Path:
        """Run with a real SIGINT that arrives right after the ``which``-th write of record.json and its label, then return the run folder."""
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, previous)
        real_write_record = run_record.Record.write_record
        calls: list[int] = []

        def write_record(record: run_record.Record, folder: Path) -> None:
            real_write_record(record, folder)
            calls.append(1)
            if len(calls) == which:
                self.interrupt()

        with (
            mock.patch.object(commit_guards, "tool_version", return_value="tool 1"),
            mock.patch.object(commit_guards, "run_cases"),
            mock.patch.object(run_record.Record, "write_record", autospec=True, side_effect=write_record),
            self.assertRaises(KeyboardInterrupt),
        ):
            commit_guards.main()
        return next((self.runs / "runs").iterdir())

    def test_a_real_interrupt_right_after_the_record_is_written_turns_the_saved_record_and_its_label_into_a_blocked_one(self) -> None:
        folder = self.run_interrupted_after_the_record_write(which=1)
        saved = self.record()
        self.assertEqual("blocked: interrupted", saved["outcome"], "a record that says pass would be accepted by --compare")
        self.assertEqual(run_record.FAILED_END, json.loads((folder / "retention.json").read_text())["end_condition"])
        self.assertIn("blocked: interrupted: record in", self.printed.getvalue())
        self.assertEqual("temporary repository removed", saved["cleanup"])
        self.assertEqual([], list(self.temp.iterdir()))

    def test_the_saved_record_is_rewritten_once_and_in_the_same_folder(self) -> None:
        self.run_interrupted_after_the_record_write(which=1)
        self.assertEqual(1, len(list((self.runs / "runs").iterdir())), "no second folder")
        self.assertEqual(1, self.printed.getvalue().count(": record in"), "one verdict is printed")

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


class FixtureHookTest(unittest.TestCase):
    """The hook install of a fixture resolves its hook folder in the fixture's own Git environment, so a global ``core.hooksPath`` cannot redirect it."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="commit_guard_hooks_")
        self.addCleanup(scratch.cleanup)
        self.shared = Path(scratch.name) / "shared_hooks"
        self.shared.mkdir()
        self.original = "#!/bin/sh\n# the shared hook of the owner\n"
        (self.shared / worktree_scan_hook.HOOK_NAME).write_text(self.original)
        home = Path(scratch.name) / "home"
        home.mkdir()
        (home / ".gitconfig").write_text(f"[core]\n\thooksPath = {self.shared}\n")
        poisoned = {"HOME": str(home), "GIT_CONFIG_GLOBAL": str(home / ".gitconfig"), "XDG_CONFIG_HOME": str(home / ".config")}
        patcher = mock.patch.dict(os.environ, poisoned)
        patcher.start()
        self.addCleanup(patcher.stop)
        temp = Path(scratch.name) / "temp"
        temp.mkdir()
        tempdir = mock.patch.object(tempfile, "tempdir", str(temp))
        tempdir.start()
        self.addCleanup(tempdir.stop)
        self.fixtures: list[commit_guards.Fixture] = []
        self.assertEqual(str(self.shared), run_command("git", ["config", "--global", "core.hooksPath"], timeout=60, check=True).stdout.strip())

    def fixture(self) -> commit_guards.Fixture:
        fixture = commit_guards.Fixture(run_record.Record(feature="f", slug="s", rerun="r"))
        self.addCleanup(fixture.remove)
        fixture.repo.mkdir()
        fixture.must("git", "init", "--quiet", "--initial-branch", "main")
        self.fixtures.append(fixture)
        return fixture

    def test_the_hook_is_written_in_the_fixtures_own_hook_folder_and_not_in_the_global_one(self) -> None:
        fixture = self.fixture()
        fixture.install_scan_hook()
        own = fixture.repo / ".git" / "hooks" / worktree_scan_hook.HOOK_NAME
        self.assertEqual(worktree_scan_hook.HOOK_TEXT, own.read_text())
        self.assertEqual(self.original, (self.shared / worktree_scan_hook.HOOK_NAME).read_text(), "the shared hook is not overwritten")
        self.assertEqual([worktree_scan_hook.HOOK_NAME], sorted(path.name for path in self.shared.iterdir()), "nothing else is written there")

    def test_two_fixtures_never_touch_the_shared_hook(self) -> None:
        first, second = self.fixture(), self.fixture()
        threads = [threading.Thread(target=fixture.install_scan_hook) for fixture in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        for fixture in (first, second):
            self.assertEqual(worktree_scan_hook.HOOK_TEXT, (fixture.repo / ".git" / "hooks" / worktree_scan_hook.HOOK_NAME).read_text())
            self.assertFalse((fixture.repo / ".git" / "hooks" / worktree_scan_hook.KEPT_NAME).exists())
        self.assertEqual(self.original, (self.shared / worktree_scan_hook.HOOK_NAME).read_text())
        self.assertFalse((self.shared / worktree_scan_hook.KEPT_NAME).exists(), "the shared hook is not moved aside")

    def test_the_fixture_reads_no_global_git_setting_of_the_caller(self) -> None:
        fixture = self.fixture()
        for name in ("HOME", "GIT_CONFIG_GLOBAL", "XDG_CONFIG_HOME"):
            self.assertTrue(Path(fixture.env[name]).is_relative_to(fixture.home), name)
        self.assertEqual("1", fixture.env["GIT_CONFIG_NOSYSTEM"])
        result = run_command("git", ["config", "--global", "--get", "core.hooksPath"], cwd=fixture.repo, env=fixture.env, timeout=60)
        self.assertEqual(1, result.returncode, "Git answers 1 for a setting that is not set")
        self.assertEqual("", result.stdout, "no value is read from a global file")

    def test_a_warning_on_standard_error_does_not_fail_the_check_of_the_global_settings(self) -> None:
        """A Git on macOS can print a sandbox warning on standard error; only the exit status and the standard output decide."""
        fixture = self.fixture()
        noisy = {**fixture.env, "GIT_TRACE": "1"}
        result = run_command("git", ["config", "--global", "--get", "core.hooksPath"], cwd=fixture.repo, env=noisy, timeout=60)
        self.assertNotEqual("", result.stderr, "the trace puts text on standard error")
        self.assertEqual((1, ""), (result.returncode, result.stdout))


class CloseAfterTest(unittest.TestCase):
    """The blocked reason of ``close_after`` may be a function that is read after the cleanup, so the cleanup can set it."""

    def close(self, blocked: str | Callable[[], str], cleanup: Callable[[], None] = lambda: None) -> run_record.Record:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        record.expect("case", "step", "e", "o", True)
        with (
            tempfile.TemporaryDirectory(prefix="close_after_") as root,
            mock.patch.object(run_record, "ROOT", Path(root)),
            mock.patch.object(sys, "stdout", io.StringIO()),
        ):
            record.close_after(cleanup, [], blocked=blocked)
        return record

    def test_a_reason_set_by_the_cleanup_blocks_the_run(self) -> None:
        reasons: list[str] = []
        record = self.close(lambda: reasons[0] if reasons else "", lambda: reasons.append("set by the cleanup"))
        self.assertEqual("blocked: set by the cleanup", record.outcome)

    def test_a_text_reason_and_no_reason_keep_their_meaning(self) -> None:
        self.assertEqual("blocked: given", self.close("given").outcome)
        self.assertEqual("pass", self.close("").outcome)
        self.assertEqual("pass", self.close(lambda: "").outcome)

    def test_a_keyboard_interrupt_of_the_cleanup_blocks_the_run_as_interrupted(self) -> None:
        def cleanup() -> None:
            raise KeyboardInterrupt

        record = run_record.Record(feature="f", slug="s", rerun="r")
        with (
            tempfile.TemporaryDirectory(prefix="close_after_") as root,
            mock.patch.object(run_record, "ROOT", Path(root)),
            mock.patch.object(sys, "stdout", io.StringIO()),
            self.assertRaises(KeyboardInterrupt),
        ):
            record.close_after(cleanup, [])
        self.assertEqual("blocked: interrupted", record.outcome)


class CloseAfterLateReasonTest(unittest.TestCase):
    """A reason that appears while the record is written (an interrupt that arrives then) is in the saved record, and a run that has none writes it once."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="close_after_late_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        for patcher in (mock.patch.object(run_record, "ROOT", self.root), mock.patch.object(sys, "stdout", io.StringIO())):
            patcher.start()
            self.addCleanup(patcher.stop)

    def close(self, blocked: str | Callable[[], str]) -> tuple[int, run_record.Record, int]:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        record.expect("case", "step", "e", "o", True)
        real_write_record = run_record.Record.write_record
        writes: list[int] = []

        def write_record(record: run_record.Record, folder: Path) -> None:
            writes.append(1)
            real_write_record(record, folder)

        with mock.patch.object(run_record.Record, "write_record", autospec=True, side_effect=write_record):
            code = record.close_after(lambda: None, [], blocked=blocked)
        return code, record, len(writes)

    def saved(self) -> tuple[dict[str, object], dict[str, object]]:
        (folder,) = (self.root / "runs").iterdir()
        return json.loads((folder / "record.json").read_text()), json.loads((folder / "retention.json").read_text())

    def test_a_reason_that_appears_during_the_write_blocks_the_saved_record_and_its_label(self) -> None:
        reasons = iter(["", "interrupted"])
        code, record, writes = self.close(lambda: next(reasons))
        self.assertEqual(1, code)
        self.assertEqual("blocked: interrupted", record.outcome)
        saved, label = self.saved()
        self.assertEqual("blocked: interrupted", saved["outcome"])
        self.assertEqual(run_record.FAILED_END, label["end_condition"])
        self.assertEqual(2, writes)
        self.assertEqual(1, len(list((self.root / "runs").iterdir())), "the same folder")

    def test_a_reason_that_is_there_before_the_write_is_written_once(self) -> None:
        for blocked in ("given", lambda: "given"):
            with self.subTest(blocked=blocked):
                shutil.rmtree(self.root / "runs", ignore_errors=True)
                code, record, writes = self.close(blocked)
                self.assertEqual((1, "blocked: given", 1), (code, record.outcome, writes))

    def test_a_run_that_never_has_a_reason_is_written_once_and_passes(self) -> None:
        for blocked in ("", lambda: ""):
            with self.subTest(blocked=blocked):
                shutil.rmtree(self.root / "runs", ignore_errors=True)
                code, record, writes = self.close(blocked)
                self.assertEqual((0, "pass", 1), (code, record.outcome, writes))
                self.assertEqual("pass", self.saved()[0]["outcome"])

    def test_a_reason_that_changes_after_the_write_keeps_the_first_reason(self) -> None:
        reasons = iter(["first", "second"])
        code, record, writes = self.close(lambda: next(reasons))
        self.assertEqual((1, "blocked: first", 1), (code, record.outcome, writes))

    def test_a_failed_run_stays_failed_when_the_reason_appears_late(self) -> None:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        record.expect("case", "step", "e", "o", False)
        reasons = iter(["", "interrupted"])
        record.close_after(lambda: None, [], blocked=lambda: next(reasons))
        self.assertEqual("blocked: interrupted", self.saved()[0]["outcome"], "an interrupt is the verdict, as it is for a run that passed")


class TreeDigestTest(TemporaryRepository):
    """The digest of the copied files holds each file's Git mode, so two runs of the same bytes with another executable bit are not the same code."""

    def setUp(self) -> None:
        super().setUp()
        patcher = mock.patch.object(commit_guards, "ROOT", self.repo)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.script = self.repo / "run.sh"
        self.script.write_text("#!/bin/sh\necho planted\n")
        self.script.chmod(0o644)

    def digest(self) -> str:
        return commit_guards.tree_digest(["run.sh"])

    def test_an_executable_bit_with_the_same_bytes_changes_the_digest(self) -> None:
        plain = self.digest()
        self.script.chmod(0o755)
        self.assertNotEqual(plain, self.digest())
        self.script.chmod(0o644)
        self.assertEqual(plain, self.digest())

    def test_only_the_owner_executable_bit_counts_as_git_counts_it(self) -> None:
        plain = self.digest()
        for mode in (0o600, 0o664, 0o611, 0o444):
            with self.subTest(mode=oct(mode)):
                self.script.chmod(mode)
                self.assertEqual(plain, self.digest())
        self.script.chmod(0o700)
        executable = commit_guards.git_mode(self.script)
        self.assertEqual("100755", executable)
        for mode in (0o700, 0o744, 0o755, 0o775):
            with self.subTest(mode=oct(mode)):
                self.script.chmod(mode)
                self.assertEqual(executable, commit_guards.git_mode(self.script))
        self.assertNotEqual(plain, self.digest())

    def test_the_mode_is_the_one_git_records_for_the_copied_file(self) -> None:
        for mode, expected in ((0o644, "100644"), (0o755, "100755")):
            with self.subTest(mode=oct(mode)):
                self.script.chmod(mode)
                copy = self.repo / f"copy_{expected}.sh"
                shutil.copy2(self.script, copy)
                self.git("add", "--", copy.name)
                self.assertEqual(expected, commit_guards.git_mode(self.script))
                self.assertEqual(expected, self.git("ls-files", "--stage", "--", copy.name).stdout.split()[0])

    def test_a_link_to_a_file_counts_as_the_file_that_the_copy_becomes(self) -> None:
        self.script.chmod(0o755)
        link = self.repo / "link.sh"
        link.symlink_to(self.script)
        self.assertEqual("100755", commit_guards.git_mode(link))
        self.script.chmod(0o644)
        self.assertEqual("100644", commit_guards.git_mode(link), "a link itself has mode 777 on this computer; the file it names decides")

    def test_the_path_the_bytes_and_the_mode_each_change_the_digest(self) -> None:
        base = self.digest()
        (self.repo / "other.sh").write_text(self.script.read_text())
        self.assertNotEqual(base, commit_guards.tree_digest(["other.sh"]))
        self.script.write_text("changed\n")
        self.assertNotEqual(base, self.digest())
        self.assertEqual(commit_guards.tree_digest(["run.sh", "other.sh"]), commit_guards.tree_digest(["run.sh", "other.sh"]))
        self.assertNotEqual(commit_guards.tree_digest(["run.sh", "other.sh"]), commit_guards.tree_digest(["other.sh", "run.sh"]))


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
