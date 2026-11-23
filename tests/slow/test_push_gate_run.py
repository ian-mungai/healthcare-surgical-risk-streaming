"""The push gate as a real pre-push hook: real pushes from a temporary repository to a bare repository on disk.

Each planted fault is its own case with the push of a clean commit as its control (``test_a_clean_commit_pushes``): the gate stops the push by the
check's name and the remote keeps its commit. The faults are a planted secret (also one that only a merge's conflict resolution adds), a message that is
not a Conventional Commit (also an earlier unpushed commit's), a tracked file with an uncommitted change, a private value in a file, a file that Ruff
would reformat, a type error and a failing quick test of the changed file. The selection of the changed files' checks is in tests/fast/test_push_gate.py.
"""

from __future__ import annotations

import shutil
import sys
import unittest

from scripts.process import CompletedProcess, run_command
from scripts.quality import push_gate, push_guard_hook
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository
from tests.fast.test_push_gate import GOOD_CODE, GOOD_TEST, HOOK, PYPROJECT, planted_address, planted_token


class PushGateTest(TemporaryRepository):
    """The gate as a real pre-push hook, in a temporary repository that pushes to a bare repository on disk."""

    def setUp(self) -> None:
        super().setUp()
        self.remote = self.repo.parent / "remote.git"
        run_command("git", ["init", "--quiet", "--bare", str(self.remote)], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        self.git("remote", "add", "origin", str(self.remote))
        for folder in (".venv", ".tools"):
            (self.repo / folder).symlink_to(ROOT / folder)
        for name in (".gitignore", ".pre-commit-config.yaml"):
            shutil.copy(ROOT / name, self.repo / name)
        self.write("pyproject.toml", PYPROJECT)
        self.write("app/__init__.py", "")
        self.write("app/calc.py", GOOD_CODE)
        self.write("tests/__init__.py", "")
        self.write("tests/fast/__init__.py", "")
        self.write("tests/fast/test_calc.py", GOOD_TEST)
        self.write("notes.txt", "start\n")
        self.commit("chore: baseline")
        self.git("push", "--quiet", "origin", "main")
        run_command(sys.executable, ["-m", "pre_commit", "install", "--hook-type", HOOK], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        push_guard_hook.install(self.repo)

    def write(self, path: str, text: str) -> None:
        (self.repo / path).parent.mkdir(parents=True, exist_ok=True)
        (self.repo / path).write_text(text)

    def commit(self, message: str, *paths: str) -> None:
        self.git("add", "--", *(paths or (".",)))
        self.git("commit", "--quiet", "-m", message)

    def remote_head(self) -> str:
        return run_command("git", ["--git-dir", str(self.remote), "rev-parse", "main"], env=self.env, timeout=TIMEOUT_SECONDS, check=True).stdout.strip()

    def push(self) -> CompletedProcess[str]:
        return run_command("git", ["push", "origin", "main"], cwd=self.repo, env=self.env, timeout=300)

    def assert_refused_by(self, name: str) -> None:
        before = self.remote_head()
        run = self.push()
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(f"push gate stopped by {name}", run.stdout + run.stderr)
        self.assertEqual(self.remote_head(), before, "the remote keeps its commit")

    def test_a_clean_commit_pushes(self) -> None:
        self.write("app/calc.py", GOOD_CODE + "\n\ndef triple(value: int) -> int:\n    return value * 3\n")
        self.commit("feat: add triple")
        run = self.push()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(self.remote_head(), self.git("rev-parse", "HEAD").stdout.strip())
        self.assertIn("push gate passed", run.stdout + run.stderr)

    def test_a_planted_secret_stops_the_push(self) -> None:
        self.write("notes.txt", f"token {planted_token()}\n")
        self.commit("chore: add a note")
        self.assert_refused_by(push_gate.SECRETS)

    def test_a_secret_that_only_a_merge_adds_stops_the_push(self) -> None:
        """Neither parent holds the token: the merge's conflict resolution does, and a log that shows no merge diff would miss it."""
        self.git("checkout", "--quiet", "-b", "side")
        self.write("notes.txt", "side\n")
        self.commit("chore: side note")
        self.git("checkout", "--quiet", "main")
        self.write("notes.txt", "main\n")
        self.commit("chore: main note")
        merge = run_command("git", ["merge", "--no-ff", "-m", "chore: merge the side note", "side"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
        self.assertNotEqual(merge.returncode, 0, "the notes conflict")
        self.write("notes.txt", f"token {planted_token()}\n")
        self.git("add", "--", "notes.txt")
        self.git("commit", "--quiet", "-m", "chore: merge the side note")
        self.assert_refused_by(push_gate.SECRETS)

    def test_a_message_that_is_not_a_conventional_commit_stops_the_push(self) -> None:
        self.write("notes.txt", "second\n")
        self.commit("added a note")
        self.assert_refused_by(push_gate.MESSAGES)

    def test_a_bad_message_of_an_earlier_unpushed_commit_stops_the_push(self) -> None:
        self.write("notes.txt", "second\n")
        self.commit("added a note")
        self.write("notes.txt", "third\n")
        self.commit("chore: a good message")
        self.assert_refused_by(push_gate.MESSAGES)

    def test_a_tracked_file_with_an_uncommitted_change_stops_the_push(self) -> None:
        self.write("notes.txt", "second\n")
        self.commit("chore: second note")
        self.write("notes.txt", "uncommitted\n")
        self.assert_refused_by(push_gate.TRACKED)

    def test_an_untracked_file_does_not_stop_the_push(self) -> None:
        self.write("notes.txt", "second\n")
        self.commit("chore: second note")
        self.write("scratch.txt", "untracked\n")
        self.assertEqual(self.push().returncode, 0)

    def test_a_private_value_stops_the_push(self) -> None:
        self.write("notes.txt", f"contact {planted_address()}\n")
        self.commit("chore: add a contact")
        self.assert_refused_by(push_gate.PRIVACY)

    def test_a_python_file_that_ruff_would_reformat_stops_the_push(self) -> None:
        self.write("app/calc.py", "def double(value: int) -> int:\n  return value*2\n")
        self.commit("fix: change the spacing")
        self.assert_refused_by(push_gate.RUFF_FORMAT)

    def test_a_type_error_stops_the_push(self) -> None:
        self.write("app/calc.py", 'def double(value: int) -> int:\n    return "two"\n')
        self.commit("fix: return a word")
        self.assert_refused_by(push_gate.MYPY)

    def test_a_failing_quick_test_of_the_changed_file_stops_the_push(self) -> None:
        self.write("app/calc.py", "def double(value: int) -> int:\n    return value * 3\n")
        self.commit("fix: triple the value")
        self.assert_refused_by(push_gate.QUICK)

    def test_a_push_that_sends_no_commit_runs_no_check(self) -> None:
        """Pre-commit starts the gate only when the push sends commits: the push of the commit that the remote already holds prints no gate result."""
        run = self.push()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertNotIn("push gate", run.stdout + run.stderr)


if __name__ == "__main__":
    unittest.main()
