"""The push gate as a real pre-push hook: real pushes from a temporary repository to a bare repository on disk.

Each planted fault is its own case with the push of a clean commit as its control (``test_a_clean_commit_pushes``): the gate stops the push by the
check's name and the remote keeps its commit. The faults are a planted secret (also one that only a merge's conflict resolution adds), a message that is
not a Conventional Commit (also an earlier unpushed commit's), a tracked file with an uncommitted change, a private value in a file, a file that Ruff
would reformat, a type error and a failing quick test of the changed file. The selection of the changed files' checks is in tests/fast/test_push_gate.py.
"""

from __future__ import annotations

import shlex
import shutil
import sys
import unittest
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, run_command
from scripts.quality import push_gate, push_guard_hook
from scripts.stack.yaml_text import load_yaml
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

    def assert_refused_by(self, name: str, *, also: str = "") -> None:
        before = self.remote_head()
        run = self.push()
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(f"push gate stopped by {name}", run.stdout + run.stderr)
        self.assertIn(also, run.stdout + run.stderr)
        self.assertEqual(self.remote_head(), before, "the remote keeps its commit")

    def push_to(self, *arguments: str) -> CompletedProcess[str]:
        return run_command("git", ["push", *arguments], cwd=self.repo, env=self.env, timeout=300)

    def make_remote(self, name: str) -> Path:
        folder = self.repo.parent / f"{name}.git"
        run_command("git", ["init", "--quiet", "--bare", str(folder)], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        self.git("remote", "add", name, str(folder))
        return folder

    def remote_has(self, folder: Path, ref: str) -> bool:
        return run_command("git", ["--git-dir", str(folder), "rev-parse", "--verify", "--quiet", ref], env=self.env, timeout=TIMEOUT_SECONDS).returncode == 0

    def secret_branch(self, name: str, message: str) -> None:
        """A branch that holds a planted secret under ``message``, with the checkout back on main."""
        self.git("checkout", "--quiet", "-b", name)
        self.write("notes.txt", f"token {planted_token()}\n")
        self.commit(message)
        self.git("checkout", "--quiet", "main")

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
        self.assert_refused_by(push_gate.SECRETS, also="RuleID:")

    def test_the_secret_diagnostic_is_a_detection_with_the_value_redacted(self) -> None:
        self.write("notes.txt", f"token {planted_token()}\n")
        self.commit("chore: add a note")
        output = (run := self.push()).stdout + run.stderr
        self.assertIn("REDACTED", output)
        self.assertNotIn(planted_token(), output)

    def test_a_branch_that_is_not_the_checked_out_head_is_scanned_by_what_git_sends(self) -> None:
        """Finding 1: a pushed ref other than HEAD with a secret and a bad message is stopped, and the remote does not get the branch."""
        self.secret_branch("side", "added stuff")
        run = self.push_to("origin", "side")
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(f"push gate stopped by {push_gate.SECRETS}", run.stdout + run.stderr)
        self.assertFalse(self.remote_has(self.remote, "refs/heads/side"))

    def test_a_clean_branch_that_is_not_the_checked_out_head_is_refused_by_name(self) -> None:
        """The working-tree checks read HEAD, so a pushed commit other than HEAD cannot be vouched for."""
        self.git("checkout", "--quiet", "-b", "side")
        self.write("notes.txt", "side\n")
        self.commit("chore: side note")
        self.git("checkout", "--quiet", "main")
        run = self.push_to("origin", "side")
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(f"push gate stopped by {push_gate.HEAD}", run.stdout + run.stderr)
        self.assertFalse(self.remote_has(self.remote, "refs/heads/side"))

    def test_a_first_push_to_a_second_remote_is_scanned_although_the_first_remote_tracks_the_tip(self) -> None:
        self.write("notes.txt", f"token {planted_token()}\n")
        self.commit("chore: add a note")
        self.assertEqual(self.push_to("--no-verify", "origin", "main").returncode, 0, "the secret reached the first remote without the gate")
        other = self.make_remote("other")
        run = self.push_to("other", "main")
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn(f"push gate stopped by {push_gate.SECRETS}", run.stdout + run.stderr)
        self.assertFalse(self.remote_has(other, "refs/heads/main"))

    def test_a_stale_tracking_ref_does_not_hide_what_the_remote_lacks(self) -> None:
        """The remote is behind the tip that origin/main still names: the range starts at the sha that the remote holds."""
        baseline = self.git("rev-parse", "HEAD").stdout.strip()
        self.write("notes.txt", f"token {planted_token()}\n")
        self.commit("chore: add a note")
        self.assertEqual(self.push_to("--no-verify", "origin", "main").returncode, 0)
        run_command("git", ["--git-dir", str(self.remote), "update-ref", "refs/heads/main", baseline], env=self.env, timeout=TIMEOUT_SECONDS, check=True)
        self.assertEqual(self.git("rev-parse", "origin/main").stdout.strip(), self.git("rev-parse", "HEAD").stdout.strip(), "the tracking ref is stale")
        self.assert_refused_by(push_gate.SECRETS)

    def test_a_delete_sends_nothing_and_passes_with_a_changed_tracked_file(self) -> None:
        self.assertEqual(self.push_to("origin", "main:extra").returncode, 0)
        self.write("notes.txt", "uncommitted\n")
        run = self.push_to("origin", ":extra")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertFalse(self.remote_has(self.remote, "refs/heads/extra"))

    def test_a_tag_on_a_commit_that_the_remote_holds_runs_no_check(self) -> None:
        self.git("tag", "v1")
        run = self.push_to("origin", "v1")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertNotIn("push gate", run.stdout + run.stderr)

    def test_a_secret_in_the_body_of_a_valid_commit_message_stops_the_push(self) -> None:
        self.write("notes.txt", "second\n")
        self.commit(f"chore: add a note\n\ntoken {planted_token()}")
        self.assert_refused_by(push_gate.SECRETS, also="RuleID:")

    def test_a_deleted_file_selects_the_tests_that_name_it(self) -> None:
        self.git("rm", "--quiet", "--", "app/calc.py")
        self.git("commit", "--quiet", "-m", "refactor: remove the calculator")
        self.assert_refused_by(push_gate.QUICK)

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

    def test_the_hosted_history_scan_finds_the_secret_that_only_a_merge_adds(self) -> None:
        """Finding 3: the hook that the hosted run uses reads merge diffs too, so the secret that the push gate finds is found there as well."""
        self.git("checkout", "--quiet", "-b", "side")
        self.write("notes.txt", "side\n")
        self.commit("chore: side note")
        self.git("checkout", "--quiet", "main")
        self.write("notes.txt", "main\n")
        self.commit("chore: main note")
        run_command("git", ["merge", "--no-ff", "-m", "chore: merge the side note", "side"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
        self.write("notes.txt", f"token {planted_token()}\n")
        self.git("add", "--", "notes.txt")
        self.git("commit", "--quiet", "-m", "chore: merge the side note")
        config: Any = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
        entry = next(hook["entry"] for repo in config["repos"] for hook in repo["hooks"] if hook["id"] == "gitleaks-history")
        program, *arguments = shlex.split(entry)
        run = run_command(str(ROOT / program), [*arguments, "."], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)

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
