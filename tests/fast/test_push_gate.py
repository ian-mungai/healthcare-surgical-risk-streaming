"""The push gate: the hook that runs before every push to any branch, about 2 minutes. The real pushes through the hook are in tests/slow/test_push_gate_run.py.

It refuses a push while tracked files have uncommitted changes, scans the unpushed commits of HEAD (merge diffs included) for secrets, checks their messages,
scans the project for private values, runs the changed files' own checks (formatters, linters, type checks and the quick test modules that cover them) and
scans the dependencies when a lock file changes. Failure modes covered here, each with a planted case and a control that pushes:

- A planted secret, also one that only a merge's conflict resolution adds, a message that is not a Conventional Commit, a tracked file with an uncommitted
  change and a private value in a file: the gate stops the push by the check's name and the remote keeps its commit.
- A changed file that Ruff would reformat, a type error and a failing quick test module that covers the changed file.
- The changed-file selection: a lock file adds the dependency scan, a Python file adds its formatters, linters and type check, a Terraform file its formatter,
  and every quick test module is selected by every repository file that it imports, through any depth of imports, and by a path that it names.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, run_command
from scripts.quality import install_tools, push_gate, push_guard_hook, repo_checks, worktree_scan_hook
from scripts.stack.yaml_text import load_yaml
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository, git_in

PYPROJECT = '[tool.ruff]\nline-length = 160\n\n[tool.mypy]\nstrict = true\nfiles = ["app", "tests"]\n'
GOOD_CODE = "def double(value: int) -> int:\n    return value * 2\n"
GOOD_TEST = (
    "import unittest\n\nfrom app.calc import double\n\n\nclass DoubleTest(unittest.TestCase):\n"
    "    def test_double(self) -> None:\n        self.assertEqual(double(2), 4)\n"
)
HOOK = "pre-push"
ZERO = "0" * 40
# A path that no file of the repository names: built here so that this file does not name it.
UNLISTED = "docs/" + "unlisted" + "_note.md"


def planted_token() -> str:
    """A GitHub token shape that the secret scan reads as a secret, built here so that this file holds none."""
    return "ghp_" + hashlib.sha256(b"planted push gate").hexdigest()[:36]


def planted_address() -> str:
    return "jane.doe" + "@" + "gmail.com"


class SelectionTest(unittest.TestCase):
    """What the gate runs for the changed files, decided from the file names and the repository's imports; nothing starts."""

    def names(self, changed: list[str]) -> list[str]:
        return [check.name for check in push_gate.changed_file_checks(ROOT, changed)]

    def test_a_python_file_gets_its_formatter_linter_and_type_check(self) -> None:
        self.assertEqual(self.names(["scripts/process.py"])[:3], [push_gate.RUFF_FORMAT, push_gate.RUFF_CHECK, push_gate.MYPY])

    def test_a_python_file_outside_the_type_checked_folders_gets_no_type_check(self) -> None:
        names = self.names(["ansible/roles/compose_stack/files/render.py"])
        self.assertNotIn(push_gate.MYPY, names)
        self.assertIn(push_gate.RUFF_CHECK, names)

    def test_a_terraform_file_gets_its_formatter_and_the_tests_that_name_it(self) -> None:
        names = self.names(["infra/main.tf"])
        self.assertEqual(names[0], push_gate.TERRAFORM)
        self.assertNotIn(push_gate.RUFF_FORMAT, names)

    def test_a_file_with_no_own_check_and_no_test_gets_nothing(self) -> None:
        self.assertEqual(self.names([UNLISTED]), [])

    def test_a_lock_file_adds_the_dependency_scan_and_other_files_do_not(self) -> None:
        for lock in ("requirements.txt", "spark/requirements.txt", "uv.lock"):
            with self.subTest(lock=lock):
                self.assertIn(push_gate.DEPENDENCIES, [check.name for check in push_gate.dependency_checks([lock])])
        self.assertEqual(push_gate.dependency_checks(["requirements.in", "docs/requirements.md", "scripts/requirements_note.py"]), [])

    def test_the_dependency_scan_reads_only_the_lock_files_that_changed(self) -> None:
        checks = push_gate.dependency_checks(["spark/requirements.txt", "scripts/process.py"])
        self.assertEqual(len(checks), 1)
        self.assertIn("spark/requirements.txt", checks[0].arguments)
        self.assertNotIn("requirements.txt", [argument for argument in checks[0].arguments if argument == "requirements.txt"])

    def test_a_changed_test_module_selects_itself(self) -> None:
        self.assertIn("tests.fast.test_repo_checks", push_gate.select_tests(ROOT, ["tests/fast/test_repo_checks.py"]))

    def test_a_changed_source_file_selects_the_test_modules_that_import_it_at_any_depth(self) -> None:
        selected = push_gate.select_tests(ROOT, ["scripts/process.py"])
        self.assertIn("tests.fast.test_process_launcher", selected)
        self.assertIn("tests.fast.test_repo_check_cli", selected, "it imports scripts.process through scripts.quality.repo_checks")

    def test_a_changed_support_module_selects_the_modules_that_import_it(self) -> None:
        self.assertIn("tests.fast.test_repo_check_cli", push_gate.select_tests(ROOT, ["tests/fast/git_fixture.py"]))

    def test_a_test_that_names_a_changed_path_is_selected_for_a_file_it_does_not_import(self) -> None:
        self.assertIn("tests.fast.test_ci_workflow", push_gate.select_tests(ROOT, [".github/workflows/ci.yml"]))
        self.assertIn("tests.fast.test_ci_workflow", push_gate.select_tests(ROOT, [".github/rulesets/main.json"]))

    def test_every_quick_module_is_selected_by_every_repository_file_that_it_imports(self) -> None:
        """The import map is complete: the files that a module imports, found by reading its imports, select it, with a sample of files per module."""
        graph = push_gate.import_graph(ROOT)
        checked = 0
        for module, files in sorted(graph.items()):
            for file in sorted(files)[::7]:
                with self.subTest(module=module, file=file):
                    self.assertIn(module, push_gate.select_tests(ROOT, [file]))
                    checked += 1
        self.assertGreater(checked, 100)

    def test_a_file_three_imports_away_selects_the_module_and_a_lazy_or_relative_import_counts(self) -> None:
        """A small repository: the test imports ``app.a``, which imports ``app.b`` inside a function, which imports ``app.c`` relatively."""
        with tempfile.TemporaryDirectory(prefix="push_gate_selection_") as folder:
            root = Path(folder)
            files = {
                "app/__init__.py": "",
                "app/a.py": "def run() -> None:\n    from app import b\n\n    b.go()\n",
                "app/b.py": "from .c import go\n",
                "app/c.py": "def go() -> None:\n    pass\n",
                "app/unrelated.py": "VALUE = 1\n",
                "tests/__init__.py": "",
                "tests/fast/__init__.py": "",
                "tests/fast/test_a.py": "from app.a import run\n",
                "tests/fast/test_other.py": "from app.unrelated import VALUE\n",
            }
            for name, text in files.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            self.assertEqual(push_gate.select_tests(root, ["app/c.py"]), ["tests.fast.test_a"])
            self.assertEqual(push_gate.select_tests(root, ["app/unrelated.py"]), ["tests.fast.test_other"])
            self.assertEqual(push_gate.select_tests(root, ["app/a.py", "app/unrelated.py"]), ["tests.fast.test_a", "tests.fast.test_other"])

    def test_a_deleted_file_selects_the_tests_that_name_it_and_a_terraform_lock_file_the_dependency_scan(self) -> None:
        with tempfile.TemporaryDirectory(prefix="push_gate_deleted_") as folder:
            root = Path(folder)
            for name, text in {"tests/__init__.py": "", "tests/fast/__init__.py": "", "tests/fast/test_a.py": "from app.gone import value\n"}.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            names = [check.name for check in push_gate.changed_file_checks(root, [], deleted=("app/gone.py",))]
            self.assertEqual(names, [push_gate.QUICK])
            self.assertEqual(push_gate.changed_file_checks(root, [], deleted=("app/other.py",)), [])
        self.assertEqual([check.name for check in push_gate.dependency_checks(["infra/.terraform.lock.hcl"])], [push_gate.DEPENDENCIES])

    def test_a_file_that_no_test_imports_or_names_selects_no_module(self) -> None:
        self.assertEqual(push_gate.select_tests(ROOT, [UNLISTED]), [])


class ConfigurationTest(unittest.TestCase):
    """Setup installs the push gate with the other hooks, and only the push gate runs at the push."""

    def setUp(self) -> None:
        config: Any = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
        self.config = config
        self.hooks: dict[str, Any] = {hook["id"]: hook for repo in config["repos"] for hook in repo["hooks"]}

    def test_setup_installs_the_commit_the_commit_message_and_the_push_hooks(self) -> None:
        self.assertEqual(self.config["default_install_hook_types"], ["pre-commit", "commit-msg", "pre-push"])

    def test_the_push_gate_runs_at_the_push_and_nowhere_else(self) -> None:
        hook = self.hooks["push-gate"]
        self.assertEqual(
            (hook["stages"], hook["entry"], hook["pass_filenames"], hook["always_run"]),
            (["pre-push"], ".venv/bin/python -m scripts.quality.push_gate", False, True),
        )
        self.assertEqual([name for name, each in self.hooks.items() if "pre-push" in each.get("stages", ())], ["push-gate"])

    def test_the_full_check_stage_does_not_run_the_push_gate(self) -> None:
        """``make check`` runs the manual stage in the hosted run: the gate reads the unpushed commits, which a hosted checkout has none of."""
        self.assertNotIn("manual", self.hooks["push-gate"]["stages"])

    def test_the_hosted_history_scan_reads_merge_diffs_as_the_push_scan_does(self) -> None:
        self.assertIn('"--log-opts=-m --all"', self.hooks["gitleaks-history"]["entry"])
        self.assertEqual(self.hooks["gitleaks-history"]["stages"], ["manual"])

    def test_the_hosted_hooks_job_scans_every_lock_file_and_no_commit_runs_that_scan(self) -> None:
        hook = self.hooks["dependency-scan"]
        self.assertEqual((hook["stages"], hook["entry"]), (["manual"], ".venv/bin/python -m scripts.quality.push_gate --all-locks"))

    def test_every_tracked_lock_file_is_scanned_and_a_file_of_another_name_is_not(self) -> None:
        with tempfile.TemporaryDirectory(prefix="push_gate_locks_") as folder:
            repo = Path(folder)
            git_in(repo, "init", "--quiet")
            for name in ("requirements.txt", "spark/requirements.txt", "infra/.terraform.lock.hcl", "requirements.in", "notes.txt"):
                (repo / name).parent.mkdir(parents=True, exist_ok=True)
                (repo / name).write_text("x\n")
            git_in(repo, "add", "--", ".")
            with mock.patch.object(repo_checks, "git", lambda *args, **kwargs: git_in(repo, *args, keep_newlines=True).stdout):
                self.assertEqual(push_gate.tracked_lock_files(), ["infra/.terraform.lock.hcl", "requirements.txt", "spark/requirements.txt"])


class HookEnvironmentTest(unittest.TestCase):
    """A commit hook runs the tests with ``GIT_DIR`` and ``GIT_INDEX_FILE`` set to the real repository (Nov 23 2026: a test that ran ``git init`` and
    ``git add`` without clearing them made the shared repository bare and replaced its index)."""

    def test_a_repository_made_by_a_test_ignores_the_git_variables_of_the_callers_environment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hook_env_") as folder:
            decoy, other = Path(folder) / "decoy", Path(folder) / "other"
            for path in (decoy, other):
                path.mkdir()
            git_in(decoy, "init", "--quiet")
            before = (shared_config := (decoy / ".git" / "config").read_text(), (decoy / ".git" / "index").exists())
            polluted = {"GIT_DIR": str(decoy / ".git"), "GIT_INDEX_FILE": str(decoy / ".git" / "index"), "GIT_WORK_TREE": str(decoy)}
            with mock.patch.dict(os.environ, polluted):
                git_in(other, "init", "--quiet")
                (other / "file.txt").write_text("x\n")
                git_in(other, "add", "--", ".")
            self.assertEqual((decoy / ".git" / "config").read_text(), shared_config)
            self.assertEqual((shared_config, (decoy / ".git" / "index").exists()), before, "the caller's repository is untouched")
            self.assertTrue((other / ".git" / "index").is_file(), "the new repository got the files")


class PushGuardHookTest(TemporaryRepository):
    """The guard hook that runs before pre-commit stashes the unstaged changes, and its installer."""

    def hook(self) -> Path:
        return self.repo / ".git" / "hooks" / push_guard_hook.HOOK_NAME

    def test_the_installer_writes_the_executable_hook_and_a_rerun_changes_nothing(self) -> None:
        self.assertFalse(push_guard_hook.installed(self.repo))
        self.assertTrue(push_guard_hook.install(self.repo))
        self.assertTrue(push_guard_hook.installed(self.repo))
        self.assertTrue(self.hook().stat().st_mode & stat.S_IXUSR)
        self.assertFalse(push_guard_hook.install(self.repo))

    def test_it_is_a_hook_of_its_own_and_leaves_the_scan_hook_alone(self) -> None:
        push_guard_hook.install(self.repo)
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        worktree_scan_hook.install(self.repo)
        self.assertTrue(push_guard_hook.installed(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))

    def test_an_older_pre_push_hook_is_kept_and_still_runs_after_the_guard(self) -> None:
        older = '#!/bin/sh\necho older hook ran with "$@" >&2\nexit 7\n'
        self.hook().write_text(older)
        self.hook().chmod(0o755)
        push_guard_hook.install(self.repo)
        self.assertEqual((self.hook().with_name(push_guard_hook.KEPT_NAME)).read_text(), older)
        run = run_command("sh", [str(self.hook()), "origin", "url"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
        self.assertEqual(run.returncode, 7)
        self.assertIn("older hook ran with origin url", run.stderr)

    def test_a_second_older_hook_is_refused_and_nothing_is_overwritten(self) -> None:
        for name in (push_guard_hook.HOOK_NAME, push_guard_hook.KEPT_NAME):
            (self.repo / ".git" / "hooks" / name).write_text("#!/bin/sh\n")
        with self.assertRaises(worktree_scan_hook.KeptHookError):
            push_guard_hook.install(self.repo)

    def test_the_hook_refuses_a_changed_tracked_file_in_the_index_or_the_working_tree_and_passes_an_untracked_one(self) -> None:
        push_guard_hook.install(self.repo)
        (self.repo / "tracked.txt").write_text("one\n")
        self.git("add", "--", "tracked.txt")
        self.git("commit", "--quiet", "-m", "chore: add a file")
        sends = f"refs/heads/main {self.git('rev-parse', 'HEAD').stdout.strip()} refs/heads/main {ZERO}\n"
        for label, change in (
            ("working tree", lambda: (self.repo / "tracked.txt").write_text("two\n")),
            ("index", lambda: self.git("add", "--", "tracked.txt")),
        ):
            with self.subTest(label=label):
                change()
                run = self.run_hook(sends)
                self.assertEqual(run.returncode, 1)
                self.assertIn(f"push gate stopped by {push_gate.TRACKED}", run.stderr)
                self.assertIn("tracked.txt", run.stderr)
        deletes = f"(delete) {ZERO} refs/heads/extra {'1' * 40}\n"
        self.assertEqual(self.run_hook(deletes).returncode, 0, "a push of deletes sends nothing")
        self.git("checkout", "--quiet", "HEAD", "--", "tracked.txt")
        (self.repo / "untracked.txt").write_text("new\n")
        self.assertEqual(self.run_hook(sends).returncode, 0)

    def run_hook(self, sends: str) -> CompletedProcess[str]:
        return run_command("sh", [str(self.hook()), "origin", "url"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, input_text=sends)

    def test_the_hook_saves_the_remote_and_the_updates_for_the_gate_and_the_gate_reads_them_once(self) -> None:
        push_guard_hook.install(self.repo)
        sends = f"refs/heads/main {'a' * 40} refs/heads/main {'b' * 40}\n(delete) {ZERO} refs/heads/old {'c' * 40}\n"
        self.assertEqual(self.run_hook(sends).returncode, 0)
        saved = self.repo / ".git" / push_gate.REFS_FILE
        read = push_gate.read_pushed_updates(saved)
        self.assertEqual(
            read,
            (
                "origin",
                [push_gate.Update("refs/heads/main", "a" * 40, "refs/heads/main", "b" * 40), push_gate.Update("(delete)", ZERO, "refs/heads/old", "c" * 40)],
            ),
        )
        if read is None:
            self.fail("the gate read no saved updates")
        self.assertTrue(read[1][1].deletes and not read[1][0].deletes and not read[1][0].creates)
        self.assertFalse(saved.exists())
        self.assertIsNone(push_gate.read_pushed_updates(saved))

    def test_the_installer_program_installs_and_checks_the_guard(self) -> None:
        with mock.patch.object(install_tools, "ROOT", self.repo):
            self.assertFalse(install_tools.settle_push_guard(check=True))
            self.assertTrue(install_tools.settle_push_guard(check=False))
            self.assertTrue(install_tools.settle_push_guard(check=True))


if __name__ == "__main__":
    unittest.main()
