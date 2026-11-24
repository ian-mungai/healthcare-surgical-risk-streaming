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

import contextlib
import hashlib
import io
import itertools
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, run_command
from scripts.quality import install_tools, push_gate, push_hook, repo_checks, worktree_scan_hook
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

    def test_a_deleted_file_selects_the_tests_that_name_it_and_a_terraform_lock_file_is_not_scanned(self) -> None:
        with tempfile.TemporaryDirectory(prefix="push_gate_deleted_") as folder:
            root = Path(folder)
            for name, text in {"tests/__init__.py": "", "tests/fast/__init__.py": "", "tests/fast/test_a.py": "from app.gone import value\n"}.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            names = [check.name for check in push_gate.changed_file_checks(root, [], deleted=("app/gone.py",))]
            self.assertEqual(names, [push_gate.QUICK])
            self.assertEqual(push_gate.changed_file_checks(root, [], deleted=("docs/other.md",)), [])
        self.assertEqual(push_gate.dependency_checks(["infra/.terraform.lock.hcl"]), [], "Trivy finds no package in a Terraform lock file")

    def test_a_rename_or_a_delete_of_a_python_file_selects_the_whole_quick_group(self) -> None:
        """Round 2, finding 3: the tests that imported the old path cannot be found from the new graph, so the whole group runs."""
        with tempfile.TemporaryDirectory(prefix="push_gate_removed_") as folder:
            root = Path(folder)
            files = {
                "tests/__init__.py": "",
                "tests/fast/__init__.py": "",
                "tests/fast/test_a.py": "def test() -> None:\n    pass\n",
                "tests/fast/test_b.py": "def test() -> None:\n    pass\n",
                "app/new.py": "VALUE = 1\n",
            }
            for name, text in files.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            both = ("-m", "unittest", "tests.fast.test_a", "tests.fast.test_b")
            for label, changed, removed in (("a rename", ["app/new.py"], ("app/old.py",)), ("a delete", [], ("app/old.py",))):
                with self.subTest(label=label):
                    quick = [check for check in push_gate.changed_file_checks(root, changed, deleted=removed) if check.name == push_gate.QUICK]
                    self.assertEqual([check.arguments for check in quick], [both])
            self.assertEqual(push_gate.changed_file_checks(root, [], deleted=("docs/old.md",)), [], "a removed file that is not Python selects only by name")

    def test_a_rename_followed_by_recreating_the_old_path_still_counts_the_old_path_as_removed(self) -> None:
        """Round 3, finding 4: the source of a rename is removed even when a later commit puts a file at that path again."""
        with tempfile.TemporaryDirectory(prefix="push_gate_recreate_") as folder:
            repo = Path(folder)
            git_in(repo, "init", "--quiet")
            commit = ("-c", "user.name=T", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m")
            (repo / "old.py").write_text("VALUE = 1\n" * 20)
            git_in(repo, "add", "--", ".")
            git_in(repo, *commit, "chore: add")
            git_in(repo, "mv", "old.py", "new.py")
            git_in(repo, *commit, "refactor: rename")
            (repo / "old.py").write_text("OTHER = 2\n")
            git_in(repo, "add", "--", ".")
            git_in(repo, *commit, "feat: recreate the old path")
            commits = git_in(repo, "rev-list", "HEAD").stdout.split()
            with mock.patch.object(repo_checks, "git", lambda *args, **kwargs: git_in(repo, *args, keep_newlines=kwargs.get("keep_newlines", False)).stdout):
                present, removed = push_gate.changed_files(repo, commits[:2])
            self.assertEqual((present, removed), (["new.py", "old.py"], ["old.py"]))

    def test_the_changed_files_of_a_rename_name_the_new_path_and_list_the_old_one_as_removed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="push_gate_rename_") as folder:
            repo = Path(folder)
            git_in(repo, "init", "--quiet")
            (repo / "old.py").write_text("VALUE = 1\n" * 20)
            git_in(repo, "add", "--", ".")
            git_in(repo, "-c", "user.name=T", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m", "chore: add")
            git_in(repo, "mv", "old.py", "new.py")
            git_in(repo, "-c", "user.name=T", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m", "refactor: rename")
            commits = git_in(repo, "rev-list", "HEAD").stdout.split()
            with mock.patch.object(repo_checks, "git", lambda *args, **kwargs: git_in(repo, *args, keep_newlines=kwargs.get("keep_newlines", False)).stdout):
                self.assertEqual(push_gate.changed_files(repo, commits[:1]), (["new.py"], ["old.py"]))

    def test_a_file_that_no_test_imports_or_names_selects_no_module(self) -> None:
        self.assertEqual(push_gate.select_tests(ROOT, [UNLISTED]), [])


class ConfigurationTest(unittest.TestCase):
    """The hooks that pre-commit installs, and the manual stage that the hosted run adds."""

    def setUp(self) -> None:
        config: Any = load_yaml((ROOT / ".pre-commit-config.yaml").read_text())
        self.config = config
        self.hooks: dict[str, Any] = {hook["id"]: hook for repo in config["repos"] for hook in repo["hooks"]}

    def test_pre_commit_installs_the_commit_and_commit_message_hooks_and_no_push_hook(self) -> None:
        self.assertEqual(self.config["default_install_hook_types"], ["pre-commit", "commit-msg"])

    def test_the_push_gate_is_no_pre_commit_hook_because_pre_commit_gives_its_hooks_no_input(self) -> None:
        self.assertNotIn("push-gate", self.hooks)
        self.assertEqual([name for name, each in self.hooks.items() if "pre-push" in each.get("stages", ())], [])

    def test_the_hosted_manual_stage_scans_the_messages_of_the_whole_history(self) -> None:
        hook = self.hooks["commit-message-history"]
        self.assertEqual((hook["stages"], hook["entry"]), (["manual"], ".venv/bin/python -m scripts.quality.repo_checks commit-range HEAD"))

    def test_the_hook_text_runs_the_gate_with_the_arguments_and_the_input_of_git(self) -> None:
        self.assertIn('.venv/bin/python -m scripts.quality.push_gate "$@"', push_hook.HOOK_TEXT)
        self.assertIn('refs="$(cat)"', push_hook.HOOK_TEXT)
        self.assertNotIn("push_gate_refs", push_hook.HOOK_TEXT)

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
                self.assertEqual(push_gate.tracked_lock_files(), ["requirements.txt", "spark/requirements.txt"])


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


class RepositoryWalkTest(unittest.TestCase):
    """A folder that cannot be read is an error, never a folder with no Python file: a test module that imports a file in it would not be selected."""

    def test_a_folder_that_cannot_be_read_stops_the_walk_and_a_readable_one_is_listed(self) -> None:
        with tempfile.TemporaryDirectory() as made:
            root = Path(made).resolve()
            (root / "pkg").mkdir()
            (root / "pkg" / "inner.py").write_text("X = 1\n")
            (root / "locked").mkdir()
            (root / "locked" / "hidden.py").write_text("Y = 1\n")
            self.assertEqual({"pkg.inner": "pkg/inner.py", "locked.hidden": "locked/hidden.py"}, push_gate.repository_python_files(root))
            push_gate.repository_python_files.cache_clear()
            (root / "locked").chmod(0)
            try:
                with self.assertRaises(PermissionError):
                    push_gate.repository_python_files(root)
            finally:
                (root / "locked").chmod(0o755)
                push_gate.repository_python_files.cache_clear()


class UnreadableFolderTest(TemporaryRepository):
    """Join round 1, finding 2: a folder that cannot be read stops the gate with one fixed line, never a traceback that prints the folder's path."""

    def test_the_gate_command_names_an_unreadable_folder_by_a_fixed_line_and_prints_no_path_or_traceback(self) -> None:
        (self.repo / "locked_folder_name").mkdir()
        (self.repo / "locked_folder_name" / "hidden.py").write_text("Y = 1\n")
        (self.repo / "changed.py").write_text("X = 1\n")
        (self.repo / push_gate.QUICK_FOLDER).mkdir(parents=True)
        (self.repo / push_gate.QUICK_FOLDER / "test_one.py").write_text("X = 1\n")
        self.git("add", "--", "changed.py", push_gate.QUICK_FOLDER)
        self.git("commit", "--quiet", "-m", "feat: add a module")
        head = self.git("rev-parse", "HEAD").stdout.strip()
        (self.repo / "locked_folder_name").chmod(0)
        try:
            run = run_command(
                sys.executable,
                ["-m", "scripts.quality.push_gate", "origin", "url"],
                cwd=self.repo,
                env=self.env,
                timeout=TIMEOUT_SECONDS,
                input_text=f"refs/heads/main {head} refs/heads/main {ZERO}\n",
            )
        finally:
            (self.repo / "locked_folder_name").chmod(0o755)
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
        self.assertEqual(run.stderr, f"{push_gate.UNREADABLE}\n")
        self.assertNotIn("Traceback", run.stderr)
        self.assertNotIn("locked_folder_name", run.stdout + run.stderr)

    def test_a_filesystem_error_at_the_command_boundary_prints_the_fixed_line_and_returns_1(self) -> None:
        error = PermissionError(13, "Permission denied", "/private/folder")
        err = io.StringIO()
        with mock.patch.object(push_gate, "main", side_effect=error), contextlib.redirect_stderr(err):
            self.assertEqual(push_gate.command("", by_hook=True), 1)
        self.assertEqual(err.getvalue(), f"{push_gate.UNREADABLE}\n")

    def test_a_failure_that_is_not_a_filesystem_error_is_not_hidden(self) -> None:
        with mock.patch.object(push_gate, "main", side_effect=ValueError("a defect")), self.assertRaises(ValueError):
            push_gate.command("", by_hook=True)


class PredecessorGuardTest(TemporaryRepository):
    """Round 2, finding 4: the guards of earlier versions sit in ``pre-push.legacy`` and refuse a push that sends only deletes; the installer removes them."""

    def test_the_installer_removes_a_shipped_predecessor_guard_and_keeps_an_older_hook_of_the_owner(self) -> None:
        legacy = self.repo / ".git" / "hooks" / "pre-push.legacy"
        for text in push_hook.PREDECESSOR_TEXTS:
            legacy.write_text(text)
            legacy.chmod(0o755)
            with mock.patch.object(install_tools, "ROOT", self.repo):
                self.assertTrue(install_tools.settle_push_guard(check=False))
            self.assertFalse(legacy.exists(), "the predecessor guard is gone")
        legacy.write_text("#!/bin/sh\necho the owner's own hook\n")
        with mock.patch.object(install_tools, "ROOT", self.repo):
            install_tools.settle_push_guard(check=False)
        self.assertTrue(legacy.exists(), "a hook that the project never shipped stays")


class PushHookTest(TemporaryRepository):
    """The plain pre-push hook that runs the gate with Git's input, and its installer."""

    def hook(self) -> Path:
        return self.repo / ".git" / "hooks" / push_hook.HOOK_NAME

    def test_the_installer_writes_the_executable_hook_and_a_rerun_changes_nothing(self) -> None:
        self.assertFalse(push_hook.installed(self.repo))
        self.assertTrue(push_hook.install(self.repo))
        self.assertTrue(push_hook.installed(self.repo))
        self.assertTrue(self.hook().stat().st_mode & stat.S_IXUSR)
        self.assertFalse(push_hook.install(self.repo))

    def test_it_is_a_hook_of_its_own_and_leaves_the_scan_hook_alone(self) -> None:
        push_hook.install(self.repo)
        self.assertFalse(worktree_scan_hook.installed(self.repo))
        worktree_scan_hook.install(self.repo)
        self.assertTrue(push_hook.installed(self.repo))
        self.assertTrue(worktree_scan_hook.installed(self.repo))

    def stub_gate(self, exit_code: int) -> None:
        """A gate that records its arguments and input and exits as told, where the hook looks for it."""
        stub = self.repo / ".venv" / "bin" / "python"
        stub.parent.mkdir(parents=True, exist_ok=True)
        stub.write_text(f'#!/bin/sh\necho "gate $@" > "{self.repo}/gate_args"\ncat > "{self.repo}/gate_input"\nexit {exit_code}\n')
        stub.chmod(0o755)
        gate = self.repo / "scripts" / "quality" / "push_gate.py"
        gate.parent.mkdir(parents=True, exist_ok=True)
        gate.write_text(f"GATE_VERSION = {push_gate.GATE_VERSION}\n")

    def test_an_older_pre_push_hook_is_kept_and_runs_after_the_gate_with_the_same_arguments_and_input(self) -> None:
        older = f'#!/bin/sh\necho "older $@" > "{self.repo}/older_args"\ncat > "{self.repo}/older_input"\nexit 7\n'
        self.hook().write_text(older)
        self.hook().chmod(0o755)
        push_hook.install(self.repo)
        self.assertEqual(self.hook().with_name(push_hook.KEPT_NAME).read_text(), older)
        self.stub_gate(0)
        lines = f"HEAD@{{2 days ago}} {'a' * 40} refs/heads/main {'b' * 40}\n"
        run = self.run_hook(lines)
        self.assertEqual(run.returncode, 7, run.stderr)
        self.assertEqual((self.repo / "gate_input").read_text(), lines, "the gate reads Git's input, spaces and all")
        self.assertEqual((self.repo / "older_input").read_text(), lines)
        self.assertEqual((self.repo / "gate_args").read_text(), "gate -m scripts.quality.push_gate origin url\n")
        self.assertEqual((self.repo / "older_args").read_text(), "older origin url\n")

    def test_a_failed_gate_stops_the_hook_and_the_older_hook_does_not_run(self) -> None:
        self.hook().write_text(f'#!/bin/sh\necho ran > "{self.repo}/older_ran"\n')
        self.hook().chmod(0o755)
        push_hook.install(self.repo)
        self.stub_gate(3)
        self.assertEqual(self.run_hook("x\n").returncode, 3)
        self.assertFalse((self.repo / "older_ran").exists())

    def test_the_hook_saves_no_file_of_the_input(self) -> None:
        push_hook.install(self.repo)
        self.stub_gate(0)
        before = sorted(path.name for path in (self.repo / ".git").iterdir())
        self.run_hook(f"refs/heads/main {'a' * 40} refs/heads/main {ZERO}\n")
        self.assertEqual(sorted(path.name for path in (self.repo / ".git").iterdir()), before)

    def test_a_pre_push_file_that_holds_an_earlier_text_of_the_scan_hook_is_the_owners_hook_and_is_kept(self) -> None:
        """Each hook knows only its own earlier texts: the scan hook's earlier text in ``pre-push`` is not a file that this project shipped for the gate."""
        earlier = worktree_scan_hook.EARLIER_HOOK_TEXTS[0]
        self.hook().write_text(earlier)
        self.hook().chmod(0o755)
        push_hook.install(self.repo)
        self.assertEqual(earlier, self.hook().with_name(push_hook.KEPT_NAME).read_text())
        self.assertTrue(push_hook.installed(self.repo))

    def test_a_second_older_hook_is_refused_and_nothing_is_overwritten(self) -> None:
        for name in (push_hook.HOOK_NAME, push_hook.KEPT_NAME):
            (self.repo / ".git" / "hooks" / name).write_text("#!/bin/sh\n")
        with self.assertRaises(worktree_scan_hook.KeptHookError):
            push_hook.install(self.repo)

    def test_a_kept_hook_that_cannot_be_kept_shows_the_fixed_repair_message(self) -> None:
        for name in (push_hook.HOOK_NAME, push_hook.KEPT_NAME):
            (self.repo / ".git" / "hooks" / name).write_text("#!/bin/sh\n")
        err = io.StringIO()
        with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(err):
            self.assertFalse(install_tools.settle_push_guard(check=False))
        self.assertEqual(f"push hook: {push_hook.KEPT_REFUSAL}\n", err.getvalue())

    def test_the_setup_entry_point_prints_the_fixed_repair_message_and_fails_when_an_owner_hook_cannot_be_kept(self) -> None:
        """Join round 4, finding 2: the line that setup printed was only ``push hook: KeptHookError``, which names no repair."""
        for name in (push_hook.HOOK_NAME, push_hook.KEPT_NAME):
            (self.repo / ".git" / "hooks" / name).write_text("#!/bin/sh\n")
        before = self.hooks_state()
        err = io.StringIO()
        with (
            mock.patch.object(install_tools, "ROOT", self.repo),
            mock.patch.object(install_tools, "settle", return_value=True),
            mock.patch.object(install_tools, "settle_hook", return_value=True),
            mock.patch.object(sys, "argv", ["install_tools"]),
            contextlib.redirect_stderr(err),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(1, install_tools.main())
        self.assertEqual(f"push hook: {push_hook.KEPT_REFUSAL}\n", err.getvalue())
        self.assertEqual(before, self.hooks_state())

    def test_a_pre_push_script_that_is_a_link_is_refused_and_neither_the_link_nor_its_target_changes(self) -> None:
        """The replacement of an earlier hook is the owned-file write: a link is not a regular file, so it is never followed or replaced."""
        target = self.repo / "pre_commit_script"
        target.write_text("#!/usr/bin/env python3\n# File generated by pre-commit: https://pre-commit.com\n")
        self.hook().symlink_to(target)
        before = target.read_text()
        with self.assertRaises(OSError):
            push_hook.install(self.repo)
        self.assertTrue(self.hook().is_symlink())
        self.assertEqual(before, target.read_text())

    def test_an_owner_hook_that_the_predecessor_guard_kept_keeps_running_after_the_guard_is_removed(self) -> None:
        """Round 3, finding 5: the guard kept the owner's hook as ``pre-push.legacy.previous``; removing the guard must not orphan it."""
        hooks = self.repo / ".git" / "hooks"
        (hooks / "pre-push.legacy").write_text(push_hook.PREDECESSOR_TEXTS[0])
        (hooks / "pre-push.legacy").chmod(0o755)
        (hooks / "pre-push.legacy.previous").write_text(f'#!/bin/sh\ncat > "{self.repo}/owner_input"\nexit 9\n')
        (hooks / "pre-push.legacy.previous").chmod(0o755)
        push_hook.install(self.repo)
        self.assertFalse((hooks / "pre-push.legacy").exists())
        self.assertFalse((hooks / "pre-push.legacy.previous").exists())
        self.stub_gate(0)
        run = self.run_hook("line\n")
        self.assertEqual(run.returncode, 9, run.stderr)
        self.assertEqual((self.repo / "owner_input").read_text(), "line\n")

    def test_a_hooks_folder_that_is_a_link_to_a_folder_keeps_the_owners_hook_and_writes_the_gate_in_the_real_folder(self) -> None:
        """Join round 2b, finding 1: the owner's hook moved aside and the write into the linked folder was then refused."""
        real = self.repo.parent / "shared_hooks"
        real.mkdir()
        (real / push_hook.HOOK_NAME).write_text("#!/bin/sh\nexit 7\n")
        (real / push_hook.HOOK_NAME).chmod(0o755)
        link = self.repo / ".git" / "hooks"
        shutil.rmtree(link)
        link.symlink_to(real)
        self.assertTrue(push_hook.install(self.repo))
        self.assertTrue(link.is_symlink())
        self.assertEqual("#!/bin/sh\nexit 7\n", (real / push_hook.KEPT_NAME).read_text())
        self.assertEqual(push_hook.HOOK_TEXT, (real / push_hook.HOOK_NAME).read_text())
        self.assertTrue(push_hook.installed(self.repo))
        self.assertFalse(push_hook.install(self.repo))

    def test_a_hooks_folder_that_leads_to_no_folder_is_refused_and_nothing_is_created_or_moved(self) -> None:
        link = self.repo / ".git" / "hooks"
        shutil.rmtree(link)
        gone = self.repo.parent / "gone_hooks"
        link.symlink_to(gone)
        with self.assertRaises(OSError):
            push_hook.install(self.repo)
        self.assertFalse(os.path.lexists(gone))
        self.assertEqual(str(gone), os.readlink(link))

    def hooks_state(self) -> dict[str, tuple[str, str | bytes]]:
        """Every entry of the hooks folder with its kind and content, a link by its target, so that a change of any name shows."""
        state: dict[str, tuple[str, str | bytes]] = {}
        for path in sorted((self.repo / ".git" / "hooks").iterdir()):
            state[path.name] = ("link", os.readlink(path)) if path.is_symlink() else ("file", path.read_bytes())
        return state

    def assert_refused_and_untouched(self) -> None:
        before = self.hooks_state()
        with self.assertRaises(worktree_scan_hook.KeptHookError) as refusal:
            push_hook.install(self.repo)
        self.assertEqual(before, self.hooks_state(), "a refused setup changes no entry of the hooks folder")
        self.assertEqual(str(refusal.exception), push_hook.KEPT_REFUSAL)
        err = io.StringIO()
        with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(err):
            self.assertFalse(install_tools.settle_push_guard(check=False))
        self.assertEqual(f"push hook: {push_hook.KEPT_REFUSAL}\n", err.getvalue())
        self.assertEqual(before, self.hooks_state())

    def test_two_different_owner_hooks_at_pre_push_and_pre_push_legacy_previous_are_refused_and_neither_changes(self) -> None:
        """Join round 1, finding 1: the first move took the kept name after the check, and the second move overwrote it."""
        hooks = self.repo / ".git" / "hooks"
        for name, text in ((push_hook.HOOK_NAME, "#!/bin/sh\necho first owner hook\n"), (push_hook.ORPHAN_NAME, "#!/bin/sh\necho second owner hook\n")):
            (hooks / name).write_text(text)
            (hooks / name).chmod(0o755)
        self.assert_refused_and_untouched()

    def test_the_same_two_owner_hooks_with_a_shipped_guard_between_them_are_refused_and_nothing_changes(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.HOOK_NAME).write_text("#!/bin/sh\necho first owner hook\n")
        (hooks / push_hook.PREDECESSOR_NAME).write_text(push_hook.PREDECESSOR_TEXTS[0])
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\necho second owner hook\n")
        self.assert_refused_and_untouched()

    def test_a_dangling_link_at_the_kept_name_is_a_taken_name_and_is_never_overwritten_by_the_orphan_move(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.PREDECESSOR_NAME).write_text(push_hook.PREDECESSOR_TEXTS[0])
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\necho owner hook\n")
        (hooks / push_hook.KEPT_NAME).symlink_to(self.repo / "missing_target")
        self.assert_refused_and_untouched()

    def test_a_dangling_link_at_the_kept_name_is_a_taken_name_for_an_owner_hook_at_pre_push(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.HOOK_NAME).write_text("#!/bin/sh\necho owner hook\n")
        (hooks / push_hook.KEPT_NAME).symlink_to(self.repo / "missing_target")
        self.assert_refused_and_untouched()

    def test_a_dangling_link_at_pre_push_is_an_owner_hook_that_needs_the_kept_name_like_the_orphan_does(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.HOOK_NAME).symlink_to(self.repo / "missing_target")
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\necho owner hook\n")
        self.assert_refused_and_untouched()

    def test_a_dangling_link_at_pre_push_alone_is_kept_as_it_is(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        target = self.repo / "missing_target"
        (hooks / push_hook.HOOK_NAME).symlink_to(target)
        push_hook.install(self.repo)
        self.assertEqual(os.readlink(hooks / push_hook.KEPT_NAME), str(target))
        self.assertTrue(push_hook.installed(self.repo))

    def test_a_pre_commit_script_at_pre_push_is_replaced_in_place_and_does_not_need_the_kept_name_for_the_orphan_move(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.HOOK_NAME).write_text("#!/bin/sh\n# File generated by pre-commit\n")
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\necho owner hook\n")
        self.assertTrue(push_hook.install(self.repo))
        self.assertEqual((hooks / push_hook.KEPT_NAME).read_text(), "#!/bin/sh\necho owner hook\n")
        self.assertEqual((hooks / push_hook.HOOK_NAME).read_text(), push_hook.HOOK_TEXT)
        self.assertFalse(os.path.lexists(hooks / push_hook.ORPHAN_NAME))

    def test_the_installed_hook_is_not_an_owner_hook_so_an_orphan_beside_it_is_moved_to_the_kept_name(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        push_hook.install(self.repo)
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\necho owner hook\n")
        self.assertTrue(push_hook.install(self.repo))
        self.assertEqual((hooks / push_hook.KEPT_NAME).read_text(), "#!/bin/sh\necho owner hook\n")
        self.assertEqual((hooks / push_hook.HOOK_NAME).read_text(), push_hook.HOOK_TEXT)

    def test_a_dangling_link_at_pre_push_legacy_with_an_orphan_hook_beside_it_is_refused_because_the_orphan_would_stop_running(self) -> None:
        """Join round 2, finding 1: the orphan is not moved while ``pre-push.legacy`` is an owner's file, and the new hook runs only ``pre-push.previous``."""
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.PREDECESSOR_NAME).symlink_to(self.repo / "missing_target")
        (hooks / push_hook.ORPHAN_NAME).write_text(f'#!/bin/sh\ncat > "{self.repo}/owner_input"\nexit 9\n')
        (hooks / push_hook.ORPHAN_NAME).chmod(0o755)
        self.assert_refused_and_untouched()

    def test_an_owner_hook_at_pre_push_legacy_is_refused_when_the_new_hook_would_replace_the_pre_commit_script_that_ran_it(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.HOOK_NAME).write_text("#!/bin/sh\n# File generated by pre-commit\n")
        (hooks / push_hook.PREDECESSOR_NAME).write_text("#!/bin/sh\necho owner hook\n")
        self.assert_refused_and_untouched()

    def test_an_owner_hook_at_pre_push_legacy_with_no_pre_push_is_refused_because_the_new_hook_would_not_run_it(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.PREDECESSOR_NAME).write_text("#!/bin/sh\necho owner hook\n")
        self.assert_refused_and_untouched()

    def test_an_owner_hook_at_pre_push_legacy_beside_the_installed_wrapper_is_refused_and_is_not_reported_as_installed_and_verified(self) -> None:
        """Join round 3, finding 1: an earlier installer stranded the owner's hook at ``pre-push.legacy``; the wrapper runs only ``pre-push.previous``."""
        hooks = self.repo / ".git" / "hooks"
        push_hook.install(self.repo)
        (hooks / push_hook.PREDECESSOR_NAME).write_text("#!/bin/sh\necho owner hook\n")
        (hooks / push_hook.PREDECESSOR_NAME).chmod(0o755)
        self.assertFalse(push_hook.installed(self.repo), "a stranded owner hook means that the push hook is not settled")
        before = self.hooks_state()
        for check in (False, True):
            out, err = io.StringIO(), io.StringIO()
            with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertFalse(install_tools.settle_push_guard(check=check))
            self.assertNotIn("installed and verified", out.getvalue())
        self.assert_refused_and_untouched()
        self.assertEqual(before, self.hooks_state())

    def test_a_dangling_link_at_pre_push_legacy_beside_the_installed_wrapper_is_not_an_owner_hook_and_the_setup_stays_settled(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        push_hook.install(self.repo)
        (hooks / push_hook.PREDECESSOR_NAME).symlink_to(self.repo / "missing_target")
        before = self.hooks_state()
        with mock.patch.object(install_tools, "ROOT", self.repo):
            self.assertTrue(install_tools.settle_push_guard(check=False))
        self.assertEqual(before, self.hooks_state())

    def test_an_owner_hook_with_a_byte_that_is_not_utf_8_in_a_comment_is_judged_by_its_bytes_and_never_raises_a_decode_error(self) -> None:
        """Join round 3, finding 3: ``pre-push.legacy`` and ``pre-push`` were read as text, so a ``0xff`` byte raised ``UnicodeDecodeError`` at setup."""
        hooks = self.repo / ".git" / "hooks"
        comment = b"#!/bin/sh\n# owner note \xff\nexit 0\n"
        (hooks / push_hook.PREDECESSOR_NAME).write_bytes(comment)
        self.assert_refused_and_untouched()
        (hooks / push_hook.PREDECESSOR_NAME).unlink()
        (hooks / push_hook.HOOK_NAME).write_bytes(comment)
        (hooks / push_hook.HOOK_NAME).chmod(0o755)
        self.assertTrue(push_hook.install(self.repo))
        self.assertEqual(comment, (hooks / push_hook.KEPT_NAME).read_bytes(), "the owner's hook is kept byte for byte")
        self.assertTrue(push_hook.installed(self.repo))
        marked = b"#!/usr/bin/env python\n# File generated by pre-commit \xff\n"
        (hooks / push_hook.HOOK_NAME).unlink()
        (hooks / push_hook.KEPT_NAME).unlink()
        (hooks / push_hook.HOOK_NAME).write_bytes(marked)
        self.assertTrue(push_hook.install(self.repo))
        self.assertFalse(os.path.lexists(hooks / push_hook.KEPT_NAME), "a script that pre-commit wrote is replaced and not kept")
        self.assertTrue(push_hook.installed(self.repo))

    def test_a_shipped_guard_is_recognised_by_its_bytes_whatever_the_locale_of_the_machine_is(self) -> None:
        """Join round 3, finding 3: the guards are compared as bytes, so a locale that decodes the file differently cannot change the answer."""
        hooks = self.repo / ".git" / "hooks"
        for text in push_hook.PREDECESSOR_TEXTS:
            (hooks / push_hook.PREDECESSOR_NAME).write_bytes(text.encode())
            with mock.patch.object(Path, "read_text", side_effect=AssertionError("the hook files are read as bytes")):
                self.assertTrue(push_hook.predecessors(self.repo))
        (hooks / push_hook.PREDECESSOR_NAME).write_bytes(push_hook.PREDECESSOR_TEXTS[0].encode() + b"# \xff\n")
        with mock.patch.object(Path, "read_text", side_effect=AssertionError("the hook files are read as bytes")):
            self.assertEqual([], push_hook.predecessors(self.repo), "a guard with one more byte is the owner's hook")

    def test_the_fixed_repair_message_names_the_three_places_and_the_kept_name_and_says_that_nothing_was_changed(self) -> None:
        for part in (
            push_hook.HOOK_NAME,
            push_hook.PREDECESSOR_NAME,
            push_hook.ORPHAN_NAME,
            push_hook.KEPT_NAME,
            "compare the files",
            "run setup again",
            "nothing was changed",
        ):
            self.assertIn(part, push_hook.KEPT_REFUSAL)

    SLOT_EXIT_CODES = {push_hook.HOOK_NAME: 11, push_hook.PREDECESSOR_NAME: 12, push_hook.ORPHAN_NAME: 13, push_hook.KEPT_NAME: 14}

    def lay_out(self, layout: dict[str, str]) -> dict[str, int]:
        """Put each slot of the hooks folder into its state (missing, dangling link, executable owner hook or, at ``pre-push``, the installed wrapper) and
        return the owner hooks' exit codes."""
        hooks = self.repo / ".git" / "hooks"
        owners: dict[str, int] = {}
        for name, state in layout.items():
            if state == "installed":
                (hooks / name).write_text(push_hook.HOOK_TEXT)
                (hooks / name).chmod(worktree_scan_hook.HOOK_MODE)
            elif state == "dangling":
                (hooks / name).symlink_to(self.repo / f"missing_target_of_{name}")
            elif state == "regular":
                code = self.SLOT_EXIT_CODES[name]
                (hooks / name).write_text(f'#!/bin/sh\necho "{name}" >> "{self.repo}/owner_ran"\ncat > /dev/null\nexit {code}\n')
                (hooks / name).chmod(0o755)
                owners[name] = code
        return owners

    @staticmethod
    def must_refuse(layout: dict[str, str]) -> bool:
        """The rule: the new hook runs only ``pre-push.previous``, which takes ``pre-push`` and, with no ``pre-push.legacy``, the orphan."""
        present = {name for name, state in layout.items() if state != "missing"}
        owner_at_pre_push = layout[push_hook.HOOK_NAME] in ("dangling", "regular")
        carried = int(owner_at_pre_push) + int(push_hook.ORPHAN_NAME in present and push_hook.PREDECESSOR_NAME not in present)
        orphan_stays = push_hook.ORPHAN_NAME in present and push_hook.PREDECESSOR_NAME in present
        legacy_hook_lost = layout[push_hook.PREDECESSOR_NAME] == "regular"
        return carried > 1 or (carried == 1 and push_hook.KEPT_NAME in present) or orphan_stays or legacy_hook_lost

    def test_every_layout_of_the_four_names_runs_the_owners_hook_or_fails_setup_and_changes_nothing(self) -> None:
        """Join rounds 2 and 3: of the 108 layouts (``pre-push`` also holds the installed wrapper), each setup fails with the fixed line and no change, or
        the new hook runs the owner's hook."""
        names = list(self.SLOT_EXIT_CODES)
        runs = failures = 0
        slot_states = [("missing", "dangling", "regular", "installed") if name == push_hook.HOOK_NAME else ("missing", "dangling", "regular") for name in names]
        for states in itertools.product(*slot_states):
            layout = dict(zip(names, states, strict=True))
            with self.subTest(layout=layout):
                shutil.rmtree(self.repo / ".git" / "hooks")
                (self.repo / ".git" / "hooks").mkdir()
                for leftover in ("owner_ran", "gate_args", "gate_input"):
                    (self.repo / leftover).unlink(missing_ok=True)
                owners = self.lay_out(layout)
                before = self.hooks_state()
                err = io.StringIO()
                with mock.patch.object(install_tools, "ROOT", self.repo), contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
                    settled = install_tools.settle_push_guard(check=False)
                self.assertEqual(self.must_refuse(layout), not settled, "the setup is refused exactly when an owner hook or its name would be lost")
                if not settled:
                    failures += 1
                    self.assertEqual(f"push hook: {push_hook.KEPT_REFUSAL}\n", err.getvalue())
                    self.assertEqual(before, self.hooks_state(), "a refused setup changes no entry of the hooks folder")
                    continue
                runs += 1
                self.assertTrue(push_hook.installed(self.repo))
                self.stub_gate(0)
                run = self.run_hook("line\n")
                self.assertLessEqual(len(owners), 1, f"two owner hooks cannot both run: {owners}")
                self.assertEqual(next(iter(owners.values()), 0), run.returncode, run.stderr)
                ran = (self.repo / "owner_ran").read_text().split() if (self.repo / "owner_ran").exists() else []
                self.assertEqual(len(owners), len(ran), f"the owner's hook ran once: {ran}")
        self.assertEqual(4 * 3 ** (len(names) - 1), runs + failures)
        self.assertGreater(runs, 0)
        self.assertGreater(failures, 0)

    def test_a_setup_that_is_refused_makes_the_installer_program_fail(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        (hooks / push_hook.PREDECESSOR_NAME).symlink_to(self.repo / "missing_target")
        (hooks / push_hook.ORPHAN_NAME).write_text("#!/bin/sh\nexit 9\n")
        (hooks / push_hook.ORPHAN_NAME).chmod(0o755)
        err = io.StringIO()
        with (
            mock.patch.object(install_tools, "ROOT", self.repo),
            mock.patch.object(install_tools, "TOOL_LIST", ()),
            mock.patch.object(install_tools, "settle_hook", return_value=True),
            mock.patch.object(sys, "argv", ["install_tools"]),
            contextlib.redirect_stderr(err),
        ):
            self.assertEqual(1, install_tools.main())

    def test_an_orphan_that_is_a_dangling_link_is_moved_to_the_kept_name_as_it_is_and_the_hook_is_not_installed_while_it_stays(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        target = self.repo / "missing_target"
        (hooks / push_hook.ORPHAN_NAME).symlink_to(target)
        push_hook.install(self.repo)
        self.assertTrue((hooks / push_hook.KEPT_NAME).is_symlink())
        self.assertEqual(os.readlink(hooks / push_hook.KEPT_NAME), str(target))
        self.assertFalse(os.path.lexists(hooks / push_hook.ORPHAN_NAME))
        self.assertTrue(push_hook.installed(self.repo))

    def test_an_orphan_link_that_is_left_means_the_hook_is_not_installed(self) -> None:
        hooks = self.repo / ".git" / "hooks"
        push_hook.install(self.repo)
        (hooks / push_hook.ORPHAN_NAME).symlink_to(self.repo / "missing_target")
        self.assertFalse(push_hook.installed(self.repo))

    def test_the_hook_refuses_a_gate_that_is_older_than_the_version_it_was_installed_for(self) -> None:
        push_hook.install(self.repo)
        self.assertIn(f"-lt {push_hook.REQUIRED_GATE_VERSION}", self.hook().read_text())
        self.assertEqual(push_gate.GATE_VERSION, push_hook.REQUIRED_GATE_VERSION, "the setup installs the hook for the gate that it ships")

    def test_a_failed_write_of_the_new_hook_leaves_the_old_hook_active(self) -> None:
        """Round 4, finding 1: every old form of the hook stays in place, whole and executable, until the new one has replaced it."""
        older = push_hook.HOOK_TEXT.replace(f"-lt {push_hook.REQUIRED_GATE_VERSION}", "-lt 1").replace(
            f"version {push_hook.REQUIRED_GATE_VERSION}", "version 1"
        )
        for label, text in (
            ("before versions", push_hook.ROUND_TWO_TEXT),
            ("an earlier version", older),
            ("a pre-commit script", "#!/bin/sh\n# File generated by pre-commit\n"),
        ):
            with self.subTest(label=label):
                self.hook().write_text(text)
                self.hook().chmod(0o755)
                before = sorted(path.name for path in self.hook().parent.iterdir())
                with mock.patch.object(os, "replace", side_effect=OSError("disk full")), self.assertRaises(OSError):
                    push_hook.install(self.repo)
                self.assertEqual(self.hook().read_text(), text, "the old hook is still the active one")
                self.assertTrue(self.hook().stat().st_mode & stat.S_IXUSR)
                self.assertEqual(sorted(path.name for path in self.hook().parent.iterdir()), before, "no temporary file is left")

    def test_a_standard_input_that_cannot_be_read_fails_the_push_and_is_not_an_empty_push(self) -> None:
        """Round 4, finding 3: a read error is not "nothing is pushed"."""
        push_hook.install(self.repo)
        self.stub_gate(0)
        folder = self.repo / "a_folder"
        folder.mkdir()
        run = run_command("sh", ["-c", 'sh "$0" origin url < "$1"', str(self.hook()), str(folder)], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS)
        self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertIn("push gate stopped by input", run.stderr)
        self.assertFalse((self.repo / "gate_input").exists(), "the gate did not run on a missing input")

    def test_an_empty_input_that_was_read_is_still_an_empty_push(self) -> None:
        push_hook.install(self.repo)
        self.stub_gate(0)
        self.assertEqual(self.run_hook("").returncode, 0)
        self.assertEqual((self.repo / "gate_input").read_text(), "\n")

    def test_a_hook_of_an_earlier_version_is_replaced_and_not_kept(self) -> None:
        for earlier in (push_hook.ROUND_TWO_TEXT, push_hook.HOOK_TEXT.replace(f"-lt {push_hook.REQUIRED_GATE_VERSION}", "-lt 1")):
            self.hook().write_text(earlier)
            self.hook().chmod(0o755)
            self.assertFalse(push_hook.installed(self.repo) and earlier != push_hook.HOOK_TEXT)
            push_hook.install(self.repo)
            self.assertTrue(push_hook.installed(self.repo))
            self.assertFalse(self.hook().with_name(push_hook.KEPT_NAME).exists())

    def test_a_pre_push_script_that_pre_commit_wrote_is_replaced_and_not_kept(self) -> None:
        self.hook().write_text("#!/usr/bin/env python3\n# File generated by pre-commit: https://pre-commit.com\n")
        self.hook().chmod(0o755)
        self.assertFalse(push_hook.installed(self.repo))
        push_hook.install(self.repo)
        self.assertTrue(push_hook.installed(self.repo))
        self.assertFalse(self.hook().with_name(push_hook.KEPT_NAME).exists())

    def run_hook(self, sends: str) -> CompletedProcess[str]:
        return run_command("sh", [str(self.hook()), "origin", "url"], cwd=self.repo, env=self.env, timeout=TIMEOUT_SECONDS, input_text=sends)

    def test_the_installer_program_installs_and_checks_the_guard(self) -> None:
        with mock.patch.object(install_tools, "ROOT", self.repo):
            self.assertFalse(install_tools.settle_push_guard(check=True))
            self.assertTrue(install_tools.settle_push_guard(check=False))
            self.assertTrue(install_tools.settle_push_guard(check=True))


if __name__ == "__main__":
    unittest.main()
