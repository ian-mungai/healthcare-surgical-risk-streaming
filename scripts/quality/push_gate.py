"""The push gate: the pre-push hook of about 2 minutes that runs before every push to any branch.

Run by pre-commit at the pre-push stage (``.venv/bin/python -m scripts.quality.push_gate``) from the repository root. It stops at the first check that fails
and names it: ``push gate stopped by <check>``. In order:

1. Tracked files have no uncommitted change, so the checks read the pushed commit.
2. The secret scan (the pinned gitleaks) of the unpushed commits of HEAD, merge diffs included.
3. The commit message of every unpushed commit of HEAD.
4. The privacy scan of every file that Git does not ignore.
5. The changed files' own checks: Ruff's formatter and linter, MyPy, Terraform's formatter and the quick test modules that cover the changed files.
6. The dependency scan (the pinned Trivy) of each lock file that changed.

The unpushed commits are the ones that ``git rev-list HEAD --not --remotes`` lists, the whole history for a first push. Pre-commit starts the gate only when
the push sends commits. The full test groups, the Terraform and Ansible checks and every other check run in the hosted CI run of the pushed branch.
"""

from __future__ import annotations

import ast
import functools
import os
import sys
import time
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.process import TimeoutExpired, run_command
from scripts.quality import repo_checks

ROOT = Path(__file__).resolve().parents[2]
TRACKED = "tracked changes"
SECRETS = "secret scan"
MESSAGES = "commit messages"
PRIVACY = "privacy scan"
RUFF_FORMAT = "ruff format"
RUFF_CHECK = "ruff check"
MYPY = "mypy"
TERRAFORM = "terraform fmt"
QUICK = "quick tests"
DEPENDENCIES = "dependency scan"
LOCK_FILES = ("requirements.txt", "uv.lock")
CHECK_TIMEOUT_SECONDS = 120
QUICK_TIMEOUT_SECONDS = 300
GIT_TIMEOUT_SECONDS = 60
QUICK_FOLDER = "tests/fast"
UNPUSHED = ("HEAD", "--not", "--remotes")
SKIPPED_FOLDERS = {".venv", ".tools", ".git", ".mypy_cache", ".ruff_cache", "node_modules", "runs"}
MIN_MENTION = 4


@dataclass(frozen=True)
class Check:
    """One command of the gate, named so that a failure names it."""

    name: str
    program: str
    arguments: tuple[str, ...]
    timeout: int = CHECK_TIMEOUT_SECONDS


def git(*arguments: str) -> str:
    return repo_checks.git(*arguments)


def tracked_changes() -> list[str]:
    """The tracked files that differ from HEAD in the index or the working tree. Untracked files are not listed."""
    lines = git("status", "--porcelain", "--untracked-files=no").splitlines()
    return [line[3:] for line in lines]


def unpushed_commits() -> list[str]:
    """The commits that no remote branch holds, newest first, as ``git rev-list HEAD --not --remotes`` lists them."""
    return git("rev-list", *UNPUSHED).split()


def changed_files(root: Path) -> list[str]:
    """The files that the unpushed commits add, copy, modify or rename (each merge against each parent) and that still exist, sorted."""
    listed = repo_checks.git("log", "-m", "--name-only", "--diff-filter=ACMR", "--format=", "-z", *UNPUSHED, keep_newlines=True)
    return sorted(name for name in set(listed.split("\0")) - {""} if (root / name).is_file())


def python_files(changed: list[str]) -> list[str]:
    return [name for name in changed if name.endswith(".py")]


def mypy_folders(root: Path) -> tuple[str, ...]:
    """The folders and files that the project's MyPy configuration checks."""
    configuration = tomllib.loads((root / "pyproject.toml").read_text()) if (root / "pyproject.toml").is_file() else {}
    return tuple(configuration.get("tool", {}).get("mypy", {}).get("files", ()))


def in_folders(name: str, folders: tuple[str, ...]) -> bool:
    return any(name == folder or name.startswith(folder.rstrip("/") + "/") for folder in folders)


def module_name(file: str) -> str:
    """The dotted name of a Python file of the repository."""
    path = Path(file)
    parts = path.parent.parts if path.name == "__init__.py" else (*path.parent.parts, path.stem)
    return ".".join(parts)


@functools.cache
def repository_python_files(root: Path) -> dict[str, str]:
    """Each dotted module name of the repository's Python files with its file. Tool folders, run folders and the worktrees inside the repository are skipped."""
    names: dict[str, str] = {}
    for folder, subfolders, files in os.walk(root):
        subfolders[:] = sorted(name for name in subfolders if name not in SKIPPED_FOLDERS and not name.startswith(".worktrees"))
        for file in sorted(files):
            if file.endswith(".py"):
                relative = str((Path(folder) / file).relative_to(root))
                names[module_name(relative)] = relative
    return names


def imported_modules(source: str, own: str, files: dict[str, str]) -> set[str]:
    """The repository modules that ``source`` imports: ``import a.b``, ``from a import b`` (also when ``b`` is a module) and a relative import."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                package = own.split(".") if files.get(own, "").endswith("__init__.py") else own.split(".")[:-1]
                base = ".".join([*package[: len(package) - node.level + 1], *([node.module] if node.module else [])])
            names = [base, *(f"{base}.{alias.name}" for alias in node.names)]
        for name in names:
            parts = name.split(".")
            found.update(".".join(parts[:count]) for count in range(1, len(parts) + 1) if ".".join(parts[:count]) in files)
    return found


@functools.cache
def import_graph(root: Path) -> dict[str, set[str]]:
    """For each quick test module, every repository file that it imports, through any depth of imports (the module's own file excluded)."""
    files = repository_python_files(root)
    direct: dict[str, set[str]] = {}
    for name, file in files.items():
        try:
            direct[name] = imported_modules((root / file).read_text(), name, files)
        except SyntaxError, UnicodeDecodeError:
            direct[name] = set()
    graph: dict[str, set[str]] = {}
    for name, file in files.items():
        if not (file.startswith(f"{QUICK_FOLDER}/test_") and file.endswith(".py")):
            continue
        reached: set[str] = set()
        pending = [name]
        while pending:
            for imported in direct.get(pending.pop(), set()) - reached - {name}:
                reached.add(imported)
                pending.append(imported)
        graph[name] = {files[module] for module in reached}
    return graph


def mentions(changed: str) -> tuple[str, ...]:
    """What a test that reads a file without importing it writes about it: its path, its file name and, for a Python file, its dotted module."""
    found = [changed, Path(changed).name]
    if changed.endswith(".py"):
        found.append(module_name(changed))
    return tuple(found)


def select_tests(root: Path, changed: list[str]) -> list[str]:
    """The quick test modules (dotted names) that cover the changed files: a changed test module, a module that imports a changed file through any depth
    of imports and a module whose text names a changed path, file name or module."""
    graph = import_graph(root)
    texts = {name: (root / f"{name.replace('.', '/')}.py").read_text() for name in graph}
    selected = set()
    for name, reached in graph.items():
        own = f"{name.replace('.', '/')}.py"
        for file in changed:
            if file == own or file in reached or any(text in texts[name] for text in mentions(file) if len(text) >= MIN_MENTION):
                selected.add(name)
    return sorted(selected)


def changed_file_checks(repo: Path, changed: list[str], tools: Path = ROOT) -> list[Check]:
    """The checks of the changed files' own tools, run in ``repo`` with the pinned tools of ``tools``. Python gets Ruff and, in the folders that the
    repository's MyPy configuration checks, MyPy; Terraform gets its formatter; the quick test modules that cover the files run together."""
    checks: list[Check] = []
    python = python_files(changed)
    if python:
        checks.append(Check(RUFF_FORMAT, str(tools / ".venv/bin/ruff"), ("format", "--check", "--", *python)))
        checks.append(Check(RUFF_CHECK, str(tools / ".venv/bin/ruff"), ("check", "--", *python)))
        typed = [name for name in python if in_folders(name, mypy_folders(repo))]
        if typed:
            checks.append(Check(MYPY, str(tools / ".venv/bin/mypy"), ("--", *typed)))
    terraform = [name for name in changed if name.endswith(".tf")]
    if terraform:
        checks.append(Check(TERRAFORM, str(tools / ".tools/bin/terraform"), ("fmt", "-check", "-diff", "--", *terraform)))
    modules = select_tests(repo, changed) if (repo / QUICK_FOLDER).is_dir() else []
    if modules:
        checks.append(Check(QUICK, str(tools / ".venv/bin/python"), ("-m", "unittest", *modules), QUICK_TIMEOUT_SECONDS))
    return checks


def dependency_checks(changed: list[str], tools: Path = ROOT) -> list[Check]:
    """The vulnerability scan of each lock file that changed, failing on a HIGH or CRITICAL finding."""
    locks = [name for name in changed if Path(name).name in LOCK_FILES]
    cache = str(tools / ".tools/trivy_cache")
    scan = ("fs", "--scanners", "vuln", "--severity", "HIGH,CRITICAL", "--exit-code", "1", "--quiet", "--cache-dir", cache)
    return [Check(DEPENDENCIES, str(tools / ".tools/bin/trivy"), (*scan, lock)) for lock in locks]


def secret_check(tools: Path = ROOT) -> Check:
    """The pinned gitleaks over the unpushed commits, with each merge's diff against each parent (``-m``): the default log shows none."""
    return Check(SECRETS, str(tools / ".tools/bin/gitleaks"), ("git", "--redact", "--no-banner", f"--log-opts=-m {' '.join(UNPUSHED)}", "."))


def run_check(check: Check) -> str | None:
    """Run one command in the current folder. Return None when it passed, or what went wrong, at its deadline too."""
    try:
        run = run_command(check.program, list(check.arguments), timeout=check.timeout)
    except TimeoutExpired:
        return f"it did not finish in {check.timeout} seconds"
    except OSError as error:
        return f"it could not start: {error.strerror or error}"
    if run.returncode == 0:
        return None
    return f"it exited with {run.returncode}\n{(run.stdout + run.stderr).strip()[-3000:]}"


def commit_message_problems(commits: list[str]) -> str | None:
    findings = []
    for commit in commits:
        findings += repo_checks.message_findings(f"commit {commit}", git("show", "-s", "--format=%B", commit))
    return "\n".join(f"{finding.location}: {finding.problem}\n  fix: {finding.fix}" for finding in findings) or None


def privacy_problems() -> str | None:
    findings, unreviewed = repo_checks.privacy_scan()
    lines = [*unreviewed, *(f"{repo_checks.on_one_line(finding.location)}: {finding.problem}\n  fix: {finding.fix}" for finding in findings)]
    return "\n".join(lines) or None


def tracked_problems() -> str | None:
    changed = tracked_changes()
    return (
        "tracked files have uncommitted changes, so the checks would not read the pushed commit: commit or restore them\n" + "\n".join(changed)
        if changed
        else None
    )


def main() -> int:
    """Run the gate in the current repository and return 1 at the first check that fails."""
    started = time.monotonic()
    repo = Path.cwd()
    commits = unpushed_commits()
    steps: list[tuple[str, Callable[[], str | None]]] = [(TRACKED, tracked_problems)]
    if commits:
        steps += [(SECRETS, lambda: run_check(secret_check())), (MESSAGES, lambda: commit_message_problems(commits))]
    steps.append((PRIVACY, privacy_problems))
    if commits:
        changed = changed_files(repo)
        steps += [(check.name, functools.partial(run_check, check)) for check in [*changed_file_checks(repo, changed), *dependency_checks(changed)]]
    durations = []
    for name, step in steps:
        began = time.monotonic()
        problem = step()
        durations.append(f"  {name}: {time.monotonic() - began:.1f} s")
        if problem is not None:
            sys.stderr.write(f"push gate stopped by {name}: {problem}\n")
            return 1
    sys.stdout.write("push gate passed in {:.1f} s\n{}\n".format(time.monotonic() - started, "\n".join(durations)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
