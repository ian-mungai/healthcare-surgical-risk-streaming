"""The push gate: the pre-push hook of about 2 minutes that runs before every push to any branch.

Run by the plain Git ``pre-push`` hook (``scripts/quality/push_hook.py``) from the repository root, with Git's standard input: one line
``<local ref> <local sha> <remote ref> <remote sha>`` for each ref that the push updates. A run by hand has no such line and checks the commits that no remote
branch holds. It stops at the first check that fails and names it: ``push gate stopped by <check>``. In order, when the push sends something:

1. Tracked files have no uncommitted change, so the checks read the pushed commit.
2. The secret scan (the pinned gitleaks) of the pushed commits' diffs, merge diffs included, and of their whole messages.
3. The commit message of every pushed commit.
4. Every pushed commit that has working-tree checks to run is the checked-out HEAD.
5. The privacy scan of every file that Git does not ignore.
6. The changed files' own checks: Ruff's formatter and linter, MyPy, Terraform's formatter and the quick test modules that cover the changed files.
   A rename or a delete of a Python file runs the whole quick group.
7. The dependency scan (the pinned Trivy) of each lock file that changed.

The pushed commits are what Git's lines send, read conservatively because the receiver's history is not known: every commit of the local sha that is
not reachable from the remote sha that the line names, and the whole history when the line names none (a new ref) or names one that this clone lacks.
The tracking refs of a remote are never trusted for that range, since a changed push URL or a stale ref would hide commits. They only narrow the
changed-file checks, which are quality checks that the hosted run repeats in full. A push that sends only deletes runs no check.
"""

from __future__ import annotations

import ast
import functools
import os
import re
import sys
import time
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.process import TimeoutExpired, run_command
from scripts.quality import repo_checks

ROOT = Path(__file__).resolve().parents[2]
# The version of this gate. ``make setup`` installs the shared pre-push hook for it, and the hook refuses a worktree whose gate is older or missing.
GATE_VERSION = 3
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
HEAD = "checked-out head"
# A Terraform lock file pins each provider by hash. Trivy 0.76.2 detects no package in it, so providers are not vulnerability-scanned.
LOCK_FILES = ("requirements.txt", "uv.lock")
ZERO_SHA = re.compile(r"0+")
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
    input_text: str | None = None


@dataclass(frozen=True)
class Update:
    """One line that Git writes to a pre-push hook: ``<local ref> <local sha> <remote ref> <remote sha>``."""

    local_ref: str
    local_sha: str
    remote_ref: str
    remote_sha: str

    @property
    def deletes(self) -> bool:
        return bool(ZERO_SHA.fullmatch(self.local_sha))

    @property
    def creates(self) -> bool:
        return bool(ZERO_SHA.fullmatch(self.remote_sha))


def git(*arguments: str) -> str:
    return repo_checks.git(*arguments)


def tracked_changes() -> list[str]:
    """The tracked files that differ from HEAD in the index or the working tree. Untracked files are not listed."""
    lines = git("status", "--porcelain", "--untracked-files=no").splitlines()
    return [line[3:] for line in lines]


def parse_updates(text: str) -> list[Update]:
    """The updates in Git's input to a pre-push hook. The last three fields are fixed (a local ref can hold spaces, as ``HEAD@{2 days ago}`` does)."""
    updates = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.rstrip("\n").rsplit(" ", 3)
        if len(parts) != 4:
            raise ValueError(f"cannot read the pushed ref line {line!r}: it is not <local ref> <local sha> <remote ref> <remote sha>")
        updates.append(Update(*parts))
    return updates


def commit_exists(sha: str) -> bool:
    return run_command("git", ["cat-file", "-e", f"{sha}^{{commit}}"], timeout=GIT_TIMEOUT_SECONDS).returncode == 0


@dataclass(frozen=True)
class Range:
    """The commits of ``tip`` that none of the ``excludes`` reaches; no exclude means the whole history of the tip."""

    tip: str
    excludes: tuple[str, ...] = ()

    def arguments(self) -> list[str]:
        return [self.tip, "--not", *self.excludes] if self.excludes else [self.tip]

    def commits(self) -> list[str]:
        return git("rev-list", *self.arguments()).split()


def pushed_ranges(updates: list[Update]) -> list[Range]:
    """What the push sends, read conservatively: for each update that is not a delete, the local sha without the remote sha that the line names, if this clone
    has it, and otherwise the whole history. Tracking refs are not used: a changed push URL or a stale ref would hide commits."""
    return [
        Range(update.local_sha, (update.remote_sha,) if not update.creates and commit_exists(update.remote_sha) else ())
        for update in updates
        if not update.deletes
    ]


def checked_ranges(ranges: list[Range]) -> list[Range]:
    """The same ranges without anything that a tracking ref holds, for the changed-file checks, which the hosted run repeats in full."""
    return [Range(each.tip, (*each.excludes, "--remotes")) for each in ranges]


def unpushed_range() -> Range:
    """Without Git's input (a run by hand): the commits that no remote branch holds, as ``git rev-list HEAD --not --remotes`` lists them."""
    return Range("HEAD", ("--remotes",))


def commits_of(ranges: list[Range]) -> list[str]:
    """Every commit of the ranges, newest first and each once."""
    return list(dict.fromkeys(commit for each in ranges for commit in each.commits()))


def changed_files(root: Path, commits: list[str]) -> tuple[list[str], list[str]]:
    """The files that the commits add, copy, modify or rename to (each merge against each parent) and that still exist, and the paths that they delete or rename
    away from, which no longer exist."""
    if not commits:
        return [], []
    listed = repo_checks.git("log", "-m", "--name-status", "--diff-filter=ACMRD", "--format=", "-z", "--no-walk=unsorted", *commits, keep_newlines=True)
    tokens = [token.strip("\n") for token in listed.split("\0") if token.strip("\n")]
    present: set[str] = set()
    deleted: set[str] = set()
    renamed_from: set[str] = set()
    position = 0
    while position < len(tokens):
        status = tokens[position][:1]
        count = 3 if status in ("R", "C") else 2
        paths = tokens[position + 1 : position + count]
        if status == "D":
            deleted.update(paths)
        elif status == "R":
            renamed_from.add(paths[0])
            present.add(paths[1])
        else:
            present.add(paths[-1])
        position += count
    return sorted(name for name in present if (root / name).is_file()), sorted(renamed_from | {name for name in deleted if not (root / name).exists()})


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


def changed_file_checks(repo: Path, changed: list[str], tools: Path = ROOT, deleted: tuple[str, ...] = ()) -> list[Check]:
    """The checks of the changed files' own tools, run in ``repo`` with the pinned tools of ``tools``. Python gets Ruff and, in the folders that the
    repository's MyPy configuration checks, MyPy; Terraform gets its formatter; the quick test modules that cover the changed and deleted files run together,
    all of them when a Python file was renamed or deleted."""
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
    modules: list[str] = []
    if (repo / QUICK_FOLDER).is_dir():
        modules = sorted(import_graph(repo)) if python_files(list(deleted)) else select_tests(repo, [*changed, *deleted])
    if modules:
        checks.append(Check(QUICK, str(tools / ".venv/bin/python"), ("-m", "unittest", *modules), QUICK_TIMEOUT_SECONDS))
    return checks


def dependency_checks(changed: list[str], tools: Path = ROOT) -> list[Check]:
    """The vulnerability scan of each lock file that changed, failing on a HIGH or CRITICAL finding."""
    locks = [name for name in changed if Path(name).name in LOCK_FILES]
    cache = str(tools / ".tools/trivy_cache")
    scan = ("fs", "--scanners", "vuln", "--severity", "HIGH,CRITICAL", "--exit-code", "1", "--quiet", "--cache-dir", cache)
    return [Check(DEPENDENCIES, str(tools / ".tools/bin/trivy"), (*scan, lock)) for lock in locks]


def secret_checks(ranges: list[Range], commits: list[str], tools: Path = ROOT) -> list[Check]:
    """The pinned gitleaks over the pushed commits: their diffs, with each merge's diff against each parent (``-m``; the default log shows none), and
    their whole messages, which the diff scan does not read. A finding prints its rule with the value redacted."""
    gitleaks = str(tools / ".tools/bin/gitleaks")
    common = ("--redact", "--no-banner", "--no-color", "--verbose")
    messages = "\n\n".join(git("show", "-s", "--format=%B", commit) for commit in commits)
    diffs = [Check(SECRETS, gitleaks, ("git", *common, f"--log-opts=-m {' '.join(each.arguments())}", ".")) for each in ranges]
    return [*diffs, Check(SECRETS, gitleaks, ("stdin", *common), input_text=messages)]


def run_check(check: Check) -> str | None:
    """Run one command in the current folder. Return None when it passed, or what went wrong, at its deadline too."""
    try:
        run = run_command(check.program, list(check.arguments), timeout=check.timeout, input_text=check.input_text)
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
        findings += repo_checks.message_findings(f"commit {commit[: repo_checks.SHORT_SHA]}", git("show", "-s", "--format=%B", commit))
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


def head_problems(updates: list[Update]) -> str | None:
    """The working-tree checks (and the privacy scan of the outgoing tree) read the checked-out HEAD, so every ref that sends a commit must be at HEAD,
    whatever the tracking refs say."""
    head = git("rev-parse", "HEAD").strip()
    for update in updates:
        if update.deletes or not commits_of(pushed_ranges([update])):
            continue
        if git("rev-parse", f"{update.local_sha}^{{commit}}").strip() != head:
            return f"{update.local_ref} is not the checked-out HEAD, so the checks that read the working tree cannot vouch for it: check it out and push again"
    return None


def tracked_lock_files() -> list[str]:
    """Every lock file that Git tracks, for the hosted dependency scan of the whole repository."""
    listed = repo_checks.git("ls-files", "-z", keep_newlines=True).split("\0")
    return sorted(name for name in listed if Path(name).name in LOCK_FILES)


def scan_all_dependencies() -> int:
    """The hosted dependency scan: every tracked lock file, not only the ones that a push changes. Return 1 and name each lock file that fails."""
    failed = 0
    for check in dependency_checks(tracked_lock_files()):
        problem = run_check(check)
        sys.stdout.write(f"{check.arguments[-1]}: {'clean' if problem is None else 'FAILED'}\n")
        if problem is not None:
            sys.stderr.write(f"{DEPENDENCIES} failed for {check.arguments[-1]}: {problem}\n")
            failed = 1
    return failed


def main(stdin: str, *, by_hook: bool = False) -> int:
    """Run the gate in the current repository on Git's input ``stdin`` and return 1 at the first check that fails.

    The hook passes Git's arguments, so ``by_hook`` is true: no input line then means that the push sends nothing. A run by hand has neither.
    """
    started = time.monotonic()
    repo = Path.cwd()
    if stdin.strip() or by_hook:
        updates = parse_updates(stdin)
        ranges = pushed_ranges(updates)
    else:
        updates = [Update("HEAD", git("rev-parse", "HEAD").strip(), "", "0" * 40)]
        ranges = [unpushed_range()]
    commits = commits_of(ranges)
    if not commits:
        sys.stdout.write("push gate: the push sends no commit, so no check runs\n")
        return 0
    steps: list[tuple[str, Callable[[], str | None]]] = [(TRACKED, tracked_problems)]
    steps += [(check.name, functools.partial(run_check, check)) for check in secret_checks(ranges, commits)]
    steps += [(MESSAGES, lambda: commit_message_problems(commits)), (HEAD, lambda: head_problems(updates))]
    steps.append((PRIVACY, privacy_problems))
    changed, deleted = changed_files(repo, commits_of(checked_ranges(ranges)))
    steps += [
        (check.name, functools.partial(run_check, check))
        for check in [*changed_file_checks(repo, changed, deleted=tuple(deleted)), *dependency_checks(changed)]
    ]
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
    sys.exit(scan_all_dependencies() if sys.argv[1:] == ["--all-locks"] else main("" if sys.stdin.isatty() else sys.stdin.read(), by_hook=len(sys.argv) > 1))
