"""The job groups of the CI workflow and the changed paths that select them, from the map in ``scripts/quality/ci_groups.json``.

Each group's inputs are derived from what its job runs, not listed by hand:

- The map names each group's commands (``make <target>``). The Makefile recipes of those targets name the Python modules they start
  (``-m <module>``). A group whose job commits through the hooks (the guard suite) also starts every module that an ``entry:`` line of
  ``.pre-commit-config.yaml`` names, and every test module that a ``discover --start-directory`` entry finds. The plan job's own module is
  named in the map. These are the entry modules.
- From the entry modules, the inputs hold every module that a Python import reaches, with the packages around it, and every module that a
  string names in full (a module that a command starts with ``-m``). A module that loads code by path names that path in the map.
- The guard suite writes its planted files in its copy of the tree, so for the guards every path that the suite writes, and every path
  above or below one, is an input.
- Each group adds the files that its tools read, each with its reason.

A file that the hooks job alone checks is on ``none``, with its reason. A file on no list is an error that a fast test reports, and a
changed path on no list selects every group. A changed input of the plan selects every group. The map's patterns are ``fnmatch``
patterns matched against the whole path, and their ``*`` also matches ``/``.

The changed paths are the two-dot difference of the trees of the start commit and the checkout, without rename detection, so a rename
counts with both names, a deleted file counts and a rollback lists the files it restores. A range that cannot be verified, a map or a
Makefile that cannot be read and an entry module that is missing all select every group.

The derivation reads Python imports. A data file that a fast test reads is not an input of the guards: the hooks job runs the same test
on the same file in every run, and its result decides the run.
"""

from __future__ import annotations

import ast
import fnmatch
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality.repo_checks import git
from tests.e2e import commit_guards

CONFIG = Path("scripts/quality/ci_groups.json")
MAKEFILE = "Makefile"
HOOKS = ".pre-commit-config.yaml"
COMMIT = re.compile(r"[0-9a-f]{40}")
EVENTS = ("push", "pull_request")
GIT_TIMEOUT_SECONDS = 60
COMMAND = re.compile(r"make ([A-Za-z0-9_-]+)")
TARGET = re.compile(r"([A-Za-z0-9_-]+):(?:\s|$)")
MODULE_FLAG = re.compile(r"(?:^|\s)-m\s+([A-Za-z_][\w.]*)")
DISCOVERY = re.compile(r"\bdiscover\s+--start-directory\s+(\S+)")
HOOK_ENTRY = re.compile(r"^\s+entry:\s*(.+)$", re.MULTILINE)
HOOK_ID = re.compile(r"^\s+- id:", re.MULTILINE)
DOTTED = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+")
LOADER_MODULES = {"importlib", "runpy"}
LOADER_CALLS = {"__import__", "exec", "eval", "compile"}


class Unverified(Exception):
    """The range or the inputs cannot be verified, so every group runs; the message is the reason."""


@dataclass(frozen=True)
class Decision:
    run: bool
    reason: str


@dataclass(frozen=True)
class Inputs:
    """The inputs of one job: its entry modules, the modules they reach, the paths its tools read and the paths its suite writes."""

    entries: list[str]
    modules: Mapping[str, str]
    paths: list[dict[str, str]] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)

    def reason(self, path: str) -> str | None:
        """Why ``path`` is an input of the job, or None."""
        if path in self.modules:
            return self.modules[path]
        entry = entry_for(path, self.paths)
        if entry:
            return f"{entry['pattern']}: {entry['reason']}"
        written = next((target for target in self.writes if overlaps(path, target)), None)
        return f"the guard suite writes {written} in its copy of the tree" if written is not None else None


def overlaps(path: str, target: str) -> bool:
    """Whether a tracked ``path`` and a written ``target`` are one path, one is inside the other, or name one Python module."""
    if path == target or path.startswith(target + "/") or target.startswith(path + "/"):
        return True
    return target.endswith(".py") and path.startswith(target.removesuffix(".py") + "/")


def load(root: Path) -> dict[str, Any]:
    """The map of the checkout at ``root``."""
    config: dict[str, Any] = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    return config


def entry_for(path: str, entries: list[dict[str, str]]) -> dict[str, str] | None:
    return next((entry for entry in entries if fnmatch.fnmatchcase(path, entry["pattern"])), None)


def module_file(root: Path, name: str) -> str | None:
    """The file of the repository module ``name`` at ``root``, or None for a module of another package."""
    base = name.replace(".", "/")
    return next((path for path in (f"{base}.py", f"{base}/__init__.py") if (root / path).is_file()), None)


def repository_module(root: Path, name: str) -> str:
    """The file of ``name``, which must be a module of this repository because its top package is one."""
    path = module_file(root, name)
    if path is None:
        raise Unverified(f"the entry module {name} is not in the checkout")
    return path


def is_repository_package(root: Path, name: str) -> bool:
    return (root / name.split(".")[0] / "__init__.py").is_file()


def imported_names(path: str, tree: ast.Module) -> set[str]:
    """Every module name that ``tree`` imports, by absolute or relative import, and every dotted name that a string holds in full."""
    package = path.removesuffix(".py").replace("/", ".").removesuffix(".__init__")
    if not path.endswith("__init__.py"):
        package = package.rpartition(".")[0]
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parent = ".".join(package.split(".")[: len(package.split(".")) - node.level + 1])
                base = f"{parent}.{base}".strip(".")
            names.add(base)
            names.update(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and DOTTED.fullmatch(node.value):
            names.add(node.value)
    return {prefix for name in names if name for prefix in prefixes(name)}


def prefixes(name: str) -> list[str]:
    parts = name.split(".")
    return [".".join(parts[: index + 1]) for index in range(len(parts))]


def reached_modules(root: Path, entries: list[str]) -> dict[str, str]:
    """Every repository file that the ``entries`` reach through imports, with the entry each was reached from first."""
    reached: dict[str, str] = {}
    queue = [(name, name) for entry in entries for name in prefixes(entry)]
    while queue:
        name, entry = queue.pop(0)
        path = module_file(root, name)
        if path is None or path in reached:
            continue
        reached[path] = f"the job's Python reaches it from {entry}"
        tree = ast.parse((root / path).read_text(encoding="utf-8"), filename=path)
        queue.extend((imported, entry) for imported in sorted(imported_names(path, tree)))
    return reached


def make_targets(root: Path) -> dict[str, list[str]]:
    """The recipe lines of each target of the Makefile."""
    targets: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in (root / MAKEFILE).read_text(encoding="utf-8").splitlines():
        if line.startswith("\t") and current is not None:
            current.append(line.strip())
            continue
        match = TARGET.match(line)
        current = targets.setdefault(match.group(1), []) if match and not line.startswith(".") else None
    return targets


def command_modules(root: Path, commands: list[str]) -> set[str]:
    """The repository modules that the Makefile recipes of ``commands`` start with ``-m``."""
    targets = make_targets(root)
    modules: set[str] = set()
    for command in commands:
        match = COMMAND.fullmatch(command)
        if not match or match.group(1) not in targets:
            raise Unverified(f"the command {command!r} names no target of the {MAKEFILE}")
        for line in targets[match.group(1)]:
            if "$(MAKE)" in line or re.search(r"(?:^|\s)make\s", line):
                raise Unverified(f"the {MAKEFILE} target {match.group(1)} starts make, which the plan does not follow")
            modules.update(name for name in MODULE_FLAG.findall(line) if is_repository_package(root, name))
    return modules


def hook_modules(root: Path) -> set[str]:
    """The repository modules that the hooks start: each ``-m`` module of an entry and each test module that a discovery entry finds."""
    text = (root / HOOKS).read_text(encoding="utf-8")
    entries = HOOK_ENTRY.findall(text)
    if len(entries) != len(HOOK_ID.findall(text)):
        raise Unverified(f"a hook of {HOOKS} has no entry on one line")
    modules = {name for entry in entries for name in MODULE_FLAG.findall(entry) if is_repository_package(root, name)}
    for entry in entries:
        for folder in DISCOVERY.findall(entry):
            modules.update(path.relative_to(root).with_suffix("").as_posix().replace("/", ".") for path in (root / folder).glob("test*.py"))
    return modules


def suite_writes() -> list[str]:
    """Every path that the guard suite writes in its copy of the tree: its linked folders, its tracked note and each planted file."""
    paths = {commit_guards.TRACKED_NOTE, *commit_guards.LINKED_FOLDERS}
    for case in commit_guards.planted_cases():
        for change in (case.control, case.fault):
            paths.update(change.files, change.unstaged)
    return sorted(paths)


def derived(root: Path, entries: set[str], paths: list[dict[str, str]], writes: list[str]) -> Inputs:
    for name in entries:
        repository_module(root, name)
    ordered = sorted(entries)
    return Inputs(ordered, reached_modules(root, ordered), paths, writes)


def group_inputs(root: Path, config: dict[str, Any]) -> dict[str, Inputs]:
    """The derived inputs of the plan job and of each group that can skip, for the checkout at ``root``."""
    inputs = {"plan": derived(root, set(config["plan"]["modules"]), config["plan"]["paths"], [])}
    for name, group in config["groups"].items():
        if group.get("always"):
            continue
        entries = command_modules(root, group["commands"]) | (hook_modules(root) if group.get("commits_through_hooks") else set())
        inputs[name] = derived(root, entries, group["paths"], suite_writes() if group.get("commits_through_hooks") else [])
    return inputs


def code_loaders(root: Path, files: list[str]) -> list[str]:
    """The ``files`` that can load code by path: they import ``importlib`` or ``runpy``, or call ``exec``, ``eval``, ``compile`` or ``__import__``."""
    found = []
    for path in files:
        tree = ast.parse((root / path).read_text(encoding="utf-8"), filename=path)
        for node in ast.walk(tree):
            imported = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""] if isinstance(node, ast.ImportFrom) else []
            called = isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in LOADER_CALLS
            if called or any(name.split(".")[0] in LOADER_MODULES for name in imported):
                found.append(path)
                break
    return sorted(found)


def unmapped(paths: list[str], config: dict[str, Any], inputs: dict[str, Inputs]) -> list[str]:
    """The paths that are an input of no job and are not on ``none``."""
    return [path for path in paths if entry_for(path, config["none"]) is None and all(job.reason(path) is None for job in inputs.values())]


def exit_code(root: Path, *args: str) -> int:
    env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    return run_command("git", args, cwd=root, env=env, timeout=GIT_TIMEOUT_SECONDS).returncode


def verify_range(root: Path, event: str, start: str, run_commit: str) -> None:
    """Raise ``Unverified`` unless ``start`` to the checkout is a range the plan can compare for ``event``."""
    if event not in EVENTS:
        raise Unverified(f"the event {event} has no range to compare")
    if not COMMIT.fullmatch(start) or set(start) == {"0"}:
        raise Unverified("the push has no earlier commit (a first push) or the event names no start commit")
    if exit_code(root, "cat-file", "-e", f"{start}^{{commit}}") != 0:
        raise Unverified(f"the start commit {start} is not in this checkout")
    head = git("rev-parse", "HEAD", cwd=root).strip()
    if head != run_commit:
        raise Unverified(f"the checkout {head} is not the commit of the run {run_commit or 'none'}")
    if event == "push":
        ancestor = exit_code(root, "merge-base", "--is-ancestor", start, head)
        if ancestor == 1:
            raise Unverified(f"the start commit {start} is not an ancestor of {head} (a force push)")
        if ancestor != 0:
            raise Unverified(f"Git could not compare {start} with {head}")


def changed_paths(root: Path, start: str, end: str) -> list[str]:
    """Every path whose content differs between the trees of ``start`` and ``end``: both names of a rename and every deleted file."""
    listed_paths = git("diff", "--name-only", "--no-renames", "-z", start, end, "--", cwd=root, keep_newlines=True)
    return sorted(set(listed_paths.split("\0")) - {""})


def widening(config: dict[str, Any], inputs: dict[str, Inputs], changed: list[str]) -> str | None:
    """The reason that ``changed`` selects every group, or None."""
    for path in changed:
        reason = inputs["plan"].reason(path)
        if reason:
            return f"{path} changes what the plan runs ({reason})"
    missing = unmapped(changed, config, inputs)
    return f"{missing[0]} is on no list of {CONFIG}" if missing else None


def select(config: dict[str, Any], inputs: dict[str, Inputs], changed: list[str] | None, everything: str) -> dict[str, Decision]:
    """The decision for each group. ``changed`` None means every group runs for the reason ``everything``."""
    decisions = {}
    for name, group in config["groups"].items():
        if group.get("always"):
            decisions[name] = Decision(True, group["reason"])
        elif changed is None:
            decisions[name] = Decision(True, everything)
        else:
            hit = next(((path, reason) for path in changed if (reason := inputs[name].reason(path))), None)
            if hit:
                decisions[name] = Decision(True, f"selected: {hit[0]} is an input ({hit[1]})")
            else:
                decisions[name] = Decision(False, f"skipped: none of the {len(changed)} changed paths is an input of its job")
    return decisions
