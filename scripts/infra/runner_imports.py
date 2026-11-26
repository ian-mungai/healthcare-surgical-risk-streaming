"""The third-party imports of the native host runners, found without running them, and the packages that the runner checkout's lock files pin.

The host process of a runner (``tests/e2e/silver_signals.py``, ``silver_hypotension.py``, ``gold_dimensions.py`` and ``stream_host.py``) imports the
repository's own modules (``scripts``, ``spark``, ``replay``, ``features``, ``casefile``, ``tests``) and, through them, a few packages. The Spark and Avro
code that the runners start runs in containers (``python -m spark.jobs...`` inside the image), so it is not in the host process. ``closure`` follows every
import, absolute or relative and at any depth of a file, from the entry files through the repository's own files that the source holds. The result is the
top-level names that are neither the standard library nor the repository, with the files that import them.

A ``Source`` is where the files are read from: the disk (``DiskSource``), or the files of one commit with the text of the files being placed laid over them
(``OverlaySource``), so that the check of a placement reads the exact bytes that are sent and the exact files that the deploy archived.
"""

from __future__ import annotations

import ast
import importlib.metadata
import re
import sys
import warnings
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

FIRST_PARTY = ("spark", "scripts", "features", "orchestration", "replay", "casefile", "tests")
DYNAMIC_CALLS = ("spec_from_file_location", "import_module", "__import__")
LOCKED_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?==", re.MULTILINE)
# A runner that loads a module at run time by its path: the module's file is read as part of the runner's imports, and no other file under tests/e2e/ may do it.
LOADED_BY_PATH = {"tests/e2e/stream_host.py": ("ansible/roles/compose_stack/library/kafka_topics.py",)}


class Source(Protocol):
    def read(self, relative: str) -> str | None:
        """The text of the file at a path relative to the repository's root, or None when the source holds no such file."""


@dataclass
class DiskSource:
    root: Path

    def read(self, relative: str) -> str | None:
        path = self.root / relative
        return path.read_text() if path.is_file() else None


@dataclass
class OverlaySource:
    """The files of ``base`` with ``overlay`` laid over them: a path in the overlay is read from the overlay, even when ``base`` has none."""

    base: Source
    overlay: Mapping[str, str]

    def read(self, relative: str) -> str | None:
        if relative in self.overlay:
            return self.overlay[relative]
        return self.base.read(relative)


@dataclass
class Closure:
    files: set[str] = field(default_factory=set)
    third_party: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    dynamic: set[str] = field(default_factory=set)
    missing: list[str] = field(default_factory=list)
    unreadable: list[str] = field(default_factory=list)


def module_file(source: Source, module: str) -> str | None:
    base = "/".join(module.split("."))
    for candidate in (f"{base}.py", f"{base}/__init__.py"):
        if source.read(candidate) is not None:
            return candidate
    return None


def module_files(source: Source, module: str) -> list[str]:
    """The files that importing ``module`` runs: its own file (or its package's ``__init__``) and the ``__init__`` of each package above it.
    Empty when the module has no file."""
    if module_file(source, module) is None:
        return []
    parts = module.split(".")
    return [path for end in range(1, len(parts) + 1) if (path := module_file(source, ".".join(parts[:end]))) is not None]


def absolute_module(file: str, level: int, module: str | None) -> str | None:
    """The dotted name that a relative import of ``file`` means, or None when it climbs out of the repository. ``level`` 1 is the file's own package."""
    package = file.split("/")[:-1]
    keep = len(package) - (level - 1)
    if keep < 0:
        return None
    return ".".join([*package[:keep], *([module] if module else [])])


def imported_names(tree: ast.AST, file: str) -> list[tuple[str | None, list[str]]]:
    """Each import of a file at any depth, with a relative one resolved against the file's own package: its module (None for one that leaves the repository)
    and the names that a ``from`` import takes from it."""
    found: list[tuple[str | None, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(alias.name, []) for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [alias.name for alias in node.names]
            if node.level == 0 and node.module:
                found.append((node.module, names))
            elif node.level > 0:
                found.append((absolute_module(file, node.level, node.module), names))
    return found


def uses_dynamic_import(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = node.func
            name = function.attr if isinstance(function, ast.Attribute) else function.id if isinstance(function, ast.Name) else ""
            if name in DYNAMIC_CALLS:
                return True
    return False


def parsed(text: str, file: str) -> ast.AST | None:
    """The syntax tree of a file, or None when the text is not valid Python: it must compile, which also refuses what only the parser accepts (a ``return``
    outside a function). The code is never run. The text of an error or a warning is never kept or shown: it can hold a line of the file."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            compile(text, file, "exec", dont_inherit=True)
            return ast.parse(text, file)
    except SyntaxError, ValueError, RecursionError, MemoryError:
        return None


def top_level_statements(statements: Sequence[ast.AST]) -> Iterator[ast.AST]:
    """Each statement that runs when a module is imported: the file's own statements and those nested in an ``if``, ``try``, ``with`` or loop, but not the
    body of a function or a class."""
    for node in statements:
        yield node
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            for part in ("body", "orelse", "finalbody", "handlers"):
                yield from top_level_statements(getattr(node, part, []))


def target_names(target: ast.AST | None, context: type[ast.expr_context] = ast.Store) -> set[str]:
    """The bare names that a target stores, or with ``ast.Del`` removes. An attribute or an item of a name leaves the name as it was."""
    return {node.id for node in ast.walk(target) if isinstance(node, ast.Name) and isinstance(node.ctx, context)} if target is not None else set()


def sibling_module(path: str) -> str:
    return path.removesuffix(".py").replace("/", ".")


def literal_all(tree: ast.Module) -> set[str] | None:
    """The names of a module's ``__all__`` when the module holds it as one list or tuple of strings, assigned directly at its top level, and names it nowhere
    else; otherwise None."""
    uses = [node for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == "__all__"]
    values = [
        node.value
        for node in tree.body
        if (isinstance(node, ast.Assign) and any(target in uses for target in node.targets)) or (isinstance(node, ast.AnnAssign) and node.target in uses)
    ]
    if len(uses) != 1 or len(values) != 1 or not isinstance(values[0], ast.List | ast.Tuple):
        return None
    listed = [element.value for element in values[0].elts if isinstance(element, ast.Constant) and isinstance(element.value, str)]
    return set(listed) if len(listed) == len(values[0].elts) else None


def star_names(source: Source, path: str, seen: frozenset[str], siblings: Mapping[str, str]) -> tuple[set[str], bool]:
    """The names that ``from <sibling> import *`` takes from the sibling at ``path``, and whether other names may exist: with an ``__all__``, the names that it
    lists and the sibling holds; without one, each name without a leading underscore. An ``__all__`` that ``literal_all`` cannot read lists no name."""
    names, open_ended = exported_names(source, sibling_module(path), seen, siblings)
    if "__all__" not in names:
        return {name for name in names if not name.startswith("_")}, open_ended
    tree = parsed(source.read(path) or "", path)
    listed = literal_all(tree) if isinstance(tree, ast.Module) else None
    return (set(), True) if listed is None else (listed & names, open_ended)


def exported_names(source: Source, module: str, seen: frozenset[str] = frozenset(), siblings: Mapping[str, str] | None = None) -> tuple[set[str], bool]:
    """The names that a module's file defines or imports at its top level, and whether other names may exist that this read cannot list: a star import of a
    module that is not the repository's own or that has no readable file, and a module ``__getattr__``. An annotation with no value defines no name, and a
    ``del`` that is the file's own statement removes the names bound before it. ``siblings`` (see ``sibling_problems``) answers a star import by a bare name."""
    siblings = siblings or {}
    file = module_file(source, module)
    text = source.read(file) if file is not None else None
    tree = parsed(text, file or module) if text is not None else None
    if file is None or tree is None or module in seen:
        return set(), True
    names: set[str] = set()
    open_ended = False
    own = tree.body if isinstance(tree, ast.Module) else []
    for node in top_level_statements(own):
        if isinstance(node, ast.Import):
            names |= {alias.asname or alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name != "*":
                    names.add(alias.asname or alias.name)
                    continue
                target = absolute_module(file, node.level, node.module) if node.level else node.module
                if not node.level and target and target in siblings:
                    more, wide = star_names(source, siblings[target], seen | {module}, siblings)
                elif target and target.split(".")[0] in FIRST_PARTY:
                    more, wide = exported_names(source, target, seen | {module})
                else:
                    more, wide = set(), True
                names |= more - {"__all__"}
                open_ended = open_ended or wide
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
            open_ended = open_ended or node.name == "__getattr__"
        elif isinstance(node, ast.TypeAlias):
            names |= target_names(node.name)
        elif isinstance(node, ast.Assign):
            names |= set().union(*(target_names(target) for target in node.targets))
        elif isinstance(node, ast.AugAssign | ast.For | ast.AsyncFor) or (isinstance(node, ast.AnnAssign) and node.value is not None):
            names |= target_names(node.target)
        elif isinstance(node, ast.With | ast.AsyncWith):
            names |= set().union(*(target_names(item.optional_vars) for item in node.items))
        elif isinstance(node, ast.Delete) and node in own:
            names -= set().union(*(target_names(target, ast.Del) for target in node.targets))
    return names, open_ended


def sibling_problems(source: Source, siblings: Mapping[str, str]) -> list[str]:
    """One line for each import that a sibling file cannot answer. ``siblings`` maps the bare name by which they import each other to each one's path. A
    sibling is a module and not a package, so it has no submodule; a name that a ``from`` import takes from it must be one that this read lists at its top
    level, including what it takes from another sibling, so a name that only a star import from outside the repository and the siblings or a module
    ``__getattr__`` could give is refused. A file that is not there or is not valid Python is left to ``closure``, which reports it."""
    problems: list[str] = []
    for file in sorted(siblings.values()):
        text = source.read(file)
        tree = parsed(text, file) if text is not None else None
        if tree is None:
            continue
        for module, names in imported_names(tree, file):
            sibling, _, below = (module or "").partition(".")
            if sibling not in siblings:
                continue
            if below:
                problems.append(f"{file}: {module} cannot be imported: the probe {sibling} is a module, not a package")
                continue
            held = exported_names(source, sibling_module(siblings[sibling]), siblings=siblings)[0]
            problems += [f"{file}: the probe {sibling} has no name {name} at its top level" for name in names if name != "*" and name not in held]
    return list(dict.fromkeys(problems))


def closure(source: Source, entries: Sequence[str]) -> Closure:
    result = Closure()
    pending = list(entries)
    while pending:
        file = pending.pop()
        if file in result.files:
            continue
        text = source.read(file)
        if text is None:
            result.missing.append(f"{file}: the file is not there")
            continue
        result.files.add(file)
        tree = parsed(text, file)
        if tree is None:
            result.unreadable.append(f"{file}: the file is not valid Python, so its imports cannot be read")
            continue
        if uses_dynamic_import(tree):
            result.dynamic.add(file)
        for module, names in imported_names(tree, file):
            if module is None:
                result.missing.append(f"{file}: a relative import leaves the repository")
                continue
            top = module.split(".")[0]
            if top in FIRST_PARTY or module == "":
                targets = module_files(source, module) if module else []
                if not targets and module:
                    result.missing.append(f"{file}: {module} has no file")
                exports = exported_names(source, module) if targets else None
                for name in names:
                    sub = module_files(source, f"{module}.{name}" if module else name)
                    targets += sub
                    if name == "*" or sub:
                        continue
                    if module == "":
                        result.missing.append(f"{file}: {name} has no file")
                    elif exports is not None and name not in exports[0] and not exports[1]:
                        result.missing.append(f"{file}: {module} has no name {name} and no submodule of that name")
                pending += targets
            elif top not in sys.stdlib_module_names:
                result.third_party[top].add(file)
    return result


def normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def locked_distributions(texts: list[str]) -> set[str]:
    """The names of the distributions that the lock files pin, in the one form that pip compares."""
    return {normalized(match.group(1)) for text in texts for match in LOCKED_LINE.finditer(text)}


def unlocked(third_party: dict[str, set[str]], locked: set[str], distributions: dict[str, list[str]] | None = None) -> list[str]:
    """One line for each top-level name whose distribution the lock files do not pin, with the files that import it.
    A name that no installed distribution provides is refused too: the lock files could not install it either."""
    providers = distributions if distributions is not None else importlib.metadata.packages_distributions()
    problems: list[str] = []
    for name, files in sorted(third_party.items()):
        names = {normalized(distribution) for distribution in providers.get(name, [])}
        where = ", ".join(sorted(files)[:3])
        if not names:
            problems.append(f"{name}: no distribution is known for it (imported by {where})")
        elif not names & locked:
            problems.append(f"{name}: {sorted(names)} is not in the lock files (imported by {where})")
    return problems
