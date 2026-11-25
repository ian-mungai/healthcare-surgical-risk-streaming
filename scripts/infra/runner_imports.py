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
from collections import defaultdict
from collections.abc import Mapping, Sequence
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
        tree = ast.parse(text, file)
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
                for name in names:
                    targets += module_files(source, f"{module}.{name}" if module else name)
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
