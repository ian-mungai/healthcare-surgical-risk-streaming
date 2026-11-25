"""The third-party imports of the native host runners, found without running them, and the packages that the runner checkout's lock files pin.

The host process of a runner (``tests/e2e/silver_signals.py``, ``silver_hypotension.py``, ``gold_dimensions.py`` and ``stream_host.py``) imports the
repository's own modules (``scripts``, ``spark``, ``replay``, ``features``, ``casefile``, ``tests``) and, through them, a few packages. The Spark and Avro
code that the runners start runs in containers (``python -m spark.jobs...`` inside the image), so it is not in the host process. ``closure`` follows every
import, at any depth of a file, from the entry files through the repository's own files that exist on disk (the runners, which Git does not track, are read
where they are). The result is the top-level names that are neither the standard library nor the repository, with the files that import them.
"""

from __future__ import annotations

import ast
import importlib.metadata
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

FIRST_PARTY = ("spark", "scripts", "features", "orchestration", "replay", "casefile", "tests")
DYNAMIC_CALLS = ("spec_from_file_location", "import_module", "__import__")
LOCKED_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?==", re.MULTILINE)


@dataclass
class Closure:
    files: set[Path] = field(default_factory=set)
    third_party: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    dynamic: set[str] = field(default_factory=set)
    missing: list[str] = field(default_factory=list)


def module_file(root: Path, module: str) -> Path | None:
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def module_files(root: Path, module: str) -> list[Path]:
    """The files that importing ``module`` runs: its own file (or its package's ``__init__``) and the ``__init__`` of each package above it.
    Empty when the module has no file."""
    if module_file(root, module) is None:
        return []
    parts = module.split(".")
    found = [path for end in range(1, len(parts) + 1) if (path := module_file(root, ".".join(parts[:end]))) is not None]
    return found


def imported_names(tree: ast.AST) -> list[tuple[str, list[str]]]:
    """Each absolute import of a file at any depth: its module and the names that a ``from`` import takes from it."""
    found: list[tuple[str, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(alias.name, []) for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append((node.module, [alias.name for alias in node.names]))
    return found


def uses_dynamic_import(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = node.func
            name = function.attr if isinstance(function, ast.Attribute) else function.id if isinstance(function, ast.Name) else ""
            if name in DYNAMIC_CALLS:
                return True
    return False


def closure(root: Path, entries: list[Path]) -> Closure:
    result = Closure()
    pending = list(entries)
    while pending:
        file = pending.pop()
        if file in result.files:
            continue
        result.files.add(file)
        relative = file.relative_to(root).as_posix()
        tree = ast.parse(file.read_text(), str(file))
        if uses_dynamic_import(tree):
            result.dynamic.add(relative)
        for module, names in imported_names(tree):
            top = module.split(".")[0]
            if top in FIRST_PARTY:
                targets = module_files(root, module)
                if not targets:
                    result.missing.append(f"{relative}: {module} has no file")
                for name in names:
                    targets += module_files(root, f"{module}.{name}")
                pending += targets
            elif top not in sys.stdlib_module_names:
                result.third_party[top].add(relative)
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
