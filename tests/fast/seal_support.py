"""Guards for acceptance runners that hold a copy of the clinical file: a static scan for a host-side read of it, and a cleanup that always scrubs.

``host_parse_problems`` judges the source of a runner. The clinical and laboratory files hold the outcomes of every patient, the held-out ones included, so a
runner may check them by SHA-256 and copy their bytes for a container, and may never open, read or parse them itself. The scan refuses the import of a CSV or
table reader, a call of ``DictReader``, ``reader`` or ``read_csv``, and a call that opens or reads a path whose text, or a name that came from it, mentions the
clinical or the laboratory file. A problem names the line only, never the text.

``run_cleanup`` runs every step of a cleanup and then the scrub, whatever a step raises (a timeout of ``docker ps`` included), and raises the first error after
the scrub. ``scrub_tree`` removes a folder, also one with read-only parts, and says whether it is gone.
"""

from __future__ import annotations

import ast
import contextlib
import os
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path

SEALED_WORDS = ("clinical", "lab_data")
READER_MODULES = ("csv", "pandas")
READER_NAMES = frozenset({"DictReader", "read_csv", "read_table"})
READ_METHODS = frozenset({"open", "read_text", "read_bytes", "read", "readline", "readlines"})


def mentions_sealed(text: str) -> bool:
    lowered = text.lower()
    return any(word in lowered for word in SEALED_WORDS)


def tainted_names(tree: ast.AST) -> set[str]:
    """The names that hold, or came from, a path or a text that mentions a sealed file, followed until no name is added."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and mentions_sealed(node.id):
            names.add(node.id)
        elif isinstance(node, ast.arg) and mentions_sealed(node.arg):
            names.add(node.arg)
    assignments: list[tuple[list[ast.expr], ast.AST]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            assignments.append((node.targets, node.value))
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            assignments.append(([node.target], node.value))
        elif isinstance(node, ast.With):
            assignments.extend(([item.optional_vars], item.context_expr) for item in node.items if item.optional_vars is not None)
    changed = True
    while changed:
        changed = False
        for targets, value in assignments:
            used = {part.id for part in ast.walk(value) if isinstance(part, ast.Name)}
            if mentions_sealed(ast.unparse(value)) or used & names:
                for target in targets:
                    for part in ast.walk(target):
                        if isinstance(part, ast.Name) and part.id not in names:
                            names.add(part.id)
                            changed = True
    return names


def is_reader_call(call: ast.Call) -> bool:
    function = call.func
    return (isinstance(function, ast.Name) and function.id in ({"open"} | READER_NAMES)) or (
        isinstance(function, ast.Attribute)
        and (function.attr in READ_METHODS or function.attr in READER_NAMES or (function.attr == "reader" and ast.unparse(function.value) == "csv"))
    )


def reader_import(node: ast.AST) -> bool:
    if isinstance(node, ast.Import):
        return any(alias.name.split(".")[0] in READER_MODULES for alias in node.names)
    return isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in READER_MODULES


def host_parse_problems(source: str) -> list[str]:
    """The lines of ``source`` that read or parse a sealed file on the host, or import a reader that could."""
    tree = ast.parse(source)
    tainted = tainted_names(tree)
    problems: list[str] = []
    for node in ast.walk(tree):
        if reader_import(node):
            problems.append(f"line {getattr(node, 'lineno', 0)}: a CSV or table reader is imported")
        elif isinstance(node, ast.Call) and is_reader_call(node):
            names = {part.id for part in ast.walk(node) if isinstance(part, ast.Name)}
            function = node.func
            parser = (isinstance(function, ast.Name) and function.id in READER_NAMES) or (
                isinstance(function, ast.Attribute) and (function.attr in READER_NAMES or function.attr == "reader")
            )
            if parser or mentions_sealed(ast.unparse(node)) or names & tainted:
                problems.append(f"line {node.lineno}: a sealed file is read on the host")
    return sorted(set(problems), key=lambda item: int(item.split(":")[0].split()[1]))


def run_cleanup(steps: Sequence[Callable[[], object]], scrub: Callable[[], object]) -> None:
    """Run every step, then ``scrub``, whatever a step raises. The first error is raised after the scrub."""
    first: BaseException | None = None
    try:
        for step in steps:
            try:
                step()
            except BaseException as error:
                first = first or error
    finally:
        scrub()
    if first is not None:
        raise first


def scrub_tree(path: Path) -> bool:
    """Remove ``path`` and everything under it, also a read-only part, and return whether it is gone."""

    def retry(function: Callable[[str], object], target: str, _error: BaseException) -> None:
        for item in (os.path.dirname(target), target):
            with contextlib.suppress(OSError):
                os.chmod(item, 0o700)
        with contextlib.suppress(OSError):
            function(target)

    shutil.rmtree(path, onexc=retry)
    return not path.exists()
