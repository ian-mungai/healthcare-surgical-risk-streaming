"""Every name that a module imports from another module of this repository is defined there.

A branch that removes a helper (the sealing branch removed ``unit`` and ``RESULT_PREFIX``) breaks every module that still imports it, and a module that no
test imports breaks only when its job starts on the cluster. This check reads each file as text (``ast``), never runs it, so it needs no Spark: it
resolves ``from <module> import <name>`` against the names that the target module defines at its top level (a class, a function, an assignment, an import,
also inside ``if`` and ``try``), or against a module of the same name in the package.

Failure modes covered here:

- A name that the target module never defines (the removed ``unit``): refused, named by file, line and name.
- A name that the target module only imports from a third module: accepted, because it is a name of the target (a re-export).
- A submodule of a package (``from spark.silver import tables``): accepted.
- A relative import, a star import or a module outside the repository's own roots: not judged here.
- A target module whose file is missing: refused.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIRST_PARTY = ("spark", "scripts", "features", "orchestration", "replay", "tests")


def module_file(root: Path, module: str) -> Path | None:
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def defined_names(tree: ast.AST) -> set[str] | None:
    """The top-level names of a module, or ``None`` when it re-exports with ``import *`` (every name is then possible)."""
    names: set[str] = set()

    def walk(body: list[ast.stmt]) -> bool:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(node.name)
            elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names.update(name.id for target in targets for name in ast.walk(target) if isinstance(name, ast.Name))
            elif isinstance(node, ast.Import):
                names.update((alias.asname or alias.name).split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if any(alias.name == "*" for alias in node.names):
                    return False
                names.update(alias.asname or alias.name for alias in node.names)
            elif isinstance(node, ast.If | ast.Try | ast.With):
                parts = [node.body, getattr(node, "orelse", []), getattr(node, "finalbody", [])]
                parts += [handler.body for handler in getattr(node, "handlers", [])]
                if not all(walk(part) for part in parts):
                    return False
        return True

    return names if walk(tree.body) else None  # type: ignore[attr-defined]


def unresolved_imports(root: Path, files: list[Path]) -> list[str]:
    """One line for each ``from <first-party module> import <name>`` whose name the module does not define: ``file:line: module.name``."""
    problems: list[str] = []
    cache: dict[Path, set[str] | None] = {}
    for file in files:
        for node in ast.walk(ast.parse(file.read_text(), str(file))):
            if not isinstance(node, ast.ImportFrom) or node.level or not node.module or node.module.split(".")[0] not in FIRST_PARTY:
                continue
            target = module_file(root, node.module)
            where = f"{file.relative_to(root)}:{node.lineno}"
            if target is None:
                problems.append(f"{where}: {node.module} has no file")
                continue
            if target not in cache:
                cache[target] = defined_names(ast.parse(target.read_text(), str(target)))
            names = cache[target]
            for alias in node.names:
                if alias.name == "*" or names is None or alias.name in names or module_file(root, f"{node.module}.{alias.name}"):
                    continue
                problems.append(f"{where}: {node.module}.{alias.name}")
    return problems


def first_party_python() -> list[Path]:
    return sorted(file for root in FIRST_PARTY for file in (ROOT / root).rglob("*.py") if "__pycache__" not in file.parts)


class RepositoryImports(unittest.TestCase):
    def test_every_tracked_module_imports_only_names_that_exist(self) -> None:
        files = first_party_python()
        self.assertGreater(len(files), 200)
        self.assertEqual([], unresolved_imports(ROOT, files))


class Resolver(unittest.TestCase):
    """The check itself, on a small tree: each rule has a file that breaks it and one that does not."""

    def tree(self, files: dict[str, str]) -> tuple[Path, list[Path]]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="imports_")))
        for name, text in files.items():
            (folder / name).parent.mkdir(parents=True, exist_ok=True)
            (folder / name).write_text(text)
        return folder, [folder / name for name in files if name.startswith("spark/user")]

    def problems(self, files: dict[str, str]) -> list[str]:
        root, users = self.tree(files)
        return unresolved_imports(root, users)

    BASE = {"spark/__init__.py": "", "spark/tool.py": "import os\nfrom spark.other import moved\n\ndef kept(): ...\n\nCONSTANT = 1\n\nclass Thing: ...\n"}

    def test_a_removed_name_is_refused_with_its_file_and_line(self) -> None:
        user = {"spark/user.py": "from spark.tool import kept, unit\n"}
        self.assertEqual(["spark/user.py:1: spark.tool.unit"], self.problems({**self.BASE, **user, "spark/other.py": "moved = 1\n"}))

    def test_every_kind_of_definition_is_a_name(self) -> None:
        user = {"spark/user.py": "from spark.tool import kept, CONSTANT, Thing, moved, os\n"}
        self.assertEqual([], self.problems({**self.BASE, **user, "spark/other.py": "moved = 1\n"}))

    def test_a_name_defined_inside_a_try_or_an_if_is_a_name(self) -> None:
        tool = "try:\n    import fast as engine\nexcept ImportError:\n    engine = None\nif True:\n    def late(): ...\n"
        user = {"spark/user.py": "from spark.tool import engine, late\n"}
        self.assertEqual([], self.problems({"spark/__init__.py": "", "spark/tool.py": tool, **user}))

    def test_a_submodule_is_a_name_of_its_package(self) -> None:
        files = {"spark/__init__.py": "", "spark/pkg/__init__.py": "", "spark/pkg/inner.py": "", "spark/user.py": "from spark.pkg import inner, absent\n"}
        self.assertEqual(["spark/user.py:1: spark.pkg.absent"], self.problems(files))

    def test_a_missing_module_is_refused(self) -> None:
        self.assertEqual(["spark/user.py:1: spark.gone has no file"], self.problems({"spark/__init__.py": "", "spark/user.py": "from spark.gone import x\n"}))

    def test_a_star_import_relative_import_and_outside_module_are_not_judged(self) -> None:
        files = {
            "spark/__init__.py": "",
            "spark/wide.py": "from os.path import *\n",
            "spark/user.py": "from spark.wide import anything\nfrom . import sibling\nfrom os import path, nothing_here\n",
        }
        self.assertEqual([], self.problems(files))

    def test_every_refused_import_of_one_line_is_named(self) -> None:
        user = {"spark/user.py": "from spark.tool import unit, RESULT_PREFIX, kept\n"}
        self.assertEqual(
            ["spark/user.py:1: spark.tool.unit", "spark/user.py:1: spark.tool.RESULT_PREFIX"],
            self.problems({**self.BASE, **user, "spark/other.py": "moved = 1\n"}),
        )


if __name__ == "__main__":
    unittest.main()
