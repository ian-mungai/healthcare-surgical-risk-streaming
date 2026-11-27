"""The lists of classes that the diagnostic line may name (``spark/internal_classes.py``) are the classes that the project defines and the libraries have.

Failure modes covered here:

- A new exception class of the project is added and not listed: its errors would show as ``other`` and the line would hide the one thing that it is for. The
  test reads the source of every package (no import) and fails until the list holds exactly the exception classes that the packages define at the top of a
  module.
- A listed name is wrong (a typo, a class that moved): the class would never be named. Each listed class that can be imported here is found and is an
  exception class, and a class that is on the list twice, or twice under two names, is refused.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import unittest
from pathlib import Path

from spark import internal_classes, result_file
from spark.internal_diagnostic import BUILTIN_CLASSES, CODE_LINES, INTERNAL_EXIT_CODE

ROOT = Path(__file__).resolve().parents[2]


def defined_exception_classes() -> set[str]:
    """``module.Class`` of every class at the top of a module of the packages that has a base that is an exception class (a builtin, or one found earlier)."""
    bases: dict[str, list[str]] = {}
    for package in internal_classes.PACKAGES:
        for source in sorted((ROOT / package).rglob("*.py")):
            module = ".".join(source.relative_to(ROOT).with_suffix("").parts).removesuffix(".__init__")
            for node in ast.parse(source.read_text()).body:
                if isinstance(node, ast.ClassDef):
                    bases[f"{module}.{node.name}"] = [base.id if isinstance(base, ast.Name) else getattr(base, "attr", "") for base in node.bases]
    known = {name for name, item in vars(builtins).items() if isinstance(item, type) and issubclass(item, BaseException)}
    found: set[str] = set()
    grew = True
    while grew:
        grew = False
        for path, names in bases.items():
            if path not in found and any(name in known or name in {item.rpartition(".")[2] for item in found} for name in names):
                found.add(path)
                grew = True
    return found


class TheLists(unittest.TestCase):
    def test_the_project_classes_are_exactly_the_exception_classes_that_the_packages_define(self) -> None:
        listed = set(internal_classes.PROJECT_CLASSES)
        defined = defined_exception_classes()
        self.assertEqual([], sorted(defined - listed), "add these classes to PROJECT_CLASSES in spark/internal_classes.py")
        self.assertEqual([], sorted(listed - defined), "remove these classes from PROJECT_CLASSES: no package defines them")

    def test_no_class_is_listed_twice(self) -> None:
        every = [
            *internal_classes.PROJECT_CLASSES,
            *internal_classes.LIBRARY_CLASSES,
            *(f"{internal_classes.PYSPARK_MODULE}.{n}" for n in internal_classes.PYSPARK_CLASSES),
        ]
        self.assertEqual(len(every), len(set(every)))
        self.assertEqual(sorted(internal_classes.PROJECT_CLASSES), list(internal_classes.PROJECT_CLASSES))

    def test_every_listed_class_that_can_be_imported_here_is_an_exception_class_of_that_name(self) -> None:
        for path in (*internal_classes.PROJECT_CLASSES, *internal_classes.LIBRARY_CLASSES):
            module_name, _, name = path.rpartition(".")
            with self.subTest(path=path):
                try:
                    module = importlib.import_module(module_name)
                except ModuleNotFoundError as missing:
                    if missing.name is not None and missing.name.split(".")[0] in internal_classes.PACKAGES:
                        raise
                    self.skipTest(f"{missing.name} is not installed here")
                found = getattr(module, name)
                self.assertTrue(isinstance(found, type) and issubclass(found, BaseException), path)

    def test_a_builtin_alias_is_named_by_the_class(self) -> None:
        self.assertEqual("builtins.OSError", BUILTIN_CLASSES[id(IOError)])
        self.assertEqual(len(BUILTIN_CLASSES), len(set(BUILTIN_CLASSES.values())))

    def test_the_code_of_an_internal_error_and_its_lines_are_those_of_the_jobs(self) -> None:
        self.assertEqual(result_file.INTERNAL_EXIT_CODE, INTERNAL_EXIT_CODE)
        self.assertEqual(("internal_error", result_file.line_of("internal_error")), CODE_LINES)


if __name__ == "__main__":
    unittest.main()
