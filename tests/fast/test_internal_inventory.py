"""The fixed inventory of trusted code locations (``spark/internal_inventory.py``) is what the generator writes for the tracked files, and nothing else.

The diagnostic line of a sealed job names a frame only when its file and its function are in this inventory (``spark/internal_diagnostic.py``), so the
inventory is the one list of what a frame may say. A name that data can build (a generated file name, a function compiled at run time) is never in it.

Failure modes covered here:

- A function is added, renamed or removed, or a file is added or removed, and the inventory is not regenerated: a real frame of the project would show as
  ``other`` (a stale entry the other way: a name that no code defines would be trusted). The test compares the module with the output of the generator for
  the files that ``git ls-files`` lists, and the generator reads the AST of each file, so the module is exactly the tracked code.
- The inventory holds a name that the line cannot carry safely: a path or a name with a character outside ``[A-Za-z0-9_/.]``, a name that is longer than the
  limits of the line, a file outside the packages, or a file that is not tracked (an untracked or generated file is no trusted code).
- The generator misses a kind of definition (a method, a nested function, an ``async`` function, a class whose body raised) or invents one (a lambda, a
  variable, an imported name), or it writes a different text on a second run, or it hides a file that does not parse.
"""

from __future__ import annotations

import ast
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.process import run_command
from scripts.quality import internal_inventory as generator
from spark import internal_diagnostic
from spark.internal_classes import NAME_LIMIT, PACKAGES, PATH_LIMIT
from spark.internal_inventory import INVENTORY

ROOT = Path(__file__).resolve().parents[2]
TIMEOUT_SECONDS = 60


def tracked_package_files() -> list[str]:
    listing = run_command("git", ["ls-files", "-z", "--", "*.py"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout
    return sorted(path for path in listing.split("\0") if path.split("/")[0] in PACKAGES and path != generator.GENERATED)


def names_in(source: str) -> set[str]:
    return {node.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)}


class TheModule(unittest.TestCase):
    def test_it_is_what_the_generator_writes_for_the_tracked_files(self) -> None:
        self.assertEqual(
            generator.render(generator.build(ROOT)),
            (ROOT / generator.GENERATED).read_text(encoding="utf-8"),
            "run: .venv/bin/python -m scripts.quality.internal_inventory",
        )

    def test_it_lists_every_tracked_file_of_the_packages_and_no_other(self) -> None:
        self.assertEqual(tracked_package_files(), sorted(INVENTORY))

    def test_each_file_lists_exactly_the_names_that_its_source_defines(self) -> None:
        for path, names in INVENTORY.items():
            with self.subTest(path=path):
                self.assertEqual(sorted(names_in((ROOT / path).read_text(encoding="utf-8"))), list(names))

    def test_every_path_and_name_can_be_carried_by_the_line(self) -> None:
        for path, names in INVENTORY.items():
            with self.subTest(path=path):
                self.assertRegex(path, r"[A-Za-z0-9_]+(?:/[A-Za-z0-9_]+)*\.py")
                self.assertLessEqual(len(path), PATH_LIMIT)
                for name in names:
                    self.assertRegex(name, r"[A-Za-z_][A-Za-z0-9_]*")
                    self.assertLessEqual(len(name), NAME_LIMIT)

    def test_the_generated_file_is_not_in_it_and_the_check_command_agrees(self) -> None:
        self.assertNotIn(generator.GENERATED, INVENTORY)
        self.assertEqual(0, generator.main(["--check"]))


class TheGenerator(unittest.TestCase):
    SOURCE = (
        "import os\n"
        "import json as j\n"
        "X = lambda: 1\n"
        "value = 3\n"
        "def top():\n"
        "    def inner():\n"
        "        return [n for n in range(3)]\n"
        "    return inner\n"
        "async def later():\n"
        "    return None\n"
        "class Box:\n"
        "    attribute = 1\n"
        "    def method(self):\n"
        "        return (n for n in ())\n"
        "    @property\n"
        "    def prop(self):\n"
        "        return 1\n"
    )

    def test_it_lists_functions_methods_nested_and_async_functions_and_classes_only(self) -> None:
        self.assertEqual({"spark/a.py": ["Box", "inner", "later", "method", "prop", "top"]}, generator.inventory_of({"spark/a.py": self.SOURCE}))

    def test_it_lists_a_file_with_no_definition_as_empty(self) -> None:
        self.assertEqual({"spark/b.py": []}, generator.inventory_of({"spark/b.py": "value = 1\n"}))

    def test_it_refuses_a_file_that_does_not_parse_and_a_name_that_is_not_ascii(self) -> None:
        with self.assertRaises(generator.InventoryError):
            generator.inventory_of({"spark/c.py": "def broken(:\n"})
        with self.assertRaises(generator.InventoryError):
            generator.inventory_of({"spark/d.py": "def é():\n    pass\n"})
        with self.assertRaises(generator.InventoryError):
            generator.inventory_of({"spark/e.py": "def " + "f" * (NAME_LIMIT + 1) + "():\n    pass\n"})
        with self.assertRaises(generator.InventoryError):
            generator.inventory_of({"spark/has-hyphen.py": "x = 1\n"})
        with self.assertRaises(generator.InventoryError):
            generator.inventory_of({"spark/" + "d/" * 100 + "x.py": "x = 1\n"})

    def test_the_text_is_the_same_on_every_run_and_sorted_and_ends_with_a_newline(self) -> None:
        files = {"spark/b.py": "def two():\n    pass\n\n\ndef one():\n    pass\n", "casefile/a.py": "class A:\n    pass\n"}
        first = generator.render(generator.inventory_of(files))
        self.assertEqual(first, generator.render(generator.inventory_of(dict(reversed(files.items())))))
        self.assertTrue(first.endswith("\n"))
        self.assertLess(first.index('"casefile/a.py"'), first.index('"spark/b.py"'))
        self.assertLess(first.index('"one"'), first.index('"two"'))
        self.assertLessEqual(max(len(line) for line in first.splitlines()), 160)
        namespace: dict[str, object] = {}
        exec(compile(first, "<generated>", "exec"), namespace)  # noqa: S102 (the generated module must run as it is)
        self.assertEqual({"casefile/a.py": ("A",), "spark/b.py": ("one", "two")}, namespace["INVENTORY"])

    def test_a_long_list_of_names_is_wrapped_inside_the_line_limit(self) -> None:
        source = "".join(f"def function_number_{n:04d}():\n    pass\n\n\n" for n in range(60))
        text = generator.render(generator.inventory_of({"spark/long.py": source}))
        self.assertLessEqual(max(len(line) for line in text.splitlines()), 160)
        namespace: dict[str, object] = {}
        exec(compile(text, "<generated>", "exec"), namespace)  # noqa: S102
        self.assertEqual(60, len(namespace["INVENTORY"]["spark/long.py"]))  # type: ignore[index]

    def test_only_the_tracked_files_of_the_packages_are_read_and_the_generated_file_never(self) -> None:
        listed = ["spark/a.py", "tests/fast/x.py", "docs/y.py", "spark/internal_inventory.py", "spark/notes.txt", "scripts/quality/z.py", "setup.py"]
        self.assertEqual(["scripts/quality/z.py", "spark/a.py"], generator.package_files(listed))

    def test_the_check_command_reports_a_stale_module_and_the_write_command_fixes_it(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            target = root / generator.GENERATED
            target.parent.mkdir(parents=True)
            source = root / "spark" / "a.py"
            source.write_text("def one():\n    pass\n")
            files = {"spark/a.py": source.read_text(encoding="utf-8")}
            target.write_text(generator.render(generator.inventory_of(files)))
            self.assertEqual(0, generator.check(root, files))
            source.write_text("def one():\n    pass\n\n\ndef two():\n    pass\n")
            files = {"spark/a.py": source.read_text(encoding="utf-8")}
            with contextlib.redirect_stderr(io.StringIO()) as report:
                self.assertEqual(1, generator.check(root, files))
            self.assertIn("stale", report.getvalue())
            self.assertEqual(0, generator.write(root, files))
            self.assertEqual(0, generator.check(root, files))
            self.assertIn('"two"', target.read_text(encoding="utf-8"))
            before = target.stat().st_mtime_ns
            self.assertEqual(0, generator.write(root, files))
            self.assertEqual(before, target.stat().st_mtime_ns)  # nothing changed, nothing written


class TheFixedNames(unittest.TestCase):
    def test_the_names_of_the_code_that_has_no_function_of_its_own_are_fixed(self) -> None:
        self.assertEqual({"<module>", "<lambda>", "<genexpr>", "<listcomp>", "<dictcomp>", "<setcomp>"}, set(internal_diagnostic.FIXED_NAMES))


if __name__ == "__main__":
    unittest.main()
