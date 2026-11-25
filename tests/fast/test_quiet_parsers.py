"""Every command line is read by the quiet parser, and no usage message repeats a value of the command line.

A plain ``argparse.ArgumentParser`` repeats a mistyped word in its error. ``scripts.ingestion.closed.QuietParser`` prints one fixed text. The
scan below fails for any plain parser outside that module. Argument-type errors, which a parser would otherwise print, name the argument and never its value.
"""

from __future__ import annotations

import ast
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.process import run_command
from scripts.stack import caddy
from tests.fast.git_fixture import TIMEOUT_SECONDS

ROOT = Path(__file__).resolve().parents[2]
QUIET_MODULE = "scripts/ingestion/closed.py"
MARKER = "zz-marker-value-zz"


def tracked_python() -> list[str]:
    listing = run_command("git", ["ls-files", "-z", "--", "*.py"], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=True).stdout
    return [name for name in listing.split("\0") if name and (ROOT / name).is_file()]


def plain_parser_calls(source: str) -> list[int]:
    lines = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "ArgumentParser":
            lines.append(node.lineno)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "ArgumentParser":
            lines.append(node.lineno)
    return lines


def value_repeating_messages(source: str, functions: tuple[str, ...]) -> list[int]:
    """Lines where an f-string inside one of ``functions`` converts a value with ``!r``, which would repeat a command-line word."""
    lines = []
    for function in ast.walk(ast.parse(source)):
        if isinstance(function, ast.FunctionDef) and function.name in functions:
            lines += [node.lineno for node in ast.walk(function) if isinstance(node, ast.FormattedValue) and node.conversion == ord("r")]
    return lines


PARSED_NAMES = frozenset({"arguments", "args", "parsed", "namespace", "options"})
FILE_ERRORS = frozenset({"OSError", "IOError", "FileNotFoundError", "PermissionError", "IsADirectoryError", "NotADirectoryError"})


def chain_root(node: ast.AST) -> str | None:
    while isinstance(node, ast.Attribute):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def error_sinks(tree: ast.AST) -> list[ast.AST]:
    """The parts of a module that make an error message: what a ``raise`` builds and what is written to standard error."""
    found: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and node.exc is not None:
            found.append(node.exc)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "write" and ast.unparse(node.func.value) == "sys.stderr":
            found.append(node)
    return found


def interpolations(sink: ast.AST) -> list[ast.expr]:
    return [part.value for part in ast.walk(sink) if isinstance(part, ast.FormattedValue)]


def argument_values_in_messages(source: str) -> list[int]:
    """Lines where an error message interpolates an attribute of the parsed command line, or the text of an error about a file, which names its path."""
    tree = ast.parse(source)
    lines = []
    for sink in error_sinks(tree):
        for value in interpolations(sink):
            if any(isinstance(part, ast.Attribute) and chain_root(part) in PARSED_NAMES for part in ast.walk(value)):
                lines.append(value.lineno)
    for handler in (node for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler) and node.name):
        caught = {ast.unparse(item) for item in (handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type])} if handler.type else set()
        if caught & FILE_ERRORS:
            for statement in handler.body:
                for sink in error_sinks(statement):
                    lines += [value.lineno for value in interpolations(sink) if isinstance(value, ast.Name) and value.id == handler.name]
    return sorted(set(lines))


class QuietParserTests(unittest.TestCase):
    def test_no_plain_argument_parser_outside_the_quiet_parsers_module(self) -> None:
        found = {name: plain_parser_calls((ROOT / name).read_text()) for name in tracked_python() if name != QUIET_MODULE}
        self.assertEqual({}, {name: lines for name, lines in found.items() if lines})

    def test_the_quiet_parser_repeats_no_word(self) -> None:
        from scripts.ingestion.closed import QuietParser

        parser = QuietParser(prog="x")
        parser.add_argument("--count", type=int)
        for arguments in (["--count", MARKER], [MARKER], ["--" + MARKER]):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit) as stop:
                import contextlib
                import io

                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    parser.parse_args(arguments)
            self.assertEqual(2, stop.exception.code)
            self.assertNotIn(MARKER, err.getvalue())

    def test_no_error_message_interpolates_a_command_line_value_or_the_text_of_a_file_error(self) -> None:
        found = {name: argument_values_in_messages((ROOT / name).read_text()) for name in tracked_python() if not name.startswith(("tests/", "ansible/"))}
        self.assertEqual({}, {name: lines for name, lines in found.items() if lines})

    def test_the_scan_finds_each_form_that_it_is_written_for(self) -> None:
        attribute = "def main(arguments):\n    raise Failure(f'bad {arguments.branch}')\n"
        written = "import sys\n\n\ndef main(args):\n    sys.stderr.write(f'{args.x}')\n"
        file_error = "def read(path):\n    try:\n        return path.read_text()\n    except OSError as error:\n        raise Failure(f'{error}') from None\n"
        fixed = "def main(arguments):\n    raise Failure('fixed text')\n"
        self.assertEqual([2], argument_values_in_messages(attribute))
        self.assertEqual([5], argument_values_in_messages(written))
        self.assertEqual([5], argument_values_in_messages(file_error))
        self.assertEqual([], argument_values_in_messages(fixed))

    def test_caddy_render_names_no_file_when_it_cannot_read_the_routes_file(self) -> None:
        err = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as folder,
            mock.patch.object(sys, "argv", ["caddy", "render", str(Path(folder) / "secret_name_marker.txt")]),
            contextlib.redirect_stderr(err),
        ):
            code = caddy.main()
        self.assertEqual(1, code)
        self.assertNotIn("secret_name_marker", err.getvalue())

    def test_the_named_messages_repeat_no_value(self) -> None:
        for name, functions in (
            ("scripts/stack/groups.py", ("whole_number",)),
            ("scripts/stack/admission.py", ("whole_number",)),
            ("spark/maintenance.py", ("finite_seconds", "table_argument")),
            ("orchestration/surgical_risk/concurrency.py", ("heavy_memory_mib",)),
            ("scripts/stack/capacity_sampler.py", ("parse",)),
        ):
            with self.subTest(name=name):
                self.assertEqual([], value_repeating_messages((ROOT / name).read_text(), functions))


if __name__ == "__main__":
    unittest.main()
