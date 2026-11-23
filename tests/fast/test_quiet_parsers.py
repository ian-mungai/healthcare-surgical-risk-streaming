"""Every command line is read by the quiet parser, and no usage message repeats a value of the command line.

A plain ``argparse.ArgumentParser`` repeats a mistyped word in its error. ``scripts.ingestion.closed.QuietParser`` prints one fixed text. The
scan below fails for any plain parser outside that module. Argument-type errors, which a parser would otherwise print, name the argument and never its value.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.process import run_command
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
