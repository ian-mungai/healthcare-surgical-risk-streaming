"""Every text read of an input file in the quality, ingestion and sample scripts names its encoding, so the machine's locale cannot change what a file says.

A read that names none uses the locale's encoding. Under ``en_US.ISO8859-1`` with UTF-8 mode off, a file with a ``0xff`` byte is then read as ``ÿ`` and accepted
or echoed, where the fixed line for an unreadable file is the answer. Join round 3, finding 2 and its class.
Round 4, finding 4: a bare ``open`` and ``encoding=None`` are reads that name none.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

from tests.fast.locale_support import non_utf_8_locale

ROOT = Path(__file__).resolve().parents[2]
FOLDERS = ("scripts/quality", "scripts/ingestion", "scripts/sample")
NOT_FILES = {"os", "tarfile", "gzip"}

FILE_FIRST = ("io", "builtins")


def is_none(value: ast.expr | None) -> bool:
    return isinstance(value, ast.Constant) and value.value is None


def argument(node: ast.Call, name: str, position: int) -> ast.expr | None:
    """The expression passed as ``name``, by keyword or at ``position``; ``None`` when the call passes none."""
    for keyword in node.keywords:
        if keyword.arg == name:
            return keyword.value
    return node.args[position] if len(node.args) > position else None


def unnamed_reads(source: str) -> list[int]:
    """The lines of ``read_text``, ``write_text`` and text-mode ``open`` calls that pass no ``encoding`` or pass ``encoding=None``.

    The builtin ``open``, ``builtins.open`` and ``io.open`` take the file first, so their mode and encoding sit one place after those of ``Path.open``.
    """
    found = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        owner = func.value.id if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) else None
        if name in ("read_text", "write_text") and isinstance(func, ast.Attribute):
            encoding = argument(node, "encoding", 0 if name == "read_text" else 1)
        elif name == "open" and owner not in NOT_FILES:
            shift = int(not isinstance(func, ast.Attribute) or owner in FILE_FIRST)
            mode = argument(node, "mode", shift)
            if isinstance(mode, ast.Constant) and isinstance(mode.value, str) and "b" in mode.value:
                continue
            encoding = argument(node, "encoding", shift + 2)
        else:
            continue
        if encoding is None or is_none(encoding):
            found.append(node.lineno)
    return found


class TextReadTest(unittest.TestCase):
    def test_no_text_read_or_write_in_the_quality_ingestion_and_sample_scripts_leaves_the_encoding_to_the_locale(self) -> None:
        scripts = [path for folder in FOLDERS for path in sorted((ROOT / folder).glob("*.py"))]
        for expected in ("scripts/quality/push_hook.py", "scripts/ingestion/record_run.py", "scripts/ingestion/connection.py", "scripts/sample/candidates.py"):
            self.assertIn(ROOT / expected, scripts, "the scan covers the files that read input")
        problems = [f"{path.relative_to(ROOT)}:{line}" for path in scripts for line in unnamed_reads(path.read_text(encoding="utf-8"))]
        self.assertEqual([], problems)

    def test_the_scan_finds_a_read_without_an_encoding_and_passes_one_with_it_and_a_binary_open(self) -> None:
        for source in (
            "path.read_text()",
            "path.write_text('x')",
            "path.open('w')",
            "path.open()",
            "lock(x).open(mode='a')",
            "open('input')",
            "open('input', 'r')",
            "open('input', mode='w')",
            "io.open('input')",
            "path.read_text(encoding=None)",
            "path.write_text('x', encoding=None)",
            "open('input', encoding=None)",
            "path.open('r', -1, None)",
            "open('input', 'r', -1, None)",
            "builtins.open('input', 'r', -1, None)",
            "io.open('input', 'r', -1, None)",
        ):
            with self.subTest(source=source):
                self.assertEqual([1], unnamed_reads(source))
        self.assertEqual([2, 4], unnamed_reads("x = 1\nopen(a)\nok = open(b, encoding='utf-8')\nhandle = open(c)\n"))
        for source in (
            "path.read_text(encoding='utf-8')",
            "path.read_text('utf-8')",
            "path.write_text('x', 'utf-8')",
            "path.read_text(encoding=name)",
            "open('input', encoding='utf-8')",
            "open('input', 'r', -1, 'utf-8')",
            "path.open('r', -1, 'utf-8')",
            "open('input', 'rb')",
            "open('input', mode='wb')",
            "path.open('rb')",
            "path.open(mode='ab')",
            "io.open('input', 'rb')",
            "builtins.open('input', 'r', -1, 'utf-8')",
            "os.open(path, flags)",
            "tarfile.open(name)",
            "gzip.open(path, 'rb')",
        ):
            with self.subTest(source=source):
                self.assertEqual([], unnamed_reads(source))

    def test_the_simulated_locale_reads_a_byte_that_is_not_utf_8_where_an_explicit_encoding_refuses_it(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "input"
            path.write_bytes(b"a\xff")
            with self.assertRaises(UnicodeDecodeError):
                path.read_text(encoding="utf-8")
            with non_utf_8_locale():
                self.assertEqual("a\xff", path.read_text())
                with self.assertRaises(UnicodeDecodeError):
                    path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
