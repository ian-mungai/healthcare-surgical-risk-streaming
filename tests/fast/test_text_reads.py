"""Every text read of an input file in the quality and ingestion scripts names its encoding, so the machine's locale cannot change what a file says.

A read that names none uses the locale's encoding. Under ``en_US.ISO8859-1`` with UTF-8 mode off, a file with a ``0xff`` byte is then read as ``ÿ`` and accepted
or echoed, where the fixed line for an unreadable file is the answer. Join round 3, finding 2 and its class.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path

from tests.fast.locale_support import non_utf_8_locale

ROOT = Path(__file__).resolve().parents[2]
FOLDERS = ("scripts/quality", "scripts/ingestion")
NOT_FILES = {"os", "tarfile", "gzip", "io"}


def unnamed_reads(source: str) -> list[int]:
    """The lines of ``read_text``, ``write_text`` and text-mode ``open`` calls that pass no ``encoding``."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)) or any(keyword.arg == "encoding" for keyword in node.keywords):
            continue
        name = node.func.attr
        if name in ("read_text", "write_text"):
            found.append(node.lineno)
        elif name == "open" and not (isinstance(node.func.value, ast.Name) and node.func.value.id in NOT_FILES):
            mode = node.args[0] if node.args else next((keyword.value for keyword in node.keywords if keyword.arg == "mode"), None)
            if not (isinstance(mode, ast.Constant) and isinstance(mode.value, str) and "b" in mode.value):
                found.append(node.lineno)
    return found


class TextReadTest(unittest.TestCase):
    def test_no_text_read_or_write_in_the_quality_and_ingestion_scripts_leaves_the_encoding_to_the_locale(self) -> None:
        scripts = [path for folder in FOLDERS for path in sorted((ROOT / folder).glob("*.py"))]
        for expected in ("scripts/quality/push_hook.py", "scripts/ingestion/record_run.py", "scripts/ingestion/connection.py"):
            self.assertIn(ROOT / expected, scripts, "the scan covers the files that read input")
        problems = [f"{path.relative_to(ROOT)}:{line}" for path in scripts for line in unnamed_reads(path.read_text(encoding="utf-8"))]
        self.assertEqual([], problems)

    def test_the_scan_finds_a_read_without_an_encoding_and_passes_one_with_it_and_a_binary_open(self) -> None:
        self.assertEqual([1], unnamed_reads("path.read_text()"))
        self.assertEqual([1], unnamed_reads("path.write_text('x')"))
        self.assertEqual([1], unnamed_reads("path.open('w')"))
        self.assertEqual([1], unnamed_reads("path.open()"))
        self.assertEqual([1], unnamed_reads("lock(x).open(mode='a')"))
        self.assertEqual([], unnamed_reads("path.read_text(encoding='utf-8')"))
        self.assertEqual([], unnamed_reads("path.open('rb')\nos.open(path, flags)\ntarfile.open(name)\ngzip.open(path, 'rb')"))

    def test_the_simulated_locale_reads_a_byte_that_is_not_utf_8_where_an_explicit_encoding_refuses_it(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "input"
            path.write_bytes(b"a\xff")
            with self.assertRaises(UnicodeDecodeError):
                path.read_text()
            with non_utf_8_locale():
                self.assertEqual("a\xff", path.read_text())
                with self.assertRaises(UnicodeDecodeError):
                    path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
