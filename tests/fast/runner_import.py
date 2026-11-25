"""The one way that a test imports an ignored runner of ``tests/e2e``.

An ignored runner holds the held-out rule's names and its own source is attached to every record that it writes. A runner that does not parse, or that fails
while it is imported, would otherwise print the line of its source or the message of its error (and so a figure that the line holds) into the output of the
test run. ``safe_import`` names the file label, the line of a syntax error or the class of any other error, with a fixed message and no cause, and
``tests/fast/test_runner_import.py`` holds every test module to it.
"""

from __future__ import annotations

import importlib
import importlib.util
import unittest
from pathlib import Path
from types import ModuleType

E2E = Path(__file__).resolve().parents[1] / "e2e"
ABSENT = "the untracked runners are not on this machine"


class RunnerImportFailed(Exception):
    """A runner that could not be imported. Its text names a file label, a line or a class, and never the message, the source or the context of the error."""


def label_of(error: SyntaxError, label: str) -> str:
    """The file name that a syntax error names (the base name only, as a nested import may name another file), or the label of the runner."""
    return Path(error.filename).name if error.filename else label


def safe_failure(error: Exception, label: str) -> RunnerImportFailed:
    """The failure that stands for an error of an import: the file and the line of a syntax error, or the class of any other error."""
    if isinstance(error, SyntaxError):
        return RunnerImportFailed(f"{label_of(error, label)}: the runner does not parse (line {error.lineno})")
    return RunnerImportFailed(f"{label}: the runner import failed with {type(error).__name__}")


def safe_import(module_name: str, label: str) -> ModuleType:
    """The module, or a ``RunnerImportFailed`` that names the label and the line or the class of the error and nothing else (``from None``: no cause and no
    context, so no traceback of the source or the message)."""
    try:
        return importlib.import_module(module_name)
    except unittest.SkipTest:
        raise
    except Exception as error:
        raise safe_failure(error, label) from None


def safe_execute(path: Path, module_name: str) -> ModuleType:
    """A new module made from the file at ``path`` and executed, with the same failures as ``safe_import``."""
    label = path.name
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(label)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except unittest.SkipTest:
        raise
    except Exception as error:
        raise safe_failure(error, label) from None
    return module


def present(name: str) -> bool:
    return (E2E / f"{name}.py").is_file()


def import_runner(name: str) -> ModuleType:
    """The ignored runner ``tests/e2e/<name>.py``. A test that needs it is skipped when it is not on this machine."""
    if not present(name):
        raise unittest.SkipTest(ABSENT)
    return safe_import(f"tests.e2e.{name}", f"{name}.py")
