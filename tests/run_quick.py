"""Run the quick test group, ``tests/fast``, the way the commit hook and ``make test-fast`` do, and refuse any import of the slow group.

Usage, from the repository root: ``.venv/bin/python -P tests/run_quick.py``, as a script, so that no package of the project loads before the
finder is in place; ``-P`` keeps this file's folder off the path, so no file beside it can shadow a standard module.
The output is that of ``python -m unittest discover``, and a run that finds no test exits 5, as unittest does.

While the quick group loads and runs, a module finder placed first in ``sys.meta_path`` refuses every module named ``tests.slow`` or under it,
however the import is written. A refused import raises ``ImportError`` (never ``ModuleNotFoundError``, which ``from tests import slow`` would
swallow), so the test module fails to load or the test errors, and the run fails with the refused name. After the run, a module loaded from a
file under ``tests/slow`` by any other name (for example by a discovery of that folder by path) also fails the run, naming the file.
Limit: a quick test that removes the finder from ``sys.meta_path`` or catches its ``ImportError`` is deliberate evasion and is not guarded.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import sys
import unittest
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType

TOP = Path(__file__).resolve().parents[1]
SLOW = "tests.slow"


class RefuseSlowGroup(importlib.abc.MetaPathFinder):
    """Refuses ``tests.slow`` and every module under it."""

    def find_spec(self, fullname: str, path: Sequence[str] | None, target: ModuleType | None = None) -> importlib.machinery.ModuleSpec | None:
        if fullname == SLOW or fullname.startswith(SLOW + "."):
            raise ImportError(f"the quick group imported {fullname}; tests under tests/slow run only with make test-slow", name=fullname)
        return None


def slow_files_loaded() -> list[str]:
    """The files under ``tests/slow`` of every loaded module, whatever name it was loaded under."""
    slow = TOP / "tests" / "slow"
    found = {Path(str(getattr(module, "__file__", None) or "")).resolve() for module in list(sys.modules.values())}
    return sorted(str(path.relative_to(TOP)) for path in found if path.is_relative_to(slow))


def main() -> int:
    # This checkout's root comes first, whatever PYTHONPATH names: another checkout's tests package must never stand in for this one's.
    # The script's own folder (on the path when run without -P) is dropped.
    here = Path(__file__).resolve().parent
    sys.path[:] = [str(TOP), *(entry for entry in sys.path if not entry or Path(entry).resolve() not in (TOP, here))]
    sys.meta_path.insert(0, RefuseSlowGroup())
    argv = ["python -m unittest", "discover", "--start-directory", str(TOP / "tests" / "fast"), "--top-level-directory", str(TOP)]
    result = unittest.main(module=None, argv=argv, exit=False).result
    loaded = slow_files_loaded()
    for path in loaded:
        sys.stderr.write(f"the quick group loaded {path}; tests under tests/slow run only with make test-slow\n")
    if not result.wasSuccessful() or loaded:
        return 1
    if result.testsRun == 0 and not result.skipped:
        return 5  # unittest's own exit code for a run that found no test
    return 0


if __name__ == "__main__":
    sys.exit(main())
