"""Run the quick test group, ``tests/fast``, the way the commit hook and ``make test-fast`` do, and refuse any import of the slow group.

Usage, from the repository root: ``.venv/bin/python -m tests.run_quick``. The output and exit code are those of ``python -m unittest discover``.

While the quick group loads and runs, a module finder placed first in ``sys.meta_path`` refuses every module named ``tests.slow`` or under it,
however the import is written. A refused import raises ``ImportError`` (never ``ModuleNotFoundError``, which ``from tests import slow`` would
swallow), so the test module fails to load or the test errors, and the run fails with the refused name.
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


def main() -> None:
    if str(TOP) not in sys.path:
        sys.path.insert(0, str(TOP))
    sys.meta_path.insert(0, RefuseSlowGroup())
    unittest.main(module=None, argv=["python -m unittest", "discover", "--start-directory", str(TOP / "tests" / "fast"), "--top-level-directory", str(TOP)])


if __name__ == "__main__":
    main()
