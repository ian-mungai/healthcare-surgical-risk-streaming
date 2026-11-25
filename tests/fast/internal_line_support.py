"""The shape of the one diagnostic line that a sealed job writes to standard error on ``internal_error``, and the checks that tests share."""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

from spark import internal_diagnostic
from spark.internal_diagnostic import is_diagnostic

OWN_FILE = str(Path(internal_diagnostic.__file__).parent / "fake_job.py")  # a file of a package of the project that does not exist


def split_lines(err: str, job: str) -> tuple[list[str], list[str]]:
    """The lines of standard error that are not the diagnostic line, and the bodies of the diagnostic lines (without the job prefix)."""
    prefix = f"{job}: internal_error class="
    rest = [line for line in err.splitlines() if not line.startswith(prefix)]
    found = [line[len(job) + 2 :] for line in err.splitlines() if line.startswith(prefix)]
    return rest, found


def assert_internal(case: unittest.TestCase, err: str, job: str, code_line: str | None, classes: tuple[str, ...], marker: str) -> list[str]:
    """Standard error is the code line (when given) and exactly one diagnostic line of the one shape of the producer (``is_diagnostic``) that names ``classes``
    first, with no ``marker``. Gives the frames."""
    rest, found = split_lines(err, job)
    case.assertEqual([] if code_line is None else [code_line], rest)
    case.assertEqual(1, len(found), err)
    case.assertTrue(is_diagnostic(found[0]), found[0])
    head, _, tail = found[0].partition(" frames=")
    case.assertEqual(list(classes), head.partition("class=")[2].split(" <- ")[: len(classes)])
    case.assertNotIn(marker, err)
    return [frame for frame in tail.split(" | ") if frame]


class RaisingLoader:
    """A source loader that raises when the source of a file is asked for (``linecache`` asks it for every frame that ``traceback`` prints)."""

    def get_source(self, name: str) -> str:
        raise RuntimeError("the loader failed")


def error_with_raising_loader() -> KeyError:
    """An error that was raised in a frame of a project file whose module has a source loader that raises."""
    scope: dict[str, Any] = {"__name__": "fake_job", "__loader__": RaisingLoader()}
    exec(compile("def entry():\n    raise KeyError\n", OWN_FILE, "exec"), scope)  # noqa: S102 (a stand-in for a module of the project)
    try:
        scope["entry"]()
    except KeyError as error:
        return error
    raise AssertionError("no error")
