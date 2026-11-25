"""The shape of the one diagnostic line that a sealed job writes to standard error on ``internal_error``, and the checks that tests share."""

from __future__ import annotations

import re
import unittest

CLASS = r"[A-Za-z0-9_.<>]+"
FRAME = r"[A-Za-z0-9_.<>-]+:\d+:[A-Za-z0-9_.<>]+"
BODY = re.compile(rf"^internal_error class=(?P<classes>{CLASS}(?: <- {CLASS}){{0,4}}) frames=(?P<frames>{FRAME}(?: \| {FRAME}){{0,39}})?$")


def split_lines(err: str, job: str) -> tuple[list[str], list[str]]:
    """The lines of standard error that are not the diagnostic line, and the bodies of the diagnostic lines (without the job prefix)."""
    prefix = f"{job}: internal_error class="
    rest = [line for line in err.splitlines() if not line.startswith(prefix)]
    found = [line[len(job) + 2 :] for line in err.splitlines() if line.startswith(prefix)]
    return rest, found


def assert_internal(case: unittest.TestCase, err: str, job: str, code_line: str | None, classes: tuple[str, ...], marker: str) -> list[str]:
    """Standard error is the code line (when given) and exactly one diagnostic line of the right shape that names ``classes`` first, with no ``marker``. Gives
    the frames."""
    rest, found = split_lines(err, job)
    case.assertEqual([] if code_line is None else [code_line], rest)
    case.assertEqual(1, len(found), err)
    match = BODY.match(found[0]) or case.fail(found[0])
    case.assertEqual(list(classes), match["classes"].split(" <- ")[: len(classes)])
    case.assertNotIn(marker, err)
    return (match["frames"] or "").split(" | ")
