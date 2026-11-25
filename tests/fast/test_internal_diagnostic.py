"""The line that a sealed job writes on ``internal_error`` names the classes and the code frames of the error and nothing else.

Failure modes covered here:

- Every sealed job ended an unexpected error as the bare code, so a run of 30 to 80 minutes that ended as ``internal_error`` could not be told from any other,
  and each diagnosis took a patch and a rerun. The line names the class chain and the frames, so the place of the failure is known from the first run.
- The line carries a value: the message, the arguments, the ``repr`` or a local variable of a frame can hold a value of the test set, and the line goes to a
  stream that the operator keeps. Only module-qualified class names and ``file-name:line:function`` frames are written, and the characters of a name are
  limited, so that no other text can pass as one.
- The chain is cut or unbounded: the cause is followed before the context, a cycle ends, and the chain and the frames have a limit (5 classes, 40 frames,
  the innermost frames kept and the innermost last).
- A call site that maps an unexpected error to ``internal_error`` writes only the code (the result-file ending, the failed result write, the Silver ending,
  the Bronze lab ending, the reconcile ending and its failed write, the signal profile, the gold dimensions job and the table maintenance job).
"""

from __future__ import annotations

import re
import unittest
from collections.abc import Callable

from spark.internal_diagnostic import CLASS_LIMIT, FRAME_LIMIT, NAME_LIMIT, internal_error_line
from tests.fast.internal_line_support import BODY, assert_internal

MARKER = "MARKER_4417"


class Secretive(Exception):
    """An error whose every text form holds the marker."""

    def __init__(self) -> None:
        super().__init__(MARKER, {"row": MARKER})
        self.payload = MARKER

    def __str__(self) -> str:
        return MARKER

    def __repr__(self) -> str:
        return f"Secretive({MARKER})"


def innermost(local: str) -> None:
    held = local + MARKER  # noqa: F841 (a local variable that holds the marker)
    raise Secretive


def middle() -> None:
    innermost(MARKER)


def outer() -> None:
    middle()


def caught(raiser: Callable[[], object]) -> BaseException:
    try:
        raiser()
    except BaseException as error:
        return error
    raise AssertionError("no error")


def nested(depth: int) -> None:
    if depth == 0:
        raise ValueError(MARKER)
    try:
        nested(depth - 1)
    except Exception as error:
        raise (KeyError if depth % 2 else ValueError)(MARKER) from error


def recurse(depth: int) -> None:
    if depth == 0:
        raise RuntimeError(MARKER)
    recurse(depth - 1)


class TheLine(unittest.TestCase):
    def test_it_holds_the_class_and_the_frames_and_no_text_of_the_error(self) -> None:
        line = internal_error_line(caught(outer))
        self.assertIsNotNone(BODY.match(line), line)
        self.assertNotIn(MARKER, line)
        self.assertEqual(1, len(line.splitlines()))
        self.assertTrue(line.startswith("internal_error class=tests.fast.test_internal_diagnostic.Secretive frames="), line)
        frames = line.partition(" frames=")[2].split(" | ")
        names = [frame.split(":")[2] for frame in frames]
        self.assertEqual(["caught", "outer", "middle", "innermost"], names)
        self.assertTrue(all(frame.startswith("test_internal_diagnostic.py:") for frame in frames), frames)
        self.assertNotIn("/", line)

    def test_a_cause_is_followed_before_a_context_and_the_top_error_comes_first(self) -> None:
        def raiser() -> None:
            try:
                try:
                    raise ValueError(MARKER)
                except ValueError:
                    raise KeyError(MARKER) from OSError(MARKER)  # the cause wins over the context (the ValueError)
            except KeyError:
                raise RuntimeError(MARKER)  # noqa: B904 (the implicit context is the KeyError)

        classes = internal_error_line(caught(raiser)).partition(" frames=")[0].partition("class=")[2]
        self.assertEqual("builtins.RuntimeError <- builtins.KeyError <- builtins.OSError", classes)

    def test_a_chain_is_cut_at_five_classes(self) -> None:
        line = internal_error_line(caught(lambda: nested(10)))
        classes = line.partition(" frames=")[0].partition("class=")[2].split(" <- ")
        self.assertEqual(CLASS_LIMIT, len(classes))
        self.assertEqual(["builtins.ValueError", "builtins.KeyError", "builtins.ValueError", "builtins.KeyError", "builtins.ValueError"], classes)

    def test_a_chain_that_loops_ends(self) -> None:
        first, second = ValueError(MARKER), KeyError(MARKER)
        first.__context__, second.__context__ = second, first
        line = internal_error_line(first)
        self.assertEqual("internal_error class=builtins.ValueError <- builtins.KeyError frames=", line)

    def test_the_frames_are_cut_at_forty_with_the_innermost_last(self) -> None:
        line = internal_error_line(caught(lambda: recurse(200)))
        frames = line.partition(" frames=")[2].split(" | ")
        self.assertEqual(FRAME_LIMIT, len(frames))
        self.assertTrue(frames[-1].endswith(":recurse"), frames[-1])
        self.assertNotIn("caught", [frame.split(":")[2] for frame in frames])

    def test_an_error_that_was_never_raised_has_a_class_and_no_frames(self) -> None:
        self.assertEqual("internal_error class=builtins.RuntimeError frames=", internal_error_line(RuntimeError(MARKER)))

    def test_a_name_holds_only_name_characters(self) -> None:
        odd = type("Bad name;\n" + MARKER, (Exception,), {"__module__": "mod ule/x"})
        line = internal_error_line(odd())
        self.assertEqual(1, len(line.splitlines()))
        self.assertIsNotNone(re.match(rf"^internal_error class={CLASS_CHARACTERS}+ frames=$", line), line)

    def test_a_name_is_cut_at_its_limit(self) -> None:
        long = type("C" * 500, (Exception,), {})
        classes = internal_error_line(long()).partition(" frames=")[0].partition("class=")[2]
        self.assertEqual(NAME_LIMIT, len(classes))

    def test_the_helper_check_of_the_tests_catches_a_message(self) -> None:
        with self.assertRaises(AssertionError):
            assert_internal(self, f"job: internal_error class=builtins.RuntimeError frames=\n{MARKER}\n", "job", None, ("builtins.RuntimeError",), MARKER)


CLASS_CHARACTERS = r"[A-Za-z0-9_.<>]"


if __name__ == "__main__":
    unittest.main()
