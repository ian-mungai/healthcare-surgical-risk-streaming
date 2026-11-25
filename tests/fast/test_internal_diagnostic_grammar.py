"""The one grammar of the diagnostic line, shared by the producer and the readers (``is_diagnostic`` and ``accepted_diagnostics``).

Failure modes covered here:

- A reader accepts a line that the producer cannot print: a class of 121 characters, a class that is not on the lists, a path or a hyphenated file name in a
  frame, 6 classes, 41 frames, two ``other`` frames side by side, any text after the line. Such a line may hold a value, so it must count as unknown.
- A reader rejects a line that the producer prints (the hyphenated name that the old filter counted as unknown): every line of a wide set of errors is
  accepted, including the errors with names that carry a hyphen, a dot, a space, a newline or 500 characters.
- A diagnostic is accepted on any exit code or at any place: it is a diagnostic only on exit 70, only as the one line directly after the fixed
  ``internal_error`` line (bare, or with the flag of the sealed jobs), and a second one is not.
"""

from __future__ import annotations

import unittest
from typing import Any

from spark.internal_diagnostic import CLASS_LIMIT, FRAME_LIMIT, UNAVAILABLE_LINE, accepted_diagnostics, internal_error_line, is_diagnostic
from tests.fast.test_internal_diagnostic import MARKER, caught, from_file, nested, outer

OWN = "internal_error class=builtins.KeyError frames=other | cli.py:63:run | q.py:9:go"


class Accepted(unittest.TestCase):
    def test_a_line_of_the_producer_is_accepted(self) -> None:
        for line in (
            OWN,
            "internal_error class=builtins.RuntimeError frames=",
            "internal_error class=other frames=other",
            "internal_error class=scripts.stack.nessie.NessieError <- builtins.OSError <- other frames=run.py:1:<module> | a.py:2:<lambda> | b.py:3:<listcomp>",
            UNAVAILABLE_LINE,
            "internal_error class=" + " <- ".join(["builtins.KeyError"] * CLASS_LIMIT) + " frames=" + " | ".join(f"a.py:{n}:f" for n in range(FRAME_LIMIT)),
            "internal_error class=builtins.KeyError frames=other | a.py:1:other | other",  # a function that is called "other" is a frame, not the word
        ):
            with self.subTest(line=line[:80]):
                self.assertTrue(is_diagnostic(line))

    def test_every_line_of_a_wide_set_of_errors_is_accepted(self) -> None:
        odd = ("A-b", "has space", "new\nline", "C" * 500, "é", "")
        errors: list[BaseException] = [caught(outer), caught(lambda: nested(10)), RuntimeError(MARKER), KeyboardInterrupt(), ExceptionGroup("g", [OSError()])]
        errors += [type(name, (Exception,), {"__module__": module})(MARKER) for name in odd for module in ("m", "m-n", "m n", "")]
        errors += [from_file(f"/x/{name}-y.py", "def entry():\n    raise KeyError\n") for name in ("spark", "a b")]
        for error in errors:
            line = internal_error_line(error)
            with self.subTest(line=line[:80]):
                self.assertTrue(is_diagnostic(line))


class Refused(unittest.TestCase):
    def test_a_line_that_the_producer_cannot_print_is_refused(self) -> None:
        many_frames = " | ".join(f"a.py:{n}:f" for n in range(FRAME_LIMIT + 1))
        many_classes = " <- ".join(["builtins.KeyError"] * (CLASS_LIMIT + 1))
        for line in (
            "internal_error class=" + "C" * 121 + " frames=",  # a class of 121 characters
            "internal_error class=builtins.NotAClass frames=",  # not on the lists
            "internal_error class=tests.fast.x.Secretive frames=",
            f"internal_error class=spark.settings.{MARKER} frames=",
            "internal_error class=builtins.KeyError frames=/x/a.py:1:f",  # a path
            "internal_error class=builtins.KeyError frames=a-b.py:1:f",  # a hyphen: the project has no such file
            "internal_error class=builtins.KeyError frames=a.py:1:f-g",
            "internal_error class=builtins.KeyError frames=a.py:12345678:f",
            "internal_error class=builtins.KeyError frames=a.py:x:f",
            "internal_error class=builtins.KeyError frames=a.py:1:f text",
            "internal_error class=builtins.KeyError frames=a.py:1:f | ",
            "internal_error class=builtins.KeyError frames=other | other",  # a run of other is one
            "internal_error class=builtins.KeyError frames=a.py:1:f | other | other | b.py:2:g",
            f"internal_error class={many_classes} frames=",
            f"internal_error class=builtins.KeyError frames={many_frames}",
            "internal_error class=builtins.KeyError frames=a.py:1:" + "f" * 101,
            "internal_error class= frames=",
            "internal_error class=builtins.KeyError",
            "internal_error class=builtins.KeyError frames=unavailable",
            "internal_error class=unavailable frames=",
            "internal_error class=unavailable frames=unavailable ",
            OWN + "\n",
            OWN + " text",
            " " + OWN,
            "internal_error",
            "internal_error: failed",
            "",
        ):
            with self.subTest(line=line[:80]):
                self.assertFalse(is_diagnostic(line))


class Places(unittest.TestCase):
    CODE = "internal_error"

    def test_a_diagnostic_is_one_only_on_exit_70(self) -> None:
        for code in (0, 1, 2, 69, 71, 137):
            with self.subTest(code=code):
                self.assertEqual(frozenset(), accepted_diagnostics(["branch_missing", OWN], code))
                self.assertEqual(frozenset(), accepted_diagnostics([self.CODE, OWN], code))
        self.assertEqual(frozenset({1}), accepted_diagnostics([self.CODE, OWN], 70))

    def test_a_diagnostic_is_one_only_directly_after_the_fixed_code_line(self) -> None:
        for bodies in (
            [OWN],
            ["usage", OWN],
            ["branch_missing", OWN],
            [self.CODE, "settings", OWN],
            [OWN, self.CODE],
            ["internal_error: 7 rows", OWN],
            ["internal_error class=builtins.KeyError frames=", OWN],
        ):
            with self.subTest(bodies=bodies):
                self.assertEqual(frozenset(), accepted_diagnostics(bodies, 70))

    def test_the_code_line_may_have_the_flag_of_the_sealed_jobs(self) -> None:
        self.assertEqual(frozenset({1}), accepted_diagnostics(["internal_error: failed", OWN], 70))

    def test_a_second_diagnostic_line_is_not_one_and_a_second_code_with_its_own_is(self) -> None:
        self.assertEqual(frozenset({1}), accepted_diagnostics([self.CODE, OWN, OWN], 70))
        self.assertEqual(frozenset({1, 3}), accepted_diagnostics([self.CODE, OWN, self.CODE, OWN], 70))

    def test_a_line_of_another_form_after_the_code_is_not_one(self) -> None:
        for text in ("internal_error class=builtins.KeyError frames=/x/a.py:1:f", "internal_error class=" + "C" * 121 + " frames=", "message"):
            with self.subTest(text=text[:40]):
                self.assertEqual(frozenset(), accepted_diagnostics([self.CODE, text], 70))


class Shared(unittest.TestCase):
    def test_a_reader_has_the_check_of_the_producer_and_no_pattern_of_its_own(self) -> None:
        from spark import internal_diagnostic
        from tests.fast.test_runner_gold_sealed import runner

        for name in ("gold_dimensions", "signal_profile"):
            module: Any = runner(name)
            with self.subTest(runner=name):
                self.assertIs(internal_diagnostic.accepted_diagnostics, module.accepted_diagnostics)
                self.assertFalse(hasattr(module, "DIAGNOSTIC_LINE"))


if __name__ == "__main__":
    unittest.main()
