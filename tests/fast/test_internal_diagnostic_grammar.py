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
from tests.fast.test_internal_diagnostic import ENTRY, FOREIGN, INNER, MARKER, ROOT, TRACKED, caught, from_file, nested, outer

OTHER_FILE = "spark/profile/checks.py"
NAMED = f"{TRACKED}:63:{ENTRY}"
NAMED_TWO = f"{TRACKED}:9:{INNER}"
OWN = f"internal_error class=builtins.KeyError frames=other | {NAMED} | {NAMED_TWO}"


class Accepted(unittest.TestCase):
    def test_a_line_of_the_producer_is_accepted(self) -> None:
        for line in (
            OWN,
            "internal_error class=builtins.RuntimeError frames=",
            "internal_error class=other frames=other",
            "internal_error class=scripts.stack.nessie.NessieError <- builtins.OSError <- other"
            f" frames={TRACKED}:1:<module> | {TRACKED}:2:<lambda> | {OTHER_FILE}:3:<listcomp>",
            f"internal_error class=builtins.KeyError frames={TRACKED}:0:<genexpr> | {TRACKED}:9999999:<dictcomp> | {TRACKED}:7:<setcomp>",
            UNAVAILABLE_LINE,
            "internal_error class="
            + " <- ".join(["builtins.KeyError"] * CLASS_LIMIT)
            + " frames="
            + " | ".join(f"{TRACKED}:{n}:{ENTRY}" for n in range(FRAME_LIMIT)),
        ):
            with self.subTest(line=line[:80]):
                self.assertTrue(is_diagnostic(line))

    def test_every_line_of_a_wide_set_of_errors_is_accepted(self) -> None:
        odd = ("A-b", "has space", "new\nline", "C" * 500, "é", "")
        errors: list[BaseException] = [caught(outer), caught(lambda: nested(10)), RuntimeError(MARKER), KeyboardInterrupt(), ExceptionGroup("g", [OSError()])]
        errors += [type(name, (Exception,), {"__module__": module})(MARKER) for name in odd for module in ("m", "m-n", "m n", "")]
        source = f"def {ENTRY}():\n    raise KeyError\n"
        errors += [from_file(f"/x/{name}-y.py", source) for name in ("spark", "a b")]
        errors += [from_file(str(ROOT / TRACKED), source), from_file(f"{ROOT}/spark/../{TRACKED}", source)]
        errors += [from_file(str(ROOT / "spark" / f"{name}.py"), source) for name in odd]
        for error in errors:
            line = internal_error_line(error)
            with self.subTest(line=line[:80]):
                self.assertTrue(is_diagnostic(line))


class Refused(unittest.TestCase):
    def test_a_line_that_the_producer_cannot_print_is_refused(self) -> None:
        many_frames = " | ".join(f"{TRACKED}:{n}:{ENTRY}" for n in range(FRAME_LIMIT + 1))
        many_classes = " <- ".join(["builtins.KeyError"] * (CLASS_LIMIT + 1))
        for line in (
            "internal_error class=" + "C" * 121 + " frames=",  # a class of 121 characters
            "internal_error class=builtins.NotAClass frames=",  # not on the lists
            "internal_error class=tests.fast.x.Secretive frames=",
            f"internal_error class=spark.settings.{MARKER} frames=",
            f"internal_error class=builtins.KeyError frames=/x/{TRACKED}:1:{ENTRY}",  # a path outside the project
            f"internal_error class=builtins.KeyError frames={ROOT}/{TRACKED}:1:{ENTRY}",  # a full path
            f"internal_error class=builtins.KeyError frames=result_file.py:1:{ENTRY}",  # the old form: a base name
            f"internal_error class=builtins.KeyError frames=spark/{MARKER}.py:1:{ENTRY}",  # a file that the inventory does not hold
            f"internal_error class=builtins.KeyError frames=spark/result_file-x.py:1:{ENTRY}",
            f"internal_error class=builtins.KeyError frames=spark/../{TRACKED}:1:{ENTRY}",  # a path that is not in its one form
            f"internal_error class=builtins.KeyError frames=spark//result_file.py:1:{ENTRY}",
            f"internal_error class=builtins.KeyError frames=./{TRACKED}:1:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:{MARKER}",  # a function that the file does not define
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:{ENTRY}-g",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:{FOREIGN}",  # a function of another file
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:<string>",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:{ENTRY} text",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:{ENTRY} | ",
            f"internal_error class=builtins.KeyError frames={TRACKED}:01:{ENTRY}",  # a number with a leading zero
            f"internal_error class=builtins.KeyError frames={TRACKED}:00:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:0000001:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:+1:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1_0:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:\u0661:{ENTRY}",  # a digit of another script
            f"internal_error class=builtins.KeyError frames={TRACKED}:10000000:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:12345678:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:x:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:-1:{ENTRY}",
            f"internal_error class=builtins.KeyError frames={TRACKED}::{ENTRY}",
            "internal_error class=builtins.KeyError frames=other | other",  # a run of other is one
            f"internal_error class=builtins.KeyError frames={NAMED} | other | other | {NAMED_TWO}",
            f"internal_error class={many_classes} frames=",
            f"internal_error class=builtins.KeyError frames={many_frames}",
            f"internal_error class=builtins.KeyError frames={TRACKED}:1:" + "f" * 101,
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

    def test_a_frame_whose_function_is_called_other_is_a_name_only_when_the_file_defines_it(self) -> None:
        self.assertFalse(is_diagnostic(f"internal_error class=builtins.KeyError frames=other | {TRACKED}:1:other | other"))
        self.assertFalse(is_diagnostic(f"internal_error class=builtins.KeyError frames={TRACKED}:1:other"))


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

    def test_a_second_diagnostic_line_is_not_one_and_a_second_pair_makes_every_diagnostic_unknown(self) -> None:
        self.assertEqual(frozenset({1}), accepted_diagnostics([self.CODE, OWN, OWN], 70))
        for bodies in (
            [self.CODE, OWN, self.CODE, OWN],
            ["internal_error: failed", OWN, "internal_error: failed", OWN],
            [self.CODE, OWN, "internal_error: failed", OWN],
            [self.CODE, OWN, OWN, self.CODE, OWN],
            [self.CODE, OWN, "settings", self.CODE, OWN],
        ):
            with self.subTest(bodies=bodies):
                self.assertEqual(frozenset(), accepted_diagnostics(bodies, 70))
        self.assertEqual(frozenset({3}), accepted_diagnostics([self.CODE, "message", self.CODE, OWN], 70))

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
