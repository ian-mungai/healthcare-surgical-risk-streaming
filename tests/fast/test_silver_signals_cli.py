"""The command line of the Silver signals job prints fixed codes and never an argument, a branch name or the text of an upstream error.

Failure modes covered here:

- An extra or a malformed argument, or a branch name that Nessie would refuse, is echoed by the argument parser.
- A failed check, a missing branch, a settings error or any other error carries text of its own into the output.
- A failed check prints no line, or prints a line that is not a code and a count, or the exit code does not tell a failed check from a bad call.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from argparse import Namespace
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run

MARKER = "MARKER_8731"


def invoke(argv: list[str], execute: Any = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(argv, execute or (lambda arguments: {"mode": arguments.mode, "branch": arguments.branch}))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def raising(error: Exception) -> Any:
    def execute(_arguments: Namespace) -> dict[str, Any]:
        raise error

    return execute


class CommandLine(unittest.TestCase):
    def test_a_good_call_prints_one_result_line_and_nothing_else(self) -> None:
        for argv in (["build", "--branch", "b"], ["build", "--branch", "b", "--base", "main"], ["verify", "--branch", "b"]):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((0, ""), (code, err))
                self.assertEqual({"mode": argv[0], "branch": "b"}, json.loads(out.removeprefix("RESULT ")))

    def test_a_bad_call_prints_the_code_usage_and_does_not_echo_the_argument(self) -> None:
        for argv in (
            [],
            ["build"],
            ["clean", "--branch", "b"],
            ["build", "--branch", MARKER + " bad"],
            ["build", "--branch", "b", f"--{MARKER}"],
            ["verify", "--branch", "b", "--base", MARKER],
            ["verify", "--branch", "b", "--pinned-release"],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))

    def test_a_failed_check_prints_each_problem_as_a_code_and_a_count_and_exits_1(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SilverSignalsError("numeric_verdict_differs: 3 rows", "waveform_shape: 1 row")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("silver_signals: numeric_verdict_differs: 3 rows\nsilver_signals: waveform_shape: 1 row\n", err)

    def test_an_error_of_nessie_is_a_code_and_never_its_text(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        for error, expected in ((missing, "branch_missing"), (other, "catalog_error"), (NessieError(MARKER, status=404, error_code="OTHER"), "catalog_error")):
            with self.subTest(expected=expected):
                code, out, err = invoke(["verify", "--branch", "b"], raising(error))
                self.assertEqual((1, "", f"silver_signals: {expected}\n"), (code, out, err))

    def test_a_setting_that_cannot_be_used_and_any_other_error_are_codes(self) -> None:
        code, out, err = invoke(["verify", "--branch", "b"], raising(SettingsError(MARKER)))
        self.assertEqual((2, "", "silver_signals: settings\n"), (code, out, err))
        code, out, err = invoke(["verify", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, "", "silver_signals: internal_error\n"), (code, out, err))


if __name__ == "__main__":
    unittest.main()
