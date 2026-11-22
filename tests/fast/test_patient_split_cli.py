"""The command line of the patient split job prints fixed codes and never an argument, a branch name or the text of an upstream error.

Failure modes covered here:

- An extra or a malformed argument is echoed by the argument parser.
- A missing branch is reported through Nessie's own error, which names the branch and the server's text.
- Any other upstream error, a settings error or a check failure carries text that reaches the output.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from argparse import Namespace
from typing import Any

from scripts.stack.nessie import NessieConflict, NessieError
from spark.settings import SettingsError
from spark.split_cli import run
from spark.split_table import PatientSplitError

MARKER = "MARKER_8731"


def invoke(argv: list[str], execute: Any = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(argv, execute or (lambda arguments: {"mode": arguments.mode}))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def raising(error: Exception) -> Any:
    def execute(_arguments: Namespace) -> dict[str, Any]:
        raise error

    return execute


class CommandLineTest(unittest.TestCase):
    def test_a_good_call_prints_one_result_line(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"])
        self.assertEqual(
            (
                0,
                "",
            ),
            (code, err),
        )
        self.assertEqual({"mode": "build"}, json.loads(out.removeprefix("RESULT ")))

    def test_a_marker_in_any_bad_argument_reaches_no_output(self) -> None:
        for argv in (
            ["build", "--branch", "b", f"--{MARKER}"],
            ["build", "--branch", "b", MARKER],
            ["build", f"--branch={MARKER}", "--base"],
            [MARKER],
            ["verify", "--branch", "b", "--fixed-sample"],
            ["verify", "--branch", "b", "--base", MARKER],
            [],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual(2, code)
                self.assertEqual(("", "patient_split: usage\n"), (out, err))

    def test_a_missing_branch_is_a_code_and_not_the_error_from_nessie(self) -> None:
        error = NessieError(f"GET /trees/{MARKER} was refused: {MARKER}", status=404, error_code="REFERENCE_NOT_FOUND")
        code, out, err = invoke(["verify", "--branch", MARKER], raising(error))
        self.assertEqual((1, "", "patient_split: branch_missing\n"), (code, out, err))

    def test_any_other_upstream_error_is_a_code_and_not_its_text(self) -> None:
        for error in (NessieError(f"failed {MARKER}", status=500), NessieError(f"timeout {MARKER}"), NessieConflict(f"exists {MARKER}", status=409)):
            with self.subTest(error=type(error).__name__, status=error.status):
                code, out, err = invoke(["build", "--branch", MARKER], raising(error))
                self.assertEqual((1, "", "patient_split: catalog_error\n"), (code, out, err))

    def test_a_settings_error_is_a_code_and_not_its_text(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SettingsError(f"the variable {MARKER} is not set")))
        self.assertEqual((2, "", "patient_split: settings\n"), (code, out, err))

    def test_a_failed_check_prints_its_codes_one_to_a_line(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(PatientSplitError("two_sets: 1 patient", "rule_mismatch: 1 patient")))
        self.assertEqual((1, "", "patient_split: two_sets: 1 patient\npatient_split: rule_mismatch: 1 patient\n"), (code, out, err))


if __name__ == "__main__":
    unittest.main()
