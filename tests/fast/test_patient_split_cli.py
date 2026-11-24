"""The command line of the patient split job prints fixed codes and never an argument, a branch name or the text of an upstream error.

Failure modes covered here:

- An extra or a malformed argument is echoed by the argument parser.
- A missing branch is reported through Nessie's own error, which names the branch and the server's text.
- Any other upstream error, a settings error or a check failure carries text that reaches the output.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import math
import os
import threading
import unittest
from argparse import Namespace
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieConflict, NessieError
from spark.settings import SettingsError
from spark.split_cli import number_setting, open_client, run, start_deadline
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

    def test_only_the_reference_not_found_answer_is_a_missing_branch(self) -> None:
        for error_code, wanted in (("REFERENCE_NOT_FOUND", "branch_missing"), ("CONTENT_NOT_FOUND", "catalog_error"), (None, "catalog_error")):
            with self.subTest(error_code=error_code):
                code, out, err = invoke(["verify", "--branch", "b"], raising(NessieError(f"missing {MARKER}", status=404, error_code=error_code)))
                self.assertEqual((1, "", f"patient_split: {wanted}\n"), (code, out, err))

    def test_a_reference_not_found_code_on_another_status_is_not_a_missing_branch(self) -> None:
        code, _, err = invoke(["verify", "--branch", "b"], raising(NessieError("x", status=500, error_code="REFERENCE_NOT_FOUND")))
        self.assertEqual((1, "patient_split: catalog_error\n"), (code, err))

    def test_any_other_upstream_error_is_a_code_and_not_its_text(self) -> None:
        for error in (NessieError(f"failed {MARKER}", status=500), NessieError(f"timeout {MARKER}"), NessieConflict(f"exists {MARKER}", status=409)):
            with self.subTest(error=type(error).__name__, status=error.status):
                code, out, err = invoke(["build", "--branch", MARKER], raising(error))
                self.assertEqual((1, "", "patient_split: catalog_error\n"), (code, out, err))

    def test_a_settings_error_is_a_code_and_not_its_text(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SettingsError(f"the variable {MARKER} is not set")))
        self.assertEqual((2, "", "patient_split: settings\n"), (code, out, err))

    def test_an_exception_that_nobody_mapped_is_one_fixed_code_with_no_text_and_no_traceback(self) -> None:
        for error in (RuntimeError(f"boom {MARKER}"), ValueError(f"bad value {MARKER}"), KeyError(MARKER), OSError(f"no file {MARKER}")):
            with self.subTest(error=type(error).__name__):
                code, out, err = invoke(["build", "--branch", "b"], raising(error))
                self.assertEqual((70, "", "patient_split: internal_error\n"), (code, out, err))

    def test_a_failed_check_prints_its_codes_one_to_a_line(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(PatientSplitError("two_sets: failed", "rule_mismatch: failed")))
        self.assertEqual((1, "", "patient_split: two_sets: failed\npatient_split: rule_mismatch: failed\n"), (code, out, err))


class DeadlineTest(unittest.TestCase):
    """A deadline stops the process with a fixed code and never prints how long it was."""

    def test_the_timer_prints_a_fixed_code_and_exits_124_without_the_duration(self) -> None:
        exited = threading.Event()
        codes: list[int] = []

        def stop(code: int) -> None:
            codes.append(code)
            exited.set()

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            start_deadline(0.0517, exit_with=stop)
            self.assertTrue(exited.wait(5), "the deadline did not fire within 5 seconds")
        self.assertEqual(([124], "patient_split: timeout\n"), (codes, err.getvalue()))
        self.assertNotIn("0.05", err.getvalue())


class ReferenceNameTest(unittest.TestCase):
    """A branch or base name that Nessie would refuse is a usage error before anything is called."""

    def test_a_name_that_is_not_plain_is_a_usage_error_and_a_plain_name_is_not(self) -> None:
        for argv in (
            ["build", "--branch", ""],
            ["build", "--branch", "a b"],
            ["build", "--branch", "b", "--base", "x;y"],
            ["verify", "--branch", "-x"],
            ["build", "--branch", "b" * 200],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((2, "", "patient_split: usage\n"), (code, out, err))
        for name in ("main", "split_52c8bf", "p6_trunk_ab12.x-y", "a" * 128):
            with self.subTest(name=name):
                self.assertEqual(0, invoke(["build", "--branch", name, "--base", name])[0])


class SettingsTest(unittest.TestCase):
    """A setting that cannot be parsed ends as ``settings`` and never shows its value."""

    def test_a_number_setting_is_read_or_defaulted(self) -> None:
        with mock.patch.dict(os.environ, {"X_SECONDS": "2.5"}):
            self.assertEqual(2.5, number_setting("X_SECONDS", 9.0, 100.0))
        with mock.patch.dict(os.environ):
            os.environ.pop("X_SECONDS", None)
            self.assertEqual(9.0, number_setting("X_SECONDS", 9.0, 100.0))

    def test_a_number_setting_that_is_not_a_positive_number_is_refused_without_its_value(self) -> None:
        for value in (MARKER, "", "0", "-1", "nan", "inf", f"1{MARKER}", "1e100", "100.5"):
            with self.subTest(value=value), mock.patch.dict(os.environ, {"X_SECONDS": value}), self.assertRaises(SettingsError) as caught:
                number_setting("X_SECONDS", 9.0, 100.0)
            self.assertNotIn(MARKER, str(caught.exception))
        self.assertTrue(math.isfinite(number_setting("X_SECONDS", 9.0, 100.0)))

    def test_a_nessie_address_with_a_bad_port_is_a_settings_error_without_its_text(self) -> None:
        with self.assertRaises(SettingsError) as caught:
            open_client(f"http://nessie:{MARKER}/api/v2", 1.0)
        self.assertNotIn(MARKER, str(caught.exception))

    def test_a_nessie_address_that_is_not_plain_http_is_a_settings_error(self) -> None:
        for uri in ("https://nessie:19120/api/v2", "ftp://nessie/api/v2", "nessie:19120", "http://", ""):
            with self.subTest(uri=uri), self.assertRaises(SettingsError):
                open_client(uri, 1.0)

    def test_a_marker_in_each_setting_of_the_job_reaches_no_output(self) -> None:
        job = importlib.import_module("spark.jobs.patient_split")
        good = {
            "NESSIE_URI": "http://nessie:19120/api/v2",
            "WAREHOUSE_LOCATION": "file:///warehouse/",
            "JOB_DEADLINE_SECONDS": "5",
            "NESSIE_TIMEOUT_SECONDS": "5",
        }
        for name, value in (
            ("JOB_DEADLINE_SECONDS", MARKER),
            ("NESSIE_TIMEOUT_SECONDS", MARKER),
            ("NESSIE_URI", f"http://nessie:{MARKER}/api/v2"),
            ("NESSIE_URI", ""),
        ):
            with self.subTest(name=name), mock.patch.dict(os.environ, {**good, name: value}):
                code, out, err = invoke(["build", "--branch", "b"], job.execute)
            self.assertEqual((2, "", "patient_split: settings\n"), (code, out, err))


if __name__ == "__main__":
    unittest.main()
