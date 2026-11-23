"""The command line of the signal profile job prints fixed codes and never an argument, a branch name or the text of an upstream error.

Failure modes covered here:

- An extra or a malformed argument, or a branch name that Nessie would refuse, is echoed by the argument parser.
- A failed check, a missing branch, a settings error or any other error carries text of its own into the output.
- The namespace commit of a branch that moved ends the job with the exception of the patient split, which the command line maps to an internal error
  and not to the refusal ``branch_moved``.
- The watchdog that stops a job that runs too long names another job, or holds a duration.
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
from spark.jobs.signal_profile import ProfileLake
from spark.profile.checks import SignalProfileError
from spark.profile.cli import JOB, run
from spark.settings import SettingsError
from spark.split_cli import start_deadline

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
        for argv in (
            ["build", "--branch", "b"],
            ["build", "--branch", "b", "--base", "main"],
            ["build", "--branch", "b", "--pinned-release"],
            ["verify", "--branch", "b"],
            ["verify", "--branch", "b", "--pinned-release"],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((0, ""), (code, err))
                self.assertEqual({"mode": argv[0], "branch": "b"}, json.loads(out.removeprefix("RESULT ")))

    def test_the_pinned_release_flag_is_read(self) -> None:
        seen: list[bool] = []

        def execute(arguments: Namespace) -> dict[str, Any]:
            seen.append(arguments.pinned_release)
            return {}

        invoke(["verify", "--branch", "b", "--pinned-release"], execute)
        invoke(["verify", "--branch", "b"], execute)
        self.assertEqual([True, False], seen)

    def test_a_bad_call_prints_the_code_usage_and_does_not_echo_the_argument(self) -> None:
        for argv in (
            [],
            ["build"],
            ["clean", "--branch", "b"],
            ["build", "--branch", MARKER + " bad"],
            ["build", "--branch", "b", f"--{MARKER}"],
            ["verify", "--branch", "b", "--base", MARKER],
            ["verify", "--branch", "b", "--base", MARKER, "--pinned-release"],
            ["build", "--branch", "b", f"--pinned-release={MARKER}"],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((2, "", "signal_profile: usage\n"), (code, out, err))

    def test_a_failed_check_prints_each_problem_as_a_code_and_a_count_and_exits_1(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SignalProfileError("profile_partition: 3 rows", "unscored_cases: 1 case")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("signal_profile: profile_partition: 3 rows\nsignal_profile: unscored_cases: 1 case\n", err)

    def test_an_error_of_nessie_is_a_code_and_never_its_text(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        for error, expected in ((missing, "branch_missing"), (other, "catalog_error"), (NessieError(MARKER, status=404, error_code="OTHER"), "catalog_error")):
            with self.subTest(expected=expected):
                code, out, err = invoke(["verify", "--branch", "b"], raising(error))
                self.assertEqual((1, "", f"signal_profile: {expected}\n"), (code, out, err))

    def test_a_setting_that_cannot_be_used_and_any_other_error_are_codes(self) -> None:
        code, out, err = invoke(["verify", "--branch", "b"], raising(SettingsError(MARKER)))
        self.assertEqual((2, "", "signal_profile: settings\n"), (code, out, err))
        code, out, err = invoke(["verify", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, "", "signal_profile: internal_error\n"), (code, out, err))


class Watchdog(unittest.TestCase):
    def test_the_watchdog_names_this_job_and_holds_no_duration(self) -> None:
        err, stops = io.StringIO(), list[int]()
        with contextlib.redirect_stderr(err):
            timer = start_deadline(0.0517, exit_with=stops.append, job=JOB)
            timer.join(5)
        self.assertEqual(([124], "signal_profile: timeout\n"), (stops, err.getvalue()))

    def test_the_patient_split_watchdog_keeps_its_own_name(self) -> None:
        err, stops = io.StringIO(), list[int]()
        with contextlib.redirect_stderr(err):
            timer = start_deadline(0.0517, exit_with=stops.append)
            timer.join(5)
        self.assertEqual(([124], "patient_split: timeout\n"), (stops, err.getvalue()))


class NamespaceCommit(unittest.TestCase):
    def test_a_branch_that_moved_during_the_namespace_commit_is_the_refusal_branch_moved(self) -> None:
        class Client:
            def has_namespace(self, branch: str, hash_value: str, namespace: str) -> bool:
                return False

            def request(self, method: str, path: str, **_options: Any) -> dict[str, Any]:
                return {"logEntries": []}

        class Session:
            def sql(self, text: str) -> None:
                return None

        lake = ProfileLake(Session(), Client(), "branch", {})  # type: ignore[arg-type]
        with self.assertRaises(SignalProfileError) as caught:
            lake.prepare("a" * 64)
        self.assertEqual(["branch_moved: the branch head is not the one that was checked"], caught.exception.problems)
        code, out, err = invoke(["build", "--branch", "b"], raising(caught.exception))
        self.assertEqual((1, "", "signal_profile: branch_moved: the branch head is not the one that was checked\n"), (code, out, err))


if __name__ == "__main__":
    unittest.main()
