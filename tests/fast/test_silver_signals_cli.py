"""The command line of the Silver signals job writes its result as one file and prints fixed codes, and never an argument, a branch name or the text of an
upstream error.

Failure modes covered here:

- The result goes to standard output as a line that a caller must pick out of the job's other output, so a line of any other text can stand for it.
- The call takes no ``--result`` path, a relative one, or one that exists, and the job runs and writes over it or beside it.
- The result file is seen half written, holds a document that is not the result, is written over a file that is there, or leaves a temporary file behind.
- A refusal, a failed upstream, a bad setting or an internal error leaves no result file, or one whose codes are not the codes printed on standard error.
- A result that cannot be written (a value that is not JSON, a rename that fails) ends the job with exit 0 and no file.
- A path that cannot hold its part file (a folder that is not there, a name of 255 bytes) is found only after the job ran, and the job's work is lost. The
  part file is reserved before ``execute``, and a failed reservation is a bad call: ``usage``, exit 2, and nothing runs.
- A job that the watchdog stops, or a file that appears while the job runs, leaves a part file or is written over.
- A line of standard error holds a code without the flag, or text after the code: every printed line is ``<job>: <code>: failed``.

- An extra or a malformed argument, or a branch name that Nessie would refuse, is echoed by the argument parser.
- A failed check, a missing branch, a settings error or any other error carries text of its own into the output.
- The namespace commit of a branch that moved ends the job with the exception of the patient split, which the command line maps to an internal error
  and not to the refusal ``branch_moved``.
- The watchdog that stops a job that runs too long names another job, or holds a duration.
- A failed check prints no line, or prints a line that is not a code and a flag, or the exit code does not tell a failed check from a bad call.
- A table whose columns are not those of its card prints a count of columns, or the name of a table, and not the code and flag ``schema: failed``.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import stat
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieError
from spark.jobs.silver_signals import SignalsLake
from spark.settings import SettingsError
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import JOB, run
from spark.split_cli import start_deadline
from spark.split_table import schema_problems

MARKER = "MARKER_8731"
PART = "result.json.part"


def invoke(argv: list[str], execute: Any = None, result: Path | None = None, job: str = JOB) -> tuple[int, str, str]:
    """Run the command line with ``--result`` at ``result`` (none when ``result`` is ``None``), and give the exit code, standard output and standard error."""
    out, err = io.StringIO(), io.StringIO()
    code = 0
    arguments = [*argv, "--result", str(result)] if result is not None else argv
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(arguments, execute or (lambda parsed: {"mode": parsed.mode, "branch": parsed.branch}), job)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


class InFolder(unittest.TestCase):
    """A test with a folder of its own and a result path in it that does not exist."""

    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.path = self.folder / "result.json"

    def call(self, argv: list[str], execute: Any = None) -> tuple[int, str, str]:
        return invoke(argv, execute, self.path)

    def document(self) -> Any:
        return json.loads(self.path.read_text())

    def left(self) -> list[str]:
        return sorted(item.name for item in self.folder.iterdir())


def raising(error: BaseException) -> Any:
    def execute(_arguments: Namespace) -> dict[str, Any]:
        raise error

    return execute


class CommandLine(InFolder):
    def test_a_good_call_writes_the_result_as_one_file_and_prints_nothing(self) -> None:
        for argv in (["build", "--branch", "b"], ["build", "--branch", "b", "--base", "main"], ["verify", "--branch", "b"]):
            with self.subTest(argv=argv):
                self.path.unlink(missing_ok=True)
                code, out, err = self.call(argv)
                self.assertEqual((0, "", ""), (code, out, err))
                self.assertEqual({"mode": argv[0], "branch": "b"}, self.document())
                self.assertEqual(["result.json"], self.left())

    def test_the_file_is_one_object_that_a_caller_can_read_without_the_job_s_other_output(self) -> None:
        self.call(["verify", "--branch", "b"], lambda parsed: {"mode": "verify", "list": [1, 2], "nested": {"a": None, "b": True}})
        text = self.path.read_text()
        self.assertEqual({"mode": "verify", "list": [1, 2], "nested": {"a": None, "b": True}}, json.loads(text))
        self.assertTrue(text.endswith("\n"))
        self.assertEqual(0o644, stat.S_IMODE(self.path.stat().st_mode))

    def test_the_file_is_written_whole_to_a_private_file_and_then_linked_into_place(self) -> None:
        seen: list[tuple[str, str, Any, bool]] = []
        link = os.link

        def spy(source: str, target: str) -> None:
            if str(target) == str(self.path):
                seen.append((str(source), str(target), json.loads(Path(source).read_text()), self.path.exists()))
            link(source, target)

        with mock.patch("os.link", spy):
            code, out, err = self.call(["build", "--branch", "b"])
        self.assertEqual((0, "", ""), (code, out, err))
        self.assertEqual(1, len(seen))
        source, target, whole, existed = seen[0]
        self.assertEqual(str(self.path), target)
        self.assertRegex(Path(source).name, r"\.[0-9a-f]{8}\.part")
        self.assertEqual({"mode": "build", "branch": "b"}, whole)
        self.assertFalse(existed)
        self.assertEqual(["result.json"], self.left())

    def test_a_call_without_a_result_path_or_with_a_path_that_is_not_usable_is_a_bad_call(self) -> None:
        self.path.write_text("kept")
        (self.folder / "held.part").write_text("held")
        ran: list[int] = []

        def ran_once(parsed: Namespace) -> dict[str, Any]:
            ran.append(1)
            return {}

        for name, argv in (
            ("no path", ["build", "--branch", "b"]),
            ("a relative path", ["build", "--branch", "b", "--result", "result.json"]),
            ("a path that exists", ["build", "--branch", "b", "--result", str(self.path)]),
            ("a folder", ["build", "--branch", "b", "--result", str(self.folder)]),
            ("an empty path", ["build", "--branch", "b", "--result", ""]),
            ("a path that ends in a slash", ["build", "--branch", "b", "--result", str(self.folder / "x") + "/"]),
            ("two result paths", ["build", "--branch", "b", "--result", str(self.folder / "a"), "--result", str(self.folder / "b")]),
            ("a part file that exists", ["build", "--branch", "b", "--result", str(self.folder / "held")]),
        ):
            with self.subTest(name):
                code, out, err = invoke(argv, ran_once)
                self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))
                self.assertEqual([], ran)
        self.assertEqual("kept", self.path.read_text())
        self.assertEqual("held", (self.folder / "held.part").read_text())
        self.assertEqual(["held.part", "result.json"], self.left())

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
                code, out, err = self.call(argv)
                self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))
                self.assertEqual([], self.left())

    def test_a_result_path_with_the_marker_is_never_echoed(self) -> None:
        marked = self.folder / MARKER
        marked.mkdir()
        code, out, err = invoke(["build", "--branch", "b", "--result", "relative/" + MARKER])
        self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))
        code, out, err = invoke(["build", "--branch", "b", "--result", str(marked)])
        self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))

    def test_a_failed_check_prints_each_problem_as_a_code_and_a_flag_and_writes_them_to_the_file(self) -> None:
        code, out, err = self.call(["build", "--branch", "b"], raising(SilverSignalsError("numeric_verdict_differs: failed", "waveform_shape: failed")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("silver_signals: numeric_verdict_differs: failed\nsilver_signals: waveform_shape: failed\n", err)
        self.assertEqual(
            {"job": "silver_signals", "mode": "build", "refusal": ["numeric_verdict_differs: failed", "waveform_shape: failed"]},
            self.document(),
        )
        self.assertEqual(["result.json"], self.left())

    def test_a_table_with_the_wrong_columns_prints_the_code_schema_and_a_flag(self) -> None:
        code, out, err = self.call(
            ["verify", "--branch", "b"], raising(SilverSignalsError(*schema_problems([("stray", "string")], [("a", "string"), ("b", "int")])))
        )
        self.assertEqual((1, "", "silver_signals: schema: failed\n"), (code, out, err))
        self.assertEqual({"job": "silver_signals", "mode": "verify", "refusal": ["schema: failed"]}, self.document())

    def test_an_error_of_nessie_is_a_code_and_never_its_text(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        for error, expected in ((missing, "branch_missing"), (other, "catalog_error"), (NessieError(MARKER, status=404, error_code="OTHER"), "catalog_error")):
            with self.subTest(expected=expected):
                self.path.unlink(missing_ok=True)
                code, out, err = self.call(["verify", "--branch", "b"], raising(error))
                self.assertEqual((1, "", f"silver_signals: {expected}: failed\n"), (code, out, err))
                self.assertEqual({"job": "silver_signals", "mode": "verify", "refusal": [expected]}, self.document())
                self.assertNotIn(MARKER, self.path.read_text())

    def test_a_setting_that_cannot_be_used_and_any_other_error_are_codes(self) -> None:
        code, out, err = self.call(["verify", "--branch", "b"], raising(SettingsError(MARKER)))
        self.assertEqual((2, "", "silver_signals: settings: failed\n"), (code, out, err))
        self.assertEqual({"job": "silver_signals", "mode": "verify", "refusal": ["settings"]}, self.document())
        self.path.unlink()
        code, out, err = self.call(["verify", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, "", "silver_signals: internal_error: failed\n"), (code, out, err))
        self.assertEqual({"job": "silver_signals", "mode": "verify", "refusal": ["internal_error"]}, self.document())
        self.assertNotIn(MARKER, self.path.read_text())

    def test_a_result_that_is_not_json_is_an_internal_error_with_the_refusal_document(self) -> None:
        for name, execute in (
            ("a value that is not JSON", lambda parsed: {"mode": {1, 2}}),
            ("a value that is not a number", lambda parsed: {"mode": float("nan")}),
        ):
            with self.subTest(name):
                code, out, err = self.call(["build", "--branch", "b"], execute)
                self.assertEqual((70, "", "silver_signals: internal_error: failed\n"), (code, out, err))
                self.assertEqual({"job": "silver_signals", "mode": "build", "refusal": ["internal_error"]}, self.document())
                self.assertEqual(["result.json"], self.left())
                self.path.unlink()

    def test_a_file_that_cannot_be_written_after_the_run_is_an_internal_error_with_no_file_and_no_part_file(self) -> None:
        link = os.link

        def refuse(source: str, target: str) -> None:
            if not str(target).endswith(".part"):
                raise OSError(f"{MARKER} disk full")
            link(source, target)

        for name, execute in (
            ("a result", None),
            ("a refusal", raising(SilverSignalsError("numeric_verdict_differs: failed"))),
        ):
            with self.subTest(name), mock.patch("os.link", refuse):
                code, out, err = self.call(["build", "--branch", "b"], execute)
                self.assertEqual((70, "", "silver_signals: internal_error: failed\n"), (code, out, err))
                self.assertEqual([], self.left())

    def test_a_folder_that_is_not_there_is_a_bad_call_and_the_job_does_not_run(self) -> None:
        ran: list[int] = []

        def ran_once(parsed: Namespace) -> dict[str, Any]:
            ran.append(1)
            return {}

        for execute in (ran_once, raising(SilverSignalsError("numeric_verdict_differs: failed"))):
            code, out, err = invoke(["build", "--branch", "b"], execute, self.folder / "no_such_folder" / "result.json")
            self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))
        self.assertEqual(([], []), (ran, self.left()))

    def test_a_name_that_cannot_hold_its_part_file_is_a_bad_call_and_the_job_does_not_run(self) -> None:
        ran: list[int] = []

        def ran_once(parsed: Namespace) -> dict[str, Any]:
            ran.append(1)
            return {}

        code, out, err = invoke(["build", "--branch", "b"], ran_once, self.folder / ("r" * 255))
        self.assertEqual((2, "", "silver_signals: usage\n"), (code, out, err))
        code, out, err = invoke(["build", "--branch", "b"], ran_once, self.folder / ("r" * 250))
        self.assertEqual((0, "", ""), (code, out, err))
        self.assertEqual(([1], ["r" * 250]), (ran, self.left()))

    def test_the_part_file_is_reserved_before_the_job_runs_and_gone_after_it(self) -> None:
        seen: list[list[str]] = []

        def execute(parsed: Namespace) -> dict[str, Any]:
            seen.append(self.left())
            return {"mode": parsed.mode}

        code, _, _ = self.call(["build", "--branch", "b"], execute)
        self.assertEqual((0, [[PART]]), (code, seen))
        self.assertEqual(["result.json"], self.left())

    def test_the_part_file_is_reserved_before_the_job_raises(self) -> None:
        seen: list[list[str]] = []

        def execute(parsed: Namespace) -> dict[str, Any]:
            seen.append(self.left())
            raise SilverSignalsError("a: failed")

        self.call(["build", "--branch", "b"], execute)
        self.assertEqual([[PART]], seen)

    def test_a_job_that_raises_past_the_mapping_leaves_no_part_file(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self.call(["build", "--branch", "b"], raising(KeyboardInterrupt()))
        self.assertEqual([], self.left())

    def test_the_watchdog_removes_the_part_file_that_the_job_reserved(self) -> None:
        seen: list[list[str]] = []
        stops = list[int]()

        def execute(parsed: Namespace) -> dict[str, Any]:
            with contextlib.redirect_stderr(io.StringIO()):
                start_deadline(0.01, exit_with=stops.append, job=JOB).join(5)
            seen.append(self.left())
            return {}

        self.call(["build", "--branch", "b"], execute)
        self.assertEqual(([124], [[]]), (stops, seen))

    def test_a_file_that_appears_while_the_job_runs_is_never_written_over(self) -> None:
        def execute(parsed: Namespace) -> dict[str, Any]:
            self.path.write_text("other")
            return {"mode": parsed.mode}

        code, out, err = self.call(["build", "--branch", "b"], execute)
        self.assertEqual((70, "", "silver_signals: internal_error: failed\n"), (code, out, err))
        self.assertEqual("other", self.path.read_text())
        self.assertEqual(["result.json"], self.left())

    def test_a_problem_that_holds_more_than_its_code_is_printed_and_written_as_its_code_and_the_flag(self) -> None:
        code, out, err = self.call(["build", "--branch", "b"], raising(SilverSignalsError(f"waveform_shape: {MARKER}", "numeric_verdict_differs")))
        self.assertEqual((1, "", "silver_signals: waveform_shape: failed\nsilver_signals: numeric_verdict_differs: failed\n"), (code, out, err))
        self.assertEqual(["waveform_shape: failed", "numeric_verdict_differs: failed"], self.document()["refusal"])
        self.assertNotIn(MARKER, self.path.read_text())

    def test_every_printed_line_is_the_job_a_code_and_the_flag(self) -> None:
        errors = (
            SilverSignalsError("a: failed", "b: failed"),
            NessieError(MARKER, status=404, error_code="REFERENCE_NOT_FOUND"),
            NessieError(MARKER, status=500, error_code="X"),
            SettingsError(MARKER),
            RuntimeError(MARKER),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__):
                self.path.unlink(missing_ok=True)
                _, out, err = self.call(["verify", "--branch", "b"], raising(error))
                self.assertEqual("", out)
                self.assertTrue(err.endswith("\n"))
                for line in err.splitlines():
                    self.assertRegex(line, r"^silver_signals: [a-z_]+: failed$")

    def test_the_hypotension_job_prints_its_own_name(self) -> None:
        code, out, err = invoke(["build", "--branch", "b"], raising(SettingsError(MARKER)), self.path, job="silver_hypotension")
        self.assertEqual((2, "", "silver_hypotension: settings: failed\n"), (code, out, err))
        self.assertEqual({"job": "silver_hypotension", "mode": "build", "refusal": ["settings"]}, self.document())

    def test_no_stream_carries_the_result(self) -> None:
        code, out, err = self.call(["build", "--branch", "b"], lambda parsed: {"mode": "build", "marker": MARKER})
        self.assertEqual((0, "", ""), (code, out, err))
        self.assertIn(MARKER, self.path.read_text())


class Watchdog(unittest.TestCase):
    def test_the_watchdog_names_this_job_and_holds_no_duration(self) -> None:
        err, stops = io.StringIO(), list[int]()
        with contextlib.redirect_stderr(err):
            timer = start_deadline(0.0517, exit_with=stops.append, job=JOB)
            timer.join(5)
        self.assertEqual(([124], "silver_signals: timeout\n"), (stops, err.getvalue()))

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

        lake = SignalsLake(Session(), Client(), "branch", {})  # type: ignore[arg-type]
        with self.assertRaises(SilverSignalsError) as caught:
            lake.prepare("a" * 64)
        self.assertEqual(["branch_moved: failed"], caught.exception.problems)
        with tempfile.TemporaryDirectory() as folder:
            result = Path(folder) / "result.json"
            code, out, err = invoke(["build", "--branch", "b"], raising(caught.exception), result)
            self.assertEqual((1, "", "silver_signals: branch_moved: failed\n"), (code, out, err))
            self.assertEqual(
                {"job": "silver_signals", "mode": "build", "refusal": ["branch_moved: failed"]},
                json.loads(result.read_text()),
            )


if __name__ == "__main__":
    unittest.main()
