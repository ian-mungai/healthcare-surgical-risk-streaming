"""The command line of the Bronze clinical and laboratory job: one result file, fixed codes and nothing from an error's text. No Spark runs here.

Both tables hold creatinine results or outcome columns, so nothing that can change with a held-out patient leaves the job. The result reaches the caller through
one file at ``--result`` and through nothing else (``spark/result_file.py``); the streams stay for the operator.

Failure modes covered here:

- A refusal prints the text of its error (a column name, a repeated-row count, a branch or hash, an upstream message), so a value or a count leaves the job.
- A caller reads a free-form stream because a refusal writes no file, or reads a file whose fields differ from one ending to the next.
- A bad call echoes its argument, or a result path that cannot hold a file is written to anyway.
- A job that cannot write its file ends with a result line or no code.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieConflict, NessieError
from spark.bronze.publication import PublicationError
from spark.bronze_clinical_lab import BronzeRulesError
from spark.bronze_clinical_lab_cli import EXIT_CODES, code_of, run
from spark.bronze_clinical_lab_run import CHECK_NAMES, CHECKS_PASSED, JOB, NotABranch, SourceNotInHistory
from spark.jobs import bronze_clinical_lab as job
from spark.session import start_deadline
from spark.settings import SettingsError
from tests.fast.test_bronze_job_sequence import BRANCH, HASH, Harness

MARKER = "MARKER_9917"
KEYS = {"job", "command", "branch", "passed", "diagnostics"}
RAN = {"branch_hash", "default_branch", "source_hash", "tables"}


def invoke(argv: list[str], execute: Any) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(argv, execute)
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def raising(error: Exception) -> Any:
    def execute(arguments: Any) -> dict[str, Any]:
        raise error

    return execute


class Endings(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="bronze_cli_"))
        self.path = f"{self.folder}/result.json"
        self.argv = ["verify", "--branch", BRANCH, "--source-hash", HASH, "--result", self.path]

    def written(self) -> dict[str, Any]:
        document: dict[str, Any] = json.loads(Path(self.path).read_text())
        return document

    def verified(self, **plant: Any) -> dict[str, Any]:
        harness = Harness()
        for key, value in plant.items():
            setattr(harness.spark, key, value)
        return harness.run("verify")

    def test_a_clean_run_writes_its_result_with_no_diagnostic_and_prints_one_result_line(self) -> None:
        result = self.verified()
        code, out, err = invoke(self.argv, lambda arguments: result)
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({**result, "diagnostics": []}, self.written())
        self.assertEqual({"job", "command", "branch", "passed", "diagnostics", *RAN}, set(self.written()))
        self.assertEqual({**result, "diagnostics": []}, json.loads(out.removeprefix("RESULT ")))

    def test_a_failed_check_writes_the_result_with_one_code_and_prints_the_failed_checks_by_name(self) -> None:
        result = self.verified(duplicates=3)
        code, out, err = invoke(self.argv, lambda arguments: result)
        self.assertEqual((1, ""), (code, out))
        self.assertEqual({**result, "diagnostics": [{"code": "checks_failed"}]}, self.written())
        self.assertFalse(self.written()["passed"])
        self.assertEqual(f"{JOB}: checks_failed\n{JOB}: clinical_cases: key unique: failed\n{JOB}: lab_results: key unique: failed\n", err)

    def test_every_other_ending_is_one_code_in_the_file_and_the_text_of_the_error_is_in_no_output(self) -> None:
        endings: tuple[tuple[Exception, int, str], ...] = (
            (SourceNotInHistory(f"{MARKER} is not in the history"), 1, "source_hash_not_in_history"),
            (NotABranch(f"{MARKER} is not a branch"), 1, "not_a_branch"),
            (NessieConflict(f"{MARKER} refused", status=409), 1, "branch_conflict"),
            (NessieError(f"{MARKER} refused", status=500), 1, "catalog_error"),
            (PublicationError(f"{MARKER} not published"), 1, "publication_refused"),
            (SettingsError(MARKER), 2, "settings"),
            (BronzeRulesError(MARKER, code="source_table_missing"), 2, "source_table_missing"),
            (BronzeRulesError(MARKER), 2, "rules_broken"),
            (RuntimeError(MARKER), 70, "internal_error"),
        )
        for error, status, expected in endings:
            with self.subTest(expected=expected):
                code, out, err = invoke(self.argv, raising(error))
                self.assertEqual((status, ""), (code, out))
                self.assertEqual(f"{JOB}: {expected}\n", err)
                self.assertEqual(
                    {"job": JOB, "command": "verify", "branch": BRANCH, "passed": False, "diagnostics": [{"code": expected}]},
                    self.written(),
                )
                self.assertNotIn(MARKER, Path(self.path).read_text())
                os.remove(self.path)

    def test_a_refusal_file_has_exactly_the_closed_set_of_fields_and_a_run_file_adds_only_the_run_fields(self) -> None:
        invoke(self.argv, raising(SettingsError(MARKER)))
        self.assertEqual(KEYS, set(self.written()))
        os.remove(self.path)
        invoke(self.argv, lambda arguments: self.verified())
        self.assertEqual(KEYS | RAN, set(self.written()))

    def test_a_bad_call_prints_usage_and_does_not_echo_the_argument_and_writes_nothing(self) -> None:
        ran: list[str] = []

        def execute(arguments: Any) -> dict[str, Any]:
            ran.append("run")
            return {}

        for argv in (
            [],
            ["verify"],
            ["verify", "--branch", BRANCH, "--source-hash", HASH],
            ["verify", "--branch", BRANCH, "--source-hash", HASH, "--result", "relative.json"],
            ["verify", "--branch", BRANCH, "--source-hash", HASH, "--result", self.path, "--result", self.path + "2"],
            ["write", "--branch", BRANCH, "--source-hash", HASH, "--result", self.path, "--base", MARKER],
            [MARKER, "--branch", BRANCH, "--source-hash", HASH, "--result", self.path],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv, execute)
                self.assertEqual((2, "", f"{JOB}: usage\n"), (code, out, err))
        self.assertEqual(([], []), (ran, os.listdir(self.folder)))

    def test_a_path_that_cannot_hold_a_result_is_usage_and_the_job_does_not_run(self) -> None:
        Path(self.path).write_text("old")
        ran: list[str] = []

        def execute(arguments: Any) -> dict[str, Any]:
            ran.append("run")
            return {}

        code, out, err = invoke(self.argv, execute)
        self.assertEqual((2, "", f"{JOB}: usage\n"), (code, out, err))
        self.assertEqual(([], "old"), (ran, Path(self.path).read_text()))

    def test_a_file_that_cannot_be_written_is_internal_error_with_no_file_and_no_result_line(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError(f"{MARKER} disk full")

        with mock.patch("os.rename", refuse):
            code, out, err = invoke(self.argv, lambda arguments: self.verified())
        self.assertEqual((70, "", f"{JOB}: internal_error\n"), (code, out, err))
        self.assertEqual([], os.listdir(self.folder))

    def test_the_file_is_the_same_whatever_the_streams_hold(self) -> None:
        def noisy(arguments: Any) -> dict[str, Any]:
            sys.stdout.write("WARN a line that no reader may rely on\n")
            sys.stderr.write(f"WARNING: Using incubator modules: jdk.incubator.vector {MARKER}\n")
            return self.verified()

        code, _, _ = invoke(self.argv, noisy)
        self.assertEqual(0, code)
        self.assertEqual({**self.verified(), "diagnostics": []}, self.written())


class Codes(unittest.TestCase):
    """The closed lists that the end-to-end runner reads a file against: the exit status of each code and the checks of a table."""

    def test_each_code_that_an_error_can_give_has_its_exit_status_and_no_other_code_is_listed(self) -> None:
        errors: list[Exception] = [
            SourceNotInHistory("x"),
            NotABranch("x"),
            NessieConflict("x"),
            NessieError("x"),
            PublicationError("x"),
            SettingsError("x"),
            RuntimeError("x"),
            *[BronzeRulesError("x", code=code) for code in ("columns_refused", "source_table_missing", "repeated_rows_over_limit", "rules_broken")],
        ]
        found = {code_of(error) for error in errors}
        self.assertEqual({(EXIT_CODES[code], code) for _, code in found}, found)
        self.assertEqual({code for _, code in found}, set(EXIT_CODES))

    def test_a_passed_run_holds_exactly_the_checks_of_a_passed_table_for_each_kind(self) -> None:
        result = Harness().run("verify")
        self.assertEqual(
            {"clinical_cases": CHECKS_PASSED["clinical"], "lab_results": CHECKS_PASSED["lab"]},
            {name: tuple(check["name"] for check in table["checks"]) for name, table in result["tables"].items()},
        )

    def test_every_check_name_that_the_job_adds_is_in_the_closed_list_and_no_other_is(self) -> None:
        source = (Path(__file__).resolve().parents[2] / "spark" / "bronze_clinical_lab_run.py").read_text()
        added = set(re.findall(r'report\.add\(\s*"([^"]+)"', source))
        self.assertEqual(added, set(CHECK_NAMES))
        self.assertTrue(set(CHECKS_PASSED["clinical"]) | set(CHECKS_PASSED["lab"]) <= set(CHECK_NAMES))


class Deadline(unittest.TestCase):
    """The watchdog of the job ends the process with the fixed code ``timeout`` and exit 124, and names no duration. It writes no result file."""

    def test_the_timer_prints_the_fixed_code_and_exits_124_without_the_duration(self) -> None:
        exited = threading.Event()
        codes: list[int] = []

        def stop(code: int) -> None:
            codes.append(code)
            exited.set()

        err = io.StringIO()
        with contextlib.redirect_stderr(err), mock.patch("spark.session.os._exit", stop):
            start_deadline(0.0517, JOB)
            self.assertTrue(exited.wait(5), "the deadline did not fire within 5 seconds")
        self.assertEqual(([124], f"{JOB}: timeout\n"), (codes, err.getvalue()))
        self.assertNotIn("0.05", err.getvalue())


class Job(unittest.TestCase):
    """The job's own ``main``: a refusal anywhere in its setup is a code in the file too."""

    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="bronze_job_"))
        self.path = f"{self.folder}/result.json"

    def call(self, command: str = "verify", **patches: Any) -> int:
        argv = [command, "--branch", BRANCH, "--source-hash", HASH, "--result", self.path]
        with contextlib.ExitStack() as stack, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            stack.enter_context(mock.patch.object(job, "start_deadline"))
            stack.enter_context(mock.patch.object(job, "NessieClient"))
            stack.enter_context(mock.patch.object(job, "required_environment", return_value="http://127.0.0.1:1/api/v2"))
            for name, value in patches.items():
                stack.enter_context(mock.patch.object(job, name, **value))
            return job.main(argv)

    def test_a_setting_that_is_missing_is_the_settings_code_in_the_file(self) -> None:
        self.assertEqual(2, self.call(required_environment={"side_effect": SettingsError(MARKER)}))
        self.assertEqual([{"code": "settings"}], json.loads(Path(self.path).read_text())["diagnostics"])

    def test_the_sequence_runs_for_write_and_verify_and_publish_and_each_ends_in_its_file(self) -> None:
        result = Harness().run("verify")
        for command, runner in (("write", "run_job"), ("verify", "run_job"), ("publish", "publish_job")):
            with self.subTest(command):
                self.assertEqual(0, self.call(command, **{runner: {"return_value": result}}))
                self.assertEqual({**result, "diagnostics": []}, json.loads(Path(self.path).read_text()))
                os.remove(self.path)

    def test_a_refusal_of_the_publication_is_a_code_and_not_its_text(self) -> None:
        self.assertEqual(1, self.call("publish", publish_job={"side_effect": PublicationError(MARKER)}))
        self.assertEqual("publication_refused", json.loads(Path(self.path).read_text())["diagnostics"][0]["code"])
        self.assertNotIn(MARKER, Path(self.path).read_text())


if __name__ == "__main__":
    unittest.main()
