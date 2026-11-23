"""The command line of the reconciliation job prints fixed codes and never an argument, a branch name or the text of an upstream error, and the job holds the
image's modules.

Failure modes covered here:

- A mode that writes (``build``) is accepted by a job that only reads.
- An extra or malformed argument, or a branch name that Nessie would refuse, is echoed by the argument parser.
- A failed check, a missing branch, a settings error or any other error carries text of its own into the output.
- The watchdog that stops a job that runs too long names another job or holds a duration.
- The job is told to read a table at the head of the branch instead of the hash that it resolved, so a branch that moves changes what it reads.
- A module that the job imports, directly or through another, is left out of the image or of the build context.
"""

from __future__ import annotations

import contextlib
import io
import json
import unittest
from argparse import Namespace
from typing import Any

from scripts.stack.nessie import NessieError
from spark.jobs.silver_reconcile import ReconcileLake
from spark.reconcile.checks import SOURCE_TABLES, ReconcileError
from spark.reconcile.cli import JOB, run
from spark.settings import SettingsError
from spark.split_cli import start_deadline
from tests.fast.test_replay_image import ignore_rules, imported_files
from tests.fast.test_silver_signals_image import ROOT
from tests.fast.test_spark_bronze_image import DOCKERFILE, IGNORE_FILE, image_problems

MARKER = "MARKER_5521"


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
        code, out, err = invoke(["verify", "--branch", "b"])
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({"mode": "verify", "branch": "b"}, json.loads(out.removeprefix("RESULT ")))

    def test_a_bad_call_prints_the_code_usage_and_does_not_echo_the_argument(self) -> None:
        for argv in (
            [],
            ["verify"],
            ["build", "--branch", "b"],
            ["clean", "--branch", "b"],
            ["verify", "--branch", MARKER + " bad"],
            ["verify", "--branch", "b", f"--{MARKER}"],
            ["verify", "--branch", "b", "--base", MARKER],
            ["verify", "--branch", "b", "--pinned-release"],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv)
                self.assertEqual((2, "", "silver_reconcile: usage\n"), (code, out, err))

    def test_a_failed_check_prints_each_problem_as_a_code_and_a_count_and_exits_1(self) -> None:
        code, out, err = invoke(["verify", "--branch", "b"], raising(ReconcileError("cases_key_unaccounted: 3 keys", "lab_results_held_out: a held-out key")))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("silver_reconcile: cases_key_unaccounted: 3 keys\nsilver_reconcile: lab_results_held_out: a held-out key\n", err)

    def test_an_error_of_nessie_is_a_code_and_never_its_text(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        for error, expected in ((missing, "branch_missing"), (other, "catalog_error"), (NessieError(MARKER, status=404, error_code="OTHER"), "catalog_error")):
            with self.subTest(expected=expected):
                code, out, err = invoke(["verify", "--branch", "b"], raising(error))
                self.assertEqual((1, "", f"silver_reconcile: {expected}\n"), (code, out, err))

    def test_a_setting_that_cannot_be_used_and_any_other_error_are_codes(self) -> None:
        code, out, err = invoke(["verify", "--branch", "b"], raising(SettingsError(MARKER)))
        self.assertEqual((2, "", "silver_reconcile: settings\n"), (code, out, err))
        code, out, err = invoke(["verify", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, "", "silver_reconcile: internal_error\n"), (code, out, err))


class Watchdog(unittest.TestCase):
    def test_the_watchdog_names_this_job_and_holds_no_duration(self) -> None:
        err, stops = io.StringIO(), list[int]()
        with contextlib.redirect_stderr(err):
            timer = start_deadline(0.0517, exit_with=stops.append, job=JOB)
            timer.join(5)
        self.assertEqual(([124], "silver_reconcile: timeout\n"), (stops, err.getvalue()))


class Reads(unittest.TestCase):
    def test_every_table_is_read_through_the_catalog_pinned_to_the_hash(self) -> None:
        asked: list[str] = []

        class Session:
            class catalog:  # noqa: N801
                @staticmethod
                def tableExists(name: str) -> bool:  # noqa: N802
                    asked.append(name)
                    return False

        lake = ReconcileLake(Session(), object(), "branch", "a" * 64)  # type: ignore[arg-type]
        self.assertEqual({table: None for table in SOURCE_TABLES}, {table: lake.columns(f"lake.{table}") for table in SOURCE_TABLES})
        self.assertEqual([f"lake.{table}" for table in SOURCE_TABLES], asked)
        self.assertEqual("a" * 64, lake.at)

    def test_every_table_that_is_not_there_is_counted_before_any_query_runs(self) -> None:
        class Session:
            class catalog:  # noqa: N801
                @staticmethod
                def tableExists(name: str) -> bool:  # noqa: N802
                    return False

        lake = ReconcileLake(Session(), object(), "branch", "a" * 64)  # type: ignore[arg-type]
        self.assertEqual([f"source_missing: {len(SOURCE_TABLES)} tables"], lake.source_problems())


class Image(unittest.TestCase):
    def sources(self) -> list[str]:
        return sorted(str(path.relative_to(ROOT)) for path in (ROOT / "spark" / "reconcile").glob("*.py")) + ["spark/jobs/silver_reconcile.py"]

    def test_every_module_that_the_job_imports_is_in_the_image(self) -> None:
        needed = imported_files(self.sources())
        for path in ("features/signal_validity.py", "spark/silver_cases_sql.py", "spark/split_cli.py", "scripts/stack/nessie.py"):
            self.assertIn(path, needed)
        self.assertEqual([], image_problems(DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text()), needed))

    def test_a_module_left_out_of_the_image_or_the_context_is_named(self) -> None:
        needed = imported_files(self.sources())
        text, rules = DOCKERFILE.read_text(), ignore_rules(IGNORE_FILE.read_text())
        path = "features/signal_validity.py"
        self.assertIn(f"{path}: the build's ignore file leaves it out", image_problems(text, [*rules, path], needed))
        without = "\n".join(line for line in text.splitlines() if path not in line)
        self.assertIn(f"{path}: no COPY line puts it at /opt/jobs/{path}", image_problems(without, rules, needed))


if __name__ == "__main__":
    unittest.main()
