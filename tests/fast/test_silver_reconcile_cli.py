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
from spark.reconcile.checks import OWNER_REFUSAL, SOURCE_TABLES, SPLIT_REFUSAL, ReconcileError
from spark.reconcile.cli import JOB, run
from spark.settings import SettingsError
from spark.silver_cases_sql import ownership_query
from spark.split_cli import start_deadline
from spark.split_table import RULE_VERSION
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
        lake.source_problems()
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


class Refusals(unittest.TestCase):
    """The split table and the owner of each Bronze case are checked by the Silver cases job's own checks before any count, and a refusal is one fixed code."""

    TRAINING_PATIENT, TEST_PATIENT = "3", "1"

    def lake(self, split: list[dict[str, Any]], owner: int = 0) -> tuple[ReconcileLake, list[str]]:
        asked: list[str] = []

        class Result:
            def __init__(self, rows: list[dict[str, Any]]) -> None:
                self.rows = rows

            def collect(self) -> list[dict[str, Any]]:
                return self.rows

        class Session:
            @staticmethod
            def table(name: str) -> Result:
                asked.append(f"table {name}")
                return Result(split)

            @staticmethod
            def sql(query: str) -> Result:
                asked.append(query)
                return Result([{"n": owner}])

        return ReconcileLake(Session(), object(), "branch", "a" * 64), asked  # type: ignore[arg-type]

    def row(self, subjectid: str, name: str) -> dict[str, Any]:
        return {"subjectid": subjectid, "set": name, "rule_version": RULE_VERSION, "source_snapshot_id": 7}

    def good(self) -> list[dict[str, Any]]:
        return [self.row(self.TRAINING_PATIENT, "training"), self.row(self.TEST_PATIENT, "test")]

    def test_a_split_that_follows_the_rule_and_cases_with_one_owner_are_no_refusal(self) -> None:
        lake, asked = self.lake(self.good())
        self.assertEqual([], lake.refusals())
        self.assertIn("table lake.silver.patient_split", asked)
        self.assertIn(ownership_query("lake.bronze.clinical_cases"), asked)

    def test_a_test_set_patient_labelled_training_is_refused_with_one_fixed_code(self) -> None:
        lake, _ = self.lake([self.row(self.TRAINING_PATIENT, "training"), self.row(self.TEST_PATIENT, "training")])
        self.assertEqual([SPLIT_REFUSAL], lake.refusals())

    def test_every_other_break_of_the_split_rule_is_the_same_code_and_never_a_count(self) -> None:
        wrong_version = {**self.row(self.TEST_PATIENT, "test"), "rule_version": "another"}
        twice = [*self.good(), self.row(self.TEST_PATIENT, "training")]
        for case, rows in (("another rule version", [self.good()[0], wrong_version]), ("a patient in two sets", twice), ("no row", [])):
            with self.subTest(case=case):
                lake, _ = self.lake(rows)
                self.assertEqual([SPLIT_REFUSAL], lake.refusals())

    def test_a_bronze_case_with_two_owners_is_refused_with_one_fixed_code(self) -> None:
        lake, _ = self.lake(self.good(), owner=1)
        self.assertEqual([OWNER_REFUSAL], lake.refusals())

    def test_both_refusals_are_named_in_one_run(self) -> None:
        lake, _ = self.lake([self.row(self.TEST_PATIENT, "training")], owner=1)
        self.assertEqual([SPLIT_REFUSAL, OWNER_REFUSAL], lake.refusals())

    def test_the_job_gives_its_refusals_to_the_run_in_the_order_that_the_run_reads_them(self) -> None:
        lake, _ = self.lake(self.good())
        operations = lake.operations()
        self.assertEqual(lake.refusals(), operations.refusals())
        self.assertEqual(lake.head, operations.head)


class Flags(unittest.TestCase):
    """The flag of a table is set by a held-out defect of its keys or of the join that its Silver build makes, and a join never gives a held-out count."""

    def lake(self, ones: list[str]) -> ReconcileLake:
        class Result:
            def __init__(self, value: int) -> None:
                self.value = value

            def collect(self) -> list[dict[str, int]]:
                return [{"n": self.value}]

        class Session:
            def sql(self, query: str) -> Result:
                return Result(1 if any(marker in query for marker in ones) else 0)

        return ReconcileLake(Session(), object(), "branch", "a" * 64)  # type: ignore[arg-type]

    def test_a_held_out_join_failure_sets_the_flag_of_its_table_only(self) -> None:
        owners = {
            "numeric_observations": "silver.numeric_observations x LEFT ANTI JOIN dev",
            "waveform_seconds": "silver.waveform_seconds x LEFT ANTI JOIN dev",
            "lab_results": "silver.lab_results x LEFT ANTI JOIN dev",
            "cases": "silver.cases x LEFT ANTI JOIN dev",
        }
        for name, marker in owners.items():
            lake = self.lake([marker])
            with self.subTest(name=name):
                self.assertEqual({other: other == name for other in owners}, {other: lake.held_out_problem(other) for other in owners})

    def test_a_held_out_key_defect_sets_the_flag_without_a_join_failure(self) -> None:
        lake = self.lake(["bf AS"])
        self.assertTrue(all(lake.held_out_problem(name) for name in ("numeric_observations", "cases")))
        self.assertFalse(self.lake([]).held_out_problem("cases"))


class Execute(unittest.TestCase):
    """``execute`` with the Spark session, the Nessie client and the settings replaced by stand-ins that record how they are used."""

    def run_execute(self, outcome: Any) -> tuple[list[tuple[Any, ...]], dict[str, Any] | None, Exception | None]:
        import spark.jobs.silver_reconcile as job

        calls: list[tuple[Any, ...]] = []

        class Client:
            def config(self) -> None:
                calls.append(("config",))

            def reference(self, name: str) -> Any:
                calls.append(("reference", name))
                return type("Reference", (), {"hash": "c" * 64})()

        class Session:
            def stop(self) -> None:
                calls.append(("stop",))

        def build_session(name: str, ref: str, ref_hash: str | None = None) -> Any:
            calls.append(("session", name, ref, ref_hash))
            return Session()

        def run_verify(operations: Any, head: str) -> Any:
            calls.append(("verify", head))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        settings = type("Settings", (), {"nessie_uri": "u", "nessie_timeout_seconds": 1.0, "deadline_seconds": 5.0})()
        patches = {
            "read_settings": lambda: settings,
            "open_client": lambda uri, timeout: Client(),
            "start_deadline": lambda seconds, job=None: calls.append(("deadline", seconds, job)),
            "build_session": build_session,
            "run_verify": run_verify,
        }
        originals = {name: getattr(job, name) for name in patches}
        for name, value in patches.items():
            setattr(job, name, value)
        try:
            result = job.execute(Namespace(mode="verify", branch="b"))
            error = None
        except Exception as caught:
            result, error = None, caught
        finally:
            for name, value in originals.items():
                setattr(job, name, value)
        return calls, result, error

    def test_the_session_and_every_read_are_pinned_to_the_hash_that_the_branch_had_at_the_start(self) -> None:
        outcome = type("Outcome", (), {"facts": lambda self: {"outcome": "verified"}})()
        calls, result, error = self.run_execute(outcome)
        self.assertIsNone(error)
        self.assertEqual(
            [
                ("deadline", 5.0, "silver_reconcile"),
                ("config",),
                ("reference", "b"),
                ("session", "silver_reconcile", "b", "c" * 64),
                ("verify", "c" * 64),
                ("stop",),
            ],
            calls,
        )
        self.assertEqual({"mode": "verify", "branch": "b", "job": "silver_reconcile", "outcome": "verified"}, result)

    def test_the_session_is_stopped_when_a_check_fails(self) -> None:
        calls, result, error = self.run_execute(ReconcileError("cases_key_unaccounted: 1 key"))
        self.assertIsNone(result)
        self.assertIsInstance(error, ReconcileError)
        self.assertEqual(("stop",), calls[-1])


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
