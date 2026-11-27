"""The command line of the reconciliation job prints fixed codes and never an argument, a branch name or the text of an upstream error, and the job holds the
image's modules.

Failure modes covered here:

- A mode that writes (``build``) is accepted by a job that only reads.
- An extra or malformed argument, or a branch name that Nessie would refuse, is echoed by the argument parser.
- A failed check, a missing branch, a settings error or any other error carries text of its own into the output.
- The watchdog that stops a job that runs too long names another job or holds a duration.
- The job is told to read a table at the head of the branch instead of the hash that it resolved, so a branch that moves changes what it reads.
- A module that the job imports, directly or through another, is left out of the image or of the build context.
- The result reaches the caller through the free text of the job's two streams, so that a line of them can carry a value or be read as a result. It is one
  file at a path on the command line, written whole into its part file and then linked, with a closed schema, and it never overwrites a file.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.nessie import NessieError
from spark.jobs.silver_reconcile import ReconcileLake
from spark.reconcile.checks import OWNER_REFUSAL, SOURCE_TABLES, SPLIT_REFUSAL, ReconcileError
from spark.reconcile.cli import JOB, run
from spark.settings import SettingsError
from spark.silver_cases_sql import ownership_query
from spark.split_cli import start_deadline
from spark.split_table import RULE_VERSION
from tests.fast.internal_line_support import assert_internal
from tests.fast.test_replay_image import ignore_rules, imported_files
from tests.fast.test_silver_signals_image import ROOT
from tests.fast.test_spark_bronze_image import DOCKERFILE, IGNORE_FILE, image_problems

MARKER = "MARKER_5521"


def invoke(argv: list[str], execute: Any = None, folder: str | None = None) -> tuple[int, str, str]:
    """Run the command line. A ``verify`` call with no ``--result`` gets one in ``folder`` (a fresh folder of the caller), so that it is judged for its own
    arguments."""
    out, err = io.StringIO(), io.StringIO()
    code = 0
    call = [*argv, "--result", f"{folder}/result.json"] if folder is not None and "verify" in argv and "--result" not in argv else argv
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = run(call, execute or (lambda arguments: {"mode": arguments.mode, "branch": arguments.branch}))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def raising(error: Exception) -> Any:
    def execute(_arguments: Namespace) -> dict[str, Any]:
        raise error

    return execute


class CommandLine(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="reconcile_cli_"))

    def call(self, argv: list[str], execute: Any = None) -> tuple[int, str, str]:
        return invoke(argv, execute, self.folder)

    def test_a_good_call_prints_nothing_and_the_result_is_the_file(self) -> None:
        code, out, err = self.call(["verify", "--branch", "b"])
        self.assertEqual((0, "", ""), (code, out, err))
        self.assertEqual({"mode": "verify", "branch": "b", "diagnostics": []}, read_file(self.folder))

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
                code, out, err = self.call(argv)
                self.assertEqual((2, "", "silver_reconcile: usage\n"), (code, out, err))
        self.assertEqual([], os.listdir(self.folder))

    def test_a_failed_check_prints_each_problem_as_a_code_and_a_count_and_exits_1(self) -> None:
        code, out, err = self.call(
            ["verify", "--branch", "b"], raising(ReconcileError("cases_key_unaccounted: 3 keys", "lab_results_held_out: a held-out key"))
        )
        self.assertEqual((1, ""), (code, out))
        self.assertEqual("silver_reconcile: cases_key_unaccounted: 3 keys\nsilver_reconcile: lab_results_held_out: a held-out key\n", err)

    def test_an_error_of_nessie_is_a_code_and_never_its_text(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        for error, expected in ((missing, "branch_missing"), (other, "catalog_error"), (NessieError(MARKER, status=404, error_code="OTHER"), "catalog_error")):
            with self.subTest(expected=expected):
                if os.path.lexists(f"{self.folder}/result.json"):
                    os.remove(f"{self.folder}/result.json")
                code, out, err = self.call(["verify", "--branch", "b"], raising(error))
                self.assertEqual((1, "", f"silver_reconcile: {expected}\n"), (code, out, err))

    def test_a_setting_that_cannot_be_used_and_any_other_error_are_codes(self) -> None:
        code, out, err = self.call(["verify", "--branch", "b"], raising(SettingsError(MARKER)))
        self.assertEqual((2, "", "silver_reconcile: settings\n"), (code, out, err))
        os.remove(f"{self.folder}/result.json")
        code, out, err = self.call(["verify", "--branch", "b"], raising(RuntimeError(MARKER)))
        self.assertEqual((70, ""), (code, out))
        assert_internal(self, err, "silver_reconcile", "silver_reconcile: internal_error", (), MARKER)


def read_file(folder: str) -> Any:
    return json.loads(Path(f"{folder}/result.json").read_text())


class ResultFile(unittest.TestCase):
    """The result is one file at ``--result``: the closed schema, written whole and linked into place, for every ending that the job maps to a code."""

    def setUp(self) -> None:
        self.folder = self.enterContext(tempfile.TemporaryDirectory(prefix="reconcile_result_"))
        self.path = f"{self.folder}/result.json"

    def call(self, execute: Any = None, argv: list[str] | None = None) -> tuple[int, str, str]:
        return invoke(argv or ["verify", "--branch", "b", "--result", self.path], execute)

    def test_a_clean_run_writes_exactly_its_result_and_an_empty_list_of_diagnostics(self) -> None:
        facts = {"outcome": "verified", "branch_hash": "a" * 64, "reconciled": {}}
        code, out, _ = self.call(lambda arguments: {"mode": arguments.mode, "branch": arguments.branch, **facts})
        self.assertEqual(0, code)
        self.assertEqual({"mode": "verify", "branch": "b", **facts, "diagnostics": []}, read_file(self.folder))
        self.assertEqual(["result.json"], os.listdir(self.folder))
        self.assertEqual("", out)

    def test_the_file_is_the_same_whatever_the_streams_hold(self) -> None:
        def noisy(arguments: Namespace) -> dict[str, Any]:
            sys.stdout.write("WARN a line that no reader may rely on\n")
            sys.stderr.write(f"WARNING: Using incubator modules: jdk.incubator.vector {MARKER}\n")
            return {"mode": arguments.mode, "branch": arguments.branch}

        code, _, _ = self.call(noisy)
        self.assertEqual(0, code)
        self.assertEqual({"mode": "verify", "branch": "b", "diagnostics": []}, read_file(self.folder))

    def test_a_failed_check_writes_each_problem_as_a_code_and_a_count_or_no_count_and_the_flag_of_each_table(self) -> None:
        flags = {"numeric_observations": True, "waveform_seconds": True, "cases": True, "lab_results": False}
        error = ReconcileError("cases_key_unaccounted: 3 keys", "case_split_join_empty: 0 rows", "lab_results_held_out: a held-out key", held_out_clean=flags)
        code, out, _ = self.call(raising(error))
        self.assertEqual((1, ""), (code, out))
        self.assertEqual(
            {
                "mode": "verify",
                "branch": "b",
                "job": JOB,
                "outcome": "failed",
                "diagnostics": [
                    {"code": "cases_key_unaccounted", "count": 3},
                    {"code": "case_split_join_empty", "count": 0},
                    {"code": "lab_results_held_out", "count": None},
                ],
                "held_out_clean": flags,
            },
            read_file(self.folder),
        )

    def test_a_refusal_and_a_failure_before_the_flags_hold_no_count_and_no_flag(self) -> None:
        code, _, _ = self.call(raising(ReconcileError(SPLIT_REFUSAL, OWNER_REFUSAL)))
        self.assertEqual(1, code)
        document = read_file(self.folder)
        self.assertEqual(
            [{"code": "split_rule_broken", "count": None}, {"code": "ambiguous_case_owner", "count": None}],
            document["diagnostics"],
        )
        self.assertIsNone(document["held_out_clean"])

    def test_every_other_ending_is_one_code_in_the_file_and_the_text_of_the_error_is_not(self) -> None:
        missing = NessieError(f"{MARKER} not found", status=404, error_code="REFERENCE_NOT_FOUND")
        other = NessieError(f"{MARKER} refused", status=409, error_code="REFERENCE_CONFLICT")
        endings = (
            (missing, 1, "branch_missing"),
            (other, 1, "catalog_error"),
            (SettingsError(MARKER), 2, "settings"),
            (RuntimeError(MARKER), 70, "internal_error"),
        )
        for error, status, expected in endings:
            with self.subTest(expected=expected):
                code, _, _ = self.call(raising(error))
                self.assertEqual(status, code)
                document = read_file(self.folder)
                self.assertEqual([{"code": expected, "count": None}], document["diagnostics"])
                self.assertEqual({"mode", "branch", "job", "outcome", "diagnostics", "held_out_clean"}, set(document))
                self.assertEqual(("failed", None), (document["outcome"], document["held_out_clean"]))
                self.assertNotIn(MARKER, Path(self.path).read_text())
                os.remove(self.path)

    def test_a_file_that_is_there_is_never_replaced_and_the_job_does_not_run(self) -> None:
        Path(self.path).write_text("old")
        ran: list[str] = []

        def execute(arguments: Namespace) -> dict[str, Any]:
            ran.append("run")
            return {}

        code, out, err = self.call(execute)
        self.assertEqual((2, "", "silver_reconcile: usage\n"), (code, out, err))
        self.assertEqual(([], "old"), (ran, Path(self.path).read_text()))
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_path_that_cannot_hold_a_result_is_usage_and_is_not_echoed(self) -> None:
        Path(f"{self.folder}/{MARKER}_directory").mkdir()
        Path(f"{self.folder}/{MARKER}_file").write_text("x")
        paths = (
            "",
            "result.json",
            f"./{MARKER}.json",
            f"{self.folder}/{MARKER}_missing/result.json",
            f"{self.folder}/{MARKER}_file/result.json",
            f"{self.folder}/{MARKER}_directory",
            f"{self.folder}/{MARKER}_dangling",
            f"{self.folder}/result\x00.json",
        )
        os.symlink(f"{self.folder}/{MARKER}_nowhere", f"{self.folder}/{MARKER}_dangling")
        for path in paths:
            with self.subTest(path=path):
                code, out, err = invoke(["verify", "--branch", "b", "--result", path], lambda arguments: {})
                self.assertEqual((2, "", "silver_reconcile: usage\n"), (code, out, err))
        self.assertEqual({f"{MARKER}_directory", f"{MARKER}_file", f"{MARKER}_dangling"}, set(os.listdir(self.folder)))

    def test_a_file_of_the_old_part_name_does_not_refuse_the_path_because_the_job_no_longer_writes_one(self) -> None:
        Path(f"{self.path}.part").write_text("x")
        code, _, _ = self.call()
        self.assertEqual(0, code)
        self.assertEqual(["result.json", "result.json.part"], sorted(os.listdir(self.folder)))

    def test_the_result_option_is_given_once_and_is_required(self) -> None:
        for argv in (
            ["verify", "--branch", "b"],
            ["verify", "--branch", "b", "--result"],
            ["verify", "--branch", "b", "--result", self.path, "--result", self.path + "2"],
        ):
            with self.subTest(argv=argv):
                code, out, err = invoke(argv, lambda arguments: {})
                self.assertEqual((2, "", "silver_reconcile: usage\n"), (code, out, err))
        self.assertEqual([], os.listdir(self.folder))

    def test_the_file_is_not_there_while_the_job_runs_and_appears_whole_in_one_link(self) -> None:
        seen: dict[str, Any] = {}

        def during(arguments: Namespace) -> dict[str, Any]:
            seen["during"] = os.listdir(self.folder)
            return {"mode": arguments.mode, "branch": arguments.branch}

        real = os.link

        def link(source: str, target: str) -> None:
            seen["target_before"] = os.path.lexists(target)
            seen["whole"] = json.loads(Path(source).read_text())
            seen["names"] = (os.path.basename(source).startswith(".") and os.path.basename(source).endswith(".part"), os.path.basename(target))
            real(source, target)

        with mock.patch("scripts.ingestion.output_file.os.link", link):
            code, _, _ = self.call(during)
        self.assertEqual(0, code)
        self.assertEqual([], seen["during"])
        self.assertIs(False, seen["target_before"])
        self.assertEqual({"mode": "verify", "branch": "b", "diagnostics": []}, seen["whole"])
        self.assertEqual((True, "result.json"), seen["names"])
        self.assertEqual(["result.json"], os.listdir(self.folder))

    def test_a_file_that_cannot_be_written_is_internal_error_with_no_file_and_no_part_left_and_no_result_line(self) -> None:
        def refuse(source: str, target: str) -> None:
            raise OSError(f"{MARKER} disk full")

        with mock.patch("scripts.ingestion.output_file.os.link", refuse):
            code, out, err = self.call()
        self.assertEqual((70, ""), (code, out))
        assert_internal(self, err, "silver_reconcile", "silver_reconcile: internal_error", (), MARKER)
        self.assertEqual([], os.listdir(self.folder))

    def test_the_file_is_readable_by_another_user_and_is_not_executable(self) -> None:
        self.call()
        self.assertEqual(0o644, os.stat(self.path).st_mode & 0o777)


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
