"""What the Silver signals and hypotension end-to-end runners print and save, tested on their own code with stand-ins for Docker, Nessie and Spark.

The runners ``tests/e2e/silver_signals.py`` and ``tests/e2e/silver_hypotension.py`` stay untracked, so each test that needs one skips when it is not on this
machine. The rule is the runner rule of owner decision D: a runner prints and saves only exit codes, fixed codes, Boolean flags, seconds, and snapshot IDs and
hashes that match their format and that Nessie confirms. A count, a row, a case ID or a digest may be recorded only when it is computed from development cases
alone by a query that cannot read a test-set case, and the saved field's name says so (``development_...``). A comparison that covers every case or any
test-set case happens, and only whether it matched is recorded.

Failure modes covered here:

- A runner prints a count that the independent read or the decoder answers (the rows of a table, the causes, the arterial seconds), or the rows of a case, or a
  case number in an event list, because a stand-in answered with them.
- A runner saves a digest of the events (which covers every case that the event table holds, a test-set case too, so it is no development-only value and no
  Nessie ID) in the facts that go to the record, or prints it, or the job writes one into its result for the runner to read.
- A rebuild that differs from the tables that the build wrote passes, because the runner no longer compares an events digest and must rest on the job's pass
  flags and the snapshot IDs.
- A stand-in answers a count, a list of rows or a list of events where one flag would do: the comparison is made in the container and only the flag leaves it.
- A refusal that a runner expects is not the line that the real command line prints (``source_table: 0 tables`` for ``source_table: failed``).
- The spec file of a stand-in (it holds the expected counts and the memberships of the patient sets) leaves the scratch folder that the run removes.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import importlib.util
import json
import re
import sys
import tempfile
import types
import unittest
from argparse import Namespace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from spark.silver import cli
from spark.silver.checks import FLAG, SilverSignalsError
from tests.e2e.run_record import Record
from tests.fast import test_silver_acceptance as accept
from tests.fast.silver_accept import HYPOTENSION_JOB, accept_refusal, accept_write

E2E = Path(__file__).resolve().parents[1] / "e2e"
HASH = re.compile(r"[0-9a-f]{64}")
SECONDS_AND_EXIT = re.compile(r"(?:exit|code) \d+|after \d+ seconds|\d+ seconds")
MARKERS = ("48171", "90417", "5521", "8812", "3331", "77123", "60917")


def runner(name: str) -> ModuleType:
    path = E2E / f"{name}.py"
    if not path.is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def digits_left(text: str) -> list[str]:
    """The digits of an observation after the exit codes, the seconds and the hashes (which Nessie confirmed) are taken out: none may be left."""
    rest = SECONDS_AND_EXIT.sub("", HASH.sub("", text))
    return re.findall(r"\d+", rest)


def record() -> Record:
    return Record(feature="test", slug="test", rerun="none")


class FakeNessie:
    """The Nessie calls that the cases of a runner make: a head for each reference, the table entries and the snapshot ID of each table."""

    def __init__(self, snapshots: dict[str, int] | None = None) -> None:
        self.snapshots = snapshots or {}
        self.heads: dict[str, str] = {}
        self.created: list[str] = []
        self.empty: set[str] = set()

    def reference(self, name: str) -> Any:
        return SimpleNamespace(hash=self.heads.setdefault(name, "c" * 64))

    def create_branch(self, name: str, base: str) -> None:
        self.created.append(name)

    def create_tag(self, name: str, hash_value: str, base: str) -> None:
        self.created.append(name)

    def merge(self, source: str, target: str, expected: str, message: str, source_hash: str | None = None) -> str:
        return "d" * 64

    def entries(self, name: str) -> list[str]:
        return [] if name in self.empty else list(self.snapshots)

    def request(self, method: str, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        table = path.rsplit("/contents/", 1)[-1]
        return {"content": {"snapshotId": self.snapshots[table]}} if table in self.snapshots else {}


class FakeStack:
    """The stack of a runner: every stand-in answers from ``answers`` by its mode, every other job from ``jobs``."""

    def __init__(self, module: ModuleType, answers: dict[str, dict[str, Any]], jobs: dict[str, dict[str, Any]] | None = None) -> None:
        self.module = module
        self.answers = answers
        self.jobs = jobs or {}
        self.specs: list[dict[str, Any]] = []
        self.data_directory = Path("/nonexistent")
        self.stalls: list[str] = []

    def call(self, document: dict[str, Any] | None) -> Any:
        return self.module.Call(0, document, 7.0, False)

    def write_spec(self, spec: dict[str, Any]) -> None:
        self.specs.append(spec)

    def stand_in(self, mode: str, branch: str) -> Any:
        return self.call(self.answers.get(mode, {}))

    def job(self, arguments: list[str], mounts: list[str] | None = None, kind: str = "silver") -> Any:
        return self.call(self.jobs.get(kind, {}))

    def spark_containers(self) -> list[str]:
        return []


def written_signals(**changes: Any) -> dict[str, Any]:
    return accept.written(**changes)


def done_for(module: ModuleType, document: dict[str, Any], catalog: Any, *, job: str | None = None) -> Any:
    call = module.Call(0, document, 9.0, False)
    return module.Done(job, call, catalog) if job else module.Done(call, catalog)


def bind_build_pair(run: Any, module: ModuleType, document: dict[str, Any], catalog: Any) -> None:
    """Make the build of the pair of jobs answer one accepted write, and the snapshot IDs of the tables those that Nessie holds."""
    done = done_for(module, document, catalog, job=module.JOB)
    run.build_pair = lambda base, label, case: ("hyp_b_1", done)
    held = {"silver.art_mbp_1s": 11, "silver.hypotension_events": 12, "silver.waveform_seconds": 1, "bronze.clinical_cases": 2, "silver.patient_split": 3}
    run.table_snapshot = lambda branch, table: held.get(table, 1)


def bind_silver(run: Any, module: ModuleType, document: dict[str, Any], catalog: Any) -> None:
    done = done_for(module, document, catalog, job=module.JOB)
    run.silver = lambda job, mode, branch, base=None: done


def observations(run: Any) -> list[str]:
    return [item.observed for item in run.record.observations]


def facts_text(run: Any) -> str:
    return json.dumps(run.facts, sort_keys=True, default=str)


class RealRefusal(unittest.TestCase):
    """The refusal that the real command line writes for a base without ``silver.waveform_seconds`` is the one that the runner expects."""

    def real(self) -> tuple[int, dict[str, Any]]:
        def execute(arguments: Namespace) -> dict[str, Any]:
            raise SilverSignalsError(f"source_table: {FLAG}")

        status, document, _ = cli.ending(Namespace(mode="build"), execute, HYPOTENSION_JOB)
        return status, document

    def test_the_refusal_of_the_real_command_line_passes_the_accept_function_with_the_fixed_code_and_the_flag(self) -> None:
        status, document = self.real()
        self.assertEqual({"job": HYPOTENSION_JOB, "mode": "build", "refusal": ["source_table: failed"]}, document)
        self.assertTrue(accept_refusal(status, document, HYPOTENSION_JOB, "build", ["source_table: failed"]).ok)

    def test_the_old_expectation_with_a_count_in_it_does_not_pass(self) -> None:
        status, document = self.real()
        self.assertFalse(accept_refusal(status, document, HYPOTENSION_JOB, "build", ["source_table: 0 tables"]).ok)

    def test_what_the_runner_expects_for_a_base_without_the_waveform_table_is_what_the_real_command_line_writes(self) -> None:
        source = Path(runner("silver_hypotension").__file__ or "").read_text()
        wanted: list[list[str]] = []
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "refused" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and str(first.value).startswith("a base without silver.waveform_seconds"):
                    wanted.append(ast.literal_eval(node.args[4]))
        self.assertEqual(1, len(wanted))
        status, document = self.real()
        self.assertTrue(accept_refusal(status, document, HYPOTENSION_JOB, "build", wanted[0]).ok, wanted)


class StandInScript(unittest.TestCase):
    """The stand-in programs run in a container with Spark. Their pure functions run here, with the Spark modules stood in for."""

    def load(self, name: str) -> dict[str, Any]:
        module = runner(name)
        stubs: dict[str, ModuleType] = {}
        for stub in ("pyspark", "pyspark.sql", "spark.bronze", "spark.bronze.tables", "spark.session"):
            stubs[stub] = types.ModuleType(stub)
        stubs["pyspark.sql"].functions = types.ModuleType("functions")  # type: ignore[attr-defined]
        stubs["spark.session"].build_session = lambda *args: None  # type: ignore[attr-defined]
        stubs["spark.bronze.tables"].NUMERIC = stubs["spark.bronze.tables"].WAVEFORM = stubs["spark.bronze.tables"].TRACKS = None  # type: ignore[attr-defined]
        stubs["spark.bronze"].tables = stubs["spark.bronze.tables"]  # type: ignore[attr-defined]
        source = module.STAND_IN.rsplit("\nmain()", 1)[0]
        with (
            tempfile.TemporaryDirectory(prefix="stand_in_") as folder,
            mock.patch.dict(sys.modules, {**stubs, "pyspark.sql.functions": stubs["pyspark.sql"].functions}),
        ):
            path = Path(folder) / f"{name}_stand_in.py"
            path.write_text(source)
            spec = importlib.util.spec_from_file_location(f"{name}_stand_in", path)
            if spec is None or spec.loader is None:
                raise unittest.SkipTest("the stand-in script cannot be loaded")
            loaded = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loaded)
        return vars(loaded)

    def test_a_row_check_answers_one_flag_and_the_rows_never_leave_the_container_for_numeric_readings(self) -> None:
        script = self.load("silver_signals")
        expected = [["Solar8000/ART_MBP", 1, 20.0, True, None, []], ["Solar8000/ART_MBP", 5, float("nan"), False, "missing", [1]]]
        rows = [
            {"track": "Solar8000/ART_MBP", "time": 1, "source_occurrence": 0, "value": 20.0, "ledger": [], "valid": True, "invalid_cause": None},
            {"track": "Solar8000/ART_MBP", "time": 5, "source_occurrence": 0, "value": float("nan"), "ledger": [1], "valid": False, "invalid_cause": "missing"},
        ]
        self.assertIs(True, script["numeric_rows_match"](rows, expected))
        self.assertIs(False, script["numeric_rows_match"](rows[:1], expected))
        self.assertIs(False, script["numeric_rows_match"]([*rows, {**rows[0], "time": 9}], expected))
        self.assertIs(False, script["numeric_rows_match"]([rows[0], {**rows[1], "ledger": [2]}], expected))
        self.assertIs(False, script["numeric_rows_match"]([rows[0], {**rows[1], "invalid_cause": "flat"}], expected))
        self.assertIs(False, script["numeric_rows_match"]([rows[0], {**rows[1], "source_occurrence": 1}], expected))
        self.assertIs(False, script["numeric_rows_match"]([{**rows[0], "value": 21.0}, rows[1]], expected))

    def test_a_row_check_answers_one_flag_for_the_waveform_seconds(self) -> None:
        script = self.load("silver_signals")
        expected = [[0, True, None], [30, False, "out_of_range"]]
        rows: list[dict[str, Any]] = [
            {"case_second": 0, "valid": True, "invalid_cause": None},
            {"case_second": 30, "valid": False, "invalid_cause": "out_of_range"},
        ]
        self.assertIs(True, script["waveform_rows_match"](rows, expected))
        self.assertIs(True, script["waveform_rows_match"]([*rows, {"case_second": 99, "valid": True, "invalid_cause": None}], expected))
        self.assertIs(False, script["waveform_rows_match"](rows[:1], expected))
        self.assertIs(False, script["waveform_rows_match"]([rows[0], {**rows[1], "invalid_cause": "flat"}], expected))
        self.assertIs(False, script["waveform_rows_match"]([rows[0], {**rows[1], "valid": True}], expected))

    def test_the_moved_null_is_one_flag(self) -> None:
        script = self.load("silver_signals")
        spec = {"second": 91, "cause": "missing", "samples": [None, 100.0, 120.0, 110.0]}
        row = {"case_second": 91, "invalid_cause": "missing", "samples": [None, 100.0, 120.0, 110.0]}
        self.assertIs(True, script["second_matches"]([row], spec))
        self.assertIs(False, script["second_matches"]([], spec))
        self.assertIs(False, script["second_matches"]([{**row, "samples": [100.0, None, 120.0, 110.0]}], spec))
        self.assertIs(False, script["second_matches"]([{**row, "invalid_cause": "flat"}], spec))

    def test_the_stand_in_answers_of_the_signals_runner_hold_no_number_but_a_flag(self) -> None:
        source = runner("silver_signals").STAND_IN
        emitted = re.findall(r"emit\((\{[^\n]*\})\)", source)
        self.assertTrue(emitted)
        for text in emitted:
            self.assertEqual([], re.findall(r"\b\d+\b", text), text)

    def test_the_synthetic_patients_that_the_runner_computes_are_the_ones_that_the_stand_in_chooses(self) -> None:
        script = self.load("silver_hypotension")
        module = runner("silver_hypotension")
        self.assertEqual(script["synthetic_patient"](True, 9_100_000), module.synthetic_patient(True, 9_100_000))
        self.assertEqual(script["synthetic_patient"](False, 9_100_000), module.synthetic_patient(False, 9_100_000))
        self.assertNotEqual(module.synthetic_patient(True, 9_100_000), module.synthetic_patient(False, 9_100_000))

    def test_the_probes_and_the_plant_events_are_two_flags(self) -> None:
        script = self.load("silver_hypotension")
        stored = {(1, 10): {"value": 70.0, "valid": True, "invalid_cause": None}, (1, 11): {"value": None, "valid": False, "invalid_cause": "flat"}}
        wanted = [[10, 70.0, True, None], [11, None, False, "flat"]]
        self.assertIs(True, script["probes_match"](stored, 1, wanted))
        self.assertIs(False, script["probes_match"](stored, 1, [*wanted, [12, 60.0, True, None]]))
        self.assertIs(False, script["probes_match"](stored, 2, wanted))
        self.assertIs(False, script["probes_match"](stored, 1, [[10, 71.0, True, None], wanted[1]]))
        rows = [
            {"case_id": 1, "onset_second": 2011, "duration_seconds": 60, "end_reason": "recovered"},
            {"case_id": 2, "onset_second": 5, "duration_seconds": 61, "end_reason": "x"},
        ]
        self.assertIs(True, script["plant_events_match"](rows, 1, [[2011, 60, "recovered"]]))
        self.assertIs(False, script["plant_events_match"](rows, 1, [[2011, 59, "recovered"]]))
        self.assertIs(False, script["plant_events_match"]([{**rows[0], "duration_seconds": 61}], 1, [[2011, 60, "recovered"]]))
        self.assertIs(False, script["plant_events_match"]([{**rows[0], "end_reason": "operation_end"}], 1, [[2011, 60, "recovered"]]))
        self.assertIs(False, script["plant_events_match"](rows, 1, []))
        self.assertIs(False, script["plant_events_match"](rows[:1], 3, [[2011, 60, "recovered"]]))

    def test_the_listed_events_are_one_flag_over_the_listed_cases_only(self) -> None:
        script = self.load("silver_hypotension")
        expected = {(1, 5, 60), (9, 5, 70)}
        self.assertIs(True, script["listed_events_equal"](expected, {(1, 5, 60)}, [1]))
        self.assertIs(False, script["listed_events_equal"](expected, set(), [1]))
        self.assertIs(False, script["listed_events_equal"](expected, {(1, 5, 61)}, [1]))
        self.assertIs(True, script["listed_events_equal"](expected, expected, [1, 9]))


class SignalsRecord(unittest.TestCase):
    """The cases of the signals runner record flags only, whatever a stand-in or the decoder answers."""

    def run_of(self, answers: dict[str, dict[str, Any]], jobs: dict[str, dict[str, Any]] | None = None) -> tuple[Any, ModuleType]:
        module = runner("silver_signals")
        stack = FakeStack(module, answers, jobs)
        snapshots = {"bronze.numeric_observations": 1, "bronze.waveform_seconds": 2, "bronze.tracks": 3}
        run = module.Run(stack, FakeNessie(snapshots), record(), {case: {"sha256": "e" * 64} for case in module.CASES})
        return run, module

    def assert_flags_only(self, run: Any) -> None:
        for text in observations(run):
            self.assertEqual([], digits_left(text), text)
            for marker in MARKERS:
                self.assertNotIn(marker, text)
        for marker in MARKERS:
            self.assertNotIn(marker, facts_text(run))

    def test_the_decoder_job_counts_are_compared_and_never_recorded(self) -> None:
        decoder = {
            "tables": {
                "numeric_observations": {"rows": 48171, "duplicate_keys": 0},
                "waveform_seconds": {"rows": 90417, "duplicate_keys": 0},
                "tracks": {"rows": 5521, "duplicate_keys": 0},
            },
            "seconds": {"908": 8812},
        }
        run, _ = self.run_of({"check_plant": {"arterial_track_of_the_plant_case": 1, "plant_case_has_one_arterial_track": True}}, {"decoder": decoder})
        self.assertTrue(run.setup())
        self.assert_flags_only(run)

    def test_a_decoder_with_an_empty_table_or_a_duplicate_key_fails_the_setup(self) -> None:
        for name, change in (("an empty table", {"rows": 0}), ("a duplicate key", {"duplicate_keys": 1})):
            with self.subTest(name):
                tables = {key: {"rows": 5, "duplicate_keys": 0} for key in ("numeric_observations", "waveform_seconds", "tracks")}
                tables["tracks"].update(change)
                run, _ = self.run_of(
                    {"check_plant": {"plant_case_has_one_arterial_track": True, "arterial_track_of_the_plant_case": 1}}, {"decoder": {"tables": tables}}
                )
                self.assertFalse(run.setup())

    def test_the_independent_read_records_its_flags_and_none_of_the_counts_that_the_stand_in_answers(self) -> None:
        answer = {
            "numeric": {"absent": 0, "verdict_mismatch": 0, "ledger_mismatch": 0, "value_mismatch": 0, "expected_rows": 44840},
            "waveform": {"absent": 0, "verdict_mismatch": 0, "sample_mismatch": 0, "field_mismatch": 0, "expected_rows": 90417},
            "bronze_numeric": 48171,
            "bronze_duplicates": 3331,
            "bronze_waveform": 90417,
            "silver_numeric": 44840,
            "silver_waveform": 90417,
            "numeric_causes": {"valid": 5521},
            "waveform_causes": {"valid": 77123},
            "numeric_matches_bronze": True,
            "waveform_matches_bronze": True,
            "readings_add_up": True,
            "schema": {"numeric": [], "waveform": []},
        }
        run, module = self.run_of({"dump_control": answer})
        answer["schema"] = {
            "numeric": [list(item) for item in module.NUMERIC_TABLE.columns],
            "waveform": [list(item) for item in module.WAVEFORM_TABLE.columns],
        }
        run.independent({"branch": "b"})
        self.assertEqual([True], [item.passed for item in run.record.observations])
        self.assert_flags_only(run)

    def test_each_flag_of_the_independent_read_is_needed(self) -> None:
        for flag in ("numeric_matches_bronze", "waveform_matches_bronze", "readings_add_up"):
            with self.subTest(flag):
                run, module = self.run_of({})
                answer: dict[str, Any] = {name: True for name in ("numeric_matches_bronze", "waveform_matches_bronze", "readings_add_up")}
                answer[flag] = False
                answer["schema"] = {
                    "numeric": [list(item) for item in module.NUMERIC_TABLE.columns],
                    "waveform": [list(item) for item in module.WAVEFORM_TABLE.columns],
                }
                run.stack.answers["dump_control"] = answer
                run.independent({"branch": "b"})
                self.assertEqual([False], [item.passed for item in run.record.observations])
                answer[flag] = True
                run.independent({"branch": "b"})
                self.assertEqual([False, True], [item.passed for item in run.record.observations])

    def test_the_rows_that_a_stand_in_reads_back_are_one_flag_and_never_printed(self) -> None:
        run, module = self.run_of(
            {"plant_numeric": {}, "dump_rows": {"rows": [{"track": "x", "time": 77123, "value": 5521.0}]}, "check_rows": {"rows_as_expected": True}}
        )
        write = written_signals()
        catalog = accept.WROTE
        run.silver = lambda mode, branch, base=None: done_for(module, write, catalog)
        run.plant_numeric()
        text = observations(run)[0]
        self.assertEqual([], digits_left(text), text)
        self.assertEqual("planted: a duplicate source reading and the numeric limits", run.record.observations[0].case)

    def test_the_two_tables_after_an_empty_rebuild_are_two_flags(self) -> None:
        run, module = self.run_of(
            {
                "bronze_empty_waveform": {},
                "dump_control": {"silver_waveform": 0, "silver_numeric": 44840, "waveform_table_empty": True, "numeric_table_has_rows": True},
            }
        )
        write = written_signals()
        run.silver = lambda mode, branch, base=None: done_for(module, write, accept.WROTE)
        run.empty_rebuild()
        text = observations(run)[0]
        self.assertEqual([], digits_left(text), text)
        self.assertNotIn("44840", text)

    def test_no_container_left_is_a_flag(self) -> None:
        run, module = self.run_of({})
        run.stack.spark_containers = lambda: ["a", "b", "c"]
        run.silver = lambda mode, branch, base=None: done_for(module, accept.refusal(), accept.WROTE)
        run.bounded_failure()
        for text in observations(run):
            self.assertEqual([], digits_left(text), text)


class HypotensionRecord(unittest.TestCase):
    """The cases of the hypotension runner record flags only, and keep no digest, no event row and no case number."""

    def run_of(self, answers: dict[str, dict[str, Any]], jobs: dict[str, dict[str, Any]] | None = None) -> tuple[Any, ModuleType]:
        module = runner("silver_hypotension")
        stack = FakeStack(module, answers, jobs)
        snapshots = {"silver.art_mbp_1s": 11, "silver.hypotension_events": 12, "bronze.numeric_observations": 1}
        clinical = [[str(case), str(9_000_000 + case), "500", "3950"] for case in module.CASES]
        nessie = FakeNessie(snapshots)
        nessie.heads["hyp_b_1"] = accept.WRITE_HASH
        run = module.Run(stack, nessie, record(), {case: {"sha256": "e" * 64} for case in module.CASES}, clinical)
        nessie.empty.add(run.empty)
        return run, module

    def assert_flags_only(self, run: Any) -> None:
        for text in observations(run):
            self.assertEqual([], digits_left(text), text)
            for marker in MARKERS:
                self.assertNotIn(marker, text)
        for marker in MARKERS:
            self.assertNotIn(marker, facts_text(run))

    def oracle_answer(self, module: ModuleType, **changes: Any) -> dict[str, Any]:
        base = {
            "arterial_seconds": 48171,
            "series_rows": 48171,
            "series_absent": 0,
            "series_mismatch": 0,
            "events_equal": 1,
            "event_times_flag": 0,
            "series_as_expected": True,
            "events_as_expected": True,
            "event_times_ok": True,
            "listed_events_equal": True,
            "expected_events": [[908, 3331, 60]],
            "stored_events": [[908, 3331, 60]],
            "schema": {
                "series": [list(item) for item in module.ART_MBP_1S.columns],
                "events": [list(item) for item in module.HYPOTENSION_EVENTS.columns],
            },
        }
        return {**base, **changes}

    def test_the_decoder_counts_and_the_small_tables_are_compared_and_never_recorded(self) -> None:
        decoder = {
            "tables": {name: {"rows": 48171, "duplicate_keys": 0} for name in ("numeric_observations", "waveform_seconds", "tracks")},
            "seconds": {"908": 8812},
        }
        run, _ = self.run_of(
            {"load_small": {"clinical_rows": 4, "split_rows": 4, "clinical_rows_as_expected": True, "split_rows_as_expected": True}}, {"decoder": decoder}
        )
        self.assertTrue(run.setup())
        self.assert_flags_only(run)

    def test_the_independent_read_records_flags_and_no_count(self) -> None:
        run, module = self.run_of({})
        run.stack.answers["dump_oracle"] = self.oracle_answer(module)
        result = run.oracle("b", "the fixed sample", run.clinical, list_cases=list(module.CASES))
        self.assertEqual([True], [item.passed for item in run.record.observations])
        self.assertTrue(result)
        self.assert_flags_only(run)

    def test_each_flag_of_the_independent_read_is_needed(self) -> None:
        for flag in ("series_as_expected", "events_as_expected", "event_times_ok"):
            with self.subTest(flag):
                run, module = self.run_of({})
                run.stack.answers["dump_oracle"] = self.oracle_answer(module, **{flag: False})
                run.oracle("b", "x", run.clinical)
                self.assertEqual([False], [item.passed for item in run.record.observations])

    def test_the_event_list_of_the_fixed_sample_is_one_flag_with_no_row_and_no_case_number(self) -> None:
        run, module = self.run_of({})
        run.stack.answers["dump_oracle"] = self.oracle_answer(module)
        run.control_events({"branch": "b"})
        for item in run.record.observations:
            self.assertEqual([], digits_left(item.observed), item.observed)
        self.assertNotIn("fixed_sample_expected_events", run.facts)
        self.assertEqual([True, True], [item.passed for item in run.record.observations])

    def test_a_listed_event_list_that_differs_fails(self) -> None:
        run, module = self.run_of({})
        run.stack.answers["dump_oracle"] = self.oracle_answer(module, listed_events_equal=False)
        run.control_events({"branch": "b"})
        self.assertFalse(run.record.observations[-1].passed)

    def hypotension_documents(self, **changes: Any) -> tuple[dict[str, Any], Any]:
        document = accept.hypotension(branch="b", **changes)
        catalog = accept.HYPO_WROTE
        return document, catalog

    def test_the_control_records_no_digest_and_a_result_that_holds_one_is_not_accepted(self) -> None:
        run, module = self.run_of({})
        document, catalog = self.hypotension_documents()
        bind_build_pair(run, module, document, catalog)
        run.control()
        self.assertNotIn("digest", facts_text(run))
        self.assertNotIn("digest", " ".join(observations(run)))
        self.assertTrue(run.record.observations[-1].passed)
        for marker in MARKERS:
            self.assertNotIn(marker, facts_text(run))
            for text in observations(run):
                self.assertNotIn(marker, text)
        document, catalog = self.hypotension_documents(development_events_digest=90417)
        self.assertFalse(accept_write(0, document, HYPOTENSION_JOB, catalog).ok)

    def test_the_runner_reads_no_digest_from_a_result(self) -> None:
        source = Path(runner("silver_hypotension").__file__ or "").read_text()
        reads = [line for line in source.splitlines() if re.search(r"events_digest(?!\": 9)", line) and "extra field" not in line]
        self.assertEqual([], reads)

    def test_a_rebuild_that_is_the_same_tables_passes_and_a_rebuild_that_differs_fails(self) -> None:
        both = {"art_mbp_1s": True, "hypotension_events": True}
        moved = {**accept.HYPO_SNAPSHOTS, "hypotension_events": 13}
        for label, changes, catalog_changes, passed in (
            ("the same tables", {}, {}, True),
            ("the events table differs from the plan", {"passed": {**both, "hypotension_events": False}}, {}, False),
            ("the series differs from the plan", {"passed": {**both, "art_mbp_1s": False}}, {}, False),
            ("another snapshot of the events that Nessie holds too", {"snapshots": moved}, {"snapshots": moved}, False),
        ):
            with self.subTest(label):
                run, module = self.run_of({})
                document, catalog = self.hypotension_documents()
                bind_build_pair(run, module, document, catalog)
                built = run.control()
                kept = {
                    **accept.hypotension(branch="b"),
                    "write_hash": None,
                    "branch_hash": accept.HEAD,
                    "snapshots": dict(accept.HYPO_SNAPSHOTS),
                    **changes,
                }
                kept_catalog = dataclasses.replace(accept.HYPO_KEPT, snapshots=catalog_changes.get("snapshots", accept.HYPO_KEPT.snapshots))
                bind_silver(run, module, kept, kept_catalog)
                with mock.patch.object(run, "head", return_value=accept.HEAD):
                    run.merge_tag_rebuild(built)
                self.assertEqual(passed, run.record.observations[-1].passed)

    def test_the_plant_records_flags_for_the_events_and_the_probed_readings(self) -> None:
        run, module = self.run_of({})
        run.stack.answers.update(
            {
                "load_plant": {
                    "rows_as_expected": True,
                    "waveform_rows": 48171,
                    "clinical_rows": 2,
                    "split_rows": 2,
                    "patients": {"9000001": "9100001", "9000002": "9100002"},
                },
                "dump_oracle": self.oracle_answer(
                    module,
                    probes_match=True,
                    plant_events_match=True,
                    held_out_seconds_in_input=True,
                    held_out_series_built=True,
                    held_out_event_rows=False,
                    probes={"1010": [70.0, True, None]},
                    plant_events=[[2011, 60, "recovered"]],
                ),
            }
        )
        document, catalog = self.hypotension_documents()
        bind_build_pair(run, module, document, catalog)
        run.verify = lambda branch, label, expected: None
        self.assertIsNotNone(run.plant())
        self.assertTrue(all(item.passed for item in run.record.observations), [item.case for item in run.record.observations if not item.passed])
        for item in run.record.observations:
            self.assertEqual([], digits_left(item.observed), item.observed)
        self.assert_flags_only(run)

    def test_a_probe_flag_that_is_false_fails_the_plant(self) -> None:
        for name in ("probes_match", "plant_events_match"):
            with self.subTest(name):
                run, module = self.run_of({})
                flags = {
                    "probes_match": True,
                    "plant_events_match": True,
                    "held_out_seconds_in_input": True,
                    "held_out_series_built": True,
                    "held_out_event_rows": False,
                    name: False,
                }
                run.stack.answers.update({"load_plant": {"rows_as_expected": True}, "dump_oracle": self.oracle_answer(module, **flags)})
                document, catalog = self.hypotension_documents()
                bind_build_pair(run, module, document, catalog)
                run.verify = lambda branch, label, expected: None
                run.plant()
                self.assertFalse(all(item.passed for item in run.record.observations))


class RunnerSource(unittest.TestCase):
    """The text of the runners: what a refusal expects, where a spec file lives and what a log line holds."""

    NAMES = ("silver_signals", "silver_hypotension", "silver_patient_split", "silver_cases")

    def sources(self) -> dict[str, str]:
        found = {name: (E2E / f"{name}.py").read_text() for name in self.NAMES if (E2E / f"{name}.py").is_file()}
        if not found:
            self.skipTest("the untracked runners are not on this machine")
        return found

    def test_the_spec_file_is_written_only_into_the_data_folder_that_the_scratch_folder_holds_and_the_run_removes(self) -> None:
        for name, source in self.sources().items():
            with self.subTest(runner=name):
                texts = [line.strip() for line in source.splitlines() if '"spec.json"' in line and "open(" not in line]
                self.assertEqual(['path = self.data_directory / "spec.json"'], texts)
                self.assertIn('data = scratch / "data"', source)

    def test_no_file_of_the_data_folder_is_copied_to_the_record(self) -> None:
        for name, source in self.sources().items():
            with self.subTest(runner=name):
                for line in source.splitlines():
                    if re.search(r"shutil\.(?:copyfile|copytree|copy|move)\(", line) and "data" in line.split("(", 1)[1].split(",", 1)[0]:
                        self.assertIn("/ ", line, line)
                        self.assertNotIn("spec", line, line)
                self.assertNotRegex(source, r"copy\w*\([^)]*spec")

    def test_the_run_removes_the_scratch_folder_in_a_finally_block(self) -> None:
        for name, source in self.sources().items():
            with self.subTest(runner=name):
                removing = [
                    any(re.search(r"(?:rmtree|scrub_tree)\(scratch", ast.unparse(statement)) for statement in node.finalbody)
                    for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Try)
                ]
                self.assertIn(True, removing)


class Planted(unittest.TestCase):
    """The checks above fail on the defect that each is for."""

    def test_a_runner_that_prints_a_count_fails_the_digit_rule(self) -> None:
        self.assertEqual(["48171"], digits_left("exit 0 after 12 seconds, rows 48171"))
        self.assertEqual([], digits_left("exit 0 after 12 seconds, flags True"))
        self.assertEqual([], digits_left(f"hash {'a' * 64}, exit 1"))

    def test_the_helpers_read_a_text_that_is_not_there_as_a_skip_not_a_pass(self) -> None:
        with mock.patch.object(Path, "is_file", return_value=False), self.assertRaises(unittest.SkipTest):
            runner("silver_signals")


if __name__ == "__main__":
    unittest.main()
