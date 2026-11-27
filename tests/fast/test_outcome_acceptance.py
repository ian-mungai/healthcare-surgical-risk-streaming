"""The pass or fail rules that the end-to-end runs of the six outcome runners share, tested on the results of the real jobs.

No Spark runs here. The six runners (Bronze, its container helper, patient split, Silver cases, signals, hypotension) stay untracked, so each rule that
decides from a job's output lives in ``tests/fast/outcome_accept.py``, where this module tests it. The rules read one result file with a closed schema,
hashes, snapshot IDs and flags, and never a count, a digest or a field that says whether a table was written. The good documents here are made by the jobs' own
code (``ending``, ``result_document``), so a field that a job gains or loses fails a rule.

Failure modes covered here:

- A result file that is not one plain file holding one JSON document is read, or a file that a job left beside it.
- A result with a field that the job does not write (at the top, in a table or in a check) passes, or one with a flag that is not a Boolean, a snapshot ID
  that is a Boolean or a check that the job does not make.
- A result of another job, command, mode or branch than the one that was asked for passes, or a result that disagrees with its exit status.
- A table with a failed value, row-count or key check passes as equal to the loaded table, whether it is the clinical or the laboratory table.
- A build that moved the branch passes as a rebuild that wrote nothing, or the reverse.
- A refusal with another code, another number of codes or a count passes.
- A result of the split or the Silver cases job passes with text that is not a hash in a hash field, a snapshot ID that is missing, null, a Boolean or text,
  an empty map of snapshot IDs, an eighth key or a missing one among the seven tables that the cases job reads.
- The comparison of the snapshot IDs with Nessie is driven by what the document holds, so that a key that the job added is looked up and a key that it dropped
  is never missed.
- A job that the watchdog stopped leaves its part file, and a reader refuses the result folder for it.
- A runner records the length of a stream, a tail of one, a stream in a log line, a count in a log line or an observation, a count that a stand-in prints
  or emits, a digest that is saved, a refusal expectation with a count in it, or an identifier that it chose by looking at data (``runner_leaks``). The
  Silver signals and hypotension runners are linted like the other four.
- An observation, a fact or a printed line of one of the eight runners (the six above, the Gold dimensions runner and the signal profile runner) is anything
  but the output of the one sealed helper (``sink_leaks``): an f-string, a ``str()``, ``repr()`` or ``format`` text, a raw variable, a hand-made
  ``ConfirmedId``, a flag name that is not a constant, or a fact written by hand.
- The sink check reads the eight outcome runners by name and the count lint the first six. Another local runner (``catalog_proof.py``) is neither read nor
  refused.
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from argparse import Namespace
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieError
from spark import split_table
from spark.bronze_clinical_lab_cli import ending
from spark.jobs.patient_split import result_document as split_document
from spark.result_file import SealedError
from spark.result_file import ending as sealed_ending
from spark.silver_cases import cutoff_rows, measure_rows
from spark.silver_cases_run import result_document as cases_document
from spark.silver_cases_run import run_build, run_verify
from spark.split_table import run_build as split_build
from tests.fast import test_bronze_job_sequence as bronze
from tests.fast import test_patient_split as split
from tests.fast import test_silver_cases_job as cases
from tests.fast.outcome_accept import (
    BRONZE_COMMANDS,
    CASES_SNAPSHOT_TABLES,
    COUNT_LINTED_RUNNERS,
    OUTCOME_RUNNERS,
    all_passed,
    bronze_problem,
    cases_problem,
    codes_are,
    count_linted_paths,
    equals_loaded,
    fits,
    kept_at_base,
    moved,
    no_removed_fields,
    outcome_runner_paths,
    read_result_file,
    runner_leaks,
    same_source,
    sealed_table,
    sink_leaks,
    snapshots_are,
    split_problem,
    wrote,
)

HASH_A, HASH_B = "a" * 64, "b" * 64
# The fake lake of the split tests names its commits h0, h1, hn; a real Nessie hash is 64 hexadecimal characters.
HASHES = {"h0": HASH_A, "h1": HASH_B, "hn": "c" * 64, "h2": "d" * 64}
HASH_FIELDS = ("base_hash", "checked_hash", "write_hash", "branch_hash")
BRANCH = bronze.BRANCH
# Every key that a result or a stand-in of the runners has answered with a count, a list of rows or causes, or a digest.
# Written out here, not read from the rule.
WRITTEN_COUNT_KEYS = (
    "rows",
    "columns",
    "patients",
    "fixed_sample_cases_checked",
    "development_cases",
    "bronze_numeric",
    "bronze_duplicates",
    "bronze_waveform",
    "silver_numeric",
    "silver_waveform",
    "numeric_causes",
    "waveform_causes",
    "arterial_seconds",
    "series_rows",
    "series_absent",
    "series_mismatch",
    "clinical_rows",
    "split_rows",
    "waveform_rows",
    "duplicate_keys",
    "seconds",
    "expected_events",
    "stored_events",
    "probes",
    "plant_events",
    "arterial_track_of_the_plant_case",
    "expected_rows",
    "numeric",
    "waveform",
)
COUNT_LINTED = (
    "bronze_clinical_lab.py",
    "bronze_clinical_lab_container.py",
    "silver_patient_split.py",
    "silver_cases.py",
    "silver_signals.py",
    "silver_hypotension.py",
)
RUNNERS = (*COUNT_LINTED, "gold_dimensions.py", "signal_profile.py")


def bronze_run(command: str = "verify", **plant: Any) -> tuple[int, dict[str, Any]]:
    harness = bronze.Harness()
    for key, value in plant.items():
        setattr(harness.spark, key, value)
    result = harness.run(command)
    status, document, _ = ending(Namespace(command=command, branch=BRANCH), lambda arguments: result)
    return status, document


def refused(error: Exception) -> tuple[int, dict[str, Any]]:
    def execute(arguments: Namespace) -> dict[str, Any]:
        raise error

    status, document, _ = ending(Namespace(command="write", branch=BRANCH), execute)
    return status, document


def table_of(name: str = "clinical_cases", source: int = 7, bronze_id: int = 9, command: str = "verify") -> dict[str, Any]:
    document: dict[str, Any] = copy.deepcopy(bronze_run(command)[1]["tables"][name])
    document["source_snapshot_id"], document["bronze_snapshot_id"] = source, bronze_id
    return document


def fail(table: dict[str, Any], name: str) -> dict[str, Any]:
    changed = copy.deepcopy(table)
    for check in changed["checks"]:
        if check["name"] == name:
            check["passed"] = False
    changed["passed"] = False
    return changed


class ResultFile(unittest.TestCase):
    """The folder that a job's one result file is read from."""

    def setUp(self) -> None:
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="accept_")))

    def put(self, text: str | bytes) -> None:
        (self.folder / "result.json").write_bytes(text if isinstance(text, bytes) else text.encode())

    def test_an_empty_folder_is_no_file_and_no_problem(self) -> None:
        self.assertEqual((None, None), read_result_file(self.folder))

    def test_a_part_file_alone_is_a_job_that_did_not_finish_and_is_no_result_and_no_problem(self) -> None:
        (self.folder / "result.json.part").write_text('{"half')
        self.assertEqual((None, None), read_result_file(self.folder))

    def test_a_part_file_beside_a_result_is_refused(self) -> None:
        (self.folder / "result.json.part").write_text("{}")
        self.put("{}")
        document, problem = read_result_file(self.folder)
        self.assertEqual((None, "the result folder holds a file other than the result"), (document, problem))

    def test_one_plain_file_with_one_json_document_is_read(self) -> None:
        self.put(json.dumps({"a": 1}))
        self.assertEqual(({"a": 1}, None), read_result_file(self.folder))

    def test_each_defect_of_the_folder_or_the_file_is_a_fixed_text_that_holds_nothing_of_the_file(self) -> None:
        marker = "MARKER_2291"

        def another_file() -> None:
            (self.folder / "x").write_text(marker)
            self.put("{}")

        cases_: dict[str, Callable[[], object]] = {
            "another file": another_file,
            "a directory": (self.folder / "result.json").mkdir,
            "a link": lambda: os.symlink(self.folder / "elsewhere", self.folder / "result.json"),
            "not json": lambda: self.put(f"{{{marker}"),
            "not a document": lambda: self.put(json.dumps([marker])),
            "a repeated key": lambda: self.put('{"a": 1, "a": 2, "m": "' + marker + '"}'),
            "not text": lambda: self.put(b"\xff\xfe" + marker.encode()),
            "too large": lambda: self.put(json.dumps({"a": "x" * 1_100_000})),
        }
        for name, plant in cases_.items():
            with self.subTest(name):
                for entry in self.folder.iterdir():
                    entry.rmdir() if entry.is_dir() else entry.unlink()
                plant()
                document, problem = read_result_file(self.folder)
                self.assertIsNone(document)
                self.assertIsInstance(problem, str)
                self.assertNotIn(marker, str(problem))


class Fits(unittest.TestCase):
    def test_a_dict_is_closed_a_list_holds_one_kind_and_a_bool_is_not_a_number(self) -> None:
        self.assertTrue(fits({"a": 1, "b": [True]}, {"a": int, "b": [bool]}))
        self.assertFalse(fits({"a": 1, "b": [True], "c": 0}, {"a": int, "b": [bool]}))
        self.assertFalse(fits({"a": 1}, {"a": int, "b": [bool]}))
        self.assertFalse(fits({"a": True}, {"a": int}))
        self.assertFalse(fits({"a": 1}, {"a": bool}))
        self.assertFalse(fits({"a": 1.0}, {"a": int}))
        self.assertFalse(fits({"a": 1, "b": [1]}, {"a": int, "b": [bool]}))

    def test_a_literal_an_alternative_and_a_rule_are_exact(self) -> None:
        self.assertTrue(fits("x", "x"))
        self.assertFalse(fits("y", "x"))
        self.assertTrue(fits(None, (int, None)))
        self.assertTrue(fits(3, (int, None)))
        self.assertFalse(fits("3", (int, None)))
        self.assertTrue(fits("b", frozenset({"a", "b"})))
        self.assertFalse(fits(1, frozenset({True})))
        self.assertTrue(fits("abc", lambda value: isinstance(value, str) and value.startswith("a")))
        self.assertFalse(fits(1, lambda value: isinstance(value, str)))


class BronzeFile(unittest.TestCase):
    """A Bronze result file against the closed schema, the asked command and branch and the exit status."""

    def problem(self, document: Any, status: int = 0, command: str = "verify") -> str | None:
        return bronze_problem(document, command, BRANCH, status)

    def test_a_passed_run_of_each_command_is_accepted_with_exit_0(self) -> None:
        for command in BRONZE_COMMANDS:
            status, document = bronze_run(command)
            self.assertEqual((0, None), (status, self.problem(document, status, command)), command)

    def test_a_run_with_a_failed_check_is_accepted_with_exit_1_and_not_with_exit_0(self) -> None:
        status, document = bronze_run(duplicates=2)
        self.assertEqual((1, None), (status, self.problem(document, 1)))
        self.assertIsNotNone(self.problem(document, 0))

    def test_each_refusal_code_is_accepted_with_its_exit_status_and_with_no_other(self) -> None:
        errors = (NessieError("x", status=500), SealedError("x"), RuntimeError("x"))
        for error in errors:
            status, document = refused(error)
            self.assertIsNone(self.problem(document, status, "write"), type(error).__name__)
            self.assertIsNotNone(self.problem(document, status + 1, "write"), type(error).__name__)

    def test_a_field_that_the_job_does_not_write_fails_at_every_level(self) -> None:
        for where in ("top", "table", "check", "diagnostic", "snapshots"):
            _, document = bronze_run()
            if where == "top":
                document["rows"] = 6
            elif where == "table":
                document["tables"]["lab_results"]["keys_digest"] = "x"
            elif where == "check":
                document["tables"]["lab_results"]["checks"][0]["detail"] = "loaded 6"
            elif where == "diagnostic":
                document["diagnostics"] = [{"code": "x", "count": 3}]
            else:
                document["tables"]["clinical_cases"]["source_metadata_location"] = "file:/w/m.json"
            with self.subTest(where):
                self.assertIsNotNone(self.problem(document))

    def test_a_missing_field_a_missing_table_and_a_wrong_type_each_fail(self) -> None:
        mutations: dict[str, Callable[[dict[str, Any]], object]] = {
            "no branch hash": lambda d: d.pop("branch_hash"),
            "hash not a hash": lambda d: d.update(branch_hash="not-a-hash"),
            "source hash short": lambda d: d.update(source_hash="abc"),
            "no tables": lambda d: d.update(tables={}),
            "one table": lambda d: d["tables"].pop("lab_results"),
            "a third table": lambda d: d["tables"].update(extra=copy.deepcopy(d["tables"]["lab_results"])),
            "snapshot is a bool": lambda d: d["tables"]["lab_results"].update(source_snapshot_id=True),
            "snapshot is text": lambda d: d["tables"]["lab_results"].update(bronze_snapshot_id="9"),
            "table flag is a number": lambda d: d["tables"]["lab_results"].update(passed=1),
            "check flag is a number": lambda d: d["tables"]["lab_results"]["checks"][0].update(passed=1),
            "unknown check": lambda d: d["tables"]["lab_results"]["checks"].append({"name": "made up", "passed": True}),
            "a check twice": lambda d: d["tables"]["lab_results"]["checks"].append(copy.deepcopy(d["tables"]["lab_results"]["checks"][0])),
            "a check missing": lambda d: d["tables"]["lab_results"]["checks"].pop(),
            "checks out of order": lambda d: d["tables"]["lab_results"]["checks"].reverse(),
            "table name changed": lambda d: d["tables"]["lab_results"].update(bronze_table="bronze.other"),
            "top flag true over a failed table": lambda d: d["tables"]["lab_results"]["checks"][0].update(passed=False),
            "top flag false over passed tables": lambda d: d.update(passed=False),
            "diagnostic on a passed run": lambda d: d.update(diagnostics=[{"code": "checks_failed"}]),
        }
        for name, mutate in mutations.items():
            _, document = bronze_run()
            mutate(document)
            with self.subTest(name):
                self.assertIsNotNone(self.problem(document))

    def test_a_failed_run_must_say_checks_failed_and_hold_a_failed_flag(self) -> None:
        _, document = bronze_run(duplicates=2)
        plants: dict[str, Callable[[dict[str, Any]], object]] = {
            "no diagnostic": lambda d: d.update(diagnostics=[]),
            "another code": lambda d: d.update(diagnostics=[{"code": "settings"}]),
            "two codes": lambda d: d.update(diagnostics=[{"code": "checks_failed"}, {"code": "checks_failed"}]),
            "top flag true": lambda d: d.update(passed=True),
        }
        for name, mutate in plants.items():
            changed = copy.deepcopy(document)
            mutate(changed)
            with self.subTest(name):
                self.assertIsNotNone(self.problem(changed, 1))

    def test_another_job_command_or_branch_fails(self) -> None:
        status, document = bronze_run()
        for key, value in (("job", "other"), ("command", "write"), ("branch", "another")):
            with self.subTest(key):
                self.assertIsNotNone(self.problem({**document, key: value}, status))
        self.assertIsNotNone(self.problem(document, status, "write"))

    def test_a_refusal_has_exactly_one_known_code_and_the_closed_fields(self) -> None:
        status, document = refused(NessieError("x", status=500))
        for name, changed in {
            "a count": {**document, "diagnostics": [{"code": "catalog_error", "count": 2}]},
            "an unknown code": {**document, "diagnostics": [{"code": "made_up"}]},
            "two codes": {**document, "diagnostics": [{"code": "catalog_error"}, {"code": "catalog_error"}]},
            "an extra field": {**document, "tables": {}},
            "passed true": {**document, "passed": True},
            "no diagnostics": {**document, "diagnostics": []},
        }.items():
            with self.subTest(name):
                self.assertIsNotNone(self.problem(changed, status, "write"))

    def test_a_document_that_is_not_a_dict_fails(self) -> None:
        documents: list[Any] = [None, [], "x", 1]
        for document in documents:
            self.assertIsNotNone(self.problem(document))


class EqualsLoaded(unittest.TestCase):
    def test_a_passed_table_with_both_snapshot_ids_equals_the_loaded_table_for_either_table(self) -> None:
        for name in ("clinical_cases", "lab_results"):
            self.assertTrue(equals_loaded(table_of(name)), name)

    def test_each_of_the_four_checks_that_prove_equality_is_needed_for_either_table(self) -> None:
        for name in ("clinical_cases", "lab_results"):
            for check in ("row count", "no value changed", "key unique", "key not null"):
                self.assertFalse(equals_loaded(fail(table_of(name), check)), f"{name} {check}")

    def test_each_of_the_four_checks_is_needed_on_its_own_even_when_the_table_flag_says_passed(self) -> None:
        for name in ("clinical_cases", "lab_results"):
            for check in ("row count", "no value changed", "key unique", "key not null"):
                table = table_of(name)
                for item in table["checks"]:
                    if item["name"] == check:
                        item["passed"] = False
                self.assertTrue(table["passed"])
                self.assertFalse(equals_loaded(table), f"{name} {check}")

    def test_a_check_or_a_snapshot_that_is_missing_is_not_equality(self) -> None:
        missing = table_of()
        missing["checks"] = [check for check in missing["checks"] if check["name"] != "no value changed"]
        self.assertFalse(equals_loaded(missing))
        self.assertFalse(equals_loaded({**table_of(), "bronze_snapshot_id": None}))
        self.assertFalse(equals_loaded({**table_of(), "source_snapshot_id": True}))
        self.assertFalse(equals_loaded({**table_of(), "passed": False}))
        self.assertFalse(equals_loaded({}))

    def test_a_table_that_holds_a_count_a_digest_a_path_or_a_detail_is_not_sealed_even_when_it_passed(self) -> None:
        self.assertTrue(sealed_table(table_of()))
        for key in ("rows", "source_rows", "keys_digest", "rows_digest", "source_metadata_location"):
            self.assertFalse(sealed_table({**table_of(), key: 1}), key)
            self.assertFalse(equals_loaded({**table_of(), key: 1}), key)
        detailed = table_of()
        detailed["checks"][0]["detail"] = "loaded 6, bronze 6"
        self.assertFalse(sealed_table(detailed))
        self.assertFalse(sealed_table({**table_of(), "checks": []}))

    def test_the_snapshot_ids_must_be_the_ones_that_nessie_holds(self) -> None:
        self.assertTrue(snapshots_are(table_of(source=7, bronze_id=9), 7, 9))
        self.assertFalse(snapshots_are(table_of(source=7, bronze_id=9), 7, 10))
        self.assertFalse(snapshots_are(table_of(source=7, bronze_id=9), 8, 9))
        self.assertFalse(snapshots_are(table_of(source=7, bronze_id=9), None, 9))
        self.assertFalse(snapshots_are({**table_of(), "source_snapshot_id": None}, None, 9))
        self.assertFalse(snapshots_are({**table_of(source=1, bronze_id=1)}, True, True))


class Moved(unittest.TestCase):
    def test_a_moved_source_needs_both_tables_equal_and_both_snapshot_ids_new(self) -> None:
        self.assertTrue(moved(table_of(source=5, bronze_id=9), table_of(source=6, bronze_id=10)))
        self.assertFalse(moved(table_of(source=5, bronze_id=9), table_of(source=5, bronze_id=10)))
        self.assertFalse(moved(table_of(source=5, bronze_id=9), table_of(source=6, bronze_id=9)))
        self.assertFalse(moved(table_of(source=5, bronze_id=9), fail(table_of(source=6, bronze_id=10), "no value changed")))

    def test_the_same_source_needs_the_same_source_snapshot_and_both_tables_equal(self) -> None:
        self.assertTrue(same_source(table_of(source=5, bronze_id=9), table_of(source=5, bronze_id=12)))
        self.assertFalse(same_source(table_of(source=5, bronze_id=9), table_of(source=6, bronze_id=12)))
        self.assertFalse(same_source(table_of(source=5, bronze_id=9), fail(table_of(source=5, bronze_id=12), "row count")))


class Hashes(unittest.TestCase):
    def test_a_write_moves_the_branch_off_the_base_and_a_rebuild_that_changed_nothing_leaves_it(self) -> None:
        moved_branch = {"base_hash": HASH_A, "branch_hash": HASH_B}
        quiet = {"base_hash": HASH_A, "branch_hash": HASH_A}
        self.assertTrue(wrote(moved_branch))
        self.assertFalse(wrote(quiet))
        self.assertTrue(kept_at_base(quiet))
        self.assertFalse(kept_at_base(moved_branch))

    def test_text_that_is_not_a_hash_is_neither_a_write_nor_a_kept_branch(self) -> None:
        for text in ("a", "h0", "A" * 64, "g" * 64, "a" * 63):
            self.assertFalse(wrote({"base_hash": text, "branch_hash": HASH_B}), text)
            self.assertFalse(wrote({"base_hash": HASH_A, "branch_hash": text}), text)
            self.assertFalse(kept_at_base({"base_hash": text, "branch_hash": text}), text)

    def test_a_hash_that_is_missing_or_not_a_string_is_neither(self) -> None:
        for result in ({}, {"base_hash": None, "branch_hash": None}, {"base_hash": "", "branch_hash": ""}, {"base_hash": 1, "branch_hash": 1}):
            self.assertFalse(wrote(result), result)
            self.assertFalse(kept_at_base(result), result)


class Flags(unittest.TestCase):
    TABLES = ("cases", "lab", "operation_length_measure", "score_cutoffs")

    def flags(self, **changes: Any) -> dict[str, Any]:
        return {"passed": {**dict.fromkeys(self.TABLES, True), **changes}}

    def test_every_named_table_needs_its_flag_true_and_no_other_table_is_allowed(self) -> None:
        self.assertTrue(all_passed(self.flags(), self.TABLES))
        self.assertFalse(all_passed(self.flags(lab=False), self.TABLES))
        self.assertFalse(all_passed(self.flags(lab=1), self.TABLES))
        self.assertFalse(all_passed(self.flags(extra=True), self.TABLES))
        self.assertFalse(all_passed({"passed": {"cases": True}}, self.TABLES))
        self.assertFalse(all_passed({}, ("cases",)))

    def test_a_result_with_a_removed_field_is_refused(self) -> None:
        self.assertTrue(no_removed_fields({"passed": {"cases": True}, "checks_run": [], "development_cases": 5}))
        for key in ("outcome", "written", "cases", "patients", "counts"):
            self.assertFalse(no_removed_fields({key: 1}), key)
        self.assertFalse(no_removed_fields({"measure": [{"rows": 1}]}))


class RunnerLeaks(unittest.TestCase):
    """``runner_leaks`` finds, by rule name and line, each way that an end-to-end runner can record a count, a length or a data-chosen identifier."""

    BAD = (
        ("stream_length", 'streams = f"standard output {len(os.fsencode(out))} bytes"'),
        ("stream_length", "total = len(err)"),
        ("stream_tail", 'log.append(f"{out[-3000:]}")'),
        ("stream_tail", "tail = err[-200:] if code else ''"),
        ("count_in_text", 'observed = f"rows {len(rows)} of {total}"'),
        ("count_in_text", 'observed = f"counts {counts}, left {len(left)}"'),
        ("count_in_result", "observed = f\"rows {result.get('rows')}, columns {result.get('columns')}\""),
        ("count_in_result", "observed = f\"checked {result.get('fixed_sample_cases_checked')}\""),
        ("count_printed", 'print("RESULT " + json.dumps({"rows": loaded.count(), "columns": len(loaded.columns)}))'),
        ("data_identifier", 'observed = f"group of the swap {source.differing_group}"'),
        ("data_identifier", 'observed = f"cell {source.zero_cell}, key {source.lab_key}"'),
        ("stream_logged", 'self.log.append(f"== docker {" ".join(arguments[:3])}: exit {code}\\n{out}{err}")'),
        ("stream_logged", 'log.append(f"build said {output}")'),
        ("count_emitted", '        "silver_numeric": session.table(SNUM).count(),'),
        ("count_emitted", '    emit({"planted": len(rows), "tracks_added": added})'),
        ("count_emitted", '        "numeric": {key: int(value or 0) for key, value in numeric.items()},'),
        ("count_in_result", "observed = f\"rows {result.get('silver_numeric')}, bronze {result.get('bronze_numeric')}\""),
        ("count_in_result", "observed = f\"arterial seconds {result.get('arterial_seconds')}, absent {result.get('series_absent')}\""),
        ("count_in_result", "observed = f\"rows {result.get('clinical_rows')}, causes {result.get('numeric_causes')}\""),
        ("count_in_result", "observed = f\"events {result.get('expected_events')} vs {result.get('stored_events')}, probes {result.get('probes')}\""),
        ("digest_saved", 'self.facts.update(control_branch=branch, events_digest=result["events_digest"])'),
        ("digest_saved", 'self.facts["digest"] = result.get("events_digest")'),
        ("digest_saved", "record.environment.update(events_digest=str(digest))"),
        ("refusal_count", '            ["source_table: 0 tables"],'),
        ("refusal_count", 'wanted = ["two_sets: 2 patients", "key_null: failed"]'),
    )
    CLEAN = (
        "observed = f\"exit {code} after {seconds:.0f} seconds, rows as expected {result.get('rows_as_expected')}\"",
        'print("RESULT " + json.dumps({"rows_as_expected": loaded.count() == spec["rows"], "columns_as_expected": len(loaded.columns) == spec["columns"]}))',
        'self.log.append(f"== spark {args}: exit {code} after {seconds:.0f} seconds, result file read")',
        "held = len(items) == 3",
        'self.stack.write_spec({"rows": len(self.expected_cases), "patients": len(self.expected_sets)})',
        "observed = f\"every case checked {result.get('fixed_sample_cases_checked') == len(self.candidates)}, all deleted {deleted == len(set(made))}\"",
        'observed = f"flags {dict.fromkeys(flags, True)}"',
        "self.log.append(f\"== docker {' '.join(arguments[:3])}: exit {code} after {seconds:.0f} seconds\")",
        '        "numeric_equal": numeric.get("absent") == numeric.get("verdict_mismatch") == 0,',
        '    emit({"planted": True, "all_read": len(held) == len(expected)})',
        '            ["source_table: failed"],',
        'observed = f"no container left {not left}, all deleted {deleted == len(set(run.made)) + len(run.tags)}"',
    )

    def test_each_bad_line_is_found_by_its_rule_and_its_line_number(self) -> None:
        for name, line in self.BAD:
            with self.subTest(name, line=line[:50]):
                self.assertEqual([(name, 2)], runner_leaks(f"x = 1\n{line}\ny = 2\n"))

    def test_every_key_of_the_count_list_is_found_when_it_is_printed_and_not_when_it_is_compared(self) -> None:
        for key in WRITTEN_COUNT_KEYS:
            with self.subTest(key):
                self.assertEqual([("count_in_result", 1)], runner_leaks(f"observed = f\"{{result.get('{key}')}}\"\n"))
                self.assertEqual([], runner_leaks(f"flag = result.get('{key}') == expected\n"))

    def test_a_clean_line_is_not_found(self) -> None:
        for line in self.CLEAN:
            with self.subTest(line=line[:50]):
                self.assertEqual([], runner_leaks(f"{line}\n"))

    def test_a_comment_and_a_docstring_line_are_not_a_leak_but_code_after_them_is(self) -> None:
        self.assertEqual([], runner_leaks("# the log holds len(os.fsencode(out)) once\n"))
        self.assertEqual([(("stream_length"), 2)], runner_leaks("# a comment\nn = len(out)\n"))

    def test_the_lint_reads_the_eight_outcome_runners_by_name_and_no_other_local_runner(self) -> None:
        self.assertEqual(RUNNERS, OUTCOME_RUNNERS)
        self.assertEqual(COUNT_LINTED, COUNT_LINTED_RUNNERS)
        self.assertEqual(("gold_dimensions.py", "signal_profile.py"), OUTCOME_RUNNERS[-2:])
        with tempfile.TemporaryDirectory(prefix="e2e_") as folder_name:
            folder = Path(folder_name)
            for name in ("catalog_proof.py", "run_record.py", RUNNERS[0], RUNNERS[3], RUNNERS[6], RUNNERS[7]):
                (folder / name).write_text("x = 1\n")
            self.assertEqual([RUNNERS[0], RUNNERS[3], RUNNERS[6], RUNNERS[7]], [path.name for path in outcome_runner_paths(folder)])

    def test_the_count_lint_reads_the_first_six_runners_only(self) -> None:
        """The Gold and profile runners hold Spark programs that count rows inside the container and compare the counts there, so the line lint of what a
        runner prints reads the six older runners. The sink check reads all eight."""
        with tempfile.TemporaryDirectory(prefix="e2e_") as folder_name:
            folder = Path(folder_name)
            for name in RUNNERS:
                (folder / name).write_text("x = 1\n")
            self.assertEqual(list(COUNT_LINTED), [path.name for path in count_linted_paths(folder)])

    def test_the_runners_of_this_machine_leak_nothing(self) -> None:
        present = count_linted_paths(Path(__file__).resolve().parents[1] / "e2e")
        if not present:
            self.skipTest("the untracked runners are not on this machine")
        for path in present:
            with self.subTest(runner=path.name):
                self.assertEqual([], runner_leaks(path.read_text()))


class SinkLeaks(unittest.TestCase):
    """``sink_leaks`` reads the syntax tree of a runner and finds each observation, fact or printed line that is not the output of ``sealed_observed``."""

    BAD = (
        ("observation_not_sealed", 'record.expect("c", "s", "e", f"rows {len(rows)}", ok is True)'),
        ("observation_not_sealed", 'self.record.expect("c", "s", "e", f"flags {flags}", ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", observed, ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", str(value), ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", repr(value), ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", "snapshots {}".format(ids), ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", "exit " + str(code), ok is True)'),
        ("observation_not_sealed", 'self.record.expect(case, "s", "e", observed=f"{value}", passed=ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", accepted.facts("write_hash"), ok is True)'),
        ("observation_not_sealed", 'expect("c", "s", "e", sealed_text, ok is True)'),
        ("fact_not_sealed", "self.facts.update(merged_hash=merged)"),
        ("fact_not_sealed", 'self.facts.update({"a": value})'),
        ("fact_not_sealed", 'self.facts["a"] = value'),
        ("fact_not_sealed", 'facts["stalled"] = bool(stack.stalls)'),
        ("fact_not_sealed", "record.environment.update({key: str(value) for key, value in run.facts.items()})"),
        ("fact_not_sealed", 'record.environment["scope"] = scope'),
        ("fact_not_sealed", 'record.environment["stalled"] = str(bool(run.stack.stalls))'),
        ("fact_not_sealed", 'record.environment = {"scope": scope}'),
        ("fact_not_sealed", "self.facts = result"),
        ("fact_not_sealed", "self.facts.update(result)"),
        ("fact_not_sealed", 'self.facts.update(sealed_observed({"a": True}).facts, extra=1)'),
        ("raw_in_sealed", 'expect("c", "s", "e", sealed_observed({"a": True}, codes=[f"{code}"]), ok is True)'),
        ("raw_in_sealed", 'expect("c", "s", "e", sealed_observed({"a": True}, exit_code=int(str(code))), ok is True)'),
        ("raw_in_sealed", 'expect("c", "s", "e", sealed_observed({"a": repr(value) == "x"}), ok is True)'),
        ("raw_in_sealed", 'expect("c", "s", "e", sealed_observed({"a": "{}".format(value) == "x"}), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed({f"rows {n}": True}), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed({name: True}), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed(flags), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed({**flags}), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed(dict.fromkeys(names, True)), ok is True)'),
        ("flag_name_not_constant", 'expect("c", "s", "e", sealed_observed({"a" + name: True}), ok is True)'),
        ("forged_id", "found = ConfirmedId(7)"),
        ("forged_id", "found = sealed_record.ConfirmedId(7, token)"),
        ("confirmed_not_made", 'expect("c", "s", "e", sealed_observed(confirmed={"id": 7}), ok is True)'),
        ("confirmed_not_made", 'expect("c", "s", "e", sealed_observed(confirmed={"id": f"{x}"}), ok is True)'),
        ("printed_raw", 'print(f"{value}")'),
        ("printed_raw", 'sys.stdout.write(f"PASS {case}: {observed}\\n")'),
        ("printed_raw", "sys.stdout.write(text)"),
        ("printed_raw", 'sys.stdout.write("x " + value)'),
        ("printed_raw", 'print(json.dumps({"rows": 3}))'),
        ("expected_not_fixed", 'record.expect("c", "s", f"rows {len(rows)}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"the job exits {wanted}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'self.record.expect("c", "s", expected, sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", str(count), sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", "rows " + count, sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", expected=what, observed="x", passed=ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"{FROZEN} and {rows}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"{FROZEN[0]}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"the set holds {TOTAL}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"{FROZEN}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", f"{IMAGE}{PREFIX}", sealed_observed({"a": True}), ok is True)'),
        ("expected_not_fixed", 'record.expect("c", "s", "a" if ok else f"{TOTAL}", sealed_observed({"a": True}), ok is True)'),
        ("log_not_sealed", 'log.append(f"the set holds {TOTAL}")'),
        ("limits_not_fixed", 'record.limits.append(f"within {PIN_SIZE} seconds")'),
        ("cleanup_not_fixed", 'cleanup.append(f"left {PIN_SIZE}")'),
        ("expected_not_fixed", 'record.expect("c", "s", f"{FROZEN!r:>{width}}", sealed_observed({"a": True}), ok is True)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), ok)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), accepted.ok)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), code == 0 and ok)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), ok or count)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), len(rows))'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), result.get("passed"))'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), passes_as_write(a, b))'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), 1)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), None)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), count if ok else code == 0)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), passed=count)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), (ok is True) + count)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), [ok])'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), code == 0 if ok else count)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), -count)'),
        ("passed_not_boolean", 'record.expect("c", "s", "e", sealed_observed({"a": True}), ~count)'),
        ("limits_not_fixed", 'record.limits.insert("a fixed text")'),
        ("limits_not_fixed", 'record.limits.insert(0, "a fixed text", "another")'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["nessie", seen.stdout.strip()]))'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["nessie", out]))'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["nessie", *err]))'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["nessie", result.stdout]))'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["nessie", " ".join(parts)]))'),
        ("raw_in_sealed", "log.append(sealed_observed(words=words))"),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["a"] + parts))'),
        ("raw_in_sealed", 'log.append(sealed_observed(words=["a", *parts.split()]))'),
        ("log_not_sealed", 'log.append(f"== docker {code}")'),
        ("log_not_sealed", 'self.log.append(f"== spark: exit {code} after {seconds:.0f} seconds")'),
        ("log_not_sealed", "self.log.append(text)"),
        ("log_not_sealed", "log.append(seen.stdout.strip())"),
        ("log_not_sealed", 'log.append("== a " + value)'),
        ("log_not_sealed", 'log.extend([f"== {x}"])'),
        ("log_not_sealed", "log.insert(0, value)"),
        ("log_not_sealed", 'log.append(f"== stat {seen.stdout}")'),
        ("log_not_sealed", 'log.append(sealed_observed({"a": True}).facts)'),
        ("log_not_sealed", 'log.append(sealed_observed({"a": True}) + value)'),
        ("log_not_sealed", 'log += [f"{x}"]'),
        ("limits_not_fixed", 'record.limits.append(f"stalled {len(steps)} times")'),
        ("limits_not_fixed", "record.limits.append(value)"),
        ("limits_not_fixed", 'record.limits = ["fixed", f"rows {n}"]'),
        ("limits_not_fixed", "record.limits = values"),
        ("limits_not_fixed", 'record.limits.insert(0, f"{count}")'),
        ("limits_not_fixed", "record.limits.extend([value])"),
        ("limits_not_fixed", "record.limits.extend(values)"),
        ("limits_not_fixed", "record.limits += [value]"),
        ("limits_not_fixed", 'self.record.limits.append(f"steps {run.stack.stalls}")'),
        ("cleanup_not_fixed", 'record.cleanup = f"cleanup failed: {error}"'),
        ("cleanup_not_fixed", "record.cleanup = text"),
        ("cleanup_not_fixed", 'record.cleanup = "; ".join(other)'),
        ("cleanup_not_fixed", 'record.cleanup = ", ".join(cleanup)'),
        ("cleanup_not_fixed", "self.cleanup = str(error)"),
        ("cleanup_not_fixed", 'cleanup.append(f"scratch removed: {gone}")'),
        ("cleanup_not_fixed", "cleanup.append(finish(run, record))"),
        ("cleanup_not_fixed", 'cleanup += [f"{x}"]'),
        ("command_not_sealed", "self.record.commands.append(f\"docker {' '.join(arguments[:2])} ...\")"),
        ("command_not_sealed", "record.commands.append(command)"),
        ("command_not_sealed", 'record.commands.append("docker " + value)'),
    )
    CLEAN = (
        'record.expect("c", "s", "e", sealed_observed({"x": ok}, exit_code=code, seconds=seconds), ok is True)',
        'self.record.expect("c", "s", "e", sealed_observed({"x": ok}, exits={"plant exit": a, "exit": b}, codes=codes(result)), ok is True)',
        'self.record.expect("c", "s", "e", sealed_observed({"a": ids == other}, confirmed=found), ok is True)',
        'self.record.expect("c", "s", "e", sealed_observed(confirmed={"id": made}), ok is True)',
        'expect("c", "s", "e", sealed_observed(error=error), False)',
        'expect("c", "s", "e", sealed_observed(reason=accepted.reason), accepted.ok is True)',
        'record.expect("input", "setup", "e", "BRONZE_DATA_DIR is not set", False)',
        'expect(f"planted: {label}", "planted fault", "verify exits", sealed_observed({"x": ok}), ok is True)',
        "expect = self.record.expect",
        "def expect(self, case, step, expected, observed, passed):\n    return passed",
        "self.observations.append(Observation(case, step, expected, observed, passed))",
        'self.facts.update(sealed_observed({"x": True}, confirmed={"merged_hash": made}).facts)',
        "self.facts.update(result.facts)",
        "self.facts: dict[str, Any] = {}",
        "self.facts = {}",
        "facts = {}",
        "facts = dict(run.facts)",
        "facts = run_local(record, log, source)",
        "record.environment.update(run.facts)",
        'record.environment = {"target": "local Docker", "images": "spark 4.1.4"}',
        'record.environment["scope"] = "full" if full else "development cases"',
        'record.environment["scope"] = "full"',
        'sys.stdout.write(sealed_observed({"x": True}).line(True, "c", "control"))',
        'sys.stdout.write("edited\\n")',
        'print("fixed text")',
        'texts = [" ".join((item.case, item.step, item.observed)) for item in record.observations]',
        'record.expect("c", "s", "the file has the size of the pin", sealed_observed({"x": ok}), ok is True)',
        'record.expect("c", "s", f"the file has the pinned size {IMAGE} and owner {NESSIE_USER}", sealed_observed({"x": ok}), ok is True)',
        'record.expect("c", "s", "a" if full else "b", sealed_observed({"x": ok}), ok is True)',
        'record.expect("c", "s", sealed_observed({"x": True}), sealed_observed({"x": ok}), ok is True)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), code == 0 and bool(merged) and all(done.values()) and failure is None)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), not twice)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), True)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), False)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), any(x) or isinstance(y, int))',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), (a == 1 if b else c == 1) and 0 < n <= 3)',
        'record.expect("c", "s", "e", sealed_observed({"x": ok}), passed=held is None)',
        'log.append(sealed_observed(words=["docker", *arguments[:3]], exit_code=code))',
        'self.log.append(sealed_observed(words=["spark", *arguments], exit_code=code, seconds=seconds, flags={"result file refused": bool(problem)}))',
        'self.log.append("== a fixed line")',
        'log.append("== a " if x else "== b")',
        'log.extend([sealed_observed({"x": ok}), "fixed"])',
        'record.limits.append(sealed_observed({"x": ok}))',
        'record.limits.append(f"a step that gives no result within {JOB_SECONDS} seconds fails the run")',
        'record.limits = ["a fixed", f"a step within {JOB_SECONDS} seconds", sealed_observed({"x": ok})]',
        'record.limits.insert(0, "fixed")',
        "record.limits = []",
        'record.limits += ["fixed"]',
        'record.limits.extend(["fixed", sealed_observed({"x": ok})])',
        "limits: list[str] = []",
        "self.log = log",
        "log: list[str] = []",
        'commands = parser.add_subparsers(dest="command", required=True)',
        "commands: list[str] = []",
        'record.cleanup = "; ".join(cleanup)',
        'record.cleanup = "pending"',
        'record.cleanup = sealed_observed({"x": ok})',
        'cleanup.append(sealed_observed({"x": ok}))',
        'cleanup.append("fixed")',
        "cleanup: list[str] = []",
        'self.record.commands.append(sealed_observed(words=["docker", *arguments[:2]]))',
        'record.commands.append("docker compose up")',
        'log.append(sealed_observed(words=["removed", "the", kind, "of", PROJECT, *arguments[:3], *arguments], exit_code=code))',
        "log.append(sealed_observed(words=(label, step, *arguments[:1])))",
        "texts = [*log, *record.limits, *record.commands, record.cleanup]",
        '# record.expect("c", "s", "e", f"rows {n}", ok is True)',
        '"""record.expect(c, s, e, f"rows {n}", ok)"""',
    )

    def test_each_bad_line_is_found_by_its_rule_and_its_line_number(self) -> None:
        for name, line in self.BAD:
            with self.subTest(name, line=line[:60]):
                self.assertEqual([(name, 2)], sink_leaks(f"x = 1\n{line}\ny = 2\n"))

    def test_a_clean_line_is_not_found(self) -> None:
        for line in self.CLEAN:
            with self.subTest(line=line[:60]):
                self.assertEqual([], sink_leaks(f"{line}\n"))

    def test_a_sink_inside_a_method_a_branch_and_a_continued_call_is_found_at_the_line_of_the_call(self) -> None:
        source = (
            "class Run:\n"
            "    def case(self):\n"
            "        if x:\n"
            "            self.record.expect(\n"
            '                "c",\n'
            '                "s",\n'
            '                "e",\n'
            '                f"rows {n}",\n'
            "                ok,\n"
            "            )\n"
        )
        self.assertEqual([("observation_not_sealed", 4)], sink_leaks(source))

    def test_every_sealed_call_that_goes_into_a_fact_is_found_when_it_is_not_the_facts_attribute(self) -> None:
        self.assertEqual([("fact_not_sealed", 1)], sink_leaks('self.facts.update(sealed_observed({"x": True}))\n'))
        self.assertEqual([("fact_not_sealed", 1)], sink_leaks('self.facts.update(sealed_observed({"x": True}).text)\n'))

    FORWARD = (
        "class Run:\n"
        "    def refused(self, case, expected, observed, ok):\n"
        "        return self.record.expect(case, 'planted', expected, sealed_observed({'a': ok}), ok is True)\n"
        "\n"
        "    def go(self):\n"
        "@CALLS@"
    )

    def forward(self, *calls: str) -> list[tuple[str, int]]:
        return sink_leaks(self.FORWARD.replace("@CALLS@", "".join(f"        {call}\n" for call in calls)))

    def test_an_expectation_that_is_a_parameter_is_checked_where_the_function_is_called(self) -> None:
        self.assertEqual(
            [], self.forward('self.refused("c", "the job exits 1", result, True)', 'self.refused("c", f"the job exits {JOB_SECONDS}", result, True)')
        )
        self.assertEqual([("expected_not_fixed", 6)], self.forward('self.refused("c", f"rows {len(rows)}", result, True)'))
        self.assertEqual([("expected_not_fixed", 6)], self.forward('self.refused("c", expected=f"rows {len(rows)}", observed=result, ok=True)'))
        self.assertEqual([("expected_not_fixed", 6)], self.forward('self.refused("c", wanted, result, True)'))
        self.assertEqual([("expected_not_fixed", 7)], self.forward('self.refused("c", "fixed", result, True)', 'self.refused("c", str(count), result, True)'))
        self.assertEqual(
            [("expected_not_fixed", 6), ("expected_not_fixed", 7)],
            self.forward('self.refused("c", text, result, True)', 'self.refused("c", text, result, False)'),
        )

    CHAIN = (
        "class Run:\n"
        "    def refused(self, case, expected, ok):\n"
        "        return self.record.expect(case, 'planted', expected, sealed_observed({'a': ok}), ok is True)\n"
        "\n"
        "    def build(self, case, wanted, expected, ok):\n"
        "        self.refused(case, expected, ok)\n"
        "\n"
        "    def go(self):\n"
        "@CALLS@"
    )

    def test_an_expectation_that_goes_through_two_functions_is_checked_where_the_first_one_is_called(self) -> None:
        def chain(*calls: str) -> list[tuple[str, int]]:
            return sink_leaks(self.CHAIN.replace("@CALLS@", "".join(f"        {call}\n" for call in calls)))

        self.assertEqual([], chain('self.build("c", [], "the job exits 1", True)'))
        self.assertEqual([("expected_not_fixed", 9)], chain('self.build("c", [], f"rows {len(rows)}", True)'))
        self.assertEqual([("expected_not_fixed", 9)], chain('self.build("c", [], expected=what, ok=True)'))
        self.assertEqual([("expected_not_fixed", 10)], chain('self.build("c", [], "fixed", True)', 'self.build("c", [], str(n), True)'))

    DEEP = (
        "class Run:\n"
        "    def top(self, case, text):\n"
        "        self.mid(case, text)\n"
        "\n"
        "    def mid(self, case, text):\n"
        "        self.low(case, text)\n"
        "\n"
        "    def low(self, case, expected):\n"
        "        return self.record.expect(case, 'planted', expected, sealed_observed({'a': True}), True)\n"
        "\n"
        "    def go(self):\n"
        "@CALLS@"
    )

    def test_an_expectation_that_goes_through_three_functions_is_checked_where_the_first_one_is_called(self) -> None:
        def deep(*calls: str) -> list[tuple[str, int]]:
            return sink_leaks(self.DEEP.replace("@CALLS@", "".join(f"        {call}\n" for call in calls)))

        self.assertEqual([], deep('self.top("c", "fixed")'))
        self.assertEqual([("expected_not_fixed", 12)], deep('self.top("c", f"rows {len(rows)}")'))

    def test_an_expectation_that_is_a_name_that_is_no_parameter_of_the_function_is_found(self) -> None:
        source = "def go(self, expected):\n    expected = f'rows {n}'\n    self.record.expect('c', 's', other, sealed_observed({'a': True}), True)\n"
        self.assertEqual([("expected_not_fixed", 3)], sink_leaks(source))

    def test_a_source_that_does_not_parse_is_an_error_and_not_a_clean_runner(self) -> None:
        with self.assertRaises(SyntaxError):
            sink_leaks("def (:\n")

    def test_the_runners_of_this_machine_record_through_the_helper_only(self) -> None:
        present = outcome_runner_paths(Path(__file__).resolve().parents[1] / "e2e")
        if not present:
            self.skipTest("the untracked runners are not on this machine")
        for path in present:
            with self.subTest(runner=path.name):
                self.assertEqual([], sink_leaks(path.read_text()))


def split_run(mode: str = "build") -> tuple[int, dict[str, Any]]:
    arguments = Namespace(mode=mode, branch="b", pinned_release=True)
    if mode == "build":
        facts = split_build(split.Lake(split.FULL, snapshot=11).operations(), "h0", pinned=True, sample=None).facts()
    else:
        facts = split_table.run_verify(
            split.Lake(split.FULL, existing=split.rows_of(split.FULL, snapshot=11)).operations(), "h0", pinned=True, sample=None
        ).facts()
    document = {**split_document(arguments, "h0" if mode == "build" else None, facts), "diagnostics": []}
    return 0, {key: HASHES.get(value, value) if key in HASH_FIELDS else value for key, value in document.items()}


def cases_run(mode: str = "build") -> tuple[int, dict[str, Any]]:
    arguments = Namespace(mode=mode, branch="b", pinned_release=True)
    held = (measure_rows(cases.LENGTHS), cutoff_rows(cases.LENGTHS))
    outcome = run_build(cases.FakeOps(), pinned=True) if mode == "build" else run_verify(cases.FakeOps(held=held), pinned=True)
    return 0, {**cases_document(arguments, "a" * 64 if mode == "build" else None, "c" * 64, outcome), "diagnostics": []}


def sealed_refusal(job: str, *problems: str) -> tuple[int, dict[str, Any]]:
    def execute(arguments: Namespace) -> dict[str, Any]:
        raise SealedError(*problems)

    status, document, _ = sealed_ending(Namespace(mode="build", branch="b"), execute, job)
    return status, document


class SealedFile(unittest.TestCase):
    """The result files of the patient split and the Silver cases jobs."""

    CHECKERS = (("split", split_problem, split_run, "patient_split"), ("cases", cases_problem, cases_run, "silver_cases"))

    def test_a_clean_build_and_a_clean_verify_of_each_job_are_accepted_with_exit_0(self) -> None:
        for name, checker, run, _ in self.CHECKERS:
            for mode in ("build", "verify"):
                status, document = run(mode)
                self.assertIsNone(checker(document, mode, "b", status), f"{name} {mode}")

    def test_another_mode_or_branch_or_exit_status_fails(self) -> None:
        for name, checker, run, _ in self.CHECKERS:
            status, document = run("build")
            self.assertIsNotNone(checker(document, "verify", "b", status), name)
            self.assertIsNotNone(checker(document, "build", "other", status), name)
            self.assertIsNotNone(checker(document, "build", "b", 1), name)

    def test_the_facts_of_a_job_share_no_list_with_the_module_that_makes_them(self) -> None:
        for _, _, run, _ in self.CHECKERS:
            _, first = run("build")
            first["checks_run"].append("planted")
            self.assertNotIn("planted", run("build")[1]["checks_run"])

    def test_a_field_that_the_job_does_not_write_fails_at_the_top_and_in_each_map(self) -> None:
        for name, checker, run, _ in self.CHECKERS:
            for where in ("top", "passed", "checks_run"):
                status, document = run("build")
                if where == "top":
                    document["outcome"] = "written"
                elif where == "passed":
                    document["passed"]["extra"] = True
                else:
                    document["checks_run"].append(5)
                with self.subTest(job=name, where=where):
                    self.assertIsNotNone(checker(document, "build", "b", status))

    def test_a_flag_that_is_false_or_not_a_boolean_or_missing_fails(self) -> None:
        for name, checker, run, _ in self.CHECKERS:
            status, document = run("build")
            table = next(iter(document["passed"]))
            for value in (False, 1, None):
                changed = copy.deepcopy(document)
                changed["passed"][table] = value
                with self.subTest(job=name, value=value):
                    self.assertIsNotNone(checker(changed, "build", "b", status))
            del document["passed"][table]
            self.assertIsNotNone(checker(document, "build", "b", status), name)

    def test_the_cases_result_has_one_flag_for_each_physical_table(self) -> None:
        _, document = cases_run()
        self.assertEqual({"cases", "lab", "operation_length_measure", "score_cutoffs"}, set(document["passed"]))

    def test_a_hash_field_that_is_not_a_hash_fails_in_either_job(self) -> None:
        for name, checker, run, _ in self.CHECKERS:
            for mode in ("build", "verify"):
                status, document = run(mode)
                for key in (key for key in HASH_FIELDS if document.get(key) is not None and key in document):
                    for bad in ("checked", "A" * 64, "a" * 63, "a" * 65, "g" * 64, "", 5, True, ["a" * 64]):
                        with self.subTest(job=name, mode=mode, key=key, bad=str(bad)[:10]):
                            self.assertIsNotNone(checker({**document, key: bad}, mode, "b", status))

    def test_a_hash_that_is_null_is_accepted_only_where_the_job_writes_none(self) -> None:
        status, document = split_run("verify")
        self.assertIsNone(document["base_hash"])
        self.assertIsNone(split_problem(document, "verify", "b", status))
        for key in ("checked_hash", "branch_hash"):
            self.assertIsNotNone(split_problem({**document, key: None}, "verify", "b", status), key)
        status, document = cases_run("verify")
        self.assertIsNotNone(cases_problem({**document, "branch_hash": None}, "verify", "b", status))

    def test_a_snapshot_id_of_the_split_that_is_missing_null_text_or_a_boolean_fails(self) -> None:
        status, document = split_run("build")
        for key in ("snapshot_id", "source_snapshot_id"):
            for bad in (None, True, "9", 1.5, [1]):
                with self.subTest(key=key, bad=repr(bad)):
                    self.assertIsNotNone(split_problem({**document, key: bad}, "build", "b", status))
            missing = {name: value for name, value in document.items() if name != key}
            self.assertIsNotNone(split_problem(missing, "build", "b", status), key)

    def test_the_snapshot_ids_of_the_cases_result_are_exactly_the_seven_tables_each_a_whole_number(self) -> None:
        status, document = cases_run("build")
        self.assertEqual(7, len(CASES_SNAPSHOT_TABLES))
        self.assertEqual(set(CASES_SNAPSHOT_TABLES), set(document["snapshot_ids"]))
        self.assertIsNone(cases_problem(document, "build", "b", status))
        first = CASES_SNAPSHOT_TABLES[0]
        plants: dict[str, Any] = {
            "an empty map": {},
            "an eighth key": {**document["snapshot_ids"], "silver.other": 5},
            "a key missing": {key: value for key, value in document["snapshot_ids"].items() if key != first},
            "a key renamed": {**{key: value for key, value in document["snapshot_ids"].items() if key != first}, "silver.renamed": 5},
            "a null value": {**document["snapshot_ids"], first: None},
            "a Boolean value": {**document["snapshot_ids"], first: True},
            "a text value": {**document["snapshot_ids"], first: "9"},
            "a float value": {**document["snapshot_ids"], first: 9.5},
            "not a map": [5] * 7,
        }
        for name, value in plants.items():
            with self.subTest(name):
                self.assertIsNotNone(cases_problem({**document, "snapshot_ids": value}, "build", "b", status))

    def test_a_number_that_is_text_or_a_boolean_fails(self) -> None:
        status, document = cases_run()
        for key in ("development_cases", "development_lab_rows"):
            for value in ("5", True, 5.5):
                changed = {**document, key: value}
                with self.subTest(key=key, value=value):
                    self.assertIsNotNone(cases_problem(changed, "build", "b", status))

    def test_a_refusal_is_the_closed_form_with_codes_and_no_count_and_the_exit_status_of_its_codes(self) -> None:
        for job, checker in (("patient_split", split_problem), ("silver_cases", cases_problem)):
            status, document = sealed_refusal(job, "two_sets: failed", "source_table: 2 tables")
            self.assertEqual((1, None), (status, checker(document, "build", "b", status)), job)
            self.assertIsNotNone(checker(document, "build", "b", 0), job)
            self.assertIsNotNone(checker({**document, "diagnostics": [{"code": "two_sets", "count": 2}]}, "build", "b", 1), job)
            self.assertIsNotNone(checker({**document, "diagnostics": [{"code": "Two Sets"}]}, "build", "b", 1), job)
            self.assertIsNotNone(checker({**document, "diagnostics": []}, "build", "b", 1), job)
            self.assertIsNotNone(checker({**document, "extra": 1}, "build", "b", 1), job)
            settings = {**document, "diagnostics": [{"code": "settings"}]}
            self.assertIsNone(checker(settings, "build", "b", 2), job)
            self.assertIsNotNone(checker(settings, "build", "b", 1), job)

    def test_the_codes_of_a_refusal_are_compared_in_order_and_exactly(self) -> None:
        _, document = sealed_refusal("patient_split", "key_null: failed", "split_join: failed")
        self.assertTrue(codes_are(document, ["key_null", "split_join"]))
        self.assertFalse(codes_are(document, ["split_join", "key_null"]))
        self.assertFalse(codes_are(document, ["key_null"]))
        self.assertFalse(codes_are(document, ["key_null", "split_join", "extra"]))
        self.assertFalse(codes_are({}, ["key_null"]))
        self.assertFalse(codes_are({"diagnostics": [{"code": "key_null", "count": 1}]}, ["key_null"]))
        self.assertTrue(codes_are({"diagnostics": []}, []))


if __name__ == "__main__":
    unittest.main()
