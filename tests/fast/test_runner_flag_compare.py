"""The profile and reconcile runners compare a full-cohort or held-out total inside Spark and take one pass flag out of the container.

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, Nessie and Spark are stand-ins.

Failure modes covered here (owner decision D, fix round 2):

- The profile runner dumps the stored counts table, rows with a ``matches`` flag for each count, and records the names of the counts that did not match: the
  counts are of the full cohort, so a held-out recording changes which names it records.
- The reconcile stand-in prints the number of laboratory rows (and the rows of the clinical and signal tables and the patients of the split), which a held-out
  fixture row changes, and the runner records it.
- The profile stand-in prints the rows of the full release's clinical table and track list and the patients of its split (the host compares them with the
  pinned numbers), and a stand-in program of either runner prints a total in a load step instead of a flag.

Failure modes covered here (fix round 3, owner decision D):

- The profile runner expects the printed outcome ``written`` after the plant that rewrites only ``source_counts``. The build says ``unchanged`` (the rewrite of
  that table alone never changes the printed outcome), so the case fails on a correct build. The runner expects ``unchanged`` and reads the rewrite from the
  snapshot ID of ``source_counts`` in Nessie, which must be a whole number on the base and another one on the built branch.
- The runner records a pass when the comparison read printed ``as_planted: true`` and then exited 1: the read needs exit 0 and the true flag.
- The profile dump prints and logs the row totals of the numeric and waveform tables, which a held-out change moves.
"""

from __future__ import annotations

import ast
import importlib
import json
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"
NAMES = ("cases_with_arterial_waveform", "tracks")
TOTAL = 987654
LOAD_FUNCTIONS = ("load_signals", "load_clinical", "load_lab", "write_split")
PROFILE_LOADS = ("load_clinical", "load_track_list", "write_split")
FLAG_NODES = (ast.Compare, ast.BoolOp, ast.Constant)


def flag_shaped(node: ast.expr) -> bool:
    """A comparison, a boolean operation, a constant, a dictionary of comparisons or a name that holds a case number, steps or a snapshot ID."""
    if isinstance(node, ast.DictComp):
        return flag_shaped(node.value)
    return isinstance(node, FLAG_NODES) or (isinstance(node, ast.Name) and node.id in ALLOWED_NAMES)


ALLOWED_NAMES = {"STEPS", "lab_case", "held_out_exists", "pin_checked", "snapshot"}


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def texts(record: Record) -> str:
    return json.dumps([item.observed for item in record.observations], sort_keys=True, default=str)


def result_values(source: str, functions: tuple[str, ...]) -> list[tuple[str, str, ast.expr]]:
    """``(function, key, value node)`` for each key of each ``print("RESULT " + json.dumps({...}))`` in the named functions of a stand-in program."""
    found: list[tuple[str, str, ast.expr]] = []
    for function in (node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name in functions):
        for call in (node for node in ast.walk(function) if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "print"):
            dumps = [node for node in ast.walk(call) if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "dumps"]
            for dumped in dumps:
                for node in dumped.args:
                    if isinstance(node, ast.Dict):
                        found.extend((function.name, str(getattr(key, "value", "")), value) for key, value in zip(node.keys, node.values, strict=True))
    return found


def total_values(source: str, functions: tuple[str, ...]) -> list[str]:
    """``function.key`` of each printed value that is not a flag, a constant or an allowed name: a total, a count or a list of rows."""
    return sorted(f"{function}.{key}" for function, key, value in result_values(source, functions) if not flag_shaped(value))


class Rule(unittest.TestCase):
    def test_a_printed_count_is_found_and_a_flag_is_not(self) -> None:
        source = 'def load_lab(session, spec):\n    print("RESULT " + json.dumps({"rows": session.table(LAB).count(), "has_rows": rows > 0, "steps": STEPS}))\n'
        self.assertEqual(["load_lab.rows"], total_values(source, ("load_lab",)))

    def test_a_dictionary_of_counts_and_a_length_are_found(self) -> None:
        source = 'def load_signals(session, spec):\n    print("RESULT " + json.dumps({"rows": counts, "patients": len(rows)}))\n'
        self.assertEqual(["load_signals.patients", "load_signals.rows"], total_values(source, ("load_signals",)))

    def test_a_function_that_is_not_named_is_not_judged(self) -> None:
        source = 'def dump(session, spec):\n    print("RESULT " + json.dumps({"rows": 3}))\n'
        self.assertEqual([], total_values(source, ("load_lab",)))


class ProfileMismatch(unittest.TestCase):
    COUNTS = "silver.source_counts"

    def run_of(self, stand: Any, outcome: Any = "unchanged", snapshots: tuple[Any, Any] = (31, 32), build_code: int = 0) -> tuple[Any, Record, list[str]]:
        """The mismatch case on stand-ins: the build prints ``outcome``, and Nessie holds ``snapshots`` (the base, the built branch) for ``source_counts``."""
        module = runner("signal_profile")
        record = Record(feature="test", slug="test", rerun="none")
        run = module.Run.__new__(module.Run)
        modes: list[str] = []
        run.record, run.trunk = record, "trunk"
        run.stack = SimpleNamespace(
            profile=lambda mode, branch, base=None, pinned=False: (
                build_code,
                {"outcome": outcome, "source_counts": {"pinned": True, "passed": True}},
                "",
                0.0,
            )
        )
        run.fork = lambda base, label: f"{label}_branch"
        run.name = lambda base: f"{base}_built"
        run.table_snapshot = lambda branch, table: snapshots[1] if branch.endswith("_built") and table == self.COUNTS else snapshots[0]

        def asked(mode: str, branch: str, **values: Any) -> tuple[int, dict[str, Any], str, float]:
            modes.append(mode)
            answer: tuple[int, dict[str, Any], str, float] = stand(mode, values)
            return answer

        run.stand = asked
        run.mismatch()
        return run, record, modes

    @staticmethod
    def leaky(mode: str, values: dict[str, Any]) -> tuple[int, dict[str, Any], str, float]:
        """What an old dump answers: each stored count with its ``matches`` flag, the names of the mismatches among them."""
        rows = [{"name": name, "matches": name not in NAMES, "value": None, "expected": 1.0} for name in (*NAMES, "patients")]
        return 0, {"counts": rows, "as_planted": True, "tampered": "track_list"}, "", 0.0

    @staticmethod
    def answering(answer: Any, code: int = 0, plant_code: int = 0) -> Any:
        def stand(mode: str, values: dict[str, Any]) -> tuple[int, dict[str, Any], str, float]:
            return (plant_code if mode == "tamper" else code), {"as_planted": answer}, "", 0.0

        return stand

    def passed(self, record: Record) -> bool:
        return bool(record.observations) and all(item.passed for item in record.observations)

    def test_the_mismatch_case_asks_for_one_flag_and_records_no_name_of_a_count(self) -> None:
        _, record, modes = self.run_of(self.leaky)
        self.assertNotIn("dump", modes)
        self.assertEqual(["tamper", "counts_flag"], modes)
        for name in NAMES:
            self.assertNotIn(name, texts(record))
        self.assertTrue(self.passed(record), texts(record))

    def test_the_mismatch_case_fails_when_the_flag_is_false_or_not_a_flag(self) -> None:
        answers: list[Any] = [False, None, 1, "True", [], {}]
        for answer in answers:
            with self.subTest(answer=answer):
                _, record, _ = self.run_of(self.answering(answer))
                self.assertFalse(self.passed(record))

    def test_the_mismatch_case_fails_when_the_flag_read_exits_non_zero_after_printing_the_true_flag(self) -> None:
        for code in (1, 2, 70, 124):
            with self.subTest(code=code):
                _, record, _ = self.run_of(self.answering(True, code=code))
                self.assertFalse(self.passed(record))
        _, record, _ = self.run_of(self.answering(True))
        self.assertTrue(self.passed(record), texts(record))

    def test_the_mismatch_case_fails_when_the_plant_exits_non_zero(self) -> None:
        _, record, _ = self.run_of(self.answering(True, plant_code=1))
        self.assertFalse(self.passed(record))

    def test_the_build_that_rewrites_only_the_source_counts_says_unchanged(self) -> None:
        _, record, _ = self.run_of(self.answering(True), outcome="unchanged")
        self.assertTrue(self.passed(record), texts(record))
        for outcome in ("written", "verified", None, True):
            with self.subTest(outcome=outcome):
                _, record, _ = self.run_of(self.answering(True), outcome=outcome)
                self.assertFalse(self.passed(record))

    def test_the_rewrite_is_read_from_the_snapshot_id_of_the_source_counts_in_nessie(self) -> None:
        for snapshots in ((31, 31), (31, None), (None, 32), (None, None), ("31", "32"), (True, 2)):
            with self.subTest(snapshots=snapshots):
                _, record, _ = self.run_of(self.answering(True), snapshots=snapshots)
                self.assertFalse(self.passed(record))
        _, record, _ = self.run_of(self.answering(True), snapshots=(31, 4_000_000_000))
        self.assertTrue(self.passed(record), texts(record))

    def test_the_mismatch_case_fails_when_the_build_exits_non_zero(self) -> None:
        _, record, modes = self.run_of(self.answering(True), build_code=1)
        self.assertFalse(self.passed(record))
        self.assertNotIn("counts_flag", modes)

    def test_the_stand_in_compares_the_names_inside_the_container(self) -> None:
        module = runner("signal_profile")
        source = module.STAND_IN
        names = [node.name for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)]
        self.assertIn("counts_flag", names)
        self.assertEqual({"as_planted"}, {key for function, key, _ in result_values(source, ("counts_flag",))})
        self.assertEqual([], total_values(source, ("counts_flag",)))

    def test_the_dump_holds_no_flag_for_a_named_count(self) -> None:
        source = runner("signal_profile").STAND_IN
        dump = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "dump")
        result = next(
            node.value
            for node in ast.walk(dump)
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "result" and isinstance(node.value, ast.Dict)
        )
        values = {getattr(key, "value", ""): value for key, value in zip(result.keys, result.values, strict=True)}
        row = values["counts"].elt  # type: ignore[attr-defined]
        self.assertEqual({"name", "value", "expected"}, {key.value for key in row.keys})
        self.assertIn("count_flags", values)

    def test_the_dump_prints_no_total_of_a_waveform_or_numeric_table_that_a_held_out_change_moves(self) -> None:
        source = runner("signal_profile").STAND_IN
        dump = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "dump")
        result = next(
            node.value
            for node in ast.walk(dump)
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "result" and isinstance(node.value, ast.Dict)
        )
        self.assertNotIn("silver_rows", {getattr(key, "value", "") for key in result.keys})
        self.assertEqual([], whole_table_counts(dump, ("SNUM", "SWAV")))


def whole_table_counts(function: ast.FunctionDef, tables: tuple[str, ...]) -> list[str]:
    """The tables of ``tables`` that ``function`` counts with no ``filter`` between ``session.table(<name>)`` and ``.count()``: a total of every case."""
    found: list[str] = []
    for call in (node for node in ast.walk(function) if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "count"):
        chain: ast.expr = call.func.value  # type: ignore[attr-defined]
        filtered, table = False, None
        while True:
            if isinstance(chain, ast.Call) and isinstance(chain.func, ast.Attribute):
                if chain.func.attr == "table" and chain.args and isinstance(chain.args[0], ast.Name) and chain.args[0].id in tables:
                    table = chain.args[0].id
                filtered = filtered or chain.func.attr == "filter"
                chain = chain.func.value
            elif isinstance(chain, ast.Attribute):
                chain = chain.value
            else:
                break
        if table is not None and not filtered:
            found.append(table)
    return sorted(found)


class WholeTableCounts(unittest.TestCase):
    def found(self, body: str) -> list[str]:
        function = next(node for node in ast.parse(f"def dump(session, spec):\n    {body}\n").body if isinstance(node, ast.FunctionDef))
        return whole_table_counts(function, ("SNUM", "SWAV"))

    def test_a_count_of_a_whole_table_is_found_and_a_count_of_chosen_cases_is_not(self) -> None:
        self.assertEqual(["SNUM"], self.found("n = session.table(SNUM).count()"))
        self.assertEqual(["SNUM", "SWAV"], self.found('n = {"a": session.table(SNUM).count(), "b": session.table(SWAV).count()}'))
        self.assertEqual([], self.found('n = session.table(SNUM).filter(F.col("case_id").isin(ids)).count()'))
        self.assertEqual([], self.found("n = session.table(CNT).count()"))


class ProfileLoads(unittest.TestCase):
    def answers(self, **flags: bool) -> list[tuple[int, dict[str, Any], str, float]]:
        steps = ["pin_check", "pin_ok", "first_read"]
        return [
            (0, {"rows_as_pinned": flags.get("cases", True), "pin_checked": True, "steps": steps, "rows": TOTAL}, "", 0.0),
            (0, {"rows_as_pinned": flags.get("tracks", True), "pin_checked": True, "steps": steps, "rows": TOTAL}, "", 0.0),
            (0, {"patients_as_pinned": flags.get("patients", True), "clinical_snapshot": 5, "patients": TOTAL}, "", 0.0),
        ]

    def run_setup(self, answers: list[tuple[int, dict[str, Any], str, float]]) -> Record:
        module = runner("signal_profile")
        record = Record(feature="test", slug="test", rerun="none")
        run = module.Run.__new__(module.Run)
        run.record, run.facts, run.made, run.trunk = record, {}, [], "trunk"
        run.nessie = SimpleNamespace(create_branch=lambda *_: None)
        run.stack = SimpleNamespace(silver=lambda *_, **__: (1, {}, "", 0.0))
        loaded = {"rows": {"numeric": 3, "waveform": 3, "tracks": 3}, "arterial_track_of_the_plant_case": 1}
        run.publish = lambda label, steps: (True, [(0, loaded, "", 0.0), (0, {"first": 1}, "", 0.0)] if label == "bronze" else answers)
        run.name = lambda base: base
        run.setup()
        return record

    def test_the_setup_takes_flags_for_the_release_totals_and_records_no_total(self) -> None:
        record = self.run_setup(self.answers())
        self.assertNotIn(str(TOTAL), texts(record))
        self.assertTrue(all(item.passed for item in record.observations[:3]), texts(record))

    def test_the_setup_fails_when_a_total_flag_is_false_or_missing(self) -> None:
        for name in ("cases", "tracks", "patients"):
            with self.subTest(name=name):
                record = self.run_setup(self.answers(**{name: False}))
                self.assertFalse(record.observations[2].passed, texts(record))
        record = self.run_setup([(0, {"rows": TOTAL, "pin_checked": True}, "", 0.0)] * 3)
        self.assertFalse(record.observations[2].passed, texts(record))

    def test_the_load_steps_of_the_stand_in_print_flags_and_no_total(self) -> None:
        self.assertEqual([], total_values(runner("signal_profile").STAND_IN, PROFILE_LOADS))


class ReconcileLoads(unittest.TestCase):
    def stand_answers(self) -> dict[str, dict[str, Any]]:
        """What the load steps answer: the flags, and a total that an old stand-in also printed."""
        steps = ["pin_check", "pin_ok", "first_read"]
        return {
            "load_signals": {"loaded": {"numeric": True, "waveform": True, "tracks": True}, "planted_one": True, "rows": {"numeric": TOTAL}},
            "load_clinical": {"cases_loaded": True, "pin_checked": True, "steps": steps, "rows": TOTAL},
            "load_lab": {
                "has_rows": True,
                "lab_case": runner("silver_reconcile").CASES[0],
                "held_out_lab_exists": True,
                "pin_checked": True,
                "steps": steps,
                "rows": TOTAL,
            },
            "write_split": {"split_written": True, "held_patient_in_test": True, "patients": TOTAL},
        }

    def run_setup(self) -> tuple[Record, bool]:
        module = runner("silver_reconcile")
        record = Record(feature="test", slug="test", rerun="none")
        run = module.Run.__new__(module.Run)
        answers = self.stand_answers()
        run.record, run.facts, run.made, run.trunk, run.lab_case = record, {}, [], "trunk", 0
        run.nessie = SimpleNamespace(create_branch=lambda *_: None)
        run.stack = SimpleNamespace(silver_signals=lambda *_: (1, {}, "", 0.0), silver_cases=lambda *_: (1, {}, "", 0.0))
        run.publish = lambda label, modes: (True, [(0, answers[mode], "", 0.0) for mode in modes])
        run.name = lambda base: base
        return record, run.setup()

    def test_the_setup_takes_flags_and_records_no_total(self) -> None:
        record, _ = self.run_setup()
        self.assertNotIn(str(TOTAL), texts(record))
        for item in record.observations[:3]:
            self.assertTrue(item.passed, item.observed)

    def test_the_setup_fails_when_a_flag_is_false(self) -> None:
        for mode, key in (("load_signals", "loaded"), ("load_clinical", "cases_loaded"), ("load_lab", "has_rows"), ("write_split", "split_written")):
            with self.subTest(mode=mode):
                module = runner("silver_reconcile")
                self.assertIn(mode, module.STAND_IN)
                answers = self.stand_answers()
                answers[mode][key] = False if key != "loaded" else {"numeric": True, "waveform": False, "tracks": True}
                record = Record(feature="test", slug="test", rerun="none")
                run = module.Run.__new__(module.Run)
                run.record, run.facts, run.made, run.trunk, run.lab_case = record, {}, [], "trunk", 0
                run.nessie = SimpleNamespace(create_branch=lambda *_: None)
                run.stack = SimpleNamespace(silver_signals=lambda *_: (1, {}, "", 0.0), silver_cases=lambda *_: (1, {}, "", 0.0))
                run.publish = lambda label, modes, answers=answers: (True, [(0, answers[mode], "", 0.0) for mode in modes])
                run.name = lambda base: base
                run.setup()
                self.assertFalse(all(item.passed for item in record.observations))

    def test_the_load_steps_of_the_stand_in_print_flags_and_no_total(self) -> None:
        source = runner("silver_reconcile").STAND_IN
        self.assertEqual([], total_values(source, LOAD_FUNCTIONS))


if __name__ == "__main__":
    unittest.main()
