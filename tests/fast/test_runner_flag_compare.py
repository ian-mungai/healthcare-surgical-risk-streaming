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
- The profile dump prints and logs the row totals of the numeric and waveform tables, which a held-out change moves. ``tests/fast/test_runner_gold_sealed.py``
  runs the dump on stand-in tables where only held-out rows change and requires an identical dump (fix round 4: the syntax guard that stood here missed a count
  through a variable and a filter on a non-null case).

Failure modes covered here (fix round 5, owner decision D):

- The syntax guard of the load steps missed a total that a local variable carries to the print (and let the names ``snapshot``, ``lab_case``,
  ``held_out_exists`` and ``STEPS`` through). Each runner's load steps now run on stand-in files and tables where only the held-out rows change, and the printed
  load output must stay identical. The planted copies of the stand-ins show that the check sees a total through a variable, through arithmetic, through a list
  of rows, under an allowed name and in a laboratory case that is chosen among the held-out cases too.
"""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import importlib
import io
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from spark.split_table import COLUMNS, RULE_VERSION, SplitRow, build_assignments
from tests.e2e.run_record import Record
from tests.fast.fake_spark import Functions, Session

E2E = Path(__file__).resolve().parents[1] / "e2e"
NAMES = ("cases_with_arterial_waveform", "tracks")
TOTAL = 987654
FLAG_NODES = (ast.Compare, ast.BoolOp, ast.Constant)


def flag_shaped(node: ast.expr) -> bool:
    """A comparison, a boolean operation, a constant, a dictionary of comparisons or a name that holds a case number, steps or a snapshot ID."""
    if isinstance(node, ast.DictComp):
        return flag_shaped(node.value)
    return isinstance(node, FLAG_NODES) or (isinstance(node, ast.Name) and node.id in ALLOWED_NAMES)


ALLOWED_NAMES = {"STEPS"}


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
        self.assertEqual({"name", "expected"}, {key.value for key in row.keys if key is not None})
        spread = [value for key, value in zip(row.keys, row.values, strict=True) if key is None]
        self.assertEqual(1, len(spread))
        self.assertIsInstance(spread[0], ast.IfExp)
        self.assertEqual({"value"}, {key.value for key in spread[0].orelse.keys}, "the value is held for a count that is not sealed only")
        self.assertEqual([], spread[0].body.keys, "a sealed count holds no value field")
        self.assertIn("count_flags", values)
        self.assertIn("sealed_values_empty", values)

    def test_the_dump_holds_no_row_total_of_the_silver_tables(self) -> None:
        source = runner("signal_profile").STAND_IN
        dump = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "dump")
        result = next(
            node.value
            for node in ast.walk(dump)
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "result" and isinstance(node.value, ast.Dict)
        )
        self.assertNotIn("silver_rows", {getattr(key, "value", "") for key in result.keys})


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


CLINICAL_COLUMNS = ["caseid", "subjectid", "caseend", "opstart", "opend", "death_inhosp"]
DEVELOPMENT = [908, 1147]
HELD_BASE = 7000


def held_cases(held: int) -> list[int]:
    return [HELD_BASE + index for index in range(held)]


def subject_of(case: int) -> str:
    """A patient number in the published form."""
    return str(case * 10 + 1)


def test_set_patient() -> str:
    """A patient number in the published form that the split rule puts in the test set."""
    return next(str(number) for number in range(1, 1000) if build_assignments([str(number)])[str(number)] == "test")


def stand_in_namespace(source: str, functions: tuple[str, ...], **given: Any) -> dict[str, Any]:
    """The names that the load steps of a stand-in program define, with Spark, the files and the project's row builders stood in. The program's own ``step``,
    ``first_read`` and ``frame`` and the named load functions run as they are written."""
    wanted = {"step", "first_read", "frame", *functions}
    tree = ast.parse(source)
    keep: list[ast.stmt] = [node for node in tree.body if (isinstance(node, ast.FunctionDef) and node.name in wanted) or isinstance(node, ast.Assign)]
    table_spec = SimpleNamespace(ddl="stand-in")
    names: dict[str, Any] = {
        "json": json,
        "sys": sys,
        "Path": Path,
        "F": Functions,
        "B": SimpleNamespace(NUMERIC=table_spec, WAVEFORM=table_spec, TRACKS=table_spec),
        "COLUMNS": COLUMNS,
        "RULE_VERSION": RULE_VERSION,
        "SplitRow": SplitRow,
        "build_assignments": build_assignments,
        "values_query": lambda rows: ("values", [dataclasses.asdict(row) for row in rows]),
        "snapshot_of": lambda session, table: 5,
        "pin_check": lambda name, path, pin: (names["step"]("pin_check"), names["step"]("pin_ok")),
        **given,
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), "<stand-in>", "exec"), names)  # noqa: S102
    return names


def printed_result(call: Any, *arguments: Any) -> dict[str, Any]:
    """Run a load step and return the one ``RESULT`` line that it printed."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        call(*arguments)
    lines = [line for line in out.getvalue().splitlines() if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise AssertionError(out.getvalue())
    document: dict[str, Any] = json.loads(lines[0][len("RESULT ") :])
    return document


class LoadStepCase(unittest.TestCase):
    """The base of the two load-step tests: a stand-in program, a planted copy of it and the output of its load steps for a number of held-out cases."""

    runner_name = ""
    source = ""

    def setUp(self) -> None:
        self.source = runner(self.runner_name).STAND_IN

    def outputs(self, held: int, source: str | None = None) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def plant(self, line: str, replacement: str) -> str:
        """The program with ``line`` (which must be there) replaced."""
        self.assertIn(line, self.source)
        return self.source.replace(line, replacement)

    def assert_total_is_seen(self, function: str, planted: str) -> None:
        """A stand-in with a total in its output gives another output for a different number of held-out cases."""
        self.assertNotEqual(self.outputs(1, planted)[function], self.outputs(4, planted)[function])


class ProfileLoadSteps(LoadStepCase):
    """The load steps of the profile runner's stand-in, run on files and tables where only the held-out rows change: the printed load output stays identical
    (fix round 5: the syntax guard that stood here missed a total that a local variable carries to the print)."""

    runner_name = "signal_profile"
    FUNCTIONS = ("load_clinical", "load_track_list", "write_split")
    CLINICAL = 'print("RESULT " + json.dumps({"rows_as_pinned": session.table(CLIN).count() == spec["expected"]["cases"], '
    TRACKS = 'print("RESULT " + json.dumps({"rows_as_pinned": session.table(TLIST).count() == spec["expected"]["tracks"], '
    SPLIT = 'print("RESULT " + json.dumps({"patients_as_pinned": len(rows) == spec["expected"]["patients"], '

    def inputs(self, held: int) -> tuple[Session, dict[str, Any]]:
        cases = [*DEVELOPMENT, *held_cases(held), 424242]
        clinical = [
            {"caseid": str(case), "subjectid": subject_of(case), "caseend": "9", "opstart": "1", "opend": "8", "death_inhosp": str(case % 2 + held)}
            for case in cases
        ]
        tracks = [{"caseid": str(case), "tname": f"T{index}", "tid": f"{case:04d}{index}"} for case in cases for index in range(1 + (case >= HELD_BASE) * held)]
        spec = {"pins": {"clinical": {}, "track_list": {}}, "expected": {"cases": len(clinical), "tracks": len(tracks), "patients": len(clinical)}}
        files = {"/pinned/clinical_data.csv": (clinical, CLINICAL_COLUMNS), "/pinned/trks_plain.csv": (tracks, ["caseid", "tname", "tid"])}
        return Session(files=files), spec

    def outputs(self, held: int, source: str | None = None) -> dict[str, dict[str, Any]]:
        names = stand_in_namespace(source or self.source, self.FUNCTIONS)
        session, spec = self.inputs(held)
        found = {name: printed_result(names[name], session, spec) for name in ("load_clinical", "load_track_list")}
        found["write_split"] = printed_result(names["write_split"], session, spec, False)
        again = printed_result(names["write_split"], session, spec, True)
        self.assertEqual(found["write_split"], again)
        return found

    def test_every_load_step_prints_flags_that_are_true_on_the_stand_in_tables(self) -> None:
        for step, result in self.outputs(1).items():
            with self.subTest(step=step):
                flags = {key: value for key, value in result.items() if key not in ("steps", "clinical_snapshot")}
                self.assertTrue(flags)
                self.assertTrue(all(value is True for value in flags.values()), result)

    def test_a_change_of_held_out_rows_alone_leaves_the_load_output_identical(self) -> None:
        self.assertEqual(self.outputs(1), self.outputs(4))

    def test_the_check_sees_a_total_that_a_local_variable_carries_to_the_print(self) -> None:
        plants = {
            "load_clinical": self.plant(
                self.CLINICAL, 'total = session.table(CLIN).count(); print("RESULT " + json.dumps({"rows_as_pinned": True, "rows": total, '
            ),
            "load_track_list": self.plant(
                self.TRACKS, 'total = session.table(TLIST).count(); print("RESULT " + json.dumps({"rows_as_pinned": True, "rows": total, '
            ),
            "write_split": self.plant(self.SPLIT, 'total = len(rows); print("RESULT " + json.dumps({"patients_as_pinned": True, "patients": total, '),
        }
        for function, planted in plants.items():
            with self.subTest(function=function):
                self.assert_total_is_seen(function, planted)

    def test_the_check_sees_a_total_that_a_name_of_the_old_guard_carries(self) -> None:
        """The syntax guard let the names ``snapshot`` and ``STEPS`` through, so a total held in one of them was never seen."""
        planted = self.plant("snapshot = snapshot_of(session, CLIN)", "snapshot = session.table(CLIN).count()")
        self.assert_total_is_seen("write_split", planted)

    def test_the_check_sees_a_total_through_arithmetic_and_a_list_of_rows(self) -> None:
        for label, extra in (("arithmetic", "len(rows) + 0"), ("a list", "[row.subjectid for row in rows]")):
            with self.subTest(label):
                self.assert_total_is_seen("write_split", self.plant(self.SPLIT, self.SPLIT + f'"extra": {extra}, '))


class ReconcileLoadSteps(LoadStepCase):
    """The load steps of the reconcile runner's stand-in, run on files where only the held-out rows change: the printed load output stays identical."""

    runner_name = "silver_reconcile"
    FUNCTIONS = ("load_signals", "load_clinical", "load_lab", "write_split")
    SIGNALS = 'print("RESULT " + json.dumps({"loaded": '
    LAB = '{"has_rows": session.table(LAB).count() > 0, "lab_case": lab_case, '
    DEVELOPMENT_ONLY = 'development = {case: n for case, n in per_case.items() if case != spec["held_case"]}'

    def inputs(self, held: int) -> tuple[Session, dict[str, Any]]:
        """The sample is the development cases and the one held-out case. ``held`` is how many laboratory rows the held-out case has (ten times it) and how
        many rows the case outside the sample has."""
        cases = [*DEVELOPMENT, HELD_BASE]
        clinical = [
            {"caseid": str(case), "subjectid": subject_of(case), "caseend": "9", "opstart": "1", "opend": "8", "death_inhosp": str(held)}
            for case in (*cases, *([424242] * held))
        ]
        lab = [
            {"caseid": str(case), "dt": str(index), "name": f"n{index}", "result": str(held)}
            for case in (*cases, 424242)
            for index in range(10 * held if case >= HELD_BASE else 2 + DEVELOPMENT.index(case))
        ]
        spec = {
            "cases": cases,
            "files": {str(case): {"sha256": "x", "path": "p"} for case in cases},
            "numeric_case": DEVELOPMENT[0],
            "held_case": HELD_BASE,
            "held_patient": test_set_patient(),
            "pins": {"clinical": {}, "lab": {}},
        }
        files = {"/pinned/clinical_data.csv": (clinical, CLINICAL_COLUMNS), "/pinned/lab_data.csv": (lab, ["caseid", "dt", "name", "result"])}
        return Session(files=files), spec

    def outputs(self, held: int, source: str | None = None) -> dict[str, dict[str, Any]]:
        def case_rows(path: Path, case: int, kind: str, lineage: Any) -> list[dict[str, Any]]:
            many = 5 * held if case >= HELD_BASE else 2
            return [{"case_id": case, "track": "T", "case_time_us": index, "source_occurrence": 0, "value": 1.0} for index in range(many)]

        names = stand_in_namespace(
            source or self.source,
            self.FUNCTIONS,
            case_rows=case_rows,
            Lineage=lambda *_: None,
            Window=None,
            occurrences=lambda frame, F, Window: frame,
        )
        session, spec = self.inputs(held)
        return {name: printed_result(names[name], session, spec) for name in self.FUNCTIONS}

    def test_every_load_step_prints_flags_and_one_development_case_that_are_right_on_the_stand_in_tables(self) -> None:
        found = self.outputs(1)
        for step, result in found.items():
            with self.subTest(step=step):
                flags = {key: value for key, value in result.items() if key not in ("steps", "lab_case")}
                self.assertTrue(flags)
                self.assertTrue(all(value is True or isinstance(value, dict) for value in flags.values()), result)
        self.assertEqual(DEVELOPMENT[1], found["load_lab"]["lab_case"])

    def test_a_change_of_held_out_rows_alone_leaves_the_load_output_identical(self) -> None:
        self.assertEqual(self.outputs(1), self.outputs(4))

    def test_the_check_sees_a_total_that_a_local_variable_carries_to_the_print(self) -> None:
        plants = {
            "load_signals": self.plant(self.SIGNALS, 'total = counts; print("RESULT " + json.dumps({"rows": total, "loaded": '),
            "load_lab": self.plant(self.LAB, '{"has_rows": session.table(LAB).count() > 0, "lab_case": lab_case, "per_case": per_case, '),
        }
        for function, planted in plants.items():
            with self.subTest(function=function):
                self.assert_total_is_seen(function, planted)

    def test_the_check_sees_a_total_that_a_name_of_the_old_guard_carries(self) -> None:
        """The syntax guard let the names ``held_out_exists`` and ``lab_case`` through, so a total held in one of them was never seen."""
        planted = self.plant(
            'held_out_exists = kept.filter(F.col("caseid").cast("bigint") == spec["held_case"]).limit(1).count() > 0',
            "held_out_exists = session.table(LAB).count()",
        )
        self.assert_total_is_seen("load_lab", planted)

    def test_the_check_sees_a_laboratory_case_that_is_chosen_among_the_held_out_cases_too(self) -> None:
        planted = self.plant(self.DEVELOPMENT_ONLY, "development = per_case")
        self.assertNotEqual(self.outputs(1, planted)["load_lab"], self.outputs(1)["load_lab"])


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


if __name__ == "__main__":
    unittest.main()
