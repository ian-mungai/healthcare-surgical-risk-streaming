"""The Gold dimensions runner is sealed: it loads the stand-in tables through the sealed Silver cases helper, reads its refusals through a Gold adapter, and
takes the lab dimension's dump and history comparison out of Spark as flags.

The runner stays untracked, so each test skips when it is not on this machine. Docker, Nessie and Spark are stand-ins.

Failure modes covered here (fix round 4, owner decision D):

- The Gold setup asks the stand-in loader with the old Bronze contract: the loader raises ``KeyError('expected_cases')`` on a spec that holds the cases
  only, and the setup rejects a valid answer with all four loader flags true because it expects ``cases`` and ``types``.
- The sealed launcher returns empty standard error, so the refusal code that the real command line prints (``empty_source: failed``, exit 1) is never seen. A
  Gold adapter reads the codes from the stream and keeps only known codes, and a line that holds anything else (a value, a key, a count of the lab dimension)
  is dropped.
- The lab dimension's dump and history comparison return counts (members, versions, prior and after rows), which a held-out laboratory row changes. They
  return flags that Spark makes, and a held-out laboratory change leaves the flags identical.
- The profile dump holds a total that a held-out row changes. A test runs the dump on stand-in tables where only held-out rows change and requires an
  identical dump (the earlier guard read the syntax of the dump and missed a count through a variable and a filter on a non-null case).
"""

from __future__ import annotations

import ast
import contextlib
import copy
import csv
import importlib
import io
import json
import runpy
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def texts(record: Record) -> str:
    return json.dumps([item.observed for item in record.observations], sort_keys=True, default=str)


def namespace(source: str, **given: Any) -> dict[str, Any]:
    """The names that a stand-in helper program defines, run with the given names (the Spark module is never imported)."""
    with tempfile.TemporaryDirectory(prefix="gold_helpers_") as folder:
        path = Path(folder) / "helpers.py"
        path.write_text(source)
        return runpy.run_path(str(path), init_globals={"json": json, **given})


class SetupLoad(unittest.TestCase):
    """The setup's call of the loader of the sealed Silver cases helper."""

    FLAGS = ("cases_as_expected", "columns_as_expected", "every_column_a_string", "lab_total_ok")

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gold_load_")))
        cases = [str(case) for case in self.module.CASES]
        with open(self.folder / "lab.csv", "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["caseid", "dt", "name", "result"])
            for case in (*cases, *cases[:2], "424242"):
                writer.writerow([case, "1", "x", "2"])
        self.lab_rows = len(cases) + 2

    def setup_of(self, answer: dict[str, Any], code: int = 0) -> tuple[Record, list[dict[str, Any]]]:
        record = Record(feature="test", slug="test", rerun="none")
        specs: list[dict[str, Any]] = []
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.made, run.trunk, run.sources = record, [], "trunk", (self.folder / "clinical.csv", self.folder / "lab.csv")
        run.nessie = SimpleNamespace(create_branch=lambda *_: None)
        run.stack = SimpleNamespace(write_spec=specs.append, stand_in=lambda *_, **__: (code, answer, "", 0.1), split=lambda *_, **__: (1, {}, "", 0.0))
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.merge = lambda *_, **__: "merged"
        run.recreate = lambda *_: None
        run.setup()
        return record, specs

    def answer(self, **flags: Any) -> dict[str, Any]:
        return {name: flags.get(name, True) for name in self.FLAGS}

    def test_the_spec_holds_every_key_that_the_loader_reads(self) -> None:
        base = self.module.base
        branch = next(node for node in ast.walk(ast.parse(base.STAND_IN)) if isinstance(node, ast.If) and ast.unparse(node.test) == "mode == 'load'")
        read = {
            node.slice.value
            for statement in branch.body
            for node in ast.walk(statement)
            if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "spec" and isinstance(node.slice, ast.Constant)
        }
        _, specs = self.setup_of(self.answer())
        self.assertTrue(read)
        self.assertEqual(set(), read - set(specs[0]))

    def test_the_spec_names_the_sample_size_the_column_count_and_the_laboratory_rows_of_the_sample(self) -> None:
        _, specs = self.setup_of(self.answer())
        self.assertEqual([str(case) for case in self.module.CASES], specs[0]["cases"])
        self.assertEqual(len(self.module.CASES), specs[0]["expected_cases"])
        self.assertEqual(self.module.base.CLINICAL_COLUMNS, specs[0]["columns"])
        self.assertEqual(self.lab_rows, specs[0]["lab_rows"])

    def test_an_answer_with_the_four_flags_true_passes(self) -> None:
        record, _ = self.setup_of(self.answer())
        self.assertTrue(record.observations[0].passed, texts(record))

    def test_each_false_or_missing_or_untrue_flag_fails(self) -> None:
        for name in self.FLAGS:
            for value in (False, None, 1, "True", [True]):
                with self.subTest(name=name, value=value):
                    record, _ = self.setup_of(self.answer(**{name: value}))
                    self.assertFalse(record.observations[0].passed)
            with self.subTest(name=name, value="missing"):
                record, _ = self.setup_of({key: True for key in self.FLAGS if key != name})
                self.assertFalse(record.observations[0].passed)

    def test_the_old_answer_and_a_failed_exit_fail(self) -> None:
        record, _ = self.setup_of({"cases": len(self.module.CASES), "types": ["string"]})
        self.assertFalse(record.observations[0].passed)
        record, _ = self.setup_of(self.answer(), code=1)
        self.assertFalse(record.observations[0].passed)

    def test_the_laboratory_rows_of_the_sample_count_the_rows_of_the_sample_cases_only(self) -> None:
        self.assertEqual(self.lab_rows, self.module.lab_rows_of(self.folder / "lab.csv", [str(case) for case in self.module.CASES]))
        self.assertEqual(0, self.module.lab_rows_of(self.folder / "lab.csv", ["1"]))


class Refusals(unittest.TestCase):
    """The Gold adapter: the codes of the Gold job's stream, known codes only."""

    LINES = {
        "empty_source: failed": "gold_dimensions: empty_source: failed",
        "key_rule: dim_case, 1 member": "gold_dimensions: key_rule: dim_case, 1 member",
        "unknown_member: dim_track, 2 rows": "gold_dimensions: unknown_member: dim_track, 2 rows",
        "branch_missing": "gold_dimensions: branch_missing",
        "usage": "gold_dimensions: usage",
        "timeout": "gold_dimensions: timeout",
        "output_schema: 1 table": "gold_dimensions: output_schema: 1 table",
    }

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def test_a_known_code_is_kept_in_each_of_its_forms(self) -> None:
        for wanted, line in self.LINES.items():
            with self.subTest(wanted):
                self.assertEqual([wanted], self.module.codes(line + "\n"))

    def test_everything_else_is_dropped(self) -> None:
        unknown = [
            "gold_dimensions: key_rule: dim_lab_test, 3 members",
            "gold_dimensions: brand_new_code",
            "gold_dimensions: usage: failed",
            "gold_dimensions: timeout: 1 row",
            "gold_dimensions: key_rule",
            "a_different_job: usage",
            "gold_dimensions: empty_source: failed 4417",
            "gold_dimensions: member_removed: dim_case, SECRET_4417",
            "gold_dimensions: key_rule: other_name, 1 member",
            "Traceback (most recent call last):",
            "bronze_decode: internal_error",
            "  gold_dimensions: usage",
            "",
        ]
        for line in unknown:
            with self.subTest(line=line):
                self.assertEqual([], self.module.codes(line + "\n"))

    def test_a_mixed_stream_gives_the_known_codes_in_order(self) -> None:
        stream = "WARN spark 4417\ngold_dimensions: key_null: dim_case, 1 row\ngold_dimensions: nothing\ngold_dimensions: empty_source: failed\n"
        self.assertEqual(["key_null: dim_case, 1 row", "empty_source: failed"], self.module.codes(stream))

    def stack(self, streams: list[Any]) -> Any:
        record, log = Record(feature="test", slug="test", rerun="none"), list[str]()
        directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gold_stack_")))
        stack = self.module.GoldStack(Path("compose"), "proj_x1", directory, record, log)
        answers = iter(streams)

        def answer(program: str, arguments: list[str], **_: Any) -> Any:
            if "--entrypoint" not in arguments:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            item = next(answers)
            if isinstance(item, Exception):
                raise item
            return SimpleNamespace(returncode=item[0], stdout=item[1], stderr=item[2])

        self.enterContext(mock.patch.object(self.module.base, "run_command", side_effect=answer))
        return stack

    def test_the_job_returns_the_known_codes_of_its_stream_as_its_error_text(self) -> None:
        stack = self.stack([(1, "", "WARN 4417\ngold_dimensions: empty_source: failed\nTraceback 4417\n")])
        code, _, err, _ = stack.gold("build", "branch", base_branch="trunk")
        self.assertEqual(1, code)
        self.assertEqual(["empty_source: failed"], self.module.codes(err))
        self.assertNotIn("4417", err)

    def test_the_empty_lab_refusal_is_accepted_through_the_gold_launcher(self) -> None:
        stack = self.stack([(1, "", "gold_dimensions: empty_source: failed\n")])
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.gold, run.trunk = record, stack, "trunk"
        run.stack = SimpleNamespace(spark_containers=lambda: [])
        run.head = lambda branch: "h"
        run.name = lambda base: base
        run.refused_from("an empty laboratory table", "built", "source", self.module.PLANTED_LAB_CODES)
        self.assertTrue(record.observations[0].passed, texts(record))

    def test_the_stream_of_a_stopped_attempt_is_not_kept_for_the_next_job(self) -> None:
        stack = self.stack([(1, "", "gold_dimensions: key_null: dim_case, 1 row\n"), TimeoutExpired(["x"], 1.0)])
        command = ["docker", "compose", "run", "--entrypoint", "python", "spark"]
        self.assertEqual((1, "", "gold_dimensions: key_null: dim_case, 1 row\n"), stack.run(command))
        with self.assertRaises(TimeoutExpired):
            stack.run(command)
        self.assertEqual("", stack.last_codes)

    def test_the_command_that_is_not_a_job_keeps_its_own_streams(self) -> None:
        def answer(program: str, arguments: list[str], **_: Any) -> Any:
            return SimpleNamespace(returncode=3, stdout="out 4417", stderr="gold_dimensions: usage\nerr 4417")

        stack = self.stack([])
        stack.last_codes = "kept"
        with mock.patch.object(self.module.base, "run_command", side_effect=answer):
            self.assertEqual((3, "out 4417", "gold_dimensions: usage\nerr 4417"), stack.run(["docker", "ps"]))
        self.assertEqual("kept", stack.last_codes)

    def test_a_job_returns_no_raw_stream_to_its_caller(self) -> None:
        stack = self.stack([])

        def answer(program: str, arguments: list[str], **_: Any) -> Any:
            return SimpleNamespace(returncode=1, stdout="RESULT {}\nout 4417", stderr="gold_dimensions: empty_source: failed\nerr 4417")

        with mock.patch.object(self.module.base, "run_command", side_effect=answer):
            code, out, err = stack.run(["docker", "compose", "run", "--entrypoint", "python", "spark"])
        self.assertEqual((1, "gold_dimensions: empty_source: failed\n"), (code, err))
        self.assertIn("out 4417", out)

    def test_a_sealed_job_still_returns_no_error_text(self) -> None:
        stack = self.stack([])

        def sealed_job(self: Any, *arguments: Any, **keywords: Any) -> tuple[int, dict[str, Any], str, float]:
            self.last_codes = "gold_dimensions: key_null: dim_case, 1 row\n"
            return 1, {"x": 1}, "", 0.1

        with mock.patch.object(self.module.base.Stack, "job", sealed_job):
            self.assertEqual("", stack.job(["-m", "x"], sealed=("cases", "build", "b"))[2])
            self.assertEqual("gold_dimensions: key_null: dim_case, 1 row\n", stack.job(["-m", "x"])[2])


class HistoryFlags(unittest.TestCase):
    """The history comparison of the lab dimension is made in Spark and leaves it as flags."""

    COLUMNS = ["lab_test_key", "lab_test_name", "valid_from", "valid_to", "is_current"]
    EXPECT = {"added": False, "closing": False, "corrected": False, "closed_before": False}

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.helpers = namespace(self.module.HISTORY_HELPERS)

    def rows(self, names: list[str]) -> list[tuple[Any, ...]]:
        return [(-1, None, "2026-11-23", None, True), *[(index + 1, name, "2026-11-23", None, True) for index, name in enumerate(names)]]

    def compare(self, prior: list[tuple[Any, ...]], after: list[tuple[Any, ...]], expect: dict[str, bool] | None) -> dict[str, Any]:
        found: dict[str, Any] = self.helpers["compare_history"](self.COLUMNS, "lab_test_key", prior, after, "2026-11-24", expect)
        return found

    def test_with_an_expectation_only_flags_leave_the_comparison(self) -> None:
        found = self.compare(self.rows(["a", "b"]), self.rows(["a", "b"]), self.EXPECT)
        self.assertEqual({"controls"}, set(found))
        self.assertTrue(found["controls"])
        self.assertTrue(all(value is True for value in found["controls"].values()), found)

    def test_a_held_out_laboratory_row_leaves_the_flags_identical(self) -> None:
        small = self.compare(self.rows(["a", "b"]), self.rows(["a", "b"]), self.EXPECT)
        larger = self.compare(self.rows(["a", "b", "held_out_1", "held_out_2"]), self.rows(["a", "b", "held_out_1", "held_out_2"]), self.EXPECT)
        self.assertEqual(small, larger)

    def test_a_fault_is_a_false_flag(self) -> None:
        prior = self.rows(["a", "b"])
        cleared = [(*row[:1], None, *row[2:]) if row[0] == 1 else row for row in prior]
        found = self.compare(prior, cleared, self.EXPECT)
        self.assertIs(False, found["controls"]["prior_versions_kept_whole"])
        extra = self.compare(prior, [*prior, (9, "c", "2026-11-24", None, True)], self.EXPECT)
        self.assertIs(False, extra["controls"]["version_count_change"])
        self.assertIs(False, extra["controls"]["openings"] and extra["controls"]["version_count_change"])

    def test_without_an_expectation_the_open_dimensions_keep_their_counts(self) -> None:
        found = self.compare(self.rows(["a"]), self.rows(["a"]), None)
        self.assertEqual(2, found["prior_rows"])
        self.assertEqual(2, found["after_rows"])

    def sealed_facts(self, **changes: Any) -> dict[str, Any]:
        facts: dict[str, Any] = {}
        for item in self.module.DIMENSIONS:
            if item.sealed:
                facts[item.name] = {"controls": {name: True for name in self.module.HISTORY_CONTROLS}}
            else:
                facts[item.name] = {"columns_equal": True, "prior_rows": 2, "closed_prior": 0, "after_rows": 2, "newly_closed": 0, "opened": 0}
                facts[item.name].update(opened_ok=0, opened_corrected=0, prior_changed=0, closed_changed=0, other_state_change=0)
        for name, value in changes.items():
            facts[name] = value
        return facts

    def test_the_host_judges_the_flags_of_the_sealed_dimension_and_the_counts_of_the_others(self) -> None:
        lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)
        facts = self.sealed_facts()
        self.assertTrue(self.module.history_complete(facts))
        self.assertEqual([], self.module.history_problems(facts, set(), set(), set(), set()))
        broken = {name: name != "closings" for name in self.module.HISTORY_CONTROLS}
        facts = self.sealed_facts(**{lab: {"controls": broken}})
        self.assertEqual([f"{lab}: closings"], self.module.history_problems(facts, set(), set(), set(), set()))

    def test_a_sealed_answer_that_holds_a_count_or_lacks_a_flag_is_not_complete(self) -> None:
        lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)
        answers: list[Any] = [{}, {"controls": {}}, {"controls": {name: 1 for name in self.module.HISTORY_CONTROLS}}, {"prior_rows": 3}, None]
        for answer in answers:
            with self.subTest(answer=answer):
                self.assertFalse(self.module.history_complete(self.sealed_facts(**{lab: answer})))

    def test_the_run_asks_spark_with_the_expectation_of_each_dimension(self) -> None:
        specs: list[dict[str, Any]] = []
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.stack = SimpleNamespace(write_spec=specs.append, stand_in=lambda *_: (0, self.sealed_facts(), "", 0.1))
        expect = self.module.expectations({"dim_case"}, {"dim_case"}, set(), {"dim_case"})
        run.history("branch", {item.name: 1 for item in self.module.DIMENSIONS}, "2026-11-24", expect)
        self.assertEqual({item.name for item in self.module.DIMENSIONS}, set(specs[0]["expect"]))
        self.assertEqual({"added": True, "closing": True, "corrected": False, "closed_before": True}, specs[0]["expect"]["dim_case"])
        self.assertEqual({"added": False, "closing": False, "corrected": False, "closed_before": False}, specs[0]["expect"]["dim_track"])

    def test_the_container_answers_the_flags_for_the_sealed_dimension_only(self) -> None:
        tree = ast.parse(self.module.HISTORY_HELPERS)
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "gold_history")
        self.assertIn("SEALED", {node.id for node in ast.walk(function) if isinstance(node, ast.Name)})
        self.assertIn("expect", ast.unparse(function))


class DumpFlags(unittest.TestCase):
    """The independent read of the lab dimension leaves Spark as flags and the schema, never as a count."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.helpers = namespace(self.module.GOLD_HELPERS)

    def counts(self, members: int = 3, **changes: Any) -> dict[str, Any]:
        found: dict[str, Any] = {
            "rows": members + 1,
            "members": members,
            "expected_members": members,
            "extra_members": 0,
            "missing_members": 0,
            "versions": members,
            "wrong_keys": 0,
            "distinct_keys": members + 1,
            "unknown_rows": 1,
            "unknown_holding_a_value": 0,
        }
        found.update(changes)
        return found

    def facts(self, name: str, counts: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = self.helpers["facts_for"](name, counts, ["a"], ["string"])
        return result

    def test_the_sealed_dimension_holds_no_count(self) -> None:
        lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)
        found = self.facts(lab, self.counts())
        self.assertEqual({"flags", "columns", "types"}, set(found))
        self.assertTrue(all(value is True for value in found["flags"].values()))
        self.assertEqual([], [value for value in found["flags"].values() if not isinstance(value, bool)])

    def test_a_held_out_laboratory_row_leaves_the_flags_of_the_sealed_dimension_identical(self) -> None:
        lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)
        self.assertEqual(self.facts(lab, self.counts(3)), self.facts(lab, self.counts(300)))

    def test_each_fault_is_a_false_flag(self) -> None:
        lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)
        faults = {
            "members_as_source": {"expected_members": 4},
            "no_extra_or_missing": {"extra_members": 1},
            "keys_ok": {"wrong_keys": 1},
            "one_version_each": {"versions": 5},
            "keys_distinct": {"distinct_keys": 3},
            "one_unknown": {"unknown_rows": 2},
            "unknown_empty": {"unknown_holding_a_value": 1},
        }
        for flag, change in faults.items():
            with self.subTest(flag=flag):
                self.assertIs(False, self.facts(lab, self.counts(**change))["flags"][flag])
        self.assertIs(False, self.facts(lab, self.counts(0, expected_members=0))["flags"]["members_as_source"])

    def test_an_open_dimension_keeps_its_counts_beside_the_flags(self) -> None:
        found = self.facts("dim_case", self.counts())
        self.assertEqual(3, found["members"])
        self.assertIn("flags", found)

    def test_the_dump_of_the_container_builds_each_dimension_through_the_helper(self) -> None:
        function = next(node for node in ast.parse(self.module.GOLD_HELPERS).body if isinstance(node, ast.FunctionDef) and node.name == "gold_dump")
        assigned = [node for node in ast.walk(function) if isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == "out[name]"]
        self.assertEqual(1, len(assigned))
        self.assertIn("facts_for(", ast.unparse(assigned[0].value))

    def good(self, item: Any) -> dict[str, Any]:
        found: dict[str, Any] = self.helpers["facts_for"](item.name, self.counts(), [c.name for c in item.columns], [c.type for c in item.columns])
        return found

    def test_the_stand_in_program_holds_each_helper_and_the_host_names_every_flag(self) -> None:
        defined = {node.name for node in ast.parse(self.module.STAND_IN).body if isinstance(node, ast.FunctionDef)}
        self.assertGreaterEqual(defined, {"dump_flags", "facts_for", "history_controls", "compare_history", "gold_history", "gold_dump"})
        self.assertEqual(set(self.module.DUMP_FLAGS), set(self.helpers["dump_flags"](self.counts())))

    def test_the_helpers_in_the_container_have_no_annotation_that_needs_a_name_of_the_host(self) -> None:
        for source in (self.module.GOLD_HELPERS, self.module.HISTORY_HELPERS):
            for node in ast.walk(ast.parse(source)):
                if isinstance(node, ast.FunctionDef):
                    with self.subTest(function=node.name):
                        self.assertIsNone(node.returns)
                        self.assertEqual([], [arg.arg for arg in [*node.args.args, *node.args.kwonlyargs] if arg.annotation is not None])

    def test_the_host_judges_the_flags_the_columns_and_the_types(self) -> None:
        for item in self.module.DIMENSIONS:
            with self.subTest(item=item.name):
                facts = self.good(item)
                self.assertEqual([], self.module.dump_problems(item, facts))
                self.assertTrue(self.module.dump_problems(item, {}))
                self.assertTrue(self.module.dump_problems(item, {**facts, "columns": ["x"]}))
                self.assertTrue(self.module.dump_problems(item, {**facts, "types": ["x"]}))
                self.assertTrue(self.module.dump_problems(item, {**facts, "flags": {**facts["flags"], "keys_ok": False}}))
                self.assertTrue(self.module.dump_problems(item, {**facts, "flags": {**facts["flags"], "keys_ok": 1}}))

    def test_the_first_night_needs_no_count_of_the_sealed_dimension(self) -> None:
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        record = Record(feature="test", slug="test", rerun="none")
        run.record, run.trunk, run.facts = record, "trunk", {}
        names = [item.name for item in self.module.DIMENSIONS]

        def dump(branch: str) -> dict[str, Any]:
            grown = branch != "trunk"
            found = {}
            for item in self.module.DIMENSIONS:
                counts = self.counts(**({"versions": 4, "members": 3} if grown and item.name == "dim_case" else {}))
                found[item.name] = self.helpers["facts_for"](item.name, counts, [c.name for c in item.columns], [c.type for c in item.columns])
            return found

        night = self.module.NIGHTS[0]
        healthy = {item.name: {"controls": {name: True for name in self.module.HISTORY_CONTROLS}} for item in self.module.DIMENSIONS if item.sealed}
        run.fork = lambda base, label: f"{label}_branch"
        run.edit = lambda *_, **__: 0
        run.digest = lambda branch: {"x": 1}
        run.dump = dump
        run.table_snapshot = lambda branch, table: 1
        run.name = lambda base: base
        run.gold = SimpleNamespace(gold=lambda *_, **__: (0, {"written": {n: n in night.written for n in names if n != "dim_lab_test"}}, "", 0.1))
        open_facts = {
            "columns_equal": True,
            "prior_rows": 3,
            "closed_prior": 0,
            "after_rows": 4,
            "newly_closed": 1,
            "opened": 1,
            "opened_ok": 1,
            "opened_corrected": 1,
            "prior_changed": 0,
            "closed_changed": 0,
            "other_state_change": 0,
        }
        quiet = {**open_facts, "after_rows": 3, "newly_closed": 0, "opened": 0, "opened_ok": 0, "opened_corrected": 0}
        run.history = lambda *_: {item.name: healthy.get(item.name) or (open_facts if item.name == "dim_case" else quiet) for item in self.module.DIMENSIONS}
        run.verify_and_merge = lambda *_, **__: None
        run.nights()
        self.assertTrue(record.observations[0].passed, texts(record))
        for name in names:
            with self.subTest(keys_not_the_rule=name):
                damaged = Record(feature="test", slug="test", rerun="none")
                run.record = damaged
                run.dump = lambda branch, name=name: {
                    **dump(branch),
                    name: {**dump(branch)[name], "flags": {**dump(branch)[name]["flags"], "keys_ok": False}},
                }
                run.nights()
                self.assertFalse(damaged.observations[0].passed)


class FakeFrame:
    """A table of dictionaries with the few calls that the profile dump makes."""

    def __init__(self, rows: list[dict[str, Any]], columns: list[str]) -> None:
        self.rows, self.columns = rows, columns

    def filter(self, condition: Any) -> FakeFrame:
        return FakeFrame([row for row in self.rows if condition.fn(row)], self.columns)

    def select(self, *names: str) -> FakeFrame:
        return FakeFrame([{name: row[name] for name in names} for row in self.rows], list(names))

    def collect(self) -> list[FakeRow]:
        return [FakeRow(row) for row in self.rows]

    def count(self) -> int:
        return len(self.rows)

    @property
    def schema(self) -> Any:
        return SimpleNamespace(fields=[SimpleNamespace(name=name, dataType=SimpleNamespace(simpleString=lambda: "string")) for name in self.columns])


class FakeRow(dict[str, Any]):
    def asDict(self) -> dict[str, Any]:
        return dict(self)


class FakeColumn:
    def __init__(self, fn: Any) -> None:
        self.fn = fn

    def cast(self, kind: str) -> FakeColumn:
        return FakeColumn(lambda row: None if self.fn(row) is None else (int(self.fn(row)) if kind in ("bigint", "int") else str(self.fn(row))))

    def isin(self, *values: Any) -> FakeColumn:
        wanted = values[0] if len(values) == 1 and isinstance(values[0], list | tuple | set) else values
        return FakeColumn(lambda row: self.fn(row) in wanted)

    def isNotNull(self) -> FakeColumn:
        return FakeColumn(lambda row: self.fn(row) is not None)

    def __and__(self, other: FakeColumn) -> FakeColumn:
        return FakeColumn(lambda row: bool(self.fn(row)) and bool(other.fn(row)))

    def __or__(self, other: FakeColumn) -> FakeColumn:
        return FakeColumn(lambda row: bool(self.fn(row)) or bool(other.fn(row)))

    def __invert__(self) -> FakeColumn:
        return FakeColumn(lambda row: not self.fn(row))

    def __eq__(self, other: object) -> FakeColumn:  # type: ignore[override]
        return FakeColumn(lambda row: self.fn(row) == (other.fn(row) if isinstance(other, FakeColumn) else other))

    __hash__ = None  # type: ignore[assignment]


class FakeFunctions:
    @staticmethod
    def col(name: str) -> FakeColumn:
        return FakeColumn(lambda row: row[name])


class FakeSession:
    """The tables of the profile dump. ``sql`` runs the dump's own query on SQLite, with the three-part names written as one quoted name."""

    def __init__(self, tables: dict[str, tuple[list[dict[str, Any]], list[str]]]) -> None:
        self.tables = tables

    def table(self, name: str) -> FakeFrame:
        rows, columns = self.tables[name]
        return FakeFrame(copy.deepcopy(rows), columns)

    def sql(self, query: str) -> FakeFrame:
        database = sqlite3.connect(":memory:")
        database.row_factory = sqlite3.Row
        for name, (rows, columns) in self.tables.items():
            quoted = '"' + name + '"'
            database.execute("CREATE TABLE " + quoted + " (" + ", ".join(columns) + ")")
            marks = ", ".join("?" for _ in columns)
            database.executemany(" ".join(("INSERT INTO", quoted, "VALUES (" + marks + ")")), [[row[c] for c in columns] for row in rows])
            query = query.replace(name, f'"{name}"')
        found = [dict(row) for row in database.execute(query).fetchall()]
        database.close()
        return FakeFrame(found, list(found[0]) if found else [])


class ProfileDump(unittest.TestCase):
    """The dump of the profile runner's stand-in, run on tables where only held-out rows change."""

    DEV, HELD = [1, 2], [7, 9]
    CAUSES = ["missing"]

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def tables(self, extra: bool = False) -> dict[str, tuple[list[dict[str, Any]], list[str]]]:
        """The dump's input tables. With ``extra`` the held-out cases change: one case has more rows, another has none, and a row has no case at all."""
        held = [9] if extra else [7, 9]
        loose = [None] if extra else []
        copies = 3 if extra else 1

        def numeric_row(case: int | None, index: int) -> dict[str, Any]:
            valid = index % 2 == 0
            return {"case_id": case, "track": "T1", "valid": valid, "invalid_cause": None if valid else "missing", "case_time_us": index}

        def waveform_row(case: int | None, index: int) -> dict[str, Any]:
            return {"case_id": case, "track": "SNUADC/ART", "case_second": index, "valid": index % 3 != 0, "invalid_cause": None}

        numeric = [numeric_row(case, index) for index, case in enumerate(self.DEV)]
        numeric += [numeric_row(case, 100 + index) for index, case in enumerate([*held, *loose] * copies)]
        waveform = [waveform_row(case, index) for index, case in enumerate(self.DEV)]
        waveform += [waveform_row(case, 100 + index) for index, case in enumerate([*held, *loose] * copies)]
        clinical = [{"caseid": str(case), "opstart": "0", "opend": str(100 + int(case))} for case in [*self.DEV, *held]]
        stored = {"kind": "numeric", "track": "T1", "cases": 2, "rows": 3, "valid_rows": 2, "cause_missing": 1, "valid_share": 2 / 3}
        names = ["case_id", "track", "valid", "invalid_cause", "case_time_us"]
        return {
            "lake.silver.numeric_observations": (numeric, names),
            "lake.silver.waveform_seconds": (waveform, ["case_id", "track", "case_second", "valid", "invalid_cause"]),
            "lake.bronze.clinical_cases": (clinical, ["caseid", "opstart", "opend"]),
            "lake.silver.signal_profile": ([stored], list(stored)),
            "lake.silver.unscored_share": ([{"a": 1}], ["a"]),
            "lake.silver.source_counts": ([{"name": "n", "value": 1.0, "expected": 1.0, "matches": True}], ["name", "value", "expected", "matches"]),
            "lake.silver.warning_grid_rule": ([{"rule": "r", "written_at": None}], ["rule", "written_at"]),
        }

    def run_dump(self, tables: dict[str, tuple[list[dict[str, Any]], list[str]]], source: str | None = None) -> dict[str, Any]:
        """Run the stand-in's own ``dump`` (and the two functions it calls) on ``tables``, with Spark stood in, and return what it printed."""
        tree = ast.parse(source or self.module.STAND_IN)
        keep: list[ast.stmt] = [
            node
            for node in tree.body
            if (isinstance(node, ast.FunctionDef) and node.name in ("dump", "profile_sql", "grid_by_case"))
            or (isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant | ast.Tuple))
        ]
        names = namespace(ast.unparse(ast.Module(body=keep, type_ignores=[])), F=FakeFunctions)
        spec = {"dev_cases": self.DEV, "causes": self.CAUSES}
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            names["dump"](FakeSession(tables), spec)
        line = printed.getvalue().strip()
        self.assertTrue(line.startswith("RESULT "), line)
        document: dict[str, Any] = json.loads(line[len("RESULT ") :])
        return document

    def test_the_dump_reads_the_development_rows(self) -> None:
        found = self.run_dump(self.tables())
        self.assertEqual({"numeric_rows": 2, "waveform_rows": 2, "tracks": 2}, found["development"])

    def test_a_change_of_held_out_rows_alone_leaves_the_dump_identical(self) -> None:
        self.assertEqual(self.run_dump(self.tables()), self.run_dump(self.tables(extra=True)))

    def test_the_check_sees_a_count_of_a_whole_table_through_a_variable(self) -> None:
        source = self.module.STAND_IN
        line = '    dev_numeric = session.table(SNUM).filter(F.col("case_id").isin(spec["dev_cases"])).count()\n'
        self.assertIn(line, source)
        planted = source.replace(line, "    whole = session.table(SNUM)\n    dev_numeric = whole.count()\n")
        self.assertNotEqual(self.run_dump(self.tables(), planted), self.run_dump(self.tables(extra=True), planted))

    def test_the_check_sees_a_count_that_filters_on_a_non_null_case(self) -> None:
        source = self.module.STAND_IN
        line = '    dev_waveform = session.table(SWAV).filter(F.col("case_id").isin(spec["dev_cases"])).count()\n'
        self.assertIn(line, source)
        planted = source.replace(line, '    dev_waveform = session.table(SWAV).filter(F.col("case_id").isNotNull()).count()\n')
        self.assertNotEqual(self.run_dump(self.tables(), planted), self.run_dump(self.tables(extra=True), planted))


if __name__ == "__main__":
    unittest.main()
