"""The Gold, Silver cases and signal profile runners check every reply and every stream before they save, merge or print it (fix round 6, owner decision D).

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, Nessie and Spark are stand-ins.

Failure modes covered here:

- The Gold runner saved values of a rejected reply: a count in ``branch_hash``, a mismatched lab snapshot ID after the catalog comparison failed, a count inside
  an unexpected lab column name, and a list in ``outcome`` raised ``TypeError``. Every field of the reply now has its type and format, and the facts are saved
  only after Nessie confirms them.
- The Silver cases runner's sample setup gave the loader no lab row count (``lab_total_ok`` was always false), merged a loader whose other flags were false and
  merged a split whose base and branch hashes were equal.
- The Gold runner merged a night whose build observation failed and an independent read that failed, and its adapter kept ``unknown_member: dim_track, 1 row``,
  which the producer never prints.
- The signal profile runner kept any text that began with the job's prefix and saved the raw streams of every job in its log.
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from spark.jobs import signal_profile as profile_job
from spark.profile import checks, grid_rule
from spark.profile import cli as profile_cli
from spark.profile import run as profile_run
from tests.e2e.run_record import Record
from tests.fast.test_runner_gold_profile_sealed import reachable_flag_codes
from tests.fast.test_runner_gold_sealed import LAB_COUNT, healthy_result, recorder, runner, texts
from tests.fast.test_runner_setup_builds import stand_in_stack

HEX = "0123456789abcdef"
E2E = Path(__file__).resolve().parents[1] / "e2e"


class GoldResultFormat(unittest.TestCase):
    """Every field of the Gold job's result has its type and format, and a value of the wrong kind never raises."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def problems(self, **edits: Any) -> list[str]:
        found: list[str] = self.module.sealed_result_problems({**healthy_result(self.module), **edits})
        return found

    def test_the_healthy_result_has_no_problem(self) -> None:
        self.assertEqual([], self.problems())
        self.assertEqual([], self.problems(base_hash=None, mode="verify", outcome="verified"))
        self.assertEqual([], self.problems(base_hash=HEX[:16] * 4))

    def test_a_hash_that_is_not_64_lowercase_hexadecimal_digits_is_a_problem(self) -> None:
        wrong: list[Any] = [LAB_COUNT, str(LAB_COUNT), True, [LAB_COUNT], {"count": LAB_COUNT}, "b" * 63, "b" * 65, "B" * 64, "g" * 64, "b" * 63 + " ", ""]
        for name in ("branch_hash", "base_hash"):
            for value in wrong:
                with self.subTest(name=name, value=value):
                    self.assertTrue(self.problems(**{name: value}))
        with self.subTest("a branch hash that is none"):
            self.assertTrue(self.problems(branch_hash=None))

    def test_a_branch_that_is_not_a_plain_name_is_a_problem(self) -> None:
        for value in (LAB_COUNT, None, ["branch"], {"a": 1}, "", "two words", "line\nbreak", "x" * 200):
            with self.subTest(value=value):
                self.assertTrue(self.problems(branch=value))

    def test_an_outcome_or_a_mode_of_an_unhashable_kind_is_a_problem_and_raises_nothing(self) -> None:
        outcomes: list[Any] = [["written"], {"written": 1}, [LAB_COUNT], [{}], None, 1, True]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                self.assertTrue(self.problems(outcome=outcome))
        modes: list[Any] = [["build"], {"build": 1}, None, 1, "other"]
        for mode in modes:
            with self.subTest(mode=mode):
                self.assertTrue(self.problems(mode=mode))

    def test_a_snapshot_id_that_is_not_a_whole_number_is_a_problem(self) -> None:
        healthy = healthy_result(self.module)
        lab = next(item.table for item in self.module.DIMENSIONS if item.sealed)
        for value in (True, "7", 7.5, [7], {"a": 7}):
            with self.subTest(value=value):
                self.assertTrue(self.problems(snapshot_ids={**healthy["snapshot_ids"], lab: value}))


class GoldControlSaves(unittest.TestCase):
    """The control saves the hashes and the snapshot IDs only after Nessie holds exactly them, and stops when it does not."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.lab_table = next(item.table for item in self.module.DIMENSIONS if item.sealed)

    def run_of(self, answer: dict[str, Any], *, head: str = "b" * 64, snapshot: Any = None) -> tuple[Any, Record]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.trunk, run.facts = record, "p7_trunk_x1", {}
        run.gold = SimpleNamespace(gold=lambda *_, **__: (0, answer, "", 0.1))
        run.stack = SimpleNamespace(spark_containers=lambda: [])
        run.nessie = SimpleNamespace(entries=lambda branch: {item.table: 1 for item in self.module.DIMENSIONS})
        run.head = lambda branch: head if branch != "p7_trunk_x1" else "a" * 64
        run.table_snapshot = snapshot or (lambda branch, table: 7)
        run.name = lambda base: f"p7_{base}_x1"
        return run, record

    def control(self, run: Any) -> Any:
        with contextlib.redirect_stdout(io.StringIO()):
            return run.control()

    def saved(self, run: Any, record: Record) -> str:
        record.environment.update(run.facts)
        return json.dumps(asdict(record), default=str)

    def test_a_confirmed_result_saves_its_hashes_and_snapshot_ids(self) -> None:
        run, record = self.run_of(healthy_result(self.module))
        self.assertTrue(self.control(run))
        tables = {f"snapshot_{item.table.replace('.', '_')}" for item in self.module.DIMENSIONS}
        self.assertEqual({"trunk", "branch", "base_hash", "branch_hash", *tables}, set(run.facts))
        self.assertEqual(("a" * 64, "b" * 64), (run.facts["base_hash"], run.facts["branch_hash"]))
        self.assertEqual({"7"}, {run.facts[name] for name in tables})
        self.assertEqual(("p7_trunk_x1", "p7_gold_x1"), (run.facts["trunk"], run.facts["branch"]))

    def test_a_snapshot_ID_that_Nessie_does_not_hold_saves_no_fact_and_stops_the_run(self) -> None:
        lab = self.lab_table
        run, record = self.run_of(healthy_result(self.module), snapshot=lambda branch, table: 8 if table == lab else 7)
        self.assertFalse(self.control(run))
        self.assertEqual({}, run.facts)
        self.assertFalse(record.observations[-1].passed)
        self.assertNotIn("snapshot_ids", self.saved(run, record))

    def test_a_branch_hash_that_Nessie_does_not_hold_saves_no_fact_and_stops_the_run(self) -> None:
        run, record = self.run_of(healthy_result(self.module), head="c" * 64)
        self.assertFalse(self.control(run))
        self.assertEqual({}, run.facts)
        self.assertNotIn("b" * 64, self.saved(run, record))

    def test_a_table_that_the_branch_does_not_hold_saves_no_fact_and_stops_the_run(self) -> None:
        run, record = self.run_of(healthy_result(self.module))
        run.nessie = SimpleNamespace(entries=lambda branch: {item.table: 1 for item in self.module.DIMENSIONS if item.table != self.lab_table})
        self.assertFalse(self.control(run))
        self.assertEqual({}, run.facts)
        self.assertFalse(record.observations[-1].passed)

    def test_a_missing_snapshot_in_Nessie_saves_no_fact(self) -> None:
        run, record = self.run_of(healthy_result(self.module), snapshot=lambda branch, table: None)
        self.assertFalse(self.control(run))
        self.assertEqual({}, run.facts)

    def test_a_result_of_the_wrong_format_saves_no_fact_and_never_its_value(self) -> None:
        for edits in ({"branch_hash": LAB_COUNT}, {"base_hash": LAB_COUNT}, {"outcome": [LAB_COUNT]}):
            with self.subTest(edits=edits):
                run, record = self.run_of({**healthy_result(self.module), **edits})
                self.assertFalse(self.control(run))
                self.assertEqual({}, run.facts)
                self.assertNotIn(str(LAB_COUNT), self.saved(run, record))
                self.assertNotIn(str(LAB_COUNT), texts(record))


class GoldDumpColumns(unittest.TestCase):
    """The independent read holds only the column names and types that the dimension's card lists."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.lab = next(item for item in self.module.DIMENSIONS if item.sealed)

    def good(self, item: Any) -> dict[str, Any]:
        counts = {name: 1 for name in self.module.DUMP_COUNTS}
        flags = {name: True for name in self.module.DUMP_FLAGS}
        base = {"flags": flags, "columns": [c.name for c in item.columns], "types": [c.type for c in item.columns]}
        return base if item.sealed else {**counts, **base}

    def test_a_reply_with_the_card_columns_in_any_order_has_no_shape_problem(self) -> None:
        for item in self.module.DIMENSIONS:
            facts = self.good(item)
            self.assertEqual([], self.module.dump_shape_problems(item, facts))
            reordered = {**facts, "columns": list(reversed(facts["columns"])), "types": list(reversed(facts["types"]))}
            self.assertEqual([], self.module.dump_shape_problems(item, reordered))
            self.assertTrue(self.module.dump_problems(item, reordered))

    def test_a_column_name_or_type_that_the_card_does_not_list_is_a_shape_problem(self) -> None:
        facts = self.good(self.lab)
        other = next(item for item in self.module.DIMENSIONS if not item.sealed)
        wrong: dict[str, dict[str, Any]] = {
            "a count in a column name": {"columns": [*facts["columns"][:-1], f"count_{LAB_COUNT}"]},
            "a count as a column name": {"columns": [*facts["columns"][:-1], str(LAB_COUNT)]},
            "a column of another dimension": {"columns": [*facts["columns"][:-1], other.columns[1].name]},
            "an extra column": {"columns": [*facts["columns"], facts["columns"][0]]},
            "a count in a type": {"types": [*facts["types"][:-1], f"decimal({LAB_COUNT},0)"]},
            "an extra type": {"types": [*facts["types"], facts["types"][0]]},
        }
        for label, edit in wrong.items():
            with self.subTest(label):
                self.assertTrue(self.module.dump_shape_problems(self.lab, {**facts, **edit}), label)

    def test_the_independent_read_never_records_a_column_name_or_type_that_the_card_does_not_list(self) -> None:
        for name, wrong in (("columns", f"count_{LAB_COUNT}"), ("types", f"decimal({LAB_COUNT},0)")):
            with self.subTest(name=name):
                replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
                replies[self.lab.name] = {**replies[self.lab.name], name: [*replies[self.lab.name][name][:-1], wrong]}
                record = Record(feature="test", slug="test", rerun="none")
                run = self.module.GoldRun.__new__(self.module.GoldRun)
                run.record, run.dump = record, lambda branch, replies=replies: replies
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    self.assertFalse(run.read_dimensions({"branch": "built"}))
                self.assertFalse(all(item.passed for item in record.observations))
                for text in (texts(record), out.getvalue(), json.dumps(asdict(record), default=str)):
                    self.assertNotIn(str(LAB_COUNT), text)

    def test_the_independent_read_answers_true_for_replies_that_pass_and_false_when_any_dimension_fails(self) -> None:
        replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
        for item in self.module.DIMENSIONS:
            for broken, wanted in ((False, True), (True, False)):
                with self.subTest(item=item.name, broken=broken):
                    edit = {"flags": {**replies[item.name]["flags"], "keys_ok": not broken}}
                    record = Record(feature="test", slug="test", rerun="none")
                    run = self.module.GoldRun.__new__(self.module.GoldRun)
                    run.record, run.dump = record, lambda branch, item=item, edit=edit: {**replies, item.name: {**replies[item.name], **edit}}
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertIs(wanted, run.read_dimensions({"branch": "built"}))


class GoldHistoryFacts(unittest.TestCase):
    """A history answer is complete only when each fact is of its kind, so a list or a text in a count raises nothing and is never a fact."""

    OPEN = {
        "columns_equal": True,
        "prior_rows": 3,
        "closed_prior": 0,
        "after_rows": 3,
        "newly_closed": 0,
        "opened": 0,
        "opened_ok": 0,
        "opened_corrected": 0,
        "prior_changed": 0,
        "closed_changed": 0,
        "other_state_change": 0,
    }

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def answer(self, **edits: Any) -> dict[str, Any]:
        controls = {name: True for name in self.module.HISTORY_CONTROLS}
        found = {item.name: ({"controls": controls} if item.sealed else dict(self.OPEN)) for item in self.module.DIMENSIONS}
        open_name = next(item.name for item in self.module.DIMENSIONS if not item.sealed)
        found[open_name] = {**found[open_name], **edits}
        return found

    def test_an_answer_with_every_fact_of_its_kind_is_complete(self) -> None:
        self.assertTrue(self.module.history_complete(self.answer()))

    def test_a_fact_of_another_kind_is_not_complete(self) -> None:
        for name in self.OPEN:
            wrong: list[Any] = [True, False] if name != "columns_equal" else [1, "True", None, [True]]
            if name != "columns_equal":
                wrong += [[1], "1", None, -1, 1.5, {"a": 1}]
            for value in wrong:
                with self.subTest(name=name, value=value):
                    self.assertFalse(self.module.history_complete(self.answer(**{name: value})))

    def test_the_controls_of_an_incomplete_answer_are_never_asked_for(self) -> None:
        self.assertFalse(self.module.planted_caught(self.answer(prior_rows=[1]), "version_count_change", set(), set(), set(), set()))


class GoldGates(unittest.TestCase):
    """A failed observation is never followed by a merge."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.names = [item.name for item in self.module.DIMENSIONS]

    def night_run(self, code: int = 0, answer: dict[str, Any] | None = None) -> tuple[Any, Record, list[tuple[Any, ...]]]:
        night = self.module.NIGHTS[0]
        record = Record(feature="test", slug="test", rerun="none")
        merged: list[tuple[Any, ...]] = []
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.trunk, run.facts = record, "trunk", {}
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
        controls = {"controls": {name: True for name in self.module.HISTORY_CONTROLS}}

        def dump(branch: str) -> dict[str, Any]:
            grown = branch != "trunk"
            return {name: {"versions": 4 if grown and name == "dim_case" else 3, "members": 3, "flags": {"keys_ok": True}} for name in self.names}

        built = answer if answer is not None else {**healthy_result(self.module), "written": {n: n in night.written for n in self.names if n != "dim_lab_test"}}
        run.fork = lambda base, label: f"{label}_branch"
        run.edit = lambda *_, **__: 0
        run.digest = lambda branch: {"x": 1}
        run.dump = dump
        run.table_snapshot = lambda branch, table: 1
        run.name = lambda base: base
        run.gold = SimpleNamespace(gold=lambda *_, **__: (code, built, "", 0.1))
        run.history = lambda *_: {item.name: controls if item.sealed else (open_facts if item.name == "dim_case" else quiet) for item in self.module.DIMENSIONS}
        run.verify_and_merge = lambda *arguments, **_: merged.append(arguments)
        return run, record, merged

    def test_a_night_whose_observation_passes_is_verified_and_merged(self) -> None:
        run, record, merged = self.night_run()
        run.nights()
        self.assertTrue(record.observations[0].passed, texts(record))
        self.assertEqual(1, len(merged))

    def test_a_night_whose_observation_fails_is_never_verified_or_merged(self) -> None:
        pre_seal = {**healthy_result(self.module), "members": {**healthy_result(self.module)["members"], "dim_lab_test": LAB_COUNT}}
        for label, code, answer in (("a failed exit", 1, None), ("a result of the old shape", 0, pre_seal)):
            with self.subTest(label):
                run, record, merged = self.night_run(code=code, answer=answer)
                run.nights()
                self.assertFalse(record.observations[0].passed)
                self.assertEqual([], merged)

    def test_the_first_build_is_merged_only_after_the_independent_read_passes(self) -> None:
        for passed, wanted in ((True, 1), (False, 0), (None, 0)):
            with self.subTest(passed=passed):
                merges: list[tuple[Any, ...]] = []
                run = SimpleNamespace(
                    setup=lambda: True,
                    control=lambda: {"branch": "built"},
                    read_dimensions=lambda built, passed=passed: passed,
                    verify_and_merge=lambda *arguments, merges=merges: merges.append(arguments),
                    rebuild=lambda: None,
                    planted_faults=lambda: None,
                    nights=lambda: None,
                    bounded=lambda: None,
                )
                self.module.cases(run)
                self.assertEqual(wanted, len(merges))


class GoldSetupMerges(unittest.TestCase):
    """The Gold setup merges the split, the decoder's tables and the Silver cases tables only after the flags of each answer are true (the same gate as the
    loader's), and a decoder answer of another shape raises nothing."""

    FLAGS = ("cases_as_expected", "columns_as_expected", "every_column_a_string", "lab_total_ok")
    SPLIT = {"base_hash": "a" * 64, "branch_hash": "b" * 64, "passed": {"patient_split": True}}
    DECODE = {"tables": {"tracks": {"rows": 3}}}

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gold_setup_")))
        with open(self.folder / "lab.csv", "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["caseid", "dt", "name", "result"])
            writer.writerow([str(self.module.CASES[0]), "1", "x", "2"])
        self.silver_good: dict[str, Any] = {"base_hash": "a" * 64, "branch_hash": "b" * 64, "passed": {name: True for name in self.module.base.TABLE_GROUPS}}

    def merged(self, *, split: tuple[int, Any] | None = None, decode: tuple[int, Any] | None = None, silver: tuple[int, Any] | None = None) -> list[str]:
        merges: list[tuple[Any, ...]] = []
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.made, run.trunk, run.facts = Record(feature="test", slug="test", rerun="none"), [], "trunk", {}
        run.sources = (self.folder / "clinical.csv", self.folder / "lab.csv")
        run.nessie = SimpleNamespace(create_branch=lambda *_: None)
        loader = {name: True for name in self.FLAGS}
        split, decode, silver = split or (0, self.SPLIT), decode or (0, self.DECODE), silver or (0, self.silver_good)
        run.stack = SimpleNamespace(
            write_spec=lambda *_: None,
            stand_in=lambda *_, **__: (0, loader, "", 0.1),
            split=lambda *_, **__: (split[0], split[1], "", 0.1),
            job=lambda *_, **__: (decode[0], decode[1], "", 0.1),
            silver=lambda *_, **__: (silver[0], silver[1], "", 0.1),
            data_directory=self.folder,
        )
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.merge = recorder(merges)
        run.recreate = lambda *_: None
        run.forget = lambda *_: None
        run.head = lambda branch: "h"
        run.setup()
        return [arguments[2] for arguments in merges]

    def test_every_answer_that_passes_is_merged_in_order(self) -> None:
        self.assertEqual(["stand-in bronze tables", "patient split", "bronze from the decoder job", "silver cases"], self.merged())

    def test_a_split_that_did_not_write_or_holds_a_false_flag_is_never_merged(self) -> None:
        for label, answer in (
            ("equal hashes", {**self.SPLIT, "branch_hash": "a" * 64}),
            ("a false flag", {**self.SPLIT, "passed": {"patient_split": False}}),
            ("a removed field", {**self.SPLIT, "patients": 4417}),
        ):
            with self.subTest(label):
                self.assertEqual(["stand-in bronze tables"], self.merged(split=(0, answer)))
        self.assertEqual(["stand-in bronze tables"], self.merged(split=(1, self.SPLIT)))

    def test_a_decoder_answer_without_track_rows_or_of_another_shape_is_never_merged(self) -> None:
        for label, answer in (
            ("no rows", {"tables": {"tracks": {"rows": 0}}}),
            ("no tracks table", {"tables": {}}),
            ("rows that are a list", {"tables": {"tracks": {"rows": [3]}}}),
            ("rows that are a Boolean", {"tables": {"tracks": {"rows": True}}}),
            ("tables that are a list", {"tables": [1]}),
            ("a table that is a count", {"tables": {"tracks": 3}}),
            ("no tables", {}),
        ):
            with self.subTest(label):
                self.assertEqual(["stand-in bronze tables", "patient split"], self.merged(decode=(0, answer)))
        self.assertEqual(["stand-in bronze tables", "patient split"], self.merged(decode=(1, self.DECODE)))

    def test_a_silver_build_that_did_not_write_or_holds_a_false_flag_is_never_merged(self) -> None:
        good = self.silver_good
        for label, answer in (
            ("equal hashes", {**good, "branch_hash": "a" * 64}),
            ("a false flag", {**good, "passed": {**good["passed"], "cases": False}}),
            ("a missing flag", {**good, "passed": {key: value for key, value in good["passed"].items() if key != "lab"}}),
            ("a removed field", {**good, "outcome": "written"}),
        ):
            with self.subTest(label):
                self.assertEqual(["stand-in bronze tables", "patient split", "bronze from the decoder job"], self.merged(silver=(0, answer)))
        self.assertEqual(["stand-in bronze tables", "patient split", "bronze from the decoder job"], self.merged(silver=(1, good)))


class GoldUnknownMember(unittest.TestCase):
    """The adapter keeps only the counts of ``unknown_member`` that the producer can print: any number of rows but one."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")

    def test_a_count_of_one_is_dropped_and_every_other_count_is_kept(self) -> None:
        self.assertEqual([], self.module.codes("gold_dimensions: unknown_member: dim_track, 1 row\n"))
        for count in (0, 2, 3, 10):
            line = f"unknown_member: dim_track, {count} rows"
            with self.subTest(line=line):
                self.assertEqual([line], self.module.codes(f"gold_dimensions: {line}\n"))
        self.assertEqual([], self.module.codes("gold_dimensions: unknown_member: dim_track, 1 rows\n"))
        self.assertEqual([], self.module.codes("gold_dimensions: unknown_member: dim_lab_test, 2 rows\n"))

    def test_the_producer_never_prints_a_count_of_one_and_the_adapter_keeps_each_line_it_prints(self) -> None:
        producer = __import__("spark.gold_dimensions", fromlist=["x"])
        names = (*producer.INPUT_FACTS, *producer.OUTPUT_FACTS)
        for rows in range(6):
            facts = {item.name: {name: 0 for name in names} for item in producer.DIMENSIONS}
            for item in producer.DIMENSIONS:
                facts[item.name].update(members=1, unknown_rows=rows)
            for line in producer.output_problems(facts):
                with self.subTest(rows=rows, line=line):
                    self.assertEqual([line], self.module.codes(f"gold_dimensions: {line}\n"))
                    self.assertNotIn("unknown_member: dim_track, 1 row", line)


class SilverSampleSetup(unittest.TestCase):
    """The sample-sized setup of the Silver cases runner asks the loader for the lab rows of the development cases, merges only with the loader's four flags
    true, and merges the split only when it wrote (its base and branch hashes differ) and every flag is true."""

    FLAGS = ("cases_as_expected", "columns_as_expected", "every_column_a_string", "lab_total_ok")
    SPLIT = {"base_hash": "a" * 64, "branch_hash": "b" * 64, "passed": {"patient_split": True}}

    def setUp(self) -> None:
        self.module = runner("silver_cases")
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="silver_sample_")))
        with open(self.folder / "lab.csv", "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["caseid", "dt", "name", "result"])
            for case in ("1", "1", "1", "2", "2", "3"):
                writer.writerow([case, "1", "x", "2"])

    def loader(self, **flags: Any) -> dict[str, Any]:
        return {name: flags.get(name, True) for name in self.FLAGS}

    def sample(self, loader: dict[str, Any], split: dict[str, Any], *, code: int = 0, split_code: int = 0) -> tuple[list[str], list[dict[str, Any]]]:
        merges: list[tuple[Any, ...]] = []
        specs: list[dict[str, Any]] = []
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.made, run.trunk, run.facts, run.labels = Record(feature="test", slug="test", rerun="none"), [], "trunk", {}, {}
        run.nessie = SimpleNamespace(create_branch=lambda *_: None, entries=lambda *_: {})
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.recreate = lambda *_: None
        run.fork = lambda base, label: label
        run.merge = recorder(merges)
        run.candidates = [SimpleNamespace(case=1, slot="a_development"), SimpleNamespace(case=2, slot="b_development"), SimpleNamespace(case=3, slot="c_test")]
        run.sources = (self.folder / "clinical.csv", self.folder / "lab.csv")
        run.stack = SimpleNamespace(
            write_spec=specs.append,
            stand_in=lambda *_, **__: (code, loader, "", 0.1),
            split=lambda *_, **__: (split_code, split, "", 0.1),
            silver=lambda *_, **__: (1, {}, "", 0.1),
        )
        run.head = lambda branch: "h"
        run.table_snapshot = lambda *_: 1
        with contextlib.suppress(Exception):
            run.sample_sized()
        return [arguments[2] for arguments in merges], specs

    def test_the_lab_rows_of_the_development_cases_are_counted_from_the_published_file(self) -> None:
        self.assertEqual(5, self.module.lab_rows_of(self.folder / "lab.csv", ["1", "2"]))
        self.assertEqual(0, self.module.lab_rows_of(self.folder / "lab.csv", ["9"]))

    def test_the_loader_is_asked_for_the_lab_rows_of_the_development_cases_only(self) -> None:
        _, specs = self.sample(self.loader(), self.SPLIT)
        self.assertEqual({"cases": ["1", "2"], "expected_cases": 2, "lab_rows": 5, "columns": self.module.CLINICAL_COLUMNS}, specs[0])

    def test_a_loader_with_a_flag_that_is_not_true_or_a_missing_or_extra_flag_is_never_merged(self) -> None:
        for name in self.FLAGS:
            for value in (False, None, 1, "True"):
                with self.subTest(name=name, value=value):
                    self.assertEqual([], self.sample(self.loader(**{name: value}), self.SPLIT)[0])
            with self.subTest(name=name, value="missing"):
                self.assertEqual([], self.sample({key: True for key in self.FLAGS if key != name}, self.SPLIT)[0])
        with self.subTest("an extra key"):
            self.assertEqual([], self.sample({**self.loader(), "rows": 4417}, self.SPLIT)[0])
        with self.subTest("a failed exit"):
            self.assertEqual([], self.sample(self.loader(), self.SPLIT, code=1)[0])

    def test_a_loader_with_four_true_flags_and_a_split_that_wrote_are_merged(self) -> None:
        self.assertEqual(["sample-sized bronze", "sample-sized split"], self.sample(self.loader(), self.SPLIT)[0])

    def test_a_split_that_did_not_write_or_that_holds_a_false_flag_or_a_removed_field_is_never_merged(self) -> None:
        bad: dict[str, dict[str, Any]] = {
            "equal hashes": {**self.SPLIT, "branch_hash": "a" * 64},
            "no base hash": {**self.SPLIT, "base_hash": None},
            "a hash that is no hash": {**self.SPLIT, "branch_hash": "b"},
            "a false pass flag": {**self.SPLIT, "passed": {"patient_split": False}},
            "a pass flag that is 1": {**self.SPLIT, "passed": {"patient_split": 1}},
            "no pass flag": {**self.SPLIT, "passed": {}},
            "a removed field": {**self.SPLIT, "patients": 4417},
        }
        for label, answer in bad.items():
            with self.subTest(label):
                self.assertEqual(["sample-sized bronze"], self.sample(self.loader(), answer)[0])
        with self.subTest("a failed exit"):
            self.assertEqual(["sample-sized bronze"], self.sample(self.loader(), self.SPLIT, split_code=1)[0])


class ProfileLines(unittest.TestCase):
    """The profile runner keeps exactly the lines that the profile job can print, each a code and its detail, and nothing else that begins with its prefix."""

    PREFIX = "signal_profile: "

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def printed(self, *problems: str) -> list[str]:
        """The lines that the real command line writes to standard error when the job raises these problems."""
        err = io.StringIO()

        def execute(arguments: Any) -> dict[str, Any]:
            raise checks.SignalProfileError(*problems)

        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            profile_cli.run(["verify", "--branch", "b"], execute)
        return err.getvalue().splitlines()

    def check_lines(self, number: int) -> list[str]:
        lines: list[str] = []
        profile = checks.ProfileFacts(number % 2, number, number, number, number, number + 5, number + 6, number + 7)
        source = checks.InputFacts(5, 5, 5, number + 1)
        lines += checks.profile_problems(profile, source) + checks.profile_problems(checks.ProfileFacts(0, 0, 0, 0, 0, 0, 0, 0), checks.InputFacts(0, 0, 0, 0))
        for rows in (0, 1, 2, number + 1):
            for flag in (True, False):
                unscored = checks.UnscoredFacts(rows, 2, 3, 3, 1, 0.5 if flag else None, number, number + 1, 60, 1, 300, 1, 2, 3, 4)
                lines += checks.unscored_problems(unscored, source)
        counts = checks.CountsFacts(number, number, number, number, number, number)
        lines += checks.counts_problems(counts, pinned=True) + checks.counts_problems(counts, pinned=False)
        wanted = grid_rule.planned_rule(7)
        for rows in (0, 2, number + 2):
            lines += grid_rule.rule_problems(grid_rule.RuleRead(rows, wanted, 7, True), wanted)
        other = grid_rule.GridRule("x", "y", "z", 1, 2, 3, 4)
        for dated in (True, False):
            lines += grid_rule.rule_problems(grid_rule.RuleRead(1, other, 7, dated), wanted)
        lines += grid_rule.rule_problems(grid_rule.RuleRead(1, wanted, 7, False), wanted)
        return lines

    def table_lines(self) -> list[str]:
        unit = grid_rule.unit
        lines = []
        for tables in range(1, len(profile_run.OUTPUTS) + 1):
            lines += [f"{profile_run.SCHEMA_CODE}: {unit(tables, 'table')}", f"table_missing: {unit(tables, 'table')}"]
        lines += [f"source_no_snapshot: {unit(count, 'table')}" for count in range(1, len(profile_run.SOURCE_NAMES) + 1)]
        lines += [f"development_differs: {unit(count, 'case')}" for count in (1, 2, 9)]
        lines += ["source_table: 0 tables", profile_job.NO_INPUT_FACTS, "source_changed: 1 read of the inputs differs"]
        lines += [profile_run.BRANCH_MOVED, profile_run.READBACK_DIFFERS, profile_run.PLAN_DIFFERS]
        return lines

    def flag_lines(self) -> list[str]:
        split = __import__("spark.split_table", fromlist=["x"])
        return [split.flag(code) for code in sorted(reachable_flag_codes())]

    def generic_lines(self) -> list[str]:
        return ["usage", "settings", "branch_missing", "catalog_error", "internal_error", "timeout"]

    def every_line(self) -> list[str]:
        lines = [line for number in (1, 2, 7) for line in self.check_lines(number)]
        return [*lines, *self.table_lines(), *self.flag_lines(), *self.generic_lines()]

    def test_every_line_that_the_job_can_print_is_kept_unchanged(self) -> None:
        lines = self.every_line()
        self.assertGreater(len(lines), 100)
        for line in lines:
            with self.subTest(line=line):
                self.assertTrue(self.module.known_line(line), line)
                self.assertEqual([line], self.module.codes(f"{self.PREFIX}{line}\n"))

    def test_the_lines_of_the_real_command_line_are_kept_with_their_prefix(self) -> None:
        problems = [*self.check_lines(3)[:6], *self.table_lines()]
        stream = "\n".join(self.printed(*problems)) + "\n"
        self.assertEqual([f"{self.PREFIX}{line}" for line in problems], self.module.problem_lines(stream))
        self.assertEqual(problems, self.module.codes(stream))

    def test_the_known_codes_are_the_codes_that_the_job_can_print(self) -> None:
        printed = {line.split(":")[0] for line in self.every_line()}
        self.assertEqual(printed, self.module.KNOWN_CODES)

    def test_a_line_that_the_job_never_prints_is_dropped(self) -> None:
        dropped = [
            "anything at all",
            "key: 31337 rows",
            "counts_rows: 31337 rows and a name",
            "counts_rows: 1 rows",
            "counts_rows: 2 row",
            "counts_rows: 0 rows",
            "counts_rows: 01 row",
            "counts_rows: -1 row",
            "counts_rows: 1 row ",
            " counts_rows: 1 row",
            "counts_rows: 1 member",
            "counts_names: 1 row",
            "counts_rows: failed",
            "unscored_order: 2 rows",
            "unscored_share: 3 rows",
            "profile_empty: 2 tables",
            "input_empty: 1 row",
            "table_missing: 1 tables",
            "table_missing: 0 tables",
            "table_missing: 9 tables",
            "table_schema: 5 tables",
            "source_table: 1 table",
            "source_no_snapshot: 6 tables",
            "development_empty: 1 case",
            "development_differs: 0 cases",
            "grid_rule_changed: 0 columns",
            "grid_rule_changed: 9 columns",
            "branch_moved: the branch head is another one",
            "source_changed: 2 reads of the inputs differ",
            "usage: 1 row",
            "usage extra",
            "refused: 4417",
            "Traceback (most recent call last):",
            "java.lang.OutOfMemoryError: 31337",
            "pin_mismatch: clinical_data",
            "STEP first_read",
            "",
        ]
        for line in dropped:
            with self.subTest(line=line):
                self.assertEqual([], self.module.codes(f"{self.PREFIX}{line}\n"))
                self.assertEqual([], self.module.problem_lines(f"{self.PREFIX}{line}\n"))

    def test_a_line_without_the_prefix_is_dropped_even_when_it_is_a_known_line(self) -> None:
        for stream in ("counts_rows: 1 row\n", " signal_profile: usage\n", "x signal_profile: usage\n", "SIGNAL_PROFILE: usage\n", "signal_profile:usage\n"):
            with self.subTest(stream=stream):
                self.assertEqual([], self.module.codes(stream))

    def test_a_mixed_stream_gives_the_known_lines_in_order(self) -> None:
        stream = (
            "WARN jvm 31337\n"
            "signal_profile: counts_rows: 1 row\n"
            "signal_profile: key: 31337 rows\n"
            "Traceback (most recent call last):\n"
            "signal_profile: usage\n"
            "signal_profile: counts_names: 3 names tail\n"
        )
        self.assertEqual(["counts_rows: 1 row", "usage"], self.module.codes(stream))
        self.assertEqual(["signal_profile: counts_rows: 1 row", "signal_profile: usage"], self.module.problem_lines(stream))


class ProfileRefusals(unittest.TestCase):
    """A case that expects exact lines refuses a run in which the runner dropped a line of the job's own prefix."""

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def run_of(self, dropped: int) -> tuple[Any, Record]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.Run.__new__(self.module.Run)
        run.record = record
        run.name = lambda base: base
        run.stack = SimpleNamespace(unknown_lines=dropped, job=lambda *_, **__: (1, {}, "signal_profile: branch_missing\n", 0.1), spark_containers=lambda: [])
        return run, record

    def test_a_refusal_passes_only_when_no_line_was_dropped(self) -> None:
        for dropped, wanted in ((0, True), (1, False)):
            with self.subTest(dropped=dropped):
                run, record = self.run_of(dropped)
                run.refused("case", "expected", "signal_profile: usage\n", 1, ["usage"], True)
                self.assertIs(wanted, record.observations[0].passed)

    def test_the_bounded_failures_pass_only_when_no_line_was_dropped(self) -> None:
        for dropped, wanted in ((0, True), (1, False)):
            with self.subTest(dropped=dropped):
                run, record = self.run_of(dropped)
                run.stack.job = lambda arguments, *_, **__: (
                    (2, {}, "signal_profile: usage\n", 0.1) if arguments[-1].startswith("--") else (1, {}, "signal_profile: branch_missing\n", 0.1)
                )
                run.bounded_failure()
                self.assertEqual([wanted, wanted], [item.passed for item in record.observations[:2]])


class ProfileStreams(unittest.TestCase):
    """The profile runner returns and logs only the known lines of a job's streams: the codes of the profile job, and the fixed lines of its stand-in."""

    STREAM = (
        "WARN jvm the value 31337\n"
        "signal_profile: counts_rows: 1 row\n"
        "signal_profile: key: 31337 rows\n"
        "Traceback (most recent call last):\n"
        'ValueError: the outcome is "31337"\n'
        "signal_profile: usage\n"
    )
    OUT = 'INFO an output line 31337\nRESULT {"outcome": "unchanged"}\n'

    def setUp(self) -> None:
        self.module = runner("signal_profile")

    def job(self, arguments: list[str], err: str, code: int = 1) -> tuple[tuple[int, dict[str, Any], str, float], str]:
        stack, _, _ = stand_in_stack(self, self.module, None, code, out=self.OUT, err=err)
        result = stack.job(arguments)
        return result, "\n".join(stack.log)

    def test_the_profile_job_returns_its_known_lines_and_logs_no_raw_stream(self) -> None:
        (code, document, err, _), log = self.job(["-m", "spark.jobs.signal_profile", "verify", "--branch", "b"], self.STREAM)
        self.assertEqual((1, {}), (code, document))
        self.assertEqual("signal_profile: counts_rows: 1 row\nsignal_profile: usage\n", err)
        self.assertEqual(["counts_rows: 1 row", "usage"], self.module.codes(err))
        for text in ("31337", "Traceback", "WARN", "INFO", "ValueError", "counts_rows"):
            self.assertNotIn(text, log)
        self.assertIn("exit 1", log)

    def test_the_stand_in_returns_only_its_fixed_lines_and_logs_no_raw_stream(self) -> None:
        stream = (
            "WARN jvm 31337\nSTEP pin_check\nSTEP anything 31337\nlog STEP pin_ok 31337\nSTEP first_read 31337\npin_mismatch: clinical_data\n"
            "pin_mismatch: clinical_data 31337\nx pin_mismatch: track_list\npin_mismatch: 31337\nTraceback (most recent call last):\n"
        )
        (code, _, err, _), log = self.job(["/data/stand_in.py", "load_clinical", "b"], stream)
        self.assertEqual("STEP pin_check\npin_mismatch: clinical_data\n", err)
        self.assertIn("pin_mismatch: clinical_data", err)
        self.assertEqual(["STEP pin_check"], [line for line in err.splitlines() if line.startswith("STEP ")])
        for text in ("31337", "Traceback", "WARN"):
            self.assertNotIn(text, log)

    def test_the_log_holds_no_size_and_no_content_of_a_stream(self) -> None:
        _, log = self.job(["-m", "spark.jobs.signal_profile", "verify", "--branch", "b"], self.STREAM)
        for size in (len(self.OUT.encode()), len(self.STREAM.encode())):
            self.assertNotIn(str(size), log)
        self.assertNotIn("bytes", log)

    def test_a_line_of_the_job_that_is_dropped_is_counted_so_that_a_case_can_refuse_it(self) -> None:
        stack, _, _ = stand_in_stack(self, self.module, None, 1, out=self.OUT, err=self.STREAM)
        stack.job(["-m", "spark.jobs.signal_profile", "verify", "--branch", "b"])
        self.assertEqual(1, stack.unknown_lines)
        clean, _, _ = stand_in_stack(self, self.module, None, 1, out=self.OUT, err="signal_profile: usage\nWARN jvm\n")
        clean.job(["-m", "spark.jobs.signal_profile", "verify", "--branch", "b"])
        self.assertEqual(0, clean.unknown_lines)

    def test_the_docker_and_compose_commands_log_their_exit_and_no_output(self) -> None:
        stack, _, _ = stand_in_stack(self, self.module, None, 1, out=self.OUT, err=self.STREAM)
        stack.docker("image", "inspect", "--format", "x", "tag")
        stack.compose_run("up", "--detach", "postgres")
        log = "\n".join(stack.log)
        for text in ("31337", "Traceback", "WARN", "INFO", "ValueError"):
            self.assertNotIn(text, log)
        self.assertIn("exit 1", log)

    def test_no_runner_logs_the_tail_of_an_image_step_that_failed(self) -> None:
        for name in ("signal_profile", "gold_dimensions"):
            with self.subTest(name=name):
                runner(name)
                self.assertFalse("output[-3000:]" in (E2E / f"{name}.py").read_text())

    def test_a_clean_run_returns_an_empty_error_text(self) -> None:
        (code, document, err, _), _ = self.job(["-m", "spark.jobs.signal_profile", "verify", "--branch", "b"], "INFO only jvm text 31337\n", code=0)
        self.assertEqual((0, ""), (code, err))


if __name__ == "__main__":
    unittest.main()
