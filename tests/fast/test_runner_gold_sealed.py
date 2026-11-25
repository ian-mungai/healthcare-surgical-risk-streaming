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

Failure modes covered here (fix round 5, owner decision D):

- The Gold adapter dropped the real command line's ``source_missing`` and ``source_schema`` refusals and kept a count after an unknown dimension or after a
  code with no dimension. It now keeps exactly the lines that the producer can print (each code with its detail), tested with the lines of the real producer.
- A result of the pre-seal shape was rejected, but its lab count reached the record, and a lab reply with a member count was accepted by the independent read.
  The result and each reply are checked for their exact keys and Boolean and whole-number types before anything of them is formatted, and an invalid one is
  recorded as a fixed error.
- The Gold and Silver cases setups merged the stand-in Bronze tables (and the Silver cases runner the split and the sample-sized tables) before they checked
  the flags of the answer.
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
from spark.internal_diagnostic import internal_error_line
from spark.settings import SettingsError
from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def texts(record: Record) -> str:
    return json.dumps([item.observed for item in record.observations], sort_keys=True, default=str)


def recorder(calls: list[tuple[Any, ...]]) -> Any:
    """A stand-in for ``Run.merge`` that records the arguments of each call and answers that the merge was made."""

    def merge(*arguments: Any, **_: Any) -> str:
        calls.append(arguments)
        return "merged"

    return merge


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
        self.merges: list[tuple[Any, ...]] = []

    def setup_of(self, answer: dict[str, Any], code: int = 0) -> tuple[Record, list[dict[str, Any]]]:
        record = Record(feature="test", slug="test", rerun="none")
        specs: list[dict[str, Any]] = []
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.made, run.trunk, run.sources = record, [], "trunk", (self.folder / "clinical.csv", self.folder / "lab.csv")
        run.nessie = SimpleNamespace(create_branch=lambda *_: None)
        run.stack = SimpleNamespace(write_spec=specs.append, stand_in=lambda *_, **__: (code, answer, "", 0.1), split=lambda *_, **__: (1, {}, "", 0.0))
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.merge = recorder(self.merges)
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

    def test_the_loader_tables_are_merged_only_after_the_four_flags_are_true(self) -> None:
        for name in self.FLAGS:
            for value in (False, None, 1, "True", [True]):
                with self.subTest(name=name, value=value):
                    self.merges.clear()
                    self.setup_of(self.answer(**{name: value}))
                    self.assertEqual([], self.merges)
            with self.subTest(name=name, value="missing"):
                self.merges.clear()
                self.setup_of({key: True for key in self.FLAGS if key != name})
                self.assertEqual([], self.merges)
        self.merges.clear()
        self.setup_of({"cases": len(self.module.CASES), "types": ["string"]})
        self.assertEqual([], self.merges)
        self.setup_of({**self.answer(), "rows": 4417})
        self.assertEqual([], self.merges)
        self.setup_of(self.answer(), code=1)
        self.assertEqual([], self.merges)
        self.setup_of(self.answer())
        self.assertEqual(1, len(self.merges))
        self.assertEqual("stand-in bronze tables", self.merges[0][2])

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

    LAB_PAIR = "WARN 4417\ngold_dimensions: empty_source: failed\ngold_dimensions: empty_source: dim_lab_test, 2 members\n"

    def test_a_line_of_the_job_prefix_that_the_adapter_drops_is_counted_across_the_attempts_and_reset_by_each_job(self) -> None:
        command = ["docker", "compose", "run", "--entrypoint", "python", "spark"]
        stack = self.stack(
            [(1, "", self.LAB_PAIR), (1, "", "WARN 4417\ngold_dimensions: empty_source: failed\n"), (1, "", "gold_dimensions: empty_source: failed\n")]
        )
        self.assertEqual(0, stack.unknown_lines)
        stack.run(command)
        self.assertEqual(1, stack.unknown_lines)
        stack.run(command)
        self.assertEqual(1, stack.unknown_lines, "a clean attempt after one that dropped a line keeps the count: the job's attempts add up")
        stack.run(["docker", "ps"])
        self.assertEqual(1, stack.unknown_lines, "a command that is not a job leaves the count")
        stack.gold("build", "branch", base_branch="trunk")
        self.assertEqual(0, stack.unknown_lines, "each job starts its count at zero")

    def test_a_stopped_attempt_keeps_its_count_for_the_retry_and_the_next_job_starts_at_zero(self) -> None:
        stack = self.stack([(1, "", self.LAB_PAIR), TimeoutExpired(["x"], 1.0), (1, "", "gold_dimensions: empty_source: failed\n")])
        command = ["docker", "compose", "run", "--entrypoint", "python", "spark"]
        stack.run(command)
        self.assertEqual(1, stack.unknown_lines)
        with self.assertRaises(TimeoutExpired):
            stack.run(command)
        self.assertEqual(1, stack.unknown_lines)
        stack.gold("build", "branch", base_branch="trunk")
        self.assertEqual(0, stack.unknown_lines)

    def test_a_stream_of_lines_that_do_not_begin_with_the_prefix_drops_nothing_that_is_counted(self) -> None:
        stack = self.stack([(1, "", "WARN 4417\nTraceback 4417\ngold_dimensions: key_null: dim_case, 1 row\n")])
        stack.run(["docker", "compose", "run", "--entrypoint", "python", "spark"])
        self.assertEqual(0, stack.unknown_lines)

    def test_the_lab_refusal_that_also_prints_a_sealed_count_fails_the_case_end_to_end(self) -> None:
        for stream, wanted in (("gold_dimensions: empty_source: failed\n", True), (self.LAB_PAIR, False)):
            with self.subTest(stream=stream[-40:]):
                stack = self.stack([(1, "", stream)])
                record = Record(feature="test", slug="test", rerun="none")
                run = self.module.GoldRun.__new__(self.module.GoldRun)
                run.record, run.gold, run.trunk = record, stack, "trunk"
                run.stack = SimpleNamespace(spark_containers=lambda: [])
                run.head = lambda branch: "h"
                run.name = lambda base: base
                run.refused_from("an empty laboratory table", "built", "source", self.module.PLANTED_LAB_CODES)
                self.assertEqual(wanted, record.observations[0].passed, texts(record))
                self.assertNotIn("dim_lab_test", texts(record))
                self.assertNotIn("2 members", texts(record))

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

    def test_the_diagnostic_line_of_an_internal_error_is_not_returned_and_not_counted_as_unknown(self) -> None:
        lines = "gold_dimensions: internal_error\ngold_dimensions: internal_error class=builtins.RuntimeError <- builtins.OSError frames=a.py:1:f | b.py:2:g\n"
        stack = self.stack([(70, "", lines)])
        code, _, err = stack.run(["docker", "compose", "run", "--entrypoint", "python", "spark"])
        self.assertEqual((70, "gold_dimensions: internal_error\n"), (code, err))
        self.assertEqual(0, stack.unknown_lines)

    def counted(self, code: int, *lines: str) -> int:
        """The unknown lines of one run of the Gold command that ended with exit code ``code`` and wrote ``lines`` (each with the prefix of the job)."""
        stack = self.stack([(code, "", "".join(f"gold_dimensions: {line}\n" for line in lines))])
        stack.run(["docker", "compose", "run", "--entrypoint", "python", "spark"])
        return int(stack.unknown_lines)

    def test_a_line_that_is_shaped_like_the_diagnostic_line_but_holds_text_is_counted_as_unknown(self) -> None:
        for text in (
            "internal_error class=builtins.RuntimeError frames=a.py:1:f message",
            "internal_error class=builtins.RuntimeError: boom frames=",
            "internal_error class=a frames=",
            "internal_error class=builtins.RuntimeError frames=/x/a.py:1:f",
            "internal_error class=" + "C" * 121 + " frames=",
            "internal_error class=builtins.NotAClass frames=",
            "internal_error class=builtins.RuntimeError frames=a-b.py:1:f",
            "internal_error class=builtins.RuntimeError frames=other | other",
        ):
            with self.subTest(text=text[:60]):
                self.assertEqual(1, self.counted(70, "internal_error", text))

    def test_a_diagnostic_line_is_unknown_on_any_exit_code_but_70(self) -> None:
        diagnostic = "internal_error class=builtins.KeyError frames=a.py:1:f"
        for code, first in ((1, "branch_missing"), (1, "internal_error"), (2, "internal_error"), (0, "internal_error")):
            with self.subTest(code=code, first=first):
                self.assertEqual(1, self.counted(code, first, diagnostic))
        self.assertEqual(0, self.counted(70, "internal_error", diagnostic))

    def test_two_diagnostic_lines_on_one_internal_error_are_one_unknown_and_so_is_one_that_follows_another_code(self) -> None:
        diagnostic = "internal_error class=builtins.KeyError frames=a.py:1:f"
        self.assertEqual(1, self.counted(70, "internal_error", diagnostic, diagnostic))
        self.assertEqual(1, self.counted(70, "settings", diagnostic))
        self.assertEqual(1, self.counted(70, diagnostic))

    def test_every_line_that_the_producer_prints_for_an_error_is_known_even_with_a_hyphen_in_a_name(self) -> None:
        for name, module in (("A-b", "m-n"), ("C" * 500, "m"), ("has space", "")):
            with self.subTest(name=name[:10]):
                error = type(name, (Exception,), {"__module__": module})("MARKER_4417")
                self.assertEqual(0, self.counted(70, "internal_error", internal_error_line(error)))

    def test_a_sealed_job_still_returns_no_error_text(self) -> None:
        stack = self.stack([])

        def sealed_job(self: Any, *arguments: Any, **keywords: Any) -> tuple[int, dict[str, Any], str, float]:
            self.last_codes = "gold_dimensions: key_null: dim_case, 1 row\n"
            return 1, {"x": 1}, "", 0.1

        with mock.patch.object(self.module.base.Stack, "job", sealed_job):
            self.assertEqual("", stack.job(["-m", "x"], sealed=("cases", "build", "b"))[2])
            self.assertEqual("gold_dimensions: key_null: dim_case, 1 row\n", stack.job(["-m", "x"])[2])


class SilverLoaderFlags(unittest.TestCase):
    """The Silver cases runner merges the stand-in Bronze tables only after the loader's four flags are true (fix round 5)."""

    FLAGS = ("cases_as_expected", "columns_as_expected", "every_column_a_string", "lab_total_ok")

    def setUp(self) -> None:
        self.module = runner("silver_cases")

    def setup_of(self, answer: dict[str, Any], code: int = 0) -> tuple[Record, list[tuple[Any, ...]]]:
        record = Record(feature="test", slug="test", rerun="none")
        merges: list[tuple[Any, ...]] = []
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.made, run.trunk, run.facts = record, [], "trunk", {}
        run.nessie = SimpleNamespace(create_branch=lambda *_: None, entries=lambda *_: {})
        run.stack = SimpleNamespace(write_spec=lambda *_: None, stand_in=lambda *_, **__: (code, answer, "", 0.1))
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.recreate = lambda *_: None
        run.merge = recorder(merges)
        run.known = None
        with mock.patch.object(self.module.Expected, "from_container", side_effect=RuntimeError("not reached")), contextlib.suppress(RuntimeError):
            run.setup()
        return record, merges

    def answer(self, **flags: Any) -> dict[str, Any]:
        return {name: flags.get(name, True) for name in self.FLAGS}

    def test_a_false_missing_or_extra_flag_is_never_merged(self) -> None:
        for name in self.FLAGS:
            for value in (False, None, 1, "True"):
                with self.subTest(name=name, value=value):
                    record, merges = self.setup_of(self.answer(**{name: value}))
                    self.assertEqual([], merges)
                    self.assertFalse(record.observations[0].passed)
            with self.subTest(name=name, value="missing"):
                self.assertEqual([], self.setup_of({key: True for key in self.FLAGS if key != name})[1])
        with self.subTest("extra key"):
            self.assertEqual([], self.setup_of({**self.answer(), "rows": 4417})[1])
        with self.subTest("failed exit"):
            self.assertEqual([], self.setup_of(self.answer(), code=1)[1])

    def test_four_true_flags_are_merged_once(self) -> None:
        record, merges = self.setup_of(self.answer())
        self.assertEqual(1, len(merges))
        self.assertEqual("stand-in bronze tables", merges[0][2])
        self.assertTrue(record.observations[0].passed)


class SilverMergeOrder(unittest.TestCase):
    """The Silver cases runner merges a sealed job's branch only after every flag of the job's answer is true: the patient split (fix round 5, the same order
    as the loader of the setup). The sample-sized setup is in ``tests/fast/test_runner_reply_checks.py``."""

    GOOD_SPLIT = {"base_hash": "a" * 64, "branch_hash": "b" * 64, "passed": {"patient_split": True}}

    def setUp(self) -> None:
        self.module = runner("silver_cases")

    def run_of(self) -> tuple[Any, list[tuple[Any, ...]]]:
        merges: list[tuple[Any, ...]] = []
        run = self.module.Run.__new__(self.module.Run)
        run.record, run.made, run.trunk, run.facts, run.labels = Record(feature="test", slug="test", rerun="none"), [], "trunk", {}, {}
        run.nessie = SimpleNamespace(create_branch=lambda *_: None, entries=lambda *_: {"bronze.clinical_cases": 1, "bronze.lab_results": 1})
        run.staged = contextlib.nullcontext
        run.name = lambda base: base
        run.recreate = lambda *_: None
        run.fork = lambda base, label: label
        run.merge = recorder(merges)
        run.candidates = []
        run.known = SimpleNamespace(cases_as_expected=True, patients_as_expected=True, counts_as_frozen=True, lab_total_is_published=True)
        return run, merges

    def patient_split(self, split: dict[str, Any], code: int = 0) -> list[str]:
        run, merges = self.run_of()
        loader = {name: True for name in ("cases_as_expected", "columns_as_expected", "every_column_a_string", "lab_total_ok")}
        run.stack = SimpleNamespace(
            write_spec=lambda *_: None,
            stand_in=lambda *_, **__: (0, loader, "", 0.1),
            split=lambda *_, **__: (code, split, "", 0.1),
            spark_containers=lambda: [],
        )
        with mock.patch.object(self.module.Expected, "from_container", return_value=run.known), contextlib.suppress(Exception):
            run.setup()
        return [arguments[2] for arguments in merges]

    def test_a_patient_split_with_a_false_flag_or_a_removed_field_is_never_merged(self) -> None:
        bad: dict[str, dict[str, Any]] = {
            "a false pass flag": {**self.GOOD_SPLIT, "passed": {"patient_split": False}},
            "a pass flag that is 1": {**self.GOOD_SPLIT, "passed": {"patient_split": 1}},
            "no pass flag": {**self.GOOD_SPLIT, "passed": {}},
            "no write": {**self.GOOD_SPLIT, "branch_hash": "a" * 64},
            "a removed field": {**self.GOOD_SPLIT, "patients": 4417},
        }
        for label, answer in bad.items():
            with self.subTest(label):
                self.assertEqual(["stand-in bronze tables"], self.patient_split(answer))
        with self.subTest("a failed exit"):
            self.assertEqual(["stand-in bronze tables"], self.patient_split(self.GOOD_SPLIT, code=1))

    def test_a_patient_split_with_every_flag_true_is_merged(self) -> None:
        self.assertEqual(["stand-in bronze tables", "patient split"], self.patient_split(self.GOOD_SPLIT))


def healthy_result(module: ModuleType) -> dict[str, Any]:
    """The result of a Gold build as the sealed job prints it: no member count and no write indicator for the lab dimension."""
    open_names = [item.name for item in module.DIMENSIONS if not item.sealed]
    return {
        "mode": "build",
        "branch": "branch",
        "base_hash": "a" * 64,
        "branch_hash": "b" * 64,
        "outcome": "written",
        "written": {name: True for name in open_names},
        "checks_run": list(module.CHECKS),
        "members": {name: 4 for name in open_names},
        "passed": {item.name: True for item in module.DIMENSIONS},
        "snapshot_ids": {item.table: 7 for item in module.DIMENSIONS},
    }


LAB_COUNT = 31337


def pre_seal_result(module: ModuleType) -> dict[str, Any]:
    """The shape before the seal: the lab dimension has a member count and a write indicator."""
    found = healthy_result(module)
    lab = next(item.name for item in module.DIMENSIONS if item.sealed)
    found["members"][lab] = LAB_COUNT
    found["written"][lab] = True
    return found


class SealedResult(unittest.TestCase):
    """The Gold result is checked for its exact keys and Boolean types before anything of it is formatted, and an invalid one is recorded as a fixed error."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.lab = next(item.name for item in self.module.DIMENSIONS if item.sealed)

    def test_the_result_of_the_sealed_job_has_no_problem(self) -> None:
        self.assertEqual([], self.module.sealed_result_problems(healthy_result(self.module)))

    def test_each_wrong_shape_is_a_problem(self) -> None:
        healthy = healthy_result(self.module)
        case = next(item.name for item in self.module.DIMENSIONS if item.name == "dim_case")

        def changed(**edits: Any) -> dict[str, Any]:
            return {**copy.deepcopy(healthy), **edits}

        def without(name: str) -> dict[str, Any]:
            return {key: value for key, value in copy.deepcopy(healthy).items() if key != name}

        wrong: dict[str, dict[str, Any]] = {
            "an extra count": changed(lab_members=LAB_COUNT),
            "a result with no members": without("members"),
            "a result with no written": without("written"),
            "a result with no passed": without("passed"),
            "a result with no outcome": without("outcome"),
            "a result with no snapshot IDs": without("snapshot_ids"),
            "a lab member count": changed(members={**healthy["members"], self.lab: LAB_COUNT}),
            "a lab write indicator": changed(written={**healthy["written"], self.lab: True}),
            "a written flag that is 1": changed(written={**healthy["written"], case: 1}),
            "a written flag that is a text": changed(written={**healthy["written"], case: "True"}),
            "a member count that is a Boolean": changed(members={**healthy["members"], case: True}),
            "a member count that is a text": changed(members={**healthy["members"], case: "4"}),
            "a member count below zero": changed(members={**healthy["members"], case: -1}),
            "a missing member count": changed(members={key: value for key, value in healthy["members"].items() if key != case}),
            "a passed flag that is false": changed(passed={**healthy["passed"], self.lab: False}),
            "a passed flag that is 1": changed(passed={**healthy["passed"], case: 1}),
            "a missing passed flag": changed(passed={key: value for key, value in healthy["passed"].items() if key != self.lab}),
            "an extra passed flag": changed(passed={**healthy["passed"], "dim_extra": True}),
            "an outcome that is not one of the three": changed(outcome="maybe"),
            "an outcome that is a number": changed(outcome=1),
            "other checks": changed(checks_run=["input_sources"]),
            "a snapshot ID that is a text": changed(snapshot_ids={**healthy["snapshot_ids"], f"gold.{self.lab}": "7"}),
            "a missing snapshot ID": changed(snapshot_ids={key: value for key, value in healthy["snapshot_ids"].items() if key != f"gold.{self.lab}"}),
            "an extra snapshot ID": changed(snapshot_ids={**healthy["snapshot_ids"], "gold.dim_extra": 1}),
            "a result that is not a mapping": {},
        }
        for label, document in wrong.items():
            with self.subTest(label):
                self.assertTrue(self.module.sealed_result_problems(document), label)
        for label, answer in (("none", None), ("a list", []), ("a text", "written")):
            with self.subTest(label):
                self.assertTrue(self.module.sealed_result_problems(answer))

    def test_a_verify_result_with_the_verified_outcome_has_no_problem(self) -> None:
        found = {**healthy_result(self.module), "mode": "verify", "outcome": "verified", "base_hash": None, "written": {}}
        found["written"] = {item.name: False for item in self.module.DIMENSIONS if not item.sealed}
        self.assertEqual([], self.module.sealed_result_problems(found))

    def run_of(self, answer: dict[str, Any], code: int = 0) -> tuple[Any, Record]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record, run.trunk, run.facts = record, "p7_trunk_x1", {}
        run.gold = SimpleNamespace(gold=lambda *_, **__: (code, answer, "", 0.1))
        run.stack = SimpleNamespace(spark_containers=lambda: [])
        run.nessie = SimpleNamespace(entries=lambda branch: {item.table: 1 for item in self.module.DIMENSIONS})
        run.head = lambda branch: "b" * 64 if branch != "p7_trunk_x1" else "a" * 64
        run.table_snapshot = lambda branch, table: 7
        run.name = lambda base: f"p7_{base}_x1"
        run.merge = lambda *_, **__: "merged"
        return run, record

    def assert_sealed(self, record: Record, out: str) -> None:
        for text in (texts(record), out, json.dumps([item.__dict__ for item in record.observations], default=str)):
            self.assertNotIn(str(LAB_COUNT), text)
            self.assertNotIn(self.lab, text)

    def test_the_control_records_the_healthy_result_and_passes(self) -> None:
        run, record = self.run_of(healthy_result(self.module))
        with contextlib.redirect_stdout(io.StringIO()):
            built = run.control()
        self.assertTrue(built)
        self.assertTrue(all(item.passed for item in record.observations), texts(record))

    def test_the_control_records_a_pre_seal_result_as_a_fixed_error_and_never_its_lab_count(self) -> None:
        run, record = self.run_of(pre_seal_result(self.module))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            built = run.control()
        self.assertFalse(built)
        self.assertFalse(record.observations[0].passed)
        self.assert_sealed(record, out.getvalue())
        self.assertEqual(1, len(record.observations))

    def test_the_control_never_formats_an_extra_key_a_wrong_type_or_a_text_of_a_pre_seal_result(self) -> None:
        changes: dict[str, dict[str, Any]] = {
            "an extra key": {"lab_members": LAB_COUNT},
            "a wrong type": {"members": {"dim_case": True}},
            "a lab key in written": {"written": {self.lab: True}},
        }
        for label, change in changes.items():
            with self.subTest(label):
                run, record = self.run_of({**healthy_result(self.module), **change})
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    run.control()
                self.assertFalse(record.observations[0].passed)
                self.assert_sealed(record, out.getvalue())
                self.assertIn("the result has the sealed shape False", record.observations[0].observed)

    def test_the_verify_the_rebuild_and_the_nights_record_a_pre_seal_result_as_a_fixed_error(self) -> None:
        pre = pre_seal_result(self.module)
        run, record = self.run_of({**pre, "mode": "verify", "outcome": "verified", "branch_hash": "b" * 64})
        self.assertIsNone(run.verify_and_merge("branch", "message"))
        self.assertFalse(record.observations[0].passed)
        self.assert_sealed(record, "")
        run, record = self.run_of({**pre, "outcome": "unchanged"})
        run.rebuild()
        self.assertFalse(record.observations[0].passed)
        self.assert_sealed(record, "")

    def test_a_night_records_a_pre_seal_build_result_as_a_fixed_error(self) -> None:
        run, record = self.run_of(pre_seal_result(self.module))
        names = [item.name for item in self.module.DIMENSIONS]
        night = self.module.NIGHTS[0]
        healthy = {item.name: {"controls": {name: True for name in self.module.HISTORY_CONTROLS}} for item in self.module.DIMENSIONS if item.sealed}
        quiet = {
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
        dump = {
            item.name: {"flags": {name: True for name in self.module.DUMP_FLAGS}, "columns": [], "types": [], "versions": 3, "members": 3}
            for item in self.module.DIMENSIONS
        }
        run.fork = lambda base, label: f"{label}_branch"
        run.edit = lambda *_, **__: 0
        run.digest = lambda branch: {"x": 1}
        run.dump = lambda branch: dump
        run.history = lambda *_: {name: healthy.get(name) or quiet for name in names}
        run.verify_and_merge = lambda *_, **__: None
        run.gold = SimpleNamespace(gold=lambda *_, **__: (0, pre_seal_result(self.module), "", 0.1))
        run.nights()
        self.assertEqual(night.title, record.observations[0].case)
        self.assertFalse(record.observations[0].passed)
        self.assert_sealed(record, "")


class DumpShape(unittest.TestCase):
    """The independent read is checked for its exact keys and types before it is formatted."""

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.helpers = namespace(self.module.GOLD_HELPERS)
        self.lab = next(item for item in self.module.DIMENSIONS if item.sealed)

    def counts(self) -> dict[str, int]:
        return {
            "rows": 4,
            "members": 3,
            "expected_members": 3,
            "extra_members": 0,
            "missing_members": 0,
            "versions": 3,
            "wrong_keys": 0,
            "distinct_keys": 4,
            "unknown_rows": 1,
            "unknown_holding_a_value": 0,
        }

    def good(self, item: Any) -> dict[str, Any]:
        found: dict[str, Any] = self.helpers["facts_for"](item.name, self.counts(), [c.name for c in item.columns], [c.type for c in item.columns])
        return found

    def test_a_reply_of_the_exact_shape_has_no_problem(self) -> None:
        for item in self.module.DIMENSIONS:
            with self.subTest(item=item.name):
                self.assertEqual([], self.module.dump_problems(item, self.good(item)))

    def test_a_lab_reply_with_a_member_count_or_any_extra_key_is_a_problem(self) -> None:
        for extra in ({"members": LAB_COUNT}, {"rows": LAB_COUNT}, {"versions": 3}, {"anything": True}):
            with self.subTest(extra=extra):
                self.assertTrue(self.module.dump_problems(self.lab, {**self.good(self.lab), **extra}))

    def test_a_reply_with_a_missing_key_or_a_wrong_type_is_a_problem(self) -> None:
        case = next(item for item in self.module.DIMENSIONS if item.name == "dim_case")
        for item in (case, self.lab):
            good = self.good(item)
            variants: dict[str, dict[str, Any]] = {
                "an extra flag": {**good, "flags": {**good["flags"], "extra": True}},
                "a missing flag": {**good, "flags": {key: value for key, value in good["flags"].items() if key != "keys_ok"}},
                "a flag that is 1": {**good, "flags": {**good["flags"], "keys_ok": 1}},
                "flags that are a list": {**good, "flags": [True] * 7},
                "columns that are a text": {**good, "columns": "x"},
                "columns that hold a number": {**good, "columns": [*good["columns"][:-1], 4]},
                "types that are a tuple": {**good, "types": tuple(good["types"])},
                "no types": {key: value for key, value in good.items() if key != "types"},
            }
            for label, facts in variants.items():
                with self.subTest(item=item.name, label=label):
                    self.assertTrue(self.module.dump_problems(item, facts))
        open_good = self.good(case)
        for name in self.counts():
            with self.subTest(count=name):
                self.assertTrue(self.module.dump_problems(case, {**open_good, name: True}))
                self.assertTrue(self.module.dump_problems(case, {key: value for key, value in open_good.items() if key != name}))
                self.assertTrue(self.module.dump_problems(case, {**open_good, name: "3"}))

    def run_of(self, replies: dict[str, Any]) -> tuple[Record, str]:
        record = Record(feature="test", slug="test", rerun="none")
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.record = record
        run.dump = lambda branch: replies
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            run.read_dimensions({"branch": "built"})
        return record, out.getvalue()

    def test_the_independent_read_records_a_fixed_error_for_columns_and_types_of_the_wrong_shape(self) -> None:
        for name, wrong in (("columns", [LAB_COUNT]), ("types", str(LAB_COUNT)), ("columns", {"x": LAB_COUNT}), ("types", [[LAB_COUNT]])):
            with self.subTest(name=name, wrong=wrong):
                replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
                replies[self.lab.name] = {**replies[self.lab.name], name: wrong}
                self.assertTrue(self.module.dump_shape_problems(self.lab, replies[self.lab.name]))
                record, out = self.run_of(replies)
                self.assertFalse(all(item.passed for item in record.observations))
                for text in (texts(record), out):
                    self.assertNotIn(str(LAB_COUNT), text)

    def test_the_dump_keeps_only_the_replies_of_the_exact_shape(self) -> None:
        replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
        replies[self.lab.name] = {**replies[self.lab.name], "members": LAB_COUNT}
        run = self.module.GoldRun.__new__(self.module.GoldRun)
        run.stack = SimpleNamespace(write_spec=lambda *_: None, stand_in=lambda *_: (0, replies, "", 0.1))
        kept = run.dump("branch")
        self.assertEqual({item.name for item in self.module.DIMENSIONS} - {self.lab.name}, set(kept))
        self.assertNotIn(str(LAB_COUNT), json.dumps(kept))
        self.assertEqual(
            {}, run.dump.__func__(SimpleNamespace(stack=SimpleNamespace(write_spec=lambda *_: None, stand_in=lambda *_: (1, replies, "", 0.1))), "b")
        )
        self.assertEqual(
            {}, run.dump.__func__(SimpleNamespace(stack=SimpleNamespace(write_spec=lambda *_: None, stand_in=lambda *_: (0, ["x"], "", 0.1))), "b")
        )

    def test_the_independent_read_passes_the_replies_of_the_exact_shape(self) -> None:
        record, _ = self.run_of({item.name: self.good(item) for item in self.module.DIMENSIONS})
        self.assertTrue(all(item.passed for item in record.observations), texts(record))

    def test_the_independent_read_fails_a_lab_reply_with_a_member_count_and_never_records_it(self) -> None:
        replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
        replies[self.lab.name] = {**replies[self.lab.name], "members": LAB_COUNT, "rows": LAB_COUNT + 1}
        record, out = self.run_of(replies)
        failed = [item for item in record.observations if not item.passed]
        self.assertEqual([self.lab.name], [item.case.removeprefix("an independent read of ") for item in failed])
        for text in (texts(record), out):
            self.assertNotIn(str(LAB_COUNT), text)
            self.assertNotIn(str(LAB_COUNT + 1), text)

    def test_the_independent_read_records_a_fixed_error_for_each_reply_of_the_wrong_shape(self) -> None:
        wrong_replies: list[Any] = [{"members": LAB_COUNT}, {"flags": {"keys_ok": LAB_COUNT}, "columns": [], "types": []}, "text 31337", [LAB_COUNT]]
        for reply in wrong_replies:
            with self.subTest(reply=reply):
                replies = {item.name: self.good(item) for item in self.module.DIMENSIONS}
                replies[self.lab.name] = reply
                record, out = self.run_of(replies)
                self.assertFalse(all(item.passed for item in record.observations))
                for text in (texts(record), out):
                    self.assertNotIn(str(LAB_COUNT), text)


class ProducerLines(unittest.TestCase):
    """Every line that the real Gold command line can print goes through the adapter, and nothing else does (fix round 5).

    The lines come from the producer itself: ``spark/gold_dimensions.py`` (the checks), ``spark/gold_dimensions_run.py`` (the command line and the schema
    checks) and ``spark/jobs/gold_dimensions.py`` (the source and inherited schema checks). The adapter keeps the code and the detail that the producer
    can print for it and drops every other combination: an unknown dimension, a count after a code that takes no dimension, a code with a dimension that has
    no such count, a count of the lab dimension (its failures print as ``<code>: failed``) and a noun or a number that the producer never writes.
    """

    COUNTS = (1, 2, 7)

    def setUp(self) -> None:
        self.module = runner("gold_dimensions")
        self.run_module = importlib.import_module("spark.gold_dimensions_run")
        self.producer = importlib.import_module("spark.gold_dimensions")
        self.job = importlib.import_module("spark.jobs.gold_dimensions")

    def printed(self, error: BaseException | None = None, argv: list[str] | None = None) -> list[str]:
        """The lines that the real command line writes to standard error when ``execute`` raises ``error`` (or when ``argv`` is a bad call)."""
        err = io.StringIO()

        def execute(arguments: Any) -> dict[str, Any]:
            if error is not None:
                raise error
            return {}

        with contextlib.redirect_stderr(err), contextlib.suppress(SystemExit):
            self.run_module.run(argv or ["verify", "--branch", "b"], execute)
        return err.getvalue().splitlines()

    def facts(self, **values: int) -> dict[str, int]:
        names = (*self.producer.INPUT_FACTS, *self.producer.OUTPUT_FACTS)
        return {name: values.get(name, 0) for name in names}

    def check_lines(self, count: int) -> list[str]:
        """The lines of the input, output and frozen checks for every dimension, with ``count`` as each count."""
        names = [item.name for item in self.producer.DIMENSIONS]
        inputs = {name: self.facts(source_members=0, ambiguous_members=count, key_null_rows=count) for name in names}
        output_facts = {name: count for name in (*self.producer.OUTPUT_FACTS,)}
        outputs = {name: self.facts(**{**output_facts, "members": 0, "unknown_rows": count + 1}) for name in names}
        empty = {name: self.facts(**{**output_facts, "members": 1, "unknown_rows": 0}) for name in names}
        held = {name: (count, count) for name in names}
        lines: list[str] = []
        for problems in (
            self.producer.input_problems(inputs),
            self.producer.output_problems(outputs),
            self.producer.output_problems(empty),
            self.producer.frozen_problems(held),
        ):
            lines += self.printed(self.producer.GoldDimensionsError(*problems))
        return lines

    def table_lines(self) -> list[str]:
        lines: list[str] = []
        schemas = self.producer.SOURCE_SCHEMAS
        for missing in range(len(schemas) + 1):
            for wrong in range(len(schemas) - missing + 1):
                found: dict[str, Any] = {}
                for index, (table, fields) in enumerate(schemas.items()):
                    found[table] = None if index < missing else ([("x", "string")] if index < missing + wrong else list(fields))
                lines += self.printed(self.producer.GoldDimensionsError(*self.producer.source_schema_problems(found)))
        names = [item.name for item in self.producer.DIMENSIONS]
        for tables in range(1, len(names) + 1):
            ops = SimpleNamespace(
                source_facts=lambda: {name: self.facts(source_members=1) for name in names},
                tables_missing=lambda tables=tables: tables,
                output_schema_tables_wrong=lambda tables=tables: tables,
            )
            for call in (self.run_module.run_verify, self.run_module.check_stored_schema):
                try:
                    call(ops)
                except self.producer.GoldDimensionsError as error:
                    lines += self.printed(error)
        lake = object.__new__(self.job.GoldLake)
        lake.columns = lambda name: [("x", "string")]
        with self.assertRaises(self.producer.GoldDimensionsError) as caught:
            lake.held_changes()
        return lines + self.printed(caught.exception)

    def fixed_lines(self) -> list[str]:
        """The lines of the fixed messages that hold no code count: a bad name, type, date, key or fact, and a moved branch."""
        raisers = [
            lambda: self.producer.dimension("dim_nothing"),
            lambda: self.producer.version_date("not a date"),
            lambda: self.producer.surrogate_key("dim_case", "", "2026-01-01"),
            lambda: self.producer.count_of({}, "members"),
            lambda: self.producer.facts_of({}, self.producer.DIMENSIONS[0]),
            lambda: self.producer.frozen_problems({}),
            lambda: importlib.import_module("spark.gold_dimensions_sql").plain("bad name"),
            lambda: importlib.import_module("spark.gold_dimensions_sql").spark_type("nothing"),
            self.build_without_a_date,
        ]
        lines = [line for raiser in raisers for line in self.raised(raiser)]
        return lines + self.printed(self.producer.GoldDimensionsError(self.job.BRANCH_MOVED))

    def build_without_a_date(self) -> None:
        lake = object.__new__(self.job.GoldLake)
        lake.valid_from = None
        lake.version_start()

    def raised(self, raiser: Any) -> list[str]:
        try:
            raiser()
        except self.producer.GoldDimensionsError as error:
            return self.printed(error)
        self.fail("the producer raised nothing")
        return []

    @staticmethod
    def without_diagnostic(lines: list[str]) -> list[str]:
        """The lines of a run without the diagnostic line of classes and frames, which the adapter drops (``test_the_diagnostic_line_...``)."""
        return [line for line in lines if " internal_error class=" not in line]

    def test_the_diagnostic_line_of_the_real_job_is_not_a_code(self) -> None:
        printed = self.printed(RuntimeError("x"))
        self.assertEqual(["gold_dimensions: internal_error"], printed[:1])
        self.assertEqual(2, len(printed))
        self.assertEqual(["internal_error"], self.module.codes("\n".join(printed) + "\n"))

    def generic_lines(self) -> list[str]:
        lines = self.printed(argv=["nothing"]) + self.printed(SettingsError("x")) + self.without_diagnostic(self.printed(RuntimeError("x")))
        lines += self.printed(self.run_module.NessieError("x", status=404, error_code=self.run_module.REFERENCE_NOT_FOUND))
        lines += self.printed(self.run_module.NessieError("x", status=500))
        start = importlib.import_module("spark.split_cli")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            start.start_deadline(0.0, exit_with=lambda code: None, job="gold_dimensions").join()
        return lines + err.getvalue().splitlines()

    def every_line(self) -> list[str]:
        lines = [line for count in self.COUNTS for line in self.check_lines(count)]
        return [*lines, *self.table_lines(), *self.fixed_lines(), *self.generic_lines()]

    def test_the_producer_prints_every_form_that_the_adapter_must_keep(self) -> None:
        lines = self.every_line()
        stripped = {line.removeprefix(self.module.JOB_PREFIX) for line in lines}
        for wanted in (
            "usage",
            "settings",
            "branch_missing",
            "catalog_error",
            "internal_error",
            "timeout",
            "empty_source: failed",
            "empty_source: dim_case, 0 members",
            "key_null: dim_patient, 7 rows",
            "unknown_member: dim_track, 0 rows",
            "unknown_member: dim_track, 8 rows",
            "member_removed: dim_department, 1 member",
            "version_date_not_after: dim_surgery_type, 2 members",
            "version_chain: dim_case, 1 version",
            "key_duplicate: dim_track, 2 keys",
            "source_missing: 1 table",
            "source_missing: 3 tables",
            "source_schema: 2 tables",
            "output_schema: 6 tables",
            "table_missing: 1 table",
            "inherited_schema: 1 table",
            "branch_moved: failed",
        ):
            self.assertIn(wanted, stripped)

    def test_every_line_that_the_producer_prints_goes_through_the_adapter_unchanged(self) -> None:
        lines = self.every_line()
        self.assertGreater(len(lines), 100)
        for line in lines:
            with self.subTest(line=line):
                self.assertTrue(line.startswith(self.module.JOB_PREFIX), line)
                self.assertEqual([line.removeprefix(self.module.JOB_PREFIX)], self.module.codes(line + "\n"))

    def test_the_source_refusals_of_the_real_command_line_are_kept(self) -> None:
        stream = "gold_dimensions: source_missing: 1 table\ngold_dimensions: source_schema: 2 tables\n"
        self.assertEqual(["source_missing: 1 table", "source_schema: 2 tables"], self.module.codes(stream))

    def test_a_count_after_an_unknown_dimension_or_after_a_code_without_a_dimension_is_dropped(self) -> None:
        dropped = [
            "key_rule: dim_nothing, 1 member",
            "key_rule: dim_, 1 member",
            "key_rule: 1 member",
            "key_rule: dim_case 1 member",
            "empty_source: 1 member",
            "version_chain: 3 versions",
            "source_missing: dim_case, 1 table",
            "source_schema: dim_lab_test, 1 table",
            "output_schema: dim_case, 1 table",
            "inherited_schema: dim_case, 1 table",
            "table_missing: 1 member",
        ]
        for line in dropped:
            with self.subTest(line=line):
                self.assertEqual([], self.module.codes(f"gold_dimensions: {line}\n"))

    def test_a_count_of_the_lab_dimension_is_dropped_whatever_the_code(self) -> None:
        for code in self.module.DIMENSION_NOUNS:
            for detail in ("0 members", "1 member", "2 rows", "1 key", "3 versions"):
                with self.subTest(code=code, detail=detail):
                    self.assertEqual([], self.module.codes(f"gold_dimensions: {code}: dim_lab_test, {detail}\n"))

    def test_a_form_that_the_producer_never_writes_is_dropped(self) -> None:
        dropped = [
            "key_rule: dim_case, 1 members",
            "key_rule: dim_case, 2 member",
            "key_rule: dim_case, 1 row",
            "key_rule: dim_case, 1 key",
            "key_rule: dim_case, 0 members",
            "key_rule: dim_case, 01 member",
            "key_rule: dim_case, -1 member",
            "empty_source: dim_case, 3 members",
            "no_members: dim_track, 1 member",
            "key_null: dim_track, 1 row",
            "key_null: dim_department, 1 row",
            "key_null: failed",
            "output_schema: failed",
            "source_missing: failed",
            "table_missing: 0 tables",
            "source_missing: 4 tables",
            "source_schema: 1 tables",
            "output_schema: 7 tables",
            "inherited_schema: 2 tables",
            "inherited_schema: 1 tables",
            "branch_moved: dim_case, 1 row",
            "branch_moved",
            "bad_fact: a count",
            "bad_date: a date",
            "usage: dim_case, 1 row",
        ]
        for line in dropped:
            with self.subTest(line=line):
                self.assertEqual([], self.module.codes(f"gold_dimensions: {line}\n"))

    def test_the_known_codes_are_the_codes_that_the_producer_can_print(self) -> None:
        printed = {line.removeprefix(self.module.JOB_PREFIX).split(":")[0] for line in self.every_line()}
        known = {*self.module.GENERIC_CODES, *self.module.DIMENSION_NOUNS, *self.module.TABLE_CODES, *self.module.FIXED_LINES_CODES}
        self.assertEqual(printed, known)


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
        built = healthy_result(self.module)
        built["written"] = {n: n in night.written for n in names if n != "dim_lab_test"}
        run.gold = SimpleNamespace(gold=lambda *_, **__: (0, built, "", 0.1))
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

    FULL = None

    def tables(self, extra: bool = False, full: float | None = None) -> dict[str, tuple[list[dict[str, Any]], list[str]]]:
        """The dump's input tables. With ``extra`` the held-out cases change: one case has more rows, another has none, and a row has no case at all.
        ``full`` is the value that the table holds for the one count of the full cohort."""
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
            "lake.silver.source_counts": (
                [
                    {"name": "n", "value": 1.0, "expected": 1.0, "matches": True},
                    {"name": "whole_cohort_count", "value": self.FULL if full is None else full, "expected": 5.0, "matches": True},
                ],
                ["name", "value", "expected", "matches"],
            ),
            "lake.silver.warning_grid_rule": ([{"rule": "r", "written_at": None}], ["rule", "written_at"]),
        }

    printed = ""

    def run_dump(
        self, tables: dict[str, tuple[list[dict[str, Any]], list[str]]], source: str | None = None, sealed: tuple[str, ...] = ("whole_cohort_count",)
    ) -> dict[str, Any]:
        """Run the stand-in's own ``dump`` (and the two functions it calls) on ``tables``, with Spark stood in, and return what it printed."""
        tree = ast.parse(source or self.module.STAND_IN)
        keep: list[ast.stmt] = [
            node
            for node in tree.body
            if (isinstance(node, ast.FunctionDef) and node.name in ("dump", "profile_sql", "grid_by_case"))
            or (isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant | ast.Tuple))
        ]
        names = namespace(ast.unparse(ast.Module(body=keep, type_ignores=[])), F=FakeFunctions)
        spec = {"dev_cases": self.DEV, "causes": self.CAUSES, "sealed": list(sealed)}
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            names["dump"](FakeSession(tables), spec)
        self.printed = printed.getvalue()
        line = self.printed.strip()
        self.assertTrue(line.startswith("RESULT "), line)
        document: dict[str, Any] = json.loads(line[len("RESULT ") :])
        return document

    def test_the_dump_reads_the_development_rows(self) -> None:
        found = self.run_dump(self.tables())
        self.assertEqual({"numeric_rows": 2, "waveform_rows": 2, "tracks": 2}, found["development"])

    def test_a_value_of_a_full_cohort_count_never_leaves_the_container_and_only_a_flag_says_it_was_there(self) -> None:
        planted = self.run_dump(self.tables(full=424242.5))
        self.assertNotIn("424242", self.printed)
        self.assertIs(False, planted["sealed_values_empty"])
        row = next(item for item in planted["counts"] if item["name"] == "whole_cohort_count")
        self.assertNotIn("value", row)
        self.assertEqual(5.0, row["expected"])

    def test_a_full_cohort_count_with_no_value_gives_a_true_flag_and_a_development_count_keeps_its_value(self) -> None:
        found = self.run_dump(self.tables())
        self.assertIs(True, found["sealed_values_empty"])
        self.assertEqual(1.0, next(item for item in found["counts"] if item["name"] == "n")["value"])

    def test_the_flag_looks_at_the_names_that_the_runner_sends_and_a_zero_value_is_a_value(self) -> None:
        self.assertIs(False, self.run_dump(self.tables(full=0.0))["sealed_values_empty"])
        self.assertIs(True, self.run_dump(self.tables(full=424242.5), sealed=())["sealed_values_empty"])

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
