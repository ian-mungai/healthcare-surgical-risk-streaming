"""What the outcome runners record after a comparison with Nessie fails or when a stand-in answers a value that is not a flag, tested on the runners' own code.

The runners stay untracked, so each test skips when its runner is not on this machine. Docker, Nessie and Spark are stand-ins.

Failure modes covered here (fix round 4, owner decision D):

- The Silver cases runner saves the snapshot IDs of a verify result at the tag of a merge (full and sample) without comparing all seven with Nessie at the tag:
  a wrong laboratory snapshot ID passes and is saved.
- The Silver signals and hypotension runners read the snapshot IDs from Nessie a second time after the accepted call. A read that fails (a timeout) is a
  ``None``. The runner still prints the reported ID, caches ``None`` as a fact and returns the result for the next case. It records the flag of the comparison,
  saves nothing and returns nothing.
- The Silver cases and patient split runners print the value that a stand-in answered for a flag field (a count, text) instead of whether the value is ``True``.
- A runner prints or saves a raw value that a stand-in or a result holds, whatever the field is.
"""

from __future__ import annotations

import contextlib
import functools
import importlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.sample.candidates import CANDIDATES_FILE, read_candidates
from tests.e2e import run_record
from tests.e2e.run_record import Record
from tests.fast import test_silver_acceptance as accept
from tests.fast.outcome_accept import CASES_SNAPSHOT_TABLES
from tests.fast.sealed_record import Sealed, sealed_observed
from tests.fast.silver_accept import Catalog
from tests.fast.test_runner_catalog_facts import FakeNessie as TaggedNessie
from tests.fast.test_silver_runner_records import FakeNessie, FakeStack, bind_build_pair, done_for

E2E = Path(__file__).resolve().parents[1] / "e2e"
MERGED = "d" * 64
MARKERS = ("60917", "77123", "SECRET_4417", "4417")


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def texts(run: Any) -> str:
    return json.dumps([item.observed for item in run.record.observations] + [run.facts], sort_keys=True, default=str)


class CasesTag(unittest.TestCase):
    """``tag`` of the Silver cases runner compares all seven snapshot IDs with Nessie at the tag before it saves them, for both labels."""

    def run_of(self, held: dict[str, int], label: str) -> Any:
        module = runner("silver_cases")
        run = module.Run.__new__(module.Run)
        run.nessie = TaggedNessie({f"p6_{label}_run": MERGED}, held, [])
        run.record, run.facts, run.trunk, run.sample_trunk = Record(feature="test", slug="test", rerun="none"), {}, "trunk", "sample_trunk"
        run.name = lambda base: f"p6_{label}_run"
        return run

    def test_seven_ids_that_nessie_holds_at_the_tag_are_saved_for_the_full_and_the_sample_tag(self) -> None:
        held = {table: 3 for table in CASES_SNAPSHOT_TABLES}
        for label in ("full", "sample"):
            with self.subTest(label):
                run = self.run_of(held, label)
                run.tag(MERGED, label, dict(held))
                self.assertTrue(all(item.passed for item in run.record.observations), texts(run))
                self.assertEqual(f"{label}_tag" in run.facts, True, texts(run))
                self.assertEqual(MERGED, run.facts[f"{label}_tag_hash"])
                self.assertEqual(7, sum(1 for key in run.facts if key.startswith(f"{label}_snapshot_")), texts(run))

    def test_a_wrong_snapshot_id_for_any_one_of_the_seven_tables_saves_nothing_and_fails_the_case(self) -> None:
        held = {table: 3 for table in CASES_SNAPSHOT_TABLES}
        for label in ("full", "sample"):
            for table in CASES_SNAPSHOT_TABLES:
                with self.subTest(label=label, table=table):
                    run = self.run_of(held, label)
                    run.tag(MERGED, label, {**held, table: 60917})
                    self.assertEqual({}, run.facts, texts(run))
                    self.assertFalse(all(item.passed for item in run.record.observations))
                    self.assertNotIn("60917", texts(run))

    def test_a_table_that_the_verify_result_leaves_out_or_nessie_does_not_hold_saves_nothing(self) -> None:
        held = {table: 3 for table in CASES_SNAPSHOT_TABLES}
        first = CASES_SNAPSHOT_TABLES[0]
        for name, reported, nessie_holds in (
            ("a table left out", {key: value for key, value in held.items() if key != first}, held),
            ("an eighth table", {**held, "silver.other": 5}, held),
            ("a null", {**held, first: None}, held),
            ("a Boolean", {**held, first: True}, held),
            ("a table that Nessie does not hold", dict(held), {key: value for key, value in held.items() if key != first}),
        ):
            with self.subTest(name):
                run = self.run_of(nessie_holds, "full")
                run.tag(MERGED, "full", reported)
                self.assertEqual({}, run.facts, texts(run))


class SecondReadFails(unittest.TestCase):
    """The control of the signals and hypotension runners after a failed second read of the snapshot IDs."""

    SNAPSHOTS = {"numeric_observations": 60917, "waveform_seconds": 77123}

    def test_the_signals_control_records_the_flag_saves_nothing_and_returns_nothing(self) -> None:
        module = runner("silver_signals")
        catalog = Catalog("b", accept.HEAD, accept.HEAD, accept.WRITE_HASH, accept.SOURCES, dict(self.SNAPSHOTS))
        document = accept.written(snapshots=dict(self.SNAPSHOTS))
        run = module.Run(FakeStack(module, {}), FakeNessie(), Record(feature="test", slug="test", rerun="none"), {})
        run.silver = lambda mode, branch, base=None: done_for(module, document, catalog)
        run.table_snapshot = lambda branch, table: None
        self.assertEqual({}, run.control())
        self.assertEqual({}, run.facts, texts(run))
        for marker in MARKERS:
            self.assertNotIn(marker, texts(run))
        self.assertNotIn("None", texts(run))
        self.assertFalse(all(item.passed for item in run.record.observations))

    def test_the_hypotension_control_records_the_flag_saves_nothing_and_returns_nothing(self) -> None:
        module = runner("silver_hypotension")
        snapshots = {"art_mbp_1s": 60917, "hypotension_events": 77123}
        catalog = Catalog("b", accept.HEAD, accept.HEAD, accept.WRITE_HASH, accept.HYPO_SOURCES, dict(snapshots))
        document = accept.hypotension(snapshots=dict(snapshots))
        run = module.Run(FakeStack(module, {}), FakeNessie(), Record(feature="test", slug="test", rerun="none"), {}, [])
        bind_build_pair(run, module, document, catalog)
        run.table_snapshot = lambda branch, table: None
        self.assertEqual({}, run.control())
        self.assertEqual({}, run.facts, texts(run))
        for marker in MARKERS:
            self.assertNotIn(marker, texts(run))
        self.assertNotIn("None", texts(run))
        self.assertFalse(all(item.passed for item in run.record.observations))

    def test_a_second_read_that_agrees_saves_the_confirmed_ids_of_the_tables_and_the_hashes(self) -> None:
        module = runner("silver_signals")
        catalog = Catalog("b", accept.HEAD, accept.HEAD, accept.WRITE_HASH, accept.SOURCES, dict(self.SNAPSHOTS))
        document = accept.written(snapshots=dict(self.SNAPSHOTS))
        held = {f"silver.{name}": value for name, value in self.SNAPSHOTS.items()} | {f"bronze.{name}": value for name, value in accept.SOURCES.items()}
        run = module.Run(FakeStack(module, {}), FakeNessie(held), Record(feature="test", slug="test", rerun="none"), {})
        run.silver = lambda mode, branch, base=None: done_for(module, document, catalog)
        run.extra_key_fails = lambda done: None
        run.name = lambda base: "signals_x_1"
        run.nessie.heads.update({run.trunk: accept.HEAD, "signals_x_1": accept.WRITE_HASH})
        self.assertNotEqual({}, run.control())
        self.assertEqual("60917", run.facts["snapshot_numeric_observations"], texts(run))
        self.assertEqual("77123", run.facts["snapshot_waveform_seconds"])
        self.assertEqual(accept.WRITE_HASH, run.facts["branch_hash"])
        self.assertEqual({"numeric_observations": 60917, "waveform_seconds": 77123}, run.built_snapshots)


class StandInFlagFields(unittest.TestCase):
    """A stand-in that answers a value that is not ``True`` in a flag field: the observation holds whether it is ``True`` and never the value."""

    def test_the_cases_independent_read_records_true_or_false_for_each_expected_flag(self) -> None:
        module = runner("silver_cases")
        flags_case = (
            "some_case_sampled",
            "every_sampled_case_found",
            "no_cell_wrong",
            "sealed_outcome_equal",
            "one_row_for_each_case",
            "no_null_subjectid",
            "every_case_has_a_split_row",
            "never_feature_property_equal",
            "columns_equal",
            "types_equal",
        )
        answer = {"cases": dict.fromkeys(flags_case, "SECRET_4417"), "lab": {"rows_as_expected": 4417, "types_equal": "SECRET_4417"}}
        run = module.Run.__new__(module.Run)
        run.record, run.facts = Record(feature="test", slug="test", rerun="none"), {}
        run.known = SimpleNamespace(lab_development_collapsed=1, lab_development_rows=2)
        run.stack = SimpleNamespace(write_spec=lambda spec: None, stand_in=lambda mode, branch, **keywords: (0, answer, "", 3.0))
        run.independent_read({"branch": "b"})
        self.assertEqual(2, len(run.record.observations))
        self.assertFalse(any(item.passed for item in run.record.observations))
        for item in run.record.observations:
            self.assertIn("types equal False", item.observed)
            self.assertNotIn("True", item.observed)
        for marker in MARKERS:
            self.assertNotIn(marker, texts(run))

    def test_the_split_independent_read_records_true_or_false_for_each_expected_flag(self) -> None:
        module = runner("silver_patient_split")
        flags = ("patients_as_expected", "counts_as_expected", "none_in_two_sets", "none_off_rule", "development_in_place", "test_in_place")
        answer = dict.fromkeys(flags, "SECRET_4417")
        run = module.Run.__new__(module.Run)
        run.record, run.facts, run.candidates, run.expected_sets = Record(feature="test", slug="test", rerun="none"), {}, [], {}
        run.stack = SimpleNamespace(write_spec=lambda spec: None, stand_in=lambda mode, branch, **keywords: (0, answer, "", 3.0))
        run.independent_read({"branch": "b"})
        self.assertEqual(1, len(run.record.observations))
        self.assertFalse(run.record.observations[0].passed)
        self.assertEqual(6, run.record.observations[0].observed.count("False"))
        self.assertNotIn("True", run.record.observations[0].observed)
        for marker in MARKERS:
            self.assertNotIn(marker, texts(run))


class StackLines(unittest.TestCase):
    """The log lines and the commands that the runners' own command code writes are the helper's text, with process answers that carry values."""

    SILVER = ("silver_patient_split", "silver_cases", "silver_signals", "silver_hypotension")

    def stack_of(self, module: ModuleType) -> Any:
        record, log = Record(feature="test", slug="test", rerun="none"), list[str]()
        if module.__name__.endswith("bronze_clinical_lab"):
            stack = module.Stack(Path("compose"), record, log)
        else:
            stack = module.Stack(Path("compose"), "proj_x1", Path("data"), record, log)
        stack.run = lambda *arguments, **keywords: (3, "SECRET_4417 out", "SECRET_4417 err")
        return stack

    def test_the_docker_and_compose_lines_hold_the_command_words_and_the_exit_code_only(self) -> None:
        for name in self.SILVER:
            with self.subTest(name):
                stack = self.stack_of(runner(name))
                stack.docker("rm", "-f", "4417", "SECRET_4417 x")
                stack.compose_run("up", "--detach", "--wait", "SECRET_4417")
                self.assertEqual(["docker rm -f <other>, exit 3", "compose up --detach --wait, exit 3"], [str(item) for item in stack.log])
                self.assertEqual(["docker rm -f", "docker compose up --detach --wait"], [str(item) for item in stack.record.commands])
                self.assertNotIn("4417", json.dumps([stack.log, stack.record.commands]))

    def test_the_bronze_compose_line_holds_the_command_words_and_the_exit_code_only(self) -> None:
        stack = self.stack_of(runner("bronze_clinical_lab"))
        stack.compose_run("up", "--detach", "SECRET_4417 x")
        self.assertEqual(["compose up --detach <other>, exit 3"], [str(item) for item in stack.log])
        self.assertEqual(["docker compose up --detach <other>"], [str(item) for item in stack.record.commands])

    def test_the_removal_of_a_project_logs_the_words_and_the_exit_code_only(self) -> None:
        for name in self.SILVER:
            with self.subTest(name):
                module = runner(name)
                answers = iter(
                    [SimpleNamespace(returncode=0, stdout="SECRET_4417 4417\n", stderr="")] * 3 + [SimpleNamespace(returncode=5, stdout="", stderr="")] * 9
                )
                log: list[str] = []
                with tempfile.TemporaryDirectory(prefix="sealed_runner_") as folder, mock.patch.object(module, "run_command", side_effect=answers):
                    module.remove_project(module.Stack(Path(folder), "proj_x1", Path(folder), SimpleNamespace(commands=[]), log), log)
                self.assertTrue(log, name)
                for item in log:
                    self.assertNotIn("4417", str(item))
                    self.assertIs(Sealed, type(item), str(item))


class SavedRecord(unittest.TestCase):
    """After the runner's own ``docker`` and ``compose_run`` wrappers, the record is saved and read back, with sealed text in every saved field."""

    ALL = ("silver_patient_split", "silver_cases", "silver_signals", "silver_hypotension", "bronze_clinical_lab")

    def save(self, record: Record, log: list[str]) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="sealed_runner_") as root, mock.patch.object(run_record, "ROOT", Path(root)):
            with contextlib.redirect_stdout(io.StringIO()):
                record.close(log)
            (folder,) = list((Path(root) / "runs").iterdir())
            self.assertTrue((folder / "retention.json").is_file())
            self.assertEqual(len(log) > 0, bool((folder / "log.txt").read_text().strip()))
            document: dict[str, Any] = json.loads((folder / "record.json").read_text())
            return document

    def test_each_runner_saves_its_record_after_its_own_command_wrappers_with_sealed_text_in_every_field(self) -> None:
        for name in self.ALL:
            with self.subTest(name):
                module = runner(name)
                record, log = Record(feature="test", slug="test", rerun="none"), list[str]()
                if name == "bronze_clinical_lab":
                    stack = module.Stack(Path("compose"), record, log)
                else:
                    stack = module.Stack(Path("compose"), "proj_x1", Path("data"), record, log)
                stack.run = lambda *arguments, **keywords: (3, "SECRET_4417 out", "SECRET_4417 err")
                if name != "bronze_clinical_lab":
                    stack.docker("rm", "-f", "4417", "SECRET_4417 x")
                stack.compose_run("up", "--detach", "--rows=4417")
                record.limits.append(sealed_observed({"stalled": False}))
                record.environment["flag"] = sealed_observed({"x": True})
                record.cleanup = sealed_observed({"scratch folder removed": True})
                record.expect("a case", "a step", sealed_observed({"expected flag": True}), sealed_observed({"x": True}, exit_code=3), False)
                document = self.save(record, log)
                self.assertTrue(all(type(item) is str for item in document["commands"]))
                self.assertIn("docker compose up --detach --rows=<other>", document["commands"])
                self.assertEqual(["stalled False"], document["limits"])
                self.assertEqual("scratch folder removed True", document["cleanup"])
                self.assertEqual("expected flag True", document["observations"][0]["expected"])
                self.assertNotIn("4417", json.dumps(document))

    def test_the_cleanup_of_each_runner_closes_with_a_sealed_cleanup_text(self) -> None:
        for name in self.ALL:
            with self.subTest(name):
                runner(name)
                record = Record(feature="test", slug="test", rerun="none")
                clean = functools.partial(setattr, record, "cleanup", sealed_observed({"scratch folder removed": True}))
                with tempfile.TemporaryDirectory(prefix="sealed_runner_") as root, mock.patch.object(run_record, "ROOT", Path(root)):
                    with contextlib.redirect_stdout(io.StringIO()):
                        record.close_after(clean, [])
                    (folder,) = list((Path(root) / "runs").iterdir())
                    self.assertEqual("scratch folder removed True", json.loads((folder / "record.json").read_text())["cleanup"])


class LimitsText(unittest.TestCase):
    """The limits that a runner saves say nothing that its record does not do."""

    def test_the_hypotension_limits_do_not_say_that_the_expected_event_list_is_saved(self) -> None:
        source = (E2E / "silver_hypotension.py").read_text() if (E2E / "silver_hypotension.py").is_file() else ""
        if not source:
            raise unittest.SkipTest("the untracked runners are not on this machine")
        self.assertNotIn("kept in the record", source)
        self.assertIn("never the list", source)


class DryRun(unittest.TestCase):
    """The dry run of the Silver cases runner passes its observations through the sealed helper with the answers of a dry stack: no error."""

    def test_the_dry_run_plans_the_reference_names_and_no_name_twice(self) -> None:
        module = runner("silver_cases")
        with mock.patch.object(module, "read_candidates", side_effect=AssertionError("the dry plan reads the list")):
            names = module.planned_reference_names(read_candidates(CANDIDATES_FILE))
        self.assertIsInstance(names, list)
        self.assertEqual(len(names), len(set(names)))

    def test_the_expected_facts_of_the_container_are_flags_whatever_the_container_answers(self) -> None:
        module = runner("silver_cases")
        answer = {
            "cases_as_expected": "SECRET_4417",
            "patients_as_expected": 4417,
            "counts_as_frozen": None,
            "lab_total_ok": "SECRET_4417",
            "development_cases": 5,
            "measure": [],
            "cutoffs": [],
            "lab_development_rows": 1,
            "lab_development_collapsed": 1,
        }
        stack = SimpleNamespace(write_spec=lambda spec: None, stand_in=lambda mode, branch: (0, answer, "", 1.0))
        expected = module.Expected.from_container(stack, "b")
        self.assertEqual(
            (False, False, False, False),
            (expected.cases_as_expected, expected.patients_as_expected, expected.counts_as_frozen, expected.lab_total_is_published),
        )


if __name__ == "__main__":
    unittest.main()
