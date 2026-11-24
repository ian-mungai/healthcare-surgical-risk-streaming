"""A runner saves a hash or a snapshot ID that a job reported only when Nessie confirms it.

The runners are untracked, so each test skips when its runner is not on this machine. No Docker or Spark runs here: the stack and Nessie are stand-ins.

Failure modes covered here:

- A runner finds that Nessie disagrees with a snapshot ID that a job reported (a well-formed ID, another number) and still saves the ID in the facts that go to
  the record, as if it were confirmed. A failed check records its observation and the facts stay as they were; the call returns nothing, so no later case runs
  on a result that was not confirmed.
- The same for a branch hash, a base hash, a checked hash and a write hash that are well-formed (64 hexadecimal characters) and are not the ones that the
  catalog holds, and for the source snapshot ID of the loaded table.
- A runner keeps a claimed hash after the history check of the commits fails.
"""

from __future__ import annotations

import importlib
import json
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from tests.e2e.run_record import Record
from tests.fast import test_outcome_acceptance as accept
from tests.fast.outcome_accept import CASES_SNAPSHOT_TABLES

E2E = Path(__file__).resolve().parents[1] / "e2e"
BASE, HEAD, OTHER, CHECKED = "a" * 64, "b" * 64, "c" * 64, "e" * 64
WRONG_HASH = "f" * 64
MERGED = "d" * 64


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


class FakeNessie:
    def __init__(self, heads: dict[str, str], snapshots: dict[str, int], entries: list[str], log: dict[str, Any] | None = None) -> None:
        self.heads, self.snapshots, self.table_names, self.log = heads, snapshots, entries, log or {}

    def reference(self, name: str) -> Any:
        return SimpleNamespace(hash=self.heads.get(name.split("@")[0], OTHER), kind="TAG" if "run" in name else "BRANCH")

    def entries(self, name: str) -> list[str]:
        return self.table_names

    def create_branch(self, name: str, base: str) -> None:
        return None

    def create_tag(self, name: str, hash_value: str, base: str) -> None:
        return None

    def merge(self, source: str, target: str, expected: str, message: str, source_hash: str | None = None) -> str:
        return MERGED

    def request(self, method: str, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        if path.endswith("/history"):
            return self.log
        table = path.rsplit("/contents/", 1)[-1]
        return {"content": {"snapshotId": self.snapshots[table]}} if table in self.snapshots else {}


def saved(facts: dict[str, Any]) -> str:
    return json.dumps(facts, sort_keys=True, default=str)


class BronzeFacts(unittest.TestCase):
    """``control_write`` of the Bronze clinical and laboratory runner."""

    SNAPSHOTS = {"raw_records.clinical_data": 7, "raw_records.lab_data": 7, "bronze.clinical_cases": 9, "bronze.lab_results": 9}

    def build(self, snapshots: dict[str, int] | None = None, branch_hash: str = HEAD, **changes: Any) -> tuple[Any, dict[str, Any]]:
        module = runner("bronze_clinical_lab")
        _, document = accept.bronze_run("write")
        document["branch_hash"], document["source_hash"] = branch_hash, BASE
        for table in document["tables"].values():
            table["source_snapshot_id"], table["bronze_snapshot_id"] = 7, 9
        document.update(changes)
        heads = {"main": BASE, "bronze_a": HEAD}
        nessie = FakeNessie(heads, {**self.SNAPSHOTS, **(snapshots or {})}, ["bronze.clinical_cases", "bronze.lab_results"])
        stack = SimpleNamespace(job=lambda command, branch, merged, **keywords: (0, document, 4.0))
        source = SimpleNamespace(scope="development", repeated_groups_as_published=True)
        run = module.Run(stack, nessie, source, Record(feature="test", slug="test", rerun="none"))
        run.suffix = "x"
        run.name = lambda base: "bronze_a"
        return run, document

    def test_a_write_that_nessie_confirms_saves_the_snapshot_ids_and_the_hashes(self) -> None:
        run, _ = self.build()
        result = run.control_write(BASE, BASE)
        self.assertTrue(result)
        self.assertEqual(
            {"clinical_source_snapshot_id": "7", "lab_source_snapshot_id": "7", "clinical_bronze_snapshot_id": "9", "lab_bronze_snapshot_id": "9"},
            {key: value for key, value in run.facts.items() if key.endswith("snapshot_id")},
        )
        self.assertEqual(
            {"merged_hash": BASE, "branch": "bronze_a", "branch_hash": HEAD}, {key: run.facts[key] for key in ("merged_hash", "branch", "branch_hash")}
        )

    def test_a_well_formed_snapshot_id_or_hash_that_nessie_does_not_hold_saves_nothing(self) -> None:
        plants = {
            "another Bronze snapshot ID for the clinical table": ({"bronze.clinical_cases": 10}, HEAD),
            "another Bronze snapshot ID for the laboratory table": ({"bronze.lab_results": 10}, HEAD),
            "another source snapshot ID for the clinical table": ({"raw_records.clinical_data": 8}, HEAD),
            "another source snapshot ID for the laboratory table": ({"raw_records.lab_data": 8}, HEAD),
            "another branch hash": ({}, WRONG_HASH),
        }
        for name, (snapshots, branch_hash) in plants.items():
            with self.subTest(name):
                run, _ = self.build(snapshots, branch_hash)
                result = run.control_write(BASE, BASE)
                self.assertEqual({}, result)
                self.assertEqual({}, run.facts, saved(run.facts))
                self.assertFalse(all(item.passed for item in run.record.observations))

    def test_a_merged_hash_that_main_does_not_hold_saves_nothing(self) -> None:
        run, _ = self.build()
        self.assertEqual({}, run.control_write(BASE, WRONG_HASH))
        self.assertEqual({}, run.facts, saved(run.facts))


class SplitFacts(unittest.TestCase):
    """``first_build`` of the patient split runner."""

    def history(self, checked: str = BASE) -> dict[str, Any]:
        top = {
            "parentCommitHash": CHECKED,
            "commitMeta": {"hash": HEAD},
            "operations": [{"type": "PUT", "key": {"elements": ["silver", "patient_split"]}}],
        }
        second = {"parentCommitHash": checked, "commitMeta": {"hash": CHECKED}, "operations": [{"type": "PUT", "key": {"elements": ["silver"]}}]}
        return {"logEntries": [top, second]}

    def build(self, snapshots: dict[str, int] | None = None, **changes: Any) -> tuple[Any, dict[str, Any]]:
        module = runner("silver_patient_split")
        _, document = accept.split_run("build")
        document.update({"base_hash": BASE, "checked_hash": BASE, "write_hash": HEAD, "branch_hash": HEAD, "checks_run": list(module.CHECKS_FULL)})
        document.update({"fixed_sample_cases_checked": 3, **changes})
        base_snapshots = {"silver.patient_split": document["snapshot_id"], "bronze.clinical_cases": document["source_snapshot_id"]}
        nessie = FakeNessie(
            {"main": BASE, "split_x": HEAD, "p6_trunk_x": BASE}, {**base_snapshots, **(snapshots or {})}, ["silver.patient_split"], self.history()
        )
        stack = SimpleNamespace(split=lambda mode, branch, base=None, **keywords: (0, document, "", 5.0), spark_containers=lambda: [])
        run = module.Run.__new__(module.Run)
        run.stack, run.nessie, run.record = stack, nessie, Record(feature="test", slug="test", rerun="none")
        run.suffix, run.trunk, run.made, run.facts = "x", "p6_trunk_x", [], {}
        run.candidates = [object(), object(), object()]
        run.name = lambda base: "split_x"
        return run, document

    def test_a_build_that_nessie_confirms_saves_the_hashes_and_the_snapshot_ids(self) -> None:
        run, document = self.build()
        result = run.first_build()
        self.assertTrue(result)
        self.assertEqual(
            {
                "base_hash": BASE,
                "branch_hash": HEAD,
                "checked_hash": BASE,
                "write_hash": HEAD,
                "table_snapshot_id": str(document["snapshot_id"]),
                "source_snapshot_id": str(document["source_snapshot_id"]),
            },
            {key: run.facts[key] for key in ("base_hash", "branch_hash", "checked_hash", "write_hash", "table_snapshot_id", "source_snapshot_id")},
        )

    def test_a_well_formed_snapshot_id_or_hash_that_nessie_does_not_hold_saves_nothing(self) -> None:
        document = accept.split_run("build")[1]
        plants: dict[str, tuple[dict[str, int], dict[str, Any]]] = {
            "another snapshot ID": ({"silver.patient_split": document["snapshot_id"] + 1}, {}),
            "another source snapshot ID": ({"bronze.clinical_cases": document["source_snapshot_id"] + 1}, {}),
            "another checked hash": ({}, {"checked_hash": WRONG_HASH}),
            "another write hash": ({}, {"write_hash": WRONG_HASH}),
            "another branch hash": ({}, {"branch_hash": WRONG_HASH}),
            "another base hash": ({}, {"base_hash": WRONG_HASH}),
        }
        for name, (snapshots, changes) in plants.items():
            with self.subTest(name):
                run, _ = self.build(snapshots, **changes)
                result = run.first_build()
                self.assertEqual({}, result)
                self.assertEqual({}, run.facts, saved(run.facts))
                self.assertFalse(all(item.passed for item in run.record.observations))

    def test_a_trunk_that_is_not_a_reference_name_saves_nothing(self) -> None:
        run, _ = self.build()
        run.trunk = "trunk"
        run.nessie.heads["trunk"] = BASE
        self.assertEqual({}, run.first_build())
        self.assertEqual({}, run.facts, saved(run.facts))

    def test_a_checked_hash_that_the_history_does_not_show_is_not_kept(self) -> None:
        run, _ = self.build()
        run.nessie.log = self.history(checked=WRONG_HASH)
        self.assertEqual({}, run.first_build())
        self.assertEqual({}, run.facts, saved(run.facts))


class MergeAndTagFacts(unittest.TestCase):
    """The merge hash and the tag are saved only when the check of the tag, or of the rebuild after the merge, passed."""

    def test_the_tag_of_the_cases_runner_is_saved_only_when_it_points_at_the_merged_hash(self) -> None:
        module = runner("silver_cases")
        tables = {table: 3 for table in CASES_SNAPSHOT_TABLES}
        for tag_hash, saved_tag in ((MERGED, True), (WRONG_HASH, False)):
            with self.subTest(tag_hash=tag_hash[:2]):
                run = module.Run.__new__(module.Run)
                run.nessie = FakeNessie({"p6_full_run": tag_hash}, dict(tables), [])
                run.record, run.facts, run.trunk = Record(feature="test", slug="test", rerun="none"), {}, "trunk"
                run.name = lambda base: "p6_full_run"
                run.tag(MERGED, "full", tables)
                self.assertEqual(saved_tag, "full_tag_hash" in run.facts, saved(run.facts))
                self.assertEqual(saved_tag, any(key.startswith("full_snapshot_") for key in run.facts))

    def test_the_tag_of_the_signals_runner_is_saved_only_when_it_points_at_the_merged_hash(self) -> None:
        module = runner("silver_signals")
        for tag_hash, saved_tag in ((MERGED, True), (WRONG_HASH, False)):
            with self.subTest(tag_hash=tag_hash[:2]):
                nessie = FakeNessie({"run-sig-x": tag_hash, "again_x_1": BASE}, {}, ["silver.numeric_observations", "silver.waveform_seconds"])
                run = module.Run(SimpleNamespace(), nessie, Record(feature="test", slug="test", rerun="none"), {})
                run.suffix = "x"
                run.silver = lambda mode, branch, base=None: SimpleNamespace(
                    keep=lambda: SimpleNamespace(ok=False, result={}, reason="the exit code is not the expected one"), code=1, seconds=1.0
                )
                run.merge_tag_rebuild({"branch": "b"})
                self.assertEqual(saved_tag, "tag_hash" in run.facts, saved(run.facts))
                self.assertEqual(saved_tag, "merged_hash" in run.facts)

    def test_the_merged_hash_of_the_split_runner_is_saved_only_when_the_rebuild_after_it_passed(self) -> None:
        module = runner("silver_patient_split")
        document = accept.split_run("build")[1]
        for rebuilt_hash, saved_hash in ((MERGED, True), (WRONG_HASH, False)):
            with self.subTest(rebuilt_hash=rebuilt_hash[:2]):
                kept = {**document, "base_hash": rebuilt_hash, "write_hash": None, "branch_hash": rebuilt_hash, "checked_hash": MERGED}
                nessie = FakeNessie({"split_x": MERGED, "p6_trunk_x": MERGED}, {}, [])
                run = module.Run.__new__(module.Run)
                run.stack, run.nessie = SimpleNamespace(split=lambda mode, branch, base=None, answer=kept, **keywords: (0, answer, "", 5.0)), nessie
                run.record, run.facts, run.trunk, run.made = Record(feature="test", slug="test", rerun="none"), {}, "p6_trunk_x", []
                run.suffix = "x"
                run.name = lambda base: "split_x"
                run.merge_and_rebuild({"branch": "b", "snapshot_id": document["snapshot_id"]})
                self.assertEqual(saved_hash, "merged_hash" in run.facts, saved(run.facts))


if __name__ == "__main__":
    unittest.main()
