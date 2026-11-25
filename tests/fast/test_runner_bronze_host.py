"""The decisions of the phase 5 host runner (``tests/e2e/bronze_host.py``): what it refuses before it touches the host, what counts as a match with the
independent read, what counts as a passed control or a held planted fault, and what counts as left behind.

The runner stays untracked, so each test skips when it is not on this machine. No host, no Docker and no AWS access are used: every decision is a pure function.

Failure modes covered here:

- A reference file that is not a manifest reference, or a source hash that is not a Nessie hash, starts a run on the host with nothing to read.
- A manifest that did not pass, one with no case file or two clinical tables, or one that holds a case outside the development set, is read or decoded.
- The comparison with the independent read passes when a payload differs, when a key is on one side only, when a table is empty, or when a row count differs.
- A passing control is accepted although main moved, a table is missing, the published hash is not the checked hash or the snapshots differ.
- A planted fault is accepted as held although the guard that stopped it is not the named one, the named check did not fail, or the base or the trunk moved.
- A reference of the run, a second tag, or a changed listing of the landing and manifests prefixes is not found as left behind; a folder outside the
  run's own is removed.
"""

from __future__ import annotations

import importlib
import json
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

E2E = Path(__file__).resolve().parents[1] / "e2e"
HASH = "a" * 64
OTHER = "b" * 64
REFERENCE = json.dumps({"bucket": "b", "key": "manifests/x/" + HASH + ".json", "sha256": HASH, "version_id": "v1"})
TABLES = ("numeric_observations", "waveform_seconds", "tracks", "string_events")


def runner() -> ModuleType:
    if not (E2E / "bronze_host.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.bronze_host")


def dumps(**changes: str) -> dict[str, str]:
    base = {name: f'["k1"]\t{HASH}\n["k2"]\t{OTHER}\n' for name in TABLES}
    return {**base, **changes}


class Inputs(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()

    def test_a_reference_file_and_no_source_hash(self) -> None:
        reference, source = self.module.read_inputs(REFERENCE, None)
        self.assertEqual(("b", "v1"), (reference.bucket, reference.version_id))
        self.assertIsNone(source)

    def test_a_reference_that_is_not_json_is_refused_and_names_the_phase_4_step(self) -> None:
        with self.assertRaises(self.module.Refusal) as caught:
            self.module.read_inputs("not json", None)
        self.assertEqual("reference_unreadable", caught.exception.code)
        self.assertIn("phase 4", str(caught.exception))

    def test_a_reference_with_a_missing_field_is_refused(self) -> None:
        text = json.dumps({"bucket": "b", "key": "k", "sha256": HASH})
        with self.assertRaises(self.module.Refusal) as caught:
            self.module.read_inputs(text, None)
        self.assertEqual("reference_unreadable", caught.exception.code)

    def test_a_source_hash_that_is_not_a_nessie_hash_is_refused(self) -> None:
        for bad in ("main", "xyz", "", HASH[:-1] + "g"):
            with self.subTest(bad=bad), self.assertRaises(self.module.Refusal) as caught:
                self.module.read_inputs(REFERENCE, bad)
            self.assertEqual("source_hash_form", caught.exception.code)

    def test_a_source_hash_of_the_nessie_form_is_kept(self) -> None:
        self.assertEqual(HASH, self.module.read_inputs(REFERENCE, HASH)[1])


class Sample(unittest.TestCase):
    DEVELOPMENT = frozenset({908, 1147})
    KEYS = ["vitaldb/1.0.0/vital_files/0908.vital", "vitaldb/1.0.0/vital_files/1147.vital", "vitaldb/1.0.0/clinical_data.csv"]

    def setUp(self) -> None:
        self.module = runner()

    def test_a_passed_manifest_of_development_cases_is_accepted(self) -> None:
        self.assertIsNone(self.module.sample_problem("passed", self.KEYS, self.DEVELOPMENT))

    def test_a_manifest_that_did_not_pass_is_refused(self) -> None:
        self.assertEqual("manifest_not_passed", self.module.sample_problem("failed", self.KEYS, self.DEVELOPMENT))

    def test_a_manifest_with_no_case_file_is_refused(self) -> None:
        self.assertEqual("no_case_files", self.module.sample_problem("passed", ["vitaldb/1.0.0/clinical_data.csv"], self.DEVELOPMENT))

    def test_a_manifest_with_no_clinical_table_or_two_is_refused(self) -> None:
        self.assertEqual("clinical_table", self.module.sample_problem("passed", self.KEYS[:2], self.DEVELOPMENT))
        self.assertEqual("clinical_table", self.module.sample_problem("passed", [*self.KEYS, "other/clinical_data.csv"], self.DEVELOPMENT))

    def test_a_held_out_case_is_refused_before_any_read(self) -> None:
        keys = [*self.KEYS, "vitaldb/1.0.0/vital_files/0565.vital"]
        self.assertEqual("case_not_development", self.module.sample_problem("passed", keys, self.DEVELOPMENT))

    def test_a_case_that_the_candidates_do_not_list_is_refused(self) -> None:
        keys = [*self.KEYS, "vitaldb/1.0.0/vital_files/0777.vital"]
        self.assertEqual("case_not_development", self.module.sample_problem("passed", keys, self.DEVELOPMENT))

    def test_the_cases_are_the_numbers_of_the_case_files_in_order(self) -> None:
        self.assertEqual([908, 1147], self.module.case_numbers(list(reversed(self.KEYS))))


class Comparison(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()

    def test_equal_dumps_match(self) -> None:
        report = self.module.compare_tables(dumps(), dumps())
        self.assertTrue(self.module.tables_equal(report))

    def test_a_different_payload_is_found(self) -> None:
        other = dumps(numeric_observations=f'["k1"]\t{OTHER}\n["k2"]\t{OTHER}\n')
        report = self.module.compare_tables(dumps(), other)
        self.assertEqual(1, report["numeric_observations"]["different"])
        self.assertFalse(self.module.tables_equal(report))

    def test_a_key_on_one_side_only_is_found_on_that_side(self) -> None:
        short = dumps(tracks=f'["k1"]\t{HASH}\n')
        report = self.module.compare_tables(dumps(), short)
        self.assertEqual((1, 0), (report["tracks"]["only_left"], report["tracks"]["only_right"]))
        self.assertFalse(self.module.tables_equal(report))

    def test_a_key_that_is_in_the_other_dump_only_is_found_when_the_row_counts_are_equal(self) -> None:
        swapped = dumps(string_events=f'["k1"]\t{HASH}\n["k3"]\t{OTHER}\n')
        report = self.module.compare_tables(dumps(), swapped)
        self.assertEqual((1, 1), (report["string_events"]["only_left"], report["string_events"]["only_right"]))
        self.assertFalse(self.module.tables_equal(report))

    def test_an_empty_table_on_both_sides_is_not_a_match_but_is_identical(self) -> None:
        report = self.module.compare_tables(dumps(string_events=""), dumps(string_events=""))
        self.assertFalse(self.module.tables_equal(report))
        self.assertTrue(self.module.tables_identical(report))

    def test_a_missing_table_is_not_equal(self) -> None:
        half = {name: text for name, text in dumps().items() if name != "tracks"}
        self.assertFalse(self.module.tables_equal(self.module.compare_tables(dumps(), half)))

    def test_a_repeated_key_is_not_hidden_by_the_dictionary(self) -> None:
        doubled = dumps(numeric_observations=f'["k1"]\t{HASH}\n["k1"]\t{HASH}\n["k2"]\t{OTHER}\n')
        report = self.module.compare_tables(dumps(), doubled)
        self.assertEqual([2, 3], report["numeric_observations"]["rows"])
        self.assertFalse(self.module.tables_equal(report))


class Control(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()
        self.tables = {name: {"snapshot_id": 10 + number} for number, name in enumerate(TABLES)}
        self.snapshots = {f"bronze.{name}": facts["snapshot_id"] for name, facts in self.tables.items()}
        self.result: dict[str, Any] = {
            "passed": True,
            "checked_hash": "c" * 64,
            "tables": self.tables,
            "checks": {"checks": ["x"] * 3, "failed": []},
            "publication": {"status": "published", "published_hash": "c" * 64, "snapshots": self.snapshots},
        }

    def problem(self, **changes: object) -> str | None:
        held = {
            "code": 0,
            "result": self.result,
            "main_before": "m" * 64,
            "main_after": "m" * 64,
            "base_after": "c" * 64,
            "entries": [f"bronze.{n}" for n in TABLES],
            "plan_size": 3,
        }
        problem: str | None = self.module.control_problem(**{**held, **changes})
        return problem

    def test_a_published_control_passes(self) -> None:
        self.assertIsNone(self.problem())

    def test_main_that_moved_fails(self) -> None:
        self.assertEqual("main_moved", self.problem(main_after="n" * 64))

    def test_a_failed_exit_fails(self) -> None:
        self.assertEqual("exit_code", self.problem(code=1))

    def test_a_published_hash_that_is_not_the_checked_hash_fails(self) -> None:
        self.result["publication"]["published_hash"] = "d" * 64
        self.assertEqual("published_hash", self.problem())

    def test_a_base_that_is_not_at_the_checked_hash_fails(self) -> None:
        self.assertEqual("base_hash", self.problem(base_after="d" * 64))

    def test_a_missing_table_on_the_base_fails(self) -> None:
        self.assertEqual("tables_missing", self.problem(entries=["bronze.tracks"]))

    def test_other_snapshots_fail(self) -> None:
        self.result["publication"]["snapshots"] = {**self.snapshots, "bronze.tracks": 999}
        self.assertEqual("snapshots", self.problem())

    def test_a_failed_check_or_a_short_plan_fails(self) -> None:
        self.result["checks"]["failed"] = [{"table": "tracks", "name": "x"}]
        self.assertEqual("checks_failed", self.problem())
        self.result["checks"] = {"checks": ["x"], "failed": []}
        self.assertEqual("checks_count", self.problem())

    def test_a_status_other_than_published_fails(self) -> None:
        self.result["publication"]["status"] = "unchanged"
        self.assertEqual("status", self.problem())

    def test_a_result_that_did_not_pass_fails(self) -> None:
        self.result["passed"] = False
        self.assertEqual("not_passed", self.problem())


class Planted(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()
        self.found: dict[str, Any] = {
            "publication": {"status": "blocked", "blocked_by": ["key"]},
            "checks": {"failed": [{"table": "numeric_observations", "name": "case_id not null"}]},
        }

    def held(self, **changes: object) -> bool:
        asked = {
            "code": 1,
            "found": self.found,
            "guard": "key",
            "table": "numeric_observations",
            "check": "case_id not null",
            "base_unchanged": True,
            "trunk_unchanged": True,
        }
        return bool(self.module.planted_held(**{**asked, **changes}))

    def test_a_stopped_fault_with_everything_unchanged_is_held(self) -> None:
        self.assertTrue(self.held())

    def test_an_exit_of_zero_is_not_held(self) -> None:
        self.assertFalse(self.held(code=0))

    def test_another_guard_is_not_held(self) -> None:
        self.assertFalse(self.held(guard="structure"))

    def test_a_check_that_did_not_fail_is_not_held(self) -> None:
        self.assertFalse(self.held(check="key unique"))

    def test_a_moved_base_or_trunk_is_not_held(self) -> None:
        self.assertFalse(self.held(base_unchanged=False))
        self.assertFalse(self.held(trunk_unchanged=False))

    def test_a_published_fault_is_not_held(self) -> None:
        self.found["publication"]["status"] = "published"
        self.assertFalse(self.held())


class Leftovers(unittest.TestCase):
    def setUp(self) -> None:
        self.module = runner()

    def test_only_the_run_tag_may_stay(self) -> None:
        names = ["main", "other_branch", "p5h_ab12cd_trunk", "run-p5h_ab12cd"]
        self.assertEqual(["p5h_ab12cd_trunk"], self.module.left_behind(names, "p5h_ab12cd", "run-p5h_ab12cd"))

    def test_nothing_left_when_only_the_tag_and_foreign_references_remain(self) -> None:
        self.assertEqual([], self.module.left_behind(["main", "run-p5h_ab12cd", "p5h_ffffff_x"], "p5h_ab12cd", "run-p5h_ab12cd"))

    def test_a_second_tag_of_the_run_is_found(self) -> None:
        self.assertEqual(["run-p5h_ab12cd-2"], self.module.left_behind(["run-p5h_ab12cd", "run-p5h_ab12cd-2"], "p5h_ab12cd", "run-p5h_ab12cd"))

    def test_a_missing_tag_is_found(self) -> None:
        self.assertEqual(["run-p5h_ab12cd"], self.module.left_behind(["main"], "p5h_ab12cd", "run-p5h_ab12cd", tag_required=True))

    def test_the_listing_digest_ignores_order_and_sees_a_new_version(self) -> None:
        first = [("landing/a", "v1"), ("manifests/m", "v2")]
        self.assertEqual(self.module.listing_digest(first), self.module.listing_digest(list(reversed(first))))
        self.assertNotEqual(self.module.listing_digest(first), self.module.listing_digest([*first, ("landing/a", "v3")]))
        self.assertNotEqual(self.module.listing_digest(first), self.module.listing_digest([("landing/a", "v1")]))

    def test_the_listing_digest_sees_a_changed_version_of_the_same_key(self) -> None:
        self.assertNotEqual(self.module.listing_digest([("landing/a", "v1")]), self.module.listing_digest([("landing/a", "v2")]))

    def test_only_a_folder_of_the_run_may_be_removed(self) -> None:
        suffix = "ab12cd"
        self.assertTrue(self.module.removable_folder(f"/opt/platform/p5h_{suffix}", suffix))
        for path in (
            "/opt/platform",
            "/opt/platform/p5h_ffffff",
            f"/opt/platform/p5h_{suffix}/../x",
            f"/opt/platform/p5h_{suffix}/out",
            "/",
            "",
            f"/opt/platform/p5h_{suffix} x",
        ):
            with self.subTest(path=path):
                self.assertFalse(self.module.removable_folder(path, suffix))


if __name__ == "__main__":
    unittest.main()
