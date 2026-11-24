"""The pass or fail rules that the end-to-end runs of the Bronze clinical, patient split and Silver cases jobs share, tested on plain results.

No Spark runs here. The three runners stay untracked, so each rule that decides from a job's output lives in ``tests/fast/outcome_accept.py``, where this module
tests it. The rules read hashes, snapshot IDs and flags and never a count, a digest or a field that says whether a table was written.

Failure modes covered here:

- A clinical table that holds a count, a digest or a check detail passes as sealed.
- A clinical table with a failed value, row-count or key check passes as equal to the loaded table.
- A build that moved the branch passes as a rebuild that wrote nothing, or the reverse.
- A result that holds a removed field passes, or one pass flag false passes.
- A refusal passes with a count or a name after its code.
"""

from __future__ import annotations

import copy
import unittest
from typing import Any

from tests.fast.outcome_accept import (
    all_passed,
    equals_loaded,
    kept_at_base,
    moved,
    no_removed_fields,
    refusal_is_flags,
    same_source,
    sealed_table,
    wrote,
)

CHECKS = ("source_changed", "bronze table exists", "columns", "key not null", "row count", "no value changed", "key unique")
HASH_A, HASH_B = "a" * 64, "b" * 64


def clinical(source: int = 5, bronze: int = 9) -> dict[str, Any]:
    return {
        "source_table": "raw_records.clinical_data",
        "bronze_table": "bronze.clinical_cases",
        "source_metadata_location": "file:/w/m.json",
        "source_snapshot_id": source,
        "bronze_snapshot_id": bronze,
        "passed": True,
        "checks": [{"name": name, "passed": True} for name in CHECKS],
    }


def fail(table: dict[str, Any], name: str) -> dict[str, Any]:
    changed = copy.deepcopy(table)
    for check in changed["checks"]:
        if check["name"] == name:
            check["passed"] = False
    changed["passed"] = False
    return changed


class Sealed(unittest.TestCase):
    def test_a_table_with_snapshot_ids_a_flag_and_checks_that_hold_a_name_and_a_flag_is_sealed(self) -> None:
        self.assertTrue(sealed_table(clinical()))

    def test_a_count_a_digest_or_a_check_detail_is_not_sealed(self) -> None:
        for key in ("rows", "source_rows", "keys_digest", "rows_digest"):
            self.assertFalse(sealed_table({**clinical(), key: 1}), key)
        withdetail = clinical()
        withdetail["checks"][0]["detail"] = "loaded 6, bronze 6"
        self.assertFalse(sealed_table(withdetail))
        self.assertFalse(sealed_table({**clinical(), "checks": []}))


class EqualsLoaded(unittest.TestCase):
    def test_a_passed_table_with_both_snapshot_ids_equals_the_loaded_table(self) -> None:
        self.assertTrue(equals_loaded(clinical()))

    def test_each_of_the_four_checks_that_prove_equality_is_needed(self) -> None:
        for name in ("row count", "no value changed", "key unique", "key not null"):
            self.assertFalse(equals_loaded(fail(clinical(), name)), name)

    def test_a_check_that_is_missing_or_a_snapshot_that_is_missing_is_not_equality(self) -> None:
        missing = clinical()
        missing["checks"] = [check for check in missing["checks"] if check["name"] != "no value changed"]
        self.assertFalse(equals_loaded(missing))
        self.assertFalse(equals_loaded({**clinical(), "bronze_snapshot_id": None}))
        self.assertFalse(equals_loaded({**clinical(), "source_snapshot_id": True}))
        self.assertFalse(equals_loaded({**clinical(), "passed": False}))
        self.assertFalse(equals_loaded({}))

    def test_a_table_that_holds_a_count_is_not_accepted_even_when_it_passed(self) -> None:
        self.assertFalse(equals_loaded({**clinical(), "rows": 6}))


class Moved(unittest.TestCase):
    def test_a_moved_source_needs_both_tables_equal_and_both_snapshot_ids_new(self) -> None:
        self.assertTrue(moved(clinical(5, 9), clinical(6, 10)))
        self.assertFalse(moved(clinical(5, 9), clinical(5, 10)))
        self.assertFalse(moved(clinical(5, 9), clinical(6, 9)))
        self.assertFalse(moved(clinical(5, 9), fail(clinical(6, 10), "no value changed")))

    def test_the_same_source_needs_the_same_source_snapshot_and_both_tables_equal(self) -> None:
        self.assertTrue(same_source(clinical(5, 9), clinical(5, 12)))
        self.assertFalse(same_source(clinical(5, 9), clinical(6, 12)))
        self.assertFalse(same_source(clinical(5, 9), fail(clinical(5, 12), "row count")))


class Hashes(unittest.TestCase):
    def test_a_write_moves_the_branch_off_the_base_and_a_rebuild_that_changed_nothing_leaves_it(self) -> None:
        moved_branch = {"base_hash": HASH_A, "branch_hash": HASH_B}
        quiet = {"base_hash": HASH_A, "branch_hash": HASH_A}
        self.assertTrue(wrote(moved_branch))
        self.assertFalse(wrote(quiet))
        self.assertTrue(kept_at_base(quiet))
        self.assertFalse(kept_at_base(moved_branch))

    def test_a_hash_that_is_missing_or_not_a_string_is_neither(self) -> None:
        for result in ({}, {"base_hash": None, "branch_hash": None}, {"base_hash": "", "branch_hash": ""}, {"base_hash": 1, "branch_hash": 1}):
            self.assertFalse(wrote(result), result)
            self.assertFalse(kept_at_base(result), result)


class Flags(unittest.TestCase):
    def test_every_named_table_needs_its_flag_true_and_no_other_table_is_allowed(self) -> None:
        self.assertTrue(all_passed({"passed": {"cases": True, "lab": True, "rules": True}}, ("cases", "lab", "rules")))
        self.assertFalse(all_passed({"passed": {"cases": True, "lab": False, "rules": True}}, ("cases", "lab", "rules")))
        self.assertFalse(all_passed({"passed": {"cases": True, "lab": True}}, ("cases", "lab", "rules")))
        self.assertFalse(all_passed({"passed": {"cases": True, "lab": True, "rules": True, "x": True}}, ("cases", "lab", "rules")))
        self.assertFalse(all_passed({"passed": {"cases": 1, "lab": True, "rules": True}}, ("cases", "lab", "rules")))
        self.assertFalse(all_passed({}, ("cases",)))

    def test_a_result_with_a_removed_field_is_refused(self) -> None:
        self.assertTrue(no_removed_fields({"passed": {"cases": True}, "checks_run": [], "development_cases": 5}))
        for key in ("outcome", "written", "cases", "patients", "counts"):
            self.assertFalse(no_removed_fields({key: 1}), key)
        self.assertFalse(no_removed_fields({"measure": [{"rows": 1}]}))


class Refusals(unittest.TestCase):
    def test_a_refusal_is_a_list_of_the_wanted_flags_in_order_with_nothing_else(self) -> None:
        self.assertTrue(refusal_is_flags(["key_null: failed", "split_join: failed"], ["key_null", "split_join"]))
        self.assertFalse(refusal_is_flags(["split_join: failed", "key_null: failed"], ["key_null", "split_join"]))
        self.assertFalse(refusal_is_flags(["key_null: 3 rows"], ["key_null"]))
        self.assertFalse(refusal_is_flags(["key_null: failed", "extra: failed"], ["key_null"]))
        self.assertFalse(refusal_is_flags(["key_null: failed extra"], ["key_null"]))
        self.assertFalse(refusal_is_flags([], ["key_null"]))

    def test_a_count_of_tables_is_the_one_count_a_refusal_may_hold(self) -> None:
        self.assertTrue(refusal_is_flags(["output_schema: 2 tables"], ["output_schema: 2 tables"]))
        self.assertFalse(refusal_is_flags(["output_schema: 2 tables"], ["output_schema"]))


if __name__ == "__main__":
    unittest.main()
