"""The acceptance rules of the Silver labels end-to-end run (``tests/fast/label_accept.py``) accept only the result they describe. No Docker runs here.

The runner ``tests/e2e/silver_labels.py`` stays untracked, so each rule that decides pass or fail from the job's result is here, where it has a test.

Failure modes covered here:

- A write is read from a field that a held-out row changes (``outcome``) and not from the hashes and the snapshot IDs: an empty result, a result whose write
  hash is not the branch hash or whose branch did not move, or a result that still holds ``outcome``, passes.
- A result that wrote nothing is accepted when it names no write hash by missing the key, or when its branch hash is not the checked one.
- A first build passes with a pass flag false or missing, a snapshot ID missing, null or not a whole number, a check missing or in another order, or a key that
  the sealed set does not hold.
- A change of the held-out patients' ``icu_days`` passes when the measure or the kidney labels got a new snapshot, when the intensive care labels kept theirs,
  or when the build wrote more than the intensive care labels.
- A rebuild after the merge passes when it wrote, when the branch is not the checked hash or when a snapshot ID differs from the build's.
"""

from __future__ import annotations

import unittest
from typing import Any

from spark.silver.label_checks import CHECK_NAMES, TABLES
from tests.fast.label_accept import SEALED_KEYS, build_ok, icu_change_ok, kept, rebuild_ok, sealed_keys_only, verify_ok, wrote

HEAD, WRITE = "a" * 64, "b" * 64
MEASURE, ICU, KIDNEY = TABLES
SNAPSHOTS = {MEASURE: 11, ICU: 12, KIDNEY: 13}
PASSED = {table: True for table in TABLES}
CALL = {"mode": "build", "branch": "x", "base_hash": HEAD, "job": "silver_labels", "tables": ["silver.icu_days_measure"]}


def written(**changes: Any) -> dict[str, Any]:
    return {
        **CALL,
        "checked_hash": HEAD,
        "write_hash": WRITE,
        "branch_hash": WRITE,
        "source_snapshots": {"cases": 1, "lab_results": 2, "patient_split": 3},
        "snapshots": SNAPSHOTS,
        "passed": PASSED,
        "icu_measure": [],
        "checks_run": list(CHECK_NAMES),
        **changes,
    }


def unwritten(**changes: Any) -> dict[str, Any]:
    return written(**{"write_hash": None, "branch_hash": HEAD, **changes})


def without(result: dict[str, Any], key: str) -> dict[str, Any]:
    return {name: value for name, value in result.items() if name != key}


class Writes(unittest.TestCase):
    def test_a_result_that_names_a_new_write_hash_as_the_branch_hash_is_a_write(self) -> None:
        self.assertTrue(wrote(written()))
        self.assertFalse(kept(written()))

    def test_a_write_needs_each_hash(self) -> None:
        for change in (
            {"write_hash": None},
            {"branch_hash": HEAD},
            {"write_hash": HEAD, "branch_hash": HEAD},
            {"checked_hash": WRITE},
            {"checked_hash": None},
            {"write_hash": ""},
        ):
            with self.subTest(change=change):
                self.assertFalse(wrote(written(**change)))
        self.assertFalse(wrote({}))
        self.assertFalse(wrote({"outcome": "written"}))

    def test_a_result_that_names_no_write_and_stays_on_the_checked_hash_is_kept(self) -> None:
        self.assertTrue(kept(unwritten()))
        self.assertFalse(wrote(unwritten()))

    def test_a_kept_result_needs_each_hash(self) -> None:
        for change in ({"write_hash": WRITE}, {"branch_hash": WRITE}, {"checked_hash": None, "branch_hash": None}):
            with self.subTest(change=change):
                self.assertFalse(kept(unwritten(**change)))
        self.assertFalse(kept({}))
        self.assertFalse(kept({"outcome": "unchanged"}))
        self.assertFalse(kept(without(unwritten(), "write_hash")))


class Sealed(unittest.TestCase):
    def test_the_sealed_keys_are_the_ones_of_the_result_and_never_an_outcome(self) -> None:
        self.assertEqual(
            {"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "icu_measure", "checks_run"}, set(SEALED_KEYS)
        )
        self.assertNotIn("outcome", SEALED_KEYS)

    def test_a_result_with_only_the_sealed_keys_and_the_call_keys_passes(self) -> None:
        self.assertTrue(sealed_keys_only(written()))
        self.assertTrue(sealed_keys_only({}))

    def test_any_other_key_fails(self) -> None:
        for key in ("outcome", "rows", "fingerprints", "counts", "patients"):
            with self.subTest(key=key):
                self.assertFalse(sealed_keys_only(written(**{key: 1})))


class FirstBuild(unittest.TestCase):
    def test_a_build_that_wrote_with_every_flag_snapshot_and_check_passes(self) -> None:
        self.assertTrue(build_ok(written()))

    def test_each_part_is_needed(self) -> None:
        for name, result in (
            ("nothing written", unwritten()),
            ("an outcome", written(outcome="written")),
            ("a flag false", written(passed={**PASSED, ICU: False})),
            ("a flag missing", written(passed={MEASURE: True, ICU: True})),
            ("no flags", without(written(), "passed")),
            ("a flag that is not a boolean", written(passed={**PASSED, ICU: 1})),
            ("a snapshot missing", written(snapshots={MEASURE: 11, ICU: 12})),
            ("a snapshot null", written(snapshots={**SNAPSHOTS, KIDNEY: None})),
            ("a snapshot that is a text", written(snapshots={**SNAPSHOTS, KIDNEY: "13"})),
            ("a snapshot that is a boolean", written(snapshots={**SNAPSHOTS, KIDNEY: True})),
            ("an extra snapshot", written(snapshots={**SNAPSHOTS, "other": 1})),
            ("no snapshots", without(written(), "snapshots")),
            ("a check missing", written(checks_run=list(CHECK_NAMES)[:-1])),
            ("checks in another order", written(checks_run=list(reversed(CHECK_NAMES)))),
            ("no checks", without(written(), "checks_run")),
            ("a key that is not sealed", written(rows=3)),
        ):
            with self.subTest(name):
                self.assertFalse(build_ok(result))


class Verify(unittest.TestCase):
    def test_a_verify_that_wrote_nothing_and_stayed_on_the_hash_passes(self) -> None:
        self.assertTrue(verify_ok(unwritten(), HEAD))

    def test_each_part_is_needed(self) -> None:
        for name, result, before in (
            ("a write", written(), HEAD),
            ("another hash than before", unwritten(), WRITE),
            ("a flag false", unwritten(passed={**PASSED, MEASURE: False}), HEAD),
            ("an outcome", unwritten(outcome="verified"), HEAD),
            ("a check missing", unwritten(checks_run=list(CHECK_NAMES)[1:]), HEAD),
        ):
            with self.subTest(name):
                self.assertFalse(verify_ok(result, before))


class Rebuild(unittest.TestCase):
    def test_a_rebuild_that_wrote_nothing_and_kept_every_snapshot_passes(self) -> None:
        self.assertTrue(rebuild_ok(unwritten(), SNAPSHOTS))

    def test_each_part_is_needed(self) -> None:
        for name, result, built in (
            ("a write", written(), SNAPSHOTS),
            ("a snapshot that changed", unwritten(snapshots={**SNAPSHOTS, ICU: 99}), SNAPSHOTS),
            ("no snapshots in the build", unwritten(), None),
            ("no snapshots in the rebuild", without(unwritten(), "snapshots"), SNAPSHOTS),
            ("a flag false", unwritten(passed={**PASSED, KIDNEY: False}), SNAPSHOTS),
            ("an outcome", unwritten(outcome="unchanged"), SNAPSHOTS),
        ):
            with self.subTest(name):
                self.assertFalse(rebuild_ok(result, built))


class HeldOutIcuChange(unittest.TestCase):
    def changed(self, **changes: Any) -> dict[str, Any]:
        return written(snapshots={MEASURE: 11, ICU: 99, KIDNEY: 13}, **changes)

    def test_one_new_snapshot_for_the_intensive_care_labels_only_passes(self) -> None:
        self.assertTrue(icu_change_ok(self.changed(), SNAPSHOTS, [ICU]))

    def test_each_part_is_needed(self) -> None:
        for name, result, last in (
            ("nothing written", unwritten(snapshots=SNAPSHOTS), [ICU]),
            ("nothing written but new snapshots", unwritten(snapshots={MEASURE: 11, ICU: 99, KIDNEY: 13}), [ICU]),
            ("the labels snapshot kept", written(), [ICU]),
            ("the labels snapshot null", written(snapshots={MEASURE: 11, ICU: None, KIDNEY: 13}), [ICU]),
            ("the measure snapshot new", written(snapshots={MEASURE: 98, ICU: 99, KIDNEY: 13}), [ICU]),
            ("the kidney snapshot new", written(snapshots={MEASURE: 11, ICU: 99, KIDNEY: 98}), [ICU]),
            ("more than one commit", self.changed(), [MEASURE, ICU]),
            ("the kidney commit instead", self.changed(), [KIDNEY]),
            ("no commit", self.changed(), []),
            ("a flag false", self.changed(passed={**PASSED, ICU: False}), [ICU]),
            ("a digest key", self.changed(fingerprints=1), [ICU]),
            ("a row key", self.changed(rows=1), [ICU]),
            ("an outcome", self.changed(outcome="written"), [ICU]),
        ):
            with self.subTest(name):
                self.assertFalse(icu_change_ok(result, SNAPSHOTS, last))


if __name__ == "__main__":
    unittest.main()
