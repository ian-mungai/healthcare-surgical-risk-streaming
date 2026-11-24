"""The acceptance rules that the Silver end-to-end runs share (``tests/fast/silver_accept.py``) accept only the result they describe. No Docker runs here.

The runners are not tracked, so each rule that decides pass or fail from a job's output is here, where it has a test.

Failure modes covered here:

- A refusal passes when its code is the wanted one but its text is not (``rule_mismatch: 37 patients`` for ``rule_mismatch: failed``), when a line is
  missing or extra, or when the order differs.
- A write is read from a field that a held-out row changes (``outcome``) and not from the hashes: an empty result, or a result whose write hash is not the
  branch hash, or whose branch did not move, passes as a write.
- A result that wrote nothing is accepted when it names no write hash by missing the key, or when its branch hash is not the checked one.
- A moved-null rebuild passes with the snapshot IDs missing, the waveform snapshot unchanged, a pass flag false or missing, or nothing written.
"""

from __future__ import annotations

import unittest
from typing import Any

from tests.fast.silver_accept import kept, moved_null_ok, refusal_matches, wrote

HEAD, WRITE = "a" * 64, "b" * 64
BEFORE = {"numeric_observations": 11, "waveform_seconds": 12}
PASSED = {"numeric_observations": True, "waveform_seconds": True}


def written(**changes: Any) -> dict[str, Any]:
    return {
        "checked_hash": HEAD,
        "write_hash": WRITE,
        "branch_hash": WRITE,
        "snapshots": {"numeric_observations": 11, "waveform_seconds": 13},
        "passed": PASSED,
        **changes,
    }


def unwritten(**changes: Any) -> dict[str, Any]:
    return {"checked_hash": HEAD, "write_hash": None, "branch_hash": HEAD, "snapshots": BEFORE, "passed": PASSED, **changes}


class Refusals(unittest.TestCase):
    def test_a_refusal_matches_only_its_complete_lines_in_order(self) -> None:
        self.assertTrue(refusal_matches(["rule_mismatch: failed"], ["rule_mismatch: failed"]))
        self.assertTrue(refusal_matches(["event_short: failed", "event_times: failed"], ["event_short: failed", "event_times: failed"]))
        self.assertTrue(refusal_matches(["source_table: 0 tables"], ["source_table: 0 tables"]))

    def test_a_line_with_the_wanted_code_and_other_text_does_not_match(self) -> None:
        self.assertFalse(refusal_matches(["rule_mismatch: 37 patients"], ["rule_mismatch: failed"]))
        self.assertFalse(refusal_matches(["schema: the clinical table has 4 columns"], ["schema: failed"]))
        self.assertFalse(refusal_matches(["rule_mismatch"], ["rule_mismatch: failed"]))

    def test_a_missing_extra_or_reordered_line_does_not_match(self) -> None:
        wanted = ["event_short: failed", "event_times: failed"]
        self.assertFalse(refusal_matches(wanted[:1], wanted))
        self.assertFalse(refusal_matches([*wanted, "event_differs: failed"], wanted))
        self.assertFalse(refusal_matches(wanted[::-1], wanted))
        self.assertFalse(refusal_matches([], wanted))

    def test_no_line_matches_no_line_only_when_none_is_wanted(self) -> None:
        self.assertTrue(refusal_matches([], []))
        self.assertFalse(refusal_matches(["usage"], []))


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
        missing = unwritten()
        del missing["write_hash"]
        self.assertFalse(kept(missing))


class MovedNull(unittest.TestCase):
    def test_a_write_with_both_snapshot_ids_a_new_waveform_snapshot_and_both_flags_passes(self) -> None:
        self.assertTrue(moved_null_ok(written(), BEFORE))

    def test_each_part_is_needed(self) -> None:
        for name, result in (
            ("nothing written", written(write_hash=None, branch_hash=HEAD)),
            ("no snapshots", {k: v for k, v in written().items() if k != "snapshots"}),
            ("empty snapshots", written(snapshots={})),
            ("no numeric snapshot", written(snapshots={"waveform_seconds": 13})),
            ("no waveform snapshot", written(snapshots={"numeric_observations": 11})),
            ("waveform snapshot null", written(snapshots={"numeric_observations": 11, "waveform_seconds": None})),
            ("waveform snapshot unchanged", written(snapshots=BEFORE)),
            ("no flags", {k: v for k, v in written().items() if k != "passed"}),
            ("numeric flag false", written(passed={"numeric_observations": False, "waveform_seconds": True})),
            ("waveform flag false", written(passed={"numeric_observations": True, "waveform_seconds": False})),
            ("a flag missing", written(passed={"numeric_observations": True})),
            ("a flag that is not a boolean", written(passed={"numeric_observations": 1, "waveform_seconds": True})),
            ("only outcome", {"outcome": "written"}),
        ):
            with self.subTest(name):
                self.assertFalse(moved_null_ok(result, BEFORE))

    def test_a_before_without_a_waveform_snapshot_cannot_show_a_change(self) -> None:
        self.assertFalse(moved_null_ok(written(), {"numeric_observations": 11}))
        self.assertFalse(moved_null_ok(written(), {}))
        self.assertFalse(moved_null_ok(written(snapshots={"numeric_observations": 11, "waveform_seconds": None}), {"waveform_seconds": None}))


if __name__ == "__main__":
    unittest.main()
