"""The acceptance rules that the Silver end-to-end runs share (``tests/fast/silver_accept.py``) accept only the result they describe. No Docker runs here.

The runners are not tracked, so each rule that decides pass or fail from a job's output is here, where it has a test.

Failure modes covered here:

- A refusal passes when its code is the wanted one but its text is not (``rule_mismatch: 37 patients`` for ``rule_mismatch: failed``), when a line is
  missing or extra, or when the order differs.
- A write is read from a field that a held-out row changes (``outcome``) and not from the hashes: an empty result, or a result whose write hash is not the
  branch hash, or whose branch did not move, passes as a write.
- A result that wrote nothing is accepted when it names no write hash by missing the key, or when its branch hash is not the checked one.
- A moved-null rebuild passes with the snapshot IDs missing, the waveform snapshot unchanged, a pass flag false or missing, or nothing written.
- A valid result passes with one more key than the job writes (``outcome: written`` or any other), at the top or inside ``snapshots``, ``source_snapshots`` or
  ``passed``, with a key missing, or with the keys of the other job.
- A result passes with a branch hash, a write hash or a snapshot ID that the catalog does not hold (another hash, another ID, a table without an ID).
- A stand-in script that calls a name nobody defines (a helper that was dropped) passes: it fails only when the container runs it.
"""

from __future__ import annotations

import unittest
from typing import Any

from tests.fast.silver_accept import HYPOTENSION_JOB, SIGNALS_JOB, catalog_matches, kept, moved_null_ok, refusal_matches, sealed, undefined_names, wrote

HEAD, WRITE = "a" * 64, "b" * 64
BEFORE = {"numeric_observations": 11, "waveform_seconds": 12}
PASSED = {"numeric_observations": True, "waveform_seconds": True}


def envelope(job: str) -> dict[str, Any]:
    return {"mode": "build", "branch": "b", "base_hash": HEAD, "job": job, "tables": ["lake.silver.a", "lake.silver.b"], "checks_run": ["a", "b"]}


def written(**changes: Any) -> dict[str, Any]:
    return {
        **envelope(SIGNALS_JOB),
        "checked_hash": HEAD,
        "write_hash": WRITE,
        "branch_hash": WRITE,
        "source_snapshots": {"numeric_observations": 1, "waveform_seconds": 2, "tracks": 3},
        "snapshots": {"numeric_observations": 11, "waveform_seconds": 13},
        "passed": PASSED,
        **changes,
    }


def unwritten(**changes: Any) -> dict[str, Any]:
    return {**written(), "write_hash": None, "branch_hash": HEAD, "snapshots": BEFORE, **changes}


def hypotension(**changes: Any) -> dict[str, Any]:
    return {
        **envelope(HYPOTENSION_JOB),
        "checked_hash": HEAD,
        "write_hash": WRITE,
        "branch_hash": WRITE,
        "source_snapshots": {"waveform_seconds": 1, "clinical_cases": 2, "patient_split": 3},
        "snapshots": {"art_mbp_1s": 11, "hypotension_events": 12},
        "passed": {"art_mbp_1s": True, "hypotension_events": True},
        "development_events_digest": 9,
        **changes,
    }


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
        self.assertTrue(wrote(written(), SIGNALS_JOB))
        self.assertFalse(kept(written(), SIGNALS_JOB))

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
                self.assertFalse(wrote(written(**change), SIGNALS_JOB))
        self.assertFalse(wrote({}, SIGNALS_JOB))
        self.assertFalse(wrote({"outcome": "written"}, SIGNALS_JOB))

    def test_a_result_that_names_no_write_and_stays_on_the_checked_hash_is_kept(self) -> None:
        self.assertTrue(kept(unwritten(), SIGNALS_JOB))
        self.assertFalse(wrote(unwritten(), SIGNALS_JOB))

    def test_a_kept_result_needs_each_hash(self) -> None:
        for change in ({"write_hash": WRITE}, {"branch_hash": WRITE}, {"checked_hash": None, "branch_hash": None}):
            with self.subTest(change=change):
                self.assertFalse(kept(unwritten(**change), SIGNALS_JOB))
        self.assertFalse(kept({}, SIGNALS_JOB))
        self.assertFalse(kept({"outcome": "unchanged"}, SIGNALS_JOB))
        missing = unwritten()
        del missing["write_hash"]
        self.assertFalse(kept(missing, SIGNALS_JOB))


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


class ResultKeys(unittest.TestCase):
    def test_a_result_with_exactly_the_keys_of_its_job_is_sealed(self) -> None:
        self.assertTrue(sealed(written(), SIGNALS_JOB))
        self.assertTrue(sealed(unwritten(), SIGNALS_JOB))
        self.assertTrue(sealed(hypotension(), HYPOTENSION_JOB))
        self.assertTrue(wrote(hypotension(), HYPOTENSION_JOB))
        self.assertTrue(kept(hypotension(write_hash=None, branch_hash=HEAD), HYPOTENSION_JOB))

    def test_one_more_key_at_the_top_fails_every_rule_that_reads_a_result(self) -> None:
        for extra in ({"outcome": "written"}, {"outcome": "unchanged"}, {"rows": 3}, {"x": None}, {"development_events_digest": 9}):
            with self.subTest(extra=extra):
                self.assertFalse(sealed(written(**extra), SIGNALS_JOB))
                self.assertFalse(wrote(written(**extra), SIGNALS_JOB))
                self.assertFalse(kept(unwritten(**extra), SIGNALS_JOB))
                self.assertFalse(moved_null_ok(written(**extra), BEFORE))
        for extra in ({"outcome": "written"}, {"count": 1}):
            with self.subTest(extra=extra):
                self.assertFalse(wrote(hypotension(**extra), HYPOTENSION_JOB))
                self.assertFalse(kept(hypotension(write_hash=None, branch_hash=HEAD, **extra), HYPOTENSION_JOB))

    def test_one_key_missing_or_the_keys_of_the_other_job_fail(self) -> None:
        for key in written():
            with self.subTest(missing=key):
                self.assertFalse(sealed({k: v for k, v in written().items() if k != key}, SIGNALS_JOB))
        self.assertFalse(sealed({k: v for k, v in hypotension().items() if k != "development_events_digest"}, HYPOTENSION_JOB))
        self.assertFalse(sealed(hypotension(), SIGNALS_JOB))
        self.assertFalse(sealed(written(), HYPOTENSION_JOB))
        self.assertFalse(sealed({}, SIGNALS_JOB))

    def test_one_more_key_inside_a_table_map_fails(self) -> None:
        for name in ("snapshots", "source_snapshots", "passed"):
            with self.subTest(name=name):
                result = written()
                result[name] = {**result[name], "outcome": 1}
                self.assertFalse(sealed(result, SIGNALS_JOB))
                result = written()
                result[name] = {key: value for key, value in list(result[name].items())[1:]}
                self.assertFalse(sealed(result, SIGNALS_JOB))
        self.assertFalse(sealed(hypotension(snapshots={"art_mbp_1s": 1, "hypotension_events": 2, "x": 3}), HYPOTENSION_JOB))
        self.assertFalse(sealed(written(snapshots=None), SIGNALS_JOB))

    def test_an_unknown_job_is_never_sealed(self) -> None:
        self.assertFalse(sealed(written(), "silver_cases"))


class Catalog(unittest.TestCase):
    SNAPSHOTS = {"numeric_observations": 11, "waveform_seconds": 13}

    def test_a_result_that_names_the_hash_and_the_snapshot_ids_that_the_catalog_holds_matches(self) -> None:
        self.assertTrue(catalog_matches(written(), WRITE, self.SNAPSHOTS))
        self.assertTrue(catalog_matches(unwritten(), HEAD, BEFORE))

    def test_a_branch_hash_a_write_hash_or_a_snapshot_ID_that_the_catalog_does_not_hold_fails(self) -> None:
        other = "c" * 64
        for name, result, head, snapshots in (
            ("branch hash is not the head", written(), other, self.SNAPSHOTS),
            ("reported branch hash differs", written(branch_hash=other), WRITE, self.SNAPSHOTS),
            ("write hash differs", written(write_hash=other), WRITE, self.SNAPSHOTS),
            ("a snapshot ID differs", written(snapshots={"numeric_observations": 11, "waveform_seconds": 99}), WRITE, self.SNAPSHOTS),
            ("a snapshot ID is reported and the catalog holds none", written(), WRITE, {"numeric_observations": 11, "waveform_seconds": None}),
            ("a table that the catalog does not hold", written(), WRITE, {"numeric_observations": 11}),
            ("a table that the result does not hold", written(snapshots={"numeric_observations": 11}), WRITE, self.SNAPSHOTS),
            ("none of either", written(snapshots={}), WRITE, {}),
            (
                "both none",
                written(snapshots={"numeric_observations": None, "waveform_seconds": None}),
                WRITE,
                {"numeric_observations": None, "waveform_seconds": None},
            ),
            ("the kept result's write hash", unwritten(write_hash=WRITE), HEAD, BEFORE),
            ("no head", written(), "", self.SNAPSHOTS),
            ("no head and a result with no hash", unwritten(checked_hash="", branch_hash=""), "", BEFORE),
        ):
            with self.subTest(name):
                self.assertFalse(catalog_matches(result, head, snapshots))

    def test_the_snapshot_IDs_are_whole_numbers_and_not_flags(self) -> None:
        self.assertFalse(
            catalog_matches(written(snapshots={"numeric_observations": True, "waveform_seconds": 1}), WRITE, {"numeric_observations": 1, "waveform_seconds": 1})
        )


class StandInNames(unittest.TestCase):
    def test_a_script_whose_names_are_all_defined_has_no_undefined_name(self) -> None:
        source = """
import json, os
from fractions import Fraction
from pyspark.sql import functions as F

CASE = 1
STEPS = [1, 2]


class Step:
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        return self

    def __exit__(self, *error):
        return False


def helper(session, value, *rest, key=None, **more):
    total = sum(item for item in rest if item)
    with Step("x") as step:
        found = {name: value for name in more}
    try:
        pass
    except (OSError, ValueError) as caught:
        raise SystemExit(str(caught)) from None
    return [lambda n: n + total, found, step, F.col("a"), Fraction(1), json.dumps({}), os.sep, CASE, key, session]


def use_later():
    return defined_below(1)


def defined_below(x):
    global LATE
    LATE = x
    return LATE, print, __name__, __file__


def reads_late():
    return LATE


def declares_only():
    global NEVER_SET


def main():
    return helper(None, 1), use_later(), STEPS, [i for i in range(3) if i], {k for k in "ab"}, {k: k for k in "ab"}


main()
"""
        self.assertEqual([], undefined_names(source))

    def test_a_helper_that_is_called_and_never_defined_is_named(self) -> None:
        source = "def plant(session):\n    overwrite(session, 1)\n\n\ndef main():\n    plant(None)\n\n\nmain()\n"
        self.assertEqual(["overwrite"], undefined_names(source))

    def test_each_place_a_name_can_be_used_is_checked(self) -> None:
        for name, source in (
            ("module level", "missing_a(1)\n"),
            ("a function", "def f():\n    return missing_a\n"),
            ("a nested function", "def f():\n    def g():\n        return missing_a\n    return g\n"),
            ("a class body", "class C:\n    x = missing_a\n"),
            ("a method", "class C:\n    def f(self):\n        return missing_a\n"),
            ("a comprehension", "def f():\n    return [missing_a(i) for i in range(3)]\n"),
            ("a lambda", "f = lambda: missing_a\n"),
            ("a default value", "def f(x=missing_a):\n    return x\n"),
            ("a decorator", "@missing_a\ndef f():\n    return 1\n"),
            ("a base class", "class C(missing_a):\n    pass\n"),
            ("an annotation that runs", "def f(x: missing_a):\n    return x\n"),
        ):
            with self.subTest(name):
                self.assertEqual(["missing_a"], undefined_names(source))

    def test_a_name_that_only_another_function_defines_is_not_defined_here(self) -> None:
        source = "def a():\n    local = 1\n    return local\n\n\ndef b():\n    return local\n"
        self.assertEqual(["local"], undefined_names(source))

    def test_every_undefined_name_is_listed_once_in_order_of_the_sorted_names(self) -> None:
        self.assertEqual(["alpha", "beta"], undefined_names("def f():\n    return beta + alpha + beta\n"))

    def test_a_script_that_does_not_compile_raises_instead_of_passing(self) -> None:
        with self.assertRaises(SyntaxError):
            undefined_names("def f(:\n")


if __name__ == "__main__":
    unittest.main()
