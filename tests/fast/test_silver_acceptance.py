"""The acceptance rules that the Silver end-to-end runs share (``tests/fast/silver_accept.py``) accept only the result they describe. No Docker runs here.

The runners are not tracked, so each rule that decides pass or fail from a job's output is here, where it has a test. A runner reads the result file that it
asked for with ``--result`` and nothing else: never a line of standard output or of standard error.

Failure modes covered here:

- A refusal passes when its code is the wanted one but its text is not (``rule_mismatch: 37 patients`` for ``rule_mismatch: failed``), when a line is
  missing or extra, or when the order differs; or the refusal document has a key more, another job or a code that is not text.
- A result file that holds two documents (an extra one before the real one), that is not a JSON object, that repeats a key, that holds ``NaN`` or that is not
  there passes as the result.
- A write is read from a field that a held-out row changes (``outcome``) and not from the hashes: an empty result, or a result whose write hash is not the
  branch hash, or whose branch did not move, passes as a write.
- A result that wrote nothing is accepted when it names no write hash by missing the key, or when its branch hash is not the checked one.
- A moved-null rebuild passes with the snapshot IDs missing, the waveform snapshot unchanged, a pass flag false or missing, or nothing written.
- A valid result passes with one more key than the job writes, at any level, with a key missing, with the keys of the other job, with a value of the wrong
  type (a flag of ``1``, a snapshot ID of ``true``, a table list that is not the job's), or with ``checks_run`` that is not the job's list of check names
  (one more name, a name missing, another order, a number among the names).
- A result passes with a checked hash, a base hash, a branch hash, a write hash or a snapshot ID (of a source or of an output) that the catalog does not hold
  (another hash, another ID, a table without an ID, a source snapshot 17 where the catalog holds 1).
- A stand-in script that calls a name nobody defines (a helper that was dropped) passes: it fails only when the container runs it.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from spark.jobs.silver_hypotension import CHECK_NAMES as HYPOTENSION_CHECKS
from spark.jobs.silver_signals import CHECK_NAMES as SIGNALS_CHECKS
from tests.fast.silver_accept import (
    CHECKS,
    HYPOTENSION_JOB,
    SIGNALS_JOB,
    Catalog,
    catalog_matches,
    kept,
    moved_null_ok,
    read_result,
    refusal_matches,
    refused,
    sealed,
    undefined_names,
    wrote,
)

HEAD, WRITE = "a" * 64, "b" * 64
BEFORE = {"numeric_observations": 11, "waveform_seconds": 12}
AFTER = {"numeric_observations": 11, "waveform_seconds": 13}
SOURCES = {"numeric_observations": 1, "waveform_seconds": 2, "tracks": 3}
PASSED = {"numeric_observations": True, "waveform_seconds": True}
WROTE = Catalog(checked=HEAD, base=HEAD, head=WRITE, sources=SOURCES, snapshots=AFTER)
KEPT = Catalog(checked=HEAD, base=HEAD, head=HEAD, sources=SOURCES, snapshots=BEFORE)
VERIFIED = Catalog(checked=HEAD, base=None, head=HEAD, sources=SOURCES, snapshots=BEFORE)
HYPO_SOURCES = {"waveform_seconds": 1, "clinical_cases": 2, "patient_split": 3}
HYPO_SNAPSHOTS = {"art_mbp_1s": 11, "hypotension_events": 12}
HYPO_WROTE = Catalog(checked=HEAD, base=HEAD, head=WRITE, sources=HYPO_SOURCES, snapshots=HYPO_SNAPSHOTS)
HYPO_KEPT = Catalog(checked=HEAD, base=HEAD, head=HEAD, sources=HYPO_SOURCES, snapshots=HYPO_SNAPSHOTS)


def written(**changes: Any) -> dict[str, Any]:
    return {
        "mode": "build",
        "branch": "b",
        "base_hash": HEAD,
        "job": SIGNALS_JOB,
        "tables": ["silver.numeric_observations", "silver.waveform_seconds"],
        "checks_run": list(SIGNALS_CHECKS),
        "checked_hash": HEAD,
        "write_hash": WRITE,
        "branch_hash": WRITE,
        "source_snapshots": dict(SOURCES),
        "snapshots": dict(AFTER),
        "passed": dict(PASSED),
        **changes,
    }


def unwritten(**changes: Any) -> dict[str, Any]:
    return {**written(), "write_hash": None, "branch_hash": HEAD, "snapshots": dict(BEFORE), **changes}


def verified(**changes: Any) -> dict[str, Any]:
    return {**unwritten(), "mode": "verify", "base_hash": None, **changes}


def hypotension(**changes: Any) -> dict[str, Any]:
    return {
        **written(),
        "job": HYPOTENSION_JOB,
        "tables": ["silver.art_mbp_1s", "silver.hypotension_events"],
        "checks_run": list(HYPOTENSION_CHECKS),
        "source_snapshots": dict(HYPO_SOURCES),
        "snapshots": dict(HYPO_SNAPSHOTS),
        "passed": {"art_mbp_1s": True, "hypotension_events": True},
        "development_events_digest": 9,
        **changes,
    }


def refusal(**changes: Any) -> dict[str, Any]:
    return {"job": SIGNALS_JOB, "mode": "build", "refusal": ["waveform_duplicate_second: failed"], **changes}


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


class RefusalDocuments(unittest.TestCase):
    WANTED = ["waveform_duplicate_second: failed"]

    def test_a_refusal_document_with_the_wanted_codes_of_the_job_is_accepted(self) -> None:
        self.assertTrue(refused(refusal(), SIGNALS_JOB, "build", self.WANTED))
        self.assertTrue(refused(refusal(mode="verify", refusal=["a: failed", "b: failed"]), SIGNALS_JOB, "verify", ["a: failed", "b: failed"]))
        self.assertTrue(refused(refusal(job=HYPOTENSION_JOB, refusal=["branch_missing"]), HYPOTENSION_JOB, "build", ["branch_missing"]))

    def test_each_part_of_the_document_is_needed(self) -> None:
        for name, document in (
            ("no document", None),
            ("a list", ["waveform_duplicate_second: failed"]),
            ("another code", refusal(refusal=["waveform_without_track: failed"])),
            ("a count in the text", refusal(refusal=["waveform_duplicate_second: 3 seconds"])),
            ("one line more", refusal(refusal=[*self.WANTED, "x: failed"])),
            ("a line missing", refusal(refusal=[])),
            ("a code that is not text", refusal(refusal=[*self.WANTED, 17])),
            ("a text, not a list", refusal(refusal="waveform_duplicate_second: failed")),
            ("another job", refusal(job=HYPOTENSION_JOB)),
            ("another mode", refusal(mode="verify")),
            ("an unknown mode", refusal(mode="clean")),
            ("one key more", {**refusal(), "outcome": "refused"}),
            ("a success result", written()),
            ("no job", {key: value for key, value in refusal().items() if key != "job"}),
            ("no refusal", {key: value for key, value in refusal().items() if key != "refusal"}),
        ):
            with self.subTest(name):
                self.assertFalse(refused(document, SIGNALS_JOB, "build", self.WANTED))


class ReadResult(unittest.TestCase):
    def read(self, text: str | bytes | None) -> dict[str, Any] | None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            if isinstance(text, str):
                path.write_text(text)
            elif text is not None:
                path.write_bytes(text)
            return read_result(path)

    def test_a_file_that_holds_exactly_one_object_is_read(self) -> None:
        self.assertEqual(written(), self.read(json.dumps(written())))
        self.assertEqual(written(), self.read(json.dumps(written(), indent=2, sort_keys=True) + "\n"))

    def test_a_file_that_holds_anything_else_is_not_a_result(self) -> None:
        extra = json.dumps({"mode": "build", "extra": 1})
        for name, text in (
            ("no file", None),
            ("an empty file", ""),
            ("an extra result before the real one", extra + "\n" + json.dumps(written())),
            ("an extra result after the real one", json.dumps(written()) + "\n" + extra),
            ("a result line prefix", "RESULT " + json.dumps(written())),
            ("a list", json.dumps([written()])),
            ("a number", "17"),
            ("not json", "{"),
            ("a key twice", '{"mode": "build", "mode": "verify"}'),
            ("a key twice inside a map", '{"snapshots": {"a": 1, "a": 2}}'),
            ("not a number", '{"development_events_digest": NaN}'),
            ("an infinity", '{"development_events_digest": Infinity}'),
            ("bytes that are not text", b"\xff\xfe{}"),
        ):
            with self.subTest(name):
                self.assertIsNone(self.read(text))

    def test_a_path_that_is_a_folder_is_not_a_result(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            self.assertIsNone(read_result(Path(folder)))


class Writes(unittest.TestCase):
    def test_a_result_that_names_a_new_write_hash_as_the_branch_hash_is_a_write(self) -> None:
        self.assertTrue(wrote(written(), SIGNALS_JOB, WROTE))
        self.assertFalse(kept(written(), SIGNALS_JOB, WROTE))

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
                self.assertFalse(wrote(written(**change), SIGNALS_JOB, WROTE))
        self.assertFalse(wrote({}, SIGNALS_JOB, WROTE))
        self.assertFalse(wrote({"outcome": "written"}, SIGNALS_JOB, WROTE))
        self.assertFalse(wrote(None, SIGNALS_JOB, WROTE))

    def test_a_catalog_that_did_not_move_is_not_a_write(self) -> None:
        self.assertFalse(wrote(written(), SIGNALS_JOB, KEPT))

    def test_a_write_hash_that_is_the_checked_hash_is_no_write_although_the_catalog_agrees(self) -> None:
        self.assertFalse(wrote(unwritten(write_hash=HEAD), SIGNALS_JOB, KEPT))
        self.assertTrue(catalog_matches(unwritten(write_hash=HEAD), KEPT))

    def test_a_result_that_names_no_write_and_stays_on_the_checked_hash_is_kept(self) -> None:
        self.assertTrue(kept(unwritten(), SIGNALS_JOB, KEPT))
        self.assertFalse(wrote(unwritten(), SIGNALS_JOB, KEPT))

    def test_a_kept_result_needs_each_hash(self) -> None:
        for change in ({"write_hash": WRITE}, {"branch_hash": WRITE}, {"checked_hash": None, "branch_hash": None}):
            with self.subTest(change=change):
                self.assertFalse(kept(unwritten(**change), SIGNALS_JOB, KEPT))
        self.assertFalse(kept({}, SIGNALS_JOB, KEPT))
        self.assertFalse(kept({"outcome": "unchanged"}, SIGNALS_JOB, KEPT))
        self.assertFalse(kept(None, SIGNALS_JOB, KEPT))
        missing = unwritten()
        del missing["write_hash"]
        self.assertFalse(kept(missing, SIGNALS_JOB, KEPT))

    def test_a_catalog_that_moved_is_not_a_kept_result(self) -> None:
        self.assertFalse(kept(unwritten(), SIGNALS_JOB, WROTE))

    def test_a_result_with_no_write_hash_whose_branch_moved_is_not_a_kept_result_although_the_catalog_agrees(self) -> None:
        self.assertFalse(kept(written(write_hash=None), SIGNALS_JOB, WROTE))

    def test_a_verify_is_kept_with_no_base_hash_and_a_build_needs_its_base_hash(self) -> None:
        self.assertTrue(kept(verified(), SIGNALS_JOB, VERIFIED))
        self.assertFalse(kept(verified(base_hash=HEAD), SIGNALS_JOB, VERIFIED))
        self.assertFalse(kept(unwritten(base_hash=None), SIGNALS_JOB, KEPT))
        self.assertFalse(kept(verified(), SIGNALS_JOB, KEPT))
        self.assertFalse(kept(unwritten(), SIGNALS_JOB, VERIFIED))

    def test_the_hypotension_job_has_the_same_rules(self) -> None:
        self.assertTrue(wrote(hypotension(), HYPOTENSION_JOB, HYPO_WROTE))
        self.assertTrue(kept(hypotension(write_hash=None, branch_hash=HEAD), HYPOTENSION_JOB, HYPO_KEPT))
        self.assertFalse(wrote(hypotension(), SIGNALS_JOB, HYPO_WROTE))


class MovedNull(unittest.TestCase):
    def test_a_write_with_both_snapshot_ids_a_new_waveform_snapshot_and_both_flags_passes(self) -> None:
        self.assertTrue(moved_null_ok(written(), WROTE, BEFORE))

    def test_each_part_is_needed(self) -> None:
        for name, result in (
            ("nothing written", unwritten()),
            ("no snapshots", {k: v for k, v in written().items() if k != "snapshots"}),
            ("empty snapshots", written(snapshots={})),
            ("no numeric snapshot", written(snapshots={"waveform_seconds": 13})),
            ("no waveform snapshot", written(snapshots={"numeric_observations": 11})),
            ("waveform snapshot null", written(snapshots={"numeric_observations": 11, "waveform_seconds": None})),
            ("waveform snapshot unchanged", written(snapshots=dict(BEFORE))),
            ("no flags", {k: v for k, v in written().items() if k != "passed"}),
            ("numeric flag false", written(passed={"numeric_observations": False, "waveform_seconds": True})),
            ("waveform flag false", written(passed={"numeric_observations": True, "waveform_seconds": False})),
            ("a flag missing", written(passed={"numeric_observations": True})),
            ("a flag that is not a boolean", written(passed={"numeric_observations": 1, "waveform_seconds": True})),
            ("only outcome", {"outcome": "written"}),
            ("no result", None),
        ):
            with self.subTest(name):
                self.assertFalse(moved_null_ok(result, WROTE, BEFORE))

    def test_a_snapshot_that_the_catalog_does_not_hold_is_not_a_moved_null(self) -> None:
        self.assertFalse(moved_null_ok(written(), Catalog(HEAD, HEAD, WRITE, SOURCES, {"numeric_observations": 11, "waveform_seconds": 99}), BEFORE))

    def test_a_waveform_snapshot_that_did_not_change_is_not_a_moved_null_although_the_catalog_agrees(self) -> None:
        agrees = Catalog(checked=HEAD, base=HEAD, head=WRITE, sources=SOURCES, snapshots=BEFORE)
        self.assertTrue(wrote(written(snapshots=dict(BEFORE)), SIGNALS_JOB, agrees))
        self.assertFalse(moved_null_ok(written(snapshots=dict(BEFORE)), agrees, BEFORE))

    def test_a_before_without_a_waveform_snapshot_cannot_show_a_change(self) -> None:
        self.assertFalse(moved_null_ok(written(), WROTE, {"numeric_observations": 11}))
        self.assertFalse(moved_null_ok(written(), WROTE, {}))
        self.assertFalse(moved_null_ok(written(snapshots={"numeric_observations": 11, "waveform_seconds": None}), WROTE, {"waveform_seconds": None}))


class ResultKeys(unittest.TestCase):
    def test_a_result_with_exactly_the_keys_and_types_of_its_job_is_sealed(self) -> None:
        self.assertTrue(sealed(written(), SIGNALS_JOB))
        self.assertTrue(sealed(unwritten(), SIGNALS_JOB))
        self.assertTrue(sealed(verified(), SIGNALS_JOB))
        self.assertTrue(sealed(hypotension(), HYPOTENSION_JOB))
        self.assertTrue(sealed(hypotension(mode="verify", base_hash=None, write_hash=None, branch_hash=HEAD), HYPOTENSION_JOB))

    def test_one_more_key_at_the_top_fails_every_rule_that_reads_a_result(self) -> None:
        for extra in ({"outcome": "written"}, {"outcome": "unchanged"}, {"rows": 3}, {"x": None}, {"development_events_digest": 9}):
            with self.subTest(extra=extra):
                self.assertFalse(sealed(written(**extra), SIGNALS_JOB))
                self.assertFalse(wrote(written(**extra), SIGNALS_JOB, WROTE))
                self.assertFalse(kept(unwritten(**extra), SIGNALS_JOB, KEPT))
                self.assertFalse(moved_null_ok(written(**extra), WROTE, BEFORE))
        for extra in ({"outcome": "written"}, {"count": 1}):
            with self.subTest(extra=extra):
                self.assertFalse(wrote(hypotension(**extra), HYPOTENSION_JOB, HYPO_WROTE))
                self.assertFalse(kept(hypotension(write_hash=None, branch_hash=HEAD, **extra), HYPOTENSION_JOB, HYPO_KEPT))

    def test_one_key_missing_or_the_keys_of_the_other_job_fail(self) -> None:
        for key in written():
            with self.subTest(missing=key):
                self.assertFalse(sealed({k: v for k, v in written().items() if k != key}, SIGNALS_JOB))
        self.assertFalse(sealed({k: v for k, v in hypotension().items() if k != "development_events_digest"}, HYPOTENSION_JOB))
        self.assertFalse(sealed(hypotension(), SIGNALS_JOB))
        self.assertFalse(sealed(written(), HYPOTENSION_JOB))
        self.assertFalse(sealed({}, SIGNALS_JOB))
        self.assertFalse(sealed(None, SIGNALS_JOB))

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

    def test_a_flag_is_a_boolean_and_not_a_number(self) -> None:
        for flags in (
            {"numeric_observations": 1, "waveform_seconds": True},
            {"numeric_observations": True, "waveform_seconds": 1},
            {"numeric_observations": 0, "waveform_seconds": False},
            {"numeric_observations": "true", "waveform_seconds": True},
            {"numeric_observations": None, "waveform_seconds": True},
        ):
            with self.subTest(flags=flags):
                self.assertFalse(sealed(written(passed=flags), SIGNALS_JOB))
                self.assertFalse(wrote(written(passed=flags), SIGNALS_JOB, WROTE))
        self.assertFalse(sealed(hypotension(passed={"art_mbp_1s": 1, "hypotension_events": True}), HYPOTENSION_JOB))

    def test_a_snapshot_id_is_a_whole_number(self) -> None:
        for name in ("snapshots", "source_snapshots"):
            for value in (True, 1.0, "1", None, [1]):
                with self.subTest(name=name, value=value):
                    result = written()
                    result[name] = {**result[name], next(iter(result[name])): value}
                    self.assertFalse(sealed(result, SIGNALS_JOB))
        self.assertFalse(sealed(hypotension(development_events_digest=True), HYPOTENSION_JOB))
        self.assertFalse(sealed(hypotension(development_events_digest="9"), HYPOTENSION_JOB))
        self.assertFalse(sealed(hypotension(development_events_digest=None), HYPOTENSION_JOB))

    def test_the_checks_that_ran_are_the_jobs_list_of_check_names(self) -> None:
        self.assertEqual(SIGNALS_CHECKS, CHECKS[SIGNALS_JOB])
        self.assertEqual(HYPOTENSION_CHECKS, CHECKS[HYPOTENSION_JOB])
        names = list(SIGNALS_CHECKS)
        for name, checks in (
            ("a number appended", [*names, 17]),
            ("a name appended", [*names, "extra_check"]),
            ("a name missing", names[1:]),
            ("the last name missing", names[:-1]),
            ("another order", names[::-1]),
            ("a name twice", [*names, names[0]]),
            ("empty", []),
            ("a text", ",".join(names)),
            ("a tuple", tuple(names)),
            ("none", None),
            ("the other job's names", list(HYPOTENSION_CHECKS)),
        ):
            with self.subTest(name):
                self.assertFalse(sealed(written(checks_run=checks), SIGNALS_JOB))
        self.assertFalse(sealed(hypotension(checks_run=[*HYPOTENSION_CHECKS, 17]), HYPOTENSION_JOB))
        self.assertFalse(sealed(hypotension(checks_run=list(SIGNALS_CHECKS)), HYPOTENSION_JOB))

    def test_the_envelope_is_the_jobs(self) -> None:
        changes: list[dict[str, Any]] = [
            {"mode": "clean"},
            {"mode": None},
            {"branch": ""},
            {"branch": 3},
            {"base_hash": ""},
            {"base_hash": 3},
            {"job": HYPOTENSION_JOB},
            {"tables": []},
            {"tables": ["silver.numeric_observations"]},
            {"tables": ["silver.waveform_seconds", "silver.numeric_observations"]},
            {"tables": ["lake.silver.numeric_observations", "lake.silver.waveform_seconds"]},
            {"tables": "silver.numeric_observations"},
            {"checked_hash": ""},
            {"checked_hash": 3},
            {"branch_hash": ""},
            {"write_hash": 3},
            {"write_hash": ""},
        ]
        for change in changes:
            with self.subTest(change=change):
                self.assertFalse(sealed(written(**change), SIGNALS_JOB))

    def test_a_mode_is_build_or_verify_whatever_the_base_hash(self) -> None:
        for mode in ("clean", "", None, "Build", 1):
            for base in (None, HEAD):
                with self.subTest(mode=mode, base=base):
                    self.assertFalse(sealed(written(mode=mode, base_hash=base), SIGNALS_JOB))

    def test_a_build_has_a_base_hash_and_a_verify_has_none(self) -> None:
        self.assertFalse(sealed(written(base_hash=None), SIGNALS_JOB))
        self.assertFalse(sealed(verified(base_hash=HEAD), SIGNALS_JOB))

    def test_an_unknown_job_is_never_sealed(self) -> None:
        self.assertFalse(sealed(written(), "silver_cases"))


class CatalogHolds(unittest.TestCase):
    OTHER = "c" * 64

    def test_a_result_that_names_the_hashes_and_the_snapshot_ids_that_the_catalog_holds_matches(self) -> None:
        self.assertTrue(catalog_matches(written(), WROTE))
        self.assertTrue(catalog_matches(unwritten(), KEPT))
        self.assertTrue(catalog_matches(verified(), VERIFIED))
        self.assertTrue(catalog_matches(hypotension(), HYPO_WROTE))

    def test_a_hash_or_a_snapshot_ID_that_the_catalog_does_not_hold_fails(self) -> None:
        other = self.OTHER
        for name, result, catalog in (
            ("branch hash is not the head", written(), Catalog(HEAD, HEAD, other, SOURCES, AFTER)),
            ("reported branch hash differs", written(branch_hash=other), WROTE),
            ("write hash differs", written(write_hash=other), WROTE),
            ("checked hash differs", written(checked_hash=other), WROTE),
            ("base hash differs", written(base_hash=other), WROTE),
            ("the catalog's checked hash differs", written(), Catalog(other, HEAD, WRITE, SOURCES, AFTER)),
            ("the catalog's base differs", written(), Catalog(HEAD, other, WRITE, SOURCES, AFTER)),
            ("a verify that names a base hash", verified(base_hash=HEAD), VERIFIED),
            ("a verify of a catalog that has a base", verified(), KEPT),
            ("an output snapshot ID differs", written(snapshots={"numeric_observations": 11, "waveform_seconds": 99}), WROTE),
            (
                "an output snapshot ID is reported and the catalog holds none",
                written(),
                Catalog(HEAD, HEAD, WRITE, SOURCES, {**AFTER, "waveform_seconds": None}),
            ),
            ("a table that the catalog does not hold", written(), Catalog(HEAD, HEAD, WRITE, SOURCES, {"numeric_observations": 11})),
            ("a table that the result does not hold", written(snapshots={"numeric_observations": 11}), WROTE),
            ("none of either", written(snapshots={}), Catalog(HEAD, HEAD, WRITE, SOURCES, {})),
            (
                "both none",
                written(snapshots={"numeric_observations": None, "waveform_seconds": None}),
                Catalog(HEAD, HEAD, WRITE, SOURCES, {"numeric_observations": None, "waveform_seconds": None}),
            ),
            ("a source snapshot 17 where the catalog holds 1", written(source_snapshots={**SOURCES, "numeric_observations": 17}), WROTE),
            ("every source snapshot differs", written(source_snapshots={"numeric_observations": 7, "waveform_seconds": 8, "tracks": 9}), WROTE),
            ("a source that the catalog does not hold", written(), Catalog(HEAD, HEAD, WRITE, {"numeric_observations": 1, "waveform_seconds": 2}, AFTER)),
            ("a source that the result does not hold", written(source_snapshots={"numeric_observations": 1, "waveform_seconds": 2}), WROTE),
            ("a source with no ID in the catalog", written(), Catalog(HEAD, HEAD, WRITE, {**SOURCES, "tracks": None}, AFTER)),
            ("the kept result's write hash", unwritten(write_hash=WRITE), KEPT),
            ("no head", written(), Catalog(HEAD, HEAD, "", SOURCES, AFTER)),
            ("no head and a result with no hash", unwritten(checked_hash="", branch_hash=""), Catalog("", "", "", SOURCES, BEFORE)),
            ("no result", None, WROTE),
        ):
            with self.subTest(name):
                self.assertFalse(catalog_matches(result, catalog))

    def test_a_hypotension_source_snapshot_that_the_catalog_does_not_hold_fails(self) -> None:
        self.assertFalse(catalog_matches(hypotension(source_snapshots={**HYPO_SOURCES, "clinical_cases": 17}), HYPO_WROTE))
        self.assertFalse(catalog_matches(hypotension(snapshots={**HYPO_SNAPSHOTS, "hypotension_events": 17}), HYPO_WROTE))

    def test_the_snapshot_IDs_are_whole_numbers_and_not_flags(self) -> None:
        self.assertFalse(
            catalog_matches(
                written(snapshots={"numeric_observations": True, "waveform_seconds": 1}),
                Catalog(HEAD, HEAD, WRITE, SOURCES, {"numeric_observations": 1, "waveform_seconds": 1}),
            )
        )
        self.assertFalse(
            catalog_matches(
                written(source_snapshots={"numeric_observations": True, "waveform_seconds": 2, "tracks": 3}),
                Catalog(HEAD, HEAD, WRITE, {**SOURCES, "numeric_observations": 1}, AFTER),
            )
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
