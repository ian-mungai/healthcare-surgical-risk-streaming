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
- A result that names another branch than the one that was asked for (``different_branch``) passes when its hashes and snapshot IDs match the catalog: the
  branch is compared first.
- The record of a rejected result holds a value of the file (a snapshot 17, a synthetic field, a refusal code that was not wanted): a rejected result gives one
  fixed phrase of the closed list ``REASONS`` or ``REFUSAL_REASONS`` and no value.
- A call is recorded from its file alone, without its exit code: a call that exited 1 with a write result that matches the catalog gets its write hash printed
  and saved, a refusal whose code text matches but whose process exited 3 gets that text printed, and a call that was to write is read from a result of a call
  that wrote nothing (a cached snapshot ID). One accept function for each kind of call (write, keep, refusal, usage) takes the exit code first and gives either
  the accepted document or a fixed phrase and an empty result.
- A call that exits with a usage error passes when a result file is there, because the parse of a file that holds ``[]`` returns no document: the presence of
  anything in the result folder is read before any parse, and a folder that cannot be listed counts as holding something.
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
    AS_KEEP,
    AS_WRITE,
    CHECKS,
    HYPOTENSION_JOB,
    NOT_EMPTY,
    REASONS,
    REFUSAL_REASONS,
    SIGNALS_JOB,
    USAGE_REASONS,
    WRONG_EXIT,
    Accepted,
    Catalog,
    accept_keep,
    accept_refusal,
    accept_usage,
    accept_write,
    catalog_matches,
    moved_null_ok,
    read_result,
    refusal_matches,
    refusal_rejection,
    rejection,
    result_present,
    sealed,
    undefined_names,
)

HEAD, WRITE_HASH = "a" * 64, "b" * 64
BEFORE = {"numeric_observations": 11, "waveform_seconds": 12}
AFTER = {"numeric_observations": 11, "waveform_seconds": 13}
SOURCES = {"numeric_observations": 1, "waveform_seconds": 2, "tracks": 3}
PASSED = {"numeric_observations": True, "waveform_seconds": True}
WROTE = Catalog(branch="b", checked=HEAD, base=HEAD, head=WRITE_HASH, sources=SOURCES, snapshots=AFTER)
KEPT = Catalog(branch="b", checked=HEAD, base=HEAD, head=HEAD, sources=SOURCES, snapshots=BEFORE)
VERIFIED = Catalog(branch="b", checked=HEAD, base=None, head=HEAD, sources=SOURCES, snapshots=BEFORE)
HYPO_SOURCES = {"waveform_seconds": 1, "clinical_cases": 2, "patient_split": 3}
HYPO_SNAPSHOTS = {"art_mbp_1s": 11, "hypotension_events": 12}
HYPO_WROTE = Catalog(branch="b", checked=HEAD, base=HEAD, head=WRITE_HASH, sources=HYPO_SOURCES, snapshots=HYPO_SNAPSHOTS)
HYPO_KEPT = Catalog(branch="b", checked=HEAD, base=HEAD, head=HEAD, sources=HYPO_SOURCES, snapshots=HYPO_SNAPSHOTS)


def written(**changes: Any) -> dict[str, Any]:
    return {
        "mode": "build",
        "branch": "b",
        "base_hash": HEAD,
        "job": SIGNALS_JOB,
        "tables": ["silver.numeric_observations", "silver.waveform_seconds"],
        "checks_run": list(SIGNALS_CHECKS),
        "checked_hash": HEAD,
        "write_hash": WRITE_HASH,
        "branch_hash": WRITE_HASH,
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
        **changes,
    }


def refusal(**changes: Any) -> dict[str, Any]:
    return {"job": SIGNALS_JOB, "mode": "build", "refusal": ["waveform_duplicate_second: failed"], **changes}


def wrote(document: Any, job: str, catalog: Catalog | None) -> bool:
    """Whether a call that exited 0 with this document is accepted as a write."""
    return accept_write(0, document, job, catalog).ok


def kept(document: Any, job: str, catalog: Catalog | None) -> bool:
    """Whether a call that exited 0 with this document is accepted as a call that wrote nothing."""
    return accept_keep(0, document, job, catalog).ok


def refused(document: Any, job: str, mode: str, wanted: list[str]) -> bool:
    """Whether a call that exited 1 with this document is accepted as the wanted refusal."""
    return accept_refusal(1, document, job, mode, wanted).ok


def moved(document: Any, catalog: Catalog | None, before: dict[str, Any]) -> bool:
    """Whether a write that exited 0 with this document is a rebuild after a null that moved inside one second."""
    return moved_null_ok(accept_write(0, document, SIGNALS_JOB, catalog), before)


class Refusals(unittest.TestCase):
    def test_a_refusal_matches_only_its_complete_lines_in_order(self) -> None:
        self.assertTrue(refusal_matches(["rule_mismatch: failed"], ["rule_mismatch: failed"]))
        self.assertTrue(refusal_matches(["event_short: failed", "event_times: failed"], ["event_short: failed", "event_times: failed"]))
        self.assertTrue(refusal_matches(["source_table: failed"], ["source_table: failed"]))

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
            ("not a number", '{"snapshots": NaN}'),
            ("an infinity", '{"snapshots": Infinity}'),
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
            {"checked_hash": WRITE_HASH},
            {"checked_hash": None},
            {"write_hash": ""},
        ):
            with self.subTest(change=change):
                self.assertFalse(wrote(written(**change), SIGNALS_JOB, WROTE))
        self.assertFalse(wrote({}, SIGNALS_JOB, WROTE))
        self.assertFalse(wrote({"outcome": "written"}, SIGNALS_JOB, WROTE))
        self.assertFalse(wrote(None, SIGNALS_JOB, WROTE))

    def test_a_result_that_names_another_branch_is_no_write_and_no_keep_although_every_hash_and_snapshot_agrees(self) -> None:
        self.assertFalse(wrote(written(branch="different_branch"), SIGNALS_JOB, WROTE))
        self.assertFalse(kept(unwritten(branch="different_branch"), SIGNALS_JOB, KEPT))
        self.assertFalse(kept(verified(branch="different_branch"), SIGNALS_JOB, VERIFIED))

    def test_a_catalog_that_did_not_move_is_not_a_write(self) -> None:
        self.assertFalse(wrote(written(), SIGNALS_JOB, KEPT))

    def test_a_write_hash_that_is_the_checked_hash_is_no_write_although_the_catalog_agrees(self) -> None:
        self.assertFalse(wrote(unwritten(write_hash=HEAD), SIGNALS_JOB, KEPT))
        self.assertTrue(catalog_matches(unwritten(write_hash=HEAD), KEPT))

    def test_a_result_that_names_no_write_and_stays_on_the_checked_hash_is_kept(self) -> None:
        self.assertTrue(kept(unwritten(), SIGNALS_JOB, KEPT))
        self.assertFalse(wrote(unwritten(), SIGNALS_JOB, KEPT))

    def test_a_kept_result_needs_each_hash(self) -> None:
        for change in ({"write_hash": WRITE_HASH}, {"branch_hash": WRITE_HASH}, {"checked_hash": None, "branch_hash": None}):
            with self.subTest(change=change):
                self.assertFalse(kept(unwritten(**change), SIGNALS_JOB, KEPT))
        self.assertFalse(kept({}, SIGNALS_JOB, KEPT))
        self.assertFalse(kept({"outcome": "unchanged"}, SIGNALS_JOB, KEPT))
        self.assertFalse(kept(None, SIGNALS_JOB, KEPT))
        missing = unwritten()
        del missing["write_hash"]
        self.assertFalse(kept(missing, SIGNALS_JOB, KEPT))

    def test_a_write_hash_that_is_the_checked_hash_is_no_keep_although_the_catalog_agrees(self) -> None:
        self.assertTrue(catalog_matches(unwritten(write_hash=HEAD), KEPT))
        self.assertFalse(kept(unwritten(write_hash=HEAD), SIGNALS_JOB, KEPT))
        self.assertEqual("the result is not a call that wrote nothing", rejection(unwritten(write_hash=HEAD), SIGNALS_JOB, KEPT, AS_KEEP))

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
        self.assertTrue(moved(written(), WROTE, BEFORE))

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
                self.assertFalse(moved(result, WROTE, BEFORE))

    def test_a_snapshot_that_the_catalog_does_not_hold_is_not_a_moved_null(self) -> None:
        self.assertFalse(moved(written(), Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {"numeric_observations": 11, "waveform_seconds": 99}), BEFORE))

    def test_a_waveform_snapshot_that_did_not_change_is_not_a_moved_null_although_the_catalog_agrees(self) -> None:
        agrees = Catalog(branch="b", checked=HEAD, base=HEAD, head=WRITE_HASH, sources=SOURCES, snapshots=BEFORE)
        self.assertTrue(wrote(written(snapshots=dict(BEFORE)), SIGNALS_JOB, agrees))
        self.assertFalse(moved(written(snapshots=dict(BEFORE)), agrees, BEFORE))

    def test_a_before_without_a_waveform_snapshot_cannot_show_a_change(self) -> None:
        self.assertFalse(moved(written(), WROTE, {"numeric_observations": 11}))
        self.assertFalse(moved(written(), WROTE, {}))
        self.assertFalse(moved(written(snapshots={"numeric_observations": 11, "waveform_seconds": None}), WROTE, {"waveform_seconds": None}))


class ResultKeys(unittest.TestCase):
    def test_a_result_with_exactly_the_keys_and_types_of_its_job_is_sealed(self) -> None:
        self.assertTrue(sealed(written(), SIGNALS_JOB))
        self.assertTrue(sealed(unwritten(), SIGNALS_JOB))
        self.assertTrue(sealed(verified(), SIGNALS_JOB))
        self.assertTrue(sealed(hypotension(), HYPOTENSION_JOB))
        self.assertTrue(sealed(hypotension(mode="verify", base_hash=None, write_hash=None, branch_hash=HEAD), HYPOTENSION_JOB))

    def test_one_more_key_at_the_top_fails_every_rule_that_reads_a_result(self) -> None:
        for extra in ({"outcome": "written"}, {"outcome": "unchanged"}, {"rows": 3}, {"x": None}):
            with self.subTest(extra=extra):
                self.assertFalse(sealed(written(**extra), SIGNALS_JOB))
                self.assertFalse(wrote(written(**extra), SIGNALS_JOB, WROTE))
                self.assertFalse(kept(unwritten(**extra), SIGNALS_JOB, KEPT))
                self.assertFalse(moved(written(**extra), WROTE, BEFORE))
        for extra in ({"outcome": "written"}, {"count": 1}, {"development_events_digest": 9}):
            with self.subTest(extra=extra):
                self.assertFalse(sealed(hypotension(**extra), HYPOTENSION_JOB))
                self.assertFalse(wrote(hypotension(**extra), HYPOTENSION_JOB, HYPO_WROTE))
                self.assertFalse(kept(hypotension(write_hash=None, branch_hash=HEAD, **extra), HYPOTENSION_JOB, HYPO_KEPT))

    def test_one_key_missing_or_the_keys_of_the_other_job_fail(self) -> None:
        for key in written():
            with self.subTest(missing=key):
                self.assertFalse(sealed({k: v for k, v in written().items() if k != key}, SIGNALS_JOB))
        for key in hypotension():
            with self.subTest(missing=key, job=HYPOTENSION_JOB):
                self.assertFalse(sealed({k: v for k, v in hypotension().items() if k != key}, HYPOTENSION_JOB))
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
        for value in (True, 1.0, "1", None, [1]):
            with self.subTest(job=HYPOTENSION_JOB, value=value):
                self.assertFalse(sealed(hypotension(snapshots={"art_mbp_1s": value, "hypotension_events": 12}), HYPOTENSION_JOB))

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
            ("branch hash is not the head", written(), Catalog("b", HEAD, HEAD, other, SOURCES, AFTER)),
            ("reported branch hash differs", written(branch_hash=other), WROTE),
            ("write hash differs", written(write_hash=other), WROTE),
            ("checked hash differs", written(checked_hash=other), WROTE),
            ("base hash differs", written(base_hash=other), WROTE),
            ("the catalog's checked hash differs", written(), Catalog("b", other, HEAD, WRITE_HASH, SOURCES, AFTER)),
            ("the catalog's base differs", written(), Catalog("b", HEAD, other, WRITE_HASH, SOURCES, AFTER)),
            ("a verify that names a base hash", verified(base_hash=HEAD), VERIFIED),
            ("a verify of a catalog that has a base", verified(), KEPT),
            ("an output snapshot ID differs", written(snapshots={"numeric_observations": 11, "waveform_seconds": 99}), WROTE),
            (
                "an output snapshot ID is reported and the catalog holds none",
                written(),
                Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {**AFTER, "waveform_seconds": None}),
            ),
            ("a table that the catalog does not hold", written(), Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {"numeric_observations": 11})),
            ("a table that the result does not hold", written(snapshots={"numeric_observations": 11}), WROTE),
            ("none of either", written(snapshots={}), Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {})),
            (
                "both none",
                written(snapshots={"numeric_observations": None, "waveform_seconds": None}),
                Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {"numeric_observations": None, "waveform_seconds": None}),
            ),
            ("a source snapshot 17 where the catalog holds 1", written(source_snapshots={**SOURCES, "numeric_observations": 17}), WROTE),
            ("every source snapshot differs", written(source_snapshots={"numeric_observations": 7, "waveform_seconds": 8, "tracks": 9}), WROTE),
            (
                "a source that the catalog does not hold",
                written(),
                Catalog("b", HEAD, HEAD, WRITE_HASH, {"numeric_observations": 1, "waveform_seconds": 2}, AFTER),
            ),
            ("a source that the result does not hold", written(source_snapshots={"numeric_observations": 1, "waveform_seconds": 2}), WROTE),
            ("a source with no ID in the catalog", written(), Catalog("b", HEAD, HEAD, WRITE_HASH, {**SOURCES, "tracks": None}, AFTER)),
            ("the kept result's write hash", unwritten(write_hash=WRITE_HASH), KEPT),
            ("no head", written(), Catalog("b", HEAD, HEAD, "", SOURCES, AFTER)),
            ("no head and a result with no hash", unwritten(checked_hash="", branch_hash=""), Catalog("b", "", "", "", SOURCES, BEFORE)),
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
                Catalog("b", HEAD, HEAD, WRITE_HASH, SOURCES, {"numeric_observations": 1, "waveform_seconds": 1}),
            )
        )
        self.assertFalse(
            catalog_matches(
                written(source_snapshots={"numeric_observations": True, "waveform_seconds": 2, "tracks": 3}),
                Catalog("b", HEAD, HEAD, WRITE_HASH, {**SOURCES, "numeric_observations": 1}, AFTER),
            )
        )


class Rejections(unittest.TestCase):
    """A rejected result gives one fixed phrase from a closed list: the rule that failed, and never a value of the file."""

    def test_a_result_that_passes_its_outcome_has_no_rejection(self) -> None:
        self.assertIsNone(rejection(written(), SIGNALS_JOB, WROTE, AS_WRITE))
        self.assertIsNone(rejection(unwritten(), SIGNALS_JOB, KEPT, AS_KEEP))
        self.assertIsNone(rejection(verified(), SIGNALS_JOB, VERIFIED, AS_KEEP))
        self.assertIsNone(rejection(hypotension(), HYPOTENSION_JOB, HYPO_WROTE, AS_WRITE))

    def test_each_failed_rule_names_its_own_phrase(self) -> None:
        for name, document, catalog, outcome, reason in (
            ("no file", None, WROTE, AS_WRITE, "no result file"),
            ("no catalog", written(), None, AS_WRITE, "the catalog was not read"),
            ("another branch", written(branch="different_branch"), WROTE, AS_WRITE, "the branch is not the requested one"),
            ("an extra key", written(outcome="written"), WROTE, AS_WRITE, "the schema is not the job's closed schema"),
            ("another source snapshot", written(source_snapshots={**SOURCES, "tracks": 17}), WROTE, AS_WRITE, "a hash or a snapshot differs from the catalog"),
            ("a keep read as a write", unwritten(), KEPT, AS_WRITE, "the result is not a write"),
            ("a write read as a keep", written(), WROTE, AS_KEEP, "the result is not a call that wrote nothing"),
        ):
            with self.subTest(name):
                self.assertEqual(reason, rejection(document, SIGNALS_JOB, catalog, outcome))

    def test_the_branch_is_compared_before_the_schema_and_the_catalog(self) -> None:
        both = written(branch="different_branch", outcome="written", checked_hash="0" * 64)
        self.assertEqual("the branch is not the requested one", rejection(both, SIGNALS_JOB, WROTE, AS_WRITE))
        self.assertFalse(catalog_matches(written(branch="different_branch"), WROTE))

    def test_every_phrase_is_in_the_closed_list_and_holds_no_value_of_the_file(self) -> None:
        planted = "synthetic_value_17"
        for name, document, catalog in (
            ("a snapshot 17", written(snapshots={"numeric_observations": 17, "waveform_seconds": 17}), WROTE),
            ("a synthetic field", written(synthetic=planted), WROTE),
            ("another branch", written(branch=planted), WROTE),
            ("a hash", written(checked_hash=planted), WROTE),
            ("a list", [planted], WROTE),
        ):
            with self.subTest(name):
                reason = rejection(document, SIGNALS_JOB, catalog, AS_WRITE)
                self.assertIn(reason, REASONS)
                self.assertNotIn(planted, str(reason))
                self.assertNotIn("17", str(reason))

    def test_a_check_that_the_rejection_does_not_make_is_caught_by_the_other_rules(self) -> None:
        self.assertIsNotNone(rejection(written(branch="x"), SIGNALS_JOB, WROTE, AS_WRITE))
        self.assertIsNotNone(rejection(written(write_hash=HEAD, branch_hash=HEAD), SIGNALS_JOB, KEPT, AS_WRITE))


class Evidence(unittest.TestCase):
    NAMES = ("write_hash", "branch_hash", "passed")

    def test_a_result_that_was_accepted_gives_its_named_fields(self) -> None:
        text = accept_write(0, written(), SIGNALS_JOB, WROTE).facts(*self.NAMES)
        self.assertIn(f"write hash {WRITE_HASH}", text)
        self.assertIn(f"branch hash {WRITE_HASH}", text)
        self.assertIn("passed {'numeric_observations': True, 'waveform_seconds': True}", text)

    def test_a_result_that_failed_gives_the_fixed_phrase_and_no_value(self) -> None:
        for name, document in (
            ("a snapshot 17 and a synthetic field", written(synthetic="synthetic_value_17", snapshots={"numeric_observations": 17, "waveform_seconds": 17})),
            ("another branch", written(branch="different_branch")),
            ("a refusal code in a success result", written(passed={"numeric_observations": "unexpected_code_17", "waveform_seconds": True})),
            ("no file", None),
        ):
            with self.subTest(name):
                text = accept_write(0, document, SIGNALS_JOB, WROTE).facts(*self.NAMES)
                self.assertEqual(f"result rejected: {rejection(document, SIGNALS_JOB, WROTE, AS_WRITE)}", text)
                for value in ("17", "synthetic", "different_branch", "unexpected", HEAD, WRITE_HASH):
                    self.assertNotIn(value, text)

    def test_a_rejected_catalog_gives_the_fixed_phrase(self) -> None:
        self.assertEqual("result rejected: the catalog was not read", accept_write(0, written(), SIGNALS_JOB, None).facts(*self.NAMES))


class RefusalEvidence(unittest.TestCase):
    WANTED = ["waveform_duplicate_second: failed"]

    def test_a_refusal_with_the_wanted_codes_gives_its_codes(self) -> None:
        self.assertIsNone(refusal_rejection(refusal(), SIGNALS_JOB, "build", self.WANTED))
        self.assertEqual("refusal ['waveform_duplicate_second: failed']", accept_refusal(1, refusal(), SIGNALS_JOB, "build", self.WANTED).facts())

    def test_a_refusal_that_failed_gives_a_fixed_phrase_and_no_code_or_value(self) -> None:
        for name, document, reason in (
            ("no file", None, "no result file"),
            ("an unexpected code", refusal(refusal=["unexpected_code_17: failed"]), "the refusal codes are not the wanted ones"),
            ("an extra key", {**refusal(), "synthetic": "synthetic_value_17"}, "the refusal document is not the closed refusal schema"),
            ("a success result", written(), "the refusal document is not the closed refusal schema"),
            ("another job", refusal(job=HYPOTENSION_JOB, refusal=["unexpected_code_17: failed"]), "the refusal document is not the closed refusal schema"),
        ):
            with self.subTest(name):
                self.assertEqual(reason, refusal_rejection(document, SIGNALS_JOB, "build", self.WANTED))
                text = accept_refusal(1, document, SIGNALS_JOB, "build", self.WANTED).facts()
                self.assertEqual(f"refusal rejected: {reason}", text)
                for value in ("17", "unexpected", "synthetic", HEAD):
                    self.assertNotIn(value, text)


class UsageExit(unittest.TestCase):
    def test_a_usage_exit_with_no_file_passes_and_with_a_file_fails(self) -> None:
        self.assertTrue(accept_usage(2, False).ok)
        self.assertFalse(accept_usage(2, True).ok)
        self.assertFalse(accept_usage(1, False).ok)
        self.assertFalse(accept_usage(0, False).ok)

    def test_each_failed_usage_rule_gives_its_own_fixed_phrase(self) -> None:
        self.assertEqual(NOT_EMPTY, accept_usage(2, True).reason)
        self.assertEqual(WRONG_EXIT, accept_usage(1, False).reason)
        self.assertEqual(WRONG_EXIT, accept_usage(1, True).reason)
        self.assertEqual("usage exit 2 with an empty result folder", accept_usage(2, False).facts())
        self.assertEqual(f"usage rejected: {NOT_EMPTY}", accept_usage(2, True).facts())

    def test_a_folder_with_anything_in_it_has_a_result_present_before_any_parse(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertFalse(result_present(root))
            (root / "result.json").write_text("[]")
            self.assertTrue(result_present(root))
            self.assertIsNone(read_result(root / "result.json"))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "result.json").symlink_to(root / "nowhere")
            self.assertTrue(result_present(root))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "other.tmp").write_bytes(b"\xff")
            self.assertTrue(result_present(root))

    def test_a_folder_that_cannot_be_listed_counts_as_present(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            self.assertTrue(result_present(Path(folder) / "gone"))

    def test_a_usage_exit_with_the_file_of_an_empty_list_fails_the_check(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "result.json").write_text("[]")
            self.assertFalse(accept_usage(2, result_present(root)).ok)


class AcceptedCalls(unittest.TestCase):
    """A call is accepted for the outcome that it expected, by its exit code and its file together. A call that was not accepted gives a fixed phrase, an empty
    result and no hash, snapshot ID or code text anywhere in what the record can print."""

    WANTED = ["waveform_duplicate_second: failed"]

    def assert_closed(self, accepted: Accepted, reasons: tuple[str, ...], reason: str, *forbidden: str) -> None:
        self.assertFalse(accepted.ok)
        self.assertEqual(reason, accepted.reason)
        self.assertIn(reason, reasons)
        self.assertEqual({}, dict(accepted.result))
        printed = accepted.facts("write_hash", "branch_hash", "snapshots", "checked_hash", "passed") + repr(accepted)
        for value in forbidden:
            self.assertNotIn(value, printed)

    def test_a_write_that_exited_1_is_rejected_and_its_write_hash_is_neither_printed_nor_kept(self) -> None:
        self.assertTrue(accept_write(0, written(), SIGNALS_JOB, WROTE).ok)
        accepted = accept_write(1, written(), SIGNALS_JOB, WROTE)
        self.assert_closed(accepted, REASONS, WRONG_EXIT, WRITE_HASH, HEAD, "11", "13")
        self.assertEqual(f"result rejected: {WRONG_EXIT}", accepted.facts("write_hash"))

    def test_a_keep_where_a_write_was_expected_is_rejected_and_gives_no_snapshot_ID(self) -> None:
        self.assertTrue(accept_keep(0, unwritten(), SIGNALS_JOB, KEPT).ok)
        accepted = accept_write(0, unwritten(), SIGNALS_JOB, KEPT)
        self.assert_closed(accepted, REASONS, "the result is not a write", "11", "12")
        self.assertIsNone(accepted.result.get("snapshots"))

    def test_a_write_where_a_keep_was_expected_is_rejected_and_gives_no_hash(self) -> None:
        self.assert_closed(accept_keep(0, written(), SIGNALS_JOB, WROTE), REASONS, "the result is not a call that wrote nothing", WRITE_HASH, "13")

    def test_a_refusal_with_the_wanted_code_text_that_exited_3_is_rejected_and_prints_no_code(self) -> None:
        self.assertTrue(accept_refusal(1, refusal(), SIGNALS_JOB, "build", self.WANTED).ok)
        accepted = accept_refusal(3, refusal(), SIGNALS_JOB, "build", self.WANTED)
        self.assert_closed(accepted, REFUSAL_REASONS, WRONG_EXIT, "waveform_duplicate_second", "failed")
        self.assertEqual(f"refusal rejected: {WRONG_EXIT}", accepted.facts())

    def test_a_refusal_exit_other_than_the_expected_one_fails_and_a_stated_exit_passes(self) -> None:
        for code in (0, 2, 3, 124, -9):
            with self.subTest(code=code):
                self.assertFalse(accept_refusal(code, refusal(), SIGNALS_JOB, "build", self.WANTED).ok)
        self.assertTrue(accept_refusal(7, refusal(), SIGNALS_JOB, "build", self.WANTED, expected=7).ok)
        self.assertFalse(accept_refusal(1, refusal(), SIGNALS_JOB, "build", self.WANTED, expected=7).ok)

    def test_each_exit_other_than_0_fails_a_write_and_a_keep_whatever_the_file_holds(self) -> None:
        for code in (1, 2, 3, 124, -9):
            with self.subTest(code=code):
                self.assertEqual(WRONG_EXIT, accept_write(code, written(), SIGNALS_JOB, WROTE).reason)
                self.assertEqual(WRONG_EXIT, accept_keep(code, unwritten(), SIGNALS_JOB, KEPT).reason)
                self.assertEqual(WRONG_EXIT, accept_keep(code, verified(), SIGNALS_JOB, VERIFIED).reason)

    def test_the_exit_code_is_judged_before_the_file(self) -> None:
        self.assertEqual(WRONG_EXIT, accept_write(1, None, SIGNALS_JOB, None).reason)
        self.assertEqual(WRONG_EXIT, accept_refusal(0, None, SIGNALS_JOB, "build", self.WANTED).reason)

    def test_an_accepted_call_holds_its_document_and_nothing_else(self) -> None:
        accepted = accept_write(0, written(), SIGNALS_JOB, WROTE)
        self.assertTrue(accepted.ok)
        self.assertIsNone(accepted.reason)
        self.assertEqual(written(), dict(accepted.result))
        refusal_ok = accept_refusal(1, refusal(), SIGNALS_JOB, "build", self.WANTED)
        self.assertEqual(refusal(), dict(refusal_ok.result))

    def test_a_planted_value_in_a_rejected_call_never_reaches_the_record(self) -> None:
        planted = "synthetic_value_17"
        for name, accepted in (
            ("a write that exited 1", accept_write(1, written(synthetic=planted), SIGNALS_JOB, WROTE)),
            ("a keep read as a write", accept_write(0, unwritten(note=planted), SIGNALS_JOB, KEPT)),
            ("a refusal that exited 3", accept_refusal(3, refusal(refusal=[planted]), SIGNALS_JOB, "build", [planted])),
            ("a refusal of another job", accept_refusal(1, refusal(refusal=[planted], job=HYPOTENSION_JOB), SIGNALS_JOB, "build", [planted])),
        ):
            with self.subTest(name):
                self.assertFalse(accepted.ok)
                self.assertNotIn(planted, accepted.facts("write_hash") + repr(accepted))

    def test_a_refusal_with_the_right_exit_and_other_codes_is_rejected_and_holds_no_document(self) -> None:
        planted = "unexpected_code_17: failed"
        for name, document in (
            ("other codes", refusal(refusal=[planted])),
            ("another job", refusal(job=HYPOTENSION_JOB, refusal=[planted])),
            ("a success result", written(synthetic=planted)),
        ):
            with self.subTest(name):
                accepted = accept_refusal(1, document, SIGNALS_JOB, "build", self.WANTED)
                self.assertFalse(accepted.ok)
                self.assertEqual({}, dict(accepted.result))
                self.assertIsNone(accepted.document)
                self.assertNotIn(planted, accepted.facts() + repr(accepted))

    def test_a_moved_null_needs_an_accepted_write(self) -> None:
        self.assertTrue(moved_null_ok(accept_write(0, written(), SIGNALS_JOB, WROTE), BEFORE))
        self.assertFalse(moved_null_ok(accept_write(1, written(), SIGNALS_JOB, WROTE), BEFORE))
        self.assertFalse(moved_null_ok(accept_keep(0, unwritten(), SIGNALS_JOB, KEPT), BEFORE))
        self.assertFalse(moved_null_ok(accept_refusal(1, refusal(), SIGNALS_JOB, "build", ["waveform_duplicate_second: failed"]), BEFORE))
        self.assertFalse(moved_null_ok(accept_usage(2, False), BEFORE))

    def test_a_moved_null_needs_a_write_of_the_signals_job(self) -> None:
        """Recheck 6: an accepted keep whose snapshot differs from an older one, and an accepted write of the hypotension job, are not a moved null."""
        self.assertFalse(moved_null_ok(accept_keep(0, unwritten(), SIGNALS_JOB, KEPT), {**BEFORE, "waveform_seconds": 13}))
        self.assertFalse(moved_null_ok(accept_write(0, hypotension(), HYPOTENSION_JOB, HYPO_WROTE), BEFORE))

    def test_the_accept_functions_of_the_other_job_and_a_wrong_catalog_reject(self) -> None:
        self.assertTrue(accept_write(0, hypotension(), HYPOTENSION_JOB, HYPO_WROTE).ok)
        self.assertFalse(accept_write(0, hypotension(), SIGNALS_JOB, HYPO_WROTE).ok)
        self.assertFalse(accept_write(0, written(), SIGNALS_JOB, KEPT).ok)

    def test_every_usage_phrase_is_in_its_closed_list(self) -> None:
        for accepted in (accept_usage(1, False), accept_usage(2, True), accept_usage(0, True)):
            self.assertIn(accepted.reason, USAGE_REASONS)
            self.assertEqual({}, dict(accepted.result))


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
