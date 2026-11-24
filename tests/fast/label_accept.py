"""The pass or fail rules that the Silver labels end-to-end run shares, as plain functions on the job's result line. Nothing here imports Spark or Docker.

The runner ``tests/e2e/silver_labels.py`` stays untracked, so each rule that decides from the job's result lives here, where
``tests/fast/test_silver_labels_acceptance.py`` tests it. The rules read hashes, snapshot IDs and pass flags and never ``outcome``: a result that says whether a
table was written changes when only a held-out row changes, so no result holds it.

A call of the job is read from its one result file (``--result``) and never from a stream: ``accepted`` returns the file of a call that exited 0 and names the
job, the mode and the branch that were asked for, ``refusal_ok`` judges the refusal file of a call that was to be refused (the closed refusal schema of
``tests/fast/silver_accept.py`` with exactly the wanted codes) and ``empty_exit_ok`` judges a call that writes no file (a bad call, a job that its deadline
stopped).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from spark.silver.label_checks import CHECK_NAMES, ICU, KIDNEY, MEASURE, TABLES
from tests.fast.silver_accept import EXIT_OK, EXIT_REFUSED, accept_refusal

JOB = "silver_labels"

CALL_KEYS = frozenset({"mode", "branch", "base_hash", "job", "tables"})
SEALED_KEYS = frozenset({"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "icu_measure", "checks_run"})


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def accepted(code: int, document: Any, mode: str, branch: str) -> dict[str, Any]:
    """The document of a call that exited 0 and whose file names this job, the mode that was asked for and the branch that was asked for, and an empty one for
    any other call. The rules below then judge it."""
    if code != EXIT_OK or not isinstance(document, Mapping):
        return {}
    return dict(document) if document.get("job") == JOB and document.get("mode") == mode and document.get("branch") == branch else {}


def refusal_ok(code: int, document: Any, mode: str, wanted: Sequence[str], expected: int = EXIT_REFUSED) -> bool:
    """Whether the call was the wanted refusal: it exited with ``expected`` and its file is the closed refusal document of this job in this mode, with exactly
    the wanted codes (each ``code: failed``) in order. The text of standard error is never read."""
    return accept_refusal(code, document, JOB, mode, wanted, expected).ok


def empty_exit_ok(code: int, present: bool, expected: int) -> bool:
    """Whether a call that writes no file (a bad call, ``usage``, exit 2; a job that its deadline stopped, exit 124) exited with ``expected`` and left its
    result folder empty, read before any parse."""
    return code == expected and not present


def wrote(result: Mapping[str, Any]) -> bool:
    """Whether the result is of a write: it names the checked hash and a write hash, which is the hash of the branch and is not the checked one."""
    checked, written, branch = result.get("checked_hash"), result.get("write_hash"), result.get("branch_hash")
    return named(checked) and named(written) and written == branch and written != checked


def kept(result: Mapping[str, Any]) -> bool:
    """Whether the result is of a call that wrote nothing: it names the checked hash, holds ``write_hash`` with no value and has the branch at that hash."""
    checked = result.get("checked_hash")
    return named(checked) and "write_hash" in result and result["write_hash"] is None and result.get("branch_hash") == checked


def sealed_keys_only(result: Mapping[str, Any]) -> bool:
    """Whether the result holds only the call's keys and the sealed keys: no ``outcome``, no count and no digest."""
    return set(result) - CALL_KEYS <= SEALED_KEYS


def snapshots_named(result: Mapping[str, Any]) -> bool:
    """Whether the result names a snapshot ID, a whole number, for each of the three tables and for no other."""
    snapshots = result.get("snapshots")
    return isinstance(snapshots, Mapping) and set(snapshots) == set(TABLES) and all(whole(value) for value in snapshots.values())


def all_passed(result: Mapping[str, Any]) -> bool:
    """Whether each table has its pass flag and every flag is the boolean true."""
    flags = result.get("passed")
    return isinstance(flags, Mapping) and set(flags) == set(TABLES) and all(flag is True for flag in flags.values())


def all_checks_ran(result: Mapping[str, Any]) -> bool:
    return result.get("checks_run") == list(CHECK_NAMES)


def build_ok(result: Mapping[str, Any]) -> bool:
    """Whether a first build is accepted: it wrote, every table has its snapshot and its true flag, every check ran and the result holds only sealed keys."""
    return wrote(result) and snapshots_named(result) and all_passed(result) and all_checks_ran(result) and sealed_keys_only(result)


def verify_ok(result: Mapping[str, Any], before: str) -> bool:
    """Whether a verify is accepted: it wrote nothing, the branch is at ``before``, every flag is true, every check ran and the keys are sealed."""
    return kept(result) and result.get("branch_hash") == before and all_passed(result) and all_checks_ran(result) and sealed_keys_only(result)


def rebuild_ok(result: Mapping[str, Any], built: Mapping[str, Any] | None) -> bool:
    """Whether a rebuild after the merge is accepted: it wrote nothing, every snapshot ID is the build's, every flag is true and the keys are sealed."""
    return kept(result) and snapshots_named(result) and result.get("snapshots") == built and all_passed(result) and sealed_keys_only(result)


def icu_change_ok(result: Mapping[str, Any], before: Mapping[str, Any], last_commits: Sequence[str]) -> bool:
    """Whether a rebuild after a change of the held-out patients' ``icu_days`` is accepted: it wrote one commit that holds the intensive care labels only, the
    measure and the kidney injury labels keep their snapshot IDs, the intensive care labels have a new one, every flag is true and the keys are sealed."""
    snapshots = result.get("snapshots")
    if not (wrote(result) and snapshots_named(result) and isinstance(snapshots, Mapping)):
        return False
    return (
        list(last_commits) == [ICU]
        and snapshots[MEASURE] == before.get(MEASURE)
        and snapshots[KIDNEY] == before.get(KIDNEY)
        and snapshots[ICU] != before.get(ICU)
        and all_passed(result)
        and sealed_keys_only(result)
    )
