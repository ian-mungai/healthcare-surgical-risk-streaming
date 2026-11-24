"""The pass or fail rules that the Silver end-to-end runs share, as plain functions on a job's output. Nothing here imports Spark or Docker.

The runners ``tests/e2e/silver_signals.py`` and ``tests/e2e/silver_hypotension.py`` stay untracked, so each rule that decides from a job's output lives here,
where ``tests/fast/test_silver_acceptance.py`` tests it. The rules read hashes and snapshot IDs and never ``outcome``: a result that says whether a table was
written changes when only a held-out row changes, so no result holds it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

TABLES = ("numeric_observations", "waveform_seconds")


def refusal_matches(found: Sequence[str], wanted: Sequence[str]) -> bool:
    """Whether the lines that a job wrote are exactly the wanted lines, each complete (``code: failed``), in order."""
    return list(found) == list(wanted)


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def wrote(result: Mapping[str, Any]) -> bool:
    """Whether the result is of a write: it names the checked hash and a write hash, which is the hash of the branch and is not the checked one."""
    checked, written, branch = result.get("checked_hash"), result.get("write_hash"), result.get("branch_hash")
    return named(checked) and named(written) and written == branch and written != checked


def kept(result: Mapping[str, Any]) -> bool:
    """Whether the result is of a call that wrote nothing: it names the checked hash, holds ``write_hash`` with no value and has the branch at that hash."""
    checked = result.get("checked_hash")
    return named(checked) and "write_hash" in result and result["write_hash"] is None and result.get("branch_hash") == checked


def moved_null_ok(result: Mapping[str, Any], before: Mapping[str, Any]) -> bool:
    """Whether a rebuild after a null moved inside one second is accepted: it wrote, it names both snapshot IDs, the waveform snapshot ID is another than
    ``before`` holds and both pass flags are true."""
    snapshots, flags = result.get("snapshots"), result.get("passed")
    if not isinstance(snapshots, Mapping) or not isinstance(flags, Mapping):
        return False
    now = [snapshots.get(table) for table in TABLES]
    if not all(isinstance(snapshot, int) and not isinstance(snapshot, bool) for snapshot in now):
        return False
    old = before.get("waveform_seconds")
    return wrote(result) and isinstance(old, int) and now[1] != old and all(flags.get(table) is True for table in TABLES)
