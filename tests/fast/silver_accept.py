"""The pass or fail rules that the Silver end-to-end runs share, as plain functions on a job's output. Nothing here imports Spark or Docker.

The runners ``tests/e2e/silver_signals.py`` and ``tests/e2e/silver_hypotension.py`` stay untracked, so each rule that decides from a job's output lives here,
where ``tests/fast/test_silver_acceptance.py`` tests it. The rules read hashes and snapshot IDs and never ``outcome``: a result that says whether a table was
written changes when only a held-out row changes, so no result holds it.
"""

from __future__ import annotations

import builtins
import symtable
from collections.abc import Mapping, Sequence
from typing import Any

SIGNALS_JOB, HYPOTENSION_JOB = "silver_signals", "silver_hypotension"
TABLES = ("numeric_observations", "waveform_seconds")
ENVELOPE_KEYS = frozenset({"mode", "branch", "base_hash", "job", "tables"})
FACT_KEYS = frozenset({"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "checks_run"})
# The exact keys of the result line of each job, read at the command-line boundary: any other key, also a harmless one, fails the control.
RESULT_KEYS = {SIGNALS_JOB: ENVELOPE_KEYS | FACT_KEYS, HYPOTENSION_JOB: ENVELOPE_KEYS | FACT_KEYS | {"development_events_digest"}}
# The exact keys of the three maps in the result of each job: the tables that it writes, the sources that it reads and the tables with a pass flag.
TABLE_KEYS = {
    SIGNALS_JOB: {
        "snapshots": TABLES,
        "source_snapshots": ("numeric_observations", "waveform_seconds", "tracks"),
        "passed": TABLES,
    },
    HYPOTENSION_JOB: {
        "snapshots": ("art_mbp_1s", "hypotension_events"),
        "source_snapshots": ("waveform_seconds", "clinical_cases", "patient_split"),
        "passed": ("art_mbp_1s", "hypotension_events"),
    },
}


def refusal_matches(found: Sequence[str], wanted: Sequence[str]) -> bool:
    """Whether the lines that a job wrote are exactly the wanted lines, each complete (``code: failed``), in order."""
    return list(found) == list(wanted)


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def sealed(result: Mapping[str, Any], job: str) -> bool:
    """Whether the result has exactly the keys that the job writes, at the top and in each map of tables, so that a field that the job does not write
    (``outcome`` or any other) fails the control."""
    wanted = RESULT_KEYS.get(job)
    if wanted is None or set(result) != wanted:
        return False
    return all(isinstance(result[name], Mapping) and set(result[name]) == set(keys) for name, keys in TABLE_KEYS[job].items())


def wrote(result: Mapping[str, Any], job: str) -> bool:
    """Whether the result is of a write: it is sealed, names the checked hash and a write hash, which is the hash of the branch and is not the checked one."""
    checked, written, branch = result.get("checked_hash"), result.get("write_hash"), result.get("branch_hash")
    return sealed(result, job) and named(checked) and named(written) and written == branch and written != checked


def kept(result: Mapping[str, Any], job: str) -> bool:
    """Whether the result is of a call that wrote nothing: it is sealed, names the checked hash, holds ``write_hash`` with no value and has the branch there."""
    checked = result.get("checked_hash")
    return sealed(result, job) and named(checked) and result["write_hash"] is None and result.get("branch_hash") == checked


def catalog_matches(result: Mapping[str, Any], head: str, snapshots: Mapping[str, int | None]) -> bool:
    """Whether the hashes and the snapshot IDs that the result reports are those that Nessie holds: ``head`` is the hash of the branch that the catalog gives
    and ``snapshots`` the snapshot ID that the catalog gives for each table. The branch hash is the head, the write hash is the head or has no value, and
    every reported snapshot ID is a whole number that equals the catalog's, for the same tables."""
    reported = result.get("snapshots")
    if not isinstance(reported, Mapping) or not reported or not named(head) or result.get("branch_hash") != head:
        return False
    if result.get("write_hash") not in (None, head):
        return False
    return set(reported) == set(snapshots) and all(
        isinstance(value, int) and not isinstance(value, bool) and snapshots[name] == value for name, value in reported.items()
    )


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
    return wrote(result, SIGNALS_JOB) and isinstance(old, int) and now[1] != old and all(flags.get(table) is True for table in TABLES)


def undefined_names(source: str) -> list[str]:
    """The sorted names that a script reads and nothing defines. A script defines a name when it assigns, imports or defines it at the top level or sets it
    under a ``global`` statement in a function, and Python defines the built-in names.

    A script that does not compile raises ``SyntaxError``. The embedded stand-in scripts of the runners run inside a container, where a name that is not defined
    raises ``NameError`` only when the line runs, so the check reads their symbol tables before any container starts."""
    compile(source, "<script>", "exec")
    top = symtable.symtable(source, "<script>", "exec")
    defined = set(dir(builtins)) | {"__file__"}
    defined |= {symbol.get_name() for symbol in top.get_symbols() if symbol.is_assigned() or symbol.is_imported()}
    found: set[str] = set()

    def walk(table: symtable.SymbolTable) -> None:
        for symbol in table.get_symbols():
            if symbol.is_declared_global() and symbol.is_assigned():
                defined.add(symbol.get_name())
            if symbol.is_global() and symbol.is_referenced():
                found.add(symbol.get_name())
        for child in table.get_children():
            walk(child)

    walk(top)
    return sorted(found - defined)
