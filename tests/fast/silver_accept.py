"""The pass or fail rules that the Silver end-to-end runs share, as plain functions on a job's result file. Nothing here imports Spark or Docker.

The runners ``tests/e2e/silver_signals.py`` and ``tests/e2e/silver_hypotension.py`` stay untracked, so each rule that decides from a job's output lives here,
where ``tests/fast/test_silver_acceptance.py`` tests it. A runner asks each job for one result file (``--result``) and reads that file only, never a line of
the job's standard output or standard error. The file is accepted when all of these hold:

- it holds one JSON object and nothing else, with no key twice and no ``NaN`` (``read_result``);
- its schema is closed: the exact keys and the exact types at every level, every pass flag a Boolean, every snapshot ID a whole number, and ``checks_run``
  the job's own list of check names, imported from the job's module and never copied (``sealed``);
- its ``branch`` is the branch that the runner asked for, compared before anything else (``Catalog.branch``, ``branch_is``);
- its hashes and snapshot IDs are the catalog state that the runner captured from Nessie, before the call (the checked hash, the base hash and the snapshot
  IDs of the sources) and after it (the branch hash and the snapshot IDs of the tables) (``Catalog``, ``catalog_matches``).

A refusal is judged by the exit code and by the refusal document in the same file (``refused``), and never by the text of standard error. The rules read
hashes and snapshot IDs and never ``outcome``: a result that says whether a table was written changes when only a held-out row changes, so no result holds it.

A runner records the fields of a file only after the file passed its rule (``evidence``, ``refusal_evidence``). A file that failed gives one fixed phrase
from ``REASONS`` and ``REFUSAL_REASONS``: the rule that failed, never a value of the file. A call that exits with a usage error passes only when its result
folder held nothing, checked before any parse (``result_present``, ``usage_refused``).
"""

from __future__ import annotations

import builtins
import json
import symtable
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spark.jobs.silver_hypotension import CHECK_NAMES as HYPOTENSION_CHECKS
from spark.jobs.silver_signals import CHECK_NAMES as SIGNALS_CHECKS
from spark.silver.tables import NAMESPACE

SIGNALS_JOB, HYPOTENSION_JOB = "silver_signals", "silver_hypotension"
TABLES = ("numeric_observations", "waveform_seconds")
MODES = ("build", "verify")
ENVELOPE_KEYS = frozenset({"mode", "branch", "base_hash", "job", "tables"})
FACT_KEYS = frozenset({"checked_hash", "write_hash", "branch_hash", "source_snapshots", "snapshots", "passed", "checks_run"})
# The exact keys of the result of each job: any other key, also a harmless one, fails the control.
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
# The names of the checks that each job runs, taken from the module of the job.
CHECKS = {SIGNALS_JOB: SIGNALS_CHECKS, HYPOTENSION_JOB: HYPOTENSION_CHECKS}
# The tables that each job names in its result, in order.
TABLE_LISTS = {job: [f"{NAMESPACE}.{table}" for table in keys["snapshots"]] for job, keys in TABLE_KEYS.items()}
REFUSAL_KEYS = frozenset({"job", "mode", "refusal"})
AS_WRITE, AS_KEEP = "write", "keep"
# The fixed phrases that say which rule a result failed: the closed list, with no value of the file in any of them.
NO_FILE = "no result file"
NO_CATALOG = "the catalog was not read"
OTHER_BRANCH = "the branch is not the requested one"
NOT_CLOSED = "the schema is not the job's closed schema"
OFF_CATALOG = "a hash or a snapshot differs from the catalog"
NOT_A_WRITE = "the result is not a write"
NOT_A_KEEP = "the result is not a call that wrote nothing"
REASONS = (NO_FILE, NO_CATALOG, OTHER_BRANCH, NOT_CLOSED, OFF_CATALOG, NOT_A_WRITE, NOT_A_KEEP)
REFUSAL_NOT_CLOSED = "the refusal document is not the closed refusal schema"
REFUSAL_OTHER_CODES = "the refusal codes are not the wanted ones"
REFUSAL_REASONS = (NO_FILE, REFUSAL_NOT_CLOSED, REFUSAL_OTHER_CODES)


@dataclass(frozen=True, slots=True)
class Catalog:
    """What the runner read from Nessie around one call.

    ``branch`` is the branch that the runner asked the job for (``--branch``): the result must name it. ``checked`` is the hash that the job must have
    checked: the head of the base before a build, the head of the branch before a verify. ``base`` is the head of the base before a build and ``None`` for a
    verify. ``head`` is the head of the branch after the call. ``sources`` holds the snapshot ID of each source table at ``checked`` and ``snapshots`` the
    snapshot ID of each table that the job writes, on the branch at ``head``.
    """

    branch: str
    checked: str
    base: str | None
    head: str
    sources: Mapping[str, int | None]
    snapshots: Mapping[str, int | None]


def refusal_matches(found: Sequence[str], wanted: Sequence[str]) -> bool:
    """Whether the lines that a job wrote are exactly the wanted lines, each complete (``code: failed``), in order."""
    return list(found) == list(wanted)


def named(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def whole(value: Any) -> bool:
    """Whether the value is a whole number and not a flag (a Boolean is an ``int`` in Python)."""
    return type(value) is int


def flag(value: Any) -> bool:
    return type(value) is bool


def no_repeat(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    if len({key for key, _ in pairs}) != len(pairs):
        raise ValueError("a key twice")
    return dict(pairs)


def no_constant(name: str) -> Any:
    raise ValueError(name)


def read_result(path: Path) -> dict[str, Any] | None:
    """The document in the result file, or ``None``. The file must exist and hold exactly one JSON object, with no key twice and no ``NaN`` or infinity: two
    documents, a line of other text, a list or a file that is not there is no result."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_repeat, parse_constant=no_constant)
    except OSError, ValueError, RecursionError:
        return None
    return document if isinstance(document, dict) else None


def maps_ok(result: Mapping[str, Any], job: str) -> bool:
    """The three maps of tables have the exact keys of the job, a whole number for each snapshot ID and a Boolean for each pass flag."""
    for name, keys in TABLE_KEYS[job].items():
        held = result[name]
        if not isinstance(held, Mapping) or set(held) != set(keys):
            return False
        if not all(flag(value) if name == "passed" else whole(value) for value in held.values()):
            return False
    return True


def sealed(result: Any, job: str) -> bool:
    """Whether the result is of the closed schema that the job writes: the exact keys and the exact types at every level, a Boolean for each pass flag, the
    job's own table names and its own list of check names. A field that the job does not write (``outcome`` or any other), a flag of ``1`` or a check name
    that the job does not run fails the control."""
    wanted = RESULT_KEYS.get(job)
    if wanted is None or not isinstance(result, Mapping) or set(result) != wanted:
        return False
    build = result["mode"] == "build"
    return (
        result["mode"] in MODES
        and named(result["branch"])
        and result["job"] == job
        and (named(result["base_hash"]) if build else result["base_hash"] is None)
        and type(result["tables"]) is list
        and result["tables"] == TABLE_LISTS[job]
        and named(result["checked_hash"])
        and named(result["branch_hash"])
        and (result["write_hash"] is None or named(result["write_hash"]))
        and type(result["checks_run"]) is list
        and result["checks_run"] == list(CHECKS[job])
        and maps_ok(result, job)
        and (whole(result["development_events_digest"]) if job == HYPOTENSION_JOB else True)
    )


def refused(document: Any, job: str, mode: str, wanted: Sequence[str]) -> bool:
    """Whether the document is the refusal of this job in this mode with exactly the wanted codes, each complete, in order: the keys are ``job``, ``mode``
    and ``refusal``, and no other."""
    return (
        isinstance(document, Mapping)
        and set(document) == REFUSAL_KEYS
        and document["job"] == job
        and mode in MODES
        and document["mode"] == mode
        and type(document["refusal"]) is list
        and all(isinstance(line, str) for line in document["refusal"])
        and refusal_matches(document["refusal"], wanted)
    )


def refusal_rejection(document: Any, job: str, mode: str, wanted: Sequence[str]) -> str | None:
    """The fixed phrase of the rule that a refusal document fails, or ``None`` when ``refused`` passes it. A phrase is one of ``REFUSAL_REASONS``: it holds
    no code or other value of the document."""
    if not isinstance(document, Mapping):
        return NO_FILE
    closed = (
        set(document) == REFUSAL_KEYS
        and document["job"] == job
        and mode in MODES
        and document["mode"] == mode
        and type(document["refusal"]) is list
        and all(isinstance(line, str) for line in document["refusal"])
    )
    if not closed:
        return REFUSAL_NOT_CLOSED
    return None if refusal_matches(document["refusal"], wanted) else REFUSAL_OTHER_CODES


def refusal_evidence(document: Any, job: str, mode: str, wanted: Sequence[str]) -> str:
    """The codes of a refusal for the record, written only when the document is the wanted refusal (so the codes are the wanted ones). A document that failed
    gives its fixed phrase and no value of the file."""
    reason = refusal_rejection(document, job, mode, wanted)
    return f"refusal rejected: {reason}" if reason is not None else f"refusal {document['refusal']}"


def snapshots_match(reported: Any, held: Mapping[str, int | None]) -> bool:
    """Whether the reported map has the catalog's tables and, for each, the same whole snapshot ID. A table with no ID in the catalog never matches."""
    return (
        isinstance(reported, Mapping)
        and set(reported) == set(held)
        and bool(held)
        and all(whole(value) and held[name] == value for name, value in reported.items())
    )


def branch_is(result: Any, catalog: Catalog) -> bool:
    """Whether the result names the branch that was asked for. A result of another branch fails here, whatever its hashes and snapshot IDs are."""
    return isinstance(result, Mapping) and named(catalog.branch) and result.get("branch") == catalog.branch


def catalog_matches(result: Any, catalog: Catalog) -> bool:
    """Whether the result names the requested branch, compared first, and every hash and every snapshot ID that it reports is the one that the catalog held:
    the checked hash and the base hash before the call, the branch hash after it (and the write hash, which is the branch hash or has no value), and the
    snapshot IDs of the sources and of the tables."""
    if not branch_is(result, catalog) or not all(key in result for key in ("checked_hash", "base_hash", "branch_hash", "write_hash")):
        return False
    return (
        named(catalog.checked)
        and named(catalog.head)
        and result["checked_hash"] == catalog.checked
        and result["base_hash"] == catalog.base
        and (catalog.base is None or named(catalog.base))
        and result["branch_hash"] == catalog.head
        and result["write_hash"] in (None, catalog.head)
        and snapshots_match(result.get("source_snapshots"), catalog.sources)
        and snapshots_match(result.get("snapshots"), catalog.snapshots)
    )


def rejection(document: Any, job: str, catalog: Catalog | None, outcome: str) -> str | None:
    """The fixed phrase of the first rule that the document fails as the ``outcome`` (``AS_WRITE`` or ``AS_KEEP``) of ``job``, or ``None`` when it passes. The
    rules run in this order: a file that is there, a catalog that was read, the requested branch, the closed schema, the hashes and snapshot IDs of the catalog
    and then the outcome. A phrase is one of ``REASONS``: it names the rule and holds no value of the file."""
    if not isinstance(document, Mapping):
        return NO_FILE
    if catalog is None:
        return NO_CATALOG
    if not branch_is(document, catalog):
        return OTHER_BRANCH
    if not sealed(document, job):
        return NOT_CLOSED
    if not catalog_matches(document, catalog):
        return OFF_CATALOG
    if outcome == AS_WRITE:
        return None if named(document["write_hash"]) and document["write_hash"] == document["branch_hash"] != document["checked_hash"] else NOT_A_WRITE
    return None if document["write_hash"] is None and document["branch_hash"] == document["checked_hash"] else NOT_A_KEEP


def wrote(result: Any, job: str, catalog: Catalog) -> bool:
    """Whether the result is of a write: it names the requested branch, it is sealed, the catalog holds what it reports, and its write hash is the branch hash
    and not the checked hash."""
    return rejection(result, job, catalog, AS_WRITE) is None


def kept(result: Any, job: str, catalog: Catalog) -> bool:
    """Whether the result is of a call that wrote nothing: it names the requested branch, it is sealed, the catalog holds what it reports, ``write_hash`` has no
    value and the branch is at the checked hash."""
    return rejection(result, job, catalog, AS_KEEP) is None


def evidence(document: Any, job: str, catalog: Catalog | None, outcome: str, names: Sequence[str]) -> str:
    """The named fields of a result for the record, written only when the result passed its rule. A result that failed gives its fixed phrase and no value of
    the file: not a field, a count of the file or a refusal code."""
    reason = rejection(document, job, catalog, outcome)
    if reason is not None:
        return f"result rejected: {reason}"
    return ", ".join(f"{name.replace('_', ' ')} {document[name]}" for name in names)


def result_present(folder: Path) -> bool:
    """Whether the result folder of a call holds anything at all (a file of any name, a link or a folder), read before any parse and before any cleanup. A
    folder that cannot be listed counts as holding something."""
    try:
        return any(True for _ in folder.iterdir())
    except OSError:
        return True


def usage_refused(code: int, present: bool) -> bool:
    """Whether a call that was given a bad argument passed: it exits 2 and its result folder was empty. A usage exit with a file there, of any content (``[]``
    too), fails."""
    return code == 2 and not present


def moved_null_ok(result: Any, catalog: Catalog, before: Mapping[str, Any]) -> bool:
    """Whether a rebuild after a null moved inside one second is accepted: it wrote, the catalog holds its hashes and snapshot IDs, the waveform snapshot ID is
    another than ``before`` holds and both pass flags are true."""
    old = before.get("waveform_seconds")
    if not wrote(result, SIGNALS_JOB, catalog) or not whole(old):
        return False
    return result["snapshots"]["waveform_seconds"] != old and all(result["passed"][table] is True for table in TABLES)


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
