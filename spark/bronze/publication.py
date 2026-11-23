"""Publication of a Bronze branch: the checks run at one hash of the branch, and only the output of the job that passed them at that hash is published.

The base reference is assigned to the checked commit itself.

The order, with nothing written to the base before the last step:

1. the branch must be a branch, the base must be another name, and the branch must hold every table that the job wrote, at the snapshot it wrote (when the
   caller knows it), at its head;
2. the head of the branch is read and the checks run at exactly that hash (``run_checks`` reads a catalog pinned to it);
3. the head is read again. A branch that moved while it was checked is not published: the checks judged another hash;
4. a base whose head is the checked hash is already published: the run ends as ``unchanged``;
5. the base head is read and must be a commit in the log of the checked hash: the branch was made on the base as it is now, so the checked commit is a fast
   forward of the base. A base that is not an ancestor blocks the publication (``base_moved``) and the job is rerun on a fresh branch;
6. the entries of the base head and of the checked hash are compared by their whole content: the keys that differ must be exactly the tables that the job wrote,
   plus the namespaces that hold them if they are missing from the base. Any other change, any edit or removal of an existing namespace, and a table of the job
   that the base already holds as the branch has it, are refused;
7. the base is assigned to the checked commit, expecting the head that was read (Nessie 0.110.1, API v2, ``PUT /trees/{name}@{hash}``:
   ``NessieClient.assign_branch``). Nessie applies it at once or refuses with a conflict, because the base is no longer at the expected hash: that is
   ``base_moved``, and the base keeps the head that it has. The published commit is the checked commit itself, so what readers see is what was checked.

An assignment that failed or timed out may still have been applied, so the base head is read once: the checked hash means published, the head that was read
means nothing happened, and any other head means the base moved. The request is never sent twice by one run, and a rerun that finds the checked hash on the
base is ``unchanged``. No marker, no history search and no merge commit are needed, because the assignment is atomic and repeating it changes nothing.

The functions need only the operations of ``PublishOps``, so the sequence runs on fakes in the fast tests. ``NessiePublisher`` is those operations over
Nessie's HTTP API.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import quote

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError, Reference
from scripts.stack.tags import paged
from spark.bronze.checks import Verdict
from spark.bronze_clinical_lab_run import HISTORY_PAGE_LIMIT, HISTORY_PAGE_SIZE, NessieAdapter

GUARD_BRANCH_MOVED = "branch_moved"
GUARD_BASE_MOVED = "base_moved"
PUBLISHED, UNCHANGED, BLOCKED = "published", "unchanged", "blocked"
TABLE_KIND = "ICEBERG_TABLE"
NO_SNAPSHOT = -1


class PublicationError(RuntimeError):
    """The branch was not published and the base is as it was, or the answer of Nessie is not the one that was asked for. The message never holds a data
    value."""


@dataclass(frozen=True, slots=True)
class Entry:
    """One content of a Nessie reference: its kind, its whole content as canonical JSON and, for a table, the location of its metadata file and its snapshot."""

    kind: str
    location: str | None = None
    snapshot_id: int | None = None
    content: str = ""


class PublishOps(Protocol):
    """What publication needs from Nessie. ``reference`` arguments are ``name@hash``."""

    def is_branch(self, name: str) -> bool: ...

    def head(self, branch: str) -> str: ...

    def entries(self, reference: str) -> dict[str, Entry]: ...

    def descends(self, branch: str, branch_hash: str, base_hash: str) -> bool: ...

    def assign(self, base: str, expected_base_hash: str, branch: str, branch_hash: str) -> str: ...


@dataclass(slots=True)
class Publication:
    """The outcome: ``published`` (this run assigned the base), ``unchanged`` (the base already was the checked commit) or ``blocked`` (a guard stopped it)."""

    status: str
    branch: str
    base: str
    branch_hash: str
    base_hash_before: str | None = None
    published_hash: str | None = None
    snapshots: dict[str, int] = field(default_factory=dict)
    blocked_by: list[str] = field(default_factory=list)
    verdict: Verdict | None = None


def verify(ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str) -> tuple[str, Verdict]:
    """The head of the branch and the verdict of the checks at it. The branch must still have that head when the checks end."""
    if not ops.is_branch(branch):
        raise PublicationError(f"{branch} is not a branch")
    checked = ops.head(branch)
    return checked, run_checks(checked)


def written_problem(entries: Mapping[str, Entry], expected: Mapping[str, int | None], reference: str) -> str | None:
    """Why ``reference`` does not hold the tables of the job at the snapshots it wrote, or ``None``."""
    for key, snapshot in expected.items():
        found = entries.get(key)
        if found is None or found.kind != TABLE_KIND:
            return f"{reference} does not hold the table {key}"
        if snapshot is not None and found.snapshot_id != snapshot:
            return f"{reference} holds another snapshot of {key} than the job wrote"
    return None


def parent_namespaces(keys: Mapping[str, Any]) -> set[str]:
    """The dotted names of the namespaces that hold the tables ``keys``: every proper prefix of a table's name."""
    found: set[str] = set()
    for key in keys:
        parts = key.split(".")
        found.update(".".join(parts[:length]) for length in range(1, len(parts)))
    return found


def output_problem(base_entries: Mapping[str, Entry], branch_entries: Mapping[str, Entry], expected: Mapping[str, int | None]) -> str | None:
    """Why the change from the base to the branch is not exactly the output of the job, or ``None``.

    The keys whose entry differs, or that only one side holds, must be the job's tables and the namespaces that hold them and are missing from the base. An
    edit or a removal of an existing namespace is a change like any other. A table of the job that the base already holds as it is, was not written by this job.
    """
    changed = {key for key in base_entries.keys() | branch_entries.keys() if base_entries.get(key) != branch_entries.get(key)}
    created = {key for key in parent_namespaces(expected) if key not in base_entries and key in branch_entries}
    other = sorted(key for key in changed if key not in expected and key not in created)
    unchanged = sorted(key for key in expected if key not in changed)
    if other:
        return f"the branch changes {', '.join(other[:5])}, which the job did not write: nothing was published"
    if unchanged:
        return f"the table {unchanged[0]} is on the base as the branch holds it, so this job did not write it: nothing was published"
    return None


def snapshots_of(entries: Mapping[str, Entry], expected: Mapping[str, int | None]) -> dict[str, int]:
    return {key: snapshot for key in expected if (snapshot := entries[key].snapshot_id) is not None}


def assigned(ops: PublishOps, base: str, expected_base: str, branch: str, checked: str) -> bool:
    """Send the assignment once. ``True`` when the base is now the checked commit, ``False`` when the base moved (it keeps the head that it has).

    A conflict is a base that is not at the expected hash. A failure with no answer is settled by reading the base head once: the checked hash is a
    publication, the head that was read means the assignment did not happen, and another head is a base that moved.
    """
    try:
        answered = ops.assign(base, expected_base, branch, checked)
    except NessieConflict:
        return False
    except NessieError as error:
        found = ops.head(base)
        if found == checked:
            return True
        if found == expected_base:
            raise PublicationError(
                f"the assignment of {base} to the checked commit did not finish ({type(error).__name__}) and was not applied: nothing changed"
            ) from None
        return False
    if answered != checked:
        raise PublicationError(f"Nessie answered the assignment of {base} with another commit than the checked one: read the history of {base}")
    return True


def publish(ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str, base: str, expected: Mapping[str, int | None]) -> Publication:
    """Check the branch and, only when every check passed at its head, assign ``base`` to that head.

    ``expected`` maps the Nessie content key of each table that the job wrote to the snapshot it wrote, or to ``None`` where the caller does not know it.
    """
    if branch == base:
        raise PublicationError("the base of a publication cannot be the branch that is published")
    if not ops.is_branch(base):
        raise PublicationError(f"{base} is not a branch")
    if not ops.is_branch(branch):
        raise PublicationError(f"{branch} is not a branch")
    started = ops.head(branch)
    branch_entries = ops.entries(f"{branch}@{started}")
    problem = written_problem(branch_entries, expected, branch)
    if problem:
        raise PublicationError(problem)
    checked, result = verify(ops, run_checks, branch)
    outcome = Publication(BLOCKED, branch, base, checked, verdict=result)
    if not result.passed:
        outcome.blocked_by = [guard for guard, passed in result.guards.items() if not passed]
        return outcome
    if ops.head(branch) != checked or checked != started:
        outcome.blocked_by = [GUARD_BRANCH_MOVED]
        return outcome
    expected_base = ops.head(base)
    outcome.base_hash_before = expected_base
    if expected_base == checked:
        outcome.status, outcome.published_hash, outcome.snapshots = UNCHANGED, checked, snapshots_of(branch_entries, expected)
        return outcome
    if not ops.descends(branch, checked, expected_base):
        outcome.blocked_by = [GUARD_BASE_MOVED]
        return outcome
    problem = output_problem(ops.entries(f"{base}@{expected_base}"), branch_entries, expected)
    if problem:
        raise PublicationError(problem)
    if not assigned(ops, base, expected_base, branch, checked):
        outcome.blocked_by = [GUARD_BASE_MOVED]
        return outcome
    outcome.status, outcome.published_hash, outcome.snapshots = PUBLISHED, checked, snapshots_of(branch_entries, expected)
    return outcome


class NessiePublisher(NessieAdapter):
    """``PublishOps`` over Nessie's HTTP API. ``is_branch`` and the head of a branch are the Bronze jobs' own (``NessieAdapter``)."""

    def __init__(self, client: NessieClient) -> None:
        super().__init__(client)

    def entries(self, reference: str) -> dict[str, Entry]:
        """The contents of ``reference`` (a name or ``name@hash``) by dotted key, each with its whole content. A table needs a location and a snapshot ID."""
        found: dict[str, Entry] = {}
        for item in paged(self.client, f"/trees/{quote(reference, safe='@')}/entries", "entries", {"content": "true"}):
            elements = (item.get("name") or {}).get("elements") if isinstance(item.get("name"), dict) else None
            if not isinstance(elements, list) or not elements or not all(isinstance(part, str) and part for part in elements):
                raise NessieError("Nessie answered with an entry that has no name made of text elements")
            kind = item.get("type")
            if not isinstance(kind, str):
                raise NessieError("Nessie answered with an entry that has no type")
            content = item.get("content")
            text = json.dumps(content, sort_keys=True) if isinstance(content, dict) else ""
            if kind != TABLE_KIND:
                found[".".join(elements)] = Entry(kind, content=text)
                continue
            location = content.get("metadataLocation") if isinstance(content, dict) else None
            snapshot = content.get("snapshotId") if isinstance(content, dict) else None
            if not isinstance(location, str) or not location or isinstance(snapshot, bool) or not isinstance(snapshot, int):
                raise NessieError(f"the table {'.'.join(elements)} at {reference} has no metadata location and snapshot ID in Nessie's answer")
            found[".".join(elements)] = Entry(kind, location, None if snapshot == NO_SNAPSHOT else snapshot, text)
        return found

    def log(self, reference: str) -> Iterator[tuple[str, str]]:
        """The ``(hash, message)`` of each commit in the log of ``reference`` (a name or ``name@hash``), newest first, page by page.

        A log that is not in the expected form, or that is longer than ``HISTORY_PAGE_LIMIT`` pages, is an error and never taken for a log with no commit.
        """
        token = ""
        for _ in range(HISTORY_PAGE_LIMIT):
            query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
            page = self.client.request("GET", f"/trees/{quote(reference, safe='@')}/history", query=query)
            entries = page.get("logEntries") if isinstance(page, dict) else None
            if not isinstance(entries, list) or not isinstance(page.get("hasMore"), bool):
                raise NessieError("Nessie's answer for the log is not in the expected form")
            for entry in entries:
                meta = entry.get("commitMeta") if isinstance(entry, dict) else None
                message, commit = (meta.get("message"), meta.get("hash")) if isinstance(meta, dict) else (None, None)
                if not isinstance(message, str) or not isinstance(commit, str):
                    raise NessieError("Nessie's answer for the log is not in the expected form")
                yield commit, message
            token = page.get("token") or ""
            if not page["hasMore"]:
                return
            if not token:
                raise NessieError("Nessie's answer for the log has more pages and names no token")
        raise NessieError("the log is too long to search")

    def descends(self, branch: str, branch_hash: str, base_hash: str) -> bool:
        """Whether ``base_hash`` is a commit in the log of the branch at ``branch_hash``: the branch was made on the base as it was then."""
        return any(commit == base_hash for commit, _ in self.log(f"{branch}@{branch_hash}"))

    def assign(self, base: str, expected_base_hash: str, branch: str, branch_hash: str) -> str:
        """Assign ``base`` to ``branch_hash``, expecting the head ``expected_base_hash`` (``NessieClient.assign_branch``). Returns the hash that Nessie says."""
        return self.client.assign_branch(base, expected_base_hash, Reference("BRANCH", branch, branch_hash)).hash
