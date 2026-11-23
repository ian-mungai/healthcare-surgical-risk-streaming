"""Publication of a Bronze branch: the checks run at one hash of the branch, and only the output of the job that passed them at that hash is merged, once.

The order, with nothing written to the base before the last step:

1. the branch must be a branch, the base must be another name, and the branch must hold every table that the job wrote, at the snapshot it wrote (when the
   caller knows it), at its head;
2. the head of the branch is read and the checks run at exactly that hash (``run_checks`` reads a catalog pinned to it);
3. the head is read again. A branch that moved while it was checked is not merged: the checks judged another hash;
4. a merge of this branch at this hash that the history of the base already shows ends the run as ``unchanged``, after its tables are compared with the
   checked ones;
5. the base head must be an ancestor of the checked hash: the branch was made on the base as it is now. Nessie merges from the common ancestor of the two to the
   checked source, so a base that changed since would give a mixed result. A base that moved blocks the publication (``base_moved``) before anything is sent,
   and the job is rerun on a fresh branch;
6. the entries of the base head and of the checked hash are compared by their whole content: the keys that differ must be exactly the tables that the job wrote,
   plus the namespaces that are their parents. A branch that carries another change, or that leaves one of the job's tables as the base already has it, is
   refused;
7. the run makes the marker (a tag named by the checked hash). Only the run that made it sends the request: a marker that is already there belongs to another
   publisher, or to a request with no known outcome, and ends the run as ``MergeOutcomeOpen`` with nothing sent. The history is read once more after the
   marker is made, so a merge that landed meanwhile is reported and not sent again;
8. the branch is merged **at the checked hash** into the base, expecting the head that was read. The request is sent once. A refusal because the base moved
   leaves the base as it was and removes the marker;
9. the tables on the base at the merge are compared with the tables on the branch at the checked hash (the whole content of each), so a merge that did not
   bring exactly the checked tables is an error, and so is one that was recovered from the history. The marker is then removed.

A merge request that failed or timed out may still have been applied, so the history of the base is searched once: a merge found is the result, and none found
is ``MergeOutcomeOpen``, which leaves the marker. A later run reports the open outcome again, without sending, until the history shows the merge and the
operator has removed the marker, or the merge is found and the run removes it.

The functions need only the operations of ``PublishOps``, so the sequence runs on fakes in the fast tests. ``NessiePublisher`` is those operations over
Nessie's HTTP API.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import quote

from scripts.stack.nessie import HASH, NessieClient, NessieConflict, NessieError
from scripts.stack.tags import paged
from spark.bronze.checks import Verdict
from spark.bronze_clinical_lab_run import HISTORY_PAGE_LIMIT, HISTORY_PAGE_SIZE, NessieAdapter

GUARD_BRANCH_MOVED = "branch_moved"
GUARD_BASE_MOVED = "base_moved"
BRANCH_LINE = "bronze-branch: "
SOURCE_LINE = "bronze-source: "
PENDING_PREFIX = "bronze-merge-pending-"
PENDING_HASH_CHARACTERS = 32
BRANCH_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
MERGED, UNCHANGED, BLOCKED = "merged", "unchanged", "blocked"
NAMESPACE_KIND, TABLE_KIND = "NAMESPACE", "ICEBERG_TABLE"
NO_SNAPSHOT = -1


class PublicationError(RuntimeError):
    """The branch was not published and the base is as it was, or a merge is not what was checked. The message names the cause and never holds a data value."""


class MergeOutcomeOpen(PublicationError):
    """A merge request was sent and the history of the base does not show it. Read the history of the base before anything is sent again."""


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

    def merge_commit(self, base: str, branch: str, branch_hash: str) -> str | None: ...

    def merge(self, branch: str, base: str, expected_base_hash: str, branch_hash: str, message: str) -> str: ...

    def descends(self, branch: str, branch_hash: str, base_hash: str) -> bool: ...

    def set_pending(self, name: str, branch: str, branch_hash: str) -> bool: ...

    def clear_pending(self, name: str) -> None: ...


@dataclass(slots=True)
class Publication:
    """The outcome: ``merged`` (this run merged), ``unchanged`` (an earlier run had merged it) or ``blocked`` (a guard stopped it and the base is unchanged)."""

    status: str
    branch: str
    base: str
    branch_hash: str
    base_hash_before: str | None = None
    merged_hash: str | None = None
    snapshots: dict[str, int] = field(default_factory=dict)
    blocked_by: list[str] = field(default_factory=list)
    verdict: Verdict | None = None


def merge_message(branch: str, branch_hash: str) -> str:
    """The message of the merge commit. Its last two lines name the branch and the checked hash, which is how a rerun and a timed-out merge find it."""
    if BRANCH_NAME.fullmatch(branch) is None or HASH.fullmatch(branch_hash) is None:
        raise PublicationError("a branch name is letters, digits, dots, underscores, dashes and slashes, and a hash is 64 lower-case hex digits")
    return f"Bronze: merge the checked tables of {branch}\n{BRANCH_LINE}{branch}\n{SOURCE_LINE}{branch_hash}"


def pending_name(branch_hash: str) -> str:
    """The name of the tag that marks a merge request of this checked hash whose outcome is not known."""
    if HASH.fullmatch(branch_hash) is None:
        raise PublicationError("a hash is 64 lower-case hex digits")
    return f"{PENDING_PREFIX}{branch_hash[:PENDING_HASH_CHARACTERS]}"


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

    The keys whose entry differs, or that only one side holds, must be the job's tables and, as far as they differ, the namespaces that are their parents.
    A table of the job that the base already holds as it is, was not written by this job.
    """
    changed = {key for key in base_entries.keys() | branch_entries.keys() if base_entries.get(key) != branch_entries.get(key)}
    allowed = parent_namespaces(expected)
    other = sorted(key for key in changed if key not in expected and key not in allowed)
    unchanged = sorted(key for key in expected if key not in changed)
    if other:
        return f"the branch changes {', '.join(other[:5])}, which the job did not write: nothing was published"
    if unchanged:
        return f"the table {unchanged[0]} is on the base as the branch holds it, so this job did not write it: nothing was published"
    return None


def confirmed(ops: PublishOps, base_reference: str, branch_reference: str, expected: Mapping[str, int | None], where: str) -> dict[str, int]:
    """The snapshot of each table on the base at the merge, after they are compared with the tables on the branch at the checked hash."""
    merged, checked = ops.entries(base_reference), ops.entries(branch_reference)
    if any(merged.get(key) != checked.get(key) for key in expected):
        raise PublicationError(f"the tables of {where} after the merge are not the tables that were checked: read the history of the base")
    return {key: snapshot for key in expected if (snapshot := merged[key].snapshot_id) is not None}


def settle_marker(ops: PublishOps, name: str) -> None:
    """Remove the marker of a merge whose outcome is known. A marker that cannot be removed is left: it only keeps a later run from sending again."""
    try:
        ops.clear_pending(name)
    except NessieError:
        return


def publish(ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str, base: str, expected: Mapping[str, int | None]) -> Publication:
    """Check the branch and, only when every check passed at its head, merge that head into ``base``.

    ``expected`` maps the Nessie content key of each table that the job wrote to the snapshot it wrote, or to ``None`` where the caller does not know it.
    """
    if branch == base:
        raise PublicationError("the base of a publication cannot be the branch that is published")
    if not ops.is_branch(base):
        raise PublicationError(f"{base} is not a branch")
    if not ops.is_branch(branch):
        raise PublicationError(f"{branch} is not a branch")
    started = ops.head(branch)
    problem = written_problem(ops.entries(f"{branch}@{started}"), expected, branch)
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
    marker = pending_name(checked)
    earlier = ops.merge_commit(base, branch, checked)
    if earlier is not None:
        outcome.status, outcome.merged_hash = UNCHANGED, earlier
        outcome.snapshots = confirmed(ops, f"{base}@{earlier}", f"{branch}@{checked}", expected, base)
        settle_marker(ops, marker)
        return outcome
    expected_base = ops.head(base)
    outcome.base_hash_before = expected_base
    if not ops.descends(branch, checked, expected_base):
        outcome.blocked_by = [GUARD_BASE_MOVED]
        return outcome
    problem = output_problem(ops.entries(f"{base}@{expected_base}"), ops.entries(f"{branch}@{checked}"), expected)
    if problem:
        raise PublicationError(problem)
    if not ops.set_pending(marker, branch, checked):
        raise MergeOutcomeOpen(
            f"the tag {marker} is held by another publisher or by a merge of {branch} into {base} with no known outcome: nothing is sent. "
            f"Read the history of {base}, then remove the tag if the merge is not there"
        )
    landed = ops.merge_commit(base, branch, checked)
    if landed is not None:
        outcome.status, outcome.merged_hash = UNCHANGED, landed
        outcome.snapshots = confirmed(ops, f"{base}@{landed}", f"{branch}@{checked}", expected, base)
        settle_marker(ops, marker)
        return outcome
    try:
        merged = ops.merge(branch, base, expected_base, checked, merge_message(branch, checked))
    except NessieConflict as error:
        settle_marker(ops, marker)
        raise PublicationError(f"Nessie refused the merge of {branch} into {base} because {base} moved: nothing was published ({error.error_code})") from None
    except NessieError as error:
        found = ops.merge_commit(base, branch, checked)
        if found is None:
            raise MergeOutcomeOpen(
                f"the merge of {branch} into {base} did not finish ({type(error).__name__}) and the history of {base} does not show it: read it before a retry"
            ) from None
        merged = found
    outcome.snapshots = confirmed(ops, f"{base}@{merged}", f"{branch}@{checked}", expected, base)
    outcome.status, outcome.merged_hash = MERGED, merged
    settle_marker(ops, marker)
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

    def merge_commit(self, base: str, branch: str, branch_hash: str) -> str | None:
        """The hash of the commit in the history of ``base`` whose message ends with the lines of ``merge_message``, newest first, or None."""
        wanted = merge_message(branch, branch_hash).split("\n")[-2:]
        return next((commit for commit, message in self.log(base) if message.split("\n")[-2:] == wanted), None)

    def descends(self, branch: str, branch_hash: str, base_hash: str) -> bool:
        """Whether ``base_hash`` is a commit in the log of the branch at ``branch_hash``: the branch was made on the base as it was then."""
        return any(commit == base_hash for commit, _ in self.log(f"{branch}@{branch_hash}"))

    def merge(self, branch: str, base: str, expected_base_hash: str, branch_hash: str, message: str) -> str:
        return self.client.merge(branch, base, expected_base_hash, message, source_hash=branch_hash)

    def set_pending(self, name: str, branch: str, branch_hash: str) -> bool:
        """Make the marker tag. ``False`` when a tag of that name is already there (Nessie's conflict answer): this run did not make it."""
        try:
            self.client.create_tag(name, branch_hash, branch)
        except NessieConflict:
            return False
        return True

    def clear_pending(self, name: str) -> None:
        try:
            marker = self.client.reference(name)
        except NessieError as error:
            if error.status == 404:
                return
            raise
        self.client.delete_reference(name, marker.hash, "TAG")
