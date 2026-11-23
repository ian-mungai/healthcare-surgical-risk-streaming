"""Publication of a Bronze branch: the checks run at one hash of the branch, and only a branch that passed them at that hash is merged into the base, once.

The order, with nothing written to the base before the last step:

1. the branch must be a branch, the base must be another name, and every table must be on the branch at its head;
2. the head of the branch is read and the checks run at exactly that hash (``run_checks`` reads a catalog pinned to it);
3. the head is read again. A branch that moved while it was checked is not merged: the checks judged another hash;
4. when the checks passed and the head is the checked hash, the base's head is read and the branch is merged **at the checked hash** into the base, expecting
   the head that was read. The request is sent once. A refusal because the base moved leaves the base as it was;
5. the tables on the base at the merge are compared with the tables on the branch at the checked hash (metadata location and snapshot), so a merge that did not
   bring exactly the checked tables is an error.

A rerun on a merged branch finds the merge in the history of the base by the last two lines of its message and merges nothing more. A merge request that failed
or timed out may still have been applied, so the history of the base is searched once: a merge found is the result, and none found is ``MergeOutcomeOpen``,
which the operator settles by reading the history of the base. The request is never sent again by this run.

The functions need only the operations of ``PublishOps``, so the sequence runs on fakes in the fast tests. ``NessiePublisher`` is those operations over
Nessie's HTTP API.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import quote

from scripts.stack.nessie import HASH, NessieClient, NessieConflict, NessieError
from spark.bronze.checks import Verdict
from spark.bronze_clinical_lab import TableContent
from spark.bronze_clinical_lab_run import HISTORY_PAGE_LIMIT, HISTORY_PAGE_SIZE, NessieAdapter

GUARD_BRANCH_MOVED = "branch_moved"
BRANCH_LINE = "bronze-branch: "
SOURCE_LINE = "bronze-source: "
BRANCH_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}")
MERGED, UNCHANGED, BLOCKED = "merged", "unchanged", "blocked"


class PublicationError(RuntimeError):
    """The branch was not published and the base is as it was. The message names the cause and never holds a data value."""


class MergeOutcomeOpen(PublicationError):
    """A merge request was sent and the history of the base does not show it. Read the history of the base before anything is sent again."""


class PublishOps(Protocol):
    """What publication needs from Nessie."""

    def is_branch(self, name: str) -> bool: ...

    def head(self, ref: str) -> str: ...

    def content(self, reference: str, key: str) -> TableContent | None: ...

    def merge_commit(self, base: str, branch: str, branch_hash: str) -> str | None: ...

    def merge(self, branch: str, base: str, expected_base_hash: str, branch_hash: str, message: str) -> str: ...


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


def verify(ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str) -> tuple[str, Verdict]:
    """The head of the branch and the verdict of the checks at it. The branch must still have that head when the checks end."""
    if not ops.is_branch(branch):
        raise PublicationError(f"{branch} is not a branch")
    checked = ops.head(branch)
    return checked, run_checks(checked)


def snapshots_of(ops: PublishOps, reference: str, keys: Sequence[str]) -> dict[str, int]:
    found = {key: ops.content(reference, key) for key in keys}
    absent = [key for key, value in found.items() if value is None]
    if absent:
        raise PublicationError(f"{reference} does not hold the table {absent[0]}")
    return {key: value.snapshot_id for key, value in found.items() if value is not None}


def publish(ops: PublishOps, run_checks: Callable[[str], Verdict], branch: str, base: str, keys: Sequence[str]) -> Publication:
    """Check the branch and, only when every check passed at its head, merge that head into ``base``. ``keys`` are the Nessie content keys of the tables."""
    if branch == base:
        raise PublicationError("the base of a publication cannot be the branch that is published")
    if not ops.is_branch(base):
        raise PublicationError(f"{base} is not a branch")
    if not ops.is_branch(branch):
        raise PublicationError(f"{branch} is not a branch")
    started = ops.head(branch)
    snapshots_of(ops, f"{branch}@{started}", keys)
    checked, result = verify(ops, run_checks, branch)
    outcome = Publication(BLOCKED, branch, base, checked, verdict=result)
    if not result.passed:
        outcome.blocked_by = [guard for guard, passed in result.guards.items() if not passed]
        return outcome
    if ops.head(branch) != checked or checked != started:
        outcome.blocked_by = [GUARD_BRANCH_MOVED]
        return outcome
    earlier = ops.merge_commit(base, branch, checked)
    if earlier is not None:
        outcome.status, outcome.merged_hash = UNCHANGED, earlier
        outcome.snapshots = snapshots_of(ops, f"{base}@{earlier}", keys)
        return outcome
    expected = ops.head(base)
    outcome.base_hash_before = expected
    wanted = {key: ops.content(f"{branch}@{checked}", key) for key in keys}
    try:
        merged = ops.merge(branch, base, expected, checked, merge_message(branch, checked))
    except NessieConflict as error:
        raise PublicationError(f"Nessie refused the merge of {branch} into {base} because {base} moved: nothing was published ({error.error_code})") from None
    except NessieError as error:
        found = ops.merge_commit(base, branch, checked)
        if found is None:
            raise MergeOutcomeOpen(
                f"the merge of {branch} into {base} did not finish ({type(error).__name__}) and the history of {base} does not show it: read it before a retry"
            ) from None
        merged = found
    if {key: ops.content(f"{base}@{merged}", key) for key in keys} != wanted:
        raise PublicationError(f"the tables of {base} after the merge are not the tables that were checked on {branch}: read the history of {base}")
    outcome.status, outcome.merged_hash = MERGED, merged
    outcome.snapshots = snapshots_of(ops, f"{base}@{merged}", keys)
    return outcome


class NessiePublisher(NessieAdapter):
    """``PublishOps`` over Nessie's HTTP API. ``content``, ``is_branch`` and the head of a branch are the Bronze jobs' own (``NessieAdapter``)."""

    def __init__(self, client: NessieClient) -> None:
        super().__init__(client)

    def head(self, ref: str) -> str:
        return self.client.reference(ref).hash

    def merge_commit(self, base: str, branch: str, branch_hash: str) -> str | None:
        """The hash of the commit in the history of ``base`` whose message ends with the lines of ``merge_message``, newest first, or None.

        A log that is not in the expected form, or that is longer than ``HISTORY_PAGE_LIMIT`` pages, is an error and never taken for a log with no merge.
        """
        wanted = merge_message(branch, branch_hash).split("\n")[-2:]
        token = ""
        for _ in range(HISTORY_PAGE_LIMIT):
            query = {"max-records": str(HISTORY_PAGE_SIZE), **({"page-token": token} if token else {})}
            page = self.client.request("GET", f"/trees/{quote(base, safe='')}/history", query=query)
            entries = page.get("logEntries") if isinstance(page, dict) else None
            if not isinstance(entries, list) or not isinstance(page.get("hasMore"), bool):
                raise NessieError("Nessie's answer for the log is not in the expected form")
            for entry in entries:
                meta = entry.get("commitMeta") if isinstance(entry, dict) else None
                message, commit = (meta.get("message"), meta.get("hash")) if isinstance(meta, dict) else (None, None)
                if not isinstance(message, str) or not isinstance(commit, str):
                    raise NessieError("Nessie's answer for the log is not in the expected form")
                if message.split("\n")[-2:] == wanted:
                    return commit
            token = page.get("token") or ""
            if not page["hasMore"]:
                return None
            if not token:
                raise NessieError("Nessie's answer for the log has more pages and names no token")
        raise NessieError("the history of the base is too long to search for an earlier merge")

    def merge(self, branch: str, base: str, expected_base_hash: str, branch_hash: str, message: str) -> str:
        return self.client.merge(branch, base, expected_base_hash, message, source_hash=branch_hash)
