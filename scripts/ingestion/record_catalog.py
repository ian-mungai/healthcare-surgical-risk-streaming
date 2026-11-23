"""The catalog calls of the record load, over the project's Nessie client (``scripts.stack.nessie``).

The load branch is created at an exact hash of ``main``. A table's snapshot on a branch is the ``snapshotId`` of its Iceberg table content, read from the
content of the key ``raw_records.<table>`` at the branch's head.

Every answer that the load reads is read through a closed shape. A history page must hold a list of log entries, each with a ``commitMeta`` that holds a text
``message`` and a ``hash`` of 64 lower case hex digits, a boolean ``hasMore``, and, when it says there is more, a text token that this scan has not used.
A page of any other shape cannot show that a commit is absent, so a scan for absence refuses it (``cannot establish absence``). An entries answer or a
content answer of another shape is refused as well. No message holds a value of an answer.

A branch is deleted through the typed form of Nessie's API: ``DELETE /api/v2/trees/{name}@{hash}?type=BRANCH``. The query parameter ``type`` is the
"optional expected type of the reference being deleted" of ``deleteReferenceV2`` in ``api/model/src/main/java/org/projectnessie/api/v2/http/HttpTreeApi.java``
of ``projectnessie/nessie`` at the tag ``nessie-0.110.1``, the pinned version. A tag of the name is then not found by the call, and nothing is deleted.

No failure of Nessie reaches a message as Nessie's own text: every call of this adapter that fails becomes ``catalog_error: <operation> failed``, and the
original exception is not chained (``from None``), because Nessie's answer can hold text that a caller or a proxy put there.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

from scripts.ingestion.record_definitions import MAIN_BRANCH, NAMESPACE
from scripts.ingestion.record_load import BRANCH_KIND, BranchExists, BranchMoved, LoadError, RefState, manifest_line, recorded_manifest
from scripts.stack.nessie import NessieClient, NessieConflict, NessieError, Reference

NOT_FOUND = 404
HISTORY_PAGE = 100
HISTORY_LIMIT = 5000
HASH = re.compile(r"[0-9a-f]{64}")


def not_established(what: str) -> LoadError:
    return LoadError(f"cannot establish absence: {what}")


class CatalogError(LoadError):
    """A call to the catalog failed. The message is the fixed code ``catalog_error`` and the name of the operation, never a word of Nessie's answer."""


def catalog_error(operation: str) -> CatalogError:
    return CatalogError(f"catalog_error: {operation} failed")


class NessieCatalog:
    def __init__(self, client: NessieClient) -> None:
        self.client = client

    def head(self, branch: str) -> str:
        try:
            return self.client.reference(branch).hash
        except NessieError:
            raise catalog_error("head") from None

    def reference(self, name: str) -> RefState | None:
        """The kind and the hash of the reference ``name``, or None when there is none. A tag and a branch of one name are told apart by the kind."""
        try:
            found = self.client.reference(name)
        except NessieError as error:
            if error.status == NOT_FOUND:
                return None
            raise catalog_error("reference") from None
        return RefState(found.kind, found.hash)

    def create_branch(self, branch: str, at: str) -> str:
        """Create the branch at exactly the hash ``at`` of main and return the hash it was made at.

        Raise ``BranchExists`` when the name is taken, and ``LoadError`` when Nessie answers with another hash than the one that was asked for.
        """
        if HASH.fullmatch(at) is None:
            raise ValueError("a branch is made at a hash of 64 lower case hexadecimal digits")
        try:
            made = self.client.create_reference(BRANCH_KIND, branch, Reference(BRANCH_KIND, MAIN_BRANCH, at)).hash
        except NessieConflict:
            raise BranchExists(f"the branch {branch} exists") from None
        except NessieError:
            raise catalog_error("create_branch") from None
        if made != at:
            raise LoadError(f"Nessie made the branch {branch} at another hash than the one that was asked for")
        return made

    def delete_branch(self, branch: str, head: str) -> None:
        """Delete the reference of the name, expected to be a branch, at exactly ``head``, through the typed form of the call.

        ``BranchMoved`` when it is at another hash; a branch that is already gone, or a reference of another kind (which the typed call does not find), is
        not deleted and is not an error here.
        """
        try:
            self.client.delete_reference(branch, head, BRANCH_KIND)
        except NessieConflict:
            raise BranchMoved(f"the branch {branch} is no longer at the head that was read: it is refused and not deleted") from None
        except NessieError as error:
            if error.status != NOT_FOUND:
                raise catalog_error("delete_branch") from None

    def history(self, name: str, found: Callable[[str, str], bool], what: str) -> bool:
        """Read the history of ``name`` newest first, in pages, for at most ``HISTORY_LIMIT`` commits, and return whether ``found(hash, message)`` held for one.

        A history that ends inside the scan without one is False. A page of another shape, or a history that is longer than the scan and holds none inside
        it, raises ``LoadError`` (``cannot establish absence``): neither proves an absence.
        """
        seen = 0
        used: set[str] = set()
        token: str | None = None
        while seen < HISTORY_LIMIT:
            query = {"max-records": str(min(HISTORY_PAGE, HISTORY_LIMIT - seen))} | ({} if token is None else {"page-token": token})
            try:
                answer = self.client.request("GET", f"/trees/{quote(name, safe='')}/history", None, query)
            except NessieError:
                raise catalog_error("history") from None
            commits, token = self.page(answer, used, what)
            if any(found(commit, message) for commit, message in commits):
                return True
            seen += len(commits)
            if token is None:
                return False
        raise not_established(f"{what} is longer than the {HISTORY_LIMIT} commits that are scanned")

    @staticmethod
    def page(answer: Any, used: set[str], what: str) -> tuple[list[tuple[str, str]], str | None]:
        """The commits of one history page as (hash, message) pairs and the token of the next page, or None on the last page. Raise ``LoadError`` for any
        page of another shape, so that it cannot be taken for the end of a history."""
        entries = answer.get("logEntries") if isinstance(answer, dict) else None
        more = answer.get("hasMore") if isinstance(answer, dict) else None
        if not isinstance(entries, list) or type(more) is not bool:
            raise not_established(f"{what} gave a page that is not of the documented form")
        commits: list[tuple[str, str]] = []
        for entry in entries:
            meta = entry.get("commitMeta") if isinstance(entry, dict) else None
            message = meta.get("message") if isinstance(meta, dict) else None
            commit = meta.get("hash") if isinstance(meta, dict) else None
            if type(message) is not str or type(commit) is not str or HASH.fullmatch(commit) is None:
                raise not_established(f"{what} gave a page with an entry that is not of the documented form")
            commits.append((commit, message))
        if not more:
            return commits, None
        token = answer.get("token")
        if type(token) is not str or not token or token in used or not commits:
            raise not_established(f"{what} says that there are more pages and gives no new token and entries to go on with")
        used.add(token)
        return commits, token

    def has_recorded_manifest(self, sha256: str) -> bool:
        """Whether any commit on main carries the ``records-manifest`` line for this manifest.

        The history is read through ``history``: an absence is claimed only when it ended inside the scan and every page was well formed.
        """
        manifest_line(sha256)
        return self.history(MAIN_BRANCH, lambda _commit, message: recorded_manifest(message) == sha256, f"the history of {MAIN_BRANCH}")

    def descends_from(self, branch: str, ancestor: str) -> bool:
        """Whether the commit ``ancestor`` is the head of the branch or in its history. False only when the whole history was read and does not hold it."""
        if HASH.fullmatch(ancestor) is None:
            raise ValueError("an ancestor is a hash of 64 lower case hexadecimal digits")
        if self.head(branch) == ancestor:
            return True
        return self.history(branch, lambda commit, _message: commit == ancestor, f"the history of {branch}")

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        """The snapshot of each Iceberg table of the raw namespace at the head of the branch, by table name. A table with no snapshot is not listed.

        An answer that is not in the documented shape, or that says that there are more entries than it holds, is refused.
        """
        found: dict[str, int] = {}
        for key in self.table_keys(branch):
            try:
                answer = self.client.request("GET", f"/trees/{quote(branch, safe='')}/contents/{quote(key, safe='')}")
            except NessieError:
                raise catalog_error("contents") from None
            content = answer.get("content") if isinstance(answer, dict) else None
            if not isinstance(content, dict):
                raise LoadError("Nessie's content answer holds no content")
            if content.get("type") != "ICEBERG_TABLE":
                continue
            snapshot = content.get("snapshotId")
            if "snapshotId" not in content:
                continue
            if type(snapshot) is not int:
                raise LoadError("Nessie's content answer holds a snapshot that is not a whole number")
            found[key.partition(".")[2]] = snapshot
        return found

    def table_keys(self, branch: str) -> list[str]:
        """The keys of the raw namespace's tables at the head of the branch, as dotted names."""
        try:
            answer = self.client.request("GET", f"/trees/{quote(branch, safe='')}/entries")
        except NessieError:
            raise catalog_error("entries") from None
        entries = answer.get("entries") if isinstance(answer, dict) else None
        more = answer.get("hasMore") if isinstance(answer, dict) else None
        if not isinstance(entries, list) or type(more) is not bool or more:
            raise LoadError("Nessie's entries answer is not a complete list of entries")
        keys: list[str] = []
        for entry in entries:
            name = entry.get("name") if isinstance(entry, dict) else None
            elements = name.get("elements") if isinstance(name, dict) else None
            if not isinstance(elements, list) or not all(type(element) is str for element in elements):
                raise LoadError("Nessie's entries answer holds an entry that is not of the documented form")
            key = ".".join(elements)
            namespace, _, table = key.partition(".")
            if namespace == NAMESPACE and table and "." not in table:
                keys.append(key)
        return sorted(keys)
