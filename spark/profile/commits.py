"""The check of the commit that drops a table of the signal profile, from the branch's log. No Spark runs here.

The job rebuilds the predecessor ``silver.unscored_share`` (12 columns) by a drop and a create on its own branch. The create is checked by the patient split's
``write_commit`` (one commit directly on the checked hash that only puts the allowed keys). The drop is the same shape with the other operation: it is the head,
one commit directly on the checked hash, and it only deletes the one key.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def drop_commit(entries: Sequence[Mapping[str, Any]], checked: str, key: tuple[str, ...]) -> str | None:
    """The hash of the drop, from the branch's log (newest first, with each commit's operations), or ``None``.

    The drop is the head when it is one commit directly on the checked hash and every operation of it deletes ``key``. Any other head, such as a commit by
    someone else, two commits, a commit with no operation, or a commit that puts or deletes another key, is not the drop.
    """
    if not entries:
        return None
    entry = entries[0]
    commit = (entry.get("commitMeta") or {}).get("hash")
    operations = entry.get("operations") or []
    if not isinstance(commit, str) or entry.get("parentCommitHash") != checked or not operations:
        return None
    for operation in operations:
        if operation.get("type") != "DELETE" or tuple((operation.get("key") or {}).get("elements") or ()) != key:
            return None
    return commit
