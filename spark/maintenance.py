"""The decisions of table maintenance: which snapshots it may expire and which orphan files it may delete, so that it never removes what a tag points to.

Iceberg's ``expire_snapshots`` and ``remove_orphan_files`` look at the metadata of the one branch they run on. Nessie keeps every table's older metadata under
its tags, and Iceberg cannot see it. So the maintenance job names the snapshots to expire itself, from this plan, and deletes only the orphan files that no tag
needs. The functions take plain facts and import nothing from Spark, so the plan is the same on any host and is proved on its own.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

from scripts.stack.tags import TableSnapshot

FILE_SCHEMES = re.compile(r"file:(//)?(?=/)")
S3_SCHEMES = re.compile(r"s3[an]://")
TABLE_NAME = re.compile(r"\w+(\.\w+)+", re.ASCII)
MS_PER_MINUTE = 60_000


class RetentionError(RuntimeError):
    """A maintenance plan would remove something that a tag points to, or asks for less than the rule allows. Nothing is removed."""


@dataclass(frozen=True)
class SnapshotInfo:
    snapshot_id: int
    committed_ms: int


def protected_snapshots(tagged: Mapping[str, Sequence[TableSnapshot]]) -> dict[str, dict[int, str]]:
    """For each table the snapshot IDs that tags point to, each named by its tag (the first tag in name order when several point to it)."""
    found: dict[str, dict[int, str]] = {}
    for tag in sorted(tagged):
        for item in tagged[tag]:
            if item.snapshot_id is not None:
                found.setdefault(item.table, {}).setdefault(item.snapshot_id, tag)
    return found


def plan_expiry(snapshots: Sequence[SnapshotInfo], current: int | None, protected: Mapping[int, str], older_than_ms: int, retain_last: int) -> list[int]:
    """The IDs to expire, oldest first: snapshots committed before ``older_than_ms`` that no tag points to.

    The ``retain_last`` newest snapshots by commit time and the snapshot the table is at now are kept whatever their age. Keeping the newest by time is
    stricter than Iceberg's rule, which counts only the ancestors of the current snapshot.
    """
    if retain_last < 1:
        raise RetentionError("maintenance keeps at least the newest snapshot of a table: retain_last must be 1 or more")
    ordered = sorted(snapshots, key=lambda item: (item.committed_ms, item.snapshot_id))
    kept = {item.snapshot_id for item in ordered[-retain_last:]} | set(protected) | ({current} if current is not None else set())
    return [item.snapshot_id for item in ordered if item.snapshot_id not in kept and item.committed_ms < older_than_ms]


def guard_expiry(table: str, planned: Collection[int], protected: Mapping[int, str]) -> None:
    """Refuse a plan that holds a snapshot that a tag points to. The job runs this on the list it hands to Iceberg, whatever made the list."""
    held = sorted(snapshot for snapshot in planned if snapshot in protected)
    if held:
        raise RetentionError(f"{table}: the plan would expire snapshot {held[0]}, which the tag {protected[held[0]]} points to: nothing was expired")


def normal_path(path: str) -> str:
    """One spelling of a file path, so that ``file:/a``, ``file:///a`` and ``/a`` are one file and ``s3a://`` and ``s3n://`` are ``s3://``."""
    return S3_SCHEMES.sub("s3://", FILE_SCHEMES.sub("", path, count=1), count=1)


def removable_orphans(candidates: Collection[str], protected_files: Collection[str]) -> tuple[list[str], list[str]]:
    """The orphan candidates that may be deleted and the ones kept because a tag needs them, each in the order given."""
    needed = {normal_path(path) for path in protected_files}
    removable = [path for path in candidates if normal_path(path) not in needed]
    return removable, [path for path in candidates if normal_path(path) in needed]


def in_flight_branches(branches: Collection[str], trunk: str) -> list[str]:
    """The branches other than the trunk. A private branch means a run may be writing files that no metadata names yet, so maintenance waits for it."""
    return sorted(name for name in branches if name != trunk)


def table_identifier(table: str) -> str:
    """``table`` when it is plain names joined by dots, which a statement can hold safely, else ``RetentionError``."""
    if TABLE_NAME.fullmatch(table) is None:
        raise RetentionError("a table name must be plain letters, digits and underscores joined by dots")
    return table


def cut_off_ms(now_ms: int, minutes: int) -> int:
    """The time in milliseconds that is ``minutes`` before ``now_ms``."""
    if minutes < 0:
        raise RetentionError("an age in minutes cannot be negative")
    return now_ms - minutes * MS_PER_MINUTE
