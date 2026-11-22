"""The table maintenance job: expire old snapshots and delete orphan files on the trunk, and keep everything that a Nessie tag points to.

``python -m spark.jobs.table_maintenance [--trunk main] [--expire-older-than-minutes 7200] [--retain-last 1] [--orphan-older-than-minutes 4320] [--dry-run]``

The defaults are Iceberg's own: snapshots older than 5 days, the newest snapshot kept, orphan files older than 3 days. The job
refuses to run while a branch other than the trunk exists, because a run that is still writing has files that no metadata names yet. Then it:

1. lists every tag and the snapshot of each table at each tag, and reads each tagged snapshot through a catalog opened on the tag (the rows and a checksum of
   their values), so a tag that already does not read stops the job before it changes anything;
2. for each table on the trunk, plans the snapshots to expire with ``spark.maintenance.plan_expiry``, which leaves out every snapshot that a tag points to,
   checks the plan again with ``guard_expiry`` and expires exactly those snapshot IDs with Iceberg's ``expire_snapshots`` procedure. The Nessie catalog makes
   every table with ``gc.enabled`` false, so Iceberg's procedures refuse it: the job turns the flag on for a table around its own steps 2 and 3 and puts it
   back as it was;
3. finds the orphan files with ``remove_orphan_files`` as a dry run, leaves out every file that a tag needs (its metadata file, its manifest list, its manifests
   and its data files) and deletes the others itself, through the table's file IO;
4. reads each tagged snapshot again and refuses to finish with exit 0 unless every one gives the same snapshot, rows and checksum as before.

``--dry-run`` plans the expiry, reports it and changes nothing. It lists no orphan files, because Iceberg lists them only when the table's ``gc.enabled``
is on. The job prints one line that starts with ``RESULT `` and holds the plan, what it removed and the tagged reads. Settings come from the environment,
as for the catalog proof, and ``JOB_DEADLINE_SECONDS`` ends the job.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from scripts.stack.nessie import NessieClient, NessieError
from scripts.stack.tags import TableSnapshot, list_references, list_tags, table_snapshots
from spark.catalog import add_catalog, required_environment
from spark.maintenance import (
    RetentionError,
    SnapshotInfo,
    cut_off_ms,
    guard_expiry,
    in_flight_branches,
    plan_expiry,
    protected_snapshots,
    removable_orphans,
    table_identifier,
)
from spark.session import build_session, start_deadline
from spark.settings import CATALOG, SettingsError

JOB = "table_maintenance"
RESULT_PREFIX = "RESULT "
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
DEFAULT_JOB_DEADLINE_SECONDS = 900.0
DEFAULT_EXPIRE_MINUTES = 5 * 24 * 60
DEFAULT_ORPHAN_MINUTES = 3 * 24 * 60
MS_PER_SECOND = 1000
GC_PROPERTY = "gc.enabled"


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog=f"python -m spark.jobs.{JOB}", description="Expire old snapshots and delete orphan files, keeping every tagged snapshot."
    )
    command.add_argument("--trunk", default="main", help="the branch whose tables are maintained")
    command.add_argument("--expire-older-than-minutes", type=int, default=DEFAULT_EXPIRE_MINUTES)
    command.add_argument("--retain-last", type=int, default=1, help="how many of the newest snapshots of a table are kept whatever their age")
    command.add_argument("--orphan-older-than-minutes", type=int, default=DEFAULT_ORPHAN_MINUTES)
    command.add_argument("--dry-run", action="store_true", help="plan and report, change nothing")
    return command


def iceberg_table(session: Any, name: str) -> Any:
    return session._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session._jsparkSession, name)


def read_tagged(session: Any, tagged: dict[str, tuple[TableSnapshot, ...]], pins: dict[str, str]) -> list[dict[str, Any]]:
    """Read each tagged table through the catalog opened on its tag: the snapshot it is at, its row count and a checksum over every value.

    The checksum reads every column of every row, so a data file that is gone fails here. A snapshot that differs from Nessie's is an error.
    """
    reads = []
    for tag in sorted(tagged):
        for item in tagged[tag]:
            if item.snapshot_id is None:
                continue
            name = f"{pins[tag]}.{table_identifier(item.table)}"
            try:
                snapshot = int(iceberg_table(session, name).currentSnapshot().snapshotId())
                row = session.table(name).selectExpr("count(*) AS rows", "sum(hash(*)) AS checksum").collect()[0]
            except Exception as error:
                raise RetentionError(f"the tag {tag} no longer reads {item.table}: {type(error).__name__}: {str(error).splitlines()[0][:300]}") from error
            if snapshot != item.snapshot_id:
                raise RetentionError(f"the tag {tag} reads {item.table} at snapshot {snapshot}, not at {item.snapshot_id}")
            reads.append(
                {
                    "tag": tag,
                    "table": item.table,
                    "snapshot_id": snapshot,
                    "rows": int(row["rows"]),
                    "checksum": None if row["checksum"] is None else int(row["checksum"]),
                }
            )
    return reads


def tagged_files(session: Any, tagged: dict[str, tuple[TableSnapshot, ...]], pins: dict[str, str]) -> set[str]:
    """Every file that reading a tagged snapshot needs: the metadata file, its older metadata files, the manifest list, the manifests and the data files."""
    needed: set[str] = set()
    for tag in sorted(tagged):
        for item in tagged[tag]:
            if item.snapshot_id is None:
                continue
            name = f"{pins[tag]}.{table_identifier(item.table)}"
            needed.add(item.metadata_location)
            files = session.table(f"{name}.all_files").select("file_path")
            manifests = session.table(f"{name}.all_manifests").select("path")
            lists = session.table(f"{name}.snapshots").where(f"snapshot_id = {int(item.snapshot_id)}").select("manifest_list")
            earlier = session.table(f"{name}.metadata_log_entries").select("file")
            for frame in (files, manifests, lists, earlier):
                needed.update(str(row[0]) for row in frame.collect() if row[0])
    return needed


def planned_expiry(session: Any, item: TableSnapshot, protected: dict[int, str], arguments: argparse.Namespace, now_ms: int) -> list[int]:
    """The snapshot IDs of one trunk table to expire, oldest first. The plan leaves out every tagged snapshot and the guard checks the list again."""
    table = table_identifier(item.table)
    rows = session.table(f"{CATALOG}.{table}.snapshots").selectExpr("snapshot_id", "unix_millis(committed_at) AS committed_ms").collect()
    history = [SnapshotInfo(int(row["snapshot_id"]), int(row["committed_ms"])) for row in rows]
    planned = plan_expiry(history, item.snapshot_id, protected, cut_off_ms(now_ms, arguments.expire_older_than_minutes), arguments.retain_last)
    guard_expiry(table, planned, protected)
    return planned


@contextmanager
def gc_allowed(session: Any, table: str) -> Iterator[None]:
    """Allow Iceberg's maintenance procedures on the table for the length of the block, and put its ``gc.enabled`` property back as it was.

    The Nessie catalog creates every table with ``gc.enabled`` false, because Iceberg sees only the one reference it works on and the procedures could delete
    files that another branch or a tag needs. The job knows every tag, so it opens the flag only around its own plan.
    """
    name = f"{CATALOG}.{table}"
    previous = iceberg_table(session, name).properties().get(GC_PROPERTY)
    session.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('{GC_PROPERTY}' = 'true')")
    try:
        yield
    finally:
        if previous is None:
            session.sql(f"ALTER TABLE {name} UNSET TBLPROPERTIES ('{GC_PROPERTY}')")
        else:
            session.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('{GC_PROPERTY}' = '{previous}')")


def remove_orphans(session: Any, table: str, needed: set[str], arguments: argparse.Namespace, now_ms: int) -> dict[str, int]:
    """Find one table's orphan files, leave out the ones that a tag needs and delete the rest. Returns the counts."""
    cutoff = datetime.fromtimestamp(cut_off_ms(now_ms, arguments.orphan_older_than_minutes) / MS_PER_SECOND, UTC).strftime("%Y-%m-%d %H:%M:%S")
    found = session.sql(f"CALL {CATALOG}.system.remove_orphan_files(table => '{table}', older_than => TIMESTAMP '{cutoff}', dry_run => true)").collect()
    removable, kept = removable_orphans([str(row[0]) for row in found], needed)
    if removable:
        files = iceberg_table(session, f"{CATALOG}.{table}").io()
        for path in removable:
            files.deleteFile(path)
    return {"orphans_found": len(found), "orphans_removed": len(removable), "orphans_kept_for_tags": len(kept)}


def maintain(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run the maintenance and return its result, or raise ``RetentionError`` when a tagged snapshot is not as it was."""
    client = NessieClient(required_environment("NESSIE_URI"), float(os.environ.get("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS)))
    client.config()
    branches = list_references(client, "BRANCH")
    if arguments.trunk not in branches:
        raise RetentionError(f"the trunk {arguments.trunk} does not exist")
    waiting = in_flight_branches(branches, arguments.trunk)
    if waiting:
        raise RetentionError(f"the branches {', '.join(waiting)} exist, so a run may be writing: maintenance waits until they are merged or deleted")
    tagged = {tag: table_snapshots(client, tag) for tag in sorted(list_tags(client))}
    protected = protected_snapshots(tagged)
    trunk_tables = table_snapshots(client, arguments.trunk)
    session = build_session(JOB, arguments.trunk)
    try:
        session.conf.set("spark.sql.session.timeZone", "UTC")
        pins = {tag: f"pin{number}" for number, tag in enumerate(tagged)}
        for tag, catalog in pins.items():
            add_catalog(session, catalog, tag)
        before = read_tagged(session, tagged, pins)
        now_ms = int(time.time() * MS_PER_SECOND)
        needed = tagged_files(session, tagged, pins)
        expired: dict[str, list[int]] = {}
        orphans: dict[str, dict[str, int]] = {}
        for item in trunk_tables:
            table = table_identifier(item.table)
            expired[table] = planned_expiry(session, item, protected.get(table, {}), arguments, now_ms)
            if arguments.dry_run:
                continue
            with gc_allowed(session, table):
                if expired[table]:
                    ids = ", ".join(str(number) for number in expired[table])
                    session.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', snapshot_ids => array({ids}))").collect()
                orphans[table] = remove_orphans(session, table, needed, arguments, now_ms)
        after = read_tagged(session, tagged, pins)
    finally:
        session.stop()
    if before != after:
        raise RetentionError(f"a tagged read changed during maintenance: before {before}, after {after}")
    return {
        "trunk": arguments.trunk,
        "dry_run": arguments.dry_run,
        "tags": sorted(tagged),
        "expired_snapshots": expired,
        "orphans": orphans,
        "tagged_reads": after,
    }


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        start_deadline(float(os.environ.get("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS)), JOB)
        result = maintain(arguments)
    except RetentionError as error:
        sys.stderr.write(f"{JOB}: refused: {error}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {error}\n")
        return 1
    except SettingsError as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
