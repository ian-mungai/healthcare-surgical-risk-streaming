"""The table maintenance job: compact the tables of the trunk and prove that every Nessie tag still reads what it read. It deletes nothing.

``python -m spark.jobs.table_maintenance [--trunk main] [--table schema.table ...] [--rewrite-all] [--dry-run] [--deadline-seconds 900]``

Owner's decision (Nov 22 2026): maintenance deletes nothing before teardown. The job calls Iceberg's ``rewrite_data_files`` and ``rewrite_manifests``
for each table of the trunk, or for the tables named. Each rewrite writes new files and one commit. The files it replaces stay on disk, because older
snapshots, and the tags that point to them, still reference them. The job never sets a table property and calls no procedure that deletes a snapshot
or a file.

Each run:

1. refuses to start while a branch other than the trunk exists, because a run may be writing;
2. refuses a table whose Iceberg properties could let a commit or a procedure delete a file that a tag needs (``spark.preconditions``), naming
   the table, before any rewrite;
3. lists every tag and reads every table at every tag through a catalog opened on the tag, with no table cache: the snapshot, the row count and a
   checksum over every value;
4. rewrites, or on ``--dry-run`` only lists, the files and manifests of each table;
5. lists the tags again and reads every table at every tag again, and exits 1 when a tag vanished or moved or any read differs from the one before.
   A tag made during the run is reported in ``tags_added`` and is read too, and is not a failure: a compaction adds a commit and removes nothing.

The job prints one line that starts with ``RESULT `` and holds the plan, the rewrites and the reads before and after. It reads ``NESSIE_URI``,
``WAREHOUSE_LOCATION`` and ``AWS_REGION`` from the environment, like the catalog proof, and ends at ``--deadline-seconds``. With ``--rewrite-all``
every data file is rewritten, which a small table needs, because Iceberg's size rules leave a few small files as they are.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from typing import Any

from scripts.stack.nessie import NessieError
from scripts.stack.tags import TableSnapshot, TagClient, list_references, list_tags, table_snapshots
from spark.catalog import add_catalog, required_environment
from spark.maintenance import RetentionError, compare_reads, compare_tags, in_flight_branches, parse, table_identifier
from spark.preconditions import PROPERTY_KEYS, precondition_problems
from spark.session import build_session, start_deadline
from spark.settings import CATALOG, SettingsError

JOB = "table_maintenance"
RESULT_PREFIX = "RESULT "


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
            checksum = None if row["checksum"] is None else int(row["checksum"])
            reads.append({"tag": tag, "table": item.table, "snapshot_id": snapshot, "rows": int(row["rows"]), "checksum": checksum})
    return reads


def open_pins(session: Any, tags: Sequence[str], pins: dict[str, str]) -> None:
    """Open a catalog on each tag that has none yet. A tag keeps its catalog name for the whole run."""
    for tag in tags:
        if tag not in pins:
            pins[tag] = f"pin{len(pins)}"
            add_catalog(session, pins[tag], tag)


def row_dict(row: Any) -> dict[str, int]:
    return {name: int(row[name]) for name in row.asDict() if row[name] is not None}


def refuse_unsafe_tables(session: Any, tables: Sequence[str]) -> None:
    """Read the two properties of each table and refuse every table that could lose a file, before any rewrite."""
    problems: list[str] = []
    for table in tables:
        properties = iceberg_table(session, f"{CATALOG}.{table}").properties()
        problems += precondition_problems(table, {key: properties.get(key) for key in PROPERTY_KEYS})
    if problems:
        raise RetentionError("; ".join(problems))


def compact(session: Any, table: str, rewrite_all: bool, dry_run: bool) -> dict[str, Any]:
    """Rewrite the data files and then the manifests of one table, or on a dry run count what is there. Returns what Iceberg reported."""
    name = f"{CATALOG}.{table}"
    if dry_run:
        files = session.table(f"{name}.files").selectExpr("count(*) AS data_files", "coalesce(sum(file_size_in_bytes), 0) AS bytes").collect()[0]
        manifests = session.table(f"{name}.manifests").selectExpr("count(*) AS manifests").collect()[0]
        return {"would_rewrite": {**row_dict(files), **row_dict(manifests)}}
    options = ", options => map('rewrite-all', 'true')" if rewrite_all else ""
    data = session.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}'{options})").collect()[0]
    manifests = session.sql(f"CALL {CATALOG}.system.rewrite_manifests('{table}')").collect()[0]
    return {"rewrite_data_files": row_dict(data), "rewrite_manifests": row_dict(manifests)}


def maintain(arguments: Any) -> dict[str, Any]:
    """Run the maintenance and return its result, or raise ``RetentionError`` when it must not start or a tag does not read as it did."""
    client = TagClient(required_environment("NESSIE_URI"), arguments.deadline_seconds)
    client.config()
    branches = list_references(client, "BRANCH")
    if arguments.trunk not in branches:
        raise RetentionError(f"the trunk {arguments.trunk} does not exist")
    waiting = in_flight_branches(branches, arguments.trunk)
    if waiting:
        raise RetentionError(f"the branches {', '.join(waiting)} exist, so a run may be writing: maintenance waits until they are merged or deleted")
    on_trunk = [item.table for item in table_snapshots(client, arguments.trunk)]
    missing = sorted(set(arguments.tables) - set(on_trunk))
    if missing:
        raise RetentionError(f"the tables {', '.join(missing)} are not on {arguments.trunk}")
    tables = [table_identifier(table) for table in (arguments.tables or on_trunk)]
    tags_before = list_tags(client)
    tagged_before = {tag: table_snapshots(client, tag) for tag in sorted(tags_before)}
    session = build_session(JOB, arguments.trunk)
    try:
        pins: dict[str, str] = {}
        open_pins(session, sorted(tagged_before), pins)
        refuse_unsafe_tables(session, tables)
        before = read_tagged(session, tagged_before, pins)
        compacted = {table: compact(session, table, arguments.rewrite_all, arguments.dry_run) for table in tables}
        tags_after = list_tags(client)
        tagged_after = {tag: table_snapshots(client, tag) for tag in sorted(tags_after)}
        open_pins(session, sorted(tagged_after), pins)
        after = read_tagged(session, tagged_after, pins)
    finally:
        session.stop()
    changed, added = compare_tags(tags_before, tags_after)
    differences = changed + compare_reads(before, after)
    if differences:
        raise RetentionError("a tag does not read as it did: " + "; ".join(differences))
    return {
        "trunk": arguments.trunk,
        "dry_run": arguments.dry_run,
        "tags": sorted(tags_before),
        "tags_added": added,
        "tables": compacted,
        "tagged_reads_before": before,
        "tagged_reads_after": after,
    }


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse(argv)
    try:
        start_deadline(arguments.deadline_seconds, JOB)
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
