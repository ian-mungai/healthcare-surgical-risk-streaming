"""The tag step of a reported run, and the reads that table maintenance needs from Nessie's tags.

After a run's branch is merged, the merged hash gets a Nessie tag whose name holds the run ID, and the tag, the hash and the snapshot ID of each table at that
hash go into a record in the run's folder. The step is idempotent: a rerun with the same run ID and hash changes nothing, and the same tag name on another hash
is refused, in Nessie and in the folder, so a run's pin never moves. Table maintenance keeps every snapshot that a tag points to, and it finds them with
``list_tags`` and ``table_snapshots``.

The command line runs in a container that reaches Nessie at ``NESSIE_URI``:

``python -m scripts.stack.tags tag --run-id <id> --hash <merged hash> --run-folder <folder> [--trunk main]``

It says ``tagged`` when it made the tag or wrote the record, and ``unchanged`` when both were already as asked. Exit 0 is success, 1 is a refusal by Nessie
or by a record that holds another hash, and 2 is a bad argument. The step writes ``data_version.json`` into the folder it is given and makes the folder when
it is absent. A folder that the run folder writer would name ``*_<run id>`` must not exist before that writer runs, so give this step another folder.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from scripts.stack.nessie import HASH, NessieClient, NessieConflict, NessieError

RUN_TAG_PREFIX = "run-"
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}")
RECORD_NAME = "data_version.json"
TABLE_TYPE = "ICEBERG_TABLE"


class TagError(ValueError):
    """An argument is not one that a tag step can use."""


class TagConflict(NessieError):
    """A tag or a recorded version of this run is already at another hash. Nothing is changed."""


@dataclass(frozen=True)
class TableSnapshot:
    table: str
    snapshot_id: int | None
    metadata_location: str


@dataclass(frozen=True)
class TagResult:
    run_id: str
    tag: str
    hash: str
    trunk: str
    created: bool
    tables: tuple[TableSnapshot, ...]


def tag_name(run_id: str) -> str:
    """The tag of a run: ``run-`` and the run ID, which is letters, digits, dashes and underscores, at most 96 characters."""
    if RUN_ID.fullmatch(run_id) is None:
        raise TagError("the run ID must be 1 to 96 letters, digits, dashes and underscores, starting with a letter or a digit")
    return f"{RUN_TAG_PREFIX}{run_id}"


def paged(client: NessieClient, path: str, field: str, query: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Every item of ``field`` in a list answer that Nessie pages with ``token``, in order."""
    items: list[dict[str, Any]] = []
    token = ""
    while True:
        document = client.request("GET", path, None, {**(query or {}), **({"pageToken": token} if token else {})})
        if not isinstance(document, dict):
            raise NessieError(f"GET {path} answered with something that is not an object")
        items.extend(item for item in document.get(field) or [] if isinstance(item, dict))
        token = document.get("token") or ""
        if not document.get("hasMore") or not token:
            return items


def list_references(client: NessieClient, kind: str) -> dict[str, str]:
    """Every reference of the type ``kind`` (``TAG`` or ``BRANCH``) with the hash it points to."""
    return {str(item["name"]): str(item["hash"]) for item in paged(client, "/trees", "references") if item.get("type") == kind}


def list_tags(client: NessieClient) -> dict[str, str]:
    """Every tag of the repository with the hash it points to."""
    return list_references(client, "TAG")


def table_snapshots(client: NessieClient, reference: str) -> tuple[TableSnapshot, ...]:
    """The Iceberg tables at the head of ``reference`` with the snapshot ID and metadata file that Nessie holds for each, in the order of their names."""
    found = []
    for entry in paged(client, f"/trees/{quote(reference, safe='')}/entries", "entries", {"content": "true"}):
        content = entry.get("content") or {}
        if entry.get("type") != TABLE_TYPE or not isinstance(content, dict):
            continue
        snapshot = content.get("snapshotId")
        found.append(
            TableSnapshot(
                ".".join((entry.get("name") or {}).get("elements") or []),
                int(snapshot) if isinstance(snapshot, int) and snapshot >= 0 else None,
                str(content.get("metadataLocation") or ""),
            )
        )
    return tuple(sorted(found, key=lambda item: item.table))


def tag_run(client: NessieClient, run_id: str, merged_hash: str, trunk: str = "main") -> TagResult:
    """Tag ``merged_hash``, a hash in the history of ``trunk``, for ``run_id``. A tag of that name at that hash stays, and one at another hash is refused."""
    name = tag_name(run_id)
    if HASH.fullmatch(merged_hash) is None:
        raise TagError("the hash must be 64 lower-case hexadecimal digits")
    created = False
    try:
        existing = client.reference(name)
    except NessieError as error:
        if error.status != 404:
            raise
        existing = None
    if existing is None:
        try:
            client.create_tag(name, merged_hash, trunk)
            created = True
        except NessieConflict:
            existing = client.reference(name)
    if existing is not None and (existing.kind != "TAG" or existing.hash != merged_hash):
        raise TagConflict(f"{name} already exists as a {existing.kind.lower()} at {existing.hash}, not at {merged_hash}: a run's tag never moves")
    pinned = client.reference(name)
    if pinned.kind != "TAG" or pinned.hash != merged_hash:
        raise NessieError(f"{name} reads back as a {pinned.kind.lower()} at {pinned.hash}, not as a tag at {merged_hash}")
    return TagResult(run_id, name, merged_hash, trunk, created, table_snapshots(client, name))


def version_record(result: TagResult) -> dict[str, Any]:
    """The record of a run's data version: the tag, the hash and each table's snapshot ID."""
    return {
        "run_id": result.run_id,
        "tag": result.tag,
        "hash": result.hash,
        "trunk": result.trunk,
        "tables": [{"table": item.table, "snapshot_id": item.snapshot_id, "metadata_location": item.metadata_location} for item in result.tables],
    }


def write_version_record(folder: Path, result: TagResult) -> bool:
    """Write the record into ``folder``. Returns False when the folder already holds this very record, and refuses one that holds another."""
    target = folder / RECORD_NAME
    record = version_record(result)
    if target.exists():
        try:
            held = json.loads(target.read_text())
        except (OSError, ValueError) as error:
            raise TagConflict(f"{target} exists and cannot be read as JSON ({type(error).__name__}): it is not replaced") from error
        if held == record:
            return False
        raise TagConflict(f"{target} already records {result.tag} at {held.get('hash')}, not at {result.hash}: it is not replaced")
    folder.mkdir(parents=True, exist_ok=True)
    descriptor, staging = tempfile.mkstemp(prefix=".writing_", dir=folder)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(json.dumps(record, indent=2) + "\n")
        os.chmod(staging, 0o644)
        os.replace(staging, target)
    except BaseException:
        Path(staging).unlink(missing_ok=True)
        raise
    return True


def main(argv: list[str] | None = None) -> int:
    """Tag a run's merged hash and record it. Exit 0 when the tag and the record are as asked, 1 when Nessie or a record refuses, 2 for bad input."""
    parser = argparse.ArgumentParser(
        prog="scripts.stack.tags", description="Tag a run's merged hash and write the tag, hash and snapshot IDs into its run folder."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    tag = commands.add_parser("tag")
    tag.add_argument("--run-id", required=True)
    tag.add_argument("--hash", required=True, help="the hash that the merge of the run's branch returned")
    tag.add_argument("--run-folder", required=True, type=Path)
    tag.add_argument("--trunk", default="main")
    arguments = parser.parse_args(argv)
    uri = os.environ.get("NESSIE_URI", "")
    if not uri:
        sys.stderr.write("NESSIE_URI is not set: it names Nessie's API, such as http://nessie:19120/api/v2\n")
        return 2
    try:
        tag_name(arguments.run_id)
        if HASH.fullmatch(arguments.hash) is None:
            raise TagError("the hash must be 64 lower-case hexadecimal digits")
    except TagError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    try:
        result = tag_run(NessieClient(uri), arguments.run_id, arguments.hash, arguments.trunk)
        written = write_version_record(arguments.run_folder, result)
    except NessieError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    word = "tagged" if result.created or written else "unchanged"
    sys.stdout.write(f"{word}: {result.tag} is at {result.hash} with {len(result.tables)} tables, recorded in {arguments.run_folder / RECORD_NAME}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
