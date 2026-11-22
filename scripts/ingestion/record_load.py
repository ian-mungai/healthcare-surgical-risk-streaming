"""The record load: the three Airbyte record connections run on one Nessie load branch, in a fixed order, and the record of what they loaded.

The order is the contract, and each step stops the load before the next one starts:

1. The gate and the exact-version copy of the two CSV tables (``record_copy``). A failed or missing check (no manifest), a missing or substituted version
   and a copy whose hash differs fail here, before Airbyte or the catalog is called. The gate also runs for a run that already has its record.
2. A run that has its record is finished: its branch must still be at the recorded head with the recorded snapshots, and the call returns the record and
   changes nothing. A run ID that was loaded from another manifest is refused.
3. The destination, the three sources and the three connections are made to match ``record_definitions`` for this run. The copies are confirmed again,
   and no job may be running on any of the connections. The copies are confirmed before each sync, so one replaced while an earlier sync read it is found.
4. The load branch ``load_<run ID>`` is created from the head of main. A branch of that name with no record is the leftover of an unfinished try and is
   deleted first. Airbyte is asked which branch its destination writes to, and the load stops unless it is the load branch.
5. Each table is synced in turn: ``clinical_data``, ``lab_data``, then ``track_list``. Each wait ends at its deadline, cancels the job and fails. A sync
   that ends in any status but success fails the load and the later syncs do not start.
6. The branch head must have moved, and every table must have a new snapshot on the branch.
7. The record (``<run ID>.json``) is written once and whole: the branch, its head before and after, each table's snapshot and job, the manifest, the
   copies and the pinned connectors.

The load never merges. The table checks run on the branch, and the merge follows only when they pass.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ingestion.airbyte_api import AirbyteError
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import Copied, CopyingStore, confirm_copies, copy_bound_versions
from scripts.ingestion.record_definitions import (
    DESTINATION,
    SOURCE_FILE,
    SOURCE_S3,
    TABLES,
    Environment,
    branch_name,
    connection_configuration,
    destination_configuration,
    source_configuration,
)

SUCCEEDED = "succeeded"
ENDED_WITHOUT_SUCCESS = ("failed", "cancelled", "incomplete")
RECORD_FIELDS = ("run_id", "manifest_sha256", "branch", "base_hash", "head_hash", "snapshots", "jobs", "copies", "connectors")
COPY_FIELDS = ("table", "source_key", "key", "version_id", "sha256", "size")


class LoadError(RuntimeError):
    """The load cannot go on or did not give the tables it should. The message names a table, a branch or a status and never a value of a source."""


class SyncDeadline(LoadError):
    """A sync did not end within its deadline."""


class Catalog(Protocol):
    def head(self, branch: str) -> str: ...

    def has_branch(self, branch: str) -> bool: ...

    def create_branch(self, branch: str) -> str:
        """Create the branch at the head of main and return that head."""
        ...

    def delete_branch(self, branch: str, head: str) -> None: ...

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        """The snapshot of each table of the raw namespace on the branch, by table name."""
        ...


class RecordAirbyte(Protocol):
    def ensure_destination(self, configuration: Mapping[str, Any]) -> None: ...

    def ensure_source(self, table: str, configuration: Mapping[str, Any]) -> None: ...

    def ensure_connection(self, table: str, connection: Mapping[str, Any]) -> str: ...

    def destination_branch_now(self) -> str: ...

    def running_jobs(self, connection_id: str) -> list[int]: ...

    def start_sync(self, connection_id: str) -> int: ...

    def job_status(self, job: int) -> str: ...

    def cancel_job(self, job: int) -> None: ...


@dataclass(frozen=True, slots=True)
class Deadlines:
    """Seconds: how long one sync may run, and how long to wait between two reads of its status."""

    sync_seconds: float = 3600.0
    poll_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class LoadResult:
    run_id: str
    manifest_sha256: str
    branch: str
    base_hash: str
    head_hash: str
    snapshots: dict[str, int]
    jobs: dict[str, int]
    copies: tuple[Copied, ...]
    connectors: tuple[str, ...]

    def to_json(self) -> str:
        document = asdict(self)
        document["copies"] = [asdict(copy) for copy in self.copies]
        document["connectors"] = list(self.connectors)
        return json.dumps(document, indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> LoadResult:
        try:
            document = json.loads(text)
            if not isinstance(document, dict) or set(document) != set(RECORD_FIELDS):
                raise ValueError
            copies = tuple(Copied(**entry) for entry in document["copies"] if set(entry) == set(COPY_FIELDS))
            snapshots, jobs = document["snapshots"], document["jobs"]
            if len(copies) != len(document["copies"]) or not all(isinstance(value, int) for value in (*snapshots.values(), *jobs.values())):
                raise ValueError
            return cls(
                str(document["run_id"]),
                str(document["manifest_sha256"]),
                str(document["branch"]),
                str(document["base_hash"]),
                str(document["head_hash"]),
                dict(snapshots),
                dict(jobs),
                copies,
                tuple(str(image) for image in document["connectors"]),
            )
        except ValueError, TypeError, AttributeError:
            raise LoadError("the record of this run is not a record of this form") from None


def record_path(record_dir: Path, run_id: str) -> Path:
    return record_dir / f"{run_id}.json"


def read_record(record_dir: Path, run_id: str) -> LoadResult | None:
    path = record_path(record_dir, run_id)
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError:
        raise LoadError("the record of this run cannot be read") from None
    return LoadResult.from_json(text)


def write_record(record_dir: Path, result: LoadResult) -> Path:
    """Write the record whole under its final name, or not at all. A record that exists is not replaced."""
    record_dir.mkdir(parents=True, exist_ok=True)
    final = record_path(record_dir, result.run_id)
    descriptor, temporary = tempfile.mkstemp(prefix=".writing_", dir=record_dir)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(result.to_json())
        os.chmod(temporary, 0o644)
        os.link(temporary, final)
    except FileExistsError:
        raise LoadError("the record of this run exists and is not replaced") from None
    finally:
        os.unlink(temporary)
    return final


def confirm_finished(record: LoadResult, reference: ManifestRef, catalog: Catalog) -> LoadResult:
    """The record of a finished run, once its manifest, its branch head and its snapshots are as they were recorded."""
    if record.manifest_sha256 != reference.sha256:
        raise LoadError("this run ID was loaded from another manifest")
    if not catalog.has_branch(record.branch) or catalog.head(record.branch) != record.head_hash:
        raise LoadError(f"the branch {record.branch} is not at the head that the load recorded")
    now = catalog.snapshot_ids(record.branch)
    if {table: now.get(table) for table in record.snapshots} != record.snapshots:
        raise LoadError(f"the tables on the branch {record.branch} are not the snapshots that the load recorded")
    return record


def wait_for_job(airbyte: RecordAirbyte, job: int, table: str, deadlines: Deadlines, clock: Callable[[], float], sleep: Callable[[float], None]) -> None:
    """Wait for the sync to succeed. Raise ``LoadError`` for any other end, and ``SyncDeadline`` after the job is cancelled when the deadline passes."""
    deadline = clock() + deadlines.sync_seconds
    while True:
        status = airbyte.job_status(job)
        if status == SUCCEEDED:
            return
        if status in ENDED_WITHOUT_SUCCESS:
            raise LoadError(f"the sync of {table} ended with the status {status}")
        if clock() >= deadline:
            try:
                airbyte.cancel_job(job)
                cancelled = "and was cancelled"
            except AirbyteError:
                cancelled = "and could not be cancelled: cancel it in Airbyte"
            raise SyncDeadline(f"the sync of {table} did not end within {deadlines.sync_seconds:g} seconds {cancelled}")
        sleep(deadlines.poll_seconds)


def result_tags(result: LoadResult, prefix: str) -> dict[str, str]:
    """The facts of a load as run tags under ``prefix``: the branch, its two heads, the manifest and each table's snapshot. A value is short."""
    tags = {
        f"{prefix}/load_branch": result.branch,
        f"{prefix}/base_hash": result.base_hash,
        f"{prefix}/head_hash": result.head_hash,
        f"{prefix}/manifest_sha256": result.manifest_sha256,
    }
    tags.update({f"{prefix}/snapshot_{table}": str(snapshot) for table, snapshot in sorted(result.snapshots.items())})
    return tags


def connectors() -> tuple[str, ...]:
    return tuple(sorted(connector.image for connector in (SOURCE_S3, SOURCE_FILE, DESTINATION)))


def load_records(
    *,
    store: CopyingStore,
    reference: ManifestRef,
    run_id: str,
    environment: Environment,
    catalog: Catalog,
    airbyte: RecordAirbyte,
    deadlines: Deadlines,
    record_dir: Path,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> LoadResult:
    """Run the load of the module text and return its record. Raise ``GateRefused``, ``CopyError`` or ``LoadError``, and ``ValueError`` for a run ID."""
    branch = branch_name(run_id)
    copies = copy_bound_versions(store, reference, run_id)
    finished = read_record(record_dir, run_id)
    if finished is not None:
        return confirm_finished(finished, reference, catalog)
    confirm_copies(store, run_id, copies)
    airbyte.ensure_destination(destination_configuration(environment, branch))
    connection_ids: dict[str, str] = {}
    for table in TABLES:
        airbyte.ensure_source(table.name, source_configuration(table, run_id, environment))
        connection_ids[table.name] = airbyte.ensure_connection(table.name, connection_configuration(table))
    confirm_copies(store, run_id, copies)
    if any(airbyte.running_jobs(identity) for identity in connection_ids.values()):
        raise LoadError("a sync of a record connection is already running")
    if catalog.has_branch(branch):
        catalog.delete_branch(branch, catalog.head(branch))
    base_hash = catalog.create_branch(branch)
    before = catalog.snapshot_ids(branch)
    if airbyte.destination_branch_now() != branch:
        raise LoadError(f"the destination of Airbyte does not write to the branch {branch}")
    jobs: dict[str, int] = {}
    for table in TABLES:
        confirm_copies(store, run_id, copies)
        jobs[table.name] = airbyte.start_sync(connection_ids[table.name])
        wait_for_job(airbyte, jobs[table.name], table.name, deadlines, clock, sleep)
    head_hash = catalog.head(branch)
    if head_hash == base_hash:
        raise LoadError(f"the head of {branch} did not move: the syncs wrote nothing")
    snapshots = catalog.snapshot_ids(branch)
    unchanged = [table.name for table in TABLES if snapshots.get(table.name) is None or snapshots.get(table.name) == before.get(table.name)]
    if unchanged:
        raise LoadError(f"no new snapshot of {', '.join(unchanged)} on the branch {branch}")
    loaded = {table.name: snapshots[table.name] for table in TABLES}
    result = LoadResult(run_id, reference.sha256, branch, base_hash, head_hash, loaded, jobs, copies, connectors())
    write_record(record_dir, result)
    return result
