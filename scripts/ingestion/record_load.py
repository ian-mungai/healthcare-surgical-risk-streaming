"""The record load: the three Airbyte record connections run on one Nessie load branch, in a fixed order, and the record of what they loaded.

The order is the contract, and each step stops the load before the next one starts:

1. The gate (``verify_before_read``) confirms the manifest and that each CSV table's current landed version is the bound one. A failed or missing check (no
   manifest), a missing version and a substituted version are refused here, before anything is copied, started or branched.
2. A run that has its record is finished: its branch must still be at the recorded head with the recorded snapshots, the versions that its state file names are
   removed if they are still there, and the call returns the record and changes nothing else. A run ID that was loaded from another manifest is refused.
3. The same manifest on another run: when any commit on the default branch carries the line ``records-manifest: <SHA-256>`` (the convention below) for this
   manifest, the earlier run's record is returned and nothing is copied, started or branched. A history that is longer than the scan and does not hold the
   line cannot prove that the manifest is new, so the load is refused (``cannot establish absence``).
4. Everything that the run would touch and may not own is found first, before any copy, settings change or sync, and nothing is deleted: a reference of
   the run's name is refused unless it is a branch (its kind is read, so a tag is refused), the state file names it and its history holds the hash that it
   was made at; no job of the three connections may be active (the job list must be in the documented shape). A version at a read key that the state file
   does not name is refused by the copy's first look, before any copy.
5. The exact-version copy of the two CSV tables (``record_copy``) to ``records_input/<run ID>/<SHA-256>/<table>.csv``. A copy whose hash differs fails here.
6. The destination, the three sources and the three connections are made to match ``record_definitions`` for this run. The copies are confirmed again.
7. The load branch ``load_<run ID>`` is made at the head of main. A branch that the state file records and that the first look found is the leftover of an
   unfinished try: it is deleted at exactly the head that was read then (Nessie refuses a branch that moved), and the state entry is cleared. The new record
   (the name and the hash it is made at; the kind is always a branch) is written to the state file before the branch is made, so a stop at any point leaves
   either nothing to own or a branch that is recorded. Airbyte is asked which branch its destination writes to, and the load stops unless it is the load branch.
8. Each table is synced in turn: ``clinical_data``, ``lab_data``, then ``track_list``. The copies are confirmed before each sync, so one replaced while an
   earlier sync read it is found. A sync that succeeds after its deadline is not accepted. Each request gets the time that is left of its own budget: the
   sync's deadline for the reads of the wait, the confirmation's for the cancellation and its reads. A job that passes its deadline, or whose status cannot be
   read or is outside the statuses of the API, is cancelled and read until it is terminal. A job that is not confirmed terminal inside the confirmation's
   budget, for any reason (a read that timed out, a cancellation that could not be asked for), fails the load as ``job_still_active`` with its ID, which the
   state file holds from the start of the job. A sync that ends in any status but success fails the load and the later syncs do not start.
9. After the last sync every read key must hold exactly the one version that the run wrote, current or not (``copy_changed`` otherwise): the connector reads
   by key, so a version that was current while it read, or one restored afterwards, shows as a second entry. The branch is then left unmerged and nothing is
   recorded as finished.
10. The branch head must have moved, and every table must have a new snapshot on the branch.
11. The record (``<run ID>.json``) is written once and whole: the branch, its head before and after, each table's snapshot and job, the manifest, the copies and
    the pinned connectors. Then the versions that the state file names are deleted by key and version ID. A load that fails keeps its copies for inspection. A
    removal that fails leaves the record, and a second call removes them.

The load never merges. The table checks run on the branch, and the merge follows only when they pass.

The convention that the merge step follows: the message of the merge commit on the default branch holds one line ``records-manifest: <SHA-256 of the manifest
that the load read>``, 64 lower case hex digits, as the load's record names it (``manifest_sha256``).
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, NoReturn, Protocol

from scripts.ingestion.airbyte_api import ACTIVE_STATUSES, TERMINAL_STATUSES, AirbyteError
from scripts.ingestion.folder import list_files
from scripts.ingestion.gate import verify_before_read
from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_copy import (
    Copied,
    CopyError,
    CopyingStore,
    confirm_copies,
    copy_bound_versions,
    remove_copies,
    verify_read_keys,
)
from scripts.ingestion.record_definitions import (
    DESTINATION,
    MAIN_BRANCH,
    RECORD_SOURCE_KEYS,
    SOURCE_FILE,
    SOURCE_S3,
    TABLES,
    Environment,
    branch_name,
    connection_configuration,
    destination_configuration,
    source_configuration,
)
from scripts.ingestion.record_state import RunState
from scripts.ingestion.store import StoreError

SUCCEEDED = "succeeded"
ENDED_WITHOUT_SUCCESS = tuple(status for status in TERMINAL_STATUSES if status != SUCCEEDED)
LOAD_RECORD_FORMAT = "load_record_2"
LEGACY_LOAD_RECORD_FORMAT = "load_record_1"
LOAD_RECORD_FILE = "{run_id}.load_record.json"
OWN_FIELDS = ("run_id", "manifest_sha256", "base_hash", "head_hash", "snapshots", "jobs", "copies", "connectors")
LOAD_RECORD_FIELDS = ("format", "branch", "branch_hash", "base_hash", "manifest", "tables")
RECORD_FIELDS = (*OWN_FIELDS, *(name for name in LOAD_RECORD_FIELDS if name not in OWN_FIELDS))
COPY_FIELDS = ("table", "source_key", "key", "version_id", "sha256", "size")
SHA256_TEXT = re.compile(r"[0-9a-f]{64}")
MANIFEST_LINE = re.compile(r"^records-manifest: ([0-9a-f]{64})$", re.MULTILINE)


class LoadError(RuntimeError):
    """The load cannot go on or did not give the tables it should. The message names a table, a branch or a status and never a value of a source."""


class SyncDeadline(LoadError):
    """A sync did not end within its deadline, or succeeded after it and is not accepted."""


class JobStillActive(LoadError):
    """A job is still active after its cancellation was asked for, or the cancellation could not be asked for. The message starts with ``job_still_active``."""


class BranchExists(LoadError):
    """The branch could not be made because the name is taken."""


class BranchMoved(LoadError):
    """The branch is no longer at the head that the caller read, so it was not deleted."""


BRANCH_KIND = "BRANCH"


@dataclass(frozen=True, slots=True)
class RefState:
    """What the catalog says a reference is: its kind (``BRANCH``, ``TAG``) and the hash it points to."""

    kind: str
    hash: str


class Catalog(Protocol):
    def head(self, branch: str) -> str: ...

    def reference(self, name: str) -> RefState | None:
        """The kind and hash of the reference of this name, or None when there is none."""
        ...

    def has_recorded_manifest(self, sha256: str) -> bool:
        """Whether any commit of the default branch's history carries the ``records-manifest`` line for this manifest. Raise ``LoadError`` when the history is
        longer than the scan and the line was not found: absence cannot then be established."""
        ...

    def descends_from(self, branch: str, ancestor: str) -> bool:
        """Whether the commit ``ancestor`` is the head of the branch or in its history. Raise ``LoadError`` when that cannot be established."""
        ...

    def create_branch(self, branch: str, at: str) -> str:
        """Create the branch at exactly the hash ``at`` of main and return that hash. Raise ``BranchExists`` when the name is taken."""
        ...

    def delete_branch(self, branch: str, head: str) -> None:
        """Delete the branch when it is still at ``head``. Raise ``BranchMoved`` when it is at another hash. A branch that is gone is not an error."""
        ...

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        """The snapshot of each table of the raw namespace on the branch, by table name."""
        ...


class RecordAirbyte(Protocol):
    def find_connection_ids(self) -> dict[str, str]:
        """The connections of the three tables that exist now, by table name. Nothing is created or changed."""
        ...

    def ensure_destination(self, configuration: Mapping[str, Any]) -> None: ...

    def ensure_source(self, table: str, configuration: Mapping[str, Any]) -> None: ...

    def ensure_connection(self, table: str, connection: Mapping[str, Any]) -> str: ...

    def destination_branch_now(self) -> str: ...

    def running_jobs(self, connection_id: str) -> list[int]:
        """The IDs of the active sync jobs of the connection. Raise ``AirbyteError`` for an answer that is not in the documented shape."""
        ...

    def start_sync(self, connection_id: str) -> int: ...

    def job_status(self, job: int, *, timeout: float | None = None) -> str:
        """The status of the job, one of the statuses of the API, read inside ``timeout`` seconds. Raise ``AirbyteError`` for any other answer or no answer."""
        ...

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None:
        """Ask for the cancellation of the job inside ``timeout`` seconds. Raise ``AirbyteError`` when it cannot be asked for."""
        ...


@dataclass(frozen=True, slots=True)
class Deadlines:
    """Seconds: how long one sync may run, how long to wait between two reads of its status, and how long to wait for a cancelled job to become terminal."""

    sync_seconds: float = 3600.0
    poll_seconds: float = 10.0
    cancel_confirm_seconds: float = 120.0


@dataclass(frozen=True, slots=True)
class LoadResult:
    run_id: str
    manifest: ManifestRef
    branch: str
    base_hash: str
    head_hash: str
    snapshots: dict[str, int]
    jobs: dict[str, int]
    copies: tuple[Copied, ...]
    connectors: tuple[str, ...]

    @property
    def manifest_sha256(self) -> str:
        return self.manifest.sha256

    def load_record(self) -> dict[str, Any]:
        """The six fields that the table checks read as ``load_record_2``: format, branch, branch head, base head, manifest, snapshots."""
        return {
            "format": LOAD_RECORD_FORMAT,
            "branch": self.branch,
            "branch_hash": self.head_hash,
            "base_hash": self.base_hash,
            "manifest": asdict(self.manifest),
            "tables": dict(self.snapshots),
        }

    def to_json(self) -> str:
        """The run record: this tool's own fields beside the five of ``load_record``."""
        document = {
            "run_id": self.run_id,
            "manifest_sha256": self.manifest.sha256,
            "base_hash": self.base_hash,
            "head_hash": self.head_hash,
            "snapshots": dict(self.snapshots),
            "jobs": dict(self.jobs),
            "copies": [asdict(copy) for copy in self.copies],
            "connectors": list(self.connectors),
            **self.load_record(),
        }
        return json.dumps(document, indent=2, sort_keys=True) + "\n"

    def load_record_json(self) -> str:
        """Exactly the six fields and no other, because the table checks' parser refuses a record with any other field."""
        return json.dumps(self.load_record(), indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> LoadResult:
        try:
            document = json.loads(text)
            if isinstance(document, dict) and document.get("format") == LEGACY_LOAD_RECORD_FORMAT:
                document = {**document, "format": LOAD_RECORD_FORMAT}
            if not isinstance(document, dict) or set(document) != set(RECORD_FIELDS):
                raise ValueError
            copies = tuple(Copied(**entry) for entry in document["copies"] if set(entry) == set(COPY_FIELDS))
            snapshots, jobs = document["snapshots"], document["jobs"]
            if len(copies) != len(document["copies"]) or not all(isinstance(value, int) for value in (*snapshots.values(), *jobs.values())):
                raise ValueError
            result = cls(
                str(document["run_id"]),
                ManifestRef(**document["manifest"]),
                str(document["branch"]),
                str(document["base_hash"]),
                str(document["head_hash"]),
                dict(snapshots),
                dict(jobs),
                copies,
                tuple(str(image) for image in document["connectors"]),
            )
            if json.loads(result.to_json()) != document:
                raise ValueError
            return result
        except ValueError, TypeError, AttributeError:
            raise LoadError("the record of this run is not a record of this form") from None


def manifest_line(sha256: str) -> str:
    """The line that a merge commit's message holds to say which manifest the merged load read. Raise ``ValueError`` unless it is 64 lower case hex digits."""
    if SHA256_TEXT.fullmatch(sha256) is None:
        raise ValueError("a manifest SHA-256 is 64 lower case hexadecimal digits")
    return f"records-manifest: {sha256}"


def recorded_manifest(message: str) -> str | None:
    """The manifest SHA-256 that a commit message records, or None when it has no well-formed line."""
    found = MANIFEST_LINE.search(message)
    return found.group(1) if found else None


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


def write_load_record(record_dir: Path, result: LoadResult) -> Path:
    """Write the file for the table checks whole under its final name. It is replaced by the same content when a retry writes it again."""
    final = record_dir / LOAD_RECORD_FILE.format(run_id=result.run_id)
    descriptor, temporary = tempfile.mkstemp(prefix=".writing_", dir=record_dir)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(result.load_record_json())
        os.chmod(temporary, 0o644)
        os.replace(temporary, final)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return final


def current_load_record_text(path: Path) -> str:
    """The text of the file for the table checks at ``path``, made again from the full run record beside it when version 1 wrote it.

    Version 1 wrote five fields and no base head. Its full run record saved the base head, so the file is regenerated from that record in the current
    format. A file of the current format is returned as it is, and the table checks' parser judges it.
    """
    text = path.read_text()
    try:
        document = json.loads(text)
    except ValueError:
        return text
    if not isinstance(document, dict) or document.get("format") != LEGACY_LOAD_RECORD_FORMAT:
        return text
    run_id = path.name.removesuffix(LOAD_RECORD_FILE.format(run_id=""))
    if not run_id or path.name != LOAD_RECORD_FILE.format(run_id=run_id):
        raise LoadError("the load record is of version 1 and its name does not give a run ID")
    full = read_record(path.parent, run_id)
    if full is None:
        raise LoadError("the load record is of version 1 and the full record that it is made again from is missing")
    write_load_record(path.parent, full)
    return full.load_record_json()


def write_record(record_dir: Path, result: LoadResult) -> Path:
    """Write the file for the table checks, then the run record whole under its final name, or not at all. A run record that exists is not replaced.

    The run record is the mark of a finished load, so it is written last: a stop between the two files leaves a run that a retry loads again.
    """
    record_dir.mkdir(parents=True, exist_ok=True)
    write_load_record(record_dir, result)
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


def earlier_record(record_dir: Path, sha256: str) -> LoadResult | None:
    """The record of the newest file in ``record_dir`` whose manifest is ``sha256``, or None. A file that is not a record is not one."""
    found: list[tuple[float, str, LoadResult]] = []
    for path in list_files(record_dir, "*.json"):
        try:
            record = LoadResult.from_json(path.read_text())
        except LoadError:
            continue
        modified = path.stat().st_mtime
        if record.manifest_sha256 == sha256:
            found.append((modified, path.name, record))
    return max(found, key=lambda item: item[:2])[2] if found else None


def confirm_finished(record: LoadResult, reference: ManifestRef, catalog: Catalog) -> LoadResult:
    """The record of a finished run, once its manifest, its branch head and its snapshots are as they were recorded."""
    if record.manifest_sha256 != reference.sha256:
        raise LoadError("this run ID was loaded from another manifest")
    found = catalog.reference(record.branch)
    if found is None or found.kind != BRANCH_KIND or found.hash != record.head_hash:
        raise LoadError(f"the branch {record.branch} is not at the head that the load recorded")
    now = catalog.snapshot_ids(record.branch)
    if {table: now.get(table) for table in record.snapshots} != record.snapshots:
        raise LoadError(f"the tables on the branch {record.branch} are not the snapshots that the load recorded")
    return record


def wait_for_job(airbyte: RecordAirbyte, job: int, table: str, deadlines: Deadlines, clock: Callable[[], float], sleep: Callable[[float], None]) -> None:
    """Wait for the sync to succeed inside its deadline. Raise ``LoadError`` for any other end.

    Each status read is given what is left of the sync's budget. A success that is read after the deadline is not accepted. When the deadline passes, or a
    status cannot be read, or its text is not one of the statuses of the API, the job is stopped: ``stop_job`` cancels it and reads it until it is terminal.
    """
    deadline = clock() + deadlines.sync_seconds
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            stop_job(airbyte, job, table, deadlines, clock, sleep, f"did not end within {deadlines.sync_seconds:g} seconds", late=True)
        try:
            status = airbyte.job_status(job, timeout=remaining)
        except AirbyteError:
            stop_job(airbyte, job, table, deadlines, clock, sleep, "has a status that could not be read")
        now = clock()
        if status == SUCCEEDED:
            if now > deadline:
                raise SyncDeadline(f"the sync of {table} succeeded after its deadline of {deadlines.sync_seconds:g} seconds and is not accepted")
            return
        if status in ENDED_WITHOUT_SUCCESS:
            raise LoadError(f"the sync of {table} ended with the status {status}")
        if status not in ACTIVE_STATUSES:
            stop_job(airbyte, job, table, deadlines, clock, sleep, "has a status outside the statuses of the API")
        sleep(max(0.0, min(deadlines.poll_seconds, deadline - now)))


def stop_job(
    airbyte: RecordAirbyte,
    job: int,
    table: str,
    deadlines: Deadlines,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    because: str,
    *,
    late: bool = False,
) -> NoReturn:
    """Cancel a job that cannot be waited for and read it until it is terminal. Always raises.

    The whole of the cancellation and its confirmation has one budget, ``cancel_confirm_seconds``, and each request is given what is left of it. A job that
    is terminal inside the budget raises ``SyncDeadline`` (``late``) or ``LoadError``. A job that is not confirmed terminal inside the budget, whatever the
    reason (a cancellation that could not be asked for, a read that timed out, a status that is not one of the statuses of the API), raises ``JobStillActive``
    with the job's ID. An answer that comes back after the budget has passed is not a confirmation, whatever it says: the clock is read after each read. The
    budget is exceeded by at most the overshoot of one exchange, which ``airbyte_api`` states.
    """
    ends = clock() + deadlines.cancel_confirm_seconds
    asked = True
    try:
        airbyte.cancel_job(job, timeout=deadlines.cancel_confirm_seconds)
    except AirbyteError:
        asked = False
    while True:
        remaining = ends - clock()
        if remaining <= 0:
            break
        try:
            status = airbyte.job_status(job, timeout=remaining)
        except AirbyteError:
            status = ""
        if clock() > ends:
            break
        if status in TERMINAL_STATUSES:
            ended = "was cancelled" if status == "cancelled" else f"ended as {status}"
            raise (SyncDeadline if late else LoadError)(f"the sync of {table} {because} and {ended}")
        sleep(max(0.0, min(deadlines.poll_seconds, ends - clock())))
    reason = "its cancellation was asked for" if asked else "its cancellation could not be asked for"
    raise JobStillActive(
        f"job_still_active: job {job} ({table}) was not confirmed as ended {deadlines.cancel_confirm_seconds:g} seconds after {reason}: cancel it in Airbyte"
    )


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


def remove_all(store: CopyingStore, run_id: str, state: RunState) -> None:
    try:
        remove_copies(store, run_id, state)
    except CopyError, StoreError:
        raise LoadError("the load is recorded but its copies in the read prefix could not be removed; call again to remove them") from None


def owned_leftover(catalog: Catalog, state: RunState, branch: str) -> str | None:
    """The head of the leftover branch of an unfinished try that this run owns, or None when there is no branch of the name.

    A reference of the name is owned only when it is a branch (the kind is read, so a tag of the name is refused), the state file records the name, and the
    branch's history holds the hash that the run made it at: the triple of kind, name and creation hash. Anything else is refused, and nothing is deleted.
    The hash that is returned is the one that the deletion is sent with, and the deletion names the kind as well.
    """
    found = catalog.reference(branch)
    if found is None:
        return None
    if found.kind != BRANCH_KIND:
        raise LoadError(f"the reference {branch} exists and is not a branch: it is refused and not deleted")
    if state.branch is None or state.branch[0] != branch:
        raise LoadError(f"the branch {branch} exists and this host has no record that this run created it: it is refused and not deleted")
    if not catalog.descends_from(branch, state.branch[1]):
        raise LoadError(f"the branch {branch} is not the branch that this run created: it is refused and not deleted")
    return found.hash


def make_branch(catalog: Catalog, state: RunState, branch: str, leftover: str | None) -> str:
    """Delete the owned leftover, clear its entry, record the new pair and then make the branch. Return the hash that it was made at.

    The order is the safety: the entry that authorised the deletion is cleared before a new branch can exist, and the new pair is written before the branch
    is made, so a stop between any two of these steps leaves no branch that the state file does not name, and a retry finds either nothing to own or a branch
    it can prove it made. A name that is taken when the branch is made is not owned, so its pair is forgotten. The ownership check refuses before any write, but
    the read of ``main`` comes after the deletion: a ``main`` hash that is refused there leaves the leftover deleted and its entry cleared, and a retry owns
    nothing and makes the branch again.
    """
    if leftover is not None:
        catalog.delete_branch(branch, leftover)
    if state.branch is not None:
        state.forget_branch()
    at = catalog.head(MAIN_BRANCH)
    state.record_branch(branch, at)
    try:
        made = catalog.create_branch(branch, at)
    except BranchExists:
        state.forget_branch()
        raise
    if made != at:
        raise LoadError(f"the branch {branch} was not made at the hash that was asked for")
    return made


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
    entries = verify_before_read(store, reference, RECORD_SOURCE_KEYS)
    state = RunState(record_dir, run_id)
    finished = read_record(record_dir, run_id)
    if finished is not None:
        result = confirm_finished(finished, reference, catalog)
        write_load_record(record_dir, result)
        remove_all(store, run_id, state)
        return result
    if catalog.has_recorded_manifest(reference.sha256):
        earlier = earlier_record(record_dir, reference.sha256)
        if earlier is None:
            raise LoadError("the default branch records this manifest but this host keeps no record of its load")
        return earlier
    leftover = owned_leftover(catalog, state, branch)
    if any(airbyte.running_jobs(identity) for identity in airbyte.find_connection_ids().values()):
        raise LoadError("a sync of a record connection is already running")
    copies = copy_bound_versions(store, reference, run_id, state, entries)
    confirm_copies(store, run_id, copies)
    hashes = {copy.table: copy.sha256 for copy in copies}
    airbyte.ensure_destination(destination_configuration(environment, branch))
    connection_ids: dict[str, str] = {}
    for table in TABLES:
        airbyte.ensure_source(table.name, source_configuration(table, run_id, environment, hashes.get(table.name, "")))
        connection_ids[table.name] = airbyte.ensure_connection(table.name, connection_configuration(table))
    confirm_copies(store, run_id, copies)
    base_hash = make_branch(catalog, state, branch, leftover)
    before = catalog.snapshot_ids(branch)
    if airbyte.destination_branch_now() != branch:
        raise LoadError(f"the destination of Airbyte does not write to the branch {branch}")
    jobs: dict[str, int] = {}
    for table in TABLES:
        confirm_copies(store, run_id, copies)
        jobs[table.name] = airbyte.start_sync(connection_ids[table.name])
        state.record_job(table.name, jobs[table.name])
        wait_for_job(airbyte, jobs[table.name], table.name, deadlines, clock, sleep)
    verify_read_keys(store, copies)
    head_hash = catalog.head(branch)
    if head_hash == base_hash:
        raise LoadError(f"the head of {branch} did not move: the syncs wrote nothing")
    snapshots = catalog.snapshot_ids(branch)
    unchanged = [table.name for table in TABLES if snapshots.get(table.name) is None or snapshots.get(table.name) == before.get(table.name)]
    if unchanged:
        raise LoadError(f"no new snapshot of {', '.join(unchanged)} on the branch {branch}")
    loaded = {table.name: snapshots[table.name] for table in TABLES}
    result = LoadResult(run_id, reference, branch, base_hash, head_hash, loaded, jobs, copies, connectors())
    write_record(record_dir, result)
    remove_all(store, run_id, state)
    return result
