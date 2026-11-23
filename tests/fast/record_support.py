"""Shared set-up for the fast tests of the record connections: a release with the two CSV tables landed and a manifest of its check, in a stand-in store that
also copies an exact object version, and the stand-ins of the catalog and of Airbyte that the load talks to.

Each stand-in keeps a log of its calls, so a test proves which calls were made and in which order, and each has switches that plant one fault. The real
services are proven by the acceptance test and, for Airbyte, on the host: these stand-ins hold only what the code under test depends on.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ingestion.airbyte_api import AirbyteError
from scripts.ingestion.manifest import ManifestRef, publish_manifest
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.record_load import BranchExists, BranchMoved, LoadError, RefState
from scripts.ingestion.record_state import RunState
from scripts.ingestion.store import ObjectVersion, StoreError
from tests.fast.ingestion_support import SOURCE_PREFIX, Landed, list_text_for, selection_for
from tests.fast.memory_store import MemoryStore

CLINICAL = b"caseid,subjectid,age\n0001,00042,60\n0002,00042,71\n"
LAB = b"caseid,dt,name,result\n0001,10,cr,0.9\n0002,20,cr,1.1\n"
RELEASE_FILES = {"clinical_data.csv": CLINICAL, "lab_data.csv": LAB, "vital_files/0001.vital": b"recording one, with enough bytes to need several blocks"}
RUN = "run_20261122_ab12cd34"
BASE_HASH = "a" * 64


class CopyingMemoryStore(MemoryStore):
    """The stand-in store with the copy of one exact version. ``corrupt`` names source keys whose copy gets other bytes."""

    def __init__(self, bucket: str = "example-bucket", *, versioned: bool = True) -> None:
        super().__init__(bucket, versioned=versioned)
        self.copies: list[tuple[str, str, str]] = []
        self.corrupt: set[str] = set()
        self.deletes: list[tuple[str, str]] = []
        self.fail_deletes = False
        self.corrupt_same_length: set[str] = set()

    def copy_version(self, version: ObjectVersion, key: str) -> ObjectVersion:
        self.copies.append((version.key, version.version_id, key))
        data = self.find(version).data
        if version.key in self.corrupt:
            data += b"!"
        if version.key in self.corrupt_same_length:
            data = data[:-1] + bytes([data[-1] ^ 1])
        return self.put_object(key, data)

    def delete_version(self, key: str, version_id: str) -> None:
        """Delete exactly this version, as the real store does: a version that is not there is not an error, and a failing store raises."""
        if self.fail_deletes:
            raise StoreError(Notice(Code.VERSION_MISSING))
        self.deletes.append((key, version_id))
        if key in self.history:
            self.remove_version(key, version_id)


@dataclass
class RecordLanded(Landed):
    """A landed set whose store also copies an exact version."""

    store: CopyingMemoryStore


class MemoryLedger:
    """The run's record of the object versions it wrote, in memory: what ``RunState`` keeps in a file."""

    def __init__(self) -> None:
        self.recorded: dict[str, set[str]] = {}

    def versions(self, key: str) -> frozenset[str]:
        return frozenset(self.recorded.get(key, set()))

    def keys(self) -> frozenset[str]:
        return frozenset(key for key, versions in self.recorded.items() if versions)

    def add(self, key: str, version_id: str) -> None:
        self.recorded.setdefault(key, set()).add(version_id)

    def discard(self, key: str, version_id: str) -> None:
        self.recorded.get(key, set()).discard(version_id)


def landed_records(source: Mapping[str, bytes] = RELEASE_FILES, store: CopyingMemoryStore | None = None) -> tuple[RecordLanded, ManifestRef]:
    """Every file of ``source`` landed, one version each, the check passed and the manifest published. The store is a ``CopyingMemoryStore``, or ``store``."""
    list_bytes = list_text_for(source).encode()
    store = CopyingMemoryStore() if store is None else store
    landed = RecordLanded(store, selection_for(list(source), list_bytes), list_bytes, {}, {}, {})
    for path, data in source.items():
        landed.land(path, data)
    return landed, publish_manifest(store, landed.selection, landed.check())


def source_key(name: str) -> str:
    return SOURCE_PREFIX + name


class Stopped(RuntimeError):
    """A stop of the process between two steps of the load, planted by a test."""


class Interrupts:
    """Names of the calls after which, or before which, the process stops: ``stop_after`` raises ``Stopped`` once the call has had its effect."""

    def __init__(self) -> None:
        self.stop_before: set[str] = set()
        self.stop_after: set[str] = set()

    def before(self, name: str) -> None:
        if name in self.stop_before:
            self.stop_before.discard(name)
            raise Stopped(name)

    def after(self, name: str) -> None:
        if name in self.stop_after:
            self.stop_after.discard(name)
            raise Stopped(name)


class FakeCatalog(Interrupts):
    """The catalog calls of the load: the branches with their chains of commits (oldest first) and the snapshot of each table on each branch.

    ``created_at`` is the hash each branch was made at, which a test compares with what the run's state file said at the time of a deletion: ``violations``
    lists each deletion of a branch that the state file did not name at that hash.
    """

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []
        self.chains: dict[str, list[str]] = {"main": [BASE_HASH]}
        self.tables: dict[str, dict[str, int]] = {"main": {}}
        self.counter = 0
        self.recorded: set[str] = set()
        self.cannot_establish_absence = False
        self.created_at: dict[str, str] = {}
        self.state_dir: Path | None = None
        self.violations: list[str] = []
        self.deleted: list[str] = []
        self.kinds: dict[str, str] = {}
        self.typed_misses: list[str] = []

    @property
    def heads(self) -> _Heads:
        return _Heads(self)

    def advance_main(self) -> str:
        """Another commit on main, as a merge of another load makes one."""
        self.counter += 1
        self.chains["main"].append(f"{self.counter + 5000:064x}")
        return self.chains["main"][-1]

    def has_recorded_manifest(self, sha256: str) -> bool:
        self.calls.append("has_recorded_manifest")
        if sha256 in self.recorded:
            return True
        if self.cannot_establish_absence:
            raise LoadError("cannot establish absence: the history is longer than the scan")
        return False

    def head(self, branch: str) -> str:
        self.calls.append(f"head {branch}")
        return self.chains[branch][-1]

    def reference(self, name: str) -> RefState | None:
        self.calls.append(f"reference {name}")
        return RefState(self.kinds.get(name, "BRANCH"), self.chains[name][-1]) if name in self.chains else None

    def descends_from(self, branch: str, ancestor: str) -> bool:
        self.calls.append(f"descends_from {branch}")
        return ancestor in self.chains[branch]

    def create_branch(self, branch: str, at: str) -> str:
        self.calls.append(f"create_branch {branch}")
        self.before("create_branch")
        if branch in self.chains:
            raise BranchExists(branch)
        self.chains[branch] = self.chains["main"][: self.chains["main"].index(at) + 1]
        self.tables[branch] = dict(self.tables["main"])
        self.created_at[branch] = at
        self.after("create_branch")
        return at

    def make_branch(self, branch: str, *, root: str | None = None) -> None:
        """A branch that another writer made: from main's head, or, with ``root``, from a history that main does not hold."""
        self.chains[branch] = [root] if root else list(self.chains["main"])
        self.tables[branch] = dict(self.tables["main"])
        self.created_at[branch] = self.chains[branch][-1]

    def make_tag(self, name: str, *, at: str) -> None:
        """A tag of another writer, at the hash ``at`` of main: a reference that is not a branch, whatever its name."""
        self.chains[name] = self.chains["main"][: self.chains["main"].index(at) + 1]
        self.tables[name] = dict(self.tables["main"])
        self.created_at[name] = at
        self.kinds[name] = "TAG"

    def delete_branch(self, branch: str, head: str) -> None:
        self.calls.append(f"delete_branch {branch}")
        if self.kinds.get(branch, "BRANCH") != "BRANCH":
            self.typed_misses.append(branch)
            return
        self.before("delete_branch")
        recorded = RunState(self.state_dir, RUN).branch if self.state_dir is not None else None
        if recorded != (branch, self.created_at.get(branch)):
            self.violations.append(f"{branch} was deleted while the state file named {recorded} and the branch was made at {self.created_at.get(branch)}")
        if self.chains[branch][-1] != head:
            raise BranchMoved(branch)
        del self.chains[branch], self.tables[branch]
        self.deleted.append(branch)
        self.after("delete_branch")

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        self.calls.append(f"snapshot_ids {branch}")
        return {name.removeprefix("raw_records."): snapshot for name, snapshot in self.tables[branch].items() if name.startswith("raw_records.")}

    def commit_table(self, branch: str, table: str) -> None:
        """What a sync does to its branch: the table has a new snapshot and the branch a new head."""
        self.counter += 1
        self.tables[branch][f"raw_records.{table}"] = 1000 + self.counter
        self.chains[branch].append(f"{self.counter:064x}")


class _Heads:
    """The heads of the branches as a mapping, for the tests that read or set one."""

    def __init__(self, catalog: FakeCatalog) -> None:
        self.catalog = catalog

    def __getitem__(self, branch: str) -> str:
        return self.catalog.chains[branch][-1]

    def __setitem__(self, branch: str, value: str) -> None:
        self.catalog.chains[branch].append(value)

    def __delitem__(self, branch: str) -> None:
        del self.catalog.chains[branch]

    def __contains__(self, branch: object) -> bool:
        return branch in self.catalog.chains

    def get(self, branch: str) -> str | None:
        return self.catalog.chains[branch][-1] if branch in self.catalog.chains else None


class CommitsTables(Protocol):
    def commit_table(self, branch: str, table: str) -> None: ...


class FakeAirbyte(Interrupts):
    """The Airbyte calls of the load. A sync commits its table to the branch that the destination named when the sync started.

    ``outcomes`` maps a table to the final status of its job (``succeeded`` by default). ``stuck`` is a job that ends as ``cancelled`` once it is cancelled, and
    ``unkillable`` is a job that acknowledges a cancellation and still reports ``running``. ``polls`` is how many status reads answer ``running`` before the job
    ends and answer ``polls_as``. With a ``clock``, each status read takes ``latency`` seconds of it, so a job can end after its deadline although it is read
    at once. ``unreadable`` is how many status reads fail as a call that did not finish, ``unreadable_after_cancel`` the same after a cancellation, and
    ``status_text`` replaces the answer of every read with that text. ``timeouts`` lists each status read and cancellation with the time that it was given
    and the clock's time when it was asked.
    """

    def __init__(self, catalog: CommitsTables) -> None:
        super().__init__()
        self.catalog = catalog
        self.polls_as = "running"
        self.unreadable = 0
        self.unreadable_after_cancel = False
        self.status_text: str | None = None
        self.timeouts: list[tuple[str, float | None, float]] = []
        self.calls: list[str] = []
        self.destination_branch = ""
        self.read_from: dict[str, str] = {}
        self.outcomes: dict[str, str] = {}
        self.running: list[int] = []
        self.running_for: dict[str, list[int]] = {}
        self.jobs: dict[int, dict[str, Any]] = {}
        self.polls = 1
        self.cancelled: list[int] = []
        self.commits = True
        self.skip_tables: set[str] = set()
        self.after_start: dict[str, Callable[[], None]] = {}
        self.on_ensure_connection: Callable[[str], None] | None = None
        self.on_first_call: Callable[[], None] | None = None
        self.report_branch: str | None = None
        self.known: dict[str, str] = {}
        self.clock: Any = None
        self.latency = 0.0
        self.cancel_fails = False
        self.running_error = False
        self.start_error = False
        self.on_status: Callable[[], None] | None = None

    def find_connection_ids(self) -> dict[str, str]:
        self.calls.append("find_connection_ids")
        return dict(self.known)

    def ensure_destination(self, configuration: Mapping[str, Any]) -> None:
        if self.on_first_call is not None:
            self.on_first_call()
        self.calls.append("ensure_destination")
        self.destination_branch = configuration["main_branch_name"]

    def ensure_source(self, table: str, configuration: Mapping[str, Any]) -> None:
        self.calls.append(f"ensure_source {table}")
        streams = configuration.get("streams")
        self.read_from[table] = streams[0]["globs"][0] if streams else configuration["url"]

    def ensure_connection(self, table: str, connection: Mapping[str, Any]) -> str:
        self.calls.append(f"ensure_connection {table}")
        if self.on_ensure_connection is not None:
            self.on_ensure_connection(table)
        self.known[table] = f"connection_{table}"
        return f"connection_{table}"

    def destination_branch_now(self) -> str:
        self.calls.append("destination_branch_now")
        return self.destination_branch if self.report_branch is None else self.report_branch

    def running_jobs(self, connection_id: str) -> list[int]:
        self.calls.append(f"running_jobs {connection_id}")
        if self.running_error:
            raise AirbyteError("GET jobs gave a job entry that is not an object")
        return list(self.running_for.get(connection_id, self.running))

    def start_sync(self, connection_id: str) -> int:
        table = connection_id.removeprefix("connection_")
        self.calls.append(f"start_sync {table}")
        if self.start_error:
            raise AirbyteError("POST jobs has no whole number field jobId: a job may have started that cannot be named")
        job = len(self.jobs) + 1
        self.jobs[job] = {"table": table, "branch": self.destination_branch, "polls": 0}
        if table in self.after_start:
            self.after_start[table]()
        self.after(f"start_sync {table}")
        return job

    def job_status(self, job: int, *, timeout: float | None = None) -> str:
        state = self.jobs[job]
        if self.on_status is not None:
            self.on_status()
        state["polls"] += 1
        if state["polls"] > 500:
            raise AssertionError("the status of a job was read 500 times: the wait has no deadline")
        outcome = self.outcomes.get(state["table"], "succeeded")
        self.calls.append(f"job_status {state['table']}")
        self.timeouts.append(("job_status", timeout, self.clock.now if self.clock is not None else 0.0))
        if self.clock is not None:
            self.clock.now += self.latency
        if self.unreadable > 0 or (self.unreadable_after_cancel and state.get("cancelled")):
            self.unreadable = max(0, self.unreadable - 1)
            raise AirbyteError("GET jobs did not finish within its deadline")
        if self.status_text is not None:
            return self.status_text
        if outcome == "stuck" and state.get("cancelled"):
            return "cancelled"
        if outcome in ("stuck", "unkillable") or state["polls"] <= self.polls:
            return self.polls_as
        if outcome == "succeeded" and self.commits and state["table"] not in self.skip_tables:
            self.catalog.commit_table(state["branch"], state["table"])
        return outcome

    def cancel_job(self, job: int, *, timeout: float | None = None) -> None:
        self.calls.append(f"cancel_job {self.jobs[job]['table']}")
        self.timeouts.append(("cancel_job", timeout, self.clock.now if self.clock is not None else 0.0))
        if self.cancel_fails:
            raise AirbyteError("DELETE jobs could not reach Airbyte")
        self.jobs[job]["cancelled"] = True
        self.cancelled.append(job)


class FakeClock:
    """A clock that moves only when the code sleeps, so a deadline passes in a test without waiting."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
