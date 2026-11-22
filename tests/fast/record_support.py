"""Shared set-up for the fast tests of the record connections: a release with the two CSV tables landed and a manifest of its check, in a stand-in store that
also copies an exact object version, and the stand-ins of the catalog and of Airbyte that the load talks to.

Each stand-in keeps a log of its calls, so a test proves which calls were made and in which order, and each has switches that plant one fault. The real
services are proven by the acceptance test and, for Airbyte, on the host: these stand-ins hold only what the code under test depends on.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from scripts.ingestion.airbyte_api import AirbyteError
from scripts.ingestion.manifest import ManifestRef, publish_manifest
from scripts.ingestion.notice import Code, Notice
from scripts.ingestion.record_load import LoadError
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

    def delete_version(self, version: ObjectVersion) -> None:
        if self.fail_deletes:
            raise StoreError(Notice(Code.VERSION_MISSING))
        self.deletes.append((version.key, version.version_id))
        self.remove_version(version.key, version.version_id)


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


def landed_records(source: Mapping[str, bytes] = RELEASE_FILES) -> tuple[RecordLanded, ManifestRef]:
    """Every file of ``source`` landed, one version each, the check passed and the manifest published. The store is a ``CopyingMemoryStore``."""
    list_bytes = list_text_for(source).encode()
    store = CopyingMemoryStore()
    landed = RecordLanded(store, selection_for(list(source), list_bytes), list_bytes, {}, {}, {})
    for path, data in source.items():
        landed.land(path, data)
    return landed, publish_manifest(store, landed.selection, landed.check())


def source_key(name: str) -> str:
    return SOURCE_PREFIX + name


class FakeCatalog:
    """The catalog calls of the load: the branches with their heads and the snapshot of each table on each branch."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.heads: dict[str, str] = {"main": BASE_HASH}
        self.tables: dict[str, dict[str, int]] = {"main": {}}
        self.counter = 0
        self.recorded: set[str] = set()
        self.cannot_establish_absence = False

    def has_recorded_manifest(self, sha256: str) -> bool:
        self.calls.append("has_recorded_manifest")
        if sha256 in self.recorded:
            return True
        if self.cannot_establish_absence:
            raise LoadError("cannot establish absence: the history is longer than the scan")
        return False

    def head(self, branch: str) -> str:
        self.calls.append(f"head {branch}")
        return self.heads[branch]

    def has_branch(self, branch: str) -> bool:
        self.calls.append(f"has_branch {branch}")
        return branch in self.heads

    def create_branch(self, branch: str) -> str:
        self.calls.append(f"create_branch {branch}")
        if branch in self.heads:
            raise FileExistsError(branch)
        self.heads[branch] = self.heads["main"]
        self.tables[branch] = dict(self.tables["main"])
        return self.heads[branch]

    def delete_branch(self, branch: str, head: str) -> None:
        self.calls.append(f"delete_branch {branch}")
        if self.heads.get(branch) != head:
            raise FileExistsError(branch)
        del self.heads[branch], self.tables[branch]

    def snapshot_ids(self, branch: str) -> dict[str, int]:
        self.calls.append(f"snapshot_ids {branch}")
        return {name.removeprefix("raw_records."): snapshot for name, snapshot in self.tables[branch].items() if name.startswith("raw_records.")}

    def commit_table(self, branch: str, table: str) -> None:
        """What a sync does to its branch: the table has a new snapshot and the branch a new head."""
        self.counter += 1
        self.tables[branch][f"raw_records.{table}"] = 1000 + self.counter
        self.heads[branch] = f"{self.counter:064x}"


class CommitsTables(Protocol):
    def commit_table(self, branch: str, table: str) -> None: ...


class FakeAirbyte:
    """The Airbyte calls of the load. A sync commits its table to the branch that the destination named when the sync started.

    ``outcomes`` maps a table to the final status of its job (``succeeded`` by default). ``stuck`` is a job that ends as ``cancelled`` once it is cancelled, and
    ``unkillable`` is a job that acknowledges a cancellation and still reports ``running``. ``polls`` is how many status reads answer ``running`` before the job
    ends. With a ``clock``, each status read takes ``latency`` seconds of it, so a job can end after its deadline although it is read at once.
    """

    def __init__(self, catalog: CommitsTables) -> None:
        self.catalog = catalog
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
        return list(self.running_for.get(connection_id, self.running))

    def start_sync(self, connection_id: str) -> int:
        table = connection_id.removeprefix("connection_")
        self.calls.append(f"start_sync {table}")
        job = len(self.jobs) + 1
        self.jobs[job] = {"table": table, "branch": self.destination_branch, "polls": 0}
        if table in self.after_start:
            self.after_start[table]()
        return job

    def job_status(self, job: int) -> str:
        state = self.jobs[job]
        state["polls"] += 1
        if state["polls"] > 500:
            raise AssertionError("the status of a job was read 500 times: the wait has no deadline")
        outcome = self.outcomes.get(state["table"], "succeeded")
        self.calls.append(f"job_status {state['table']}")
        if self.clock is not None:
            self.clock.now += self.latency
        if outcome == "stuck" and state.get("cancelled"):
            return "cancelled"
        if outcome in ("stuck", "unkillable") or state["polls"] <= self.polls:
            return "running"
        if outcome == "succeeded" and self.commits and state["table"] not in self.skip_tables:
            self.catalog.commit_table(state["branch"], state["table"])
        return outcome

    def cancel_job(self, job: int) -> None:
        self.calls.append(f"cancel_job {self.jobs[job]['table']}")
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
