"""The capacity probe's record: what was read and what was not, written once as one JSON file, with no judgment in it.

The record holds the samples as read, the cgroup files of the Spark container and of each required service as read with the time of each read, the Docker
events and the windows they were read in, the configured limits copied beside the figures as plain values and the job as it ended. It never compares a figure
with a limit, a cap or a reserve and never says that a run passed: the owner reads the figures and decides. ``run_state`` is ``recorded`` when the loop ran to
the end of the Spark job and the finalization finished, and ``interrupted`` when a stop flag, the deadline or an error ended it earlier (the record still holds
what was collected). ``gaps`` lists plain facts, each a fixed code with a count or a step name, never a response body or an address.
"""

from __future__ import annotations

import json
import os
import platform
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.stack.host_figures import CgroupMemory, HostFigures
from scripts.stack.probe_docker import ContainerEvent, ProbeError

SCHEMA = 4
RECORD_NAME = "capacity_probe.json"
SPARK_SERVICE = "spark"
RUN_RECORDED = "recorded"
RUN_INTERRUPTED = "interrupted"
GAP_CODES = ("events_window_missing", "sample_error", "spark_not_seen", "cgroup_unreadable", "cleanup_step_failed", "job_result_unreadable")
STREAM_LAG_NOTE = "not measured: the stream path does not exist in this phase; the live batch of the phase that builds it measures stream lag"
NOTES = (
    "the job's Iceberg files stay in the warehouse until table maintenance removes them (phase 2 slice 13)",
    "memory.peak counts page cache that the kernel can still reclaim and covers the time since the container started",
    "a sample's covers_start_ns and covers_end_ns are the times of the two host readings that its rates and its processor shares come from",
)
RESULT_TEXT_LIMIT = 200
RESULT_FIELDS: Mapping[str, type] = {
    "branch": str,
    "branch_hash": str,
    "table": str,
    "rows": int,
    "memory_limit_mib": int,
    "python_version": str,
    "uid": int,
    "snapshot_id": int,
    "spark_version": str,
    "jvm_max_heap_mib": int,
}


@dataclass(frozen=True)
class Sample:
    """One reading of the host and of the containers. ``missing`` names each figure that was not available to this sample."""

    taken_ns: int
    utc: str
    covers_start_ns: int
    covers_end_ns: int
    missing: tuple[str, ...]
    spark_running: bool
    services_running: Mapping[str, bool]
    host: HostFigures | None
    containers_bytes: Mapping[str, int]


@dataclass(frozen=True)
class CgroupRead:
    read_at_ns: int
    memory: CgroupMemory


@dataclass(frozen=True)
class ServiceSnapshot:
    """What a required service showed at one moment. ``None`` means the figure was not read."""

    running: bool | None
    restarts: int | None
    started_at: float | None
    oom_killed_flag: bool | None
    docker_memory_cap_bytes: int | None
    cgroup: CgroupRead | None


@dataclass(frozen=True)
class CleanupStep:
    name: str
    verified_absent: bool
    detail: str


@dataclass(frozen=True)
class JobFacts:
    exit_code: int
    oom_killed_flag: bool
    seconds: float
    result: Mapping[str, Any]
    result_readable: bool


@dataclass(frozen=True)
class Collected:
    run_state: str
    cause: str
    step: str
    mode: str
    samples: Sequence[Sample]
    spark_cgroup: Sequence[CgroupRead]
    events: Sequence[ContainerEvent]
    windows: Sequence[tuple[int, int]]
    run_start_ns: int
    job_exit_ns: int | None
    start_snapshot: Mapping[str, ServiceSnapshot]
    end_snapshot: Mapping[str, ServiceSnapshot]
    job: JobFacts | None
    cleanup: Sequence[CleanupStep]
    sample_errors: int
    cgroup_unreadable: int
    scratch: str


def read_result(result: Mapping[str, Any]) -> dict[str, Any] | None:
    """The job's documented fields as read, or None when one is missing or of another type. A value is never compared with anything."""
    clean: dict[str, Any] = {}
    for name, kind in RESULT_FIELDS.items():
        value = result.get(name)
        if not isinstance(value, kind) or isinstance(value, bool) or (isinstance(value, str) and not 0 < len(value) <= RESULT_TEXT_LIMIT):
            return None
        clean[name] = value
    return clean


def event_window_holes(windows: Sequence[tuple[int, int]], run_start_ns: int, job_exit_ns: int | None) -> int:
    """How many facts say that the event windows are not one chain from before the group start to after the Spark job's exit."""
    if not windows:
        return 1
    holes = int(windows[0][0] > run_start_ns)
    holes += sum(1 for earlier, later in zip(windows, windows[1:], strict=False) if later[0] != earlier[1])
    return holes + int(job_exit_ns is not None and windows[-1][1] < job_exit_ns)


def gaps_of(run: Collected) -> list[dict[str, Any]]:
    """The plain facts about what was not read, each a fixed code with a count or a step name."""
    found: list[dict[str, Any]] = []
    if holes := event_window_holes(run.windows, run.run_start_ns, run.job_exit_ns):
        found.append({"code": "events_window_missing", "count": holes})
    if run.sample_errors:
        found.append({"code": "sample_error", "count": run.sample_errors})
    if not any(sample.spark_running for sample in run.samples):
        found.append({"code": "spark_not_seen"})
    snapshots = (*run.start_snapshot.values(), *run.end_snapshot.values())
    unreadable = run.cgroup_unreadable + sum(1 for item in snapshots if item.running and item.cgroup is None)
    if run.mode == "host" and unreadable:
        found.append({"code": "cgroup_unreadable", "count": unreadable})
    found += [{"code": "cleanup_step_failed", "step": step.name} for step in run.cleanup if not step.verified_absent]
    if run.job is not None and not run.job.result_readable:
        found.append({"code": "job_result_unreadable"})
    return found


def sample_document(sample: Sample) -> dict[str, Any]:
    host = None
    if sample.host is not None:
        host = {
            "memory_used_mib": sample.host.memory_used_mib,
            "memory_available_mib": sample.host.memory_available_mib,
            "memory_available_kib": sample.host.memory_available_kib,
            "swap_used_mib": sample.host.swap_used_mib,
            "cpu_percent": sample.host.cpu_percent,
            "cpu_core_percent": list(sample.host.cpu_core_percent),
            "disk_read_mib_s": sample.host.disk_read_mib_s,
            "disk_write_mib_s": sample.host.disk_write_mib_s,
            "free_disk_mib": sample.host.free_disk_mib,
        }
    return {
        "taken_ns": sample.taken_ns,
        "utc": sample.utc,
        "covers_start_ns": sample.covers_start_ns,
        "covers_end_ns": sample.covers_end_ns,
        "missing": list(sample.missing),
        "spark_running": sample.spark_running,
        "services_running": dict(sample.services_running),
        "host": host,
        "containers_bytes": dict(sample.containers_bytes),
    }


def cgroup_document(read: CgroupRead | None) -> dict[str, Any] | None:
    if read is None:
        return None
    return {
        "read_at_ns": read.read_at_ns,
        "memory_peak_bytes": read.memory.peak_bytes,
        "memory_max_bytes": read.memory.max_bytes,
        "memory_events": dict(read.memory.events),
    }


def snapshot_document(snapshot: ServiceSnapshot) -> dict[str, Any]:
    return {
        "running": snapshot.running,
        "restarts": snapshot.restarts,
        "started_at": snapshot.started_at,
        "oom_killed_flag": snapshot.oom_killed_flag,
        "docker_memory_cap_bytes": snapshot.docker_memory_cap_bytes,
        "cgroup": cgroup_document(snapshot.cgroup),
    }


def build_record(
    run: Collected,
    *,
    limits_mib: Mapping[str, int],
    reserve_mib: int,
    required: Sequence[str],
    interval_seconds: float,
    deadline_seconds: float,
    started_utc: str,
    finished_utc: str,
    job_arguments: Sequence[str],
) -> dict[str, Any]:
    """The record of a run, as read."""
    job = run.job
    return {
        "schema": SCHEMA,
        "run_state": run.run_state,
        "interruption": {"cause": run.cause, "step": run.step},
        "mode": run.mode,
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        "computer": {"system": platform.system(), "machine": platform.machine()},
        "interval_seconds": interval_seconds,
        "run_deadline_seconds": deadline_seconds,
        "configured": {"host_reserve_mib": reserve_mib, "limits_mib": dict(limits_mib)},
        "required_services": list(required),
        "workload": {
            "service": SPARK_SERVICE,
            "arguments": list(job_arguments),
            "exit_code": None if job is None else job.exit_code,
            "oom_killed_flag": None if job is None else job.oom_killed_flag,
            "seconds": None if job is None else round(job.seconds, 1),
            "result": {} if job is None else dict(job.result),
        },
        "scratch_volume": run.scratch,
        "samples": [sample_document(sample) for sample in run.samples],
        "spark_cgroup": [cgroup_document(read) for read in run.spark_cgroup],
        "services_at_start": {name: snapshot_document(item) for name, item in run.start_snapshot.items()},
        "services_at_end": {name: snapshot_document(item) for name, item in run.end_snapshot.items()},
        "events": [{"action": event.action, "service": event.service, "detail": event.detail, "time_ns": event.nano} for event in run.events],
        "event_windows": [{"since_ns": since, "until_ns": until} for since, until in run.windows],
        "cleanup": [{"step": step.name, "verified_absent": step.verified_absent, "detail": step.detail} for step in run.cleanup],
        "gaps": gaps_of(run),
        "notes": list(NOTES),
        "stream_lag": STREAM_LAG_NOTE,
    }


def check_destination(folder: Path) -> None:
    """Refuse a folder that holds a record or cannot take one, a link that leads nowhere included. It creates nothing."""
    if os.path.lexists(folder / RECORD_NAME):
        raise ProbeError(f"{folder} already holds a record: a probe run writes a new folder")
    existing = folder
    for step in (folder, *folder.parents):
        if os.path.lexists(step):
            existing = step
            break
    if not existing.is_dir() or not os.access(existing, os.W_OK | os.X_OK):
        raise ProbeError(f"cannot write the record into {folder}: {existing} is not a folder that can be written")


def write_document(folder: Path, document: Mapping[str, Any]) -> Path:
    """Write the record into ``folder``, which must not hold one. The file appears whole or not at all."""
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / RECORD_NAME
    handle, name = tempfile.mkstemp(dir=folder, prefix=".record_")
    temporary = Path(name)
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(json.dumps(document, indent=2, allow_nan=False) + "\n")
        try:
            os.link(temporary, target)
        except FileExistsError as error:
            raise ProbeError(f"{folder} already holds a record: a probe run writes a new folder") from error
    finally:
        temporary.unlink(missing_ok=True)
    return target
