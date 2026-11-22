"""The capacity probe's evidence, its verdict and its record.

A run is ``complete`` only when every measurement that the card asks for was made and every figure is inside its limit. The verdict has four values:

- ``failed``: a figure is outside its limit or an event, a cleanup step or the job itself failed. Each finding is listed with its service or step.
- ``not_measured``: nothing failed, but a measurement is missing: a loop that did not finish, event windows that do not chain from before the group start to
  after the Spark job's exit, fewer than ``MIN_OVERLAP_SAMPLES`` samples that hold the running Spark container and every required service (each with a CPU
  figure on the host), or a container with no cap, peak or kill count.
- ``local_only``: the same on a computer that is not the Linux host (only Docker's figures exist). It is never ``complete``.
- ``complete``: the Linux host, nothing failed and nothing missing.

Memory is compared in bytes, and rounding is for display only. A container's cap must equal the configured limit and its peak must be at or below it. The
host keeps its reserve when the lowest ``MemAvailable`` in KiB is at least the reserve in KiB.
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

from scripts.stack.host_figures import BYTES_PER_MIB, KIB_PER_MIB, HostFigures
from scripts.stack.probe_docker import ContainerEvent, ProbeError, SamplingError

SCHEMA = 3
RECORD_NAME = "capacity_probe.json"
SPARK_SERVICE = "spark"
STARTUP_PHASE = "startup"
OVERLAP_PHASE = "overlap"
MIN_OVERLAP_SAMPLES = 3
VERDICT_COMPLETE = "complete"
VERDICT_LOCAL_ONLY = "local_only"
VERDICT_NOT_MEASURED = "not_measured"
VERDICT_FAILED = "failed"
PASSING_VERDICTS = frozenset({VERDICT_COMPLETE, VERDICT_LOCAL_ONLY})
STREAM_LAG_NOTE = "not measured: the stream path does not exist in this phase; the live batch of the phase that builds it measures stream lag"
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
LIMIT_NOTES = (
    "the job's Iceberg files stay in the warehouse until table maintenance removes them (phase 2 slice 13)",
    "a container's peak on the host is its cgroup's memory.peak, which counts page cache the kernel can still reclaim, so it is an upper bound",
)


@dataclass(frozen=True)
class Sample:
    """One reading of the host and of the containers. ``spark_running`` is the state of the named Spark container at the same iteration."""

    seconds: float
    utc: str
    host: HostFigures | None
    containers_bytes: Mapping[str, int]
    spark_running: bool = False


@dataclass(frozen=True)
class ServiceBaseline:
    """What a required service showed when the always-on group was up and before the Spark job started."""

    restarts: int
    oom_kills: int | None
    started_before_run: bool | None


@dataclass(frozen=True)
class ContainerEvidence:
    """What one container showed at the end of its time in the run. ``None`` means the figure could not be read."""

    service: str
    running: bool
    cap_bytes: int | None
    peak_bytes: int | None
    oom_kills: int | None
    oom_killed: bool
    restarts: int
    source: str


@dataclass(frozen=True)
class CleanupStep:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class JobOutcome:
    exit_code: int
    oom_killed: bool
    seconds: float
    result: Mapping[str, Any]
    result_error: str
    evidence: ContainerEvidence | None


@dataclass(frozen=True)
class Execution:
    outcome: JobOutcome | None
    failure: str
    samples: Sequence[Sample]
    loop_complete: bool
    events: Sequence[ContainerEvent]
    windows: Sequence[tuple[int, int]]
    run_start_ns: int
    job_exit_ns: int | None
    baseline: Mapping[str, ServiceBaseline]
    evidence: Mapping[str, ContainerEvidence]
    cleanup: Sequence[CleanupStep]
    interrupted: bool = False
    evidence_error: str = ""
    addresses: Mapping[str, str] | None = None
    scratch: str = ""


@dataclass(frozen=True)
class Assessment:
    failures: tuple[str, ...]
    gaps: tuple[str, ...]
    checks: Mapping[str, bool | None]
    lowest_available_kib: int | None
    overlap: tuple[Sample, ...]
    polled_peak_bytes: Mapping[str, int]

    def verdict(self, host_measured: bool) -> str:
        if self.failures:
            return VERDICT_FAILED
        if self.gaps:
            return VERDICT_NOT_MEASURED
        return VERDICT_COMPLETE if host_measured else VERDICT_LOCAL_ONLY


def mib(value: float) -> float:
    return round(value / BYTES_PER_MIB, 1)


def polled_peaks(samples: Sequence[Sample]) -> dict[str, int]:
    """The highest memory in bytes that any sample showed for each service."""
    peaks: dict[str, int] = {}
    for sample in samples:
        for name, used in sample.containers_bytes.items():
            peaks[name] = max(peaks.get(name, 0), used)
    return dict(sorted(peaks.items()))


def peaks_of(samples: Sequence[Sample]) -> dict[str, Any]:
    """The peaks of a series: the highest use and the lowest available memory and free space of the host, and the highest memory of each service."""
    if not samples:
        raise SamplingError("the series holds no sample")
    hosts = [sample.host for sample in samples if sample.host is not None]
    cpu = [item.cpu_percent for item in hosts if item.cpu_percent is not None]
    cores = [value for item in hosts for value in item.cpu_core_percent if value is not None]
    host = (
        {
            "memory_used_mib": max(item.memory_used_mib for item in hosts),
            "memory_available_lowest_mib": min(item.memory_available_mib for item in hosts),
            "swap_used_mib": max(item.swap_used_mib for item in hosts),
            "cpu_percent": max(cpu, default=None),
            "cpu_core_percent": max(cores, default=None),
            "disk_read_mib_s": max(item.disk_read_mib_s for item in hosts),
            "disk_write_mib_s": max(item.disk_write_mib_s for item in hosts),
            "free_disk_lowest_mib": min(item.free_disk_mib for item in hosts),
        }
        if hosts
        else None
    )
    return {"samples": len(samples), "host": host, "containers_mib": {name: mib(value) for name, value in polled_peaks(samples).items()}}


def startup_samples(samples: Sequence[Sample]) -> list[Sample]:
    """The samples before the Spark container was first seen running."""
    found: list[Sample] = []
    for sample in samples:
        if sample.spark_running:
            break
        found.append(sample)
    return found


def overlap_samples(samples: Sequence[Sample], required: Sequence[str], *, host_measured: bool) -> list[Sample]:
    """Exactly the samples that hold the named Spark container running and a memory figure for every required service, each with a CPU figure on the host."""
    found = []
    for sample in samples:
        holds = sample.spark_running and SPARK_SERVICE in sample.containers_bytes and all(name in sample.containers_bytes for name in required)
        has_cpu = not host_measured or (sample.host is not None and sample.host.cpu_percent is not None)
        if holds and has_cpu:
            found.append(sample)
    return found


def coverage_gaps(windows: Sequence[tuple[int, int]], run_start_ns: int, job_exit_ns: int | None) -> list[str]:
    """Why the event windows are not one chain from before the group start to after the Spark job's exit. Empty means they are."""
    if not windows:
        return ["no event window was read"]
    gaps = []
    if windows[0][0] > run_start_ns:
        gaps.append("the first event window starts after the run started")
    if any(later[0] != earlier[1] for earlier, later in zip(windows, windows[1:], strict=False)):
        gaps.append("the event windows are not contiguous")
    if job_exit_ns is not None and windows[-1][1] < job_exit_ns:
        gaps.append("the last event window ends before the Spark job's exit")
    return gaps


def validate_result(result: Mapping[str, Any], *, branch: str, rows: int, memory_limit_mib: int) -> tuple[dict[str, Any], str]:
    """The job's result reduced to its documented fields, and an empty text or the reason the result is not the one that was asked for.

    A field of the wrong type, a branch other than the requested one, a row count other than the requested one and a memory limit other than the configured one
    are each refused. Nothing the job printed outside the documented fields is kept.
    """
    clean: dict[str, Any] = {}
    for name, kind in RESULT_FIELDS.items():
        value = result.get(name)
        if not isinstance(value, kind) or isinstance(value, bool) or (isinstance(value, str) and not 0 < len(value) <= RESULT_TEXT_LIMIT):
            return {}, f"the job's result has no valid {name}"
        clean[name] = value
    if clean["branch"] != branch:
        return {}, "the job's result names another branch than the requested one"
    if clean["rows"] != rows:
        return {}, f"the job wrote {clean['rows']} rows and {rows} were requested"
    if clean["memory_limit_mib"] != memory_limit_mib:
        return {}, f"the job saw a memory limit of {clean['memory_limit_mib']} MiB and {memory_limit_mib} MiB is configured"
    return clean, ""


def container_findings(
    service: str,
    evidence: ContainerEvidence | None,
    configured_bytes: int,
    polled_peak: int | None,
    baseline: ServiceBaseline | None,
    *,
    required: bool,
) -> tuple[list[str], list[str]]:
    """The failures and the gaps of one container: its cap, its peak, its kills since the start and its restarts."""
    if evidence is None:
        return [], [f"service {service}: no evidence was collected"]
    failures: list[str] = []
    gaps: list[str] = []
    if required and not evidence.running:
        failures.append(f"service {service}: not running at the end of the run")
    if evidence.cap_bytes is None:
        gaps.append(f"service {service}: its memory cap was not read")
    elif evidence.cap_bytes != configured_bytes:
        failures.append(f"service {service}: the memory cap is {evidence.cap_bytes} bytes and the configured limit is {configured_bytes} bytes")
    peak = evidence.peak_bytes if evidence.peak_bytes is not None else polled_peak
    if peak is None:
        gaps.append(f"service {service}: its peak memory was not measured")
    elif peak > configured_bytes:
        failures.append(f"service {service}: the peak memory is {peak} bytes, above the limit of {configured_bytes} bytes")
    if evidence.oom_killed:
        failures.append(f"service {service}: its container was killed for memory")
    if required:
        if baseline is None:
            gaps.append(f"service {service}: its restarts and kills were not counted at the start")
        else:
            if evidence.restarts != baseline.restarts:
                failures.append(f"service {service}: restarted {evidence.restarts - baseline.restarts} time(s) during the run")
            if evidence.oom_kills is not None and baseline.oom_kills is not None and evidence.oom_kills > baseline.oom_kills:
                failures.append(f"service {service}: the kernel killed a process for memory {evidence.oom_kills - baseline.oom_kills} time(s) during the run")
            if evidence.oom_kills is not None and baseline.oom_kills is None:
                gaps.append(f"service {service}: its kills for memory were not counted at the start")
    elif evidence.oom_kills:
        failures.append(f"service {service}: the kernel killed a process for memory {evidence.oom_kills} time(s)")
    return failures, gaps


def assess(execution: Execution, *, limits_mib: Mapping[str, int], required: Sequence[str], reserve_mib: int, host_measured: bool) -> Assessment:
    """Every finding of a run against the limits and the reserve, and the checks that sum them up."""
    failures: list[str] = [execution.failure] if execution.failure else []
    gaps: list[str] = []
    samples, outcome = execution.samples, execution.outcome
    polled = polled_peaks(samples)
    unlisted = sorted(name for name in polled if name not in limits_mib)
    failures += [f"service {name}: has no limit in the limits file" for name in unlisted]
    container_failures: list[str] = []
    for service in (*required, SPARK_SERVICE):
        is_required = service != SPARK_SERVICE
        if service == SPARK_SERVICE and outcome is None:
            continue
        evidence = execution.evidence.get(service) if is_required else outcome.evidence if outcome is not None else None
        found, missing = container_findings(
            service, evidence, limits_mib[service] * BYTES_PER_MIB, polled.get(service), execution.baseline.get(service), required=is_required
        )
        container_failures += found
        gaps += missing
    lifecycle = [f"service {event.service}: {event.describe()} during the run" for event in execution.events if event.service in required]
    lifecycle += [
        f"service {SPARK_SERVICE}: {event.describe()} during the run" for event in execution.events if event.service == SPARK_SERVICE and event.action == "oom"
    ]
    spark_oom = any(event.service == SPARK_SERVICE and event.action == "oom" for event in execution.events)
    completed = outcome is not None and outcome.exit_code == 0 and not outcome.oom_killed and not outcome.result_error and not spark_oom
    if outcome is not None and outcome.exit_code != 0:
        failures.append(f"the Spark job exited with status {outcome.exit_code}")
    if outcome is not None and outcome.oom_killed:
        failures.append("the Spark job was killed for memory")
    if outcome is not None and outcome.result_error:
        failures.append(outcome.result_error)
    failures += container_failures + lifecycle
    failures += [f"cleanup step {step.name} failed: {step.detail}" for step in execution.cleanup if not step.ok]
    if execution.interrupted:
        failures.append("the run was interrupted")
    if execution.evidence_error:
        gaps.append(f"the end-of-run evidence could not be read: {execution.evidence_error}")
    if not execution.loop_complete:
        gaps.append("the sampling loop did not run to its end")
    chain = coverage_gaps(execution.windows, execution.run_start_ns, execution.job_exit_ns)
    gaps += chain
    overlap = overlap_samples(samples, required, host_measured=host_measured)
    if len(overlap) < MIN_OVERLAP_SAMPLES:
        gaps.append(f"{len(overlap)} samples hold the running Spark container and every required service, and {MIN_OVERLAP_SAMPLES} are needed")
    available = [sample.host.memory_available_kib for sample in samples if sample.host is not None]
    lowest = min(available) if available else None
    reserve_kept: bool | None = None
    if host_measured:
        if lowest is None:
            gaps.append("no sample holds a host figure")
        else:
            reserve_kept = lowest >= reserve_mib * KIB_PER_MIB
            if not reserve_kept:
                failures.append(f"the host lost its reserve: the lowest available memory was {lowest} KiB and the reserve is {reserve_mib * KIB_PER_MIB} KiB")
    checks: dict[str, bool | None] = {
        "completed": completed,
        "loop_complete": execution.loop_complete,
        "events_covered": not chain,
        "overlap_samples_enough": len(overlap) >= MIN_OVERLAP_SAMPLES,
        "containers_within_limits": not container_failures and not unlisted,
        "no_lifecycle_events": not lifecycle,
        "host_reserve_kept": reserve_kept,
        "cleanup_ok": all(step.ok for step in execution.cleanup),
    }
    return Assessment(tuple(failures), tuple(gaps), checks, lowest, tuple(overlap), polled)


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
        "seconds": round(sample.seconds, 1),
        "utc": sample.utc,
        "spark_running": sample.spark_running,
        "host": host,
        "containers_bytes": dict(sample.containers_bytes),
    }


def peak_scope(service: str, baseline: ServiceBaseline | None, source: str | None) -> str:
    """What a container's peak covers, so a reader does not take a long-running service's peak for the run's."""
    if service == SPARK_SERVICE:
        return "the job container's whole life"
    if source != "cgroup":
        return "the highest figure of the samples taken during the run"
    if baseline is None or baseline.started_before_run is None:
        return "since the container started (when it started is not known)"
    return "since the container started, which was before the run" if baseline.started_before_run else "since the container started, which was during the run"


def container_document(
    service: str, evidence: ContainerEvidence | None, configured_mib: int, polled: int | None, baseline: ServiceBaseline | None
) -> dict[str, Any]:
    return {
        "configured_bytes": configured_mib * BYTES_PER_MIB,
        "cap_bytes": None if evidence is None else evidence.cap_bytes,
        "peak_bytes": None if evidence is None else evidence.peak_bytes,
        "peak_source": None if evidence is None else evidence.source,
        "peak_scope": peak_scope(service, baseline, None if evidence is None else evidence.source),
        "polled_peak_bytes": polled,
        "oom_kills_before": None if baseline is None else baseline.oom_kills,
        "oom_kills_after": None if evidence is None else evidence.oom_kills,
        "restarts_before": None if baseline is None else baseline.restarts,
        "restarts_after": None if evidence is None else evidence.restarts,
        "started_before_run": None if baseline is None else baseline.started_before_run,
        "running_at_end": None if evidence is None else evidence.running,
    }


def build_document(
    execution: Execution,
    assessment: Assessment,
    *,
    limits_mib: Mapping[str, int],
    required: Sequence[str],
    reserve_mib: int,
    host_measured: bool,
    interval_seconds: float,
    deadline_seconds: float,
    started_utc: str,
    finished_utc: str,
    job_arguments: Sequence[str],
) -> dict[str, Any]:
    """The record of a run: the verdict with its findings, the checks, the peaks of the whole run, of its start-up and of its overlap, and the series."""
    samples, outcome = execution.samples, execution.outcome
    peaks: dict[str, Any] = {}
    if samples:
        peaks["all"] = peaks_of(samples)
        before = startup_samples(samples)
        if before:
            peaks["startup"] = peaks_of(before)
        if assessment.overlap:
            peaks["overlap"] = peaks_of(assessment.overlap)
    containers = {
        service: container_document(
            service,
            execution.evidence.get(service) if service != SPARK_SERVICE else (outcome.evidence if outcome else None),
            limits_mib[service],
            assessment.polled_peak_bytes.get(service),
            execution.baseline.get(service),
        )
        for service in (*required, SPARK_SERVICE)
    }
    return {
        "schema": SCHEMA,
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        "verdict": assessment.verdict(host_measured),
        "interrupted": execution.interrupted,
        "failures": list(assessment.failures),
        "gaps": list(assessment.gaps),
        "checks": dict(assessment.checks),
        "computer": {"system": platform.system(), "machine": platform.machine()},
        "host_figures": {
            "measured": host_measured,
            "note": "read from the Linux host's /proc, statvfs and the containers' cgroups"
            if host_measured
            else "not measured: this computer is not the Linux host; only Docker's figures are real",
        },
        "interval_seconds": interval_seconds,
        "run_deadline_seconds": deadline_seconds,
        "reserve_mib": reserve_mib,
        "lowest_available_kib": assessment.lowest_available_kib,
        "overlap_samples": len(assessment.overlap),
        "addresses": dict(execution.addresses or {}),
        "scratch_volume": execution.scratch,
        "containers": containers,
        "events": [{"action": event.action, "service": event.service, "detail": event.detail} for event in execution.events],
        "event_windows": {
            "count": len(execution.windows),
            "first_since_ns": execution.windows[0][0] if execution.windows else None,
            "last_until_ns": execution.windows[-1][1] if execution.windows else None,
            "run_start_ns": execution.run_start_ns,
            "job_exit_ns": execution.job_exit_ns,
        },
        "workload": {
            "service": SPARK_SERVICE,
            "arguments": list(job_arguments),
            "exit_code": None if outcome is None else outcome.exit_code,
            "oom_killed": None if outcome is None else outcome.oom_killed,
            "seconds": None if outcome is None else round(outcome.seconds, 1),
            "result": {} if outcome is None else dict(outcome.result),
        },
        "cleanup": [{"step": step.name, "ok": step.ok, "detail": step.detail} for step in execution.cleanup],
        "limits": list(LIMIT_NOTES),
        "peaks": peaks,
        "stream_lag": STREAM_LAG_NOTE,
        "series": [sample_document(sample) for sample in samples],
    }


def check_destination(folder: Path) -> None:
    """Refuse a folder that already holds a record or cannot take one, before anything starts. The check creates nothing."""
    if os.path.lexists(folder / RECORD_NAME):
        raise ProbeError(f"{folder} already holds a record: a probe run writes a new folder")
    existing = folder
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
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
