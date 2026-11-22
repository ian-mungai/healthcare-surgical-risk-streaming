"""The capacity probe: the always-on group and one Spark job run together while the host's figures are sampled, and the peaks are saved.

``.venv/bin/python -m scripts.stack.capacity_probe [--output-directory <folder>]`` runs on the Linux host. It

1. reads the host's counters once (a host that cannot be read fails here, before anything starts),
2. starts a sampler that reads, at a fixed interval and under a deadline, the memory in use and available, the swap in use, the CPU of each core, the disk read
   and write rates and the free space of the data disk, and each Compose container's memory from Docker,
3. starts the always-on group through the group command (``scripts.stack.groups``, with its admission check) in the phase ``startup``,
4. asks the admission check for the batch group and runs the Spark job once, with a deadline, in the phase ``overlap``, and
5. stops the sampler, judges the figures and writes ``capacity_probe.json`` into a new folder under ``runs/capacity_probe/``.

The record holds the peaks of the whole run and of each phase, whether the job completed, whether every container stayed within the limit of
``compose/limits.yml`` and whether the host kept its reserve (the lowest available memory is at least ``host_reserve_mib``), and the whole series. A failure at
any step, a sampling failure included, is written into the record and ends the command with exit status 1. The command writes nothing into the limits file:
the owner reads the measured figures and decides about the reserve.

The Spark job is ``spark.jobs.catalog_proof`` with a fixed row count on a private Nessie branch of its own, which the command deletes afterwards. The group
command starts a service with its image's entry point and no arguments, which this job cannot use, so the probe asks the group's admission check
(``groups.admit``, the function ``start_group`` uses) and then runs the job as a one-off with the group's Compose profile and limits.

``--docker-figures-only`` is for a computer that is not the Linux host (the Docker figures are real and every host figure is marked as not measured). Its record
never claims the host reserve. Stream lag is not measured: the stream path does not exist in this phase.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import secrets
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ROOT
from scripts.stack.admission import AdmissionError
from scripts.stack.groups import GroupError, Settings, admit, compose_arguments, find_group, rendered_environment, settings_from_environment, start_group
from scripts.stack.host_figures import FiguresError, HostFigures, HostReading, HostSources, host_figures, parse_container_memory, read_host
from scripts.stack.limits import Limits, LimitsError, load_limits
from scripts.stack.nessie import NessieClient, NessieError

SCHEMA = 1
RECORD_NAME = "capacity_probe.json"
RUNS_FOLDER = ROOT / "runs" / "capacity_probe"
ALWAYS_ON_GROUP = "supporting"
SPARK_SERVICE = "spark"
STARTUP_PHASE = "startup"
OVERLAP_PHASE = "overlap"
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_START_DEADLINE_SECONDS = 300.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0
DEFAULT_WAIT_SECONDS = 0.0
DEFAULT_ROWS = 2_000_000
DEFAULT_NESSIE_PORT = 19120
SAMPLING_SLACK_SECONDS = 120.0
DOCKER_SECONDS = 30.0
CLEANUP_SECONDS = 60.0
NOT_FOUND = 404
RESULT_PREFIX = "RESULT "
STREAM_LAG_NOTE = "not measured: the stream path does not exist in this phase; the live batch of the phase that builds it measures stream lag"
VERDICT_COMPLETE = "complete"
VERDICT_DOCKER_ONLY = "docker figures only"
VERDICT_FAILED = "failed"
PASSING_VERDICTS = frozenset({VERDICT_COMPLETE, VERDICT_DOCKER_ONLY})


class ProbeError(RuntimeError):
    """A step of the probe failed. The message names the step and holds no text of a container."""


class SamplingError(ProbeError):
    """The sampling failed or ran past its deadline."""


@dataclass(frozen=True)
class Sample:
    seconds: float
    utc: str
    phase: str
    host: HostFigures | None
    containers_mib: Mapping[str, float]


@dataclass(frozen=True)
class JobOutcome:
    exit_code: int
    oom_killed: bool
    seconds: float
    result: Mapping[str, Any]


@dataclass(frozen=True)
class Steps:
    """The three steps of a run, each of which raises ``ProbeError`` when it cannot be done."""

    start_supporting: Callable[[], None]
    admit_job: Callable[[], None]
    run_job: Callable[[], JobOutcome]


@dataclass(frozen=True)
class Execution:
    outcome: JobOutcome | None
    failure: str
    samples: Sequence[Sample]
    error: SamplingError | None


@dataclass(frozen=True)
class Judgement:
    containers_over: Mapping[str, tuple[float, int]]
    containers_unlisted: tuple[str, ...]
    reserve_kept: bool | None
    lowest_available_mib: float | None

    @property
    def containers_within_limits(self) -> bool:
        return not self.containers_over and not self.containers_unlisted


def collect(
    take: Callable[[], Sample],
    *,
    samples: list[Sample],
    interval_seconds: float,
    deadline_seconds: float,
    stopped: Callable[[], bool],
    wait: Callable[[float], bool],
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Append a sample every ``interval_seconds`` until ``stopped`` is true, and one more after the stop. ``wait`` returns true when the stop came during it.

    The time a sample takes is taken from the wait, so the interval stays fixed. A reading that fails raises ``SamplingError`` with the number of the sample.
    The deadline passing raises ``SamplingError`` too. The samples taken stay in ``samples`` in both cases.
    """
    started = clock()

    def take_one() -> None:
        try:
            samples.append(take())
        except (ProbeError, FiguresError) as error:
            raise SamplingError(f"sample {len(samples) + 1} failed: {error}") from error

    while True:
        if clock() - started >= deadline_seconds:
            raise SamplingError(f"the sampling deadline of {deadline_seconds:g} seconds passed after {len(samples)} samples")
        began = clock()
        take_one()
        if stopped():
            return
        if wait(max(0.0, interval_seconds - (clock() - began))):
            take_one()
            return


class BackgroundSampler:
    """``collect`` on a thread of its own. ``phase`` is read by the sampling function when it labels a sample."""

    def __init__(self, take: Callable[[], Sample], *, interval_seconds: float, deadline_seconds: float, join_seconds: float) -> None:
        self.take = take
        self.interval_seconds = interval_seconds
        self.deadline_seconds = deadline_seconds
        self.join_seconds = join_seconds
        self.phase = STARTUP_PHASE
        self.samples: list[Sample] = []
        self.error: SamplingError | None = None
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.work, name="capacity-probe-sampler", daemon=True)

    def work(self) -> None:
        try:
            collect(
                self.take,
                samples=self.samples,
                interval_seconds=self.interval_seconds,
                deadline_seconds=self.deadline_seconds,
                stopped=self.stop_event.is_set,
                wait=self.stop_event.wait,
            )
        except SamplingError as error:
            self.error = error

    @property
    def running(self) -> bool:
        return self.thread.is_alive()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        """End the sampling and wait for the thread. A thread that is still running after ``join_seconds`` is a sampling failure."""
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(self.join_seconds)
        if self.thread.is_alive():
            raise SamplingError(f"the sampling did not stop within {self.join_seconds:g} seconds")


def execute(steps: Steps, sampler: BackgroundSampler) -> Execution:
    """Run the steps in order under the sampler. The first step that fails ends the run, and the sampler is stopped after every outcome."""
    failure = ""
    outcome: JobOutcome | None = None
    sampler.phase = STARTUP_PHASE
    sampler.start()
    try:
        try:
            steps.start_supporting()
        except ProbeError as error:
            failure = f"starting the always-on group failed: {error}"
        if not failure:
            sampler.phase = OVERLAP_PHASE
            try:
                steps.admit_job()
            except ProbeError as error:
                failure = f"admission of the Spark job refused: {error}"
        if not failure:
            try:
                outcome = steps.run_job()
            except ProbeError as error:
                failure = f"the Spark job could not run: {error}"
    finally:
        stop_error: SamplingError | None = None
        try:
            sampler.stop()
        except SamplingError as stop_failure:
            stop_error = stop_failure
    sampling_error = sampler.error or stop_error
    if sampling_error is not None:
        failure = f"{failure}; sampling failed: {sampling_error}" if failure else f"sampling failed: {sampling_error}"
    return Execution(outcome, failure, list(sampler.samples), sampling_error)


def rounded(value: float) -> float:
    return round(value, 1)


def peaks_of(samples: Sequence[Sample]) -> dict[str, Any]:
    """The peaks of a series: the highest use and the lowest available memory and free space of the host, and the highest memory of each service."""
    if not samples:
        raise SamplingError("the series holds no sample")
    hosts = [sample.host for sample in samples if sample.host is not None]
    containers: dict[str, float] = {}
    for sample in samples:
        for name, used in sample.containers_mib.items():
            containers[name] = max(containers.get(name, 0.0), used)
    host = (
        {
            "memory_used_mib": max(item.memory_used_mib for item in hosts),
            "memory_available_lowest_mib": min(item.memory_available_mib for item in hosts),
            "swap_used_mib": max(item.swap_used_mib for item in hosts),
            "cpu_percent": max(item.cpu_percent for item in hosts),
            "cpu_core_percent": max(max(item.cpu_core_percent, default=0.0) for item in hosts),
            "disk_read_mib_s": max(item.disk_read_mib_s for item in hosts),
            "disk_write_mib_s": max(item.disk_write_mib_s for item in hosts),
            "free_disk_lowest_mib": min(item.free_disk_mib for item in hosts),
        }
        if hosts
        else None
    )
    return {"samples": len(samples), "host": host, "containers_mib": {name: rounded(value) for name, value in sorted(containers.items())}}


def judge(samples: Sequence[Sample], limits_mib: Mapping[str, int], reserve_mib: int) -> Judgement:
    """Each service's peak against its limit in the limits file, and the lowest available memory against the reserve."""
    peaks: dict[str, float] = {}
    for sample in samples:
        for name, used in sample.containers_mib.items():
            peaks[name] = max(peaks.get(name, 0.0), used)
    over = {name: (rounded(peak), limits_mib[name]) for name, peak in sorted(peaks.items()) if name in limits_mib and peak > limits_mib[name]}
    unlisted = tuple(sorted(name for name in peaks if name not in limits_mib))
    available = [sample.host.memory_available_mib for sample in samples if sample.host is not None]
    lowest = min(available) if available else None
    return Judgement(over, unlisted, None if lowest is None else lowest >= reserve_mib, lowest)


def parse_job_result(output: str) -> dict[str, Any]:
    """The document on the last ``RESULT`` line of the job's output, or an empty one when there is none or it is not a mapping."""
    lines = [line for line in output.splitlines() if line.startswith(RESULT_PREFIX)]
    if not lines:
        return {}
    try:
        document = json.loads(lines[-1][len(RESULT_PREFIX) :])
    except ValueError:
        return {}
    return document if isinstance(document, dict) else {}


def job_arguments(stamp: str, rows: int) -> list[str]:
    return ["--branch", f"capacity_probe_{stamp.lower()}", "--rows", str(rows)]


def sample_document(sample: Sample) -> dict[str, Any]:
    host = None if sample.host is None else {**asdict(sample.host), "cpu_core_percent": list(sample.host.cpu_core_percent)}
    return {"seconds": rounded(sample.seconds), "utc": sample.utc, "phase": sample.phase, "host": host, "containers_mib": dict(sample.containers_mib)}


def verdict_of(failure: str, checks: Mapping[str, bool | None], host_measured: bool) -> str:
    if failure or not all(checks[name] for name in ("completed", "sampling_ok", "containers_within_limits")):
        return VERDICT_FAILED
    if host_measured:
        return VERDICT_COMPLETE if checks["host_reserve_kept"] is True else VERDICT_FAILED
    return VERDICT_DOCKER_ONLY


def build_document(
    execution: Execution,
    *,
    limits_mib: Mapping[str, int],
    reserve_mib: int,
    host_measured: bool,
    interval_seconds: float,
    deadline_seconds: float,
    started_utc: str,
    finished_utc: str,
    job_arguments: Sequence[str],
) -> dict[str, Any]:
    """The record of a run: the checks and the verdict, the peaks of the whole run and of each phase, the workload and the whole series."""
    samples = execution.samples
    outcome = execution.outcome
    judged = judge(samples, limits_mib, reserve_mib)
    checks: dict[str, bool | None] = {
        "completed": outcome is not None and outcome.exit_code == 0 and not outcome.oom_killed,
        "sampling_ok": execution.error is None and bool(samples),
        "containers_within_limits": bool(samples) and judged.containers_within_limits,
        "host_reserve_kept": judged.reserve_kept if host_measured else None,
    }
    peaks: dict[str, Any] = {}
    if samples:
        peaks["all"] = peaks_of(samples)
        for phase in (STARTUP_PHASE, OVERLAP_PHASE):
            phase_samples = [sample for sample in samples if sample.phase == phase]
            if phase_samples:
                peaks[phase] = peaks_of(phase_samples)
    return {
        "schema": SCHEMA,
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        "verdict": verdict_of(execution.failure, checks, host_measured),
        "failure": execution.failure,
        "checks": checks,
        "computer": {"system": platform.system(), "machine": platform.machine()},
        "host_figures": {
            "measured": host_measured,
            "note": "read from the Linux host's /proc and statvfs"
            if host_measured
            else "not measured: this computer is not the Linux host; only the Docker figures are real",
        },
        "interval_seconds": interval_seconds,
        "sampling_deadline_seconds": deadline_seconds,
        "reserve_mib": reserve_mib,
        "lowest_available_mib": judged.lowest_available_mib,
        "limits_mib": dict(limits_mib),
        "containers_over_limit": {name: {"peak_mib": peak, "limit_mib": limit} for name, (peak, limit) in judged.containers_over.items()},
        "containers_without_limit": list(judged.containers_unlisted),
        "workload": {
            "service": SPARK_SERVICE,
            "arguments": list(job_arguments),
            "exit_code": None if outcome is None else outcome.exit_code,
            "oom_killed": None if outcome is None else outcome.oom_killed,
            "seconds": None if outcome is None else rounded(outcome.seconds),
            "result": {} if outcome is None else dict(outcome.result),
        },
        "peaks": peaks,
        "stream_lag": STREAM_LAG_NOTE,
        "series": [sample_document(sample) for sample in samples],
    }


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


@dataclass(frozen=True)
class Options:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    start_deadline_seconds: float = DEFAULT_START_DEADLINE_SECONDS
    job_deadline_seconds: float = DEFAULT_JOB_DEADLINE_SECONDS
    wait_seconds: float = DEFAULT_WAIT_SECONDS
    rows: int = DEFAULT_ROWS
    data_path: Path = Path("/var/lib/docker")
    nessie_port: int = DEFAULT_NESSIE_PORT
    host_figures: bool = True
    output_directory: Path | None = None


def utc_text() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(message: str) -> None:
    sys.stdout.write(f"probe: {message}\n")


def read_containers(project: str) -> dict[str, float]:
    """The memory in MiB of each service of the Compose project, from Docker."""
    commands = (
        ["ps", "--filter", f"label=com.docker.compose.project={project}", "--format", '{{.ID}}|{{.Label "com.docker.compose.service"}}'],
        ["stats", "--no-stream", "--format", "{{.ID}}|{{.MemUsage}}"],
    )
    outputs: list[str] = []
    for arguments in commands:
        try:
            result = run_command("docker", arguments, timeout=DOCKER_SECONDS)
        except FileNotFoundError as error:
            raise ProbeError("docker is not installed or not on the path") from error
        except TimeoutExpired as error:
            raise ProbeError(timeout_text(f"docker {arguments[0]}", DOCKER_SECONDS)) from error
        if result.returncode != 0:
            raise ProbeError(f"docker {arguments[0]} exited {result.returncode}")
        outputs.append(result.stdout)
    return parse_container_memory(outputs[0], outputs[1])


class Reader:
    """The sampling function of a run: one host reading and one Docker reading each time, the host figures being those of the interval since the last."""

    def __init__(self, project: str, sources: HostSources | None, sampler: Callable[[], BackgroundSampler]) -> None:
        self.project = project
        self.sources = sources
        self.sampler = sampler
        self.started = time.monotonic()
        self.previous: HostReading | None = read_host(sources) if sources is not None else None

    def take(self) -> Sample:
        figures = None
        if self.sources is not None and self.previous is not None:
            current = read_host(self.sources)
            figures = host_figures(self.previous, current)
            self.previous = current
        containers = read_containers(self.project)
        return Sample(round(time.monotonic() - self.started, 1), utc_text(), self.sampler().phase, figures, containers)


def spark_group(limits: Limits) -> str:
    if SPARK_SERVICE not in limits.services:
        raise ProbeError(f"the limits file holds no {SPARK_SERVICE} service")
    return limits.services[SPARK_SERVICE].group


def container_state(name: str) -> tuple[bool, bool]:
    """Whether the one-off container exists and whether the kernel killed it for memory."""
    try:
        result = run_command("docker", ["inspect", "--format", "{{.State.OOMKilled}}", name], timeout=DOCKER_SECONDS)
    except FileNotFoundError, TimeoutExpired:
        return False, False
    return result.returncode == 0, result.returncode == 0 and result.stdout.strip() == "true"


def remove_container(name: str) -> None:
    try:
        run_command("docker", ["rm", "--force", name], timeout=CLEANUP_SECONDS)
    except FileNotFoundError, TimeoutExpired:
        return


def run_spark_job(settings: Settings, limits: Limits, arguments: Sequence[str], stamp: str, deadline_seconds: float) -> JobOutcome:
    """Run the Spark job once as a one-off container, wait for it under a deadline, read its exit status and out-of-memory flag and remove the container."""
    name = f"{settings.project}_capacity_probe_{stamp.lower()}"
    command = compose_arguments(settings, spark_group(limits), "run", "--no-deps", "--no-TTY", "--name", name, SPARK_SERVICE, *arguments)
    started = time.monotonic()
    try:
        try:
            result = run_command("docker", command, timeout=deadline_seconds, env=rendered_environment(limits))
        except FileNotFoundError as error:
            raise ProbeError("docker is not installed or not on the path") from error
        except TimeoutExpired as error:
            raise ProbeError(timeout_text("the Spark job", deadline_seconds)) from error
        _, killed = container_state(name)
    finally:
        remove_container(name)
    return JobOutcome(result.returncode, killed, time.monotonic() - started, parse_job_result(result.stdout))


def remove_branch(port: int, branch: str) -> str:
    """Delete the job's private Nessie branch and return what happened, in a few words."""
    client = NessieClient(f"http://127.0.0.1:{port}/api/v2", DOCKER_SECONDS)
    try:
        client.delete_reference(branch, client.reference(branch).hash)
    except NessieError as error:
        return f"no branch {branch} existed" if error.status == NOT_FOUND else f"the branch {branch} was not removed: {error}"
    return f"the branch {branch} was removed"


def build_steps(settings: Settings, limits: Limits, options: Options, arguments: Sequence[str], stamp: str) -> Steps:
    def start_supporting() -> None:
        try:
            start_group(settings, ALWAYS_ON_GROUP, wait_seconds=options.wait_seconds, deadline_seconds=options.start_deadline_seconds, log=say)
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error

    def admit_job() -> None:
        try:
            decision = admit(settings, limits, find_group(limits, spark_group(limits)), options.wait_seconds, say)
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error
        say(f"admission of the Spark job: group {decision.group} admitted")

    def run_job() -> JobOutcome:
        return run_spark_job(settings, limits, arguments, stamp, options.job_deadline_seconds)

    return Steps(start_supporting, admit_job, run_job)


def run_probe(settings: Settings, options: Options) -> tuple[dict[str, Any], Path]:
    """Run the probe, write the record and return it with its path. A host that cannot be read raises before anything starts."""
    limits = load_limits(settings.limits_file)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    unique = f"{stamp}{secrets.token_hex(2)}"
    arguments = job_arguments(unique, options.rows)
    sources = HostSources(data_path=options.data_path) if options.host_figures else None
    holder: list[BackgroundSampler] = []
    try:
        reader = Reader(settings.project, sources, lambda: holder[0])
    except FiguresError as error:
        raise ProbeError(f"the host's figures cannot be read ({error}); on a computer that is not the Linux host, pass --docker-figures-only") from error
    deadline = options.wait_seconds * 2 + options.start_deadline_seconds + options.job_deadline_seconds + SAMPLING_SLACK_SECONDS
    sampler = BackgroundSampler(reader.take, interval_seconds=options.interval_seconds, deadline_seconds=deadline, join_seconds=DOCKER_SECONDS * 3)
    holder.append(sampler)
    started_utc = utc_text()
    execution = execute(build_steps(settings, limits, options, arguments, unique), sampler)
    cleanup = remove_branch(options.nessie_port, arguments[1])
    say(cleanup)
    document = build_document(
        execution,
        limits_mib={name: entry.memory_limit_mib for name, entry in limits.services.items() if entry.runtime == "compose"},
        reserve_mib=limits.host_reserve_mib,
        host_measured=options.host_figures,
        interval_seconds=options.interval_seconds,
        deadline_seconds=deadline,
        started_utc=started_utc,
        finished_utc=utc_text(),
        job_arguments=arguments,
    )
    document["cleanup"] = cleanup
    folder = options.output_directory or RUNS_FOLDER / stamp
    return document, write_document(folder, document)


def positive_seconds(text: str) -> float:
    try:
        value = float(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from error
    if not value > 0:
        raise argparse.ArgumentTypeError("the value must be more than 0")
    return value


def positive_whole(text: str) -> int:
    if not text.isascii() or not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number of at least 1")
    return int(text)


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="python -m scripts.stack.capacity_probe", description=__doc__.split("\n\n")[0] if __doc__ else None)
    command.add_argument("--output-directory", type=Path, help="a new folder for the record (default: a new folder under runs/capacity_probe)")
    command.add_argument("--interval-seconds", type=positive_seconds, default=DEFAULT_INTERVAL_SECONDS, help="seconds between two samples")
    command.add_argument(
        "--start-deadline-seconds", type=positive_seconds, default=DEFAULT_START_DEADLINE_SECONDS, help="how long the always-on group has to become healthy"
    )
    command.add_argument("--job-deadline-seconds", type=positive_seconds, default=DEFAULT_JOB_DEADLINE_SECONDS, help="how long the Spark job has to finish")
    command.add_argument("--wait-seconds", type=float, default=DEFAULT_WAIT_SECONDS, help="how long a group that does not fit waits for memory (default 0)")
    command.add_argument("--rows", type=positive_whole, default=DEFAULT_ROWS, help="the Spark job's row count: the fixed workload")
    command.add_argument("--data-path", type=Path, default=Path("/var/lib/docker"), help="a folder on the data disk, for its free space and its disk activity")
    command.add_argument(
        "--nessie-port", type=positive_whole, default=int(os.environ.get("NESSIE_HOST_PORT") or DEFAULT_NESSIE_PORT), help="Nessie's port on 127.0.0.1"
    )
    command.add_argument("--compose-directory", type=Path, help="the compose folder, in place of STACK_COMPOSE_DIRECTORY or the repository's compose folder")
    command.add_argument("--project", help="the Compose project name, in place of COMPOSE_PROJECT_NAME or the name in compose.yml")
    command.add_argument(
        "--available-mib", type=int, help="test input: the host's available memory for the admission check, in place of the reading of /proc/meminfo"
    )
    command.add_argument(
        "--docker-figures-only",
        action="store_true",
        help="this computer is not the Linux host: record the Docker figures and mark every host figure as not measured",
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    options = Options(
        interval_seconds=arguments.interval_seconds,
        start_deadline_seconds=arguments.start_deadline_seconds,
        job_deadline_seconds=arguments.job_deadline_seconds,
        wait_seconds=arguments.wait_seconds,
        rows=arguments.rows,
        data_path=arguments.data_path,
        nessie_port=arguments.nessie_port,
        host_figures=not arguments.docker_figures_only,
        output_directory=arguments.output_directory,
    )
    try:
        settings = settings_from_environment(arguments.compose_directory, arguments.project, arguments.available_mib)
        document, path = run_probe(settings, options)
    except (ProbeError, GroupError, LimitsError) as error:
        sys.stderr.write(f"capacity probe: refused: {error}\n")
        return 1
    say(f"verdict: {document['verdict']}" + (f" ({document['failure']})" if document["failure"] else ""))
    say(f"record: {path}")
    return 0 if document["verdict"] in PASSING_VERDICTS else 1


if __name__ == "__main__":
    sys.exit(main())
