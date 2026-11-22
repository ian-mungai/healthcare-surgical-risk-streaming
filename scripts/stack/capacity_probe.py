"""The capacity probe: the always-on group and one Spark job run together while the host's figures are measured, and the evidence is saved.

``.venv/bin/python -m scripts.stack.capacity_probe [--output-directory <folder>]`` runs on the Linux host. Before it starts anything it checks that the output
folder can take a new record and that no Spark container of the project exists. Then it

1. reads the host's counters once (a host that cannot be read fails here),
2. starts an event reader that follows the project's ``die``, ``oom``, ``kill`` and ``restart`` events in contiguous windows for the whole run,
3. starts a sampler that reads, at a fixed interval, the memory in use and available, the swap in use, the CPU of each core, the disk read and write rates
   and the free space of the data disk, and each container's memory from Docker,
4. starts the always-on group through the group command (``scripts.stack.groups``, with its admission check) in the phase ``startup`` and counts each
   required service's restarts,
5. asks the admission check for the batch group, runs the Spark job once as a detached container with a deadline in the phase ``overlap``, takes a sample
   the moment the container is seen running and reads its cgroup (``memory.peak``, ``memory.max``, ``memory.events``) about once a second until it exits,
   then reads its state, its exit status, its out-of-memory flag and its output,
6. reads each required service's cgroup, restarts and state, stops the sampler and the event reader and confirms that each stopped normally,
7. removes the job container and the job's Nessie branch and verifies that each is gone, also after an error or an interrupt, and
8. judges the evidence (``scripts.stack.probe_record``) and writes ``capacity_probe.json`` into a new folder under ``runs/capacity_probe/``.

The verdict is ``complete``, ``local_only``, ``not_measured`` or ``failed`` (see ``probe_record``). The command exits 0 for the first two only. One monotonic
deadline for the whole run is cut into every wait and every Docker command. The command writes nothing into the limits file: the owner reads the measured
figures and decides about the reserve.

The Spark job is ``spark.jobs.catalog_proof`` with a fixed row count on a private Nessie branch of its own. The group command starts a service with its image's
entry point and no arguments, which this job cannot use, so the probe asks the group's admission check (``groups.admit``, the function ``start_group`` uses)
and then runs the job with the group's Compose profile and limits.

``--docker-figures-only`` is for a computer that is not the Linux host: Docker's figures are real, every host figure is marked as not measured and the verdict
is at best ``local_only``. Stream lag is not measured: the stream path does not exist in this phase.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import secrets
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ROOT
from scripts.stack.admission import AdmissionError
from scripts.stack.groups import GroupError, Settings, admit, compose_arguments, find_group, rendered_environment, settings_from_environment, start_group
from scripts.stack.host_figures import (
    MIN_CPU_SECONDS,
    FiguresError,
    HostReading,
    HostSources,
    host_figures,
    parse_container_usage,
    read_cgroup_memory,
    read_host,
)
from scripts.stack.limits import Limits, LimitsError, load_limits
from scripts.stack.nessie import NessieClient, NessieError
from scripts.stack.probe_docker import DOCKER_SECONDS, NANOSECONDS, ContainerEvent, Docker, ProbeError, Runner, SamplingError
from scripts.stack.probe_record import (
    OVERLAP_PHASE,
    PASSING_VERDICTS,
    SPARK_SERVICE,
    STARTUP_PHASE,
    CleanupStep,
    ContainerEvidence,
    Execution,
    JobOutcome,
    Sample,
    assess,
    build_document,
    check_destination,
    validate_result,
    write_document,
)

RUNS_FOLDER = ROOT / "runs" / "capacity_probe"
ALWAYS_ON_GROUP = "supporting"
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_START_DEADLINE_SECONDS = 300.0
DEFAULT_JOB_DEADLINE_SECONDS = 1800.0
DEFAULT_ROWS = 2_000_000
DEFAULT_NESSIE_PORT = 19120
RUN_SLACK_SECONDS = 120.0
CLEANUP_SECONDS = 60.0
WATCH_SECONDS = 1.0
EVENTS_SLICE_SECONDS = 5.0
COMPOSE_RUN_SECONDS = 120.0
NOT_FOUND = 404
RESULT_PREFIX = "RESULT "
INTERRUPTED_EXIT = 130
START_FAILURE = "starting the always-on group failed"
Pause = Callable[[float], None]


class Budget:
    """One monotonic deadline for the whole run. Every wait and every reading asks it for the time that is left."""

    def __init__(self, total_seconds: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.total_seconds = total_seconds
        self.clock = clock
        self.started = clock()

    def remaining(self) -> float:
        return self.total_seconds - (self.clock() - self.started)

    def limit(self, seconds: float) -> float:
        """The shorter of ``seconds`` and the time that is left. A run with no time left raises ``ProbeError``."""
        left = self.remaining()
        if left <= 0:
            raise ProbeError(f"the run's deadline of {self.total_seconds:g} seconds has passed")
        return min(seconds, left)


def collect(
    take: Callable[[], Sample],
    *,
    samples: list[Sample],
    interval_seconds: float,
    deadline_seconds: float,
    stopped: Callable[[], bool],
    wait: Callable[[float], bool],
    first_delay_seconds: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Append a sample every ``interval_seconds`` until ``stopped`` is true, and one more after the stop.

    ``wait`` returns true when the stop came during it; any other return ends the wait early and the next sample is taken at once. The first sample follows a
    wait of ``first_delay_seconds``. The time a sample takes is taken from the wait, so the interval stays fixed. A reading that fails raises ``SamplingError``
    with the number of the sample. So does a sample that finishes after the deadline, which is not kept. The samples taken stay in ``samples``.
    """
    started = clock()

    def take_one() -> None:
        try:
            sample = take()
        except (ProbeError, FiguresError) as error:
            raise SamplingError(f"sample {len(samples) + 1} failed: {error}") from error
        if clock() - started > deadline_seconds:
            raise SamplingError(f"the sampling deadline of {deadline_seconds:g} seconds passed after {len(samples)} samples")
        samples.append(sample)

    if first_delay_seconds > 0 and wait(first_delay_seconds):
        take_one()
        return
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


class Worker:
    """A thread that runs one function until it returns or fails. Any failure, whatever its type, becomes ``error`` and never ends the thread silently."""

    def __init__(self, name: str, join_seconds: float) -> None:
        self.join_seconds = join_seconds
        self.stop_event = threading.Event()
        self.error = ""
        self.finished = False
        self.thread = threading.Thread(target=self.guarded, name=name, daemon=True)

    def guarded(self) -> None:
        try:
            self.work()
            self.finished = True
        except SamplingError as error:
            self.error = str(error)
        except Exception as error:  # a reader that dies for any reason must show it in the verdict
            self.error = f"the {self.thread.name} stopped with {type(error).__name__}: {error}"

    def work(self) -> None:
        raise NotImplementedError

    @property
    def running(self) -> bool:
        return self.thread.is_alive()

    @property
    def confirmed(self) -> bool:
        """The stop was asked for, the thread ended by its own return, and it reported no error."""
        return self.stop_event.is_set() and not self.running and self.finished and not self.error

    def start(self) -> None:
        self.thread.start()

    def request_stop(self) -> None:
        self.stop_event.set()

    def stop(self) -> None:
        """Ask the thread to stop and wait for it. A thread that is still running after ``join_seconds`` is a failure."""
        self.request_stop()
        if self.thread.is_alive():
            self.thread.join(self.join_seconds)
        if self.thread.is_alive():
            raise SamplingError(f"the {self.thread.name} did not stop within {self.join_seconds:g} seconds")


class BackgroundSampler(Worker):
    """``collect`` on a thread of its own. ``phase`` is read by the sampling function when it labels a sample. ``sample_now`` asks for a sample at once."""

    def __init__(
        self, take: Callable[[], Sample], *, interval_seconds: float, deadline_seconds: float, join_seconds: float, first_delay_seconds: float = 0.0
    ) -> None:
        super().__init__("sampler", join_seconds)
        self.take = take
        self.interval_seconds = interval_seconds
        self.deadline_seconds = deadline_seconds
        self.first_delay_seconds = first_delay_seconds
        self.phase = STARTUP_PHASE
        self.samples: list[Sample] = []
        self.wake = threading.Event()

    def wait(self, seconds: float) -> bool:
        self.wake.wait(seconds)
        self.wake.clear()
        return self.stop_event.is_set()

    def work(self) -> None:
        collect(
            self.take,
            samples=self.samples,
            interval_seconds=self.interval_seconds,
            deadline_seconds=self.deadline_seconds,
            stopped=self.stop_event.is_set,
            wait=self.wait,
            first_delay_seconds=self.first_delay_seconds,
        )

    def sample_now(self) -> None:
        self.wake.set()

    def request_stop(self) -> None:
        self.stop_event.set()
        self.wake.set()


class EventsReader(Worker):
    """The project's lifecycle events for the whole run, read in contiguous windows so that no window is lost to Docker's short event memory.

    Each window is one ``docker events --since <start> --until <end>`` command, which ends at its end. After the stop, one last window ends at the present.
    """

    def __init__(
        self, docker: Docker, project: str, *, join_seconds: float, slice_seconds: float = EVENTS_SLICE_SECONDS, clock_ns: Callable[[], int] = time.time_ns
    ) -> None:
        super().__init__("event reader", join_seconds)
        self.docker = docker
        self.project = project
        self.slice_seconds = slice_seconds
        self.clock_ns = clock_ns
        self.since = 0
        self.events: list[ContainerEvent] = []
        self.windows = 0
        self.seen: set[tuple[int, str, str]] = set()

    def start(self) -> None:
        self.since = self.clock_ns()
        super().start()

    def work(self) -> None:
        since = self.since
        while True:
            stopping = self.stop_event.is_set()
            until = self.clock_ns() if stopping else since + round(self.slice_seconds * NANOSECONDS)
            if until > since:
                for event in self.docker.events(self.project, since, until, seconds=self.slice_seconds + DOCKER_SECONDS):
                    key = (event.nano, event.action, event.name)
                    if key not in self.seen:
                        self.seen.add(key)
                        self.events.append(event)
                self.windows += 1
                since = until
            if stopping:
                return


class Reader:
    """The sampling function of a run: one host reading and one Docker reading each time, the host figures being those of the interval since the last."""

    def __init__(self, project: str, sources: HostSources | None, docker: Docker, phase: Callable[[], str]) -> None:
        self.project = project
        self.sources = sources
        self.docker = docker
        self.phase = phase
        self.started = time.monotonic()
        self.previous: HostReading | None = read_host(sources) if sources is not None else None

    def take(self) -> Sample:
        figures = None
        if self.sources is not None and self.previous is not None:
            current = read_host(self.sources)
            figures = host_figures(self.previous, current)
            self.previous = current
        listing, statistics = self.docker.usage_texts(self.project)
        return Sample(round(time.monotonic() - self.started, 1), utc_text(), self.phase(), figures, parse_container_usage(listing, statistics))


@dataclass(frozen=True)
class Steps:
    """The steps of a run. Each raises ``ProbeError`` when it cannot be done."""

    start_supporting: Callable[[], None]
    baseline: Callable[[], Mapping[str, int]]
    admit_job: Callable[[], None]
    run_job: Callable[[Callable[[], None]], JobOutcome]
    final_evidence: Callable[[], Mapping[str, ContainerEvidence]]
    cleanup: Callable[[], list[CleanupStep]]


def stopped_normally(worker: Worker) -> str:
    """Stop a worker and return the failure text, which is empty when it stopped."""
    try:
        worker.stop()
    except SamplingError as error:
        return str(error)
    return ""


def run_steps(steps: Steps, sampler: BackgroundSampler) -> tuple[JobOutcome | None, str, Mapping[str, int], Mapping[str, ContainerEvidence], str, bool]:
    """The outcome, the failure, the restart baseline, the end evidence, the evidence error and whether the run was interrupted."""
    outcome: JobOutcome | None = None
    failure, evidence_error = "", ""
    baseline: Mapping[str, int] = {}
    evidence: Mapping[str, ContainerEvidence] = {}
    interrupted = False
    try:
        try:
            steps.start_supporting()
            baseline = steps.baseline()
        except ProbeError as error:
            failure = f"{START_FAILURE}: {error}"
        if not failure:
            sampler.phase = OVERLAP_PHASE
            try:
                steps.admit_job()
            except ProbeError as error:
                failure = f"admission of the Spark job refused: {error}"
        if not failure:
            try:
                outcome = steps.run_job(sampler.sample_now)
            except ProbeError as error:
                failure = f"the Spark job could not run: {error}"
        if not failure.startswith(START_FAILURE):
            try:
                evidence = steps.final_evidence()
            except (ProbeError, FiguresError) as error:
                evidence_error = str(error)
    except KeyboardInterrupt:
        interrupted = True
    return outcome, failure, baseline, evidence, evidence_error, interrupted


def execute(steps: Steps, sampler: BackgroundSampler, events: EventsReader) -> Execution:
    """Run the steps in order under the sampler and the event reader. Both are stopped and the cleanup runs after every outcome, an interrupt included."""
    outcome: JobOutcome | None = None
    failure, evidence_error = "", ""
    baseline: Mapping[str, int] = {}
    evidence: Mapping[str, ContainerEvidence] = {}
    interrupted = False
    cleanup: list[CleanupStep] = []
    sampler.phase = STARTUP_PHASE
    events.start()
    sampler.start()
    try:
        outcome, failure, baseline, evidence, evidence_error, interrupted = run_steps(steps, sampler)
    finally:
        sampler_error = stopped_normally(sampler) or sampler.error
        events_error = stopped_normally(events) or events.error
        try:
            cleanup = steps.cleanup()
        except Exception as error:  # a cleanup that fails in an unforeseen way is a failed cleanup and never a silent one
            cleanup = [CleanupStep("cleanup", False, f"stopped with {type(error).__name__}")]
    return Execution(
        outcome,
        failure,
        list(sampler.samples),
        sampler_error,
        sampler.confirmed,
        list(events.events),
        events_error,
        events.confirmed,
        baseline,
        evidence,
        cleanup,
        interrupted,
        evidence_error,
    )


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


def job_arguments(unique: str, rows: int) -> list[str]:
    return ["--branch", f"capacity_probe_{unique.lower()}", "--rows", str(rows)]


def utc_text() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(message: str) -> None:
    sys.stdout.write(f"probe: {message}\n")


def nessie_detail(error: NessieError) -> str:
    """What a failed Nessie call says: its status or that it did not answer, and never the answer's text."""
    return f"Nessie answered with status {error.status}" if error.status is not None else "Nessie did not answer"


def branch_exists(client: Any, branch: str) -> bool:
    try:
        client.reference(branch)
    except NessieError as error:
        if error.status == NOT_FOUND:
            return False
        raise
    return True


def remove_branch(client: Any, branch: str) -> CleanupStep:
    """Delete the job's private Nessie branch and verify that it is gone. A branch that never existed is gone. A call that fails leaves the step unverified."""
    name = "nessie branch"
    try:
        if not branch_exists(client, branch):
            return CleanupStep(name, True, "no branch existed")
        client.delete_reference(branch, client.reference(branch).hash)
        if branch_exists(client, branch):
            return CleanupStep(name, False, "the branch is still present after its deletion")
    except NessieError as error:
        return CleanupStep(name, False, nessie_detail(error))
    return CleanupStep(name, True, "removed and absent")


def remove_container(docker: Docker, container: str) -> CleanupStep:
    """Remove the job container and verify that no container of that name is left."""
    name = "job container"
    try:
        docker.remove(container)
        present = docker.exists(container)
    except ProbeError as error:
        return CleanupStep(name, False, str(error))
    return CleanupStep(name, not present, "no container remains" if not present else "still present after its removal")


def required_services(limits: Limits) -> tuple[str, ...]:
    return tuple(sorted(name for name, entry in limits.services.items() if entry.group == ALWAYS_ON_GROUP and entry.runtime == "compose"))


def spark_group(limits: Limits) -> str:
    if SPARK_SERVICE not in limits.services:
        raise ProbeError(f"the limits file holds no {SPARK_SERVICE} service")
    return limits.services[SPARK_SERVICE].group


@dataclass(frozen=True)
class Options:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    start_deadline_seconds: float = DEFAULT_START_DEADLINE_SECONDS
    job_deadline_seconds: float = DEFAULT_JOB_DEADLINE_SECONDS
    wait_seconds: float = 0.0
    rows: int = DEFAULT_ROWS
    data_path: Path = Path("/var/lib/docker")
    nessie_port: int = DEFAULT_NESSIE_PORT
    host_figures: bool = True
    output_directory: Path | None = None


class Probe:
    """The steps of one run on one stack."""

    def __init__(
        self,
        settings: Settings,
        limits: Limits,
        options: Options,
        *,
        docker: Docker,
        budget: Budget,
        arguments: Sequence[str],
        container: str,
        runner: Runner = run_command,
        pause: Pause = time.sleep,
    ) -> None:
        self.settings, self.limits, self.options = settings, limits, options
        self.docker, self.budget, self.arguments, self.container = docker, budget, list(arguments), container
        self.runner, self.pause = runner, pause
        self.required = required_services(limits)
        self.job_started = False

    def start_supporting(self) -> None:
        try:
            wait = min(self.options.wait_seconds, self.budget.remaining())
            start_group(
                self.settings,
                ALWAYS_ON_GROUP,
                wait_seconds=max(0.0, wait),
                deadline_seconds=self.budget.limit(self.options.start_deadline_seconds),
                log=say,
            )
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error

    def service_container(self, service: str) -> str:
        found = self.docker.list_containers(self.settings.project, service=service)
        if not found:
            raise ProbeError(f"service {service} is not running")
        return found[0].name

    def baseline(self) -> dict[str, int]:
        """Each required service's restart count, which the end of the run is compared with."""
        return {service: self.docker.state(self.service_container(service)).restarts for service in self.required}

    def admit_job(self) -> None:
        try:
            wait = max(0.0, min(self.options.wait_seconds, self.budget.remaining()))
            decision = admit(self.settings, self.limits, find_group(self.limits, spark_group(self.limits)), wait, say)
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error
        say(f"admission of the Spark job: group {decision.group} admitted")

    def final_evidence(self) -> dict[str, ContainerEvidence]:
        """Each required service's cap, peak, kills and restarts at the end of the run, from its cgroup on the host and from Docker on another computer."""
        found: dict[str, ContainerEvidence] = {}
        for service in self.required:
            containers = self.docker.list_containers(self.settings.project, service=service)
            if not containers:
                found[service] = ContainerEvidence(service, False, None, None, None, False, 0, "none")
                continue
            state = self.docker.state(containers[0].name)
            if self.options.host_figures:
                memory = read_cgroup_memory(state.pid)
                found[service] = ContainerEvidence(
                    service, state.status == "running", memory.max_bytes, memory.peak_bytes, memory.oom_kills, state.oom_killed, state.restarts, "cgroup"
                )
            else:
                found[service] = ContainerEvidence(
                    service, state.status == "running", state.memory_cap_bytes or None, None, None, state.oom_killed, state.restarts, "docker"
                )
        return found

    def start_job(self) -> None:
        self.job_started = True
        command = compose_arguments(
            self.settings, spark_group(self.limits), "run", "--detach", "--no-deps", "--no-TTY", "--name", self.container, SPARK_SERVICE, *self.arguments
        )
        timeout = self.budget.limit(COMPOSE_RUN_SECONDS)
        try:
            result = self.runner("docker", command, timeout=timeout, env=rendered_environment(self.limits))
        except FileNotFoundError as error:
            raise ProbeError("docker is not installed or not on the path") from error
        except TimeoutExpired as error:
            raise ProbeError(timeout_text("starting the Spark job", timeout)) from error
        if result.returncode != 0:
            raise ProbeError(f"starting the Spark job: docker exited {result.returncode}")

    def run_job(self, on_running: Callable[[], None]) -> JobOutcome:
        """Start the job as a detached container, watch it until it exits and read its state and output. The container is removed by the cleanup, after this."""
        self.start_job()
        began = time.monotonic()
        peak: int | None = None
        cap: int | None = None
        kills: int | None = None
        seen = False
        while True:
            state = self.docker.state(self.container)
            if state.status == "running":
                if not seen:
                    seen = True
                    on_running()
                if self.options.host_figures:
                    try:
                        memory = read_cgroup_memory(state.pid)
                    except FiguresError:
                        memory = None
                    if memory is not None:
                        peak, cap, kills = max(peak or 0, memory.peak_bytes), memory.max_bytes, memory.oom_kills
            elif state.status in ("exited", "dead"):
                break
            if time.monotonic() - began >= self.options.job_deadline_seconds:
                raise ProbeError(timeout_text("the Spark job", self.options.job_deadline_seconds))
            self.pause(self.budget.limit(WATCH_SECONDS))
        seconds = time.monotonic() - began
        result, error = validate_result(
            parse_job_result(self.docker.logs(self.container)),
            branch=self.arguments[1],
            rows=int(self.arguments[3]),
            memory_limit_mib=self.limits.services[SPARK_SERVICE].memory_limit_mib,
        )
        host = self.options.host_figures
        evidence = ContainerEvidence(
            SPARK_SERVICE,
            False,
            cap if host else (state.memory_cap_bytes or None),
            peak,
            kills,
            state.oom_killed,
            state.restarts,
            "cgroup" if host else "docker",
        )
        return JobOutcome(state.exit_code, state.oom_killed, seconds, result, error, evidence)

    def cleanup(self, docker: Docker, client: Any) -> list[CleanupStep]:
        branch = remove_branch(client, self.arguments[1]) if self.job_started else CleanupStep("nessie branch", True, "the job did not start")
        return [remove_container(docker, self.container), branch]


def positive_finite(text: str) -> float:
    try:
        value = float(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from error
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"{text!r} must be a finite number above 0")
    return value


def positive_whole(text: str) -> int:
    if not text.isascii() or not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number of at least 1")
    return int(text)


def preflight(docker: Docker, project: str, folder: Path) -> None:
    """Refuse before anything starts: a folder that cannot take a record, and any Spark container of the project, running or stopped."""
    check_destination(folder)
    existing = docker.list_containers(project, service=SPARK_SERVICE, include_stopped=True)
    if existing:
        raise ProbeError(f"a Spark container of this project exists ({', '.join(item.name for item in existing)}): wait for its job to end or remove it")


def run_probe(settings: Settings, options: Options, *, runner: Runner = run_command, client: Any = None) -> tuple[dict[str, Any], Path]:
    """Run the probe, write the record and return it with its path. An interrupt writes the record and is raised again after it."""
    limits = load_limits(settings.limits_file)
    spark_group(limits)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    unique = f"{stamp}{secrets.token_hex(2)}"
    folder = options.output_directory or RUNS_FOLDER / stamp
    budget = Budget(options.wait_seconds * 2 + options.start_deadline_seconds + options.job_deadline_seconds + RUN_SLACK_SECONDS)
    docker, plain = Docker(runner=runner, limit=budget.limit), Docker(runner=runner, limit=lambda seconds: min(seconds, CLEANUP_SECONDS))
    sources = HostSources(data_path=options.data_path) if options.host_figures else None
    holder: list[BackgroundSampler] = []
    try:
        reader = Reader(settings.project, sources, docker, lambda: holder[0].phase)
    except FiguresError as error:
        raise ProbeError(f"the host's figures cannot be read ({error}); on a computer that is not the Linux host, pass --docker-figures-only") from error
    preflight(plain, settings.project, folder)
    arguments = job_arguments(unique, options.rows)
    join = DOCKER_SECONDS * 3
    sampler = BackgroundSampler(
        reader.take, interval_seconds=options.interval_seconds, deadline_seconds=budget.remaining(), join_seconds=join, first_delay_seconds=MIN_CPU_SECONDS
    )
    holder.append(sampler)
    events = EventsReader(docker, settings.project, join_seconds=join + EVENTS_SLICE_SECONDS)
    probe = Probe(
        settings,
        limits,
        options,
        docker=docker,
        budget=budget,
        arguments=arguments,
        container=f"{settings.project}_capacity_probe_{unique.lower()}",
        runner=runner,
    )
    nessie = client or NessieClient(f"http://127.0.0.1:{options.nessie_port}/api/v2", DOCKER_SECONDS)
    steps = Steps(probe.start_supporting, probe.baseline, probe.admit_job, probe.run_job, probe.final_evidence, lambda: probe.cleanup(plain, nessie))
    started_utc = utc_text()
    execution = execute(steps, sampler, events)
    for step in execution.cleanup:
        say(f"cleanup {step.name}: {'ok' if step.ok else 'FAILED'}: {step.detail}")
    limits_mib = {name: entry.memory_limit_mib for name, entry in limits.services.items() if entry.runtime == "compose"}
    assessment = assess(execution, limits_mib=limits_mib, required=probe.required, reserve_mib=limits.host_reserve_mib, host_measured=options.host_figures)
    document = build_document(
        execution,
        assessment,
        limits_mib=limits_mib,
        required=probe.required,
        reserve_mib=limits.host_reserve_mib,
        host_measured=options.host_figures,
        interval_seconds=options.interval_seconds,
        deadline_seconds=budget.total_seconds,
        started_utc=started_utc,
        finished_utc=utc_text(),
        job_arguments=arguments,
    )
    path = write_document(folder, document)
    if execution.interrupted:
        raise KeyboardInterrupt
    return document, path


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="python -m scripts.stack.capacity_probe", description=__doc__.split("\n\n")[0] if __doc__ else None)
    command.add_argument("--output-directory", type=Path, help="a new folder for the record (default: a new folder under runs/capacity_probe)")
    command.add_argument("--interval-seconds", type=positive_finite, default=DEFAULT_INTERVAL_SECONDS, help="seconds between two samples")
    command.add_argument(
        "--start-deadline-seconds", type=positive_finite, default=DEFAULT_START_DEADLINE_SECONDS, help="how long the always-on group has to become healthy"
    )
    command.add_argument("--job-deadline-seconds", type=positive_finite, default=DEFAULT_JOB_DEADLINE_SECONDS, help="how long the Spark job has to finish")
    command.add_argument(
        "--wait-seconds", type=positive_finite, default=None, help="how long a group that does not fit waits for memory (default: it does not wait)"
    )
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
        "--docker-figures-only", action="store_true", help="this computer is not the Linux host: Docker's figures only, and the verdict is at best local_only"
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    options = Options(
        interval_seconds=arguments.interval_seconds,
        start_deadline_seconds=arguments.start_deadline_seconds,
        job_deadline_seconds=arguments.job_deadline_seconds,
        wait_seconds=arguments.wait_seconds or 0.0,
        rows=arguments.rows,
        data_path=arguments.data_path,
        nessie_port=arguments.nessie_port,
        host_figures=not arguments.docker_figures_only,
        output_directory=arguments.output_directory,
    )
    try:
        settings = settings_from_environment(arguments.compose_directory, arguments.project, arguments.available_mib)
        document, path = run_probe(settings, options)
    except KeyboardInterrupt:
        sys.stderr.write("capacity probe: interrupted: the cleanup ran and the record names each step\n")
        return INTERRUPTED_EXIT
    except (ProbeError, GroupError, LimitsError) as error:
        sys.stderr.write(f"capacity probe: refused: {error}\n")
        return 1
    findings = document["failures"] + document["gaps"]
    say(f"verdict: {document['verdict']}" + (f" ({'; '.join(findings)})" if findings else ""))
    say(f"record: {path}")
    return 0 if document["verdict"] in PASSING_VERDICTS else 1


if __name__ == "__main__":
    sys.exit(main())
