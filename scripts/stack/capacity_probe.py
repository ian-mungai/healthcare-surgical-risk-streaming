"""The capacity probe: the always-on group and one Spark job run together while the host's figures are measured, and the evidence is saved.

``.venv/bin/python -m scripts.stack.capacity_probe [--output-directory <folder>]`` runs on the Linux host. It is one process with one thread and one absolute
monotonic deadline. Before it starts anything it checks that the output folder can take a new record, that no Spark container of the project exists and that
the job's catalog address and the address of its cleanup are one Nessie (resolved from the Compose files and ``host.env``, recorded in the record). Then it

1. starts the always-on group through the group command (``scripts.stack.groups``, with its admission check). Each time the group command reports progress,
   the probe takes a sample and reads the events that came since its cursor,
2. counts each required service's restarts and kills for memory, and checks that the catalog answers,
3. asks the admission check for the batch group and starts the Spark job as a detached container with a scratch volume of its own,
4. loops: read the Spark container's state, read its cgroup, take one sample (host figures and every container's memory), read the events with
   ``docker events --since <cursor> --until <now>`` (the end is in the past, so the command returns at once, and the cursor moves only after a command that
   succeeded), then wait out the rest of the interval. The loop ends after the Spark container has exited and one more iteration has run,
5. reads each required service's cgroup, restarts and state,
6. removes the job container, then its scratch volume, then its Nessie branch, and verifies each is gone, and
7. judges the evidence (``scripts.stack.probe_record``) and writes ``capacity_probe.json`` into a new folder under ``runs/capacity_probe/``.

A SIGINT or SIGTERM only sets a flag. The loop leaves at its next check, every step after it runs in full and the command exits 130 once the record is written.
A second signal during the end is ignored. No wait or reading is accepted after the deadline.

The verdict is ``complete``, ``local_only``, ``not_measured`` or ``failed`` (see ``probe_record``). The command exits 0 for the first two only. The command
writes nothing into the limits file: the owner reads the measured figures and decides about the reserve.

The Spark job is ``spark.jobs.catalog_proof`` with a fixed row count on a private Nessie branch of its own. The group command starts a service with its image's
entry point and no arguments, which this job cannot use, so the probe asks the group's admission check (``groups.admit``, the function ``start_group`` uses)
and then runs the job with the group's Compose profile and limits. Its ``/scratch`` is a named volume of this run, which replaces the service's shared one for
this container through ``compose run --volume`` and is removed in the cleanup.

``--docker-figures-only`` is for a computer that is not the Linux host: Docker's figures are real, every host figure is marked as not measured and the verdict
is at best ``local_only``. Stream lag is not measured: the stream path does not exist in this phase.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import secrets
import signal
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ROOT
from scripts.stack.admission import AdmissionError
from scripts.stack.groups import GroupError, Settings, admit, compose_arguments, find_group, rendered_environment, settings_from_environment, start_group
from scripts.stack.host_figures import (
    CgroupMemory,
    FiguresError,
    HostFigures,
    HostReading,
    HostSources,
    host_figures,
    parse_container_usage,
    read_cgroup_memory,
    read_host,
)
from scripts.stack.limits import Limits, LimitsError, load_limits
from scripts.stack.nessie import NessieClient, NessieError
from scripts.stack.probe_docker import NANOSECONDS, ContainerEvent, ContainerState, Docker, ProbeError, Runner
from scripts.stack.probe_record import (
    PASSING_VERDICTS,
    SPARK_SERVICE,
    CleanupStep,
    ContainerEvidence,
    Execution,
    JobOutcome,
    Sample,
    ServiceBaseline,
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
RUN_SLACK_SECONDS = 120.0
CLEANUP_SECONDS = 60.0
EVENTS_SECONDS = 15.0
COMPOSE_RUN_SECONDS = 120.0
SLEEP_SLICE_SECONDS = 0.2
NOT_FOUND = 404
RESULT_PREFIX = "RESULT "
INTERRUPTED_EXIT = 130
START_FAILURE = "starting the always-on group failed"
SCRATCH_MOUNT = "/scratch"
SPARK_URI_DEFAULT = re.compile(r"NESSIE_URI: \$\{NESSIE_URI:-([^}]+)\}")
NESSIE_PORT_DEFAULT = re.compile(r"127\.0\.0\.1:\$\{NESSIE_HOST_PORT:-(\d+)\}:(\d+)")
Pause = Callable[[float], None]


class Stopped(ProbeError):
    """A SIGINT or SIGTERM was seen where the run cannot go on."""


class StopFlag:
    """The run's stop flag. A signal handler only counts the signal and never raises, so a signal cannot end a step half way."""

    def __init__(self) -> None:
        self.count = 0
        self.previous: dict[int, Any] = {}

    @property
    def requested(self) -> bool:
        return self.count > 0

    def handle(self, number: int, frame: object) -> None:
        del number, frame
        self.count += 1

    def install(self) -> None:
        for number in (signal.SIGINT, signal.SIGTERM):
            self.previous[number] = signal.signal(number, self.handle)

    def restore(self) -> None:
        for number, handler in self.previous.items():
            signal.signal(number, handler)
        self.previous.clear()

    def sleep(self, seconds: float, *, pause: Pause = time.sleep) -> None:
        """Wait ``seconds`` in short slices, and stop waiting when a signal came."""
        left = seconds
        while left > 0 and not self.requested:
            step = min(SLEEP_SLICE_SECONDS, left)
            pause(step)
            left -= step


class Budget:
    """One monotonic deadline for the whole run. Every wait and every reading asks it for the time that is left, and no result is accepted after it."""

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

    def accept(self, label: str) -> None:
        """Refuse a result that arrived after the deadline."""
        if self.remaining() <= 0:
            raise ProbeError(f"{label} finished after the deadline of {self.total_seconds:g} seconds")


class Reader:
    """One host reading and one Docker reading, the host figures being those of the interval since the last reading."""

    def __init__(self, project: str, sources: HostSources | None, docker: Docker, *, now: Callable[[], float] = time.monotonic) -> None:
        self.project = project
        self.sources = sources
        self.docker = docker
        self.now = now
        self.started = now()
        self.previous: HostReading | None = read_host(sources) if sources is not None else None

    def take(self, *, spark_running: bool) -> Sample:
        figures: HostFigures | None = None
        if self.sources is not None and self.previous is not None:
            current = read_host(self.sources)
            figures = host_figures(self.previous, current)
            self.previous = current
        listing, statistics = self.docker.usage_texts(self.project)
        return Sample(round(self.now() - self.started, 1), utc_text(), figures, parse_container_usage(listing, statistics), spark_running)


@dataclass
class SparkWatch:
    """What the loop saw of the Spark container: the highest cgroup reading, its cap and kill count, and when it was seen to have exited."""

    peak_bytes: int | None = None
    cap_bytes: int | None = None
    oom_kills: int | None = None
    seen_running: bool = False
    exit_state: ContainerState | None = None
    exit_ns: int | None = None


@dataclass
class Monitor:
    """The sampling loop. One iteration reads the Spark state, takes one sample and reads the events since the cursor, and never runs on another thread."""

    docker: Docker
    project: str
    reader: Reader
    budget: Budget
    stop: StopFlag
    interval_seconds: float
    read_cgroup: Callable[[int], CgroupMemory] | None = None
    clock_ns: Callable[[], int] = time.time_ns
    monotonic: Callable[[], float] = time.monotonic
    pause: Pause = time.sleep
    samples: list[Sample] = field(default_factory=list)
    events: list[ContainerEvent] = field(default_factory=list)
    windows: list[tuple[int, int]] = field(default_factory=list)
    spark: SparkWatch = field(default_factory=SparkWatch)
    run_start_ns: int = 0
    cursor_ns: int = 0
    last_tick: float = float("-inf")
    seen: set[tuple[int, str, str]] = field(default_factory=set)

    def begin(self) -> None:
        """Start the event cursor, before anything of the run starts."""
        self.run_start_ns = self.cursor_ns = self.clock_ns()

    def advance_events(self) -> None:
        """Read the events from the cursor to now. The cursor moves only to the end of a command that succeeded."""
        until = self.clock_ns()
        if until <= self.cursor_ns:
            return
        found = self.docker.events(self.project, self.cursor_ns, until, seconds=EVENTS_SECONDS)
        for event in found:
            key = (event.nano, event.action, event.name)
            if key not in self.seen:
                self.seen.add(key)
                self.events.append(event)
        self.windows.append((self.cursor_ns, until))
        self.cursor_ns = until

    def watch(self, state: ContainerState) -> None:
        if state.status != "running":
            return
        self.spark.seen_running = True
        if self.read_cgroup is None or state.pid <= 0:
            return
        try:
            memory = self.read_cgroup(state.pid)
        except FiguresError:
            return
        self.spark.peak_bytes = max(self.spark.peak_bytes or 0, memory.peak_bytes)
        self.spark.cap_bytes, self.spark.oom_kills = memory.max_bytes, memory.oom_kills

    def tick(self, container: str | None = None) -> ContainerState | None:
        """One iteration: the Spark state (when a container is named), one sample, the events since the cursor. No step is accepted after the deadline."""
        state = self.docker.state(container) if container else None
        if state is not None:
            self.watch(state)
        self.samples.append(self.reader.take(spark_running=state is not None and state.status == "running"))
        self.advance_events()
        self.last_tick = self.monotonic()
        self.budget.accept("a sample")
        return state

    def hook(self) -> None:
        """Called by the group command each time it reports progress: take an iteration when one is due, and leave at once when a signal came."""
        if self.stop.requested:
            raise Stopped("a signal came while the always-on group started")
        if self.monotonic() - self.last_tick >= self.interval_seconds:
            self.tick()

    def watch_job(self, container: str, job_seconds: float) -> ContainerState:
        """Iterate until the Spark container has exited and one more iteration has run, and return its state at the exit."""
        job_deadline = self.monotonic() + job_seconds
        while True:
            if self.stop.requested:
                raise Stopped("a signal came while the job ran")
            self.budget.limit(self.interval_seconds)
            if self.monotonic() >= job_deadline:
                raise ProbeError(timeout_text("the Spark job", job_seconds))
            began = self.monotonic()
            state = self.tick(container)
            if state is None:
                raise ProbeError("the Spark container's state was not read")
            if self.spark.exit_state is not None:
                return self.spark.exit_state
            if state.status in ("exited", "dead"):
                self.spark.exit_state, self.spark.exit_ns = state, self.clock_ns()
                self.budget.accept("the Spark job's exit")
                continue
            self.stop.sleep(max(0.0, min(self.interval_seconds - (self.monotonic() - began), self.budget.remaining())), pause=self.pause)


@dataclass(frozen=True)
class Steps:
    """The steps of a run. Each raises ``ProbeError`` when it cannot be done."""

    start_supporting: Callable[[Callable[[], None]], None]
    baseline: Callable[[], Mapping[str, ServiceBaseline]]
    check_catalog: Callable[[], None]
    admit_job: Callable[[], None]
    start_job: Callable[[], None]
    container: str
    job_seconds: float
    finish_job: Callable[[ContainerState, SparkWatch], JobOutcome]
    final_evidence: Callable[[], Mapping[str, ContainerEvidence]]
    cleanup: Callable[[], list[CleanupStep]]
    addresses: Mapping[str, str] = field(default_factory=dict)
    scratch: str = ""


def run_to_end(
    steps: Steps, monitor: Monitor
) -> tuple[JobOutcome | None, str, Mapping[str, ServiceBaseline], Mapping[str, ContainerEvidence], str, bool, bool]:
    """The outcome, the failure, the baseline, the end evidence, the evidence error, whether the loop ran to its end and whether a signal stopped the run."""
    outcome: JobOutcome | None = None
    failure, evidence_error = "", ""
    baseline: Mapping[str, ServiceBaseline] = {}
    evidence: Mapping[str, ContainerEvidence] = {}
    complete = interrupted = False
    try:
        try:
            steps.start_supporting(monitor.hook)
            monitor.budget.accept("the start of the always-on group")
            baseline = steps.baseline()
        except Stopped:
            raise
        except ProbeError as error:
            failure = f"{START_FAILURE}: {error}"
        before_job = (
            ("the catalog check failed", steps.check_catalog),
            ("admission of the Spark job refused", steps.admit_job),
            ("the Spark job could not start", steps.start_job),
        )
        for label, step in before_job:
            if failure:
                break
            try:
                step()
            except Stopped:
                raise
            except ProbeError as error:
                failure = f"{label}: {error}"
        if not failure:
            try:
                outcome = steps.finish_job(monitor.watch_job(steps.container, steps.job_seconds), monitor.spark)
                complete = True
            except Stopped:
                raise
            except ProbeError as error:
                failure = f"the Spark job could not run: {error}"
        if not failure.startswith(START_FAILURE):
            try:
                evidence = steps.final_evidence()
            except (ProbeError, FiguresError) as error:
                evidence_error = str(error)
    except Stopped:
        interrupted = True
    return outcome, failure, baseline, evidence, evidence_error, complete, interrupted


def execute(steps: Steps, monitor: Monitor) -> Execution:
    """Run the steps in order under the loop. The cleanup runs after every outcome, a signal included, and a signal that comes during it changes nothing."""
    monitor.begin()
    cleanup: list[CleanupStep] = []
    try:
        outcome, failure, baseline, evidence, evidence_error, complete, interrupted = run_to_end(steps, monitor)
    finally:
        try:
            cleanup = steps.cleanup()
        except Exception as error:  # a cleanup that fails in an unforeseen way is a failed cleanup and never a silent one
            cleanup = [CleanupStep("cleanup", False, f"stopped with {type(error).__name__}")]
    return Execution(
        outcome,
        failure,
        list(monitor.samples),
        complete,
        list(monitor.events),
        list(monitor.windows),
        monitor.run_start_ns,
        monitor.spark.exit_ns,
        baseline,
        evidence,
        cleanup,
        interrupted,
        evidence_error,
        dict(steps.addresses),
        steps.scratch,
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
    """Stop and remove the job container and verify that no container of that name is left."""
    name = "job container"
    try:
        docker.remove(container)
        present = docker.exists(container)
    except ProbeError as error:
        return CleanupStep(name, False, str(error))
    return CleanupStep(name, not present, "no container remains" if not present else "still present after its removal")


def remove_scratch(docker: Docker, volume: str) -> CleanupStep:
    """Remove the run's scratch volume and verify that it is gone."""
    name = "scratch volume"
    try:
        docker.remove_volume(volume)
        present = docker.volume_exists(volume)
    except ProbeError as error:
        return CleanupStep(name, False, str(error))
    return CleanupStep(name, not present, "no volume remains" if not present else "still present after its removal")


def guarded(name: str, step: Callable[[], CleanupStep]) -> CleanupStep:
    """A cleanup step whose failure of any kind, a malformed answer included, is that step's failure and never ends the others."""
    try:
        return step()
    except Exception as error:  # the next step must still run
        return CleanupStep(name, False, f"stopped with {type(error).__name__}")


def required_services(limits: Limits) -> tuple[str, ...]:
    return tuple(sorted(name for name, entry in limits.services.items() if entry.group == ALWAYS_ON_GROUP and entry.runtime == "compose"))


def spark_group(limits: Limits) -> str:
    if SPARK_SERVICE not in limits.services:
        raise ProbeError(f"the limits file holds no {SPARK_SERVICE} service")
    return limits.services[SPARK_SERVICE].group


def env_file_values(path: Path | None) -> dict[str, str]:
    """The ``KEY=VALUE`` lines of a Compose environment file."""
    if path is None or not path.is_file():
        return {}
    found = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() and not key.lstrip().startswith("#"):
            found[key.strip()] = value.strip()
    return found


@dataclass(frozen=True)
class NessieTarget:
    job_uri: str
    cleanup_uri: str


def resolve_nessie(settings: Settings, environ: Mapping[str, str]) -> NessieTarget:
    """The address the job uses for its catalog and the address that the cleanup and the catalog check use on this computer, which must be one Nessie.

    Both come from the same Compose settings the job runs with: the process environment, then ``host.env``, then the defaults of the service files. The job's
    address must be the project's Nessie service, whose published port on this computer is then the cleanup's address. Anything else cannot be resolved.
    """
    files = settings.compose_directory / "services"
    try:
        job_default = SPARK_URI_DEFAULT.search((files / "spark.yml").read_text())
        published = NESSIE_PORT_DEFAULT.search((files / "nessie.yml").read_text())
    except OSError as error:
        raise ProbeError(f"cannot read the Nessie settings from the service files: {error.strerror}") from error
    if job_default is None or published is None:
        raise ProbeError("the service files give no default for the Nessie address or its published port")
    values = env_file_values(settings.host_env)
    job_uri = environ.get("NESSIE_URI") or values.get("NESSIE_URI") or job_default.group(1)
    port = environ.get("NESSIE_HOST_PORT") or values.get("NESSIE_HOST_PORT") or published.group(1)
    if job_uri != f"http://nessie:{published.group(2)}/api/v2":
        raise ProbeError(f"the job's catalog address {job_uri} is not the project's Nessie service, so the address of its cleanup cannot be resolved")
    if not port.isascii() or not port.isdigit() or not 0 < int(port) < 65536:
        raise ProbeError("the published port of Nessie is not a port number")
    return NessieTarget(job_uri, f"http://127.0.0.1:{int(port)}/api/v2")


@dataclass(frozen=True)
class Options:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    start_deadline_seconds: float = DEFAULT_START_DEADLINE_SECONDS
    job_deadline_seconds: float = DEFAULT_JOB_DEADLINE_SECONDS
    wait_seconds: float = 0.0
    rows: int = DEFAULT_ROWS
    data_path: Path = Path("/var/lib/docker")
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
        monitor: Monitor,
        arguments: Sequence[str],
        container: str,
        scratch: str,
        client: Any,
        runner: Runner = run_command,
    ) -> None:
        self.settings, self.limits, self.options = settings, limits, options
        self.docker, self.budget, self.monitor, self.arguments = docker, budget, monitor, list(arguments)
        self.container, self.scratch, self.client, self.runner = container, scratch, client, runner
        self.required = required_services(limits)
        self.job_started = False
        self.scratch_made = False
        self.job_began = 0.0

    def start_supporting(self, hook: Callable[[], None]) -> None:
        def progress(message: str) -> None:
            say(message)
            hook()

        try:
            start_group(
                self.settings,
                ALWAYS_ON_GROUP,
                wait_seconds=max(0.0, min(self.options.wait_seconds, self.budget.remaining())),
                deadline_seconds=self.budget.limit(self.options.start_deadline_seconds),
                log=progress,
            )
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error

    def service_state(self, service: str) -> ContainerState:
        found = self.docker.list_containers(self.settings.project, service=service)
        if not found:
            raise ProbeError(f"service {service} is not running")
        return self.docker.state(found[0].name)

    def baseline(self) -> dict[str, ServiceBaseline]:
        """Each required service's restart count and kills for memory, and whether it started before the run. The end of the run is compared with them."""
        found = {}
        for service in self.required:
            state = self.service_state(service)
            kills = None
            if self.options.host_figures:
                try:
                    kills = read_cgroup_memory(state.pid).oom_kills
                except FiguresError as error:
                    raise ProbeError(str(error)) from error
            started_before = state.started_at < self.monitor.run_start_ns / NANOSECONDS if state.started_at else None
            found[service] = ServiceBaseline(state.restarts, kills, started_before)
        return found

    def check_catalog(self) -> None:
        """Check that the catalog answers at the address that the cleanup will use, so a cleanup that cannot be verified is known before the job starts."""
        try:
            self.client.config()
        except NessieError as error:
            raise ProbeError(nessie_detail(error)) from error

    def admit_job(self) -> None:
        try:
            wait = max(0.0, min(self.options.wait_seconds, self.budget.remaining()))
            decision = admit(self.settings, self.limits, find_group(self.limits, spark_group(self.limits)), wait, say)
        except (GroupError, AdmissionError, LimitsError) as error:
            raise ProbeError(str(error)) from error
        say(f"admission of the Spark job: group {decision.group} admitted")

    def start_job(self) -> None:
        """Create the run's scratch volume and start the job as a detached container that mounts it at ``/scratch``."""
        self.job_started = True
        self.docker.create_volume(self.scratch, self.settings.project)
        self.scratch_made = True
        spark_arguments = ("--name", self.container, "--volume", f"{self.scratch}:{SCRATCH_MOUNT}", SPARK_SERVICE, *self.arguments)
        command = compose_arguments(self.settings, spark_group(self.limits), "run", "--detach", "--no-deps", "--no-TTY", *spark_arguments)
        timeout = self.budget.limit(COMPOSE_RUN_SECONDS)
        try:
            result = self.runner("docker", command, timeout=timeout, env=rendered_environment(self.limits))
        except FileNotFoundError as error:
            raise ProbeError("docker is not installed or not on the path") from error
        except TimeoutExpired as error:
            raise ProbeError(timeout_text("starting the Spark job", timeout)) from error
        if result.returncode != 0:
            raise ProbeError(f"starting the Spark job: docker exited {result.returncode}")
        self.budget.accept("starting the Spark job")
        self.job_began = time.monotonic()

    def finish_job(self, state: ContainerState, watch: SparkWatch) -> JobOutcome:
        """Read the job's output and build its outcome and evidence from its exit state and the loop's cgroup readings."""
        result, error = validate_result(
            parse_job_result(self.docker.logs(self.container)),
            branch=self.arguments[1],
            rows=int(self.arguments[3]),
            memory_limit_mib=self.limits.services[SPARK_SERVICE].memory_limit_mib,
        )
        host = self.options.host_figures
        cap = watch.cap_bytes if host else (state.memory_cap_bytes or None)
        evidence = ContainerEvidence(
            SPARK_SERVICE, False, cap, watch.peak_bytes, watch.oom_kills, state.oom_killed, state.restarts, "cgroup" if host else "docker"
        )
        return JobOutcome(state.exit_code, state.oom_killed, time.monotonic() - self.job_began, result, error, evidence)

    def final_evidence(self) -> dict[str, ContainerEvidence]:
        """Each required service's cap, peak, kills and restarts at the end of the run, from its cgroup on the host and from Docker on another computer."""
        found: dict[str, ContainerEvidence] = {}
        for service in self.required:
            containers = self.docker.list_containers(self.settings.project, service=service)
            if not containers:
                found[service] = ContainerEvidence(service, False, None, None, None, False, 0, "none")
                continue
            state = self.docker.state(containers[0].name)
            running = state.status == "running"
            if self.options.host_figures:
                memory = read_cgroup_memory(state.pid)
                found[service] = ContainerEvidence(
                    service, running, memory.max_bytes, memory.peak_bytes, memory.oom_kills, state.oom_killed, state.restarts, "cgroup"
                )
            else:
                found[service] = ContainerEvidence(service, running, state.memory_cap_bytes or None, None, None, state.oom_killed, state.restarts, "docker")
        return found

    def cleanup(self, docker: Docker) -> list[CleanupStep]:
        """Remove and verify the job container, then its scratch volume, then its branch. A step after a failed container step is not tried."""
        container = guarded("job container", lambda: remove_container(docker, self.container))
        blocked = "" if container.ok else "not tried: the job container is still present"
        steps = [container]
        for name, made, action in (
            ("scratch volume", self.scratch_made, lambda: remove_scratch(docker, self.scratch)),
            ("nessie branch", self.job_started, lambda: remove_branch(self.client, self.arguments[1])),
        ):
            if not made:
                steps.append(CleanupStep(name, True, "the job did not start"))
            elif blocked:
                steps.append(CleanupStep(name, False, blocked))
            else:
                steps.append(guarded(name, action))
        return steps


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


def run_probe(
    settings: Settings,
    options: Options,
    *,
    runner: Runner = run_command,
    client: Any = None,
    environ: Mapping[str, str] | None = None,
    stop: StopFlag | None = None,
) -> tuple[dict[str, Any], Path]:
    """Run the probe, write the record and return it with its path. The stop flag, a signal handler's, only ever ends the loop early."""
    limits = load_limits(settings.limits_file)
    spark_group(limits)
    target = resolve_nessie(settings, os.environ if environ is None else environ)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    unique = f"{stamp}{secrets.token_hex(2)}"
    folder = options.output_directory or RUNS_FOLDER / stamp
    budget = Budget(options.wait_seconds * 2 + options.start_deadline_seconds + options.job_deadline_seconds + RUN_SLACK_SECONDS)
    docker, plain = Docker(runner=runner, limit=budget.limit), Docker(runner=runner, limit=lambda seconds: min(seconds, CLEANUP_SECONDS))
    sources = HostSources(data_path=options.data_path) if options.host_figures else None
    try:
        reader = Reader(settings.project, sources, docker)
    except FiguresError as error:
        raise ProbeError(f"the host's figures cannot be read ({error}); on a computer that is not the Linux host, pass --docker-figures-only") from error
    preflight(plain, settings.project, folder)
    arguments = job_arguments(unique, options.rows)
    cgroup = read_cgroup_memory if options.host_figures else None
    monitor = Monitor(docker, settings.project, reader, budget, stop or StopFlag(), options.interval_seconds, read_cgroup=cgroup)
    container = f"{settings.project}_capacity_probe_{unique.lower()}"
    scratch = f"{settings.project}_capacity_probe_scratch_{unique.lower()}"
    nessie = client or NessieClient(target.cleanup_uri, 30.0)
    probe = Probe(
        settings,
        limits,
        options,
        docker=docker,
        budget=budget,
        monitor=monitor,
        arguments=arguments,
        container=container,
        scratch=scratch,
        client=nessie,
        runner=runner,
    )
    steps = Steps(
        probe.start_supporting,
        probe.baseline,
        probe.check_catalog,
        probe.admit_job,
        probe.start_job,
        container,
        options.job_deadline_seconds,
        probe.finish_job,
        probe.final_evidence,
        lambda: probe.cleanup(plain),
        {"job_catalog": target.job_uri, "cleanup_catalog": target.cleanup_uri},
        scratch,
    )
    started_utc = utc_text()
    execution = execute(steps, monitor)
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
    return document, write_document(folder, document)


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
    command.add_argument("--compose-directory", type=Path, help="the compose folder, in place of STACK_COMPOSE_DIRECTORY or the repository's compose folder")
    command.add_argument("--project", help="the Compose project name, in place of COMPOSE_PROJECT_NAME or the name in compose.yml")
    command.add_argument(
        "--available-mib", type=int, help="test input, with --docker-figures-only: the available memory for the admission check, in place of /proc/meminfo"
    )
    command.add_argument(
        "--docker-figures-only", action="store_true", help="this computer is not the Linux host: Docker's figures only, and the verdict is at best local_only"
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    command = parser()
    arguments = command.parse_args(argv)
    if arguments.available_mib is not None and not arguments.docker_figures_only:
        command.error("--available-mib replaces the host's memory reading and needs --docker-figures-only")
    options = Options(
        interval_seconds=arguments.interval_seconds,
        start_deadline_seconds=arguments.start_deadline_seconds,
        job_deadline_seconds=arguments.job_deadline_seconds,
        wait_seconds=arguments.wait_seconds or 0.0,
        rows=arguments.rows,
        data_path=arguments.data_path,
        host_figures=not arguments.docker_figures_only,
        output_directory=arguments.output_directory,
    )
    stop = StopFlag()
    stop.install()
    try:
        settings = settings_from_environment(arguments.compose_directory, arguments.project, arguments.available_mib)
        document, path = run_probe(settings, options, stop=stop)
    except (ProbeError, GroupError, LimitsError) as error:
        sys.stderr.write(f"capacity probe: refused: {error}\n")
        return 1
    finally:
        stop.restore()
    findings = document["failures"] + document["gaps"]
    say(f"verdict: {document['verdict']}" + (f" ({'; '.join(findings)})" if findings else ""))
    say(f"record: {path}")
    if stop.requested:
        return INTERRUPTED_EXIT
    return 0 if document["verdict"] in PASSING_VERDICTS else 1


if __name__ == "__main__":
    sys.exit(main())
