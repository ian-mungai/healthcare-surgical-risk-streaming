"""The capacity probe: the always-on group and one Spark job run together while the host's figures are read, and what was read is saved.

``.venv/bin/python -m scripts.stack.capacity_probe [--output-directory <folder>]`` runs on the Linux host. It judges nothing: it never compares a figure with a
limit, a cap or a reserve, never calls the admission check and never says that a run passed. The owner reads the record and decides. It is one process with one
thread and one absolute monotonic deadline. Before it starts anything it checks that the output folder can take a new record, that no Spark container of
the project exists and that the job's catalog address and the address of its cleanup are one Nessie (resolved from the Compose files and ``host.env``). Then it

1. starts the always-on group (Compose ``up`` and the wait for health share one deadline). Each time the wait reports progress, the probe takes a sample and
   reads the events that came since its cursor,
2. reads each required service's state and cgroup files, checks that the catalog answers and starts the Spark job as a detached container with a scratch
   volume of its own,
3. loops: read the Spark container's state and cgroup files, take one sample (host figures and every container's memory), read the events with
   ``docker events --since <cursor less a few seconds> --until <now>`` (the end is in the past, so the command returns at once, and the cursor moves only
   after a command that succeeded), then wait out the rest of the interval. A reading that fails is named in its sample and counted, and the loop goes on.
   The loop ends after the Spark container has exited and one more iteration has run,
4. reads each required service again, removes the job container, then its scratch volume, then its Nessie branch, and looks each one up again, and
5. writes ``capacity_probe.json`` into a new folder under ``runs/capacity_probe/`` (see ``scripts.stack.probe_record``).

A SIGINT or SIGTERM only sets a flag. The loop leaves at its next check, every step after it runs in full and the record is written. No wait or reading is taken
after the deadline. A run that did not reach the end of the Spark job writes ``run_state: interrupted`` with a fixed cause and step.

``--docker-figures-only`` is for a computer that is not the Linux host: the record's mode is ``local``, Docker's figures are real and each sample names the host
figures as missing. Stream lag is not measured: the stream path does not exist in this phase.
"""

from __future__ import annotations

import argparse
import dataclasses
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
from scripts.stack.groups import (
    COMPOSE_DIRECTORY_VARIABLE,
    HOST_SETTINGS_FILE,
    PROJECT_VARIABLE,
    GroupError,
    Settings,
    compose_arguments,
    find_group,
    project_name,
    read_states,
    rendered_environment,
    up_arguments,
    wait_ready,
)
from scripts.stack.groups import docker as run_group_docker
from scripts.stack.host_figures import (
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
from scripts.stack.probe_docker import NANOSECONDS, ContainerEvent, ContainerState, Docker, ProbeError, Runner
from scripts.stack.probe_record import (
    RUN_INTERRUPTED,
    RUN_RECORDED,
    SPARK_SERVICE,
    CgroupRead,
    CleanupStep,
    Collected,
    JobFacts,
    Sample,
    ServiceSnapshot,
    build_record,
    check_destination,
    read_result,
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
EVENT_OVERLAP_NS = 5 * NANOSECONDS
NOT_FOUND = 404
RESULT_PREFIX = "RESULT "
INTERRUPTED_EXIT = 130
SCRATCH_MOUNT = "/scratch"
SPARK_URI_DEFAULT = re.compile(r"NESSIE_URI: \$\{NESSIE_URI:-([^}]+)\}")
NESSIE_PORT_DEFAULT = re.compile(r"127\.0\.0\.1:\$\{NESSIE_HOST_PORT:-(\d+)\}:(\d+)")
EXIT_NOTE = "The exit status says that the tool ran and nothing about capacity."
Pause = Callable[[float], None]


class Deadline(ProbeError):
    """The run's deadline or the job's own deadline has passed."""


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
    """One monotonic deadline for the whole run. Every wait and every reading asks it for the time that is left, and no result is taken after it."""

    def __init__(self, total_seconds: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        self.total_seconds = total_seconds
        self.clock = clock
        self.started = clock()

    def remaining(self) -> float:
        return self.total_seconds - (self.clock() - self.started)

    def limit(self, seconds: float) -> float:
        """The shorter of ``seconds`` and the time that is left. A run with no time left raises ``Deadline``."""
        left = self.remaining()
        if left <= 0:
            raise Deadline(f"the run's deadline of {self.total_seconds:g} seconds has passed")
        return min(seconds, left)

    def accept(self, label: str) -> None:
        """Refuse a result that arrived after the deadline."""
        if self.remaining() <= 0:
            raise Deadline(f"{label} finished after the deadline of {self.total_seconds:g} seconds")


def utc_text() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(message: str) -> None:
    sys.stdout.write(f"probe: {message}\n")


class Reader:
    """One host reading and one Docker reading. A reading that fails is named in the sample's ``missing`` list and counted, and never ends the run."""

    def __init__(
        self,
        project: str,
        sources: HostSources | None,
        docker: Docker,
        required: Sequence[str],
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self.project, self.sources, self.docker, self.required = project, sources, docker, tuple(required)
        self.clock_ns, self.now = clock_ns, now
        self.previous: HostReading | None = read_host(sources, now) if sources is not None else None
        self.last_end_ns = clock_ns()

    def take(self, *, spark_running: bool) -> tuple[Sample, int]:
        """The sample and the number of readings that failed. Host figures that this mode does not have are named as missing and are not failures."""
        missing: list[str] = []
        errors = 0
        figures = None
        if self.sources is None or self.previous is None:
            missing.append("host_figures")
        else:
            try:
                current = read_host(self.sources, self.now)
                figures = host_figures(self.previous, current)
                self.previous = current
            except FiguresError:
                missing.append("host_figures")
                errors += 1
        usage: dict[str, int] = {}
        try:
            listing, statistics = self.docker.usage_texts(self.project)
            usage = parse_container_usage(listing, statistics)
        except ProbeError, FiguresError:
            missing.append("container_memory")
            errors += 1
        taken = self.clock_ns()
        started, running = self.last_end_ns, {name: name in usage for name in (*self.required, SPARK_SERVICE)}
        if figures is not None or self.sources is None:
            self.last_end_ns = taken
        return Sample(taken, utc_text(), started, taken, tuple(missing), spark_running, running, figures, usage), errors


@dataclass
class SparkWatch:
    """What the loop saw of the Spark container: whether it ran and its state at the exit."""

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
    read_cgroup: Callable[[int], Any] | None = None
    clock_ns: Callable[[], int] = time.time_ns
    monotonic: Callable[[], float] = time.monotonic
    pause: Pause = time.sleep
    samples: list[Sample] = field(default_factory=list)
    events: list[ContainerEvent] = field(default_factory=list)
    windows: list[tuple[int, int]] = field(default_factory=list)
    spark_reads: list[CgroupRead] = field(default_factory=list)
    spark: SparkWatch = field(default_factory=SparkWatch)
    sample_errors: int = 0
    cgroup_unreadable: int = 0
    run_start_ns: int = 0
    cursor_ns: int = 0
    last_tick: float = float("-inf")
    seen: set[tuple[int, str, str]] = field(default_factory=set)

    def begin(self) -> None:
        """Start the event cursor, before anything of the run starts."""
        self.run_start_ns = self.cursor_ns = self.clock_ns()

    def advance_events(self) -> None:
        """Read the events from the cursor to now. The cursor moves only to the end of a command that succeeded.

        Docker can publish an event a moment after its own time stamp. A command that starts exactly at the cursor would never see such an event, so each
        command asks from ``EVENT_OVERLAP_NS`` before the cursor (never before the run started) and an event seen twice is kept once. The windows still chain.
        """
        until = self.clock_ns()
        if until <= self.cursor_ns:
            return
        found = self.docker.events(self.project, max(self.run_start_ns, self.cursor_ns - EVENT_OVERLAP_NS), until, seconds=EVENTS_SECONDS)
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
            self.spark_reads.append(CgroupRead(self.clock_ns(), self.read_cgroup(state.pid)))
        except FiguresError:
            self.cgroup_unreadable += 1

    def tick(self, container: str | None = None) -> ContainerState | None:
        """One iteration: the Spark state (when a container is named), one sample, the events since the cursor. A reading that fails is named in the sample."""
        state, failed = None, []
        if container:
            try:
                state = self.docker.state(container)
                self.watch(state)
            except ProbeError:
                failed.append("spark_state")
        sample, read_errors = self.reader.take(spark_running=state is not None and state.status == "running")
        try:
            self.advance_events()
        except ProbeError:
            failed.append("events")
        if failed or read_errors:
            self.sample_errors += 1
            sample = dataclasses.replace(sample, missing=(*sample.missing, *failed))
        self.samples.append(sample)
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
                raise Deadline(timeout_text("the Spark job", job_seconds))
            began = self.monotonic()
            state = self.tick(container)
            if self.spark.exit_state is not None:
                return self.spark.exit_state
            if state is not None and state.status in ("exited", "dead"):
                self.spark.exit_state, self.spark.exit_ns = state, self.clock_ns()
                self.budget.accept("the Spark job's exit")
                continue
            self.stop.sleep(max(0.0, min(self.interval_seconds - (self.monotonic() - began), self.budget.remaining())), pause=self.pause)


@dataclass(frozen=True)
class Steps:
    """The steps of a run. Each raises ``ProbeError`` when it cannot be done."""

    start_supporting: Callable[[Callable[[], None]], None]
    snapshot: Callable[[], Mapping[str, ServiceSnapshot]]
    check_catalog: Callable[[], None]
    start_job: Callable[[], None]
    container: str
    job_seconds: float
    finish_job: Callable[[ContainerState], JobFacts]
    cleanup: Callable[[], list[CleanupStep]]
    scratch: str = ""


def run_to_end(steps: Steps, monitor: Monitor) -> tuple[str, str, Mapping[str, ServiceSnapshot], Mapping[str, ServiceSnapshot], JobFacts | None]:
    """The cause (empty when the loop ran to the end of the job), the step it ended in, the two snapshots and the job."""
    stage, cause, job = "always_on_start", "", None
    start: Mapping[str, ServiceSnapshot] = {}
    end: Mapping[str, ServiceSnapshot] = {}
    try:
        steps.start_supporting(monitor.hook)
        monitor.budget.accept("the start of the always-on group")
        stage = "snapshot_start"
        start = steps.snapshot()
        stage = "catalog_check"
        steps.check_catalog()
        stage = "job_start"
        steps.start_job()
        stage = "job_watch"
        job = steps.finish_job(monitor.watch_job(steps.container, steps.job_seconds))
    except Stopped:
        cause = "stop_flag"
    except Deadline:
        cause = "deadline"
    except ProbeError:
        cause = "error"
    if start:
        end = guarded_snapshot(steps)
    return cause, stage if cause else "", start, end, job


def guarded_snapshot(steps: Steps) -> Mapping[str, ServiceSnapshot]:
    try:
        return steps.snapshot()
    except ProbeError:
        return {}


def execute(steps: Steps, monitor: Monitor, mode: str) -> Collected:
    """Run the steps in order under the loop and collect what was read. The cleanup runs after every outcome, a signal included."""
    monitor.begin()
    cleanup: list[CleanupStep] = []
    try:
        cause, step, start, end, job = run_to_end(steps, monitor)
    finally:
        try:
            cleanup = steps.cleanup()
        except Exception as error:  # a cleanup that fails in an unforeseen way is a step that was not verified, never a silent one
            cleanup = [CleanupStep("cleanup", False, f"stopped with {type(error).__name__}")]
    return Collected(
        RUN_INTERRUPTED if cause else RUN_RECORDED,
        cause,
        step,
        mode,
        list(monitor.samples),
        list(monitor.spark_reads),
        list(monitor.events),
        list(monitor.windows),
        monitor.run_start_ns,
        monitor.spark.exit_ns,
        start,
        end,
        job,
        cleanup,
        monitor.sample_errors,
        monitor.cgroup_unreadable,
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


def nessie_detail(error: NessieError) -> str:
    """What a failed Nessie call says: its status or that it did not answer, and never the answer's text or an address."""
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
    """Delete the job's private Nessie branch and look it up again. A branch that never existed is absent. A call that fails leaves the step not verified."""
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
    """Stop and remove the job container and look for a container of that name again."""
    name = "job container"
    try:
        docker.remove(container)
        present = docker.exists(container)
    except ProbeError as error:
        return CleanupStep(name, False, str(error))
    return CleanupStep(name, not present, "no container remains" if not present else "still present after its removal")


def remove_scratch(docker: Docker, volume: str) -> CleanupStep:
    """Remove the run's scratch volume by its recorded name and look for it again."""
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
    address must be the project's Nessie service, whose published port on this computer is then the cleanup's address. Anything else cannot be resolved, and the
    message never holds an address.
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
        raise ProbeError("the job's catalog address is not the project's Nessie service, so the address of its cleanup cannot be resolved")
    if not port.isascii() or not port.isdigit() or not 0 < int(port) < 65536:
        raise ProbeError("the published port of Nessie is not a port number")
    return NessieTarget(job_uri, f"http://127.0.0.1:{int(port)}/api/v2")


def probe_settings(compose_directory: Path | None, project: str | None) -> Settings:
    """The Compose settings from the arguments, then the environment, then the repository. No admission figure is read: this probe has no admission."""
    directory = compose_directory or Path(os.environ.get(COMPOSE_DIRECTORY_VARIABLE) or ROOT / "compose")
    host_env = directory / HOST_SETTINGS_FILE
    return Settings(
        compose_directory=directory,
        project=project or os.environ.get(PROJECT_VARIABLE) or project_name(directory),
        host_env=host_env if host_env.is_file() else None,
    )


@dataclass(frozen=True)
class Options:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    start_deadline_seconds: float = DEFAULT_START_DEADLINE_SECONDS
    job_deadline_seconds: float = DEFAULT_JOB_DEADLINE_SECONDS
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
        arguments: Sequence[str],
        container: str,
        scratch: str,
        client: Any,
        runner: Runner = run_command,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.settings, self.limits, self.options = settings, limits, options
        self.docker, self.budget, self.arguments = docker, budget, list(arguments)
        self.container, self.scratch, self.client, self.runner, self.clock_ns = container, scratch, client, runner, clock_ns
        self.required = required_services(limits)
        self.job_started = False
        self.scratch_named = False
        self.job_began = 0.0

    def start_supporting(self, hook: Callable[[], None]) -> None:
        """Start the always-on group, with no admission check. Compose ``up`` and the wait for health share one deadline."""
        started, allowed = self.budget.clock(), self.budget.limit(self.options.start_deadline_seconds)

        def progress(message: str) -> None:
            say(message)
            hook()

        try:
            group = find_group(self.limits, ALWAYS_ON_GROUP)
            states = read_states(self.settings)
            if not all(name in states and states[name].state == "running" for name in group.compose):
                run_group_docker(
                    up_arguments(self.settings, group), timeout=allowed, label="starting the always-on group", env=rendered_environment(self.limits)
                )
            left = allowed - (self.budget.clock() - started)
            if left <= 0:
                raise Deadline("the start of the always-on group finished after its deadline")
            wait_ready(self.settings, group, left, progress)
        except GroupError as error:
            raise ProbeError(str(error)) from error

    def snapshot(self) -> dict[str, ServiceSnapshot]:
        """What each required service shows now: its state and, on the host, its cgroup files with the read time. A figure not read is ``None``."""
        found: dict[str, ServiceSnapshot] = {}
        for service in self.required:
            containers = self.docker.list_containers(self.settings.project, service=service)
            if not containers:
                found[service] = ServiceSnapshot(False, None, None, None, None, None)
                continue
            state = self.docker.state(containers[0].name)
            cgroup = None
            if self.options.host_figures:
                try:
                    cgroup = CgroupRead(self.clock_ns(), read_cgroup_memory(state.pid))
                except FiguresError:
                    cgroup = None
            found[service] = ServiceSnapshot(
                state.status == "running", state.restarts, state.started_at, state.oom_killed, state.memory_cap_bytes or None, cgroup
            )
        return found

    def check_catalog(self) -> None:
        """Check that the catalog answers at the address that the cleanup will use, so a cleanup that cannot be checked is known before the job starts."""
        try:
            self.client.config()
        except NessieError as error:
            raise ProbeError(nessie_detail(error)) from error

    def start_job(self) -> None:
        """Name the run's scratch volume, create it and start the job as a detached container that mounts it at ``/scratch``.

        The volume is named before it is created, so the cleanup removes it by its name even when the creation timed out.
        """
        self.job_started = True
        self.scratch_named = True
        self.docker.create_volume(self.scratch, self.settings.project)
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

    def finish_job(self, state: ContainerState) -> JobFacts:
        """The job as it ended: its exit status, its out-of-memory flag, its time and the documented fields of its result as read."""
        read = read_result(parse_job_result(self.docker.logs(self.container)))
        return JobFacts(state.exit_code, state.oom_killed, time.monotonic() - self.job_began, read or {}, read is not None)

    def cleanup(self, docker: Docker) -> list[CleanupStep]:
        """Remove and look up again the job container, then its scratch volume, then its branch. A step after a container that is still there is not tried."""
        container = guarded("job container", lambda: remove_container(docker, self.container))
        blocked = "" if container.verified_absent else "not tried: the job container is still present"
        steps = [container]
        for name, wanted, action in (
            ("scratch volume", self.scratch_named, lambda: remove_scratch(docker, self.scratch)),
            ("nessie branch", self.job_started, lambda: remove_branch(self.client, self.arguments[1])),
        ):
            if not wanted:
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
    budget = Budget(options.start_deadline_seconds + options.job_deadline_seconds + RUN_SLACK_SECONDS)
    docker, plain = Docker(runner=runner, limit=budget.limit), Docker(runner=runner, limit=lambda seconds: min(seconds, CLEANUP_SECONDS))
    sources = HostSources(data_path=options.data_path) if options.host_figures else None
    required = required_services(limits)
    try:
        reader = Reader(settings.project, sources, docker, required)
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
        settings, limits, options, docker=docker, budget=budget, arguments=arguments, container=container, scratch=scratch, client=nessie, runner=runner
    )
    steps = Steps(
        probe.start_supporting,
        probe.snapshot,
        probe.check_catalog,
        probe.start_job,
        container,
        options.job_deadline_seconds,
        probe.finish_job,
        lambda: probe.cleanup(plain),
        scratch,
    )
    started_utc = utc_text()
    run = execute(steps, monitor, "host" if options.host_figures else "local")
    for step in run.cleanup:
        say(f"cleanup {step.name}: {'verified' if step.verified_absent else 'NOT verified'}: {step.detail}")
    limits_mib = {name: entry.memory_limit_mib for name, entry in limits.services.items() if entry.runtime == "compose"}
    record = build_record(
        run,
        limits_mib=limits_mib,
        reserve_mib=limits.host_reserve_mib,
        required=required,
        interval_seconds=options.interval_seconds,
        deadline_seconds=budget.total_seconds,
        started_utc=started_utc,
        finished_utc=utc_text(),
        job_arguments=arguments,
    )
    return record, write_document(folder, record)


def exit_status(document: Mapping[str, Any], stop_requested: bool) -> int:
    """0 when the record says the loop ran to the end of the Spark job, else 130 after a signal and 1 otherwise. It says nothing about capacity."""
    if document["run_state"] == RUN_RECORDED:
        return 0
    return INTERRUPTED_EXIT if stop_requested else 1


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog="python -m scripts.stack.capacity_probe", description=__doc__.split("\n\n")[0] if __doc__ else None, epilog=EXIT_NOTE
    )
    command.add_argument("--output-directory", type=Path, help="a new folder for the record (default: a new folder under runs/capacity_probe)")
    command.add_argument("--interval-seconds", type=positive_finite, default=DEFAULT_INTERVAL_SECONDS, help="seconds between two samples")
    command.add_argument(
        "--start-deadline-seconds", type=positive_finite, default=DEFAULT_START_DEADLINE_SECONDS, help="how long the always-on group has to become healthy"
    )
    command.add_argument("--job-deadline-seconds", type=positive_finite, default=DEFAULT_JOB_DEADLINE_SECONDS, help="how long the Spark job has to finish")
    command.add_argument("--rows", type=positive_whole, default=DEFAULT_ROWS, help="the Spark job's row count: the fixed workload")
    command.add_argument("--data-path", type=Path, default=Path("/var/lib/docker"), help="a folder on the data disk, for its free space and its disk activity")
    command.add_argument("--compose-directory", type=Path, help="the compose folder, in place of STACK_COMPOSE_DIRECTORY or the repository's compose folder")
    command.add_argument("--project", help="the Compose project name, in place of COMPOSE_PROJECT_NAME or the name in compose.yml")
    command.add_argument(
        "--docker-figures-only",
        action="store_true",
        help="this computer is not the Linux host: the record's mode is local and the host figures are named as missing",
    )
    return command


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    options = Options(
        interval_seconds=arguments.interval_seconds,
        start_deadline_seconds=arguments.start_deadline_seconds,
        job_deadline_seconds=arguments.job_deadline_seconds,
        rows=arguments.rows,
        data_path=arguments.data_path,
        host_figures=not arguments.docker_figures_only,
        output_directory=arguments.output_directory,
    )
    stop = StopFlag()
    stop.install()
    try:
        document, path = run_probe(probe_settings(arguments.compose_directory, arguments.project), options, stop=stop)
    except (ProbeError, GroupError, LimitsError) as error:
        sys.stderr.write(f"capacity probe: refused: {error}\n")
        return 1
    finally:
        stop.restore()
    gaps = ", ".join(
        f"{gap['code']}{':' + str(gap['count']) if 'count' in gap else ''}{':' + gap['step'] if 'step' in gap else ''}" for gap in document["gaps"]
    )
    say(
        f"run state: {document['run_state']}"
        + (f" ({document['interruption']['cause']} in {document['interruption']['step']})" if document["interruption"]["cause"] else "")
    )
    say(f"gaps: {gaps or 'none'}")
    say(f"record: {path}")
    return exit_status(document, stop.requested)


if __name__ == "__main__":
    sys.exit(main())
