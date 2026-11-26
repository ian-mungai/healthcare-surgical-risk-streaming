"""Start and stop a service group: Compose services by profile and Airbyte's workloads through k3s, behind the admission check.

The limits file (``compose/limits.yml``) is the one place that gives a service its group. A Compose service file carries the profile of its group, and a
fast test holds the two together. The always-on group (``supporting``) has no profile, so a plain ``docker compose up`` starts it and nothing else.

``up`` asks the admission check (``check_group`` of ``scripts.stack.admission``) with the services that run now, Compose and k3s together. A group that does
not fit waits and is admitted when memory frees, or fails at its deadline with the four figures. It then starts the group's services that do not run, with
``--no-recreate`` so that a shared service is never replaced, and waits until each reports healthy or its deadline passes. ``down`` stops the group's
services and leaves every other service running. Both are safe to repeat: a group that is up is not started again, and a group that is down is not stopped
again. A message holds the service's name and a closed list of fields of its state (state, health, exit code, out-of-memory flag, restarts), never a
container's own text, so no secret can reach it.

``up`` leaves AIStor out of its group unless the AIStor step recorded that it started the server (``aistor.state`` in the compose folder), because the
server needs its licence, which the deploy places only when the owner holds one. The admission check still counts AIStor's memory.

``up`` refuses a Spark group (``batch_<workload>``) before Compose starts a container: the service starts no job by itself, and a stopped container of an older
version would restart its old job under ``--no-recreate``. A Spark job starts with ``scripts/stack/spark_run.py``, which holds the one Spark slot.

``.venv/bin/python -m scripts.stack.groups up|down <group>`` runs from the repository or inside the Dagster image. The compose folder is
``--compose-directory``, ``STACK_COMPOSE_DIRECTORY`` or the repository's ``compose`` folder. Exit status: 0 done, 1 refused or failed, 75 the group waits
and the wait ended, 2 wrong usage. ``STACK_TEST_AVAILABLE_MIB`` and ``--available-mib`` are test inputs that replace the reading of the host's available
memory, as ``scripts.stack.admission`` documents for its own test inputs.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.project_config import ROOT
from scripts.stack.admission import EXIT_WAIT, SUPPORTING_GROUP, AdmissionError, Decision, check_group, describe
from scripts.stack.job_workloads import SPARK_GROUPS
from scripts.stack.limits import SERVICE_NAME, Limits, LimitsError, load_limits, render_environment
from scripts.stack.yaml_text import YamlError, load_yaml

SPARK_REFUSAL = (
    "group-up never starts a Spark group, because its service starts no job and a stopped container of an older version would restart its old job. "
    "Start a job with python -m scripts.stack.spark_run <job> in the code location container"
)
ALWAYS_ON_GROUPS = frozenset({SUPPORTING_GROUP})
# The services that need AIStor's licence, and the record of the AIStor step in the compose folder (compose_stack_storage_services and
# compose_stack_aistor_state_file of ansible/group_vars/all.yml). A fast test holds both names to the group variables.
LICENSED_SERVICES = ("aistor",)
AISTOR_STATE_NAME = "aistor.state"
AISTOR_STARTED = "started"
COMPOSE_DIRECTORY_VARIABLE = "STACK_COMPOSE_DIRECTORY"
PROJECT_VARIABLE = "COMPOSE_PROJECT_NAME"
AVAILABLE_VARIABLE = "STACK_TEST_AVAILABLE_MIB"
COMPOSE_LABEL = "com.docker.compose"
HOST_SETTINGS_FILE = "host.env"
# The digest settings of the project's three images (the five services that use them). They come from the rendered host settings file and from nothing else.
PROJECT_IMAGE_SETTINGS = frozenset({"IMAGE_DAGSTER", "IMAGE_REPLAY", "IMAGE_SPARK"})
COMPOSE_FILE = "compose.yml"
LIMITS_FILE_NAME = "limits.yml"
COMMAND_SECONDS = 60
STOP_SECONDS = 30
START_DEADLINE_SECONDS = 300
STOP_DEADLINE_SECONDS = 120
POLL_SECONDS = 3
ADMISSION_POLL_SECONDS = 15
STATES = frozenset({"created", "running", "paused", "restarting", "removing", "exited", "dead"})
HEALTH = frozenset({"healthy", "unhealthy", "starting", "none"})
ALIVE_STATES = frozenset({"running", "restarting", "paused"})
INSPECT_FORMAT = (
    f'{{{{index .Config.Labels "{COMPOSE_LABEL}.service"}}}}|{{{{.State.Status}}}}'
    "|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.RestartCount}}"
)
SMALL_NUMBER = re.compile(r"[0-9]{1,6}")
Log = Callable[[str], None]


class GroupError(RuntimeError):
    """A group cannot be started or stopped. The message names the group or the service and holds no text of a container."""


class SparkGroupRefused(GroupError):
    """``group-up`` was asked to start a Spark group, which it never does."""


class GroupWaits(GroupError):
    """The group does not fit in the available memory and the wait for memory ended. The message holds the admission check's four figures."""


class WorkloadControl(Protocol):
    """The control of a group's k3s workloads, which the Docker socket does not reach."""

    def running(self, workloads: Sequence[str]) -> frozenset[str]: ...

    def start(self, workloads: Sequence[str], deadline_seconds: float) -> None: ...

    def stop(self, workloads: Sequence[str], deadline_seconds: float) -> None: ...


@dataclass(frozen=True)
class Group:
    name: str
    compose: tuple[str, ...]
    k3s: tuple[str, ...]


@dataclass(frozen=True)
class ServiceState:
    service: str
    state: str
    health: str
    exit_code: int
    oom_killed: bool
    restarts: int

    @property
    def ready(self) -> bool:
        return self.state == "running" and self.health in ("healthy", "none")


@dataclass(frozen=True)
class Settings:
    compose_directory: Path
    project: str
    host_env: Path | None = None
    available_mib: int | None = None
    k3s: WorkloadControl | None = field(default=None, compare=False)

    @property
    def limits_file(self) -> Path:
        return self.compose_directory / LIMITS_FILE_NAME


@dataclass(frozen=True)
class StartResult:
    group: str
    started: tuple[str, ...]
    decision: Decision


def no_log(message: str) -> None:
    del message


def profile_of(group: str) -> str | None:
    """The Compose profile of a group, or None for a group that is always on."""
    return None if group in ALWAYS_ON_GROUPS else group


def groups_of(limits: Limits) -> dict[str, Group]:
    names = sorted({entry.group for entry in limits.services.values()})
    return {
        name: Group(
            name,
            tuple(sorted(service for service, entry in limits.services.items() if entry.group == name and entry.runtime == "compose")),
            tuple(sorted(service for service, entry in limits.services.items() if entry.group == name and entry.runtime == "k3s")),
        )
        for name in names
    }


def find_group(limits: Limits, name: str) -> Group:
    if not SERVICE_NAME.fullmatch(name):
        raise GroupError(f"{name!r} is not a valid group name: it must be lower case letters, digits and underscores, starting with a letter")
    found = groups_of(limits)
    if name not in found:
        raise GroupError(f"unknown group {name!r}: the limits file holds the groups {', '.join(found)}")
    return found[name]


def check_stoppable(group: str) -> None:
    if group in ALWAYS_ON_GROUPS:
        raise GroupError(f"a group that is always on is never stopped by a group command ({group})")


def compose_arguments(settings: Settings, group: str, *tail: str) -> list[str]:
    """The ``docker`` arguments of a Compose command: the project and its files named, the group's profile enabled when it has one, then ``tail``."""
    directory = settings.compose_directory
    profile = profile_of(group)
    return [
        "compose",
        "--project-directory",
        str(directory),
        "--file",
        str(directory / COMPOSE_FILE),
        "--project-name",
        settings.project,
        *(["--env-file", str(settings.host_env)] if settings.host_env is not None else []),
        *(["--profile", profile] if profile is not None else []),
        *tail,
    ]


def up_arguments(settings: Settings, group: Group) -> list[str]:
    """The command that starts a group: its own services only, detached, and never replacing a container that exists (a shared service is not replaced)."""
    return compose_arguments(settings, group.name, "up", "--detach", "--no-recreate", *group.compose)


def stop_arguments(settings: Settings, group: str, names: Sequence[str]) -> list[str]:
    """The command that stops the named services of a group, with a time for each to stop by itself."""
    return compose_arguments(settings, group, "stop", "--timeout", str(STOP_SECONDS), *names)


def project_name(compose_directory: Path) -> str:
    try:
        document = load_yaml((compose_directory / COMPOSE_FILE).read_text())
    except (OSError, YamlError) as error:
        raise GroupError(f"cannot read the project name from {COMPOSE_FILE} in the compose folder: {type(error).__name__}") from error
    name = document.get("name") if isinstance(document, dict) else None
    if not isinstance(name, str) or not SERVICE_NAME.fullmatch(name):
        raise GroupError(f"{COMPOSE_FILE} gives no valid project name")
    return name


def whole_number(text: str) -> int:
    if not re.fullmatch(r"[0-9]+", text):
        raise GroupError("a memory figure must be a whole number")
    return int(text)


def settings_from_environment(
    compose_directory: Path | None = None, project: str | None = None, available_mib: int | None = None, k3s: WorkloadControl | None = None
) -> Settings:
    """The settings from the arguments, then the environment, then the repository's compose folder."""
    directory = compose_directory or Path(os.environ.get(COMPOSE_DIRECTORY_VARIABLE) or ROOT / "compose")
    host_env = directory / HOST_SETTINGS_FILE
    stated = os.environ.get(AVAILABLE_VARIABLE)
    return Settings(
        compose_directory=directory,
        project=project or os.environ.get(PROJECT_VARIABLE) or project_name(directory),
        host_env=host_env if host_env.is_file() else None,
        available_mib=available_mib if available_mib is not None else (whole_number(stated) if stated else None),
        k3s=k3s,
    )


def rendered_environment(limits: Limits) -> dict[str, str]:
    """The environment of a Compose command: the memory limits from the limits file, then the process's own variables, which win. The digest settings of the
    project's images are the exception: a shell variable of one of those names would win over the rendered host settings file, so none is passed."""
    limits_text = dict(line.split("=", 1) for line in render_environment(limits).splitlines())
    return limits_text | {name: value for name, value in os.environ.items() if name not in PROJECT_IMAGE_SETTINGS}


def docker(arguments: Sequence[str], *, timeout: float, label: str, env: Mapping[str, str] | None = None) -> str:
    """The output of a docker command. A failure names the label and the exit status, never the command's arguments or its output."""
    try:
        result = run_command("docker", arguments, timeout=timeout, env=env)
    except FileNotFoundError as error:
        raise GroupError(f"{label}: docker is not installed or not on the path") from error
    except TimeoutExpired as error:
        raise GroupError(f"{label}: {timeout_text('docker', timeout)}") from error
    if result.returncode != 0:
        raise GroupError(f"{label}: docker exited {result.returncode}")
    return result.stdout


def parse_inspect(text: str) -> dict[str, ServiceState]:
    """The states in the output of ``docker inspect`` with ``INSPECT_FORMAT``. A line of another size is dropped and a value outside its list is ``unknown``.

    Several containers can carry one service name (the one-offs of ``docker compose run`` and the stopped containers of earlier runs). A container that is alive
    is never replaced by a stopped one, so a live job holds its service whatever order Docker lists them in."""
    found: dict[str, ServiceState] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) != 6 or not SERVICE_NAME.fullmatch(parts[0]):
            continue
        service, state, health, exit_code, oom, restarts = parts
        if service in found and found[service].state in ALIVE_STATES and state not in ALIVE_STATES:
            continue
        found[service] = ServiceState(
            service,
            state if state in STATES else "unknown",
            health if health in HEALTH else "unknown",
            int(exit_code) if SMALL_NUMBER.fullmatch(exit_code) else -1,
            oom == "true",
            int(restarts) if SMALL_NUMBER.fullmatch(restarts) else 0,
        )
    return found


def describe_states(states: Iterable[ServiceState], missing: Sequence[str] = ()) -> str:
    """One line for the services: the name and the five fields of each state, and the names with no container."""
    parts = {
        state.service: (
            f"{state.service}: state {state.state}, health {state.health}, exit code {state.exit_code}, "
            f"out of memory {str(state.oom_killed).lower()}, restarts {state.restarts}"
        )
        for state in states
    }
    parts |= {name: f"{name}: no container" for name in missing}
    return "; ".join(parts[name] for name in sorted(parts))


def read_states(settings: Settings, timeout: float = COMMAND_SECONDS) -> dict[str, ServiceState]:
    """The state of every container of the project, by service name. Both reads together take at most ``timeout`` seconds."""
    started = time.monotonic()
    listed = docker(
        ["ps", "--all", "--quiet", "--no-trunc", "--filter", f"label={COMPOSE_LABEL}.project={settings.project}"],
        timeout=timeout,
        label="listing the containers",
    )
    identifiers = listed.split()
    if not identifiers:
        return {}
    left = timeout - (time.monotonic() - started)
    if left <= 0:
        raise GroupError(f"reading the containers: {timeout_text('docker', timeout)}")
    return parse_inspect(docker(["inspect", "--format", INSPECT_FORMAT, *identifiers], timeout=left, label="reading the containers"))


def running_services(settings: Settings, limits: Limits, timeout: float = COMMAND_SECONDS) -> frozenset[str]:
    """The names of the services that are alive now: Compose services whose container runs, is paused or restarts (each holds its memory), and k3s workloads
    from the k3s control. The reads of the containers take at most ``timeout`` seconds."""
    names = {name for name, state in read_states(settings, timeout).items() if state.state in ALIVE_STATES}
    k3s = sorted(name for name, entry in limits.services.items() if entry.runtime == "k3s")
    if k3s and settings.k3s is not None:
        names |= settings.k3s.running(k3s)
    return frozenset(names)


def require_control(settings: Settings, group: Group) -> None:
    if group.k3s and settings.k3s is None:
        raise GroupError(f"the group {group.name} holds k3s workloads and no k3s control is configured")


def wait_ready(settings: Settings, group: Group, deadline_seconds: float, log: Log) -> None:
    """Wait until every Compose service of the group is running and healthy. At the deadline, or when a service has stopped, name each service that is not."""
    started = time.monotonic()
    while True:
        states = read_states(settings)
        waiting = [name for name in group.compose if name not in states or not states[name].ready]
        if not waiting:
            if time.monotonic() - started >= deadline_seconds:
                raise GroupError(f"group {group.name}: ready after the deadline of {deadline_seconds:g} seconds")
            return
        stopped = [name for name in waiting if name in states and states[name].state in ("exited", "dead")]
        report = describe_states([states[name] for name in waiting if name in states], [name for name in waiting if name not in states])
        if stopped:
            raise GroupError(f"group {group.name}: a service stopped: {report}")
        if time.monotonic() - started >= deadline_seconds:
            raise GroupError(f"group {group.name}: not healthy after {deadline_seconds:g} seconds: {report}")
        log(f"group {group.name}: waiting for {', '.join(waiting)}")
        time.sleep(POLL_SECONDS)


def start_compose(settings: Settings, limits: Limits, group: Group, deadline_seconds: float, log: Log) -> None:
    if not group.compose:
        return
    states = read_states(settings)
    if not all(name in states and states[name].state == "running" for name in group.compose):
        docker(
            up_arguments(settings, group),
            timeout=deadline_seconds,
            label=f"starting group {group.name}",
            env=rendered_environment(limits),
        )
    wait_ready(settings, group, deadline_seconds, log)


def stop_compose(settings: Settings, limits: Limits, group: Group, keep: Collection[str], log: Log) -> tuple[str, ...]:
    """Stop the group's Compose services that are alive and not in ``keep``, and return their names. Nothing alive means nothing to do."""
    states = read_states(settings)
    alive = tuple(name for name in group.compose if name not in keep and name in states and states[name].state in ALIVE_STATES)
    if not alive:
        return ()
    docker(
        stop_arguments(settings, group.name, alive),
        timeout=STOP_DEADLINE_SECONDS,
        label=f"stopping group {group.name}",
        env=rendered_environment(limits),
    )
    after = read_states(settings)
    left = [after[name] for name in alive if name in after and after[name].state in ALIVE_STATES]
    if left:
        raise GroupError(f"group {group.name}: a service did not stop: {describe_states(left)}")
    log(f"group {group.name}: stopped {', '.join(alive)}")
    return alive


def admit(settings: Settings, limits: Limits, group: Group, wait_seconds: float, log: Log) -> Decision:
    """The decision of the admission check. A group that does not fit is asked again until it fits or ``wait_seconds`` have passed."""
    started = time.monotonic()
    while True:
        decision = check_group(group.name, limits_path=settings.limits_file, available_mib=settings.available_mib, running=running_services(settings, limits))
        if decision.admitted:
            return decision
        if time.monotonic() - started >= wait_seconds:
            raise GroupWaits(f"after waiting {wait_seconds:g} seconds: {describe(decision)}")
        log(f"admission: {describe(decision)}; waiting for memory")
        time.sleep(ADMISSION_POLL_SECONDS)


def refuse_spark_group(group: Group) -> None:
    """Refuse a Spark group before Compose starts a container. A stopped container of an older version keeps its old entry point under ``up --no-recreate``
    and would start its old job, and the service starts no job by itself. A job starts with ``scripts/stack/spark_run.py``, which takes the slot."""
    if group.name in SPARK_GROUPS:
        raise SparkGroupRefused(f"the group {group.name} is a Spark group: {SPARK_REFUSAL}")


def aistor_started(settings: Settings) -> bool:
    """Whether the AIStor step recorded a start of the server. It records ``skipped`` when the host has no licence and removes the record when it fails."""
    try:
        return (settings.compose_directory / AISTOR_STATE_NAME).read_text().strip() == AISTOR_STARTED
    except OSError, UnicodeError:
        return False


def licensed_group(settings: Settings, group: Group, log: Log) -> Group:
    """The group without the services that need AIStor's licence, unless the AIStor step recorded a start. The same gate as the deploy's."""
    gated = [name for name in group.compose if name in LICENSED_SERVICES]
    if not gated or aistor_started(settings):
        return group
    log(f"group {group.name}: {', '.join(gated)} is left out, because {AISTOR_STATE_NAME} records no start: the deploy starts it with its licence")
    return replace(group, compose=tuple(name for name in group.compose if name not in gated))


def start_group(
    settings: Settings, group_name: str, *, wait_seconds: float = 0, deadline_seconds: float = START_DEADLINE_SECONDS, log: Log = no_log
) -> StartResult:
    """Admit and start a group, and return the services that this call started. A group that is up is not started again."""
    limits = load_limits(settings.limits_file)
    group = find_group(limits, group_name)
    refuse_spark_group(group)
    require_control(settings, group)
    group = licensed_group(settings, group, log)
    before = running_services(settings, limits)
    decision = admit(settings, limits, group, wait_seconds, log)
    log(f"admission: {describe(decision)}")
    start_compose(settings, limits, group, deadline_seconds, log)
    if group.k3s and settings.k3s is not None:
        settings.k3s.start(group.k3s, deadline_seconds)
    return StartResult(group.name, tuple(sorted(running_services(settings, limits) - before)), decision)


def stop_group(
    settings: Settings, group_name: str, *, keep: Collection[str] = (), deadline_seconds: float = STOP_DEADLINE_SECONDS, log: Log = no_log
) -> tuple[str, ...]:
    """Stop a group's services that are not in ``keep`` and return the names stopped. Every other service keeps running."""
    check_stoppable(group_name)
    limits = load_limits(settings.limits_file)
    group = find_group(limits, group_name)
    require_control(settings, group)
    stopped = list(stop_compose(settings, limits, group, keep, log))
    if group.k3s and settings.k3s is not None:
        workloads = tuple(name for name in group.k3s if name not in keep)
        alive = sorted(settings.k3s.running(workloads))
        if alive:
            settings.k3s.stop(alive, deadline_seconds)
            stopped.extend(alive)
    return tuple(stopped)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog="python -m scripts.stack.groups", description=__doc__.split("\n\n")[0] if __doc__ else None)
    command.add_argument("action", choices=("up", "down"))
    command.add_argument("group")
    command.add_argument("--compose-directory", type=Path, help="the compose folder, in place of STACK_COMPOSE_DIRECTORY or the repository's compose folder")
    command.add_argument("--project", help="the Compose project name, in place of COMPOSE_PROJECT_NAME or the name in compose.yml")
    command.add_argument("--available-mib", type=int, help="test input: the host's available memory in MiB, in place of the reading of /proc/meminfo")
    command.add_argument("--wait-seconds", type=float, default=0, help="how long a group that does not fit waits for memory (default 0: report and stop)")
    command.add_argument("--deadline-seconds", type=float, default=START_DEADLINE_SECONDS, help="how long the group's services have to become healthy")
    return command


def say(message: str) -> None:
    sys.stdout.write(f"groups: {message}\n")


def main(argv: Sequence[str] | None = None) -> int:
    from scripts.stack.groups_k3s import control_from_environment  # the k3s control imports this module's errors

    arguments = parser().parse_args(argv)
    try:
        settings = settings_from_environment(arguments.compose_directory, arguments.project, arguments.available_mib, control_from_environment(os.environ))
        if arguments.action == "up":
            result = start_group(settings, arguments.group, wait_seconds=arguments.wait_seconds, deadline_seconds=arguments.deadline_seconds, log=say)
            say(f"group {result.group} is up" + (f": started {', '.join(result.started)}" if result.started else ": nothing to start"))
        else:
            stopped = stop_group(settings, arguments.group, log=say)
            say(f"group {arguments.group} is down" + (f": stopped {', '.join(stopped)}" if stopped else ": nothing was running"))
    except GroupWaits as error:
        sys.stderr.write(f"groups: waits: {describe_error(error)}\n")
        return EXIT_WAIT
    except SparkGroupRefused:
        sys.stderr.write(f"groups: refused: {SPARK_REFUSAL}\n")
        return 1
    except (GroupError, AdmissionError, LimitsError) as error:
        sys.stderr.write(f"groups: refused: {describe_error(error)}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
