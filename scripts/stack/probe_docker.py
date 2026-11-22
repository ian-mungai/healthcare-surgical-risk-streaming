"""The capacity probe's Docker readings: the containers of a Compose project, one container's state, its logs, its removal and the project's lifecycle events.

Each reading is one ``docker`` command through the project's launcher, with a deadline that the caller's ``limit`` function has cut to the time the whole run
has left. The parsers are pure functions over the text of a command formatted with one of the formats below, so fixed text proves them. A command that fails,
a line that is not in its form and a deadline that passes each raise ``ProbeError`` with the step and the exit status, and never a container's own text.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command, timeout_text

COMPOSE_LABEL = "com.docker.compose"
DOCKER_SECONDS = 30.0
NANOSECONDS = 10**9
LIST_FORMAT = '{{.ID}}|{{.Names}}|{{.Label "com.docker.compose.service"}}'
STATS_FORMAT = "{{.ID}}|{{.MemUsage}}"
INSPECT_FORMAT = "{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.Pid}}|{{.RestartCount}}|{{.HostConfig.Memory}}|{{.State.StartedAt}}"
LIFECYCLE_ACTIONS = ("die", "oom", "kill", "restart")
LIST_LINE = re.compile(r"([0-9a-f]{12})\|([A-Za-z0-9_.-]+)\|([A-Za-z0-9_.-]*)")
STATE_LINE = re.compile(r"([a-z]+)\|(-?\d+)\|(true|false)\|(\d+)\|(\d+)\|(\d+)\|(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z")
NAME_TEXT = re.compile(r"[A-Za-z0-9_.-]+")
VOLUME_LABELS = ("com.docker.compose.project", "surgical_risk.capacity_probe")
Runner = Callable[..., CompletedProcess[str]]
Limiter = Callable[[float], float]


class ProbeError(RuntimeError):
    """A step of the probe failed. The message names the step and holds no text of a container."""


@dataclass(frozen=True)
class ContainerInfo:
    id: str
    name: str
    service: str


@dataclass(frozen=True)
class ContainerState:
    status: str
    exit_code: int
    oom_killed: bool
    pid: int
    restarts: int
    memory_cap_bytes: int
    started_at: float


@dataclass(frozen=True)
class ContainerEvent:
    nano: int
    action: str
    service: str
    name: str
    detail: str

    def describe(self) -> str:
        return f"{self.action} of {self.service or self.name}" + (f" ({self.detail})" if self.detail else "")


def parse_listing(text: str) -> list[ContainerInfo]:
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        match = LIST_LINE.fullmatch(line.strip())
        if match is None:
            raise ProbeError(f"docker ps line {number} is not in the form <container>|<name>|<service>")
        found.append(ContainerInfo(*match.groups()))
    return found


def started_seconds(stamp: str, fraction: str | None) -> float:
    """The Unix time of a container's start, from the time Docker prints. A container that never started has 0."""
    if stamp.startswith("0001"):
        return 0.0
    return datetime.fromisoformat(stamp + "+00:00").timestamp() + (float(f"0.{fraction}") if fraction else 0.0)


def parse_state(text: str) -> ContainerState:
    """The state of a container from ``docker inspect`` with ``INSPECT_FORMAT``."""
    match = STATE_LINE.fullmatch(text.strip())
    if match is None:
        raise ProbeError("docker inspect did not answer in the expected form")
    status, exit_code, oom, pid, restarts, memory, started, fraction = match.groups()
    return ContainerState(status, int(exit_code), oom == "true", int(pid), int(restarts), int(memory), started_seconds(started, fraction))


def event_detail(attributes: dict[str, Any], action: str) -> str:
    keys = {"die": "exitCode", "kill": "signal"}
    value = attributes.get(keys.get(action, ""), "")
    return f"{keys[action]} {value}" if action in keys and isinstance(value, str) and value else ""


def parse_events(text: str) -> list[ContainerEvent]:
    """The events of ``docker events --format '{{json .}}'``, one JSON document on each line."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            document = json.loads(line)
            actor = document["Actor"]["Attributes"]
            action, nano = document["Action"], document["timeNano"]
            name, service = actor.get("name", ""), actor.get(f"{COMPOSE_LABEL}.service", "")
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise ProbeError(f"docker events line {number} is not in the expected form") from error
        if not isinstance(action, str) or not isinstance(nano, int) or isinstance(nano, bool) or not all(isinstance(item, str) for item in (name, service)):
            raise ProbeError(f"docker events line {number} is not in the expected form")
        found.append(ContainerEvent(nano, action.split(":")[0], service, name, event_detail(actor, action)))
    return found


def timestamp(nano: int) -> str:
    """A Unix time in nanoseconds in the form ``docker events`` takes, written with whole numbers so that no digit is lost."""
    return f"{nano // NANOSECONDS}.{nano % NANOSECONDS:09d}"


class Docker:
    """The Docker commands of one run. ``runner`` starts a program like ``run_command``. ``limit`` cuts a deadline to the time that is left."""

    def __init__(self, *, runner: Runner = run_command, limit: Limiter = lambda seconds: seconds) -> None:
        self.runner = runner
        self.limit = limit

    def run(self, arguments: Sequence[str], *, label: str, seconds: float = DOCKER_SECONDS, accepted: Sequence[int] = (0,)) -> CompletedProcess[str]:
        timeout = self.limit(seconds)
        try:
            result = self.runner("docker", list(arguments), timeout=timeout)
        except FileNotFoundError as error:
            raise ProbeError(f"{label}: docker is not installed or not on the path") from error
        except TimeoutExpired as error:
            raise ProbeError(f"{label}: {timeout_text('docker', timeout)}") from error
        if result.returncode not in accepted:
            raise ProbeError(f"{label}: docker exited {result.returncode}")
        return result

    def list_containers(self, project: str, *, service: str | None = None, include_stopped: bool = False) -> list[ContainerInfo]:
        filters = [f"label={COMPOSE_LABEL}.project={project}", *([f"label={COMPOSE_LABEL}.service={service}"] if service else [])]
        arguments = ["ps", *(["--all"] if include_stopped else []), *[item for text in filters for item in ("--filter", text)], "--format", LIST_FORMAT]
        return parse_listing(self.run(arguments, label="listing the containers").stdout)

    def usage_texts(self, project: str) -> tuple[str, str]:
        """The listing of the project's running containers and the memory statistics of every running container, as text."""
        listing = self.run(
            ["ps", "--filter", f"label={COMPOSE_LABEL}.project={project}", "--format", '{{.ID}}|{{.Label "com.docker.compose.service"}}'],
            label="listing the containers",
        ).stdout
        return listing, self.run(["stats", "--no-stream", "--format", STATS_FORMAT], label="reading the container statistics").stdout

    def state(self, container: str) -> ContainerState:
        return parse_state(self.run(["inspect", "--format", INSPECT_FORMAT, container], label="reading a container's state").stdout)

    def logs(self, container: str) -> str:
        return self.run(["logs", container], label="reading the job's output").stdout

    def remove(self, container: str) -> None:
        self.run(["rm", "--force", container], label="removing a container", accepted=(0, 1))

    def exists(self, container: str) -> bool:
        """Whether a container of this exact name exists, stopped or not. A command that fails is an error and never an answer."""
        self.check_name(container)
        result = self.run(["ps", "--all", "--filter", f"name=^/{container}$", "--format", "{{.ID}}"], label="looking for a container")
        return bool(result.stdout.strip())

    def create_volume(self, name: str, project: str) -> None:
        """Create the run's own named volume, labelled with the project so that the project's removal finds it."""
        self.check_name(name)
        labels = [item for label in VOLUME_LABELS for item in ("--label", f"{label}={project if label.endswith('project') else 'run'}")]
        self.run(["volume", "create", *labels, name], label="creating the job's scratch volume")

    def volume_exists(self, name: str) -> bool:
        self.check_name(name)
        return bool(self.run(["volume", "ls", "--quiet", "--filter", f"name=^{name}$"], label="looking for the job's scratch volume").stdout.strip())

    def remove_volume(self, name: str) -> None:
        self.check_name(name)
        self.run(["volume", "rm", "--force", name], label="removing the job's scratch volume", accepted=(0, 1))

    @staticmethod
    def check_name(name: str) -> None:
        if not NAME_TEXT.fullmatch(name):
            raise ProbeError("a name holds a character that a name may not")

    def events(self, project: str, since_nano: int, until_nano: int, *, seconds: float) -> list[ContainerEvent]:
        """The lifecycle events (die, oom, kill, restart) of the project's containers between two instants. The command ends at ``until_nano``."""
        filters = [f"label={COMPOSE_LABEL}.project={project}", "type=container", *[f"event={action}" for action in LIFECYCLE_ACTIONS]]
        arguments = [
            "events", "--since", timestamp(since_nano), "--until", timestamp(until_nano), *[item for text in filters for item in ("--filter", text)],
            "--format", "{{json .}}",
        ]  # fmt: skip
        return parse_events(self.run(arguments, label="reading the container events", seconds=seconds).stdout)
