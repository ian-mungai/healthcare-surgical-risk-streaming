"""The one Spark slot: one Spark job runs at a time on a Docker engine, whichever of the four Spark groups it belongs to and however it starts.

The host's memory figure assumes the limit of one Spark job (``infra/heavy_memory.tf``), but each Spark workload is its own service group, so the memory check
of one group cannot see another group's job. The slot is a container name that Docker itself keeps unique. Every supported launcher starts its Spark container
with ``--name SLOT_NAME`` (Docker: https://docs.docker.com/reference/cli/docker/container/run/#name and, for a one-off of a Compose service,
https://docs.docker.com/reference/cli/docker/compose/run/), so a second launch fails on the name and is reported as the slot being busy. No lock file stands
between the check and the start: the engine decides, once, when it creates the container.

The slot is held exactly as long as the container exists. A client that is killed, a restart of the code location container and a ``docker pause`` of the
job change nothing, because the container is the holder and not the process that started it. A launch that passes its deadline stops and removes its own
container (the one that carries its own ``OWNER_LABEL`` value, by container ID), reads the engine again and reports the slot as free only when the container is
gone. A container with the slot's name that another launch started is never touched. A container that is left over (exited, not removed) keeps the name, so
the slot stays busy and its state is in the message: remove that container by hand.

Before it starts a container, a launcher also reads which Spark services of its project are alive (running, paused or restarting): a service container that
does not carry the slot's name, such as one of an older version, holds the slot too. A wait is bounded by a deadline that every read obeys: a read gets the
time that is left, and a free reading that arrives after the deadline admits nothing. The Dagster runs of the Spark groups share the run tag
``surgical_risk/spark`` (``orchestration/surgical_risk/concurrency.py``), which the run queue limits to one. ``group-up`` starts no Spark group
(``scripts/stack/groups.py``).
"""

from __future__ import annotations

import argparse
import contextlib
import math
import time
import uuid
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from subprocess import CompletedProcess

from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.stack.job_workloads import SPARK_SERVICES

SLOT_NAME = "surgical-risk-spark-slot"
OWNER_LABEL = "surgical-risk.spark_slot_owner"
POLL_SECONDS = 5.0
# The most that one read of the engine may take, and the time that the stop of a container may take before it is removed by force.
READ_SECONDS = 60.0
STOP_SECONDS = 30
# The words of the daemon's refusal of a name that exists (Docker's "Conflict. The container name ... is already in use").
CONFLICT_TEXT = "is already in use"
LISTING = '{{.ID}}|{{.State}}|{{.Label "' + OWNER_LABEL + '"}}'

Docker = Callable[[Sequence[str], float], CompletedProcess[str]]
Running = Callable[[float], Collection[str]]
BuildArguments = Callable[[Sequence[str]], Sequence[str]]


class SlotError(RuntimeError):
    """The slot cannot be claimed. The message holds the reason and never the text of a container."""


class SlotBusy(SlotError):
    """The one Spark slot is taken. The message names the container or the Spark services that hold it."""


@dataclass(frozen=True, slots=True)
class SlotContainer:
    """The container that has the slot's name: its full ID, its state and the value of its owner label (empty when it has none)."""

    id: str
    state: str
    owner: str


def spark_holders(running: Collection[str]) -> tuple[str, ...]:
    """The Spark services among the live services, in name order."""
    return tuple(sorted(set(running) & set(SPARK_SERVICES)))


def busy_message(holders: Sequence[str]) -> str:
    return f"the one Spark slot is held by {', '.join(holders)}: one Spark job runs at a time"


def finite_seconds(value: str | float) -> float:
    """A number of seconds that is finite and not negative. Anything else (``nan``, ``inf``, a negative number, a word) is refused."""
    try:
        seconds = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("not a number") from None
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError("not a finite number of seconds that is 0 or more")
    return seconds


def checked_wait(wait_seconds: float) -> float:
    """``wait_seconds`` when it is a finite number of seconds that is 0 or more, else a ``SlotError``."""
    try:
        return finite_seconds(wait_seconds)
    except argparse.ArgumentTypeError:
        raise SlotError("the wait for the one Spark slot must be a finite number of seconds that is 0 or more") from None


def new_owner() -> str:
    """The owner value of one launch: it tells this launch's container from every other container that has the slot's name."""
    return uuid.uuid4().hex


def slot_options(owner: str) -> list[str]:
    """The options of ``docker compose run`` that make a launch take the slot: the slot's name, and the label that marks the container as this launch's."""
    return ["--name", SLOT_NAME, "--label", f"{OWNER_LABEL}={owner}"]


def local_docker(env: dict[str, str] | None = None) -> Docker:
    """The ``docker`` of this machine: each call runs one command with a deadline."""

    def call(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
        return run_command("docker", arguments, timeout=timeout, env=env)

    return call


def slot_container(docker: Docker, timeout: float = READ_SECONDS) -> SlotContainer | None:
    """The container that has the slot's name in any state, or ``None``. A failed read is a ``SlotError``: a slot that cannot be read is not free."""
    try:
        result = docker(["container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{SLOT_NAME}$", "--format", LISTING], timeout)
    except TimeoutExpired:
        raise SlotError(f"the container of the one Spark slot cannot be read: {timeout_text('docker', timeout)}") from None
    except FileNotFoundError:
        raise SlotError("the container of the one Spark slot cannot be read: docker is not installed or not on the path") from None
    if result.returncode != 0:
        raise SlotError(f"the container of the one Spark slot cannot be read: docker exited {result.returncode}")
    for line in result.stdout.splitlines():
        parts = line.split("|", 2)
        if len(parts) == 3 and parts[0]:
            return SlotContainer(*parts)
    return None


def remove_owned(docker: Docker, owner: str) -> None:
    """Stop and remove the container of this launch, by ID, and read the engine again. Another launch's container is left as it is.

    A ``SlotError`` means that the container of this launch is still there: the slot is not free.
    """
    found = slot_container(docker)
    if found is None or found.owner != owner:
        return
    for words, limit in (
        (["container", "stop", "--time", str(STOP_SECONDS), found.id], STOP_SECONDS + READ_SECONDS),
        (["container", "rm", "--force", found.id], READ_SECONDS),
    ):
        with contextlib.suppress(TimeoutExpired, FileNotFoundError):
            docker(words, limit)
    left = slot_container(docker)
    if left is not None and left.id == found.id:
        raise SlotError(
            f"the container of the one Spark slot ({SLOT_NAME}) is still there after it was stopped and removed: remove it with docker container rm --force"
        )


def lost_the_name(docker: Docker, result: CompletedProcess[str], owner: str) -> bool:
    """Whether a failed launch failed on the slot's name: the daemon's conflict text, and a container of another launch under the name now. A job that prints
    the same words is not taken for a conflict, because no other container has the name then, and the job is never started a second time."""
    if result.returncode == 0 or CONFLICT_TEXT not in result.stderr:
        return False
    found = slot_container(docker)
    return found is not None and found.owner != owner


def holder_text(docker: Docker, timeout: float) -> tuple[str, ...]:
    """The holder of the slot's name as a message word, empty when no container has the name."""
    found = slot_container(docker, timeout)
    return () if found is None else (f"the container {SLOT_NAME} ({found.state})",)


def run_in_slot(
    build: BuildArguments,
    *,
    running: Running,
    deadline_seconds: float,
    env: dict[str, str] | None = None,
    wait_seconds: float = 0,
    poll_seconds: float = POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    docker: Docker | None = None,
) -> CompletedProcess[str]:
    """Start one Spark container in the slot and return its result, or raise ``SlotBusy`` when the slot stays taken for ``wait_seconds``.

    ``build`` returns the ``docker`` arguments of the launch from the slot's options (``slot_options``), which it puts before the service name. ``running``
    reads the live services within the seconds that it is given. A positive wait is a deadline for every read: each read gets the time that is left, a free
    reading after the deadline admits nothing, and each sleep is cut to the time that is left. With no wait the first reading decides.

    When the container passes ``deadline_seconds``, its client is killed by ``run_command`` and this function stops and removes the container of this launch
    before it raises ``TimeoutExpired``. If the container cannot be removed it raises ``SlotError``.
    """
    wait = checked_wait(wait_seconds)
    call = docker or local_docker(env)
    owner = new_owner()
    started = clock()
    holders: tuple[str, ...] = ()
    while True:
        remaining = wait - (clock() - started)
        if wait > 0 and remaining <= 0:
            raise SlotBusy(busy_message(holders) if holders else f"the wait of {wait:g} seconds ended before the slot was read as free")
        read_seconds = READ_SECONDS if wait == 0 else min(READ_SECONDS, remaining)
        holders = spark_holders(running(read_seconds)) or holder_text(call, read_seconds)
        if not holders and wait > 0 and clock() - started > wait:
            raise SlotBusy(f"the wait of {wait:g} seconds ended before the slot was read as free")
        if not holders:
            result = launch(call, build, owner, deadline_seconds)
            if not lost_the_name(call, result, owner):
                return result
            holders = (f"the container {SLOT_NAME}",)
        remaining = wait - (clock() - started)
        if remaining <= 0:
            raise SlotBusy(busy_message(holders))
        sleep(min(poll_seconds, remaining))


def launch(docker: Docker, build: BuildArguments, owner: str, deadline_seconds: float) -> CompletedProcess[str]:
    """Run the launch. Whatever ends it without a result (a deadline, an interrupt, a failure of the program) ends the container of this launch too."""
    try:
        return docker(build(slot_options(owner)), deadline_seconds)
    except TimeoutExpired:
        remove_owned(docker, owner)
        raise
    except BaseException:
        with contextlib.suppress(Exception):
            remove_owned(docker, owner)
        raise
