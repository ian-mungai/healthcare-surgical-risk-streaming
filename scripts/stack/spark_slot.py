"""The one Spark slot: one Spark job runs at a time on a Docker engine, whichever of the four Spark groups it belongs to and however it starts.

The host's memory figure assumes the limit of one Spark job (``infra/heavy_memory.tf``), but each Spark workload is its own service group, so the memory check
of one group cannot see another group's job. The slot is a container name that Docker itself keeps unique. Every supported launcher starts its Spark container
with ``--name SLOT_NAME`` (Docker: https://docs.docker.com/reference/cli/docker/container/run/#name and, for a one-off of a Compose service,
https://docs.docker.com/reference/cli/docker/compose/run/), so a second launch fails on the name and is reported as the slot being busy. No lock file stands
between the check and the start: the engine decides, once, when it creates the container.

The slot is held exactly as long as the container exists. A client that is killed, a restart of the code location container and a ``docker pause`` of the
job change nothing, because the container is the holder and not the process that started it. No launch uses ``--rm``: every launcher ends through ``run_owned``,
the one cleanup path, which after success, failure, a deadline or an interrupt stops and removes only its own container (the one that carries its own
``OWNER_LABEL`` value, by container ID), reads the engine again and reports a container that survives with its ID and state. A container with the slot's name
that another launch started is never touched. A container that is left over (a launcher that was killed, or a removal that failed) keeps the name, so the slot
stays busy: remove that container by hand.

The container that stays after its job is also how a launcher knows whether the engine created it. A launch counts as refused at its create step only when the
command exited 1, wrote nothing on standard output, wrote the engine's whole refusal of the slot's name (``NAME_REFUSAL``) and the engine holds no container of
this launch under the name. A job whose container was created is therefore never started a second time, whatever it printed.

Before it starts a container, a launcher also reads which Spark services of its project are alive (running, paused or restarting): a service container that
does not carry the slot's name, such as one of an older version, holds the slot too. A wait is bounded by a deadline that every read before a start obeys: the
time that is left is computed before each one, none starts at or after the end of the wait, and a free reading at or after it admits nothing. The Dagster runs
of the Spark groups share the run tag ``surgical_risk/spark`` (``orchestration/surgical_risk/concurrency.py``), which the run queue limits to one.
``group-up`` starts no Spark group (``scripts/stack/groups.py``).
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import math
import re
import sys
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
# The whole refusal of Docker Engine 29.9.3 for the slot's name (``nameConflictError``), which Compose passes on from the create step of a one-off, and
# Compose's exit status then: https://github.com/moby/moby/blob/docker-v29.9.3/daemon/errors.go#L55-L63
# and https://github.com/docker/compose/blob/v5.8.0/pkg/compose/run.go#L197-L205
NAME_REFUSAL = re.compile(
    rf'Error response from daemon: Conflict\. The container name "/{SLOT_NAME}" is already in use by container "[0-9a-f]{{64}}"\. '
    r"You have to remove \(or rename\) that container to be able to reuse that name\."
)
REFUSED_STATUS = 1
LIVE_STATES = frozenset({"running", "paused", "restarting"})
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


def require_free(docker: Docker, timeout: float = READ_SECONDS) -> None:
    """Raise ``SlotBusy``, with the container's ID and state, while any container has the slot's name. No launch or retry starts while one does."""
    held = slot_container(docker, timeout)
    if held is not None:
        raise SlotBusy(f"the one Spark slot is held by the container {SLOT_NAME}, ID {held.id} ({held.state}): one Spark job runs at a time")


def clear_owned(docker: Docker, owner: str, seen: list[str] | None = None) -> str | None:
    """Stop and remove the container of this launch, by ID, and read the engine again. Another launch's container is left as it is.

    Return ``None`` when no container of this launch is left, else the report of the one that is, or may be, with its ID and state: the slot is not free.
    The report holds fixed words, the slot's name and the engine's ID and state only. The ID of a container of this launch that a read finds goes into ``seen``.
    """
    try:
        found = slot_container(docker)
    except SlotError:
        return (
            f"the container of the one Spark slot cannot be read: this launch's container may still hold the slot ({SLOT_NAME}): "
            "check it with docker container ls --all"
        )
    if found is None or found.owner != owner:
        return None
    if seen is not None:
        seen.append(found.id)
    stop = (["container", "stop", "--time", str(STOP_SECONDS), found.id], STOP_SECONDS + READ_SECONDS)
    remove = (["container", "rm", "--force", "--volumes", found.id], READ_SECONDS)
    for words, limit in (stop, remove) if found.state in LIVE_STATES else (remove,):
        # A command that fails in any way is judged by the next read, which names what is left.
        with contextlib.suppress(Exception):
            docker(words, limit)
    try:
        left = slot_container(docker)
    except SlotError:
        return (
            f"the container of the one Spark slot cannot be read after its removal: container {found.id} (last read {found.state}) may still hold the slot "
            f"({SLOT_NAME}): check it with docker container ls --all"
        )
    if left is None or left.id != found.id:
        return None
    return (
        f"the container of the one Spark slot ({SLOT_NAME}) is still there after it was stopped and removed: container {left.id} ({left.state}); "
        f"remove it with docker container rm --force {left.id}"
    )


def remove_owned(docker: Docker, owner: str) -> None:
    """``settle_owned``, or the ``SlotError`` that holds its report."""
    report = settle_owned(docker, owner)
    if report is not None:
        raise SlotError(report)


def end_owned(docker: Docker, owner: str, seen: list[str] | None = None) -> None:
    """``settle_owned`` for an ending that already has a result or an error: its report goes to standard error and hides neither."""
    report = settle_owned(docker, owner, seen)
    if report is not None:
        sys.stderr.write(f"spark_slot: {report}\n")


def settle_owned(docker: Docker, owner: str, seen: list[str] | None = None) -> str | None:
    """``clear_owned`` for the end of a launch. A cleanup that is cut short is run once more, so one interrupt during it skips no removal; the interrupt is
    raised after the report is written. A failure of the cleanup itself is a report and never an exception."""
    interrupt: BaseException | None = None
    report: str | None = f"the cleanup did not finish: the one Spark slot ({SLOT_NAME}) may still be held; check it with docker container ls --all"
    for _ in range(2):
        try:
            report = clear_owned(docker, owner, seen)
            break
        except BaseException as error:
            if not isinstance(error, Exception):
                interrupt = interrupt or error
    if interrupt is not None:
        if report is not None:
            sys.stderr.write(f"spark_slot: {report}\n")
        raise interrupt
    return report


def refused_by_name(result: CompletedProcess[str]) -> bool:
    """Whether the command's own result can be the engine's refusal to create the slot's container: its exit status, no output and the whole refusal."""
    return result.returncode == REFUSED_STATUS and not result.stdout and NAME_REFUSAL.search(result.stderr) is not None


def lost_the_name(docker: Docker, result: CompletedProcess[str], owner: str, read_seconds: Callable[[], float] = lambda: READ_SECONDS) -> bool:
    """Whether the engine refused to create this launch's container: ``refused_by_name``, and the engine holds no container of this launch under the name.
    A created container stays until ``run_owned`` removes it, so its job is never taken for a refusal. The holder that won can be gone by this read."""
    if not refused_by_name(result):
        return False
    found = slot_container(docker, read_seconds())
    return found is None or found.owner != owner


def run_owned(
    docker: Docker, owner: str, start: Callable[[], CompletedProcess[str]], read_seconds: Callable[[], float] = lambda: READ_SECONDS
) -> tuple[CompletedProcess[str], bool]:
    """The one launch and cleanup path of every launcher: run ``start``, and whatever ends it, end the container of this launch.

    Return the result and whether the engine refused the create (no job ran). A deadline is raised only when the container is gone, else a ``SlotError``
    names it. An interrupt or any other failure is raised as it is, after a surviving container is reported. When the owner check of a refusal cannot run (the
    wait is used up or the read fails), the result is the job's own if the cleanup finds this launch's container, else the check's error is raised.
    """
    unchecked: SlotError | None = None
    try:
        result = start()
        try:
            if lost_the_name(docker, result, owner, read_seconds):
                return result, True
        except SlotError as error:
            unchecked = error
    except TimeoutExpired:
        report = settle_owned(docker, owner)
        if report is not None:
            raise SlotError(report) from None
        raise
    except BaseException:
        end_owned(docker, owner)
        raise
    seen: list[str] = []
    end_owned(docker, owner, seen)
    if unchecked is not None and not seen:
        raise unchecked
    return result, False


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
    reads the live services within the seconds that it is given. A positive wait is a deadline for every read before a start: each read gets the time that is
    left, none starts at or after the end of the wait, a free reading at or after it admits nothing, and each sleep is cut to the time that is left. A launch's
    arguments are built before the last clock check: no launch is dispatched at or after the end of the wait. With no wait the first reading decides.

    A refused create is tried again only within the wait that is left. A job whose container was created is run once. ``run_owned`` ends the container: when
    it passes ``deadline_seconds``, its client is killed by ``run_command`` and it is stopped and removed before ``TimeoutExpired`` is raised, or a
    ``SlotError`` names it.
    """
    wait = checked_wait(wait_seconds)
    call = docker or local_docker(env)
    owner = new_owner()
    started = clock()
    holders: tuple[str, ...] = ()

    def read_seconds() -> float:
        """The seconds of the next admission read: what is left of the wait and at most ``READ_SECONDS``. No read starts at or after the end of the wait."""
        if wait == 0:
            return READ_SECONDS
        left = wait - (clock() - started)
        if left <= 0:
            raise SlotBusy(busy_message(holders) if holders else f"the wait of {wait:g} seconds ended before the slot was read as free")
        return min(READ_SECONDS, left)

    while True:
        holders = spark_holders(running(read_seconds())) or holder_text(call, read_seconds())
        if not holders:
            arguments = build(slot_options(owner))
            if wait > 0 and clock() - started >= wait:
                raise SlotBusy(f"the wait of {wait:g} seconds ended before the slot was read as free")
            result, refused = run_owned(call, owner, functools.partial(call, arguments, deadline_seconds), read_seconds)
            if not refused:
                return result
            holders = (f"the container {SLOT_NAME}",)
        remaining = wait - (clock() - started)
        if remaining <= 0:
            raise SlotBusy(busy_message(holders))
        sleep(min(poll_seconds, remaining))
