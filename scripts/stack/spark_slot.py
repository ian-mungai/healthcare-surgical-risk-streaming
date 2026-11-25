"""The one Spark slot: one Spark job runs at a time, whichever of the four Spark groups it belongs to and however it starts.

The host's memory figure assumes the limit of one Spark job (``infra/heavy_memory.tf``), but each Spark workload is its own service group, so the memory check
of one group cannot see another group's job. ``spark_slot`` is the rule that does. It takes the one file lock ``LOCK_FILE``, so two launchers cannot both pass
the check, then asks which Spark services run (the one-off containers of ``docker compose run`` carry their service's name too) and refuses while any does.
The body of the ``with`` block holds the lock, so a launcher that runs the container inside it holds the slot until the container exits, and the lock goes
when the block ends or the process dies. A wait is bounded by a deadline: ``SlotBusy`` names the holders when it ends. The Dagster runs of the Spark groups
share the run tag ``surgical_risk/spark`` (``orchestration/surgical_risk/concurrency.py``), which the run queue limits to one.

The lock is one file for every launcher: ``/run/dagster/spark_slot.lock``, on the memory file system of the code location container. The table checks
one-off and the manual one-off (``scripts/stack/spark_run.py``) both run in that container, so both take the same file. No argument and no environment
variable moves it, because two launchers that name two files do not exclude each other. A host has no such folder, so a launcher on the host fails with
``SlotError`` and never takes a second lock. ``group-up`` starts no Spark group (``scripts/stack/groups.py``), so no launcher outside the container takes
a Spark slot.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import time
from collections.abc import Callable, Collection, Iterator
from pathlib import Path

from scripts.stack.job_workloads import SPARK_SERVICES

LOCK_FILE = Path("/run/dagster/spark_slot.lock")
POLL_SECONDS = 5.0


class SlotError(RuntimeError):
    """The slot cannot be claimed. The message holds the reason and never the contents of a file."""


class SlotBusy(SlotError):
    """The one Spark slot is taken. The message names the Spark services that run or says that another launcher is claiming the slot."""


def spark_holders(running: Collection[str]) -> tuple[str, ...]:
    """The Spark services among the running services, in name order."""
    return tuple(sorted(set(running) & set(SPARK_SERVICES)))


def busy_message(holders: tuple[str, ...]) -> str:
    return f"the one Spark slot is held by {', '.join(holders)}: one Spark job runs at a time" if holders else "another launcher is claiming the one Spark slot"


def open_lock() -> int:
    try:
        return os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as error:
        raise SlotError(
            f"the lock file of the one Spark slot cannot be opened in {LOCK_FILE.parent} ({error.strerror}): run the job in the code location container"
        ) from error


@contextlib.contextmanager
def spark_slot(
    running: Callable[[], Collection[str]],
    *,
    wait_seconds: float = 0,
    poll_seconds: float = POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[None]:
    """Hold the one Spark slot for the body, or raise ``SlotBusy`` when it stays taken for ``wait_seconds``. ``running`` reads the running service names.

    The first reading admits a free slot whatever the wait. A later reading admits only within the wait: each sleep is cut to the time that is left, and a free
    reading that comes after the deadline is refused.
    """
    started = clock()
    descriptor = open_lock()
    try:
        attempts = 0
        while True:
            attempts += 1
            late = False
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                holders: tuple[str, ...] = ()
            else:
                holders = spark_holders(running())
                late = attempts > 1 and clock() - started > wait_seconds
                if not holders and not late:
                    break
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            remaining = wait_seconds - (clock() - started)
            if remaining <= 0:
                raise SlotBusy(f"the wait of {wait_seconds:g} seconds ended before the slot was read as free" if late else busy_message(holders))
            sleep(min(poll_seconds, remaining))
        yield
    finally:
        os.close(descriptor)
