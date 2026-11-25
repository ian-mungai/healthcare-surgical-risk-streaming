"""The one Spark slot: one Spark job runs at a time, whichever of the four Spark groups it belongs to and however it starts.

The host's memory figure assumes the limit of one Spark job (``infra/heavy_memory.tf``), but each Spark workload is its own service group, so the memory check
of one group cannot see another group's job. ``spark_slot`` is the rule that does. It takes a file lock, so two launchers on one host cannot both pass the
check, then asks which Spark services run (the one-off containers of ``docker compose run`` carry their service's name too) and refuses while any does. The body
of the ``with`` block holds the lock, so a launcher that runs the container inside it holds the slot until the container exits, and the lock goes when the
block ends or the process dies. A wait is bounded by a deadline: ``SlotBusy`` names the holders when it ends. The Dagster runs of the Spark groups share the run
tag ``surgical_risk/spark`` (``orchestration/surgical_risk/concurrency.py``), which the run queue limits to one.

The lock file is ``SPARK_SLOT_LOCK`` or a file in the host's temporary folder. The check of the running services is across containers, and the file lock
serialises the launchers that share that folder, so a launcher inside a container that does not share it relies on the running-services check and the run tag.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import tempfile
import time
from collections.abc import Callable, Collection, Iterator
from pathlib import Path

from scripts.stack.job_workloads import SPARK_SERVICES

LOCK_VARIABLE = "SPARK_SLOT_LOCK"
LOCK_NAME = "surgical_risk_spark_slot.lock"
POLL_SECONDS = 5.0


class SlotBusy(RuntimeError):
    """The one Spark slot is taken. The message names the Spark services that run or says that another launcher is claiming the slot."""


def spark_holders(running: Collection[str]) -> tuple[str, ...]:
    """The Spark services among the running services, in name order."""
    return tuple(sorted(set(running) & set(SPARK_SERVICES)))


def lock_path() -> Path:
    return Path(os.environ.get(LOCK_VARIABLE) or Path(tempfile.gettempdir()) / LOCK_NAME)


def busy_message(holders: tuple[str, ...]) -> str:
    return f"the one Spark slot is held by {', '.join(holders)}: one Spark job runs at a time" if holders else "another launcher is claiming the one Spark slot"


@contextlib.contextmanager
def spark_slot(
    running: Callable[[], Collection[str]],
    *,
    wait_seconds: float = 0,
    lock_file: Path | None = None,
    poll_seconds: float = POLL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[None]:
    """Hold the one Spark slot for the body, or raise ``SlotBusy`` when it stays taken for ``wait_seconds``. ``running`` reads the running service names."""
    started = clock()
    descriptor = os.open(lock_file or lock_path(), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                holders: tuple[str, ...] = ()
            else:
                holders = spark_holders(running())
                if not holders:
                    break
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            if clock() - started >= wait_seconds:
                raise SlotBusy(busy_message(holders))
            sleep(poll_seconds)
        yield
    finally:
        os.close(descriptor)
