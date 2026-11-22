"""A child process for the signal tests: the probe's real loop, stop flag and end of run, on a fake Docker.

``python -m tests.fast.probe_signal_child <marker folder>`` installs the stop flag, runs ``execute`` with a job that never ends and a cleanup that takes about
two seconds, and writes one marker file for each step: ``ready`` when the loop runs, ``cleanup_started``, ``cleanup_done``, ``record_written`` (holding the
signal count and whether the run was interrupted). It exits 130 when a signal came. The parent test sends it one or two signals.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from pathlib import Path

from scripts.stack.capacity_probe import Budget, Monitor, Reader, Steps, StopFlag, execute
from scripts.stack.probe_docker import ContainerState, Docker
from scripts.stack.probe_record import CleanupStep, JobOutcome, Sample

CLEANUP_SECONDS = 2.0


class Quiet(Docker):
    def state(self, container: str) -> ContainerState:
        return ContainerState("running", 0, False, 0, 0, 0, 0.0)

    def events(self, project: str, since_nano: int, until_nano: int, *, seconds: float) -> list:  # type: ignore[type-arg]
        return []


class NoReader(Reader):
    def __init__(self, folder: Path) -> None:
        self.folder = folder

    def take(self, *, spark_running: bool) -> Sample:
        (self.folder / "ready").write_text("1")
        return Sample(0.0, "t", None, {}, spark_running)


def main(folder: Path) -> int:
    stop = StopFlag()
    stop.install()
    docker = Quiet()
    monitor = Monitor(docker, "p", NoReader(folder), Budget(15.0), stop, 0.2)

    def nothing() -> None:
        return None

    def cleanup() -> list[CleanupStep]:
        (folder / "cleanup_started").write_text("1")
        time.sleep(CLEANUP_SECONDS)
        (folder / "cleanup_done").write_text("1")
        return [CleanupStep("job container", True, "no container remains")]

    def finish(state: ContainerState, watch: object) -> JobOutcome:
        raise AssertionError("the job never ends")

    started: Callable[[Callable[[], None]], None] = lambda hook: None  # noqa: E731
    steps = Steps(started, dict, nothing, nothing, nothing, "job", 15.0, finish, dict, cleanup)
    execution = execute(steps, monitor)
    (folder / "record_written").write_text(f"{stop.count} {execution.interrupted}")
    stop.restore()
    return 130 if stop.requested else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
