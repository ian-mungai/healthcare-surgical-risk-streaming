"""The capacity sampler: one JSON line for each iteration, as read, and nothing else.

``.venv/bin/python -m scripts.stack.capacity_sampler --output <new file> --interval-seconds <n> --max-seconds <n>`` starts nothing, launches nothing,
cleans up nothing and derives nothing. The owner or the lead starts the always-on group and the Spark job with the existing stack commands in one
terminal and runs the sampler in another, from before the group starts until after the job ends. The sampler never compares a figure with a limit and
never states a judgment. The owner reads the file.

The file starts with a ``header`` line (the schema, the start time, the interval, the files read and the configured limits copied from
``compose/limits.yml``) and ends with an ``end`` line (the reason, ``stop_flag`` or ``max_seconds``, and the count of ``sample`` lines written). A
sample line holds the readings of one iteration, each with the wall and monotonic times taken immediately before and after it: ``host`` (the raw
counters of the Linux host), ``containers`` (the rows of ``docker ps --all`` as read) and ``memory`` (the rows of ``docker stats --no-stream`` as
read, with their parsed bytes). A reading that fails is named in ``missing`` by a fixed code and a fixed cause and its key is absent from the line.
``seq`` counts the iterations from 0. Each line is written to the file and synced before the next reading starts.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command
from scripts.stack.host_figures import FiguresError, HostSources, parse_size_bytes, read_host
from scripts.stack.limits import LIMITS_FILE, LimitsError, load_limits

SCHEMA = 5
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_RUN_SECONDS = 3600.0
MAX_INTERVAL_SECONDS = 3600.0
MAX_RUN_SECONDS = 604800.0
SLEEP_SLICE_SECONDS = 0.25
MIN_CALL_SECONDS = 0.01
READINGS = ("host", "containers", "memory")
CAUSES = ("timeout", "exit_status", "unparsable", "os_error")
EXIT_NOTE = "The exit status says nothing about capacity: it is 0 when the end line was written. A file with no end line was cut off."
STATS_ROW = re.compile(r"([^|]+)\|(\S+) / (\S+)")
Runner = Callable[..., Any]
Clock = Callable[[], float]


class SamplerError(RuntimeError):
    """The output cannot be used or written. The message is fixed and holds no text from a command."""


class Failure(Exception):
    """One reading failed, with one of the fixed causes."""

    def __init__(self, cause: str) -> None:
        super().__init__(cause)
        self.cause = cause


class StopFlag:
    """A SIGINT or SIGTERM only counts. It never raises, so a signal cannot end a reading or a write half way."""

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

    def sleep(self, seconds: float, *, pause: Callable[[float], None] = time.sleep) -> None:
        """Wait in short slices and stop waiting when a signal came."""
        left = seconds
        while left > 0 and not self.requested:
            pause(min(SLEEP_SLICE_SECONDS, left))
            left -= SLEEP_SLICE_SECONDS


def emit(text: str, stream: Any = None) -> None:
    """Print one line. A standard output that is closed or broken never stops the sampler."""
    try:
        target = stream or sys.stdout
        target.write(text + "\n")
        target.flush()
    except OSError, ValueError, AttributeError:
        return


def check_output(path: Path) -> None:
    """Refuse an output path that exists or is a link, and a parent that is not a folder (a link that leads nowhere is not one). It creates nothing."""
    if os.path.lexists(path):
        raise SamplerError("the output path exists")
    if not path.parent.is_dir():
        raise SamplerError("the parent of the output path is not a folder")


class Output:
    """The output file: opened once for append, for its owner only, and every line is written and synced before the call returns."""

    def __init__(self, path: Path) -> None:
        try:
            self.fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_EXCL, 0o600)
        except OSError as error:
            raise SamplerError("the output path cannot be created") from error

    def write_line(self, line: dict[str, Any]) -> None:
        data = (json.dumps(line, separators=(",", ":"), allow_nan=False) + "\n").encode()
        try:
            while data:
                data = data[os.write(self.fd, data) :]
            os.fsync(self.fd)
        except OSError as error:
            raise SamplerError("cannot append to the output file") from error

    def close(self) -> None:
        os.close(self.fd)


def cause_of(error: BaseException) -> str:
    if isinstance(error, Failure):
        return error.cause
    if isinstance(error, TimeoutExpired):
        return "timeout"
    if isinstance(error, FiguresError):
        return "os_error" if isinstance(error.__cause__, OSError) else "unparsable"
    return "os_error"


def timed(clock: Clock, wall_ns: Callable[[], int], read: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """The reading with the wall and monotonic times taken immediately before and after it. A failure becomes a ``Failure`` with its cause."""
    before = (wall_ns(), clock())
    try:
        body = read()
    except (Failure, TimeoutExpired, FiguresError, OSError, RuntimeError) as error:
        raise Failure(cause_of(error)) from error
    after = (wall_ns(), clock())
    return {"wall_ns_before": before[0], "wall_ns_after": after[0], "mono_before": before[1], "mono_after": after[1], **body}


def host_body(sources: HostSources) -> dict[str, Any]:
    reading = read_host(sources)
    memory = reading.memory
    return {
        "memory_kib": {"total": memory.total_kib, "available": memory.available_kib, "swap_total": memory.swap_total_kib, "swap_free": memory.swap_free_kib},
        "cpu": {name: list(times.counts) for name, times in reading.cpu.items()},
        "disk": {"read_bytes": reading.disk.read_bytes, "written_bytes": reading.disk.written_bytes},
        "free_disk_mib": reading.free_disk_mib,
    }


def docker_text(runner: Runner, arguments: list[str], timeout: float) -> str:
    if timeout < MIN_CALL_SECONDS:
        raise Failure("timeout")
    result = runner("docker", arguments, timeout=timeout)
    if result.returncode != 0:
        raise Failure("exit_status")
    return str(result.stdout)


def containers_body(runner: Runner, timeout: float) -> dict[str, Any]:
    rows = []
    for line in docker_text(runner, ["ps", "--all", "--format", '{{.Names}}|{{.State}}|{{.Label "com.docker.compose.project"}}'], timeout).splitlines():
        parts = line.split("|")
        if len(parts) != 3 or not parts[0]:
            raise Failure("unparsable")
        rows.append({"name": parts[0], "state": parts[1], "project": parts[2]})
    return {"rows": rows}


def memory_body(runner: Runner, timeout: float) -> dict[str, Any]:
    rows = []
    for line in docker_text(runner, ["stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"], timeout).splitlines():
        match = STATS_ROW.fullmatch(line.strip())
        if match is None:
            raise Failure("unparsable")
        name, usage, limit = match.groups()
        try:
            rows.append(
                {"name": name, "usage_text": usage, "limit_text": limit, "usage_bytes": parse_size_bytes(usage), "limit_bytes": parse_size_bytes(limit)}
            )
        except FiguresError as error:
            raise Failure("unparsable") from error
    return {"rows": rows}


def header_line(interval: float, max_seconds: float, sources: HostSources, limits_path: Path, wall_ns: Callable[[], int]) -> dict[str, Any]:
    limits = load_limits(limits_path)
    return {
        "kind": "header",
        "schema": SCHEMA,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "started_wall_ns": wall_ns(),
        "interval_seconds": interval,
        "max_seconds": max_seconds,
        "sources": {"memory": str(sources.meminfo), "cpu": str(sources.stat), "disk": str(sources.diskstats), "free_disk": str(sources.data_path)},
        "configured": {
            "host_reserve_mib": limits.host_reserve_mib,
            "limits": {
                name: {"runtime": entry.runtime, "group": entry.group, "memory_limit_mib": entry.memory_limit_mib} for name, entry in limits.services.items()
            },
        },
    }


def sample_line(seq: int, began: float, interval: float, sources: HostSources, runner: Runner, clock: Clock, wall_ns: Callable[[], int]) -> dict[str, Any]:
    """The line of one iteration. A reading that fails is named in ``missing`` and its key is absent. Each docker call gets what is left of the interval."""
    line: dict[str, Any] = {"kind": "sample", "seq": seq}
    missing: list[dict[str, str]] = []

    def left() -> float:
        return min(interval, interval - (clock() - began))

    readers = (
        ("host", lambda: host_body(sources)),
        ("containers", lambda: containers_body(runner, left())),
        ("memory", lambda: memory_body(runner, left())),
    )
    for name, read in readers:
        try:
            line[name] = timed(clock, wall_ns, read)
        except Failure as failure:
            missing.append({"reading": name, "cause": failure.cause})
    line["missing"] = missing
    return line


def run_sampler(
    output: Path,
    *,
    interval_seconds: float,
    max_seconds: float,
    sources: HostSources = HostSources(),  # noqa: B008
    limits_path: Path = LIMITS_FILE,
    runner: Runner = run_command,
    stop: StopFlag | None = None,
    clock: Clock = time.monotonic,
    wall_ns: Callable[[], int] = time.time_ns,
    pause: Callable[[float], None] = time.sleep,
    say: Callable[[str], None] = emit,
) -> int:
    """Write the header, one line for each iteration and the end line. Return 0 when the end line was written."""
    flag = stop or StopFlag()
    try:
        check_output(output)
        header = header_line(interval_seconds, max_seconds, sources, limits_path, wall_ns)
        out = Output(output)
    except SamplerError as error:
        say_error(str(error))
        return 1
    except LimitsError:
        say_error("the configured limits cannot be read")
        return 1
    seq, reason = 0, ""
    try:
        out.write_line(header)
        started = clock()
        while True:
            if flag.requested:
                reason = "stop_flag"
                break
            if clock() - started >= max_seconds:
                reason = "max_seconds"
                break
            began = clock()
            out.write_line(sample_line(seq, began, interval_seconds, sources, runner, clock, wall_ns))
            seq += 1
            flag.sleep(max(0.0, interval_seconds - (clock() - began)), pause=pause)
        out.write_line({"kind": "end", "reason": reason, "samples": seq})
    except SamplerError as error:
        say_error(str(error))
        return 1
    finally:
        out.close()
    say(f"sampler: end line written after {seq} samples ({reason})")
    return 0


def say_error(detail: str) -> None:
    emit(f"capacity sampler: {detail}", sys.stderr)


def bounded_seconds(top: float) -> Callable[[str], float]:
    def parse(text: str) -> float:
        try:
            value = float(text)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"{text!r} is not a number") from error
        if not math.isfinite(value) or not 0 < value <= top:
            raise argparse.ArgumentTypeError(f"{text!r} must be a finite number above 0 and at most {top:g}")
        return value

    return parse


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        prog="python -m scripts.stack.capacity_sampler", description=__doc__.split("\n\n")[0] if __doc__ else None, epilog=EXIT_NOTE
    )
    command.add_argument("--output", type=Path, required=True, help="a new file for the lines; an existing path, a link or a dangling link above it is refused")
    command.add_argument(
        "--interval-seconds", type=bounded_seconds(MAX_INTERVAL_SECONDS), default=DEFAULT_INTERVAL_SECONDS, help="seconds between two iterations"
    )
    command.add_argument("--max-seconds", type=bounded_seconds(MAX_RUN_SECONDS), default=DEFAULT_RUN_SECONDS, help="how long to sample at the most")
    command.add_argument("--data-path", type=Path, default=Path("/var/lib/docker"), help="a folder on the data disk, for its free space and its disk counters")
    return command


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    stop = StopFlag()
    stop.install()
    try:
        return run_sampler(
            arguments.output,
            interval_seconds=arguments.interval_seconds,
            max_seconds=arguments.max_seconds,
            sources=HostSources(data_path=arguments.data_path),
            stop=stop,
        )
    finally:
        stop.restore()


if __name__ == "__main__":
    sys.exit(main())
