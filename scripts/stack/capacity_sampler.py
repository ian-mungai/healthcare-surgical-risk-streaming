"""The capacity sampler: one JSON line for each iteration, as read, and nothing else.

``.venv/bin/python -m scripts.stack.capacity_sampler --output <new file> --interval-seconds <n> --max-seconds <n>`` starts no service and no job (the
only programs it runs are the docker readings and, for a staging source, the read-only walk below), cleans up nothing and derives nothing. The owner or
the lead starts the always-on group and the Spark job with the existing stack commands in one terminal and runs the sampler in another, from before the
group starts until after the job ends. The sampler never compares a figure with a limit and never states a judgment. The owner reads the file.

The file starts with a ``header`` line (the schema, the start time, the interval, the files read and the configured limits copied from
``compose/limits.yml``) and ends with an ``end`` line (the reason, ``stop_flag`` or ``max_seconds``, and the count of ``sample`` lines written). A
sample line holds the readings of one iteration, each with the wall and monotonic times taken immediately before and after it: ``host`` (the raw
counters of the Linux host), ``containers`` (the rows of ``docker ps --all`` as read), ``memory`` (the rows of ``docker stats --no-stream`` as
read, with their parsed bytes) and, when ``--staging NAME=PATTERN`` is given, ``staging`` (for each named pattern the number of matches and files and the
bytes of the files under the matches, apparent and allocated, as read). A reading that fails is named in ``missing`` by a fixed code and a fixed cause and
its key is absent from the line. ``seq`` counts the iterations from 0.

A staging pattern is a shell pattern of absolute paths, for example ``/var/lib/docker/volumes/*staging/_data``. Each source is read by a child program,
``scripts.stack.staging_walk`` (its docstring holds the rules of the walk), that the sampler starts through the project's one launcher and kills at the end
of the reading's interval, so a slow folder cannot hold the loop. A child that is killed is a ``timeout``, a folder it may not read is an ``os_error``, a
failed child is an ``exit_status`` and an answer that is not four whole numbers is ``unparsable``; each is named in ``missing`` and is never read as a zero.
The sources of one sample share the one deadline. The sampler reads and changes nothing in those folders. A pattern with no match is a row of zeros: the
volume does not exist yet. Start the sampler with an interval of at least one second, because each child needs the time of a program start.

Each reading is one function whose whole body (start, wait, decode, parse) runs inside one boundary, and each has a deadline that is a point fixed before
it starts (the end of its interval). A reading that ends after its deadline is a ``timeout`` whatever it returned, and its key is absent. The docker
program is found once at the start: the sampler refuses to start, before it creates the file, if it cannot find it. The wait between samples is never
longer than the time left in the run. Each line is one ``os.write`` call, synced before the next reading starts. A write that takes fewer bytes than the
line is an append failure: the sampler stops with a non-zero status and writes nothing more. So a file with no end line, or whose last line is
incomplete, was cut off. Nothing is printed to standard output, so a standard output that is closed or broken cannot change the exit status. Messages
go to standard error.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ingestion.closed import QuietParser
from scripts.process import ProcessGroupSurvived, TimeoutExpired, run_command
from scripts.stack.host_figures import FiguresError, HostSources, parse_size_bytes, read_host
from scripts.stack.limits import LIMITS_FILE, LimitsError, load_limits
from scripts.stack.staging_walk import EXIT_OS_ERROR

SCHEMA = 6
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_RUN_SECONDS = 3600.0
MAX_INTERVAL_SECONDS = 3600.0
MAX_RUN_SECONDS = 604800.0
SLEEP_SLICE_SECONDS = 0.25
MIN_CALL_SECONDS = 0.01
CAUSES = ("timeout", "exit_status", "unparsable", "os_error")
EXIT_NOTE = (
    "The exit status says nothing about capacity: it is 0 when the end line was written. "
    "A file with no end line, or whose last line is incomplete, was cut off. Messages go to standard error and nothing is printed to standard output."
)
STATS_ROW = re.compile(r"([^|]+)\|(\S+) / (\S+)")
STAGING_NAME = re.compile(r"[a-z][a-z0-9_]{0,31}")
STAGING_FORM = "a staging source is NAME=PATTERN: a lowercase name of up to 32 characters and an absolute path pattern with no control character"
ROOT = Path(__file__).resolve().parents[2]
STAGING_PROGRAM = (sys.executable, "-B", "-m", "scripts.stack.staging_walk")
STAGING_FIGURES = ("matches", "files", "apparent_bytes", "allocated_bytes")
Runner = Callable[..., Any]
Clock = Callable[[], float]


class SamplerError(RuntimeError):
    """The output cannot be used or written. The message is fixed and holds no text from a command."""


@dataclass(frozen=True)
class StagingSource:
    """A named place that holds staged files: a shell pattern of absolute paths."""

    name: str
    pattern: str


def parse_staging(text: str) -> StagingSource:
    """The source of one ``NAME=PATTERN`` argument. The message of a refusal holds no word of the argument."""
    name, separator, pattern = text.partition("=")
    if not separator or STAGING_NAME.fullmatch(name) is None or not pattern.startswith("/") or any(ord(character) < 32 for character in pattern):
        raise SamplerError(STAGING_FORM)
    return StagingSource(name, pattern)


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

    def sleep(self, seconds: float, *, pause: Callable[[float], None] = time.sleep, clock: Clock = time.monotonic) -> None:
        """Wait in short slices until a monotonic end, and stop waiting when a signal came. A pause that overran is never followed by another."""
        end = clock() + seconds
        while not self.requested and (left := end - clock()) > 0:
            pause(min(SLEEP_SLICE_SECONDS, left))


def emit(text: str) -> None:
    """Print one line to standard error. The sampler prints nothing to standard output, and a standard error that is closed or broken never stops it."""
    try:
        sys.stderr.write(text + "\n")
        sys.stderr.flush()
    except OSError, ValueError, AttributeError:
        return


def check_output(path: Path) -> None:
    """Refuse an output path that exists or is a link, and a parent that is not a folder (a link that leads nowhere is not one). It creates nothing."""
    if os.path.lexists(path):
        raise SamplerError("the output path exists")
    if not path.parent.is_dir():
        raise SamplerError("the parent of the output path is not a folder")


class Output:
    """The output file: opened once for append, for its owner only. A line is one write, synced before the call returns. A short write is a failure."""

    def __init__(self, path: Path) -> None:
        try:
            self.fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_EXCL, 0o600)
        except OSError as error:
            raise SamplerError("the output path cannot be created") from error

    def write_line(self, line: dict[str, Any]) -> None:
        data = (json.dumps(line, separators=(",", ":"), allow_nan=False) + "\n").encode()
        try:
            if os.write(self.fd, data) != len(data):
                raise SamplerError("cannot append to the output file")
            os.fsync(self.fd)
        except OSError as error:
            raise SamplerError("cannot append to the output file") from error

    def close(self) -> None:
        os.close(self.fd)


def cause_of(error: Exception) -> str:
    """The fixed cause of an exception raised inside a reading.

    ``OSError`` (a missing program, no more descriptors, a counter file that cannot be read, also wrapped in ``FiguresError``) and ``ProcessGroupSurvived``
    are ``os_error``. ``TimeoutExpired`` is ``timeout``. A line that is not in its form (``FiguresError``), a strict decoding error
    (``UnicodeDecodeError``) and a number that cannot be parsed (``ValueError``, ``ArithmeticError``) are ``unparsable``. A non-zero status is
    ``exit_status`` (a ``Failure`` raised by the reading). Any other type is a programming error: it is named ``unparsable`` as well, so that the line is
    still written for the readings that succeeded and the loop goes on.
    """
    if isinstance(error, Failure):
        return error.cause
    if isinstance(error, TimeoutExpired):
        return "timeout"
    if isinstance(error, ProcessGroupSurvived):
        return "os_error"
    if isinstance(error, FiguresError):
        return "os_error" if isinstance(error.__cause__, OSError) else "unparsable"
    if isinstance(error, OSError):
        return "os_error"
    return "unparsable"


def timed(clock: Clock, wall_ns: Callable[[], int], deadline: float, read: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """One reading with the wall and monotonic times taken immediately before and after it. Its whole body (start, wait, decode, parse) runs inside.

    Every exception becomes a ``Failure`` with its cause. A reading whose ``mono_after`` is past ``deadline``, a point fixed before it started, is a
    ``timeout`` whatever it returned and whatever failure it ended with.
    """
    before = (wall_ns(), clock())
    failure: Exception | None = None
    body: dict[str, Any] = {}
    try:
        body = read()
    except Exception as error:  # the boundary: no exception ends the loop or drops the line
        failure = error
    after = (wall_ns(), clock())
    if after[1] > deadline:
        raise Failure("timeout") from failure
    if failure is not None:
        raise Failure(cause_of(failure)) from failure
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


def docker_text(runner: Runner, program: str, arguments: list[str], deadline: float, clock: Clock) -> str:
    """The standard output of one docker call, decoded strictly by the launcher. The call gets what is left until ``deadline``."""
    left = deadline - clock()
    if left < MIN_CALL_SECONDS:
        raise Failure("timeout")
    result = runner(program, arguments, timeout=left)
    if result.returncode != 0:
        raise Failure("exit_status")
    return str(result.stdout)


def containers_body(runner: Runner, program: str, deadline: float, clock: Clock) -> dict[str, Any]:
    rows = []
    arguments = ["ps", "--all", "--format", '{{.Names}}|{{.State}}|{{.Label "com.docker.compose.project"}}']
    for line in docker_text(runner, program, arguments, deadline, clock).splitlines():
        parts = line.split("|")
        if len(parts) != 3 or not parts[0]:
            raise Failure("unparsable")
        rows.append({"name": parts[0], "state": parts[1], "project": parts[2]})
    return {"rows": rows}


def memory_body(runner: Runner, program: str, deadline: float, clock: Clock) -> dict[str, Any]:
    rows = []
    for line in docker_text(runner, program, ["stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}"], deadline, clock).splitlines():
        match = STATS_ROW.fullmatch(line.strip())
        if match is None:
            raise Failure("unparsable")
        name, usage, limit = match.groups()
        rows.append({"name": name, "usage_text": usage, "limit_text": limit, "usage_bytes": parse_size_bytes(usage), "limit_bytes": parse_size_bytes(limit)})
    return {"rows": rows}


def staging_row(source: StagingSource, deadline: float, clock: Clock, runner: Runner, program: Sequence[str]) -> dict[str, Any]:
    """The row of one source, from the child program that reads it. The child gets what is left until ``deadline`` and is killed when that is over."""
    left = deadline - clock()
    if left < MIN_CALL_SECONDS:
        raise Failure("timeout")
    result = runner(program[0], [*program[1:], source.pattern], cwd=ROOT, timeout=left)
    if result.returncode == EXIT_OS_ERROR:
        raise Failure("os_error")
    if result.returncode != 0:
        raise Failure("exit_status")
    try:
        figures = json.loads(result.stdout)
    except ValueError:
        raise Failure("unparsable") from None
    if type(figures) is not dict or list(figures) != list(STAGING_FIGURES) or any(type(value) is not int or value < 0 for value in figures.values()):
        raise Failure("unparsable")
    return {"name": source.name, **figures}


def staging_body(sources: Sequence[StagingSource], deadline: float, clock: Clock, runner: Runner, program: Sequence[str]) -> dict[str, Any]:
    return {"rows": [staging_row(source, deadline, clock, runner, program) for source in sources]}


def header_line(
    interval: float, max_seconds: float, sources: HostSources, limits_path: Path, wall_ns: Callable[[], int], staging: Sequence[StagingSource] = ()
) -> dict[str, Any]:
    limits = load_limits(limits_path)
    return {
        "kind": "header",
        "schema": SCHEMA,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "started_wall_ns": wall_ns(),
        "interval_seconds": interval,
        "max_seconds": max_seconds,
        "sources": {
            "memory": str(sources.meminfo),
            "cpu": str(sources.stat),
            "disk": str(sources.diskstats),
            "free_disk": str(sources.data_path),
            "limits": str(limits_path),
            **({"staging": {source.name: source.pattern for source in staging}} if staging else {}),
        },
        "configured": {
            "host_reserve_mib": limits.host_reserve_mib,
            "limits": {
                name: {"runtime": entry.runtime, "group": entry.group, "memory_limit_mib": entry.memory_limit_mib} for name, entry in limits.services.items()
            },
        },
    }


def sample_line(
    seq: int,
    deadline: float,
    sources: HostSources,
    runner: Runner,
    program: str,
    clock: Clock,
    wall_ns: Callable[[], int],
    staging: Sequence[StagingSource] = (),
    staging_runner: Runner = run_command,
    staging_program: Sequence[str] = STAGING_PROGRAM,
) -> dict[str, Any]:
    """The line of one iteration. A reading that fails is named in ``missing`` and its key is absent. All readings share one deadline, fixed first."""
    line: dict[str, Any] = {"kind": "sample", "seq": seq}
    missing: list[dict[str, str]] = []
    readers: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("host", lambda: host_body(sources)),
        ("containers", lambda: containers_body(runner, program, deadline, clock)),
        ("memory", lambda: memory_body(runner, program, deadline, clock)),
    ]
    if staging:
        readers.append(("staging", lambda: staging_body(staging, deadline, clock, staging_runner, staging_program)))
    for name, read in readers:
        try:
            line[name] = timed(clock, wall_ns, deadline, read)
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
    docker: str | None = None,
    stop: StopFlag | None = None,
    clock: Clock = time.monotonic,
    wall_ns: Callable[[], int] = time.time_ns,
    pause: Callable[[float], None] = time.sleep,
    say: Callable[[str], None] = emit,
    staging: Sequence[StagingSource] = (),
    staging_runner: Runner = run_command,
    staging_program: Sequence[str] = STAGING_PROGRAM,
) -> int:
    """Write the header, one line for each iteration and the end line. Return 0 when the end line was written."""
    flag = stop or StopFlag()
    program = docker or shutil.which("docker")
    try:
        check_output(output)
        if program is None:
            raise SamplerError("the docker program cannot be found")
        if len({source.name for source in staging}) != len(staging):
            raise SamplerError("two staging sources have one name")
        header = header_line(interval_seconds, max_seconds, sources, limits_path, wall_ns, staging)
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
            out.write_line(sample_line(seq, began + interval_seconds, sources, runner, program, clock, wall_ns, staging, staging_runner, staging_program))
            seq += 1
            flag.sleep(max(0.0, min(interval_seconds - (clock() - began), max_seconds - (clock() - started))), pause=pause, clock=clock)
        out.write_line({"kind": "end", "reason": reason, "samples": seq})
    except SamplerError as error:
        say_error(str(error))
        return 1
    finally:
        out.close()
    say(f"sampler: end line written after {seq} samples ({reason})")
    return 0


def say_error(detail: str) -> None:
    emit(f"capacity sampler: {detail}")


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


def staging_argument(text: str) -> StagingSource:
    try:
        return parse_staging(text)
    except SamplerError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog="python -m scripts.stack.capacity_sampler", description=__doc__.split("\n\n")[0] if __doc__ else None, epilog=EXIT_NOTE)
    command.add_argument("--output", type=Path, required=True, help="a new file for the lines; an existing path, a link or a dangling link above it is refused")
    command.add_argument(
        "--interval-seconds", type=bounded_seconds(MAX_INTERVAL_SECONDS), default=DEFAULT_INTERVAL_SECONDS, help="seconds between two iterations"
    )
    command.add_argument("--max-seconds", type=bounded_seconds(MAX_RUN_SECONDS), default=DEFAULT_RUN_SECONDS, help="how long to sample at the most")
    command.add_argument(
        "--staging",
        type=staging_argument,
        action="append",
        default=[],
        metavar="NAME=PATTERN",
        help="a place that holds staged files, read in each sample; repeat for each place",
    )
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
            staging=arguments.staging,
        )
    finally:
        stop.restore()


if __name__ == "__main__":
    sys.exit(main())
