"""The admission check: whether a service group fits in the host's available memory before it starts.

The limits file (``compose/limits.yml``, read only through ``load_limits``) holds the reserve and, for each service of both runtimes, its group and its
memory limit. ``decide`` is a pure decision over stated figures. The group is admitted when

    reserve + limits of the supporting services not running + limits of the group's services not running <= available memory

A service that runs is left out of both sums, because its use is already inside the host's available figure. A service that does not run is counted by its
limit. When every service of the group and of the supporting group runs, nothing starts and the group is admitted. ``describe`` writes one message
with the four figures in MiB. A group that waits is not a failure of the program: it exits with ``EXIT_WAIT``.

``.venv/bin/python -m scripts.stack.admission <group>`` reads the host's available memory (``MemAvailable`` of ``/proc/meminfo``, Linux only) and the
running Compose services of the stack's project, then decides. The test inputs ``--limits``, ``--available-mib`` and ``--running`` replace the limits
file, the memory reading and the running-services reading. Names that ``--running`` gives and the limits file does not hold are ignored. Exit status:
0 admitted, 75 the group waits, 1 refused (unknown group, invalid group name, limits file that fails its checks, host that cannot be read), 2 wrong usage.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.process import TimeoutExpired, run_command, timeout_text
from scripts.stack.limits import LIMITS_FILE, SERVICE_NAME, Limits, LimitsError, load_limits

EXIT_ADMITTED = 0
EXIT_REFUSED = 1
EXIT_WAIT = 75
SUPPORTING_GROUP = "supporting"
COMPOSE_PROJECT = "surgical_risk"
MEMINFO = Path("/proc/meminfo")
AVAILABLE_LINE = re.compile(r"^MemAvailable:\s+(\d+) kB$", re.MULTILINE)
DOCKER_DEADLINE_SECONDS = 30
KIB_PER_MIB = 1024


class AdmissionError(ValueError):
    """The group or the host cannot be judged, which is a refusal and not a wait."""


@dataclass(frozen=True)
class Decision:
    group: str
    reserve_mib: int
    supporting_mib: int
    group_mib: int
    available_mib: int

    @property
    def needed_mib(self) -> int:
        return self.reserve_mib + self.supporting_mib + self.group_mib

    @property
    def nothing_to_start(self) -> bool:
        return self.supporting_mib + self.group_mib == 0

    @property
    def admitted(self) -> bool:
        return self.nothing_to_start or self.needed_mib <= self.available_mib


def not_running_mib(limits: Limits, group: str, running: Collection[str]) -> int:
    return sum(entry.memory_limit_mib for name, entry in limits.services.items() if entry.group == group and name not in running)


def decide(limits: Limits, group: str, running: Collection[str], available_mib: int) -> Decision:
    """The decision for ``group`` over the limits, the names of the services that run and the host's available memory in MiB."""
    if not SERVICE_NAME.fullmatch(group):
        raise AdmissionError(f"{group!r} is not a valid group name: it must be lower case letters, digits and underscores, starting with a letter")
    known = sorted({entry.group for entry in limits.services.values()})
    if group not in known:
        raise AdmissionError(f"unknown group {group!r}: the limits file holds the groups {', '.join(known)}")
    supporting_mib = 0 if group == SUPPORTING_GROUP else not_running_mib(limits, SUPPORTING_GROUP, running)
    return Decision(group, limits.host_reserve_mib, supporting_mib, not_running_mib(limits, group, running), available_mib)


def describe(decision: Decision) -> str:
    """The one message of a decision: the four figures with their names, and how far the group is from fitting."""
    verdict = "admitted" if decision.admitted else "waits"
    margin = decision.available_mib - decision.needed_mib
    ending = "nothing to start" if decision.nothing_to_start else (f"{margin} MiB spare" if decision.admitted else f"{-margin} MiB short")
    return (
        f"group {decision.group} {verdict}: reserve {decision.reserve_mib} MiB, supporting services {decision.supporting_mib} MiB, "
        f"group {decision.group_mib} MiB, available {decision.available_mib} MiB ({decision.needed_mib} MiB needed in all, {ending})"
    )


def parse_available_mib(text: str) -> int:
    """The ``MemAvailable`` figure of a ``/proc/meminfo`` text in whole MiB, rounded down."""
    match = AVAILABLE_LINE.search(text)
    if match is None:
        raise AdmissionError("the memory report holds no MemAvailable line, so the available memory is unknown: state it with --available-mib")
    return int(match.group(1)) // KIB_PER_MIB


def read_available_mib(path: Path = MEMINFO) -> int:
    try:
        text = path.read_text()
    except OSError as error:
        raise AdmissionError(f"cannot read the host's available memory from {path}: {error.strerror}; state it with --available-mib") from error
    return parse_available_mib(text)


def parse_running(output: str) -> frozenset[str]:
    return frozenset(line.strip() for line in output.splitlines() if line.strip())


def read_running() -> frozenset[str]:
    """The service names of the stack's running Compose containers."""
    arguments = ["ps", "--filter", f"label=com.docker.compose.project={COMPOSE_PROJECT}", "--format", '{{.Label "com.docker.compose.service"}}']
    try:
        result = run_command("docker", arguments, timeout=DOCKER_DEADLINE_SECONDS)
    except FileNotFoundError as error:
        raise AdmissionError("cannot list the running services: docker is not installed; state them with --running") from error
    except TimeoutExpired as error:
        raise AdmissionError(f"cannot list the running services: {timeout_text('docker ps', DOCKER_DEADLINE_SECONDS)}; state them with --running") from error
    if result.returncode != 0:
        raise AdmissionError(f"cannot list the running services: docker ps exited {result.returncode}; state them with --running")
    return parse_running(result.stdout)


def check_group(group: str, *, limits_path: Path = LIMITS_FILE, available_mib: int | None = None, running: Collection[str] | None = None) -> Decision:
    """The decision for ``group`` with the figures not given read from the host. Raises ``AdmissionError`` or ``LimitsError`` for a refusal."""
    limits = load_limits(limits_path)
    known_running = read_running() if running is None else running
    return decide(limits, group, known_running, read_available_mib() if available_mib is None else available_mib)


def whole_number(text: str) -> int:
    if not text.isdigit():
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number")
    return int(text)


def running_names(text: str) -> frozenset[str]:
    return frozenset(name for name in text.split(",") if name)


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="python -m scripts.stack.admission", description=__doc__.split("\n\n")[0] if __doc__ else None)
    command.add_argument("group", help="the service group to start")
    command.add_argument("--limits", type=Path, default=LIMITS_FILE, help="test input: a limits file in place of compose/limits.yml")
    command.add_argument("--available-mib", type=whole_number, help="test input: the host's available memory in MiB, in place of the reading of /proc/meminfo")
    command.add_argument("--running", type=running_names, help="test input: the names of the running services, comma separated, in place of docker's list")
    return command


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        decision = check_group(arguments.group, limits_path=arguments.limits, available_mib=arguments.available_mib, running=arguments.running)
    except (AdmissionError, LimitsError) as error:
        sys.stderr.write(f"admission: refused: {error}\n")
        return EXIT_REFUSED
    sys.stdout.write(f"admission: {describe(decision)}\n")
    return EXIT_ADMITTED if decision.admitted else EXIT_WAIT


if __name__ == "__main__":
    sys.exit(main())
