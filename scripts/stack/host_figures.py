"""The host figures of the capacity probe: memory, swap, CPU, disk activity and free disk space from the Linux host's counters, and each container's memory.

The parsers are pure functions over text. ``tests/fixtures/proc`` holds sample files in the form of ``/proc/meminfo``, ``/proc/stat`` and ``/proc/diskstats``,
so the same parsing runs on any computer. ``read_host`` reads the real sources of a Linux host and ``host_figures`` turns two readings into the figures of the
interval between them. A source that is missing, a line that is not in its form and a counter that goes back are each refused with the name of the source
and never replaced by a zero, so a figure is either measured or the run fails.

Units: memory in MiB, disk activity in MiB a second, CPU as the busy share of the interval in percent. ``/proc/diskstats`` counts 512-byte sectors.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

KIB_PER_MIB = 1024
BYTES_PER_MIB = 1024 * 1024
SECTOR_BYTES = 512
CPU_FIELDS_NEEDED = 8
DISKSTATS_FIELDS_NEEDED = 10
MIN_CPU_SECONDS = 1.0
CPU_BUSY_FIELDS = (0, 1, 2, 5, 6, 7)
CPU_IDLE_FIELDS = (3, 4)
SIZE_UNITS_BYTES = {"B": 1, "KiB": 1024, "MiB": BYTES_PER_MIB, "GiB": 1024**3, "TiB": 1024**4}
SIZE_TEXT = re.compile(r"(\d+(?:\.\d+)?)(B|KiB|MiB|GiB|TiB)")
CGROUP_ROOT = Path("/sys/fs/cgroup")
PROC_ROOT = Path("/proc")
CGROUP_LINE = re.compile(r"^0::(/\S*)$", re.MULTILINE)
CPU_LINE = re.compile(r"^(cpu\d*)\s+(.*)$", re.MULTILINE)
PS_LINE = re.compile(r"([0-9a-f]{12})\|([A-Za-z0-9_.-]+)")
STATS_LINE = re.compile(r"([0-9a-f]{12})\|(\S+) / (\S+)")


class FiguresError(RuntimeError):
    """A host figure cannot be read or cannot be trusted. The message names the source and holds no command's output."""


@dataclass(frozen=True)
class Memory:
    total_kib: int
    available_kib: int
    swap_total_kib: int
    swap_free_kib: int

    @property
    def total_mib(self) -> float:
        return self.total_kib / KIB_PER_MIB

    @property
    def available_mib(self) -> float:
        return self.available_kib / KIB_PER_MIB

    @property
    def used_mib(self) -> float:
        return (self.total_kib - self.available_kib) / KIB_PER_MIB

    @property
    def swap_used_mib(self) -> float:
        return (self.swap_total_kib - self.swap_free_kib) / KIB_PER_MIB


@dataclass(frozen=True)
class CpuTimes:
    """The first eight counters of a ``cpu`` line in clock ticks: user, nice, system, idle, iowait, irq, softirq and steal."""

    counts: tuple[int, ...]


@dataclass(frozen=True)
class DiskCounters:
    read_bytes: int
    written_bytes: int


def meminfo_figure(text: str, name: str) -> int:
    match = re.search(rf"^{name}:\s+(\d+) kB$", text, re.MULTILINE)
    if match is None:
        raise FiguresError(f"the memory report holds no {name} line in kB")
    return int(match.group(1))


def parse_meminfo(text: str) -> Memory:
    """The memory and swap figures of a ``/proc/meminfo`` text. A missing line is refused by its name."""
    memory = Memory(*(meminfo_figure(text, name) for name in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")))
    if memory.swap_free_kib > memory.swap_total_kib:
        raise FiguresError("the memory report holds a SwapFree that is more than SwapTotal")
    if memory.available_kib > memory.total_kib:
        raise FiguresError("the memory report holds a MemAvailable that is more than MemTotal")
    return memory


def parse_cpu_times(text: str) -> dict[str, CpuTimes]:
    """The busy and total time of the ``cpu`` line and of each ``cpuN`` line of a ``/proc/stat`` text, in clock ticks.

    Idle and waiting for the disk are idle. Guest time is already inside user time, so only the first eight figures count.
    """
    found: dict[str, CpuTimes] = {}
    for name, rest in CPU_LINE.findall(text):
        fields = rest.split()
        if len(fields) < CPU_FIELDS_NEEDED or not all(field.isascii() and field.isdigit() for field in fields):
            raise FiguresError(f"the CPU report line {name} holds fewer than {CPU_FIELDS_NEEDED} whole numbers")
        found[name] = CpuTimes(tuple(int(field) for field in fields[:CPU_FIELDS_NEEDED]))
    if "cpu" not in found:
        raise FiguresError("the CPU report holds no cpu line")
    return found


def cpu_percent(before: CpuTimes, after: CpuTimes) -> float | None:
    """The busy share in percent of the ticks between two readings of one line, or None when no tick passed.

    Each counter's change counts from zero upward, so a counter that went back (``iowait`` can) adds nothing and the share stays from 0 to 100.
    """
    changes = [max(0, later - earlier) for earlier, later in zip(before.counts, after.counts, strict=True)]
    busy, idle = sum(changes[index] for index in CPU_BUSY_FIELDS), sum(changes[index] for index in CPU_IDLE_FIELDS)
    if busy + idle == 0:
        return None
    return round(100.0 * busy / (busy + idle), 1)


def parse_diskstats(text: str, major: int, minor: int) -> DiskCounters:
    """The bytes read and written by the device ``major:minor`` since boot, from a ``/proc/diskstats`` text."""
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] == str(major) and fields[1] == str(minor):
            if len(fields) < DISKSTATS_FIELDS_NEEDED or not all(field.isascii() and field.isdigit() for field in (fields[5], fields[9])):
                raise FiguresError(f"the disk report's row for the device {major}:{minor} holds fewer than {DISKSTATS_FIELDS_NEEDED} fields")
            return DiskCounters(int(fields[5]) * SECTOR_BYTES, int(fields[9]) * SECTOR_BYTES)
    raise FiguresError(f"the disk report has no row for the device {major}:{minor}")


def device_of(path: Path) -> tuple[int, int]:
    """The device (major, minor) of the file system that holds ``path``."""
    try:
        number = path.stat().st_dev
    except OSError as error:
        raise FiguresError(f"cannot read the device of {path}: {error.strerror}") from error
    return os.major(number), os.minor(number)


def free_disk_mib(path: Path) -> float:
    """The free space in MiB that a user can use on the file system that holds ``path``."""
    try:
        stat = os.statvfs(path)
    except OSError as error:
        raise FiguresError(f"cannot read the free disk space of {path}: {error.strerror}") from error
    return round(stat.f_bavail * stat.f_frsize / BYTES_PER_MIB, 1)


def parse_size_bytes(text: str) -> int:
    """A memory size as Docker prints it (``64.04MiB``) in whole bytes, from the printed figure. Another unit or text is refused."""
    match = SIZE_TEXT.fullmatch(text)
    if match is None:
        raise FiguresError(f"{text[:24]!r} is not a memory size in B, KiB, MiB, GiB or TiB")
    return round(Decimal(match.group(1)) * SIZE_UNITS_BYTES[match.group(2)])


def parse_container_usage(listing: str, statistics: str) -> dict[str, int]:
    """The memory in use in bytes of each Compose service of the project, as Docker prints it.

    ``listing`` has one ``<id>|<service>`` line for each container of the project (``docker ps``). ``statistics`` has one ``<id>|<used> / <limit>`` line for
    each container that runs (``docker stats``). A container of another project has no line in the listing and is left out. A container in the listing
    that has no statistics has just stopped and is left out. Two containers of one service add up. A line in another form is refused.
    """
    services: dict[str, str] = {}
    for number, line in enumerate(listing.splitlines(), start=1):
        match = PS_LINE.fullmatch(line.strip())
        if match is None:
            raise FiguresError(f"docker ps line {number} is not in the form <container>|<service>")
        services[match.group(1)] = match.group(2)
    used: dict[str, int] = {}
    for number, line in enumerate(statistics.splitlines(), start=1):
        match = STATS_LINE.fullmatch(line.strip())
        if match is None:
            raise FiguresError(f"docker stats line {number} is not in the form <container>|<used> / <limit>")
        service = services.get(match.group(1))
        if service is not None:
            used[service] = used.get(service, 0) + parse_size_bytes(match.group(2))
    return dict(sorted(used.items()))


@dataclass(frozen=True)
class CgroupMemory:
    """One container's memory files from its cgroup (version 2), as read: ``memory.peak`` and ``memory.max`` in bytes and every counter of ``memory.events``."""

    peak_bytes: int
    max_bytes: int | None
    events: Mapping[str, int]


def parse_cgroup_path(text: str) -> str:
    """The cgroup (version 2) path of a process, from its ``/proc/<pid>/cgroup`` text. Another form, or a path with ``..``, is refused."""
    match = CGROUP_LINE.search(text)
    if match is None or ".." in match.group(1).split("/"):
        raise FiguresError("the process has no cgroup version 2 path")
    return match.group(1)


def parse_memory_events(text: str) -> dict[str, int]:
    """The counters of a cgroup's ``memory.events`` file."""
    found: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isascii() and parts[1].isdigit():
            found[parts[0]] = int(parts[1])
    for name in ("oom_kill", "max"):
        if name not in found:
            raise FiguresError(f"the cgroup's memory.events holds no {name} counter")
    return found


def whole_bytes(text: str, label: str) -> int:
    value = text.strip()
    if not value.isascii() or not value.isdigit():
        raise FiguresError(f"the cgroup's {label} is not a whole number")
    return int(value)


def read_cgroup_memory(pid: int, *, proc_root: Path = PROC_ROOT, cgroup_root: Path = CGROUP_ROOT) -> CgroupMemory:
    """The memory counters of the cgroup that holds the process ``pid``. A file that cannot be read, such as ``memory.peak`` on an old kernel, is named."""
    folder = cgroup_root / parse_cgroup_path(read_source(proc_root / str(pid) / "cgroup", "cgroup path")).lstrip("/")
    peak = whole_bytes(read_source(folder / "memory.peak", "memory.peak"), "memory.peak")
    limit = read_source(folder / "memory.max", "memory.max").strip()
    events = parse_memory_events(read_source(folder / "memory.events", "memory.events"))
    return CgroupMemory(peak, None if limit == "max" else whole_bytes(limit, "memory.max"), events)


@dataclass(frozen=True)
class HostSources:
    """Where the host's counters are. The defaults are the Linux host's. ``device`` is found from ``data_path`` when it is not given."""

    meminfo: Path = Path("/proc/meminfo")
    stat: Path = Path("/proc/stat")
    diskstats: Path = Path("/proc/diskstats")
    data_path: Path = Path("/var/lib/docker")
    device: tuple[int, int] | None = None


@dataclass(frozen=True)
class HostReading:
    at: float
    memory: Memory
    cpu: Mapping[str, CpuTimes]
    disk: DiskCounters
    free_disk_mib: float


@dataclass(frozen=True)
class HostFigures:
    memory_used_mib: float
    memory_available_mib: float
    memory_available_kib: int
    swap_used_mib: float
    cpu_percent: float | None
    cpu_core_percent: tuple[float | None, ...]
    disk_read_mib_s: float
    disk_write_mib_s: float
    free_disk_mib: float


def read_source(path: Path, label: str) -> str:
    try:
        return path.read_text()
    except OSError as error:
        raise FiguresError(f"cannot read the host's {label} from {path}: {error.strerror}") from error


def read_host(sources: HostSources, clock: Callable[[], float] = time.monotonic) -> HostReading:
    """One reading of every host counter."""
    memory = parse_meminfo(read_source(sources.meminfo, "meminfo"))
    cpu = parse_cpu_times(read_source(sources.stat, "stat"))
    major, minor = sources.device if sources.device is not None else device_of(sources.data_path)
    disk = parse_diskstats(read_source(sources.diskstats, "diskstats"), major, minor)
    return HostReading(clock(), memory, cpu, disk, free_disk_mib(sources.data_path))


def core_names(reading: HostReading) -> list[str]:
    return sorted((name for name in reading.cpu if name != "cpu"), key=lambda name: int(name[3:]))


def host_figures(before: HostReading, after: HostReading) -> HostFigures:
    """The figures of the interval between two readings. Memory, swap and free space are those of the later reading."""
    seconds = after.at - before.at
    if seconds <= 0:
        raise FiguresError("no time passed between the two readings")
    cores = core_names(after)
    if cores != core_names(before):
        raise FiguresError("the number of cores changed between two readings")
    long_enough = seconds >= MIN_CPU_SECONDS
    percent = cpu_percent(before.cpu["cpu"], after.cpu["cpu"]) if long_enough else None
    per_core = tuple(cpu_percent(before.cpu[name], after.cpu[name]) if long_enough else None for name in cores)
    read, written = after.disk.read_bytes - before.disk.read_bytes, after.disk.written_bytes - before.disk.written_bytes
    if read < 0 or written < 0:
        raise FiguresError("a disk counter went back between two readings")
    return HostFigures(
        round(after.memory.used_mib, 1),
        round(after.memory.available_mib, 1),
        after.memory.available_kib,
        round(after.memory.swap_used_mib, 1),
        percent,
        per_core,
        round(read / BYTES_PER_MIB / seconds, 1),
        round(written / BYTES_PER_MIB / seconds, 1),
        after.free_disk_mib,
    )
