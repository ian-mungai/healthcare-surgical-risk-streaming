"""The Linux host's raw counters and the sizes Docker prints, parsed and nothing more: memory and swap, the CPU counters of each core, the disk counters
of one device and the free space of one folder.

The parsers are pure functions over text. ``tests/fixtures/proc`` holds sample files in the form of ``/proc/meminfo``, ``/proc/stat`` and
``/proc/diskstats``, so the same parsing runs on any computer. ``read_host`` reads the real sources of a Linux host and returns the counters as read:
it computes no rate, no share and no difference. ``parse_size_bytes`` turns a size as Docker prints it (``312.4MiB``) into bytes. A source that is
missing and a line that is not in its form are each refused with the name of the source and never replaced by a zero. ``/proc/diskstats`` counts
512-byte sectors.
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
SIZE_UNITS_BYTES = {"B": 1, "KiB": 1024, "MiB": BYTES_PER_MIB, "GiB": 1024**3, "TiB": 1024**4}
SIZE_TEXT = re.compile(r"(\d+(?:\.\d+)?)(B|KiB|MiB|GiB|TiB)")
CPU_LINE = re.compile(r"^(cpu\d*)\s+(.*)$", re.MULTILINE)


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
