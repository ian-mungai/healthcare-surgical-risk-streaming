"""The host figures are parsed from the Linux sources and from Docker's own text, and each reading that cannot be trusted is refused by name.

The fixed sample files in ``tests/fixtures/proc`` are in the form of a Linux host's ``/proc/meminfo``, ``/proc/stat`` and ``/proc/diskstats`` and of Docker's
formatted output, so the same parsing runs on this computer. Every figure is derived here from the sample files by hand.
"""

from __future__ import annotations

import dataclasses
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts.stack.host_figures import (
    FiguresError,
    HostReading,
    HostSources,
    Memory,
    device_of,
    free_disk_mib,
    parse_cpu_times,
    parse_diskstats,
    parse_meminfo,
    parse_size_bytes,
    read_host,
)
from tests.fast.proc_support import FIXTURES, fixture


class MemoryTest(unittest.TestCase):
    def test_the_memory_and_swap_figures_are_the_raw_kib_of_their_lines_and_nothing_is_derived(self) -> None:
        memory = parse_meminfo(fixture("meminfo_before.txt"))
        self.assertEqual((33554432, 20971520, 2097152, 2097152), (memory.total_kib, memory.available_kib, memory.swap_total_kib, memory.swap_free_kib))
        self.assertEqual({"total_kib", "available_kib", "swap_total_kib", "swap_free_kib"}, {field.name for field in dataclasses.fields(Memory)})

    def test_a_host_with_no_swap_has_zero_swap_as_read(self) -> None:
        text = "MemTotal: 1024 kB\nMemAvailable: 512 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n"
        memory = parse_meminfo(text)
        self.assertEqual((0, 0), (memory.swap_total_kib, memory.swap_free_kib))

    def test_a_missing_line_is_named_and_never_replaced_by_zero(self) -> None:
        for name in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
            with self.subTest(name=name):
                text = "".join(line for line in fixture("meminfo_before.txt").splitlines(keepends=True) if not line.startswith(f"{name}:"))
                with self.assertRaisesRegex(FiguresError, f"no {name} line"):
                    parse_meminfo(text)

    def test_a_figure_in_a_unit_other_than_kb_is_refused(self) -> None:
        text = fixture("meminfo_before.txt").replace("MemAvailable:   20971520 kB", "MemAvailable:   20480 MB")
        with self.assertRaisesRegex(FiguresError, "no MemAvailable line"):
            parse_meminfo(text)

    def test_more_free_swap_than_swap_or_more_available_than_total_is_refused(self) -> None:
        with self.assertRaisesRegex(FiguresError, "SwapFree.*more than SwapTotal"):
            parse_meminfo("MemTotal: 1024 kB\nMemAvailable: 512 kB\nSwapTotal: 100 kB\nSwapFree: 200 kB\n")
        with self.assertRaisesRegex(FiguresError, "MemAvailable.*more than MemTotal"):
            parse_meminfo("MemTotal: 1024 kB\nMemAvailable: 2048 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n")


class CpuTest(unittest.TestCase):
    def test_a_text_with_no_total_line_or_a_short_line_is_refused(self) -> None:
        with self.assertRaisesRegex(FiguresError, "no cpu line"):
            parse_cpu_times("intr 1 2 3\n")
        with self.assertRaisesRegex(FiguresError, "cpu0.*fewer than 8"):
            parse_cpu_times("cpu  1 2 3 4 5 6 7 8\ncpu0 1 2 3\n")


class DiskTest(unittest.TestCase):
    def test_the_bytes_read_and_written_of_one_device_come_from_its_row_in_512_byte_sectors(self) -> None:
        counters = parse_diskstats(fixture("diskstats_before.txt"), 259, 1)
        self.assertEqual(2000000 * 512, counters.read_bytes)
        self.assertEqual(1000000 * 512, counters.written_bytes)

    def test_another_device_is_not_taken_for_the_wanted_one(self) -> None:
        whole = parse_diskstats(fixture("diskstats_before.txt"), 259, 0)
        self.assertEqual(2100000 * 512, whole.read_bytes)

    def test_a_device_with_no_row_is_named_and_a_short_row_is_refused(self) -> None:
        with self.assertRaisesRegex(FiguresError, "no row for the device 259:9"):
            parse_diskstats(fixture("diskstats_before.txt"), 259, 9)
        with self.assertRaisesRegex(FiguresError, "row for the device 8:0 holds fewer than 10"):
            parse_diskstats("   8       0 sda 1 2 3 4\n", 8, 0)

    def test_the_device_of_a_folder_is_the_pair_that_the_folder_s_file_system_has(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            major, minor = device_of(Path(folder))
        self.assertIsInstance(major, int)
        self.assertIsInstance(minor, int)

    def test_the_free_space_is_what_a_user_can_use_and_not_what_only_root_can_use(self) -> None:
        stat = SimpleNamespace(f_bavail=1000, f_bfree=9000, f_frsize=4096)
        with mock.patch("scripts.stack.host_figures.os.statvfs", return_value=stat):
            self.assertEqual(round(1000 * 4096 / 1048576, 1), free_disk_mib(Path("/data")))

    def test_the_free_space_of_a_folder_is_read_and_a_missing_folder_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            self.assertGreater(free_disk_mib(Path(folder)), 0)
            with self.assertRaisesRegex(FiguresError, "cannot read the free disk space"):
                free_disk_mib(Path(folder) / "missing")


class ContainerTest(unittest.TestCase):
    def test_sizes_are_read_in_bytes_from_the_units_that_docker_prints(self) -> None:
        self.assertEqual(0, parse_size_bytes("0B"))
        self.assertEqual(512, parse_size_bytes("512B"))
        self.assertEqual(524288, parse_size_bytes("512KiB"))
        self.assertEqual(round(312.4 * 1048576), parse_size_bytes("312.4MiB"))
        self.assertEqual(round(1.2 * 1024**3), parse_size_bytes("1.2GiB"))
        self.assertEqual(1024**4, parse_size_bytes("1TiB"))

    def test_a_figure_that_rounding_would_hide_is_above_the_limit_in_bytes(self) -> None:
        self.assertGreater(parse_size_bytes("64.04MiB"), 64 * 1048576)
        self.assertEqual(64 * 1048576, parse_size_bytes("64MiB"))

    def test_a_size_in_another_unit_or_with_no_figure_is_refused(self) -> None:
        for text in ("12 parsecs", "MiB", "", "1.5.2GiB", "-3MiB"):
            with self.subTest(text=text), self.assertRaisesRegex(FiguresError, "memory size"):
                parse_size_bytes(text)


class ReadingTest(unittest.TestCase):
    def sources(self, folder: Path, stage: str) -> HostSources:
        return HostSources(
            meminfo=FIXTURES / f"meminfo_{stage}.txt",
            stat=FIXTURES / f"stat_{stage}.txt",
            diskstats=FIXTURES / f"diskstats_{stage}.txt",
            data_path=folder,
            device=(259, 1),
        )

    def test_a_source_that_cannot_be_read_is_named(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            good = self.sources(Path(folder), "before")
            for field in ("meminfo", "stat", "diskstats"):
                with self.subTest(field=field):
                    broken = HostSources(**{**good.__dict__, field: Path(folder) / "missing"})
                    with self.assertRaisesRegex(FiguresError, f"cannot read the host's {field}"):
                        read_host(broken)

    def test_a_reading_holds_the_counters_as_read_and_no_time(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            reading = read_host(self.sources(Path(folder), "before"))
        self.assertEqual({"memory", "cpu", "disk", "free_disk_mib"}, {field.name for field in dataclasses.fields(HostReading)})
        self.assertEqual(33554432, reading.memory.total_kib)
        self.assertEqual((2000000 * 512, 1000000 * 512), (reading.disk.read_bytes, reading.disk.written_bytes))
