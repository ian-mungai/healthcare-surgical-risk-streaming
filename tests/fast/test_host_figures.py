"""The host figures are parsed from the Linux sources and from Docker's own text, and each reading that cannot be trusted is refused by name.

The fixed sample files in ``tests/fixtures/proc`` are in the form of a Linux host's ``/proc/meminfo``, ``/proc/stat`` and ``/proc/diskstats`` and of Docker's
formatted output, so the same parsing runs on this computer. Every figure is derived here from the sample files by hand.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.stack.host_figures import (
    FiguresError,
    HostSources,
    cpu_percent,
    device_of,
    free_disk_mib,
    host_figures,
    parse_container_memory,
    parse_cpu_times,
    parse_diskstats,
    parse_meminfo,
    parse_size_mib,
    read_host,
)
from tests.fast.proc_support import FIXTURES, fixture


class MemoryTest(unittest.TestCase):
    def test_the_memory_and_swap_figures_come_from_their_lines_and_the_used_figure_is_the_total_less_the_available(self) -> None:
        memory = parse_meminfo(fixture("meminfo_after.txt"))
        self.assertEqual(32768.0, memory.total_mib)
        self.assertEqual(5120.0, memory.available_mib)
        self.assertEqual(27648.0, memory.used_mib)
        self.assertEqual(512.0, memory.swap_used_mib)

    def test_a_host_with_no_swap_has_no_swap_in_use(self) -> None:
        text = "MemTotal: 1024 kB\nMemAvailable: 512 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n"
        self.assertEqual(0.0, parse_meminfo(text).swap_used_mib)

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
    def test_the_busy_share_of_each_core_and_of_all_cores_comes_from_the_change_between_two_readings(self) -> None:
        before, after = parse_cpu_times(fixture("stat_before.txt")), parse_cpu_times(fixture("stat_after.txt"))
        self.assertEqual(["cpu", "cpu0", "cpu1"], sorted(before))
        self.assertEqual(50.0, cpu_percent(before["cpu0"], after["cpu0"]))
        self.assertEqual(10.0, cpu_percent(before["cpu1"], after["cpu1"]))
        self.assertEqual(30.0, cpu_percent(before["cpu"], after["cpu"]))

    def test_waiting_for_the_disk_is_idle_time_and_stolen_time_is_busy_time(self) -> None:
        before = parse_cpu_times("cpu  0 0 0 0 0 0 0 0 0 0\n")["cpu"]
        iowait = parse_cpu_times("cpu  0 0 0 0 100 0 0 0 0 0\n")["cpu"]
        steal = parse_cpu_times("cpu  0 0 0 0 0 0 0 100 0 0\n")["cpu"]
        self.assertEqual(0.0, cpu_percent(before, iowait))
        self.assertEqual(100.0, cpu_percent(before, steal))

    def test_guest_time_is_not_counted_twice(self) -> None:
        before = parse_cpu_times("cpu  0 0 0 0 0 0 0 0 0 0\n")["cpu"]
        after = parse_cpu_times("cpu  100 0 0 100 0 0 0 0 100 0\n")["cpu"]
        self.assertEqual(50.0, cpu_percent(before, after))

    def test_a_text_with_no_total_line_or_a_short_line_is_refused(self) -> None:
        with self.assertRaisesRegex(FiguresError, "no cpu line"):
            parse_cpu_times("intr 1 2 3\n")
        with self.assertRaisesRegex(FiguresError, "cpu0.*fewer than 8"):
            parse_cpu_times("cpu  1 2 3 4 5 6 7 8\ncpu0 1 2 3\n")

    def test_a_counter_that_goes_back_or_a_reading_with_no_time_between_is_refused(self) -> None:
        before, after = parse_cpu_times(fixture("stat_before.txt"))["cpu0"], parse_cpu_times(fixture("stat_after.txt"))["cpu0"]
        with self.assertRaisesRegex(FiguresError, "went back"):
            cpu_percent(after, before)
        with self.assertRaisesRegex(FiguresError, "no CPU time passed"):
            cpu_percent(before, before)


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

    def test_the_free_space_of_a_folder_is_read_and_a_missing_folder_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            self.assertGreater(free_disk_mib(Path(folder)), 0)
            with self.assertRaisesRegex(FiguresError, "cannot read the free disk space"):
                free_disk_mib(Path(folder) / "missing")


class ContainerTest(unittest.TestCase):
    def test_sizes_are_read_in_the_units_that_docker_prints(self) -> None:
        self.assertEqual(0.0, parse_size_mib("0B"))
        self.assertEqual(0.5, parse_size_mib("512KiB"))
        self.assertEqual(312.4, parse_size_mib("312.4MiB"))
        self.assertEqual(1228.8, parse_size_mib("1.2GiB"))
        self.assertEqual(1048576.0, parse_size_mib("1TiB"))

    def test_a_size_in_another_unit_or_with_no_figure_is_refused(self) -> None:
        for text in ("12 parsecs", "MiB", "", "1.5.2GiB", "-3MiB"):
            with self.subTest(text=text), self.assertRaisesRegex(FiguresError, "memory size"):
                parse_size_mib(text)

    def test_the_memory_of_each_service_of_the_project_comes_from_the_listing_and_the_statistics_joined_on_the_container(self) -> None:
        found = parse_container_memory(fixture("docker_ps.txt"), fixture("docker_stats.txt"))
        self.assertEqual({"postgres": 312.4, "nessie": 1228.8, "spark": 2560.0}, found)

    def test_a_container_of_another_project_is_left_out_and_a_container_that_has_just_gone_is_left_out(self) -> None:
        listing = "a1b2c3d4e5f6|postgres\nb2c3d4e5f6a1|nessie\n"
        statistics = "a1b2c3d4e5f6|100MiB / 1GiB\nffffffffffff|900MiB / 2GiB\n"
        self.assertEqual({"postgres": 100.0}, parse_container_memory(listing, statistics))

    def test_two_containers_of_one_service_add_up(self) -> None:
        listing = "a1b2c3d4e5f6|spark\nb2c3d4e5f6a1|spark\n"
        statistics = "a1b2c3d4e5f6|100MiB / 1GiB\nb2c3d4e5f6a1|50MiB / 1GiB\n"
        self.assertEqual({"spark": 150.0}, parse_container_memory(listing, statistics))

    def test_a_line_that_is_not_in_the_form_is_refused_and_never_skipped(self) -> None:
        with self.assertRaisesRegex(FiguresError, "docker ps line 1"):
            parse_container_memory("not a line\n", "")
        with self.assertRaisesRegex(FiguresError, "docker stats line 1"):
            parse_container_memory("a1b2c3d4e5f6|postgres\n", "garbled\n")


class ReadingTest(unittest.TestCase):
    def sources(self, folder: Path, stage: str) -> HostSources:
        return HostSources(
            meminfo=FIXTURES / f"meminfo_{stage}.txt",
            stat=FIXTURES / f"stat_{stage}.txt",
            diskstats=FIXTURES / f"diskstats_{stage}.txt",
            data_path=folder,
            device=(259, 1),
        )

    def test_two_readings_of_the_sample_files_give_every_figure_of_the_interval(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            before = read_host(self.sources(Path(folder), "before"), clock=lambda: 100.0)
            after = read_host(self.sources(Path(folder), "after"), clock=lambda: 110.0)
        figures = host_figures(before, after)
        self.assertEqual(27648.0, figures.memory_used_mib)
        self.assertEqual(5120.0, figures.memory_available_mib)
        self.assertEqual(512.0, figures.swap_used_mib)
        self.assertEqual(30.0, figures.cpu_percent)
        self.assertEqual((50.0, 10.0), figures.cpu_core_percent)
        self.assertEqual(10.0, figures.disk_read_mib_s)
        self.assertEqual(5.0, figures.disk_write_mib_s)
        self.assertGreater(figures.free_disk_mib, 0)

    def test_a_source_that_cannot_be_read_is_named(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            good = self.sources(Path(folder), "before")
            for field in ("meminfo", "stat", "diskstats"):
                with self.subTest(field=field):
                    broken = HostSources(**{**good.__dict__, field: Path(folder) / "missing"})
                    with self.assertRaisesRegex(FiguresError, f"cannot read the host's {field}"):
                        read_host(broken, clock=lambda: 0.0)

    def test_two_readings_at_the_same_time_or_in_the_wrong_order_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            first = read_host(self.sources(Path(folder), "before"), clock=lambda: 100.0)
            second = read_host(self.sources(Path(folder), "after"), clock=lambda: 100.0)
        with self.assertRaisesRegex(FiguresError, "no time passed"):
            host_figures(first, second)

    def test_a_disk_counter_that_goes_back_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            before = read_host(self.sources(Path(folder), "after"), clock=lambda: 100.0)
            after = read_host(self.sources(Path(folder), "before"), clock=lambda: 110.0)
        with self.assertRaisesRegex(FiguresError, "went back"):
            host_figures(before, after)

    def test_a_different_number_of_cores_between_two_readings_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            before = read_host(self.sources(Path(folder), "before"), clock=lambda: 100.0)
            after = read_host(self.sources(Path(folder), "after"), clock=lambda: 110.0)
        smaller = type(after)(**{**after.__dict__, "cpu": {name: times for name, times in after.cpu.items() if name != "cpu1"}})
        with self.assertRaisesRegex(FiguresError, "number of cores changed"):
            host_figures(before, smaller)


if __name__ == "__main__":
    unittest.main()
