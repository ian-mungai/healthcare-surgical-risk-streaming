"""The capacity sampler appends one JSON line for each iteration, as read, and derives and judges nothing.

Every case drives the real writer and the real entry point. A reading that fails is named in ``missing`` and its key is absent. The loop goes on.
"""

from __future__ import annotations

import dataclasses
import decimal
import errno
import io
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess, ProcessGroupSurvived, TimeoutExpired
from scripts.stack.capacity_sampler import (
    CAUSES,
    MAX_INTERVAL_SECONDS,
    MAX_RUN_SECONDS,
    SCHEMA,
    Output,
    SamplerError,
    StopFlag,
    check_output,
    parser,
    run_sampler,
)
from scripts.stack.host_figures import FiguresError, HostReading, HostSources, Memory, read_host
from scripts.stack.limits import LIMITS_FILE, load_limits

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "proc"
JUDGMENT_WORDS = re.compile(r"\b(ok|pass|passed|complete|completed|within|sufficient|accepted|success|succeeded|capacity passed|fits|enough)\b", re.IGNORECASE)
PS_TEXT = "p-nessie-1|running|p\np-spark-run-1|exited|p\nother-tool|running|\n"
STATS_TEXT = "p-nessie-1|312.4MiB / 1GiB\nother-tool|0B / 0B\n"
ALLOWED_KEYS = {
    "kind", "seq", "host", "containers", "memory", "missing", "wall_ns_before", "wall_ns_after", "mono_before", "mono_after", "memory_kib", "cpu", "disk",
    "free_disk_mib", "rows", "name", "state", "project", "usage_text", "limit_text", "usage_bytes", "limit_bytes", "reading", "cause", "total", "available",
    "swap_total", "swap_free", "read_bytes", "written_bytes",
}  # fmt: skip


class Clock:
    """A clock that moves only when a wait or a fake command moves it."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def wall_ns(self) -> int:
        return round(self.now * 1e9)

    def pause(self, seconds: float) -> None:
        self.now += seconds


class Runner:
    """A fake ``run_command`` for ``docker ps`` and ``docker stats``. ``failures`` maps ("stats", 2) to the exception or result of that call."""

    def __init__(
        self, clock: Clock | None = None, *, ps: str = PS_TEXT, stats: str = STATS_TEXT, failures: dict[tuple[str, int], Any] | None = None, cost: float = 0.0
    ) -> None:
        self.clock, self.ps, self.stats, self.failures, self.cost = clock, ps, stats, failures or {}, cost
        self.counts = {"ps": 0, "stats": 0}
        self.timeouts: list[tuple[str, float]] = []

    def __call__(self, program: str, args: Sequence[str], *, timeout: float, **_: object) -> CompletedProcess[str]:
        kind = args[0]
        self.counts[kind] += 1
        self.timeouts.append((kind, timeout))
        if self.clock is not None:
            self.clock.now += self.cost
        outcome = self.failures.get((kind, self.counts[kind]))
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, CompletedProcess):
            return outcome
        return CompletedProcess([program, *args], 0, self.ps if kind == "ps" else self.stats, "")


def drive(*arguments: Any, **keywords: Any) -> int:
    """``run_sampler`` with a docker program named, so that no test depends on the docker program of the computer. The runner is a fake."""
    keywords.setdefault("docker", "docker")
    return run_sampler(*arguments, **keywords)


def sources(folder: str) -> HostSources:
    return HostSources(FIXTURES / "meminfo_before.txt", FIXTURES / "stat_before.txt", FIXTURES / "diskstats_before.txt", Path(folder), (259, 1))


def lines_of(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def sample_lines(path: Path) -> list[dict[str, Any]]:
    return [line for line in lines_of(path) if line["kind"] == "sample"]


def sample_run(
    folder: str, *, failures: dict[tuple[str, int], Any] | None = None, max_seconds: float = 1.0, interval: float = 0.25, host: HostSources | None = None
) -> tuple[int, Path, Runner]:
    clock = Clock()
    runner = Runner(clock, failures=failures)
    output = Path(folder) / "samples.jsonl"
    code = drive(
        output,
        interval_seconds=interval,
        max_seconds=max_seconds,
        sources=host or sources(folder),
        runner=runner,
        clock=clock.monotonic,
        wall_ns=clock.wall_ns,
        pause=clock.pause,
        say=lambda text: None,
    )
    return code, output, runner


def all_keys(value: Any) -> list[str]:
    """Every key of a document, to the bottom."""
    if isinstance(value, dict):
        return [key for name, item in value.items() for key in (name, *all_keys(item))]
    if isinstance(value, list):
        return [key for item in value for key in all_keys(item)]
    return []


def every_key_and_text(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [text for key, item in value.items() for text in (str(key), *every_key_and_text(item))]
    if isinstance(value, list):
        return [text for item in value for text in every_key_and_text(item)]
    return [value] if isinstance(value, str) else []


class LineTest(unittest.TestCase):
    def test_the_file_is_a_header_then_one_line_for_each_iteration_then_an_end_line_with_the_count(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, output, _ = sample_run(folder)
            found = lines_of(output)
        self.assertEqual(0, code)
        self.assertEqual(["header", "sample", "sample", "sample", "sample", "end"], [line["kind"] for line in found])
        self.assertEqual([0, 1, 2, 3], [line["seq"] for line in found[1:5]])
        self.assertEqual({"kind": "end", "reason": "max_seconds", "samples": 4}, found[-1])

    def test_the_header_names_the_schema_the_interval_the_files_read_and_copies_the_configured_limits_as_plain_values(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            _, output, _ = sample_run(folder)
            header = lines_of(output)[0]
        limits = load_limits()
        self.assertEqual((SCHEMA, 0.25, 1.0), (header["schema"], header["interval_seconds"], header["max_seconds"]))
        self.assertEqual({"memory", "cpu", "disk", "free_disk", "limits"}, set(header["sources"]))
        self.assertEqual(str(LIMITS_FILE), header["sources"]["limits"])
        self.assertEqual(str(FIXTURES / "meminfo_before.txt"), header["sources"]["memory"])
        self.assertEqual(limits.host_reserve_mib, header["configured"]["host_reserve_mib"])
        self.assertEqual(
            {name: entry.memory_limit_mib for name, entry in limits.services.items()},
            {name: item["memory_limit_mib"] for name, item in header["configured"]["limits"].items()},
        )
        self.assertIn("started_wall_ns", header)

    def test_a_sample_holds_the_host_counters_as_read_each_reading_with_its_own_times(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            _, output, _ = sample_run(folder)
            first = sample_lines(output)[0]
        host = first["host"]
        self.assertEqual({"total": 33554432, "available": 20971520, "swap_total": 2097152, "swap_free": 2097152}, host["memory_kib"])
        self.assertEqual([400, 0, 200, 9000, 100, 0, 0, 0], host["cpu"]["cpu"])
        self.assertEqual([200, 0, 100, 4500, 50, 0, 0, 0], host["cpu"]["cpu0"])
        self.assertEqual({"read_bytes": 2000000 * 512, "written_bytes": 1000000 * 512}, host["disk"])
        self.assertGreater(host["free_disk_mib"], 0)
        for reading in ("host", "containers", "memory"):
            item = first[reading]
            self.assertLessEqual(item["wall_ns_before"], item["wall_ns_after"])
            self.assertLessEqual(item["mono_before"], item["mono_after"])

    def test_the_container_listing_and_the_memory_rows_are_as_read_and_are_never_joined(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            _, output, _ = sample_run(folder)
            first = sample_lines(output)[0]
        self.assertEqual(
            [
                {"name": "p-nessie-1", "state": "running", "project": "p"},
                {"name": "p-spark-run-1", "state": "exited", "project": "p"},
                {"name": "other-tool", "state": "running", "project": ""},
            ],
            first["containers"]["rows"],
        )
        self.assertEqual(
            [
                {"name": "p-nessie-1", "usage_text": "312.4MiB", "limit_text": "1GiB", "usage_bytes": round(312.4 * 1048576), "limit_bytes": 1024**3},
                {"name": "other-tool", "usage_text": "0B", "limit_text": "0B", "usage_bytes": 0, "limit_bytes": 0},
            ],
            first["memory"]["rows"],
        )
        self.assertNotIn(
            "p-spark-run-1", [row["name"] for row in first["memory"]["rows"]], "a container that is listed and has no memory row stays only in the listing"
        )

    def test_no_line_holds_a_key_other_than_the_readings_and_no_derived_figure(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            _, output, _ = sample_run(folder, failures={("stats", 2): OSError(errno.EMFILE, "too many open files")})
            found = lines_of(output)
        keys = {key for key in all_keys([line for line in found if line["kind"] == "sample"]) if not re.fullmatch(r"cpu\d*", key)}
        self.assertLessEqual(keys, ALLOWED_KEYS)
        for line in found:
            if line["kind"] == "sample":
                self.assertLessEqual(set(line), {"kind", "seq", "host", "containers", "memory", "missing"})


class MissingTest(unittest.TestCase):
    def test_a_stats_call_that_fails_with_too_many_open_files_is_named_and_its_key_is_absent_and_the_loop_goes_on(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, output, _ = sample_run(folder, failures={("stats", 2): OSError(errno.EMFILE, "too many open files")})
            found = sample_lines(output)
        self.assertEqual(0, code)
        self.assertEqual([{"reading": "memory", "cause": "os_error"}], found[1]["missing"])
        self.assertNotIn("memory", found[1])
        for number in (0, 2, 3):
            self.assertIn("memory", found[number])
            self.assertEqual([], found[number]["missing"])

    def test_each_reading_that_fails_for_each_cause_is_named_with_that_cause_and_its_key_is_absent_never_null(self) -> None:
        causes = {
            "timeout": TimeoutExpired("docker", 1),
            "exit_status": CompletedProcess(["docker"], 125, "", "x"),
            "unparsable": CompletedProcess(["docker"], 0, "garbled\n", ""),
            "os_error": FileNotFoundError("docker"),
        }
        self.assertEqual(set(CAUSES), set(causes))
        for cause, outcome in causes.items():
            for reading, call in (("containers", "ps"), ("memory", "stats")):
                with self.subTest(cause=cause, reading=reading), tempfile.TemporaryDirectory() as folder:
                    _, output, _ = sample_run(folder, failures={(call, 1): outcome}, max_seconds=0.25)
                    line = sample_lines(output)[0]
                self.assertEqual([{"reading": reading, "cause": cause}], line["missing"])
                self.assertNotIn(reading, line)
                self.assertIn("host", line)

    def test_a_host_read_that_fails_is_named_with_its_cause_and_the_docker_readings_are_kept(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            gone = HostSources(Path(folder) / "none", FIXTURES / "stat_before.txt", FIXTURES / "diskstats_before.txt", Path(folder), (259, 1))
            _, output, _ = sample_run(folder, host=gone, max_seconds=0.25)
            line = sample_lines(output)[0]
            garbled = Path(folder) / "garbled"
            garbled.write_text("MemTotal: many\n")
            odd = HostSources(garbled, FIXTURES / "stat_before.txt", FIXTURES / "diskstats_before.txt", Path(folder), (259, 1))
            second = Path(folder) / "second"
            second.mkdir()
            _, again, _ = sample_run(str(second), host=odd, max_seconds=0.25)
            odd_line = sample_lines(again)[0]
        self.assertEqual(
            ([{"reading": "host", "cause": "os_error"}], False, True), (line["missing"], "host" in line, "containers" in line and "memory" in line)
        )
        self.assertEqual([{"reading": "host", "cause": "unparsable"}], odd_line["missing"])

    def test_a_call_that_has_no_time_left_in_its_interval_is_named_as_a_timeout_and_is_not_made(self) -> None:
        clock = Clock()
        runner = Runner(clock, cost=2.0)
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "s.jsonl"
            drive(
                output,
                interval_seconds=1.0,
                max_seconds=1.0,
                sources=sources(folder),
                runner=runner,
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
            line = sample_lines(output)[0]
        self.assertEqual({"ps": 1, "stats": 0}, runner.counts)
        self.assertEqual([{"reading": "containers", "cause": "timeout"}, {"reading": "memory", "cause": "timeout"}], line["missing"])
        self.assertLessEqual(runner.timeouts[0][1], 1.0)

    def test_each_call_gets_what_is_left_of_the_interval(self) -> None:
        clock = Clock()
        runner = Runner(clock, cost=0.4)
        with tempfile.TemporaryDirectory() as folder:
            drive(
                Path(folder) / "s.jsonl",
                interval_seconds=1.0,
                max_seconds=1.0,
                sources=sources(folder),
                runner=runner,
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
        self.assertAlmostEqual(0.6, runner.timeouts[1][1], delta=0.05)
        self.assertLess(runner.timeouts[1][1], runner.timeouts[0][1])

    def test_a_slow_docker_call_does_not_move_the_host_reading_s_own_times(self) -> None:
        runner = Runner(cost=0.0)

        def slow(program: str, args: Sequence[str], *, timeout: float, **rest: object) -> CompletedProcess[str]:
            time.sleep(0.3)
            return runner(program, args, timeout=timeout, **rest)

        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "s.jsonl"
            drive(output, interval_seconds=2.0, max_seconds=0.1, sources=sources(folder), runner=slow, say=lambda t: None)
            line = sample_lines(output)[0]
        self.assertLess(line["host"]["wall_ns_after"] - line["host"]["wall_ns_before"], 100_000_000)
        self.assertGreaterEqual(line["containers"]["wall_ns_before"], line["host"]["wall_ns_after"])
        self.assertGreaterEqual(line["containers"]["wall_ns_after"] - line["containers"]["wall_ns_before"], 250_000_000)


def run_clocked(folder: str, runner: Runner, clock: Clock, *, interval: float = 1.0, max_seconds: float = 1.0, host: HostSources | None = None) -> Path:
    output = Path(folder) / "s.jsonl"
    drive(
        output,
        interval_seconds=interval,
        max_seconds=max_seconds,
        sources=host or sources(folder),
        runner=runner,
        clock=clock.monotonic,
        wall_ns=clock.wall_ns,
        pause=clock.pause,
        say=lambda t: None,
    )
    return output


class BoundaryTest(unittest.TestCase):
    """Each reading is one function whose whole body runs inside one boundary, so no exception drops the line."""

    def test_every_exception_a_docker_reading_can_raise_is_named_with_its_cause_and_the_other_readings_are_kept(self) -> None:
        table = {
            "FileNotFoundError: no docker program": (FileNotFoundError("docker"), "os_error"),
            "PermissionError: not allowed to run it": (PermissionError(errno.EACCES, "denied"), "os_error"),
            "OSError EMFILE: no more descriptors": (OSError(errno.EMFILE, "too many open files"), "os_error"),
            "TimeoutExpired: the call outlived its time": (TimeoutExpired("docker", 1), "timeout"),
            "ProcessGroupSurvived: a child outlived the call": (ProcessGroupSurvived("left over"), "os_error"),
            "UnicodeDecodeError: the output is not text": (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"), "unparsable"),
            "ValueError: a number is not one": (ValueError("x"), "unparsable"),
            "decimal.InvalidOperation: a size is not a number": (decimal.InvalidOperation("x"), "unparsable"),
            "KeyError: an unforeseen type": (KeyError("x"), "unparsable"),
        }
        for label, (error, cause) in table.items():
            for call, reading, other in (("ps", "containers", "memory"), ("stats", "memory", "containers")):
                with self.subTest(error=label, call=call), tempfile.TemporaryDirectory() as folder:
                    clock = Clock()
                    line = sample_lines(run_clocked(folder, Runner(clock, failures={(call, 1): error}), clock))[0]
                    self.assertEqual([{"reading": reading, "cause": cause}], line["missing"])
                    self.assertNotIn(reading, line)
                    self.assertIn(other, line)
                    self.assertIn("host", line)

    def test_a_stats_row_with_a_size_that_is_not_in_a_known_unit_is_unparsable(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            clock = Clock()
            line = sample_lines(run_clocked(folder, Runner(clock, stats="p-nessie-1|12parsecs / 1GiB\n"), clock))[0]
        self.assertEqual([{"reading": "memory", "cause": "unparsable"}], line["missing"])

    def test_every_exception_the_host_reading_can_raise_is_named_with_its_cause_and_the_docker_readings_are_kept(self) -> None:
        caused = FiguresError("cannot read the host's meminfo")
        caused.__cause__ = OSError(errno.EIO, "input/output error")
        table = {
            "OSError raised directly": (OSError(errno.EIO, "input/output error"), "os_error"),
            "FiguresError caused by an OSError": (caused, "os_error"),
            "FiguresError from a line that is not in its form": (FiguresError("no MemTotal line"), "unparsable"),
            "UnicodeDecodeError: a counter file is not text": (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"), "unparsable"),
            "ValueError": (ValueError("x"), "unparsable"),
            "ArithmeticError": (ArithmeticError("x"), "unparsable"),
            "KeyError: an unforeseen type": (KeyError("x"), "unparsable"),
        }
        for label, (error, cause) in table.items():
            with self.subTest(error=label), tempfile.TemporaryDirectory() as folder, mock.patch("scripts.stack.capacity_sampler.read_host", side_effect=error):
                clock = Clock()
                line = sample_lines(run_clocked(folder, Runner(clock), clock))[0]
                self.assertEqual([{"reading": "host", "cause": cause}], line["missing"])
                self.assertNotIn("host", line)
                self.assertIn("containers", line)
                self.assertIn("memory", line)

    def test_a_host_counter_file_that_is_not_text_is_unparsable_and_nothing_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            binary = Path(folder) / "meminfo"
            binary.write_bytes(b"\xff\xfe\x00")
            odd = HostSources(binary, FIXTURES / "stat_before.txt", FIXTURES / "diskstats_before.txt", Path(folder), (259, 1))
            clock = Clock()
            line = sample_lines(run_clocked(folder, Runner(clock), clock, host=odd))[0]
        self.assertEqual([{"reading": "host", "cause": "unparsable"}], line["missing"])
        self.assertIn("containers", line)

    def test_a_reading_that_finishes_after_its_deadline_is_a_timeout_whatever_it_returned_and_its_key_is_absent(self) -> None:
        clock = Clock()
        runner = Runner(clock, cost=1.25)
        with tempfile.TemporaryDirectory() as folder:
            line = sample_lines(run_clocked(folder, runner, clock, interval=1.0, max_seconds=1.0))[0]
        self.assertEqual([{"reading": "containers", "cause": "timeout"}, {"reading": "memory", "cause": "timeout"}], line["missing"])
        self.assertNotIn("containers", line)
        self.assertNotIn("memory", line)

    def test_a_host_reading_that_finishes_after_its_deadline_is_a_timeout_too(self) -> None:
        clock = Clock()

        def late(sources: HostSources) -> HostReading:
            clock.now += 5.0
            return read_host(sources)

        with tempfile.TemporaryDirectory() as folder, mock.patch("scripts.stack.capacity_sampler.read_host", side_effect=late):
            line = sample_lines(run_clocked(folder, Runner(clock), clock, interval=1.0, max_seconds=1.0))[0]
        self.assertEqual("host", line["missing"][0]["reading"])
        self.assertEqual("timeout", line["missing"][0]["cause"])
        self.assertNotIn("host", line)

    def test_a_missing_docker_program_refuses_to_start_before_the_output_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as folder, mock.patch("scripts.stack.capacity_sampler.shutil.which", return_value=None) as which:
            output = Path(folder) / "s.jsonl"
            errors = io.StringIO()
            with mock.patch.object(sys, "stderr", errors):
                code = run_sampler(output, interval_seconds=0.5, max_seconds=0.5, sources=sources(folder), runner=Runner(), say=lambda t: None)
            self.assertNotEqual(0, code)
            self.assertFalse(output.exists())
            self.assertEqual("capacity sampler: the docker program cannot be found\n", errors.getvalue())
            which.assert_called_once_with("docker")

    def test_the_program_is_found_once_and_every_call_gets_the_resolved_path(self) -> None:
        programs: list[str] = []
        inner = Runner()

        def recording(program: str, args: Sequence[str], *, timeout: float, **rest: object) -> CompletedProcess[str]:
            programs.append(program)
            return inner(program, args, timeout=timeout, **rest)

        with tempfile.TemporaryDirectory() as folder, mock.patch("scripts.stack.capacity_sampler.shutil.which", return_value="/opt/bin/docker") as which:
            clock = Clock()
            run_sampler(
                Path(folder) / "s.jsonl",
                interval_seconds=0.25,
                max_seconds=1.0,
                sources=sources(folder),
                runner=recording,
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
        self.assertEqual({"/opt/bin/docker"}, set(programs))
        self.assertEqual(8, len(programs))
        which.assert_called_once_with("docker")

    def test_the_wait_between_samples_is_cut_to_the_end_of_the_run(self) -> None:
        clock = Clock()
        with tempfile.TemporaryDirectory() as folder:
            found = lines_of(run_clocked(folder, Runner(clock), clock, interval=3600.0, max_seconds=0.1))
        self.assertEqual({"kind": "end", "reason": "max_seconds", "samples": 1}, found[-1])
        self.assertAlmostEqual(0.1, clock.now - 1000.0, delta=0.01)

    def test_a_short_write_is_an_append_failure_with_one_write_call_and_no_second_write(self) -> None:
        real = os.write
        calls: list[int] = []

        def short(fd: int, data: bytes) -> int:
            calls.append(len(data))
            return real(fd, data[:9]) if len(calls) == 2 else real(fd, data)

        clock, errors = Clock(), io.StringIO()
        with (
            tempfile.TemporaryDirectory() as folder,
            mock.patch("scripts.stack.capacity_sampler.os.write", side_effect=short),
            mock.patch.object(sys, "stderr", errors),
        ):
            output = Path(folder) / "s.jsonl"
            code = drive(
                output,
                interval_seconds=0.5,
                max_seconds=5.0,
                sources=sources(folder),
                runner=Runner(clock),
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
            )
            written = output.read_text()
        first, _, rest = written.partition("\n")
        self.assertNotEqual(0, code)
        self.assertEqual(2, len(calls), "one call for the header and one for the sample, never a second for the rest")
        self.assertEqual("header", json.loads(first)["kind"])
        self.assertEqual(9, len(rest.encode()), "only the nine bytes the call accepted are in the file, and the file has no end line")
        self.assertEqual("capacity sampler: cannot append to the output file\n", errors.getvalue())

    def test_nothing_is_printed_to_standard_output_and_the_messages_go_to_standard_error(self) -> None:
        clock, out, err = Clock(), io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(sys, "stdout", out), mock.patch.object(sys, "stderr", err):
            drive(
                Path(folder) / "s.jsonl",
                interval_seconds=0.5,
                max_seconds=0.5,
                sources=sources(folder),
                runner=Runner(clock),
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
            )
        self.assertEqual("", out.getvalue())
        self.assertIn("end line written", err.getvalue())

    def test_the_reading_holds_the_counters_only_and_the_memory_figures_are_raw_kib(self) -> None:
        self.assertEqual({"memory", "cpu", "disk", "free_disk_mib"}, {field.name for field in dataclasses.fields(HostReading)})
        self.assertEqual({"total_kib", "available_kib", "swap_total_kib", "swap_free_kib"}, {field.name for field in dataclasses.fields(Memory)})
        self.assertFalse(any(name.endswith("_mib") for name in dir(Memory)))


class WriterTest(unittest.TestCase):
    def test_the_file_is_created_for_its_owner_only(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            _, output, _ = sample_run(folder)
            self.assertEqual(0o600, stat.S_IMODE(output.stat().st_mode))

    def test_each_line_is_written_and_synced_before_the_next_reading_starts(self) -> None:
        order: list[str] = []
        real_write, real_sync = os.write, os.fsync
        runner = Runner()

        def tracking_runner(program: str, args: Sequence[str], *, timeout: float, **rest: object) -> CompletedProcess[str]:
            order.append(f"read {args[0]}")
            return runner(program, args, timeout=timeout, **rest)

        def tracking_write(fd: int, data: bytes) -> int:
            order.append("write")
            return real_write(fd, data)

        def tracking_sync(fd: int) -> None:
            order.append("sync")
            real_sync(fd)

        with (
            tempfile.TemporaryDirectory() as folder,
            mock.patch("scripts.stack.capacity_sampler.os.write", side_effect=tracking_write),
            mock.patch("scripts.stack.capacity_sampler.os.fsync", side_effect=tracking_sync),
        ):
            clock = Clock()
            drive(
                Path(folder) / "s.jsonl",
                interval_seconds=0.5,
                max_seconds=1.0,
                sources=sources(folder),
                runner=tracking_runner,
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
        self.assertEqual(["write", "sync"], order[:2], "the header is synced before anything is read")
        for position, item in enumerate(order):
            if item == "read ps" and position:
                self.assertEqual(["write", "sync"], order[position - 2 : position], "the line before is synced before the next reading starts")

    def test_a_broken_standard_output_does_not_touch_the_file_or_the_exit_status(self) -> None:
        class Broken(io.TextIOBase):
            def write(self, text: str) -> int:
                raise BrokenPipeError(errno.EPIPE, "broken pipe")

            def flush(self) -> None:
                raise BrokenPipeError(errno.EPIPE, "broken pipe")

            def close(self) -> None:
                return

        with tempfile.TemporaryDirectory() as folder, mock.patch.object(sys, "stdout", Broken()), mock.patch.object(sys, "stderr", io.StringIO()):
            clock = Clock()
            output = Path(folder) / "s.jsonl"
            code = drive(
                output,
                interval_seconds=0.25,
                max_seconds=0.5,
                sources=sources(folder),
                runner=Runner(clock),
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
            )
            found = lines_of(output)
        self.assertEqual((0, "end", 2), (code, found[-1]["kind"], found[-1]["samples"]))

    def test_an_append_that_fails_stops_the_sampler_with_a_fixed_message_and_the_lines_written_stay(self) -> None:
        syncs = [None, None, OSError(errno.ENOSPC, "no space left on device")]
        errors = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as folder,
            mock.patch("scripts.stack.capacity_sampler.os.fsync", side_effect=syncs),
            mock.patch.object(sys, "stderr", errors),
        ):
            clock = Clock()
            output = Path(folder) / "s.jsonl"
            code = drive(
                output,
                interval_seconds=0.25,
                max_seconds=5.0,
                sources=sources(folder),
                runner=Runner(clock),
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
            kinds = [line["kind"] for line in lines_of(output)]
        self.assertNotEqual(0, code)
        self.assertEqual("capacity sampler: cannot append to the output file\n", errors.getvalue())
        self.assertEqual(["header", "sample", "sample"], kinds)

    def test_the_end_line_that_cannot_be_written_is_a_non_zero_exit(self) -> None:
        syncs = [None, None, OSError(errno.EIO, "input/output error")]
        with (
            tempfile.TemporaryDirectory() as folder,
            mock.patch("scripts.stack.capacity_sampler.os.fsync", side_effect=syncs),
            mock.patch.object(sys, "stderr", io.StringIO()),
        ):
            clock = Clock()
            code = drive(
                Path(folder) / "s.jsonl",
                interval_seconds=0.5,
                max_seconds=0.5,
                sources=sources(folder),
                runner=Runner(clock),
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=clock.pause,
                say=lambda t: None,
            )
        self.assertNotEqual(0, code)


class StopTest(unittest.TestCase):
    def test_a_stop_flag_ends_the_loop_with_the_end_line_and_the_count_equals_the_lines_written(self) -> None:
        clock, stop = Clock(), StopFlag()

        def pause(seconds: float) -> None:
            clock.pause(seconds)
            if clock.now > 1000.6:
                stop.handle(signal.SIGINT, None)

        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "s.jsonl"
            code = drive(
                output,
                interval_seconds=0.25,
                max_seconds=60.0,
                sources=sources(folder),
                runner=Runner(clock),
                stop=stop,
                clock=clock.monotonic,
                wall_ns=clock.wall_ns,
                pause=pause,
                say=lambda t: None,
            )
            found = lines_of(output)
        self.assertEqual(0, code)
        self.assertEqual({"kind": "end", "reason": "stop_flag", "samples": len([line for line in found if line["kind"] == "sample"])}, found[-1])

    def test_the_flag_only_counts_and_a_real_signal_raises_nothing_and_the_handlers_come_back(self) -> None:
        old = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        flag = StopFlag()
        flag.install()
        try:
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGTERM)
            for _ in range(200):
                if flag.count >= 2:
                    break
                time.sleep(0.01)
        finally:
            flag.restore()
        self.assertEqual(2, flag.count)
        self.assertEqual(old, {number: signal.getsignal(number) for number in old})


class OutputTest(unittest.TestCase):
    def test_an_output_path_that_exists_is_a_link_or_has_a_dangling_link_as_a_parent_is_refused_before_anything_else(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            existing = base / "exists.jsonl"
            existing.write_text("keep\n")
            (base / "link.jsonl").symlink_to(existing)
            (base / "dangling.jsonl").symlink_to(base / "nowhere")
            (base / "dangling_parent").symlink_to(base / "nowhere")
            (base / "directory").mkdir()
            for target in (
                existing,
                base / "link.jsonl",
                base / "dangling.jsonl",
                base / "dangling_parent" / "s.jsonl",
                base / "directory",
                base / "no_such_parent" / "s.jsonl",
            ):
                with self.subTest(target=target.name), self.assertRaises(SamplerError):
                    check_output(target)
            self.assertEqual("keep\n", existing.read_text())
            check_output(base / "fresh.jsonl")
            self.assertFalse((base / "fresh.jsonl").exists(), "the check creates nothing")

    def test_the_file_is_opened_for_exclusive_creation_so_that_a_path_that_appears_after_the_check_is_never_used(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            existing = Path(folder) / "exists.jsonl"
            existing.write_text("keep\n")
            link = Path(folder) / "link.jsonl"
            link.symlink_to(existing)
            for target in (existing, link):
                with self.subTest(target=target.name), self.assertRaises(SamplerError):
                    Output(target)
            self.assertEqual("keep\n", existing.read_text())

    def test_a_file_that_appears_between_the_check_and_the_open_is_not_overwritten_and_not_appended_to(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "s.jsonl"
            check_output(target)
            target.write_text("someone else\n")
            clock = Clock()
            with mock.patch.object(sys, "stderr", io.StringIO()):
                code = drive(
                    target,
                    interval_seconds=0.5,
                    max_seconds=0.5,
                    sources=sources(folder),
                    runner=Runner(clock),
                    clock=clock.monotonic,
                    wall_ns=clock.wall_ns,
                    pause=clock.pause,
                    say=lambda t: None,
                )
            self.assertNotEqual(0, code)
            self.assertEqual("someone else\n", target.read_text())

    def test_both_durations_are_finite_above_zero_and_at_most_their_stated_upper_bound(self) -> None:
        for name, top in (("--interval-seconds", MAX_INTERVAL_SECONDS), ("--max-seconds", MAX_RUN_SECONDS)):
            for value in ("nan", "inf", "-inf", "0", "-1", "abc", "", str(top + 1)):
                with self.subTest(name=name, value=value), mock.patch.object(sys, "stderr", io.StringIO()), self.assertRaises(SystemExit):
                    parser().parse_args(["--output", "x", name, value])
            self.assertEqual(top, getattr(parser().parse_args(["--output", "x", name, str(top)]), name[2:].replace("-", "_")))

    def test_the_usage_text_says_that_the_status_tells_nothing_about_capacity_and_that_a_file_with_no_end_line_was_cut_off(self) -> None:
        text = " ".join(parser().format_help().split())
        self.assertIn("The exit status says nothing about capacity", text)
        self.assertIn("A file with no end line, or whose last line is incomplete, was cut off", text)
        self.assertIn("nothing is printed to standard output", text)


def shim(folder: Path, *, ps: str = PS_TEXT, stats: str = STATS_TEXT) -> dict[str, str]:
    """A directory with a ``docker`` program that prints the given rows, and the environment that puts it first on the path."""
    bin_folder = folder / "bin"
    bin_folder.mkdir()
    (folder / "ps.txt").write_text(ps)
    (folder / "stats.txt").write_text(stats)
    program = bin_folder / "docker"
    program.write_text('#!/bin/sh\ncase "$1" in\n  ps) cat "$SHIM_PS" ;;\n  stats) cat "$SHIM_STATS" ;;\nesac\n')
    program.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_folder}:{os.environ['PATH']}",
        "SHIM_PS": str(folder / "ps.txt"),
        "SHIM_STATS": str(folder / "stats.txt"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def start(folder: Path, env: dict[str, str], *arguments: str) -> tuple[subprocess.Popen[str], Path]:
    output = folder / "samples.jsonl"
    command = [sys.executable, "-B", "-m", "scripts.stack.capacity_sampler", "--output", str(output), *arguments]
    return subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True), output  # noqa: S603


def wait_for_samples(output: Path, count: int, process: subprocess.Popen[str]) -> None:
    for _ in range(300):
        if output.exists() and len(output.read_text().splitlines()) >= count + 1:
            return
        if process.poll() is not None:
            break
        time.sleep(0.05)
    raise AssertionError("the sampler wrote too few lines")


class EntryPointTest(unittest.TestCase):
    """The real command, in a child process, with a ``docker`` program that only prints rows."""

    def test_a_hard_kill_after_some_lines_leaves_every_line_complete_and_the_sequence_unbroken(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            process, output = start(Path(folder), shim(Path(folder)), "--interval-seconds", "0.2", "--max-seconds", "60")
            try:
                wait_for_samples(output, 3, process)
                process.send_signal(signal.SIGKILL)
                process.wait(timeout=10)
            finally:
                process.stdout and process.stdout.close()
                process.stderr and process.stderr.close()
            found = lines_of(output)
        self.assertEqual("header", found[0]["kind"])
        self.assertNotIn("end", [line["kind"] for line in found])
        self.assertEqual(list(range(len(found) - 1)), [line["seq"] for line in found[1:]])
        self.assertGreaterEqual(len(found), 4)

    def test_one_signal_and_two_signals_each_end_with_the_end_line_and_a_count_equal_to_the_lines_written(self) -> None:
        for signals in ((signal.SIGINT,), (signal.SIGINT, signal.SIGTERM)):
            with self.subTest(signals=signals), tempfile.TemporaryDirectory() as folder:
                process, output = start(Path(folder), shim(Path(folder)), "--interval-seconds", "0.2", "--max-seconds", "60")
                try:
                    wait_for_samples(output, 2, process)
                    for number in signals:
                        process.send_signal(number)
                    out, err = process.communicate(timeout=20)
                finally:
                    process.kill()
                found = lines_of(output)
                self.assertEqual(0, process.returncode, err)
                self.assertEqual({"kind": "end", "reason": "stop_flag", "samples": len(found) - 2}, found[-1])
                self.assertEqual(list(range(len(found) - 2)), [line["seq"] for line in found[1:-1]])

    def test_the_command_writes_each_line_once_refuses_an_existing_output_and_prints_no_word_of_judgment(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            env = shim(Path(folder))
            process, output = start(Path(folder), env, "--interval-seconds", "0.1", "--max-seconds", "0.5")
            out, err = process.communicate(timeout=30)
            first = output.read_text()
            again = subprocess.run(  # noqa: S603
                [sys.executable, "-B", "-m", "scripts.stack.capacity_sampler", "--output", str(output), "--max-seconds", "1"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(first, output.read_text(), "a second run did not touch the file")
        self.assertEqual(0, process.returncode, err)
        self.assertNotEqual(0, again.returncode)
        seqs = [line["seq"] for line in map(json.loads, first.splitlines()) if line["kind"] == "sample"]
        self.assertEqual(len(seqs), len(set(seqs)))
        for text in (out, err, again.stdout, again.stderr, first):
            self.assertEqual([], JUDGMENT_WORDS.findall(text), text)
        self.assertEqual([], [text for line in map(json.loads, first.splitlines()) for text in every_key_and_text(line) if JUDGMENT_WORDS.search(text)])

    def test_the_scan_does_catch_a_word_of_judgment(self) -> None:
        for text in ("ok", "capacity passed", "complete", "within limits", "sufficient memory", "Completed"):
            self.assertTrue(JUDGMENT_WORDS.search(text), text)

    def test_a_standard_output_that_goes_nowhere_does_not_stop_the_command(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "samples.jsonl"
            result = subprocess.run(  # noqa: S603
                ["/bin/sh", "-c", f'"{sys.executable}" -B -m scripts.stack.capacity_sampler --output "{output}" --interval-seconds 0.1 --max-seconds 0.4 >&-'],
                cwd=ROOT, env=shim(Path(folder)), capture_output=True, text=True,
            )  # fmt: skip
            found = lines_of(output)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("end", found[-1]["kind"])


class ClosedPipeTest(unittest.TestCase):
    def test_a_standard_output_pipe_whose_reader_has_closed_leaves_status_zero_and_the_end_line(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "samples.jsonl"
            read_end, write_end = os.pipe()
            os.close(read_end)
            command = [
                sys.executable,
                "-B",
                "-m",
                "scripts.stack.capacity_sampler",
                "--output",
                str(output),
                "--interval-seconds",
                "0.1",
                "--max-seconds",
                "0.5",
            ]
            try:
                process = subprocess.Popen(command, cwd=ROOT, env=shim(Path(folder)), stdout=write_end, stderr=subprocess.PIPE, text=True)  # noqa: S603
            finally:
                os.close(write_end)
            try:
                _, err = process.communicate(timeout=30)
            finally:
                process.kill()
            found = lines_of(output)
        self.assertEqual(0, process.returncode, err)
        self.assertEqual("end", found[-1]["kind"])


class SourceTest(unittest.TestCase):
    def test_the_sampler_module_has_no_path_to_starting_or_stopping_anything(self) -> None:
        source = (ROOT / "scripts" / "stack" / "capacity_sampler.py").read_text()
        for name in (
            "compose_arguments",
            "up_arguments",
            "start_group",
            "NessieClient",
            "docker rm",
            "docker events",
            "admit(",
            "available_mib",
            "STACK_TEST_AVAILABLE_MIB",
        ):
            self.assertNotIn(name, source)


if __name__ == "__main__":
    unittest.main()
