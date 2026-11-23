"""The disk summary turns the capacity sampler's files for the copy and for the retry into the peak staging use and the lowest free disk space.

Every case writes sampler files in the sampler's own form with figures that a reader can add up by hand, then runs the real summary entry point.
A file that cannot give a figure is refused, never summarised with a zero or a null.
"""

from __future__ import annotations

import contextlib
import io
import json
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.capacity_sampler import SCHEMA
from scripts.stack.disk_summary import SUMMARY_FORMAT, SummaryError, main, summarize

GIB = 1024**3
STAGE = "airbyte_stage"
CHECKS = "table_check_stage"
PATTERNS = {STAGE: "/var/lib/rancher/k3s/agent/kubelet/pods/*/volumes/kubernetes.io~empty-dir/*", CHECKS: "/var/lib/docker/volumes/*table_check_staging/_data"}


def header(*, schema: int = SCHEMA, staging: dict[str, str] | None = None, interval: float = 5.0) -> dict[str, Any]:
    sources: dict[str, Any] = {"memory": "/proc/meminfo", "cpu": "/proc/stat", "disk": "/proc/diskstats", "free_disk": "/", "limits": "compose/limits.yml"}
    if staging is not None:
        sources["staging"] = staging
    return {"kind": "header", "schema": schema, "interval_seconds": interval, "max_seconds": 600.0, "sources": sources, "configured": {}}


def rows(**allocated: int) -> dict[str, Any]:
    return {"rows": [{"name": name, "matches": 1, "files": 2, "apparent_bytes": size - 7, "allocated_bytes": size} for name, size in allocated.items()]}


def sample(seq: int, free: float | None, staging: dict[str, Any] | None, *, missing: list[dict[str, str]] | None = None) -> dict[str, Any]:
    line: dict[str, Any] = {"kind": "sample", "seq": seq, "missing": missing or []}
    if free is not None:
        line["host"] = {"free_disk_mib": free, "memory_kib": {}, "cpu": {}, "disk": {}}
    if staging is not None:
        line["staging"] = staging
    return line


def write(folder: str, name: str, lines: list[dict[str, Any]], *, end: bool = True) -> Path:
    path = Path(folder) / name
    final = [{"kind": "end", "reason": "stop_flag", "samples": sum(1 for line in lines if line["kind"] == "sample")}] if end else []
    path.write_text("".join(json.dumps(line) + "\n" for line in [*lines, *final]))
    return path


def copy_lines(*, patterns: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Growth to a peak of 4.5 GiB at sample 2 and back, and the lowest free space at the same sample, with a drop and a rise on each side."""
    return [
        header(staging=PATTERNS if patterns is None else patterns),
        sample(0, 150000.0, rows(**{STAGE: 0, CHECKS: 0})),
        sample(1, 148000.0, rows(**{STAGE: 2 * GIB, CHECKS: 100})),
        sample(2, 145500.0, rows(**{STAGE: 4 * GIB + GIB // 2, CHECKS: 300})),
        sample(3, 146000.0, rows(**{STAGE: 3 * GIB, CHECKS: 200})),
        sample(4, 150000.0, rows(**{STAGE: 0, CHECKS: 0})),
    ]


def retry_lines() -> list[dict[str, Any]]:
    """A retry that stages a little less and whose lowest free space comes at the first sample, so a first-value or last-value reading is wrong."""
    return [
        header(staging=PATTERNS),
        sample(0, 140000.0, rows(**{STAGE: GIB, CHECKS: 0})),
        sample(1, 141000.0, rows(**{STAGE: 3 * GIB, CHECKS: 0})),
        sample(2, 149000.0, rows(**{STAGE: 2 * GIB, CHECKS: 0})),
    ]


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


class SummaryTest(unittest.TestCase):
    def phases(self, folder: str, copy: list[dict[str, Any]] | None = None, retry: list[dict[str, Any]] | None = None) -> dict[str, Path]:
        return {"copy": write(folder, "copy.jsonl", copy or copy_lines()), "retry": write(folder, "retry.jsonl", retry or retry_lines())}

    def test_control_the_peak_and_the_lowest_free_space_of_the_copy_and_of_the_retry_are_the_figures_of_the_files(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            document = summarize(self.phases(folder), {})
        self.assertEqual(SUMMARY_FORMAT, document["format"])
        copy, retry = document["phases"]["copy"], document["phases"]["retry"]
        self.assertEqual({"lowest_mib": 145500.0, "at_seq": 2, "first_mib": 150000.0, "readings": 5, "source": "/"}, copy["free_disk"])
        self.assertEqual({"lowest_mib": 140000.0, "at_seq": 0, "first_mib": 140000.0, "readings": 3, "source": "/"}, retry["free_disk"])
        staged = {row["name"]: row for row in copy["staging"]}
        self.assertEqual((4 * GIB + GIB // 2, 2, 5), (staged[STAGE]["peak_allocated_bytes"], staged[STAGE]["at_seq"], staged[STAGE]["readings"]))
        self.assertEqual(4 * GIB + GIB // 2 - 7, staged[STAGE]["peak_apparent_bytes"])
        self.assertEqual((300, 2), (staged[CHECKS]["peak_allocated_bytes"], staged[CHECKS]["at_seq"]))
        self.assertEqual(PATTERNS[STAGE], staged[STAGE]["pattern"])
        self.assertEqual(3 * GIB, {row["name"]: row for row in retry["staging"]}[STAGE]["peak_allocated_bytes"])
        self.assertEqual((5, 5.0, True), (copy["samples"], copy["interval_seconds"], copy["end_reason"] == "stop_flag"))

    def test_the_peak_is_the_largest_reading_and_not_the_last_or_the_first_and_the_lowest_free_space_is_the_smallest(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            document = summarize(self.phases(folder), {})
        self.assertNotEqual(0, document["phases"]["copy"]["staging"][0]["peak_allocated_bytes"], "the last reading of the copy is 0")
        self.assertNotEqual(GIB, document["phases"]["retry"]["staging"][0]["peak_allocated_bytes"], "the first reading of the retry is 1 GiB")
        self.assertNotEqual(149000.0, document["phases"]["retry"]["free_disk"]["lowest_mib"], "the last reading of the retry is 149000")

    def test_a_sample_that_lacks_a_reading_is_skipped_for_that_figure_and_counted(self) -> None:
        lines = copy_lines()
        lines[2] = sample(2, None, rows(**{STAGE: 4 * GIB + GIB // 2, CHECKS: 300}), missing=[{"reading": "host", "cause": "os_error"}])
        lines[3] = sample(3, 146000.0, None, missing=[{"reading": "staging", "cause": "timeout"}])
        with tempfile.TemporaryDirectory() as folder:
            copy = summarize(self.phases(folder, copy=lines), {})["phases"]["copy"]
        self.assertEqual((146000.0, 3, 4), (copy["free_disk"]["lowest_mib"], copy["free_disk"]["at_seq"], copy["free_disk"]["readings"]))
        self.assertEqual({"host": 1, "staging": 1}, copy["missing"])
        self.assertEqual(4, copy["staging"][0]["readings"])

    def test_a_limit_gives_the_headroom_as_a_figure_with_no_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            document = summarize(self.phases(folder), {STAGE: 5 * GIB})
        copy = {row["name"]: row for row in document["phases"]["copy"]["staging"]}
        retry = {row["name"]: row for row in document["phases"]["retry"]["staging"]}
        self.assertEqual((5 * GIB, GIB // 2), (copy[STAGE]["limit_bytes"], copy[STAGE]["headroom_bytes"]))
        self.assertEqual(2 * GIB, retry[STAGE]["headroom_bytes"])
        self.assertNotIn("limit_bytes", copy[CHECKS])
        with tempfile.TemporaryDirectory() as folder:
            over = {row["name"]: row for row in summarize(self.phases(folder), {STAGE: 4 * GIB})["phases"]["copy"]["staging"]}
        self.assertEqual(-(GIB // 2), over[STAGE]["headroom_bytes"], "a peak above the limit is a negative headroom")

    def test_a_third_phase_is_summarised_beside_the_two_required_ones(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            phases = self.phases(folder) | {"limit": write(folder, "limit.jsonl", copy_lines())}
            document = summarize(phases, {})
        self.assertEqual(["copy", "retry", "limit"], list(document["phases"]))


class RefusalTest(unittest.TestCase):
    def refused(
        self, copy: list[dict[str, Any]] | None = None, retry: list[dict[str, Any]] | None = None, *, end: bool = True, schema: int | None = None
    ) -> str:
        with tempfile.TemporaryDirectory() as folder:
            phases = {"copy": write(folder, "copy.jsonl", copy or copy_lines(), end=end), "retry": write(folder, "retry.jsonl", retry or retry_lines())}
            if schema is not None:
                phases["copy"] = write(folder, "old.jsonl", [header(schema=schema, staging=PATTERNS), *copy_lines()[1:]])
            with self.assertRaises(SummaryError) as caught:
                summarize(phases, {})
        return str(caught.exception)

    def test_a_file_with_no_end_line_was_cut_off_and_is_refused_by_its_phase(self) -> None:
        message = self.refused(end=False)
        self.assertIn("copy", message)
        self.assertIn("cut off", message)
        self.assertIn("no end line", message)

    def test_a_file_whose_every_staging_reading_is_missing_has_no_peak_and_is_refused(self) -> None:
        lines = [copy_lines()[0], *(sample(n, 150000.0, None, missing=[{"reading": "staging", "cause": "os_error"}]) for n in range(3))]
        message = self.refused(copy=lines)
        self.assertIn("copy", message)
        self.assertIn("staging", message)

    def test_a_file_whose_every_host_reading_is_missing_has_no_lowest_free_space_and_is_refused(self) -> None:
        lines = [copy_lines()[0], *(sample(n, None, rows(**{STAGE: n}), missing=[{"reading": "host", "cause": "os_error"}]) for n in range(3))]
        message = self.refused(copy=lines)
        self.assertIn("free disk", message)

    def test_a_run_started_with_no_staging_source_is_refused(self) -> None:
        lines = [header(staging=None), sample(0, 150000.0, None), sample(1, 149000.0, None)]
        self.assertIn("staging source", self.refused(copy=lines))

    def test_a_file_of_the_earlier_schema_is_refused(self) -> None:
        self.assertIn("schema", self.refused(schema=SCHEMA - 1))

    def test_a_staging_name_that_a_sample_holds_and_the_header_does_not_is_refused(self) -> None:
        lines = [copy_lines()[0], sample(0, 150000.0, rows(stranger=5))]
        self.assertIn("not in the header", self.refused(copy=lines))

    def test_a_line_that_is_not_json_or_has_another_kind_is_refused_without_repeating_its_text(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "copy.jsonl"
            path.write_text(json.dumps(copy_lines()[0]) + "\nsecret-looking text\n")
            with self.assertRaises(SummaryError) as caught:
                summarize({"copy": path, "retry": write(folder, "r.jsonl", retry_lines())}, {})
        self.assertNotIn("secret", str(caught.exception))
        with tempfile.TemporaryDirectory() as folder:
            odd = [copy_lines()[0], {"kind": "mystery"}]
            self.assertTrue(self.refused(copy=odd))

    def test_a_reading_that_is_not_a_number_is_refused_as_a_file_not_in_the_sampler_s_form(self) -> None:
        lines = [copy_lines()[0], sample(0, 150000.0, rows(**{STAGE: 1, CHECKS: 1})), sample(1, "lots", rows(**{STAGE: 2, CHECKS: 2}))]  # type: ignore[arg-type]
        self.assertIn("not in the form", self.refused(copy=lines))

    def test_the_header_must_come_first_and_the_end_line_last_and_each_once(self) -> None:
        body = copy_lines()
        with tempfile.TemporaryDirectory() as folder:
            path = write(folder, "twice.jsonl", [body[0], *body[1:3], body[0], *body[3:]])
            with self.assertRaises(SummaryError) as twice:
                summarize({"copy": path, "retry": write(folder, "r.jsonl", retry_lines())}, {})
            moved = write(folder, "moved.jsonl", [*body[1:3], body[0], *body[3:]])
            with self.assertRaises(SummaryError) as late:
                summarize({"copy": moved, "retry": write(folder, "r2.jsonl", retry_lines())}, {})
        self.assertIn("another kind", str(twice.exception))
        self.assertIn("does not start with one header line", str(late.exception))

    def test_the_end_line_count_must_equal_the_samples_read(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = write(folder, "copy.jsonl", copy_lines())
            lines = path.read_text().splitlines()
            lines[-1] = json.dumps({"kind": "end", "reason": "stop_flag", "samples": 99})
            path.write_text("\n".join(lines) + "\n")
            with self.assertRaises(SummaryError) as caught:
                summarize({"copy": path, "retry": write(folder, "r.jsonl", retry_lines())}, {})
        self.assertIn("count", str(caught.exception))

    def test_the_copy_and_the_retry_are_both_required_and_no_other_name_replaces_them(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            only = write(folder, "copy.jsonl", copy_lines())
            for phases in ({"copy": only}, {"retry": only}, {"copy": only, "second": only}):
                with self.subTest(phases=sorted(phases)), self.assertRaises(SummaryError) as caught:
                    summarize(phases, {})
                self.assertIn("copy and retry", str(caught.exception))

    def test_one_file_named_for_two_phases_is_refused_whatever_its_path(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            one = write(folder, "one.jsonl", copy_lines())
            (Path(folder) / "alias.jsonl").symlink_to(one)
            (Path(folder) / "twin.jsonl").hardlink_to(one)
            for second in (one, Path(folder) / "alias.jsonl", Path(folder) / "twin.jsonl"):
                with self.subTest(second=second.name), self.assertRaises(SummaryError) as caught:
                    summarize({"copy": one, "retry": second}, {})
                self.assertIn("two phases", str(caught.exception))
            with self.assertRaises(SummaryError) as third:
                summarize({"copy": one, "retry": write(folder, "r.jsonl", retry_lines()), "limit": one}, {})
        self.assertIn("two phases", str(third.exception))

    def test_a_header_whose_staging_name_or_pattern_is_not_in_the_sampler_s_form_is_refused(self) -> None:
        bad = {"Upper": "/a/*", "ok_name": "relative/path", "bad name": "/a", "ok": "/a\n"}
        for name, pattern in bad.items():
            with self.subTest(name=name):
                lines = [header(staging={name: pattern}), sample(0, 150000.0, rows(**{name: 1}))]
                self.assertIn("staging", self.refused(copy=lines))

    def test_a_header_whose_free_disk_source_or_interval_is_not_a_plain_value_is_refused(self) -> None:
        for change in (
            {"interval_seconds": "five"},
            {"interval_seconds": True},
            {"sources": {"free_disk": 7, "staging": PATTERNS}},
            {"sources": {"free_disk": "/a\nb", "staging": PATTERNS}},
        ):
            with self.subTest(change=change):
                first = {**header(staging=PATTERNS), **change}
                self.assertIn("header", self.refused(copy=[first, *copy_lines()[1:]]))

    def test_a_missing_reading_with_a_name_the_sampler_does_not_write_is_refused_without_repeating_it(self) -> None:
        lines = copy_lines()
        lines[1] = sample(0, 150000.0, rows(**{STAGE: 0, CHECKS: 0}), missing=[{"reading": "secret-looking", "cause": "os_error"}])
        message = self.refused(copy=lines)
        self.assertIn("missing", message)
        self.assertNotIn("secret-looking", message)

    def test_an_end_line_with_a_reason_the_sampler_does_not_write_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = write(folder, "copy.jsonl", copy_lines())
            lines = path.read_text().splitlines()
            lines[-1] = json.dumps({"kind": "end", "reason": "secret-looking reason", "samples": 5})
            path.write_text("\n".join(lines) + "\n")
            with self.assertRaises(SummaryError) as caught:
                summarize({"copy": path, "retry": write(folder, "r.jsonl", retry_lines())}, {})
        self.assertIn("reason", str(caught.exception))
        self.assertNotIn("secret-looking", str(caught.exception))

    def test_a_limit_for_a_name_no_file_holds_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            phases = {"copy": write(folder, "c.jsonl", copy_lines()), "retry": write(folder, "r.jsonl", retry_lines())}
            with self.assertRaises(SummaryError) as caught:
                summarize(phases, {"stranger": 1})
        self.assertIn("limit", str(caught.exception))


class CommandTest(unittest.TestCase):
    def arguments(self, folder: str, output: str = "disk.json", *extra: str) -> list[str]:
        copy, retry = write(folder, "copy.jsonl", copy_lines()), write(folder, "retry.jsonl", retry_lines())
        return ["--phase", f"copy={copy}", "--phase", f"retry={retry}", "--output", str(Path(folder) / output), *extra]

    def test_the_command_writes_the_record_for_its_owner_only_and_prints_each_figure_once(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, out, err = run(*self.arguments(folder, "disk.json", "--limit", f"{STAGE}={5 * GIB}"))
            path = Path(folder) / "disk.json"
            document = json.loads(path.read_text())
            mode = stat.S_IMODE(path.stat().st_mode)
        self.assertEqual((0, ""), (code, err))
        self.assertEqual(0o600, mode)
        self.assertEqual(4 * GIB + GIB // 2, document["phases"]["copy"]["staging"][0]["peak_allocated_bytes"])
        self.assertIn("copy", out)
        self.assertIn("retry", out)

    def test_the_same_files_give_the_same_bytes_in_two_records(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            first = self.arguments(folder, "one.json")
            self.assertEqual(0, run(*first)[0])
            second = [*first[:-1], str(Path(folder) / "two.json")]
            self.assertEqual(0, run(*second)[0])
            self.assertEqual((Path(folder) / "one.json").read_text(), (Path(folder) / "two.json").read_text())

    def test_an_output_that_exists_is_refused_and_left_as_it_was(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            existing = Path(folder) / "disk.json"
            existing.write_text("kept")
            code, _, err = run(*self.arguments(folder))
            self.assertEqual("kept", existing.read_text())
        self.assertEqual(1, code)
        self.assertIn("exists", err)

    def test_a_refused_file_leaves_no_output(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            arguments = self.arguments(folder)
            write(folder, "retry.jsonl", retry_lines(), end=False)
            code, _, err = run(*arguments)
            self.assertFalse((Path(folder) / "disk.json").exists())
        self.assertEqual(1, code)
        self.assertIn("retry", err)

    def test_the_same_file_for_two_phases_writes_no_record(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            one = write(folder, "one.jsonl", copy_lines())
            code, _, err = run("--phase", f"copy={one}", "--phase", f"retry={one}", "--output", str(Path(folder) / "disk.json"))
            self.assertEqual((1, False), (code, (Path(folder) / "disk.json").exists()))
        self.assertIn("two phases", err)

    def test_an_option_the_command_does_not_know_is_refused_without_repeating_its_value(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, out, err = run(*self.arguments(folder), "--bogus", "private-value", "--phasee=private-too")
            self.assertEqual((2, False), (code, (Path(folder) / "disk.json").exists()))
        self.assertEqual([], [word for word in ("private-value", "private-too", "--bogus") if word in out + err])

    def test_a_phase_named_twice_is_a_usage_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            arguments = self.arguments(folder)
            code, _, _ = run(*arguments, "--phase", f"copy={Path(folder) / 'copy.jsonl'}")
            self.assertEqual((2, False), (code, (Path(folder) / "disk.json").exists()))

    def test_a_malformed_limit_or_phase_is_a_usage_refusal_that_repeats_no_word_of_it(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            for bad in (
                ["--limit", "no_equals_sign"],
                ["--limit", f"{STAGE}=-5"],
                ["--limit", f"{STAGE}=1.5"],
                ["--phase", "noequals"],
                ["--phase", "UPPER=/x"],
            ):
                with self.subTest(bad=bad):
                    code, out, err = run(*self.arguments(folder), *bad)
                    self.assertNotEqual(0, code)
                    self.assertEqual([], [word for word in ("no_equals_sign", "UPPER", "noequals") if word in err + out])


if __name__ == "__main__":
    unittest.main()
