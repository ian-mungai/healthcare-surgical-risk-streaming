"""The copy cost record: the request and transfer cost of the copy, estimated from the passed check result's object counts and bytes.

Every case builds a check result with sizes that a reader can add up by hand, runs the real command and reads the record it writes. The expected figures
are computed here from the published prices, not taken from the code under test. A result that is not a passed, unedited one is refused with no output.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import stat
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from scripts.ingestion.check import CheckedFile, CheckResult, Finding
from scripts.ingestion.copy_cost import COST_FORMAT, CostError, estimate, main
from scripts.ingestion.notice import Cause, ClosedError

MIB = 1024**2
GIB = 1024**3
SIZES = (1_000_000, 5 * MIB, 5 * MIB + 1, 1_610_612_736)
BUCKET = "imungai-example-bucket"
PUT_PRICE = Decimal("0.000005")
INTER_REGION_PER_GIB = Decimal("0.02")
SIX_PLACES = Decimal("0.000001")


RETRIEVED = "2026-11-23T10:00:00+00:00"
SINCE = datetime(2026, 11, 23, 9, tzinfo=UTC)


def checked(number: int, size: int, retrieved: str = RETRIEVED) -> CheckedFile:
    return CheckedFile(
        BUCKET,
        f"landing/file_{number}.vital",
        f"version{number}",
        "ab" * 32,
        f"vitaldb/1.0.0/vital_files/{number:04d}.vital",
        size,
        "1.0.0",
        retrieved,
    )


def result_for(
    sizes: tuple[int, ...] = SIZES, *, findings: tuple[Finding, ...] = (), times: tuple[str, ...] | None = None, name: str = "full_release"
) -> CheckResult:
    files = tuple(checked(number, size, times[number - 1] if times else RETRIEVED) for number, size in enumerate(sizes, 1))
    bare = CheckResult(name, "cd" * 32, "ef" * 32, len(files), files, findings)
    return CheckResult(bare.selection_name, bare.selection_sha256, bare.list_sha256, bare.file_count, bare.files, bare.findings, bare.result_sha256)


def run(*arguments: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def high_requests(sizes: tuple[int, ...], extra: int) -> int:
    """Each object as a multipart upload of 5 MiB parts, the smallest S3 allows: an initiate, the parts and a complete. Each extra object has one part."""
    return sum(max(1, math.ceil(size / (5 * MIB))) + 2 for size in sizes) + 3 * extra


class EstimateTest(unittest.TestCase):
    def test_control_the_request_cost_is_the_put_tier_price_for_the_lowest_and_the_highest_count_of_write_requests(self) -> None:
        record = estimate(result_for(), region="us-west-2", extra_objects=2, since=SINCE)
        low, high = len(SIZES) + 2, high_requests(SIZES, 2)
        self.assertEqual({"low": low, "high": high}, record["write_requests"])
        self.assertEqual({"low": str((low * PUT_PRICE).quantize(SIX_PLACES)), "high": str((high * PUT_PRICE).quantize(SIX_PLACES))}, record["request_cost_usd"])
        self.assertEqual({"verified_objects": 4, "payload_objects": 4, "payload_bytes": sum(SIZES), "extra_objects": 2}, record["copy"])
        self.assertEqual(COST_FORMAT, record["format"])

    def test_the_transfer_billed_to_the_project_is_zero_and_the_same_bytes_cost_the_inter_region_price_if_the_publisher_bucket_billed_the_requester(
        self,
    ) -> None:
        record = estimate(result_for(), region="us-west-2", extra_objects=0, since=SINCE)
        scenario = (Decimal(sum(SIZES)) / Decimal(GIB) * INTER_REGION_PER_GIB).quantize(SIX_PLACES)
        self.assertEqual("0.000000", record["transfer_cost_usd"]["billed_to_project"])
        self.assertEqual(str(scenario), record["transfer_cost_usd"]["if_requester_pays"])
        self.assertGreater(scenario, 0)

    def test_the_total_is_the_request_cost_alone_because_the_transfer_billed_to_the_project_is_zero(self) -> None:
        record = estimate(result_for(), region="us-west-2", extra_objects=0, since=SINCE)
        self.assertEqual(record["request_cost_usd"], record["total_usd"])

    def test_the_record_names_its_prices_their_sources_and_what_it_leaves_out(self) -> None:
        record = estimate(result_for(), region="us-west-2", extra_objects=0, since=SINCE)
        self.assertEqual("2026-11-23", record["prices"]["read_on"])
        self.assertTrue(all(source.startswith("https://") for source in record["prices"]["sources"]))
        self.assertEqual(
            {"put_tier_usd_per_request": "0.000005", "inter_region_out_usd_per_gib": "0.02"},
            {k: record["prices"][k] for k in ("put_tier_usd_per_request", "inter_region_out_usd_per_gib")},
        )
        self.assertGreaterEqual(len(record["not_counted"]), 3)

    def test_the_record_holds_the_check_result_s_digest_and_no_key_bucket_or_version(self) -> None:
        result = result_for()
        text = json.dumps(estimate(result, region="us-west-2", extra_objects=0, since=SINCE))
        self.assertIn(result.result_sha256, text)
        for private in (BUCKET, "landing/", "version1", "vitaldb/"):
            self.assertNotIn(private, text)

    def test_a_result_that_has_not_passed_is_refused(self) -> None:
        failing = result_for(findings=(Finding(1, Cause.LANDED_OBJECT_MISSING, "vitaldb/1.0.0/vital_files/0001.vital"),))
        with self.assertRaises(ClosedError) as caught:
            estimate(failing, region="us-west-2", extra_objects=0, since=SINCE)
        self.assertIn("did not pass", str(caught.exception))

    def test_a_region_without_published_prices_in_the_code_is_refused(self) -> None:
        with self.assertRaises(CostError) as caught:
            estimate(result_for(), region="eu-west-1", extra_objects=0, since=SINCE)
        self.assertIn("region", str(caught.exception))

    def test_a_negative_count_of_extra_objects_is_refused(self) -> None:
        with self.assertRaises(CostError) as caught:
            estimate(result_for(), region="us-west-2", extra_objects=-1, since=SINCE)
        self.assertIn("extra", str(caught.exception))


class CommandTest(unittest.TestCase):
    def arguments(self, folder: str, result: CheckResult | None = None, *extra: str) -> list[str]:
        path = Path(folder) / "result.json"
        path.write_text((result or result_for()).to_json())
        return [
            "--result",
            str(path),
            "--region",
            "us-west-2",
            "--since",
            "2026-11-23T09:00:00Z",
            "--extra-objects",
            "2",
            "--output",
            str(Path(folder) / "copy_cost.json"),
            *extra,
        ]

    def test_the_command_writes_the_record_for_its_owner_only_and_the_record_equals_the_estimate(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            code, out, err = run(*self.arguments(folder))
            path = Path(folder) / "copy_cost.json"
            document = json.loads(path.read_text())
            mode = stat.S_IMODE(path.stat().st_mode)
        self.assertEqual((0, ""), (code, err))
        self.assertEqual(0o600, mode)
        self.assertEqual(estimate(result_for(), region="us-west-2", extra_objects=2, since=SINCE), document)
        self.assertIn(document["request_cost_usd"]["high"], out)

    def test_the_same_result_gives_the_same_bytes_in_two_records(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            first = self.arguments(folder)
            self.assertEqual(0, run(*first)[0])
            second = [*first[:-1], str(Path(folder) / "again.json")]
            self.assertEqual(0, run(*second)[0])
            self.assertEqual((Path(folder) / "copy_cost.json").read_text(), (Path(folder) / "again.json").read_text())

    def test_an_edited_result_is_refused_with_no_output(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            arguments = self.arguments(folder)
            path = Path(arguments[1])
            document: dict[str, Any] = json.loads(path.read_text())
            document["files"][0]["size"] += 1
            path.write_text(json.dumps(document))
            code, _, err = run(*arguments)
            self.assertFalse((Path(folder) / "copy_cost.json").exists())
        self.assertEqual(1, code)
        self.assertIn("edited", err)

    def test_a_result_that_is_not_a_result_is_refused_without_repeating_its_text(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            arguments = self.arguments(folder)
            Path(arguments[1]).write_text("private-looking text")
            code, out, err = run(*arguments)
        self.assertEqual(1, code)
        self.assertNotIn("private-looking", out + err)

    def test_an_output_that_exists_is_refused_and_left_as_it_was(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            existing = Path(folder) / "copy_cost.json"
            existing.write_text("kept")
            code, _, err = run(*self.arguments(folder))
            self.assertEqual("kept", existing.read_text())
        self.assertEqual(1, code)
        self.assertIn("exists", err)

    def test_a_count_of_extra_objects_that_is_not_a_whole_number_is_a_usage_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            arguments = self.arguments(folder)
            arguments[arguments.index("--extra-objects") + 1] = "1.5"
            code, out, err = run(*arguments)
            self.assertEqual((2, False), (code, (Path(folder) / "copy_cost.json").exists()))
        self.assertNotIn("1.5", out + err)


class RunWindowTest(unittest.TestCase):
    """The copy is incremental: a run writes only the versions that are new, so only the versions written inside the run's window cost requests."""

    def run_with(self, result: CheckResult, *window: str, extra: str = "0") -> tuple[int, dict[str, Any], str]:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            path.write_text(result.to_json())
            output = Path(folder) / "copy_cost.json"
            code, _, err = run("--result", str(path), "--region", "us-west-2", "--extra-objects", extra, "--output", str(output), *window)
            return code, (json.loads(output.read_text()) if output.exists() else {}), err

    def test_a_run_that_wrote_nothing_estimates_zero_writes_though_every_object_is_verified(self) -> None:
        code, record, err = self.run_with(result_for(), "--since", "2026-11-23T11:00:00Z")
        self.assertEqual((0, ""), (code, err))
        self.assertEqual(
            {"payload_objects": 0, "payload_bytes": 0, "extra_objects": 0},
            {k: record["copy"][k] for k in ("payload_objects", "payload_bytes", "extra_objects")},
        )
        self.assertEqual({"low": 0, "high": 0}, record["write_requests"])
        self.assertEqual({"low": "0.000000", "high": "0.000000"}, record["request_cost_usd"])
        self.assertEqual(len(SIZES), record["copy"]["verified_objects"])

    def test_only_the_versions_written_inside_the_window_are_counted(self) -> None:
        old, new = "2026-11-23T08:00:00+00:00", "2026-11-23T12:00:00+00:00"
        code, record, _ = self.run_with(result_for(times=(old, new, old, new)), "--since", "2026-11-23T11:00:00Z")
        counted = (SIZES[1], SIZES[3])
        self.assertEqual(0, code)
        self.assertEqual((2, sum(counted)), (record["copy"]["payload_objects"], record["copy"]["payload_bytes"]))
        self.assertEqual({"low": 2, "high": high_requests(counted, 0)}, record["write_requests"])

    def test_the_window_starts_at_its_first_instant_and_ends_before_its_last_when_an_end_is_given(self) -> None:
        times = ("2026-11-23T11:00:00+00:00", "2026-11-23T11:59:59+00:00", "2026-11-23T12:00:00+00:00", "2026-11-23T10:59:59+00:00")
        _, record, _ = self.run_with(result_for(times=times), "--since", "2026-11-23T11:00:00Z", "--until", "2026-11-23T12:00:00Z")
        self.assertEqual(
            (2, SIZES[0] + SIZES[1]), (record["copy"]["payload_objects"], record["copy"]["payload_bytes"]), "the first instant counts, the end does not"
        )

    def test_a_window_is_read_with_its_time_zone(self) -> None:
        _, record, _ = self.run_with(result_for(), "--since", "2026-11-23T11:30:00+02:00")
        self.assertEqual(len(SIZES), record["copy"]["payload_objects"], "11:30 at +02:00 is 09:30 UTC, before the objects' 10:00 UTC")

    def test_the_record_names_its_window_and_the_sensitivity_figure_counts_only_the_bytes_written_in_it(self) -> None:
        old, new = "2026-11-23T08:00:00+00:00", "2026-11-23T12:00:00+00:00"
        _, record, _ = self.run_with(result_for(times=(old, new, old, new)), "--since", "2026-11-23T11:00:00Z", "--until", "2026-11-23T13:00:00Z")
        written = SIZES[1] + SIZES[3]
        self.assertEqual({"since": "2026-11-23T11:00:00+00:00", "until": "2026-11-23T13:00:00+00:00"}, record["window"])
        self.assertEqual(str((Decimal(written) / Decimal(GIB) * INTER_REGION_PER_GIB).quantize(SIX_PLACES)), record["transfer_cost_usd"]["if_requester_pays"])

    def test_the_extra_objects_of_a_run_that_wrote_no_payload_are_still_counted(self) -> None:
        _, record, _ = self.run_with(result_for(), "--since", "2026-11-23T11:00:00Z", extra="2")
        self.assertEqual({"low": 2, "high": 6}, record["write_requests"])

    def test_a_window_with_no_start_a_start_without_a_zone_or_an_end_before_the_start_is_refused_with_no_output(self) -> None:
        for window in (
            (),
            ("--since", "2026-11-23T11:00:00"),
            ("--since", "yesterday"),
            ("--since", "2026-11-23T12:00:00Z", "--until", "2026-11-23T11:00:00Z"),
        ):
            with self.subTest(window=window):
                code, record, _ = self.run_with(result_for(), *window)
                self.assertNotEqual(0, code)
                self.assertEqual({}, record)

    def test_a_result_whose_retrieval_time_is_not_a_time_is_refused_with_no_output(self) -> None:
        code, record, err = self.run_with(result_for(times=("not a time",) * len(SIZES)), "--since", "2026-11-23T09:00:00Z")
        self.assertEqual((1, {}), (code, record))
        self.assertIn("time", err)
        self.assertNotIn("not a time", err)

    def test_an_edited_label_of_the_result_is_not_copied_into_the_record(self) -> None:
        _, record, _ = self.run_with(result_for(name="SECRET-LABEL"), "--since", "2026-11-23T09:00:00Z")
        self.assertEqual(len(SIZES), record["copy"]["payload_objects"])
        self.assertNotIn("SECRET-LABEL", json.dumps(record))

    def test_an_option_the_command_does_not_know_is_refused_without_repeating_its_value(self) -> None:
        code, record, err = self.run_with(result_for(), "--since", "2026-11-23T09:00:00Z", "--bogus", "private-value", "--extra-objectss=private-too")
        self.assertEqual((2, {}), (code, record))
        self.assertEqual([], [word for word in ("private-value", "private-too", "--bogus") if word in err])


if __name__ == "__main__":
    unittest.main()
