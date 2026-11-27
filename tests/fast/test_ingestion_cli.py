"""The ingestion command line, run through ``main`` with the stand-in store in the place of S3.

Every refusal exits 1 with its cause on standard error and raises nothing: an input error is reported, never a traceback, and no message echoes a value of
an input. Each case has the passing run of the same three commands as its control.
"""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts import aws_cli
from scripts.ingestion import cli
from scripts.process import CompletedProcess
from scripts.project_config import ConfigurationError
from tests.fast.ingestion_support import Landed, landed_set, pin_the_synthetic_list, tracked_pin
from tests.fast.memory_store import MemoryStore

MARKER = "private-value"
PIN_LIST_KEY = "vitaldb/1.0.0/SHA256SUMS.txt"
CONFIGURATION = {"AWS_PROFILE": "example_profile", "AWS_REGION": "us-west-2", "S3_BUCKET": "example-bucket"}


def setUpModule() -> None:
    pin_the_synthetic_list()


class Workspace:
    """The files of one run of the commands: the selection, the publisher's list, the pairing records, the result and the reference."""

    def __init__(self, folder: str, landed: Landed) -> None:
        self.landed = landed
        self.folder = Path(folder)
        selection = landed.selection
        self.selection_document: dict[str, Any] = {
            "name": selection.name,
            "release": selection.release,
            "source_bucket": selection.source_bucket,
            "source_prefix": selection.source_prefix,
            "measurement_period": selection.measurement_period,
            "checksum_list": {"key": selection.checksum_list_key, "sha256": selection.checksum_list_sha256},
            "files": list(selection.files),
        }
        self.selection = self.write("selection.json", json.dumps(self.selection_document))
        self.checksum_list = self.folder / "SHA256SUMS.txt"
        self.checksum_list.write_bytes(landed.list_bytes)
        self.pairings = self.write("pairings.json", json.dumps([dataclasses.asdict(record) for record in landed.pairings()]))
        self.result = self.folder / "result.json"
        self.reference = self.folder / "reference.json"

    def write(self, name: str, text: str) -> Path:
        path = self.folder / name
        path.write_text(text)
        return path

    def run(self, *arguments: str, store: MemoryStore | None = None) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(cli, "load_configuration", return_value=CONFIGURATION),
            mock.patch.object(cli, "S3Store", return_value=store or self.landed.store),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = cli.main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def check(self) -> tuple[int, str, str]:
        return self.run(
            "check",
            "--selection",
            str(self.selection),
            "--checksum-list",
            str(self.checksum_list),
            "--pairings",
            str(self.pairings),
            "--result",
            str(self.result),
        )

    def publish(self) -> tuple[int, str, str]:
        return self.run("publish", "--selection", str(self.selection), "--result", str(self.result), "--reference", str(self.reference))

    def verify(self, *keys: str) -> tuple[int, str, str]:
        return self.run("verify", "--reference", str(self.reference), *[argument for key in keys for argument in ("--source-key", key)])


class CommandTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="ingestion_cli_")
        self.addCleanup(folder.cleanup)
        self.work = Workspace(folder.name, landed_set())

    def refused(self, outcome: tuple[int, str, str], cause: str) -> None:
        code, _, errors = outcome
        self.assertEqual(1, code, errors)
        self.assertIn(cause, errors)
        self.assertNotIn(MARKER, errors)
        self.assertNotIn("Traceback", errors)

    def test_a_failed_check_says_each_kind_of_fault_and_its_count_in_lines_that_name_no_file(self) -> None:
        records = [dataclasses.asdict(record) for record in self.work.landed.pairings()]
        kept = [record for record in records if record["source_key"].endswith("clinical_data.csv")]
        self.work.write("pairings.json", json.dumps(kept))
        self.work.landed.land("clinical_data.csv", b"x" * self.work.landed.sizes["clinical_data.csv"])
        code, output, errors = self.work.check()
        self.assertEqual((1, ""), (code, output))
        lines = errors.splitlines()
        self.assertEqual(
            ["faults of kind missing: 2", "faults of kind checksum_differs: 1", "check failed: 3 faults in 3 selected files; no manifest may be published"],
            lines[-3:],
        )
        self.assertEqual(3, len([line for line in lines[:-3] if line.startswith("vitaldb/1.0.0/")]))

    def test_a_passed_check_says_no_kind_of_fault(self) -> None:
        code, output, errors = self.work.check()
        self.assertEqual(0, code, errors)
        self.assertNotIn("faults of kind", output + errors)

    def test_control_the_three_commands_pass_for_a_complete_landed_set(self) -> None:
        self.assertEqual(0, self.work.check()[0])
        self.assertEqual(0, self.work.publish()[0])
        code, output, _ = self.work.verify()
        self.assertEqual(0, code)
        self.assertIn("verified: 3 object versions", output)

    def test_an_input_file_that_cannot_be_used_is_reported_and_the_command_exits_1(self) -> None:
        self.work.selection.unlink()
        self.refused(self.work.check(), "cannot read the selection")

    def test_a_selection_a_pairing_file_a_result_and_a_reference_that_are_not_valid_are_reported(self) -> None:
        self.work.write("selection.json", json.dumps({**self.work.selection_document, MARKER: 1}))
        self.refused(self.work.check(), "the selection")
        self.work.write("selection.json", json.dumps(self.work.selection_document))
        self.work.write("pairings.json", f'[{{"source_key": "{MARKER}"}}]')
        self.refused(self.work.check(), "pairing record")
        self.work.write("pairings.json", json.dumps([dataclasses.asdict(record) for record in self.work.landed.pairings()]))
        self.work.write("result.json", f"{MARKER}")
        self.refused(self.work.publish(), "the check result")
        self.work.write("reference.json", f"{MARKER}")
        self.refused(self.work.verify(), "the manifest reference")

    def test_a_checksum_list_that_is_not_the_pinned_one_is_reported(self) -> None:
        self.work.checksum_list.write_bytes(self.work.landed.list_bytes + b"\n")
        self.refused(self.work.check(), "SHA-256")

    def test_a_failed_check_is_reported_and_its_result_publishes_nothing(self) -> None:
        self.work.landed.land("vital_files/0001.vital", b"another copy with other bytes")
        self.work.write("pairings.json", json.dumps([dataclasses.asdict(record) for record in self.work.landed.pairings()]))
        self.refused(self.work.check(), "checksum_differs")
        self.refused(self.work.publish(), "did not pass")

    def test_a_publication_that_the_check_did_not_cover_is_reported(self) -> None:
        self.assertEqual(0, self.work.check()[0])
        self.work.write("selection.json", json.dumps({**self.work.selection_document, "measurement_period": "another period"}))
        self.refused(self.work.publish(), "not for this selection")

    def test_the_gate_refuses_a_substituted_version_and_an_unknown_key_without_echoing_the_key(self) -> None:
        self.assertEqual(0, self.work.check()[0])
        self.assertEqual(0, self.work.publish()[0])
        self.refused(self.work.verify(MARKER), "requested key 1: not_in_manifest")
        self.work.landed.land("vital_files/0001.vital", b"a substituted copy of the recording")
        self.refused(self.work.verify(), "version_substituted")

    def test_a_full_release_that_is_shortened_is_reported_before_anything_is_read(self) -> None:
        self.enterContext(tracked_pin())
        reversed_range = {"case_files": {"pattern": "vital_files/{:04d}.vital", "first": 6388, "last": 1}}
        one_case_short = {"case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6387}}
        for change, cause in (
            (reversed_range, "case_files"),
            (one_case_short, "the full release's files"),
            ({"checksum_list": {"key": "k", "sha256": "0" * 64}}, "checksum_list_key is not the tracked pin's"),
            ({"checksum_list": {"key": PIN_LIST_KEY, "sha256": "0" * 64}}, "the selection's checksum_list_sha256 is not the tracked pin's"),
        ):
            self.work.write("selection.json", json.dumps(self.full_release(change)))
            with self.subTest(cause=cause):
                self.refused(self.work.check(), cause)
                self.assertEqual([], self.work.landed.store.reads)

    def full_release(self, change: dict[str, Any]) -> dict[str, Any]:
        pin = json.loads(Path("tests/fixtures/vitaldb/release.json").read_text())
        document: dict[str, Any] = {
            "name": "full_release",
            "release": "1.0.0",
            "source_bucket": pin["source"]["bucket"],
            "source_prefix": pin["source"]["release_path"],
            "measurement_period": "2016-08 to 2017-06",
            "checksum_list": {"key": pin["checksum_list"]["key"], "sha256": pin["checksum_list"]["sha256"]},
            "files": ["clinical_data.csv", "lab_data.csv"],
            "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 6388},
        }
        return document | change

    def test_a_selected_file_that_the_list_does_not_name_is_reported_by_its_position_and_never_echoed(self) -> None:
        files = [*self.work.selection_document["files"], f"{MARKER}.csv"]
        self.work.write("selection.json", json.dumps({**self.work.selection_document, "files": files}))
        self.refused(self.work.check(), "selected file 2: not_in_checksum_list")
        self.assertNotIn(MARKER, self.work.result.read_text())
        self.assertIn("vitaldb/1.0.0/clinical_data.csv", self.work.result.read_text())

    def test_a_malformed_source_prefix_is_refused_before_anything_is_read_and_never_echoed(self) -> None:
        for prefix in (f"{MARKER} x/", f"vitaldb/../{MARKER}/", f"vitaldb/./{MARKER}/", f"vitaldb//{MARKER}/", f"/{MARKER}/", MARKER, "/"):
            self.work.write("selection.json", json.dumps({**self.work.selection_document, "source_prefix": prefix}))
            with self.subTest(prefix=prefix):
                self.refused(self.work.check(), "source_prefix")
                self.assertFalse(self.work.result.exists())
                self.assertEqual([], self.work.landed.store.reads)

    def test_a_command_line_that_is_not_valid_is_refused_without_echoing_it(self) -> None:
        for arguments in ([MARKER], ["check", f"--{MARKER}"], ["verify", "--reference", "r.json", MARKER], []):
            errors = io.StringIO()
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(errors), self.assertRaises(SystemExit) as stopped:
                cli.main(arguments)
            self.assertEqual(2, stopped.exception.code)
            self.assertNotIn(MARKER, errors.getvalue())
            self.assertIn("--help", errors.getvalue())

    def test_a_configuration_error_is_reported(self) -> None:
        errors = io.StringIO()
        with mock.patch.object(cli, "load_configuration", side_effect=ConfigurationError(".env is missing")), contextlib.redirect_stderr(errors):
            self.assertEqual(1, cli.main(["verify", "--reference", str(self.work.reference)]))
        self.assertIn(".env is missing", errors.getvalue())


class AwsFailureTest(unittest.TestCase):
    """A failed AWS call is reported through the real AWS helper: its notice says the error text was kept only when the helper kept it."""

    def verify(self, keep: mock.Mock) -> tuple[int, str]:
        failed = CompletedProcess(["aws"], 255, "", "An error occurred (AccessDenied) when calling the ListObjectVersions operation: denied\n")
        with tempfile.TemporaryDirectory(prefix="ingestion_cli_aws_") as folder:
            reference = Path(folder) / "reference.json"
            sha256 = "0" * 64
            reference.write_text(json.dumps({"bucket": "example-bucket", "key": f"manifests/probe_set/{sha256}.json", "version_id": "v1", "sha256": sha256}))
            errors = io.StringIO()
            with (
                mock.patch.object(cli, "load_configuration", return_value=CONFIGURATION),
                mock.patch.object(aws_cli, "run_command", return_value=failed),
                mock.patch.object(aws_cli, "keep_raw_error", keep),
                contextlib.redirect_stderr(errors),
            ):
                code = cli.main(["verify", "--reference", str(reference)])
        return code, errors.getvalue()

    def test_control_a_kept_error_text_is_named_as_kept(self) -> None:
        code, errors = self.verify(mock.Mock(return_value=aws_cli.RAW_ERRORS / "1_s3api_list-object-versions.txt"))
        self.assertEqual((1, "an AWS call failed; its full error text is kept in an owner-only file under runs/raw_errors\n"), (code, errors))

    def test_an_error_text_that_could_not_be_kept_is_not_named_as_kept(self) -> None:
        code, errors = self.verify(mock.Mock(side_effect=OSError("no space left on the device")))
        self.assertEqual((1, "an AWS call failed; its full error text could not be kept\n"), (code, errors))


if __name__ == "__main__":
    unittest.main()
