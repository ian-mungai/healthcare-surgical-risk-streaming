"""The raw connection as data: what Airbyte is told to copy, from where, to where and how.

The definitions are built from the tracked release pin, so the selection is the pin's 6,390 keys and nothing else. These cases cover the source and destination
settings, the delivery mode, the manual schedule, the connector pins and the staging limit. Applying the definitions to a running Airbyte is covered by
``test_airbyte_connection_module`` and the host's live batch.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

from scripts.ingestion import connection
from scripts.ingestion.connection import ConnectionDefinitionError, desired_state, fixed_sample_selection, full_release_selection
from scripts.ingestion.selection import Selection
from scripts.sample.split import is_development
from tests.fast.ansible_paths import group_variables

PIN = json.loads(Path("tests/fixtures/vitaldb/release.json").read_text())
PREFIX = PIN["source"]["release_path"]
BUCKET = "imungai-example-project"
REGION = "us-west-2"
SOURCE_TAG = "4.17.1"
DESTINATION_TAG = "1.9.11"
SECRET_FIELDS = {"access_key_id", "secret_access_key", "aws_access_key_id", "aws_secret_access_key", "role_arn", "session_token"}
QUANTITY = re.compile(r"([0-9]+)(Ki|Mi|Gi|Ti|K|M|G|T)?")
UNITS = {"": 1, "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4, "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
LARGEST_CASE_FILE_BYTES = 103 * 1024 * 1024
LARGEST_CASE_FILE_STAGING_BYTES = 1_500_000_000
HOST_DISK_BYTES = 200 * 1024**3


def state(selection: Selection | None = None) -> dict[str, Any]:
    return desired_state(selection or full_release_selection(), bucket=BUCKET, region=REGION, source_tag=SOURCE_TAG, destination_tag=DESTINATION_TAG)


def keys_of(document: object) -> list[str]:
    """Every key name of a nested document."""
    if isinstance(document, dict):
        return [str(key) for key in document] + [name for value in document.values() for name in keys_of(value)]
    if isinstance(document, list):
        return [name for value in document for name in keys_of(value)]
    return []


class SelectionTest(unittest.TestCase):
    def test_the_source_selects_exactly_the_pins_two_tables_and_6388_case_files(self) -> None:
        stream = state()["source"]["configuration"]["streams"][0]
        wanted = {table["key"] for table in PIN["tables"]} | {f"{PREFIX}vital_files/{number:04d}.vital" for number in range(1, 6389)}
        self.assertEqual(6390, len(stream["globs"]))
        self.assertEqual(wanted, set(stream["globs"]))
        self.assertEqual(len(stream["globs"]), len(set(stream["globs"])))

    def test_no_selected_pattern_can_match_another_key(self) -> None:
        for pattern in state()["source"]["configuration"]["streams"][0]["globs"]:
            self.assertRegex(pattern, r"^[A-Za-z0-9._/-]+$")

    def test_the_checksum_list_is_not_a_payload_and_is_not_selected(self) -> None:
        self.assertNotIn(PIN["checksum_list"]["key"], state()["source"]["configuration"]["streams"][0]["globs"])

    def test_a_smaller_selection_selects_its_own_keys_only(self) -> None:
        text = json.dumps(
            {
                "name": "sample",
                "release": "1.0.0",
                "source_bucket": PIN["source"]["bucket"],
                "source_prefix": PREFIX,
                "measurement_period": "2016-08 to 2017-06",
                "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": PIN["checksum_list"]["sha256"]},
                "files": ["clinical_data.csv"],
                "case_files": {"pattern": "vital_files/{:04d}.vital", "first": 1, "last": 2},
            }
        )
        globs = state(Selection.from_json(text))["source"]["configuration"]["streams"][0]["globs"]
        self.assertEqual([f"{PREFIX}clinical_data.csv", f"{PREFIX}vital_files/0001.vital", f"{PREFIX}vital_files/0002.vital"], sorted(globs))


class FixedSampleTest(unittest.TestCase):
    def candidates(self) -> list[str]:
        lines = Path("tests/fixtures/vitaldb/candidates.tsv").read_text().splitlines()[1:]
        return [line.split("\t")[2] for line in lines if is_development(int(line.split("\t")[1]))]

    def test_the_fixed_sample_is_the_development_candidates_and_the_two_tables(self) -> None:
        keys = set(fixed_sample_selection().source_keys)
        self.assertEqual({table["key"] for table in PIN["tables"]} | set(self.candidates()), keys)
        self.assertEqual(11, len(keys))

    def test_the_fixed_sample_selects_no_case_of_a_test_patient(self) -> None:
        keys = set(fixed_sample_selection().source_keys)
        for case in (565, 2367, 2535, 5105):
            self.assertNotIn(f"{PREFIX}vital_files/{case:04d}.vital", keys)

    def test_the_sample_is_a_part_of_the_full_release_so_a_later_full_copy_adds_only_the_rest(self) -> None:
        self.assertLess(set(fixed_sample_selection().source_keys), set(full_release_selection().source_keys))

    def test_the_sample_is_not_named_full_release_and_keeps_the_pinned_list_and_bucket(self) -> None:
        sample = fixed_sample_selection()
        self.assertEqual("fixed_sample", sample.name)
        self.assertEqual((PIN["checksum_list"]["sha256"], PIN["source"]["bucket"]), (sample.checksum_list_sha256, sample.source_bucket))

    def test_the_selection_file_of_the_sample_is_the_form_the_check_reads(self) -> None:
        document = connection.selection_document(fixed_sample_selection())
        self.assertEqual(fixed_sample_selection(), Selection.from_json(json.dumps(document)))
        self.assertEqual(connection.selection_document(full_release_selection())["name"], "full_release")
        self.assertEqual(6390, len(connection.selection_document(full_release_selection())["files"]))


class SourceAndDestinationTest(unittest.TestCase):
    def test_the_source_reads_the_publishers_bucket_and_copies_raw_files_with_their_folders(self) -> None:
        source = state()["source"]["configuration"]
        self.assertEqual(PIN["source"]["bucket"], source["bucket"])
        self.assertEqual(PIN["source"]["region"], source["region_name"])
        self.assertEqual({"delivery_type": "use_file_transfer", "preserve_directory_structure": True}, source["delivery_method"])
        self.assertEqual(1, len(source["streams"]))

    def test_the_destination_writes_the_landing_prefix_of_the_projects_bucket_and_the_metadata_as_json_lines(self) -> None:
        destination = state()["destination"]["configuration"]
        self.assertEqual(BUCKET, destination["s3_bucket_name"])
        self.assertEqual("landing", destination["s3_bucket_path"])
        self.assertEqual(REGION, destination["s3_bucket_region"])
        self.assertEqual("JSONL", destination["format"]["format_type"])
        self.assertEqual("No Compression", destination["format"]["compression"]["compression_type"])

    def test_the_document_on_the_owners_computer_holds_no_key_and_names_no_field_for_one(self) -> None:
        document = state()
        self.assertEqual(
            set(), SECRET_FIELDS & set(keys_of({"source": document["source"]["configuration"], "destination": document["destination"]["configuration"]}))
        )
        self.assertEqual("iam_user", document["source"]["configuration"]["credentials"]["auth_type"])
        for part in ("source", "destination"):
            self.assertNotIn("secret_fields", document[part], "the module holds the one table of the fields that take the key; the document names none")
        self.assertNotIn("secret_fields", json.dumps(document))

    def test_the_source_definition_is_one_shared_pinned_definition_that_another_connection_can_reuse(self) -> None:
        shared = connection.source_definition(SOURCE_TAG)
        self.assertEqual({"name": connection.SOURCE_DEFINITION_NAME, "dockerRepository": "airbyte/source-s3", "dockerImageTag": SOURCE_TAG}, shared)
        self.assertEqual(shared, state()["source"]["definition"])
        self.assertEqual("s3_source_pinned", connection.SOURCE_DEFINITION_NAME, "the name is the one that the record connection takes the definition by")
        with self.assertRaises(ConnectionDefinitionError):
            connection.source_definition("latest")

    def test_each_connector_is_pinned_by_the_tag_it_is_given_and_never_by_latest(self) -> None:
        document = state()
        self.assertEqual(
            ("airbyte/source-s3", SOURCE_TAG), (document["source"]["definition"]["dockerRepository"], document["source"]["definition"]["dockerImageTag"])
        )
        self.assertEqual(
            ("airbyte/destination-s3", DESTINATION_TAG),
            (document["destination"]["definition"]["dockerRepository"], document["destination"]["definition"]["dockerImageTag"]),
        )
        for tag in ("latest", "", "4", "4.17"):
            with self.subTest(tag=tag), self.assertRaises(ConnectionDefinitionError):
                desired_state(full_release_selection(), bucket=BUCKET, region=REGION, source_tag=tag, destination_tag=DESTINATION_TAG)

    def test_a_bucket_or_region_of_another_form_is_refused(self) -> None:
        for bucket in ("", "Upper", "a", "has space", "landing/", "x" * 64):
            with self.subTest(bucket=bucket), self.assertRaises(ConnectionDefinitionError):
                desired_state(full_release_selection(), bucket=bucket, region=REGION, source_tag=SOURCE_TAG, destination_tag=DESTINATION_TAG)
        with self.assertRaises(ConnectionDefinitionError):
            desired_state(full_release_selection(), bucket=BUCKET, region="", source_tag=SOURCE_TAG, destination_tag=DESTINATION_TAG)

    def test_the_publisher_bucket_is_never_the_destination(self) -> None:
        with self.assertRaises(ConnectionDefinitionError):
            desired_state(full_release_selection(), bucket=PIN["source"]["bucket"], region=REGION, source_tag=SOURCE_TAG, destination_tag=DESTINATION_TAG)


class ConnectionTest(unittest.TestCase):
    def test_the_connection_has_no_schedule_so_only_the_orchestrator_starts_it(self) -> None:
        self.assertEqual({"scheduleType": "manual"}, state()["connection"]["schedule"])

    def test_the_connection_is_active_and_copies_files_incrementally_so_a_rerun_copies_nothing_new(self) -> None:
        connected = state()["connection"]
        self.assertEqual("active", connected["status"])
        self.assertEqual([{"name": connection.STREAM_NAME, "syncMode": "incremental_append", "includeFiles": True}], connected["configurations"]["streams"])
        self.assertEqual(connection.STREAM_NAME, state()["source"]["configuration"]["streams"][0]["name"])

    def test_the_names_are_fixed_so_a_second_apply_finds_what_the_first_made(self) -> None:
        document = state()
        self.assertEqual(
            (connection.SOURCE_NAME, connection.DESTINATION_NAME, connection.CONNECTION_NAME),
            (document["source"]["name"], document["destination"]["name"], document["connection"]["name"]),
        )
        self.assertEqual(3, len({connection.SOURCE_NAME, connection.DESTINATION_NAME, connection.CONNECTION_NAME}))

    def test_the_same_inputs_give_the_same_document(self) -> None:
        self.assertEqual(json.dumps(state(), sort_keys=True), json.dumps(state(), sort_keys=True))


class RenderTest(unittest.TestCase):
    def render(self, *arguments: str) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = connection.main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def common(self) -> list[str]:
        return ["render", "--bucket", BUCKET, "--region", REGION, "--source-tag", SOURCE_TAG, "--destination-tag", DESTINATION_TAG]

    def test_the_command_prints_the_document_of_the_full_release(self) -> None:
        code, output, errors = self.render(*self.common())
        self.assertEqual((0, ""), (code, errors))
        self.assertEqual(state(), json.loads(output))

    def test_a_refusal_exits_1_names_the_field_and_prints_no_document(self) -> None:
        code, output, errors = self.render("render", "--bucket", "Upper", "--region", REGION, "--source-tag", SOURCE_TAG, "--destination-tag", DESTINATION_TAG)
        self.assertEqual((1, ""), (code, output))
        self.assertIn("bucket", errors)
        self.assertNotIn("Upper", errors)

    def test_the_fixed_sample_is_rendered_by_its_flag_and_the_selection_command_prints_the_file_the_check_reads(self) -> None:
        code, output, _ = self.render(*self.common(), "--fixed-sample")
        self.assertEqual(0, code)
        self.assertEqual(state(fixed_sample_selection()), json.loads(output))
        code, output, _ = self.render("selection", "--fixed-sample")
        self.assertEqual(0, code)
        self.assertEqual(fixed_sample_selection(), Selection.from_json(output))
        code, output, _ = self.render("selection")
        self.assertEqual(full_release_selection(), Selection.from_json(output))

    def test_a_selection_file_that_is_not_the_pins_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "selection.json"
            path.write_text("{}")
            code, output, _ = self.render(*self.common(), "--selection", str(path))
        self.assertEqual((1, ""), (code, output))

    def test_a_selection_file_that_cannot_be_read_as_text_or_is_missing_exits_1_with_the_one_fixed_line_and_no_traceback(self) -> None:
        """Join round 2, finding 2: a file of invalid UTF-8 raised ``UnicodeDecodeError``, which is not an ``OSError``, and ended in a traceback."""
        with tempfile.TemporaryDirectory() as scratch:
            invalid = Path(scratch) / "invalid.json"
            invalid.write_bytes(b'{"name": "\xff\xfe"}')
            for label, path in (("invalid UTF-8", invalid), ("missing", Path(scratch) / "missing.json"), ("a folder", Path(scratch))):
                for command in (self.common(), ["selection"]):
                    with self.subTest(file=label, command=command[0]):
                        code, output, errors = self.render(*command, "--selection", str(path))
                        self.assertEqual((1, "", "cannot read the selection file\n"), (code, output, errors))


class PinTest(unittest.TestCase):
    def test_each_connector_has_its_amd64_image_digest_recorded_beside_its_version(self) -> None:
        variables = group_variables()
        for name in ("airbyte_source_s3_digest", "airbyte_destination_s3_digest"):
            with self.subTest(name=name):
                self.assertRegex(str(variables[name]), r"^sha256:[0-9a-f]{64}$")
        self.assertNotEqual(variables["airbyte_source_s3_digest"], variables["airbyte_destination_s3_digest"])


class CustomSelectionTest(unittest.TestCase):
    """A custom selection is a non-empty subset of the pinned payload keys: the two tables and the 6,388 case files and nothing else."""

    def selection(self, files: list[str]) -> Selection:
        document = {
            "name": "custom_set",
            "release": "1.0.0",
            "source_bucket": PIN["source"]["bucket"],
            "source_prefix": PREFIX,
            "measurement_period": "2016-08 to 2017-06",
            "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": PIN["checksum_list"]["sha256"]},
            "files": files,
        }
        return Selection.from_json(json.dumps(document))

    def test_a_selection_of_pinned_payloads_is_accepted(self) -> None:
        for files in (["clinical_data.csv"], ["vital_files/0001.vital"], ["lab_data.csv", "vital_files/6388.vital", "vital_files/0042.vital"]):
            with self.subTest(files=files):
                self.assertEqual(
                    sorted(f"{PREFIX}{name}" for name in files), sorted(state(self.selection(files))["source"]["configuration"]["streams"][0]["globs"])
                )

    def test_a_selection_that_names_anything_but_a_pinned_payload_is_refused_by_the_field(self) -> None:
        for name, files in {
            "the checksum list": ["SHA256SUMS.txt"],
            "the checksum list with a payload": ["SHA256SUMS.txt", "clinical_data.csv"],
            "a file of the release that is no payload": ["LICENSE.txt"],
            "a case that the release does not have": ["vital_files/9999.vital"],
            "case zero": ["vital_files/0000.vital"],
            "a folder": ["vital_files"],
            "a file of another folder": ["other/0001.vital"],
        }.items():
            with self.subTest(name), self.assertRaises(ConnectionDefinitionError) as raised:
                state(self.selection(files))
            self.assertIn("selection", str(raised.exception))
            self.assertNotIn("SHA256SUMS", str(raised.exception))

    def test_the_render_and_the_selection_commands_refuse_such_a_file_and_print_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "selection.json"
            path.write_text(
                json.dumps(
                    {
                        "name": "custom_set",
                        "release": "1.0.0",
                        "source_bucket": PIN["source"]["bucket"],
                        "source_prefix": PREFIX,
                        "measurement_period": "2016-08 to 2017-06",
                        "checksum_list": {"key": PIN["checksum_list"]["key"], "sha256": PIN["checksum_list"]["sha256"]},
                        "files": ["SHA256SUMS.txt"],
                    }
                )
            )
            for arguments in (
                ["render", "--bucket", BUCKET, "--region", REGION, "--source-tag", SOURCE_TAG, "--destination-tag", DESTINATION_TAG, "--selection", str(path)],
                ["selection", "--selection", str(path)],
            ):
                with self.subTest(command=arguments[0]):
                    output, errors = io.StringIO(), io.StringIO()
                    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                        code = connection.main(arguments)
                    self.assertEqual((1, ""), (code, output.getvalue()))
                    self.assertIn("selection", errors.getvalue())


class StagingTest(unittest.TestCase):
    def staging_bytes(self, text: str) -> int:
        match = QUANTITY.fullmatch(text)
        if match is None:
            self.fail("the staging size must be a Kubernetes quantity of whole units")
        return int(match.group(1)) * UNITS[match.group(2) or ""]

    def test_the_staging_size_is_set_and_holds_any_one_file_the_connector_may_copy(self) -> None:
        size = self.staging_bytes(str(group_variables()["airbyte_staging_size"]))
        self.assertGreaterEqual(size, LARGEST_CASE_FILE_BYTES)
        self.assertGreaterEqual(size, LARGEST_CASE_FILE_STAGING_BYTES)

    def test_the_staging_size_is_a_small_part_of_the_hosts_disk_so_a_full_copy_stops_before_the_disk_fills(self) -> None:
        self.assertLessEqual(self.staging_bytes(str(group_variables()["airbyte_staging_size"])), HOST_DISK_BYTES // 10)


if __name__ == "__main__":
    unittest.main()
