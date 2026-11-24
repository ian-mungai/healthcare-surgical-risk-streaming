"""The record connections as data: what Airbyte reads, where it writes and how each connection runs.

Each case holds one rule of the owner's decision (Nov 22 2026): every column of a CSV source is a string, the destination is the S3 Data Lake with the Nessie
catalog on the run's load branch, each sync is a full refresh with no schedule, and the document holds no secret. The planted cases change one fact.
"""

from __future__ import annotations

import codecs
import json
import unittest
from typing import Any

from scripts.ingestion import record_definitions as definitions
from scripts.ingestion.record_definitions import CSV_TABLES, DESTINATION, SOURCE_FILE, SOURCE_S3, TABLES, TRACK_LIST, Environment
from scripts.ingestion.selection import pinned_release
from scripts.ingestion.track_list import pinned_track_list
from scripts.sample.candidates import RELEASE_FILE

RUN = "run_20261122_ab12cd34"
SHA = "ab" * 32
ENVIRONMENT = Environment(bucket="example-bucket", region="us-west-2", warehouse_path="warehouse", nessie_uri="http://nessie.example:19120/api/v2")


def pinned_table_keys() -> list[str]:
    return sorted(table["key"] for table in json.loads(RELEASE_FILE.read_text())["tables"])


class TableTest(unittest.TestCase):
    def test_the_csv_tables_are_the_two_tables_of_the_release_pin_and_the_track_list_has_no_key(self) -> None:
        self.assertEqual(pinned_table_keys(), sorted(table.source_key for table in CSV_TABLES))
        self.assertEqual(["clinical_data", "lab_data", "track_list"], [table.name for table in TABLES])
        self.assertIsNone(TRACK_LIST.source_key)

    def test_the_table_name_is_the_stream_name_of_each_source(self) -> None:
        for table in TABLES:
            with self.subTest(table.name):
                configuration = definitions.source_configuration(table, RUN, ENVIRONMENT, SHA)
                names = [stream["name"] for stream in configuration["streams"]] if "streams" in configuration else [configuration["dataset_name"]]
                self.assertEqual([table.name], names)


class CsvSourceTest(unittest.TestCase):
    def configuration(self, name: str = "clinical_data") -> dict[str, Any]:
        table = next(table for table in CSV_TABLES if table.name == name)
        return definitions.source_configuration(table, RUN, ENVIRONMENT, SHA)

    def test_a_csv_source_reads_exactly_the_one_copy_of_this_run(self) -> None:
        for table in CSV_TABLES:
            with self.subTest(table.name):
                configuration = definitions.source_configuration(table, RUN, ENVIRONMENT, SHA)
                self.assertEqual("example-bucket", configuration["bucket"])
                self.assertEqual([f"records_input/{RUN}/{SHA}/{table.name}.csv"], configuration["streams"][0]["globs"])

    def test_every_column_is_a_string_and_an_empty_cell_stays_an_empty_string(self) -> None:
        csv_format = self.configuration()["streams"][0]["format"]
        self.assertEqual("csv", csv_format["filetype"])
        self.assertEqual("None", csv_format["inference_type"])
        self.assertIs(False, csv_format["strings_can_be_null"])
        self.assertEqual({"header_definition_type": "From CSV"}, csv_format["header_definition"])

    def test_a_byte_order_mark_is_not_kept_in_the_first_column_name(self) -> None:
        encoding = self.configuration()["streams"][0]["format"]["encoding"]
        self.assertEqual("utf-8-sig", encoding)
        self.assertEqual("utf-8-sig", codecs.lookup(encoding).name, "a name that the connector's own check, codecs.lookup, accepts")
        published = codecs.BOM_UTF8 + b"caseid,subjectid\r\n1,2\r\n"
        self.assertEqual("caseid", published.decode(encoding).split(",")[0])
        self.assertEqual("\ufeffcaseid", published.decode("utf-8").split(",")[0], "without it the mark would be part of the name")

    def test_no_value_is_read_as_null_so_an_empty_cell_is_an_empty_string(self) -> None:
        csv_format = self.configuration()["streams"][0]["format"]
        self.assertIs(False, csv_format["strings_can_be_null"])
        self.assertNotIn("null_values", csv_format, "the connector's default set of null markers is empty")

    def test_the_source_has_no_schema_override_and_is_not_schemaless(self) -> None:
        stream = self.configuration()["streams"][0]
        self.assertNotIn("input_schema", stream)
        self.assertIs(False, stream["schemaless"])

    def test_the_source_uses_the_pods_own_credentials_and_holds_no_key(self) -> None:
        configuration = self.configuration()
        self.assertEqual({"auth_type": "default_credentials"}, configuration["credentials"])
        self.assertNotIn("aws_access_key_id", json.dumps(configuration))

    def test_a_run_ID_that_is_not_a_plain_name_is_refused_before_it_reaches_a_key(self) -> None:
        for run in ("", "Run1", "../x", "a/b", "a b", "x" * 65, "run*"):
            with self.subTest(run), self.assertRaises(definitions.DefinitionError):
                definitions.source_configuration(CSV_TABLES[0], run, ENVIRONMENT, SHA)


class ReadKeyTest(unittest.TestCase):
    def test_the_read_key_holds_the_run_the_verified_hash_and_the_file_name(self) -> None:
        for table in CSV_TABLES:
            self.assertEqual(f"records_input/{RUN}/{SHA}/{table.name}.csv", definitions.read_key(RUN, table, SHA))

    def test_a_source_without_the_hash_or_with_a_value_that_is_not_a_hash_is_refused(self) -> None:
        for sha in ("", "ab", "AB" * 32, "../" + "a" * 61, "g" * 64, "a" * 63 + "/"):
            with self.subTest(sha=sha), self.assertRaises(definitions.DefinitionError):
                definitions.source_configuration(CSV_TABLES[0], RUN, ENVIRONMENT, sha)
            with self.subTest(read_key=sha), self.assertRaises(definitions.DefinitionError):
                definitions.read_key(RUN, CSV_TABLES[0], sha)

    def test_two_contents_or_two_runs_never_share_a_read_key(self) -> None:
        keys = {definitions.read_key(run, CSV_TABLES[0], sha) for run in (RUN, "run_other") for sha in ("a" * 64, "b" * 64)}
        self.assertEqual(4, len(keys))

    def test_the_track_list_needs_no_hash(self) -> None:
        self.assertEqual(
            definitions.source_configuration(TRACK_LIST, RUN, ENVIRONMENT, ""), definitions.source_configuration(TRACK_LIST, RUN, ENVIRONMENT, SHA)
        )


class TrackListSourceTest(unittest.TestCase):
    def test_the_file_source_reads_the_pinned_address_and_keeps_every_column_a_string(self) -> None:
        configuration = definitions.source_configuration(TRACK_LIST, RUN, ENVIRONMENT, SHA)
        self.assertEqual(pinned_track_list().url, configuration["url"])
        self.assertEqual({"storage": "HTTPS"}, configuration["provider"])
        self.assertEqual("csv", configuration["format"])
        options = json.loads(configuration["reader_options"])
        self.assertEqual("str", options["dtype"])
        self.assertIs(False, options["keep_default_na"])
        self.assertIs(False, options["na_filter"])
        self.assertEqual({"dtype", "keep_default_na", "na_filter"}, set(options), "only options of pandas.read_csv that this load means to set")

    def test_the_track_list_source_reads_nothing_from_the_projects_bucket(self) -> None:
        self.assertNotIn("example-bucket", json.dumps(definitions.source_configuration(TRACK_LIST, RUN, ENVIRONMENT, SHA)))


class EnvironmentTest(unittest.TestCase):
    def test_a_warehouse_path_with_a_dot_segment_or_in_a_prefix_of_the_load_is_refused(self) -> None:
        for path in ("a/../b", "..", "a/./b", ".", "landing", "landing/x", "records_input", "records_input/x", "a b", "", "/a"):
            with self.subTest(path), self.assertRaises(definitions.DefinitionError):
                definitions.destination_configuration(Environment("example-bucket", "us-west-2", path, "http://nessie.example:19120/api/v2"), f"load_{RUN}")


class DestinationTest(unittest.TestCase):
    def configuration(self, branch: str = f"load_{RUN}") -> dict[str, Any]:
        return definitions.destination_configuration(ENVIRONMENT, branch)

    def test_the_destination_is_the_nessie_catalog_on_the_load_branch_with_the_raw_namespace(self) -> None:
        configuration = self.configuration()
        self.assertEqual(f"load_{RUN}", configuration["main_branch_name"])
        self.assertEqual("s3://example-bucket/warehouse", configuration["warehouse_location"])
        self.assertEqual({"catalog_type": "NESSIE", "server_uri": ENVIRONMENT.nessie_uri, "namespace": "raw_records"}, configuration["catalog_type"])
        self.assertEqual(("example-bucket", "us-west-2"), (configuration["s3_bucket_name"], configuration["s3_bucket_region"]))

    def test_column_names_are_kept_as_the_csv_header_has_them(self) -> None:
        self.assertIs(False, self.configuration()["normalize_column_names"])

    def test_nessie_gets_no_token_because_the_projects_catalog_has_no_sign_in(self) -> None:
        self.assertNotIn("access_token", self.configuration()["catalog_type"])

    def test_the_document_holds_no_secret_value_or_field(self) -> None:
        text = json.dumps(self.configuration())
        for word in ("access_key_id", "secret_access_key", "password", "token"):
            self.assertNotIn(word, text)

    def test_the_load_branch_is_named_for_the_run_and_main_is_never_the_destination(self) -> None:
        self.assertEqual(f"load_{RUN}", definitions.branch_name(RUN))
        for branch in ("main", "", "load_", "load_Run", "load_a/b"):
            with self.subTest(branch), self.assertRaises(definitions.DefinitionError):
                definitions.destination_configuration(ENVIRONMENT, branch)

    def test_the_destination_is_a_function_of_the_environment_and_the_branch_only(self) -> None:
        self.assertEqual(self.configuration(), definitions.destination_configuration(ENVIRONMENT, f"load_{RUN}"))
        self.assertNotEqual(self.configuration(), self.configuration("load_other"))


class ConnectionTest(unittest.TestCase):
    def test_each_connection_is_manual_and_a_full_refresh_that_overwrites_in_the_raw_namespace(self) -> None:
        for table in TABLES:
            with self.subTest(table.name):
                connection = definitions.connection_configuration(table)
                self.assertEqual({"scheduleType": "manual"}, connection["schedule"])
                self.assertEqual(("custom_format", "raw_records"), (connection["namespaceDefinition"], connection["namespaceFormat"]))
                self.assertEqual("active", connection["status"])
                self.assertEqual({"streams": [{"name": table.name, "syncMode": "full_refresh_overwrite"}]}, connection["configurations"])
                self.assertEqual(f"vitaldb_{table.name}_records", connection["name"])

    def test_the_connection_names_are_distinct(self) -> None:
        self.assertEqual(3, len({definitions.connection_configuration(table)["name"] for table in TABLES}))


class PinTest(unittest.TestCase):
    def test_each_connector_is_pinned_by_a_full_version_and_a_linux_amd64_digest_and_the_date_it_was_read_is_recorded(self) -> None:
        for connector in (SOURCE_S3, SOURCE_FILE, DESTINATION):
            with self.subTest(connector.repository):
                self.assertRegex(connector.tag, r"[0-9]+\.[0-9]+\.[0-9]+")
                self.assertRegex(connector.digest, r"sha256:[0-9a-f]{64}")
        self.assertEqual(
            ("airbyte/source-s3", "airbyte/source-file", "airbyte/destination-s3-data-lake"),
            (SOURCE_S3.repository, SOURCE_FILE.repository, DESTINATION.repository),
        )
        self.assertRegex(definitions.PINNED_ON, r"2026-11-22")

    def test_the_two_s3_connectors_share_the_versions_and_digests_of_the_raw_connection(self) -> None:
        self.assertEqual(("4.17.1", "sha256:0ca33028218a3c166cf8802e2f955e285f6c167b043a36e9f300768ea4e2299b"), (SOURCE_S3.tag, SOURCE_S3.digest))

    def test_each_pin_is_the_tag_that_airbyte_chart_2_5_0_ships_and_the_file_source_digest_is_the_one_read_from_docker_hub(self) -> None:
        """Airbyte takes its built-in definition at the tag of its chart, and the load refuses any other tag (tags read on the host on Nov 24 2026)."""
        self.assertEqual(
            {"airbyte/source-s3:4.17.1", "airbyte/source-file:0.6.1", "airbyte/destination-s3-data-lake:0.4.2"},
            {SOURCE_S3.image, SOURCE_FILE.image, DESTINATION.image},
        )
        self.assertEqual(
            "sha256:a732c04bc4581ed11517a515f3673b43b420f1553308037efefdc6c930e20c6d",
            SOURCE_FILE.digest,
            "the linux/amd64 manifest of airbyte/source-file:0.6.1",
        )
        self.assertEqual(3, len({SOURCE_S3.digest, SOURCE_FILE.digest, DESTINATION.digest}))

    def test_the_csv_tables_come_from_the_release_path_of_the_pin(self) -> None:
        for table in CSV_TABLES:
            self.assertTrue(table.source_key and table.source_key.startswith(pinned_release().source_prefix))


if __name__ == "__main__":
    unittest.main()
