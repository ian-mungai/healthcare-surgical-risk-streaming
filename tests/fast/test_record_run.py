"""The settings of a record load: what the code location container must set, and the secret files it reads.

A missing or blank setting stops the load before any call and names the setting. A secret file that is missing or empty is named by its role, and no message
holds a secret value or a path.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.ingestion import record_run
from scripts.ingestion.record_run import SECRET_FILES, SettingsError, main, settings_from_environment
from tests.fast.ingestion_support import list_text_for, pin_the_synthetic_list
from tests.fast.locale_support import non_utf_8_locale
from tests.fast.record_support import RELEASE_FILES, FakeAirbyte, FakeCatalog, landed_records
from tests.fast.test_record_catalog import Client, Server

CLIENT_ID = "client-id-value-1"
CLIENT_VALUE = "client-value-2"
KEY_ID = "AKIAEXAMPLEEXAMPLE0"
KEY_VALUE = "access-value-3"
MARKER = "marker_command_8c3e71_never_in_an_output"


def setUpModule() -> None:
    pin_the_synthetic_list(list_text_for(RELEASE_FILES).encode())


class EnvironmentCase(unittest.TestCase):
    """A container environment with its secret files and its address file in a folder of its own."""

    def setUp(self) -> None:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="record_settings_test_")))
        files = {
            "AIRBYTE_CLIENT_ID_FILE": CLIENT_ID,
            "AIRBYTE_CLIENT_SECRET_FILE": CLIENT_VALUE,
            "AIRBYTE_AWS_ACCESS_KEY_ID_FILE": KEY_ID,
            "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE": KEY_VALUE,
        }
        self.environment = {
            "WAREHOUSE_LOCATION": "s3://example-bucket/warehouse",
            "AWS_REGION": "us-west-2",
            "NESSIE_URI": "http://nessie:19120/api/v2",
        }
        for name, value in files.items():
            path = folder / name.lower()
            path.write_text(value + "\n")
            self.environment[name] = str(path)
        (folder / "api_url").write_text("http://10.42.0.1:8001\n")
        self.environment["AIRBYTE_API_URL_FILE"] = str(folder / "api_url")
        (folder / "nessie_url").write_text("http://10.42.0.1:19120/api/v2\n")
        self.environment["AIRBYTE_NESSIE_URL_FILE"] = str(folder / "nessie_url")
        self.folder = folder


class SettingsTest(EnvironmentCase):
    def test_control_the_settings_are_read_with_their_defaults_and_the_secret_files_are_read_without_their_newline(self) -> None:
        settings = settings_from_environment(self.environment)
        self.assertEqual(
            ("example-bucket", "us-west-2", "warehouse"), (settings.environment.bucket, settings.environment.region, settings.environment.warehouse_path)
        )
        self.assertEqual("http://10.42.0.1:19120/api/v2", settings.environment.nessie_uri)
        self.assertEqual("http://nessie:19120/api/v2", settings.nessie_uri)
        self.assertEqual(
            (CLIENT_ID, CLIENT_VALUE, KEY_ID, KEY_VALUE),
            (settings.client_id, settings.client_secret, settings.aws_access_key_id, settings.aws_secret_access_key),
        )
        self.assertEqual("http://10.42.0.1:8001", settings.airbyte_url)
        self.assertEqual(
            (3600.0, 10.0, 120.0, "default"),
            (settings.deadlines.sync_seconds, settings.deadlines.poll_seconds, settings.deadlines.cancel_confirm_seconds, settings.aws_profile),
        )

    def test_a_missing_or_blank_setting_is_named(self) -> None:
        for name in ("WAREHOUSE_LOCATION", "AWS_REGION", "AIRBYTE_NESSIE_URL_FILE", "NESSIE_URI", "AIRBYTE_API_URL_FILE", "AIRBYTE_CLIENT_ID_FILE"):
            for value in (None, "", "  "):
                with self.subTest(name=name, value=value), self.assertRaises(SettingsError) as raised:
                    settings_from_environment({key: item for key, item in self.environment.items() if key != name} | ({} if value is None else {name: value}))
                self.assertIn(name, str(raised.exception))

    def test_a_secret_file_that_is_missing_or_empty_is_named_by_its_setting_and_shows_no_value_or_path(self) -> None:
        for name in ("AIRBYTE_CLIENT_ID_FILE", "AIRBYTE_CLIENT_SECRET_FILE", "AIRBYTE_AWS_ACCESS_KEY_ID_FILE", "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE"):
            path = Path(self.environment[name])
            for change in ("missing", "empty"):
                with self.subTest(name=name, change=change):
                    original = path.read_text()
                    path.unlink() if change == "missing" else path.write_text("\n")
                    try:
                        with self.assertRaises(SettingsError) as raised:
                            settings_from_environment(self.environment)
                    finally:
                        path.write_text(original)
                    self.assertIn(name, str(raised.exception))
                    for hidden in (str(self.folder), CLIENT_ID, CLIENT_VALUE, KEY_ID, KEY_VALUE):
                        self.assertNotIn(hidden, str(raised.exception))

    def test_a_file_of_invalid_utf_8_is_named_by_its_setting_like_a_missing_one_and_shows_no_path(self) -> None:
        """Join round 2, class of finding 2: ``UnicodeDecodeError`` is not an ``OSError``, so a file of invalid UTF-8 ended in a traceback."""
        for name in (
            "AIRBYTE_CLIENT_ID_FILE",
            "AIRBYTE_CLIENT_SECRET_FILE",
            "AIRBYTE_AWS_ACCESS_KEY_ID_FILE",
            "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE",
            "AIRBYTE_API_URL_FILE",
            "AIRBYTE_NESSIE_URL_FILE",
        ):
            path = Path(self.environment[name])
            original = path.read_bytes()
            # Under Latin-1 ``0xff`` is a letter and ``0x85`` after an address is white space that ``strip`` removes: only the encoding refuses them.
            invalid = original.rstrip(b"\n") + b"\x85\n" if name.endswith("_URL_FILE") else b"\xff\xfe\n"
            for locale_name, locale in (("UTF-8", contextlib.nullcontext()), ("ISO8859-1", non_utf_8_locale())):
                with self.subTest(name=name, locale=locale_name), locale:
                    path.write_bytes(invalid)
                    try:
                        with self.assertRaises(SettingsError) as raised:
                            settings_from_environment(self.environment)
                    finally:
                        path.write_bytes(original)
                    self.assertEqual(f"the file named by {name} cannot be read", str(raised.exception))

    def test_a_deadline_that_is_not_a_positive_number_is_refused(self) -> None:
        for value in ("0", "-5", "soon", "nan", "inf"):
            with self.subTest(value), self.assertRaises(SettingsError) as raised:
                settings_from_environment({**self.environment, "RECORD_SYNC_DEADLINE_SECONDS": value})
            self.assertIn("RECORD_SYNC_DEADLINE_SECONDS", str(raised.exception))

    def test_the_airbyte_address_and_the_catalog_address_must_be_plain_http_urls(self) -> None:
        with self.assertRaises(SettingsError) as raised:
            settings_from_environment({**self.environment, "NESSIE_URI": "https://elsewhere.example"})
        self.assertIn("NESSIE_URI", str(raised.exception))
        for text in ("https://10.42.0.1:19120/api/v2", "10.42.0.1:19120", "", "http://"):
            with self.subTest(text):
                (self.folder / "nessie_url").write_text(text + "\n")
                with self.assertRaises(SettingsError) as raised:
                    settings_from_environment(self.environment)
                self.assertIn("AIRBYTE_NESSIE_URL_FILE", str(raised.exception))

    def test_the_bucket_and_the_warehouse_path_come_from_the_warehouse_location_that_the_deploy_already_sets(self) -> None:
        settings = settings_from_environment({**self.environment, "WAREHOUSE_LOCATION": "s3://other-bucket/lake/warehouse"})
        self.assertEqual(("other-bucket", "lake/warehouse"), (settings.environment.bucket, settings.environment.warehouse_path))

    def test_a_warehouse_location_that_is_not_an_s3_location_of_the_project_is_refused(self) -> None:
        for text in (
            "",
            "s3://",
            "s3://example-bucket",
            "s3://example-bucket/",
            "http://example-bucket/warehouse",
            "s3://Example/x",
            "s3://example-bucket/landing/x",
            "s3://example-bucket/records_input",
            "s3://physionet-open/vitaldb",
            "s3://example-bucket/a b",
            "s3://example-bucket/../x",
        ):
            with self.subTest(text):
                with self.assertRaises(SettingsError) as raised:
                    settings_from_environment({**self.environment, "WAREHOUSE_LOCATION": text})
                self.assertIn("WAREHOUSE_LOCATION", str(raised.exception))


class ApiAddressTest(EnvironmentCase):
    """The Airbyte address is read from the file that the deploy wrote beside the credentials, and never from the environment. It is checked before any
    secret file is read, because both credentials go to it."""

    def api_url(self, text: str) -> None:
        (self.folder / "api_url").write_text(text)

    def test_the_address_comes_from_the_file_and_an_environment_value_for_it_is_ignored(self) -> None:
        settings = settings_from_environment({**self.environment, "AIRBYTE_API_URL": "http://8.8.8.8:80"})
        self.assertEqual("http://10.42.0.1:8001", settings.airbyte_url)

    def test_each_address_that_could_send_a_credential_elsewhere_is_refused_before_any_secret_file_is_read(self) -> None:
        for text in (
            "http://airbyte.example:8001",
            "http://8.8.8.8:8001",
            "http://u:p@10.42.0.1:8001",
            "http://10.42.0.1:8001/x",
            "http://[fd00::1]:8001",
            "https://10.42.0.1:8001",
            "",
        ):
            with self.subTest(text):
                self.api_url(text + "\n")
                broken = {**self.environment}
                for name in SECRET_FILES:
                    broken[name] = str(self.folder / "absent")
                with self.assertRaises(SettingsError) as raised:
                    settings_from_environment(broken)
                self.assertIn("AIRBYTE_API_URL_FILE", str(raised.exception))
                for name in SECRET_FILES:
                    self.assertNotIn(name, str(raised.exception), "no secret file was read")
                self.assertNotIn("8.8.8.8", str(raised.exception))
                self.assertNotIn("airbyte.example", str(raised.exception))

    def test_a_missing_address_file_is_named_by_its_setting_and_a_secret_file_is_still_not_read(self) -> None:
        (self.folder / "api_url").unlink()
        broken = {**self.environment, "AIRBYTE_CLIENT_ID_FILE": str(self.folder / "absent")}
        with self.assertRaises(SettingsError) as raised:
            settings_from_environment(broken)
        self.assertIn("AIRBYTE_API_URL_FILE", str(raised.exception))
        self.assertNotIn("AIRBYTE_CLIENT_ID_FILE", str(raised.exception))

    def test_the_cancel_confirmation_is_a_setting_with_a_positive_number(self) -> None:
        self.assertEqual(45.0, settings_from_environment({**self.environment, "RECORD_CANCEL_CONFIRM_SECONDS": "45"}).deadlines.cancel_confirm_seconds)
        for value in ("0", "-1", "x", "nan"):
            with self.subTest(value), self.assertRaises(SettingsError) as raised:
                settings_from_environment({**self.environment, "RECORD_CANCEL_CONFIRM_SECONDS": value})
            self.assertIn("RECORD_CANCEL_CONFIRM_SECONDS", str(raised.exception))


class CommandOutputTest(EnvironmentCase):
    """What the command prints when Nessie fails: a fixed code and an exit status, and no word of what Nessie, a proxy or a caller put in its answer."""

    def run_command(self, server: Server) -> tuple[int, str, str]:
        landed, reference = landed_records()
        (self.folder / "reference.json").write_text(reference.to_json())
        errors, output = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(record_run, "S3CopyStore", lambda *arguments: landed.store),
            mock.patch.object(record_run, "Aws", lambda *arguments: None),
            mock.patch.object(record_run, "HttpAirbyteApi", lambda *arguments: FakeAirbyte(FakeCatalog())),
            mock.patch.object(record_run, "NessieClient", lambda uri: Client(server)),
            mock.patch.dict("os.environ", {"RECORD_LOAD_DIR": str(self.folder / "loads")}),
            contextlib.redirect_stderr(errors),
            contextlib.redirect_stdout(output),
        ):
            code = main(
                ["load", "--run-id", "run_20261122_ab12cd34", "--reference", str(self.folder / "reference.json")],
                {**self.environment, "RECORD_LOAD_DIR": str(self.folder / "loads")},
            )
        return code, output.getvalue(), errors.getvalue()

    def failing(self, path: str, status: int = 500) -> Server:
        server = Server()
        server.fail = (path, status)
        server.fail_body = json.dumps({"errorCode": "X", "message": MARKER}).encode()
        return server

    def test_a_failing_history_call_prints_the_fixed_code_and_no_word_of_the_answer(self) -> None:
        for status in (500, 400, 403):
            with self.subTest(status):
                code, output, errors = self.run_command(self.failing("/trees/main/history", status))
                self.assertEqual(1, code)
                self.assertIn("CatalogError", errors)
                self.assertNotIn(MARKER, errors + output)

    def test_a_failing_reference_call_of_the_branch_check_prints_the_fixed_code_and_no_word_of_the_answer(self) -> None:
        code, output, errors = self.run_command(self.failing("/trees/load_run_20261122_ab12cd34"))
        self.assertEqual(1, code)
        self.assertIn("CatalogError", errors)
        self.assertNotIn(MARKER, errors + output)
        self.assertNotIn("Traceback", errors)


if __name__ == "__main__":
    unittest.main()
