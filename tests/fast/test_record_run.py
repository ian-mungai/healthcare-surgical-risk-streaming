"""The settings of a record load: what the code location container must set, and the secret files it reads.

A missing or blank setting stops the load before any call and names the setting. A secret file that is missing or empty is named by its role, and no message
holds a secret value or a path.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.ingestion.record_run import SettingsError, settings_from_environment

CLIENT_ID = "client-id-value-1"
CLIENT_VALUE = "client-value-2"
KEY_ID = "AKIAEXAMPLEEXAMPLE0"
KEY_VALUE = "access-value-3"


class SettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="record_settings_test_")))
        files = {
            "AIRBYTE_CLIENT_ID_FILE": CLIENT_ID,
            "AIRBYTE_CLIENT_SECRET_FILE": CLIENT_VALUE,
            "AIRBYTE_AWS_ACCESS_KEY_ID_FILE": KEY_ID,
            "AIRBYTE_AWS_SECRET_ACCESS_KEY_FILE": KEY_VALUE,
        }
        self.environment = {
            "RECORD_BUCKET": "example-bucket",
            "RECORD_REGION": "us-west-2",
            "RECORD_AIRBYTE_NESSIE_URI": "http://nessie.example:19120/api/v2",
            "NESSIE_URI": "http://nessie:19120/api/v2",
            "AIRBYTE_API_URL": "http://airbyte.example:8001",
        }
        for name, value in files.items():
            path = folder / name.lower()
            path.write_text(value + "\n")
            self.environment[name] = str(path)
        self.folder = folder

    def test_control_the_settings_are_read_with_their_defaults_and_the_secret_files_are_read_without_their_newline(self) -> None:
        settings = settings_from_environment(self.environment)
        self.assertEqual(
            ("example-bucket", "us-west-2", "warehouse"), (settings.environment.bucket, settings.environment.region, settings.environment.warehouse_path)
        )
        self.assertEqual("http://nessie.example:19120/api/v2", settings.environment.nessie_uri)
        self.assertEqual("http://nessie:19120/api/v2", settings.nessie_uri)
        self.assertEqual(
            (CLIENT_ID, CLIENT_VALUE, KEY_ID, KEY_VALUE),
            (settings.client_id, settings.client_secret, settings.aws_access_key_id, settings.aws_secret_access_key),
        )
        self.assertEqual((3600.0, 10.0, "default"), (settings.deadlines.sync_seconds, settings.deadlines.poll_seconds, settings.aws_profile))

    def test_a_missing_or_blank_setting_is_named(self) -> None:
        for name in ("RECORD_BUCKET", "RECORD_REGION", "RECORD_AIRBYTE_NESSIE_URI", "NESSIE_URI", "AIRBYTE_API_URL", "AIRBYTE_CLIENT_ID_FILE"):
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

    def test_a_deadline_that_is_not_a_positive_number_is_refused(self) -> None:
        for value in ("0", "-5", "soon", "nan", "inf"):
            with self.subTest(value), self.assertRaises(SettingsError) as raised:
                settings_from_environment({**self.environment, "RECORD_SYNC_DEADLINE_SECONDS": value})
            self.assertIn("RECORD_SYNC_DEADLINE_SECONDS", str(raised.exception))

    def test_the_airbyte_address_and_the_catalog_address_must_be_plain_http_urls(self) -> None:
        for name in ("AIRBYTE_API_URL", "NESSIE_URI", "RECORD_AIRBYTE_NESSIE_URI"):
            with self.subTest(name), self.assertRaises(SettingsError) as raised:
                settings_from_environment({**self.environment, name: "https://elsewhere.example"})
            self.assertIn(name, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
