"""The configured image repository is one Docker Hub ``<namespace>/<repository>``: no registry host, no tag, no digest, no uppercase."""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from scripts.project_config import OPTIONAL_PATTERNS, ROOT, ConfigurationError, load_configuration

BASE = "AWS_REGION=us-west-2\nAWS_PROFILE=p\nAWS_ACCOUNT_ID=" + "1" * 12 + "\nPROJECT_NAME=p\nENVIRONMENT=dev\nS3_BUCKET=bucket-one\n"
GOOD = ("acct/images", "a/b", "acct1/my.images_2", "0acct/images__x--y", "acct/images-2")
BAD = (
    "Acct/images",
    "acct/Images",
    "acct",
    "acct/",
    "/images",
    "acct/images/extra",
    "docker.io/images",
    "docker.io/acct/images",
    "my.registry/images",
    "localhost/images",
    "localhost:5000/images",
    "my-acct/images",
    "my_acct/images",
    "acct/images:tag",
    "acct/images:latest",
    "acct/images@sha256:" + "a" * 64,
    "acct/-images",
    "acct/images-",
    "acct/images..x",
    "acct/.images",
    "acct/images___x",
    "acct/ima ges",
)


class RepositoryFormTest(unittest.TestCase):
    def configuration(self, value: str) -> Path:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        path = Path(folder.name) / ".env"
        path.write_text(BASE + f"IMAGE_REPOSITORY={value}\n")
        return path

    def test_a_docker_hub_namespace_and_repository_in_lowercase_is_accepted_as_it_is(self) -> None:
        for value in GOOD:
            with self.subTest(value=value):
                self.assertEqual(value, load_configuration(self.configuration(value))["IMAGE_REPOSITORY"])

    def test_every_other_form_is_refused_by_the_key_name_and_never_the_value(self) -> None:
        for value in BAD:
            with self.subTest(value=value):
                with self.assertRaises(ConfigurationError) as raised:
                    load_configuration(self.configuration(value))
                self.assertIn("IMAGE_REPOSITORY", str(raised.exception))
                self.assertNotIn(value, str(raised.exception))

    def test_the_placeholder_of_the_example_file_is_a_value_that_the_validator_accepts(self) -> None:
        text = (ROOT / ".env.example").read_text()
        found = re.findall(r"(?m)^# IMAGE_REPOSITORY=(\S+)$", text)
        self.assertEqual(1, len(found), "one commented placeholder line")
        self.assertIsNotNone(OPTIONAL_PATTERNS["IMAGE_REPOSITORY"].fullmatch(found[0]), "a copy that only removes the comment mark must be accepted")


if __name__ == "__main__":
    unittest.main()
