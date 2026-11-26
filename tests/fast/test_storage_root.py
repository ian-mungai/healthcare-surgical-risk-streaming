"""The storage Terraform root makes the project's one bucket on AIStor, with versioning and no anonymous access, and holds no credential.

The text checks read the root's files as plain text, as the lifecycle check of ``test_bucket_lifecycle`` does for ``infra/``. The Terraform tests of the root
(``infra/storage/tests``, run by ``make check-terraform``) prove the plan with a mock provider. The planted cases here prove that each text check still looks.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from scripts.project_config import PATTERNS
from scripts.stack import lifecycle
from tests.fast.git_fixture import ROOT

STORAGE = ROOT / "infra" / "storage"
PROVIDER = "aminueza/minio"
VERSION = "3.46.2"
# An attribute or a variable that would carry a credential into the configuration, the plan or the state.
CREDENTIAL_WORDS = re.compile(r"minio_password|minio_user|access_key|secret_key|session_token|minio_credentials|password|secret", re.IGNORECASE)
ANONYMOUS_WORDS = re.compile(r"minio_s3_bucket_policy|minio_s3_bucket_anonymous|anonymous|\"\*\"|public-read", re.IGNORECASE)


def sources(folder: Path = STORAGE) -> dict[str, str]:
    return {path.name: path.read_text() for path in sorted(folder.glob("*.tf"))}


def credential_problems(files: dict[str, str]) -> list[str]:
    """Each line of a root file that names a credential, as the file and the line number."""
    return [f"{name}:{number}" for name, text in files.items() for number, line in enumerate(text.splitlines(), 1) if CREDENTIAL_WORDS.search(line)]


def anonymous_problems(files: dict[str, str]) -> list[str]:
    return [f"{name}:{number}" for name, text in files.items() for number, line in enumerate(text.splitlines(), 1) if ANONYMOUS_WORDS.search(line)]


def provider_pin_problems(versions: str, lock: str) -> list[str]:
    found: list[str] = []
    if not re.search(rf'source\s*=\s*"{PROVIDER}"\s+version\s*=\s*"={" "}{re.escape(VERSION)}"', versions):
        found.append(f"versions.tf: {PROVIDER} must be pinned to exactly {VERSION}")
    if not re.search(
        rf'provider "registry\.terraform\.io/{PROVIDER}" \{{\s+version\s*=\s*"{re.escape(VERSION)}"\s+constraints\s*=\s*"= {re.escape(VERSION)}"', lock
    ):
        found.append(f"the lock file must record {PROVIDER} {VERSION} with the constraint = {VERSION}")
    return found


class StorageRootFilesTest(unittest.TestCase):
    def test_the_root_has_the_standard_files_and_a_lock_and_a_test(self) -> None:
        names = {path.name for path in STORAGE.iterdir()}
        self.assertTrue({"main.tf", "variables.tf", "outputs.tf", "versions.tf", ".terraform.lock.hcl", "tests"} <= names, names)
        self.assertTrue(list((STORAGE / "tests").glob("*.tftest.hcl")))

    def test_the_provider_is_pinned_to_the_exact_release_in_the_version_file_and_in_the_lock(self) -> None:
        self.assertEqual([], provider_pin_problems((STORAGE / "versions.tf").read_text(), (STORAGE / ".terraform.lock.hcl").read_text()))

    def test_the_pin_check_names_a_looser_constraint_another_release_and_another_source(self) -> None:
        versions = (STORAGE / "versions.tf").read_text()
        lock = (STORAGE / ".terraform.lock.hcl").read_text()
        for label, planted in (
            ("a range", versions.replace(f'"= {VERSION}"', f'"~> {VERSION}"')),
            ("a newer release", versions.replace(VERSION, "3.47.0")),
            ("no operator", versions.replace(f'"= {VERSION}"', f'"{VERSION}"')),
            ("another source", versions.replace(PROVIDER, "other/minio")),
        ):
            with self.subTest(label):
                self.assertEqual(1, len(provider_pin_problems(planted, lock)))
        self.assertEqual(1, len(provider_pin_problems(versions, lock.replace(VERSION, "3.47.0"))))
        self.assertEqual(1, len(provider_pin_problems(versions, lock.replace(f'"= {VERSION}"', f'"~> {VERSION}"'))))

    def test_the_state_is_local_with_no_path_in_the_files_so_the_run_gives_it(self) -> None:
        versions = (STORAGE / "versions.tf").read_text()
        self.assertRegex(versions, r'backend "local" \{\}')
        self.assertNotRegex("".join(sources().values()), r"\bpath\s*=")
        self.assertNotIn('backend "s3"', "".join(sources().values()))

    def test_no_root_file_names_a_credential_so_none_reaches_the_plan_or_the_state(self) -> None:
        self.assertEqual([], credential_problems(sources()))

    def test_the_credential_check_finds_each_spelling_in_a_planted_line(self) -> None:
        for word in ("minio_password = var.x", "minio_user = var.x", "access_key = 1", "secret_key = 1", 'variable "password" {}', "session_token = 1"):
            with self.subTest(word=word):
                self.assertEqual(["main.tf:2"], credential_problems({"main.tf": f"# a\n{word}\n"}))

    def test_no_policy_or_anonymous_setting_is_declared_and_the_bucket_acl_is_private(self) -> None:
        files = sources()
        self.assertEqual([], anonymous_problems(files))
        self.assertRegex(files["main.tf"], r'acl\s*=\s*"private"')
        for word in ("minio_s3_bucket_policy", "anonymous access", "public-read", 'Principal = "*"'):
            with self.subTest(word=word):
                self.assertEqual(["main.tf:1"], anonymous_problems({"main.tf": f"{word}\n"}))

    def test_the_bucket_is_versioned_from_its_first_write_and_the_name_is_the_one_the_run_gives(self) -> None:
        text = sources()["main.tf"]
        self.assertRegex(
            text,
            r'resource "minio_s3_bucket_versioning" "data" \{\s+bucket\s*=\s*minio_s3_bucket\.data\.bucket\s+'
            r'versioning_configuration \{\s+status\s*=\s*"Enabled"',
        )
        self.assertRegex(text, r'resource "minio_s3_bucket" "data" \{\s+bucket\s*=\s*var\.bucket_name')
        self.assertRegex(text, r"force_destroy\s*=\s*false")

    def test_the_bucket_name_variable_takes_the_name_form_of_the_project_bucket_and_the_endpoint_variable_names_a_host_and_a_port(self) -> None:
        text = sources()["variables.tf"]
        self.assertIn(PATTERNS["S3_BUCKET"].pattern, text.replace("\\\\", "\\"))
        self.assertRegex(text, r'variable "endpoint"')

    def test_no_lifecycle_rule_is_declared_because_the_s3_bucket_has_none_and_the_repository_check_passes_over_the_root(self) -> None:
        self.assertEqual([], lifecycle.declared_problems(ROOT / "infra"))
        self.assertIn(STORAGE / "main.tf", list((ROOT / "infra").rglob("*.tf")), "the repository check reads the storage root too")

    def test_the_provider_block_sets_only_the_server_and_the_tls_switch_and_the_credentials_come_from_the_environment(self) -> None:
        block = re.search(r'provider "minio" \{(.*?)\n\}', sources()["main.tf"], re.DOTALL)
        self.assertIsNotNone(block)
        keys = re.findall(r"^\s*(\w+)\s*=", block.group(1) if block else "", re.MULTILINE)
        self.assertEqual(["minio_server", "minio_ssl"], keys)

    def test_make_check_terraform_formats_validates_and_tests_the_storage_root_with_the_pinned_terraform_and_a_data_folder_of_its_own(self) -> None:
        recipe = re.search(r"^check-terraform:\n((?:\t.*\n)+)", (ROOT / "Makefile").read_text(), re.MULTILINE)
        self.assertIsNotNone(recipe)
        lines = (recipe.group(1) if recipe else "").splitlines()
        for command in ("init -backend=false -input=false", "validate", "test"):
            with self.subTest(command=command):
                found = [line for line in lines if f"-chdir=infra/storage {command}" in line]
                self.assertEqual(1, len(found), lines)
                self.assertIn("$(TERRAFORM)", found[0])
                self.assertIn("TF_DATA_DIR=$(CURDIR)/.tools/terraform/static_storage", found[0])
        self.assertTrue(any("fmt -check -recursive infra" in line for line in lines), "the formatter covers the root through the recursive check of infra")

    def test_the_state_and_plan_folders_are_ignored_by_git(self) -> None:
        ignore = (ROOT / ".gitignore").read_text()
        for needle in ("runs/", "*.tfstate", "*.tfplan", ".terraform/"):
            self.assertIn(needle, ignore)


if __name__ == "__main__":
    unittest.main()
