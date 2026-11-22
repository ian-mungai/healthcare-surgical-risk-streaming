"""No S3 lifecycle rule may expire the live table paths: the warehouse prefix of the project's bucket.

Iceberg tables are live while Nessie names their metadata, and an object that a lifecycle rule expires is gone with no Iceberg or Nessie step. The project
declares no lifecycle configuration, so the check fails when the text of one appears in a Terraform file under ``infra/``, with no parsing of Terraform. It
reads the live bucket's rules in the form that S3 returns them. The cases cover the forms of a rule's scope (the whole bucket, a prefix above, equal to or below
the warehouse, a tag or size filter, the old top-level prefix), the actions that expire a live object and the ones that do not, a rule that is off, a status
that is neither Enabled nor Disabled, and what counts as no configuration. The runs for the lead on the host read the live bucket, because a build reaches
no cloud.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.stack import lifecycle
from scripts.stack.warehouse import WAREHOUSE_PREFIX

PROTECTED = f"{WAREHOUSE_PREFIX}/"
INFRA = Path(__file__).resolve().parents[2] / "infra"
CONFIGURATION = {"S3_BUCKET": "bucket-name", "AWS_PROFILE": "profile_name", "AWS_REGION": "us-west-2"}


def rule(rule_id: str, scope: dict[str, Any] | None, *, expiration: dict[str, Any] | None = None, status: str = "Enabled", **actions: Any) -> dict[str, Any]:
    document: dict[str, Any] = {"ID": rule_id, "Status": status, **actions}
    if scope is not None:
        document["Filter"] = scope
    if expiration is not None:
        document["Expiration"] = expiration
    return document


def problems(*rules: dict[str, Any]) -> list[str]:
    return lifecycle.expiring_rule_problems({"Rules": list(rules)}, PROTECTED)


class ScopeTest(unittest.TestCase):
    def test_a_bucket_with_no_rule_has_no_problem(self) -> None:
        self.assertEqual([], lifecycle.expiring_rule_problems({}, PROTECTED))
        self.assertEqual([], lifecycle.expiring_rule_problems({"Rules": []}, PROTECTED))

    def test_an_expiring_rule_that_covers_the_warehouse_is_named_whatever_the_form_of_its_scope(self) -> None:
        for label, scope in (
            ("empty prefix", {"Prefix": ""}),
            ("no filter fields", {}),
            ("prefix above", {"Prefix": "ware"}),
            ("prefix without its slash", {"Prefix": "warehouse"}),
            ("the warehouse", {"Prefix": "warehouse/"}),
            ("a table folder", {"Prefix": "warehouse/catalog_proof/"}),
            ("a tag only", {"Tag": {"Key": "temp", "Value": "yes"}}),
            ("a size only", {"ObjectSizeGreaterThan": 1}),
            ("tags without a prefix", {"And": {"Tags": [{"Key": "temp", "Value": "yes"}]}}),
            ("the warehouse and a tag", {"And": {"Prefix": "warehouse/", "Tags": [{"Key": "temp", "Value": "yes"}]}}),
        ):
            with self.subTest(label):
                found = problems(rule("cleanup", scope, expiration={"Days": 30}))
                self.assertEqual(1, len(found))
                self.assertIn("cleanup", found[0])
                self.assertIn(PROTECTED, found[0])

    def test_a_rule_with_no_filter_at_all_covers_the_whole_bucket(self) -> None:
        self.assertEqual(1, len(problems(rule("cleanup", None, expiration={"Days": 30}))))

    def test_the_old_top_level_prefix_is_read_like_a_filter_prefix(self) -> None:
        self.assertEqual(1, len(problems(rule("old", None, expiration={"Days": 5}, Prefix="warehouse/"))))
        self.assertEqual([], problems(rule("old", None, expiration={"Days": 5}, Prefix="landing/")))

    def test_a_rule_whose_scope_is_clear_of_the_warehouse_passes(self) -> None:
        for prefix in ("landing/", "state/", "warehouse-old/", "airbyte/", "wa/"):
            with self.subTest(prefix):
                self.assertEqual([], problems(rule("cleanup", {"Prefix": prefix}, expiration={"Days": 30})))
        self.assertEqual([], problems(rule("cleanup", {"And": {"Prefix": "landing/", "Tags": [{"Key": "temp", "Value": "yes"}]}}, expiration={"Days": 30})))

    def test_a_rule_that_is_off_passes(self) -> None:
        self.assertEqual([], problems(rule("cleanup", {"Prefix": ""}, expiration={"Days": 30}, status="Disabled")))

    def test_a_status_that_is_not_exactly_enabled_or_disabled_is_a_problem_whatever_the_rule_does(self) -> None:
        for status in ("enabled", "ENABLED", "Enabld", "", "On", 1, None):
            with self.subTest(status=status):
                for actions in ({"Expiration": {"Days": 30}}, {"NoncurrentVersionExpiration": {"NoncurrentDays": 30}}, {}):
                    found = problems({"ID": "cleanup", "Status": status, "Filter": {"Prefix": "warehouse/"}, **actions})
                    self.assertEqual(1, len(found))
                    self.assertIn("cleanup", found[0])
                    self.assertIn("Status", found[0])

    def test_a_rule_with_no_status_at_all_and_an_expiration_of_the_warehouse_is_a_problem(self) -> None:
        document = {"ID": "nostatus", "Filter": {"Prefix": "warehouse/"}, "Expiration": {"Days": 30}}
        found = lifecycle.expiring_rule_problems({"Rules": [document]}, PROTECTED)
        self.assertEqual(1, len(found))
        self.assertIn("nostatus", found[0])


class ActionTest(unittest.TestCase):
    def test_an_expiration_by_date_expires_a_live_object_too(self) -> None:
        self.assertEqual(1, len(problems(rule("dated", {"Prefix": ""}, expiration={"Date": "2027-01-01T00:00:00Z"}))))

    def test_the_actions_that_remove_no_live_object_pass_on_the_whole_bucket(self) -> None:
        for label, actions in (
            ("incomplete uploads", {"AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}),
            ("old versions", {"NoncurrentVersionExpiration": {"NoncurrentDays": 30}}),
            ("delete markers", {"Expiration": {"ExpiredObjectDeleteMarker": True}}),
            ("a transition", {"Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]}),
        ):
            with self.subTest(label):
                self.assertEqual([], problems(rule("housekeeping", {"Prefix": ""}, **actions)))

    def test_every_offending_rule_is_named_and_a_clear_rule_beside_it_is_not(self) -> None:
        found = problems(
            rule("first", {"Prefix": ""}, expiration={"Days": 1}),
            rule("clear", {"Prefix": "landing/"}, expiration={"Days": 1}),
            rule("second", {"Prefix": "warehouse/x/"}, expiration={"Days": 1}),
        )
        self.assertEqual(2, len(found))
        self.assertIn("first", found[0])
        self.assertIn("second", found[1])

    def test_a_configuration_that_is_not_a_list_of_rules_is_refused(self) -> None:
        for document in ({"Rules": "x"}, {"Rules": [1]}):
            with self.subTest(document=document), self.assertRaises(ValueError):
                lifecycle.expiring_rule_problems(document, PROTECTED)


BAD_NAMES = ("aws_s3_bucket_lifecycle_configuration", "lifecycle_rule")
REVIEWER_TF = """
resource "aws_s3_bucket_lifecycle_configuration" "b" {
  bucket = "b"
  rule {
    id     = "cleanup \\"}"
    status = "Enabled"
    filter {
      prefix = "warehouse/"
    }
    expiration {
      days = 30
    }
  }
}
"""
REVIEWER_JSON = """{
  "resource": {
    "aws_s3_bucket_lifecycle_configuration": {
      "b": {
        "bucket": "b",
        "rule": [{"id": "cleanup", "status": "Enabled", "filter": {"prefix": "warehouse/"}, "expiration": {"days": 30}}]
      }
    }
  }
}
"""


def declared(files: dict[str, str]) -> list[str]:
    """The problems that ``declared_problems`` finds in a temporary ``infra`` folder holding ``files`` (a name with folders in it makes them)."""
    with tempfile.TemporaryDirectory() as directory:
        for name, text in files.items():
            path = Path(directory, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        return lifecycle.declared_problems(directory)


class DeclaredPresenceTest(unittest.TestCase):
    """The project declares no lifecycle configuration: the words of one anywhere in a Terraform file are a problem. Nothing is parsed."""

    def test_the_reviewers_block_with_an_escaped_quote_and_brace_in_its_id_is_found(self) -> None:
        found = declared({"bucket.tf": REVIEWER_TF})
        self.assertEqual(1, len(found))
        self.assertIn("bucket.tf:2", found[0])
        self.assertIn("aws_s3_bucket_lifecycle_configuration", found[0])

    def test_the_same_rule_in_the_json_form_of_terraform_is_found(self) -> None:
        found = declared({"bucket.tf.json": REVIEWER_JSON})
        self.assertEqual(1, len(found))
        self.assertIn("bucket.tf.json:3", found[0])

    def test_a_comment_that_only_mentions_the_resource_type_is_refused_too_by_design(self) -> None:
        for text in ("# aws_s3_bucket_lifecycle_configuration\n", "// see aws_s3_bucket_lifecycle_configuration\n", "/* lifecycle_rule */\n"):
            with self.subTest(text=text):
                self.assertEqual(1, len(declared({"notes.tf": text})))

    def test_the_old_lifecycle_rule_block_is_found_and_each_line_of_each_word_is_named(self) -> None:
        text = 'resource "aws_s3_bucket" "b" {\n  lifecycle_rule {\n    enabled = true\n  }\n}\nresource "aws_s3_bucket_lifecycle_configuration" "c" {}\n'
        found = declared({"main.tf": text})
        self.assertEqual(2, len(found))
        self.assertIn("main.tf:2", found[0])
        self.assertIn("main.tf:6", found[1])

    def test_a_word_on_two_lines_of_one_file_names_both_lines(self) -> None:
        found = declared({"main.tf": "lifecycle_rule\n\nlifecycle_rule\n"})
        self.assertEqual(["main.tf:1", "main.tf:3"], [item.split(":")[0] + ":" + item.split(":")[1] for item in found])

    def test_the_message_says_that_a_lifecycle_rule_needs_a_deliberate_change_of_this_check(self) -> None:
        self.assertIn("deliberate change of this check", declared({"main.tf": "lifecycle_rule"})[0])

    def test_the_search_is_case_sensitive_and_only_the_two_file_endings_are_searched(self) -> None:
        self.assertEqual([], declared({"main.tf": "AWS_S3_BUCKET_LIFECYCLE_CONFIGURATION Lifecycle_Rule\n"}))
        for name in ("notes.md", "vars.tfvars", "main.tf.bak", "main.hcl", "tf", "x.tf.txt"):
            with self.subTest(name=name):
                self.assertEqual([], declared({name: "aws_s3_bucket_lifecycle_configuration lifecycle_rule\n"}))

    def test_a_file_in_a_nested_folder_is_found_with_its_path(self) -> None:
        found = declared({"modules/data/main.tf": "lifecycle_rule\n"})
        self.assertEqual(1, len(found))
        self.assertIn("modules/data/main.tf:1", found[0])

    def test_a_folder_that_is_a_symbolic_link_is_not_followed_and_a_file_that_is_one_is_read(self) -> None:
        with tempfile.TemporaryDirectory() as outer:
            infra, elsewhere = Path(outer, "infra"), Path(outer, "elsewhere")
            infra.mkdir()
            elsewhere.mkdir()
            (elsewhere / "bad.tf").write_text("lifecycle_rule\n")
            os.symlink(elsewhere, infra / "linked_folder")
            self.assertEqual([], lifecycle.declared_problems(infra))
            os.symlink(elsewhere / "bad.tf", infra / "linked_file.tf")
            found = lifecycle.declared_problems(infra)
        self.assertEqual(1, len(found))
        self.assertIn("linked_file.tf:1", found[0])

    def test_a_folder_that_holds_no_such_word_is_clear_and_a_file_that_is_not_utf8_is_still_searched(self) -> None:
        self.assertEqual([], declared({"main.tf": 'resource "aws_s3_bucket" "b" {}\n'}))
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "odd.tf").write_bytes(b"\xff\xfe lifecycle_rule\n")
            self.assertEqual(1, len(lifecycle.declared_problems(directory)))

    def test_the_repository_declares_no_lifecycle_configuration(self) -> None:
        self.assertEqual([], lifecycle.declared_problems(INFRA))
        self.assertTrue(list(INFRA.rglob("*.tf")))


def cli_error(code: str, operation: str = "GetBucketLifecycleConfiguration", message: str = "The lifecycle configuration does not exist") -> str:
    """One line of the AWS CLI's error form, as it writes it to the error output."""
    return f"\nAn error occurred ({code}) when calling the {operation} operation: {message}\n"


NO_CONFIGURATION_LINE = cli_error("NoSuchLifecycleConfiguration")
DENIED_LINE = cli_error("AccessDenied", message="Access Denied")


def run_check(stdout: str = "", stderr: str = "", returncode: int = 0, *, timeout: bool = False) -> tuple[int, str, str]:
    """``lifecycle check`` with the real shared client: only the program that it starts is stubbed, so the client's own error handling is in the test."""
    out, err = io.StringIO(), io.StringIO()
    completed = subprocess.CompletedProcess(["aws"], returncode, stdout, stderr)
    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory, "runs", "raw_errors")
        stub = (
            mock.patch("scripts.aws_cli.run_command", side_effect=TimeoutExpired("aws", 1))
            if timeout
            else mock.patch("scripts.aws_cli.run_command", return_value=completed)
        )
        with (
            mock.patch.object(lifecycle, "load_configuration", return_value=CONFIGURATION),
            mock.patch("scripts.aws_cli.RAW_ERRORS", raw),
            mock.patch.object(lifecycle, "RAW_ERRORS", raw),
            stub,
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = lifecycle.main(["check"])
    return code, out.getvalue(), err.getvalue()


def answer(document: Any) -> tuple[int, str, str]:
    return run_check(stdout=json.dumps(document))


class NoConfigurationTest(unittest.TestCase):
    """ "No configuration" is accepted only from exactly one error line of the CLI's form that has the code and the operation that was asked for."""

    def test_the_exact_form_of_the_error_for_the_operation_that_was_asked_for_is_no_configuration(self) -> None:
        code, out, _ = run_check(stderr=NO_CONFIGURATION_LINE, returncode=254)
        self.assertEqual(0, code)
        self.assertIn("no lifecycle configuration", out)

    def test_a_diagnostic_line_that_mentions_the_error_before_the_real_error_exits_1(self) -> None:
        for diagnostic in (
            "An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: retrying\n",
            "warning: An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: retrying\n",
            "An error occurred (NoSuchLifecycleConfiguration)\n",
        ):
            with self.subTest(diagnostic=diagnostic[:40]):
                code, out, err = run_check(stderr=diagnostic + DENIED_LINE, returncode=255)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)
                self.assertIn("not exactly one error line", err)

    def test_the_real_error_first_and_a_mention_after_it_exits_1_too(self) -> None:
        code, out, _ = run_check(
            stderr=DENIED_LINE + "An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: x\n",
            returncode=255,
        )
        self.assertEqual(1, code)
        self.assertNotIn("no lifecycle configuration", out)

    def test_two_error_lines_even_of_the_right_code_exit_1(self) -> None:
        code, out, _ = run_check(stderr=NO_CONFIGURATION_LINE + NO_CONFIGURATION_LINE, returncode=254)
        self.assertEqual(1, code)
        self.assertNotIn("no lifecycle configuration", out)

    def test_the_right_code_for_another_operation_exits_1(self) -> None:
        for operation in ("GetBucketPolicy", "GetBucketLifecycle", "PutBucketLifecycleConfiguration", "GetBucketLifecycleConfigurationX"):
            with self.subTest(operation=operation):
                code, out, _ = run_check(stderr=cli_error("NoSuchLifecycleConfiguration", operation), returncode=254)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)

    def test_another_code_for_the_right_operation_exits_1(self) -> None:
        for code_name in ("AccessDenied", "NoSuchBucket", "NoSuchLifecycleConfigurationX", "NoSuchLifecycle", "nosuchlifecycleconfiguration"):
            with self.subTest(code=code_name):
                code, out, _ = run_check(stderr=cli_error(code_name), returncode=254)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)

    def test_the_code_in_a_line_that_is_not_an_error_line_only_exits_1(self) -> None:
        for text in (
            "NoSuchLifecycleConfiguration\n",
            "debug: the call said NoSuchLifecycleConfiguration\n",
            "  An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: x\n",
            "notes: An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation: x\n",
            DENIED_LINE + "NoSuchLifecycleConfiguration\n",
        ):
            with self.subTest(text=text[:40]):
                code, out, _ = run_check(stderr=text, returncode=255)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)

    def test_an_error_with_no_error_line_a_malformed_one_or_a_timeout_exits_1(self) -> None:
        for text in (
            "",
            "Unable to locate credentials\n",
            "An error occurred (NoSuchLifecycleConfiguration) when calling the GetBucketLifecycleConfiguration operation\n",
        ):
            with self.subTest(text=text[:40]):
                code, out, _ = run_check(stderr=text, returncode=255)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)
        self.assertEqual(1, run_check(timeout=True)[0])

    def test_crlf_line_ends_do_not_change_the_reading(self) -> None:
        self.assertEqual(0, run_check(stderr=NO_CONFIGURATION_LINE.replace("\n", "\r\n"), returncode=254)[0])
        self.assertEqual(1, run_check(stderr=(NO_CONFIGURATION_LINE + DENIED_LINE).replace("\n", "\r\n"), returncode=254)[0])


class CommandTest(unittest.TestCase):
    def test_a_bucket_whose_rules_expire_the_warehouse_exits_1_and_names_the_rule(self) -> None:
        code, out, err = answer({"Rules": [rule("cleanup", {"Prefix": ""}, expiration={"Days": 30})]})
        self.assertEqual(1, code)
        self.assertIn("cleanup", err)
        self.assertNotIn("ok", out)

    def test_a_bucket_with_a_clear_rule_exits_0(self) -> None:
        self.assertEqual(0, answer({"Rules": [rule("cleanup", {"Prefix": "landing/"}, expiration={"Days": 30})]})[0])

    def test_a_successful_answer_that_is_not_a_json_object_with_a_list_of_rules_exits_1_and_is_never_taken_for_no_rules(self) -> None:
        documents: list[Any] = [None, {}, {"Rules": []}, {"Rules": "x"}, {"Rules": [1]}, {"Rules": None}, {"Other": 1}, [], "", 0, "null"]
        for document in documents:
            with self.subTest(document=document):
                code, out, err = answer(document)
                self.assertEqual(1, code)
                self.assertNotIn("no lifecycle configuration", out)
                self.assertNotIn("no rule", out)
                self.assertIn("lifecycle", err)

    def test_an_empty_answer_exits_1(self) -> None:
        self.assertEqual(1, run_check(stdout="")[0])

    def test_a_rule_whose_status_is_missing_or_not_exact_exits_1(self) -> None:
        missing = {"ID": "x", "Filter": {"Prefix": "warehouse/"}, "Expiration": {"Days": 30}}
        self.assertEqual(1, answer({"Rules": [missing]})[0])
        self.assertEqual(1, answer({"Rules": [rule("x", {"Prefix": "landing/"}, expiration={"Days": 30}, status="enabled")]})[0])

    def test_rules_that_expire_no_current_object_exit_0_on_the_whole_bucket(self) -> None:
        document = {
            "Rules": [
                rule("old versions", {"Prefix": ""}, NoncurrentVersionExpiration={"NoncurrentDays": 30}),
                rule("uploads", {"Prefix": ""}, AbortIncompleteMultipartUpload={"DaysAfterInitiation": 7}),
            ]
        }
        self.assertEqual(0, answer(document)[0])

    def test_a_failed_read_exits_1_names_the_code_and_is_never_taken_for_no_rules(self) -> None:
        code, out, err = run_check(stderr=DENIED_LINE, returncode=254)
        self.assertEqual(1, code)
        self.assertIn("AccessDenied", err)
        self.assertNotIn("no rule", out)


if __name__ == "__main__":
    unittest.main()
