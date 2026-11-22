"""No S3 lifecycle rule may expire the live table paths: the warehouse prefix of the project's bucket.

Iceberg tables are live while Nessie names their metadata, and an object that a lifecycle rule expires is gone with no Iceberg or Nessie step. The check reads a
lifecycle configuration in the form that S3 returns it, reads the rules that Terraform declares in the repository and reads the live bucket. The cases cover the
forms of a rule's scope (the whole bucket, a prefix above, equal to or below the warehouse, a tag or size filter, the old top-level prefix), the actions that
expire a live object and the ones that do not, and a rule that is off. The runs for the lead on the host read the live bucket, because a build reaches no cloud.
"""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import AwsError
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


BAD_TERRAFORM = """
resource "aws_s3_bucket_lifecycle_configuration" "project" {
  bucket = "b"
  rule {
    id     = "cleanup"
    status = "Enabled"
    filter {
      prefix = "%s"
    }
    expiration {
      days = 30
    }
  }
  rule {
    id     = "uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
"""


class TerraformTest(unittest.TestCase):
    def test_a_declared_rule_is_read_in_the_form_of_s3(self) -> None:
        rules = lifecycle.terraform_rules(BAD_TERRAFORM % "landing/")
        self.assertEqual(["cleanup", "uploads"], [item["ID"] for item in rules])
        self.assertEqual({"Prefix": "landing/"}, rules[0]["Filter"])
        self.assertEqual({"Days": 30}, rules[0]["Expiration"])
        self.assertEqual("Enabled", rules[0]["Status"])
        self.assertNotIn("Expiration", rules[1])

    def test_a_declared_rule_that_expires_the_warehouse_is_a_problem_and_a_clear_one_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bucket.tf").write_text(BAD_TERRAFORM % "warehouse/")
            found = lifecycle.declared_problems(directory, PROTECTED)
            Path(directory, "bucket.tf").write_text(BAD_TERRAFORM % "landing/")
            clear = lifecycle.declared_problems(directory, PROTECTED)
        self.assertEqual(1, len(found))
        self.assertIn("bucket.tf", found[0])
        self.assertIn("cleanup", found[0])
        self.assertEqual([], clear)

    def test_a_prefix_that_is_not_a_literal_and_a_block_of_the_old_form_cannot_be_judged_and_are_problems(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bucket.tf").write_text(BAD_TERRAFORM.replace('"%s"', "var.prefix"))
            variable = lifecycle.declared_problems(directory, PROTECTED)
            Path(directory, "bucket.tf").write_text('resource "aws_s3_bucket" "b" {\n  lifecycle_rule {\n    enabled = true\n  }\n}\n')
            old = lifecycle.declared_problems(directory, PROTECTED)
        self.assertEqual(1, len(variable))
        self.assertEqual(1, len(old))
        self.assertIn("lifecycle_rule", old[0])

    def test_a_rule_that_is_not_enabled_in_terraform_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "bucket.tf").write_text((BAD_TERRAFORM % "warehouse/").replace('"Enabled"', '"Disabled"'))
            self.assertEqual([], lifecycle.declared_problems(directory, PROTECTED))

    def test_the_repository_declares_no_rule_that_expires_the_warehouse(self) -> None:
        self.assertEqual([], lifecycle.declared_problems(str(INFRA), PROTECTED))


def run_check(document: Any, *, error: Exception | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()

    class FakeAws:
        def __init__(self, *_: str) -> None:
            self.calls: list[tuple[str, ...]] = []

        def call(self, *arguments: str, missing: tuple[str, ...] = ()) -> Any:
            if error is not None:
                raise error
            return document

    with (
        mock.patch.object(lifecycle, "load_configuration", return_value=CONFIGURATION),
        mock.patch.object(lifecycle, "Aws", FakeAws),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        code = lifecycle.main(["check"])
    return code, out.getvalue(), err.getvalue()


class CommandTest(unittest.TestCase):
    def test_a_bucket_whose_rules_expire_the_warehouse_exits_1_and_names_the_rule(self) -> None:
        code, out, err = run_check({"Rules": [rule("cleanup", {"Prefix": ""}, expiration={"Days": 30})]})
        self.assertEqual(1, code)
        self.assertIn("cleanup", err)
        self.assertNotIn("ok", out)

    def test_a_bucket_with_a_clear_rule_or_no_configuration_exits_0(self) -> None:
        clear = run_check({"Rules": [rule("cleanup", {"Prefix": "landing/"}, expiration={"Days": 30})]})
        none = run_check(None)
        self.assertEqual((0, 0), (clear[0], none[0]))
        self.assertIn("no rule", none[1])

    def test_a_failed_read_exits_1_and_is_never_taken_for_no_rules(self) -> None:
        code, out, err = run_check(None, error=AwsError("aws s3api failed: AccessDenied"))
        self.assertEqual(1, code)
        self.assertIn("AccessDenied", err)
        self.assertNotIn("no rule", out)


if __name__ == "__main__":
    unittest.main()
