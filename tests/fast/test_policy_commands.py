"""The grant tool's plan, apply and export commands against a canned AWS account.

``FakeAws`` answers the IAM calls the tool makes from a dictionary and records every call. The real run against the
owner's account is proven by the owner's plan and apply. These cases cover what such a run cannot readily force: a
user with an extra attachment, an inline policy or a group, and a live account that has diverged from the templates.
"""

from __future__ import annotations

import argparse
import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

from infra.iam.scripts import manage_policies
from infra.iam.scripts.manage_policies import Names, policy_arn
from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, load_documents
from scripts.aws_cli import AWS_TIMEOUT_SECONDS, Aws

ACCOUNT = "1" * 12
CONFIGURATION = {
    "AWS_REGION": "us-west-2",
    "AWS_PROFILE": "example_project_dev",
    "AWS_ACCOUNT_ID": ACCOUNT,
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "BUCKET_PREFIX": "example-project",
    "TAILSCALE_AUDIENCE": "example-audience",
}
NAMES = Names("example_project", "dev")
TAGS = {"Project": "example_project", "Environment": "dev"}
FOREIGN = "arn:aws:iam::aws:policy/AdministratorAccess"


def templates() -> dict[str, dict[str, Any]]:
    boundary = {NAMES.boundary(kind): document for kind, document in load_documents(CONFIGURATION, BOUNDARY_DIRECTORY, suffix="_boundary").items()}
    return {NAMES.policy(service): document for service, document in load_documents(CONFIGURATION).items()} | boundary


@dataclass
class Account:
    """What the canned account holds and every call made to it."""

    user_exists: bool = True
    policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    attached: list[str] = field(default_factory=list)
    inline: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    policy_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    @classmethod
    def matching_templates(cls, *, attached: bool = True, tagged: bool = True) -> Account:
        documents = templates()
        expected = [policy_arn(ACCOUNT, name) for name in documents if name != NAMES.boundary("role")]
        return cls(policies=documents, attached=expected if attached else [], policy_tags={name: dict(TAGS) for name in documents} if tagged else {})


@dataclass(frozen=True)
class FakeAws(Aws):
    account: Account = field(default_factory=Account)

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = AWS_TIMEOUT_SECONDS) -> Any:
        self.account.calls.append(arguments)
        state = self.account
        verb = arguments[1]
        if verb == "get-caller-identity":
            return {"Account": ACCOUNT}
        if verb == "get-user":
            return {"User": {}} if state.user_exists else None
        if verb == "list-policies":
            return {"Policies": [{"PolicyName": name, "Arn": policy_arn(ACCOUNT, name), "DefaultVersionId": "v1"} for name in state.policies]}
        if verb == "get-policy-version":
            return {"PolicyVersion": {"Document": state.policies[arguments[arguments.index("--policy-arn") + 1].rsplit("/", 1)[1]]}}
        if verb == "list-policy-tags":
            name = arguments[arguments.index("--policy-arn") + 1].rsplit("/", 1)[1]
            return {"Tags": [{"Key": key, "Value": value} for key, value in state.policy_tags.get(name, {}).items()]}
        if verb == "list-attached-user-policies":
            return {"AttachedPolicies": [{"PolicyName": arn.rsplit("/", 1)[1], "PolicyArn": arn} for arn in state.attached]}
        if verb == "list-user-policies":
            return {"PolicyNames": state.inline}
        if verb == "list-groups-for-user":
            return {"Groups": [{"GroupName": name} for name in state.groups]}
        return {}


def run_command(command: str, account: Account, confirm: str = "") -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    arguments = argparse.Namespace(command=command, admin_profile="admin_profile", confirm=confirm)
    with (
        mock.patch.object(manage_policies, "load_configuration", return_value=dict(CONFIGURATION)),
        mock.patch.object(manage_policies, "Aws", lambda profile, region: FakeAws(profile, region, account)),
        redirect_stdout(out),
        redirect_stderr(err),
    ):
        code = manage_policies.run(arguments)
    return code, out.getvalue(), err.getvalue()


class ExportTest(unittest.TestCase):
    def test_an_account_equal_to_the_templates_passes(self) -> None:
        code, output, _ = run_command("export", Account.matching_templates())
        self.assertEqual(0, code, output)

    def test_matching_documents_with_no_user_attachments_are_a_difference(self) -> None:
        code, output, _ = run_command("export", Account.matching_templates(attached=False))
        self.assertEqual(1, code, output)
        self.assertIn("attach-policy", output)

    def test_an_extra_attached_policy_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.attached.append(FOREIGN)
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-unexpected-attachment: policy/AdministratorAccess", output)
        self.assertNotIn(ACCOUNT, output)

    def test_the_boundary_attached_to_the_user_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.attached.append(policy_arn(ACCOUNT, NAMES.boundary("role")))
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-attached-boundary", output)

    def test_an_inline_user_policy_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.inline.append("grant_everything")
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-inline-policy: grant_everything", output)

    def test_a_group_membership_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.groups.append("admins")
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-group-membership: admins", output)

    def test_a_changed_document_is_still_a_difference(self) -> None:
        account = Account.matching_templates()
        account.policies[NAMES.policy("ec2")] = {"Version": "2012-10-17", "Statement": []}
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("update-policy", output)

    def test_a_missing_user_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.user_exists = False
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("create-user", output)

    def test_an_unlisted_policy_that_no_one_holds_is_reported_and_is_no_difference(self) -> None:
        account = Account.matching_templates()
        account.policies["example_project_old_policy"] = {"Version": "2012-10-17", "Statement": []}
        code, output, _ = run_command("export", account)
        self.assertEqual(0, code, output)
        self.assertIn("report-unlisted: example_project_old_policy", output)


class PlanTest(unittest.TestCase):
    def test_only_reports_remain_so_the_plan_says_the_grants_differ_and_fails(self) -> None:
        account = Account.matching_templates()
        account.attached.append(FOREIGN)
        code, output, _ = run_command("plan", account)
        self.assertEqual(1, code, output)
        self.assertNotIn("no change: the user, its policies and its attachments equal the templates", output)
        self.assertIn("never removes", output)

    def test_a_matching_account_plans_nothing(self) -> None:
        code, output, _ = run_command("plan", Account.matching_templates())
        self.assertEqual(0, code, output)
        self.assertIn("no change: the user, its policies and its attachments equal the templates", output)

    def test_a_plan_with_changes_still_prints_the_reports_and_the_code(self) -> None:
        account = Account.matching_templates(attached=False)
        account.groups.append("admins")
        code, output, _ = run_command("plan", account)
        self.assertEqual(0, code, output)
        self.assertIn("report-group-membership: admins", output)
        self.assertIn("steps planned", output)

    def test_apply_fails_when_a_difference_that_it_never_removes_remains(self) -> None:
        account = Account.matching_templates(attached=False)
        account.inline.append("grant_everything")
        _, plan_output, _ = run_command("plan", account)
        code_line = plan_output.rsplit("--confirm ", 1)[1].split()[0]
        code, output, _ = run_command("apply", account, confirm=code_line)
        self.assertEqual(1, code, output)
        self.assertIn("done: attach-policy", output)
        self.assertTrue(any(call[:2] == ("iam", "attach-user-policy") for call in account.calls))


class TagTest(unittest.TestCase):
    def test_policies_and_the_boundary_without_tags_are_a_difference(self) -> None:
        code, output, _ = run_command("export", Account.matching_templates(tagged=False))
        self.assertEqual(1, code, output)
        for name in templates():
            self.assertIn(f"tag-policy: {name}", output)

    def test_a_wrong_tag_value_is_a_difference(self) -> None:
        account = Account.matching_templates()
        account.policy_tags[NAMES.policy("ec2")] = TAGS | {"Environment": "prod"}
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn(f"tag-policy: {NAMES.policy('ec2')}", output)

    def test_an_extra_tag_is_reported_and_never_removed(self) -> None:
        account = Account.matching_templates()
        account.policy_tags[NAMES.boundary("role")] = TAGS | {"Owner": "someone"}
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn(f"report-extra-policy-tag: {NAMES.boundary('role')} Owner", output)

    def test_tagged_policies_and_boundary_are_no_difference(self) -> None:
        code, output, _ = run_command("export", Account.matching_templates())
        self.assertEqual(0, code, output)
        self.assertNotIn("tag-policy", output)

    def test_apply_creates_each_new_policy_and_the_boundary_with_both_tags(self) -> None:
        account = Account(user_exists=False)
        _, plan_output, _ = run_command("plan", account)
        code, output, _ = run_command("apply", account, confirm=plan_output.rsplit("--confirm ", 1)[1].split()[0])
        self.assertEqual(0, code, output)
        creates = [call for call in account.calls if call[:2] == ("iam", "create-policy")]
        self.assertEqual(sorted(templates()), sorted(call[call.index("--policy-name") + 1] for call in creates))
        for call in creates:
            self.assertIn("--tags", call)
            self.assertEqual(("--tags", "Key=Project,Value=example_project", "Key=Environment,Value=dev"), call[call.index("--tags") :])

    def test_apply_tags_an_existing_untagged_policy(self) -> None:
        account = Account.matching_templates(tagged=False)
        _, plan_output, _ = run_command("plan", account)
        code, output, _ = run_command("apply", account, confirm=plan_output.rsplit("--confirm ", 1)[1].split()[0])
        self.assertEqual(0, code, output)
        tagged = [call for call in account.calls if call[:2] == ("iam", "tag-policy")]
        self.assertEqual(sorted(policy_arn(ACCOUNT, name) for name in templates()), sorted(call[call.index("--policy-arn") + 1] for call in tagged))
        for call in tagged:
            self.assertIn("--tags", call)
            self.assertEqual(("--tags", "Key=Project,Value=example_project", "Key=Environment,Value=dev"), call[call.index("--tags") :])


if __name__ == "__main__":
    unittest.main()
