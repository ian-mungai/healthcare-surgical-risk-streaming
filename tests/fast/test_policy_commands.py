"""The grant tool's plan, apply and export commands against a canned AWS account.

``FakeAws`` answers the IAM calls the tool makes from a dictionary and records every call. The real run against the
owner's account is proven by the owner's plan and apply. These cases cover what such a run cannot readily force: a
user with an extra attachment, an inline policy or a group, and a live account that has diverged from the templates.
"""

from __future__ import annotations

import argparse
import io
import stat
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

from infra.iam.scripts import manage_policies
from infra.iam.scripts.manage_policies import Names, policy_arn
from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, load_documents
from scripts.aws_cli import AWS_TIMEOUT_SECONDS, Aws, AwsError

ACCOUNT = "1" * 12
CONFIGURATION = {
    "AWS_REGION": "us-west-2",
    "AWS_PROFILE": "example_project_dev",
    "AWS_ACCOUNT_ID": ACCOUNT,
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "S3_BUCKET": "example-project",
    "TAILSCALE_AUDIENCE": "example-audience",
}
NAMES = Names("example_project", "dev")
TAGS = {"Project": "example_project", "Environment": "dev"}
SECRET = "example-secret-" + "value"
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
    user_tags: dict[str, str] = field(default_factory=lambda: dict(TAGS))
    calls: list[tuple[str, ...]] = field(default_factory=list)
    keys: int = 0
    settings: list[tuple[tuple[str, ...], str]] = field(default_factory=list)
    profile_works: bool = False

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
            if self.profile == CONFIGURATION["AWS_PROFILE"] and not state.profile_works:
                raise AwsError("aws sts get-caller-identity failed: InvalidClientTokenId")
            return {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/{self.profile}"}
        if verb == "list-access-keys":
            return {"AccessKeyMetadata": [{"AccessKeyId": f"key{number}"} for number in range(state.keys)]}
        if verb == "create-access-key":
            state.keys += 1
            return {"AccessKey": {"AccessKeyId": "EXAMPLEKEYID", "SecretAccessKey": SECRET}}
        if arguments[:2] == ("configure", "set"):
            state.settings.append((arguments, self.profile))
            return {}
        if arguments[:2] == ("configure", "import"):
            state.profile_works = True
            return {}
        if verb == "get-user":
            return {"User": {}} if state.user_exists else None
        if verb == "list-policies":
            return {"Policies": [{"PolicyName": name, "Arn": policy_arn(ACCOUNT, name), "DefaultVersionId": "v1"} for name in state.policies]}
        if verb == "get-policy-version":
            return {"PolicyVersion": {"Document": state.policies[arguments[arguments.index("--policy-arn") + 1].rsplit("/", 1)[1]]}}
        if verb == "list-policy-tags":
            name = arguments[arguments.index("--policy-arn") + 1].rsplit("/", 1)[1]
            return {"Tags": [{"Key": key, "Value": value} for key, value in state.policy_tags.get(name, {}).items()]}
        if verb == "list-user-tags":
            return {"Tags": [{"Key": key, "Value": value} for key, value in state.user_tags.items()]}
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
        code = manage_policies.run(arguments) if command != "key" else manage_policies.guarded(arguments)
    return code, out.getvalue(), err.getvalue()


class KeyTest(unittest.TestCase):
    """``key`` gives the project user its one access key, a backup file and a working profile, and never shows the secret."""

    def run_key(self, account: Account, configuration: dict[str, str] | None = None) -> tuple[int, str, Path]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.folder = folder
        return self.rerun_key(account, configuration)

    def rerun_key(self, account: Account, configuration: dict[str, str] | None = None) -> tuple[int, str, Path]:
        with (
            mock.patch.object(manage_policies, "KEY_FOLDER", self.folder),
            mock.patch.object(manage_policies, "KEY_READY_SECONDS", 0.05),
            mock.patch.object(manage_policies, "KEY_POLL_SECONDS", 0.01),
            mock.patch.dict(CONFIGURATION, configuration or {}),
        ):
            code, out, err = run_command("key", account)
        return code, out + err, self.folder / "example_project_dev_accessKeys.csv"

    def test_a_user_without_a_key_gets_one_with_a_private_backup_and_a_working_profile(self) -> None:
        account = Account.matching_templates()
        code, output, backup = self.run_key(account)
        self.assertEqual(0, code, output)
        self.assertEqual(f"User Name,Access key ID,Secret access key\nexample_project_dev,EXAMPLEKEYID,{SECRET}\n", backup.read_text())
        self.assertEqual(0o600, stat.S_IMODE(backup.stat().st_mode))
        self.assertIn(("configure", "import", "--csv", f"file://{backup}"), account.calls)
        settings = [(call, self_profile) for call, self_profile in account.settings]
        self.assertEqual(
            [(("configure", "set", "region", "us-west-2"), "example_project_dev"), (("configure", "set", "output", "json"), "example_project_dev")], settings
        )
        self.assertNotIn(SECRET, output)
        self.assertNotIn(SECRET, repr(account.calls))

    def test_a_second_run_changes_nothing(self) -> None:
        account = Account.matching_templates()
        self.run_key(account)
        before = len([call for call in account.calls if call[1] == "create-access-key"])
        code, output, _ = self.rerun_key(account)
        self.assertEqual(0, code, output)
        self.assertIn("no change", output)
        self.assertEqual(before, len([call for call in account.calls if call[1] == "create-access-key"]))

    def test_an_existing_key_with_a_profile_that_does_not_work_is_refused_and_no_key_is_created(self) -> None:
        account = Account.matching_templates()
        account.keys = 1
        code, output, backup = self.run_key(account)
        self.assertEqual(1, code)
        self.assertIn("already has an access key and the project profile does not work", output)
        self.assertEqual(1, account.keys)
        self.assertFalse(backup.exists())

    def test_an_existing_backup_file_is_never_overwritten(self) -> None:
        account = Account.matching_templates()
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.folder = folder
        (folder / "example_project_dev_accessKeys.csv").write_text("kept")
        code, output, backup = self.rerun_key(account)
        self.assertEqual(1, code)
        self.assertIn("backup file already exists", output)
        self.assertEqual(("kept", 0), (backup.read_text(), account.keys))

    def test_a_missing_user_is_refused(self) -> None:
        code, output, _ = self.run_key(Account(user_exists=False))
        self.assertEqual(1, code)
        self.assertIn("does not exist: run apply first", output)

    def test_a_key_that_never_starts_to_work_fails_at_its_deadline(self) -> None:
        account = Account.matching_templates()
        with mock.patch.object(FakeAws, "call", autospec=True, side_effect=lambda self, *arguments, **options: stuck(self, *arguments, **options)):
            code, output, _ = self.run_key(account)
        self.assertEqual(1, code)
        self.assertIn("did not start to work within", output)


ORIGINAL_CALL = FakeAws.call


def stuck(self: FakeAws, *arguments: str, **options: Any) -> Any:
    result = ORIGINAL_CALL(self, *arguments, **options)
    self.account.profile_works = False
    return result


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

    def test_a_plan_with_changes_and_a_report_only_difference_still_prints_both_and_fails(self) -> None:
        account = Account.matching_templates(attached=False)
        account.groups.append("admins")
        code, output, _ = run_command("plan", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-group-membership: admins", output)
        self.assertIn("steps planned", output)
        self.assertIn("1 differences remain that this tool never removes: review each as the owner", output)

    def test_a_plan_with_changes_and_no_report_only_difference_succeeds(self) -> None:
        code, output, _ = run_command("plan", Account.matching_templates(attached=False))
        self.assertEqual(0, code, output)
        self.assertIn("steps planned", output)
        self.assertNotIn("never removes", output)

    def test_apply_fails_when_a_difference_that_it_never_removes_remains(self) -> None:
        account = Account.matching_templates(attached=False)
        account.inline.append("grant_everything")
        _, plan_output, _ = run_command("plan", account)
        code_line = plan_output.rsplit("--confirm ", 1)[1].split()[0]
        code, output, _ = run_command("apply", account, confirm=code_line)
        self.assertEqual(1, code, output)
        self.assertIn("done: attach-policy", output)
        self.assertTrue(any(call[:2] == ("iam", "attach-user-policy") for call in account.calls))


class UserTagTest(unittest.TestCase):
    """An existing user is read for its tags as the policies are: missing or wrong tags are planned, extra tags are reported and never removed."""

    def with_user_tags(self, tags: dict[str, str]) -> Account:
        account = Account.matching_templates()
        account.user_tags = tags
        return account

    def test_a_user_without_the_tags_is_a_difference_and_the_plan_tags_it(self) -> None:
        for tags in ({}, {"Project": "example_project"}, {"Environment": "dev"}, TAGS | {"Environment": "prod"}, TAGS | {"Project": "other_project"}):
            with self.subTest(tags=tags):
                code, output, _ = run_command("export", self.with_user_tags(tags))
                self.assertEqual(1, code, output)
                self.assertIn("tag-user: example_project_dev", output)
                self.assertNotIn("create-user", output)

    def test_a_user_with_both_tags_is_no_difference(self) -> None:
        code, output, _ = run_command("export", self.with_user_tags(dict(TAGS)))
        self.assertEqual(0, code, output)
        self.assertNotIn("tag-user", output)

    def test_an_extra_user_tag_is_reported_and_never_removed(self) -> None:
        account = self.with_user_tags(TAGS | {"Owner": "someone"})
        code, output, _ = run_command("export", account)
        self.assertEqual(1, code, output)
        self.assertIn("report-extra-user-tag: example_project_dev Owner", output)
        code, output, _ = run_command("plan", account)
        self.assertEqual(1, code, output)
        self.assertFalse(any(call[1] == "untag-user" for call in account.calls))

    def test_apply_tags_an_existing_untagged_user_with_both_tags_and_removes_none(self) -> None:
        account = self.with_user_tags({"Owner": "someone"})
        _, plan_output, _ = run_command("plan", account)
        code, output, _ = run_command("apply", account, confirm=plan_output.rsplit("--confirm ", 1)[1].split()[0])
        self.assertEqual(1, code, output)
        tagged = [call for call in account.calls if call[:2] == ("iam", "tag-user")]
        self.assertEqual(1, len(tagged))
        self.assertEqual(("--user-name", "example_project_dev", "--tags", "Key=Project,Value=example_project", "Key=Environment,Value=dev"), tagged[0][2:])
        self.assertFalse(any(call[1] == "untag-user" for call in account.calls))

    def test_a_new_user_is_created_with_its_tags_and_gets_no_tag_step(self) -> None:
        account = Account(user_exists=False)
        _, plan_output, _ = run_command("plan", account)
        self.assertNotIn("tag-user", plan_output)
        run_command("apply", account, confirm=plan_output.rsplit("--confirm ", 1)[1].split()[0])
        created = [call for call in account.calls if call[:2] == ("iam", "create-user")]
        self.assertEqual(("--tags", "Key=Project,Value=example_project", "Key=Environment,Value=dev"), created[0][created[0].index("--tags") :])
        self.assertFalse(any(call[1] == "list-user-tags" for call in account.calls))


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
