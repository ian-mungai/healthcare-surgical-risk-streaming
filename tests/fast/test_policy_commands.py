"""The grant tool's plan, apply and export commands against a canned AWS account.

``FakeAws`` answers the IAM calls the tool makes from a dictionary and records every call. The real run against the
owner's account is proven by the owner's plan and apply. These cases cover what such a run cannot readily force: a
user with an extra attachment, an inline policy or a group, and a live account that has diverged from the templates.
"""

from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import os
import re
import stat
import tempfile
import traceback
import unittest
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

from infra.iam.scripts import manage_policies
from infra.iam.scripts.manage_policies import Names, policy_arn
from infra.iam.scripts.render_policy import BOUNDARY_DIRECTORY, load_documents
from scripts import aws_cli
from scripts.aws_cli import AWS_TIMEOUT_SECONDS, Aws, AwsError, AwsTimeout
from scripts.process import CompletedProcess, TimeoutExpired

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
USER_ARN = f"arn:aws:iam::{ACCOUNT}:user/example_project_dev"
OTHER_ACCOUNT = "2" * 12
LIMIT_EXCEEDED = AwsError("aws iam create-access-key failed: LimitExceeded; its full error text is in runs/raw_errors/example.txt")


class FakeClock:
    """A clock that only moves when the code sleeps or a fake AWS call takes time."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[tuple[float, float]] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append((self.now, seconds))
        self.now += seconds


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
    user_arn: str = USER_ARN
    project_identity: dict[str, str] | None = None
    create_error: BaseException | None = None
    create_refusal: CompletedProcess[str] | None = None
    attempts: list[tuple[str, int | None]] = field(default_factory=list)
    clock: FakeClock | None = None
    sts_latency: float = 0.0
    sts_honors_timeout: bool = True
    sts_calls: list[tuple[float, float]] = field(default_factory=list)

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
        state.attempts.append((verb, self.max_attempts))
        if verb == "get-caller-identity":
            if self.profile == CONFIGURATION["AWS_PROFILE"] and state.clock is not None:
                state.sts_calls.append((state.clock.now, timeout))
                if state.sts_honors_timeout and state.sts_latency > timeout:
                    state.clock.now += timeout
                    raise TimeoutExpired("aws", timeout)
                state.clock.now += state.sts_latency
            if self.profile == CONFIGURATION["AWS_PROFILE"] and not state.profile_works:
                raise AwsError("aws sts get-caller-identity failed: InvalidClientTokenId")
            if self.profile == CONFIGURATION["AWS_PROFILE"] and state.project_identity is not None:
                return state.project_identity
            return {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/{self.profile}"}
        if verb == "list-access-keys":
            return {"AccessKeyMetadata": [{"AccessKeyId": f"key{number}"} for number in range(state.keys)]}
        if verb == "create-access-key":
            if state.create_refusal is not None:
                with mock.patch.object(aws_cli, "run_command", return_value=state.create_refusal):
                    return Aws.call(self, *arguments, missing=missing, timeout=timeout)
            if state.create_error is not None:
                raise state.create_error
            state.keys += 1
            return {"AccessKey": {"AccessKeyId": "EXAMPLEKEYID", "SecretAccessKey": SECRET}}
        if arguments[:2] == ("configure", "set"):
            state.settings.append((arguments, self.profile))
            return {}
        if arguments[:2] == ("configure", "import"):
            state.profile_works = True
            return {}
        if verb == "get-user":
            return {"User": {"Arn": state.user_arn}} if state.user_exists else None
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

    def setUp(self) -> None:
        self.aws_files = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.write_admin_config("region = us-west-2\n")
        self.enterContext(
            mock.patch.dict(os.environ, {"AWS_CONFIG_FILE": str(self.aws_files / "config"), "AWS_SHARED_CREDENTIALS_FILE": str(self.aws_files / "credentials")})
        )

    def write_admin_config(self, settings: str, profile: str = "admin_profile") -> None:
        (self.aws_files / "config").write_text(f"[profile {profile}]\n{settings}")
        (self.aws_files / "credentials").write_text("")

    def run_key(self, account: Account, configuration: dict[str, str] | None = None, *, ready: float = 0.05, poll: float = 0.01) -> tuple[int, str, Path]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.folder = folder
        return self.rerun_key(account, configuration, ready=ready, poll=poll)

    def rerun_key(self, account: Account, configuration: dict[str, str] | None = None, *, ready: float = 0.05, poll: float = 0.01) -> tuple[int, str, Path]:
        with (
            mock.patch.object(manage_policies, "KEY_FOLDER", self.folder),
            mock.patch.object(manage_policies, "KEY_READY_SECONDS", ready),
            mock.patch.object(manage_policies, "KEY_POLL_SECONDS", poll),
            mock.patch.dict(CONFIGURATION, configuration or {}),
        ):
            code, out, err = run_command("key", account)
        return code, out + err, self.folder / "example_project_dev_accessKeys.csv"

    def creations(self, account: Account) -> int:
        return len([call for call in account.calls if call[1] == "create-access-key"])

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

    def test_key_creation_is_refused_while_cli_history_is_enabled_for_the_administrator_profile(self) -> None:
        self.write_admin_config("region = us-west-2\ncli_history = enabled\n")
        account = Account.matching_templates()
        code, output, backup = self.run_key(account)
        self.assertEqual(1, code)
        self.assertIn("cli_history", output)
        self.assertIn("aws configure set cli_history disabled --profile <ADMIN_PROFILE>", output)
        self.assertNotIn("admin_profile", output, "the advice is constant and holds no value of --admin-profile")
        self.assertEqual(0, self.creations(account))
        self.assertFalse(backup.exists())
        self.assertNotIn(SECRET, output)

    def test_a_disabled_history_setting_allows_key_creation(self) -> None:
        self.write_admin_config("cli_history = disabled\n")
        account = Account.matching_templates()
        code, output, backup = self.run_key(account)
        self.assertEqual(0, code, output)
        self.assertTrue(backup.exists())

    def test_the_history_setting_of_the_project_profile_does_not_block_key_creation(self) -> None:
        (self.aws_files / "config").write_text("[profile admin_profile]\nregion = us-west-2\n[profile example_project_dev]\ncli_history = enabled\n")
        account = Account.matching_templates()
        code, output, _ = self.run_key(account)
        self.assertEqual(0, code, output)
        self.assertEqual(1, self.creations(account))

    def test_a_history_setting_that_cannot_be_read_refuses_key_creation(self) -> None:
        self.write_admin_config("cli_history = enabled\n", profile="another_profile")
        account = Account.matching_templates()
        code, output, backup = self.run_key(account)
        self.assertEqual(1, code)
        self.assertIn("cli_history", output)
        self.assertIn("--profile <ADMIN_PROFILE>", output)
        self.assertNotIn("admin_profile", output, "no message of the key command holds a value of --admin-profile")
        self.assertEqual(0, self.creations(account))
        self.assertFalse(backup.exists())

    def test_a_profile_in_another_account_or_with_another_arn_is_not_the_project_user(self) -> None:
        wrong = (
            {"Account": OTHER_ACCOUNT, "Arn": f"arn:aws:iam::{OTHER_ACCOUNT}:user/example_project_dev"},
            {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/path/example_project_dev"},
            {"Account": OTHER_ACCOUNT, "Arn": USER_ARN},
            {"Account": ACCOUNT, "Arn": f"arn:aws:iam::{OTHER_ACCOUNT}:user/example_project_dev"},
            {"Account": ACCOUNT, "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/example_project_dev"},
        )
        for identity in wrong:
            with self.subTest(identity=identity):
                existing = Account.matching_templates()
                existing.keys = 1
                existing.profile_works = True
                existing.project_identity = identity
                code, output, _ = self.run_key(existing)
                self.assertEqual(1, code)
                self.assertIn("project profile does not work", output)
                self.assertNotIn("no change", output)
                fresh = Account.matching_templates()
                fresh.project_identity = identity
                code, output, _ = self.run_key(fresh)
                self.assertEqual(1, code)
                self.assertIn("did not start to work", output)
                self.assertNotIn("the profile example_project_dev works", output)
                for value in (OTHER_ACCOUNT, *identity.values()):
                    self.assertNotIn(value, output)

    def test_a_definite_refusal_of_the_create_call_removes_the_reserved_backup_and_a_retry_succeeds(self) -> None:
        account = Account.matching_templates()
        account.create_error = LIMIT_EXCEEDED
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.folder = folder
        (folder / "other_accessKeys.csv").write_text("kept")
        code, output, backup = self.rerun_key(account)
        self.assertEqual(1, code)
        self.assertIn("AwsError", output)
        self.assertFalse(backup.exists())
        self.assertEqual("kept", (folder / "other_accessKeys.csv").read_text())
        account.create_error = None
        code, output, _ = self.rerun_key(account)
        self.assertEqual(0, code, output)
        self.assertEqual(1, account.keys)

    def test_only_the_create_call_is_limited_to_one_attempt_so_a_lost_reply_cannot_create_a_second_key(self) -> None:
        account = Account.matching_templates()
        code, output, _ = self.run_key(account)
        self.assertEqual(0, code, output)
        self.assertEqual([("create-access-key", 1)], [pair for pair in account.attempts if pair[0] == "create-access-key"])
        self.assertEqual({None}, {attempts for verb, attempts in account.attempts if verb != "create-access-key"})

    def test_a_definite_refusal_still_removes_the_reservation_when_the_private_error_file_cannot_be_written(self) -> None:
        account = Account.matching_templates()
        account.create_refusal = CompletedProcess(
            ["aws"], 254, "", "An error occurred (LimitExceeded) when calling the CreateAccessKey operation: example text"
        )
        with mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError("disk full")):
            code, output, backup = self.run_key(account)
        self.assertEqual(1, code)
        self.assertIn("AwsError", output)
        self.assertFalse(backup.exists())
        account.create_refusal = None
        code, output, _ = self.rerun_key(account)
        self.assertEqual(0, code, output)
        self.assertEqual(1, account.keys)

    def test_an_uncertain_result_of_the_create_call_keeps_the_backup_and_says_to_check_the_keys(self) -> None:
        uncertain = (
            AwsTimeout("aws iam create-access-key", 120),
            AwsError("aws iam create-access-key failed: Could not connect to the endpoint URL"),
            AwsError("aws iam create-access-key failed: ServiceFailure; its full error text is in runs/raw_errors/example.txt"),
            AwsError("aws iam create-access-key failed: Throttling; its full error text is in runs/raw_errors/example.txt"),
            AwsError("aws iam create-access-key failed: LimitExceededElsewhere; its full error text is in runs/raw_errors/example.txt"),
            AwsError("aws iam create-user failed: LimitExceeded; its full error text is in runs/raw_errors/example.txt"),
            AwsError("aws iam create-access-key failed: "),
        )
        for error in uncertain:
            with self.subTest(error=repr(error)):
                account = Account.matching_templates()
                account.create_error = error
                code, output, backup = self.run_key(account)
                self.assertEqual(1, code)
                self.assertTrue(backup.exists())
                self.assertEqual("", backup.read_text())
                self.assertIn("check the access keys of example_project_dev as the owner before a retry", output)

    def test_a_create_reply_that_cannot_be_read_keeps_the_backup_gives_the_advice_and_shows_no_secret(self) -> None:
        replies = (f"not json {SECRET}", f'{{"Other": "{SECRET}"}}', f'{{"AccessKey": "{SECRET}"}}', f'{{"AccessKey": {{"AccessKeyId": "{SECRET}"}}}}')
        for reply in replies:
            with self.subTest(reply=reply.split(SECRET)[0]):
                account = Account.matching_templates()
                account.create_refusal = CompletedProcess(["aws"], 0, reply, "")
                code, output, backup = self.run_key(account)
                self.assertEqual(1, code)
                self.assertIn("check the access keys of example_project_dev as the owner before a retry", output)
                self.assertTrue(backup.exists())
                self.assertNotIn(SECRET, output)
                error = self.raised_by_write_new_key(account)
                self.assertNotIn(SECRET, "".join(traceback.format_exception(error)))
                self.assertIsNone(error.__context__)

    def test_a_failed_write_or_flush_of_the_backup_after_the_key_exists_keeps_the_reservation_gives_the_advice_and_shows_no_secret(self) -> None:
        class FailingHandle:
            fails_in = "write"

            def __init__(self, descriptor: int, mode: str) -> None:
                os.close(descriptor)

            def __enter__(self) -> FailingHandle:
                return self

            def __exit__(self, *details: object) -> None:
                if self.fails_in in ("close", "flush and close"):
                    raise OSError("the disk is full")

            def write(self, text: str) -> int:
                if self.fails_in == "write":
                    raise OSError(f"the disk is full {text}")
                return len(text)

            def flush(self) -> None:
                if self.fails_in != "close":
                    raise OSError("the disk is full")

        for fails_in in ("write", "flush", "close", "flush and close"):
            with self.subTest(fails_in=fails_in), mock.patch.object(FailingHandle, "fails_in", fails_in), mock.patch.object(os, "fdopen", FailingHandle):
                account = Account.matching_templates()
                code, output, backup = self.run_key(account)
                self.assertEqual(1, code)
                self.assertEqual(1, account.keys)
                self.assertTrue(backup.exists())
                self.assertIn("check the access keys of example_project_dev as the owner before a retry", output)
                self.assertNotIn(SECRET, output)
                error = self.raised_by_write_new_key(Account.matching_templates())
                self.assertNotIn(SECRET, "".join(traceback.format_exception(error)))
                self.assertIsNone(error.__context__)

    def test_a_backup_file_that_cannot_be_opened_creates_no_key_says_so_and_leaves_no_reservation(self) -> None:
        account = Account.matching_templates()
        with mock.patch.object(os, "fdopen", side_effect=OSError("too many open files")):
            code, output, backup = self.run_key(account)
        self.assertEqual(1, code)
        self.assertEqual(0, account.keys)
        self.assertEqual(0, self.creations(account))
        self.assertFalse(backup.exists())
        self.assertIn("could not be opened, so no key was created", output)
        self.assertNotIn("check the access keys", output)
        code, output, _ = self.rerun_key(account)
        self.assertEqual((0, 1), (code, account.keys), output)

    def raised_by_write_new_key(self, account: Account) -> BaseException:
        backup = self.folder / "direct_accessKeys.csv"
        with mock.patch.object(aws_cli, "run_command", return_value=account.create_refusal) if account.create_refusal else contextlib.nullcontext():
            try:
                manage_policies.write_new_key(FakeAws("admin_profile", "us-west-2", account), "example_project_dev", backup)
            except ValueError as error:
                return error
        raise AssertionError("write_new_key raised nothing")

    def test_every_ready_wait_call_gets_the_time_that_is_left_and_no_sleep_passes_the_deadline(self) -> None:
        clock = FakeClock()
        account = Account.matching_templates()
        account.clock = clock
        account.sts_latency = 100.0
        with mock.patch.object(manage_policies, "time", clock), mock.patch.object(FakeAws, "call", autospec=True, side_effect=stuck):
            code, output, _ = self.run_key(account, ready=120.0, poll=5.0)
        self.assertEqual(1, code)
        self.assertIn("did not start to work", output)
        self.assertEqual(2, len(account.sts_calls))
        for started, timeout in account.sts_calls:
            self.assertGreater(timeout, 0)
            self.assertLessEqual(started + timeout, 120.0 + 1e-9)
        self.assertLessEqual(clock.now, 120.0 + 1e-9)

    def test_no_sleep_in_the_ready_wait_passes_the_deadline(self) -> None:
        clock = FakeClock()
        account = Account.matching_templates()
        account.clock = clock
        account.profile_works = False
        with mock.patch.object(manage_policies, "time", clock), mock.patch.object(FakeAws, "call", autospec=True, side_effect=stuck):
            code, output, _ = self.run_key(account, ready=12.0, poll=5.0)
        self.assertEqual(1, code)
        self.assertIn("did not start to work", output)
        self.assertEqual([5.0, 5.0, 2.0], [seconds for _, seconds in clock.sleeps])
        self.assertEqual(12.0, clock.now)

    def test_an_answer_that_arrives_after_the_deadline_is_rejected(self) -> None:
        clock = FakeClock()
        account = Account.matching_templates()
        account.clock = clock
        account.sts_latency = 130.0
        account.sts_honors_timeout = False
        with mock.patch.object(manage_policies, "time", clock):
            code, output, _ = self.run_key(account, ready=120.0, poll=5.0)
        self.assertEqual(1, code)
        self.assertIn("did not start to work", output)
        self.assertNotIn("the profile example_project_dev works", output)

    def test_an_answer_inside_the_deadline_is_accepted(self) -> None:
        clock = FakeClock()
        account = Account.matching_templates()
        account.clock = clock
        account.sts_latency = 100.0
        with mock.patch.object(manage_policies, "time", clock):
            code, output, _ = self.run_key(account, ready=120.0, poll=5.0)
        self.assertEqual(0, code, output)
        self.assertIn("the profile example_project_dev works", output)


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


REPORT_ONLY: dict[str, Callable[[Account], object]] = {
    "report-attached-boundary": lambda account: account.attached.append(policy_arn(ACCOUNT, NAMES.boundary("role"))),
    "report-unexpected-attachment": lambda account: account.attached.append(FOREIGN),
    "report-inline-policy": lambda account: account.inline.append("grant_everything"),
    "report-group-membership": lambda account: account.groups.append("admins"),
    "report-extra-policy-tag": lambda account: account.policy_tags[NAMES.policy("ec2")].update({"Owner": "someone"}),
    "report-extra-user-tag": lambda account: account.user_tags.update({"Owner": "someone"}),
    "report-unlisted": lambda account: account.policies.update({"example_project_old_policy": {"Version": "2012-10-17", "Statement": []}}),
}


class ReportOnlyStatusTest(unittest.TestCase):
    """``plan`` and ``apply`` exit 1 while any report-only step remains, whichever kind it is, with changes pending or not."""

    def test_the_tested_kinds_are_every_report_step_kind_the_tool_can_plan(self) -> None:
        self.assertEqual(set(re.findall(r'"(report-[a-z-]+)"', inspect.getsource(manage_policies))), set(REPORT_ONLY))

    def test_each_report_only_step_makes_plan_exit_1_when_nothing_else_changes(self) -> None:
        for kind, plant in REPORT_ONLY.items():
            with self.subTest(kind=kind):
                account = Account.matching_templates()
                plant(account)
                code, output, _ = run_command("plan", account)
                self.assertIn(kind, output)
                self.assertEqual(1, code, output)
                self.assertNotIn("equal the templates", output)

    def test_each_report_only_step_makes_apply_exit_1_when_nothing_else_changes(self) -> None:
        for kind, plant in REPORT_ONLY.items():
            with self.subTest(kind=kind):
                account = Account.matching_templates()
                plant(account)
                code, output, _ = run_command("apply", account)
                self.assertIn(kind, output)
                self.assertEqual(1, code, output)

    def test_each_report_only_step_makes_plan_and_apply_exit_1_after_the_changes_are_planned_and_applied(self) -> None:
        for kind, plant in REPORT_ONLY.items():
            with self.subTest(kind=kind):
                account = Account.matching_templates(attached=False)
                plant(account)
                code, plan_output, _ = run_command("plan", account)
                self.assertEqual(1, code, plan_output)
                self.assertIn("steps planned", plan_output)
                code, output, _ = run_command("apply", account, confirm=plan_output.rsplit("--confirm ", 1)[1].split()[0])
                self.assertEqual(1, code, output)
                self.assertIn("done: attach-policy", output)

    def test_an_account_with_no_report_only_step_exits_0(self) -> None:
        for command in ("plan", "apply"):
            with self.subTest(command=command):
                code, output, _ = run_command(command, Account.matching_templates())
                self.assertEqual(0, code, output)


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
