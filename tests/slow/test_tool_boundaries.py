"""What each tool boundary shows when a tool fails, is slow or cannot keep its private output.

A real timeout or write failure with private names in the arguments needs a slow tool or a full disk, so each case drives the real function
with a stand-in for the launcher that raises what the launcher raises. Failure modes covered here:

- A timeout shows the command's arguments, which hold bucket and role names, in the message or in the printed exception chain.
- A timeout hides partial output, or keeps it where the evidence export can read it.
- A caller that catches ``TimeoutExpired`` stops catching a timeout.
- The call that must not repeat lets the AWS command line retry, and other calls get a changed environment.
- A private error file that cannot be written hides the AWS error code.
- A Terraform failure gives no exit status, or a failed write of its private file hides the status.
"""

from __future__ import annotations

import stat
import tempfile
import traceback
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from infra.iam.scripts import manage_policies
from scripts import aws_cli
from scripts.aws_cli import Aws, AwsError
from scripts.infra import host, teardown, terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import WrapperError
from scripts.process import CompletedProcess, TimeoutExpired
from scripts.project_config import ConfigurationError

BUCKET = "example-private-bucket"
ROLE = "private_role_name"
PRIVATE_ARGUMENTS = ["--bucket", BUCKET, "--role-name", ROLE]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-project", "default", ["a", "b"], "verified", bucket_creation="created")


def raised(call: Callable[[], Any]) -> Exception:
    """The exception the call raises, or a failure when it raises none."""
    try:
        call()
    except Exception as error:
        return error
    raise AssertionError("the call raised nothing")


def printed(error: BaseException) -> str:
    """Everything a terminal shows for the exception: its text and its whole chain."""
    return "".join(traceback.format_exception(error))


class Boundary(unittest.TestCase):
    def assert_private_values_absent(self, error: BaseException) -> None:
        for value in (BUCKET, ROLE):
            self.assertNotIn(value, str(error))
            self.assertNotIn(value, printed(error))
            self.assertNotIn(value, repr(error))
        self.assertIsNone(error.__cause__)
        self.assertTrue(error.__suppress_context__ or error.__context__ is None)


class AwsCallTest(Boundary):
    def setUp(self) -> None:
        self.scratch = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(aws_cli, "RAW_ERRORS", self.scratch / "runs" / "raw_errors"))
        self.aws = Aws("profile", "us-west-2")

    def test_a_call_with_an_attempt_limit_sets_the_tool_retry_setting_and_keeps_the_rest_of_the_environment(self) -> None:
        with (
            mock.patch.object(aws_cli, "run_command", return_value=CompletedProcess(["aws"], 0, "{}", "")) as launched,
            mock.patch.dict("os.environ", {"HOME": "/x"}),
        ):
            self.aws.single_attempt().call("iam", "create-access-key")
        environment = launched.call_args.kwargs["env"]
        self.assertEqual("1", environment["AWS_MAX_ATTEMPTS"])
        self.assertEqual("/x", environment["HOME"])

    def test_a_call_with_no_attempt_limit_leaves_the_environment_to_the_launcher(self) -> None:
        with mock.patch.object(aws_cli, "run_command", return_value=CompletedProcess(["aws"], 0, "{}", "")) as launched:
            self.aws.call("iam", "get-user")
        self.assertIsNone(launched.call_args.kwargs["env"])

    def test_a_failed_write_of_the_private_error_file_still_gives_the_error_code_and_says_the_text_was_not_kept(self) -> None:
        failed = CompletedProcess(["aws"], 254, "", f"An error occurred (LimitExceeded) when calling the CreateAccessKey operation: {BUCKET}")
        with (
            mock.patch.object(aws_cli, "run_command", return_value=failed),
            mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError(f"no space left for {ROLE}")),
        ):
            error = raised(lambda: self.aws.call("iam", "create-access-key"))
        self.assertIsInstance(error, AwsError)
        self.assertEqual("aws iam create-access-key failed: LimitExceeded; its full error text could not be kept", str(error))
        self.assert_private_values_absent(error)

    def test_a_failed_write_with_no_error_code_gives_the_exit_status(self) -> None:
        failed = CompletedProcess(["aws"], 130, "", f"no code in this text {BUCKET}")
        with mock.patch.object(aws_cli, "run_command", return_value=failed), mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError("full")):
            error = raised(lambda: self.aws.call("s3api", "head-bucket"))
        self.assertEqual("aws s3api head-bucket failed: exit status 130; its full error text could not be kept", str(error))

    def timed_out(self, **partial: bytes | str) -> Exception:
        timeout = TimeoutExpired(["/usr/bin/aws", "s3api", "delete-object", *PRIVATE_ARGUMENTS, "--profile", "profile"], 7.5, **partial)
        with mock.patch.object(aws_cli, "run_command", side_effect=timeout):
            return raised(lambda: self.aws.call("s3api", "delete-object", *PRIVATE_ARGUMENTS, timeout=7.5))

    def test_a_timeout_names_the_tool_its_first_two_words_and_the_deadline_only(self) -> None:
        error = self.timed_out()
        self.assertEqual("aws s3api delete-object timed out after 7.5 seconds", str(error))
        self.assert_private_values_absent(error)
        self.assertNotIn(BUCKET, str(error.__dict__))

    def test_a_timeout_is_still_a_timeout_expired_and_an_aws_error(self) -> None:
        error = self.timed_out()
        self.assertIsInstance(error, TimeoutExpired)
        self.assertIsInstance(error, AwsError)

    def test_partial_error_output_of_a_timeout_goes_to_the_owner_only_file_and_not_to_the_message(self) -> None:
        error = self.timed_out(stderr=f"partial text {BUCKET} {ROLE}".encode())
        self.assert_private_values_absent(error)
        kept = list((self.scratch / "runs" / "raw_errors").iterdir())
        self.assertEqual(1, len(kept))
        self.assertEqual(f"partial text {BUCKET} {ROLE}", kept[0].read_text())
        self.assertEqual(0o600, stat.S_IMODE(kept[0].stat().st_mode))
        self.assertIn(f"partial error text is in runs/raw_errors/{kept[0].name}", str(error))

    def test_a_timeout_with_no_partial_output_keeps_no_file(self) -> None:
        self.timed_out()
        self.assertFalse((self.scratch / "runs" / "raw_errors").exists())

    def test_partial_output_that_cannot_be_kept_is_said_so_and_the_timeout_still_arrives(self) -> None:
        with mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError(ROLE)):
            error = self.timed_out(stderr=b"partial")
        self.assertEqual("aws s3api delete-object timed out after 7.5 seconds; its partial error text could not be kept", str(error))
        self.assert_private_values_absent(error)

    def test_the_standard_output_of_a_timeout_is_never_kept_because_a_key_command_prints_its_secret_there(self) -> None:
        self.timed_out(output=b"secret-in-standard-output")
        self.assertFalse((self.scratch / "runs" / "raw_errors").exists())


class CallersOfATimeoutTest(unittest.TestCase):
    """The callers that catch ``TimeoutExpired`` today still catch an AWS call's timeout."""

    def aws(self) -> Aws:
        aws = Aws("profile", "us-west-2")
        timeout = aws_cli.AwsTimeout("aws iam get-role", 5)
        object.__setattr__(aws, "call", mock.Mock(side_effect=timeout))
        return aws

    def test_a_removal_check_that_times_out_reports_that_the_service_did_not_answer(self) -> None:
        entry = {"type": "aws_iam_role", "service_id": "a_role", "address": "aws_iam_role.a"}
        with mock.patch.object(teardown, "GONE_DEADLINE_SECONDS", 0.05), mock.patch.object(teardown, "GONE_POLL_SECONDS", 0.01):
            found = teardown.checked_removals(self.aws(), [entry])
        self.assertEqual("still exists: its service did not answer in time", found[0]["reported"])

    def test_a_wait_counts_a_timed_out_aws_call_as_not_ready(self) -> None:
        calls = []

        def probe(budget: float) -> str | None:
            calls.append(budget)
            if len(calls) == 1:
                return host.instance_state(self.aws(), "i-1", budget)
            return "ready"

        self.assertEqual("ready", host.wait_for("a thing", probe, 60, sleep=lambda seconds: None))

    def test_the_profile_check_of_the_key_command_counts_a_timed_out_call_as_not_working(self) -> None:
        self.assertFalse(manage_policies.profile_is_the_user(self.aws(), "arn:aws:iam::" + "1" * 12 + ":user/u"))


class TerraformCallTest(Boundary):
    def run_terraform(self, outcome: Any, *, saved: Any = None) -> Exception:
        patches = (
            mock.patch.object(
                terraform_wrapper, "run_command", **({"side_effect": outcome} if isinstance(outcome, BaseException) else {"return_value": outcome})
            ),
            mock.patch.object(terraform_wrapper, "save_raw", **({"side_effect": saved} if saved else {})),
            mock.patch.object(terraform_wrapper, "configuration_folder", return_value=Path("configuration")),
            mock.patch.object(terraform_wrapper, "pinned_terraform", return_value=Path("/pinned/terraform")),
        )
        with patches[0], patches[1] as save, patches[2], patches[3]:
            error = raised(lambda: terraform_wrapper.terraform(RUN, ["apply", "-input=false", f"-var=bucket={BUCKET}", f"-var=role={ROLE}"], 30))
        self.save = save
        return error

    def test_a_failed_call_gives_the_exit_status_and_names_the_file_that_keeps_the_output(self) -> None:
        error = self.run_terraform(CompletedProcess(["terraform"], 43, "", f"Error: {ROLE} {BUCKET}"))
        self.assertEqual(
            "terraform apply failed with exit status 43; its full output is in runs/20260101t0000-abcdef/raw/terraform_apply_failure.txt", str(error)
        )
        self.save.assert_called_once_with(RUN.run_id, "terraform_apply_failure.txt", f"Error: {ROLE} {BUCKET}")

    def test_a_failed_write_of_the_private_file_still_gives_the_exit_status(self) -> None:
        error = self.run_terraform(CompletedProcess(["terraform"], 43, "", f"Error: {ROLE}"), saved=OSError(BUCKET))
        self.assertIsInstance(error, WrapperError)
        self.assertEqual("terraform apply failed with exit status 43; its full output could not be kept", str(error))
        self.assert_private_values_absent(error)

    def test_a_timeout_names_the_command_word_and_the_deadline_and_keeps_the_partial_output_privately(self) -> None:
        timeout = TimeoutExpired(["/opt/terraform", "-chdir=x", "apply", *PRIVATE_ARGUMENTS], 30, output=f"out {BUCKET}".encode(), stderr=f"err {ROLE}")
        error = self.run_terraform(timeout)
        self.assertIsInstance(error, WrapperError)
        self.assertEqual(
            "terraform apply timed out after 30 seconds; its partial output is in runs/20260101t0000-abcdef/raw/terraform_apply_failure.txt", str(error)
        )
        self.assert_private_values_absent(error)
        self.save.assert_called_once_with(RUN.run_id, "terraform_apply_failure.txt", f"out {BUCKET}err {ROLE}")

    def test_a_timeout_with_a_failed_write_still_says_so(self) -> None:
        timeout = TimeoutExpired(["terraform", *PRIVATE_ARGUMENTS], 30, output=b"partial")
        error = self.run_terraform(timeout, saved=OSError(ROLE))
        self.assertEqual("terraform apply timed out after 30 seconds; its partial output could not be kept", str(error))
        self.assert_private_values_absent(error)

    def test_a_timeout_with_no_partial_output_writes_no_file(self) -> None:
        error = self.run_terraform(TimeoutExpired(["terraform", *PRIVATE_ARGUMENTS], 30))
        self.assertEqual("terraform apply timed out after 30 seconds", str(error))
        self.save.assert_not_called()


class HostCallTest(Boundary):
    def test_an_ansible_timeout_names_the_tool_and_the_deadline_and_keeps_the_partial_output_privately(self) -> None:
        timeout = TimeoutExpired(["ansible-playbook", "host.yml", "--extra-vars", f"{{'bucket': '{BUCKET}', 'role': '{ROLE}'}}"], 900, output=f"x {BUCKET}")
        with (
            mock.patch.object(host, "run_command", side_effect=timeout),
            mock.patch.object(host, "ssh_arguments", return_value=[]),
            mock.patch.object(terraform_wrapper, "save_raw") as save,
        ):
            error = raised(lambda: host.run_ansible(RUN.run_id, "100.64.0.1", {"bucket": BUCKET, "role": ROLE}))
        self.assertIsInstance(error, host.HostError)
        deadline = f"{host.ANSIBLE_TIMEOUT_SECONDS:g}"
        self.assertEqual(
            f"ansible-playbook host.yml timed out after {deadline} seconds; its partial output is in runs/{RUN.run_id}/raw/ansible_output.txt", str(error)
        )
        self.assert_private_values_absent(error)
        save.assert_called_once_with(RUN.run_id, "ansible_output.txt", f"x {BUCKET}")

    def test_an_ansible_timeout_with_a_failed_write_still_says_so(self) -> None:
        timeout = TimeoutExpired(["ansible-playbook", "host.yml", ROLE], 900, output=b"partial")
        with (
            mock.patch.object(host, "run_command", side_effect=timeout),
            mock.patch.object(host, "ssh_arguments", return_value=[]),
            mock.patch.object(terraform_wrapper, "save_raw", side_effect=OSError(BUCKET)),
        ):
            error = raised(lambda: host.run_ansible(RUN.run_id, "100.64.0.1", {}))
        self.assertTrue(str(error).endswith("its partial output could not be kept"), str(error))
        self.assert_private_values_absent(error)

    def test_a_tailscale_timeout_is_not_ready_and_shows_no_argument(self) -> None:
        timeout = TimeoutExpired(["/Applications/Tailscale", "status", "--json", *PRIVATE_ARGUMENTS], 20)
        with mock.patch.object(host, "run_command", side_effect=timeout):
            error = raised(lambda: host.tailscale_status(20))
        self.assertIsInstance(error, host.NoPeerError)
        self.assertEqual("tailscale status timed out after 20 seconds", str(error))
        self.assert_private_values_absent(error)

    def test_an_ssh_timeout_is_no_answer_and_raises_nothing_that_shows_an_argument(self) -> None:
        timeout = TimeoutExpired(["/usr/bin/ssh", f"user@{BUCKET}", f"echo {ROLE}"], 20)
        with mock.patch.object(host, "run_command", side_effect=timeout), mock.patch.object(host, "ssh_arguments", return_value=[]):
            self.assertIsNone(host.ssh_output(RUN.run_id, "100.64.0.1", f"echo {ROLE}", 20))


class PolicyToolTest(Boundary):
    def test_a_timeout_of_the_history_setting_read_shows_no_argument(self) -> None:
        timeout = TimeoutExpired(["/usr/bin/aws", "configure", "get", "cli_history", "--profile", ROLE], 30)
        with mock.patch.object(manage_policies, "run_command", side_effect=timeout):
            error = raised(lambda: manage_policies.refuse_while_history_records(Aws("admin_profile", "us-west-2")))
        self.assertIsInstance(error, ConfigurationError)
        self.assert_private_values_absent(error)

    def test_a_timeout_of_the_history_setting_read_names_the_command_and_the_deadline_and_no_profile(self) -> None:
        timeout = TimeoutExpired(["/usr/bin/aws", "configure", "get", "cli_history", "--profile", ROLE], 30)
        with mock.patch.object(manage_policies, "run_command", side_effect=timeout):
            error = raised(lambda: manage_policies.refuse_while_history_records(Aws("admin_profile", "us-west-2")))
        self.assertIsInstance(error, ConfigurationError)
        self.assertIn("aws configure get timed out after 30 seconds", str(error))
        self.assertNotIn("admin_profile", f"{error!s} {error!r}")
        self.assert_private_values_absent(error)

    def test_a_timeout_of_the_create_call_keeps_the_reservation_message_and_shows_no_argument(self) -> None:
        timeout = TimeoutExpired(["/usr/bin/aws", "iam", "create-access-key", "--user-name", ROLE], 120)
        with tempfile.TemporaryDirectory() as scratch, mock.patch.object(aws_cli, "run_command", side_effect=timeout):
            backup = Path(scratch) / "keys.csv"
            error = raised(lambda: manage_policies.write_new_key(Aws("admin_profile", "us-west-2"), "the_user", backup))
            self.assertTrue(backup.exists())
        self.assertIsInstance(error, ConfigurationError)
        self.assertIn("check the access keys of the_user as the owner before a retry", str(error))
        self.assertNotIn(ROLE, printed(error))


if __name__ == "__main__":
    unittest.main()
