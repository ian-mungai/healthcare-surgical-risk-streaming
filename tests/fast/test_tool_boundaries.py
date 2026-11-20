"""What the AWS call helper does with its retry setting and with a private error file that cannot be written.

A full disk with private names in the error text needs a stand-in for the launcher, so each case drives the real function with one. Failure modes covered here:

- The call that must not repeat lets the AWS command line retry, and other calls get a changed environment.
- A private error file that cannot be written hides the AWS error code, so a definite refusal looks uncertain.
"""

from __future__ import annotations

import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts import aws_cli
from scripts.aws_cli import Aws, AwsError
from scripts.process import CompletedProcess

BUCKET = "example-private-bucket"
ROLE = "private_role_name"


def raised(call: Callable[[], Any]) -> Exception:
    """The exception the call raises, or a failure when it raises none."""
    try:
        call()
    except Exception as error:
        return error
    raise AssertionError("the call raised nothing")


class AwsCallTest(unittest.TestCase):
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
        self.assertIsNone(error.__cause__)
        self.assertTrue(error.__suppress_context__)
        self.assertNotIn(ROLE, str(error))
        self.assertNotIn(BUCKET, str(error))

    def test_a_failed_write_with_no_error_code_gives_the_exit_status(self) -> None:
        failed = CompletedProcess(["aws"], 130, "", f"no code in this text {BUCKET}")
        with mock.patch.object(aws_cli, "run_command", return_value=failed), mock.patch.object(aws_cli, "keep_raw_error", side_effect=OSError("full")):
            error = raised(lambda: self.aws.call("s3api", "head-bucket"))
        self.assertEqual("aws s3api head-bucket failed: exit status 130; its full error text could not be kept", str(error))


if __name__ == "__main__":
    unittest.main()
