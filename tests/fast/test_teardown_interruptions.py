"""Cases a real teardown cannot readily force: a wrong approved entry, a state that moved, an interrupted cleanup and a failed tool call.

Each case drives the real function with a recording stand-in for the AWS command line, because the failure it covers needs a bucket of another
account, a stop between two calls or a tool error with private names in its text.
"""

from __future__ import annotations

import json
import stat
import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

from scripts import aws_cli
from scripts.aws_cli import Aws, AwsError
from scripts.infra import teardown, terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding, adoption_problem, bucket_problem
from scripts.infra.teardown import TeardownRefused, approved_problems, lock_takeover_problem, snapshot_problem
from scripts.infra.terraform_wrapper import WrapperError
from scripts.process import CompletedProcess

STATE_KEYS = ["state/dev/terraform.tfstate", "state/dev/terraform.tfstate.tflock"]
RUN = RunBinding("20260101t0000-abcdef", "c0ffee", "s3", "us-west-2", "example-project", "default", STATE_KEYS, "verified", bucket_creation="created")
SETTINGS = {"S3_BUCKET": "example-project", "ENVIRONMENT": "dev", "AWS_ACCOUNT_ID": "example-account"}
ACCOUNT = "example-account"


class RecordingAws(Aws):
    """Answers each AWS call from a list of (name prefix, answer) pairs and records the arguments of every call."""

    answers: list[tuple[tuple[str, ...], Any]]
    calls: list[tuple[str, ...]]

    def __init__(self, answers: list[tuple[tuple[str, ...], Any]]) -> None:
        super().__init__("profile", "us-west-2")
        object.__setattr__(self, "answers", answers)
        object.__setattr__(self, "calls", [])

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = 0) -> Any:
        self.calls.append(arguments)
        for prefix, answer in self.answers:
            if arguments[: len(prefix)] == prefix:
                return answer(arguments) if callable(answer) else answer
        return {}

    def named(self, *prefix: str) -> list[tuple[str, ...]]:
        return [call for call in self.calls if call[: len(prefix)] == prefix]


class ApprovedListTest(unittest.TestCase):
    def test_approved_data_under_the_data_prefixes_and_state_at_this_runs_keys_pass(self) -> None:
        approved = {"application": [["example-project", "landing/a.csv", "v1"], ["example-project", "warehouse/t/x", "v2"]], "state": [[STATE_KEYS[0], "v3"]]}
        self.assertEqual([], approved_problems(approved, RUN))

    def test_an_approved_entry_at_another_environments_state_is_refused(self) -> None:
        approved = {"application": [["example-project", "state/prod/terraform.tfstate", "v1"]], "state": []}
        self.assertEqual(["the approved inventory lists an entry outside the data prefixes: state/prod/terraform.tfstate"], approved_problems(approved, RUN))

    def test_an_approved_entry_in_another_bucket_is_refused(self) -> None:
        approved = {"application": [["another-bucket", "landing/a.csv", "v1"]], "state": []}
        self.assertEqual(["the approved inventory lists an entry in another bucket: landing/a.csv"], approved_problems(approved, RUN))

    def test_an_approved_state_entry_at_another_key_is_refused(self) -> None:
        approved = {"application": [], "state": [["state/prod/terraform.tfstate", "v1"]]}
        self.assertEqual(
            ["the approved inventory lists a state entry outside this run's state keys: state/prod/terraform.tfstate"], approved_problems(approved, RUN)
        )


class SnapshotTest(unittest.TestCase):
    def test_the_state_the_plan_was_saved_for_passes(self) -> None:
        self.assertIsNone(snapshot_problem(("lineage-1", 7), {"state_lineage": "lineage-1", "state_serial": 7}))

    def test_a_state_with_another_serial_or_lineage_is_refused_before_the_host_is_released(self) -> None:
        expected = "the state changed after the destroy plan was saved: run make teardown-plan again"
        self.assertEqual(expected, snapshot_problem(("lineage-1", 6), {"state_lineage": "lineage-1", "state_serial": 7}))
        self.assertEqual(expected, snapshot_problem(("lineage-2", 7), {"state_lineage": "lineage-1", "state_serial": 7}))


class ApplyStageTest(unittest.TestCase):
    def test_a_failed_write_of_the_raw_log_after_a_successful_apply_still_records_the_stage(self) -> None:
        finished: list[str] = []
        with (
            mock.patch.object(teardown, "terraform", return_value="Resources: 0 added, 0 changed, 3 destroyed"),
            mock.patch.object(teardown, "save_raw", side_effect=OSError("disk full")),
            mock.patch("sys.stdout"),
            mock.patch("sys.stderr"),
        ):
            teardown.apply_destroy(RUN, Path("approved.tfplan"), finished.append)
        self.assertEqual(["apply"], finished)

    def test_a_failed_apply_does_not_record_the_stage(self) -> None:
        finished: list[str] = []
        with mock.patch.object(teardown, "terraform", side_effect=WrapperError("terraform apply failed")), self.assertRaises(WrapperError):
            teardown.apply_destroy(RUN, Path("approved.tfplan"), finished.append)
        self.assertEqual([], finished)


class OwnerTest(unittest.TestCase):
    def test_every_delete_call_names_the_expected_owner(self) -> None:
        listing = {"Versions": [{"Key": "landing/a.csv", "VersionId": "v1"}]}
        aws = RecordingAws([(("s3api", "list-object-versions"), listing)])
        teardown.purge(aws, "example-project", [("landing/a.csv", "v1")], ACCOUNT)
        deletes = aws.named("s3api", "delete-object")
        self.assertEqual(1, len(deletes))
        self.assertEqual(ACCOUNT, deletes[0][deletes[0].index("--expected-bucket-owner") + 1])

    def test_the_bucket_removal_names_the_expected_owner(self) -> None:
        local = RunBinding(**{**RUN.__dict__, "backend": "local", "state_keys": ["/srv/run/terraform.tfstate"]})
        heads: Iterator[dict[str, str] | None] = iter([{}, {}, None])
        aws = RecordingAws([(("s3api", "head-bucket"), lambda _: next(heads)), (("s3api", "list-object-versions"), {})])
        with tempfile.TemporaryDirectory() as scratch:
            teardown.clean_bucket(aws, local, {"state": [], "state_lineage": "", "state_serial": 0}, Path(scratch), ACCOUNT)
        removal = aws.named("s3api", "delete-bucket")
        self.assertEqual(1, len(removal))
        self.assertEqual(ACCOUNT, removal[0][removal[0].index("--expected-bucket-owner") + 1])

    def test_a_created_bucket_that_another_account_owns_is_refused_before_any_repair(self) -> None:
        self.assertEqual(
            "this run's bucket is not owned by this account any more", adoption_problem("created", exists=True, owned=False, empty=False, short_lived=False)
        )


class LockTakeoverTest(unittest.TestCase):
    def record(self, **changes: Any) -> bytes:
        return json.dumps({"ID": "teardown-20260101t0000-abcdef", "Who": "this-computer:4242", "Path": f"example-project/{STATE_KEYS[0]}"} | changes).encode()

    def test_the_lock_of_this_runs_own_stopped_cleanup_is_taken_over(self) -> None:
        self.assertIsNone(lock_takeover_problem(self.record(), RUN, "this-computer", lambda pid: False))

    def test_a_lock_whose_cleanup_still_runs_is_kept(self) -> None:
        self.assertEqual("this run's cleanup still runs in another process", lock_takeover_problem(self.record(), RUN, "this-computer", lambda pid: True))

    def test_a_lock_of_another_holder_is_kept(self) -> None:
        other = "another process holds the state lock"
        self.assertEqual(other, lock_takeover_problem(self.record(ID="1b2c"), RUN, "this-computer", lambda pid: False))
        self.assertEqual(other, lock_takeover_problem(self.record(Who="another-computer:4242"), RUN, "this-computer", lambda pid: False))
        self.assertEqual(other, lock_takeover_problem(self.record(Who="this-computer:x"), RUN, "this-computer", lambda pid: False))
        self.assertEqual(other, lock_takeover_problem(b"not json", RUN, "this-computer", lambda pid: False))
        self.assertEqual(
            other, lock_takeover_problem(self.record(Path="example-project/state/prod/terraform.tfstate"), RUN, "this-computer", lambda pid: False)
        )

    def test_a_refused_lock_write_takes_over_exactly_the_version_this_run_left(self) -> None:
        listing = {"Versions": [{"Key": STATE_KEYS[1], "VersionId": "lock-v1", "IsLatest": True}]}
        aws = RecordingAws([(("s3api", "put-object"), None), (("s3api", "list-object-versions"), listing)])
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(teardown, "entry_content", return_value=self.record(Who=f"{teardown.this_computer()}:999999999")),
            mock.patch.object(teardown, "process_runs", return_value=False),
        ):
            self.assertEqual("lock-v1", teardown.hold_state_lock(aws, RUN, Path(scratch)))

    def test_a_refused_lock_write_with_a_foreign_lock_stops_the_cleanup(self) -> None:
        listing = {"Versions": [{"Key": STATE_KEYS[1], "VersionId": "lock-v1", "IsLatest": True}]}
        aws = RecordingAws([(("s3api", "put-object"), None), (("s3api", "list-object-versions"), listing)])
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(teardown, "entry_content", return_value=self.record(ID="1b2c")),
            self.assertRaises(TeardownRefused),
        ):
            teardown.hold_state_lock(aws, RUN, Path(scratch))


class BindingTest(unittest.TestCase):
    def test_a_binding_with_the_state_keys_of_before_the_prefix_change_is_refused(self) -> None:
        old = RunBinding(**{**RUN.__dict__, "state_keys": ["terraform_state/dev/terraform.tfstate", "terraform_state/dev/terraform.tfstate.tflock"]})
        self.assertEqual("the binding's state keys are not this environment's keys under the state prefix", bucket_problem(old, SETTINGS))
        self.assertIsNone(bucket_problem(RUN, SETTINGS))

    def test_a_local_binding_keeps_its_own_state_path(self) -> None:
        local = RunBinding(**{**RUN.__dict__, "backend": "local", "state_keys": ["/srv/run/terraform.tfstate"]})
        self.assertIsNone(bucket_problem(local, SETTINGS))

    def test_apply_refuses_a_test_binding_that_names_the_permanent_bucket_before_it_reads_a_plan(self) -> None:
        disguised = RunBinding(**{**RUN.__dict__, "bucket_mode": "short_lived"})
        with mock.patch.object(terraform_wrapper, "terraform") as called, self.assertRaises(WrapperError) as refused:
            terraform_wrapper.apply_plan(disguised, "abcdef123456", SETTINGS)
        self.assertEqual("refused before any change: the binding's test bucket is not the short-lived bucket of this run", str(refused.exception))
        called.assert_not_called()


class ErrorTextTest(unittest.TestCase):
    def test_a_failed_aws_call_names_its_error_code_and_keeps_the_private_text_in_an_owner_only_file(self) -> None:
        text = "An error occurred (AccessDenied) when calling the PutObject operation: arn:aws:s3:::example-private-bucket/role/private_role_name"
        with tempfile.TemporaryDirectory() as scratch:
            failed = CompletedProcess(["aws"], 254, "", text)
            with (
                mock.patch.object(aws_cli, "RAW_ERRORS", Path(scratch) / "raw_errors"),
                mock.patch.object(aws_cli, "run_command", return_value=failed),
                self.assertRaises(AwsError) as raised,
            ):
                Aws("profile", "us-west-2").call("s3api", "put-object", "--bucket", "example-private-bucket")
            message = str(raised.exception)
            kept = list((Path(scratch) / "raw_errors").iterdir())
            self.assertEqual(1, len(kept))
            self.assertEqual(text, kept[0].read_text())
            self.assertEqual(0o600, stat.S_IMODE(kept[0].stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(kept[0].parent.stat().st_mode))
        self.assertIn("aws s3api put-object failed: AccessDenied", message)
        self.assertNotIn("example-private-bucket", message)
        self.assertNotIn("private_role_name", message)

    def test_a_failed_terraform_call_names_the_file_that_keeps_its_output_and_none_of_the_output(self) -> None:
        failed = CompletedProcess(["terraform"], 1, "", "Error: creating IAM Role (private_role_name): bucket example-private-bucket")
        with (
            mock.patch.object(terraform_wrapper, "run_command", return_value=failed),
            mock.patch.object(terraform_wrapper, "save_raw") as saved,
            mock.patch.object(terraform_wrapper, "configuration_folder", return_value=Path("configuration")),
            self.assertRaises(WrapperError) as raised,
        ):
            terraform_wrapper.terraform(RUN, ["apply", "-input=false"], 10)
        saved.assert_called_once()
        self.assertEqual("terraform apply failed; its full output is in runs/20260101t0000-abcdef/raw/terraform_apply_failure.txt", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
