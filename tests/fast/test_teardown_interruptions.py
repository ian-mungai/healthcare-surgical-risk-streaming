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
from scripts.infra.teardown import (
    TeardownRefused,
    approved_problems,
    destroy_already_applied,
    lock_takeover_problem,
    other_entry_problems,
    owner_problem,
    read_progress,
    resume_problem,
    snapshot_problem,
    write_progress,
)
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
            mock.patch.object(terraform_wrapper, "save_raw", side_effect=OSError("disk full")),
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


class ApplyRecordTest(unittest.TestCase):
    def test_a_failed_write_of_the_raw_apply_output_still_records_what_the_apply_created(self) -> None:
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(terraform_wrapper, "ROOT", Path(scratch)),
            mock.patch.object(terraform_wrapper, "binding_problems", return_value=[]),
            mock.patch.object(terraform_wrapper, "approved_plan_copy", return_value=Path(scratch) / "approved.tfplan"),
            mock.patch.object(terraform_wrapper, "terraform", side_effect=["{}", "Apply complete! Resources: 1 added"]),
            mock.patch.object(terraform_wrapper, "save_raw", side_effect=OSError("disk full")),
            mock.patch.object(terraform_wrapper, "record_created") as recorded,
            mock.patch("sys.stdout") as printed,
        ):
            plans = Path(scratch) / "runs" / RUN.run_id / "plans"
            plans.mkdir(parents=True)
            (plans / "apply.tfplan").write_bytes(b"plan")
            (plans / "apply_binding.json").write_text("{}")
            self.assertEqual(0, terraform_wrapper.apply_plan(RUN, "code", SETTINGS))
        recorded.assert_called_once()
        self.assertIn("its raw output could not be kept", "".join(call.args[0] for call in printed.write.call_args_list))


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
        return json.dumps(
            {
                "ID": "teardown-20260101t0000-abcdef",
                "Who": "this-computer:4242",
                "Started": "Thu Jan  1 00:00:00 2026",
                "Path": f"example-project/{STATE_KEYS[0]}",
            }
            | changes
        ).encode()

    def test_the_lock_of_this_runs_own_stopped_cleanup_is_taken_over(self) -> None:
        self.assertIsNone(lock_takeover_problem(self.record(), RUN, "this-computer", lambda pid, started: False))

    def test_a_lock_whose_cleanup_still_runs_is_kept(self) -> None:
        self.assertEqual(
            "this run's cleanup still runs in another process", lock_takeover_problem(self.record(), RUN, "this-computer", lambda pid, started: True)
        )

    def test_a_lock_of_another_holder_is_kept(self) -> None:
        other = "another process holds the state lock"
        self.assertEqual(other, lock_takeover_problem(self.record(ID="1b2c"), RUN, "this-computer", lambda pid, started: False))
        self.assertEqual(other, lock_takeover_problem(self.record(Who="another-computer:4242"), RUN, "this-computer", lambda pid, started: False))
        self.assertEqual(other, lock_takeover_problem(self.record(Who="this-computer:x"), RUN, "this-computer", lambda pid, started: False))
        self.assertEqual(other, lock_takeover_problem(b"not json", RUN, "this-computer", lambda pid, started: False))
        self.assertEqual(
            other, lock_takeover_problem(self.record(Path="example-project/state/prod/terraform.tfstate"), RUN, "this-computer", lambda pid, started: False)
        )

    def test_the_start_time_recorded_in_the_lock_is_what_the_process_check_receives(self) -> None:
        seen: list[tuple[int, str]] = []

        def runs(pid: int, started: str) -> bool:
            seen.append((pid, started))
            return False

        lock_takeover_problem(self.record(), RUN, "this-computer", runs)
        self.assertEqual([(4242, "Thu Jan  1 00:00:00 2026")], seen)

    def test_a_process_with_the_recorded_id_and_another_start_time_is_not_the_stopped_cleanup(self) -> None:
        with mock.patch.object(teardown, "process_start", return_value="Fri Jan  2 00:00:00 2026"):
            self.assertFalse(teardown.process_runs(4242, "Thu Jan  1 00:00:00 2026"))
        with mock.patch.object(teardown, "process_start", return_value="Thu Jan  1 00:00:00 2026"):
            self.assertTrue(teardown.process_runs(4242, "Thu Jan  1 00:00:00 2026"))
        with mock.patch.object(teardown, "process_start", return_value=""):
            self.assertFalse(teardown.process_runs(4242, "Thu Jan  1 00:00:00 2026"))
            self.assertFalse(teardown.process_runs(4242, ""))

    def test_a_lock_with_no_recorded_start_time_is_kept(self) -> None:
        self.assertEqual(
            "another process holds the state lock", lock_takeover_problem(self.record(Started=""), RUN, "this-computer", lambda pid, started: False)
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


class OwnerBeforeAnyStageTest(unittest.TestCase):
    def test_a_bucket_this_account_owns_passes(self) -> None:
        self.assertIsNone(owner_problem(RecordingAws([(("s3api", "head-bucket"), {})]), RUN, ACCOUNT))

    def test_an_account_value_that_does_not_own_the_bucket_is_refused(self) -> None:
        answers: Iterator[dict[str, str] | None] = iter([{}, None])
        aws = RecordingAws([(("s3api", "head-bucket"), lambda _: next(answers))])
        self.assertEqual("the bucket is not owned by the account in AWS_ACCOUNT_ID", owner_problem(aws, RUN, "another-account"))
        self.assertIn("another-account", aws.calls[1])

    def test_a_bucket_that_is_already_gone_has_no_owner_to_check(self) -> None:
        self.assertIsNone(owner_problem(RecordingAws([(("s3api", "head-bucket"), None)]), RUN, ACCOUNT))


def entry(key: str) -> dict[str, str]:
    return {"Key": key, "VersionId": "v1"}


class KeptBucketTest(unittest.TestCase):
    KEPT = RunBinding(**{**RUN.__dict__, "bucket_creation": "none"})

    def test_another_environments_state_in_a_kept_bucket_is_left_alone(self) -> None:
        other = [entry("state/prod/terraform.tfstate"), entry("state/prod/terraform.tfstate.tflock")]
        self.assertEqual([], other_entry_problems(other, self.KEPT))

    def test_an_entry_under_an_unknown_prefix_in_a_kept_bucket_is_refused(self) -> None:
        expected = ["bucket: entry outside the data prefixes and this run's state keys: staging/control.txt"]
        self.assertEqual(expected, other_entry_problems([entry("staging/control.txt"), entry("state/prod/terraform.tfstate")], self.KEPT))

    def test_a_file_that_only_looks_like_state_is_refused(self) -> None:
        for key in (
            "state/terraform.tfstate",
            "state/prod/other.tfstate",
            "state/prod/deeper/terraform.tfstate",
            "xstate/prod/terraform.tfstate",
            "state/prod/terraform.tfstate.backup",
        ):
            self.assertEqual([f"bucket: entry outside the data prefixes and this run's state keys: {key}"], other_entry_problems([entry(key)], self.KEPT))

    def test_a_created_bucket_refuses_every_other_entry(self) -> None:
        expected = ["bucket: entry outside the data prefixes and this run's state keys: state/prod/terraform.tfstate"]
        self.assertEqual(expected, other_entry_problems([entry("state/prod/terraform.tfstate")], RUN))


BINDING = {"code": "c", "approved_sha256": "a", "plan_sha256": "p", "inventory_sha256": "i", "receipt_sha256": "r"}


class ResumeTest(unittest.TestCase):
    def test_a_new_approval_is_accepted_while_the_apply_stage_is_unfinished(self) -> None:
        self.assertIsNone(resume_problem(BINDING | {"finished": ["release"]}, BINDING | {"code": "new", "plan_sha256": "new"}))
        self.assertIsNone(resume_problem(None, BINDING))

    def test_after_the_apply_stage_a_rerun_must_carry_the_approval_it_started_with(self) -> None:
        expected = "this teardown started with another code: a rerun must use the approval it started with"
        self.assertEqual(expected, resume_problem(BINDING | {"finished": ["release", "apply"]}, BINDING | {"code": "new"}))
        self.assertIsNone(resume_problem(BINDING | {"finished": ["release", "apply"]}, BINDING))

    def test_a_destroy_whose_reply_was_lost_is_recognised_by_the_empty_newer_state(self) -> None:
        approved = {"state_lineage": "lineage-1", "state_serial": 7}
        recorded = BINDING | {"finished": ["release"]}
        self.assertTrue(destroy_already_applied(recorded, BINDING, ("lineage-1", 8, 0), approved))

    def test_a_state_that_still_holds_resources_or_did_not_move_is_not_an_applied_destroy(self) -> None:
        approved = {"state_lineage": "lineage-1", "state_serial": 7}
        recorded = BINDING | {"finished": ["release"]}
        self.assertFalse(destroy_already_applied(recorded, BINDING, ("lineage-1", 8, 2), approved))
        self.assertFalse(destroy_already_applied(recorded, BINDING, ("lineage-1", 7, 0), approved))
        self.assertFalse(destroy_already_applied(recorded, BINDING, ("lineage-2", 8, 0), approved))

    def test_an_applied_destroy_is_recognised_only_for_the_approval_that_passed_the_guards_and_released_the_host(self) -> None:
        approved = {"state_lineage": "lineage-1", "state_serial": 7}
        self.assertFalse(destroy_already_applied(None, BINDING, ("lineage-1", 8, 0), approved))
        self.assertFalse(destroy_already_applied(BINDING | {"finished": []}, BINDING, ("lineage-1", 8, 0), approved))
        self.assertFalse(destroy_already_applied(BINDING | {"finished": ["release"]}, BINDING | {"code": "new"}, ("lineage-1", 8, 0), approved))


class ProgressFileTest(unittest.TestCase):
    def test_a_write_that_stops_before_it_completes_leaves_the_earlier_progress_readable(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            removal = Path(scratch) / "removal"
            write_progress(removal, BINDING, ["release"])
            with mock.patch("os.replace", side_effect=OSError("stopped")), self.assertRaises(OSError):
                write_progress(removal, BINDING, ["release", "apply"])
            recorded = read_progress(removal)
            self.assertEqual(["release"], recorded["finished"] if recorded else None)
            write_progress(removal, BINDING, ["release", "apply"])
            recorded = read_progress(removal)
            self.assertEqual(["release", "apply"], recorded["finished"] if recorded else None)
            self.assertEqual(["progress.json"], sorted(path.name for path in removal.iterdir()))


class DestroyCommandTest(unittest.TestCase):
    """The whole destroy command on a scratch run folder, with every tool call replaced, for the two orders a pure function cannot show."""

    APPROVED = {"application": [], "state": [], "state_lineage": "lineage-1", "state_serial": 7, "local_state_sha256": ""}

    def folder(self, scratch: str) -> Path:
        folder = Path(scratch) / "runs" / RUN.run_id
        (folder / "plans").mkdir(parents=True)
        (folder / "teardown").mkdir()
        (folder / "inventory.json").write_text(json.dumps({"created": []}))
        (folder / "plans" / "destroy_binding.json").write_text(json.dumps({"plan_sha256": "0" * 64}))
        (folder / "plans" / "destroy.tfplan").write_bytes(b"plan")
        (folder / "teardown" / "approved_inventory.json").write_text(json.dumps(self.APPROVED))
        return folder

    def test_a_mistyped_account_is_refused_before_the_guards_the_host_release_and_the_apply(self) -> None:
        answers: Iterator[dict[str, str] | None] = iter([{}, None])
        aws = RecordingAws([(("s3api", "head-bucket"), lambda _: next(answers))])
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(teardown, "ROOT", Path(scratch)),
            mock.patch.object(teardown, "guard_problems") as guards,
            mock.patch.object(teardown, "approved_plan_copy") as copied,
            mock.patch.object(teardown, "terraform") as called,
            mock.patch("scripts.infra.host.release_tailscale_device") as released,
            self.assertRaises(TeardownRefused) as refused,
        ):
            self.folder(scratch)
            teardown.destroy(aws, SETTINGS | {"AWS_ACCOUNT_ID": "another-account"}, RUN, "code")
        self.assertEqual("refused before any deletion: the bucket is not owned by the account in AWS_ACCOUNT_ID", str(refused.exception))
        for stage in (guards, copied, called, released):
            stage.assert_not_called()
        self.assertEqual([], aws.named("s3api", "delete-object") + aws.named("s3api", "delete-bucket"))

    def test_a_rerun_after_a_lost_apply_reply_records_the_stage_and_does_not_apply_again(self) -> None:
        aws = RecordingAws([(("s3api", "head-bucket"), {})])
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(teardown, "ROOT", Path(scratch)),
            mock.patch.object(teardown, "state_snapshot", return_value=("lineage-1", 8, 0)),
            mock.patch.object(teardown, "guard_problems") as guards,
            mock.patch.object(teardown, "apply_destroy") as applied,
            mock.patch.object(teardown, "purge") as purged,
            mock.patch.object(teardown, "clean_bucket", return_value="none: no resource is retained"),
            mock.patch.object(teardown, "checked_removals", return_value=[]),
            mock.patch("sys.stdout"),
        ):
            folder = self.folder(scratch)
            files = teardown.approval_bytes(folder)
            binding = teardown.approval_binding("code", files, b"plan", None)
            write_progress(folder / "removal", binding, ["release"])
            self.assertEqual(0, teardown.destroy(aws, SETTINGS, RUN, "code"))
            recorded = read_progress(folder / "removal")
        guards.assert_not_called()
        applied.assert_not_called()
        purged.assert_called_once()
        self.assertEqual(["release", "apply", "purge", "state", "checks"], recorded["finished"] if recorded else None)

    def test_a_rerun_whose_state_still_holds_resources_runs_every_guard_again(self) -> None:
        aws = RecordingAws([(("s3api", "head-bucket"), {})])
        with (
            tempfile.TemporaryDirectory() as scratch,
            mock.patch.object(teardown, "ROOT", Path(scratch)),
            mock.patch.object(teardown, "state_snapshot", return_value=("lineage-1", 8, 2)),
            mock.patch.object(teardown, "approved_plan_copy", return_value=Path(scratch) / "approved.tfplan"),
            mock.patch.object(teardown, "guard_problems", return_value=["the state changed after the destroy plan was saved: run make teardown-plan again"]),
            mock.patch.object(teardown, "apply_destroy") as applied,
            self.assertRaises(TeardownRefused),
        ):
            folder = self.folder(scratch)
            binding = teardown.approval_binding("code", teardown.approval_bytes(folder), b"plan", None)
            write_progress(folder / "removal", binding, ["release"])
            teardown.destroy(aws, SETTINGS, RUN, "code")
        applied.assert_not_called()


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
        self.assertEqual(
            "terraform apply failed with exit status 1; its full output is in runs/20260101t0000-abcdef/raw/terraform_apply_failure.txt", str(raised.exception)
        )


if __name__ == "__main__":
    unittest.main()
