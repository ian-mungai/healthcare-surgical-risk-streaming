"""A cloud test runner removes its recovery binding only when it has confirmed that every resource the binding names is gone.

Failure modes covered here:

- A runner whose bucket-create step timed out late (the binding names a bucket that may exist) saves "no bucket was created", deletes the binding and leaves the
  bucket.
- A runner whose binding names a bucket that is not a short-lived test bucket claims that no bucket was created and deletes the binding.
- A runner deletes its binding when its cleanup failed, kept the bucket, or could not confirm that S3 reports the bucket gone.
- The teardown fixture removes its bucket and its binding after a failure during or after the apply, which may have written a role or a state that only
  the binding and the state name.
- The refusal test of a wrong state location alters the genuine binding, and a timeout keeps the altered copy.
- A bucket create that was attempted and whose own AWS call timed out exits non-zero while the bucket may still land: the runner finds it missing (S3 answers
  404 right then) and removes the binding that is the only name of the bucket.
- The teardown fixtures' summary says that each fixture removed its own resources while a fixture kept its binding because resources remain.

The helper is in the tracked ``tests/e2e/run_record.py`` and runs everywhere. The runners of the cloud checks stay untracked; their tests run only where a
runner is on disk, with the cloud replaced by stand-ins, and are skipped elsewhere.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import AwsError
from scripts.process import TimeoutExpired
from tests.e2e import run_record
from tests.e2e.run_record import CREATE_STEP, StepTimedOut, named_bucket, settle_binding, timed_command
from tests.fast import test_e2e_step_deadlines as base

CONFIGURATION = base.CONFIGURATION
TEST_BUCKET = "example-bucket-test-abcdef"
PROJECT_BUCKET = CONFIGURATION["S3_BUCKET"]
MAY_REMAIN = "may remain"
NONE_CREATED = "no bucket was created"


def done(code: int = 0, text: str = "") -> subprocess.CompletedProcess[str]:
    return base.done(code, text)


def verified_binding() -> dict[str, Any]:
    state = f"state/{CONFIGURATION['ENVIRONMENT']}/terraform.tfstate"
    return {
        "bucket": TEST_BUCKET,
        "run_id": "run_abcdef",
        "status": "verified",
        "backend": "s3",
        "workspace": "default",
        "state_keys": [state, f"{state}.tflock"],
        "bucket_creation": "created",
    }


class NamedBucketTest(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="named_")
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "binding.json"

    def test_no_file_names_no_bucket_and_that_is_not_the_same_as_a_file_that_names_none(self) -> None:
        self.assertIsNone(named_bucket(self.path))
        self.path.write_text("{}")
        self.assertEqual("", named_bucket(self.path))

    def test_a_file_that_cannot_be_read_names_no_bucket_but_is_still_a_file(self) -> None:
        for text in ("{not json", "[]", '{"bucket": 7}'):
            with self.subTest(text=text):
                self.path.write_text(text)
                self.assertEqual("", named_bucket(self.path))

    def test_a_file_names_its_bucket(self) -> None:
        self.path.write_text(json.dumps({"bucket": TEST_BUCKET}))
        self.assertEqual(TEST_BUCKET, named_bucket(self.path))


class SettleBindingTest(unittest.TestCase):
    """What a runner says about its bucket when it closes, and whether its binding file can go."""

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="settle_")
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "binding_x.json"
        self.remove = mock.Mock(return_value=("the bucket was deleted", True))

    def settle(self, *, started: bool = True, timed_out: StepTimedOut | None = None) -> tuple[str, bool]:
        return settle_binding(self.path, PROJECT_BUCKET, self.remove, started=started, timed_out=timed_out)

    def step_timeout(self, step: str) -> StepTimedOut:
        failure = StepTimedOut(f"{step} timed out after 5 seconds")
        failure.step = step
        return failure

    def test_a_run_that_stopped_before_its_create_step_created_no_bucket(self) -> None:
        self.assertEqual((NONE_CREATED, True), self.settle(started=False))
        self.remove.assert_not_called()

    def test_a_create_that_finished_and_wrote_no_binding_created_no_bucket_but_one_that_timed_out_may_have(self) -> None:
        text, settled = self.settle()
        self.assertTrue(text.startswith(NONE_CREATED))
        self.assertTrue(settled)
        text, settled = self.settle(timed_out=self.step_timeout(CREATE_STEP))
        self.assertNotIn(NONE_CREATED, text)
        self.assertIn(MAY_REMAIN, text)
        self.assertFalse(settled)
        self.remove.assert_not_called()

    def test_a_binding_that_cannot_be_read_or_names_a_bucket_that_is_no_test_bucket_is_kept_and_nothing_is_removed(self) -> None:
        for text in ("{not json", "{}", json.dumps({"bucket": PROJECT_BUCKET}), json.dumps({"bucket": "another-bucket"}), json.dumps({"bucket": 7})):
            with self.subTest(text=text):
                self.path.write_text(text)
                line, settled = self.settle()
                self.assertFalse(settled)
                self.assertIn(MAY_REMAIN, line)
                self.assertNotIn(NONE_CREATED, line)
                self.assertIn(self.path.name, line)
                self.remove.assert_not_called()

    def test_a_create_that_timed_out_keeps_the_binding_and_tries_no_cleanup_while_a_child_may_still_run(self) -> None:
        self.path.write_text(json.dumps({"bucket": TEST_BUCKET}))
        line, settled = self.settle(timed_out=self.step_timeout(CREATE_STEP))
        self.assertFalse(settled)
        self.assertIn(MAY_REMAIN, line)
        self.assertIn(self.path.name, line)
        self.remove.assert_not_called()

    def test_a_binding_whose_bucket_create_was_attempted_is_kept_and_nothing_is_removed_whichever_step_ended_the_run(self) -> None:
        self.path.write_text(json.dumps({"bucket": TEST_BUCKET, "bucket_creation": "attempted"}))
        for timed_out in (None, self.step_timeout("scripts.ingestion.cli publish")):
            with self.subTest(timed_out=timed_out):
                line, settled = self.settle(timed_out=timed_out)
                self.assertFalse(settled)
                self.assertIn(MAY_REMAIN, line)
                self.assertIn("attempted", line)
                self.assertIn(self.path.name, line)
                self.assertNotIn(NONE_CREATED, line)
        self.remove.assert_not_called()

    def test_a_binding_whose_bucket_create_finished_or_never_ran_goes_through_the_removal(self) -> None:
        for creation in ("created", "none"):
            with self.subTest(creation=creation):
                self.remove.reset_mock()
                self.path.write_text(json.dumps({"bucket": TEST_BUCKET, "bucket_creation": creation}))
                self.assertEqual(("the bucket was deleted", True), self.settle())
                self.remove.assert_called_once_with(TEST_BUCKET)

    def test_the_binding_of_a_test_bucket_goes_only_when_the_removal_confirmed_the_bucket_gone(self) -> None:
        self.path.write_text(json.dumps({"bucket": TEST_BUCKET}))
        self.assertEqual(("the bucket was deleted", True), self.settle())
        self.remove.assert_called_once_with(TEST_BUCKET)
        self.remove.return_value = ("bucket kept with 2 entries", False)
        line, settled = self.settle()
        self.assertFalse(settled)
        self.assertIn("bucket kept with 2 entries", line)
        self.assertIn(MAY_REMAIN, line)
        self.assertIn(self.path.name, line)

    def test_a_later_timeout_still_tries_the_cleanup_and_the_binding_goes_only_on_its_confirmation(self) -> None:
        self.path.write_text(json.dumps({"bucket": TEST_BUCKET}))
        later = self.step_timeout("scripts.ingestion.cli publish")
        self.assertEqual(("the bucket was deleted", True), self.settle(timed_out=later))
        self.remove.return_value = ("delete was sent but S3 still reports the bucket", False)
        self.assertFalse(self.settle(timed_out=later)[1])

    def test_a_timed_command_names_its_step_on_the_timeout_it_raises(self) -> None:
        with mock.patch.object(run_record, "run_command", side_effect=TimeoutExpired("python", 5)), self.assertRaises(StepTimedOut) as caught:
            timed_command("scripts.infra.state_bootstrap create", "python", ["-m", "x"], timeout=5)
        self.assertEqual(CREATE_STEP, caught.exception.step)


class RemovalCase(base.RunnerCase):
    """The answers of ``remove_test_bucket``: the second item is true only when S3 reported the bucket gone."""

    def aws_answers(self, head: list[Any], listing: dict[str, Any] | None = None, *, fails: bool = False) -> None:
        heads = iter(head)

        def answered(*arguments: str, **options: Any) -> Any:
            if fails:
                raise AwsError("a call failed")
            if arguments[1] == "head-bucket":
                return next(heads, {})
            return listing if arguments[1] == "list-object-versions" and listing is not None else {}

        self.aws.call.side_effect = answered

    def removal(self, *arguments: Any) -> tuple[str, bool]:
        with mock.patch.object(self.module, "GONE_DEADLINE_SECONDS", 0.2), mock.patch.object(self.module.time, "sleep", lambda seconds: None):
            result: tuple[str, bool] = self.module.remove_test_bucket(*arguments)
        return result

    def check_removal(self, call: Any) -> None:
        """The same five paths for each runner: gone, never there, kept for other contents, still reported and a failed call."""
        self.aws_answers([{}, None])
        self.assertTrue(call()[1], "S3 reported the bucket gone")
        self.aws_answers([None])
        self.assertTrue(call()[1], "S3 reports no such bucket, which is confirmed gone")
        self.aws_answers([{}], {"Versions": [{"Key": "other/key", "VersionId": "1"}]})
        self.assertFalse(call()[1], "the bucket holds an entry this run did not write")
        self.aws_answers([{}, {}, {}, {}, {}, {}, {}, {}])
        self.assertFalse(call()[1], "S3 still reports the bucket")
        self.aws_answers([], fails=True)
        self.assertFalse(call()[1], "a call failed")


class IngestionRemovalTest(RemovalCase):
    name = "ingestion_check"

    def test_a_removal_is_confirmed_only_when_s3_reports_the_bucket_gone(self) -> None:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        self.check_removal(lambda: self.removal(self.aws, TEST_BUCKET, record))


class StateBootstrapRemovalTest(RemovalCase):
    name = "state_bootstrap"

    def test_a_removal_is_confirmed_only_when_s3_reports_the_bucket_gone(self) -> None:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        entries = {"Versions": [{"Key": "other/key", "VersionId": "1"}]}
        shaped = {"blocks": {}, "versioning": "Enabled", "tags": {}}

        def readback(aws: Any, bucket: str) -> dict[str, Any]:
            listing = aws.call("s3api", "list-object-versions", "--bucket", bucket)
            return shaped | {"entries": len(listing.get("Versions", []))}

        with mock.patch.object(self.module, "readback", readback):
            self.aws_answers([{}, None], {})
            self.assertTrue(self.removal(self.aws, TEST_BUCKET, record)[1])
            self.aws_answers([None])
            self.assertTrue(self.removal(self.aws, TEST_BUCKET, record)[1])
            self.aws_answers([{}], entries)
            self.assertFalse(self.removal(self.aws, TEST_BUCKET, record)[1])
            self.aws_answers([{}] * 9, {})
            self.assertFalse(self.removal(self.aws, TEST_BUCKET, record)[1])
            self.aws_answers([], fails=True)
            self.assertFalse(self.removal(self.aws, TEST_BUCKET, record)[1])


class EnvironmentRemovalTest(RemovalCase):
    name = "environment_plan"

    def test_a_removal_is_confirmed_only_when_s3_reports_the_bucket_gone(self) -> None:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        self.check_removal(lambda: self.removal(self.aws, TEST_BUCKET, ["state", "state.tflock"], record))


class BindingCase(base.RunnerCase):
    """A runner run whose create step answers as the case says. ``create`` is one of ``timeout`` (the binding names the bucket and the step timed out),
    ``inner_timeout`` (the step itself finished with exit 1 after its own create-bucket call timed out, so the binding says the create was attempted),
    ``verified`` (the binding is whole and the step succeeded), ``project`` (the binding names the project's bucket), ``none`` (no binding is written and the
    step failed), ``unreadable`` (the binding is not JSON and the step failed). Every other command times out."""

    create = "verified"
    binding_glob = ""

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if "scripts.infra.state_bootstrap" in args and "create" in args:
            path = Path(args[args.index("--binding") + 1])
            if self.create == "timeout":
                path.write_text(json.dumps(verified_binding() | {"status": "planned", "bucket_creation": "attempted"}))
                raise TimeoutExpired(program, options["timeout"])
            if self.create == "verified":
                path.write_text(json.dumps(verified_binding()))
            elif self.create == "inner_timeout":
                path.write_text(json.dumps(verified_binding() | {"status": "planned", "bucket_creation": "attempted"}))
                return done(1, "the create-bucket call timed out")
            elif self.create == "project":
                path.write_text(json.dumps(verified_binding() | {"bucket": PROJECT_BUCKET}))
            elif self.create == "keyless":
                path.write_text(json.dumps({key: value for key, value in verified_binding().items() if key != "state_keys"}))
            elif self.create == "unreadable":
                path.write_text("{not json")
                return done(1)
            else:
                return done(1)
            return done()
        if "scripts.ingestion.fetch_list" in args:
            return done()
        raise TimeoutExpired(program, options["timeout"])

    def patched_run(self) -> int:
        """Run the runner's ``main`` with the stand-ins that its cases need."""
        return self.run_main()

    def bindings(self) -> list[Path]:
        return sorted((self.root / "runs").glob(self.binding_glob))

    def cleanup_text(self) -> str:
        text: str = self.saved()["cleanup"]
        return text

    def assert_kept_with_resources_that_may_remain(self) -> None:
        self.assertEqual(1, len(self.bindings()), "the binding is kept")
        self.assertIn(MAY_REMAIN, self.cleanup_text())
        self.assertNotIn(NONE_CREATED, self.cleanup_text())
        self.assertIn(self.bindings()[0].name, self.cleanup_text())

    def assert_attempted_create_kept(self) -> None:
        """The run's own create call timed out, and S3 then answers that the bucket does not exist: the real removal would call that gone."""
        self.aws.call.side_effect = lambda *arguments, **options: None
        self.assertEqual(1, self.run_attempt())
        self.assert_kept_with_resources_that_may_remain()
        self.assertIn("attempted", self.cleanup_text())
        self.assertEqual([], [call for call in self.aws.call.mock_calls if "delete" in str(call)])

    def run_attempt(self) -> int:
        self.create = "inner_timeout"
        return self.patched_run()

    def assert_nothing_created(self) -> None:
        self.assertEqual([], self.bindings())
        self.assertTrue(self.cleanup_text().startswith(NONE_CREATED), self.cleanup_text())


class CleanupBindingCase(BindingCase):
    """Cases for the runners whose ``remove_test_bucket`` returns its text and whether S3 confirmed the bucket gone."""

    def run_with(self, create: str, removal: Any = None) -> tuple[int, mock.Mock]:
        self.create = create
        cleanup = mock.Mock(**({"side_effect": AssertionError("no cleanup")} if removal is None else {"return_value": removal}))
        with mock.patch.object(self.module, "remove_test_bucket", cleanup):
            return self.patched_run(), cleanup

    def test_a_create_that_timed_out_keeps_its_binding_says_the_bucket_may_remain_and_removes_nothing_while_a_child_may_still_run(self) -> None:
        code, cleanup = self.run_with("timeout")
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        cleanup.assert_not_called()
        self.assertEqual([], [call for call in self.aws.call.mock_calls if "delete" in str(call)])

    def test_a_create_whose_own_call_timed_out_keeps_the_binding_although_s3_then_reports_no_such_bucket(self) -> None:
        self.assert_attempted_create_kept()

    def test_a_binding_that_names_the_project_s_bucket_is_kept_and_nothing_is_removed(self) -> None:
        code, cleanup = self.run_with("project")
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        cleanup.assert_not_called()

    def test_an_unreadable_binding_is_kept(self) -> None:
        code, cleanup = self.run_with("unreadable")
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        cleanup.assert_not_called()

    def test_a_create_that_finished_without_writing_a_binding_created_no_bucket(self) -> None:
        code, cleanup = self.run_with("none")
        self.assertEqual(1, code)
        self.assert_nothing_created()
        cleanup.assert_not_called()

    def test_the_binding_goes_only_when_the_cleanup_confirmed_the_bucket_gone(self) -> None:
        for gone in (False, True):
            with self.subTest(gone=gone):
                for path in self.bindings():
                    path.unlink()
                for folder in [path for path in (self.root / "runs").iterdir() if path.is_dir()]:
                    shutil.rmtree(folder)
                self.run_with("verified", ("the cleanup said so", gone))
                self.assertEqual(not gone, bool(self.bindings()))
                self.assertIn("the cleanup said so", self.cleanup_text())
                if not gone:
                    self.assertIn(MAY_REMAIN, self.cleanup_text())
                    self.assertIn(self.bindings()[0].name, self.cleanup_text())


class IngestionBindingTest(CleanupBindingCase):
    name = "ingestion_check"
    binding_glob = "binding_ingestion_check_*.json"

    def patched_run(self) -> int:
        version = self.module.ObjectVersion("landing/k", "v1", 1, "2026-01-01T00:00:00Z", True, False)
        with (
            mock.patch.object(self.module, "read_sample", lambda scratch: ({"a/1.vital": b"x", "a/2.vital": b"y"}, {"a/3.vital": b"z"})),
            mock.patch.object(self.module, "put_version", lambda *arguments: version),
            mock.patch.object(self.module, "S3Store", mock.MagicMock()),
        ):
            return self.run_main()

    def test_a_run_that_stopped_before_its_create_step_created_no_bucket_and_has_no_binding(self) -> None:
        with mock.patch.object(self.module, "remove_test_bucket", mock.Mock(side_effect=AssertionError("no cleanup"))):
            self.assertEqual(1, self.run_main())
        self.assert_nothing_created()
        self.assertEqual(NONE_CREATED, self.cleanup_text())


class StateBootstrapBindingTest(CleanupBindingCase):
    name = "state_bootstrap"

    def patched_run(self) -> int:
        first = {
            "blocks": self.module.BLOCKS,
            "versioning": "Enabled",
            "tags": {"Project": CONFIGURATION["PROJECT_NAME"], "Environment": CONFIGURATION["ENVIRONMENT"]},
            "entries": 0,
        }
        pinned = self.root / ".tools" / "bin" / "terraform"
        with mock.patch.object(self.module, "readback", lambda aws, bucket: first), mock.patch.object(self.module, "pinned_terraform", lambda: pinned):
            return self.run_main()

    def bindings(self) -> list[Path]:
        return [path for path in (self.root / "runs").glob("binding_state_bootstrap_*.json") if "second" not in path.name]


class EnvironmentBindingTest(BindingCase):
    name = "environment_plan"

    def bindings(self) -> list[Path]:
        return [path for path in (self.root / "runs").glob("binding_environment_plan_*.json") if "refusal" not in path.name]

    def run_with(self, create: str, removal: Any = None) -> tuple[int, mock.Mock]:
        self.create = create
        cleanup = mock.Mock(**({"side_effect": AssertionError("no cleanup")} if removal is None else {"return_value": removal}))
        with mock.patch.object(self.module, "expected_creates", lambda environment: []), mock.patch.object(self.module, "remove_test_bucket", cleanup):
            return self.run_main(), cleanup

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if "scripts.infra.terraform_wrapper" in args and "plan" in args and self.create in ("verified", "keyless"):
            return done(1)
        return super().answer(program, args, **options)

    def test_a_create_that_timed_out_keeps_its_binding_and_says_the_bucket_may_remain(self) -> None:
        code, cleanup = self.run_with("timeout")
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        cleanup.assert_not_called()

    def patched_run(self) -> int:
        with mock.patch.object(self.module, "expected_creates", lambda environment: []):
            return self.run_main()

    def test_a_create_whose_own_call_timed_out_keeps_the_binding_although_s3_then_reports_no_such_bucket(self) -> None:
        self.assert_attempted_create_kept()

    def test_a_binding_that_names_the_project_s_bucket_is_kept_and_nothing_is_removed(self) -> None:
        code, cleanup = self.run_with("project")
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        cleanup.assert_not_called()

    def test_a_create_that_finished_without_writing_a_binding_created_no_bucket(self) -> None:
        code, cleanup = self.run_with("none")
        self.assertEqual(1, code)
        self.assert_nothing_created()
        cleanup.assert_not_called()

    def test_a_binding_without_state_keys_is_kept_and_the_bucket_is_not_touched_because_its_own_entries_cannot_be_told_from_others(self) -> None:
        self.create = "keyless"
        with mock.patch.object(self.module, "expected_creates", lambda environment: []):
            code = self.run_main()
        self.assertEqual(1, code)
        self.assert_kept_with_resources_that_may_remain()
        self.assertEqual([], [call for call in self.aws.call.mock_calls if "delete" in str(call)])

    def test_the_binding_goes_only_when_the_cleanup_confirmed_the_bucket_gone(self) -> None:
        for gone in (False, True):
            with self.subTest(gone=gone):
                for path in self.bindings():
                    path.unlink()
                for folder in [path for path in (self.root / "runs").iterdir() if path.is_dir()]:
                    shutil.rmtree(folder)
                self.run_with("verified", ("the cleanup said so", gone))
                self.assertEqual(not gone, bool(self.bindings()))
                self.assertIn("the cleanup said so", self.cleanup_text())
                if not gone:
                    self.assertIn(MAY_REMAIN, self.cleanup_text())
                    self.assertIn(self.bindings()[0].name, self.cleanup_text())


class EnvironmentRefusalTest(base.RunnerCase):
    name = "environment_plan"
    seen: dict[str, Any] = {}

    def saved(self) -> dict[str, Any]:
        (record,) = (self.root / "runs").glob("*/record.json")
        loaded: dict[str, Any] = json.loads(record.read_text())
        return loaded

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if "scripts.infra.state_bootstrap" in args:
            Path(args[args.index("--binding") + 1]).write_text(json.dumps(verified_binding()))
            return done()
        if "plan" in args:
            plans = self.root / "runs" / "run_abcdef" / "plans"
            plans.mkdir(parents=True)
            (plans / "apply.json").write_text(json.dumps({"resource_changes": self.changes()}))
            return done(0, "next: apply --confirm 123456789012")
        if "000000000000" in args:
            return done(1, "refused before any change: the confirmation code")
        self.seen = json.loads(Path(args[args.index("--binding") + 1]).read_text())
        raise TimeoutExpired(program, options["timeout"])

    def changes(self) -> list[dict[str, Any]]:
        resources = [f"arn:aws:s3:::{TEST_BUCKET}/{prefix}/*" for prefix in ("landing", "warehouse")]
        storage = {"Statement": [{"Sid": "ReadAndWriteProjectObjects", "Resource": resources}]}
        host = {
            "instance_type": "m6a.2xlarge",
            "ami": "ami-1",
            "tags": {"Name": "n"},
            "user_data": "--client-id= --audience= --advertise-tags='tag:surgical-host'",
            "root_block_device": [{"volume_type": "gp3", "volume_size": 200, "iops": 3000, "throughput": 125, "encrypted": True}],
        }
        shaped = {
            "module.host[0].aws_instance.host": ("aws_instance", host),
            "module.host[0].aws_security_group.host": ("aws_security_group", {"ingress": [], "tags": {"Name": "n"}}),
            "module.host[0].aws_iam_role_policy.host_storage": ("aws_iam_role_policy", {"policy": json.dumps(storage)}),
        }
        return [{"address": address, "type": kind, "change": {"actions": ["no-op"], "after": after}} for address, (kind, after) in shaped.items()]

    def test_the_wrong_state_location_refusal_runs_on_a_binding_of_its_own_and_a_timeout_keeps_the_genuine_binding_unaltered(self) -> None:
        with mock.patch.object(self.module, "expected_creates", lambda environment: []), mock.patch.object(self.module, "remove_test_bucket", mock.Mock()):
            code = self.run_main()
        self.assertEqual(1, code)
        (genuine,) = [path for path in (self.root / "runs").glob("binding_environment_plan_*.json") if "refusal" not in path.name]
        self.assertEqual(verified_binding(), json.loads(genuine.read_text()))
        used = [args[args.index("--binding") + 1] for _, args, _ in self.launched if "--binding" in args]
        self.assertEqual(str(genuine), used[0])
        self.assertNotEqual(str(genuine), used[-1], "the refusal's apply names a binding file of its own")
        self.assertEqual(verified_binding() | {"workspace": "other_workspace"}, self.seen, "the refusal's own binding holds the other workspace")
        self.assertTrue(any("scripts.infra.terraform_wrapper apply timed out after" in failure for failure in self.failed()), self.failed())
        self.assertEqual([], list((self.root / "runs").glob("binding_environment_plan_refusal_*.json")), "the altered copy is not kept")


class TeardownBindingTest(base.RunnerCase):
    name = "teardown_fixtures"

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        raise TimeoutExpired(program, options["timeout"])

    def fixture(self, backend: str = "local") -> Any:
        fixture = self.module.Fixture(backend, backend, run_record.Record(feature="f", slug="s", rerun="r"), [], dict(CONFIGURATION))
        self.addCleanup(shutil.rmtree, fixture.scratch, ignore_errors=True)
        return fixture

    def build_with(self, failing: str, backend: str = "local") -> tuple[Any, mock.Mock, Path]:
        """Build a fixture whose steps answer as stand-ins and whose step ``failing`` (plan, code or apply) fails. Return it with the stand-in of the
        empty-bucket cleanup, which the build may call, and the binding path."""
        fixture = self.fixture(backend)

        def must(module: str, *arguments: str) -> str:
            if arguments[:1] == ("create",):
                fixture.binding_path.write_text(json.dumps(verified_binding() | {"bucket": TEST_BUCKET, "backend": backend}))
                return ""
            if arguments[:1] == ("plan",):
                if failing == "plan":
                    raise RuntimeError("the plan failed")
                return "" if failing == "code" else "to apply: apply --confirm 123456789012"
            if failing == "apply":
                raise RuntimeError("the apply failed")
            return ""

        cleanup = mock.Mock()
        with (
            mock.patch.object(fixture, "must", must),
            mock.patch.object(
                fixture, "tool", lambda module, *arguments: (1, "the run is bound to a short-lived test bucket, and the permanent bucket was asked for")
            ),
            mock.patch.object(fixture, "remove_unused_bucket", cleanup),
            mock.patch.object(fixture, "plant", mock.Mock()),
            self.assertRaises((RuntimeError, StopIteration)),
        ):
            fixture.build()
        return fixture, cleanup, fixture.binding_path

    def test_a_failure_before_the_apply_starts_may_remove_the_unused_bucket(self) -> None:
        for failing in ("plan", "code"):
            with self.subTest(failing=failing):
                _, cleanup, _ = self.build_with(failing)
                cleanup.assert_called_once()

    def test_a_failure_of_the_apply_removes_no_bucket_because_a_role_or_a_state_may_exist_that_only_the_binding_names(self) -> None:
        for backend in ("local", "s3"):
            with self.subTest(backend=backend):
                fixture, cleanup, binding = self.build_with("apply", backend)
                cleanup.assert_not_called()
                self.assertFalse(fixture.settled)
                self.assertTrue(binding.is_file())

    def test_a_fixture_that_failed_its_apply_keeps_its_binding_and_its_record_says_the_resources_may_remain(self) -> None:
        record = run_record.Record(feature="f", slug="s", rerun="r")
        fixture = self.fixture()
        fixture.binding_path.write_text(json.dumps(verified_binding()))
        cleanup = mock.Mock()
        with (
            mock.patch.object(self.module.Fixture, "build", mock.Mock(side_effect=RuntimeError("the apply failed"))),
            mock.patch.object(self.module.Fixture, "remove_unused_bucket", cleanup),
        ):
            self.module.run_fixture("local", "local", record, [], dict(CONFIGURATION))
        cleanup.assert_not_called()
        self.assertTrue(fixture.binding_path.is_file())
        self.assertIn(MAY_REMAIN, record.observations[-1].observed)

    def run_fixtures(self, *names: str, settled: tuple[str, ...] = ()) -> str:
        """Run the named fixtures with a build that fails and a passing control that settles only the fixtures in ``settled``. Each fixture's binding is
        written first, as its create step does. Return the cleanup text of the saved record."""

        def passing(fixture: Any) -> None:
            fixture.settled = fixture.name in settled

        def build(fixture: Any) -> None:
            fixture.binding_path.write_text(json.dumps(verified_binding()))

        with (
            mock.patch.object(self.module.Fixture, "build", build),
            mock.patch.object(self.module, "receipt_cases", lambda fixture: None),
            mock.patch.object(self.module, "inventory_cases", lambda fixture: None),
            mock.patch.object(self.module, "state_cases", lambda fixture: None),
            mock.patch.object(self.module, "lock_case", lambda fixture: None),
            mock.patch.object(self.module.Fixture, "passing_control", passing),
        ):
            self.run_main(*names)
        return self.cleanup_of_the_record()

    def cleanup_of_the_record(self) -> str:
        text: str = self.saved()["cleanup"]
        return text

    def test_the_summary_says_a_fixture_removed_its_resources_only_when_it_settled_and_names_the_binding_of_each_that_did_not(self) -> None:
        text = self.run_fixtures("local", "s3", settled=("local",))
        self.assertIn("binding_teardown_s3_abcd.json", text)
        self.assertNotIn("binding_teardown_local_abcd.json", text)
        self.assertIn(MAY_REMAIN, text)
        self.assertNotIn("each fixture that reached its passing control removed its own resources", text)
        self.assertTrue((self.root / "runs" / "binding_teardown_s3_abcd.json").is_file())
        self.assertFalse((self.root / "runs" / "binding_teardown_local_abcd.json").is_file())

    def test_the_summary_of_fixtures_that_all_settled_says_so_and_names_no_binding_that_remains(self) -> None:
        text = self.run_fixtures("local", "s3", settled=("local", "s3"))
        self.assertNotIn(MAY_REMAIN, text)
        self.assertNotIn(".json", text)
        self.assertIn("local", text)
        self.assertIn("s3", text)

    def test_the_summary_of_a_fixture_that_wrote_no_binding_says_so_and_does_not_claim_a_removal(self) -> None:
        with mock.patch.object(self.module.Fixture, "build", mock.Mock(side_effect=RuntimeError("the create failed"))):
            self.run_main("local")
        text = self.cleanup_of_the_record()
        self.assertIn("no binding was written", text)
        self.assertNotIn("removed", text)

    def test_the_summary_of_the_live_run_names_its_binding_and_says_nothing_was_removed(self) -> None:
        (self.root / "runs" / "run_binding.json").write_text("{}")
        self.run_main("live")
        text = self.cleanup_of_the_record()
        self.assertIn("run_binding.json", text)
        self.assertNotIn("each fixture", text)

    def aws_for_unused_bucket(self, *, gone: bool) -> None:
        heads: Any = iter([{}, *([None] if gone else [])])

        def answered(*arguments: str, **options: Any) -> Any:
            if arguments[1] == "list-object-versions":
                return {"Versions": [{"Key": "state/dev/terraform.tfstate.tflock", "VersionId": "1"}]}
            return next(heads, {}) if arguments[1] == "head-bucket" else {}

        self.aws.call.side_effect = answered

    def remove_unused(self, fixture: Any) -> None:
        with mock.patch.object(self.module, "GONE_DEADLINE_SECONDS", 0.2), mock.patch.object(self.module.time, "sleep", lambda seconds: None):
            fixture.remove_unused_bucket()

    def test_an_unused_bucket_of_a_local_state_fixture_is_kept_when_a_local_state_file_exists(self) -> None:
        for written in (True, False):
            with self.subTest(written=written):
                self.aws.reset_mock()
                fixture = self.fixture()
                state = "state/dev/terraform.tfstate"
                fixture.binding_path.write_text(json.dumps(verified_binding() | {"backend": "local", "state_keys": [state, f"{state}.tflock"]}))
                (self.root / "state" / "dev").mkdir(parents=True, exist_ok=True)
                path = self.root / state
                path.unlink(missing_ok=True)
                if written:
                    path.write_text("{}")
                self.aws_for_unused_bucket(gone=True)
                self.remove_unused(fixture)
                self.assertEqual(not written, fixture.settled)
                deleted = [call for call in self.aws.call.mock_calls if "delete-bucket" in str(call)]
                self.assertEqual(0 if written else 1, len(deleted))
                if written:
                    self.assertIn("state file", fixture.record.observations[-1].observed)

    def test_a_file_at_the_state_key_s_path_is_no_local_state_of_an_s3_fixture_and_a_bucket_that_s3_reports_missing_is_settled(self) -> None:
        fixture = self.fixture("s3")
        state = "state/dev/terraform.tfstate"
        fixture.binding_path.write_text(json.dumps(verified_binding() | {"state_keys": [state, f"{state}.tflock"]}))
        (self.root / "state" / "dev").mkdir(parents=True)
        (self.root / state).write_text("{}")
        self.aws_for_unused_bucket(gone=True)
        self.remove_unused(fixture)
        self.assertTrue(fixture.settled)
        self.aws.call.side_effect = lambda *arguments, **options: None if arguments[1] == "head-bucket" else {}
        fixture.settled = False
        self.remove_unused(fixture)
        self.assertTrue(fixture.settled, "S3 reports no such bucket, which is confirmed gone")

    def test_the_outer_deadline_of_a_destroy_counts_the_versions_that_the_bucket_holds(self) -> None:
        fixture = self.fixture()
        fixture.binding_path.write_text(json.dumps(verified_binding()))
        with (
            mock.patch.object(fixture, "entries", lambda bucket: [("key", f"v{number}") for number in range(5)]),
            mock.patch.object(run_record, "run_command", self.launch),
            mock.patch.object(self, "answer", lambda *a, **k: done()),
        ):
            fixture.tool("scripts.infra.teardown", "destroy", "--confirm", "x")
            fixture.tool("scripts.infra.teardown", "plan")
        self.assertEqual(run_record.command_deadline("scripts.infra.teardown", ["destroy"], 1800, versions=5), self.launched[0][2])
        self.assertEqual(run_record.command_deadline("scripts.infra.teardown", ["plan"], 1800), self.launched[1][2])

    def test_the_wrong_state_location_refusal_runs_on_a_binding_of_its_own_and_leaves_the_genuine_binding_unaltered_also_after_a_timeout(self) -> None:
        for backend in ("s3", "local"):
            with self.subTest(backend=backend):
                fixture = self.fixture(backend)
                state = f"{self.root}/local_state.tfstate" if backend == "local" else "state/dev/terraform.tfstate"
                genuine = verified_binding() | {"backend": backend, "state_keys": [state, f"{state}.tflock"]}
                fixture.binding_path.write_text(json.dumps(genuine))
                if backend == "local":
                    Path(state).write_text("{}")
                before = fixture.binding_path.read_text()
                with mock.patch.object(run_record, "run_command", self.launch), self.assertRaises(StepTimedOut):
                    self.module.state_location_case(fixture)
                self.assertEqual(before, fixture.binding_path.read_text())
                used = self.launched[-1][1][self.launched[-1][1].index("--binding") + 1]
                self.assertNotEqual(str(fixture.binding_path), used)
                self.assertEqual([], list((self.root / "runs").glob("binding_teardown_*refusal*.json")), "the altered copy is not kept")
                if backend == "local":
                    self.assertEqual([], list(self.root.glob("local_state.tfstate.moved")), "the copy of the state file is not kept")
                self.launched.clear()


if __name__ == "__main__":
    unittest.main()
