"""The outer deadline of an end-to-end step covers the steps that the step itself bounds, and a timeout is a failed step in the saved record.

Failure modes covered here:

- A runner's outer deadline for a Terraform wrapper or teardown command is shorter than the sum of that command's own bounded steps (the init, the plans
  and shows, the apply, the waits), so the runner stops a command that is still inside its own deadline.
- The sums drift from the commands: a step is added to a command and the outer deadline does not grow.
- A timed-out step names its arguments, which can hold private names, or is not told apart from another failure.
- A timeout leaves a runner without a record, removes the binding that the fixture's bucket needs for its cleanup or removes the bucket while a nested child
  may still run.

The deadline table and the timed command are in the tracked ``tests/e2e/run_record.py`` and run everywhere. The runners of the cloud checks stay untracked;
the tests of their handling of a timeout run only where a runner is on disk, with the cloud replaced by stand-ins, and are skipped elsewhere.
"""

from __future__ import annotations

import ast
import importlib
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.aws_cli import AWS_TIMEOUT_SECONDS
from scripts.infra import teardown, terraform_wrapper
from scripts.infra.host import RELEASE_DEADLINE_SECONDS
from scripts.infra.terraform_wrapper import APPLY_TIMEOUT_SECONDS, INIT_TIMEOUT_SECONDS, PLAN_TIMEOUT_SECONDS
from scripts.process import TimeoutExpired
from tests.e2e import run_record
from tests.e2e.run_record import StepTimedOut, command_deadline, timed_command

REPOSITORY = Path(__file__).resolve().parents[2]
MARGIN = 10 * AWS_TIMEOUT_SECONDS
DEFAULT = 300
TERRAFORM_CALLS = {
    ("scripts/infra/terraform_wrapper.py", "initialise"): 2,
    ("scripts/infra/terraform_wrapper.py", "save_plan"): 3,
    ("scripts/infra/terraform_wrapper.py", "apply_plan"): 2,
    ("scripts/infra/terraform_wrapper.py", "record_created"): 1,
    ("scripts/infra/terraform_wrapper.py", "state_json"): 1,
    ("scripts/infra/teardown.py", "state_identity"): 1,
    ("scripts/infra/teardown.py", "plan_teardown"): 0,
    ("scripts/infra/teardown.py", "guard_problems"): 1,
    ("scripts/infra/teardown.py", "apply_destroy"): 1,
}


def calls_of(path: str, function: str, name: str) -> int:
    tree = ast.parse((REPOSITORY / path).read_text())
    (node,) = (item for item in ast.walk(tree) if isinstance(item, ast.FunctionDef) and item.name == function)
    return sum(1 for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == name)


class DeadlineTableTest(unittest.TestCase):
    def test_each_command_that_runs_terraform_gets_more_than_the_sum_of_its_own_bounded_steps(self) -> None:
        sums = {
            ("scripts.infra.terraform_wrapper", "plan"): INIT_TIMEOUT_SECONDS + 4 * PLAN_TIMEOUT_SECONDS,
            ("scripts.infra.terraform_wrapper", "apply"): 2 * PLAN_TIMEOUT_SECONDS + APPLY_TIMEOUT_SECONDS,
            ("scripts.infra.inventory", ""): INIT_TIMEOUT_SECONDS + 2 * PLAN_TIMEOUT_SECONDS,
            ("scripts.infra.teardown", "plan"): INIT_TIMEOUT_SECONDS + 6 * PLAN_TIMEOUT_SECONDS,
            ("scripts.infra.teardown", "destroy"): 2 * PLAN_TIMEOUT_SECONDS
            + APPLY_TIMEOUT_SECONDS
            + RELEASE_DEADLINE_SECONDS
            + 2 * teardown.GONE_DEADLINE_SECONDS,
        }
        for (module, command), steps in sums.items():
            with self.subTest(module=module, command=command):
                arguments = [command] if command else []
                self.assertEqual(steps + MARGIN, command_deadline(module, arguments, DEFAULT))
                self.assertGreater(command_deadline(module, [*arguments, *(["--confirm", "x"] if arguments else [])], DEFAULT), steps)

    def test_a_command_without_a_terraform_step_keeps_the_deadline_the_runner_gives_it(self) -> None:
        for module, arguments in (("scripts.evidence.export", []), ("scripts.infra.state_bootstrap", ["create"]), ("scripts.infra.teardown", ["other"])):
            with self.subTest(module=module):
                self.assertEqual(DEFAULT, command_deadline(module, arguments, DEFAULT))

    def test_the_apply_step_alone_fits_inside_every_outer_deadline_of_a_command_that_applies(self) -> None:
        for module, command in (("scripts.infra.terraform_wrapper", "apply"), ("scripts.infra.teardown", "destroy")):
            self.assertGreater(command_deadline(module, [command], DEFAULT), APPLY_TIMEOUT_SECONDS + PLAN_TIMEOUT_SECONDS)

    def test_the_counted_terraform_steps_are_the_terraform_steps_that_the_commands_hold(self) -> None:
        for (path, function), count in TERRAFORM_CALLS.items():
            with self.subTest(function=function):
                self.assertEqual(count, calls_of(path, function, "terraform"))
        self.assertEqual(1, calls_of("scripts/infra/terraform_wrapper.py", "save_plan", "initialise"))
        self.assertEqual(1, calls_of("scripts/infra/teardown.py", "plan_teardown", "save_plan"))
        self.assertEqual(2, calls_of("scripts/infra/teardown.py", "plan_teardown", "state_identity"))
        self.assertEqual(1, calls_of("scripts/infra/teardown.py", "guard_problems", "state_identity"))
        self.assertEqual(1, calls_of("scripts/infra/inventory.py", "write_inventory", "state_json"))
        self.assertTrue(callable(terraform_wrapper.terraform))


class TimedCommandTest(unittest.TestCase):
    def test_a_timeout_is_a_step_timeout_that_names_the_step_and_never_its_arguments(self) -> None:
        with mock.patch.object(run_record, "run_command", side_effect=TimeoutExpired("python", 5)), self.assertRaises(StepTimedOut) as caught:
            timed_command("scripts.ingestion.cli publish", "python", ["-m", "a_private_name"], timeout=5)
        self.assertEqual("scripts.ingestion.cli publish timed out after 5 seconds", str(caught.exception))
        self.assertNotIn("a_private_name", str(caught.exception))
        self.assertNotIsInstance(caught.exception, RuntimeError)

    def test_a_command_that_finishes_returns_its_result_with_the_arguments_and_the_deadline_it_was_given(self) -> None:
        done = subprocess.CompletedProcess(["x"], 3, "out", "err")
        with mock.patch.object(run_record, "run_command", return_value=done) as launched:
            self.assertIs(done, timed_command("step", "python", ["-m", "x"], timeout=7, cwd=Path("here")))
        launched.assert_called_once_with("python", ["-m", "x"], timeout=7, cwd=Path("here"))


def runner(name: str) -> ModuleType | None:
    return importlib.import_module(f"tests.e2e.{name}") if (REPOSITORY / "tests" / "e2e" / f"{name}.py").is_file() else None


CONFIGURATION = {
    "AWS_PROFILE": "no-such-profile",
    "AWS_REGION": "us-west-2",
    "AWS_ACCOUNT_ID": "<AWS_ACCOUNT_ID>",
    "S3_BUCKET": "example-bucket",
    "PROJECT_NAME": "example",
    "ENVIRONMENT": "dev",
}


class RunnerCase(unittest.TestCase):
    """Run an untracked runner's ``main`` with a temporary root, the cloud replaced by a stand-in and the launcher replaced by a function that answers each
    command. Without the runner on disk every test is skipped."""

    name = ""

    def setUp(self) -> None:
        module = runner(self.name)
        if module is None:
            self.skipTest(f"tests/e2e/{self.name}.py is untracked and not on this disk")
        self.module: Any = module
        scratch = tempfile.TemporaryDirectory(prefix="runner_")
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        (self.root / "runs").mkdir()
        self.output = io.StringIO()
        self.aws = mock.MagicMock()
        self.launched: list[tuple[str, list[str], float]] = []
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.root),
            mock.patch.object(self.module, "ROOT", self.root, create=True),
            mock.patch.object(self.module, "run_command", self.launch, create=True),
            mock.patch.object(self.module, "load_configuration", lambda: dict(CONFIGURATION)),
            mock.patch.object(self.module, "Aws", lambda profile, region: self.aws),
            mock.patch.object(self.module.secrets, "token_hex", lambda size: "abcd"),
            mock.patch.object(sys, "stdout", self.output),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        raise NotImplementedError

    def launch(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        self.launched.append((program, list(args), options["timeout"]))
        return self.answer(program, args, **options)

    def run_main(self, *arguments: str) -> int:
        with mock.patch.object(run_record, "run_command", self.launch), mock.patch.object(sys, "argv", ["runner", *arguments]):
            code: int = self.module.main()
            return code

    def saved(self) -> dict[str, Any]:
        (folder,) = (path for path in (self.root / "runs").iterdir() if path.is_dir())
        record: dict[str, Any] = json.loads((folder / "record.json").read_text())
        return record

    def failed(self) -> list[str]:
        return [f"{item['case']}: {item['observed']}" for item in self.saved()["observations"] if not item["passed"]]


def done(code: int = 0, text: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["x"], code, text, "")


class TeardownRunnerTest(RunnerCase):
    name = "teardown_fixtures"

    def binding(self) -> Path:
        path = self.root / "runs" / "binding_teardown_local_abcd.json"
        path.write_text(json.dumps({"bucket": "example-bucket-test-1", "run_id": "r"}))
        return path

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        raise TimeoutExpired(program, options["timeout"])

    def test_a_timeout_saves_a_failure_record_that_names_the_step_and_keeps_the_binding_and_the_bucket(self) -> None:
        binding = self.binding()
        code = self.run_main("local")
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("scripts.infra.state_bootstrap create timed out after", failure)
        self.assertIn(binding.name, failure)
        self.assertTrue(binding.is_file(), "the binding that names the bucket stays for the cleanup by hand")
        self.assertEqual([], self.aws.call.mock_calls, "no bucket is removed while a nested child may still run")

    def test_a_fixture_that_stopped_before_its_own_cleanup_keeps_its_binding_and_one_that_finished_it_removes_it(self) -> None:
        for settled in (False, True):
            with self.subTest(settled=settled):
                binding = self.binding()
                fixture_class = self.module.Fixture

                def passing(fixture: Any, *, settled: bool = settled) -> None:
                    fixture.settled = settled

                with (
                    mock.patch.object(fixture_class, "build", lambda fixture: None),
                    mock.patch.object(self.module, "receipt_cases", lambda fixture: None),
                    mock.patch.object(self.module, "inventory_cases", lambda fixture: None),
                    mock.patch.object(fixture_class, "passing_control", passing),
                ):
                    self.module.run_fixture("local", "local", run_record.Record(feature="f", slug="s", rerun="r"), [], dict(CONFIGURATION))
                self.assertEqual(not settled, binding.is_file())
                binding.unlink(missing_ok=True)

    def test_a_failed_step_that_is_not_a_timeout_keeps_the_binding_too(self) -> None:
        binding = self.binding()
        record = run_record.Record(feature="f", slug="s", rerun="r")
        with mock.patch.object(self.module.Fixture, "build", mock.Mock(side_effect=RuntimeError("a step failed"))):
            self.module.run_fixture("local", "local", record, [], dict(CONFIGURATION))
        self.assertTrue(binding.is_file())
        self.assertIn(binding.name, record.observations[-1].observed)

    def test_a_failed_build_settles_its_fixture_only_when_s3_reports_the_unused_bucket_gone(self) -> None:
        for reported_gone in (False, True):
            with self.subTest(reported_gone=reported_gone):
                binding = self.root / "runs" / "binding_teardown_local_abcd.json"
                binding.write_text(json.dumps({"bucket": "example-bucket-test-1", "run_id": "r", "state_keys": ["state", "state.tflock"]}))
                heads: Any = iter([{}, *([None] if reported_gone else [])])

                def answered(*arguments: str, heads: Any = heads, **options: Any) -> Any:
                    if arguments[1] == "list-object-versions":
                        return {"Versions": [{"Key": "state.tflock", "VersionId": "1"}]}
                    return next(heads, {}) if arguments[1] == "head-bucket" else {}

                self.aws.call.side_effect = answered
                fixture = self.module.Fixture("local", "local", run_record.Record(feature="f", slug="s", rerun="r"), [], dict(CONFIGURATION))
                self.addCleanup(shutil.rmtree, fixture.scratch, ignore_errors=True)
                with mock.patch.object(self.module, "GONE_DEADLINE_SECONDS", 0.2), mock.patch.object(self.module.time, "sleep", lambda seconds: None):
                    fixture.remove_unused_bucket()
                self.assertEqual(reported_gone, fixture.settled)

    def test_a_timeout_of_the_live_run_s_own_case_saves_a_failure_record_and_removes_no_binding(self) -> None:
        live = self.root / "runs" / "run_binding.json"
        live.write_text("{}")
        code = self.run_main("live")
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("scripts.infra.inventory timed out after", failure)
        self.assertTrue(live.is_file())

    def test_the_outer_deadline_of_the_wrapper_and_teardown_commands_covers_their_own_steps(self) -> None:
        self.binding()
        fixture = self.module.Fixture("local", "local", run_record.Record(feature="f", slug="s", rerun="r"), [], dict(CONFIGURATION))
        self.addCleanup(shutil.rmtree, fixture.scratch, ignore_errors=True)
        with mock.patch.object(run_record, "run_command", self.launch), mock.patch.object(self, "answer", lambda *a, **k: done()):
            for module, command in (
                ("scripts.infra.terraform_wrapper", "apply"),
                ("scripts.infra.terraform_wrapper", "plan"),
                ("scripts.infra.teardown", "destroy"),
            ):
                fixture.tool(module, command, "--confirm", "x")
        timeouts = [timeout for _, _, timeout in self.launched]
        self.assertGreater(timeouts[0], 2 * PLAN_TIMEOUT_SECONDS + APPLY_TIMEOUT_SECONDS)
        self.assertGreater(timeouts[1], INIT_TIMEOUT_SECONDS + 4 * PLAN_TIMEOUT_SECONDS)
        self.assertGreater(timeouts[2], 2 * PLAN_TIMEOUT_SECONDS + APPLY_TIMEOUT_SECONDS + 2 * teardown.GONE_DEADLINE_SECONDS)


class IngestionRunnerTest(RunnerCase):
    name = "ingestion_check"

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if program == "aws" or ("scripts.ingestion.cli" in args and "publish" in args):
            raise TimeoutExpired(program, options["timeout"])
        if "scripts.infra.state_bootstrap" in args:
            Path(args[args.index("--binding") + 1]).write_text(json.dumps({"bucket": "example-bucket-test-1", "status": "verified"}))
        return done()

    def test_a_publish_timeout_is_a_failed_step_in_the_saved_record_and_the_cleanup_still_runs(self) -> None:
        version = self.module.ObjectVersion("landing/k", "v1", 1, "2026-01-01T00:00:00Z", True, False)
        cleanup = mock.Mock(return_value="removed")
        with (
            mock.patch.object(self.module, "read_sample", lambda scratch: ({"a/1.vital": b"x", "a/2.vital": b"y"}, {"a/3.vital": b"z"})),
            mock.patch.object(self.module, "put_version", lambda *arguments: version),
            mock.patch.object(self.module, "S3Store", mock.MagicMock()),
            mock.patch.object(self.module, "remove_test_bucket", cleanup),
        ):
            code = self.run_main()
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("scripts.ingestion.cli publish timed out after", failure)
        cleanup.assert_called_once()
        self.assertEqual("removed", self.saved()["cleanup"])

    def test_a_timeout_of_the_read_of_the_sample_is_a_failed_step_and_no_bucket_is_touched(self) -> None:
        with mock.patch.object(self.module, "remove_test_bucket", mock.Mock(side_effect=AssertionError("no bucket was created"))):
            code = self.run_main()
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("aws s3 cp timed out after", failure)
        self.assertEqual("no bucket was created", self.saved()["cleanup"])


class StateBootstrapRunnerTest(RunnerCase):
    name = "state_bootstrap"

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if "scripts.infra.state_bootstrap" in args:
            state = f"state/{CONFIGURATION['ENVIRONMENT']}/terraform.tfstate"
            binding = {
                "bucket": "example-bucket-test-abcdef",
                "run_id": "run_abcdef",
                "status": "verified",
                "backend": "s3",
                "workspace": "default",
                "state_keys": [state, f"{state}.tflock"],
                "bucket_creation": "created",
            }
            Path(args[args.index("--binding") + 1]).write_text(json.dumps(binding))
            return done()
        raise TimeoutExpired(program, options["timeout"])

    def run_to_the_init(self, pinned: Any) -> int:
        first = {
            "blocks": self.module.BLOCKS,
            "versioning": "Enabled",
            "tags": {"Project": CONFIGURATION["PROJECT_NAME"], "Environment": CONFIGURATION["ENVIRONMENT"]},
            "entries": 0,
        }
        with (
            mock.patch.object(self.module, "readback", lambda aws, bucket: first),
            mock.patch.object(self.module, "pinned_terraform", pinned),
            mock.patch.object(self.module, "remove_test_bucket", lambda aws, bucket, record: "removed"),
        ):
            return self.run_main()

    def test_a_missing_pinned_terraform_is_a_failed_step_in_the_saved_record(self) -> None:
        code = self.run_to_the_init(mock.Mock(side_effect=terraform_wrapper.WrapperError("the pinned Terraform is missing")))
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("pinned Terraform", failure)

    def test_the_init_is_recorded_with_the_pinned_path_and_its_timeout_is_a_failed_step(self) -> None:
        pinned = self.root / ".tools" / "bin" / "terraform"
        code = self.run_to_the_init(lambda: pinned)
        self.assertEqual(1, code)
        commands = self.saved()["commands"]
        self.assertTrue(any(command.startswith(".tools/bin/terraform ") for command in commands), commands)
        self.assertFalse(any(command.startswith("terraform ") for command in commands), commands)
        (failure,) = self.failed()
        self.assertIn("terraform init timed out after", failure)
        self.assertEqual(INIT_TIMEOUT_SECONDS, self.launched[-1][2])


class EnvironmentPlanRunnerTest(RunnerCase):
    name = "environment_plan"

    def answer(self, program: str, args: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if "scripts.infra.state_bootstrap" in args:
            Path(args[args.index("--binding") + 1]).write_text(
                json.dumps({"bucket": "example-bucket-test-1", "status": "verified", "run_id": "r", "state_keys": ["a", "b"]})
            )
            return done()
        raise TimeoutExpired(program, options["timeout"])

    def test_a_plan_timeout_is_a_failed_step_that_keeps_the_binding_and_removes_no_bucket(self) -> None:
        cleanup = mock.Mock(return_value="removed")
        with mock.patch.object(self.module, "expected_creates", lambda environment: []), mock.patch.object(self.module, "remove_test_bucket", cleanup):
            code = self.run_main()
        self.assertEqual(1, code)
        (failure,) = self.failed()
        self.assertIn("scripts.infra.terraform_wrapper plan timed out after", failure)
        cleanup.assert_not_called()
        self.assertEqual(1, len(list((self.root / "runs").glob("binding_environment_plan_*.json"))))
        self.assertGreater(self.launched[-1][2], INIT_TIMEOUT_SECONDS + 4 * PLAN_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
