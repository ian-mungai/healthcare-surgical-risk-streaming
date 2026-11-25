"""The storage command plans the storage root, stops for review, applies exactly the reviewed plan and then asks AIStor for the bucket's versioning.

Failure modes covered here: a credential in an argument, a plan file, a state file, a failure file or a message; a plan that differs from the reviewed one,
a wrong code or a plan for another endpoint or bucket; a second run that changes something; a versioning that is not Enabled; a host that gives no
credential or one of another form; a Terraform that is not the pinned one. Terraform, SSH and the AWS command line are stand-ins:
no cloud, no host and no Docker is reached.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infra import storage, terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import WrapperError
from scripts.process import CompletedProcess, TimeoutExpired
from scripts.project_config import ROOT as REAL_ROOT
from scripts.project_config import ConfigurationError
from tests.fast.ansible_paths import group_variables

FIRST_VALUE = "example-first-0123456789abcdef"
SECOND_VALUE = "example-second-0123456789abcdef"
ADDRESS = "storage-host.example.ts.net"
CONFIGURATION = {"S3_BUCKET": "example-project", "ENVIRONMENT": "dev", "AWS_PROFILE": "example_project_dev", "AWS_REGION": "us-west-2"}
RUN = RunBinding("20261124t0000-aaaaaa", "c" * 40, "s3", "us-west-2", "example-project", "default", ["k", "k.tflock"], "verified")


def secrets_directory() -> str:
    """The secrets folder of the group variables with its one variable, the stack's folder, filled in."""
    variables = group_variables()
    return str(variables["compose_stack_secrets_directory"]).replace("{{ compose_stack_directory }}", str(variables["compose_stack_directory"]))


class Peer:
    address = ADDRESS


def plan_json(changes: list[str]) -> str:
    return json.dumps({"resource_changes": [{"address": name, "change": {"actions": ["create"] if name in changes else ["no-op"]}} for name in ("a.b", "c.d")]})


class Stand(unittest.TestCase):
    """A repository root of its own, a Terraform that records its calls, an SSH that answers with the credentials and an AWS command line."""

    changes: list[str]
    versioning = "Enabled"

    def setUp(self) -> None:
        self.folder = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.folder, ignore_errors=True))
        (self.folder / ".tools" / "bin").mkdir(parents=True)
        (self.folder / ".tools" / "bin" / "terraform").write_text("#!/bin/sh\n")
        (self.folder / ".tools" / "bin" / "terraform").chmod(0o700)
        self.source = self.folder / "infra" / "storage"
        self.source.mkdir(parents=True)
        for name in ("main.tf", "variables.tf", "outputs.tf", "versions.tf", ".terraform.lock.hcl"):
            (self.source / name).write_text(f"# {name}\n")
        (self.source / "tests").mkdir()
        (self.source / "tests" / "storage.tftest.hcl").write_text("# test\n")
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.changes = ["a.b"]
        self.state_text = "{}"
        self.aws_calls: list[tuple[list[str], dict[str, str]]] = []
        self.ssh_commands: list[str] = []
        self.credentials = {"root_user": FIRST_VALUE, "root_password": SECOND_VALUE}
        for patcher in (
            mock.patch.object(storage, "ROOT", self.folder),
            mock.patch.object(terraform_wrapper, "ROOT", self.folder),
            mock.patch.object(storage, "run_command", side_effect=self.run_command),
            mock.patch.object(storage, "host_peer", return_value=Peer()),
            mock.patch.object(storage, "ssh_output", side_effect=self.ssh_output),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.output = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))

    def ssh_output(self, run_id: str, address: str, command: str, timeout: float) -> str | None:
        self.ssh_commands.append(command)
        name = command.rsplit("/", 1)[-1]
        value = self.credentials.get(name)
        return None if value is None else value + "\n"

    def run_command(self, program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
        environment = dict(keywords.get("env") or {})
        if Path(program).name == "aws":
            self.aws_calls.append((arguments, environment))
            body = json.dumps({"Status": self.versioning} if self.versioning else {})
            return CompletedProcess([program], 0, body, "")
        action = next(argument for argument in arguments if not argument.startswith("-chdir"))
        self.calls.append((arguments, environment))
        stdout = ""
        if action == "plan":
            out = next(argument for argument in arguments if argument.startswith("-out=")).removeprefix("-out=")
            Path(out).write_bytes(b"plan-bytes:" + ",".join(self.changes).encode())
        elif action == "workspace":
            stdout = "default\n"
        elif action == "show" and "-json" in arguments and len(arguments) > 3:
            stdout = plan_json(self.changes)
        elif action == "show":
            stdout = "the plan text\n"
        elif action == "apply":
            stdout = "Apply complete! Resources: 2 added, 0 changed, 0 destroyed.\n"
            state = self.folder / "runs" / RUN.run_id / "storage" / "terraform.tfstate"
            state.parent.mkdir(parents=True, exist_ok=True)
            state.write_text(self.state_text)
        elif action == "init":
            record = (
                Path(next(argument for argument in arguments if argument.startswith("-chdir=")).removeprefix("-chdir=")) / ".terraform" / "terraform.tfstate"
            )
            path = next(argument for argument in arguments if argument.startswith("-backend-config=path=")).removeprefix("-backend-config=path=")
            record.parent.mkdir(parents=True, exist_ok=True)
            record.write_text(json.dumps({"backend": {"type": "local", "config": {"path": path}}}))
        return CompletedProcess([program], 0, stdout, "")

    def terraform_actions(self) -> list[str]:
        return [next(argument for argument in arguments if not argument.startswith("-chdir")) for arguments, _ in self.calls]

    def plan(self) -> int:
        return storage.save_plan(RUN, CONFIGURATION, ADDRESS, storage.read_credentials(RUN.run_id, ADDRESS))

    def printed(self) -> str:
        return self.output.getvalue()


class CredentialsTest(Stand):
    def test_the_two_files_are_read_over_ssh_with_sudo_from_the_secrets_folder_of_the_group_variables_and_nothing_is_printed(self) -> None:
        folder = secrets_directory()
        credentials = storage.read_credentials(RUN.run_id, ADDRESS)
        self.assertEqual((FIRST_VALUE, SECOND_VALUE), (credentials.user, credentials.password))
        self.assertEqual([f"sudo -n cat {folder}/aistor/root_user", f"sudo -n cat {folder}/aistor/root_password"], self.ssh_commands)
        self.assertEqual("", self.printed())
        self.assertNotIn(SECOND_VALUE, repr(credentials))

    def test_a_host_that_answers_nothing_or_a_value_of_another_form_is_refused_by_a_fixed_message_that_holds_no_value(self) -> None:
        changes: tuple[tuple[str, dict[str, str | None]], ...] = (
            ("no user", {"root_user": None}),
            ("no password", {"root_password": None}),
            ("empty", {"root_password": ""}),
            ("a space", {"root_password": "has a space-0123456789"}),
            ("too short", {"root_password": "short"}),
            ("a quote", {"root_password": 'quote"-0123456789abcdef'}),
            ("a newline inside", {"root_password": "line-0123456789\nbreak-0123456789"}),
        )
        for label, change in changes:
            with self.subTest(label):
                merged: dict[str, str | None] = {"root_user": FIRST_VALUE, "root_password": SECOND_VALUE, **change}
                self.credentials = {name: value for name, value in merged.items() if value is not None}
                with self.assertRaises(storage.StorageError) as caught:
                    storage.read_credentials(RUN.run_id, ADDRESS)
                for value in change.values():
                    if value:
                        self.assertNotIn(value, str(caught.exception))
                self.assertNotIn(SECOND_VALUE, str(caught.exception))
                self.assertIn("root", str(caught.exception))

    def test_the_form_check_takes_the_form_that_the_secret_file_module_writes(self) -> None:
        import secrets

        for _ in range(50):
            self.credentials = {"root_user": secrets.token_urlsafe(32), "root_password": secrets.token_urlsafe(32)}
            storage.read_credentials(RUN.run_id, ADDRESS)

    def test_the_secret_folder_is_the_one_the_deploy_creates(self) -> None:
        self.assertEqual(secrets_directory(), storage.SECRETS_DIRECTORY)


class EndpointTest(unittest.TestCase):
    def test_the_endpoint_is_the_host_name_and_the_port_of_the_aistor_route_of_the_gateway_list(self) -> None:
        self.assertEqual(f"{ADDRESS}:9449", storage.endpoint(ADDRESS))

    def test_the_route_list_without_the_storage_route_is_refused(self) -> None:
        with mock.patch.object(storage, "load_routes", return_value=[]), self.assertRaises(storage.StorageError):
            storage.endpoint(ADDRESS)


class PlanTest(Stand):
    def credentials_object(self) -> storage.Credentials:
        return storage.Credentials(FIRST_VALUE, SECOND_VALUE)

    def test_the_plan_is_saved_with_its_text_json_and_binding_printed_with_a_code_and_the_folder_holds_only_private_files(self) -> None:
        count = self.plan()
        folder = self.folder / "runs" / RUN.run_id / "storage" / "plans"
        self.assertEqual(1, count)
        self.assertEqual(
            ["init", "workspace", "plan", "show", "show"], [action for action in self.terraform_actions() if action in ("init", "workspace", "plan", "show")]
        )
        saved = folder / "apply.tfplan"
        code = hashlib.sha256(saved.read_bytes()).hexdigest()[:12]
        self.assertIn(f"apply --confirm {code}", self.printed())
        self.assertIn("create: a.b", self.printed())
        for name in ("apply.tfplan", "apply.txt", "apply.json", "apply_binding.json"):
            with self.subTest(name):
                self.assertEqual(0o600, stat.S_IMODE((folder / name).stat().st_mode))
        self.assertEqual(0o700, stat.S_IMODE(folder.stat().st_mode))

    def test_no_argument_and_no_file_holds_a_credential_and_the_environment_of_every_call_has_the_pair_and_no_terraform_variable(self) -> None:
        self.plan()
        for arguments, environment in self.calls:
            with self.subTest(action=arguments[1] if len(arguments) > 1 else arguments[0]):
                self.assertNotIn(SECOND_VALUE, " ".join(arguments))
                self.assertNotIn(FIRST_VALUE, " ".join(arguments))
                self.assertEqual((FIRST_VALUE, SECOND_VALUE), (environment["MINIO_USER"], environment["MINIO_PASSWORD"]))
                self.assertEqual([], [name for name in environment if name.startswith("TF_VAR")])
                self.assertEqual("1", environment["TF_IN_AUTOMATION"])
        for path in (self.folder / "runs").rglob("*"):
            if path.is_file():
                self.assertNotIn(SECOND_VALUE.encode(), path.read_bytes(), path.name)
                self.assertNotIn(FIRST_VALUE.encode(), path.read_bytes(), path.name)
        self.assertNotIn(SECOND_VALUE, self.printed())

    def test_the_run_gives_the_endpoint_and_the_bucket_as_variables_and_no_variable_file_is_read(self) -> None:
        self.plan()
        plan_call = next(arguments for arguments, _ in self.calls if "plan" in arguments)
        self.assertIn(f"-var=endpoint={ADDRESS}:9449", plan_call)
        self.assertIn("-var=bucket_name=example-project", plan_call)
        self.assertEqual([], [argument for argument in plan_call if argument.startswith("-var-file")])

    def test_the_configuration_copy_holds_the_root_files_and_the_lock_and_no_test_folder_and_the_state_is_local_in_the_runs_folder(self) -> None:
        self.plan()
        copy = self.folder / ".tools" / "terraform" / RUN.run_id / "storage"
        self.assertEqual(
            [".terraform.lock.hcl", "main.tf", "outputs.tf", "variables.tf", "versions.tf"],
            sorted(path.name for path in copy.iterdir() if path.name != ".terraform"),
        )
        init = next(arguments for arguments, _ in self.calls if "init" in arguments)
        state = self.folder / "runs" / RUN.run_id / "storage" / "terraform.tfstate"
        self.assertIn(f"-backend-config=path={state}", init)
        self.assertIn("-lockfile=readonly", init)

    def test_a_plan_with_no_change_prints_no_code_and_returns_zero_and_a_later_plan_is_kept_beside_the_reviewed_one(self) -> None:
        self.plan()
        self.changes = []
        self.assertEqual(0, self.plan())
        folder = self.folder / "runs" / RUN.run_id / "storage" / "plans"
        self.assertTrue((folder / "apply_check.tfplan").is_file())
        self.assertTrue((folder / "apply.tfplan").is_file())
        self.assertEqual(1, self.printed().count("apply --confirm"))

    def test_the_backend_that_terraform_initialised_must_be_the_local_state_of_this_run(self) -> None:
        original = self.run_command

        def other_backend(program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
            result = original(program, arguments, **keywords)
            if "init" in arguments:
                record = Path(next(a for a in arguments if a.startswith("-chdir=")).removeprefix("-chdir=")) / ".terraform" / "terraform.tfstate"
                path = next(a for a in arguments if a.startswith("-backend-config=path=")).removeprefix("-backend-config=path=")
                record.write_text(json.dumps({"backend": {"type": "s3", "config": {"path": path}}}))
            return result

        with mock.patch.object(storage, "run_command", side_effect=other_backend), self.assertRaises(storage.StorageError) as caught:
            self.plan()
        self.assertIn("backend", str(caught.exception))
        self.assertNotIn("plan", self.terraform_actions())

    def test_a_heavy_input_or_a_terraform_variable_of_the_caller_does_not_reach_the_call(self) -> None:
        with mock.patch.dict(os.environ, {"TF_VAR_bucket_name": "other", "TF_CLI_ARGS": "-x", "MINIO_USER": "caller", "AWS_PROFILE": "caller"}):
            self.plan()
        for _, environment in self.calls:
            self.assertEqual([], [name for name in environment if name.startswith("TF_") and name != "TF_IN_AUTOMATION"])
            self.assertEqual(FIRST_VALUE, environment["MINIO_USER"])
            self.assertNotIn("AWS_PROFILE", environment)

    def test_a_terraform_failure_keeps_its_output_with_each_credential_replaced_and_the_message_names_the_file_and_no_value(self) -> None:
        def failing(program: str, arguments: list[str], **keywords: Any) -> CompletedProcess[str]:
            if "plan" in arguments:
                return CompletedProcess([program], 1, f"error for {FIRST_VALUE}\n", f"bad {SECOND_VALUE} at {ADDRESS} 100.64.0.7\n")
            return self.run_command(program, arguments, **keywords)

        with mock.patch.object(storage, "run_command", side_effect=failing), self.assertRaises(storage.StorageError) as caught:
            self.plan()
        message = str(caught.exception)
        self.assertIn("terraform plan failed with exit status 1", message)
        self.assertNotIn(SECOND_VALUE, message)
        self.assertNotIn(FIRST_VALUE, message)
        kept = (self.folder / "runs" / RUN.run_id / "raw" / "storage_terraform_plan_failure.txt").read_text()
        self.assertNotIn(SECOND_VALUE, kept)
        self.assertNotIn(FIRST_VALUE, kept)
        self.assertEqual(0o600, stat.S_IMODE((self.folder / "runs" / RUN.run_id / "raw" / "storage_terraform_plan_failure.txt").stat().st_mode))

    def test_a_missing_pinned_terraform_is_refused_and_names_make_setup(self) -> None:
        (self.folder / ".tools" / "bin" / "terraform").unlink()
        with self.assertRaises(WrapperError) as caught:
            self.plan()
        self.assertIn("make setup", str(caught.exception))


class ApplyTest(Stand):
    def reviewed(self) -> str:
        self.plan()
        return next(line for line in self.printed().splitlines() if "apply --confirm" in line).rsplit(" ", 1)[1]

    def apply(self, code: str, **changes: Any) -> int:
        return storage.apply_plan(RUN, code, CONFIGURATION, ADDRESS, storage.Credentials(FIRST_VALUE, SECOND_VALUE))

    def test_the_reviewed_plan_is_applied_by_its_own_bytes_and_the_state_is_checked_for_the_credentials(self) -> None:
        code = self.reviewed()
        self.assertEqual(0, self.apply(code))
        applied = next(arguments for arguments, _ in self.calls if "apply" in arguments)
        copy = Path(applied[-1])
        self.assertEqual("apply", applied[1])
        self.assertTrue(copy.name.startswith("approved_"))
        self.assertFalse(copy.exists(), "the approved copy is removed after the apply")
        self.assertIn("Apply complete", self.printed())

    def test_a_wrong_code_a_changed_plan_file_and_a_plan_for_another_bucket_or_endpoint_are_refused_before_any_terraform_call_that_changes(self) -> None:
        code = self.reviewed()
        folder = self.folder / "runs" / RUN.run_id / "storage" / "plans"
        before = len(self.calls)
        with self.assertRaises(storage.StorageError) as caught:
            self.apply("0" * 12)
        self.assertIn("confirmation code", str(caught.exception))
        binding = json.loads((folder / "apply_binding.json").read_text())
        for label, key, value in (
            ("bucket", "bucket_name", "another-project"),
            ("endpoint", "endpoint", f"other.example.ts.net:{9449}"),
            ("run", "run_id", "other"),
        ):
            with self.subTest(label):
                (folder / "apply_binding.json").write_text(json.dumps(binding | {key: value}))
                with self.assertRaises(storage.StorageError):
                    self.apply(code)
        (folder / "apply_binding.json").write_text(json.dumps(binding))
        (folder / "apply.tfplan").write_bytes(b"another plan")
        with self.assertRaises(storage.StorageError) as changed:
            self.apply(code)
        self.assertIn("differs from the plan that was reviewed", str(changed.exception))
        self.assertEqual(before, len(self.calls))
        self.assertNotIn("apply", self.terraform_actions())

    def test_no_saved_plan_is_refused(self) -> None:
        with self.assertRaises(storage.StorageError) as caught:
            self.apply("0" * 12)
        self.assertIn("no saved apply plan", str(caught.exception))

    def test_a_state_that_holds_the_user_or_the_password_is_refused_by_a_fixed_message_that_holds_no_value(self) -> None:
        for value in (FIRST_VALUE, SECOND_VALUE):
            with self.subTest(value=value[:8]):
                self.state_text = json.dumps({"resources": [{"attributes": {"note": value}}]})
                code = self.reviewed()
                with self.assertRaises(storage.StorageError) as caught:
                    self.apply(code)
                self.assertNotIn(value, str(caught.exception))
                self.assertIn("state", str(caught.exception))

    def test_a_state_that_holds_neither_passes_and_the_two_values_are_found_in_every_form_the_state_may_use(self) -> None:
        self.assertEqual([], storage.state_leaks("{}", storage.Credentials(FIRST_VALUE, SECOND_VALUE)))
        for form in (SECOND_VALUE, json.dumps(SECOND_VALUE)[1:-1], SECOND_VALUE.encode().hex()):
            with self.subTest(form=form[:10]):
                self.assertEqual(1, len(storage.state_leaks(f"x{form}y", storage.Credentials(FIRST_VALUE, SECOND_VALUE))))


class VersioningCheckTest(Stand):
    def check(self) -> None:
        storage.check_versioning(ADDRESS, CONFIGURATION["S3_BUCKET"], storage.Credentials(FIRST_VALUE, SECOND_VALUE))

    def test_an_enabled_status_passes_and_the_call_goes_to_the_endpoint_over_https_with_the_pair_in_the_environment_only(self) -> None:
        caller = {"AWS_PROFILE": "caller", "AWS_ACCESS_KEY_ID": "caller-key", "AWS_SESSION_TOKEN": "caller-token", "MINIO_PASSWORD": "caller", "TF_VAR_x": "y"}
        with mock.patch.dict(os.environ, caller):
            self.check()
        arguments, environment = self.aws_calls[0]
        self.assertEqual(["s3api", "get-bucket-versioning", "--bucket", "example-project"], arguments[:4])
        self.assertIn(f"https://{ADDRESS}:9449", arguments)
        self.assertEqual((FIRST_VALUE, SECOND_VALUE), (environment["AWS_ACCESS_KEY_ID"], environment["AWS_SECRET_ACCESS_KEY"]))
        self.assertEqual([], [name for name in environment if name in ("AWS_PROFILE", "AWS_SESSION_TOKEN", "MINIO_PASSWORD", "TF_VAR_x")])
        self.assertEqual("/dev/null", environment["AWS_CONFIG_FILE"])
        self.assertEqual("true", environment["AWS_EC2_METADATA_DISABLED"])
        self.assertNotIn(SECOND_VALUE, " ".join(arguments))
        self.assertIn("versioning of the bucket example-project is Enabled", self.printed())

    def test_every_other_answer_is_refused_and_names_the_status_it_got(self) -> None:
        for status in ("Suspended", "", None, "enabled", "Enabled "):
            with self.subTest(status=status):
                self.versioning = status  # type: ignore[assignment]
                with self.assertRaises(storage.StorageError) as caught:
                    self.check()
                self.assertIn("not Enabled", str(caught.exception))

    def test_an_answer_that_is_not_json_or_a_failed_call_is_refused_by_a_fixed_message_without_the_output(self) -> None:
        for result in (
            CompletedProcess(["aws"], 0, "not json " + SECOND_VALUE, ""),
            CompletedProcess(["aws"], 254, "", "error with " + SECOND_VALUE),
            CompletedProcess(["aws"], 0, "[]", ""),
        ):
            with (
                self.subTest(code=result.returncode, out=result.stdout[:8]),
                mock.patch.object(storage, "run_command", return_value=result),
                self.assertRaises(storage.StorageError) as caught,
            ):
                self.check()
            self.assertNotIn(SECOND_VALUE, str(caught.exception))

    def test_a_missing_aws_command_line_is_named_and_a_timeout_names_the_deadline(self) -> None:
        with mock.patch.object(storage, "run_command", side_effect=FileNotFoundError), self.assertRaises(storage.StorageError) as caught:
            self.check()
        self.assertIn("AWS command line", str(caught.exception))
        with mock.patch.object(storage, "run_command", side_effect=TimeoutExpired("aws", 60)), self.assertRaises(storage.StorageError) as caught:
            self.check()
        self.assertIn("timed out after", str(caught.exception))


class UpTest(Stand):
    def up(self, confirm: str = "") -> int:
        binding = self.folder / "runs" / "run_binding.json"
        binding.parent.mkdir(parents=True, exist_ok=True)
        binding.write_text(json.dumps(RUN.__dict__))
        with mock.patch.object(storage, "load_configuration", return_value=CONFIGURATION):
            return storage.storage_up(CONFIGURATION, binding, confirm)

    def test_the_first_run_stops_for_review_and_applies_nothing_and_does_not_ask_for_the_bucket(self) -> None:
        self.assertEqual(0, self.up())
        self.assertIn("stopped for review", self.printed())
        self.assertNotIn("apply", self.terraform_actions())
        self.assertEqual([], self.aws_calls)

    def test_the_confirmed_run_applies_the_reviewed_plan_and_then_checks_the_versioning(self) -> None:
        self.up()
        code = next(line for line in self.printed().splitlines() if "apply --confirm" in line).rsplit(" ", 1)[1]
        self.assertEqual(0, self.up(code))
        self.assertEqual(1, self.terraform_actions().count("apply"))
        self.assertEqual(1, len(self.aws_calls))
        self.assertIn("is up", self.printed().splitlines()[-1])

    def test_a_second_run_with_no_change_applies_nothing_prints_no_code_and_still_checks_the_versioning(self) -> None:
        self.up()
        self.changes = []
        self.output.truncate(0)
        self.output.seek(0)
        self.assertEqual(0, self.up())
        self.assertNotIn("apply --confirm", self.printed())
        self.assertNotIn("stopped for review", self.printed())
        self.assertEqual(0, self.terraform_actions().count("apply"))
        self.assertEqual(1, len(self.aws_calls))
        self.assertIn("is up", self.printed().splitlines()[-1])

    def test_a_confirmation_with_no_saved_plan_stops_and_applies_nothing(self) -> None:
        with self.assertRaises(storage.StorageError):
            self.up("0" * 12)
        self.assertNotIn("apply", self.terraform_actions())

    def test_a_bucket_that_does_not_answer_enabled_fails_the_run_after_a_clean_apply(self) -> None:
        self.up()
        code = next(line for line in self.printed().splitlines() if "apply --confirm" in line).rsplit(" ", 1)[1]
        self.versioning = "Suspended"
        with self.assertRaises(storage.StorageError):
            self.up(code)


class MainTest(Stand):
    def test_an_error_prints_one_line_on_standard_error_and_returns_one_and_a_help_request_prints_the_usage(self) -> None:
        errors = io.StringIO()
        with (
            mock.patch("sys.argv", ["storage", "up"]),
            mock.patch.object(storage, "load_configuration", side_effect=ConfigurationError("no configuration")),
            contextlib.redirect_stderr(errors),
        ):
            self.assertEqual(1, storage.main())
        self.assertEqual(1, len(errors.getvalue().splitlines()))
        self.assertNotIn(SECOND_VALUE, errors.getvalue())

    def test_the_command_takes_up_and_a_confirmation_only(self) -> None:
        with mock.patch("sys.argv", ["storage", "destroy"]), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            storage.main()


class RepositoryTest(unittest.TestCase):
    def test_make_storage_up_runs_the_command_with_the_optional_confirmation_the_way_host_up_does(self) -> None:
        text = (REAL_ROOT / "Makefile").read_text()
        self.assertIn("storage-up", text.split(".PHONY:", 1)[1].splitlines()[0])
        self.assertIn("$(PYTHON) -m scripts.infra.storage up $(if $(CONFIRM),--confirm $(CONFIRM))", text)

    def test_the_storage_module_calls_terraform_only_through_the_pinned_path(self) -> None:
        source = (REAL_ROOT / "scripts" / "infra" / "storage.py").read_text()
        self.assertIn("pinned_terraform()", source)
        self.assertNotIn('run_command("terraform"', source)


if __name__ == "__main__":
    unittest.main()
