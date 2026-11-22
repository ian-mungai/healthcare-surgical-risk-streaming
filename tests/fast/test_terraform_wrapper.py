"""The saved plan's binding to its state location, checked before any apply.

The wrapper's Terraform calls are proven by real plans and applies. These cases cover the refusals that must hold
before Terraform runs: a changed plan file, a wrong confirmation code, a plan saved for another state location and a
plan saved for another run.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.infra import terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import (
    WrapperError,
    approved_plan_copy,
    backend_arguments,
    backend_override,
    binding_problems,
    clean_environment,
    configuration_files,
    effective_location_problems,
    heavy_inputs,
    plan_binding,
    plan_changes,
    plan_label,
    prepare_configuration,
    run_variables,
    sanitized,
    save_plan,
    state_json,
    state_location,
    write_private,
)

RUN = RunBinding(
    "20260101t0000-abcdef",
    "c0ffee",
    "s3",
    "us-west-2",
    "example-project",
    "default",
    ["state/dev/terraform.tfstate", "state/dev/terraform.tfstate.tflock"],
    "verified",
)
LOCAL = RunBinding(
    "20260101t0000-abcdef", "c0ffee", "local", "us-west-2", "example-project", "default", ["/srv/run/terraform.tfstate"], "verified", provision_host=False
)
PLAN = b"saved plan bytes"
CODE = hashlib.sha256(PLAN).hexdigest()[:12]


class PlanBindingTest(unittest.TestCase):
    def test_backend_arguments_come_from_the_run_binding(self) -> None:
        expected = [
            "-backend-config=bucket=example-project",
            "-backend-config=key=state/dev/terraform.tfstate",
            "-backend-config=region=us-west-2",
            "-backend-config=profile=example_project_dev",
            "-backend-config=use_lockfile=true",
        ]
        self.assertEqual(expected, backend_arguments(RUN, "example_project_dev"))

    def test_local_backend_is_rendered_as_an_override_and_takes_no_backend_arguments(self) -> None:
        self.assertEqual([], backend_arguments(LOCAL, "example_project_dev"))
        self.assertEqual('terraform {\n  backend "local" {\n    path = "/srv/run/terraform.tfstate"\n  }\n}\n', backend_override(LOCAL))

    def test_s3_backend_renders_no_override(self) -> None:
        self.assertIsNone(backend_override(RUN))

    def test_local_backend_preserves_the_host_it_was_given(self) -> None:
        self.assertEqual("false", run_variables(LOCAL, {"AWS_PROFILE": "example_project_dev"})["provision_host"])
        self.assertEqual("true", run_variables(RUN, {"AWS_PROFILE": "example_project_dev"})["provision_host"])
        given = RunBinding(**{**RUN.__dict__, "provision_host": False})
        self.assertEqual("false", run_variables(given, {"AWS_PROFILE": "example_project_dev"})["provision_host"])

    def test_terraform_is_given_the_bound_bucket_and_no_other_run_value(self) -> None:
        self.assertEqual(
            {"data_bucket_name": "example-project", "aws_profile": "example_project_dev", "provision_host": "true"},
            run_variables(RUN, {"AWS_PROFILE": "example_project_dev"}),
        )

    def test_plan_saved_for_the_s3_backend_is_refused_under_the_local_backend(self) -> None:
        self.assertEqual(
            ["the plan was saved for a different state location than this run's"],
            binding_problems(LOCAL, plan_binding(RUN, PLAN, "apply"), PLAN, CODE, "apply"),
        )

    def test_a_later_plan_with_no_change_keeps_the_record_of_the_reviewed_plan(self) -> None:
        self.assertEqual("apply", plan_label("apply", changes=18, reviewed_exists=False))
        self.assertEqual("apply", plan_label("apply", changes=0, reviewed_exists=False))
        self.assertEqual("apply_check", plan_label("apply", changes=0, reviewed_exists=True))
        self.assertEqual("apply", plan_label("apply", changes=2, reviewed_exists=True))
        self.assertEqual("destroy", plan_label("destroy", changes=0, reviewed_exists=True))
        self.assertEqual("destroy", plan_label("destroy", changes=3, reviewed_exists=True))

    def test_matching_plan_passes(self) -> None:
        self.assertEqual([], binding_problems(RUN, plan_binding(RUN, PLAN, "apply"), PLAN, CODE, "apply"))

    def test_changed_plan_file_is_refused(self) -> None:
        self.assertEqual(
            ["the saved plan file differs from the plan that was reviewed"],
            binding_problems(RUN, plan_binding(RUN, PLAN, "apply"), b"other bytes", CODE, "apply"),
        )

    def test_wrong_confirmation_code_is_refused(self) -> None:
        self.assertEqual(
            ["the confirmation code does not equal the reviewed plan"], binding_problems(RUN, plan_binding(RUN, PLAN, "apply"), PLAN, "000000000000", "apply")
        )

    def test_other_state_location_is_refused(self) -> None:
        other = RunBinding(**{**RUN.__dict__, "bucket": "example-project-test-02020000abcdef"})
        self.assertEqual(
            ["the plan was saved for a different state location than this run's"],
            binding_problems(RUN, plan_binding(other, PLAN, "apply"), PLAN, CODE, "apply"),
        )

    def test_other_action_is_refused(self) -> None:
        self.assertEqual(["the plan was saved as destroy, not apply"], binding_problems(RUN, plan_binding(RUN, PLAN, "destroy"), PLAN, CODE, "apply"))

    def test_unverified_run_is_refused(self) -> None:
        planned = RunBinding(**{**RUN.__dict__, "status": "planned"})
        self.assertEqual(["the run's state location is not verified"], binding_problems(planned, plan_binding(planned, PLAN, "apply"), PLAN, CODE, "apply"))


S3_RECORD = {"backend": {"type": "s3", "config": {"bucket": RUN.bucket, "key": RUN.state_keys[0], "region": RUN.region}}}
LOCAL_RECORD = {"backend": {"type": "local", "config": {"path": LOCAL.state_keys[0]}}}


class ConfigurationCopyTest(unittest.TestCase):
    def test_a_runs_copy_takes_no_variable_file_that_terraform_loads_unasked(self) -> None:
        source = Path(tempfile.mkdtemp(prefix="configuration_"))
        for name in ("main.tf", "backend_override.tf", "extra.auto.tfvars", "terraform.tfvars", ".terraform.lock.hcl"):
            (source / name).write_text("")
        self.assertEqual(["main.tf", ".terraform.lock.hcl"], [item.name for item in configuration_files(source)])


def configuration_folder_of(root: Path) -> Path:
    return root / ".tools" / "terraform" / RUN.run_id / "config"


LIMITS = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  nessie:
    runtime: compose
    group: supporting
    memory_limit_mib: 2048
  spark:
    runtime: compose
    group: batch
    memory_limit_mib: 4096
"""
HEAVY_INPUTS = {"host_reserve_mib": 4096, "always_on_limits_mib": 3072, "spark_limit_mib": 4096}


class PrivateFilesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))
        (self.root / "compose").mkdir()
        (self.root / "compose" / "limits.yml").write_text(LIMITS)

    def test_nested_automatic_variable_files_are_left_out_of_a_runs_copy(self) -> None:
        source = self.root / "infra"
        for folder in ("modules/host", "environments"):
            (source / folder).mkdir(parents=True)
        for name in ("main.tf", ".terraform.lock.hcl", "modules/host/main.tf", "modules/host/extra.auto.tfvars", "environments/dev.tfvars"):
            (source / name).write_text("")
        (source / "environments" / "other.auto.tfvars.json").write_text("{}")
        copied = prepare_configuration(RUN)
        names = sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file())
        self.assertEqual([".terraform.lock.hcl", "environments/dev.tfvars", "main.tf", "modules/host/main.tf"], names)

    def prepare_source(self) -> None:
        source = self.root / "infra"
        for folder in ("modules/host", "environments"):
            (source / folder).mkdir(parents=True)
        for name in ("main.tf", ".terraform.lock.hcl", "modules/host/main.tf", "environments/dev.tfvars"):
            (source / name).write_text("")

    def refuse_limits(self) -> None:
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("memory_limit_mib: 4096", "memory_limit_mib: 0"))

    def test_a_runs_copy_holds_the_three_numbers_that_the_limits_loader_gives_and_a_changed_file_changes_them(self) -> None:
        self.prepare_source()
        copied = prepare_configuration(RUN, heavy_inputs())
        self.assertEqual(HEAVY_INPUTS, json.loads((copied / "heavy_inputs.auto.tfvars.json").read_text()))
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("host_reserve_mib: 4096", "host_reserve_mib: 5120"))
        self.assertEqual(5120, json.loads((prepare_configuration(RUN, heavy_inputs()) / "heavy_inputs.auto.tfvars.json").read_text())["host_reserve_mib"])

    def test_a_stale_numbers_file_of_an_earlier_plan_is_replaced_and_a_copy_without_inputs_holds_none(self) -> None:
        self.prepare_source()
        folder = prepare_configuration(RUN, '{"host_reserve_mib": 1}')
        self.assertEqual({"host_reserve_mib": 1}, json.loads((folder / "heavy_inputs.auto.tfvars.json").read_text()))
        self.assertEqual(HEAVY_INPUTS, json.loads((prepare_configuration(RUN, heavy_inputs()) / "heavy_inputs.auto.tfvars.json").read_text()))
        self.assertFalse((prepare_configuration(RUN) / "heavy_inputs.auto.tfvars.json").exists())

    def test_a_copy_for_reading_state_never_reads_the_limits_file(self) -> None:
        self.prepare_source()
        self.refuse_limits()
        self.assertFalse((prepare_configuration(RUN) / "heavy_inputs.auto.tfvars.json").exists())

    def test_a_limits_file_the_loader_refuses_stops_the_plan_before_terraform_starts_and_names_the_reason(self) -> None:
        self.prepare_source()
        self.refuse_limits()
        for action in ("apply", "destroy"):
            with (
                self.subTest(action=action),
                mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None),
                mock.patch.object(terraform_wrapper, "run_command", side_effect=AssertionError("Terraform started")),
                self.assertRaisesRegex(WrapperError, "limits.*memory_limit_mib"),
            ):
                save_plan(RUN, {"AWS_PROFILE": "example_project_dev"}, action)
        self.assertFalse((self.root / "runs").exists())

    def test_a_plan_runs_in_a_copy_that_holds_the_loaders_numbers_and_a_state_read_in_one_that_holds_none(self) -> None:
        self.prepare_source()
        record = configuration_folder_of(self.root) / ".terraform" / "terraform.tfstate"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps(S3_RECORD))
        staged: dict[str, dict[str, int] | None] = {}

        def fake_terraform(run: RunBinding, arguments: list[str], timeout: float) -> str:
            staged[arguments[0]] = None
            file = configuration_folder_of(self.root) / "heavy_inputs.auto.tfvars.json"
            if arguments[0] in ("plan", "show") and file.is_file():
                staged[arguments[0]] = json.loads(file.read_text())
            if arguments[0] == "plan":
                Path(next(item for item in arguments if item.startswith("-out="))[5:]).write_bytes(b"plan")
            return {"workspace": f"{RUN.workspace}\n", "show": "{}"}.get(arguments[0], "")

        configuration = {
            "AWS_PROFILE": "example_project_dev",
            "ENVIRONMENT": "dev",
            "TAILSCALE_CLIENT_ID": "example-id",
            "TAILSCALE_AUDIENCE": "example-audience",
        }
        with (
            mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None),
            mock.patch.object(terraform_wrapper, "terraform", side_effect=fake_terraform),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            save_plan(RUN, configuration, "apply")
            self.assertEqual(HEAVY_INPUTS, staged["plan"])
            state_json(RUN, configuration)
        self.assertIsNone(staged["show"])

    def test_a_state_read_works_with_a_limits_file_the_loader_refuses(self) -> None:
        self.prepare_source()
        self.refuse_limits()
        record = configuration_folder_of(self.root) / ".terraform" / "terraform.tfstate"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps(S3_RECORD))
        answers = {"init": "", "workspace": f"{RUN.workspace}\n", "show": '{"values": {}}'}
        with (
            mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None),
            mock.patch.object(terraform_wrapper, "terraform", side_effect=lambda run, arguments, timeout: answers[arguments[0]]),
        ):
            self.assertEqual({"values": {}}, state_json(RUN, {"AWS_PROFILE": "example_project_dev"}))

    def test_each_invocation_gets_its_own_owner_only_copy_of_the_approved_plan(self) -> None:
        (self.root / ".tools" / "terraform" / RUN.run_id / "config").mkdir(parents=True)
        first, second = approved_plan_copy(RUN, PLAN), approved_plan_copy(RUN, PLAN)
        self.assertNotEqual(first, second)
        self.assertEqual([PLAN, PLAN], [first.read_bytes(), second.read_bytes()])
        self.assertEqual([0o600, 0o600], [stat.S_IMODE(path.stat().st_mode) for path in (first, second)])

    def test_raw_plan_files_are_readable_by_their_owner_only(self) -> None:
        target = self.root / "plans" / "apply.txt"
        target.parent.mkdir()
        target.write_text("older")
        target.chmod(0o644)
        write_private(target, "plan text with private values")
        self.assertEqual(("plan text with private values", 0o600), (target.read_text(), stat.S_IMODE(target.stat().st_mode)))


def plan_document(*, resources: tuple[str, ...] = (), outputs: dict[str, str] | None = None) -> dict[str, object]:
    return {
        "resource_changes": [{"address": f"aws_thing.{name}", "change": {"actions": ["no-op"]}} for name in resources],
        "output_changes": {name: {"actions": [action]} for name, action in (outputs or {}).items()},
    }


class PlanChangesTest(unittest.TestCase):
    def test_a_plan_that_changes_only_an_output_counts_as_a_change(self) -> None:
        self.assertEqual([("update", "output.heavy_memory_mib")], plan_changes(plan_document(resources=("host",), outputs={"heavy_memory_mib": "update"})))

    def test_an_output_added_to_an_existing_state_counts_as_a_change(self) -> None:
        self.assertEqual([("create", "output.heavy_memory_mib")], plan_changes(plan_document(outputs={"heavy_memory_mib": "create"})))

    def test_a_removed_output_counts_as_a_change(self) -> None:
        self.assertEqual([("delete", "output.old")], plan_changes(plan_document(outputs={"old": "delete"})))

    def test_a_plan_with_no_action_but_no_op_has_no_change(self) -> None:
        self.assertEqual([], plan_changes(plan_document(resources=("host",), outputs={"heavy_memory_mib": "no-op"})))
        self.assertEqual([], plan_changes({}))

    def test_resource_and_output_changes_are_listed_together_with_their_actions(self) -> None:
        document = {
            "resource_changes": [{"address": "aws_thing.a", "change": {"actions": ["delete", "create"]}}],
            "output_changes": {"x": {"actions": ["update"]}},
        }
        self.assertEqual([("delete/create", "aws_thing.a"), ("update", "output.x")], plan_changes(document))


class EffectiveLocationTest(unittest.TestCase):
    def test_terraform_variables_of_the_caller_do_not_reach_terraform(self) -> None:
        cleaned = clean_environment({"PATH": "/bin", "HOME": "/srv/example", "TF_WORKSPACE": "other", "TF_VAR_run_id": "x", "TF_CLI_ARGS": "-lock=false"})
        self.assertEqual({"PATH": "/bin", "HOME": "/srv/example", "TF_IN_AUTOMATION": "1"}, cleaned)

    def test_bound_location_names_the_region_and_every_state_key(self) -> None:
        self.assertEqual({"backend": "s3", "bucket": RUN.bucket, "workspace": "default", "keys": RUN.state_keys, "region": "us-west-2"}, state_location(RUN))

    def test_matching_effective_backend_passes(self) -> None:
        self.assertEqual([], effective_location_problems(RUN, S3_RECORD, "default"))
        self.assertEqual([], effective_location_problems(LOCAL, LOCAL_RECORD, "default"))

    def test_other_workspace_is_refused(self) -> None:
        self.assertEqual(["Terraform selected the workspace other, not default"], effective_location_problems(RUN, S3_RECORD, "other"))

    def test_other_backend_values_are_refused(self) -> None:
        moved = {"backend": {"type": "s3", "config": {"bucket": "other-bucket", "key": RUN.state_keys[0], "region": RUN.region}}}
        self.assertEqual(["Terraform's backend bucket differs from this run's binding"], effective_location_problems(RUN, moved, "default"))
        self.assertEqual(["Terraform's backend type is local, not s3"], effective_location_problems(RUN, LOCAL_RECORD, "default"))
        elsewhere = {"backend": {"type": "local", "config": {"path": "/srv/other/terraform.tfstate"}}}
        self.assertEqual(["Terraform's backend path differs from this run's binding"], effective_location_problems(LOCAL, elsewhere, "default"))

    def test_missing_backend_record_is_refused(self) -> None:
        self.assertEqual(["Terraform's backend type is missing, not s3"], effective_location_problems(RUN, {}, "default"))


class SanitizedOutputTest(unittest.TestCase):
    def test_account_numbers_and_addresses_are_removed_from_printed_text(self) -> None:
        text = "role arn:aws:iam::" + "1" * 12 + ":role/example_host at 203.0.113.7 in account " + "2" * 12
        self.assertEqual("role arn:aws:iam::<ACCOUNT>:role/example_host at <ADDRESS> in account <ACCOUNT>", sanitized(text))

    def test_other_numbers_stay(self) -> None:
        self.assertEqual("3000 IOPS, version 1.16.4, 18 to add", sanitized("3000 IOPS, version 1.16.4, 18 to add"))


if __name__ == "__main__":
    unittest.main()
