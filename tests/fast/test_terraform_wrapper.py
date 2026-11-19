"""The saved plan's binding to its state location, checked before any apply.

The wrapper's Terraform calls are proven by real plans and applies. These cases cover the refusals that must hold
before Terraform runs: a changed plan file, a wrong confirmation code, a plan saved for another state location and a
plan saved for another run.
"""

from __future__ import annotations

import hashlib
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.infra import terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import (
    approved_plan_copy,
    backend_arguments,
    backend_override,
    binding_problems,
    clean_environment,
    configuration_files,
    effective_location_problems,
    plan_binding,
    plan_label,
    prepare_configuration,
    run_variables,
    sanitized,
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
    ["terraform_state/dev/terraform.tfstate", "terraform_state/dev/terraform.tfstate.tflock"],
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
            "-backend-config=key=terraform_state/dev/terraform.tfstate",
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


class PrivateFilesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))

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
