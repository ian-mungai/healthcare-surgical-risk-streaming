"""The saved plan's binding to its state location, checked before any apply.

The wrapper's Terraform calls are proven by real plans and applies. These cases cover the refusals that must hold
before Terraform runs: a changed plan file, a wrong confirmation code, a plan saved for another state location and a
plan saved for another run.
"""

from __future__ import annotations

import hashlib
import unittest

from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import (
    backend_arguments,
    backend_override,
    binding_problems,
    clean_environment,
    effective_location_problems,
    plan_binding,
    plan_label,
    run_variables,
    state_location,
)

RUN = RunBinding(
    "20260101t0000-abcdef",
    "c0ffee",
    "s3",
    "us-west-2",
    "example-state-dev-20260101t0000-abcdef",
    "default",
    ["environment/terraform.tfstate", "environment/terraform.tfstate.tflock"],
    "verified",
)
LOCAL = RunBinding("20260101t0000-abcdef", "c0ffee", "local", "us-west-2", "", "default", ["/srv/run/terraform.tfstate"], "verified", provision_host=False)
PLAN = b"saved plan bytes"
CODE = hashlib.sha256(PLAN).hexdigest()[:12]


class PlanBindingTest(unittest.TestCase):
    def test_backend_arguments_come_from_the_run_binding(self) -> None:
        expected = [
            "-backend-config=bucket=example-state-dev-20260101t0000-abcdef",
            "-backend-config=key=environment/terraform.tfstate",
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
        other = RunBinding(**{**RUN.__dict__, "state_bucket": "example-state-dev-20260202t0000-abcdef"})
        self.assertEqual(
            ["the plan was saved for a different state location than this run's"],
            binding_problems(RUN, plan_binding(other, PLAN, "apply"), PLAN, CODE, "apply"),
        )

    def test_other_action_is_refused(self) -> None:
        self.assertEqual(["the plan was saved as destroy, not apply"], binding_problems(RUN, plan_binding(RUN, PLAN, "destroy"), PLAN, CODE, "apply"))

    def test_unverified_run_is_refused(self) -> None:
        planned = RunBinding(**{**RUN.__dict__, "status": "planned"})
        self.assertEqual(["the run's state location is not verified"], binding_problems(planned, plan_binding(planned, PLAN, "apply"), PLAN, CODE, "apply"))


S3_RECORD = {"backend": {"type": "s3", "config": {"bucket": RUN.state_bucket, "key": RUN.state_keys[0], "region": RUN.region}}}
LOCAL_RECORD = {"backend": {"type": "local", "config": {"path": LOCAL.state_keys[0]}}}


class EffectiveLocationTest(unittest.TestCase):
    def test_terraform_variables_of_the_caller_do_not_reach_terraform(self) -> None:
        cleaned = clean_environment({"PATH": "/bin", "HOME": "/srv/example", "TF_WORKSPACE": "other", "TF_VAR_run_id": "x", "TF_CLI_ARGS": "-lock=false"})
        self.assertEqual({"PATH": "/bin", "HOME": "/srv/example", "TF_IN_AUTOMATION": "1"}, cleaned)

    def test_bound_location_names_the_region_and_every_state_key(self) -> None:
        self.assertEqual(
            {"backend": "s3", "bucket": RUN.state_bucket, "workspace": "default", "keys": RUN.state_keys, "region": "us-west-2"}, state_location(RUN)
        )

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


if __name__ == "__main__":
    unittest.main()
