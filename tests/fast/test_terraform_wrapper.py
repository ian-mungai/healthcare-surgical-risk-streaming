"""The saved plan's binding to its state location, checked before any apply.

The wrapper's Terraform calls are proven by real plans and applies. These cases cover the refusals that must hold
before Terraform runs: a changed plan file, a wrong confirmation code, a plan saved for another state location and a
plan saved for another run.
"""

from __future__ import annotations

import builtins
import contextlib
import hashlib
import io
import json
import os
import stat
import sys
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infra import teardown, terraform_wrapper
from scripts.infra.state_bootstrap import RunBinding
from scripts.infra.terraform_wrapper import (
    HEAVY_INPUT_NAMES,
    WrapperError,
    apply_plan,
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
    stale_inputs_problem,
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
        self.assertEqual(HEAVY_INPUTS, json.loads((copied / "heavy_inputs.tfvars.json").read_text()))
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("host_reserve_mib: 4096", "host_reserve_mib: 5120"))
        self.assertEqual(5120, json.loads((prepare_configuration(RUN, heavy_inputs()) / "heavy_inputs.tfvars.json").read_text())["host_reserve_mib"])

    def test_a_stale_numbers_file_of_an_earlier_plan_is_replaced_and_a_copy_without_inputs_holds_none(self) -> None:
        self.prepare_source()
        folder = prepare_configuration(RUN, '{"host_reserve_mib": 1}')
        self.assertEqual({"host_reserve_mib": 1}, json.loads((folder / "heavy_inputs.tfvars.json").read_text()))
        self.assertEqual(HEAVY_INPUTS, json.loads((prepare_configuration(RUN, heavy_inputs()) / "heavy_inputs.tfvars.json").read_text()))
        self.assertFalse((prepare_configuration(RUN) / "heavy_inputs.tfvars.json").exists())

    def test_a_copy_for_reading_state_never_reads_the_limits_file(self) -> None:
        self.prepare_source()
        self.refuse_limits()
        self.assertFalse((prepare_configuration(RUN) / "heavy_inputs.tfvars.json").exists())

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
        plan_arguments: list[str] = []

        def fake_terraform(run: RunBinding, arguments: list[str], timeout: float) -> str:
            staged[arguments[0]] = None
            file = configuration_folder_of(self.root) / "heavy_inputs.tfvars.json"
            if arguments[0] in ("plan", "show") and file.is_file():
                staged[arguments[0]] = json.loads(file.read_text())
            if arguments[0] == "plan":
                plan_arguments.extend(arguments)
                Path(next(item for item in arguments if item.startswith("-out="))[5:]).write_bytes(b"plan")
            if arguments[0] == "show" and len(arguments) == 3 and arguments[1] == "-json":
                return json.dumps(plan_with(HEAVY_INPUTS))
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
            variable_files = [item for item in plan_arguments if item.startswith(("-var-file=", "-var="))]
            self.assertEqual("-var-file=heavy_inputs.tfvars.json", variable_files[-1])
            self.assertEqual("-var-file=environments/dev.tfvars", variable_files[0])
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


def plan_with(inputs: Mapping[str, object] | None) -> dict[str, object]:
    """A plan's JSON with a ``variables`` section holding the given heavy inputs, or none."""
    return {"variables": {name: {"value": value} for name, value in inputs.items()}} if inputs is not None else {}


class StaleInputsTest(unittest.TestCase):
    """A saved plan holds the heavy inputs it was made with. When the limits file now gives other numbers, the plan is stale and is refused."""

    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))
        (self.root / "compose").mkdir()
        (self.root / "compose" / "limits.yml").write_text(LIMITS)

    def test_a_plan_made_with_the_current_numbers_passes(self) -> None:
        self.assertIsNone(stale_inputs_problem(plan_with(HEAVY_INPUTS)))

    def test_a_limits_change_after_the_plan_was_saved_is_refused_with_both_sets_of_numbers(self) -> None:
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("host_reserve_mib: 4096", "host_reserve_mib: 5120"))
        problem = stale_inputs_problem(plan_with(HEAVY_INPUTS))
        self.assertIsNotNone(problem)
        for text in ("the limits changed after this plan was saved; save a new plan", "4096", "5120"):
            self.assertIn(text, problem or "")

    def test_a_change_of_each_input_is_refused(self) -> None:
        for name in HEAVY_INPUT_NAMES:
            with self.subTest(name=name):
                self.assertIn("save a new plan", stale_inputs_problem(plan_with(HEAVY_INPUTS | {name: HEAVY_INPUTS[name] + 1})) or "")

    def test_a_plan_without_the_three_variables_or_with_a_changed_form_is_refused_the_same_way(self) -> None:
        for plan in (
            plan_with(None),
            plan_with({}),
            plan_with({"host_reserve_mib": 4096}),
            plan_with({name: str(value) for name, value in HEAVY_INPUTS.items()}),
        ):
            with self.subTest(plan=plan):
                self.assertIn("the limits changed after this plan was saved; save a new plan", stale_inputs_problem(plan) or "")

    def test_a_boolean_is_not_a_number_of_mib_even_when_the_limit_is_one(self) -> None:
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("memory_limit_mib: 4096", "memory_limit_mib: 1"))
        self.assertIsNone(stale_inputs_problem(plan_with(HEAVY_INPUTS | {"spark_limit_mib": 1})))
        self.assertIsNotNone(stale_inputs_problem(plan_with(HEAVY_INPUTS | {"spark_limit_mib": True})))


class ConfirmedPlanTest(unittest.TestCase):
    """Confirming a saved plan compares the numbers it was made with against the limits now, before any apply."""

    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))
        (self.root / "compose").mkdir()
        (self.root / "compose" / "limits.yml").write_text(LIMITS)
        folder = self.root / "runs" / RUN.run_id / "plans"
        folder.mkdir(parents=True)
        (folder / "apply.tfplan").write_bytes(PLAN)
        (folder / "apply_binding.json").write_text(json.dumps(plan_binding(RUN, PLAN, "apply")))
        configuration_folder_of(self.root).mkdir(parents=True)
        self.calls: list[str] = []
        self.plan_text = json.dumps(plan_with(HEAVY_INPUTS))

        def fake_terraform(run: RunBinding, arguments: list[str], timeout: float) -> str:
            self.calls.append(arguments[0])
            return "Apply complete! Resources: 0 added, 0 changed, 0 destroyed." if arguments[0] == "apply" else self.plan_text

        self.enterContext(mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None))
        self.enterContext(mock.patch.object(terraform_wrapper, "terraform", side_effect=fake_terraform))
        self.enterContext(mock.patch.object(terraform_wrapper, "record_created"))
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))

    def test_a_plan_made_with_the_current_limits_is_applied(self) -> None:
        self.assertEqual(0, apply_plan(RUN, CODE, {"AWS_PROFILE": "example_project_dev"}))
        self.assertIn("apply", self.calls)

    def test_a_plan_saved_before_a_limits_change_is_refused_before_any_apply(self) -> None:
        (self.root / "compose" / "limits.yml").write_text(LIMITS.replace("host_reserve_mib: 4096", "host_reserve_mib: 5120"))
        with self.assertRaisesRegex(WrapperError, "the limits changed after this plan was saved; save a new plan"):
            apply_plan(RUN, CODE, {"AWS_PROFILE": "example_project_dev"})
        self.assertNotIn("apply", self.calls)

    def test_a_plan_document_of_any_shape_that_is_not_the_three_whole_numbers_is_refused_with_a_wrapper_error_before_any_apply(self) -> None:
        good = {name: {"value": value} for name, value in HEAVY_INPUTS.items()}
        shapes = {
            "null": "null",
            "text": '"text"',
            "a list": "[]",
            "a number": "5",
            "an empty object": "{}",
            "variables missing keys": json.dumps({"variables": {}}),
            "variables one key": json.dumps({"variables": {"host_reserve_mib": {"value": 4096}}}),
            "variables a list": json.dumps({"variables": []}),
            "variables null": json.dumps({"variables": None}),
            "variables text": json.dumps({"variables": "text"}),
            "an entry that is a number": json.dumps({"variables": good | {"spark_limit_mib": 4096}}),
            "an entry with no value": json.dumps({"variables": good | {"spark_limit_mib": {}}}),
            "a null value": json.dumps({"variables": good | {"spark_limit_mib": {"value": None}}}),
            "a text value": json.dumps({"variables": good | {"spark_limit_mib": {"value": "4096"}}}),
            "a boolean value": json.dumps({"variables": good | {"spark_limit_mib": {"value": True}}}),
            "a fractional value": json.dumps({"variables": good | {"spark_limit_mib": {"value": 4096.5}}}),
            "a whole float value": json.dumps({"variables": good | {"spark_limit_mib": {"value": 4096.0}}}),
            "a nested value": json.dumps({"variables": good | {"spark_limit_mib": {"value": {"value": 4096}}}}),
            "another number": json.dumps({"variables": good | {"host_reserve_mib": {"value": 1}}}),
            "a truncated file": '{"variables": {"host_reserve_mib": {"val',
            "nothing": "",
        }
        for name, text in shapes.items():
            with self.subTest(shape=name):
                self.calls.clear()
                self.plan_text = text
                with self.assertRaises(WrapperError):
                    apply_plan(RUN, CODE, {"AWS_PROFILE": "example_project_dev"})
                self.assertNotIn("apply", self.calls)


class CallerVariableTest(unittest.TestCase):
    """The limits loader alone gives the three heavy inputs. An environment variable that names one is refused by its name, with no parsing."""

    def test_no_variable_that_the_wrapper_passes_names_a_heavy_input(self) -> None:
        names = set(run_variables(RUN, {"AWS_PROFILE": "example_project_dev"})) | {key.lower() for key in terraform_wrapper.TAILSCALE_KEYS}
        self.assertEqual(set(), names & set(HEAVY_INPUT_NAMES))

    def test_a_plan_is_refused_before_terraform_starts_when_the_environment_sets_a_heavy_input(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            (root / "compose").mkdir()
            (root / "compose" / "limits.yml").write_text(LIMITS)
            for name in HEAVY_INPUT_NAMES:
                with (
                    self.subTest(name=name),
                    mock.patch.object(terraform_wrapper, "ROOT", root),
                    mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None),
                    mock.patch.object(terraform_wrapper, "run_command", side_effect=AssertionError("Terraform started")),
                    mock.patch.dict(os.environ, {f"TF_VAR_{name}": "1"}),
                    self.assertRaisesRegex(WrapperError, f"TF_VAR_{name}"),
                ):
                    save_plan(RUN, {"AWS_PROFILE": "example_project_dev", "ENVIRONMENT": "dev"}, "apply")


class SavedPlanTest(unittest.TestCase):
    """Right after an up plan is saved, the wrapper reads what Terraform decided. A plan made with other heavy inputs than the loader gives is not given a
    confirmation code, whatever overrode them. That notice is a convenience and writes no state: the check that matters runs when a plan is applied. The
    figure has no effect when the host is destroyed, so a destroy plan is not checked."""

    CONFIGURATION = {"AWS_PROFILE": "example_project_dev", "ENVIRONMENT": "dev", "TAILSCALE_CLIENT_ID": "example-id", "TAILSCALE_AUDIENCE": "example-audience"}

    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))
        (self.root / "compose").mkdir()
        (self.root / "compose" / "limits.yml").write_text(LIMITS)
        for folder in ("modules", "environments"):
            (self.root / "infra" / folder).mkdir(parents=True)
        (self.root / "infra" / ".terraform.lock.hcl").write_text("")
        (self.root / "infra" / "environments" / "dev.tfvars").write_text(
            (Path(__file__).resolve().parents[2] / "infra" / "environments" / "dev.tfvars").read_text()
        )
        record = configuration_folder_of(self.root) / ".terraform" / "terraform.tfstate"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps(S3_RECORD))
        self.plan_inputs: Mapping[str, object] = dict(HEAVY_INPUTS)
        self.plans = self.root / "runs" / RUN.run_id / "plans"
        self.output = io.StringIO()
        self.enterContext(mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None))
        self.enterContext(mock.patch.object(terraform_wrapper, "terraform", side_effect=self.fake_terraform))
        self.enterContext(contextlib.redirect_stdout(self.output))

    def fake_terraform(self, run: RunBinding, arguments: list[str], timeout: float) -> str:
        if arguments[0] == "plan":
            Path(next(item for item in arguments if item.startswith("-out="))[5:]).write_bytes(b"plan bytes")
        if arguments[0] == "show" and arguments[1] == "-json" and len(arguments) == 3:
            document = plan_with(self.plan_inputs) | {"resource_changes": [{"address": "aws_thing.a", "change": {"actions": ["create"]}}]}
            return json.dumps(document)
        return {"workspace": f"{RUN.workspace}\n", "show": "text"}.get(arguments[0], "")

    def test_a_plan_made_with_the_loaders_numbers_is_saved_and_gives_its_confirmation_code(self) -> None:
        self.assertEqual(1, save_plan(RUN, self.CONFIGURATION, "apply"))
        self.assertIn("apply --confirm", self.output.getvalue())
        self.assertTrue((self.plans / "apply.tfplan").is_file())
        self.assertFalse((self.plans / "apply_refused.json").exists())

    def test_an_up_plan_whose_json_carries_another_number_gets_no_confirmation_code_and_writes_no_state(self) -> None:
        for name in HEAVY_INPUT_NAMES:
            with self.subTest(name=name):
                self.plan_inputs = HEAVY_INPUTS | {name: HEAVY_INPUTS[name] + 1}
                self.output.truncate(0)
                with self.assertRaises(WrapperError) as caught:
                    save_plan(RUN, self.CONFIGURATION, "apply")
                for text in ("overrode", str(HEAVY_INPUTS[name]), str(HEAVY_INPUTS[name] + 1)):
                    self.assertIn(text, str(caught.exception))
                self.assertNotIn("--confirm", self.output.getvalue())
                self.assertEqual(["apply_candidate.tfplan"], sorted(path.name for path in self.plans.iterdir()))

    def test_an_up_plan_without_the_variables_or_with_a_string_number_gets_no_confirmation_code_either(self) -> None:
        for inputs in ({}, {name: str(value) for name, value in HEAVY_INPUTS.items()}):
            with self.subTest(inputs=inputs):
                self.plan_inputs = inputs
                with self.assertRaisesRegex(WrapperError, "overrode"):
                    save_plan(RUN, self.CONFIGURATION, "apply")

    def test_a_destroy_plan_with_other_numbers_is_saved_and_applied_because_the_figure_does_not_matter_when_the_host_is_destroyed(self) -> None:
        self.plan_inputs = {"host_reserve_mib": 1}
        self.assertEqual(1, save_plan(RUN, self.CONFIGURATION, "destroy"))
        self.assertTrue((self.plans / "destroy.tfplan").is_file())
        self.assertTrue((self.plans / "destroy_binding.json").is_file())
        finished: list[str] = []
        with (
            mock.patch.object(terraform_wrapper, "heavy_inputs", side_effect=AssertionError("the destroy path compared the heavy inputs")),
            mock.patch.object(teardown, "terraform", return_value="Resources: 0 added, 0 changed, 1 destroyed"),
        ):
            teardown.apply_destroy(RUN, self.plans / "destroy.tfplan", finished.append)
        self.assertEqual(["apply"], finished)

    def test_the_wrapper_never_reads_a_variable_file_it_only_passes_its_path_to_terraform(self) -> None:
        """Terraform alone reads ``.tfvars`` text. A reader of its own cannot match Terraform's language, so the check is on what Terraform decided."""
        seen: list[str] = []
        real_open, real_read_text, real_read_bytes, real_path_open = builtins.open, Path.read_text, Path.read_bytes, Path.open

        def from_the_wrapper() -> bool:
            return sys._getframe(3).f_globals.get("__name__") == terraform_wrapper.__name__

        def note(path: object) -> None:
            if str(path).endswith((".tfvars", ".tfvars.json")) and from_the_wrapper():
                seen.append(str(path))

        def spy_open(file: Any, *arguments: Any, **keywords: Any) -> Any:
            note(file)
            return real_open(file, *arguments, **keywords)

        def spy_read_text(self: Path, *arguments: Any, **keywords: Any) -> str:
            note(self)
            return real_read_text(self, *arguments, **keywords)

        def spy_read_bytes(self: Path, *arguments: Any, **keywords: Any) -> bytes:
            note(self)
            return real_read_bytes(self, *arguments, **keywords)

        def spy_path_open(self: Path, *arguments: Any, **keywords: Any) -> Any:
            note(self)
            return real_path_open(self, *arguments, **keywords)

        with (
            mock.patch.object(builtins, "open", spy_open),
            mock.patch.object(Path, "read_text", spy_read_text),
            mock.patch.object(Path, "read_bytes", spy_read_bytes),
            mock.patch.object(Path, "open", spy_path_open),
        ):
            self.assertEqual(1, save_plan(RUN, self.CONFIGURATION, "apply"))
        self.assertEqual([], seen)


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
