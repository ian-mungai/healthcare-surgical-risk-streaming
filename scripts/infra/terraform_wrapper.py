"""Save a Terraform plan for this run and apply exactly the plan that was reviewed.

Run from the repository root under the project profile in ``.env``:

``.venv/bin/python -m scripts.infra.terraform_wrapper plan``
``.venv/bin/python -m scripts.infra.terraform_wrapper apply --confirm <CODE>``

``plan`` selects the backend from the run binding and the environment's variable file, saves the plan under
``runs/<RUN_ID>/plans/`` with its readable text and its JSON form and binds the plan file's hash to the run's state location. It prints
the resource changes and a confirmation code. ``apply`` refuses a changed plan file, a wrong code and a plan saved for
another state location, then applies the saved plan. ``plan --destroy`` saves a destroy plan the same way; this
command never applies one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from scripts.infra.state_bootstrap import DEFAULT_BINDING, RunBinding
from scripts.process import run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration

INIT_TIMEOUT_SECONDS = 600
PLAN_TIMEOUT_SECONDS = 900
APPLY_TIMEOUT_SECONDS = 1800
TAILSCALE_KEYS = ("TAILSCALE_CLIENT_ID", "TAILSCALE_AUDIENCE")


class WrapperError(RuntimeError):
    """The wrapper refused to continue or a Terraform call failed."""


BACKEND_OVERRIDE = ROOT / "infra" / "backend_override.tf"


def backend_override(run: RunBinding) -> str | None:
    """The Terraform override that selects the local backend, or None for the S3 backend that the configuration names."""
    if run.backend != "local":
        return None
    return f'terraform {{\n  backend "local" {{\n    path = "{run.state_keys[0]}"\n  }}\n}}\n'


def run_variables(run: RunBinding, configuration: dict[str, str]) -> dict[str, str]:
    """The Terraform variables that come from the run and not from the environment's variable file."""
    return {"run_id": run.run_id, "aws_profile": configuration["AWS_PROFILE"], "provision_host": "true" if run.provision_host else "false"}


def backend_arguments(run: RunBinding, profile: str) -> list[str]:
    if run.backend == "local":
        return []
    backend = {"bucket": run.state_bucket, "key": run.state_keys[0], "region": run.region, "profile": profile, "use_lockfile": "true"}
    return [f"-backend-config={key}={value}" for key, value in backend.items()]


def state_location(run: RunBinding) -> dict[str, str]:
    return {"backend": run.backend, "bucket": run.state_bucket, "workspace": run.workspace, "key": run.state_keys[0]}


def plan_binding(run: RunBinding, plan: bytes, action: str) -> dict[str, Any]:
    """The record that ties a saved plan file to its action, its run and the state location it was saved for."""
    return {"plan_sha256": hashlib.sha256(plan).hexdigest(), "action": action, "run_id": run.run_id, "state_location": state_location(run)}


def binding_problems(run: RunBinding, saved: dict[str, Any], plan: bytes, code: str, action: str) -> list[str]:
    """Each reason to refuse the saved plan, checked before Terraform runs."""
    problems = []
    if run.status != "verified":
        problems.append("the run's state location is not verified")
    if saved["action"] != action:
        problems.append(f"the plan was saved as {saved['action']}, not {action}")
    if saved["state_location"] != state_location(run) or saved["run_id"] != run.run_id:
        problems.append("the plan was saved for a different state location than this run's")
    if hashlib.sha256(plan).hexdigest() != saved["plan_sha256"]:
        problems.append("the saved plan file differs from the plan that was reviewed")
    elif code != saved["plan_sha256"][:12]:
        problems.append("the confirmation code does not equal the reviewed plan")
    return problems


def terraform(run: RunBinding, arguments: list[str], timeout: float) -> str:
    environment = os.environ | {"TF_IN_AUTOMATION": "1", "TF_DATA_DIR": str(ROOT / ".tools" / "terraform" / run.run_id)}
    result = run_command("terraform", ["-chdir=infra", *arguments], cwd=ROOT, env=environment, timeout=timeout)
    if result.returncode != 0:
        raise WrapperError(f"terraform {arguments[0]} failed:\n{result.stdout}{result.stderr}")
    return result.stdout


def load_run(path: Path) -> RunBinding:
    if not path.is_file():
        raise WrapperError("no run binding: run the state bootstrap step first")
    return RunBinding(**json.loads(path.read_text()))


def initialise(run: RunBinding, configuration: dict[str, str]) -> None:
    override = backend_override(run)
    if override is None:
        BACKEND_OVERRIDE.unlink(missing_ok=True)
    else:
        BACKEND_OVERRIDE.write_text(override)
    terraform(run, ["init", "-input=false", "-reconfigure", *backend_arguments(run, configuration["AWS_PROFILE"])], INIT_TIMEOUT_SECONDS)


def state_json(run: RunBinding, configuration: dict[str, str]) -> dict[str, Any]:
    """The run's current Terraform state, read from its bound backend."""
    initialise(run, configuration)
    state: dict[str, Any] = json.loads(terraform(run, ["show", "-json"], PLAN_TIMEOUT_SECONDS))
    return state


def save_plan(run: RunBinding, configuration: dict[str, str], action: str) -> int:
    """Save the plan with its text, JSON and binding, print its changes and return how many resources change."""
    if run.status != "verified":
        raise WrapperError("the run's state bucket is not verified: run the state bootstrap step")
    folder = ROOT / "runs" / run.run_id / "plans"
    folder.mkdir(parents=True, exist_ok=True)
    plan_file = folder / f"{action}.tfplan"
    initialise(run, configuration)
    missing = sorted(key for key in TAILSCALE_KEYS if not configuration.get(key))
    if missing:
        raise WrapperError(f".env: missing values for: {', '.join(missing)}")
    values = run_variables(run, configuration) | {key.lower(): configuration[key] for key in TAILSCALE_KEYS}
    variables = [f"-var-file=environments/{configuration['ENVIRONMENT']}.tfvars", *[f"-var={name}={value}" for name, value in values.items()]]
    terraform(run, ["plan", "-input=false", "-no-color", *(["-destroy"] if action == "destroy" else []), *variables, f"-out={plan_file}"], PLAN_TIMEOUT_SECONDS)
    (folder / f"{action}.txt").write_text(terraform(run, ["show", "-no-color", str(plan_file)], PLAN_TIMEOUT_SECONDS))
    plan_json = terraform(run, ["show", "-json", str(plan_file)], PLAN_TIMEOUT_SECONDS)
    (folder / f"{action}.json").write_text(plan_json)
    changes = json.loads(plan_json).get("resource_changes", [])
    saved = plan_binding(run, plan_file.read_bytes(), action)
    (folder / f"{action}_binding.json").write_text(json.dumps(saved, indent=2) + "\n")
    counts: dict[str, int] = {}
    for change in changes:
        actions = "/".join(change["change"]["actions"])
        if actions != "no-op":
            counts[actions] = counts.get(actions, 0) + 1
            sys.stdout.write(f"{actions}: {change['address']}\n")
    summary = ", ".join(f"{count} to {name}" for name, count in sorted(counts.items())) or "no change"
    sys.stdout.write(f"run {run.run_id}, {action} plan: {summary}\nfull plan text: {(folder / f'{action}.txt').relative_to(ROOT)}\n")
    if counts and action == "apply":
        sys.stdout.write(f"to apply exactly this plan after review: apply --confirm {saved['plan_sha256'][:12]}\n")
    return sum(counts.values())


def apply_plan(run: RunBinding, code: str) -> int:
    folder = ROOT / "runs" / run.run_id / "plans"
    plan_file = folder / "apply.tfplan"
    binding_file = folder / "apply_binding.json"
    if not plan_file.is_file() or not binding_file.is_file():
        raise WrapperError("no saved apply plan for this run: run plan first")
    problems = binding_problems(run, json.loads(binding_file.read_text()), plan_file.read_bytes(), code, "apply")
    if problems:
        raise WrapperError("refused before any change: " + "; ".join(problems))
    sys.stdout.write(terraform(run, ["apply", "-input=false", "-no-color", str(plan_file)], APPLY_TIMEOUT_SECONDS))
    return 0


def main() -> int:
    """Save a plan or apply the reviewed plan and return a non-zero code on a refusal or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["plan", "apply"])
    parser.add_argument("--destroy", action="store_true", help="plan: save a destroy plan")
    parser.add_argument("--confirm", default="", help="apply: the confirmation code that plan printed")
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        configuration = load_configuration()
        run = load_run(arguments.binding)
        if arguments.command == "plan":
            save_plan(run, configuration, "destroy" if arguments.destroy else "apply")
            return 0
        return apply_plan(run, arguments.confirm)
    except (ConfigurationError, WrapperError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
