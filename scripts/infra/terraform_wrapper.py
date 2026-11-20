"""Save a Terraform plan for this run and apply exactly the plan that was reviewed.

Run from the repository root under the project profile in ``.env``:

``.venv/bin/python -m scripts.infra.terraform_wrapper plan``
``.venv/bin/python -m scripts.infra.terraform_wrapper apply --confirm <CODE>``

``plan`` selects the backend from the run binding and the environment's variable file, saves the plan under
``runs/<RUN_ID>/plans/`` with its readable text and its JSON form and binds the plan file's hash to the run's state location. It prints
the resource changes and a confirmation code. ``apply`` refuses a changed plan file, a wrong code and a plan saved for
another state location, then applies the exact bytes it checked. Each run has its own copy of the configuration, and
no Terraform variable of the caller reaches Terraform. ``plan --destroy`` saves a destroy plan the same way; this
command never applies one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.infra.created import created_entries, merge_created
from scripts.infra.state_bootstrap import DEFAULT_BINDING, RunBinding, bucket_problem
from scripts.process import TimeoutExpired, partial_text, run_command, timeout_text
from scripts.project_config import ROOT, ConfigurationError, load_configuration

INIT_TIMEOUT_SECONDS = 600
PLAN_TIMEOUT_SECONDS = 900
APPLY_TIMEOUT_SECONDS = 1800
TAILSCALE_KEYS = ("TAILSCALE_CLIENT_ID", "TAILSCALE_AUDIENCE")


PRIVATE_VALUES = ((re.compile(r"\b\d{12}\b"), "<ACCOUNT>"), (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<ADDRESS>"))


def sanitized(text: str) -> str:
    """The text with account numbers and network addresses replaced, for printing and for error messages."""
    for pattern, replacement in PRIVATE_VALUES:
        text = pattern.sub(replacement, text)
    return text


def save_raw(run_id: str, name: str, text: str) -> None:
    """Keep raw tool output in an owner-only file that the evidence export leaves out."""
    raw = ROOT / "runs" / run_id / "raw"
    raw.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(raw / name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)


def kept_note(run_id: str, name: str, text: str, what: str) -> str:
    """Keep ``text`` privately as ``name`` and return the clause that tells where it is, or that it could not be kept. A failed write hides no error."""
    try:
        save_raw(run_id, name, text)
    except OSError:
        return f"its {what} could not be kept"
    return f"its {what} is in runs/{run_id}/raw/{name}"


class WrapperError(RuntimeError):
    """The wrapper refused to continue or a Terraform call failed."""


CONFIGURATION_PARTS = ("modules", "environments")
BOUND_BACKEND_KEYS = {"s3": ("bucket", "key", "region"), "local": ("path",)}


def backend_override(run: RunBinding) -> str | None:
    """The Terraform override that selects the local backend, or None for the S3 backend that the configuration names."""
    if run.backend != "local":
        return None
    return f'terraform {{\n  backend "local" {{\n    path = "{run.state_keys[0]}"\n  }}\n}}\n'


def run_variables(run: RunBinding, configuration: dict[str, str]) -> dict[str, str]:
    """The Terraform variables that come from the run and not from the environment's variable file."""
    return {"data_bucket_name": run.bucket, "aws_profile": configuration["AWS_PROFILE"], "provision_host": "true" if run.provision_host else "false"}


def backend_arguments(run: RunBinding, profile: str) -> list[str]:
    if run.backend == "local":
        return []
    backend = {"bucket": run.bucket, "key": run.state_keys[0], "region": run.region, "profile": profile, "use_lockfile": "true"}
    return [f"-backend-config={key}={value}" for key, value in backend.items()]


def state_location(run: RunBinding) -> dict[str, Any]:
    return {"backend": run.backend, "bucket": run.bucket, "workspace": run.workspace, "keys": run.state_keys, "region": run.region}


def clean_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """The caller's environment without any Terraform variable, so none can change the workspace, the inputs or the arguments."""
    return {name: value for name, value in environment.items() if not name.startswith("TF_")} | {"TF_IN_AUTOMATION": "1"}


def effective_location_problems(run: RunBinding, backend_record: dict[str, Any], workspace: str) -> list[str]:
    """Each difference between the backend and workspace Terraform initialised and this run's binding."""
    backend = backend_record.get("backend", {})
    kind = backend.get("type", "missing")
    if kind != run.backend:
        return [f"Terraform's backend type is {kind}, not {run.backend}"]
    bound = {"bucket": run.bucket, "key": run.state_keys[0], "region": run.region, "path": run.state_keys[0]}
    problems = [
        f"Terraform's backend {name} differs from this run's binding" for name in BOUND_BACKEND_KEYS[kind] if backend.get("config", {}).get(name) != bound[name]
    ]
    if workspace != run.workspace:
        problems.append(f"Terraform selected the workspace {workspace}, not {run.workspace}")
    return problems


def configuration_folder(run: RunBinding) -> Path:
    return ROOT / ".tools" / "terraform" / run.run_id / "config"


def configuration_files(source: Path) -> list[Path]:
    """The top-level files a run's copy takes: the Terraform files and the provider lock, and no variable file that Terraform would load unasked."""
    return [*sorted(item for item in source.glob("*.tf") if item.name != "backend_override.tf"), source / ".terraform.lock.hcl"]


def prepare_configuration(run: RunBinding) -> Path:
    """Give the run its own copy of the Terraform configuration with its own backend override, apart from every other run."""
    target = configuration_folder(run)
    target.mkdir(parents=True, exist_ok=True)
    for stale in target.iterdir():
        if stale.name != ".terraform":
            shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
    source = ROOT / "infra"
    for item in configuration_files(source):
        shutil.copy2(item, target / item.name)
    for part in CONFIGURATION_PARTS:
        shutil.copytree(source / part, target / part, ignore=shutil.ignore_patterns("*.auto.tfvars", "*.auto.tfvars.json"))
    override = backend_override(run)
    if override is not None:
        (target / "backend_override.tf").write_text(override)
    return target


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
    command = [f"-chdir={configuration_folder(run)}", *arguments]
    name = f"terraform_{arguments[0]}_failure.txt"
    try:
        result = run_command("terraform", command, cwd=ROOT, env=clean_environment(os.environ), timeout=timeout)
    except TimeoutExpired as error:
        partial = partial_text(error.stdout) + partial_text(error.stderr)
        note = f"; {kept_note(run.run_id, name, partial, 'partial output')}" if partial else ""
        raise WrapperError(timeout_text(f"terraform {arguments[0]}", timeout) + note) from None
    if result.returncode != 0:
        kept = kept_note(run.run_id, name, result.stdout + result.stderr, "full output")
        raise WrapperError(f"terraform {arguments[0]} failed with exit status {result.returncode}; {kept}")
    return result.stdout


def load_run(path: Path) -> RunBinding:
    if not path.is_file():
        raise WrapperError("no run binding: run the state bootstrap step first")
    return RunBinding(**json.loads(path.read_text()))


def initialise(run: RunBinding, configuration: dict[str, str]) -> None:
    """Initialise the run's own configuration copy and refuse when Terraform's backend or workspace is not the bound one."""
    mode_problem = bucket_problem(run, configuration)
    if mode_problem:
        raise WrapperError(f"refused before any plan or change: {mode_problem}")
    folder = prepare_configuration(run)
    terraform(run, ["init", "-input=false", "-reconfigure", *backend_arguments(run, configuration["AWS_PROFILE"])], INIT_TIMEOUT_SECONDS)
    record_file = folder / ".terraform" / "terraform.tfstate"
    record = json.loads(record_file.read_text()) if record_file.is_file() else {}
    problems = effective_location_problems(run, record, terraform(run, ["workspace", "show"], PLAN_TIMEOUT_SECONDS).strip())
    if problems:
        raise WrapperError("refused before any plan or change: " + "; ".join(problems))


def approved_plan_copy(run: RunBinding, plan: bytes) -> Path:
    """Write the exact bytes that were checked to an owner-only file of this invocation alone, so the plan shown and applied is the plan checked."""
    descriptor, name = tempfile.mkstemp(dir=configuration_folder(run), prefix="approved_", suffix=".tfplan")
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(plan)
    return Path(name)


def write_private(path: Path, content: str | bytes) -> None:
    """Write a raw plan file that only its owner can read, whatever mode an earlier file had."""
    path.unlink(missing_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content.encode() if isinstance(content, str) else content)


def state_json(run: RunBinding, configuration: dict[str, str]) -> dict[str, Any]:
    """The run's current Terraform state, read from its bound backend."""
    initialise(run, configuration)
    state: dict[str, Any] = json.loads(terraform(run, ["show", "-json"], PLAN_TIMEOUT_SECONDS))
    return state


def plan_label(action: str, *, changes: int, reviewed_exists: bool) -> str:
    """The name a saved plan gets: a later plan with no change is kept beside the reviewed plan, not over it."""
    return f"{action}_check" if changes == 0 and reviewed_exists else action


def save_plan(run: RunBinding, configuration: dict[str, str], action: str) -> int:
    """Save the plan with its text, JSON and binding, print its changes and return how many resources change."""
    if run.status != "verified":
        raise WrapperError("the run's bucket is not verified: run the state bootstrap step")
    folder = ROOT / "runs" / run.run_id / "plans"
    folder.mkdir(parents=True, exist_ok=True)
    folder.chmod(0o700)
    candidate = folder / f"{action}_candidate.tfplan"
    initialise(run, configuration)
    missing = sorted(key for key in TAILSCALE_KEYS if not configuration.get(key))
    if missing:
        raise WrapperError(f".env: missing values for: {', '.join(missing)}")
    values = run_variables(run, configuration) | {key.lower(): configuration[key] for key in TAILSCALE_KEYS}
    variables = [f"-var-file=environments/{configuration['ENVIRONMENT']}.tfvars", *[f"-var={name}={value}" for name, value in values.items()]]
    terraform(run, ["plan", "-input=false", "-no-color", *(["-destroy"] if action == "destroy" else []), *variables, f"-out={candidate}"], PLAN_TIMEOUT_SECONDS)
    plan_json = terraform(run, ["show", "-json", str(candidate)], PLAN_TIMEOUT_SECONDS)
    counts: dict[str, int] = {}
    for change in json.loads(plan_json).get("resource_changes", []):
        actions = "/".join(change["change"]["actions"])
        if actions != "no-op":
            counts[actions] = counts.get(actions, 0) + 1
            sys.stdout.write(f"{actions}: {change['address']}\n")
    label = plan_label(action, changes=sum(counts.values()), reviewed_exists=(folder / f"{action}.tfplan").is_file())
    plan_file = folder / f"{label}.tfplan"
    plan = candidate.read_bytes()
    candidate.unlink()
    write_private(plan_file, plan)
    write_private(folder / f"{label}.txt", terraform(run, ["show", "-no-color", str(plan_file)], PLAN_TIMEOUT_SECONDS))
    write_private(folder / f"{label}.json", plan_json)
    saved = plan_binding(run, plan, action)
    (folder / f"{label}_binding.json").write_text(json.dumps(saved, indent=2) + "\n")
    summary = ", ".join(f"{count} to {name}" for name, count in sorted(counts.items())) or "no change"
    sys.stdout.write(f"run {run.run_id}, {action} plan: {summary}\nfull plan text: {(folder / f'{label}.txt').relative_to(ROOT)}\n")
    if counts and action == "apply":
        sys.stdout.write(f"to apply exactly this plan after review: apply --confirm {saved['plan_sha256'][:12]}\n")
    return sum(counts.values())


def apply_plan(run: RunBinding, code: str, configuration: dict[str, str] | None = None) -> int:
    mode_problem = bucket_problem(run, configuration or load_configuration())
    if mode_problem:
        raise WrapperError(f"refused before any change: {mode_problem}")
    folder = ROOT / "runs" / run.run_id / "plans"
    plan_file = folder / "apply.tfplan"
    binding_file = folder / "apply_binding.json"
    if not plan_file.is_file() or not binding_file.is_file():
        raise WrapperError("no saved apply plan for this run: run plan first")
    plan = plan_file.read_bytes()
    problems = binding_problems(run, json.loads(binding_file.read_text()), plan, code, "apply")
    if problems:
        raise WrapperError("refused before any change: " + "; ".join(problems))
    approved = approved_plan_copy(run, plan)
    try:
        applied_plan = json.loads(terraform(run, ["show", "-json", str(approved)], PLAN_TIMEOUT_SECONDS))
        output = terraform(run, ["apply", "-input=false", "-no-color", str(approved)], APPLY_TIMEOUT_SECONDS)
        kept = kept_note(run.run_id, "apply_output.txt", output, "raw output")
        sys.stdout.write(next((line for line in output.splitlines() if line.startswith("Apply complete")), "the apply finished") + f"; {kept}\n")
    finally:
        approved.unlink(missing_ok=True)
    record_created(run, applied_plan)
    return 0


def record_created(run: RunBinding, applied_plan: dict[str, Any]) -> None:
    """Add what the applied plan created to the run's creation record, read from the state the apply left."""
    target = ROOT / "runs" / run.run_id / "created.json"
    recorded = json.loads(target.read_text()) if target.is_file() else []
    state: dict[str, Any] = json.loads(terraform(run, ["show", "-json"], PLAN_TIMEOUT_SECONDS))
    target.write_text(json.dumps(merge_created(recorded, created_entries(applied_plan, state)), indent=2) + "\n")


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
        return apply_plan(run, arguments.confirm, configuration)
    except (ConfigurationError, WrapperError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
