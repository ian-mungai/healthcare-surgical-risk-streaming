"""Make the project's bucket on AIStor with Terraform: save a plan, stop for review and apply exactly the reviewed plan.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make storage-up`` finds the host over Tailscale, reads AIStor's root user and password from the host's secret files over SSH into memory, saves a Terraform
plan of ``infra/storage`` and, when it changes anything, prints the changes and a confirmation code and stops for the owner's review.
``make storage-up CONFIRM=<CODE>`` applies exactly the reviewed plan. A run with nothing to change goes straight to the checks, so a second run changes nothing.
After a plan with no change or an apply, the command refuses a state file that holds either credential and asks AIStor, through the AWS command line,
whether the bucket's versioning is Enabled.

The two credentials reach Terraform only as the environment of its own process (the provider reads ``MINIO_USER`` and ``MINIO_PASSWORD``), never as an
argument, a variable, a plan, a state or a printed line. The state is the local file ``runs/<RUN_ID>/storage/terraform.tfstate``. Every wait has a deadline.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.infra.host import HostError, host_peer, ssh_output
from scripts.infra.state_bootstrap import DEFAULT_BINDING, RunBinding
from scripts.infra.terraform_wrapper import (
    APPLY_TIMEOUT_SECONDS,
    INIT_TIMEOUT_SECONDS,
    PLAN_TIMEOUT_SECONDS,
    WrapperError,
    clean_environment,
    kept_note,
    load_run,
    pinned_terraform,
    plan_changes,
    plan_label,
    sanitized,
    write_private,
)
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.folder import list_folder
from scripts.ingestion.notice import describe_error
from scripts.process import TimeoutExpired, partial_text, run_command, timeout_text
from scripts.project_config import ROOT, ConfigurationError, load_configuration
from scripts.stack.caddy import RoutesError, load_routes

SECRETS_DIRECTORY = "/opt/platform/secrets"
STORAGE_ROUTE = "aistor"
STORAGE_REGION = "us-east-1"
CREDENTIAL_FORM = re.compile(r"[A-Za-z0-9_-]{16,128}")
SSH_SECONDS = 30
CHECK_SECONDS = 60
WORKSPACE = "default"
SOURCE_FOLDER = Path("infra") / "storage"


class StorageError(RuntimeError):
    """The storage command refused to continue, a wait passed its deadline or a call failed. A message holds no credential."""


@dataclass(frozen=True)
class Credentials:
    """AIStor's root user and password, read from the host's files. Neither is in the text form of the object."""

    user: str = field(repr=False)
    password: str = field(repr=False)


def endpoint(address: str) -> str:
    """The host name and the port of the gateway's AIStor route, the address of the S3 API over Tailscale."""
    try:
        routes = [route for route in load_routes() if route.name == STORAGE_ROUTE]
    except RoutesError as error:
        raise StorageError(f"the gateway's route list cannot be read: {error}") from None
    if len(routes) != 1:
        raise StorageError(f"the gateway's route list must hold exactly one route named {STORAGE_ROUTE}")
    return f"{address}:{routes[0].port}"


def read_credentials(run_id: str, address: str) -> Credentials:
    """Read the root user and the root password over SSH with sudo and check their form. A refusal is a fixed message that holds no value."""
    found: dict[str, str] = {}
    for name in ("root_user", "root_password"):
        answer = ssh_output(run_id, address, f"sudo -n cat {SECRETS_DIRECTORY}/aistor/{name}", SSH_SECONDS)
        value = (answer or "").removesuffix("\n")
        if CREDENTIAL_FORM.fullmatch(value) is None:
            raise StorageError(f"the host gave no valid AIStor {name.replace('_', ' ')}: the deploy creates the file, so run make host-up first")
        found[name] = value
    return Credentials(found["root_user"], found["root_password"])


def redact(text: str, credentials: Credentials) -> str:
    """The text with each credential, and each address, replaced."""
    for value in (credentials.password, credentials.user):
        text = text.replace(value, "<CREDENTIAL>")
    return sanitized(text)


def environment_for(credentials: Credentials) -> dict[str, str]:
    """The environment of a Terraform call: the caller's without any Terraform, MinIO or AWS variable, and with the provider's two."""
    kept = {name: value for name, value in clean_environment(os.environ).items() if not name.startswith(("MINIO_", "AWS_"))}
    return kept | {"MINIO_USER": credentials.user, "MINIO_PASSWORD": credentials.password}


def configuration_folder(run: RunBinding) -> Path:
    return ROOT / ".tools" / "terraform" / run.run_id / "storage"


def state_file(run: RunBinding) -> Path:
    return ROOT / "runs" / run.run_id / "storage" / "terraform.tfstate"


def plans_folder(run: RunBinding) -> Path:
    return ROOT / "runs" / run.run_id / "storage" / "plans"


def prepare_configuration(run: RunBinding) -> Path:
    """Give the run its own copy of the storage root: its Terraform files and its provider lock, and no test folder."""
    target = configuration_folder(run)
    target.mkdir(parents=True, exist_ok=True)
    for stale in list_folder(target):
        if stale.name != ".terraform":
            shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
    source = ROOT / SOURCE_FOLDER
    for item in [*list_folder(source, "*.tf"), source / ".terraform.lock.hcl"]:
        shutil.copy2(item, target / item.name)
    return target


def terraform(run: RunBinding, arguments: list[str], timeout: float, credentials: Credentials) -> str:
    """One call of the pinned Terraform in the run's copy. A failure keeps its output privately with the credentials replaced and names the file."""
    action = arguments[0]
    try:
        result = run_command(
            str(pinned_terraform()), [f"-chdir={configuration_folder(run)}", *arguments], cwd=ROOT, env=environment_for(credentials), timeout=timeout
        )
    except TimeoutExpired as error:
        partial = redact(partial_text(error.stdout) + partial_text(error.stderr), credentials)
        note = f"; {kept_note(run.run_id, f'storage_terraform_{action}_failure.txt', partial, 'partial output')}" if partial else ""
        raise StorageError(timeout_text(f"terraform {action}", timeout) + note) from None
    if result.returncode != 0:
        kept = kept_note(run.run_id, f"storage_terraform_{action}_failure.txt", redact(result.stdout + result.stderr, credentials), "full output")
        raise StorageError(f"terraform {action} failed with exit status {result.returncode}; {kept}")
    return result.stdout


def initialise(run: RunBinding, credentials: Credentials) -> None:
    """Initialise the run's copy with its local state file and refuse when Terraform's backend or workspace is not the bound one. The lock file is read-only."""
    folder = prepare_configuration(run)
    state = state_file(run)
    state.parent.mkdir(parents=True, exist_ok=True)
    state.parent.chmod(0o700)
    terraform(run, ["init", "-input=false", "-reconfigure", "-lockfile=readonly", f"-backend-config=path={state}"], INIT_TIMEOUT_SECONDS, credentials)
    record_file = folder / ".terraform" / "terraform.tfstate"
    backend = (json.loads(record_file.read_text()) if record_file.is_file() else {}).get("backend", {})
    workspace = terraform(run, ["workspace", "show"], PLAN_TIMEOUT_SECONDS, credentials).strip()
    problems = []
    if backend.get("type") != "local" or backend.get("config", {}).get("path") != str(state):
        problems.append("Terraform's backend is not this run's local state file")
    if workspace != WORKSPACE:
        problems.append(f"Terraform selected the workspace {workspace}, not {WORKSPACE}")
    if problems:
        raise StorageError("refused before any plan or change: " + "; ".join(problems))


def binding_record(run: RunBinding, plan: bytes, configuration: dict[str, str], address: str) -> dict[str, str]:
    """The record that ties a saved plan to its run, its endpoint and its bucket."""
    return {
        "plan_sha256": hashlib.sha256(plan).hexdigest(),
        "action": "apply",
        "run_id": run.run_id,
        "endpoint": endpoint(address),
        "bucket_name": configuration["S3_BUCKET"],
    }


def save_plan(run: RunBinding, configuration: dict[str, str], address: str, credentials: Credentials) -> int:
    """Save the plan with its text, JSON and binding, print its changes and return how many resources change."""
    folder = plans_folder(run)
    folder.mkdir(parents=True, exist_ok=True)
    folder.chmod(0o700)
    candidate = folder / "apply_candidate.tfplan"
    initialise(run, credentials)
    variables = [f"-var=endpoint={endpoint(address)}", f"-var=bucket_name={configuration['S3_BUCKET']}"]
    terraform(run, ["plan", "-input=false", "-no-color", *variables, f"-out={candidate}"], PLAN_TIMEOUT_SECONDS, credentials)
    plan_json = terraform(run, ["show", "-json", str(candidate)], PLAN_TIMEOUT_SECONDS, credentials)
    counts: dict[str, int] = {}
    for actions, name in plan_changes(json.loads(plan_json)):
        counts[actions] = counts.get(actions, 0) + 1
        sys.stdout.write(f"{actions}: {name}\n")
    label = plan_label("apply", changes=sum(counts.values()), reviewed_exists=(folder / "apply.tfplan").is_file())
    plan = candidate.read_bytes()
    candidate.unlink()
    write_private(folder / f"{label}.tfplan", plan)
    write_private(folder / f"{label}.txt", terraform(run, ["show", "-no-color", str(folder / f"{label}.tfplan")], PLAN_TIMEOUT_SECONDS, credentials))
    write_private(folder / f"{label}.json", plan_json)
    saved = binding_record(run, plan, configuration, address)
    write_private(folder / f"{label}_binding.json", json.dumps(saved, indent=2) + "\n")
    summary = ", ".join(f"{count} to {name}" for name, count in sorted(counts.items())) or "no change"
    sys.stdout.write(f"run {run.run_id}, storage plan: {summary}\nfull plan text: {(folder / f'{label}.txt').relative_to(ROOT)}\n")
    if counts:
        sys.stdout.write(f"to apply exactly this plan after review: apply --confirm {saved['plan_sha256'][:12]}\n")
    return sum(counts.values())


def binding_problems(run: RunBinding, saved: dict[str, Any], plan: bytes, code: str, configuration: dict[str, str], address: str) -> list[str]:
    """Each reason to refuse the saved plan, checked before Terraform runs."""
    problems = []
    if saved.get("run_id") != run.run_id:
        problems.append("the plan was saved for another run")
    if saved.get("endpoint") != endpoint(address) or saved.get("bucket_name") != configuration["S3_BUCKET"]:
        problems.append("the plan was saved for another endpoint or bucket than this run's")
    if hashlib.sha256(plan).hexdigest() != saved.get("plan_sha256"):
        problems.append("the saved plan file differs from the plan that was reviewed")
    elif code != str(saved["plan_sha256"])[:12]:
        problems.append("the confirmation code does not equal the reviewed plan")
    return problems


def state_leaks(text: str, credentials: Credentials) -> list[str]:
    """The names of the credentials that a state file's text holds, in the plain form, the JSON-escaped form or the hexadecimal form."""
    found = []
    for name, value in (("user", credentials.user), ("password", credentials.password)):
        if any(form in text for form in (value, json.dumps(value)[1:-1], value.encode().hex(), value.encode().hex().upper())):
            found.append(name)
    return found


def check_state(run: RunBinding, credentials: Credentials) -> None:
    """Refuse a state file that holds a credential. The message names no value and the state file stays for the owner's review."""
    state = state_file(run)
    leaks = state_leaks(state.read_text(), credentials) if state.is_file() else []
    if leaks:
        raise StorageError(
            f"the Terraform state of this run holds the root {' and '.join(leaks)}: do not use it; the file is runs/{run.run_id}/storage/terraform.tfstate"
        )


def apply_plan(run: RunBinding, code: str, configuration: dict[str, str], address: str, credentials: Credentials) -> int:
    """Apply the saved plan by its own bytes after the checks, and refuse a state that holds a credential afterwards."""
    folder = plans_folder(run)
    plan_file, binding_file = folder / "apply.tfplan", folder / "apply_binding.json"
    if not plan_file.is_file() or not binding_file.is_file():
        raise StorageError("no saved apply plan for this run: run the command without a code first")
    plan = plan_file.read_bytes()
    problems = binding_problems(run, json.loads(binding_file.read_text()), plan, code, configuration, address)
    if problems:
        raise StorageError("refused before any change: " + "; ".join(problems))
    approved = folder / f"approved_{os.getpid()}.tfplan"
    write_private(approved, plan)
    try:
        initialise(run, credentials)
        output = terraform(run, ["apply", "-input=false", "-no-color", str(approved)], APPLY_TIMEOUT_SECONDS, credentials)
        kept = kept_note(run.run_id, "storage_apply_output.txt", redact(output, credentials), "raw output")
        sys.stdout.write(next((line for line in output.splitlines() if line.startswith("Apply complete")), "the apply finished") + f"; {kept}\n")
    finally:
        approved.unlink(missing_ok=True)
    check_state(run, credentials)
    return 0


def check_versioning(address: str, bucket: str, credentials: Credentials) -> None:
    """Ask AIStor through the AWS command line, with the root pair in the call's environment only, and refuse any answer but Enabled."""
    kept = {name: value for name, value in os.environ.items() if not name.startswith(("AWS_", "MINIO_", "TF_"))}
    environment = kept | {
        "AWS_ACCESS_KEY_ID": credentials.user,
        "AWS_SECRET_ACCESS_KEY": credentials.password,
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    arguments = [
        "s3api",
        "get-bucket-versioning",
        "--bucket",
        bucket,
        "--endpoint-url",
        f"https://{endpoint(address)}",
        "--region",
        STORAGE_REGION,
        "--output",
        "json",
    ]
    try:
        result = run_command("aws", arguments, env=environment, timeout=CHECK_SECONDS)
    except FileNotFoundError:
        raise StorageError("the AWS command line is not installed or not on PATH, so the bucket's versioning cannot be read") from None
    except TimeoutExpired:
        raise StorageError(timeout_text("the versioning check", CHECK_SECONDS)) from None
    if result.returncode != 0:
        raise StorageError(
            f"the AWS command line could not read the versioning of the bucket {bucket} (exit status {result.returncode}); are the licence and AIStor in place?"
        )
    try:
        document = json.loads(result.stdout)
    except ValueError:
        document = None
    status = document.get("Status") if isinstance(document, dict) else None
    if status != "Enabled":
        shown = repr(status) if isinstance(status, str) and status else "no status"
        raise StorageError(f"the bucket {bucket} answers versioning {shown}, not Enabled")
    sys.stdout.write(f"versioning of the bucket {bucket} is Enabled\n")


def storage_up(configuration: dict[str, str], binding: Path, confirm: str) -> int:
    run = load_run(binding)
    host_name = f"platform-{configuration['ENVIRONMENT']}"
    try:
        address = host_peer(host_name).address
    except HostError as error:
        raise StorageError(f"the host {host_name} is not reachable over Tailscale: {error}") from None
    credentials = read_credentials(run.run_id, address)
    if confirm:
        apply_plan(run, confirm, configuration, address, credentials)
    elif save_plan(run, configuration, address, credentials):
        sys.stdout.write("stopped for review: read the full plan text, then run make storage-up CONFIRM=<the code above>\n")
        return 0
    else:
        check_state(run, credentials)
    check_versioning(address, configuration["S3_BUCKET"], credentials)
    sys.stdout.write(f"run {run.run_id}: the bucket {configuration['S3_BUCKET']} on AIStor is up with versioning Enabled\n")
    return 0


def main() -> int:
    """Plan or apply the storage root and return a non-zero code on a refusal, an expired deadline or a failure."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["up"])
    parser.add_argument("--confirm", default="", help="up: the confirmation code of the reviewed plan")
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        return storage_up(load_configuration(), arguments.binding, arguments.confirm)
    except (ConfigurationError, WrapperError, HostError, StorageError) as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
