"""Plan and run the guarded teardown of the resources this run created.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make teardown-plan`` saves the destroy plan and records the approved inventory: every object version and delete
marker in the run's application buckets and in its state bucket. It prints a confirmation code for the owner.
``make host-destroy CONFIRM=<CODE>`` runs every guard first and refuses, before any deletion, when one fails:

1. The owner's signed receipt must cover the current evidence of this run and commit.
2. The saved destroy plan must be the reviewed one, saved for this run's state location.
3. Every delete target must be a resource in this run's inventory. A host that existed before the run is refused,
   and so is any host in a run that was given its host and any state bucket under the local backend.
4. Every entry in an application bucket must match an approved bucket, key and version ID.
5. Every entry in the state bucket must be at one of this run's state keys and match an approved key and version ID.

It then deletes the approved application entries by key and version ID, applies the saved destroy plan, records the
state entries that the destroy wrote, repeats the state checks, deletes the approved and recorded state entries,
removes the empty state bucket last and checks each removal through its service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.aws_cli import Aws, AwsError
from scripts.evidence.manifest import build_manifest, manifest_problems, manifest_sha256
from scripts.evidence.receipt import receipt_problems, signature_is_valid, trusted_signers
from scripts.infra.state_bootstrap import DEFAULT_BINDING, MISSING_BUCKET, RunBinding
from scripts.infra.terraform_wrapper import (
    APPLY_TIMEOUT_SECONDS,
    PLAN_TIMEOUT_SECONDS,
    WrapperError,
    approved_plan_copy,
    binding_problems,
    load_run,
    save_plan,
    terraform,
)
from scripts.project_config import ROOT, ConfigurationError, load_configuration

GONE_DEADLINE_SECONDS = 120
GONE_POLL_SECONDS = 5
Target = tuple[str, str, str]


class TeardownRefused(RuntimeError):
    """A guard failed before any deletion, or a later check stopped the teardown."""


def delete_targets(plan: dict[str, Any]) -> list[Target]:
    """The address, type and service ID of every resource the plan deletes."""
    changes = [change for change in plan.get("resource_changes", []) if "delete" in change["change"]["actions"]]
    return [(change["address"], change["type"], str((change["change"].get("before") or {}).get("id", ""))) for change in changes]


def target_problems(targets: list[Target], inventory: list[dict[str, str]], run: RunBinding, pre_provisioned: list[str]) -> list[str]:
    """Each delete target or state bucket that this run does not own."""
    owned = {(entry["type"], entry["service_id"]) for entry in inventory}
    state_buckets = [entry["service_id"] for entry in inventory if entry["address"] == "state bucket"]
    problems = []
    for address, kind, identifier in targets:
        if kind == "aws_instance" and not run.provision_host:
            problems.append(f"delete target is a host in a run that did not create its host: {address}")
        elif kind == "aws_instance" and identifier in pre_provisioned:
            problems.append(f"delete target is a host that existed before this run: {address}")
        elif (kind, identifier) not in owned:
            problems.append(f"delete target outside this run's inventory: {address}")
    if run.backend == "local" and state_buckets:
        problems.append("a state bucket is named for removal under the local backend")
    if run.backend == "s3" and run.state_bucket_creation != "created":
        problems.append("the state bucket was not created by this run")
    elif run.backend == "s3" and state_buckets != [run.state_bucket]:
        problems.append("the bound state bucket is not the state bucket in this run's inventory")
    return problems


def bucket_entry_problems(bucket: str, entries: list[dict[str, Any]], approved: list[list[str]]) -> list[str]:
    """Each object version or delete marker in an application bucket with no approved bucket, key and version ID match."""
    allowed = {(item[0], item[1], item[2]) for item in approved}
    problems = []
    for entry in entries:
        if entry["VersionId"] in ("", "null"):
            problems.append(f"{bucket}: entry with no version ID: {entry['Key']}")
        elif (bucket, entry["Key"], entry["VersionId"]) not in allowed:
            problems.append(f"{bucket}: entry outside the approved inventory: {entry['Key']}")
    return problems


def state_entry_problems(entries: list[dict[str, Any]], state_keys: list[str], approved: list[list[str]], recorded: list[list[str]] | None = None) -> list[str]:
    """Each state bucket entry at a foreign key, or with no approved or recorded key and version ID match."""
    allowed = {(item[0], item[1]) for item in [*approved, *(recorded or [])]}
    problems = []
    for entry in entries:
        if entry["Key"] not in state_keys:
            problems.append(f"state bucket: key outside this run's state keys: {entry['Key']}")
        elif (entry["Key"], entry["VersionId"]) not in allowed:
            problems.append(f"state bucket: version outside the approved inventory: {entry['Key']}")
    return problems


def bucket_entries(aws: Aws, bucket: str) -> list[dict[str, Any]]:
    listing = aws.call("s3api", "list-object-versions", "--bucket", bucket)
    return [*listing.get("Versions", []), *listing.get("DeleteMarkers", [])]


def application_buckets(inventory: list[dict[str, str]]) -> list[str]:
    return sorted(entry["service_id"] for entry in inventory if entry["type"] == "aws_s3_bucket" and entry["address"] != "state bucket")


def teardown_code(plan_hash: str, approved: dict[str, Any]) -> str:
    return hashlib.sha256((plan_hash + json.dumps(approved, sort_keys=True)).encode()).hexdigest()[:12]


def read_json(path: Path, what: str) -> Any:
    if not path.is_file():
        raise TeardownRefused(f"{what} is missing: {path.relative_to(ROOT)}")
    return json.loads(path.read_text())


def plan_teardown(aws: Aws, configuration: dict[str, str], run: RunBinding) -> int:
    folder = ROOT / "runs" / run.run_id
    inventory = read_json(folder / "inventory.json", "the run's inventory")["created"]
    save_plan(run, configuration, "destroy")
    approved = {
        "application": [[bucket, entry["Key"], entry["VersionId"]] for bucket in application_buckets(inventory) for entry in bucket_entries(aws, bucket)]
    }
    approved["state"] = [[entry["Key"], entry["VersionId"]] for entry in bucket_entries(aws, run.state_bucket)] if run.backend == "s3" else []
    (folder / "teardown").mkdir(exist_ok=True)
    (folder / "teardown" / "approved_inventory.json").write_text(json.dumps(approved, indent=2) + "\n")
    plan_hash = read_json(folder / "plans" / "destroy_binding.json", "the destroy plan's binding")["plan_sha256"]
    counts = f"{len(approved['application'])} application entries and {len(approved['state'])} state entries"
    sys.stdout.write(f"run {run.run_id}: approved inventory recorded: {counts}\n")
    sys.stdout.write(
        f"after the evidence is exported and verified, destroy exactly this with: make host-destroy CONFIRM={teardown_code(plan_hash, approved)}\n"
    )
    return 0


def read_bytes(path: Path) -> bytes | None:
    return path.read_bytes() if path.is_file() else None


def guard_problems(
    aws: Aws, run: RunBinding, folder: Path, confirm: str, plan: bytes, approved_plan: Path
) -> tuple[list[str], dict[str, Any], list[dict[str, str]]]:
    inventory_record = read_json(folder / "inventory.json", "the run's inventory")
    inventory = inventory_record["created"]
    approved = read_json(folder / "teardown" / "approved_inventory.json", "the approved inventory")
    saved = read_json(folder / "plans" / "destroy_binding.json", "the destroy plan's binding")
    receipt = read_bytes(folder / "export" / "receipt.json")
    signature = read_bytes(folder / "export" / "receipt.json.sig")
    evidence = build_manifest(folder, run.run_id, run.commit)
    current = manifest_sha256(evidence)
    problems = manifest_problems(evidence)
    problems += receipt_problems(receipt, signature_is_valid(receipt, signature, trusted_signers(run)), run, current)
    problems += binding_problems(run, saved, plan, saved["plan_sha256"][:12], "destroy")
    if confirm != teardown_code(saved["plan_sha256"], approved):
        problems.append("the confirmation code does not equal the reviewed destroy plan and approved inventory")
    if inventory_record.get("authorized_retained"):
        problems.append("this teardown does not yet keep retained items: remove them from the destroy plan first")
    pre_provisioned = json.loads((folder / "pre_provisioned.json").read_text()) if (folder / "pre_provisioned.json").is_file() else []
    saved_plan = json.loads(terraform(run, ["show", "-json", str(approved_plan)], PLAN_TIMEOUT_SECONDS))
    problems += target_problems(delete_targets(saved_plan), inventory, run, pre_provisioned)
    for bucket in application_buckets(inventory):
        problems += bucket_entry_problems(bucket, bucket_entries(aws, bucket), approved["application"])
    if run.backend == "s3":
        problems += state_entry_problems(bucket_entries(aws, run.state_bucket), run.state_keys, approved["state"])
    return problems, approved, inventory


def delete_entries(aws: Aws, bucket: str, pairs: list[tuple[str, str]]) -> None:
    for key, version in pairs:
        aws.call("s3api", "delete-object", "--bucket", bucket, "--key", key, "--version-id", version)


def wait_gone(aws: Aws, bucket: str) -> bool:
    deadline = time.monotonic() + GONE_DEADLINE_SECONDS
    while time.monotonic() < deadline:
        if aws.call("s3api", "head-bucket", "--bucket", bucket, missing=MISSING_BUCKET) is None:
            return True
        time.sleep(GONE_POLL_SECONDS)
    return False


def removal_check(aws: Aws, entry: dict[str, str]) -> str:
    """What the service reports for one inventory entry after the teardown: ``gone`` or what remains."""
    kind, identifier = entry["type"], entry["service_id"]
    if kind == "aws_s3_bucket":
        return "gone" if aws.call("s3api", "head-bucket", "--bucket", identifier, missing=MISSING_BUCKET) is None else "still exists"
    if kind == "aws_instance":
        state = aws.call(
            "ec2",
            "describe-instances",
            "--instance-ids",
            identifier,
            "--query",
            "Reservations[].Instances[].State.Name",
            missing=("InvalidInstanceID.NotFound",),
        )
        return "gone" if state in (None, [], ["terminated"]) else f"still exists: {state}"
    if kind == "aws_security_group":
        return "gone" if aws.call("ec2", "describe-security-groups", "--group-ids", identifier, missing=("InvalidGroup.NotFound",)) is None else "still exists"
    if kind == "aws_iam_role":
        return "gone" if aws.call("iam", "get-role", "--role-name", identifier, missing=("NoSuchEntity",)) is None else "still exists"
    if kind == "aws_iam_instance_profile":
        return "gone" if aws.call("iam", "get-instance-profile", "--instance-profile-name", identifier, missing=("NoSuchEntity",)) is None else "still exists"
    return "removed with its parent resource"


def settled_checks(check: Callable[[dict[str, str]], str], inventory: list[dict[str, str]], *, attempts: int, pause_seconds: float) -> list[dict[str, str]]:
    """Each inventory entry with what its service reports, asked again while a removal is still reported as existing."""
    reported = {entry["address"]: check(entry) for entry in inventory}
    for _ in range(attempts - 1):
        late = [entry for entry in inventory if reported[entry["address"]].startswith("still exists")]
        if not late:
            break
        time.sleep(pause_seconds)
        reported.update({entry["address"]: check(entry) for entry in late})
    return [entry | {"reported": reported[entry["address"]]} for entry in inventory]


def destroy(aws: Aws, run: RunBinding, confirm: str) -> int:
    folder = ROOT / "runs" / run.run_id
    plan = read_bytes(folder / "plans" / "destroy.tfplan")
    if plan is None:
        raise TeardownRefused("the saved destroy plan is missing: run make teardown-plan")
    approved_plan = approved_plan_copy(run, plan)
    try:
        return destroy_with_plan(aws, run, folder, confirm, plan, approved_plan)
    finally:
        approved_plan.unlink(missing_ok=True)


def destroy_with_plan(aws: Aws, run: RunBinding, folder: Path, confirm: str, plan: bytes, approved_plan: Path) -> int:
    problems, approved, inventory = guard_problems(aws, run, folder, confirm, plan, approved_plan)
    if problems:
        raise TeardownRefused("refused before any deletion:\n" + "\n".join(problems))
    for bucket in application_buckets(inventory):
        delete_entries(aws, bucket, [(item[1], item[2]) for item in approved["application"] if item[0] == bucket])
    sys.stdout.write(terraform(run, ["apply", "-input=false", "-no-color", str(approved_plan)], APPLY_TIMEOUT_SECONDS)[-1500:])
    removal = folder / "removal"
    removal.mkdir(exist_ok=True)
    if run.backend == "s3":
        entries = bucket_entries(aws, run.state_bucket)
        known = {(item[0], item[1]) for item in approved["state"]}
        recorded = [
            [entry["Key"], entry["VersionId"]] for entry in entries if entry["Key"] in run.state_keys and (entry["Key"], entry["VersionId"]) not in known
        ]
        (removal / "destroy_written_state_entries.json").write_text(json.dumps(recorded, indent=2) + "\n")
        left = state_entry_problems(entries, run.state_keys, approved["state"], recorded)
        if left:
            raise TeardownRefused("the environment is gone, and the state bucket is kept because:\n" + "\n".join(left))
        delete_entries(aws, run.state_bucket, [(entry["Key"], entry["VersionId"]) for entry in entries])
        aws.call("s3api", "delete-bucket", "--bucket", run.state_bucket)
        wait_gone(aws, run.state_bucket)
    checks = settled_checks(
        lambda entry: removal_check(aws, entry), inventory, attempts=GONE_DEADLINE_SECONDS // GONE_POLL_SECONDS, pause_seconds=GONE_POLL_SECONDS
    )
    remaining = [check for check in checks if check["reported"].startswith("still exists")]
    (removal / "checks.json").write_text(
        json.dumps({"run_id": run.run_id, "checks": checks, "retained_cost": "none: no resource is retained"}, indent=2) + "\n"
    )
    for check in checks:
        sys.stdout.write(f"{check['reported']}: {check['address']}\n")
    if remaining:
        raise TeardownRefused(f"{len(remaining)} resources still exist after the teardown; see {(removal / 'checks.json').relative_to(ROOT)}")
    sys.stdout.write(f"run {run.run_id}: every removal is confirmed through its service\n")
    return 0


def main() -> int:
    """Plan or run the teardown and return a non-zero code on a refusal or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["plan", "destroy"])
    parser.add_argument("--confirm", default="", help="destroy: the confirmation code that plan printed")
    parser.add_argument("--binding", type=Path, default=DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        run = load_run(arguments.binding)
        return plan_teardown(aws, configuration, run) if arguments.command == "plan" else destroy(aws, run, arguments.confirm)
    except (ConfigurationError, WrapperError, TeardownRefused, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
