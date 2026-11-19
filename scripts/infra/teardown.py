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

It then empties, by key and version ID, only the buckets that the saved plan deletes, applies the exact plan bytes it
checked, and accepts a new state bucket entry only when its content shows the destroy of this state wrote it. It
deletes the approved and recorded state entries, removes the empty state bucket last and checks each removal
through its service; a resource with no check is verified only through a parent that is gone. Each finished stage is
recorded, so a second run does nothing and an interrupted run resumes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
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
    save_raw,
    terraform,
)
from scripts.project_config import ROOT, ConfigurationError, load_configuration

GONE_DEADLINE_SECONDS = 120
GONE_POLL_SECONDS = 5
CHECK_TIMEOUT_SECONDS = 30
STAGES = ("purge", "apply", "state", "checks")
PARENT_TYPES = {"aws_iam_role_policy": "aws_iam_role", "aws_s3_bucket_versioning": "aws_s3_bucket", "aws_s3_bucket_public_access_block": "aws_s3_bucket"}
DESTROYED = re.compile(r"Resources: \d+ added, \d+ changed, (\d+) destroyed")
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
    approved: dict[str, Any] = {
        "application": [[bucket, entry["Key"], entry["VersionId"]] for bucket in application_buckets(inventory) for entry in bucket_entries(aws, bucket)]
    }
    approved["state"] = [[entry["Key"], entry["VersionId"]] for entry in bucket_entries(aws, run.state_bucket)] if run.backend == "s3" else []
    current_state = json.loads(terraform(run, ["state", "pull"], PLAN_TIMEOUT_SECONDS))
    approved["state_lineage"] = current_state["lineage"]
    approved["state_serial"] = current_state["serial"]
    approved["local_state_sha256"] = hashlib.sha256(Path(run.state_keys[0]).read_bytes()).hexdigest() if run.backend == "local" else ""
    (folder / "teardown").mkdir(exist_ok=True)
    (folder / "teardown" / "approved_inventory.json").write_text(json.dumps(approved, indent=2) + "\n")
    plan_hash = read_json(folder / "plans" / "destroy_binding.json", "the destroy plan's binding")["plan_sha256"]
    counts = f"{len(approved['application'])} application entries and {len(approved['state'])} state entries"
    sys.stdout.write(f"run {run.run_id}: approved inventory recorded: {counts}\n")
    sys.stdout.write(
        f"after the evidence is exported and verified, destroy exactly this with: make host-destroy CONFIRM={teardown_code(plan_hash, approved)}\n"
    )
    return 0


def destroy_write_problem(key: str, content: bytes | None, delete_marker: bool, state_keys: list[str], lineage: str, serial: int) -> str | None:
    """Why a state bucket entry that appeared after approval is not this run's own destroy write, or None when it is."""
    if key not in state_keys:
        return "an entry outside this run's state keys"
    try:
        record = json.loads(content or b"")
    except ValueError:
        record = None
    if key == state_keys[0]:
        if delete_marker:
            return "a delete marker at the state key"
        newer = isinstance(record, dict) and isinstance(record.get("serial"), int) and record["serial"] > serial
        own = newer and record.get("lineage") == lineage and record.get("resources") == []
        return None if own else "a state version that this run's destroy did not write"
    if delete_marker or (isinstance(record, dict) and str(record.get("Path", "")).endswith(state_keys[0])):
        return None
    return "a lock entry that is not for this run's state"


def purge_buckets(targets: list[Target], inventory: list[dict[str, str]]) -> list[str]:
    """The run's application buckets that the saved plan deletes; no other bucket is emptied."""
    deleted = {identifier for _, kind, identifier in targets if kind == "aws_s3_bucket"}
    return [bucket for bucket in application_buckets(inventory) if bucket in deleted]


def remaining_stages(finished: list[str]) -> list[str]:
    return [stage for stage in STAGES if stage not in finished]


def parent_check(entry: dict[str, str], reported: dict[tuple[str, str], str]) -> str:
    """The result for a part with no service check of its own: verified only when its parent was checked and is gone."""
    parent_type = PARENT_TYPES.get(entry["type"])
    if parent_type is None:
        return "unverified: no check for this type"
    parent = reported.get((parent_type, entry["service_id"].split(":")[0]))
    if parent is None:
        return "unverified: its parent was not checked"
    return "gone with its parent" if parent.startswith("gone") else "still exists: its parent remains"


def read_bytes(path: Path) -> bytes | None:
    return path.read_bytes() if path.is_file() else None


def guard_problems(aws: Aws, run: RunBinding, folder: Path, confirm: str, plan: bytes, approved_plan: Path) -> tuple[list[str], list[Target]]:
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
    targets = delete_targets(saved_plan)
    problems += target_problems(targets, inventory, run, pre_provisioned)
    for bucket in application_buckets(inventory):
        problems += bucket_entry_problems(bucket, bucket_entries(aws, bucket), approved["application"])
    if run.backend == "s3":
        problems += state_entry_problems(bucket_entries(aws, run.state_bucket), run.state_keys, approved["state"])
    elif hashlib.sha256(Path(run.state_keys[0]).read_bytes()).hexdigest() != approved["local_state_sha256"]:
        problems.append("the local state changed after the destroy plan was saved")
    return problems, targets


def purge(aws: Aws, bucket: str, approved: list[tuple[str, str]]) -> int:
    """Delete, by key and version ID, each approved entry that the bucket still holds, and return how many."""
    present = {(entry["Key"], entry["VersionId"]) for entry in bucket_entries(aws, bucket)}
    for key, version in sorted(present & set(approved)):
        aws.call("s3api", "delete-object", "--bucket", bucket, "--key", key, "--version-id", version)
    return len(present & set(approved))


def wait_gone(aws: Aws, bucket: str) -> bool:
    deadline = time.monotonic() + GONE_DEADLINE_SECONDS
    while (remaining := deadline - time.monotonic()) > 0:
        if aws.call("s3api", "head-bucket", "--bucket", bucket, missing=MISSING_BUCKET, timeout=remaining) is None:
            return True
        time.sleep(min(GONE_POLL_SECONDS, max(deadline - time.monotonic(), 0)))
    return False


def entry_content(aws: Aws, bucket: str, entry: dict[str, Any]) -> bytes:
    with tempfile.TemporaryDirectory(prefix="state_entry_") as scratch:
        target = Path(scratch) / "content"
        aws.call("s3api", "get-object", "--bucket", bucket, "--key", entry["Key"], "--version-id", entry["VersionId"], str(target))
        return target.read_bytes()


def clean_state_bucket(aws: Aws, run: RunBinding, approved: dict[str, Any], removal: Path) -> None:
    """Record each entry the destroy wrote, stop on any entry it did not write, then delete by key and version ID and remove the bucket."""
    listing = aws.call("s3api", "list-object-versions", "--bucket", run.state_bucket)
    markers = {(entry["Key"], entry["VersionId"]) for entry in listing.get("DeleteMarkers", [])}
    entries = [*listing.get("Versions", []), *listing.get("DeleteMarkers", [])]
    known = {(item[0], item[1]) for item in approved["state"]}
    recorded, foreign = [], []
    for entry in entries:
        pair = (entry["Key"], entry["VersionId"])
        if pair in known:
            continue
        is_marker = pair in markers
        content = None if is_marker or entry["Key"] not in run.state_keys else entry_content(aws, run.state_bucket, entry)
        problem = destroy_write_problem(entry["Key"], content, is_marker, run.state_keys, approved["state_lineage"], approved["state_serial"])
        if problem:
            foreign.append(f"state bucket: {problem}: {entry['Key']}")
        else:
            recorded.append([entry["Key"], entry["VersionId"]])
    (removal / "destroy_written_state_entries.json").write_text(json.dumps(recorded, indent=2) + "\n")
    left = foreign + state_entry_problems(entries, run.state_keys, approved["state"], recorded)
    if left:
        raise TeardownRefused("the environment is gone, and the state bucket is kept because:\n" + "\n".join(sorted(set(left))))
    for entry in entries:
        aws.call("s3api", "delete-object", "--bucket", run.state_bucket, "--key", entry["Key"], "--version-id", entry["VersionId"])
    aws.call("s3api", "delete-bucket", "--bucket", run.state_bucket)
    if not wait_gone(aws, run.state_bucket):
        raise TeardownRefused(f"S3 still reports the state bucket {GONE_DEADLINE_SECONDS} seconds after its deletion")


def removal_check(aws: Aws, entry: dict[str, str]) -> str | None:
    """What the service reports for one inventory entry after the teardown, or None for a type with no service check of its own."""
    kind, identifier = entry["type"], entry["service_id"]
    lookups: dict[str, tuple[list[str], tuple[str, ...]]] = {
        "aws_s3_bucket": (["s3api", "head-bucket", "--bucket", identifier], MISSING_BUCKET),
        "aws_security_group": (["ec2", "describe-security-groups", "--group-ids", identifier], ("InvalidGroup.NotFound",)),
        "aws_iam_role": (["iam", "get-role", "--role-name", identifier], ("NoSuchEntity",)),
        "aws_iam_instance_profile": (["iam", "get-instance-profile", "--instance-profile-name", identifier], ("NoSuchEntity",)),
        "aws_ebs_volume": (["ec2", "describe-volumes", "--volume-ids", identifier], ("InvalidVolume.NotFound",)),
        "aws_network_interface": (["ec2", "describe-network-interfaces", "--network-interface-ids", identifier], ("InvalidNetworkInterfaceID.NotFound",)),
    }
    if kind == "aws_instance":
        query = ["ec2", "describe-instances", "--instance-ids", identifier, "--query", "Reservations[].Instances[].State.Name"]
        state = aws.call(*query, missing=("InvalidInstanceID.NotFound",), timeout=CHECK_TIMEOUT_SECONDS)
        return "gone" if state in (None, [], ["terminated"]) else f"still exists: {state}"
    if kind not in lookups:
        return None
    arguments, missing = lookups[kind]
    return "gone" if aws.call(*arguments, missing=missing, timeout=CHECK_TIMEOUT_SECONDS) is None else "still exists"


def checked_removals(aws: Aws, inventory: list[dict[str, str]]) -> list[dict[str, str]]:
    """Every inventory entry with its result: a service check where one exists, else its parent's, else unverified."""
    direct = [entry for entry in inventory if removal_check(aws, entry) is not None]
    settled = settled_checks(
        lambda entry: removal_check(aws, entry) or "", direct, attempts=GONE_DEADLINE_SECONDS // GONE_POLL_SECONDS, pause_seconds=GONE_POLL_SECONDS
    )
    reported = {(check["type"], check["service_id"]): check["reported"] for check in settled}
    by_address = {check["address"]: check for check in settled}
    return [by_address.get(entry["address"]) or entry | {"reported": parent_check(entry, reported)} for entry in inventory]


def finished_stages(removal: Path) -> list[str]:
    progress = removal / "progress.json"
    finished: list[str] = json.loads(progress.read_text()) if progress.is_file() else []
    return finished


def finish_stage(removal: Path, stage: str) -> None:
    (removal / "progress.json").write_text(json.dumps([*finished_stages(removal), stage]) + "\n")


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
    """Run the stages that are left. A finished teardown does nothing, and an interrupted one resumes after its last finished stage."""
    folder = ROOT / "runs" / run.run_id
    removal = folder / "removal"
    left = remaining_stages(finished_stages(removal))
    if not left:
        sys.stdout.write(f"run {run.run_id}: the teardown is already complete; nothing was changed\n")
        return 0
    inventory = read_json(folder / "inventory.json", "the run's inventory")["created"]
    approved = read_json(folder / "teardown" / "approved_inventory.json", "the approved inventory")
    if "apply" in left:
        plan = read_bytes(folder / "plans" / "destroy.tfplan")
        if plan is None:
            raise TeardownRefused("the saved destroy plan is missing: run make teardown-plan")
        approved_plan = approved_plan_copy(run, plan)
        try:
            problems, targets = guard_problems(aws, run, folder, confirm, plan, approved_plan)
            if problems:
                raise TeardownRefused("refused before any deletion:\n" + "\n".join(problems))
            removal.mkdir(exist_ok=True)
            if "purge" in left:
                for bucket in purge_buckets(targets, inventory):
                    purge(aws, bucket, [(item[1], item[2]) for item in approved["application"] if item[0] == bucket])
                finish_stage(removal, "purge")
            output = terraform(run, ["apply", "-input=false", "-no-color", str(approved_plan)], APPLY_TIMEOUT_SECONDS)
            save_raw(run.run_id, "destroy_apply.txt", output)
            destroyed = DESTROYED.search(output)
            sys.stdout.write(
                f"run {run.run_id}: the destroy plan is applied; resources destroyed: {destroyed.group(1) if destroyed else 'see the raw output'}\n"
            )
            finish_stage(removal, "apply")
        finally:
            approved_plan.unlink(missing_ok=True)
    if "state" in left:
        if run.backend == "s3":
            clean_state_bucket(aws, run, approved, removal)
        finish_stage(removal, "state")
    checks = checked_removals(aws, inventory)
    (removal / "checks.json").write_text(
        json.dumps({"run_id": run.run_id, "checks": checks, "retained_cost": "none: no resource is retained"}, indent=2) + "\n"
    )
    for check in checks:
        sys.stdout.write(f"{check['reported']}: {check['address']}\n")
    failed = [check for check in checks if not check["reported"].startswith("gone")]
    if failed:
        raise TeardownRefused(f"{len(failed)} removals are not confirmed; see {(removal / 'checks.json').relative_to(ROOT)}")
    finish_stage(removal, "checks")
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
