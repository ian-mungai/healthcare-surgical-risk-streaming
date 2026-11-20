"""Plan and run the guarded teardown of the resources this run created, at the end of the project.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make teardown-plan`` saves the destroy plan and records the approved inventory: every object version and delete
marker under the data prefixes, and every entry at this run's state keys. It refuses when the state changed while the
plan was saved. It prints a confirmation code for the owner. ``make host-destroy CONFIRM=<CODE>`` runs every guard
first and refuses, before any deletion, when one fails:

1. The owner's signed receipt must cover the current evidence of this run and commit, and the approval files that
   were checked must be the files the evidence lists.
2. The saved destroy plan must be the reviewed one, saved for this run's state location.
3. Every delete target must be a resource in this run's inventory and never a bucket. A host that existed before the
   run is refused, and so is any host in a run that was given its host, a host with no recorded Tailscale device and
   a bucket this run did not create.
4. Every entry under the data prefixes must match an approved key and version ID.
5. Every entry at this run's state keys must match an approved key and version ID.
6. A bucket this run created must hold nothing else. In a bucket this run did not create, any other entry, such as
   another environment's state, is left alone and is never approved or deleted.

The guards also refuse an approved entry in another bucket, outside the data prefixes or outside this run's state
keys, and a state whose lineage or serial is not the one the destroy plan was saved for.

It then logs the host out of Tailscale and waits until its device has left the network, applies the exact plan
bytes it checked and records that stage at once, and only then deletes the approved data entries by key and version
ID. Every delete call names this account as the bucket's expected owner. Under the state lock, which it takes with a
write that fails when a lock exists, it accepts a new entry at this run's state keys only when its content shows the
destroy of this state wrote it, deletes this run's state entries and removes the empty bucket last when this run
created it and this account still owns it. A lock that a stopped cleanup of this run left on this computer is taken
over; any other lock stops the cleanup. It checks each removal through its service within one deadline; a resource
with no check is verified only through a parent that is gone. Each finished stage is recorded with the approval the
teardown started with: a second run does nothing, and an interrupted run resumes only with that same code, plan,
inventory, approved inventory and receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.aws_cli import Aws, AwsError
from scripts.evidence.manifest import build_manifest, manifest_problems, manifest_sha256
from scripts.evidence.receipt import receipt_problems, signature_is_valid, trusted_signers
from scripts.infra import host
from scripts.infra.created import BUCKET_ADDRESS, DEVICE_TYPE
from scripts.infra.state_bootstrap import DEFAULT_BINDING, MISSING_BUCKET, RunBinding, bucket_problem
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
from scripts.process import TimeoutExpired
from scripts.project_config import ROOT, ConfigurationError, load_configuration

GONE_DEADLINE_SECONDS = 120
GONE_POLL_SECONDS = 5
CHECK_TIMEOUT_SECONDS = 30
STAGES = ("release", "apply", "purge", "state", "checks")
DATA_PREFIXES = ("landing/", "warehouse/")
APPROVAL_FILES = ("inventory.json", "plans/destroy_binding.json", "teardown/approved_inventory.json")
PARENT_TYPES = {"aws_iam_role_policy": "aws_iam_role"}
DESTROYED = re.compile(r"Resources: \d+ added, \d+ changed, (\d+) destroyed")
Target = tuple[str, str, str]


class TeardownRefused(RuntimeError):
    """A guard failed before any deletion, or a later check stopped the teardown."""


def delete_targets(plan: dict[str, Any]) -> list[Target]:
    """The address, type and service ID of every resource the plan deletes."""
    changes = [change for change in plan.get("resource_changes", []) if "delete" in change["change"]["actions"]]
    return [(change["address"], change["type"], str((change["change"].get("before") or {}).get("id", ""))) for change in changes]


def target_problems(targets: list[Target], inventory: list[dict[str, str]], run: RunBinding, pre_provisioned: list[str]) -> list[str]:
    """Each delete target that this run does not own, and a bucket named for removal that this run did not create."""
    owned = {(entry["type"], entry["service_id"]) for entry in inventory}
    listed = [entry["service_id"] for entry in inventory if entry["address"] == BUCKET_ADDRESS]
    problems = []
    for address, kind, identifier in targets:
        if kind == "aws_s3_bucket":
            problems.append(f"delete target is a bucket, and Terraform manages no bucket: {address}")
        elif kind == "aws_instance" and not run.provision_host:
            problems.append(f"delete target is a host in a run that did not create its host: {address}")
        elif kind == "aws_instance" and identifier in pre_provisioned:
            problems.append(f"delete target is a host that existed before this run: {address}")
        elif (kind, identifier) not in owned:
            problems.append(f"delete target outside this run's inventory: {address}")
    deletes_host = any(kind == "aws_instance" for _, kind, _ in targets)
    if deletes_host and run.provision_host and not any(entry["type"] == DEVICE_TYPE for entry in inventory):
        problems.append("the plan deletes a host and the run has no record of its Tailscale device: run make host-start or make host-up first")
    if listed != ([run.bucket] if run.bucket_creation == "created" else []):
        problems.append("the bucket in this run's inventory is not the bucket this run created")
    return problems


def bucket_entry_problems(bucket: str, entries: list[dict[str, Any]], approved: list[list[str]]) -> list[str]:
    """Each data object version or delete marker in the bucket with no approved bucket, key and version ID match."""
    allowed = {(item[0], item[1], item[2]) for item in approved}
    problems = []
    for entry in entries:
        if entry["VersionId"] in ("", "null"):
            problems.append(f"{bucket}: entry with no version ID: {entry['Key']}")
        elif (bucket, entry["Key"], entry["VersionId"]) not in allowed:
            problems.append(f"{bucket}: entry outside the approved inventory: {entry['Key']}")
    return problems


def state_entry_problems(entries: list[dict[str, Any]], state_keys: list[str], approved: list[list[str]], recorded: list[list[str]] | None = None) -> list[str]:
    """Each entry at a key that is not this run's state key, or with no approved or recorded key and version ID match."""
    allowed = {(item[0], item[1]) for item in [*approved, *(recorded or [])]}
    problems = []
    for entry in entries:
        if entry["Key"] not in state_keys:
            problems.append(f"bucket: key outside this run's state keys: {entry['Key']}")
        elif (entry["Key"], entry["VersionId"]) not in allowed:
            problems.append(f"bucket: state version outside the approved inventory: {entry['Key']}")
    return problems


def bucket_entries(aws: Aws, bucket: str) -> list[dict[str, Any]]:
    listing = aws.call("s3api", "list-object-versions", "--bucket", bucket)
    return [*listing.get("Versions", []), *listing.get("DeleteMarkers", [])]


def bucket_state_keys(run: RunBinding) -> list[str]:
    """The keys in the bucket that hold this run's Terraform state: none when the state is a local file."""
    return run.state_keys if run.backend == "s3" else []


def classify_entries(entries: list[dict[str, Any]], run: RunBinding) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """The bucket's entries as data, as this run's state and as everything else. Only the data prefixes hold data; another run's state is neither."""
    keys = bucket_state_keys(run)
    data = [entry for entry in entries if entry["Key"].startswith(DATA_PREFIXES)]
    state = [entry for entry in entries if entry["Key"] in keys]
    return data, state, [entry for entry in entries if entry not in data and entry not in state]


def other_entry_problems(other: list[dict[str, Any]], run: RunBinding) -> list[str]:
    """Entries that are neither data nor this run's state are never approved or deleted. They stop the removal of a bucket this run created."""
    if run.bucket_creation != "created":
        return []
    return [f"bucket: entry outside the data prefixes and this run's state keys: {key}" for key in sorted({entry["Key"] for entry in other})]


def approved_problems(approved: dict[str, Any], run: RunBinding) -> list[str]:
    """Each approved entry that this run may never delete: in another bucket, outside the data prefixes or not at this run's state keys."""
    problems = []
    for bucket, key, _ in approved["application"]:
        if bucket != run.bucket:
            problems.append(f"the approved inventory lists an entry in another bucket: {key}")
        elif not key.startswith(DATA_PREFIXES):
            problems.append(f"the approved inventory lists an entry outside the data prefixes: {key}")
    keys = bucket_state_keys(run)
    problems += [f"the approved inventory lists a state entry outside this run's state keys: {key}" for key, _ in approved["state"] if key not in keys]
    return problems


def snapshot_problem(current: tuple[str, int], approved: dict[str, Any]) -> str | None:
    """Why the state is not the state the destroy plan was saved for, or None when its lineage and serial are the approved ones."""
    if current != (approved["state_lineage"], approved["state_serial"]):
        return "the state changed after the destroy plan was saved: run make teardown-plan again"
    return None


def state_identity(run: RunBinding) -> tuple[str, int]:
    current = json.loads(terraform(run, ["state", "pull"], PLAN_TIMEOUT_SECONDS))
    return str(current["lineage"]), int(current["serial"])


def teardown_code(plan_hash: str, approved: dict[str, Any]) -> str:
    return hashlib.sha256((plan_hash + json.dumps(approved, sort_keys=True)).encode()).hexdigest()[:12]


def read_json(path: Path, what: str) -> Any:
    if not path.is_file():
        raise TeardownRefused(f"{what} is missing: {path.relative_to(ROOT)}")
    return json.loads(path.read_text())


def plan_teardown(aws: Aws, configuration: dict[str, str], run: RunBinding) -> int:
    folder = ROOT / "runs" / run.run_id
    read_json(folder / "inventory.json", "the run's inventory")
    before = state_identity(run)
    save_plan(run, configuration, "destroy")
    data, state, _ = classify_entries(bucket_entries(aws, run.bucket), run)
    if state_identity(run) != before:
        raise TeardownRefused("the state changed while the destroy plan was saved: run make teardown-plan again")
    approved: dict[str, Any] = {
        "application": [[run.bucket, entry["Key"], entry["VersionId"]] for entry in data],
        "state": [[entry["Key"], entry["VersionId"]] for entry in state],
        "state_lineage": before[0],
        "state_serial": before[1],
    }
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
    """Why a bucket entry that appeared after approval is not this run's own destroy write, or None when it is."""
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


def local_state_problem(state_file: Path, approved_hash: str) -> str | None:
    """Why the bound local state is not the state the destroy plan was saved for, or None when it is."""
    if not state_file.is_file():
        return "the bound local state file does not exist"
    if hashlib.sha256(state_file.read_bytes()).hexdigest() != approved_hash:
        return "the local state changed after the destroy plan was saved"
    return None


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


def approval_bytes(folder: Path) -> dict[str, bytes]:
    """Each approval file, read once. Every check uses these bytes, and the signed manifest must carry their hashes."""
    found = {}
    for name in APPROVAL_FILES:
        data = read_bytes(folder / name)
        if data is None:
            raise TeardownRefused(f"an approval file is missing: {name}; run make teardown-plan")
        found[name] = data
    return found


def consumed_problems(manifest: dict[str, Any], files: dict[str, bytes]) -> list[str]:
    """Each approval file whose checked bytes are not the bytes the manifest lists."""
    listed = {item["path"]: item["sha256"] for item in manifest["files"]}
    return [f"an approval file changed while it was checked: {name}" for name, data in files.items() if listed.get(name) != hashlib.sha256(data).hexdigest()]


def approval_binding(confirm: str, files: dict[str, bytes], plan: bytes, receipt: bytes | None) -> dict[str, str]:
    """What a teardown starts with: the code, the approved inventory, the plan, the inventory and the receipt. A rerun must carry the same."""
    return {
        "code": confirm,
        "approved_sha256": hashlib.sha256(files["teardown/approved_inventory.json"]).hexdigest(),
        "plan_sha256": hashlib.sha256(plan).hexdigest(),
        "inventory_sha256": hashlib.sha256(files["inventory.json"]).hexdigest(),
        "receipt_sha256": hashlib.sha256(receipt or b"").hexdigest(),
    }


def progress_problem(recorded: dict[str, Any], current: dict[str, str]) -> str | None:
    for name, value in current.items():
        if recorded.get(name) != value:
            return f"this teardown started with another {name}: a rerun must use the approval it started with"
    return None


def guard_problems(
    aws: Aws, run: RunBinding, folder: Path, confirm: str, plan: bytes, approved_plan: Path, files: dict[str, bytes], receipt: bytes | None
) -> list[str]:
    inventory_record = json.loads(files["inventory.json"])
    inventory = inventory_record["created"]
    approved = json.loads(files["teardown/approved_inventory.json"])
    saved = json.loads(files["plans/destroy_binding.json"])
    signature = read_bytes(folder / "export" / "receipt.json.sig")
    evidence = build_manifest(folder, run.run_id, run.commit)
    problems = manifest_problems(evidence) + consumed_problems(evidence, files)
    problems += receipt_problems(receipt, signature_is_valid(receipt, signature, trusted_signers()), run, manifest_sha256(evidence))
    problems += binding_problems(run, saved, plan, saved["plan_sha256"][:12], "destroy")
    if confirm != teardown_code(saved["plan_sha256"], approved):
        problems.append("the confirmation code does not equal the reviewed destroy plan and approved inventory")
    if inventory_record.get("authorized_retained"):
        problems.append("this teardown does not yet keep retained items: remove them from the destroy plan first")
    pre_provisioned = json.loads((folder / "pre_provisioned.json").read_text()) if (folder / "pre_provisioned.json").is_file() else []
    saved_plan = json.loads(terraform(run, ["show", "-json", str(approved_plan)], PLAN_TIMEOUT_SECONDS))
    problems += target_problems(delete_targets(saved_plan), inventory, run, pre_provisioned)
    data, state, other = classify_entries(bucket_entries(aws, run.bucket), run)
    problems += approved_problems(approved, run)
    if run.backend == "s3" and (moved := snapshot_problem(state_identity(run), approved)):
        problems.append(moved)
    problems += bucket_entry_problems(run.bucket, data, approved["application"])
    problems += state_entry_problems(state, run.state_keys, approved["state"])
    problems += other_entry_problems(other, run)
    if run.backend == "local" and (problem := local_state_problem(Path(run.state_keys[0]), approved["local_state_sha256"])):
        problems.append(problem)
    return problems


def delete_entry(aws: Aws, bucket: str, key: str, version: str, account: str) -> None:
    """Delete one entry by key and version ID. S3 refuses the call when the bucket is not this account's."""
    aws.call("s3api", "delete-object", "--bucket", bucket, "--key", key, "--version-id", version, "--expected-bucket-owner", account)


def purge(aws: Aws, bucket: str, approved: list[tuple[str, str]], account: str) -> int:
    """Delete, by key and version ID, each approved entry that the bucket still holds, and return how many."""
    present = {(entry["Key"], entry["VersionId"]) for entry in bucket_entries(aws, bucket)}
    for key, version in sorted(present & set(approved)):
        delete_entry(aws, bucket, key, version, account)
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


def this_computer() -> str:
    return socket.gethostname()


def process_runs(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def lock_takeover_problem(content: bytes, run: RunBinding, computer: str, runs: Callable[[int], bool]) -> str | None:
    """Why an existing lock is not the lock a stopped cleanup of this run left on this computer, or None when this cleanup may take it over."""
    other = "another process holds the state lock"
    try:
        record = json.loads(content)
    except ValueError:
        return other
    if not isinstance(record, dict) or record.get("ID") != f"teardown-{run.run_id}" or record.get("Path") != f"{run.bucket}/{run.state_keys[0]}":
        return other
    holder, _, pid = str(record.get("Who", "")).rpartition(":")
    if holder != computer or not pid.isdigit():
        return other
    return "this run's cleanup still runs in another process" if runs(int(pid)) else None


def hold_state_lock(aws: Aws, run: RunBinding, scratch: Path) -> str:
    """Take the state lock for the cleanup and return the version ID of the lock object.

    The write succeeds only when no lock exists. A lock that a stopped cleanup of this run left on this computer is taken over by its version ID.
    """
    body = scratch / "lock"
    record = {"ID": f"teardown-{run.run_id}", "Operation": "teardown cleanup", "Who": f"{this_computer()}:{os.getpid()}"}
    body.write_text(json.dumps(record | {"Path": f"{run.bucket}/{run.state_keys[0]}"}))
    arguments = ["s3api", "put-object", "--bucket", run.bucket, "--key", run.state_keys[1], "--body", str(body), "--if-none-match", "*"]
    written = aws.call(*arguments, missing=("PreconditionFailed",))
    if written is not None:
        return str(written["VersionId"])
    listing = aws.call("s3api", "list-object-versions", "--bucket", run.bucket, "--prefix", run.state_keys[1])
    latest = [entry for entry in listing.get("Versions", []) if entry["Key"] == run.state_keys[1] and entry.get("IsLatest")]
    problem = (
        lock_takeover_problem(entry_content(aws, run.bucket, latest[0]), run, this_computer(), process_runs)
        if latest
        else "another process holds the state lock"
    )
    if problem:
        raise TeardownRefused(f"{problem}: the state entries and the bucket are kept; run the command again when it is released")
    return str(latest[0]["VersionId"])


def clean_bucket(aws: Aws, run: RunBinding, approved: dict[str, Any], removal: Path, account: str) -> str:
    """Under the state lock, delete this run's state entries, stop on any entry this run does not own, then remove the bucket this run created."""
    created = run.bucket_creation == "created"
    if aws.call("s3api", "head-bucket", "--bucket", run.bucket, missing=MISSING_BUCKET) is None:
        if created:
            return "none: no resource is retained"
        raise TeardownRefused("the bucket, which this run did not create, no longer exists")
    keys = bucket_state_keys(run)
    with tempfile.TemporaryDirectory(prefix="state_lock_") as scratch:
        lock = [keys[1], hold_state_lock(aws, run, Path(scratch))] if keys else None
        try:
            listing = aws.call("s3api", "list-object-versions", "--bucket", run.bucket)
            markers = {(entry["Key"], entry["VersionId"]) for entry in listing.get("DeleteMarkers", [])}
            data, state, other = classify_entries([*listing.get("Versions", []), *listing.get("DeleteMarkers", [])], run)
            known = {(item[0], item[1]) for item in approved["state"]} | ({(lock[0], lock[1])} if lock else set())
            recorded = [lock] if lock else []
            foreign = other_entry_problems(other, run)
            if created:
                foreign += [f"bucket: data entry that was not approved: {key}" for key in sorted({entry["Key"] for entry in data})]
            for entry in state:
                pair = (entry["Key"], entry["VersionId"])
                if pair in known:
                    continue
                content = None if pair in markers else entry_content(aws, run.bucket, entry)
                problem = destroy_write_problem(entry["Key"], content, pair in markers, keys, approved["state_lineage"], approved["state_serial"])
                if problem:
                    foreign.append(f"bucket: {problem}: {entry['Key']}")
                else:
                    recorded.append([entry["Key"], entry["VersionId"]])
            (removal / "destroy_written_state_entries.json").write_text(json.dumps(recorded, indent=2) + "\n")
            left = foreign + state_entry_problems(state, keys, approved["state"], recorded)
            if left:
                raise TeardownRefused("the environment is gone, and the bucket is kept because:\n" + "\n".join(sorted(set(left))))
            for entry in state:
                if lock is None or (entry["Key"], entry["VersionId"]) != (lock[0], lock[1]):
                    delete_entry(aws, run.bucket, entry["Key"], entry["VersionId"], account)
        finally:
            if lock:
                delete_entry(aws, run.bucket, lock[0], lock[1], account)
    if not created:
        return "the bucket is kept: this run did not create it"
    if aws.call("s3api", "head-bucket", "--bucket", run.bucket, "--expected-bucket-owner", account, missing=("(403)", "AccessDenied")) is None:
        raise TeardownRefused("the bucket is not owned by this account any more; it is kept")
    aws.call("s3api", "delete-bucket", "--bucket", run.bucket, "--expected-bucket-owner", account)
    if not wait_gone(aws, run.bucket):
        raise TeardownRefused(f"S3 still reports the bucket {GONE_DEADLINE_SECONDS} seconds after its deletion")
    return "none: no resource is retained"


def device_check(entry: dict[str, str], status: Callable[[], dict[str, Any]]) -> str:
    """What the Tailscale network reports for the host's device: neither the recorded device nor a tagged device with the host's name may remain."""
    try:
        return "gone" if host.device_gone(status(), entry["host_name"], entry["service_id"]) else "still exists"
    except host.HostError:
        return "unverified: the Tailscale network could not be read"


CHECKS: dict[str, tuple[list[str], tuple[str, ...]]] = {
    "aws_s3_bucket": (["s3api", "head-bucket", "--bucket"], MISSING_BUCKET),
    "aws_security_group": (["ec2", "describe-security-groups", "--group-ids"], ("InvalidGroup.NotFound",)),
    "aws_iam_role": (["iam", "get-role", "--role-name"], ("NoSuchEntity",)),
    "aws_iam_instance_profile": (["iam", "get-instance-profile", "--instance-profile-name"], ("NoSuchEntity",)),
    "aws_ebs_volume": (["ec2", "describe-volumes", "--volume-ids"], ("InvalidVolume.NotFound",)),
    "aws_network_interface": (["ec2", "describe-network-interfaces", "--network-interface-ids"], ("InvalidNetworkInterfaceID.NotFound",)),
}
CHECKED_TYPES = {*CHECKS, "aws_instance", DEVICE_TYPE}


def removal_check(aws: Aws, entry: dict[str, str], remaining: float) -> str:
    """What the service reports for one inventory entry of a checked type after the teardown, within the time that is left."""
    kind, identifier = entry["type"], entry["service_id"]
    timeout = min(CHECK_TIMEOUT_SECONDS, remaining)
    if kind == DEVICE_TYPE:
        return device_check(entry, lambda: host.tailscale_status(timeout))
    if kind == "aws_instance":
        query = ["ec2", "describe-instances", "--instance-ids", identifier, "--query", "Reservations[].Instances[].State.Name"]
        state = aws.call(*query, missing=("InvalidInstanceID.NotFound",), timeout=timeout)
        return "gone" if state in (None, [], ["terminated"]) else f"still exists: {state}"
    arguments, missing = CHECKS[kind]
    return "gone" if aws.call(*arguments, identifier, missing=missing, timeout=timeout) is None else "still exists"


def checked_removals(aws: Aws, inventory: list[dict[str, str]]) -> list[dict[str, str]]:
    """Every inventory entry with its result: a service check where one exists, else its parent's, else unverified. All checks share one deadline."""
    direct = [entry for entry in inventory if entry["type"] in CHECKED_TYPES]

    def check(entry: dict[str, str], remaining: float) -> str:
        try:
            return removal_check(aws, entry, remaining)
        except TimeoutExpired:
            return "still exists: its service did not answer in time"

    settled = settled_checks(check, direct, deadline_seconds=GONE_DEADLINE_SECONDS, pause_seconds=GONE_POLL_SECONDS)
    reported = {(found["type"], found["service_id"]): found["reported"] for found in settled}
    by_address = {found["address"]: found for found in settled}
    return [by_address.get(entry["address"]) or entry | {"reported": parent_check(entry, reported)} for entry in inventory]


def read_progress(removal: Path) -> dict[str, Any] | None:
    progress = removal / "progress.json"
    record: dict[str, Any] | None = json.loads(progress.read_text()) if progress.is_file() else None
    return record


def write_progress(removal: Path, binding: dict[str, str], finished: list[str]) -> None:
    removal.mkdir(exist_ok=True)
    (removal / "progress.json").write_text(json.dumps(binding | {"finished": finished}, indent=2) + "\n")


def settled_checks(
    check: Callable[[dict[str, str], float], str],
    inventory: list[dict[str, str]],
    *,
    deadline_seconds: float,
    pause_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, str]]:
    """Each entry with what its service reports. A removal still reported as existing is asked again until one deadline that all checks share."""
    deadline = clock() + deadline_seconds
    reported: dict[str, str] = {}
    pending = list(inventory)
    while pending:
        for entry in pending:
            remaining = deadline - clock()
            if remaining <= 0:
                break
            reported[entry["address"]] = check(entry, remaining)
        pending = [entry for entry in pending if reported.get(entry["address"], "still exists").startswith("still exists")]
        if not pending or deadline - clock() <= pause_seconds:
            break
        sleep(pause_seconds)
    return [entry | {"reported": reported.get(entry["address"], "unverified: not checked before the deadline")} for entry in inventory]


def apply_destroy(run: RunBinding, approved_plan: Path, finish: Callable[[str], None]) -> None:
    """Apply the checked plan and record the stage at once, so that a failed write of the raw output cannot make a rerun apply again."""
    output = terraform(run, ["apply", "-input=false", "-no-color", str(approved_plan)], APPLY_TIMEOUT_SECONDS)
    finish("apply")
    destroyed = DESTROYED.search(output)
    sys.stdout.write(f"run {run.run_id}: the destroy plan is applied; resources destroyed: {destroyed.group(1) if destroyed else 'see the raw output'}\n")
    try:
        save_raw(run.run_id, "destroy_apply.txt", output)
    except OSError as error:
        sys.stderr.write(f"the raw output of the destroy could not be kept: {type(error).__name__}\n")


def destroy(aws: Aws, configuration: dict[str, str], run: RunBinding, confirm: str) -> int:
    """Run the stages that are left. A finished teardown does nothing. An interrupted one resumes only with the approval it started with."""
    folder = ROOT / "runs" / run.run_id
    removal = folder / "removal"
    progress = read_progress(removal)
    finished: list[str] = progress["finished"] if progress else []
    left = remaining_stages(finished)
    if not left:
        sys.stdout.write(f"run {run.run_id}: the teardown is already complete; nothing was changed\n")
        return 0
    files = approval_bytes(folder)
    plan = read_bytes(folder / "plans" / "destroy.tfplan")
    if plan is None:
        raise TeardownRefused("the saved destroy plan is missing: run make teardown-plan")
    receipt = read_bytes(folder / "export" / "receipt.json")
    binding = approval_binding(confirm, files, plan, receipt)
    if progress is not None and (problem := progress_problem(progress, binding)):
        raise TeardownRefused(f"refused before any deletion: {problem}")
    inventory = json.loads(files["inventory.json"])["created"]
    approved = json.loads(files["teardown/approved_inventory.json"])

    def finish(stage: str) -> None:
        finished.append(stage)
        write_progress(removal, binding, finished)

    if "apply" in left:
        approved_plan = approved_plan_copy(run, plan)
        try:
            problems = guard_problems(aws, run, folder, confirm, plan, approved_plan, files, receipt)
            if problems:
                raise TeardownRefused("refused before any deletion:\n" + "\n".join(problems))
            write_progress(removal, binding, finished)
            if "release" in left:
                instances = [entry["service_id"] for entry in inventory if entry["type"] == "aws_instance"]
                for device in (entry for entry in inventory if entry["type"] == DEVICE_TYPE):
                    if len(instances) != 1:
                        raise TeardownRefused("refused before any deletion: the inventory lists a Tailscale device and not exactly one host instance")
                    host.release_tailscale_device(run.run_id, device["host_name"], device["service_id"], instances[0])
                finish("release")
            apply_destroy(run, approved_plan, finish)
        finally:
            approved_plan.unlink(missing_ok=True)
    if "purge" in left:
        purge(aws, run.bucket, [(item[1], item[2]) for item in approved["application"] if item[0] == run.bucket], configuration["AWS_ACCOUNT_ID"])
        finish("purge")
    if "state" in left:
        (removal / "retained.json").write_text(json.dumps(clean_bucket(aws, run, approved, removal, configuration["AWS_ACCOUNT_ID"])) + "\n")
        finish("state")
    checks = checked_removals(aws, inventory)
    (removal / "checks.json").write_text(
        json.dumps({"run_id": run.run_id, "checks": checks, "retained_cost": read_json(removal / "retained.json", "the record of what is kept")}, indent=2)
        + "\n"
    )
    for check in checks:
        sys.stdout.write(f"{check['reported']}: {check['address']}\n")
    failed = [check for check in checks if not check["reported"].startswith("gone")]
    if failed:
        raise TeardownRefused(f"{len(failed)} removals are not confirmed; see {(removal / 'checks.json').relative_to(ROOT)}")
    finish("checks")
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
        mode_problem = bucket_problem(run, configuration)
        if mode_problem:
            raise TeardownRefused(f"refused before any deletion: {mode_problem}")
        return plan_teardown(aws, configuration, run) if arguments.command == "plan" else destroy(aws, configuration, run, arguments.confirm)
    except (ConfigurationError, WrapperError, TeardownRefused, AwsError, host.HostError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
