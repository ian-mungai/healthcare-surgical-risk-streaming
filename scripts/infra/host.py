"""Create, stop and start the build and test host.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make host-up`` creates the project's bucket when it is absent and saves a Terraform plan. When the plan changes
anything, it prints the changes and a confirmation code and stops for the owner's review.
``make host-up CONFIRM=<CODE>`` applies exactly the reviewed plan, waits for the instance, for its Tailscale peer and
for SSH, runs the Ansible host setup and writes the inventory. A run with nothing to change goes straight to those
checks, so a second run changes nothing.
``make host-stop`` stops the instance, keeps its disk and prints what still bills.
``make host-start`` starts it and waits until it answers over Tailscale.
``host check --host-name <NAME> --instance-id <ID>`` is the deploy workflow's check: the peer with that name and tag must answer over
SSH as the instance with that ID.

Every wait has a deadline and fails with a message that names what did not happen.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

from scripts.aws_cli import Aws, AwsError
from scripts.infra import aistor_licence, state_bootstrap
from scripts.infra.created import DEVICE_FILE, module_resources
from scripts.infra.inventory import write_inventory
from scripts.infra.terraform_wrapper import WrapperError, apply_plan, kept_note, load_run, save_plan, stale_inputs_problem, state_json
from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.ingestion.output_file import replace_owned_file
from scripts.process import TimeoutExpired, partial_text, run_command, timeout_text
from scripts.project_config import PATTERNS, ROOT, ConfigurationError, load_configuration
from scripts.stack.warehouse import WarehouseError, warehouse_location

TAILSCALE_APP = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
HOST_TAG = "tag:surgical-host"
HOST_USER = "ubuntu"
INSTANCE_ID_COMMAND = "cat /var/lib/cloud/data/instance-id"
INSTANCE_ID_FORM = re.compile(r"i-[0-9a-f]{8,17}")
RECAP_LINE = re.compile(r"platform +: ok=\d+( +\w+=\d+)+ *")
INSTANCE_DEADLINE_SECONDS = 600
PEER_DEADLINE_SECONDS = 600
SSH_DEADLINE_SECONDS = 300
POLL_SECONDS = 10
SSH_ATTEMPT_SECONDS = 30
# The host setup may take the sum of the deadlines of every task on the path of ansible/host.yml (each include counted once per call, each retried task once
# per attempt, each loop once per item) plus a tenth, in whole five minutes. tests/fast/test_host_budget.py walks the playbook, works the sum out from the task
# files and fails when this figure is lower. The AIStor step (ansible/storage.yml: the licence check, the start of AIStor and its project user) and the
# connection step (ansible/connection.yml: the Airbyte connection step and the start of the orchestration services) have budgets of their own from the same sum.
HOST_SETUP_TIMEOUT_SECONDS = 24600
CONNECTION_TIMEOUT_SECONDS = 4800
STORAGE_TIMEOUT_SECONDS = 3000
PLAYBOOKS = {
    "host.yml": ("host setup", "ansible_output.txt", HOST_SETUP_TIMEOUT_SECONDS),
    "storage.yml": ("AIStor step", "ansible_storage_output.txt", STORAGE_TIMEOUT_SECONDS),
    "connection.yml": ("connection step", "ansible_connection_output.txt", CONNECTION_TIMEOUT_SECONDS),
}
KEY_SCRIPT = "/opt/platform/bin/airbyte_key_install"
# The remote command: the host's interpreter in isolated mode and the installed script. This command line is the one that sudo matches, and the sudoers drop-in
# (the file named below, which the role installs and this program compares by its SHA-256) turns input recording off for it.
KEY_COMMAND = ["/usr/bin/python3", "-I", KEY_SCRIPT]
SUDOERS_FILE = "/etc/sudoers.d/airbyte_key_install"
KEY_POLICY_SOURCE = Path(__file__).resolve().parents[2] / "ansible" / "roles" / "airbyte" / "files" / "airbyte_key_install.sudoers"
KEY_EXPORT_SECONDS = 60
KEY_SEND_SECONDS = 60
KEY_ID_FORM = re.compile(r"[A-Z0-9]{16,128}")
KEY_VALUE_FORM = re.compile(r"[A-Za-z0-9/+=]{20,128}")
SCRIPT_REFUSALS = ("oversized", "not_json", "bad_keys", "session_token", "empty", "bad_form", "bad_owner", "cannot_write")
KEY_MESSAGES = {
    "export_failed": "the AWS command line could not export the credentials of the project profile (exit status {status}); run it by hand to see why",
    "export_timeout": "the AWS command line did not finish within {seconds} seconds",
    "no_aws": "the AWS command line is not installed or not on PATH",
    "not_a_static_key": "the AWS command line answered for the project profile with something that is not the ID and the secret of an AWS access key",
    "session_token": "the project profile yields a session token; Airbyte's connectors need the own access key of an IAM user, not a temporary credential",
    "empty": "the AWS command line answered for the project profile with no access key ID or no secret",
    "policy_missing": "the sudo policy file for the key script is missing or cannot be read on the host; the host playbook installs it, so run it first",
    "policy_differs": "the sudo policy file for the key script on the host is not the repository's; the host playbook installs the right one, so run it first",
    "ssh_failed": "the key could not be sent to the host (exit status {status})",
    "ssh_timeout": "sending the key to the host did not finish within {seconds} seconds",
    "script_oversized": "the key script on the host refused the document: it is larger than the script accepts",
    "script_not_json": "the key script on the host refused the document: it is not a JSON object",
    "script_bad_keys": "the key script on the host refused the document: it does not hold exactly the two key values",
    "script_session_token": "the key script on the host refused the document: it holds a session token",
    "script_empty": "the key script on the host refused the document: a value is empty",
    "script_bad_form": "the key script on the host refused the document: a value is not in the form of an AWS access key",
    "script_bad_owner": "the key script on the host refused its owner options: an owner is not a numeric uid:gid that the system takes",
    "script_cannot_write": "the key script on the host could not write the key files under the secrets folder; check the folder's owner and free space",
    "script_unknown": "the key script on the host answered with something that this program does not know",
}
# AIStor's licence is placed the same way: the launcher reads it from this computer's OpenBao (scripts/infra/aistor_licence.py) and sends it on standard input
# of one SSH command to a root-owned script that the host playbook installs, with its own sudoers drop-in that turns off sudo's recording for that command line.
LICENCE_SCRIPT = "/opt/platform/bin/aistor_licence_install"
LICENCE_COMMAND = ["/usr/bin/python3", "-I", LICENCE_SCRIPT]
LICENCE_SUDOERS_FILE = "/etc/sudoers.d/aistor_licence_install"
LICENCE_POLICY_SOURCE = Path(__file__).resolve().parents[2] / "ansible" / "roles" / "compose_stack" / "files" / "aistor_licence_install.sudoers"
LICENCE_SEND_SECONDS = 60
LICENCE_REFUSALS = ("oversized", "empty", "bad_form", "bad_owner", "cannot_write")
LICENCE_MESSAGES = {
    "policy_missing": "the sudo policy file for the licence script is missing or cannot be read on the host; the host playbook installs it, so run it first",
    "policy_differs": "the sudo policy file for the licence script on the host is not the repository's; the host playbook installs the right one, run it first",
    "ssh_failed": "the licence could not be sent to the host (exit status {status})",
    "ssh_timeout": "sending the licence to the host did not finish within {seconds} seconds",
    "script_oversized": "the licence script on the host refused the licence: it is larger than the script accepts",
    "script_empty": "the licence script on the host refused the licence: it is empty",
    "script_bad_form": "the licence script on the host refused the licence: it is not ASCII text without control characters",
    "script_bad_owner": "the licence script on the host refused its owner option: it is not a numeric uid:gid that the system takes",
    "script_cannot_write": "the licence script on the host could not write the licence file under the secrets folder; check the folder's owner and free space",
    "script_unknown": "the licence script on the host answered with something that this program does not know",
}
LICENCE_ABSENT_NOTICE = (
    "AIStor is skipped: this computer's OpenBao holds no AIStor licence at secret/aistor_licence (field value), so the host has none to start the server with. "
    "The deploy goes on with everything else (Airbyte, the connection step and the rest). Store the licence at that path (references/local_machine.md, "
    "Local secrets) and run make host-up again: AIStor then starts."
)
LICENCE_NO_SOURCE_NOTICE = (
    f"AIStor is skipped: {aistor_licence.SETUP_MESSAGE}. The deploy goes on with everything else (Airbyte, the connection step and the rest). "
    "After the owner's step is done and the licence is stored, run make host-up again: AIStor then starts."
)
LICENCE_KEPT_NOTICE = (
    "AIStor started with the licence file that was already on the host: this computer sent no licence this time "
    "(it has no licence source, or its OpenBao holds none)."
)
LICENCE_UNCONFIRMED_NOTICE = (
    "AIStor: could not confirm from the host whether the server started or was skipped for want of a licence; "
    "make storage-up reads the host's record and says which."
)
AISTOR_STATE_FILE = "/opt/platform/compose/aistor.state"
# What the launcher tells the AIStor playbook about the licence: ``placed`` (the licence is on the host, new or unchanged) or ``absent`` (none was sent).
PLACEMENT_FACTS = {"placed": "placed", "unchanged": "placed", "absent": "absent", "no_source": "absent"}
GIT_TIMEOUT_SECONDS = 30
RELEASE_DEADLINE_SECONDS = 300
WORKLOAD_GID_BASE = 20000
PROJECT_WORKLOAD_GIDS = {"airbyte": 20100, "ingestion_check": 20101, "spark": 20102, "trino": 20103, "replay": 20104, "orchestrator": 20105}
STOPPED_BILLING = "still billed while stopped: the host's 200 GiB disk and the objects in the project's buckets; not billed: the instance's hours"


class HostError(RuntimeError):
    """A host command refused to continue, a wait passed its deadline or a call failed."""


class NoPeerError(HostError):
    """The Tailscale network shows no online peer with the host's name and tag yet."""


class Peer(NamedTuple):
    address: str
    device_id: str


def wait_for[T](
    what: str,
    probe: Callable[[float], T | None],
    deadline_seconds: float,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``probe`` until it returns a value, within one absolute deadline.

    Each probe gets the seconds that remain and must stay inside them. A probe call that times out counts as not ready.
    A result that comes after the deadline is no result.

    A call that is killed at its timeout can raise PermissionError on macOS when its process has just exited, so that counts as a timeout too.
    """
    deadline = clock() + deadline_seconds
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise HostError(f"deadline of {deadline_seconds} seconds passed while waiting for {what}")
        try:
            found = probe(remaining)
        except TimeoutExpired, PermissionError:
            found = None
        if clock() > deadline:
            raise HostError(f"deadline of {deadline_seconds} seconds passed while waiting for {what}")
        if found is not None:
            sys.stdout.write(f"ready: {what}\n")
            return found
        sleep(min(POLL_SECONDS, max(deadline - clock(), 0)))


def select_peer(status: dict[str, Any], host_name: str, tag: str) -> Peer:
    """The one online peer that has the host's name and tag. Two such peers are refused, because either could be the host."""
    peers = [
        peer for peer in (status.get("Peer") or {}).values() if peer.get("HostName") == host_name and peer.get("Online") and tag in (peer.get("Tags") or [])
    ]
    if not peers:
        raise NoPeerError(f"no online peer named {host_name} with {tag} in the Tailscale network")
    if len(peers) > 1:
        raise HostError(
            f"{len(peers)} online peers are named {host_name} with {tag}: remove the extra devices in the Tailscale console, then run the command again"
        )
    return Peer(str(peers[0]["DNSName"]).rstrip("."), str(peers[0]["ID"]))


def tailscale_status(timeout: float) -> dict[str, Any]:
    program = "tailscale" if shutil.which("tailscale") else TAILSCALE_APP
    try:
        result = run_command(program, ["status", "--json"], timeout=min(timeout, SSH_ATTEMPT_SECONDS))
    except FileNotFoundError:
        raise HostError("the Tailscale client is not installed on this computer") from None
    except TimeoutExpired:
        raise NoPeerError(timeout_text("tailscale status", min(timeout, SSH_ATTEMPT_SECONDS))) from None
    if result.returncode != 0:
        raise HostError(f"tailscale status failed: exit status {result.returncode}")
    status: dict[str, Any] = json.loads(result.stdout)
    return status


def host_peer(host_name: str, timeout: float = SSH_ATTEMPT_SECONDS) -> Peer:
    return select_peer(tailscale_status(timeout), host_name, HOST_TAG)


def tailscale_device_id(host_name: str) -> str:
    """The Tailscale device ID of the host's one online peer, as this computer's Tailscale status shows it."""
    return host_peer(host_name).device_id


def record_tailscale_device(run_id: str, host_name: str, device_id: str) -> Path:
    """Write the host's current Tailscale device to the run folder, over any earlier record: a host that left the network and rejoined has a new device."""
    folder = ROOT / "runs" / run_id
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / DEVICE_FILE
    replace_owned_file(target, json.dumps({"host_name": host_name, "device_id": device_id}, indent=2) + "\n", state_root=folder, mode=0o644)
    return target


def require_connected(status: dict[str, Any]) -> None:
    """Refuse a status from a client that is not running and connected: its peer list says nothing about the host's device."""
    if status.get("BackendState") != "Running":
        raise HostError("the Tailscale client on this computer is not running and connected, so the host's device cannot be checked")


def device_gone(status: dict[str, Any], host_name: str, device_id: str) -> bool:
    """Whether no peer is the recorded device or has the host's name and tag, online or not. A client that is not running and connected raises HostError."""
    require_connected(status)
    return not any(
        peer.get("ID") == device_id or (peer.get("HostName") == host_name and HOST_TAG in (peer.get("Tags") or []))
        for peer in (status.get("Peer") or {}).values()
    )


def release_tailscale_device(run_id: str, host_name: str, device_id: str, instance_id: str, deadline_seconds: float = RELEASE_DEADLINE_SECONDS) -> None:
    """Log the host out of Tailscale over SSH, then wait until its recorded device and any device with its name and tag are gone from the network.

    Run before the instance is destroyed. The peer is logged out only after it has answered over SSH with ``instance_id``, the run's instance;
    any other answer, or none, raises HostError and sends no logout. A client that is not running and connected raises HostError. A device that
    is already gone needs no logout. An ephemeral device that is offline cannot be logged out and leaves the network on its own only after a
    while, so the wait may end at its deadline.
    """
    try:
        status = tailscale_status(SSH_ATTEMPT_SECONDS)
        require_connected(status)
        peer = select_peer(status, host_name, HOST_TAG)
    except NoPeerError:
        peer = None
    if peer is not None:
        keep_host_keys_of(run_id, instance_id)
        found = ssh_output(run_id, peer.address, INSTANCE_ID_COMMAND, SSH_ATTEMPT_SECONDS)
        if found is None:
            raise HostError(f"the Tailscale peer {host_name} did not tell its instance over SSH: no logout was sent")
        require_instance(found, instance_id, host_name)
        ssh_output(run_id, peer.address, "sudo -n tailscale logout", SSH_ATTEMPT_SECONDS)

    def gone(budget: float) -> bool | None:
        try:
            return device_gone(tailscale_status(budget), host_name, device_id) or None
        except NoPeerError:
            return None

    wait_for(f"the Tailscale device of {host_name} to leave the network", gone, deadline_seconds)


def peer_online(host_name: str, timeout: float) -> bool:
    try:
        host_peer(host_name, timeout)
    except NoPeerError:
        return False
    return True


def ssh_arguments(run_id: str) -> list[str]:
    known_hosts = ROOT / "runs" / run_id / "known_hosts"
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    return ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", f"UserKnownHostsFile={known_hosts}", "-o", "StrictHostKeyChecking=accept-new"]


def keep_host_keys_of(run_id: str, instance: str) -> None:
    # A replaced host answers at the same Tailscale name with new host keys, and OpenSSH refuses a name whose saved key changed.
    folder = ROOT / "runs" / run_id
    owner = folder / "known_hosts_instance"
    if owner.is_file() and owner.read_text().strip() == instance:
        return
    (folder / "known_hosts").unlink(missing_ok=True)
    folder.mkdir(parents=True, exist_ok=True)
    replace_owned_file(owner, f"{instance}\n", state_root=folder, mode=0o644)


def ssh_output(run_id: str, address: str, command: str, timeout: float) -> str | None:
    """The output of one command on the host over SSH, or None when SSH does not answer."""
    try:
        result = run_command("ssh", [*ssh_arguments(run_id), f"{HOST_USER}@{address}", command], timeout=min(timeout, SSH_ATTEMPT_SECONDS))
    except TimeoutExpired:
        return None
    return result.stdout if result.returncode == 0 else None


def instance_id(state: dict[str, Any]) -> str:
    found = [
        item["values"]["id"]
        for item in module_resources(state.get("values", {}).get("root_module", {}))
        if item["mode"] == "managed" and item["type"] == "aws_instance"
    ]
    if len(found) != 1:
        raise HostError("the run's state does not hold exactly one host instance: run make host-up first")
    return str(found[0])


def instance_state(aws: Aws, instance: str, timeout: float) -> str:
    return str(aws.call("ec2", "describe-instances", "--instance-ids", instance, "--query", "Reservations[0].Instances[0].State.Name", timeout=timeout))


def wait_until_reachable(aws: Aws, run_id: str, instance: str, host_name: str) -> Peer:
    """Wait for the instance, its Tailscale peer and SSH, then require that the peer is this run's instance."""
    wait_for("the instance to run", lambda budget: instance_state(aws, instance, budget) == "running" or None, INSTANCE_DEADLINE_SECONDS)
    wait_for(f"the Tailscale peer {host_name} to be online", lambda budget: peer_online(host_name, budget) or None, PEER_DEADLINE_SECONDS)
    peer = host_peer(host_name)
    keep_host_keys_of(run_id, instance)
    found = wait_for("SSH over Tailscale", lambda budget: ssh_output(run_id, peer.address, INSTANCE_ID_COMMAND, budget), SSH_DEADLINE_SECONDS)
    require_instance(found, instance, host_name)
    return peer


def require_instance(found: str, expected: str, host_name: str) -> None:
    """Refuse a peer that answered with another instance ID. The message names no ID."""
    if found.strip() != expected:
        raise HostError(f"the Tailscale peer {host_name} belongs to a different instance than this run's: remove the other device, then run the command again")


def workload_variables(state: dict[str, Any], region: str) -> dict[str, Any]:
    """The Ansible variables for workload access: the region and each workload role.

    A project workload keeps its own fixed group ID, so a pod's group setting never depends on which other workloads exist.
    Any other workload gets one by name order.
    """
    roles = state.get("values", {}).get("outputs", {}).get("workload_role_arns", {}).get("value", {})
    others = sorted(name for name in roles if name not in PROJECT_WORKLOAD_GIDS)
    group_ids = PROJECT_WORKLOAD_GIDS | {name: WORKLOAD_GID_BASE + index for index, name in enumerate(others)}
    workloads = [{"name": name, "role_arn": roles[name], "gid": group_ids[name]} for name in sorted(roles)]
    return {"workload_access_region": region, "workload_access_workloads": workloads}


def saved_plan(run_id: str, *, applied: bool) -> dict[str, Any]:
    """The JSON of the saved plan that the deploy follows.

    After an apply of the reviewed plan, that plan is ``apply.json``. After a run with no change, it is the newest plan the run saved: ``apply.json`` for a
    first plan and ``apply_check.json`` for a later one.
    """
    folder = ROOT / "runs" / run_id / "plans"
    found = [folder / name for name in (("apply.json",) if applied else ("apply.json", "apply_check.json")) if (folder / name).is_file()]
    if not found:
        raise HostError(f"no saved plan JSON of run {run_id} gives the planned heavy_memory_mib: run make host-up again to plan first")
    document: dict[str, Any] = json.loads(max(found, key=lambda path: path.stat().st_mtime_ns).read_text())
    return document


def planned_heavy_figure(plan: dict[str, Any]) -> object:
    """The heavy-job figure that the saved plan gave to the output."""
    entry = plan.get("planned_values", {}).get("outputs", {}).get("heavy_memory_mib")
    return entry.get("value") if isinstance(entry, dict) else None


def require_current_inputs(plan: dict[str, Any]) -> None:
    """Refuse a deploy when the limits file now gives other heavy inputs than the plan was made with: the deploy places the current checkout."""
    try:
        problem = stale_inputs_problem(plan)
    except WrapperError as error:
        raise HostError(str(error)) from error
    if problem:
        raise HostError(problem)


def heavy_memory_variables(state: dict[str, Any], planned: object) -> dict[str, Any]:
    """The Ansible variable for the heavy-job figure that Terraform calculated, in MiB, once the state holds the planned figure.

    The orchestrator has no default, so a missing or bad figure is refused. A state figure that is not the planned one is stale: the plan was not applied,
    so the deploy would give the host an old figure.
    """
    outputs = state.get("values", {}).get("outputs", {})
    if "heavy_memory_mib" not in outputs:
        raise HostError("the run's state has no heavy_memory_mib output: it is missing, so run make host-up to plan and apply the current Terraform first")
    figure = outputs["heavy_memory_mib"].get("value")
    if not isinstance(figure, int) or isinstance(figure, bool) or figure <= 0:
        raise HostError(f"the heavy_memory_mib output of the run's state is {figure!r}, not a positive whole number of MiB")
    if figure != planned or type(planned) is not int:
        raise HostError(f"the heavy_memory_mib of the run's state is {figure!r} and the plan gave {planned!r}: the state is stale, so the plan was not applied")
    return {"compose_stack_heavy_memory_mib": figure}


def platform_commit() -> str:
    """The commit of the checkout that the deploy places on the host."""
    return run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=GIT_TIMEOUT_SECONDS, check=True).stdout.strip()


def deploy_variables(configuration: dict[str, str], commit: str) -> dict[str, Any]:
    """The Ansible variables of the stack's host settings: the deployed commit, the warehouse location of the configured bucket, the bucket that Airbyte's
    connection copies into and the bucket that the AIStor project user's policy reaches. No variable holds the AWS profile or the key: the launcher reads
    the key itself and sends it to the host without Ansible."""
    return {
        "compose_stack_platform_commit": commit,
        "compose_stack_warehouse_location": warehouse_location(configuration["S3_BUCKET"]),
        "airbyte_landing_bucket": configuration["S3_BUCKET"],
        "aistor_bucket": configuration["S3_BUCKET"],
    }


def ansible_recap(stdout: str) -> str:
    """The count lines of the playbook's recap for the host group, and nothing else: task output can hold variable values."""
    recap = stdout[stdout.rfind("PLAY RECAP") :].splitlines()[1:] if "PLAY RECAP" in stdout else []
    return "".join(f"{line.strip()}\n" for line in recap if RECAP_LINE.fullmatch(line.strip()))


def run_ansible(run_id: str, address: str, variables: dict[str, Any], playbook: str = "host.yml") -> None:
    """Run one playbook on the host and keep its whole output privately. Only the recap counts are printed, because task output can hold variable values."""
    label, output_name, budget = PLAYBOOKS[playbook]
    home = ROOT / ".tools" / "ansible"
    settings = {"ANSIBLE_CONFIG": "ansible.cfg", "ANSIBLE_HOME": str(home), "ANSIBLE_LOCAL_TEMP": str(home / "tmp")}
    environment = os.environ | settings | {"PLATFORM_HOST": address, "ANSIBLE_SSH_COMMON_ARGS": " ".join(ssh_arguments(run_id))}
    try:
        result = run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"),
            [playbook, "--extra-vars", json.dumps(variables)],
            cwd=ROOT / "ansible",
            env=environment,
            timeout=budget,
        )
    except TimeoutExpired as error:
        partial = partial_text(error.stdout) + partial_text(error.stderr)
        note = f"; {kept_note(run_id, output_name, partial, 'partial output')}" if partial else ""
        raise HostError(timeout_text(f"ansible-playbook {playbook}", budget) + note) from None
    kept = kept_note(run_id, output_name, result.stdout + result.stderr, "full output")
    sys.stdout.write(ansible_recap(result.stdout))
    if result.returncode != 0:
        raise HostError(f"the Ansible {label} failed; {kept}")
    sys.stdout.write(f"the Ansible {label} passed; {kept}\n")


def exported_key(profile: str) -> dict[str, str]:
    """The project profile's static access key from the AWS command line, checked in memory. A refusal is a fixed message with no output of the command."""
    try:
        result = run_command("aws", ["configure", "export-credentials", "--profile", profile, "--format", "process"], timeout=KEY_EXPORT_SECONDS)
    except TimeoutExpired:
        raise HostError(KEY_MESSAGES["export_timeout"].format(seconds=KEY_EXPORT_SECONDS)) from None
    except FileNotFoundError:
        raise HostError(KEY_MESSAGES["no_aws"]) from None
    if result.returncode != 0:
        raise HostError(KEY_MESSAGES["export_failed"].format(status=result.returncode))
    if not result.stdout.strip():
        raise HostError(KEY_MESSAGES["empty"])
    try:
        document = json.loads(result.stdout)
    except ValueError:
        document = None
    if not isinstance(document, dict):
        raise HostError(KEY_MESSAGES["not_a_static_key"])
    if any("token" in str(name).lower() for name in document):
        raise HostError(KEY_MESSAGES["session_token"])
    key_id, value = document.get("AccessKeyId"), document.get("SecretAccessKey")
    if not isinstance(key_id, str) or not isinstance(value, str):
        raise HostError(KEY_MESSAGES["not_a_static_key"])
    if not key_id or not value:
        raise HostError(KEY_MESSAGES["empty"])
    if KEY_ID_FORM.fullmatch(key_id) is None or KEY_VALUE_FORM.fullmatch(value) is None:
        raise HostError(KEY_MESSAGES["not_a_static_key"])
    return {"aws_access_key_id": key_id, "aws_secret_access_key": value}


def check_policy_file(run_id: str, address: str, source: Path, target: str, messages: dict[str, str]) -> None:
    """Refuse, before a secret is sent, a host whose sudo policy file for the script is missing or is not the repository's file.

    The file turns sudo's recording of input and output off for the script's command line, and the secret arrives on standard input. The check reads the file's
    SHA-256 with one remote call (``sudo -n sha256sum <file>``). It shows that the repository's file is in place unchanged. It does not read sudo's effective
    settings, whose output format the sudoers manual does not define, so the first host run also proves the effect with a marker test."""
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    try:
        result = run_command("ssh", [*ssh_arguments(run_id), f"{HOST_USER}@{address}", "sudo", "-n", "/usr/bin/sha256sum", target], timeout=KEY_SEND_SECONDS)
    except TimeoutExpired:
        raise HostError(messages["ssh_timeout"].format(seconds=KEY_SEND_SECONDS)) from None
    if result.returncode == 255:
        raise HostError(messages["ssh_failed"].format(status=255))
    if result.returncode != 0:
        raise HostError(messages["policy_missing"])
    words = result.stdout.split()
    if not words or words[0] != expected:
        raise HostError(messages["policy_differs"])


def check_sudo_policy(run_id: str, address: str) -> None:
    """The check for the key script's sudo policy file."""
    check_policy_file(run_id, address, KEY_POLICY_SOURCE, SUDOERS_FILE, KEY_MESSAGES)


def licence_fact(code: str) -> str:
    """The non-secret fact for the AIStor playbook that a placement code stands for: ``placed`` or ``absent``."""
    if not isinstance(code, str) or code not in PLACEMENT_FACTS:
        raise ValueError("the licence placement is not one of placed, unchanged, absent and no_source")
    return PLACEMENT_FACTS[code]


def place_aistor_licence(run_id: str, address: str) -> str:
    """Read AIStor's licence from this computer's OpenBao and place it on the host through one SSH command.

    Return ``placed``, ``unchanged``, ``absent`` or ``no_source``.

    The licence is read in memory (``scripts/infra/aistor_licence.py``) and written to the standard input of ``ssh <host> sudo /usr/bin/python3 -I <the licence
    script>``, which writes the one root-only file. It is never in an argument list, an environment variable, a file on this computer, an output or an error
    text; every failure is a fixed message. A licence that this computer's OpenBao does not hold (its 404) is ``absent``, and a computer with no Keychain
    item for the licence token or no ``security`` command is ``no_source``: neither is a failure, nothing is sent and no notice is printed here, because
    only the host can confirm that it has no licence (``report_aistor_state``)."""
    try:
        licence = aistor_licence.read_licence()
    except aistor_licence.NoLicenceSource:
        return "no_source"
    except aistor_licence.LicenceError as error:
        raise HostError(str(error)) from None
    if licence is None:
        return "absent"
    check_policy_file(run_id, address, LICENCE_POLICY_SOURCE, LICENCE_SUDOERS_FILE, LICENCE_MESSAGES)
    try:
        sent = run_command(
            "ssh", [*ssh_arguments(run_id), f"{HOST_USER}@{address}", "sudo", *LICENCE_COMMAND], input_text=licence, timeout=LICENCE_SEND_SECONDS
        )
    except TimeoutExpired:
        raise HostError(LICENCE_MESSAGES["ssh_timeout"].format(seconds=LICENCE_SEND_SECONDS)) from None
    code = sent.stdout.strip()
    if sent.returncode == 0 and code in ("placed", "unchanged"):
        sys.stdout.write("the AIStor licence is placed on the host\n" if code == "placed" else "the AIStor licence is in place on the host and unchanged\n")
        return code
    if sent.returncode == 3 and code in LICENCE_REFUSALS:
        raise HostError(LICENCE_MESSAGES[f"script_{code}"])
    if sent.returncode not in (0, 3):
        raise HostError(LICENCE_MESSAGES["ssh_failed"].format(status=sent.returncode))
    raise HostError(LICENCE_MESSAGES["script_unknown"])


def report_aistor_state(run_id: str, address: str, placement: str) -> None:
    """After the AIStor playbook: say whether AIStor was skipped, only when this computer sent no licence and the host's own record confirms the skip.

    The record is the file that the playbook writes (``skipped: ...`` or ``started``), read with one fixed read-only command. When it cannot be read or is not a
    known record, the notice says that the state is unconfirmed and claims no skip."""
    if licence_fact(placement) == "placed":
        return
    state = ssh_output(run_id, address, f"cat {AISTOR_STATE_FILE}", SSH_ATTEMPT_SECONDS)
    record = state.strip() if state else ""
    if record.startswith("skipped:"):
        sys.stdout.write(f"{LICENCE_NO_SOURCE_NOTICE if placement == 'no_source' else LICENCE_ABSENT_NOTICE}\n")
    elif record == "started":
        sys.stdout.write(f"{LICENCE_KEPT_NOTICE}\n")
    else:
        sys.stdout.write(f"{LICENCE_UNCONFIRMED_NOTICE}\n")


def place_project_key(run_id: str, address: str, profile: str) -> None:
    """Read the project profile's access key on this computer and place it on the host through one SSH command.

    The key is exported with the AWS command line (deadline, captured output), checked in memory and written to the standard input of ``ssh <host>
    sudo /usr/bin/python3 -I <the key script>``, which writes the two root-only files. It is never in an argument list, an environment variable, a
    file on this computer, an output or an error text. Every failure is a fixed message from ``KEY_MESSAGES``. The script's one-line answer is the
    only output read."""
    if PATTERNS["AWS_PROFILE"].fullmatch(profile) is None:
        raise HostError("AWS_PROFILE is not a profile name")
    check_sudo_policy(run_id, address)
    document = json.dumps(exported_key(profile))
    try:
        sent = run_command("ssh", [*ssh_arguments(run_id), f"{HOST_USER}@{address}", "sudo", *KEY_COMMAND], input_text=document, timeout=KEY_SEND_SECONDS)
    except TimeoutExpired:
        raise HostError(KEY_MESSAGES["ssh_timeout"].format(seconds=KEY_SEND_SECONDS)) from None
    code = sent.stdout.strip()
    if sent.returncode == 0 and code in ("placed", "unchanged"):
        sys.stdout.write(
            "the project profile's access key is placed on the host\n"
            if code == "placed"
            else "the project profile's access key is in place on the host and unchanged\n"
        )
        return
    if sent.returncode == 3 and code in SCRIPT_REFUSALS:
        raise HostError(KEY_MESSAGES[f"script_{code}"])
    if sent.returncode not in (0, 3):
        raise HostError(KEY_MESSAGES["ssh_failed"].format(status=sent.returncode))
    raise HostError(KEY_MESSAGES["script_unknown"])


def host_up(aws: Aws, configuration: dict[str, str], binding: Path, confirm: str) -> int:
    existing = load_run(binding) if binding.is_file() else None
    backend, provision_host = (existing.backend, existing.provision_host) if existing else ("s3", True)
    if state_bootstrap.create(aws, configuration, binding, backend, provision_host) != 0:
        raise HostError("the state bootstrap step failed")
    run = load_run(binding)
    if confirm:
        apply_plan(run, confirm)
    elif save_plan(run, configuration, "apply"):
        sys.stdout.write("stopped for review: read the full plan text, then run make host-up CONFIRM=<the code above>\n")
        return 0
    host_name = f"platform-{configuration['ENVIRONMENT']}"
    if run.backend == "local" or not run.provision_host:
        sys.stdout.write(f"run {run.run_id}: the host already exists and Terraform does not touch it; deploy to it with the deploy workflow\n")
        write_inventory(binding)
        return 0
    state = state_json(run, configuration)
    plan = saved_plan(run.run_id, applied=bool(confirm))
    require_current_inputs(plan)
    heavy_memory = heavy_memory_variables(state, planned_heavy_figure(plan))
    peer = wait_until_reachable(aws, run.run_id, instance_id(state), host_name)
    address = peer.address
    record_tailscale_device(run.run_id, host_name, peer.device_id)
    require_current_inputs(plan)
    variables = workload_variables(state, configuration["AWS_REGION"]) | heavy_memory | deploy_variables(configuration, platform_commit())
    run_ansible(run.run_id, address, variables)
    placement = place_aistor_licence(run.run_id, address)
    run_ansible(run.run_id, address, variables | {"aistor_licence_placement": licence_fact(placement)}, "storage.yml")
    report_aistor_state(run.run_id, address, placement)
    place_project_key(run.run_id, address, configuration["AWS_PROFILE"])
    run_ansible(run.run_id, address, variables, "connection.yml")
    write_inventory(binding)
    sys.stdout.write(f"run {run.run_id}: the host {host_name} is up and its inventory is written\n")
    return 0


def host_stop(aws: Aws, configuration: dict[str, str], binding: Path) -> int:
    run = load_run(binding)
    instance = instance_id(state_json(run, configuration))
    aws.call("ec2", "stop-instances", "--instance-ids", instance)
    wait_for("the instance to stop", lambda budget: instance_state(aws, instance, budget) == "stopped" or None, INSTANCE_DEADLINE_SECONDS)
    sys.stdout.write(f"run {run.run_id}: the host is stopped and its disk is kept\n{STOPPED_BILLING}\n")
    return 0


def host_start(aws: Aws, configuration: dict[str, str], binding: Path) -> int:
    run = load_run(binding)
    instance = instance_id(state_json(run, configuration))
    aws.call("ec2", "start-instances", "--instance-ids", instance)
    host_name = f"platform-{configuration['ENVIRONMENT']}"
    peer = wait_until_reachable(aws, run.run_id, instance, host_name)
    record_tailscale_device(run.run_id, host_name, peer.device_id)
    write_inventory(binding)
    sys.stdout.write(f"run {run.run_id}: the host {host_name} is started and answers over Tailscale\n")
    return 0


def host_check(host_name: str, instance: str) -> int:
    """Succeed only when the peer with the host's name and tag answers over SSH as the instance with the given ID."""
    address = host_peer(host_name).address
    keep_host_keys_of("deploy", instance)
    found = wait_for("SSH over Tailscale", lambda budget: ssh_output("deploy", address, INSTANCE_ID_COMMAND, budget), SSH_DEADLINE_SECONDS)
    require_instance(found, instance, host_name)
    sys.stdout.write(f"the host answers over Tailscale SSH as {host_name}\n")
    return 0


def main() -> int:
    """Run one host command and return a non-zero code on a refusal, an expired deadline or a failure."""
    parser = QuietParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["up", "stop", "start", "check"])
    parser.add_argument("--host-name", default="", help="check: the host's Tailscale name, for example platform-dev")
    parser.add_argument("--instance-id", default="", help="check: the EC2 instance ID that the host must answer with")
    parser.add_argument("--confirm", default="", help="up: the confirmation code of the reviewed plan")
    parser.add_argument("--binding", type=Path, default=state_bootstrap.DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    if arguments.command == "check" and not arguments.host_name:
        parser.error("check needs --host-name")
    if arguments.command == "check" and INSTANCE_ID_FORM.fullmatch(arguments.instance_id) is None:
        parser.error("check needs --instance-id, an EC2 instance ID: the letters i- and 8 to 17 hexadecimal digits")
    try:
        if arguments.command == "check":
            return host_check(arguments.host_name, arguments.instance_id)
        configuration = load_configuration()
        aws = Aws(configuration["AWS_PROFILE"], configuration["AWS_REGION"])
        if arguments.command == "up":
            return host_up(aws, configuration, arguments.binding, arguments.confirm)
        return (host_stop if arguments.command == "stop" else host_start)(aws, configuration, arguments.binding)
    except (ConfigurationError, WrapperError, HostError, AwsError, WarehouseError) as error:
        sys.stderr.write(f"{describe_error(error)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
