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

import argparse
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
from scripts.infra import state_bootstrap
from scripts.infra.created import DEVICE_FILE, module_resources
from scripts.infra.inventory import write_inventory
from scripts.infra.terraform_wrapper import WrapperError, apply_plan, kept_note, load_run, save_plan, state_json
from scripts.process import TimeoutExpired, partial_text, run_command, timeout_text
from scripts.project_config import ROOT, ConfigurationError, load_configuration

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
ANSIBLE_TIMEOUT_SECONDS = 2400
RELEASE_DEADLINE_SECONDS = 300
WORKLOAD_GID_BASE = 20000
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
    except FileNotFoundError as error:
        raise HostError(f"the Tailscale client is not installed on this computer: {error}") from None
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
    target = ROOT / "runs" / run_id / DEVICE_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"host_name": host_name, "device_id": device_id}, indent=2) + "\n")
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
    owner.write_text(f"{instance}\n")


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
    """The Ansible variables for workload access: the region and each workload role, with a group ID fixed by name order."""
    roles = state.get("values", {}).get("outputs", {}).get("workload_role_arns", {}).get("value", {})
    workloads = [{"name": name, "role_arn": roles[name], "gid": WORKLOAD_GID_BASE + index} for index, name in enumerate(sorted(roles))]
    return {"workload_access_region": region, "workload_access_workloads": workloads}


def ansible_recap(stdout: str) -> str:
    """The count lines of the playbook's recap for the host group, and nothing else: task output can hold variable values."""
    recap = stdout[stdout.rfind("PLAY RECAP") :].splitlines()[1:] if "PLAY RECAP" in stdout else []
    return "".join(f"{line.strip()}\n" for line in recap if RECAP_LINE.fullmatch(line.strip()))


def run_ansible(run_id: str, address: str, variables: dict[str, Any]) -> None:
    home = ROOT / ".tools" / "ansible"
    settings = {"ANSIBLE_CONFIG": "ansible.cfg", "ANSIBLE_HOME": str(home), "ANSIBLE_LOCAL_TEMP": str(home / "tmp")}
    environment = os.environ | settings | {"PLATFORM_HOST": address, "ANSIBLE_SSH_COMMON_ARGS": " ".join(ssh_arguments(run_id))}
    try:
        result = run_command(
            str(ROOT / ".venv" / "bin" / "ansible-playbook"),
            ["host.yml", "--extra-vars", json.dumps(variables)],
            cwd=ROOT / "ansible",
            env=environment,
            timeout=ANSIBLE_TIMEOUT_SECONDS,
        )
    except TimeoutExpired as error:
        partial = partial_text(error.stdout) + partial_text(error.stderr)
        note = f"; {kept_note(run_id, 'ansible_output.txt', partial, 'partial output')}" if partial else ""
        raise HostError(timeout_text("ansible-playbook host.yml", ANSIBLE_TIMEOUT_SECONDS) + note) from None
    kept = kept_note(run_id, "ansible_output.txt", result.stdout + result.stderr, "full output")
    sys.stdout.write(ansible_recap(result.stdout))
    if result.returncode != 0:
        raise HostError(f"the Ansible host setup failed; {kept}")
    sys.stdout.write(f"the Ansible host setup passed; {kept}\n")


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
    peer = wait_until_reachable(aws, run.run_id, instance_id(state), host_name)
    address = peer.address
    record_tailscale_device(run.run_id, host_name, peer.device_id)
    run_ansible(run.run_id, address, workload_variables(state, configuration["AWS_REGION"]))
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
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
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
    except (ConfigurationError, WrapperError, HostError, AwsError) as error:
        sys.stderr.write(f"{error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
