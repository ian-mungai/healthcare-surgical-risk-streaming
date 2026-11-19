"""Create, stop and start the build and test host.

Run from the repository root under the project profile in ``.env``, through ``make``:

``make host-up`` creates the run's state bucket when it is absent and saves a Terraform plan. When the plan changes
anything, it prints the changes and a confirmation code and stops for the owner's review.
``make host-up CONFIRM=<CODE>`` applies exactly the reviewed plan, waits for the instance, for its Tailscale peer and
for SSH, runs the Ansible host setup and writes the inventory. A run with nothing to change goes straight to those
checks, so a second run changes nothing.
``make host-stop`` stops the instance, keeps its disk and prints what still bills.
``make host-start`` starts it and waits until it answers over Tailscale.

Every wait has a deadline and fails with a message that names what did not happen.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.aws_cli import Aws, AwsError
from scripts.infra import state_bootstrap
from scripts.infra.inventory import module_resources, write_inventory
from scripts.infra.terraform_wrapper import WrapperError, apply_plan, load_run, save_plan, state_json
from scripts.process import TimeoutExpired, run_command
from scripts.project_config import ROOT, ConfigurationError, load_configuration

TAILSCALE_APP = "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
HOST_TAG = "tag:surgical-host"
HOST_USER = "ubuntu"
INSTANCE_DEADLINE_SECONDS = 600
PEER_DEADLINE_SECONDS = 600
SSH_DEADLINE_SECONDS = 300
POLL_SECONDS = 10
SSH_ATTEMPT_SECONDS = 30
ANSIBLE_TIMEOUT_SECONDS = 2400
WORKLOAD_GID_BASE = 20000
STOPPED_BILLING = "still billed while stopped: the host's 200 GiB disk and the objects in the project's buckets; not billed: the instance's hours"


class HostError(RuntimeError):
    """A host command refused to continue, a wait passed its deadline or a call failed."""


def wait_for(what: str, check: Callable[[], bool], deadline_seconds: int) -> None:
    deadline = time.monotonic() + deadline_seconds
    while not check():
        if time.monotonic() >= deadline:
            raise HostError(f"deadline of {deadline_seconds} seconds passed while waiting for {what}")
        time.sleep(POLL_SECONDS)
    sys.stdout.write(f"ready: {what}\n")


def select_peer(status: dict[str, Any], host_name: str, tag: str) -> str:
    """The Tailscale DNS name of the newest online peer that has the host's name and tag."""
    peers = [peer for peer in status.get("Peer", {}).values() if peer.get("HostName") == host_name and peer.get("Online") and tag in (peer.get("Tags") or [])]
    if not peers:
        raise HostError(f"no online peer named {host_name} with {tag} in the Tailscale network")
    return str(max(peers, key=lambda peer: peer["Created"])["DNSName"]).rstrip(".")


def host_peer(host_name: str) -> str:
    program = "tailscale" if shutil.which("tailscale") else TAILSCALE_APP
    try:
        result = run_command(program, ["status", "--json"], timeout=SSH_ATTEMPT_SECONDS)
    except (FileNotFoundError, TimeoutExpired) as error:
        raise HostError(f"the Tailscale client on this computer did not answer: {error}") from None
    if result.returncode != 0:
        raise HostError(f"tailscale status failed: {result.stderr.strip()}")
    return select_peer(json.loads(result.stdout), host_name, HOST_TAG)


def peer_online(host_name: str) -> bool:
    try:
        host_peer(host_name)
    except HostError:
        return False
    return True


def ssh_arguments(run_id: str) -> list[str]:
    known_hosts = ROOT / "runs" / run_id / "known_hosts"
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    return ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", f"UserKnownHostsFile={known_hosts}", "-o", "StrictHostKeyChecking=accept-new"]


def ssh_answers(run_id: str, address: str) -> bool:
    try:
        return run_command("ssh", [*ssh_arguments(run_id), f"{HOST_USER}@{address}", "true"], timeout=SSH_ATTEMPT_SECONDS).returncode == 0
    except TimeoutExpired:
        return False


def instance_id(state: dict[str, Any]) -> str:
    found = [
        item["values"]["id"]
        for item in module_resources(state.get("values", {}).get("root_module", {}))
        if item["mode"] == "managed" and item["type"] == "aws_instance"
    ]
    if len(found) != 1:
        raise HostError("the run's state does not hold exactly one host instance: run make host-up first")
    return str(found[0])


def instance_state(aws: Aws, instance: str) -> str:
    return str(aws.call("ec2", "describe-instances", "--instance-ids", instance, "--query", "Reservations[0].Instances[0].State.Name"))


def wait_until_reachable(aws: Aws, run_id: str, instance: str, host_name: str) -> str:
    wait_for("the instance to run", lambda: instance_state(aws, instance) == "running", INSTANCE_DEADLINE_SECONDS)
    wait_for(f"the Tailscale peer {host_name} to be online", lambda: peer_online(host_name), PEER_DEADLINE_SECONDS)
    address = host_peer(host_name)
    wait_for("SSH over Tailscale", lambda: ssh_answers(run_id, address), SSH_DEADLINE_SECONDS)
    return address


def workload_variables(state: dict[str, Any], region: str) -> dict[str, Any]:
    """The Ansible variables for workload access: the region and each workload role, with a group ID fixed by name order."""
    roles = state.get("values", {}).get("outputs", {}).get("workload_role_arns", {}).get("value", {})
    workloads = [{"name": name, "role_arn": roles[name], "gid": WORKLOAD_GID_BASE + index} for index, name in enumerate(sorted(roles))]
    return {"workload_access_region": region, "workload_access_workloads": workloads}


def run_ansible(run_id: str, address: str, variables: dict[str, Any]) -> None:
    environment = os.environ | {"ANSIBLE_CONFIG": "ansible.cfg", "PLATFORM_HOST": address, "ANSIBLE_SSH_COMMON_ARGS": " ".join(ssh_arguments(run_id))}
    result = run_command(
        str(ROOT / ".venv" / "bin" / "ansible-playbook"),
        ["host.yml", "--extra-vars", json.dumps(variables)],
        cwd=ROOT / "ansible",
        env=environment,
        timeout=ANSIBLE_TIMEOUT_SECONDS,
    )
    recap = result.stdout[result.stdout.rfind("PLAY RECAP") :] if "PLAY RECAP" in result.stdout else result.stdout[-2000:]
    sys.stdout.write(recap)
    if result.returncode != 0:
        raise HostError(f"the Ansible host setup failed:\n{result.stdout[-4000:]}{result.stderr[-2000:]}")


def host_up(aws: Aws, configuration: dict[str, str], binding: Path, confirm: str) -> int:
    if state_bootstrap.create(aws, configuration, binding) != 0:
        raise HostError("the state bootstrap step failed")
    run = load_run(binding)
    if confirm:
        apply_plan(run, confirm)
    elif save_plan(run, configuration, "apply"):
        sys.stdout.write("stopped for review: read the full plan text, then run make host-up CONFIRM=<the code above>\n")
        return 0
    host_name = f"platform-{configuration['ENVIRONMENT']}"
    if run.backend == "local":
        sys.stdout.write(f"run {run.run_id}: the host already exists and Terraform does not touch it; deploy to it with the deploy workflow\n")
        write_inventory(binding)
        return 0
    state = state_json(run, configuration)
    address = wait_until_reachable(aws, run.run_id, instance_id(state), host_name)
    run_ansible(run.run_id, address, workload_variables(state, configuration["AWS_REGION"]))
    write_inventory(binding)
    sys.stdout.write(f"run {run.run_id}: the host is up at {address} and its inventory is written\n")
    return 0


def host_stop(aws: Aws, configuration: dict[str, str], binding: Path) -> int:
    run = load_run(binding)
    instance = instance_id(state_json(run, configuration))
    aws.call("ec2", "stop-instances", "--instance-ids", instance)
    wait_for("the instance to stop", lambda: instance_state(aws, instance) == "stopped", INSTANCE_DEADLINE_SECONDS)
    sys.stdout.write(f"run {run.run_id}: the host is stopped and its disk is kept\n{STOPPED_BILLING}\n")
    return 0


def host_start(aws: Aws, configuration: dict[str, str], binding: Path) -> int:
    run = load_run(binding)
    instance = instance_id(state_json(run, configuration))
    aws.call("ec2", "start-instances", "--instance-ids", instance)
    address = wait_until_reachable(aws, run.run_id, instance, f"platform-{configuration['ENVIRONMENT']}")
    sys.stdout.write(f"run {run.run_id}: the host is started and answers at {address}\n")
    return 0


def host_check(host_name: str) -> int:
    address = host_peer(host_name)
    wait_for("SSH over Tailscale", lambda: ssh_answers("deploy", address), SSH_DEADLINE_SECONDS)
    sys.stdout.write(f"the host answers over Tailscale SSH as {host_name}\n")
    return 0


def main() -> int:
    """Run one host command and return a non-zero code on a refusal, an expired deadline or a failure."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("command", choices=["up", "stop", "start", "check"])
    parser.add_argument("--host-name", default="", help="check: the host's Tailscale name, for example platform-dev")
    parser.add_argument("--confirm", default="", help="up: the confirmation code of the reviewed plan")
    parser.add_argument("--binding", type=Path, default=state_bootstrap.DEFAULT_BINDING, help="the run binding file; the default is runs/run_binding.json")
    arguments = parser.parse_args()
    try:
        if arguments.command == "check":
            return host_check(arguments.host_name)
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
