"""The parts of the host commands that decide before any call: which Tailscale peer is the host and what a run owns.

The commands' calls to AWS, Tailscale, Terraform and Ansible are proven on the live host. These cases cover choices a
live run cannot readily force: a stale peer with the host's name, an offline peer, a peer without the host tag and a
Terraform state with data sources and nested modules.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import stat
import sys
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import Aws
from scripts.infra import host, state_bootstrap, terraform_wrapper
from scripts.infra.host import (
    HostError,
    Peer,
    host_check,
    instance_id,
    record_tailscale_device,
    release_tailscale_device,
    run_ansible,
    select_peer,
    tailscale_device_id,
    wait_for,
    wait_until_reachable,
    workload_variables,
)
from scripts.infra.state_bootstrap import RunBinding
from scripts.process import TimeoutExpired
from scripts.project_config import ROOT


def peer(
    name: str,
    *,
    online: bool = True,
    tags: tuple[str, ...] = ("tag:surgical-host",),
    created: str = "2026-01-02T00:00:00Z",
    dns: str = "",
    device: str = "nA1CNTRL",
) -> dict[str, object]:
    return {"HostName": name, "Online": online, "Tags": list(tags), "Created": created, "DNSName": dns or f"{name}.example.ts.net.", "ID": device}


class SelectPeerTest(unittest.TestCase):
    def test_online_tagged_peer_is_selected_with_its_device_id(self) -> None:
        status = {"Peer": {"a": peer("platform-dev", device="nB2CNTRL"), "b": peer("laptop", tags=())}}
        self.assertEqual(Peer("platform-dev.example.ts.net", "nB2CNTRL"), select_peer(status, "platform-dev", "tag:surgical-host"))

    def test_two_online_peers_with_the_name_and_tag_are_refused(self) -> None:
        old = peer("platform-dev", created="2026-01-01T00:00:00Z", dns="platform-dev.example.ts.net.", device="nOLDCNTRL")
        new = peer("platform-dev", created="2026-01-03T00:00:00Z", dns="platform-dev-1.example.ts.net.", device="nNEWCNTRL")
        with self.assertRaisesRegex(HostError, "2 online peers are named platform-dev"):
            select_peer({"Peer": {"a": old, "b": new}}, "platform-dev", "tag:surgical-host")

    def test_an_offline_twin_does_not_make_the_peer_ambiguous(self) -> None:
        status = {"Peer": {"a": peer("platform-dev", online=False, device="nOLDCNTRL"), "b": peer("platform-dev", device="nNEWCNTRL")}}
        self.assertEqual("nNEWCNTRL", select_peer(status, "platform-dev", "tag:surgical-host").device_id)

    def test_a_peer_with_another_name_does_not_make_the_peer_ambiguous(self) -> None:
        status = {"Peer": {"a": peer("platform-prod", device="nOTHERCNTRL"), "b": peer("platform-dev", device="nNEWCNTRL")}}
        self.assertEqual("nNEWCNTRL", select_peer(status, "platform-dev", "tag:surgical-host").device_id)

    def test_a_twin_without_the_tag_does_not_make_the_peer_ambiguous(self) -> None:
        status = {"Peer": {"a": peer("platform-dev", tags=(), device="nOTHERCNTRL"), "b": peer("platform-dev", device="nNEWCNTRL")}}
        self.assertEqual("nNEWCNTRL", select_peer(status, "platform-dev", "tag:surgical-host").device_id)

    def test_offline_peer_is_not_the_host(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({"Peer": {"a": peer("platform-dev", online=False)}}, "platform-dev", "tag:surgical-host")

    def test_untagged_peer_with_the_name_is_not_the_host(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({"Peer": {"a": peer("platform-dev", tags=())}}, "platform-dev", "tag:surgical-host")

    def test_status_with_no_peer_list_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({"Peer": None}, "platform-dev", "tag:surgical-host")

    def test_empty_status_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "no online peer"):
            select_peer({}, "platform-dev", "tag:surgical-host")


class FakeClock:
    """A clock that only moves when a wait sleeps or a probe uses time, so a wait's deadline is exact."""

    def __init__(self) -> None:
        self.now = 0.0

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class WaitForTest(unittest.TestCase):
    def test_the_first_result_is_returned(self) -> None:
        clock = FakeClock()
        self.assertEqual("found", wait_for("a thing", lambda budget: "found", 30, clock=lambda: clock.now, sleep=clock.sleep))

    def test_each_probe_gets_the_time_that_remains_and_the_last_sleep_stops_at_the_deadline(self) -> None:
        clock = FakeClock()
        budgets: list[float] = []

        def probe(budget: float) -> None:
            budgets.append(budget)

        with self.assertRaisesRegex(HostError, "deadline of 25 seconds passed while waiting for a thing"):
            wait_for("a thing", probe, 25, clock=lambda: clock.now, sleep=clock.sleep)
        self.assertEqual([25, 15, 5], budgets)
        self.assertEqual(25, clock.now)

    def test_a_probe_that_runs_past_the_deadline_cannot_succeed(self) -> None:
        clock = FakeClock()

        def slow(budget: float) -> str:
            clock.now += budget + 1
            return "late"

        with self.assertRaisesRegex(HostError, "deadline of 20 seconds passed"):
            wait_for("a thing", slow, 20, clock=lambda: clock.now, sleep=clock.sleep)

    def test_a_call_that_times_out_counts_as_not_ready_and_the_wait_goes_on(self) -> None:
        clock = FakeClock()
        answers = iter([True, True, False])

        def probe(budget: float) -> str | None:
            if next(answers):
                clock.now += 1
                raise TimeoutExpired("x", 1)
            return "ready"

        self.assertEqual("ready", wait_for("a thing", probe, 60, clock=lambda: clock.now, sleep=clock.sleep))

    def test_a_call_whose_kill_is_refused_because_its_process_just_exited_counts_as_a_timeout(self) -> None:
        clock = FakeClock()
        calls = []

        def probe(budget: float) -> str | None:
            calls.append(budget)
            if len(calls) == 1:
                raise PermissionError("Operation not permitted")
            return "ready"

        self.assertEqual("ready", wait_for("a thing", probe, 60, clock=lambda: clock.now, sleep=clock.sleep))

    def test_a_call_that_always_times_out_ends_at_the_deadline(self) -> None:
        clock = FakeClock()

        def probe(budget: float) -> None:
            clock.now += budget
            raise TimeoutExpired("x", budget)

        with self.assertRaisesRegex(HostError, "deadline of 30 seconds passed"):
            wait_for("a thing", probe, 30, clock=lambda: clock.now, sleep=clock.sleep)
        self.assertEqual(30, clock.now)


STATUS_TOOL = (
    '#!/bin/sh\nsleep "${FAKE_STATUS_DELAY:-0}"\n'
    'if [ -e "$FAKE_SSH_LOG" ] && [ -n "${FAKE_STATUS_AFTER:-}" ]; then cat "$FAKE_STATUS_AFTER"; else cat "$FAKE_STATUS"; fi\n'
)
SSH_TOOL = (
    '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$FAKE_SSH_LOG"\nprintf \'%s\\n\' "$FAKE_INSTANCE"\n'
    'case "$*" in *"tailscale logout"*) exit "${FAKE_LOGOUT_EXIT:-0}" ;; esac\nexit "${FAKE_SSH_EXIT:-0}"\n'
)
INSTANCE = "i-0123456789abcdef0"
OTHER_INSTANCE = "i-0fedcba9876543210"


@dataclass(frozen=True)
class FakeAws(Aws):
    state: str = "running"

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = 120) -> Any:
        return self.state


@contextmanager
def fake_tools(
    status: Mapping[str, object], instance: str, ssh_exit: int = 0, after: Mapping[str, object] | None = None, logout_exit: int = 0
) -> Iterator[Path]:
    """A ``tailscale`` that prints the given status and an ``ssh`` that logs its arguments and prints the given instance ID, first on PATH."""
    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch)
        for name, text in (("tailscale", STATUS_TOOL), ("ssh", SSH_TOOL)):
            (folder / name).write_text(text)
            (folder / name).chmod(stat.S_IRWXU)
        (folder / "status.json").write_text(json.dumps(status))
        (folder / "status_after.json").write_text(json.dumps(after if after is not None else status))
        environment = {
            "FAKE_STATUS_AFTER": str(folder / "status_after.json") if after is not None else "",
            "PATH": f"{folder}:{os.environ['PATH']}",
            "FAKE_STATUS": str(folder / "status.json"),
            "FAKE_SSH_LOG": str(folder / "ssh.log"),
            "FAKE_INSTANCE": instance,
            "FAKE_SSH_EXIT": str(ssh_exit),
            "FAKE_LOGOUT_EXIT": str(logout_exit),
        }
        with mock.patch.dict(os.environ, environment), mock.patch.object(host, "ROOT", folder), mock.patch.object(host, "POLL_SECONDS", 0.01):
            yield folder


class ReachableHostTest(unittest.TestCase):
    """The host commands' choice of the Tailscale peer, through fake ``tailscale`` and ``ssh`` programs that run as real processes."""

    def reach(self, status: Mapping[str, object], reported_instance: str, instance: str = "i-0123456789abcdef0", ssh_exit: int = 0) -> Peer:
        with fake_tools(status, reported_instance, ssh_exit):
            return wait_until_reachable(FakeAws("project", "us-west-2"), "run-one", instance, "platform-dev")

    def test_the_peer_whose_instance_is_the_runs_instance_is_reached(self) -> None:
        status = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        self.assertEqual(Peer("platform-dev.example.ts.net", "nNEWCNTRL"), self.reach(status, "i-0123456789abcdef0"))

    def test_the_peer_is_asked_for_its_instance_over_ssh(self) -> None:
        status = {"Peer": {"a": peer("platform-dev")}}
        with fake_tools(status, "i-0123456789abcdef0") as folder:
            wait_until_reachable(FakeAws("project", "us-west-2"), "run-one", "i-0123456789abcdef0", "platform-dev")
            logged = (folder / "ssh.log").read_text().splitlines()
        address = "platform-dev.example.ts.net"
        self.assertIn(f"ubuntu@{address}", logged)
        self.assertIn("cat /var/lib/cloud/data/instance-id", logged)

    def test_only_an_older_peer_is_visible_while_the_new_instance_starts(self) -> None:
        status = {"Peer": {"old": peer("platform-dev", created="2026-01-01T00:00:00Z", device="nOLDCNTRL")}}
        with self.assertRaisesRegex(HostError, "belongs to a different instance than this run's"):
            self.reach(status, "i-0fedcba9876543210")

    def test_a_refusal_names_no_instance_id(self) -> None:
        status = {"Peer": {"old": peer("platform-dev")}}
        with self.assertRaises(HostError) as caught:
            self.reach(status, "i-0fedcba9876543210")
        self.assertNotIn("i-0", str(caught.exception))

    def test_two_online_peers_are_refused_before_ssh(self) -> None:
        status = {"Peer": {"a": peer("platform-dev", device="nAAACNTRL"), "b": peer("platform-dev", device="nBBBCNTRL")}}
        with (
            fake_tools(status, "i-0123456789abcdef0") as folder,
            mock.patch.object(host, "PEER_DEADLINE_SECONDS", 1),
            self.assertRaisesRegex(HostError, "2 online peers"),
        ):
            wait_until_reachable(FakeAws("project", "us-west-2"), "run-one", "i-0123456789abcdef0", "platform-dev")
        self.assertFalse((folder / "ssh.log").exists())

    def test_a_status_with_no_peer_map_waits_and_then_expires(self) -> None:
        with mock.patch.object(host, "PEER_DEADLINE_SECONDS", 1), self.assertRaisesRegex(HostError, "while waiting for the Tailscale peer platform-dev"):
            self.reach({"Peer": None}, "i-0123456789abcdef0")

    def test_a_tailscale_client_that_does_not_answer_is_retried_until_the_deadline(self) -> None:
        with (
            mock.patch.dict(os.environ, {"FAKE_STATUS_DELAY": "5"}),
            mock.patch.object(host, "PEER_DEADLINE_SECONDS", 1),
            mock.patch.object(host, "SSH_ATTEMPT_SECONDS", 0.2),
            self.assertRaisesRegex(HostError, "while waiting for the Tailscale peer platform-dev"),
        ):
            self.reach({"Peer": {}}, "i-0123456789abcdef0")

    def test_a_peer_whose_ssh_never_answers_expires(self) -> None:
        status = {"Peer": {"a": peer("platform-dev")}}
        with mock.patch.object(host, "SSH_DEADLINE_SECONDS", 1), self.assertRaisesRegex(HostError, "while waiting for SSH over Tailscale"):
            self.reach(status, "i-0123456789abcdef0", ssh_exit=255)

    def test_an_instance_that_is_not_running_expires(self) -> None:
        with (
            fake_tools({"Peer": {}}, ""),
            mock.patch.object(host, "INSTANCE_DEADLINE_SECONDS", 1),
            self.assertRaisesRegex(HostError, "while waiting for the instance to run"),
        ):
            wait_until_reachable(FakeAws("project", "us-west-2", "pending"), "run-one", "i-0123456789abcdef0", "platform-dev")


class TailscaleDeviceTest(unittest.TestCase):
    """The host's Tailscale device: its ID from the status, its record in the run folder and its removal before the instance is destroyed."""

    def test_the_device_id_is_the_one_online_peer_s_id(self) -> None:
        with fake_tools({"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}, ""):
            self.assertEqual("nNEWCNTRL", tailscale_device_id("platform-dev"))

    def test_the_device_is_recorded_in_the_run_folder(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, mock.patch.object(host, "ROOT", Path(scratch)):
            record_tailscale_device("run-one", "platform-dev", "nNEWCNTRL")
            record_tailscale_device("run-one", "platform-dev", "nSECONDCNTRL")
            written = json.loads((Path(scratch) / "runs" / "run-one" / "tailscale_device.json").read_text())
        self.assertEqual({"host_name": "platform-dev", "device_id": "nSECONDCNTRL"}, written)

    def test_a_logged_out_host_is_awaited_until_its_device_is_gone(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, after={"Peer": {}}) as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            logged = (folder / "ssh.log").read_text().splitlines()
        address = "platform-dev.example.ts.net"
        self.assertIn(f"ubuntu@{address}", logged)
        self.assertIn("sudo -n tailscale logout", logged)

    def test_a_device_that_is_already_gone_needs_no_ssh(self) -> None:
        with fake_tools({"Peer": {}}, INSTANCE) as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            self.assertFalse((folder / "ssh.log").exists())

    def test_the_device_the_host_has_now_is_logged_out_when_it_differs_from_the_recorded_one_and_its_instance_is_the_runs(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL")}}
        with fake_tools(before, INSTANCE, after={"Peer": {}}) as folder:
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", INSTANCE, 5)
            self.assertIn("sudo -n tailscale logout", (folder / "ssh.log").read_text().splitlines())

    def test_the_peer_is_asked_for_its_instance_before_it_is_logged_out(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, after={"Peer": {}}) as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            logged = (folder / "ssh.log").read_text().splitlines()
        self.assertLess(logged.index("cat /var/lib/cloud/data/instance-id"), logged.index("sudo -n tailscale logout"))

    def test_a_peer_that_is_another_instance_is_refused_with_no_logout_and_no_id_in_the_message(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, OTHER_INSTANCE, after={"Peer": {}}) as folder:
            with self.assertRaises(HostError) as caught:
                release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            logged = (folder / "ssh.log").read_text().splitlines()
        self.assertIn("belongs to a different instance than this run's", str(caught.exception))
        self.assertNotIn("i-0", str(caught.exception))
        self.assertIn("cat /var/lib/cloud/data/instance-id", logged)
        self.assertNotIn("sudo -n tailscale logout", logged)

    def test_a_peer_whose_ssh_does_not_answer_is_refused_with_no_logout(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, ssh_exit=255, after={"Peer": {}}) as folder:
            with self.assertRaisesRegex(HostError, "did not tell its instance"):
                release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            logged = (folder / "ssh.log").read_text().splitlines()
        self.assertIn("cat /var/lib/cloud/data/instance-id", logged)
        self.assertNotIn("sudo -n tailscale logout", logged)

    def test_the_recorded_device_must_be_gone_too(self) -> None:
        after = {"Peer": {"b": peer("platform-dev", online=False, device="nRECORDEDCNTRL")}}
        before = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL"), "b": after["Peer"]["b"]}}
        with (
            fake_tools(before, INSTANCE, after=after),
            self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev to leave the network"),
        ):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", INSTANCE, 1)

    def test_a_restarted_device_that_stays_in_the_network_is_not_gone(self) -> None:
        still = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL")}}
        with fake_tools(still, INSTANCE, after=still), self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev"):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", INSTANCE, 1)

    def test_the_recorded_device_is_awaited_by_its_id_whatever_its_name_has_become(self) -> None:
        renamed = {"Peer": {"x": peer("platform-dev-1", device="nRECORDEDCNTRL")}}
        with fake_tools(renamed, INSTANCE), self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev"):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", INSTANCE, 1)

    def test_a_dropped_ssh_session_after_the_logout_is_no_failure(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, logout_exit=255, after={"Peer": {}}):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)

    def test_a_device_that_stays_in_the_network_ends_at_the_deadline(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with (
            fake_tools(before, INSTANCE),
            self.assertRaisesRegex(HostError, "deadline of 1 seconds passed while waiting for the Tailscale device of platform-dev"),
        ):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 1)

    def test_an_offline_device_cannot_be_logged_out_and_ends_at_the_deadline(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", online=False, device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE) as folder, self.assertRaisesRegex(HostError, "while waiting for the Tailscale device"):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 1)
        self.assertFalse((folder / "ssh.log").exists())

    def test_two_online_peers_are_refused_before_any_logout(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nAAACNTRL"), "b": peer("platform-dev", device="nBBBCNTRL")}}
        with fake_tools(before, INSTANCE) as folder, self.assertRaisesRegex(HostError, "2 online peers"):
            release_tailscale_device("run-one", "platform-dev", "nAAACNTRL", INSTANCE, 5)
        self.assertFalse((folder / "ssh.log").exists())

    def test_a_peer_with_another_name_is_left_alone(self) -> None:
        before = {"Peer": {"a": peer("laptop", device="nLAPTOPCNTRL", tags=())}}
        with fake_tools(before, INSTANCE) as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            self.assertFalse((folder / "ssh.log").exists())


CONFIGURATION = {"AWS_REGION": "us-west-2", "AWS_PROFILE": "example_project_dev", "ENVIRONMENT": "dev", "PROJECT_NAME": "example_project"}


def binding_file(folder: Path, *, backend: str, provision_host: bool) -> Path:
    path = folder / "run_binding.json"
    RunBinding("20260101t0000-abcdef", "0" * 40, backend, "us-west-2", "example-project", "default", ["terraform.tfstate"], "verified", provision_host).write(
        path
    )
    return path


class HostUpTest(unittest.TestCase):
    """``host_up`` follows the run binding that exists. The calls to Terraform, Ansible and the host are replaced and recorded; the live batch proves them."""

    def run_up(self, path: Path, *, confirm: str = "") -> tuple[int, dict[str, mock.Mock]]:
        names = ("save_plan", "apply_plan", "write_inventory", "state_json", "wait_until_reachable", "run_ansible", "instance_id")
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(host, "ROOT", path.parent))
            calls = {name: stack.enter_context(mock.patch.object(host, name)) for name in names}
            calls["save_plan"].return_value = 0
            calls["state_json"].return_value = {"values": {}}
            calls["wait_until_reachable"].return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")
            calls["instance_id"].return_value = "i-0123456789abcdef0"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, confirm)
            self.output = output.getvalue()
        return code, calls

    def test_a_local_run_is_followed_to_its_inventory_with_no_instance_step(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="local", provision_host=False)
            with mock.patch.object(state_bootstrap, "create", return_value=0) as create:
                code, calls = self.run_up(path)
        self.assertEqual(0, code)
        self.assertEqual(("local", False), create.call_args.args[3:])
        calls["write_inventory"].assert_called_once_with(path)
        for name in ("state_json", "wait_until_reachable", "run_ansible", "instance_id"):
            calls[name].assert_not_called()

    def test_a_run_that_was_given_its_host_on_s3_skips_the_instance_steps(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=False)
            with mock.patch.object(state_bootstrap, "create", return_value=0) as create:
                code, calls = self.run_up(path)
        self.assertEqual(0, code)
        self.assertEqual(("s3", False), create.call_args.args[3:])
        calls["write_inventory"].assert_called_once_with(path)
        for name in ("wait_until_reachable", "run_ansible", "instance_id"):
            calls[name].assert_not_called()

    def test_a_run_with_its_own_host_reaches_it_records_its_device_and_runs_ansible(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0) as create:
                code, calls = self.run_up(path)
            recorded = json.loads((Path(scratch) / "runs" / "20260101t0000-abcdef" / "tailscale_device.json").read_text())
        self.assertEqual(0, code)
        self.assertEqual(("s3", True), create.call_args.args[3:])
        calls["wait_until_reachable"].assert_called_once()
        calls["run_ansible"].assert_called_once()
        calls["write_inventory"].assert_called_once_with(path)
        self.assertEqual({"host_name": "platform-dev", "device_id": "nNEWCNTRL"}, recorded)

    def test_the_success_line_of_an_up_run_names_the_host_and_not_its_address(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0):
                self.run_up(path)
        self.assertIn("the host platform-dev is up", self.output)
        self.assertNotIn("example.ts.net", self.output)

    def test_the_success_line_of_a_start_run_names_the_host_and_not_its_address(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            stack.enter_context(mock.patch.object(host, "ROOT", Path(scratch)))
            stack.enter_context(mock.patch.object(host, "state_json", return_value={}))
            stack.enter_context(mock.patch.object(host, "instance_id", return_value=INSTANCE))
            stack.enter_context(mock.patch.object(host, "wait_until_reachable", return_value=Peer("platform-dev.example.ts.net", "nRESTARTEDCNTRL")))
            stack.enter_context(mock.patch.object(host, "write_inventory"))
            output = io.StringIO()
            stack.enter_context(contextlib.redirect_stdout(output))
            host.host_start(FakeAws("project", "us-west-2"), CONFIGURATION, path)
        self.assertIn("the host platform-dev is started", output.getvalue())
        self.assertNotIn("example.ts.net", output.getvalue())

    def test_a_started_host_records_the_device_it_has_now(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            stack.enter_context(mock.patch.object(host, "ROOT", Path(scratch)))
            stack.enter_context(mock.patch.object(host, "state_json", return_value={}))
            stack.enter_context(mock.patch.object(host, "instance_id", return_value="i-0123456789abcdef0"))
            stack.enter_context(mock.patch.object(host, "wait_until_reachable", return_value=Peer("platform-dev.example.ts.net", "nRESTARTEDCNTRL")))
            write_inventory = stack.enter_context(mock.patch.object(host, "write_inventory"))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            aws = FakeAws("project", "us-west-2")
            self.assertEqual(0, host.host_start(aws, CONFIGURATION, path))
            write_inventory.assert_called_once_with(path)
            recorded = json.loads((Path(scratch) / "runs" / "20260101t0000-abcdef" / "tailscale_device.json").read_text())
        self.assertEqual({"host_name": "platform-dev", "device_id": "nRESTARTEDCNTRL"}, recorded)

    def test_without_a_binding_the_step_creates_an_s3_run_with_its_own_host(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "run_binding.json"
            with mock.patch.object(state_bootstrap, "create", return_value=1) as create, self.assertRaisesRegex(HostError, "state bootstrap step failed"):
                self.run_up(path)
        self.assertEqual(("s3", True), create.call_args.args[3:])

    def test_a_plan_with_changes_stops_for_review_before_any_instance_step(self) -> None:
        names = ("wait_until_reachable", "run_ansible", "write_inventory")
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            stack.enter_context(mock.patch.object(state_bootstrap, "create", return_value=0))
            stack.enter_context(mock.patch.object(host, "save_plan", return_value=3))
            calls = {name: stack.enter_context(mock.patch.object(host, name)) for name in names}
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            code = host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, "")
        self.assertEqual(0, code)
        for call in calls.values():
            call.assert_not_called()


STATE = {
    "values": {
        "root_module": {
            "child_modules": [
                {
                    "resources": [
                        {
                            "address": "module.host.aws_instance.host",
                            "mode": "managed",
                            "type": "aws_instance",
                            "values": {"id": "i-0123456789abcdef0", "arn": "arn:aws:ec2:us-west-2:<ACCOUNT>:instance/i-0123456789abcdef0"},
                        },
                        {"address": "module.host.data.aws_vpc.default", "mode": "data", "type": "aws_vpc", "values": {"id": "vpc-0123456789abcdef0"}},
                    ]
                },
                {
                    "resources": [
                        {
                            "address": 'module.storage.aws_s3_bucket.this["landing"]',
                            "mode": "managed",
                            "type": "aws_s3_bucket",
                            "values": {"id": "example-landing"},
                        }
                    ]
                },
            ]
        }
    }
}


class WorkloadVariablesTest(unittest.TestCase):
    def test_each_workload_role_gets_a_group_id_fixed_by_name_order(self) -> None:
        state = {"values": {"outputs": {"workload_role_arns": {"value": {"writer": "arn:writer", "reader": "arn:reader"}}}}}
        expected = [{"name": "reader", "role_arn": "arn:reader", "gid": 20000}, {"name": "writer", "role_arn": "arn:writer", "gid": 20001}]
        self.assertEqual({"workload_access_region": "us-west-2", "workload_access_workloads": expected}, workload_variables(state, "us-west-2"))

    def test_state_without_workloads_gives_an_empty_list(self) -> None:
        self.assertEqual({"workload_access_region": "us-west-2", "workload_access_workloads": []}, workload_variables({"values": {}}, "us-west-2"))


class HostStateTest(unittest.TestCase):
    def test_the_host_instance_is_read_from_the_state(self) -> None:
        self.assertEqual("i-0123456789abcdef0", instance_id(STATE))

    def test_state_without_a_host_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "exactly one host instance"):
            instance_id({"values": {}})


PRIVATE_TEXT = "bucket example-private-bucket role arn:aws:iam::" + "1" * 12 + ":role/example_private_role"
ANSIBLE_TOOL = (
    "#!/bin/sh\n"
    f"printf 'TASK [secret task]\\nfatal: [platform]: FAILED! {PRIVATE_TEXT}\\n'\n"
    'if [ -n "$FAKE_RECAP" ]; then printf \'PLAY RECAP *****\\n%s\\n\' "$FAKE_RECAP"; fi\n'
    f"printf 'stderr line: {PRIVATE_TEXT}\\n' >&2\n"
    'exit "${FAKE_ANSIBLE_EXIT:-0}"\n'
)
RECAP = "platform                   : ok=12   changed=3    unreachable=0    failed=1    skipped=2    rescued=0    ignored=0"


class AnsibleRunTest(unittest.TestCase):
    """``run_ansible`` through a fake ``ansible-playbook`` that runs as a real process and prints private-looking text."""

    def run_it(self, *, exit_code: int, recap: str = RECAP) -> tuple[str, str | None, str]:
        output = io.StringIO()
        with fake_tools({"Peer": {}}, INSTANCE) as folder:
            tool = folder / ".venv" / "bin" / "ansible-playbook"
            tool.parent.mkdir(parents=True)
            tool.write_text(ANSIBLE_TOOL)
            tool.chmod(stat.S_IRWXU)
            (folder / "ansible").mkdir()
            environment = {"FAKE_ANSIBLE_EXIT": str(exit_code), "FAKE_RECAP": recap}
            with mock.patch.dict(os.environ, environment), mock.patch.object(terraform_wrapper, "ROOT", folder), contextlib.redirect_stdout(output):
                try:
                    run_ansible("run-one", "platform-dev.example.ts.net", {})
                except HostError as error:
                    message: str | None = str(error)
                else:
                    message = None
            raw = (folder / "runs" / "run-one" / "raw" / "ansible_output.txt").read_text()
            mode = stat.S_IMODE((folder / "runs" / "run-one" / "raw" / "ansible_output.txt").stat().st_mode)
        self.assertEqual(0o600, mode)
        return output.getvalue(), message, raw

    def test_a_failed_setup_names_only_the_failure_and_the_file_that_keeps_the_output(self) -> None:
        printed, message, raw = self.run_it(exit_code=2)
        self.assertEqual("the Ansible host setup failed; its full output is in runs/run-one/raw/ansible_output.txt", message)
        self.assertIn(f"fatal: [platform]: FAILED! {PRIVATE_TEXT}", raw)
        self.assertIn(f"stderr line: {PRIVATE_TEXT}", raw)
        self.assertNotIn("example-private-bucket", printed)
        self.assertNotIn("example_private_role", printed)

    def test_the_recap_counts_are_printed_and_no_task_text(self) -> None:
        printed, _, _ = self.run_it(exit_code=2)
        self.assertEqual(RECAP + "\n", printed)
        self.assertIn("ok=12", printed)
        self.assertIn("failed=1", printed)
        self.assertNotIn("TASK", printed)
        self.assertNotIn("secret task", printed)
        self.assertNotIn("fatal", printed)

    def test_output_with_no_recap_prints_nothing_and_still_names_the_file(self) -> None:
        printed, message, _ = self.run_it(exit_code=2, recap="")
        self.assertEqual("", printed)
        self.assertEqual("the Ansible host setup failed; its full output is in runs/run-one/raw/ansible_output.txt", message)

    def test_a_recap_line_that_is_not_the_counts_of_the_platform_host_is_not_printed(self) -> None:
        printed, _, _ = self.run_it(exit_code=2, recap=f"platform : {PRIVATE_TEXT}")
        self.assertEqual("", printed)

    def test_a_recap_line_of_another_host_name_is_not_printed(self) -> None:
        printed, _, _ = self.run_it(exit_code=2, recap=RECAP.replace("platform ", "platform-dev.example.ts.net ", 1))
        self.assertEqual("", printed)

    def test_a_successful_setup_prints_the_recap_and_raises_nothing(self) -> None:
        printed, message, _ = self.run_it(exit_code=0)
        self.assertIsNone(message)
        self.assertIn("ok=12", printed)


class TailscaleStatusFailureTest(unittest.TestCase):
    def test_a_failing_client_is_named_by_its_exit_status_and_its_error_text_is_not_repeated(self) -> None:
        with fake_tools({"Peer": {}}, INSTANCE) as folder:
            (folder / "tailscale").write_text(f"#!/bin/sh\necho '{PRIVATE_TEXT}' >&2\necho '{PRIVATE_TEXT}'\nexit 3\n")
            with self.assertRaises(HostError) as caught:
                host.tailscale_status(5)
        self.assertEqual("tailscale status failed: exit status 3", str(caught.exception))


class HostCheckTest(unittest.TestCase):
    """The deploy workflow's check: the peer found by name and tag must answer as the instance the workflow expects."""

    def check(self, reported: str, expected: str = INSTANCE, status: Mapping[str, object] | None = None) -> tuple[int, str, Path]:
        output = io.StringIO()
        with (
            fake_tools(status or {"Peer": {"a": peer("platform-dev")}}, reported) as folder,
            contextlib.redirect_stdout(output),
            mock.patch.object(host, "SSH_DEADLINE_SECONDS", 1),
        ):
            code = host_check("platform-dev", expected)
            logged = (folder / "ssh.log").read_text()
        return code, output.getvalue(), Path(logged)

    def test_the_expected_instance_passes_and_the_output_names_the_host_only(self) -> None:
        code, output, _ = self.check(INSTANCE)
        self.assertEqual(0, code)
        self.assertEqual("the host answers over Tailscale SSH as platform-dev\n", output.replace("ready: SSH over Tailscale\n", ""))

    def test_another_instance_is_refused_with_a_message_that_names_no_id(self) -> None:
        with self.assertRaises(HostError) as caught:
            self.check(OTHER_INSTANCE)
        self.assertIn("belongs to a different instance", str(caught.exception))
        self.assertNotIn("i-0", str(caught.exception))

    def test_a_peer_that_answers_with_no_instance_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "belongs to a different instance"):
            self.check("")

    def test_the_instance_is_read_over_ssh_and_not_assumed_from_a_bare_answer(self) -> None:
        _, _, logged = self.check(INSTANCE)
        self.assertIn("cat /var/lib/cloud/data/instance-id", str(logged).splitlines())


class HostCheckArgumentsTest(unittest.TestCase):
    """``host check`` needs the host's name and an EC2 instance ID. An empty or malformed ID would let any answer pass, so it is refused at the command line."""

    def run_main(self, *arguments: str) -> tuple[int | None, str]:
        errors = io.StringIO()
        with mock.patch.object(sys, "argv", ["host", *arguments]), contextlib.redirect_stderr(errors), self.assertRaises(SystemExit) as caught:
            host.main()
        return caught.exception.code if isinstance(caught.exception.code, int) else None, errors.getvalue()

    def test_a_check_without_an_instance_id_is_refused(self) -> None:
        code, errors = self.run_main("check", "--host-name", "platform-dev")
        self.assertEqual(2, code)
        self.assertIn("--instance-id", errors)

    def test_an_empty_or_malformed_instance_id_is_refused_without_naming_it(self) -> None:
        for value in ("", " ", "i-xyz", "i-123", "i-0123456789abcdef012", "platform-dev", "i-0123456789abcdef0 extra"):
            with self.subTest(value=value):
                code, errors = self.run_main("check", "--host-name", "platform-dev", "--instance-id", value)
                self.assertEqual(2, code)
                if value.strip():
                    self.assertNotIn(value, errors)

    def test_a_check_without_a_host_name_is_refused(self) -> None:
        code, errors = self.run_main("check", "--instance-id", INSTANCE)
        self.assertEqual(2, code)
        self.assertIn("--host-name", errors)


class DeployWorkflowTest(unittest.TestCase):
    """The workflow's text is the contract: the host is checked against the instance ID of a protected variable."""

    def test_the_check_step_passes_the_instance_id_from_the_protected_variable(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text()
        self.assertIn("INSTANCE_ID: ${{ vars.PLATFORM_INSTANCE_ID }}", workflow)
        self.assertIn('--instance-id "${INSTANCE_ID}"', workflow)
        self.assertIn("environment: deploy", workflow)


if __name__ == "__main__":
    unittest.main()
