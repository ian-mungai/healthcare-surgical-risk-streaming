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
import tempfile
import unittest
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import Aws
from scripts.infra import host, state_bootstrap
from scripts.infra.host import (
    HostError,
    Peer,
    instance_id,
    record_tailscale_device,
    release_tailscale_device,
    select_peer,
    tailscale_device_id,
    wait_for,
    wait_until_reachable,
    workload_variables,
)
from scripts.infra.inventory import inventory_entries
from scripts.infra.state_bootstrap import RunBinding
from scripts.process import TimeoutExpired


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
SSH_TOOL = '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$FAKE_SSH_LOG"\nprintf \'%s\\n\' "$FAKE_INSTANCE"\nexit "${FAKE_SSH_EXIT:-0}"\n'


@dataclass(frozen=True)
class FakeAws(Aws):
    state: str = "running"

    def call(self, *arguments: str, missing: tuple[str, ...] = (), timeout: float = 120) -> Any:
        return self.state


@contextmanager
def fake_tools(status: Mapping[str, object], instance: str, ssh_exit: int = 0, after: Mapping[str, object] | None = None) -> Iterator[Path]:
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
        with fake_tools(before, "", after={"Peer": {}}) as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 5)
            logged = (folder / "ssh.log").read_text().splitlines()
        address = "platform-dev.example.ts.net"
        self.assertIn(f"ubuntu@{address}", logged)
        self.assertIn("sudo -n tailscale logout", logged)

    def test_a_device_that_is_already_gone_needs_no_ssh(self) -> None:
        with fake_tools({"Peer": {}}, "") as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 5)
            self.assertFalse((folder / "ssh.log").exists())

    def test_the_device_the_host_has_now_is_logged_out_when_it_differs_from_the_recorded_one(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL")}}
        with fake_tools(before, "", after={"Peer": {}}) as folder:
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", 5)
            self.assertIn("sudo -n tailscale logout", (folder / "ssh.log").read_text().splitlines())

    def test_the_recorded_device_must_be_gone_too(self) -> None:
        after = {"Peer": {"b": peer("platform-dev", online=False, device="nRECORDEDCNTRL")}}
        before = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL"), "b": after["Peer"]["b"]}}
        with (
            fake_tools(before, "", after=after),
            self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev to leave the network"),
        ):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", 1)

    def test_a_restarted_device_that_stays_in_the_network_is_not_gone(self) -> None:
        still = {"Peer": {"a": peer("platform-dev", device="nRESTARTEDCNTRL")}}
        with fake_tools(still, "", after=still), self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev"):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", 1)

    def test_the_recorded_device_is_awaited_by_its_id_whatever_its_name_has_become(self) -> None:
        renamed = {"Peer": {"x": peer("platform-dev-1", device="nRECORDEDCNTRL")}}
        with fake_tools(renamed, ""), self.assertRaisesRegex(HostError, "while waiting for the Tailscale device of platform-dev"):
            release_tailscale_device("run-one", "platform-dev", "nRECORDEDCNTRL", 1)

    def test_a_dropped_ssh_session_after_the_logout_is_no_failure(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, "", ssh_exit=255, after={"Peer": {}}):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 5)

    def test_a_device_that_stays_in_the_network_ends_at_the_deadline(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, ""), self.assertRaisesRegex(HostError, "deadline of 1 seconds passed while waiting for the Tailscale device of platform-dev"):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 1)

    def test_an_offline_device_cannot_be_logged_out_and_ends_at_the_deadline(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", online=False, device="nNEWCNTRL")}}
        with fake_tools(before, "") as folder, self.assertRaisesRegex(HostError, "while waiting for the Tailscale device"):
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 1)
        self.assertFalse((folder / "ssh.log").exists())

    def test_two_online_peers_are_refused_before_any_logout(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nAAACNTRL"), "b": peer("platform-dev", device="nBBBCNTRL")}}
        with fake_tools(before, "") as folder, self.assertRaisesRegex(HostError, "2 online peers"):
            release_tailscale_device("run-one", "platform-dev", "nAAACNTRL", 5)
        self.assertFalse((folder / "ssh.log").exists())

    def test_a_peer_with_another_name_is_left_alone(self) -> None:
        before = {"Peer": {"a": peer("laptop", device="nLAPTOPCNTRL", tags=())}}
        with fake_tools(before, "") as folder:
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", 5)
            self.assertFalse((folder / "ssh.log").exists())


CONFIGURATION = {"AWS_REGION": "us-west-2", "AWS_PROFILE": "example_project_dev", "ENVIRONMENT": "dev", "PROJECT_NAME": "example_project"}


def binding_file(folder: Path, *, backend: str, provision_host: bool) -> Path:
    path = folder / "run_binding.json"
    state_bucket = "" if backend == "local" else "example-state-bucket"
    RunBinding("20260101t0000-abcdef", "0" * 40, backend, "us-west-2", state_bucket, "default", ["terraform.tfstate"], "verified", provision_host).write(path)
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
            with contextlib.redirect_stdout(io.StringIO()):
                code = host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, confirm)
        return code, calls

    def test_a_local_run_is_followed_to_its_inventory_with_no_instance_step(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="local", provision_host=False)
            code, calls = self.run_up(path)
        self.assertEqual(0, code)
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

    def test_a_started_host_records_the_device_it_has_now(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            stack.enter_context(mock.patch.object(host, "ROOT", Path(scratch)))
            stack.enter_context(mock.patch.object(host, "state_json", return_value={}))
            stack.enter_context(mock.patch.object(host, "instance_id", return_value="i-0123456789abcdef0"))
            stack.enter_context(mock.patch.object(host, "wait_until_reachable", return_value=Peer("platform-dev.example.ts.net", "nRESTARTEDCNTRL")))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            aws = FakeAws("project", "us-west-2")
            self.assertEqual(0, host.host_start(aws, CONFIGURATION, path))
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


class InventoryTest(unittest.TestCase):
    def test_managed_resources_and_the_state_bucket_are_listed(self) -> None:
        entries = inventory_entries(STATE, "example-state")
        expected = [
            {"address": "module.host.aws_instance.host", "type": "aws_instance", "service_id": "i-0123456789abcdef0"},
            {"address": 'module.storage.aws_s3_bucket.this["landing"]', "type": "aws_s3_bucket", "service_id": "example-landing"},
            {"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"},
        ]
        self.assertEqual(expected, entries)

    def test_the_host_instance_is_read_from_the_state(self) -> None:
        self.assertEqual("i-0123456789abcdef0", instance_id(STATE))

    def test_state_without_a_host_is_refused(self) -> None:
        with self.assertRaisesRegex(HostError, "exactly one host instance"):
            instance_id({"values": {}})

    def test_run_with_local_state_lists_no_state_bucket(self) -> None:
        self.assertEqual([], inventory_entries({"values": {}}, ""))
        self.assertNotIn("state bucket", [entry["address"] for entry in inventory_entries(STATE, "")])

    def test_empty_state_lists_only_the_state_bucket(self) -> None:
        self.assertEqual(
            [{"address": "state bucket", "type": "aws_s3_bucket", "service_id": "example-state"}], inventory_entries({"values": {}}, "example-state")
        )


if __name__ == "__main__":
    unittest.main()
