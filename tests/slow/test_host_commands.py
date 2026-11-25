"""The parts of the host commands that decide before any call: which Tailscale peer is the host and what a run owns.

The commands' calls to AWS, Tailscale, Terraform and Ansible are proven on the live host. These cases cover choices a
live run cannot readily force: a stale peer with the host's name, an offline peer, a peer without the host tag and a
Terraform state with data sources and nested modules.
"""

from __future__ import annotations

import contextlib
import functools
import io
import json
import os
import shutil
import stat
import sys
import tempfile
import time
import unittest
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.aws_cli import Aws
from scripts.infra import host, state_bootstrap, teardown, terraform_wrapper
from scripts.infra.host import (
    HostError,
    Peer,
    heavy_memory_variables,
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
from scripts.ingestion.notice import Code, Notice, render
from scripts.process import CompletedProcess, TimeoutExpired, run_command
from scripts.project_config import ROOT
from scripts.stack.warehouse import WarehouseError


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
    '#!/bin/sh\nif [ ! -e "$FAKE_SSH_LOG" ]; then for a in "$@"; do case "$a" in UserKnownHostsFile=*) f="${a#UserKnownHostsFile=}"\n'
    '{ if [ -e "$f" ]; then cat "$f"; else echo "no saved keys"; fi\n'
    '  if [ -e "${f}_instance" ]; then cat "${f}_instance"; else echo "no recorded instance"; fi; } > "$FAKE_FIRST_SSH_SAW" ;;\n'
    "esac; done; fi\n"
    'printf \'%s\\n\' "$@" >> "$FAKE_SSH_LOG"\nprintf \'%s\\n\' "$FAKE_INSTANCE"\n'
    'case "$*" in *"tailscale logout"*) exit "${FAKE_LOGOUT_EXIT:-0}" ;; esac\nexit "${FAKE_SSH_EXIT:-0}"\n'
)
CONNECTED = {"BackendState": "Running"}
INSTANCE = "i-0123456789abcdef0"
OTHER_INSTANCE = "i-0fedcba9876543210"


class FakeSshRecordTest(unittest.TestCase):
    """What the stand-in ``ssh`` records that its first call saw. A saved file that cannot be read must not be recorded as an absent one."""

    def first_call(self, folder: Path, make: Callable[[Path], None]) -> tuple[str, str, int]:
        (folder / "ssh").write_text(SSH_TOOL)
        known_hosts = folder / "known_hosts"
        make(known_hosts)
        env = {"PATH": os.environ["PATH"], "FAKE_SSH_LOG": str(folder / "ssh.log"), "FAKE_FIRST_SSH_SAW": str(folder / "saw"), "FAKE_INSTANCE": INSTANCE}
        result = run_command("sh", [str(folder / "ssh"), "-o", f"UserKnownHostsFile={known_hosts}"], env=env, timeout=30)
        saw = folder / "saw"
        return (saw.read_text() if saw.exists() else ""), result.stderr, result.returncode

    def test_control_absent_files_are_recorded_as_absent_without_a_message(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            saw, errors, code = self.first_call(Path(scratch), lambda known_hosts: None)
        self.assertEqual(("no saved keys\nno recorded instance\n", "", 0), (saw, errors, code))

    def test_control_saved_files_are_recorded_with_their_text(self) -> None:
        def make(known_hosts: Path) -> None:
            known_hosts.write_text(SAVED_KEY)
            Path(f"{known_hosts}_instance").write_text(f"{INSTANCE}\n")

        with tempfile.TemporaryDirectory() as scratch:
            saw, errors, _ = self.first_call(Path(scratch), make)
        self.assertEqual((SAVED_KEY + f"{INSTANCE}\n", ""), (saw, errors))

    @unittest.skipIf(os.geteuid() == 0, "root reads any file")
    def test_a_saved_file_that_cannot_be_read_shows_the_error_and_is_not_recorded_as_absent(self) -> None:
        def make(known_hosts: Path) -> None:
            known_hosts.write_text(SAVED_KEY)
            known_hosts.chmod(0)
            Path(f"{known_hosts}_instance").write_text(f"{INSTANCE}\n")
            Path(f"{known_hosts}_instance").chmod(0)

        with tempfile.TemporaryDirectory() as scratch:
            saw, errors, _ = self.first_call(Path(scratch), make)
        self.assertNotIn("no saved keys", saw)
        self.assertNotIn("no recorded instance", saw)
        self.assertEqual(2, errors.count("Permission denied"), errors)


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
        (folder / "status.json").write_text(json.dumps(CONNECTED | dict(status)))
        (folder / "status_after.json").write_text(json.dumps(CONNECTED | dict(after if after is not None else status)))
        environment = {
            "FAKE_STATUS_AFTER": str(folder / "status_after.json") if after is not None else "",
            "PATH": f"{folder}:{os.environ['PATH']}",
            "FAKE_STATUS": str(folder / "status.json"),
            "FAKE_SSH_LOG": str(folder / "ssh.log"),
            "FAKE_FIRST_SSH_SAW": str(folder / "first_ssh_saw"),
            "FAKE_INSTANCE": instance,
            "FAKE_SSH_EXIT": str(ssh_exit),
            "FAKE_LOGOUT_EXIT": str(logout_exit),
        }
        with mock.patch.dict(os.environ, environment), mock.patch.object(host, "ROOT", folder), mock.patch.object(host, "POLL_SECONDS", 0.01):
            yield folder


SAVED_KEY = "platform-dev.example.ts.net ssh-ed25519 AAAAsaved\n"


def save_host_keys(folder: Path, run_id: str, saved_for: str | None) -> None:
    run_folder = folder / "runs" / run_id
    run_folder.mkdir(parents=True)
    (run_folder / "known_hosts").write_text(SAVED_KEY)
    if saved_for is not None:
        (run_folder / "known_hosts_instance").write_text(f"{saved_for}\n")


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

    def first_ssh_saw_after_keys_saved_for(self, saved_for: str | None) -> str:
        with fake_tools({"Peer": {"a": peer("platform-dev")}}, INSTANCE) as folder:
            save_host_keys(folder, "run-one", saved_for)
            wait_until_reachable(FakeAws("project", "us-west-2"), "run-one", INSTANCE, "platform-dev")
            return (folder / "first_ssh_saw").read_text()

    def test_host_keys_saved_for_a_replaced_instance_are_dropped_before_the_first_ssh(self) -> None:
        self.assertEqual(f"no saved keys\n{INSTANCE}\n", self.first_ssh_saw_after_keys_saved_for(OTHER_INSTANCE))

    def test_host_keys_saved_with_no_recorded_instance_are_dropped_before_the_first_ssh(self) -> None:
        self.assertEqual(f"no saved keys\n{INSTANCE}\n", self.first_ssh_saw_after_keys_saved_for(None))

    def test_host_keys_saved_for_the_same_instance_are_kept(self) -> None:
        self.assertEqual(f"{SAVED_KEY}{INSTANCE}\n", self.first_ssh_saw_after_keys_saved_for(INSTANCE))

    def test_the_first_ssh_of_a_run_finds_the_instance_recorded(self) -> None:
        with fake_tools({"Peer": {"a": peer("platform-dev")}}, INSTANCE) as folder:
            wait_until_reachable(FakeAws("project", "us-west-2"), "run-one", INSTANCE, "platform-dev")
            self.assertEqual(f"no saved keys\n{INSTANCE}\n", (folder / "first_ssh_saw").read_text())

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

    def test_host_keys_saved_for_a_replaced_instance_are_dropped_before_the_first_ssh_of_a_release(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, after={"Peer": {}}) as folder:
            save_host_keys(folder, "run-one", OTHER_INSTANCE)
            release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            self.assertEqual(f"no saved keys\n{INSTANCE}\n", (folder / "first_ssh_saw").read_text())

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


NOT_CONNECTED = ("NeedsLogin", "Stopped", "Starting", "NoState", "NeedsMachineAuth", "running", "", None)


def with_state(state: str | None, peers: object = None) -> dict[str, object]:
    """A status from this computer's Tailscale client in the given backend state with the given peer map, absent when the state is None."""
    return {"Peer": peers} if state is None else {"BackendState": state, "Peer": peers}


class DeviceNotVerifiedTest(unittest.TestCase):
    """A missing peer means the device is gone only while this computer's Tailscale client is running and connected."""

    def test_a_connected_client_with_no_such_peer_means_gone(self) -> None:
        for peers in (None, {}, {"a": peer("laptop", tags=(), device="nLAPTOPCNTRL")}):
            with self.subTest(peers=peers):
                self.assertTrue(host.device_gone(with_state("Running", peers), "platform-dev", "nNEWCNTRL"))

    def test_a_connected_client_that_still_lists_the_device_means_not_gone(self) -> None:
        self.assertFalse(host.device_gone(with_state("Running", {"a": peer("platform-dev", device="nNEWCNTRL")}), "platform-dev", "nNEWCNTRL"))

    def test_any_other_state_raises_whatever_the_peer_map_holds(self) -> None:
        for state in NOT_CONNECTED:
            for peers in (None, {}, {"a": peer("laptop", tags=(), device="nLAPTOPCNTRL")}):
                with self.subTest(state=state, peers=peers), self.assertRaisesRegex(HostError, "not running and connected"):
                    host.device_gone(with_state(state, peers), "platform-dev", "nNEWCNTRL")

    def test_the_removal_check_reports_the_device_as_not_verified_in_any_other_state(self) -> None:
        entry = {"type": "tailscale_device", "host_name": "platform-dev", "service_id": "nNEWCNTRL"}
        for state in NOT_CONNECTED:
            with self.subTest(state=state):
                self.assertTrue(teardown.device_check(entry, functools.partial(with_state, state)).startswith("unverified"))
        self.assertEqual("gone", teardown.device_check(entry, lambda: with_state("Running")))
        self.assertEqual("still exists", teardown.device_check(entry, lambda: with_state("Running", {"a": peer("platform-dev", device="nNEWCNTRL")})))

    def test_a_release_with_a_client_that_needs_login_raises_and_contacts_no_host(self) -> None:
        for state in NOT_CONNECTED:
            with self.subTest(state=state), fake_tools(with_state(state), INSTANCE) as folder:
                (folder / "status.json").write_text(json.dumps(with_state(state)))
                with self.assertRaisesRegex(HostError, "not running and connected"):
                    release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
                self.assertFalse((folder / "ssh.log").exists())

    def test_a_client_that_is_not_connected_but_still_lists_the_peer_gets_no_ssh_session(self) -> None:
        stale = {"a": peer("platform-dev", device="nNEWCNTRL")}
        with fake_tools(with_state("NeedsLogin", stale), INSTANCE) as folder:
            (folder / "status.json").write_text(json.dumps(with_state("NeedsLogin", stale)))
            with self.assertRaisesRegex(HostError, "not running and connected"):
                release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            self.assertFalse((folder / "ssh.log").exists())

    def test_a_client_that_loses_its_network_while_the_release_waits_raises_at_once(self) -> None:
        before = {"Peer": {"a": peer("platform-dev", device="nNEWCNTRL")}}
        with fake_tools(before, INSTANCE, after=with_state("NeedsLogin")) as folder:
            (folder / "status_after.json").write_text(json.dumps(with_state("NeedsLogin")))
            with self.assertRaisesRegex(HostError, "not running and connected"):
                release_tailscale_device("run-one", "platform-dev", "nNEWCNTRL", INSTANCE, 5)
            self.assertIn("sudo -n tailscale logout", (folder / "ssh.log").read_text().splitlines())


CONFIGURATION = {
    "AWS_REGION": "us-west-2",
    "AWS_PROFILE": "example_project_dev",
    "ENVIRONMENT": "dev",
    "PROJECT_NAME": "example_project",
    "S3_BUCKET": "example-bucket",
}


def binding_file(folder: Path, *, backend: str, provision_host: bool) -> Path:
    path = folder / "run_binding.json"
    RunBinding("20260101t0000-abcdef", "0" * 40, backend, "us-west-2", "example-project", "default", ["terraform.tfstate"], "verified", provision_host).write(
        path, state_root=folder
    )
    return path


class StopAndStartWithRefusedLimitsTest(unittest.TestCase):
    """Stopping and starting the host read the run's state and never depend on the limits file: a file that the loader refuses stops a plan, not these."""

    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (self.root / "compose").mkdir()
        (self.root / "compose" / "limits.yml").write_text(
            "host_reserve_mib: 4096\nservices:\n  spark:\n    runtime: compose\n    group: batch\n    memory_limit_mib: 0\n"
        )
        for folder in ("modules", "environments"):
            (self.root / "infra" / folder).mkdir(parents=True)
        (self.root / "infra" / ".terraform.lock.hcl").write_text("")
        (self.root / "infra" / "ubuntu_build.json").write_text("{}")
        self.binding = binding_file(self.root, backend="s3", provision_host=True)
        record = self.root / ".tools" / "terraform" / "20260101t0000-abcdef" / "config" / ".terraform" / "terraform.tfstate"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps({"backend": {"type": "s3", "config": {"bucket": "example-project", "key": "terraform.tfstate", "region": "us-west-2"}}}))
        answers = {"init": "", "workspace": "default\n", "show": json.dumps(STATE)}
        self.enterContext(mock.patch.object(terraform_wrapper, "ROOT", self.root))
        self.enterContext(mock.patch.object(host, "ROOT", self.root))
        self.enterContext(mock.patch.object(terraform_wrapper, "bucket_problem", return_value=None))
        self.enterContext(mock.patch.object(terraform_wrapper, "terraform", side_effect=lambda run, arguments, timeout: answers[arguments[0]]))
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))

    def test_host_stop_stops_the_instance_whatever_the_limits_file_holds(self) -> None:
        self.assertEqual(0, host.host_stop(FakeAws("project", "us-west-2", "stopped"), CONFIGURATION, self.binding))

    def test_host_start_starts_the_instance_whatever_the_limits_file_holds(self) -> None:
        with (
            mock.patch.object(host, "wait_until_reachable", return_value=Peer("platform-dev.example.ts.net", "nRESTARTEDCNTRL")),
            mock.patch.object(host, "write_inventory"),
        ):
            self.assertEqual(0, host.host_start(FakeAws("project", "us-west-2"), CONFIGURATION, self.binding))


def heavy_state(value: object) -> dict[str, Any]:
    return {"values": {"outputs": {"heavy_memory_mib": {"value": value}}}}


def current_inputs() -> dict[str, int]:
    """The heavy inputs that the limits loader gives from the tracked file now."""
    return dict(json.loads(terraform_wrapper.heavy_inputs()))


def write_plan_json(root: Path, name: str, figure: object, *, age_seconds: int = 0, inputs: dict[str, int] | None = None) -> None:
    """A saved plan's JSON in the run's plans folder, with the planned value of the heavy figure, the heavy inputs it was made with (the current ones by
    default) and a modification time of ``age_seconds`` ago."""
    folder = root / "runs" / "20260101t0000-abcdef" / "plans"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / name
    variables = {name: {"value": value} for name, value in (current_inputs() if inputs is None else inputs).items()}
    target.write_text(json.dumps({"planned_values": {"outputs": {"heavy_memory_mib": {"value": figure}}}, "variables": variables}))
    moment = time.time() - age_seconds
    os.utime(target, (moment, moment))


class HostUpTest(unittest.TestCase):
    """``host_up`` follows the run binding that exists. The calls to Terraform, Ansible and the host are replaced and recorded; the live batch proves them."""

    def run_up(
        self,
        path: Path,
        *,
        confirm: str = "",
        state: dict[str, Any] | None = None,
        planned: dict[str, tuple[object, int]] | None = None,
        on_wait: Callable[..., object] | None = None,
    ) -> tuple[int, dict[str, mock.Mock]]:
        """``planned`` gives the saved plan files by name as the planned figure and its age in seconds. The default is one apply plan with 11008."""
        for name, (figure, age) in ({"apply.json": (11008, 0)} if planned is None else planned).items():
            write_plan_json(path.parent, name, figure, age_seconds=age)
        names = (
            "save_plan",
            "apply_plan",
            "write_inventory",
            "state_json",
            "wait_until_reachable",
            "run_ansible",
            "place_project_key",
            "instance_id",
            "platform_commit",
        )
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(host, "ROOT", path.parent))
            calls = {name: stack.enter_context(mock.patch.object(host, name)) for name in names}
            calls["save_plan"].return_value = 0
            calls["state_json"].return_value = heavy_state(11008) if state is None else state
            calls["wait_until_reachable"].return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")
            if on_wait is not None:
                calls["wait_until_reachable"].side_effect = on_wait
            calls["instance_id"].return_value = "i-0123456789abcdef0"
            calls["platform_commit"].return_value = "a" * 40
            self.calls = calls
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
        self.assertEqual(2, calls["run_ansible"].call_count)
        self.assertEqual(
            [("host.yml",), ("connection.yml",)],
            [item.args[3:] or ("host.yml",) for item in calls["run_ansible"].call_args_list],
            "the host playbook first, the connection playbook second",
        )
        calls["place_project_key"].assert_called_once_with("20260101t0000-abcdef", "platform-dev.example.ts.net", "example_project_dev")
        self.assertEqual(calls["run_ansible"].call_args_list[0].args[2], calls["run_ansible"].call_args_list[1].args[2])
        self.assertEqual(
            {
                "workload_access_region": "us-west-2",
                "workload_access_workloads": [],
                "compose_stack_heavy_memory_mib": 11008,
                "compose_stack_platform_commit": "a" * 40,
                "compose_stack_warehouse_location": "s3://example-bucket/warehouse/",
                "airbyte_landing_bucket": "example-bucket",
            },
            calls["run_ansible"].call_args_list[0].args[2],
        )
        calls["write_inventory"].assert_called_once_with(path)
        self.assertEqual({"host_name": "platform-dev", "device_id": "nNEWCNTRL"}, recorded)

    def test_a_state_with_no_usable_heavy_figure_stops_the_up_run_before_it_waits_for_the_host_or_runs_ansible(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "heavy_memory_mib"):
                self.run_up(path, state={"values": {}})
        for name in ("wait_until_reachable", "run_ansible", "write_inventory"):
            self.calls[name].assert_not_called()

    def test_a_state_figure_that_differs_from_the_planned_one_stops_the_up_run_before_it_waits_for_the_host_or_runs_ansible(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "12544.*11008|11008.*12544"):
                self.run_up(path, planned={"apply.json": (12544, 0)})
        for name in ("wait_until_reachable", "run_ansible", "write_inventory"):
            self.calls[name].assert_not_called()

    def test_a_confirmed_run_compares_the_state_with_the_plan_that_it_applied_and_not_with_a_newer_check_plan(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0):
                code, calls = self.run_up(path, confirm="abc", planned={"apply.json": (11008, 60), "apply_check.json": (9999, 0)})
        self.assertEqual(0, code)
        calls["apply_plan"].assert_called_once()
        self.assertEqual(11008, calls["run_ansible"].call_args.args[2]["compose_stack_heavy_memory_mib"])

    def test_an_unconfirmed_run_with_no_change_compares_the_state_with_the_newest_plan_it_saved(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0):
                code, calls = self.run_up(path, planned={"apply.json": (9999, 60), "apply_check.json": (11008, 0)})
            self.assertEqual(0, code)
            self.assertEqual(
                ["host.yml", "connection.yml"], [call.args[3] if len(call.args) > 3 else "host.yml" for call in calls["run_ansible"].call_args_list]
            )
            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "9999"):
                self.run_up(path, planned={"apply.json": (11008, 60), "apply_check.json": (9999, 0)})

    def test_a_saved_plan_made_with_other_inputs_than_the_limits_give_now_stops_the_up_run_before_it_waits_for_the_host(self) -> None:
        stale = current_inputs() | {"host_reserve_mib": 1}
        for confirm in ("", "abc"):
            with self.subTest(confirm=confirm), tempfile.TemporaryDirectory() as scratch:
                path = binding_file(Path(scratch), backend="s3", provision_host=True)
                write_plan_json(path.parent, "apply.json", 11008, inputs=stale)
                with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "save a new plan"):
                    self.run_up(path, confirm=confirm, planned={})
                for name in ("wait_until_reachable", "run_ansible", "write_inventory"):
                    self.calls[name].assert_not_called()

    def test_a_saved_plan_without_the_heavy_inputs_stops_the_up_run(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            write_plan_json(path.parent, "apply.json", 11008, inputs={})
            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "save a new plan"):
                self.run_up(path, planned={})
        self.calls["run_ansible"].assert_not_called()

    def test_a_limits_change_between_the_apply_and_the_deploy_is_refused_before_ansible(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            changed = json.dumps(current_inputs() | {"host_reserve_mib": 9})

            def limits_change_while_the_host_starts(*arguments: object, **keywords: object) -> Peer:
                self.enterContext(mock.patch.object(terraform_wrapper, "heavy_inputs", return_value=changed))
                return Peer("platform-dev.example.ts.net", "nNEWCNTRL")

            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "save a new plan"):
                self.run_up(path, confirm="abc", on_wait=limits_change_while_the_host_starts)
        self.calls["run_ansible"].assert_not_called()

    def test_a_run_with_no_saved_plan_stops_before_it_waits_for_the_host(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            with mock.patch.object(state_bootstrap, "create", return_value=0), self.assertRaisesRegex(HostError, "no saved plan"):
                self.run_up(path, planned={})
        self.calls["wait_until_reachable"].assert_not_called()

    def test_the_steps_of_an_up_run_are_the_host_playbook_the_key_the_connection_playbook_and_the_inventory_in_that_order(self) -> None:
        order: list[str] = []
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            write_plan_json(path.parent, "apply.json", 11008)
            stack.enter_context(mock.patch.object(state_bootstrap, "create", return_value=0))
            stack.enter_context(
                mock.patch.object(host, "run_ansible", side_effect=lambda run_id, address, variables, playbook="host.yml": order.append(f"ansible {playbook}"))
            )
            stack.enter_context(mock.patch.object(host, "place_project_key", side_effect=lambda *arguments: order.append("key")))
            stack.enter_context(mock.patch.object(host, "write_inventory", side_effect=lambda binding: order.append("inventory")))
            for name in ("save_plan", "apply_plan", "state_json", "wait_until_reachable", "instance_id", "platform_commit"):
                stack.enter_context(mock.patch.object(host, name))
            stack.enter_context(mock.patch.object(host, "ROOT", path.parent))
            host.wait_until_reachable.return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")  # type: ignore[attr-defined]
            host.state_json.return_value = heavy_state(11008)  # type: ignore[attr-defined]
            host.save_plan.return_value = 0  # type: ignore[attr-defined]
            host.platform_commit.return_value = "a" * 40  # type: ignore[attr-defined]
            with contextlib.redirect_stdout(io.StringIO()):
                host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, "")
        self.assertEqual(["ansible host.yml", "key", "ansible connection.yml", "inventory"], order)

    def test_a_failure_of_the_key_step_stops_the_run_before_the_connection_playbook_and_the_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            write_plan_json(path.parent, "apply.json", 11008)
            stack.enter_context(mock.patch.object(state_bootstrap, "create", return_value=0))
            run = stack.enter_context(mock.patch.object(host, "run_ansible"))
            stack.enter_context(mock.patch.object(host, "place_project_key", side_effect=HostError("a fixed message")))
            inventory = stack.enter_context(mock.patch.object(host, "write_inventory"))
            for name in ("save_plan", "apply_plan", "state_json", "wait_until_reachable", "instance_id", "platform_commit"):
                stack.enter_context(mock.patch.object(host, name))
            host.wait_until_reachable.return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")  # type: ignore[attr-defined]
            host.state_json.return_value = heavy_state(11008)  # type: ignore[attr-defined]
            host.save_plan.return_value = 0  # type: ignore[attr-defined]
            host.platform_commit.return_value = "a" * 40  # type: ignore[attr-defined]
            stack.enter_context(mock.patch.object(host, "ROOT", path.parent))
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(HostError):
                host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, "")
        self.assertEqual(1, run.call_count)
        inventory.assert_not_called()

    def test_a_failure_of_the_host_playbook_leaves_the_key_step_unrun(self) -> None:
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            write_plan_json(path.parent, "apply.json", 11008)
            stack.enter_context(mock.patch.object(state_bootstrap, "create", return_value=0))
            stack.enter_context(mock.patch.object(host, "run_ansible", side_effect=HostError("the Ansible host setup failed")))
            key = stack.enter_context(mock.patch.object(host, "place_project_key"))
            stack.enter_context(mock.patch.object(host, "write_inventory"))
            for name in ("save_plan", "apply_plan", "state_json", "wait_until_reachable", "instance_id", "platform_commit"):
                stack.enter_context(mock.patch.object(host, name))
            host.wait_until_reachable.return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")  # type: ignore[attr-defined]
            host.state_json.return_value = heavy_state(11008)  # type: ignore[attr-defined]
            host.save_plan.return_value = 0  # type: ignore[attr-defined]
            host.platform_commit.return_value = "a" * 40  # type: ignore[attr-defined]
            stack.enter_context(mock.patch.object(host, "ROOT", path.parent))
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(HostError):
                host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, "")
        key.assert_not_called()

    def test_the_connection_run_keeps_its_own_output_file_and_names_its_playbook(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            output = io.StringIO()
            with fake_tools({"Peer": {}}, INSTANCE) as folder:
                tool = folder / ".venv" / "bin" / "ansible-playbook"
                tool.parent.mkdir(parents=True)
                tool.write_text('#!/bin/sh\nprintf "%s\\n" "$1"\n')
                tool.chmod(stat.S_IRWXU)
                (folder / "ansible").mkdir()
                with mock.patch.object(terraform_wrapper, "ROOT", folder), contextlib.redirect_stdout(output):
                    run_ansible("run-one", "platform-dev.example.ts.net", {}, "connection.yml")
                kept = (folder / "runs" / "run-one" / "raw" / "ansible_connection_output.txt").read_text()
            self.assertIn("connection.yml", kept)
            self.assertIn("the Ansible connection step passed", output.getvalue())
            self.assertTrue(scratch)

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

    def test_a_failed_write_of_the_ansible_output_after_exit_zero_still_writes_the_inventory_and_says_so(self) -> None:
        names = ("save_plan", "write_inventory", "state_json", "wait_until_reachable", "instance_id", "place_project_key")
        with tempfile.TemporaryDirectory() as scratch, contextlib.ExitStack() as stack:
            path = binding_file(Path(scratch), backend="s3", provision_host=True)
            stack.enter_context(mock.patch.object(state_bootstrap, "create", return_value=0))
            stack.enter_context(mock.patch.object(host, "ROOT", Path(scratch)))
            calls = {name: stack.enter_context(mock.patch.object(host, name)) for name in names}
            calls["save_plan"].return_value = 0
            calls["state_json"].return_value = heavy_state(11008)
            write_plan_json(Path(scratch), "apply.json", 11008)
            calls["wait_until_reachable"].return_value = Peer("platform-dev.example.ts.net", "nNEWCNTRL")
            calls["instance_id"].return_value = "i-0123456789abcdef0"
            stack.enter_context(mock.patch.object(host, "run_command", return_value=CompletedProcess(["ansible-playbook"], 0, "ok\n", "")))
            blocked = Path(scratch) / "blocked"
            (blocked / "compose").mkdir(parents=True)
            shutil.copy(ROOT / "compose" / "limits.yml", blocked / "compose" / "limits.yml")
            (blocked / "runs").write_text("a file where the folder of the raw output should be")
            stack.enter_context(mock.patch.object(terraform_wrapper, "ROOT", blocked))
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = host.host_up(FakeAws("project", "us-west-2"), CONFIGURATION, path, "")
        self.assertEqual(0, code)
        calls["write_inventory"].assert_called_once_with(path)
        self.assertIn("the Ansible host setup passed; its full output could not be kept", output.getvalue())
        self.assertIn("the host platform-dev is up and its inventory is written", output.getvalue())

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

    def test_project_workloads_keep_their_own_group_ids_whatever_other_workloads_exist(self) -> None:
        names = ("airbyte", "ingestion_check", "trino", "replay", "orchestrator", "bronze", "silver", "gold", "stream", "maintenance")
        for others in ((), ("reader", "writer"), ("aaa_first", "dagster", "zzz_last")):
            with self.subTest(others=others):
                roles = {name: f"arn:{name}" for name in (*names, *others)}
                workloads = workload_variables({"values": {"outputs": {"workload_role_arns": {"value": roles}}}}, "us-west-2")["workload_access_workloads"]
                by_name = {workload["name"]: workload["gid"] for workload in workloads}
                self.assertEqual(
                    {
                        "airbyte": 20100,
                        "ingestion_check": 20101,
                        "trino": 20103,
                        "replay": 20104,
                        "orchestrator": 20105,
                        "bronze": 20106,
                        "silver": 20107,
                        "gold": 20108,
                        "stream": 20109,
                        "maintenance": 20110,
                    },
                    {name: by_name[name] for name in names},
                )
                self.assertEqual(len(by_name), len(set(by_name.values())))
                self.assertEqual(sorted(others), [name for name, _ in sorted(by_name.items(), key=lambda item: item[1]) if name in others])
                self.assertTrue(all(by_name[name] < 20100 for name in others))

    def test_state_without_workloads_gives_an_empty_list(self) -> None:
        self.assertEqual({"workload_access_region": "us-west-2", "workload_access_workloads": []}, workload_variables({"values": {}}, "us-west-2"))


class HeavyMemoryVariablesTest(unittest.TestCase):
    def test_the_figure_that_terraform_calculated_is_passed_to_the_deploy_as_it_is(self) -> None:
        self.assertEqual({"compose_stack_heavy_memory_mib": 11008}, heavy_memory_variables(heavy_state(11008), 11008))
        self.assertEqual({"compose_stack_heavy_memory_mib": 1}, heavy_memory_variables(heavy_state(1), 1))

    def test_a_state_figure_that_is_not_the_planned_one_is_refused_with_both_named(self) -> None:
        for planned in (12544, None, "11008", 11008.5):
            with self.subTest(planned=planned), self.assertRaisesRegex(HostError, r"state.*11008.*plan"):
                heavy_memory_variables(heavy_state(11008), planned)

    def test_a_missing_output_is_refused_with_the_output_named(self) -> None:
        states: tuple[dict[str, Any], ...] = ({}, {"values": {}}, {"values": {"outputs": {}}})
        for state in states:
            with self.subTest(state=state), self.assertRaisesRegex(HostError, "heavy_memory_mib.*missing"):
                heavy_memory_variables(state, 11008)

    def test_a_figure_that_is_not_a_positive_whole_number_is_refused_with_the_value_named(self) -> None:
        for value in (None, 0, -1, -11008, 1.5, 11008.0, "11008", True, [11008]):
            with self.subTest(value=value), self.assertRaisesRegex(HostError, "heavy_memory_mib.*positive whole number"):
                heavy_memory_variables(heavy_state(value), value)


class DeployVariablesTest(unittest.TestCase):
    def test_the_deploy_gets_the_commit_it_deploys_the_warehouse_location_and_the_bucket_of_the_connection(self) -> None:
        self.assertEqual(
            {
                "compose_stack_platform_commit": "b" * 40,
                "compose_stack_warehouse_location": "s3://example-bucket/warehouse/",
                "airbyte_landing_bucket": "example-bucket",
            },
            host.deploy_variables(CONFIGURATION, "b" * 40),
        )

    def test_a_bucket_name_that_s3_refuses_stops_the_deploy_before_ansible_runs(self) -> None:
        with self.assertRaises(WarehouseError):
            host.deploy_variables(CONFIGURATION | {"S3_BUCKET": "Not_A_Bucket"}, "b" * 40)

    def test_the_commit_is_the_head_of_the_checkout(self) -> None:
        self.assertRegex(host.platform_commit(), r"^[0-9a-f]{40}$")


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
        self.assertEqual("the Ansible host setup failed; its full output is in the run's raw folder as ansible_output.txt", message)
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
        self.assertEqual("the Ansible host setup failed; its full output is in the run's raw folder as ansible_output.txt", message)

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

    def test_a_successful_setup_names_the_file_that_keeps_the_output(self) -> None:
        printed, _, _ = self.run_it(exit_code=0)
        self.assertIn("the Ansible host setup passed; its full output is in the run's raw folder as ansible_output.txt", printed)


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

    def test_host_keys_saved_for_another_instance_are_dropped_before_the_first_ssh_of_the_check(self) -> None:
        with fake_tools({"Peer": {"a": peer("platform-dev")}}, INSTANCE) as folder, contextlib.redirect_stdout(io.StringIO()):
            save_host_keys(folder, "deploy", OTHER_INSTANCE)
            self.assertEqual(0, host_check("platform-dev", INSTANCE))
            self.assertEqual(f"no saved keys\n{INSTANCE}\n", (folder / "first_ssh_saw").read_text())

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
        self.assertIn(render(Notice(Code.USAGE)), errors)

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
        self.assertIn(render(Notice(Code.USAGE)), errors)

    def test_a_host_name_that_is_not_the_name_of_an_environments_host_is_refused_without_naming_it(self) -> None:
        for value in ("planted_name", "platform", "platform-staging", "platform-dev extra", "other-dev"):
            with self.subTest(value=value):
                code, errors = self.run_main("check", "--host-name", value, "--instance-id", INSTANCE)
                self.assertEqual(2, code)
                self.assertNotIn(value, errors)


class DeployWorkflowTest(unittest.TestCase):
    """The workflow's text is the contract: the host is checked against the instance ID of a protected variable."""

    def test_the_check_step_passes_the_instance_id_from_the_protected_variable(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text()
        self.assertIn("INSTANCE_ID: ${{ vars.PLATFORM_INSTANCE_ID }}", workflow)
        self.assertIn('--instance-id "${INSTANCE_ID}"', workflow)
        self.assertIn("environment: deploy", workflow)


if __name__ == "__main__":
    unittest.main()
