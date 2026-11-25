"""A clean-up step of a host runner that times out or fails fails the run, and the steps after it still run (fix round 5, item 1 of the owner's last round).

The runners stay untracked, so each test skips when its runner is not on this machine. Docker and Nessie are stand-ins: no cloud, Docker or Spark runs here.

Failure modes covered here:

- A ``docker network rm`` that times out raises out of the clean-up: the scratch folder is left, the record's clean-up stays "pending" and the run's only
  trace is a stopped-run line (Caddy's own removal of its test network, and the removal of the Compose project's networks in the four other local runs).
- An interrupt in the first step skips the removal of the scratch folder.
- A reference of the Nessie host run that cannot be read or deleted for a reason other than Nessie's own refusal (a timeout, a failed transport) stops the
  loop, so the references after it stay and the clean-up stays "pending".
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from tests.e2e.run_record import Record
from tests.fast.test_runner_failed_reads import DockerOfThisComputer, failed, record
from tests.fast.test_runner_stdout_alone import runner

NETWORK = "p2_caddy_net"
NETWORKS_LEFT = {"containers": [], "volumes": [], "networks": ["n1"]}
LOCAL = ("stack_caddy", "stack_nessie", "stack_monitoring", "stack_openbao", "catalog_proof")


class NetworkRemovalTimesOut(DockerOfThisComputer):
    """The Docker of this computer whose removal of a network does not answer in time."""

    def __call__(self, program: str, arguments: list[str], **keywords: object) -> SimpleNamespace:
        if arguments[:2] == ["network", "rm"]:
            self.calls.append(arguments)
            raise subprocess.TimeoutExpired("docker", 60)
        return super().__call__(program, arguments, **keywords)


class Interrupted(DockerOfThisComputer):
    def __call__(self, program: str, arguments: list[str], **keywords: object) -> SimpleNamespace:
        raise KeyboardInterrupt


def clean(name: str, docker: DockerOfThisComputer) -> tuple[Record, Path]:
    module = runner(name)
    run = record()
    scratch = Path(tempfile.mkdtemp(prefix="cleanup_timeouts_"))
    (scratch / "inner").mkdir()
    with mock.patch.object(module, "run_command", docker):
        if name == "stack_caddy":
            module.clean_local(run, [], scratch, NETWORK)
        elif name == "stack_openbao":
            module.clean_local(run, [], scratch, "p2_openbao_x")
        elif name == "catalog_proof":
            module.clean_local(run, [], scratch, [])
        elif name == "stack_monitoring":
            module.clean_local(run, [], None, scratch)
        else:
            module.clean_local(run, [], scratch)
    return run, scratch


def timed_out_case(name: str) -> str:
    """The step that holds the network removal: Caddy removes its test network on its own, the others inside the project's removal."""
    return "network removed" if name == "stack_caddy" else "project removed"


class LocalCleanup(unittest.TestCase):
    def docker(self, name: str) -> NetworkRemovalTimesOut:
        return NetworkRemovalTimesOut(dict(NETWORKS_LEFT) if name != "stack_caddy" else None)

    def test_a_network_removal_that_times_out_fails_the_run_and_names_the_step(self) -> None:
        for name in LOCAL:
            with self.subTest(runner=name):
                run, scratch = clean(name, self.docker(name))
                self.assertEqual(failed(run), [timed_out_case(name)])
                self.assertTrue(run.cleanup.startswith(f"cleanup failed ({timed_out_case(name)})"), run.cleanup)
                self.assertIn("TimeoutExpired", next(item.observed for item in run.observations if not item.passed))
                run.settle()
                self.assertEqual(run.outcome, "fail")
                self.assertFalse(scratch.exists())

    def test_a_network_removal_that_times_out_still_removes_and_checks_the_scratch_folder(self) -> None:
        for name in LOCAL:
            with self.subTest(runner=name):
                run, scratch = clean(name, self.docker(name))
                self.assertIn("scratch folder removed", [item.case for item in run.observations if item.passed])
                self.assertFalse(scratch.exists())

    def test_the_scratch_folder_that_cannot_be_removed_after_a_timeout_is_a_second_failure(self) -> None:
        for name in LOCAL:
            with self.subTest(runner=name):
                module = runner(name)
                run = record()
                scratch = Path(tempfile.mkdtemp(prefix="cleanup_timeouts_"))
                with mock.patch.object(module, "run_command", self.docker(name)), mock.patch("shutil.rmtree"):
                    args = {"stack_caddy": (NETWORK,), "stack_openbao": ("p2_openbao_x",), "catalog_proof": ([],), "stack_nessie": ()}
                    if name == "stack_monitoring":
                        module.clean_local(run, [], None, scratch)
                    else:
                        module.clean_local(run, [], scratch, *args[name])
                self.assertEqual(failed(run), [timed_out_case(name), "scratch folder removed"])
                scratch.rmdir()

    def test_an_interrupt_in_the_first_step_still_removes_the_scratch_folder(self) -> None:
        for name in LOCAL:
            with self.subTest(runner=name):
                module = runner(name)
                run = record()
                scratch = Path(tempfile.mkdtemp(prefix="cleanup_timeouts_"))
                with mock.patch.object(module, "run_command", Interrupted()), self.assertRaises(KeyboardInterrupt):
                    if name == "stack_caddy":
                        module.clean_local(run, [], scratch, NETWORK)
                    elif name == "stack_openbao":
                        module.clean_local(run, [], scratch, "p2_openbao_x")
                    elif name == "catalog_proof":
                        module.clean_local(run, [], scratch, [])
                    elif name == "stack_monitoring":
                        module.clean_local(run, [], None, scratch)
                    else:
                        module.clean_local(run, [], scratch)
                self.assertFalse(scratch.exists())
                self.assertNotEqual(run.cleanup, "pending")


class NessieHostCleanup(unittest.TestCase):
    def clean(self, error: Exception, *, on: str) -> tuple[Record, list[str]]:
        module = runner("stack_nessie")
        run = record()
        calls: list[str] = []

        def reference(name: str) -> object:
            if on == "read" and name == "b":
                raise error
            return SimpleNamespace(hash="h")

        def delete(name: str, hash_: str) -> None:
            calls.append(name)
            if on == "delete" and name == "b":
                raise error

        module.clean_host(run, [], SimpleNamespace(reference=reference, delete_reference=delete), [("branch", "a"), ("tag", "b"), ("branch", "c")])
        return run, calls

    def test_a_reference_that_times_out_or_fails_to_travel_fails_the_run_and_the_others_are_still_deleted(self) -> None:
        for error in (TimeoutError("timed out"), OSError("curl exit 7"), subprocess.TimeoutExpired("ssh", 60)):
            for on in ("read", "delete"):
                with self.subTest(error=type(error).__name__, on=on):
                    run, calls = self.clean(error, on=on)
                    self.assertEqual(failed(run), ["references deleted"])
                    self.assertTrue(run.cleanup.startswith("cleanup failed (references deleted)"), run.cleanup)
                    self.assertIn("c", calls)
                    self.assertIn("a", calls)
                    self.assertIn("deleted: 2", run.cleanup + next(item.observed for item in run.observations))
                    run.settle()
                    self.assertEqual(run.outcome, "fail")


if __name__ == "__main__":
    unittest.main()
