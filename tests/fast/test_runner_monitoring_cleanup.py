"""The monitoring runner removes its labelled test containers and volumes on the machine that holds them, and fails when one is left.

The runner stays untracked, so each test skips when it is not on this machine. The target is a stand-in for the live host's Docker: the containers and the
volumes that the run made (run 20261125T000617Z) exist there and not on this computer, so a listing on this computer's Docker finds none. No Docker, SSH or
host runs here.

Failure modes covered here:

- The cleanup lists, removes and verifies on this computer's Docker, finds nothing, reports "none left" and leaves the resources on the host.
- A resource that Docker refuses to remove reads as removed, or a listing that fails with no output reads as "none left".
- The cleanup removes the resources of another run, which carry another label.
- The host mode ends with a passing result and a clean cleanup text while something labelled is left.
"""

from __future__ import annotations

import importlib
import shlex
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e.run_record import Record

E2E = Path(__file__).resolve().parents[1] / "e2e"
OTHER_RUN = "ffffff"


def runner() -> ModuleType:
    if not (E2E / "stack_monitoring.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.stack_monitoring")


class HostDocker:
    """The Docker of the live host with the labelled resources of one run. ``stubborn`` names the resources that it refuses to remove."""

    compose_directory = "/opt/platform/compose"
    secrets_directory = "/opt/platform/secrets"
    env_files = ("/opt/platform/compose/limits.env",)
    ports: dict[str, int] = {}
    project = "surgical_risk"
    name = "live host"

    def __init__(self, token: str, *, stubborn: bool = False, listing_fails: bool = False) -> None:
        self.containers = {"c1": f"p2m_run={token}", "c2": f"p2m_run={OTHER_RUN}", "c3": "other=1"}
        self.volumes = {"v1": f"p2m_run={token}", "v2": f"p2m_run={OTHER_RUN}"}
        self.stubborn = stubborn
        self.listing_fails = listing_fails
        self.commands: list[list[str]] = []

    def labelled(self, items: dict[str, str], words: list[str]) -> list[str]:
        wanted = words[words.index("--filter") + 1].removeprefix("label=")
        return [name for name, label in items.items() if label == wanted]

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        self.commands.append(args)
        words = args[1:]
        if args[0] != "docker":
            return 127, "not served\n"
        if words[:2] == ["ps", "-aq"]:
            return (255, "") if self.listing_fails else (0, "".join(f"{name}\n" for name in self.labelled(self.containers, words)))
        if words[:2] == ["volume", "ls"]:
            return (255, "") if self.listing_fails else (0, "".join(f"{name}\n" for name in self.labelled(self.volumes, words)))
        if words[:1] == ["rm"] and "--filter" not in words:
            for name in [word for word in words if word in self.containers] if not self.stubborn else []:
                del self.containers[name]
            return (1, "removal refused\n") if self.stubborn else (0, "")
        if words[:2] == ["volume", "rm"]:
            for name in [word for word in words if word in self.volumes] if not self.stubborn else []:
                del self.volumes[name]
            return (1, "removal refused\n") if self.stubborn else (0, "")
        return 0, ""


def stack_of(module: ModuleType, target: HostDocker) -> Any:
    with mock.patch.object(module.secrets, "token_hex", return_value="abc123"):
        return module.Stack(target, Record(feature="x", slug="x", rerun="x"), [])


class MacDocker:
    """This computer's Docker: it holds nothing of the run, and every call to it is recorded."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str]]] = []

    def __call__(self, program: str, arguments: list[str], **_: object) -> SimpleNamespace:
        self.calls.append((program, arguments))
        return SimpleNamespace(returncode=0, stdout="", stderr="")


class LabelledCleanup(unittest.TestCase):
    def test_the_resources_of_the_run_are_listed_removed_and_verified_on_the_host(self) -> None:
        module = runner()
        stack = stack_of(module, HostDocker("abc123"))
        mac = MacDocker()
        with mock.patch.object(module, "run_command", mac):
            self.assertTrue(module.remove_labelled(stack, []))
        self.assertEqual(mac.calls, [])
        self.assertEqual((stack.target.containers, stack.target.volumes), ({"c2": f"p2m_run={OTHER_RUN}", "c3": "other=1"}, {"v2": f"p2m_run={OTHER_RUN}"}))

    def test_a_resource_that_the_host_refuses_to_remove_reads_as_left(self) -> None:
        module = runner()
        stack = stack_of(module, HostDocker("abc123", stubborn=True))
        self.assertFalse(module.remove_labelled(stack, []))

    def test_a_listing_that_fails_does_not_read_as_none_left(self) -> None:
        module = runner()
        stack = stack_of(module, HostDocker("abc123", listing_fails=True))
        self.assertFalse(module.remove_labelled(stack, []))

    def test_a_host_with_nothing_labelled_reads_as_clean(self) -> None:
        module = runner()
        target = HostDocker("abc123")
        target.containers, target.volumes = {}, {}
        stack = stack_of(module, target)
        self.assertTrue(module.remove_labelled(stack, []))

    def test_the_commands_name_the_label_of_this_run_only(self) -> None:
        module = runner()
        target = HostDocker("abc123")
        stack = stack_of(module, target)
        module.remove_labelled(stack, [])
        filters = {shlex.join(words) for words in target.commands if "--filter" in words}
        self.assertTrue(all("label=p2m_run=abc123" in text for text in filters))


class HostRunResult(unittest.TestCase):
    def run_host(self, module: ModuleType, target: HostDocker) -> Record:
        record = Record(feature="x", slug="x", rerun="x")

        def cases(stack: Any, scratch: object) -> None:
            for items in (target.containers, target.volumes):
                for key, label in items.items():
                    if label == "p2m_run=abc123":
                        items[key] = f"p2m_run={stack.token}"

        with (
            mock.patch.object(module, "Remote", return_value=target),
            mock.patch.object(module.Stack, "health", return_value="running/healthy"),
            mock.patch.object(module, "run_cases", cases),
            mock.patch.object(module, "run_command", MacDocker()),
        ):
            module.run_host(record, [])
        return record

    def test_a_clean_host_gives_a_passing_cleanup(self) -> None:
        module = runner()
        record = self.run_host(module, HostDocker("abc123"))
        self.assertIn("none left: True", record.cleanup)
        self.assertEqual([item.case for item in record.observations if not item.passed], [])

    def test_a_resource_left_on_the_host_fails_the_cleanup_text_and_the_run(self) -> None:
        module = runner()
        record = self.run_host(module, HostDocker("abc123", stubborn=True))
        self.assertIn("none left: False", record.cleanup)
        self.assertEqual([item.case for item in record.observations if not item.passed], ["labelled test resources"])


if __name__ == "__main__":
    unittest.main()
