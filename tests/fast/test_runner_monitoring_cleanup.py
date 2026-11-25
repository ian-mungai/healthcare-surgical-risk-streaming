"""The monitoring runner removes its labelled test containers and volumes on the machine that holds them, and fails when one is left.

The runner stays untracked, so each test skips when it is not on this machine. The target is a stand-in for the live host's Docker: the containers and the
volumes that the run made (run 20261125T000617Z) exist there and not on this computer, so a listing on this computer's Docker finds none. No Docker, SSH or
host runs here.

Failure modes covered here:

- The cleanup lists, removes and verifies on this computer's Docker, finds nothing, reports "none left" and leaves the resources on the host.
- A resource that Docker refuses to remove reads as removed, or a listing that fails with no output reads as "none left".
- The cleanup removes the resources of another run, which carry another label.
- The host mode ends with a passing result and a clean cleanup text while something labelled is left.
- The listing is read from the merged output, so a warning that SSH prints on standard error is taken as a name and reaches ``docker volume rm``.
- A listing that holds a line that is not a name this run makes (an unrelated name, a name of another run, a name with a longer token, a warning) removes
  something, or reads as readable. It must remove nothing of its kind and fail the run, also when only the first of two listings is unreadable.
"""

from __future__ import annotations

import importlib
import shlex
import subprocess
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
    """The Docker of the live host with the labelled resources of one run. ``stubborn`` refuses every removal.

    ``warning`` is what SSH prints on stderr for every call. ``stray`` is an extra line on stdout of the listing of ``stray_kind`` (a name that the listing
    would not give). ``read`` answers with stdout alone and ``run`` with stdout and stderr together, as the two calls of the runner's target do.
    """

    compose_directory = "/opt/platform/compose"
    secrets_directory = "/opt/platform/secrets"
    env_files = ("/opt/platform/compose/limits.env",)
    ports: dict[str, int] = {}
    project = "surgical_risk"
    name = "live host"

    def __init__(
        self, token: str, *, stubborn: bool = False, listing_fails: bool = False, warning: str = "", stray: str = "", stray_kind: str = "containers"
    ) -> None:
        self.containers = {
            f"p2m_marker_{token}": f"p2m_run={token}",
            f"p2m_retention_{token}": f"p2m_run={token}",
            f"p2m_marker_{OTHER_RUN}": f"p2m_run={OTHER_RUN}",
            "unrelated_service": "other=1",
        }
        self.volumes = {f"p2m_retention_{token}": f"p2m_run={token}", f"p2m_retention_{OTHER_RUN}": f"p2m_run={OTHER_RUN}"}
        self.stubborn = stubborn
        self.listing_fails = listing_fails
        self.warning = warning
        self.stray = stray
        self.stray_kind = stray_kind
        self.stray_once = False
        self.commands: list[list[str]] = []

    def labelled(self, items: dict[str, str], words: list[str]) -> list[str]:
        wanted = words[words.index("--filter") + 1].removeprefix("label=")
        return [name for name, label in items.items() if label == wanted]

    def remove(self, items: dict[str, str], names: list[str]) -> tuple[int, str]:
        refused = self.stubborn or any(name not in items for name in names)
        for name in [] if self.stubborn else [name for name in names if name in items]:
            del items[name]
        return (1, "removal refused\n") if refused else (0, "")

    def answer(self, args: list[str]) -> tuple[int, str]:
        words = args[1:]
        if args[0] != "docker":
            return 127, "not served\n"
        listing = {"ps": ("containers", self.containers), "volume": ("volumes", self.volumes)}.get(words[0]) if words[:1] != ["rm"] else None
        if listing is not None and "--filter" in words:
            if self.listing_fails:
                return 255, ""
            kind, items = listing
            ids = "--format" not in words
            names = [f"{abs(hash(name)):x}" if ids else name for name in self.labelled(items, words)]
            stray = [self.stray] if self.stray and self.stray_kind == kind else []
            if self.stray_once and stray:
                self.stray = ""
            return 0, "".join(f"{name}\n" for name in [*names, *stray])
        if words[:1] == ["rm"]:
            return self.remove(self.containers, [word for word in words[1:] if not word.startswith("-")])
        if words[:2] == ["volume", "rm"]:
            return self.remove(self.volumes, [word for word in words[2:] if not word.startswith("-")])
        return 0, ""

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        self.commands.append(args)
        code, output = self.answer(args)
        return code, output + self.warning

    def read(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        self.commands.append(args)
        return self.answer(args)


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


WARNING = "Warning: Permanently added '100.64.0.9' (ED25519) to the list of known hosts.\n"
OF_THE_RUN = {"containers": ["p2m_marker_abc123", "p2m_retention_abc123"], "volumes": ["p2m_retention_abc123"]}


def removed(target: HostDocker) -> list[str]:
    """Every name that a removal command of the run was given."""
    names: list[str] = []
    for words in target.commands:
        if words[1:2] == ["rm"]:
            names += [word for word in words[2:] if not word.startswith("-")]
        if words[1:3] == ["volume", "rm"]:
            names += [word for word in words[3:] if not word.startswith("-")]
    return names


class LabelledCleanup(unittest.TestCase):
    def test_the_resources_of_the_run_are_listed_removed_and_verified_on_the_host(self) -> None:
        module = runner()
        stack = stack_of(module, HostDocker("abc123"))
        mac = MacDocker()
        with mock.patch.object(module, "run_command", mac):
            self.assertTrue(module.remove_labelled(stack, []))
        self.assertEqual(mac.calls, [])
        self.assertEqual(
            (stack.target.containers, stack.target.volumes),
            ({f"p2m_marker_{OTHER_RUN}": f"p2m_run={OTHER_RUN}", "unrelated_service": "other=1"}, {f"p2m_retention_{OTHER_RUN}": f"p2m_run={OTHER_RUN}"}),
        )

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

    def test_the_listings_ask_for_names_and_not_for_identifiers(self) -> None:
        module = runner()
        target = HostDocker("abc123")
        module.remove_labelled(stack_of(module, target), [])
        listings = [words for words in target.commands if "--filter" in words]
        self.assertEqual(len(listings), 4)
        shapes = {(" ".join(words[1:3]) if words[1] == "volume" else words[1], words[-1]) for words in listings}
        self.assertEqual(shapes, {("volume ls", "{{.Name}}"), ("ps", "{{.Names}}")})

    def test_a_warning_on_the_error_output_of_ssh_reaches_no_removal(self) -> None:
        module = runner()
        target = HostDocker("abc123", warning=WARNING)
        self.assertTrue(module.remove_labelled(stack_of(module, target), []))
        self.assertEqual(sorted(removed(target)), sorted([*OF_THE_RUN["containers"], *OF_THE_RUN["volumes"]]))
        self.assertEqual(sorted(target.containers), sorted([f"p2m_marker_{OTHER_RUN}", "unrelated_service"]))

    def test_a_listing_with_a_name_that_the_run_does_not_make_removes_nothing_and_fails(self) -> None:
        module = runner()
        for kind in ("containers", "volumes"):
            for stray in ("unrelated_service", f"p2m_marker_{OTHER_RUN}", "p2m_marker_abc123x", "p2m_abc123", "p2m_marker_abc123_more", "Warning: key added"):
                with self.subTest(kind=kind, stray=stray):
                    target = HostDocker("abc123", stray=stray, stray_kind=kind)
                    before = (dict(target.containers), dict(target.volumes))
                    log: list[str] = []
                    self.assertFalse(module.remove_labelled(stack_of(module, target), log))
                    self.assertEqual([word for word in removed(target) if word == stray], [])
                    self.assertEqual(len(before[0 if kind == "containers" else 1]), len(target.containers if kind == "containers" else target.volumes))
                    self.assertIn(f"== labelled {kind} on live host: unreadable", log)

    def test_a_listing_that_was_unreadable_once_fails_the_run_even_when_the_next_one_is_clean(self) -> None:
        module = runner()
        target = HostDocker("abc123", stray="unrelated_service", stray_kind="volumes")
        target.stray_once = True
        target.containers, target.volumes = {}, {}
        self.assertFalse(module.remove_labelled(stack_of(module, target), []))
        self.assertEqual(removed(target), [])

    def test_a_name_of_this_run_on_the_listing_is_still_removed_with_a_stray_in_the_other_kind(self) -> None:
        module = runner()
        target = HostDocker("abc123", stray="unrelated_service", stray_kind="volumes")
        self.assertFalse(module.remove_labelled(stack_of(module, target), []))
        self.assertEqual(sorted(target.containers), sorted([f"p2m_marker_{OTHER_RUN}", "unrelated_service"]))
        self.assertIn("p2m_retention_abc123", target.volumes)


class RemovalTimesOut(HostDocker):
    """The live host's Docker whose removal of a container does not answer in time: the call raises, as the SSH call does after its deadline."""

    def run(self, args: list[str], *, stdin: str | None = None, timeout: float = 0) -> tuple[int, str]:
        if args[1:2] == ["rm"]:
            raise subprocess.TimeoutExpired("ssh", 60)
        return super().run(args, stdin=stdin, timeout=timeout)


class HostRunResult(unittest.TestCase):
    def run_host(self, module: ModuleType, target: HostDocker) -> Record:
        record = Record(feature="x", slug="x", rerun="x")

        def cases(stack: Any, scratch: object) -> None:
            for items in (target.containers, target.volumes):
                for key, label in list(items.items()):
                    if label == "p2m_run=abc123":
                        del items[key]
                        items[key.replace("abc123", stack.token)] = f"p2m_run={stack.token}"

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
        self.assertIn("labelled test resources: True", record.cleanup)
        self.assertEqual([item.case for item in record.observations if not item.passed], [])

    def test_a_warning_of_ssh_does_not_change_the_result(self) -> None:
        module = runner()
        record = self.run_host(module, HostDocker("abc123", warning=WARNING))
        self.assertIn("labelled test resources: True", record.cleanup)
        self.assertEqual([item.case for item in record.observations if not item.passed], [])

    def test_a_removal_that_times_out_fails_the_run_and_the_cleanup_says_so(self) -> None:
        module = runner()
        record = self.run_host(module, RemovalTimesOut("abc123"))
        self.assertEqual([item.case for item in record.observations if not item.passed], ["labelled test resources"])
        self.assertTrue(record.cleanup.startswith("cleanup failed (labelled test resources)"), record.cleanup)
        self.assertIn("TimeoutExpired", next(item.observed for item in record.observations if not item.passed))
        record.settle()
        self.assertEqual(record.outcome, "fail")

    def test_a_resource_left_on_the_host_fails_the_cleanup_text_and_the_run(self) -> None:
        module = runner()
        record = self.run_host(module, HostDocker("abc123", stubborn=True))
        self.assertIn("labelled test resources: False", record.cleanup)
        self.assertEqual([item.case for item in record.observations if not item.passed], ["labelled test resources"])


if __name__ == "__main__":
    unittest.main()
