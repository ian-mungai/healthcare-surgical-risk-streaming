"""The end of a host batch run: one finalizer that no signal can cut short, the memory gate, and the bounded waits.

The runners are untracked, so the tests run only where they are on disk. Nothing starts: stand-ins answer every command.

Failure modes covered here:

- A SIGTERM kills the process with the containers, topics and references still there (the default handler runs no cleanup), or a signal that arrives during
  the cleanup stops it half way.
- The topics are deleted while their producers still run, or one failed step of the cleanup skips the steps after it (the record is never written).
- The memory gate reads Docker's own figures and not the host's available memory, ignores the reserve of ``compose/limits.yml``, or counts a limit of another
  service.
- A poll of a bounded wait gets its own full timeout instead of what is left, so the wait passes its deadline.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import os
import shutil
import signal
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.stack.limits import load_limits
from tests.e2e.run_record import ROOT, Record

RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")


class FakeStack:
    """Every call of the end of a run, in order."""

    def __init__(self, events: list[str], fail: set[str] | None = None, hook: Any = None) -> None:
        self.events = events
        self.fail = fail or set()
        self.hook = hook
        self.references = ["stream_control_0123abcd"]
        self.created_topics = ["replay_events_p8h_0123abcd_control"]
        self.project = "p8-host-0123abcd"

    def step(self, name: str) -> Any:
        self.events.append(name)
        if self.hook:
            self.hook(name)
        if name in self.fail:
            raise RuntimeError(f"{name} failed")

    def stop_producers(self) -> str:
        self.step("stop_producers")
        return "producers stopped"

    def drop_run_topics(self) -> list[str]:
        self.step("drop_topics")
        return []

    def remove_project_containers(self) -> tuple[str, bool]:
        self.step("remove_containers")
        return "removed", True

    def stop_and_keep(self) -> tuple[str, bool]:
        self.step("stop_and_keep")
        return "kept", True


class FakeNessie:
    def reference(self, name: str) -> Any:
        raise AssertionError("not used here")


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class FinishCase(unittest.TestCase):
    def setUp(self) -> None:
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        self.enterContext(contextlib.redirect_stdout(io.StringIO()))
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        runs = ROOT / "runs"
        runs.mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="finish_test_", dir=runs))
        self.addCleanup(shutil.rmtree, self.root, True)
        self.evidence = self.host.Evidence("0123abcd", self.root)
        self.evidence.create()
        self.events: list[str] = []
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.inventory_calls: list[list[str]] = []

    def finalize(self, stack: FakeStack, *, failed: bool = False, record: Record | None = None, inventory_error: Exception | None = None) -> int:
        record = record or Record(feature="stand-in", slug="stand_in", rerun="none")
        if not record.observations:
            record.expect("c", "s", "e", "o", True)

        def inventory(client: Any, names: list[str]) -> list[dict[str, str]]:
            self.events.append("inventory")
            self.inventory_calls.append(names)
            if inventory_error:
                raise inventory_error
            return [{"name": name, "type": "BRANCH", "hash": "a" * 64} for name in names]

        patched = mock.patch.object(self.host, "inventory_references", inventory)
        patched.start()
        self.addCleanup(patched.stop)
        blocked = "the run was blocked" if failed else ""
        return int(self.host.finalize(record, ["log"], stack, self.work, self.evidence, nessie=FakeNessie(), blocked=blocked, failed=failed))


class Order(FinishCase):
    def test_the_producers_stop_before_their_topics_are_deleted_and_the_record_comes_last(self) -> None:
        stack = FakeStack(self.events)
        code = self.finalize(stack)
        self.assertEqual(0, code)
        self.assertLess(self.events.index("stop_producers"), self.events.index("drop_topics"))
        self.assertEqual(["stop_producers", "drop_topics", "inventory", "stop_and_keep"], self.events)
        self.assertTrue((self.evidence.folder / "record.json").is_file())
        self.assertTrue((self.evidence.folder / "inventory.json").is_file())

    def test_a_failed_run_removes_the_containers_and_keeps_the_volumes(self) -> None:
        stack = FakeStack(self.events)
        code = self.finalize(stack, failed=True)
        self.assertEqual(1, code)
        self.assertIn("remove_containers", self.events)
        self.assertNotIn("stop_and_keep", self.events)

    def test_the_inventory_lists_the_runs_references_topics_and_project(self) -> None:
        stack = FakeStack(self.events)
        self.finalize(stack)
        text = (self.evidence.folder / "inventory.json").read_text()
        for wanted in ("stream_control_0123abcd", "replay_events_p8h_0123abcd_control", "p8-host-0123abcd"):
            self.assertIn(wanted, text)

    def test_a_failed_step_does_not_skip_the_steps_after_it_and_fails_the_run(self) -> None:
        for failing in ("stop_producers", "drop_topics"):
            with self.subTest(failing=failing):
                self.events.clear()
                code = self.finalize(FakeStack(self.events, fail={failing}))
                self.assertEqual(1, code)
                self.assertEqual(["stop_producers", "drop_topics", "inventory", "remove_containers"], self.events)
                self.assertTrue((self.evidence.folder / "record.json").is_file())

    def test_a_failed_inventory_does_not_skip_the_removal_or_the_record(self) -> None:
        code = self.finalize(FakeStack(self.events), inventory_error=RuntimeError("down"))
        self.assertEqual(1, code)
        self.assertEqual(["stop_producers", "drop_topics", "inventory", "remove_containers"], self.events)
        self.assertTrue((self.evidence.folder / "record.json").is_file())

    def test_a_run_that_never_reached_nessie_still_cleans_up_and_records_an_empty_inventory(self) -> None:
        stack = FakeStack(self.events)
        stack.references = []
        code = self.host.finalize(
            Record(feature="s", slug="s", rerun="n"), ["log"], stack, self.work, self.evidence, nessie=None, blocked="the run was blocked", failed=True
        )
        self.assertEqual(1, code)
        self.assertIn("remove_containers", self.events)
        self.assertEqual([], __import__("json").loads((self.evidence.folder / "inventory.json").read_text())["references"])


class Interrupts(FinishCase):
    def test_a_sigterm_during_the_cleanup_is_held_until_the_record_is_written_and_then_raised(self) -> None:
        def hook(name: str) -> None:
            if name == "drop_topics":
                os.kill(os.getpid(), signal.SIGTERM)

        stack = FakeStack(self.events, hook=hook)
        before = signal.getsignal(signal.SIGTERM)
        with self.assertRaises(self.host.Terminated):
            self.finalize(stack)
        self.assertEqual("stop_and_keep", self.events[-1])
        self.assertTrue((self.evidence.folder / "record.json").is_file())
        self.assertEqual(before, signal.getsignal(signal.SIGTERM))

    def test_a_sigint_during_the_cleanup_is_held_and_raised_after_it(self) -> None:
        def hook(name: str) -> None:
            if name == "stop_producers":
                os.kill(os.getpid(), signal.SIGINT)

        stack = FakeStack(self.events, hook=hook)
        with self.assertRaises(KeyboardInterrupt):
            self.finalize(stack)
        self.assertIn("drop_topics", self.events)
        self.assertTrue((self.evidence.folder / "record.json").is_file())

    def test_a_sigterm_during_the_run_becomes_an_exception_that_the_run_can_clean_up_after(self) -> None:
        original = signal.getsignal(signal.SIGTERM)
        with self.assertRaises(self.host.Terminated), self.host.terminate_as_exception():
            os.kill(os.getpid(), signal.SIGTERM)
        self.assertEqual(original, signal.getsignal(signal.SIGTERM))

    def test_the_terminated_signal_is_not_an_ordinary_exception(self) -> None:
        self.assertFalse(issubclass(self.host.Terminated, Exception))


class Memory(FinishCase):
    LIMITS = {"MEM_LIMIT_REPLAY": "1024m", "MEM_LIMIT_SPARK": "4096m", "MEM_LIMIT_KAFKA": "9999m"}

    def reserve(self) -> int:
        return load_limits().host_reserve_mib

    def test_the_need_is_the_reserve_of_the_limits_file_and_the_replay_and_spark_limits_only(self) -> None:
        needed = self.reserve() + 1024 + 4096
        ok, text = self.host.memory_gate(self.LIMITS, needed)
        self.assertTrue(ok, text)
        self.assertIn(str(needed), text)
        ok, text = self.host.memory_gate(self.LIMITS, needed - 1)
        self.assertFalse(ok, text)

    def test_the_gate_reads_the_hosts_available_memory_with_the_admission_checks_reader(self) -> None:
        self.assertIs(self.host.read_available_mib, importlib.import_module("scripts.stack.admission").read_available_mib)
        self.assertFalse(hasattr(self.host, "free_memory"))

    def test_a_limit_that_is_not_in_mib_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self.host.memory_gate({"MEM_LIMIT_REPLAY": "1g", "MEM_LIMIT_SPARK": "4096m"}, 100000)


class Remaining(FinishCase):
    def test_a_poll_gets_what_is_left_of_the_wait_and_never_more_than_its_cap(self) -> None:
        self.assertEqual(60, self.host.remaining(1000.0, 60, now=900.0))
        self.assertEqual(30.0, self.host.remaining(1000.0, 60, now=970.0))

    def test_a_wait_with_a_second_or_less_left_blocks_the_run(self) -> None:
        with self.assertRaises(self.host.Blocked):
            self.host.remaining(1000.0, 60, now=999.5)
        with self.assertRaises(self.host.Blocked):
            self.host.remaining(1000.0, 60, now=1100.0)


if __name__ == "__main__":
    unittest.main()
