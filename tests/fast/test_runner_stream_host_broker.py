"""The broker restart of the host batch (``tests/e2e/stream_host.py``, with the cases of ``tests/e2e/stream_features.py``) always starts the broker again and
proves the recovery with the job that was running when the broker went down.

The runners are untracked, so the tests run only where they are on disk. Nothing starts: stand-ins answer every command and the clock is a counter.

Failure modes covered here:

- A read of the committed offsets, the check that input is still unread, the deadline of the gate or the stop itself fails with the broker down, and the
  start is skipped, so the deployed broker stays stopped for every other user of the host.
- The start does not wait for the broker to be healthy, waits with no deadline, or starts another container than the deployed one (a recreation).
- The recovery is shown by a replacement job, by a job that ran to its deadline and exited 0, or by offsets that were committed before the broker went down.
- A Ctrl-C or a SIGTERM that arrives while the broker is down stops the start (or the wait for the same container to be healthy), so the deployed broker stays
  stopped for every other user of the host.
- A read of the committed offsets that succeeds only after the deadline of the recovery is taken as the recovery.
"""

from __future__ import annotations

import importlib
import os
import signal
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from scripts.process import CompletedProcess
from tests.e2e.run_record import Record

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = (ROOT / "tests" / "e2e" / "stream_host.py", ROOT / "tests" / "e2e" / "stream_features.py")


def done(code: int = 0, text: str = "", error: str = "") -> CompletedProcess[str]:
    return CompletedProcess(["docker"], code, text, error)


class Clock:
    """A clock that only moves when a step sleeps, so a deadline can pass in a test."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeStack:
    """The calls of the restart, in order, with the answers of the test."""

    def __init__(self, module: ModuleType, committed: list[dict[int, int] | Exception], running: list[bool] | None = None) -> None:
        self.module = module
        self.events: list[str] = []
        self.committed = list(committed)
        self.running = list(running or [True])
        self.fail_stop: Exception | None = None
        self.fail_start: Exception | None = None
        self.containers: list[str] = []
        self.on_stop: Callable[[], None] | None = None
        self.on_start: Callable[[], None] | None = None
        self.on_read: Callable[[], None] | None = None

    def stop_broker(self, timeout: float) -> None:
        self.events.append("stop")
        if self.on_stop:
            self.on_stop()
        if self.fail_stop:
            raise self.fail_stop

    def start_broker(self, timeout: float) -> None:
        self.events.append("start")
        if self.on_start:
            self.on_start()
        if self.fail_start:
            raise self.fail_start
        self.events.append("ready")

    def committed_offsets(self, container: str, case: str, timeout: float = 60, *, strict: bool = False) -> dict[int, int]:
        self.events.append("committed")
        self.containers.append(container)
        if self.on_read:
            self.on_read()
        answer = self.committed.pop(0) if len(self.committed) > 1 else self.committed[0]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def docker(self, *arguments: str, timeout: float = 60) -> CompletedProcess[str]:
        self.events.append("inspect")
        self.containers.append(arguments[-1])
        state = self.running.pop(0) if len(self.running) > 1 else self.running[0]
        return done(0, "true\n" if state else "false\n")


@unittest.skipUnless(all(path.is_file() for path in RUNNERS), "the temporary runners are not in this checkout")
class RestartCase(unittest.TestCase):
    def setUp(self) -> None:
        self.features: ModuleType = importlib.import_module("tests.e2e.stream_features")
        self.host: ModuleType = importlib.import_module("tests.e2e.stream_host")
        self.clock = Clock()
        for module in (self.features, self.host):
            patched = mock.patch.object(module, "time", mock.Mock(monotonic=self.clock.monotonic, sleep=self.clock.sleep))
            patched.start()
            self.addCleanup(patched.stop)
        self.blocked = self.features.Blocked

    def deadline(self) -> float:
        return self.clock.now + 180


class StopAndStart(RestartCase):
    def test_the_broker_is_started_after_the_read_of_the_offsets(self) -> None:
        stack = FakeStack(self.features, [{0: 5, 1: 9}])
        committed, behind = self.features.restart_broker_while_unread(stack, "job", "case", {0: 10, 1: 9}, self.deadline())
        self.assertEqual(["stop", "committed", "start", "ready"], stack.events)
        self.assertEqual(({0: 5, 1: 9}, {0: 10}), (committed, behind))

    def test_the_start_is_sent_when_the_read_of_the_offsets_fails(self) -> None:
        stack = FakeStack(self.features, [self.blocked("the read failed")])
        with self.assertRaisesRegex(self.blocked, "the read failed"):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual(["stop", "committed", "start", "ready"], stack.events)

    def test_the_start_is_sent_when_the_stop_itself_fails(self) -> None:
        stack = FakeStack(self.features, [{0: 5}])
        stack.fail_stop = self.blocked("the stop timed out")
        with self.assertRaisesRegex(self.blocked, "the stop timed out"):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual(["stop", "start", "ready"], stack.events)

    def test_the_start_is_sent_when_nothing_was_unread(self) -> None:
        stack = FakeStack(self.features, [{0: 10}])
        with self.assertRaisesRegex(self.blocked, "nothing was unread"):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual(["stop", "committed", "start", "ready"], stack.events)

    def test_the_start_is_sent_when_the_deadline_of_the_gate_passes_after_the_stop(self) -> None:
        stack = FakeStack(self.features, [{0: 5}])
        original = stack.stop_broker

        def late_stop(timeout: float) -> None:
            original(timeout)
            self.clock.now += 1000

        stack.stop_broker = late_stop  # type: ignore[method-assign]
        with self.assertRaises(self.blocked):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual("ready", stack.events[-1])

    def test_a_start_that_fails_is_reported_beside_the_error_that_came_first(self) -> None:
        stack = FakeStack(self.features, [self.blocked("the read failed")])
        stack.fail_start = self.blocked("the broker did not become healthy")
        with self.assertRaisesRegex(self.blocked, "did not become healthy") as caught:
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertIn("the read failed", str(caught.exception.__context__))


class Terminated(BaseException):
    """What the run's SIGTERM handler raises (the host runner's own ``Terminated`` does the same)."""


class Interrupts(RestartCase):
    """The broker is down from the stop to the end of its start. A signal in that time waits until the broker is running and healthy again."""

    def setUp(self) -> None:
        super().setUp()
        saved = signal.signal(signal.SIGTERM, self.raise_terminated)
        self.addCleanup(signal.signal, signal.SIGTERM, saved)
        saved = signal.signal(signal.SIGINT, signal.default_int_handler)
        self.addCleanup(signal.signal, signal.SIGINT, saved)
        self.before = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}

    @staticmethod
    def raise_terminated(signum: int, frame: object) -> None:
        raise Terminated

    def test_a_sigterm_during_the_start_does_not_end_the_start_and_is_raised_again_after_it(self) -> None:
        stack = FakeStack(self.features, [{0: 5}])
        stack.on_start = lambda: os.kill(os.getpid(), signal.SIGTERM)
        with self.assertRaises(Terminated):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual(["stop", "committed", "start", "ready"], stack.events)
        self.assertEqual(self.before, {number: signal.getsignal(number) for number in self.before})

    def test_a_sigint_during_the_start_does_not_end_the_start_and_raises_after_it(self) -> None:
        stack = FakeStack(self.features, [{0: 5}])
        stack.on_start = lambda: os.kill(os.getpid(), signal.SIGINT)
        with self.assertRaises(KeyboardInterrupt):
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual(["stop", "committed", "start", "ready"], stack.events)

    def test_a_signal_during_the_stop_or_the_read_still_ends_with_the_start(self) -> None:
        for hook in ("on_stop", "on_read"):
            with self.subTest(hook=hook):
                stack = FakeStack(self.features, [{0: 5}])
                setattr(stack, hook, lambda: os.kill(os.getpid(), signal.SIGTERM))
                with self.assertRaises(Terminated):
                    self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
                self.assertEqual("ready", stack.events[-1])

    def test_a_signal_during_a_start_that_fails_still_reaches_the_caller_and_the_failure_stays_visible(self) -> None:
        stack = FakeStack(self.features, [{0: 5}])
        stack.on_start = lambda: os.kill(os.getpid(), signal.SIGINT)
        stack.fail_start = self.blocked("the broker did not become healthy")
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.features.restart_broker_while_unread(stack, "job", "case", {0: 10}, self.deadline())
        self.assertIn("did not become healthy", str(caught.exception.__context__))

    def test_the_health_check_of_the_same_container_is_not_cut_short_either(self) -> None:
        """The real host stack: the signal arrives while the first health reading is taken, and the reading that says healthy is still taken."""
        calls: list[tuple[str, ...]] = []
        health = ["true starting\n", "true healthy\n"]

        def runner(program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
            calls.append(tuple(args))
            if "inspect" in args:
                if len(health) == 2:
                    os.kill(os.getpid(), signal.SIGINT)
                return done(0, health.pop(0))
            if "ps" in args:
                return done(0, "abc123\n")
            if args[0] == "exec":
                return done(0, '{"replay_events": {"0": 5}}\n')
            return done()

        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        work = Path(scratch.name)
        (work / "run").mkdir()
        with mock.patch.object(self.host, "run_command", runner):
            stack = self.host.HostStack(record, [], "replay:test", "spark:test", work / "landed", work / "run", work)
            stack.input_topic = "replay_events"
            with self.assertRaises(KeyboardInterrupt):
                self.features.restart_broker_while_unread(stack, "abc123", "case", {0: 10}, self.deadline())
        self.assertEqual([], health)
        self.assertEqual(1, len([call for call in calls if call[-2:] == ("start", "kafka")]))


class Recovery(RestartCase):
    def test_the_recovery_is_the_original_jobs_commit_of_the_unread_offsets(self) -> None:
        stack = FakeStack(self.features, [{0: 5}, {0: 8}, {0: 10}])
        committed = self.features.await_recovery(stack, "job", "case", {0: 10}, self.deadline())
        self.assertEqual({0: 10}, committed)
        self.assertEqual({"job"}, set(stack.containers))

    def test_offsets_that_do_not_reach_every_partition_that_was_behind_are_not_a_recovery(self) -> None:
        stack = FakeStack(self.features, [{0: 10}], running=[True])
        with self.assertRaisesRegex(self.blocked, "within"):
            self.features.await_recovery(stack, "job", "case", {0: 10, 1: 7}, self.deadline())

    def test_a_job_that_ended_before_the_commit_is_no_recovery_even_with_exit_zero(self) -> None:
        stack = FakeStack(self.features, [{0: 5}], running=[True, False])
        with self.assertRaisesRegex(self.blocked, "ended before"):
            self.features.await_recovery(stack, "job", "case", {0: 10}, self.deadline())

    def test_a_poll_that_ends_after_the_deadline_blocks_the_run(self) -> None:
        stack = FakeStack(self.features, [{0: 5}], running=[True])
        with self.assertRaises(self.blocked):
            self.features.await_recovery(stack, "job", "case", {0: 10}, self.clock.now + 3)
        self.assertLess(self.clock.now, 1000 + 60)

    def test_a_read_that_succeeds_only_after_the_deadline_is_not_a_recovery(self) -> None:
        stack = FakeStack(self.features, [{0: 10}], running=[True])
        stack.on_read = lambda: setattr(self.clock, "now", self.clock.now + 500)
        with self.assertRaisesRegex(self.blocked, "within"):
            self.features.await_recovery(stack, "job", "case", {0: 10}, self.clock.now + 60)

    def test_a_read_that_returns_in_time_is_a_recovery(self) -> None:
        stack = FakeStack(self.features, [{0: 10}], running=[True])
        stack.on_read = lambda: setattr(self.clock, "now", self.clock.now + 5)
        self.assertEqual({0: 10}, self.features.await_recovery(stack, "job", "case", {0: 10}, self.clock.now + 60))

    def test_only_the_named_container_is_read(self) -> None:
        stack = FakeStack(self.features, [{0: 10}])
        self.features.await_recovery(stack, "original", "case", {0: 10}, self.deadline())
        self.assertEqual({"original"}, set(stack.containers))


class HostBroker(RestartCase):
    def stack(self, answers: dict[str, list[CompletedProcess[str]]]) -> tuple[Any, list[tuple[str, ...]]]:
        calls: list[tuple[str, ...]] = []

        def runner(program: str, args: list[str], **options: Any) -> CompletedProcess[str]:
            calls.append(tuple(args))
            for word, queue in answers.items():
                if word in args:
                    return queue.pop(0) if len(queue) > 1 else queue[0]
            return done()

        record = Record(feature="stand-in", slug="stand_in", rerun="none")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        work = Path(scratch.name)
        (work / "run").mkdir()
        with mock.patch.object(self.host, "run_command", runner):
            stack = self.host.HostStack(record, [], "replay:test", "spark:test", work / "landed", work / "run", work)
        patched = mock.patch.object(self.host, "run_command", runner)
        patched.start()
        self.addCleanup(patched.stop)
        stack.broker_container = "abc123"
        return stack, calls

    def test_the_start_sends_start_kafka_only_and_waits_for_the_same_container_to_be_healthy(self) -> None:
        stack, calls = self.stack({"inspect": [done(0, "true starting\n"), done(0, "true healthy\n")], "ps": [done(0, "abc123\n")]})
        stack.start_broker(300)
        commands = [call for call in calls if "compose" in call]
        self.assertEqual(1, len(commands))
        self.assertEqual(("start", "kafka"), commands[0][-2:])
        self.assertFalse([call for call in calls if "up" in call])
        self.assertIn("abc123", [call for call in calls if "inspect" in call][0])

    def test_a_broker_that_is_not_healthy_by_the_deadline_blocks_the_run(self) -> None:
        stack, _ = self.stack({"inspect": [done(0, "true starting\n")], "ps": [done(0, "abc123\n")]})
        with self.assertRaisesRegex(self.blocked, "healthy"):
            stack.start_broker(30)
        self.assertLess(self.clock.now, 1000 + 120)

    def test_a_recreated_broker_is_refused(self) -> None:
        stack, _ = self.stack({"inspect": [done(0, "true healthy\n")], "ps": [done(0, "def456\n")]})
        with self.assertRaisesRegex(self.blocked, "recreated|another container"):
            stack.start_broker(300)

    def test_a_start_command_that_fails_blocks_the_run(self) -> None:
        stack, _ = self.stack({"start": [done(1, "", "no")], "ps": [done(0, "abc123\n")]})
        with self.assertRaises(self.blocked):
            stack.start_broker(300)

    def test_the_stop_sends_stop_kafka_only(self) -> None:
        stack, calls = self.stack({"ps": [done(0, "abc123\n")]})
        stack.stop_broker(60)
        commands = [call for call in calls if "compose" in call]
        self.assertEqual(("stop", "kafka"), commands[0][-2:])


if __name__ == "__main__":
    unittest.main()
