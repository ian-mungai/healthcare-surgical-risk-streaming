"""A planned stop of the replay service ends a long wait at once, saves the position and does not count as an attempt of the request.

Failure modes, each with its case: a SIGTERM during a pacing wait that is not noticed until the wait ends (the process is killed at the end of Compose's grace
period, so the request is counted as a start that did not end cleanly), a stop that is noticed but followed by one more send, a stop that leaves the position
unsaved, a stop that raises the start count, three stops that use up the attempts of a valid request, and a crash that is no longer counted.
The real signal is sent to a child process, so the handler, the pipe and the clock are the ones of the service. Kafka is not needed: the replay loop runs on
the in-memory stand-ins of ``replay_support``.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable
from pathlib import Path

from replay.events.manifest import build_manifest
from replay.events.run import Replayer, RunSettings
from replay.service.serve import Outcome, serve
from replay.service.stopping import StopFlag, SystemClock
from scripts.process import run_command
from tests.fast.replay_support import MemorySink, MemoryStore, RecordingSource, boundary_recordings, boundary_schedule, saved_state
from tests.fast.test_replay_serve import RECORD, REFERENCE, Clock

ROOT = Path(__file__).resolve().parents[2]
GRACE_SECONDS = 5.0
WATCHDOG_SECONDS = 20.0
CHILD = """
import os, signal, sys, threading, time
from replay.service.stopping import StopFlag, SystemClock
flag = StopFlag()
clock = SystemClock(flag)
threading.Timer(0.5, os.kill, (os.getpid(), getattr(signal, sys.argv[1]))).start()
clock.wait_until(clock.now_us() + 300 * 1_000_000)
sys.exit(0 if flag.is_set() else 3)
"""


class StopFlagTest(unittest.TestCase):
    def flag(self) -> StopFlag:
        flag = StopFlag(handle=False)
        self.addCleanup(flag.close)
        return flag

    def test_a_wait_runs_to_its_end_when_nothing_stops_it_and_a_set_flag_returns_at_once(self) -> None:
        flag = self.flag()
        started = time.monotonic()
        self.assertFalse(flag.wait(0.2))
        self.assertGreaterEqual(time.monotonic() - started, 0.15)
        flag.request()
        started = time.monotonic()
        self.assertTrue(flag.wait(60))
        self.assertTrue(flag.wait(60))
        self.assertLess(time.monotonic() - started, 1)

    def test_a_stop_from_another_thread_ends_a_long_wait_of_the_flag_at_once(self) -> None:
        flag = self.flag()
        threading.Timer(0.2, flag.request).start()
        started = time.monotonic()
        self.assertTrue(flag.wait(30))
        self.assertLess(time.monotonic() - started, 0.9)

    def test_a_stop_from_another_thread_ends_a_long_wait_of_the_system_clock_within_a_second(self) -> None:
        flag = self.flag()
        clock = SystemClock(flag)
        threading.Timer(0.2, flag.request).start()
        started = time.monotonic()
        clock.wait_until(clock.now_us() + 300 * 1_000_000)
        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(flag.is_set())

    def test_the_clock_polls_during_a_wait_and_a_target_in_the_past_returns_at_once(self) -> None:
        flag = self.flag()
        polls: list[int] = []
        clock = SystemClock(flag, lambda: polls.append(1))
        started = time.monotonic()
        clock.wait_until(clock.now_us() + 1_300_000)
        self.assertGreaterEqual(time.monotonic() - started, 1.2)
        self.assertGreaterEqual(len(polls), 1)
        clock.wait_until(clock.now_us() - 1)

    def test_a_real_sigterm_or_sigint_during_a_300_second_wait_ends_the_process_within_five_seconds(self) -> None:
        for name in ("SIGTERM", "SIGINT"):
            with self.subTest(signal=name):
                started = time.monotonic()
                result = run_command(sys.executable, ["-c", CHILD, name], timeout=60, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT)})
                self.assertLess(time.monotonic() - started, GRACE_SECONDS)
                self.assertEqual((0, ""), (result.returncode, result.stderr))


class StoppedWaitLoopTest(unittest.TestCase):
    """The replay loop and the service loop over a real ``StopFlag`` and ``SystemClock``: a stop during a long pacing wait."""

    OPTIONS = {"rooms": 2, "window_us": 40_000_000}
    SLOW = RunSettings(speed=0.01, checkpoint_every=1000, idle_checkpoint_us=10**12)
    FAST = RunSettings(speed=1000.0, checkpoint_every=5, idle_checkpoint_us=10**12)

    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="replay_stopping_")
        self.addCleanup(folder.cleanup)
        self.inbox = Path(folder.name) / "inbox"
        self.inbox.mkdir()
        self.health = Path(folder.name) / "health.json"
        (self.inbox / "a.json").write_text(json.dumps(RECORD))
        (self.inbox / "a.reference").write_text(REFERENCE)
        self.sink, self.store = MemorySink(), MemoryStore()
        self.frozen = boundary_schedule(**self.OPTIONS)

    def start(self, settings: RunSettings, stop_after: float | None) -> tuple[float, int]:
        """One start of the service: serve the inbox once, with a stop requested ``stop_after`` seconds into the run, and return the time it took."""
        flag = StopFlag(handle=False)
        self.addCleanup(flag.close)
        results: list[Outcome] = []

        def run_request(path: Path, beat: Callable[[], None]) -> Outcome:
            replayer = Replayer(self.frozen, settings, RecordingSource(boundary_recordings()), self.sink, self.store, SystemClock(flag, beat), flag.is_set)
            result = replayer.run()
            outcome = Outcome("finished", 0) if result.finished else Outcome("stopped", 2)
            results.append(outcome)
            return outcome

        for seconds in (stop_after, WATCHDOG_SECONDS):
            if seconds is not None:
                timer = threading.Timer(seconds, flag.request)
                timer.daemon = True
                self.addCleanup(timer.cancel)
                timer.start()
        started = time.monotonic()
        sleep = flag.wait if stop_after is not None else lambda seconds: flag.request()
        serve(self.inbox, self.health, run_request, flag.is_set, Clock(), sleep, 60.0)
        return time.monotonic() - started, len(results)

    def attempts(self) -> str:
        return (self.inbox / "a.attempts").read_text().strip()

    def test_a_stop_during_a_long_pacing_wait_returns_within_five_seconds_with_the_position_saved_and_the_count_unchanged(self) -> None:
        elapsed, _ = self.start(self.SLOW, stop_after=0.5)
        self.assertLess(elapsed, GRACE_SECONDS)
        state = saved_state(self.store)
        self.assertFalse(state.finished)
        self.assertGreaterEqual(len(self.sink.sent), 1)
        self.assertEqual(len(self.sink.sent), self.sink.acknowledged)
        self.assertEqual("0", self.attempts())
        self.assertTrue((self.inbox / "a.json").is_file())
        self.assertFalse((self.inbox / "done").exists())

    def test_three_stops_during_a_wait_then_a_fourth_start_replays_normally_and_finishes(self) -> None:
        for number in (1, 2, 3):
            with self.subTest(stop=number):
                elapsed, _ = self.start(self.SLOW, stop_after=0.4)
                self.assertLess(elapsed, GRACE_SECONDS)
                self.assertEqual("0", self.attempts())
        self.start(self.FAST, stop_after=None)
        self.assertTrue(saved_state(self.store).finished)
        self.assertTrue((self.inbox / "done" / "a.json").is_file())
        self.assertEqual("finished", json.loads((self.inbox / "results" / "a.json").read_text())["outcome"])
        expected = set(build_manifest(self.frozen, boundary_recordings()).events)
        self.assertEqual(expected, {event.event_id for event in self.sink.sent})

    def test_a_crash_is_still_counted_and_three_crashes_fail_the_request_on_the_fourth_start(self) -> None:
        class Crash(RuntimeError):
            pass

        def crashing(path: Path, beat: Callable[[], None]) -> Outcome:
            raise Crash("killed")

        stop = [False]
        for count in (1, 2, 3):
            stop[0] = False
            with self.assertRaises(Crash):
                serve(self.inbox, self.health, crashing, lambda: stop[0], Clock(), lambda seconds: None, 1.0)
            self.assertEqual(str(count), self.attempts())
        ran: list[str] = []
        calls = iter([False, True])

        def never_run(path: Path, beat: Callable[[], None]) -> Outcome:
            ran.append(path.name)
            return Outcome("finished", 0)

        serve(self.inbox, self.health, never_run, lambda: next(calls), Clock(), lambda seconds: None, 1.0)
        self.assertEqual([], ran)
        result = json.loads((self.inbox / "results" / "a.json").read_text())
        self.assertEqual(("failed", "the service stopped during this request 3 times"), (result["outcome"], result["error"]))


if __name__ == "__main__":
    unittest.main()
