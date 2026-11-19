"""The process launcher stops the whole process group of its program on every exit and confirms the group is gone.

Each case runs a real shell that starts a ``sleep`` of its own and records both process IDs. Failure modes covered:

- A parent interrupt (Ctrl-C) leaves the program and its descendants running in their own session.
- A deadline stops the group, and the original ``TimeoutExpired`` still reaches the caller.
- A descendant that outlives a program that exited normally, with its output closed, keeps running.
- macOS answers the group check with a permission error while the last members of the group are unreaped. The launcher waits
  for them. A real run reaches this only by timing, so that case stubs the check.
- A process that survives the kill is reported as a failure and not as success. SIGKILL cannot be survived by a real process
  here, so that one case replaces the group check with a stub.
- The result of a normal run and the ``check`` error are unchanged.
"""

from __future__ import annotations

import contextlib
import os
import signal
import tempfile
import threading
import time
import unittest
from collections.abc import Iterator
from pathlib import Path
from subprocess import CalledProcessError
from unittest import mock

from scripts import process
from scripts.process import run_command

START_SCRIPT = 'echo $$ > "$1"; sleep 300 >/dev/null 2>&1 & echo $! >> "$1"; {tail}'
WAIT_SECONDS = 10


class LauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="process_launcher_")
        self.addCleanup(folder.cleanup)
        self.pids = Path(folder.name) / "pids"
        self.addCleanup(self.kill_leftovers)

    def kill_leftovers(self) -> None:
        for pid in self.started():
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)

    def started(self) -> list[int]:
        return [int(line) for line in self.pids.read_text().split()] if self.pids.exists() else []

    def wait_for_start(self) -> None:
        deadline = time.monotonic() + WAIT_SECONDS
        while len(self.started()) < 2:
            if time.monotonic() > deadline:
                raise TimeoutError("the test program did not record its process IDs")
            time.sleep(0.02)

    def run_program(self, tail: str, *, timeout: float = 30) -> process.CompletedProcess[str]:
        return run_command("sh", ["-c", START_SCRIPT.format(tail=tail), "sh", str(self.pids)], timeout=timeout)

    def assert_all_gone(self) -> None:
        self.assertEqual(2, len(self.started()))
        for pid in self.started():
            with self.subTest(pid="program" if pid == self.started()[0] else "descendant"), self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    @contextlib.contextmanager
    def signal_after_start(self, signum: int) -> Iterator[None]:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)

        def send() -> None:
            self.wait_for_start()
            os.kill(os.getpid(), signum)

        sender = threading.Thread(target=send)
        sender.start()
        try:
            yield
        finally:
            sender.join()
            signal.signal(signal.SIGINT, previous)

    def test_an_interrupt_stops_the_program_and_its_descendants(self) -> None:
        with self.signal_after_start(signal.SIGINT), self.assertRaises(KeyboardInterrupt):
            self.run_program("wait")
        self.assert_all_gone()

    def test_a_deadline_stops_the_program_and_its_descendants_and_raises_the_timeout(self) -> None:
        with self.assertRaises(process.TimeoutExpired):
            self.run_program("wait", timeout=1)
        self.assert_all_gone()

    def test_a_descendant_that_outlives_a_normal_exit_is_stopped(self) -> None:
        result = self.run_program("echo done")
        self.assertEqual((0, "done\n"), (result.returncode, result.stdout))
        self.assert_all_gone()

    def test_a_group_that_survives_the_kill_is_reported(self) -> None:
        def alive(pgid: int, signum: int) -> None:
            if signum == signal.SIGKILL:
                raise ProcessLookupError

        with (
            mock.patch.object(os, "killpg", side_effect=alive),
            mock.patch.object(process, "KILL_WAIT_SECONDS", 0.2),
            self.assertRaises(process.ProcessGroupSurvived),
        ):
            self.run_program("echo done")
        self.kill_leftovers()

    def test_a_group_of_unreaped_processes_is_waited_for(self) -> None:
        answers = [PermissionError(), PermissionError(), ProcessLookupError()]

        def probe(pgid: int, signum: int) -> None:
            if signum == signal.SIGKILL:
                raise PermissionError
            raise answers.pop(0)

        with mock.patch.object(os, "killpg", side_effect=probe):
            result = self.run_program("echo done")
        self.assertEqual([], answers)
        self.assertEqual(0, result.returncode)
        self.kill_leftovers()

    def test_the_result_and_the_check_error_are_unchanged(self) -> None:
        failed = self.run_program("echo out; echo err >&2; exit 3")
        self.assertEqual((3, "out\n", "err\n"), (failed.returncode, failed.stdout, failed.stderr))
        with self.assertRaises(CalledProcessError) as caught:
            run_command("sh", ["-c", "exit 4"], timeout=10, check=True)
        self.assertEqual(4, caught.exception.returncode)


if __name__ == "__main__":
    unittest.main()
