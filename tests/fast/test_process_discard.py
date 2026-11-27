"""The launcher that discards a program's output returns its exit code and nothing else (``scripts.process.run_discarding_output``).

The Silver end-to-end runs record nothing about a job's standard output or standard error but its exit code. This launcher gives the program no pipe to
write to, so no byte of either stream can change the record or stop the read, and it stops the program's whole process group on every exit like
``run_command``. Failure modes covered:

- The call returns the exit code of the program, and nothing of either stream (a number, never text).
- A byte that is not UTF-8, or a very large write, on either stream stops the call or changes its result.
- A deadline leaves the program or its descendants running, or leaks the program's arguments in the ``TimeoutExpired`` that it raises.
- An interrupt leaves the program or its descendants running.
- A descendant that outlives a normal exit keeps running.
- A program that is not there passes as an exit code.
- The program reads stdin and waits for it.
- The shared launcher ``run_command`` changes: it still captures both streams.
"""

from __future__ import annotations

import contextlib
import gc
import os
import signal
import tempfile
import threading
import time
import unittest
import warnings
from collections.abc import Iterator
from pathlib import Path

from scripts.process import TimeoutExpired, run_command, run_discarding_output

START_SCRIPT = 'echo $$ > "$1"; sleep 300 >/dev/null 2>&1 & echo $! >> "$1"; {tail}'
WAIT_SECONDS = 10
PRIVATE = "example_private_argument"


class DiscardTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="process_discard_")
        self.addCleanup(folder.cleanup)
        self.pids = Path(folder.name) / "pids"
        self.addCleanup(self.collect_unreaped)
        self.addCleanup(self.kill_leftovers)

    def collect_unreaped(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            gc.collect()

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

    def discard(self, tail: str, *, timeout: float = 30) -> int:
        return run_discarding_output("sh", ["-c", START_SCRIPT.format(tail=tail), "sh", str(self.pids), PRIVATE], timeout=timeout)

    def assert_all_gone(self) -> None:
        self.assertEqual(2, len(self.started()))
        for pid in self.started():
            with self.subTest(pid=pid), self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    @contextlib.contextmanager
    def interrupt_after_start(self) -> Iterator[None]:
        previous = signal.signal(signal.SIGINT, signal.default_int_handler)

        def send() -> None:
            self.wait_for_start()
            os.kill(os.getpid(), signal.SIGINT)

        sender = threading.Thread(target=send)
        sender.start()
        try:
            yield
        finally:
            sender.join()
            signal.signal(signal.SIGINT, previous)

    def test_the_call_returns_the_exit_code_and_a_number_only(self) -> None:
        for code in (0, 1, 2, 3):
            with self.subTest(code=code):
                result = run_discarding_output("sh", ["-c", f"echo out; echo err >&2; exit {code}"], timeout=10)
                self.assertIs(int, type(result))
                self.assertEqual(code, result)

    def test_a_byte_that_is_not_utf8_on_either_stream_changes_nothing(self) -> None:
        self.assertEqual(0, run_discarding_output("sh", ["-c", "printf 'a\\377\\376b'; printf 'c\\377\\200' >&2"], timeout=10))
        self.assertEqual(5, run_discarding_output("sh", ["-c", "printf '\\377' >&2; printf '\\377'; exit 5"], timeout=10))

    def test_a_very_large_write_on_either_stream_does_not_block_or_change_the_code(self) -> None:
        script = "head -c 5000000 /dev/zero; head -c 5000000 /dev/zero >&2; exit 7"
        self.assertEqual(7, run_discarding_output("sh", ["-c", script], timeout=30))

    def test_stdin_is_closed(self) -> None:
        self.assertEqual(0, run_discarding_output("cat", [], timeout=10))

    def test_a_deadline_stops_the_program_and_its_descendants_and_raises_the_timeout_without_the_arguments(self) -> None:
        with self.assertRaises(TimeoutExpired) as caught:
            self.discard("wait", timeout=1)
        self.assertNotIn(PRIVATE, f"{caught.exception!s} {caught.exception!r} {caught.exception.cmd!r}")
        self.assert_all_gone()

    def test_an_interrupt_stops_the_program_and_its_descendants(self) -> None:
        with self.interrupt_after_start(), self.assertRaises(KeyboardInterrupt):
            self.discard("wait")
        self.assert_all_gone()

    def test_a_descendant_that_outlives_a_normal_exit_is_stopped(self) -> None:
        self.assertEqual(0, self.discard("true"))
        self.assert_all_gone()

    def test_a_program_that_is_not_there_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            run_discarding_output("no_such_program_for_this_test", [], timeout=10)

    def test_the_shared_launcher_still_captures_both_streams(self) -> None:
        result = run_command("sh", ["-c", "echo out; echo err >&2; exit 3"], timeout=10)
        self.assertEqual((3, "out\n", "err\n"), (result.returncode, result.stdout, result.stderr))


if __name__ == "__main__":
    unittest.main()
