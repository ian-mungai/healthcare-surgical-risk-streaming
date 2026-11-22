"""The process launcher stops the whole process group of its program on every exit and confirms the group is gone.

Each case runs a real shell that starts a ``sleep`` of its own and records both process IDs. Failure modes covered:

- A parent interrupt (Ctrl-C) leaves the program and its descendants running in their own session.
- A deadline stops the group, and the original ``TimeoutExpired`` still reaches the caller.
- A descendant that outlives a program that exited normally, with its output closed, keeps running.
- macOS answers the group check with a permission error while the last members of the group are unreaped. The launcher waits
  for them. A real run reaches this only by timing, so that case stubs the check.
- A process that survives the kill is reported as a failure and not as success. SIGKILL cannot be survived by a real process
  here, so that one case replaces the group check with a stub.
- A failed cleanup after a deadline shows no argument of the timed-out command anywhere in the exception chain or its formatted traceback.
- The result of a normal run and the ``check`` error are unchanged.
- Output that is asked for byte for byte comes back with its carriage returns and bytes that are not UTF-8, so a Git listing keeps each file name.
"""

from __future__ import annotations

import contextlib
import gc
import os
import signal
import subprocess
import tempfile
import threading
import time
import traceback
import unittest
import warnings
from collections.abc import Iterator
from pathlib import Path
from subprocess import CalledProcessError
from unittest import mock

from scripts import process
from scripts.process import run_command

BUCKET = "example-private-bucket"
ROLE = "private_role_name"
START_SCRIPT = 'echo $$ > "$1"; sleep 300 >/dev/null 2>&1 & echo $! >> "$1"; {tail}'
WAIT_SECONDS = 10


class LauncherTest(unittest.TestCase):
    def setUp(self) -> None:
        folder = tempfile.TemporaryDirectory(prefix="process_launcher_")
        self.addCleanup(folder.cleanup)
        self.pids = Path(folder.name) / "pids"
        # Cleanups run last in, first out: the leftovers are killed first, then the unreaped process objects of the launcher are collected here. A case
        # that makes the launcher report a surviving group leaves its process object unreaped, and Python warns when it collects that object. Left to a
        # later collection, the warning would land in whichever test runs then and fail one that reads standard error.
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

    def run_with_private_arguments(self) -> BaseException:
        arguments = ["-c", START_SCRIPT.format(tail="wait"), "sh", str(self.pids), BUCKET, ROLE]
        with self.assertRaises(process.ProcessGroupSurvived) as caught:
            run_command("sh", arguments, timeout=1)
        self.kill_leftovers()
        return caught.exception

    def assert_chain_holds_no_private_argument(self, error: BaseException) -> None:
        text = "".join(traceback.format_exception(error))
        chain: list[BaseException] = []
        link: BaseException | None = error
        while link is not None and link not in chain:
            chain.append(link)
            for part in (link.__context__, link.__cause__):
                if part is not None and part not in chain:
                    chain.append(part)
            link = link.__context__
        for private in (BUCKET, ROLE):
            self.assertNotIn(private, text)
            for link in chain:
                self.assertNotIn(private, f"{link!s} {link!r} {link.args!r} {getattr(link, 'cmd', '')!r}")
        self.assertIn("ProcessGroupSurvived", text)
        self.assertIn("timed out after 1 seconds", text)

    def test_a_group_that_survives_after_a_deadline_shows_no_argument_of_the_timed_out_command(self) -> None:
        real_killpg = os.killpg

        def kill_but_report_alive(pgid: int, signum: int) -> None:
            if signum == signal.SIGKILL:
                real_killpg(pgid, signum)

        with mock.patch.object(os, "killpg", side_effect=kill_but_report_alive), mock.patch.object(process, "KILL_WAIT_SECONDS", 0.2):
            error = self.run_with_private_arguments()
        self.assertIsInstance(error.__context__, process.TimeoutExpired)
        self.assert_chain_holds_no_private_argument(error)

    def test_a_program_that_is_not_reaped_after_a_deadline_shows_no_argument_of_the_timed_out_command(self) -> None:
        with mock.patch.object(os, "killpg", side_effect=ProcessLookupError), mock.patch.object(process, "KILL_WAIT_SECONDS", 0.2):
            error = self.run_with_private_arguments()
        self.assert_chain_holds_no_private_argument(error)

    def test_an_interrupt_while_a_deadline_is_handled_shows_no_argument_of_the_timed_out_command(self) -> None:
        def interrupted(started: subprocess.Popen[str], *arguments: object, **options: object) -> None:
            try:
                raise subprocess.TimeoutExpired(started.args, 1)
            except subprocess.TimeoutExpired:
                raise KeyboardInterrupt from None

        with mock.patch.object(subprocess.Popen, "communicate", autospec=True, side_effect=interrupted), self.assertRaises(KeyboardInterrupt) as caught:
            run_command("sh", ["-c", "sleep 300", "sh", BUCKET, ROLE], timeout=1)
        text = "".join(traceback.format_exception(caught.exception))
        context = caught.exception.__context__
        for private in (BUCKET, ROLE):
            self.assertNotIn(private, f"{text} {context!s} {context!r}")
        self.assertIsInstance(context, subprocess.TimeoutExpired)

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

    def test_output_is_converted_to_newlines_by_default(self) -> None:
        result = run_command("printf", ["a\\rb\\r\\nc"], timeout=10)
        self.assertEqual("a\nb\nc", result.stdout)

    def test_output_kept_exact_has_its_carriage_returns_and_undecodable_bytes(self) -> None:
        result = run_command("sh", ["-c", "printf 'a\\rb\\r\\nc\\377'; printf 'e\\rf\\377' >&2"], timeout=10, keep_newlines=True)
        self.assertEqual(b"a\rb\r\nc\xff", os.fsencode(result.stdout))
        self.assertEqual(b"e\rf\xff", os.fsencode(result.stderr))

    def test_input_still_reaches_a_program_whose_output_is_kept_exact(self) -> None:
        result = run_command("cat", [], timeout=10, input_text="x\ry\n", keep_newlines=True)
        self.assertEqual("x\ry\n", result.stdout)


if __name__ == "__main__":
    unittest.main()
