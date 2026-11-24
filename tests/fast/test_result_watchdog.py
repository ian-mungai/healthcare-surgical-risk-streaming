"""The watchdog of a sealed job removes every byte that the job staged for its result, also when it fires right before the result is published.

Failure modes covered here:

- A deadline that fires at the moment of publication (the last step of ``write_reserved``) leaves a staging file that holds the whole result document, so a
  reader who lists the folder finds a file with a result in it. The reserved ``<path>.part`` is the only staging file, and the watchdog's release of it
  removes every staged byte. A real child process runs the real watchdog (``os._exit``), so the folder is read after the exit, not before it.
- The three watchdog paths differ: ``spark.session.start_deadline`` (the Bronze, Silver signals and Silver hypotension jobs), the patient split job's own and
  the Silver cases job's own. Each is run here.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess

from scripts.process import run_command

CHILD = r"""
import os
import sys

kind, path = sys.argv[1], sys.argv[2]
real = os.link


def expire_at_publication(source, target, *args, **kwargs):
    if os.fspath(target) == path:
        timer = start_deadline(0)
        timer.join(2)
        raise AssertionError("the real watchdog did not exit the process")
    return real(source, target, *args, **kwargs)


if kind == "session":
    import argparse
    from functools import partial

    from spark.result_file import add_result_option, run_sealed
    from spark.session import start_deadline as session_deadline

    start_deadline = partial(session_deadline, job="toy")
    command = argparse.ArgumentParser(prog="toy")
    modes = command.add_subparsers(dest="mode", required=True)
    mode = modes.add_parser("verify")
    mode.add_argument("--branch", required=True)
    add_result_option(mode)
    call = lambda: run_sealed(command, ["verify", "--branch", "b", "--result", path], lambda arguments: {"mode": "verify", "branch": "b"}, "toy")
elif kind == "split":
    from spark.split_cli import run, start_deadline

    call = lambda: run(["verify", "--branch", "b", "--result", path], lambda arguments: {"passed": True})
else:
    from spark.silver_cases_run import run, start_deadline

    call = lambda: run(["verify", "--branch", "b", "--result", path], lambda arguments: {"passed": True})

os.link = expire_at_publication
raise SystemExit(call())
"""


class WatchdogAtPublication(unittest.TestCase):
    def run_child(self, kind: str) -> tuple[CompletedProcess[str], list[str]]:
        folder = self.enterContext(tempfile.TemporaryDirectory(prefix="watchdog_"))
        path = f"{folder}/result.json"
        done = run_command(sys.executable, ["-c", CHILD, kind, path], timeout=30, keep_newlines=True)
        return done, sorted(entry.name for entry in Path(folder).iterdir())

    def test_a_deadline_right_before_publication_leaves_an_empty_folder_for_every_watchdog(self) -> None:
        for kind, job in (("session", "toy"), ("split", "patient_split"), ("silver", "silver_cases")):
            with self.subTest(kind=kind):
                done, left = self.run_child(kind)
                self.assertEqual((124, f"{job}: timeout\n", ""), (done.returncode, done.stderr, done.stdout), done.stderr)
                self.assertEqual([], left)


if __name__ == "__main__":
    unittest.main()
