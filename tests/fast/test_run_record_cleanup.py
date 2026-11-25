"""A clean-up step that fails, times out or is interrupted fails the record and never stops the steps after it.

Failure modes covered here:

- A step that times out (a network removal that takes more than its deadline) raises out of the clean-up: the steps after it never run, the scratch folder is
  left and the record's clean-up stays "pending".
- A step that returns False or raises is not named in the record, so the run reads as clean.
- The scratch folder is removed only when every step before it worked, and its removal is not verified.
- An interrupt in a step skips the scratch removal.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.e2e.run_record import Record, clean_up, cleanup_step


def record() -> Record:
    return Record(feature="x", slug="x", rerun="x")


def scratch_folder() -> Path:
    folder = Path(tempfile.mkdtemp(prefix="cleanup_steps_"))
    (folder / "inner").mkdir()
    return folder


def timeout() -> bool:
    raise subprocess.TimeoutExpired("docker", 60)


class OneStep(unittest.TestCase):
    def test_a_step_that_works_passes(self) -> None:
        run = record()
        self.assertTrue(cleanup_step(run, "network removed", "Docker removes the network", "removed", lambda: True))
        self.assertEqual([(item.case, item.passed, item.observed) for item in run.observations], [("network removed", True, "removed: True")])

    def test_a_step_that_returns_false_fails_and_names_itself(self) -> None:
        run = record()
        self.assertFalse(cleanup_step(run, "network removed", "Docker removes the network", "removed", lambda: False))
        self.assertEqual([(item.case, item.passed) for item in run.observations], [("network removed", False)])

    def test_a_step_that_times_out_fails_and_names_the_error(self) -> None:
        run = record()
        self.assertFalse(cleanup_step(run, "network removed", "Docker removes the network", "removed", timeout))
        self.assertEqual([(item.case, item.passed) for item in run.observations], [("network removed", False)])
        self.assertIn("TimeoutExpired", run.observations[0].observed)

    def test_an_interrupt_is_not_swallowed(self) -> None:
        def interrupted() -> bool:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            cleanup_step(record(), "x", "x", "done", interrupted)


class AllSteps(unittest.TestCase):
    def test_every_step_works_and_the_record_says_clean(self) -> None:
        run, folder = record(), scratch_folder()
        text = clean_up(run, [("project removed", "no resource left", "none left", lambda: True)], folder)
        self.assertEqual(run.cleanup, text)
        self.assertNotIn("failed", text)
        self.assertNotEqual(run.cleanup, "pending")
        self.assertFalse(folder.exists())
        self.assertEqual([item.case for item in run.observations], ["project removed", "scratch folder removed"])
        run.settle()
        self.assertEqual(run.outcome, "pass")

    def test_a_timed_out_step_fails_the_record_and_the_steps_after_it_still_run(self) -> None:
        run, folder = record(), scratch_folder()
        ran: list[str] = []

        def later() -> bool:
            ran.append("later")
            return True

        steps = [
            ("network removed", "Docker removes the network", "removed", timeout),
            ("later step", "it still runs", "done", later),
        ]
        text = clean_up(run, steps, folder)
        self.assertEqual(ran, ["later"])
        self.assertFalse(folder.exists())
        self.assertTrue(text.startswith("cleanup failed (network removed)"), text)
        self.assertEqual(run.cleanup, text)
        self.assertEqual(
            [(item.case, item.passed) for item in run.observations], [("network removed", False), ("later step", True), ("scratch folder removed", True)]
        )
        run.settle()
        self.assertEqual(run.outcome, "fail")

    def test_a_step_that_returns_false_fails_the_record(self) -> None:
        run, folder = record(), scratch_folder()
        text = clean_up(run, [("project removed", "no resource left", "none left", lambda: False)], folder)
        self.assertIn("cleanup failed (project removed)", text)
        run.settle()
        self.assertEqual(run.outcome, "fail")

    def test_a_scratch_folder_that_is_left_fails_the_record_and_names_the_step(self) -> None:
        run, folder = record(), scratch_folder()
        with mock.patch("shutil.rmtree"):
            text = clean_up(run, [], folder)
        self.assertIn("cleanup failed (scratch folder removed)", text)
        self.assertEqual([(item.case, item.passed) for item in run.observations], [("scratch folder removed", False)])
        shutil.rmtree(folder)

    def test_an_interrupt_in_a_step_still_removes_the_scratch_folder_and_sets_the_record(self) -> None:
        run, folder = record(), scratch_folder()

        def interrupted() -> bool:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            clean_up(run, [("network removed", "x", "removed", interrupted)], folder)
        self.assertFalse(folder.exists())
        self.assertNotEqual(run.cleanup, "pending")
        self.assertIn("scratch folder removed", [item.case for item in run.observations])

    def test_a_run_with_no_scratch_folder_has_no_scratch_step(self) -> None:
        run = record()
        clean_up(run, [("references deleted", "all deleted", "deleted", lambda: True)])
        self.assertEqual([item.case for item in run.observations], ["references deleted"])


if __name__ == "__main__":
    unittest.main()
