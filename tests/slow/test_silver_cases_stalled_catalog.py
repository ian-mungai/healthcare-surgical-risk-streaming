"""The Silver cases runner's deadline probe program stops itself at the job's deadline when the catalog accepts a connection and never answers.

This is the part of the probe that binds a loopback server and starts a program, so it is in the slow group (project standard Q39). The quick group keeps the
probe's case with stand-in answers (``tests/fast/test_silver_cases_deadline_probe.py``).

The probe drives the job's own command line in the container with a Nessie address that accepts a connection and never answers, a deadline of 2 seconds and a
Nessie timeout of 100 seconds. Only the job's own timer can then stop it, with exit 124. The runner is untracked, so each test skips when it is not on this
machine. The program runs here on the host as it runs in the container (the job reads its settings and starts its timer before its first call to Nessie, and
needs no Spark for that), so no Docker runs.

Failure modes covered here:

- The probe set ``JOB_DEADLINE_SECONDS=1`` and asked a live Nessie, so whether the deadline or an answer came first depended on the speed of the catalog.
- A job whose timer was removed or does not start still passed the probe, because the catalog answered before the deadline or the call failed on its own.
- The job stopped at its deadline with a line other than ``silver_cases: timeout``, or with more lines than that one.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

from scripts.process import CompletedProcess, TimeoutExpired, run_command
from tests.e2e.run_record import ROOT
from tests.fast.test_runner_gold_sealed import runner

ENVIRONMENT = {"WAREHOUSE_LOCATION": "file:///warehouse/", "AWS_PROFILE": "no-such-profile", "AWS_CONFIG_FILE": os.devnull}
LIMIT_SECONDS = 30


def on_host(program: str, folder: Path, environment: dict[str, str], seconds: float) -> CompletedProcess[str]:
    """The program of the probe, run as ``python -c`` from the repository root with its result file in ``folder`` (the container mounts ``/result``)."""
    target = str(folder / "result.json")
    env = {"PATH": os.environ.get("PATH", ""), **ENVIRONMENT, **environment}
    return run_command(sys.executable, ["-c", program.replace("/result/result.json", target)], timeout=seconds, cwd=ROOT, env=env)


class StalledCatalog(unittest.TestCase):
    def test_the_job_stops_itself_at_its_deadline_with_exit_124_and_no_result_file(self) -> None:
        module = runner("silver_cases")
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder:
            started = time.monotonic()
            done = on_host(module.STALL, Path(folder), module.STALL_ENV, LIMIT_SECONDS)
            seconds = time.monotonic() - started
            self.assertEqual(124, done.returncode, done.stderr)
            self.assertEqual("silver_cases: timeout", done.stderr.strip())
            self.assertEqual(module.DEADLINE_LINE, done.stderr.strip())
            self.assertEqual([], list(Path(folder).iterdir()))
            self.assertLess(seconds, LIMIT_SECONDS / 2)

    def test_the_same_program_without_a_timer_that_fires_is_still_waiting_after_the_deadline_has_passed(self) -> None:
        """The probe is only a proof of the timer when the Nessie timeout (100 seconds) is longer than the wait: a job without its timer waits for it."""
        module = runner("silver_cases")
        self.assertEqual("2", module.STALL_ENV["JOB_DEADLINE_SECONDS"])
        self.assertGreaterEqual(float(module.STALL_ENV["NESSIE_TIMEOUT_SECONDS"]), 100)
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder, self.assertRaises(TimeoutExpired):
            on_host(module.STALL, Path(folder), {**module.STALL_ENV, "JOB_DEADLINE_SECONDS": "2000"}, 8)
