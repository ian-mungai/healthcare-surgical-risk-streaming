"""The Silver cases runner's deadline probe does not depend on how fast the catalog answers.

The probe drives the job's own command line in the container with a Nessie address that accepts a connection and never answers, a deadline of 2 seconds and a
Nessie timeout of 100 seconds. Only the job's own timer can then stop it, with exit 124. The runner is untracked, so each test skips when it is not on this
machine. The program runs here on the host as it runs in the container (the job reads its settings and starts its timer before its first call to Nessie, and
needs no Spark for that), so no Docker runs.

Failure modes covered here:

- The probe set ``JOB_DEADLINE_SECONDS=1`` and asked a live Nessie, so whether the deadline or an answer came first depended on the speed of the catalog.
- A job whose timer was removed or does not start still passed the probe, because the catalog answered before the deadline or the call failed on its own.
- The probe passed on a job that exited 124 and left a result file or a container, or on a job that exited with another code.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command
from tests.e2e.run_record import ROOT, Record
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
            self.assertIn("silver_cases: timeout", done.stderr.splitlines())
            self.assertEqual([], list(Path(folder).iterdir()))
            self.assertLess(seconds, LIMIT_SECONDS / 2)

    def test_the_same_program_without_a_timer_that_fires_is_still_waiting_after_the_deadline_has_passed(self) -> None:
        """The probe is only a proof of the timer when the Nessie timeout (100 seconds) is longer than the wait: a job without its timer waits for it."""
        module = runner("silver_cases")
        self.assertEqual("2", module.STALL_ENV["JOB_DEADLINE_SECONDS"])
        self.assertGreaterEqual(float(module.STALL_ENV["NESSIE_TIMEOUT_SECONDS"]), 100)
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder, self.assertRaises(TimeoutExpired):
            on_host(module.STALL, Path(folder), {**module.STALL_ENV, "JOB_DEADLINE_SECONDS": "2000"}, 8)


class Probe(unittest.TestCase):
    """The case of ``bounded_failure`` that holds the probe, on a stack that answers what the test says."""

    def run_probe(self, answer: tuple[int, dict[str, Any]], left: list[str]) -> tuple[bool, list[Any]]:
        module = runner("silver_cases")
        calls: list[Any] = []

        def job(*arguments: Any, **keywords: Any) -> tuple[int, dict[str, Any], str, float]:
            calls.append((arguments, keywords))
            if arguments[0][:1] == ["-c"]:
                return answer[0], answer[1], "", 2.5
            return 2, {}, "", 0.1

        stack = SimpleNamespace(
            silver=lambda *_, **__: (1, {"diagnostics": [{"code": "branch_missing"}]}, "", 0.1),
            job=job,
            spark_containers=lambda: left,
        )
        run = module.Run.__new__(module.Run)
        run.record, run.stack, run.labels, run.suffix, run.made, run.trunk = Record(feature="t", slug="t", rerun="none"), stack, {}, "x1", [], "trunk_x1"
        run.bounded_failure()
        return run.record.observations[-1].passed, calls

    def test_the_probe_runs_the_job_command_line_with_a_deadline_of_2_seconds_and_a_nessie_timeout_of_100_seconds(self) -> None:
        module = runner("silver_cases")
        _, calls = self.run_probe((124, {}), [])
        probe = [(arguments, keywords) for arguments, keywords in calls if arguments[0][:1] == ["-c"]]
        self.assertEqual(1, len(probe))
        arguments, keywords = probe[0]
        self.assertEqual(module.STALL, arguments[0][1])
        self.assertEqual({"JOB_DEADLINE_SECONDS": "2", "NESSIE_TIMEOUT_SECONDS": "100"}, module.STALL_ENV)
        self.assertEqual(module.STALL_ENV, keywords.get("env", arguments[3] if len(arguments) > 3 else None))
        self.assertIsNotNone(keywords.get("sealed"))

    def test_the_probe_passes_only_on_exit_124_with_no_result_file_and_no_container(self) -> None:
        for label, answer, left, wanted in (
            ("a stop at the deadline", (124, {}), [], True),
            ("another exit code", (1, {}), [], False),
            ("a result file", (124, {"diagnostics": []}), [], False),
            ("a container left", (124, {}), ["spark"], False),
        ):
            with self.subTest(label):
                self.assertIs(wanted, self.run_probe(answer, left)[0])
