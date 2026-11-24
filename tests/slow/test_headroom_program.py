"""The headroom program as the hosted job runs it, with a stand-in for the ``gh`` command that prints a prepared answer.

Failure modes covered here: a run inside its limits fails, a job over its limit or a missing shard passes, a failed API command is taken for a pass,
the command asks for another run or attempt, and a missing run identifier is not reported.
"""

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.process import run_command
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS
from tests.fast.test_headroom import GUARDS_LEG, run_of


class ProgramTest(unittest.TestCase):
    """The program as the hosted job runs it, with a stand-in for the ``gh`` command that prints a prepared answer."""

    def setUp(self) -> None:
        home = tempfile.TemporaryDirectory(prefix="headroom_check_")
        self.addCleanup(home.cleanup)
        self.home = Path(home.name)
        self.stand_in = self.home / "bin" / "gh"
        self.stand_in.parent.mkdir()

    def run_program(self, answer: str, *, exit_code: int = 0) -> tuple[int, str, list[str]]:
        (self.home / "answer.json").write_text(answer)
        self.stand_in.write_text(f'#!/bin/sh\necho "$@" >> "{self.home}/arguments"\ncat "{self.home}/answer.json"\nexit {exit_code}\n')
        self.stand_in.chmod(self.stand_in.stat().st_mode | stat.S_IXUSR)
        summary = self.home / "summary.md"
        env = {
            "PATH": f"{self.stand_in.parent}{os.pathsep}{os.environ['PATH']}",
            "PYTHONPATH": str(ROOT),
            "GITHUB_REPOSITORY": "owner/repository",
            "GITHUB_RUN_ID": "4242",
            "GITHUB_STEP_SUMMARY": str(summary),
        }
        run = run_command(sys.executable, ["-m", "scripts.quality.headroom"], cwd=ROOT, env=env, timeout=TIMEOUT_SECONDS)
        arguments = (self.home / "arguments").read_text().splitlines() if (self.home / "arguments").exists() else []
        return run.returncode, run.stderr + (summary.read_text() if summary.exists() else ""), arguments

    def test_a_run_inside_its_limits_passes_and_the_durations_reach_the_summary(self) -> None:
        code, text, arguments = self.run_program(json.dumps({"jobs": run_of()}))
        self.assertEqual(code, 0, text)
        self.assertIn(GUARDS_LEG, text)
        self.assertEqual(arguments, ["api --paginate repos/owner/repository/actions/runs/4242/jobs?filter=latest&per_page=100"])

    def test_a_job_over_its_limit_fails_the_program_and_is_named(self) -> None:
        code, text, _ = self.run_program(json.dumps({"jobs": run_of({GUARDS_LEG: 24.0})}))
        self.assertEqual(code, 1)
        self.assertIn(f"{GUARDS_LEG}: took 24.0 minutes", text)

    def test_a_missing_shard_fails_the_program_and_is_named(self) -> None:
        code, text, _ = self.run_program(json.dumps({"jobs": [job for job in run_of() if job["name"] != GUARDS_LEG]}))
        self.assertEqual(code, 1)
        self.assertIn(f"{GUARDS_LEG}: it is not among the jobs of the run", text)

    def test_a_failed_api_command_fails_the_program(self) -> None:
        code, text, _ = self.run_program(json.dumps({"jobs": run_of()}), exit_code=1)
        self.assertNotEqual(code, 0, text)

    def test_a_missing_run_id_fails_the_program(self) -> None:
        self.stand_in.write_text("#!/bin/sh\nexit 0\n")
        run = run_command(
            sys.executable,
            ["-m", "scripts.quality.headroom"],
            cwd=ROOT,
            env={"PATH": os.environ["PATH"], "PYTHONPATH": str(ROOT)},
            timeout=TIMEOUT_SECONDS,
        )
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("GITHUB_RUN_ID", run.stderr)


if __name__ == "__main__":
    unittest.main()
