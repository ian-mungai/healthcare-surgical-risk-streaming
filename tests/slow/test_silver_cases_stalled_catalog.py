"""The Silver cases runner's deadline probe program stops the job at its deadline when the catalog accepts a connection and never answers, and flags only an
exact stop line.

This is the part of the probe that binds a loopback server and starts programs, so it is in the slow group (project standard Q39). The quick group keeps the
probe's case and the runner's reading of the flag with stand-in answers (``tests/fast/test_silver_cases_deadline_probe.py``).

The probe program starts the job's own command line as a child, in the container, with a Nessie address that accepts a connection and never answers, a deadline
of 2 seconds and a Nessie timeout of 100 seconds. Only the job's own timer can then stop it, with exit 124. The program keeps the child's standard error in
memory, compares all of it with exactly ``silver_cases: timeout`` and a newline, and writes one JSON flag. Nothing of the stream leaves the program. The runner
is untracked, so each test skips when it is not on this machine. The program runs here on the host as it runs in the container (the job reads its settings and
starts its timer before its first call to Nessie, and needs no Spark for that), so no Docker runs. The planted jobs are small programs at the job's module path
in a folder of their own.

Failure modes covered here:

- The probe set ``JOB_DEADLINE_SECONDS=1`` and asked a live Nessie, so whether the deadline or an answer came first depended on the speed of the catalog.
- A job whose timer was removed or does not start still passed the probe, because the catalog answered before the deadline or the call failed on its own.
- The job stopped at its deadline with a line other than ``silver_cases: timeout``, or with more lines than that one. A warning in the catalog call, a line with
  no prefix, a Python exception-group traceback and a stop line of another job beside the stop line all passed a check on the job's prefix and tracebacks.
- A text of the child's standard error left the container (on the program's own streams or in the flag).
- The program ended with no flag file, with a flag that was not the closed document, or with an exit code other than the child's, and the runner could not tell.
- The program started the child with ``subprocess.run`` and let it inherit the program's standard input. It starts the child through the project's launcher
  (``scripts.process.run_command``, which the image holds), with standard input closed, and keeps every line ending of the stream (a carriage return stays).
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from tests.e2e.run_record import ROOT
from tests.fast.test_runner_gold_sealed import runner

ENVIRONMENT = {"WAREHOUSE_LOCATION": "file:///warehouse/", "AWS_PROFILE": "no-such-profile", "AWS_CONFIG_FILE": os.devnull}
LIMIT_SECONDS = 30


def on_host(
    program: str, folder: Path, environment: dict[str, str], seconds: float, *, cwd: Path = ROOT, input_text: str | None = None
) -> CompletedProcess[str]:
    """The program of the probe, run as ``python -c`` from ``cwd`` (the repository root) with the job's result file in ``folder`` and its flag file in
    ``folder / "probe"`` (the container mounts ``/result`` and ``/probe``)."""
    (folder / "probe").mkdir(exist_ok=True)
    program = program.replace("/result/result.json", str(folder / "result.json")).replace("/probe/result.json", str(folder / "probe" / "result.json"))
    env = {"PATH": os.environ.get("PATH", ""), **ENVIRONMENT, **environment}
    return run_command(sys.executable, ["-c", program], timeout=seconds, cwd=cwd, env=env, input_text=input_text)


PLANTED_JOB = """
import os, sys, time
if os.environ.get("PLANT_STDIN"):
    os.environ["PLANT_STDERR"] = "silver_cases: timeout\\n" if sys.stdin.read() == "" else "silver_cases: stdin was open\\n"
sys.stderr.buffer.write(os.environ["PLANT_STDERR"].encode())
sys.stderr.flush()
time.sleep(float(os.environ.get("PLANT_SLEEP", "0")))
sys.exit(int(os.environ["PLANT_EXIT"]))
"""
STOP = "silver_cases: timeout\n"
EXCEPTION_GROUP = (
    "  + Exception Group Traceback (most recent call last):\n"
    '  |   File "/app/spark/jobs/silver_cases.py", line 9, in <module>\n'
    "  | ExceptionGroup: catalog (1 sub-exception)\n"
    "  +-+---------------- 1 ----------------\n"
    "    | RuntimeError: catalog failed\n"
    "    +------------------------------------\n"
)


def planted_tree(folder: Path) -> Path:
    """A folder with a ``spark.jobs.silver_cases`` module that writes what the environment says (``PLANT_STDERR``) and exits with ``PLANT_EXIT``, and with the
    launcher that the image holds at the same path (``scripts/process.py``), as the program imports it."""
    (folder / "scripts").mkdir(parents=True, exist_ok=True)
    for name in ("__init__.py", "process.py"):
        shutil.copy(ROOT / "scripts" / name, folder / "scripts" / name)
    for package in ("spark", "spark/jobs"):
        (folder / package).mkdir(parents=True, exist_ok=True)
        (folder / package / "__init__.py").write_text("")
    (folder / "spark" / "jobs" / "silver_cases.py").write_text(textwrap.dedent(PLANTED_JOB))
    return folder


class StalledCatalog(unittest.TestCase):
    def test_the_job_stops_itself_at_its_deadline_with_exit_124_and_no_result_file_and_the_program_flags_the_exact_stop_line(self) -> None:
        module = runner("silver_cases")
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder:
            started = time.monotonic()
            done = on_host(module.STALL, Path(folder), module.STALL_ENV, LIMIT_SECONDS)
            seconds = time.monotonic() - started
            self.assertEqual(124, done.returncode, done.stderr)
            self.assertEqual("silver_cases: timeout", module.DEADLINE_LINE)
            self.assertEqual({"stderr_is_stop_line": True, "exit": 124}, json.loads((Path(folder) / "probe" / "result.json").read_text()))
            self.assertEqual([], [path.name for path in Path(folder).iterdir() if path.name != "probe"])
            self.assertLess(seconds, LIMIT_SECONDS / 2)

    def test_the_program_writes_nothing_of_the_childs_standard_error_to_its_own_streams(self) -> None:
        module = runner("silver_cases")
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder:
            done = on_host(module.STALL, Path(folder), module.STALL_ENV, LIMIT_SECONDS)
            self.assertEqual("", done.stderr)
            self.assertEqual("", done.stdout)

    def test_the_same_program_without_a_timer_that_fires_is_still_waiting_after_the_deadline_has_passed(self) -> None:
        """The probe is only a proof of the timer when the Nessie timeout (100 seconds) is longer than the wait: a job without its timer waits for it.
        The wrapper's own limit (4 seconds) stops that job inside the host limit (8 seconds), so the test leaves no job running: the wrapper exits 1 with
        a false flag and the job writes no final result."""
        module = runner("silver_cases")
        self.assertEqual("2", module.STALL_ENV["JOB_DEADLINE_SECONDS"])
        self.assertGreaterEqual(float(module.STALL_ENV["NESSIE_TIMEOUT_SECONDS"]), 100)
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder:
            done = on_host(module.STALL, Path(folder), {**module.STALL_ENV, "JOB_DEADLINE_SECONDS": "2000", "PROBE_SECONDS": "4"}, 8)
            self.assertEqual(1, done.returncode)
            self.assertEqual({"stderr_is_stop_line": False, "exit": 1}, json.loads((Path(folder) / "probe" / "result.json").read_text()))
            # The stopped job never writes its result; only the name it reserved at its start (``result.json.part``) may stay.
            self.assertFalse((Path(folder) / "result.json").exists())


class PlantedJobs(unittest.TestCase):
    """The program on jobs that write what the test says: only the exact stop line, and nothing else, is flagged."""

    def run_planted(
        self, stderr: str, code: int, *, sleep: float = 0, limit: str | None = None, stdin_text: str | None = None
    ) -> tuple[CompletedProcess[str], dict[str, object] | None]:
        module = runner("silver_cases")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="planted_job_")))
        tree = planted_tree(folder / "tree")
        environment = {
            "PLANT_STDERR": stderr,
            "PLANT_EXIT": str(code),
            "PLANT_SLEEP": str(sleep),
            **({"PLANT_STDIN": "1"} if stdin_text is not None else {}),
            **({"PROBE_SECONDS": limit} if limit else {}),
        }
        done = on_host(module.STALL, folder, environment, LIMIT_SECONDS, cwd=tree, input_text=stdin_text)
        flag = folder / "probe" / "result.json"
        return done, (json.loads(flag.read_text()) if flag.exists() else None)

    def test_the_exact_stop_line_is_flagged_true_with_the_childs_exit_code(self) -> None:
        done, flag = self.run_planted(STOP, 124)
        self.assertEqual({"stderr_is_stop_line": True, "exit": 124}, flag)
        self.assertEqual(124, done.returncode)

    def test_each_planted_stream_that_is_not_exactly_the_stop_line_is_flagged_false(self) -> None:
        for label, stderr in (
            ("a warning in the catalog call before the line", "WARNING: catalog call is slow\n" + STOP),
            ("a warning after the line", STOP + "WARNING: catalog call is slow\n"),
            ("a line with no prefix", "something went wrong\n" + STOP),
            ("an exception-group traceback before the line", EXCEPTION_GROUP + STOP),
            ("an exception-group traceback after the line", STOP + EXCEPTION_GROUP),
            ("a traceback", "Traceback (most recent call last):\n  File x\nValueError: x\n" + STOP),
            ("the stop line twice", STOP + STOP),
            ("a stop line of another job", "silver_labels: timeout\n" + STOP),
            ("another line with the job's prefix", "silver_cases: failed\n" + STOP),
            ("no newline at the end", "silver_cases: timeout"),
            ("a carriage return", "silver_cases: timeout\r\n"),
            ("a space after the line", "silver_cases: timeout \n"),
            ("a blank line after the line", STOP + "\n"),
            ("a blank line before the line", "\n" + STOP),
            ("nothing", ""),
            ("another line only", "silver_cases: failed\n"),
        ):
            with self.subTest(label):
                done, flag = self.run_planted(stderr, 124)
                self.assertEqual({"stderr_is_stop_line": False, "exit": 124}, flag)
                self.assertEqual(124, done.returncode)

    def test_the_child_gets_a_closed_standard_input_whatever_the_program_was_given(self) -> None:
        """The program's own standard input here is a pipe that holds text. A child that inherited it would read that text, and flag false."""
        done, flag = self.run_planted(STOP, 124, stdin_text="text on the program's standard input")
        self.assertEqual({"stderr_is_stop_line": True, "exit": 124}, flag)
        self.assertEqual(124, done.returncode)

    def test_the_program_starts_the_child_with_the_projects_launcher_and_not_with_subprocess(self) -> None:
        module = runner("silver_cases")
        self.assertIn("from scripts.process import TimeoutExpired, run_command", module.STALL)
        self.assertIn("keep_newlines=True", module.STALL)
        self.assertNotIn("subprocess", module.STALL)

    def test_the_exit_code_is_the_childs_and_the_program_ends_with_it(self) -> None:
        for code in (0, 1, 2, 124):
            with self.subTest(code):
                done, flag = self.run_planted(STOP, code)
                self.assertEqual({"stderr_is_stop_line": True, "exit": code}, flag)
                self.assertEqual(code, done.returncode)

    def test_nothing_of_a_planted_stream_leaves_the_program_or_the_flag(self) -> None:
        planted = "planted_marker_text_of_the_catalog_call"
        done, flag = self.run_planted(f"{planted}\n" + STOP, 124)
        self.assertNotIn(planted, done.stdout + done.stderr + json.dumps(flag))
        self.assertEqual("", done.stderr)

    def test_a_child_that_outlasts_the_programs_own_limit_is_stopped_and_flagged_false_with_exit_1(self) -> None:
        done, flag = self.run_planted(STOP, 124, sleep=30, limit="2")
        self.assertEqual({"stderr_is_stop_line": False, "exit": 1}, flag)
        self.assertEqual(1, done.returncode)

    def test_a_flag_folder_that_cannot_be_written_fails_the_program_with_no_flag_file(self) -> None:
        module = runner("silver_cases")
        with tempfile.TemporaryDirectory(prefix="deadline_probe_") as folder:
            program = module.STALL.replace("/probe/result.json", str(Path(folder) / "no_such_folder" / "result.json"))
            done = on_host(program, Path(folder), module.STALL_ENV, LIMIT_SECONDS)
            self.assertNotEqual(0, done.returncode)
            self.assertNotEqual(124, done.returncode)
            self.assertFalse((Path(folder) / "no_such_folder").exists())
            self.assertEqual([], list((Path(folder) / "probe").iterdir()))


if __name__ == "__main__":
    unittest.main()
