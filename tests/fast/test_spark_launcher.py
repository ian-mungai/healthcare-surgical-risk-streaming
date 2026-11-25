"""A Spark job service runs a job only when the job's module is mapped to the service's workload, and its default command runs nothing.

Each of the four Spark services has the same entry point, ``python -m spark.launcher``, and the environment variable ``SPARK_WORKLOAD`` that names its workload.
The launcher refuses a job that ``scripts/stack/job_workloads.py`` does not map to that workload, before Spark starts, so a job cannot run under the identity
of another layer by a typed service name. With no job it prints its usage and exits 2: ``make group-up GROUP=batch_maintenance`` starts the service and no
rewrite. The checks run the real launcher in a child process, with no Spark in it, so a refusal that came too late would fail on the missing Spark.
"""

from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

from scripts.process import run_command
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, job_command, refusal_of, service_of
from spark import launcher
from tests.fast.test_spark_service import service

ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = ["python", "-m", "spark.launcher"]


def launch(*arguments: str, workload: str | None = None) -> CompletedProcess[str]:
    """The real launcher in a child process. The child has no Spark, so a job module that started would fail by an exit status other than 2 or by an import."""
    environment = {key: value for key, value in os.environ.items() if key != launcher.WORKLOAD_VARIABLE}
    if workload is not None:
        environment[launcher.WORKLOAD_VARIABLE] = workload
    return run_command(sys.executable, ["-m", "spark.launcher", *arguments], timeout=60, cwd=ROOT, env=environment)


class RefusalTest(unittest.TestCase):
    def test_every_job_is_accepted_by_its_own_workload_and_by_no_other(self) -> None:
        for job, owner in JOB_WORKLOADS.items():
            for workload in ("bronze", "silver", "gold", "maintenance", "stream"):
                with self.subTest(job=job, workload=workload):
                    self.assertEqual(None if workload == owner else "wrong_workload", refusal_of(job, workload))

    def test_a_job_that_is_not_mapped_a_missing_workload_and_a_job_that_is_not_a_name_are_refused(self) -> None:
        self.assertEqual("unknown_job", refusal_of("no_such_job", "bronze"))
        self.assertEqual("unknown_job", refusal_of("../jobs/silver_signals", "silver"))
        self.assertEqual("unknown_job", refusal_of("silver_signals.py", "silver"))
        self.assertEqual("no_workload", refusal_of("silver_signals", None))
        self.assertEqual("no_workload", refusal_of("silver_signals", ""))

    def test_the_reassignment_that_the_review_planted_is_refused_by_the_maintenance_service(self) -> None:
        self.assertEqual("wrong_workload", refusal_of("silver_signals", "maintenance"))
        self.assertEqual("wrong_workload", refusal_of("bronze_decode", "maintenance"))


class LauncherTest(unittest.TestCase):
    def test_with_no_job_it_prints_its_usage_runs_nothing_and_exits_2(self) -> None:
        with mock.patch("runpy.run_module") as started, redirect_stderr(io.StringIO()) as error:
            status = launcher.main([], {launcher.WORKLOAD_VARIABLE: "maintenance"})
        self.assertEqual(2, status)
        self.assertEqual("spark_launcher: usage\n", error.getvalue())
        started.assert_not_called()

    def test_a_mapped_job_runs_its_module_as_the_main_program_with_its_own_arguments(self) -> None:
        with mock.patch("runpy.run_module") as started, mock.patch("sys.argv", ["launcher"]):
            status = launcher.main(["table_maintenance", "--dry-run"], {launcher.WORKLOAD_VARIABLE: "maintenance"})
            argv = list(sys.argv)
        self.assertEqual(0, status)
        started.assert_called_once_with("spark.jobs.table_maintenance", run_name="__main__", alter_sys=True)
        self.assertEqual(["spark.jobs.table_maintenance", "--dry-run"], argv)

    def test_a_job_of_another_workload_is_refused_before_its_module_is_loaded_with_a_fixed_code_that_does_not_echo_the_job(self) -> None:
        with mock.patch("runpy.run_module") as started, redirect_stderr(io.StringIO()) as error:
            status = launcher.main(["silver_signals", "--branch", "secret_branch"], {launcher.WORKLOAD_VARIABLE: "maintenance"})
        self.assertEqual(2, status)
        self.assertEqual("spark_launcher: wrong_workload\n", error.getvalue())
        started.assert_not_called()

    def test_a_service_with_no_workload_runs_no_job(self) -> None:
        with mock.patch("runpy.run_module") as started, redirect_stderr(io.StringIO()) as error:
            status = launcher.main(["silver_signals"], {})
        self.assertEqual((2, "spark_launcher: no_workload\n"), (status, error.getvalue()))
        started.assert_not_called()


class ChildProcessTest(unittest.TestCase):
    def test_the_default_command_of_each_service_prints_usage_and_exits_2_and_starts_nothing(self) -> None:
        for workload in SERVICE_WORKLOADS:
            with self.subTest(workload=workload):
                done = launch(workload=workload)
                self.assertEqual((2, "", "spark_launcher: usage\n"), (done.returncode, done.stdout, done.stderr))

    def test_the_planted_reassignment_a_silver_job_in_the_maintenance_service_exits_2_before_spark_starts(self) -> None:
        done = launch("silver_signals", "--branch", "x", workload="maintenance")
        self.assertEqual((2, "", "spark_launcher: wrong_workload\n"), (done.returncode, done.stdout, done.stderr))

    def test_a_job_with_no_service_workload_exits_2(self) -> None:
        done = launch("silver_signals")
        self.assertEqual((2, "spark_launcher: no_workload\n"), (done.returncode, done.stderr))


class ServiceFilesTest(unittest.TestCase):
    def test_every_service_has_the_one_entry_point_names_its_workload_and_sets_no_command(self) -> None:
        for workload in SERVICE_WORKLOADS:
            with self.subTest(workload=workload):
                job = service(f"spark_{workload}")
                self.assertEqual(ENTRYPOINT, job["entrypoint"])
                self.assertEqual(workload, job["environment"]["SPARK_WORKLOAD"])
                self.assertNotIn("command", job)

    def test_the_image_holds_the_launcher_and_the_map_it_reads(self) -> None:
        text = (ROOT / "spark" / "Dockerfile").read_text()
        self.assertIn("COPY scripts/stack/job_workloads.py /opt/jobs/scripts/stack/job_workloads.py", text)
        self.assertIn("COPY spark /opt/jobs/spark", text)
        self.assertTrue((ROOT / "spark" / "launcher.py").is_file())

    def test_a_launcher_names_the_job_after_the_service_and_never_overrides_the_entry_point(self) -> None:
        self.assertEqual(["spark_silver", "silver_reconcile", "--branch", "x"], job_command("silver_reconcile", "--branch", "x"))
        self.assertEqual("spark_silver", service_of("silver_reconcile"))


if __name__ == "__main__":
    unittest.main()
