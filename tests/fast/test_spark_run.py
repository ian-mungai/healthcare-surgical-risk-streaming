"""The manual one-off of a Spark job goes through the one Spark slot: ``python -m scripts.stack.spark_run <job> [arguments]``.

The command runs in the code location container (``docker compose exec dagster_code_location python -m scripts.stack.spark_run ...``), where the table checks
one-off runs too, so both take the one lock file of the slot. It starts the job as ``docker compose run`` in the service and profile of the job's workload
(``scripts/stack/job_workloads.py``), through the service's own entry point, and holds the slot until the container exits. The tests run with stand-ins for
Docker and use a real file for the lock.
"""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

from scripts.process import TimeoutExpired
from scripts.stack import groups, spark_run
from scripts.stack import spark_slot as slot
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, profile_of_job, service_of
from scripts.stack.limits import LIMITS_FILE, load_limits
from scripts.stack.spark_slot import SlotBusy, spark_slot

ROOT = Path(__file__).resolve().parents[2]


class SparkRunTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="spark_run_"))) / "slot.lock"
        self.enterContext(mock.patch.object(slot, "LOCK_FILE", self.lock))
        self.output = io.StringIO()
        self.error = io.StringIO()

    def go(
        self, argv: list[str], running: Callable[[], list[str]] = lambda: [], reply: CompletedProcess[str] | Exception | None = None
    ) -> tuple[int, mock.Mock]:
        effect = reply if reply is not None else CompletedProcess([], 0, "job output\n", "")
        patch = mock.patch.object(spark_run, "run_command", **({"side_effect": effect} if isinstance(effect, Exception) else {"return_value": effect}))
        with (
            patch as started,
            contextlib.redirect_stdout(self.output),
            contextlib.redirect_stderr(self.error),
        ):
            try:
                return spark_run.main(argv, running=running), started
            except SystemExit as stop:  # the parser's usage refusal
                return int(stop.code or 0), started

    def test_the_command_starts_the_job_in_its_workloads_service_and_profile_with_its_own_arguments(self) -> None:
        status, started = self.go(["table_maintenance", "--dry-run"])
        self.assertEqual(0, status)
        program, arguments = started.call_args.args
        text = " ".join(arguments)
        self.assertEqual("docker", program)
        self.assertIn("--profile batch_maintenance run --rm --no-TTY --no-deps", text)
        self.assertTrue(text.endswith("spark_maintenance table_maintenance --dry-run"), text)
        self.assertNotIn("--entrypoint", text)
        self.assertEqual("job output\n", self.output.getvalue())

    def test_every_job_with_a_service_runs_in_that_service_and_the_environment_carries_its_memory_limit(self) -> None:
        for job, workload in JOB_WORKLOADS.items():
            if workload not in SERVICE_WORKLOADS:
                continue
            with self.subTest(job=job):
                _, started = self.go([job, "--x", "y"])
                arguments = started.call_args.args[1]
                self.assertEqual([service_of(job), job, "--x", "y"], arguments[-4:])
                self.assertEqual(profile_of_job(job), arguments[arguments.index("--profile") + 1])
                limit = f"MEM_LIMIT_SPARK_{workload.upper()}"
                self.assertEqual(groups.rendered_environment(load_limits(LIMITS_FILE))[limit], started.call_args.kwargs["env"][limit])

    def test_the_container_exit_code_is_the_exit_code_and_its_error_output_is_written(self) -> None:
        status, _ = self.go(["table_maintenance"], reply=CompletedProcess([], 3, "", "the job failed\n"))
        self.assertEqual(3, status)
        self.assertEqual("the job failed\n", self.error.getvalue())

    def test_a_job_that_is_unknown_or_has_no_service_or_is_missing_is_refused_with_status_2_and_no_container(self) -> None:
        for argv in (["no_such_job"], ["stream_features"], []):
            with self.subTest(argv=argv):
                status, started = self.go(argv)
                self.assertEqual(2, status)
                started.assert_not_called()
        self.assertFalse(self.lock.exists())

    def test_a_running_spark_service_refuses_the_start_with_its_name_and_no_container_is_made(self) -> None:
        status, started = self.go(["table_maintenance"], running=lambda: ["spark_silver", "nessie"])
        self.assertEqual(1, status)
        self.assertIn("spark_silver", self.error.getvalue())
        started.assert_not_called()

    def test_the_slot_is_held_while_the_container_runs_and_released_after_every_ending(self) -> None:
        def container(*_: object, **__: object) -> CompletedProcess[str]:
            with self.assertRaises(SlotBusy), spark_slot(lambda: []):
                pass
            return CompletedProcess([], 0, "", "")

        for effect in (container, TimeoutExpired("docker", 5), RuntimeError("the program failed")):
            with self.subTest(effect=repr(effect)), mock.patch.object(spark_run, "run_command", side_effect=effect):
                try:
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        status = spark_run.main(["table_maintenance"], running=lambda: [])
                    self.assertEqual(124 if isinstance(effect, TimeoutExpired) else 0, status)
                except RuntimeError:
                    pass
                with spark_slot(lambda: []):
                    pass

    def test_a_lock_that_cannot_be_opened_is_a_refusal_with_status_1_and_no_container(self) -> None:
        with mock.patch.object(slot, "LOCK_FILE", self.lock.parent / "no_such_folder" / "slot.lock"):
            status, started = self.go(["table_maintenance"])
        self.assertEqual(1, status)
        self.assertIn("no_such_folder", self.error.getvalue())
        started.assert_not_called()

    def test_the_wait_and_the_deadline_options_reach_the_slot_and_the_container(self) -> None:
        with mock.patch("scripts.stack.spark_run.spark_slot", wraps=spark_slot) as claimed:
            _, started = self.go(["--wait-seconds", "7", "--deadline-seconds", "99", "table_maintenance", "--dry-run"])
        self.assertEqual(7, claimed.call_args.kwargs["wait_seconds"])
        self.assertEqual(99, started.call_args.kwargs["timeout"])
        self.assertEqual(["table_maintenance", "--dry-run"], started.call_args.args[1][-2:])

    def test_the_command_has_a_default_wait_of_none_and_a_default_deadline(self) -> None:
        with mock.patch("scripts.stack.spark_run.spark_slot", wraps=spark_slot) as claimed:
            _, started = self.go(["table_maintenance"])
        self.assertEqual(0, claimed.call_args.kwargs["wait_seconds"])
        self.assertEqual(spark_run.DEADLINE_SECONDS, started.call_args.kwargs["timeout"])


if __name__ == "__main__":
    unittest.main()
