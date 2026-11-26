"""The manual one-off of a Spark job goes through the one Spark slot: ``python -m scripts.stack.spark_run <job> [arguments]``.

The command runs in the code location container (``docker compose exec dagster_code_location python -m scripts.stack.spark_run ...``), where the table checks
one-off runs too. It starts the job as ``docker compose run`` in the service and profile of the job's workload (``scripts/stack/job_workloads.py``), through
the service's own entry point, with the container name that is the one Spark slot (``spark_slot.SLOT_NAME``), and holds the slot as long as the container
exists. The tests run with ``tests/fast/spark_engine.py``, a stand-in for the Docker engine.
"""

from __future__ import annotations

import contextlib
import functools
import io
import unittest
from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

from scripts.stack import groups, spark_run
from scripts.stack import spark_slot as slot
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, profile_of_job, service_of
from scripts.stack.limits import LIMITS_FILE, load_limits
from tests.fast.spark_engine import CONFLICT, OWNER_LABEL, SLOT_NAME, Box, Engine, finishes
from tests.fast.test_spark_slot import killed

ROOT = Path(__file__).resolve().parents[2]


class SparkRunTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = Engine()
        self.enterContext(mock.patch.object(slot, "run_command", self.engine.run_command))
        self.output = io.StringIO()
        self.error = io.StringIO()

    def go(self, argv: list[str], running: Callable[[float], list[str]] = lambda _: []) -> int:
        with contextlib.redirect_stdout(self.output), contextlib.redirect_stderr(self.error):
            try:
                return spark_run.main(argv, running=running)
            except SystemExit as stop:  # the parser's usage refusal
                return int(stop.code or 0) if not isinstance(stop.code, str) else 2

    def test_the_command_starts_the_job_in_its_workloads_service_and_profile_with_its_own_arguments(self) -> None:
        self.engine.on_start = finishes(0, "job output\n")
        self.assertEqual(0, self.go(["table_maintenance", "--dry-run"]))
        (launch,) = self.engine.launches()
        text = " ".join(launch)
        self.assertIn("--profile batch_maintenance run --no-TTY --no-deps", text)
        self.assertTrue(text.endswith("spark_maintenance table_maintenance --dry-run"), text)
        self.assertNotIn("--entrypoint", text)
        self.assertEqual("job output\n", self.output.getvalue())

    def test_the_container_has_the_slots_name_and_an_owner_label_before_the_service(self) -> None:
        self.go(["table_maintenance"])
        (launch,) = self.engine.launches()
        self.assertEqual(SLOT_NAME, launch[launch.index("--name") + 1])
        self.assertTrue(launch[launch.index("--label") + 1].startswith(f"{OWNER_LABEL}="))
        self.assertLess(launch.index("--label"), launch.index("spark_maintenance"))

    def test_every_job_with_a_service_runs_in_that_service_and_the_environment_carries_its_memory_limit(self) -> None:
        for job, workload in JOB_WORKLOADS.items():
            if workload not in SERVICE_WORKLOADS:
                continue
            with self.subTest(job=job), mock.patch("scripts.stack.spark_run.run_in_slot", wraps=slot.run_in_slot) as started:
                self.go([job, "--x", "y"])
                launch = self.engine.launches()[-1]
                self.assertEqual([service_of(job), job, "--x", "y"], launch[-4:])
                self.assertEqual(profile_of_job(job), launch[launch.index("--profile") + 1])
                limit = f"MEM_LIMIT_SPARK_{workload.upper()}"
                self.assertEqual(groups.rendered_environment(load_limits(LIMITS_FILE))[limit], started.call_args.kwargs["env"][limit])

    def test_the_container_exit_code_is_the_exit_code_and_its_error_output_is_written(self) -> None:
        self.engine.on_start = finishes(3, "", "the job failed\n")
        self.assertEqual(3, self.go(["table_maintenance"]))
        self.assertEqual("the job failed\n", self.error.getvalue())

    def test_a_job_that_exits_1_with_the_refusal_words_after_the_wait_keeps_its_own_output_and_is_not_reported_as_refused(self) -> None:
        exact = f"{CONFLICT.format(name=SLOT_NAME, other='f' * 64)}\n"
        now = [0.0]

        def container(engine: Engine, box: Box) -> CompletedProcess[str]:
            now[0] = 5.0
            return CompletedProcess([], 1, "", exact)

        self.engine.on_start = container
        with mock.patch.object(spark_run, "run_in_slot", functools.partial(slot.run_in_slot, clock=lambda: now[0])):
            self.assertEqual(1, self.go(["--wait-seconds", "1", "table_maintenance"]))
        self.assertEqual(exact, self.error.getvalue())
        self.assertEqual(({}, 1), (self.engine.boxes, len(self.engine.launches())))

    def test_a_job_that_is_unknown_or_has_no_service_or_is_missing_is_refused_with_status_2_and_no_docker_call(self) -> None:
        for argv in (["no_such_job"], ["stream_features"], []):
            with self.subTest(argv=argv):
                self.assertEqual(2, self.go(argv))
        self.assertEqual([], self.engine.calls)

    def test_a_running_spark_service_refuses_the_start_with_its_name_and_no_container_is_made(self) -> None:
        self.assertEqual(1, self.go(["table_maintenance"], running=lambda _: ["spark_silver", "nessie"]))
        self.assertIn("spark_silver", self.error.getvalue())
        self.assertEqual([], self.engine.launches())

    def test_a_slot_container_of_another_launch_refuses_the_start_and_is_left_alone(self) -> None:
        for state in ("running", "paused", "exited"):
            engine = Engine()
            engine.hold(state=state)
            with self.subTest(state=state), mock.patch.object(slot, "run_command", engine.run_command):
                self.assertEqual(1, self.go(["table_maintenance"]))
            self.assertIn(SLOT_NAME, self.error.getvalue())
            self.assertEqual([], engine.launches())
            self.assertEqual(1, len(engine.boxes))

    def test_the_slot_is_held_while_the_container_runs_and_free_after_every_ending(self) -> None:
        inner: list[int] = []

        def container(engine: Engine, box: Box) -> CompletedProcess[str]:
            inner.append(self.go(["table_maintenance"]))
            return CompletedProcess([], 0, "", "")

        self.engine.on_start = container
        self.assertEqual(0, self.go(["table_maintenance"]))
        self.assertEqual([1], inner)
        for effect in (killed, finishes(5)):
            self.engine.on_start = effect
            self.go(["table_maintenance"])
            self.assertEqual({}, self.engine.boxes)

    def test_a_job_that_passes_its_deadline_is_status_124_and_its_container_is_gone(self) -> None:
        self.engine.on_start = killed
        self.assertEqual(124, self.go(["table_maintenance"]))
        self.assertEqual({}, self.engine.boxes)
        self.assertIn("passed its deadline", self.error.getvalue())

    def test_a_container_that_cannot_be_removed_after_the_deadline_is_a_refusal_with_status_1(self) -> None:
        self.engine.on_start = killed
        self.engine.removal_fails = True
        self.assertEqual(1, self.go(["table_maintenance"]))
        self.assertIn("still there", self.error.getvalue())

    def test_the_wait_and_the_deadline_options_reach_the_slot_and_the_container(self) -> None:
        with mock.patch("scripts.stack.spark_run.run_in_slot", wraps=slot.run_in_slot) as claimed:
            self.go(["--wait-seconds", "7", "--deadline-seconds", "99", "table_maintenance", "--dry-run"])
        self.assertEqual(7, claimed.call_args.kwargs["wait_seconds"])
        self.assertEqual(99, claimed.call_args.kwargs["deadline_seconds"])
        self.assertEqual(["table_maintenance", "--dry-run"], self.engine.launches()[0][-2:])

    def test_the_command_has_a_default_wait_of_none_and_a_default_deadline(self) -> None:
        with mock.patch("scripts.stack.spark_run.run_in_slot", wraps=slot.run_in_slot) as claimed:
            self.go(["table_maintenance"])
        self.assertEqual(0, claimed.call_args.kwargs["wait_seconds"])
        self.assertEqual(spark_run.DEADLINE_SECONDS, claimed.call_args.kwargs["deadline_seconds"])

    def test_a_wait_or_a_deadline_that_is_not_a_finite_number_is_a_usage_error_with_status_2_and_no_docker_call(self) -> None:
        for option, value in (
            ("--wait-seconds", "nan"),
            ("--wait-seconds", "inf"),
            ("--wait-seconds", "-inf"),
            ("--wait-seconds", "-1"),
            ("--deadline-seconds", "nan"),
            ("--deadline-seconds", "inf"),
            ("--deadline-seconds", "0"),
            ("--deadline-seconds", "-5"),
        ):
            with self.subTest(option=option, value=value):
                self.assertEqual(2, self.go([option, value, "table_maintenance"]))
        self.assertEqual([], self.engine.calls)
        self.assertNotIn("nan", self.error.getvalue())


if __name__ == "__main__":
    unittest.main()
