"""One Spark job runs at a time, across all four Spark groups and every way a job starts, and the slot is held until the job's container exits.

The memory figure of the host assumes one Spark job's limit (``infra/heavy_memory.tf``), and each Spark service is its own group, so the memory check of
one group does not see another group's job. The slot is the missing rule. It is judged in three places. The admission decision makes a Spark group wait
while any Spark service runs, also a second job of the same service. The group start (``start_group``) claims the slot, under a file lock, before it admits
and starts. The one-off launch of the table checks (``compose_runner``) holds the slot for the whole ``docker compose run``. The Dagster runs of the Spark
groups share one tag that ``orchestration/dagster.yaml`` limits to one run. The tests run with stand-ins for Docker and use real files for the lock.
"""

from __future__ import annotations

import tempfile
import unittest
from collections.abc import Collection
from contextlib import AbstractContextManager
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest import mock

from orchestration.surgical_risk.concurrency import JOB_GROUPS, SPARK_TAG_KEY, SPARK_TAG_VALUE, heavy_job_tags, job_tags
from scripts.ingestion import table_checks as run
from scripts.process import TimeoutExpired
from scripts.stack import groups
from scripts.stack.admission import decide, describe
from scripts.stack.job_workloads import SPARK_GROUPS, SPARK_SERVICES
from scripts.stack.limits import LIMITS_FILE, load_limits
from scripts.stack.spark_slot import SlotBusy, spark_holders, spark_slot
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
PLENTY = 10**6


class AdmissionTest(unittest.TestCase):
    """The decision refuses a Spark group while any Spark service runs, however much memory is free."""

    def decision(self, group: str, running: Collection[str]) -> tuple[bool, tuple[str, ...]]:
        found = decide(load_limits(LIMITS_FILE), group, running, PLENTY)
        return found.admitted, found.slot_holders

    def test_a_group_of_each_workload_waits_while_a_job_of_every_other_workload_runs(self) -> None:
        for group in SPARK_GROUPS:
            for holder in SPARK_SERVICES:
                with self.subTest(group=group, holder=holder):
                    self.assertEqual((False, (holder,)), self.decision(group, {holder}))

    def test_silver_waits_while_bronze_runs(self) -> None:
        self.assertEqual((False, ("spark_bronze",)), self.decision("batch_silver", {"spark_bronze", "nessie"}))

    def test_a_group_is_admitted_when_no_spark_service_runs(self) -> None:
        for group in SPARK_GROUPS:
            with self.subTest(group=group):
                self.assertEqual((True, ()), self.decision(group, {"nessie", "postgres"}))

    def test_a_group_that_is_not_a_spark_group_ignores_the_slot(self) -> None:
        self.assertEqual((True, ()), self.decision("ingestion", {"spark_bronze"}))
        self.assertEqual((True, ()), self.decision("supporting", {"spark_bronze"}))

    def test_the_message_names_the_slot_and_every_holder(self) -> None:
        text = describe(decide(load_limits(LIMITS_FILE), "batch_gold", {"spark_bronze", "spark_silver"}, PLENTY))
        self.assertIn("waits", text)
        self.assertIn("Spark slot", text)
        self.assertIn("spark_bronze, spark_silver", text)

    def test_every_spark_group_is_a_group_of_the_limits_file_with_one_spark_service(self) -> None:
        limits = load_limits(LIMITS_FILE).services
        for group, service in zip(SPARK_GROUPS, SPARK_SERVICES, strict=True):
            self.assertEqual(group, limits[service].group)


class SlotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="spark_slot_"))) / "slot.lock"
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def claim(self, running: list[str], wait: float = 0) -> AbstractContextManager[None]:
        return spark_slot(lambda: running, wait_seconds=wait, lock_file=self.lock, clock=self.clock, sleep=self.sleep)

    def test_holders_are_the_spark_services_that_run_in_name_order(self) -> None:
        self.assertEqual(("spark_bronze", "spark_gold"), spark_holders({"spark_gold", "nessie", "spark_bronze"}))
        self.assertEqual((), spark_holders({"nessie"}))

    def test_a_free_slot_is_claimed_and_released_when_the_work_ends(self) -> None:
        with self.claim([]):
            pass
        with self.claim([]):
            pass

    def test_a_slot_held_by_a_running_job_refuses_with_its_name_when_there_is_no_wait(self) -> None:
        with self.assertRaisesRegex(SlotBusy, "spark_bronze"), self.claim(["spark_bronze"]):
            self.fail("the body must not run")

    def test_a_second_claim_while_the_first_is_held_is_refused_and_the_lock_is_released_after_an_error(self) -> None:
        with self.claim([]), self.assertRaisesRegex(SlotBusy, "another launcher"), self.claim([]):
            self.fail("the body must not run")
        with self.assertRaises(RuntimeError), self.claim([]):
            raise RuntimeError("the job failed")
        with self.claim([]):
            pass

    def test_a_claim_waits_until_the_running_job_has_ended_and_not_longer_than_its_deadline(self) -> None:
        running = ["spark_silver"]
        calls = []

        def view() -> list[str]:
            calls.append(self.now)
            return list(running) if self.now < 30 else []

        with spark_slot(view, wait_seconds=60, lock_file=self.lock, clock=self.clock, sleep=self.sleep, poll_seconds=10):
            pass
        self.assertEqual([0.0, 10.0, 20.0, 30.0], calls)
        self.now = 0
        with (
            self.assertRaises(SlotBusy),
            spark_slot(lambda: ["spark_silver"], wait_seconds=25, lock_file=self.lock, clock=self.clock, sleep=self.sleep, poll_seconds=10),
        ):
            pass
        self.assertGreaterEqual(self.now, 25)


class GroupStartTest(unittest.TestCase):
    """``start_group`` for a Spark group claims the slot before it starts anything."""

    def setUp(self) -> None:
        self.lock = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="spark_slot_"))) / "slot.lock"
        self.settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example", available_mib=PLENTY)

    def start(self, group: str, running: set[str], docker: mock.Mock) -> groups.StartResult:
        states = {name: groups.ServiceState(name, "running", "none", 0, False, 0) for name in running}
        with (
            mock.patch.object(groups, "docker", docker),
            mock.patch.object(groups, "read_states", return_value=states),
            mock.patch.object(groups, "wait_ready"),
            mock.patch.dict("os.environ", {"SPARK_SLOT_LOCK": str(self.lock)}),
        ):
            return groups.start_group(self.settings, group)

    def test_silver_is_refused_while_bronze_runs_and_nothing_is_started(self) -> None:
        docker = mock.Mock()
        with self.assertRaises(groups.GroupError) as caught:
            self.start("batch_silver", {"spark_bronze"}, docker)
        self.assertIn("spark_bronze", str(caught.exception))
        docker.assert_not_called()

    def test_bronze_is_refused_while_another_bronze_job_runs(self) -> None:
        docker = mock.Mock()
        with self.assertRaises(groups.GroupError):
            self.start("batch_bronze", {"spark_bronze"}, docker)
        docker.assert_not_called()

    def test_a_free_slot_starts_the_group_under_the_lock_and_releases_it_after(self) -> None:
        def start_under_the_lock(*_: object, **__: object) -> str:
            with self.assertRaises(SlotBusy), spark_slot(lambda: [], lock_file=self.lock):
                pass
            return ""

        docker = mock.Mock(side_effect=start_under_the_lock)
        self.start("batch_silver", set(), docker)
        docker.assert_called_once()
        with spark_slot(lambda: [], lock_file=self.lock):
            pass

    def test_a_group_that_is_not_a_spark_group_takes_no_slot(self) -> None:
        docker = mock.Mock(return_value="")
        self.start("supporting", {"spark_bronze"}, docker)
        docker.assert_called()


class OneOffTest(unittest.TestCase):
    """The table checks one-off holds the slot for the whole of ``docker compose run``, and does not start while a Spark job runs."""

    def setUp(self) -> None:
        self.lock = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="spark_slot_"))) / "slot.lock"
        self.settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")

    def runner(self, running: list[str]) -> run.Runner:
        return run.compose_runner(
            self.settings,
            volume="stack_example_table_check_staging",
            directory="/opt/dagster/staging",
            deadline_seconds=77,
            running=lambda: running,
            slot_lock=self.lock,
        )

    def test_a_job_of_another_group_that_runs_refuses_the_start_and_no_container_is_made(self) -> None:
        for holder in SPARK_SERVICES:
            with self.subTest(holder=holder), mock.patch.object(run, "run_command") as started:
                with self.assertRaisesRegex(run.TableChecksError, holder):
                    self.runner([holder])(Path("/opt/dagster/staging/run_one"))
                started.assert_not_called()

    def test_the_slot_is_held_while_the_container_runs_and_released_when_it_exits(self) -> None:
        def container(*_: object, **__: object) -> CompletedProcess[str]:
            with self.assertRaises(SlotBusy), spark_slot(lambda: [], lock_file=self.lock):
                pass
            return CompletedProcess([], 0, "RESULT {}\n", "")

        with mock.patch.object(run, "run_command", side_effect=container) as started:
            self.assertEqual(0, self.runner([])(Path("/opt/dagster/staging/run_one")).exit_code)
        started.assert_called_once()
        with spark_slot(lambda: [], lock_file=self.lock):
            pass

    def test_the_slot_is_released_when_the_job_passes_its_deadline(self) -> None:
        with mock.patch.object(run, "run_command", side_effect=TimeoutExpired("docker", 77)):
            self.assertEqual(124, self.runner([])(Path("/opt/dagster/staging/run_one")).exit_code)
        with spark_slot(lambda: [], lock_file=self.lock):
            pass


class DagsterTagTest(unittest.TestCase):
    """The runs of every Spark group carry one tag that the run queue limits to one, so Dagster starts one Spark job at a time."""

    def tags(self, group: str) -> dict[str, str]:
        return job_tags("a_job", LIMITS_FILE, {"a_job": group}, {"HEAVY_MEMORY_MIB": "10000"})

    def test_a_job_of_each_spark_group_carries_the_one_spark_tag_and_no_other_group_does(self) -> None:
        for group in SPARK_GROUPS:
            with self.subTest(group=group):
                self.assertEqual(SPARK_TAG_VALUE, self.tags(group)[SPARK_TAG_KEY])
        self.assertNotIn(SPARK_TAG_KEY, self.tags("ingestion"))
        self.assertNotIn(SPARK_TAG_KEY, heavy_job_tags("batch_silver", LIMITS_FILE, {"HEAVY_MEMORY_MIB": "10000"}))

    def test_the_run_queue_limits_the_spark_tag_to_one_run_with_no_value_so_every_run_with_the_key_shares_it(self) -> None:
        document: Any = load_yaml((ROOT / "orchestration" / "dagster.yaml").read_text())
        entries: list[dict[str, object]] = document["concurrency"]["runs"]["tag_concurrency_limits"]
        limits = {str(entry["key"]): entry for entry in entries}
        self.assertEqual({"key": SPARK_TAG_KEY, "limit": 1}, limits[SPARK_TAG_KEY])

    def test_every_declared_job_of_a_spark_group_has_the_spark_tag(self) -> None:
        for job, group in JOB_GROUPS.items():
            if group in SPARK_GROUPS:
                self.assertIn(SPARK_TAG_KEY, job_tags(job, LIMITS_FILE, JOB_GROUPS, {"HEAVY_MEMORY_MIB": "10000"}))


if __name__ == "__main__":
    unittest.main()
