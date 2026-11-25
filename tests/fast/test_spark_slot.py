"""One Spark job runs at a time, across all four Spark groups and every way a job starts, and the slot is held until the job's container exits.

The memory figure of the host assumes one Spark job's limit (``infra/heavy_memory.tf``), and each Spark service is its own group, so the memory check of
one group does not see another group's job. The slot is the missing rule. The launchers that take it are the table checks one-off (``compose_runner``) and the
manual one-off (``scripts/stack/spark_run.py``). Both run in the code location container and take the one lock file of ``spark_slot.LOCK_FILE``, which no
argument or environment variable moves. A Spark group is never started by ``group-up``: its service starts no job, and a stopped container of an older
version would restart its old entry point. The Dagster runs of the Spark groups share one tag that ``orchestration/dagster.yaml`` limits to one run. The tests
run with stand-ins for Docker and use real files for the lock.
"""

from __future__ import annotations

import ast
import contextlib
import io
import tempfile
import unittest
from collections.abc import Collection, Iterator
from contextlib import AbstractContextManager
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any
from unittest import mock

from orchestration.surgical_risk.concurrency import JOB_GROUPS, SPARK_TAG_KEY, SPARK_TAG_VALUE, heavy_job_tags, job_tags
from scripts.ingestion import table_checks as run
from scripts.process import TimeoutExpired
from scripts.stack import groups, spark_run
from scripts.stack import spark_slot as slot
from scripts.stack.admission import decide, describe
from scripts.stack.job_workloads import SPARK_GROUPS, SPARK_SERVICES
from scripts.stack.limits import LIMITS_FILE, load_limits
from scripts.stack.spark_slot import SlotBusy, SlotError, spark_holders, spark_slot
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
CODE_FOLDER = ROOT / "orchestration" / "surgical_risk"
PLENTY = 10**6


@contextlib.contextmanager
def one_lock(lock: Path) -> Iterator[None]:
    """Move the one lock file of the stack to ``lock``, a file of the test."""
    with mock.patch.object(slot, "LOCK_FILE", lock):
        yield


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
        self.enterContext(one_lock(self.lock))
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def claim(self, running: list[str], wait: float = 0) -> AbstractContextManager[None]:
        return spark_slot(lambda: running, wait_seconds=wait, clock=self.clock, sleep=self.sleep)

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

        with spark_slot(view, wait_seconds=60, clock=self.clock, sleep=self.sleep, poll_seconds=10):
            pass
        self.assertEqual([0.0, 10.0, 20.0, 30.0], calls)
        self.now = 0
        with self.assertRaises(SlotBusy), spark_slot(lambda: ["spark_silver"], wait_seconds=25, clock=self.clock, sleep=self.sleep, poll_seconds=10):
            pass
        self.assertEqual(25, self.now)

    def test_a_wait_shorter_than_the_poll_ends_at_its_deadline_and_never_admits_after_it(self) -> None:
        """A wait of 1 second with a poll of 5 seconds slept the whole poll and admitted at 5 seconds."""
        with self.assertRaises(SlotBusy), spark_slot(lambda: ["spark_silver"] if self.now < 5 else [], wait_seconds=1, clock=self.clock, sleep=self.sleep):
            self.fail("the body must not run")
        self.assertEqual(1, self.now)

    def test_a_free_reading_that_arrives_after_the_deadline_does_not_admit(self) -> None:
        """The reading takes 4 seconds, so the slot looked free after the wait of 2 seconds ended."""
        readings = []

        def slow_view() -> list[str]:
            readings.append(self.now)
            self.now += 4 if len(readings) > 1 else 0
            return ["spark_silver"] if len(readings) == 1 else []

        with self.assertRaises(SlotBusy), spark_slot(slow_view, wait_seconds=2, clock=self.clock, sleep=self.sleep):
            self.fail("the body must not run")
        with self.claim([]):
            pass

    def test_the_first_reading_admits_a_free_slot_however_long_it_takes_and_whatever_the_wait(self) -> None:
        def slow_view() -> list[str]:
            self.now += 30
            return []

        with spark_slot(slow_view, wait_seconds=0, clock=self.clock, sleep=self.sleep):
            pass

    def test_the_one_lock_follows_no_argument_and_no_environment_variable(self) -> None:
        """Two launchers with different environments, as a host and a container have, took two lock files and both entered."""
        elsewhere = self.lock.parent / "elsewhere"
        elsewhere.mkdir()
        with (
            mock.patch.dict("os.environ", {"SPARK_SLOT_LOCK": str(elsewhere / "other.lock"), "TMPDIR": str(elsewhere)}),
            self.claim([]),
            self.assertRaisesRegex(SlotBusy, "another launcher"),
            self.claim([]),
        ):
            self.fail("the body must not run")
        self.assertEqual([], sorted(path.name for path in elsewhere.iterdir()))
        with self.assertRaises(TypeError):
            spark_slot(lambda: [], lock_file=elsewhere / "other.lock")  # type: ignore[call-arg]

    def test_the_default_lock_is_the_one_file_of_the_code_location_container(self) -> None:
        self.assertIn('LOCK_FILE = Path("/run/dagster/spark_slot.lock")', (ROOT / "scripts" / "stack" / "spark_slot.py").read_text())
        document: Any = load_yaml((ROOT / "compose" / "services" / "dagster_code_location.yml").read_text())
        compose = document["services"]["dagster_code_location"]
        self.assertIn("/run/dagster:uid=10001,gid=10001,mode=0700", compose["tmpfs"])
        dockerfile = (ROOT / "orchestration" / "Dockerfile").read_text()
        self.assertIn("stack/spark_run.py", dockerfile)
        self.assertIn("stack/spark_slot.py", dockerfile)

    def test_a_lock_that_cannot_be_opened_is_a_slot_error_that_names_the_folder_and_is_not_a_busy_slot(self) -> None:
        with (
            mock.patch.object(slot, "LOCK_FILE", self.lock.parent / "no_such_folder" / "slot.lock"),
            self.assertRaises(SlotError) as caught,
            self.claim([]),
        ):
            self.fail("the body must not run")
        self.assertNotIsInstance(caught.exception, SlotBusy)
        self.assertIn("no_such_folder", str(caught.exception))
        self.assertIn("code location", str(caught.exception))


class OccupancyTest(unittest.TestCase):
    """A container that is alive holds the slot, whatever an older container of the same service name says."""

    ROWS = {"running": "spark_silver|running|none|0|false|0", "exited": "spark_silver|exited|none|0|false|0", "created": "spark_silver|created|none|0|false|0"}

    def test_a_running_container_wins_over_an_exited_one_of_the_same_name_in_either_order(self) -> None:
        for text in ("running", "exited"), ("exited", "running"):
            with self.subTest(order=text):
                found = groups.parse_inspect("\n".join(self.ROWS[name] for name in text))
                self.assertEqual("running", found["spark_silver"].state)

    def test_every_live_state_is_kept_over_a_stopped_one(self) -> None:
        for live in ("running", "restarting", "paused"):
            for stopped in ("exited", "dead", "created"):
                with self.subTest(live=live, stopped=stopped):
                    rows = f"spark_silver|{stopped}|none|0|false|0\nspark_silver|{live}|none|0|false|0\nspark_silver|{stopped}|none|0|false|0"
                    self.assertEqual(live, groups.parse_inspect(rows)["spark_silver"].state)

    def test_two_stopped_containers_keep_the_last_one_as_before(self) -> None:
        rows = "spark_silver|exited|none|3|false|0\nspark_silver|dead|none|4|false|0"
        self.assertEqual("dead", groups.parse_inspect(rows)["spark_silver"].state)

    def test_the_running_services_hold_the_service_of_a_live_one_off_next_to_an_exited_one(self) -> None:
        settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")
        listing = "id_one id_two"
        inspected = f"{self.ROWS['exited']}\n{self.ROWS['running']}\n"
        with mock.patch.object(groups, "docker", side_effect=[listing, inspected]):
            self.assertIn("spark_silver", groups.running_services(settings, load_limits(LIMITS_FILE)))
            self.assertEqual({"spark_silver"}, set(spark_holders({"spark_silver"})))


class GroupUpTest(unittest.TestCase):
    """``group-up`` starts no Spark group: a stopped container of an older version keeps its old entry point under ``up --no-recreate``."""

    def setUp(self) -> None:
        self.settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example", available_mib=PLENTY)

    def patched(self) -> contextlib.ExitStack:
        stack = contextlib.ExitStack()
        self.docker = stack.enter_context(mock.patch.object(groups, "docker", return_value=""))
        self.states = stack.enter_context(mock.patch.object(groups, "read_states", return_value={}))
        self.running = stack.enter_context(mock.patch.object(groups, "running_services", return_value=frozenset()))
        stack.enter_context(mock.patch.object(groups, "wait_ready"))
        return stack

    def test_every_spark_group_is_refused_before_compose_starts_a_container_or_anything_is_read(self) -> None:
        for group in SPARK_GROUPS:
            with self.subTest(group=group), self.patched(), self.assertRaises(groups.GroupError) as caught:
                groups.start_group(self.settings, group)
            self.assertIn(group, str(caught.exception))
            self.assertIn("spark_run", str(caught.exception))
            self.docker.assert_not_called()
            self.states.assert_not_called()
            self.running.assert_not_called()

    def test_the_command_ends_with_exit_status_1_and_a_refusal_for_a_spark_group_and_starts_nothing(self) -> None:
        for group in SPARK_GROUPS:
            error = io.StringIO()
            with (
                self.subTest(group=group),
                self.patched(),
                contextlib.redirect_stderr(error),
                mock.patch.dict("os.environ", {"STACK_TEST_AVAILABLE_MIB": str(PLENTY)}),
            ):
                status = groups.main(["up", group, "--compose-directory", str(ROOT / "compose"), "--project", "stack_example"])
            self.assertEqual(1, status)
            self.assertIn("groups: refused: group-up never starts a Spark group", error.getvalue())
            self.assertIn("spark_run", error.getvalue())
            self.docker.assert_not_called()

    def test_a_group_that_is_not_a_spark_group_still_starts(self) -> None:
        with self.patched():
            groups.start_group(self.settings, "supporting")
            self.docker.assert_called()

    def test_a_group_down_of_a_spark_group_is_still_allowed(self) -> None:
        with self.patched():
            groups.stop_group(self.settings, "batch_silver")


class OneOffTest(unittest.TestCase):
    """The table checks one-off holds the slot for the whole of ``docker compose run``, and does not start while a Spark job runs."""

    def setUp(self) -> None:
        self.lock = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="spark_slot_"))) / "slot.lock"
        self.enterContext(one_lock(self.lock))
        self.settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")

    def runner(self, running: list[str]) -> run.Runner:
        return run.compose_runner(
            self.settings, volume="stack_example_table_check_staging", directory="/opt/dagster/staging", deadline_seconds=77, running=lambda: running
        )

    def test_a_job_of_another_group_that_runs_refuses_the_start_and_no_container_is_made(self) -> None:
        for holder in SPARK_SERVICES:
            with self.subTest(holder=holder), mock.patch.object(run, "run_command") as started:
                with self.assertRaisesRegex(run.TableChecksError, holder):
                    self.runner([holder])(Path("/opt/dagster/staging/run_one"))
                started.assert_not_called()

    def test_the_slot_is_held_while_the_container_runs_and_released_when_it_exits(self) -> None:
        def container(*_: object, **__: object) -> CompletedProcess[str]:
            with self.assertRaises(SlotBusy), spark_slot(lambda: []):
                pass
            return CompletedProcess([], 0, "RESULT {}\n", "")

        with mock.patch.object(run, "run_command", side_effect=container) as started:
            self.assertEqual(0, self.runner([])(Path("/opt/dagster/staging/run_one")).exit_code)
        started.assert_called_once()
        with spark_slot(lambda: []):
            pass

    def test_the_slot_is_released_when_the_job_passes_its_deadline(self) -> None:
        with mock.patch.object(run, "run_command", side_effect=TimeoutExpired("docker", 77)):
            self.assertEqual(124, self.runner([])(Path("/opt/dagster/staging/run_one")).exit_code)
        with spark_slot(lambda: []):
            pass

    def test_a_lock_that_cannot_be_opened_is_a_table_checks_error_and_no_container_is_made(self) -> None:
        with (
            mock.patch.object(slot, "LOCK_FILE", self.lock.parent / "no_such_folder" / "slot.lock"),
            mock.patch.object(run, "run_command") as started,
            self.assertRaisesRegex(run.TableChecksError, "no_such_folder"),
        ):
            self.runner([])(Path("/opt/dagster/staging/run_one"))
        started.assert_not_called()

    def test_the_table_checks_and_the_manual_one_off_exclude_each_other_through_the_one_lock(self) -> None:
        """Both launchers run in the code location container. While one's container runs, the other is refused, though the container is not yet listed."""
        manual_reply: list[int] = []

        def container(*_: object, **__: object) -> CompletedProcess[str]:
            with mock.patch.object(spark_run, "run_command") as other, contextlib.redirect_stderr(io.StringIO()):
                manual_reply.append(spark_run.main(["table_maintenance", "--dry-run"], running=lambda: []))
            other.assert_not_called()
            return CompletedProcess([], 0, "RESULT {}\n", "")

        with mock.patch.object(run, "run_command", side_effect=container):
            self.runner([])(Path("/opt/dagster/staging/run_one"))
        self.assertEqual([1], manual_reply)

        def manual_container(*_: object, **__: object) -> CompletedProcess[str]:
            with mock.patch.object(run, "run_command") as other, self.assertRaisesRegex(run.TableChecksError, "another launcher"):
                self.runner([])(Path("/opt/dagster/staging/run_one"))
            other.assert_not_called()
            return CompletedProcess([], 0, "", "")

        with mock.patch.object(spark_run, "run_command", side_effect=manual_container) as started:
            self.assertEqual(0, spark_run.main(["table_maintenance", "--dry-run"], running=lambda: []))
        started.assert_called_once()


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

    def spark_launching_jobs(self) -> dict[str, ast.Call | None]:
        """Each ``@job`` of a code location file that starts a Spark container (it calls ``compose_runner``), by function name, with its decorator call."""
        found: dict[str, ast.Call | None] = {}
        for path in sorted(CODE_FOLDER.glob("*.py")):
            tree = ast.parse(path.read_text())
            if "compose_runner(" not in path.read_text():
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    for decorator in node.decorator_list:
                        if getattr(decorator, "id", "") == "job" or getattr(getattr(decorator, "func", None), "id", "") == "job":
                            found[node.name] = decorator if isinstance(decorator, ast.Call) else None
        return found

    def test_the_job_that_starts_the_table_checks_container_is_declared_and_takes_the_spark_tag_from_its_group(self) -> None:
        """The deployed ``table_checks_job`` ran with no tag, so the run queue let it start beside another Spark run."""
        jobs = self.spark_launching_jobs()
        self.assertEqual(["table_checks_job"], sorted(jobs))
        self.assertEqual("batch_bronze", JOB_GROUPS["table_checks_job"])
        for name, decorator in jobs.items():
            tags = next((keyword.value for keyword in (decorator.keywords if decorator else []) if keyword.arg == "tags"), None)
            with self.subTest(job=name):
                self.assertIsInstance(tags, ast.Call)
                self.assertEqual("job_tags", getattr(getattr(tags, "func", None), "id", ""))
        self.assertIn(SPARK_TAG_KEY, job_tags("table_checks_job", LIMITS_FILE, JOB_GROUPS, {"HEAVY_MEMORY_MIB": "10000"}))

    def test_the_scan_finds_a_spark_launching_job_with_no_tag(self) -> None:
        decorator = ast.parse("@job(executor_def=in_process_executor)\ndef untagged(): ...").body[0].decorator_list[0]  # type: ignore[attr-defined]
        self.assertIsInstance(decorator, ast.Call)
        self.assertEqual([], [keyword for keyword in decorator.keywords if keyword.arg == "tags"])


if __name__ == "__main__":
    unittest.main()
