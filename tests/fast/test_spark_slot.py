"""One Spark job runs at a time, across all four Spark groups and every way a job starts, and the slot is held as long as the job's container exists.

The memory figure of the host assumes one Spark job's limit (``infra/heavy_memory.tf``), and each Spark service is its own group, so the memory check of one
group does not see another group's job. The slot is the missing rule: a container name that Docker keeps unique on the engine (``spark_slot.SLOT_NAME``). The
launchers that take it are the table checks one-off (``compose_runner``) and the manual one-off (``scripts/stack/spark_run.py``); the runners start their own
Spark containers with the same name. A second launch fails on the name, a client that is killed or a code location that restarts does not free it, and a
paused container holds it. A launch that passes its deadline removes its own container and never another launch's. A Spark group is never started by
``group-up``: its service starts no job. The Dagster runs of the Spark groups share one tag that ``orchestration/dagster.yaml`` limits to one run. The tests run
with ``tests/fast/spark_engine.py``, a stand-in for the Docker engine with the daemon's rule that a name exists once.
"""

from __future__ import annotations

import ast
import contextlib
import io
import unittest
from collections.abc import Callable, Collection, Sequence
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
from scripts.stack.spark_slot import SlotBusy, SlotError, run_in_slot, spark_holders
from scripts.stack.yaml_text import load_yaml
from tests.fast.spark_engine import OWNER_LABEL, SLOT_NAME, Box, Engine, finishes

ROOT = Path(__file__).resolve().parents[2]
CODE_FOLDER = ROOT / "orchestration" / "surgical_risk"
PLENTY = 10**6


def killed(engine: Engine, box: Box) -> CompletedProcess[str]:
    """The client is killed while the container runs: ``run_command`` raises the deadline, and the container is not stopped by that."""
    raise TimeoutExpired("docker", 5)


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
    """``run_in_slot`` over the engine stand-in: the name is the lock."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.now = 0.0
        self.reads: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def build(self, options: Sequence[str]) -> list[str]:
        return ["compose", "run", "--rm", "--no-TTY", *options, "spark_maintenance", "table_maintenance"]

    def record_read(self, seconds: float) -> list[str]:
        self.reads.append(seconds)
        return []

    def go(self, running: Callable[[float], Collection[str]] | None = None, *, wait: float = 0, poll: float = 5.0) -> CompletedProcess[str]:
        def read(seconds: float) -> Collection[str]:
            self.reads.append(seconds)
            return running(seconds) if running else []

        return run_in_slot(
            self.build, running=read, deadline_seconds=100, wait_seconds=wait, poll_seconds=poll, clock=self.clock, sleep=self.sleep, docker=self.engine.docker
        )

    def test_the_names_are_the_ones_of_the_design(self) -> None:
        self.assertEqual((SLOT_NAME, OWNER_LABEL), (slot.SLOT_NAME, slot.OWNER_LABEL))
        self.assertEqual(["--name", SLOT_NAME, "--label", f"{OWNER_LABEL}=abc"], slot.slot_options("abc"))

    def test_the_holders_are_the_spark_services_that_are_alive_in_name_order(self) -> None:
        self.assertEqual(("spark_bronze", "spark_gold"), spark_holders({"spark_gold", "nessie", "spark_bronze"}))
        self.assertEqual((), spark_holders({"nessie"}))

    def test_a_free_slot_starts_the_job_with_the_slots_name_and_its_own_label_and_leaves_no_container(self) -> None:
        result = self.go()
        self.assertEqual(0, result.returncode)
        (launch,) = self.engine.launches()
        owner = launch[launch.index("--label") + 1]
        self.assertEqual(["--name", SLOT_NAME, "--label", owner], launch[launch.index("--no-TTY") + 1 : launch.index("spark_maintenance")])
        self.assertTrue(owner.startswith(f"{OWNER_LABEL}="))
        self.assertEqual({}, self.engine.boxes)

    def test_each_launch_has_its_own_owner_value(self) -> None:
        self.go()
        self.go()
        owners = {launch[launch.index("--label") + 1] for launch in self.engine.launches()}
        self.assertEqual(2, len(owners))

    def test_the_slot_is_held_while_the_container_runs_and_free_after_it_exits(self) -> None:
        inner: list[str] = []

        def job(engine: Engine, box: Box) -> CompletedProcess[str]:
            with self.assertRaises(SlotBusy) as caught:
                self.go()
            inner.append(str(caught.exception))
            return CompletedProcess([], 0, "", "")

        self.engine.on_start = job
        self.go()
        self.assertIn(SLOT_NAME, inner[0])
        self.go()

    def test_a_container_with_the_slots_name_in_any_state_refuses_the_start_and_no_job_is_launched(self) -> None:
        for state in ("running", "paused", "restarting", "created", "exited"):
            engine = Engine()
            engine.hold(state=state)
            with self.subTest(state=state), self.assertRaisesRegex(SlotBusy, f"{SLOT_NAME} \\({state}\\)"):
                run_in_slot(self.build, running=lambda _: [], deadline_seconds=1, docker=engine.docker)
            self.assertEqual([], engine.launches())
            self.assertEqual(1, len(engine.boxes), "the container of the other launch is left as it is")

    def test_a_live_spark_service_refuses_the_start_with_its_name(self) -> None:
        with self.assertRaisesRegex(SlotBusy, "spark_bronze"):
            self.go(lambda _: ["spark_bronze", "nessie"])
        self.assertEqual([], self.engine.launches())

    def test_a_launch_that_loses_the_name_to_another_launch_after_the_read_is_busy_and_touches_nothing(self) -> None:
        """The race that the engine decides: both read a free slot, and the engine creates one container."""
        listed: list[bool] = []

        def racing(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
            reply = self.engine.docker(arguments, timeout)
            if arguments[:2] == ["container", "ls"] and not listed:
                listed.append(True)
                self.engine.hold(owner="the_other_launch")
            return reply

        with self.assertRaisesRegex(SlotBusy, SLOT_NAME):
            run_in_slot(self.build, running=lambda _: [], deadline_seconds=1, docker=racing)
        self.assertEqual(1, len(self.engine.boxes))
        self.assertEqual("the_other_launch", next(iter(self.engine.boxes.values())).labels[OWNER_LABEL])
        self.assertFalse([call for call in self.engine.calls if call[:2] in (["container", "stop"], ["container", "rm"])])

    def test_a_job_that_prints_the_conflict_words_is_not_started_twice(self) -> None:
        self.engine.on_start = finishes(1, "", "x is already in use")
        result = self.go(wait=30)
        self.assertEqual(1, result.returncode)
        self.assertEqual(1, len(self.engine.launches()))

    def test_a_container_of_a_killed_launcher_holds_the_slot_for_every_later_launcher(self) -> None:
        """A restart of the code location clears its memory file system and every file in it: the container is what holds the slot."""
        self.engine.on_start = killed
        with self.assertRaises(TimeoutExpired):
            self.go()
        self.engine.hold(owner="a_launcher_that_is_gone")
        with self.assertRaisesRegex(SlotBusy, SLOT_NAME):
            self.go()
        source = (ROOT / "scripts" / "stack" / "spark_slot.py").read_text()
        for word in ("fcntl", "LOCK_FILE", "flock"):
            self.assertNotIn(word, source)

    def test_a_job_that_passes_its_deadline_is_stopped_and_removed_by_id_before_the_deadline_is_reported(self) -> None:
        self.engine.on_start = killed
        with self.assertRaises(TimeoutExpired):
            self.go()
        self.assertEqual({}, self.engine.boxes)
        box_id = f"{1:064x}"
        self.assertIn(["container", "stop", "--time", str(slot.STOP_SECONDS), box_id], self.engine.calls)
        self.assertIn(["container", "rm", "--force", box_id], self.engine.calls)
        self.engine.on_start = finishes()
        self.assertEqual(0, self.go().returncode)

    def test_a_container_that_cannot_be_removed_is_a_slot_error_and_not_the_deadline(self) -> None:
        self.engine.on_start = killed
        self.engine.removal_fails = True
        with self.assertRaisesRegex(SlotError, f"{SLOT_NAME}.*still there"):
            self.go()

    def test_the_cleanup_removes_only_the_container_that_carries_its_own_owner(self) -> None:
        foreign = self.engine.hold(owner="another_launch")
        slot.remove_owned(self.engine.docker, "mine")
        unlabeled = Engine()
        unlabeled.hold(owner="")
        slot.remove_owned(unlabeled.docker, "mine")
        self.assertIn(foreign.id, self.engine.boxes)
        self.assertEqual(1, len(unlabeled.boxes))
        for engine in (self.engine, unlabeled):
            self.assertFalse([call for call in engine.calls if call[:2] in (["container", "stop"], ["container", "rm"])])

    def test_an_interrupt_ends_the_container_of_this_launch_and_a_failed_cleanup_hides_nothing(self) -> None:
        def interrupt(engine: Engine, box: Box) -> CompletedProcess[str]:
            raise KeyboardInterrupt

        self.engine.on_start = interrupt
        with self.assertRaises(KeyboardInterrupt):
            self.go()
        self.assertEqual({}, self.engine.boxes)
        self.engine.removal_fails = True
        with self.assertRaises(KeyboardInterrupt):
            self.go()

    def test_a_wait_ends_when_the_holder_is_gone_and_not_later_than_its_deadline(self) -> None:
        holder = self.engine.hold()
        seen: list[float] = []

        def read(seconds: float) -> list[str]:
            seen.append(self.now)
            if self.now >= 20:
                self.engine.boxes.pop(holder.id, None)
            return []

        self.go(read, wait=60, poll=10)
        self.assertEqual([0.0, 10.0, 20.0], seen)
        self.assertEqual(1, len(self.engine.launches()))
        self.now = 0
        self.engine.hold()
        with self.assertRaises(SlotBusy):
            self.go(wait=25, poll=10)
        self.assertEqual(25, self.now)

    def test_a_wait_shorter_than_the_poll_ends_at_its_deadline_and_never_admits_after_it(self) -> None:
        self.engine.hold()
        with self.assertRaises(SlotBusy):
            self.go(wait=1)
        self.assertEqual(1, self.now)

    def test_a_free_reading_that_arrives_after_the_deadline_does_not_admit_even_the_first_one(self) -> None:
        """The read takes 4 seconds, so the slot looked free after the wait of 2 seconds ended. The old code admitted the first reading whatever the wait."""

        def slow(seconds: float) -> list[str]:
            self.now += 4
            return []

        with self.assertRaisesRegex(SlotBusy, "wait of 2 seconds ended"):
            self.go(slow, wait=2)
        self.assertEqual([], self.engine.launches())

    def test_with_no_wait_the_first_reading_admits_a_free_slot_however_long_it_takes(self) -> None:
        def slow(seconds: float) -> list[str]:
            self.now += 30
            return []

        self.assertEqual(0, self.go(slow).returncode)

    def test_every_read_gets_the_time_that_is_left_of_the_wait_and_never_more_than_the_read_limit(self) -> None:
        timeouts: list[float] = []

        def recording(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
            timeouts.append(timeout)
            return self.engine.docker(arguments, timeout)

        self.engine.hold()
        with self.assertRaises(SlotBusy):
            run_in_slot(
                self.build,
                running=self.record_read,
                deadline_seconds=100,
                wait_seconds=10,
                poll_seconds=4,
                clock=self.clock,
                sleep=self.sleep,
                docker=recording,
            )
        self.assertEqual([10, 6, 2], self.reads)
        self.assertEqual([10, 6, 2], timeouts)
        self.reads.clear()
        self.engine.boxes.clear()
        self.go(wait=500)
        self.assertEqual([slot.READ_SECONDS], self.reads)
        self.reads.clear()
        self.go()
        self.assertEqual([slot.READ_SECONDS], self.reads)

    def test_a_wait_that_is_not_a_finite_number_that_is_0_or_more_is_refused_before_anything_is_read(self) -> None:
        for wait in (float("nan"), float("inf"), float("-inf"), -1.0):
            with self.subTest(wait=wait), self.assertRaises(SlotError):
                self.go(wait=wait)
        self.assertEqual([], self.reads)
        self.assertEqual([], self.engine.calls)

    def test_the_seconds_parser_takes_finite_numbers_that_are_0_or_more_and_nothing_else(self) -> None:
        self.assertEqual(0.0, slot.finite_seconds("0"))
        self.assertEqual(2.5, slot.finite_seconds("2.5"))
        for text in ("nan", "inf", "-inf", "-1", "1e999", "", "two"):
            with self.subTest(text=text), self.assertRaises(Exception) as caught:
                slot.finite_seconds(text)
            self.assertEqual("ArgumentTypeError", type(caught.exception).__name__)

    def test_a_container_that_cannot_be_read_is_a_slot_error_and_not_a_free_slot(self) -> None:
        def broken(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
            return CompletedProcess(arguments, 1, "", "")

        def slow(arguments: Sequence[str], timeout: float) -> CompletedProcess[str]:
            raise TimeoutExpired("docker", timeout)

        for docker in (broken, slow):
            with self.subTest(docker=docker.__name__), self.assertRaisesRegex(SlotError, "cannot be read"):
                run_in_slot(self.build, running=lambda _: [], deadline_seconds=1, docker=docker)


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


class PausedTest(unittest.TestCase):
    """A paused or restarting container holds its memory and the slot, and an exited or created one does not."""

    def alive(self, state: str, timeout: float | None = None) -> frozenset[str]:
        settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")
        with mock.patch.object(groups, "docker", side_effect=["id_one", f"spark_silver|{state}|none|0|false|0\n"]) as asked:
            found = (
                groups.running_services(settings, load_limits(LIMITS_FILE))
                if timeout is None
                else groups.running_services(settings, load_limits(LIMITS_FILE), timeout)
            )
        self.timeouts = [call.kwargs["timeout"] for call in asked.call_args_list]
        return found

    def test_a_running_paused_or_restarting_service_is_alive_and_a_stopped_one_is_not(self) -> None:
        for state in ("running", "paused", "restarting"):
            with self.subTest(state=state):
                self.assertEqual(frozenset({"spark_silver"}), self.alive(state))
        for state in ("exited", "created", "dead", "removing"):
            with self.subTest(state=state):
                self.assertEqual(frozenset(), self.alive(state))

    def test_the_two_reads_together_take_at_most_the_seconds_they_are_given(self) -> None:
        self.alive("paused", 8)
        self.assertEqual(8, self.timeouts[0])
        self.assertLessEqual(self.timeouts[1], 8)
        settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")
        with mock.patch.object(groups, "docker", return_value=""), mock.patch("scripts.stack.groups.time.monotonic", side_effect=[0.0, 0.0]):
            self.assertEqual(frozenset(), groups.running_services(settings, load_limits(LIMITS_FILE), 3))
        ticks = iter([0.0, 5.0])
        with (
            mock.patch.object(groups, "docker", return_value="id_one"),
            mock.patch("scripts.stack.groups.time.monotonic", side_effect=lambda: next(ticks)),
            self.assertRaisesRegex(groups.GroupError, "timed out after 3 seconds"),
        ):
            groups.running_services(settings, load_limits(LIMITS_FILE), 3)


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
    """The table checks one-off holds the slot as long as its container exists, and does not start while a Spark container of any launcher is alive."""

    def setUp(self) -> None:
        self.engine = Engine()
        self.enterContext(mock.patch.object(slot, "run_command", self.engine.run_command))
        self.settings = groups.Settings(compose_directory=ROOT / "compose", project="stack_example")
        self.staging = Path("/opt/dagster/staging/run_one")

    def runner(self, running: list[str] | None = None) -> run.Runner:
        return run.compose_runner(
            self.settings, volume="stack_example_table_check_staging", directory="/opt/dagster/staging", deadline_seconds=77, running=lambda _: running or []
        )

    def manual(self) -> tuple[int, str]:
        error = io.StringIO()
        with contextlib.redirect_stderr(error), contextlib.redirect_stdout(io.StringIO()):
            return spark_run.main(["table_maintenance", "--dry-run"], running=lambda _: []), error.getvalue()

    def test_a_job_of_another_group_that_runs_refuses_the_start_and_no_container_is_made(self) -> None:
        for holder in SPARK_SERVICES:
            with self.subTest(holder=holder), self.assertRaisesRegex(run.TableChecksError, holder):
                self.runner([holder])(self.staging)
            self.assertEqual([], self.engine.launches())

    def test_the_container_has_the_slots_name_and_the_options_come_before_the_service(self) -> None:
        self.assertEqual(0, self.runner()(self.staging).exit_code)
        (launch,) = self.engine.launches()
        self.assertEqual(SLOT_NAME, launch[launch.index("--name") + 1])
        self.assertLess(launch.index("--name"), launch.index("--user"))
        self.assertLess(launch.index("--label"), launch.index("spark_bronze"))
        self.assertEqual({}, self.engine.boxes)

    def test_the_slot_is_held_while_the_container_runs_and_free_when_it_exits(self) -> None:
        seen: list[tuple[int, str]] = []

        def container(engine: Engine, box: Box) -> CompletedProcess[str]:
            seen.append(self.manual())
            return CompletedProcess([], 0, "RESULT {}\n", "")

        self.engine.on_start = container
        self.assertEqual(0, self.runner()(self.staging).exit_code)
        self.assertEqual(1, seen[0][0])
        self.assertIn(SLOT_NAME, seen[0][1])
        self.assertEqual(0, self.manual()[0])

    def test_a_job_that_passes_its_deadline_returns_124_after_its_container_is_removed(self) -> None:
        self.engine.on_start = killed
        self.assertEqual(124, self.runner()(self.staging).exit_code)
        self.assertEqual({}, self.engine.boxes)

    def test_a_container_that_cannot_be_removed_after_the_deadline_is_a_table_checks_error(self) -> None:
        self.engine.on_start = killed
        self.engine.removal_fails = True
        with self.assertRaisesRegex(run.TableChecksError, "still there"):
            self.runner()(self.staging)

    def test_a_slot_container_of_another_launch_refuses_the_start_in_every_state_and_is_left_alone(self) -> None:
        for state in ("running", "paused"):
            engine = Engine()
            engine.hold(state=state)
            with self.subTest(state=state), mock.patch.object(slot, "run_command", engine.run_command), self.assertRaisesRegex(run.TableChecksError, SLOT_NAME):
                self.runner()(self.staging)
            self.assertEqual([], engine.launches())
            self.assertEqual(1, len(engine.boxes))

    def test_the_table_checks_and_the_manual_one_off_exclude_each_other_through_the_name_in_both_directions(self) -> None:
        manual: list[tuple[int, str]] = []

        def container(engine: Engine, box: Box) -> CompletedProcess[str]:
            manual.append(self.manual())
            return CompletedProcess([], 0, "RESULT {}\n", "")

        self.engine.on_start = container
        self.runner()(self.staging)
        self.assertEqual(1, manual[0][0])
        nested: list[str] = []

        def manual_container(engine: Engine, box: Box) -> CompletedProcess[str]:
            with self.assertRaisesRegex(run.TableChecksError, "do not start") as caught:
                self.runner()(self.staging)
            nested.append(str(caught.exception))
            return CompletedProcess([], 0, "", "")

        self.engine.on_start = manual_container
        self.assertEqual(0, self.manual()[0])
        self.assertIn(SLOT_NAME, nested[0])


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
