"""The live host's cases of the group runner and of the Dagster runner expect what the deployed stack and the tracked code do.

Both runners stay untracked, so each test skips when its runner is not on this machine. No SSH, Docker, Kubernetes or Dagster runs here.

Failure modes covered here:

- The deploy installs Airbyte with its workloads up and a job leaves a group that was up before it running, so a case that expects the job to start and stop
  the group launches it on a group that is already up and fails on correct behaviour.
- The first of the two ``group-up`` commands finds the group up, so the case passes without a start.
- A stop that fails on the pod of a sync passes the case while the run's tags say nothing is left running.
- A job, schedule or sensor joins the code location and the Dagster runner's list stays as it was.
"""

from __future__ import annotations

import ast
import json
import unittest
from types import SimpleNamespace
from typing import Any

from scripts.project_config import ROOT
from tests.e2e.run_record import Record
from tests.fast.runner_import import ABSENT, import_runner, present

CODE_FOLDER = ROOT / "orchestration" / "surgical_risk"
WORKLOADS = ("airbyte-server", "airbyte-worker", "airbyte-minio")
KEY = "surgical_risk/group/ingestion"


class Host:
    """The runner's view of the live host as the deploy leaves it, with Airbyte up. A job stops only what it started, so it leaves the scale as it found it."""

    def __init__(self, *, left_running: str = "unknown", first_up_starts: bool = True) -> None:
        self.scale = dict.fromkeys(WORKLOADS, "1")
        self.left_running = left_running
        self.first_up_starts = first_up_starts
        self.at_launch: list[set[str]] = []
        self.record = Record(feature="fast test", slug="fast_test", rerun="none")
        images = json.dumps({"images": [{"repoTags": ["docker.io/library/busybox:1"]}]})
        self.stack = SimpleNamespace(target=SimpleNamespace(run=lambda *args, **kwargs: (0, images)))

    def marks(self, services: tuple[str, ...]) -> dict[str, str]:
        return {service: "same" for service in services}

    def scale_of_each_workload(self) -> dict[str, str]:
        return dict(self.scale)

    def kubectl(self, *arguments: str, **kwargs: Any) -> tuple[int, str]:
        return 0, ""

    def run_job(self, group: str, *, fail: bool = False, resource: dict[str, Any] | None = None) -> tuple[str, str, dict[str, str], str]:
        self.at_launch.append(set(self.scale.values()))
        if (resource or {}).get("stop_deadline_seconds") == 30:
            return "run", "FAILURE", {f"{KEY}/left_running": self.left_running}, "replication-job-probe-attempt-0 (Running)"
        return "run", "FAILURE" if fail else "SUCCESS", {f"{KEY}/left_running": "none"}, ""

    def in_container(self, *arguments: str, available: int | None = None, **kwargs: Any) -> tuple[int, str]:
        if available is not None:
            return 75, ""
        was_up = set(self.scale.values()) == {"1"}
        if arguments[-2] == "down":
            self.scale = dict.fromkeys(WORKLOADS, "0")
            return 0, "groups: group ingestion is down" + (": stopped airbyte_server" if was_up else ": nothing was running")
        starts = not was_up and self.first_up_starts
        self.scale = dict.fromkeys(WORKLOADS, "1")
        return 0, "groups: group ingestion is up" + (": started airbyte_server" if starts else ": nothing to start")


def observed(host: Host, case: str) -> bool:
    found = [observation.passed for observation in host.record.observations if observation.case == case]
    if len(found) != 1:
        raise AssertionError(f"one observation of {case!r} expected, found {len(found)}")
    return found[0]


@unittest.skipUnless(present("stack_groups"), ABSENT)
class GroupRunnerOnTheHost(unittest.TestCase):
    def setUp(self) -> None:
        self.runner: Any = import_runner("stack_groups")

    def test_the_jobs_that_expect_to_start_the_group_are_launched_with_every_workload_at_zero(self) -> None:
        host = Host()
        self.runner.k3s_group(host)
        self.assertEqual([{"0"}, {"0"}], host.at_launch)
        self.assertTrue(observed(host, "ingestion group stopped before the jobs"))

    def test_each_job_of_the_sync_pod_cases_is_launched_with_every_workload_at_zero(self) -> None:
        host = Host()
        self.runner.k3s_job_pod(host)
        self.assertEqual([{"0"}, {"0"}], host.at_launch)

    def test_the_two_group_up_commands_pass_only_when_the_first_one_starts_the_group(self) -> None:
        for starts in (True, False):
            host = Host(first_up_starts=starts)
            self.runner.k3s_group(host)
            with self.subTest(first_up_starts=starts):
                self.assertEqual(starts, observed(host, "group-up of the ingestion group twice"))

    def test_the_stop_that_fails_passes_only_when_the_tags_do_not_say_nothing_is_left(self) -> None:
        for left_running in ("unknown", "none"):
            host = Host(left_running=left_running)
            self.runner.k3s_job_pod(host)
            with self.subTest(left_running=left_running):
                self.assertEqual(left_running == "unknown", observed(host, "pod of a sync that runs 900 seconds, stop deadline 30"))


def code_location_definitions() -> dict[str, list[str]]:
    """The jobs, schedules and sensors that the tracked code location's own statements define, as Dagster names them."""
    jobs: set[str] = set()
    schedules: set[str] = set()
    sensors: set[str] = set()
    has_asset = False
    trees = [ast.parse(path.read_text()) for path in sorted(CODE_FOLDER.glob("*.py"))]
    constants = {
        target.id: node.value.value
        for tree in trees
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    for tree in trees:

        def text(node: ast.expr, names: dict[str, str] = constants) -> str:
            if isinstance(node, ast.Constant):
                return str(node.value)
            return names[ast.unparse(node)]

        def name_of(call: ast.Call | None, otherwise: str) -> str:
            return next((text(keyword.value) for keyword in call.keywords if keyword.arg == "name"), otherwise) if call else otherwise

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    call = decorator if isinstance(decorator, ast.Call) else None
                    kind = getattr(call.func if call else decorator, "id", "")
                    if kind == "job":
                        jobs.add(name_of(call, node.name))
                    elif kind == "sensor":
                        sensors.add(name_of(call, node.name))
                    elif kind == "asset":
                        has_asset = True
            elif isinstance(node, ast.Call) and getattr(node.func, "id", "") == "define_asset_job":
                jobs.add(text(node.args[0]) if node.args else name_of(node, ""))
            elif isinstance(node, ast.Call) and getattr(node.func, "id", "") == "ScheduleDefinition":
                schedules.add(name_of(node, ""))
    if has_asset:
        jobs.add("__ASSET_JOB")
    return {"jobs": sorted(jobs), "schedules": sorted(schedules), "sensors": sorted(sensors)}


@unittest.skipUnless(present("stack_dagster"), ABSENT)
class DagsterRunnerDefinitions(unittest.TestCase):
    def test_the_code_location_case_expects_the_definitions_of_the_tracked_code(self) -> None:
        runner: Any = import_runner("stack_dagster")
        definitions = code_location_definitions()
        repository = {kind: [{"name": name} for name in names] for kind, names in definitions.items()}
        record = Record(feature="fast test", slug="fast_test", rerun="none")
        runner.location_loaded(SimpleNamespace(location=lambda: repository, record=record))
        self.assertEqual([True], [observation.passed for observation in record.observations], record.observations[0].observed)


if __name__ == "__main__":
    unittest.main()
