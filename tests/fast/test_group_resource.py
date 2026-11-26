"""A job that runs in a service group stops the group in its own last step, so a stop that fails fails the run, and the run's record says what may remain.

Dagster 1.13.31 logs a resource teardown that raises and still ends the run as a success (``job_execution_iterator`` looks at failed steps only), so a stop in
the teardown alone cannot fail a run. The failures listed before the code was written: the stop times out on the pod of a sync and the run ends as a success;
the tags then say nothing is left running, because no workload is ready while the pod runs; the teardown stops a second time, finds nothing ready and writes
over the record of the failed stop; a failed step skips the stop step and nothing stops the group; a group that ran before the job is stopped by it; a new
group job leaves out the stop step.

Dagster is not installed in the tools environment, so the resource is imported with stand-ins for the names that it takes from Dagster and pydantic. The
resource's own code, the group commands and the k3s control are the tracked ones; only the Kubernetes API and the Docker command are stand-ins.
"""

from __future__ import annotations

import ast
import importlib
import sys
import types
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.project_config import ROOT
from scripts.stack.groups import GroupError, Settings
from tests.fast.test_groups_k3s import Cluster, control

CODE_FOLDER = ROOT / "orchestration" / "surgical_risk"
MODULE = "orchestration.surgical_risk.groups"
STOP_STEP = "stop_service_group"
POD = "replication-job-probe-attempt-0"
TAGS = "surgical_risk/group/ingestion"


class ResourceStandIn:
    def __init__(self, **values: Any) -> None:
        for name, value in values.items():
            setattr(self, name, value)


def resource_module() -> Any:
    dagster = types.ModuleType("dagster")
    dagster.__dict__.update(
        ConfigurableResource=ResourceStandIn,
        InitResourceContext=object,
        OpExecutionContext=object,
        Nothing=object,
        In=lambda *args, **kwargs: None,
        op=lambda *args, **kwargs: lambda function: function,
    )
    pydantic = types.ModuleType("pydantic")
    pydantic.__dict__.update(PrivateAttr=lambda default=None: default)
    with mock.patch.dict(sys.modules, {"dagster": dagster, "pydantic": pydantic}):
        sys.modules.pop(MODULE, None)
        try:
            return importlib.import_module(MODULE)
        finally:
            sys.modules.pop(MODULE, None)


class Run:
    """The parts of a Dagster context that the resource uses: the run's log and the tags that are added to the run."""

    def __init__(self) -> None:
        self.messages: list[str] = []
        self.tags: list[dict[str, str]] = []
        self.log = SimpleNamespace(info=self.messages.append)
        self.dagster_run = SimpleNamespace(run_id="run_one")
        self.instance = SimpleNamespace(add_run_tags=lambda run_id, tags: self.tags.append(dict(tags)))

    def records(self) -> list[str]:
        return [message for message in self.messages if message.startswith("group record: ")]


def cluster_that_is(replicas: int) -> Cluster:
    cluster = Cluster()
    for found in cluster.objects.values():
        found.update(desired=replicas, ready=replicas)
    return cluster


class GroupResourceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = resource_module()

    @contextmanager
    def held(self, cluster: Cluster, run: Run) -> Iterator[Any]:
        """The resource of the ingestion group as a job holds it while its steps run."""
        kubernetes, _ = control(cluster)
        settings = Settings(compose_directory=ROOT / "compose", project="stack_project", available_mib=60000, k3s=kubernetes)
        with mock.patch.object(self.module, "current_settings", return_value=settings), mock.patch("scripts.stack.groups.docker", return_value=""):
            resource = self.module.GroupResource(group="ingestion", stop_deadline_seconds=5)
            with resource.yield_for_execution(run) as held:
                yield held

    def desired(self, cluster: Cluster) -> set[int]:
        return {found["desired"] for found in cluster.objects.values()}

    def test_the_stop_step_stops_what_the_run_started_and_the_teardown_then_does_nothing_more(self) -> None:
        cluster, run = cluster_that_is(0), Run()
        with self.held(cluster, run) as resource:
            self.assertEqual({1}, self.desired(cluster))
            self.module.stop_service_group(run, resource)
            self.assertEqual({0}, self.desired(cluster))
            calls = len(cluster.calls)
        self.assertEqual(calls, len(cluster.calls))
        self.assertEqual(1, len(run.records()))
        self.assertEqual(1, len(run.tags))
        self.assertEqual("none", run.tags[0][f"{TAGS}/left_running"])
        self.assertIn("airbyte_server", run.tags[0][f"{TAGS}/stopped"])

    def test_a_pod_that_outlives_the_stop_fails_the_stop_step_by_name_and_the_tags_do_not_say_nothing_is_left(self) -> None:
        cluster, run = cluster_that_is(0), Run()
        cluster.pods = {POD: "Running"}
        with self.held(cluster, run) as resource, self.assertRaisesRegex(GroupError, rf"{POD} \(Running\)"):
            self.module.stop_service_group(run, resource)
        self.assertEqual(1, len(run.records()), "the teardown writes no second record over the failed stop")
        self.assertEqual([{f"{TAGS}/stopped": "none", f"{TAGS}/left_running": "unknown"}], run.tags)

    def test_a_step_that_fails_before_the_stop_step_leaves_the_stop_to_the_teardown(self) -> None:
        cluster, run = cluster_that_is(0), Run()
        with self.assertRaisesRegex(RuntimeError, "planted"), self.held(cluster, run):
            raise RuntimeError("planted")
        self.assertEqual({0}, self.desired(cluster))
        self.assertEqual("none", run.tags[0][f"{TAGS}/left_running"])

    def test_a_group_that_was_up_before_the_job_is_left_running(self) -> None:
        cluster, run = cluster_that_is(1), Run()
        with self.held(cluster, run) as resource:
            self.module.stop_service_group(run, resource)
        self.assertEqual({1}, self.desired(cluster))
        self.assertEqual([{f"{TAGS}/stopped": "none", f"{TAGS}/left_running": "none"}], run.tags)


def group_jobs() -> dict[str, ast.FunctionDef]:
    """Each ``@job`` function of the code location that names the group resource in its decorator."""
    return {
        node.name: node
        for path in sorted(CODE_FOLDER.glob("*.py"))
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.FunctionDef)
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call) and getattr(decorator.func, "id", "") == "job" and "GroupResource" in ast.unparse(decorator)
    }


class GroupJobWiringTest(unittest.TestCase):
    """Dagster is not installed here, so each job's own statements are read: its whole body is the stop step with its other steps wired before it."""

    def test_no_module_that_defines_a_step_postpones_its_annotations(self) -> None:
        """Dagster 1.13.31 compares the annotation of a step's ``context`` as an object, and refuses to load the code location when it is a string."""
        for path in sorted(CODE_FOLDER.glob("*.py")):
            tree = ast.parse(path.read_text())
            has_step = any(
                getattr(getattr(decorator, "func", decorator), "id", "") == "op"
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef)
                for decorator in node.decorator_list
            )
            postponed = any(
                isinstance(node, ast.ImportFrom) and node.module == "__future__" and "annotations" in {item.name for item in node.names} for node in tree.body
            )
            with self.subTest(module=path.name):
                self.assertFalse(has_step and postponed)

    def test_every_group_job_ends_with_the_stop_step_after_its_other_steps(self) -> None:
        jobs = group_jobs()
        self.assertLessEqual({"group_probe_job", "record_load_job"}, set(jobs))
        for name, node in jobs.items():
            with self.subTest(job=name):
                self.assertEqual(1, len(node.body), "one statement, so no step is outside the stop step's inputs")
                call = node.body[0].value if isinstance(node.body[0], ast.Expr) else None
                if not isinstance(call, ast.Call):
                    self.fail("the job's body is not a call of the stop step")
                self.assertEqual(STOP_STEP, getattr(call.func, "id", ""))
                self.assertEqual(["after"], [keyword.arg for keyword in call.keywords])
                self.assertIsInstance(call.keywords[0].value, ast.Call, "the step before it is wired as its input")


if __name__ == "__main__":
    unittest.main()
