"""The local joined-stack runner follows the deploy's order with task files that exist, never builds an image, and checks the admission check and the group
command as the code behaves now.

The runner is not tracked, so each check skips when it is absent. The admission step runs the project's own admission and group commands: both answer from
their arguments and refuse a Spark group before any Docker call, so no Docker, server or stand-in program is used.
"""

from __future__ import annotations

import re
import types
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack.groups import LICENSED_SERVICES
from scripts.stack.limits import load_limits
from tests.fast.runner_import import ABSENT, E2E, import_runner, present

ROOT = Path(__file__).resolve().parents[2]
STEP = re.compile(r"name: (?P<role>\w+)\n        tasks_from: (?P<file>\w+\.yml)\n(?:      vars:\n(?P<vars>(?:        .*\n)+))?")


@unittest.skipUnless(present("stack_joined"), ABSENT)
class JoinedStackSteps(unittest.TestCase):
    def setUp(self) -> None:
        self.runner: Any = import_runner("stack_joined")

    def steps(self, name: str) -> list[tuple[str, str, str]]:
        text = "".join(getattr(self.runner, name)("10.0.0.1"))
        return [(found["role"], found["file"], found["vars"] or "") for found in STEP.finditer(text)]

    def test_every_role_step_names_a_task_file_that_exists(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            for role, file, _ in self.steps(name):
                with self.subTest(order=name, step=f"{role}/{file}"):
                    self.assertTrue((ROOT / "ansible" / "roles" / role / "tasks" / file).is_file())

    def test_the_grafana_password_is_made_after_openbao_started_alone_and_got_its_bootstrap(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            with self.subTest(order=name):
                found = self.steps(name)
                alone = next(index for index, step in enumerate(found) if step[1] == "start.yml" and '["openbao"]' in step[2])
                files = [(role, file) for role, file, _ in found]
                self.assertLess(files.index(("openbao", "prepare.yml")), alone)
                self.assertLess(alone, files.index(("openbao", "bootstrap.yml")))
                self.assertLess(files.index(("openbao", "bootstrap.yml")), files.index(("compose_stack", "grafana_password.yml")))

    def test_every_start_of_the_rest_leaves_out_the_storage_services_and_the_always_on_group_does_not_wait_for_them(self) -> None:
        for name in ("deploy_steps", "old_order_steps"):
            rest = [step for step in self.steps(name) if step[1] == "start.yml" and "compose_stack_start_services: []" in step[2]]
            with self.subTest(order=name):
                self.assertEqual(1, len(rest))
                self.assertIn('compose_stack_start_except: "{{ compose_stack_storage_services }}"', rest[0][2])
        for service in LICENSED_SERVICES:
            self.assertNotIn(service, self.runner.ALWAYS_ON)
            self.assertNotIn(service, self.runner.START_SERVICES)

    def test_the_run_stops_before_any_step_when_a_pinned_image_is_not_on_this_computer(self) -> None:
        asked: list[tuple[str, ...]] = []

        def absent(*arguments: str) -> tuple[int, str]:
            asked.append(arguments)
            return 1, "no such image"

        references = {"IMAGE_DAGSTER": "example/x@sha256:" + "a" * 64}
        with mock.patch.object(self.runner, "committed_references", return_value=references):
            with self.assertRaisesRegex(self.runner.Blocked, "IMAGE_DAGSTER.*not on this computer"):
                self.runner.require_pinned_images(absent)
            self.assertIsNone(self.runner.require_pinned_images(lambda *arguments: (0, "[]")))
        self.assertEqual([("image", "inspect", references["IMAGE_DAGSTER"])], asked)


@unittest.skipUnless(present("stack_joined"), ABSENT)
class JoinedStackAdmission(unittest.TestCase):
    def test_the_boundary_and_the_spark_group_refusal_hold_against_the_real_commands_with_aistor_skipped(self) -> None:
        runner: Any = import_runner("stack_joined")
        limits = load_limits()
        running = sorted(name for name, entry in limits.services.items() if entry.group == "supporting" and name not in LICENSED_SERVICES)
        observed: list[tuple[str, bool, str]] = []

        def expect(case: str, kind: str, expected: str, seen: str, passed: bool) -> bool:
            observed.append((case, passed, seen))
            return passed

        run = types.SimpleNamespace(
            record=types.SimpleNamespace(expect=expect),
            docker=lambda *arguments: (0, "\n".join(running)),
            container=lambda service: "",
            compose=ROOT / "compose",
            project="p2_join3_test",
            environment={},
        )
        runner.admission_cases(run)
        self.assertEqual(3, len(observed))
        self.assertEqual([], [(case, seen) for case, passed, seen in observed if not passed])


@unittest.skipUnless(present("stack_monitoring"), ABSENT)
class MonitoringLocalMode(unittest.TestCase):
    def test_the_local_mode_says_that_the_grafana_step_needs_a_configured_openbao_before_it_runs_it(self) -> None:
        text = (E2E / "stack_monitoring.py").read_text()
        local = text.split("def run_local(", 1)[1].split("\ndef ", 1)[0]
        self.assertLess(local.index("grafana_needs_openbao("), local.index("grafana_password_file("))
        runner: Any = import_runner("stack_monitoring")
        seen: list[tuple[str, bool, str]] = []

        def expect(case: str, kind: str, expected: str, observed: str, passed: bool) -> bool:
            seen.append((case, passed, observed))
            return passed

        record = types.SimpleNamespace(expect=expect)
        self.assertTrue(runner.grafana_needs_openbao(record, Path("/no/such/secrets")))
        self.assertEqual([False], [passed for _, passed, _ in seen])
        self.assertIn("starts no OpenBao", seen[0][2])


if __name__ == "__main__":
    unittest.main()
