"""The time that one playbook run may take is longer than the tasks that come before the Airbyte release plus the release's own limit."""

from __future__ import annotations

import re
import unittest
from typing import Any

from scripts.infra import host
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import ROLES, group_variables

# The tasks of host.yml that run before Helm took close to 1800 s in the live run of Nov 23 2026 (session 7: the run reached the release at about 1500 s
# and 2400 s ended it). The figure is the measured time rounded up, and it is the share of the budget that is not the release.
SECONDS_BEFORE_THE_RELEASE = 1800
RELEASE_FILE = ROLES / "airbyte" / "tasks" / "release.yml"
INSTALL_TASK = "Install or upgrade the release from the pinned chart archive"
TIMEOUT_FORM = re.compile(r"\{\{ airbyte_release_deadline_seconds \| int \+ (\d+) \}\}")


def release_tasks() -> list[dict[str, Any]]:
    document = load_yaml(RELEASE_FILE.read_text())
    found: list[dict[str, Any]] = []

    def walk(tasks: object) -> None:
        for task in tasks if isinstance(tasks, list) else []:
            found.append(task)
            for section in ("block", "rescue", "always"):
                walk(task.get(section))

    walk(document)
    return found


def install_task_seconds() -> int:
    """The time that the install task may take, worked out from the task's own timeout expression and the release deadline."""
    task = next(task for task in release_tasks() if task.get("name") == INSTALL_TASK)
    match = TIMEOUT_FORM.fullmatch(str(task["timeout"]))
    if match is None:
        raise AssertionError(f"the install task's timeout must be the release deadline plus a margin, not {task['timeout']!r}")
    return int(group_variables()["airbyte_release_deadline_seconds"]) + int(match.group(1))


class HostSetupBudgetTest(unittest.TestCase):
    def test_the_host_setup_budget_covers_the_tasks_before_the_release_and_the_release_task_at_its_longest(self) -> None:
        self.assertGreaterEqual(host.HOST_SETUP_TIMEOUT_SECONDS, SECONDS_BEFORE_THE_RELEASE + install_task_seconds())

    def test_the_install_task_is_found_and_its_longest_time_is_the_deadline_plus_the_margin(self) -> None:
        self.assertEqual(int(group_variables()["airbyte_release_deadline_seconds"]) + 120, install_task_seconds())

    def test_each_playbook_has_its_own_named_budget_and_the_connection_step_keeps_its_own(self) -> None:
        self.assertEqual(
            {"host.yml": host.HOST_SETUP_TIMEOUT_SECONDS, "connection.yml": host.CONNECTION_TIMEOUT_SECONDS},
            {name: budget for name, (_, _, budget) in host.PLAYBOOKS.items()},
        )
        self.assertGreater(host.CONNECTION_TIMEOUT_SECONDS, int(group_variables()["airbyte_connection_run_seconds"]))


if __name__ == "__main__":
    unittest.main()
