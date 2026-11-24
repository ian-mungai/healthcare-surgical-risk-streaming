"""The time that the host setup run may take is the sum of the deadlines of its serial tasks plus a tenth, rounded up to five minutes."""

from __future__ import annotations

import math
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.infra import host
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import ROLES, group_variables

MARGIN = 1.1
ROUND_TO_SECONDS = 300
RELEASE_FILE = ROLES / "airbyte" / "tasks" / "release.yml"
INSTALL_TASK = "Install or upgrade the release from the pinned chart archive"
RECOVERY_TASKS = ("Roll the release back to its last deployed revision", "Uninstall the release that never finished its first install or its uninstall")
EXPRESSION = re.compile(r"\{\{ (\w+) \| int(?: \+ (\d+))? \}\}")
RETRIES = re.compile(r"\{\{ \(?(\w+) / (\d+)\)? \| int \}\}|\{\{ \((\w+) \| int / (\d+)\) \| int \}\}")


def tasks_of(path: Path) -> list[dict[str, Any]]:
    """Every task of a task file, with the tasks inside blocks, rescues and always sections, in file order."""
    found: list[dict[str, Any]] = []

    def walk(tasks: object) -> None:
        for task in tasks if isinstance(tasks, list) else []:
            found.append(task)
            for section in ("block", "rescue", "always"):
                walk(task.get(section))

    walk(load_yaml(path.read_text()))
    return found


def task_named(path: Path, name: str) -> dict[str, Any]:
    return next(task for task in tasks_of(path) if task.get("name") == name)


def seconds(expression: object) -> int:
    """A time in seconds from a number or from ``{{ variable | int }}`` or ``{{ variable | int + N }}``, with the variable read from the group variables."""
    if isinstance(expression, int):
        return expression
    match = EXPRESSION.fullmatch(str(expression))
    if match is None:
        raise AssertionError(f"the time must be a number or a variable with an optional margin, not {expression!r}")
    return int(group_variables()[match.group(1)]) + int(match.group(2) or 0)


def tries(expression: object) -> int:
    """The number of tries from ``{{ (variable / N) | int }}`` or ``{{ (variable | int / N) | int }}``."""
    match = RETRIES.fullmatch(str(expression))
    if match is None:
        raise AssertionError(f"the retries must divide a deadline variable by a number, not {expression!r}")
    name, divisor = (match.group(1), match.group(2)) if match.group(1) else (match.group(3), match.group(4))
    return int(int(group_variables()[name]) / int(divisor))


def build_seconds() -> int:
    """The image builds run one after the other (the loop waits for each one), each for up to its async deadline."""
    task = next(task for task in tasks_of(ROLES / "compose_stack" / "tasks" / "build_images.yml") if "async" in task)
    if not int(task["poll"]) > 0:
        raise AssertionError("a build loop with no polling would start every build at once, and the sum would not be the time")
    return seconds(task["async"]) * len(group_variables()["compose_stack_built_images"])


def k3s_seconds() -> int:
    install = seconds(task_named(ROLES / "k3s" / "tasks" / "main.yml", "Install the pinned k3s release")["timeout"])
    wait = task_named(ROLES / "k3s" / "tasks" / "wait_ready.yml", "Wait for the k3s node to be ready")
    one_wait = tries(wait["retries"]) * (seconds(wait["timeout"]) + int(wait["delay"]))
    return install + k3s_wait_runs() * one_wait


def k3s_wait_runs() -> int:
    """How many times the role waits for the node: once after the install and once after the restart that turns secret encryption on."""
    return sum(
        1 for path in (ROLES / "k3s" / "tasks").glob("*.yml") for task in tasks_of(path) if task.get("ansible.builtin.include_tasks") == "wait_ready.yml"
    )


def compose_start_seconds() -> int:
    path = ROLES / "compose_stack" / "tasks" / "start.yml"
    names = ("Start the services", "Recreate the services that run with an older copy of a configuration file, each without its dependencies")
    return sum(seconds(task_named(path, name)["timeout"]) for name in names)


def helm_seconds() -> int:
    """Every timed task of the release file, with the rollback and the uninstall counted once (the status decides which of them runs)."""
    timed = [task for task in tasks_of(RELEASE_FILE) if "timeout" in task]
    recovery = [seconds(task["timeout"]) for task in timed if task["name"] in RECOVERY_TASKS]
    if len(recovery) != 2:
        raise AssertionError("the rollback and the uninstall must both be found")
    return sum(seconds(task["timeout"]) for task in timed if task["name"] not in RECOVERY_TASKS) + max(recovery)


def airbyte_ready_seconds() -> int:
    tasks = tasks_of(ROLES / "airbyte" / "tasks" / "verify.yml")
    waits = ("Wait for every deployment to be available", "Wait for the object store's stateful set to be ready")
    health = next(task for task in tasks if task.get("name") == "Wait for the health path to answer on the local address")
    one_try = int(health["ansible.builtin.uri"]["timeout"]) + int(health["delay"])
    return sum(seconds(task["timeout"]) for task in tasks if task.get("name") in waits) + tries(health["retries"]) * one_try


def parts() -> dict[str, int]:
    return {
        "image builds": build_seconds(),
        "k3s install and waits": k3s_seconds(),
        "Compose start": compose_start_seconds(),
        "Airbyte release": helm_seconds(),
        "Airbyte readiness": airbyte_ready_seconds(),
    }


def required_seconds() -> int:
    return math.ceil(sum(parts().values()) * MARGIN / ROUND_TO_SECONDS) * ROUND_TO_SECONDS


def shortfall(budget: float) -> str | None:
    """What is wrong with a budget for the host setup, or None when it is a whole number of five minutes that covers the sum."""
    if budget < required_seconds():
        return f"{budget} s is less than the {required_seconds()} s that the deadlines of the tasks add up to ({parts()}) plus a tenth"
    if budget % ROUND_TO_SECONDS:
        return f"{budget} s is not a whole number of {ROUND_TO_SECONDS} s"
    return None


class HostSetupBudgetTest(unittest.TestCase):
    def test_the_host_setup_budget_covers_the_deadlines_of_the_serial_tasks_plus_a_tenth_in_whole_five_minutes(self) -> None:
        self.assertIsNone(shortfall(host.HOST_SETUP_TIMEOUT_SECONDS))

    def test_a_budget_below_the_sum_or_not_in_whole_five_minutes_is_found(self) -> None:
        self.assertIsNotNone(shortfall(required_seconds() - ROUND_TO_SECONDS))
        self.assertIsNotNone(shortfall(required_seconds() + 1))
        self.assertIsNone(shortfall(required_seconds()))

    def test_the_sum_counts_three_serial_builds_and_the_release_at_its_longest(self) -> None:
        deadline = int(group_variables()["airbyte_release_deadline_seconds"])
        self.assertEqual(3 * int(group_variables()["compose_stack_build_deadline_seconds"]), build_seconds())
        install = seconds(task_named(RELEASE_FILE, INSTALL_TASK)["timeout"])
        self.assertEqual(deadline + 120, install)
        self.assertGreaterEqual(helm_seconds(), 2 * (deadline + 120), "the recovery and the install both run in a run that finds a pending release")

    def test_the_k3s_node_wait_is_counted_once_after_the_install_and_once_after_the_encryption_restart(self) -> None:
        self.assertEqual(2, k3s_wait_runs())

    def test_each_playbook_has_its_own_named_budget_and_the_connection_step_keeps_its_own(self) -> None:
        self.assertEqual(
            {"host.yml": host.HOST_SETUP_TIMEOUT_SECONDS, "connection.yml": host.CONNECTION_TIMEOUT_SECONDS},
            {name: budget for name, (_, _, budget) in host.PLAYBOOKS.items()},
        )
        self.assertGreater(host.CONNECTION_TIMEOUT_SECONDS, int(group_variables()["airbyte_connection_run_seconds"]))


if __name__ == "__main__":
    unittest.main()
