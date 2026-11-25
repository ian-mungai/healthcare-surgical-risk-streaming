"""The pull task's own end fits inside Ansible's timeout for the task, with a stated margin, worked out from the task file.

After its own deadline the shell sends TERM to the pull's group, waits for the grace, sends KILL and waits for the group to be gone, and only then removes the
login folder. Each wait polls once a second, so each can run one second over. All of that must be over before Ansible's timeout, which cannot clean up for it.
"""

from __future__ import annotations

import re
import unittest
from typing import Any

import jinja2

from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import ROLES, group_variables

TASK_FILE = ROLES / "compose_stack" / "tasks" / "pull_images.yml"
POLL_SECONDS = 1
POLLED_WAITS = 3
MARGIN_SECONDS = 4
SCRIPT_LINE = re.compile(r"^\s*(deadline|grace|reap)=(.+)$", re.MULTILINE)


def pull_block() -> dict[str, Any]:
    tasks: Any = load_yaml(TASK_FILE.read_text())
    return next(task for task in tasks if "block" in task)


def shell_task() -> dict[str, Any]:
    return next(task for task in pull_block()["block"] if "ansible.builtin.shell" in task)


def rendered_numbers(timeout: int) -> dict[str, int]:
    """The deadline, the grace and the reap wait that the shell script holds when the task's timeout is ``timeout``."""
    context = {**pull_block()["vars"], "compose_stack_pull_deadline_seconds": timeout}
    script = shell_task()["ansible.builtin.shell"]["cmd"]
    found = {name: int(jinja2.Template(expression).render(context)) for name, expression in SCRIPT_LINE.findall(script)}
    if set(found) != {"deadline", "grace", "reap"}:
        raise AssertionError(f"the shell script sets {sorted(found)}")
    return found


class PullBudgetTest(unittest.TestCase):
    def test_the_shells_own_end_fits_inside_the_tasks_timeout_with_the_stated_margin_at_the_real_timeout_and_at_the_smallest_one_the_role_accepts(self) -> None:
        reserve = int(pull_block()["vars"]["compose_stack_pull_reserve_seconds"])
        for timeout in (int(group_variables()["compose_stack_pull_deadline_seconds"]), reserve + 1):
            with self.subTest(timeout=timeout):
                numbers = rendered_numbers(timeout)
                worst_end = numbers["deadline"] + POLLED_WAITS * POLL_SECONDS + numbers["grace"] + numbers["reap"]
                self.assertGreaterEqual(numbers["deadline"], 1)
                self.assertLessEqual(worst_end + MARGIN_SECONDS, timeout, f"the shell can end at {worst_end} s of a {timeout} s timeout")

    def test_the_task_timeout_is_the_pull_deadline_and_a_deadline_too_small_for_the_reserve_is_refused_in_the_block(self) -> None:
        self.assertEqual("{{ compose_stack_pull_deadline_seconds | int }}", shell_task()["timeout"])
        refusals = [task for task in pull_block()["block"] if "ansible.builtin.assert" in task]
        conditions = [str(condition) for task in refusals for condition in task["ansible.builtin.assert"]["that"]]
        self.assertTrue(
            any("compose_stack_pull_deadline_seconds" in condition and "compose_stack_pull_reserve_seconds" in condition for condition in conditions),
            "no refusal of a deadline that leaves no time for the reserve",
        )


if __name__ == "__main__":
    unittest.main()
