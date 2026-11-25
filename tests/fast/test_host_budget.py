"""The time that the host setup run may take is the sum of the deadlines of every task on the path of ``host.yml`` plus a tenth, rounded up to five minutes.

The path is read from the playbook: its roles, and from each role's ``main.yml`` every ``include_tasks`` and ``include_role``, once per call, so a task that two
calls reach counts twice. Ansible's attempts count too: a task with ``until`` runs ``retries + 1`` times (3 retries when none are set) and waits ``delay``
between them.
"""

from __future__ import annotations

import math
import re
import unittest
from collections import Counter
from dataclasses import dataclass
from typing import Any

import jinja2

from scripts.infra import host
from scripts.project_config import ROOT
from scripts.stack.yaml_text import load_yaml
from tests.fast.ansible_paths import ROLES, group_variables

ANSIBLE = ROOT / "ansible"
MARGIN = 1.1
ROUND_TO_SECONDS = 300
DEFAULT_RETRIES = 3
DEFAULT_DELAY = 5
RELEASE_FILE = "release.yml"
INSTALL_TASK = "Install or upgrade the release from the pinned chart archive"
RECOVERY_TASKS = ("Roll the release back to its last deployed revision", "Uninstall the release that never finished its first install or its uninstall")
MODULE_TIMEOUT = ("ansible.builtin.uri", "ansible.builtin.get_url")
APT = "ansible.builtin.apt"
INCLUDES = ("include_tasks", "include_role")
UNSUPPORTED_INCLUDES = ("include", "import_tasks", "import_role")
# Task files that no include of the two playbooks reaches (the role's own main.yml when the callers include its parts one by one).
NOT_REACHED = {("openbao", "main.yml")}


@dataclass(frozen=True)
class Occurrence:
    """One run of one task: the role and file it is in, the task, and every task of its file (to find what a loop runs over)."""

    role: str
    file: str
    task: dict[str, Any]
    siblings: tuple[dict[str, Any], ...]


def flatten(tasks: object) -> list[dict[str, Any]]:
    """Every task of a task list, with the tasks inside blocks, rescues and always sections, in file order."""
    found: list[dict[str, Any]] = []
    for task in tasks if isinstance(tasks, list) else []:
        found.append(task)
        for section in ("block", "rescue", "always"):
            found.extend(flatten(task.get(section)))
    return found


def module_name(task: dict[str, Any], names: tuple[str, ...]) -> str | None:
    """The key of a task that is one of the names, with or without the ``ansible.builtin.`` prefix."""
    return next((key for key in task if key.removeprefix("ansible.builtin.") in names), None)


def occurrences(role: str, file: str, tasks: list[dict[str, Any]]) -> list[Occurrence]:
    """The runs of the tasks and, for each include, the runs of the file it includes, in order."""
    found: list[Occurrence] = []
    siblings = tuple(tasks)
    for task in tasks:
        found.append(Occurrence(role, file, task, siblings))
        if (key := module_name(task, UNSUPPORTED_INCLUDES)) is not None:
            raise AssertionError(
                f"{role}/{file}: {key} is not followed by this test, so the sum could miss the tasks it brings; use include_tasks or include_role"
            )
        if (key := module_name(task, INCLUDES)) is None:
            continue
        target = task[key]
        if key.removeprefix("ansible.builtin.") == "include_tasks":
            name = target["file"] if isinstance(target, dict) else target
            found.extend(file_occurrences(role, str(name)))
        else:
            found.extend(file_occurrences(str(target["name"]), str(target.get("tasks_from", "main.yml"))))
    return found


def file_occurrences(role: str, file: str) -> list[Occurrence]:
    if "{{" in file or "{{" in role:
        raise AssertionError(f"{role}/{file}: the name of an included file or role must be fixed text, so that the sum finds it")
    return occurrences(role, file, flatten(load_yaml((ROLES / role / "tasks" / file).read_text())))


def playbook_occurrences(playbook: str) -> list[Occurrence]:
    """The runs of every task of the playbook in order: its own tasks and each role's main.yml, whatever they include."""
    found: list[Occurrence] = []
    plays = load_yaml((ANSIBLE / playbook).read_text())
    if not isinstance(plays, list):
        raise TypeError("expected a list of plays")
    for play in plays:
        found += occurrences("", playbook, flatten(play.get("pre_tasks")))
        for role in play.get("roles", []):
            found += file_occurrences(str(role["role"] if isinstance(role, dict) else role), "main.yml")
        found += occurrences("", playbook, flatten(play.get("tasks")))
        found += occurrences("", playbook, flatten(play.get("post_tasks")))
    return found


def variables() -> dict[str, Any]:
    """The group variables and the defaults of the roles (the group variables win)."""
    found: dict[str, Any] = {}
    for path in sorted(ROLES.glob("*/defaults/main.yml")):
        defaults = load_yaml(path.read_text())
        if not isinstance(defaults, dict):
            raise TypeError("expected a mapping")
        found |= defaults
    return found | group_variables()


def number(expression: object) -> int:
    """A whole number from a number or from a template (``{{ x | int + 120 }}``, ``{{ (x / 25) | int }}``) worked out with the group variables."""
    if isinstance(expression, int):
        return expression
    try:
        return int(
            jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=jinja2.select_autoescape(default_for_string=False))
            .from_string(str(expression))
            .render(variables())
        )
    except (jinja2.TemplateError, ValueError) as error:
        raise AssertionError(f"{expression!r} is not a number or a template of the group variables: {error}") from None


def loop_length(occurrence: Occurrence) -> int:
    """How many times a task runs for its loop: the length of the list, or of the loop of the task that registered ``<name>.results``."""
    task = occurrence.task
    if "loop" not in task:
        return 1
    loop = task["loop"]
    if isinstance(loop, list):
        return len(loop)
    results = re.fullmatch(r"\{\{ (\w+)\.results \}\}", str(loop))
    if results is not None:
        source = next(sibling for sibling in occurrence.siblings if sibling.get("register") == results.group(1))
        return loop_length(Occurrence(occurrence.role, occurrence.file, source, occurrence.siblings))
    variable = re.fullmatch(r"\{\{ (\w+) \}\}", str(loop))
    if variable is None:
        raise AssertionError(f"{occurrence.role}/{occurrence.file}: the loop {loop!r} must be a list, a group variable or the results of a looped task")
    return len(variables()[variable.group(1)])


def one_try_seconds(task: dict[str, Any]) -> int | None:
    """The deadline of one try: the async deadline when the task waits for it, else the task's timeout, else the timeout of a download or request."""
    if "async" in task:
        if not number(task.get("poll", 0)) > 0:
            raise AssertionError(f"{task['name']}: an async task with no polling starts and does not wait, and its deadline would not be time spent")
        return number(task["async"])
    if "timeout" in task:
        return number(task["timeout"])
    key = module_name(task, tuple(name.removeprefix("ansible.builtin.") for name in MODULE_TIMEOUT))
    if key is not None and isinstance(task[key], dict) and "timeout" in task[key]:
        return number(task[key]["timeout"])
    return None


def attempts(task: dict[str, Any]) -> int:
    """How often Ansible runs a task: once, or with ``until`` the first run and each retry (3 when ``retries`` is not set)."""
    if "until" not in task:
        return 1
    return 1 + (max(0, number(task["retries"])) if "retries" in task else DEFAULT_RETRIES)


def task_seconds(occurrence: Occurrence) -> int:
    """The longest one run of a task takes: its tries, each for its deadline, and the waits between them, for each item of its loop."""
    task = occurrence.task
    one = one_try_seconds(task)
    if one is None and "until" not in task:
        return 0
    tries = attempts(task)
    delay = number(task.get("delay", DEFAULT_DELAY)) if "delay" in task or "until" in task else 0
    return loop_length(occurrence) * (tries * (one or 0) + (tries - 1) * delay)


def total_seconds(found: list[Occurrence]) -> dict[str, int]:
    """The seconds of each role's tasks on the path, with the rollback and the uninstall counted once (the release's status decides which of them runs)."""
    recovery = [task_seconds(item) for item in found if item.file == RELEASE_FILE and item.task.get("name") in RECOVERY_TASKS]
    if len(recovery) not in (0, 2):
        raise AssertionError("the rollback and the uninstall must both be found, or neither (a playbook that does not run the release)")
    sums: Counter[str] = Counter()
    for item in found:
        if not (item.file == RELEASE_FILE and item.task.get("name") in RECOVERY_TASKS):
            sums[item.role or "playbook"] += task_seconds(item)
    if recovery:
        sums["airbyte"] += max(recovery)
    return dict(sums)


def parts(playbook: str = "host.yml") -> dict[str, int]:
    return total_seconds(playbook_occurrences(playbook))


def required_seconds(playbook: str = "host.yml") -> int:
    return math.ceil(sum(parts(playbook).values()) * MARGIN / ROUND_TO_SECONDS) * ROUND_TO_SECONDS


def shortfall(budget: float, playbook: str = "host.yml") -> str | None:
    """What is wrong with a budget for a playbook, or None when it is a whole number of five minutes that covers the sum."""
    if budget < required_seconds(playbook):
        return f"{budget} s is less than the {required_seconds(playbook)} s that the deadlines of the tasks add up to ({parts(playbook)}) plus a tenth"
    if budget % ROUND_TO_SECONDS:
        return f"{budget} s is not a whole number of {ROUND_TO_SECONDS} s"
    return None


def runs_of_file(found: list[Occurrence], role: str, file: str) -> int:
    """How many times a task file runs: the runs of its first task."""
    first = flatten(load_yaml((ROLES / role / "tasks" / file).read_text()))[0]["name"]
    return sum(1 for item in found if (item.role, item.file) == (role, file) and item.task.get("name") == first)


def one(found: list[Occurrence], file: str, name: str) -> Occurrence:
    return next(item for item in found if item.file == file and item.task.get("name") == name)


class HostSetupBudgetTest(unittest.TestCase):
    def test_the_host_setup_budget_covers_the_deadlines_of_every_task_on_the_playbook_path_plus_a_tenth_in_whole_five_minutes(self) -> None:
        self.assertIsNone(shortfall(host.HOST_SETUP_TIMEOUT_SECONDS))

    def test_the_connection_step_budget_covers_the_deadlines_of_every_task_on_its_playbook_path_plus_a_tenth_in_whole_five_minutes(self) -> None:
        self.assertIsNone(shortfall(host.CONNECTION_TIMEOUT_SECONDS, "connection.yml"))

    def test_the_aistor_step_budget_covers_the_deadlines_of_every_task_on_its_playbook_path_plus_a_tenth_in_whole_five_minutes(self) -> None:
        self.assertIsNone(shortfall(host.STORAGE_TIMEOUT_SECONDS, "storage.yml"))

    def test_a_budget_below_the_sum_or_not_in_whole_five_minutes_is_found(self) -> None:
        self.assertIsNotNone(shortfall(required_seconds() - ROUND_TO_SECONDS))
        self.assertIsNotNone(shortfall(required_seconds() + 1))
        self.assertIsNone(shortfall(required_seconds()))

    def test_the_required_budget_is_the_sum_plus_a_tenth_rounded_up_to_five_minutes(self) -> None:
        total = sum(parts().values())
        self.assertGreater(total, 0)
        self.assertGreaterEqual(required_seconds(), total + total / 10)
        self.assertLess(required_seconds(), total + total / 10 + 300)
        self.assertEqual(0, required_seconds() % 300)

    def test_a_task_that_two_calls_reach_counts_twice_and_the_counts_come_from_the_calls_in_the_files(self) -> None:
        found = playbook_occurrences("host.yml")
        compose = (ROLES / "compose_stack" / "tasks" / "main.yml").read_text()
        self.assertEqual(
            2, compose.count("include_tasks: start.yml"), "the Compose role starts PostgreSQL alone, then the remaining services: AIStor starts in its own step"
        )
        self.assertEqual(compose.count("include_tasks: start.yml"), runs_of_file(found, "compose_stack", "start.yml"))
        storage = (ROLES / "compose_stack" / "tasks" / "storage.yml").read_text()
        self.assertEqual(1, storage.count("include_tasks: start.yml"))
        self.assertEqual(1, runs_of_file(playbook_occurrences("storage.yml"), "compose_stack", "start.yml"))
        k3s = "".join(path.read_text() for path in (ROLES / "k3s" / "tasks").glob("*.yml"))
        self.assertEqual(k3s.count("include_tasks: wait_ready.yml"), runs_of_file(found, "k3s", "wait_ready.yml"))
        self.assertEqual(2, runs_of_file(found, "k3s", "wait_ready.yml"), "once after the install and once after the restart that turns secret encryption on")
        self.assertEqual(1, runs_of_file(found, "airbyte", RELEASE_FILE))
        self.assertEqual(2, runs_of_file(found, "airbyte", "cluster.yml"), "the Compose role and the Airbyte role each include it")

    def test_the_compose_recreations_across_the_start_calls_are_in_the_sum_of_their_playbook(self) -> None:
        deadline = int(group_variables()["compose_stack_start_deadline_seconds"])
        for playbook, calls in (("host.yml", 2), ("storage.yml", 1)):
            found = playbook_occurrences(playbook)
            recreate = [item for item in found if item.file == "start.yml" and str(item.task.get("name")).startswith("Recreate the services")]
            start = [item for item in found if item.file == "start.yml" and item.task.get("name") == "Start the services"]
            with self.subTest(playbook):
                self.assertEqual(calls, len(recreate))
                self.assertEqual(calls, len(start))
                self.assertEqual(2 * calls * deadline, sum(task_seconds(item) for item in recreate + start))

    def test_an_until_task_counts_its_first_attempt_and_each_retry_with_its_delay(self) -> None:
        found = playbook_occurrences("host.yml")
        wait = one(found, "wait_ready.yml", "Wait for the k3s node to be ready")
        retries = int(group_variables()["k3s_ready_deadline_seconds"] / 25)
        self.assertEqual(12, retries)
        self.assertEqual((retries + 1) * 30 + retries * 5, task_seconds(wait), "13 attempts of 30 s and 12 delays of 5 s")
        unset = {"name": "x", "timeout": 10, "until": "false", "delay": 2}
        self.assertEqual(4 * 10 + 3 * 2, task_seconds(Occurrence("r", "f", unset, ())), "Ansible retries 3 times when until has no retries")

    def test_the_timeout_inside_a_request_or_a_download_is_the_deadline_of_its_task(self) -> None:
        found = playbook_occurrences("host.yml")
        health = one(found, "verify.yml", "Wait for the health path to answer on the local address")
        retries = int(group_variables()["airbyte_health_deadline_seconds"] / 5)
        self.assertEqual((retries + 1) * 10 + retries * 5, task_seconds(health), "25 requests of 10 s and 24 delays of 5 s")
        key = one(found, "main.yml", "Install Docker's package signing key, checked against its pinned hash")
        self.assertEqual(60, task_seconds(key))

    def test_a_loop_runs_a_timed_task_for_each_item_and_the_build_loop_waits_for_each_build(self) -> None:
        found = playbook_occurrences("host.yml")
        build = one(found, "build_images.yml", "Build each image whose folder differs from the one it was built from, or that does not exist")
        images = len(group_variables()["compose_stack_built_images"])
        self.assertEqual(images * int(group_variables()["compose_stack_build_deadline_seconds"]), task_seconds(build))
        listed = Occurrence("r", "f", {"name": "x", "timeout": 7, "loop": [1, 2, 3]}, ())
        self.assertEqual(21, task_seconds(listed))

    def test_an_async_task_that_does_not_wait_for_its_deadline_is_refused_because_its_deadline_is_not_time_spent(self) -> None:
        waiting = Occurrence("r", "f", {"name": "x", "async": 30, "poll": 2}, ())
        self.assertEqual(30, task_seconds(waiting))
        for task in ({"name": "x", "async": 30, "poll": 0}, {"name": "x", "async": 30}):
            with self.subTest(task=task), self.assertRaises(AssertionError):
                task_seconds(Occurrence("r", "f", task, ()))

    def test_the_cold_package_installs_have_a_task_deadline_that_is_counted(self) -> None:
        found = playbook_occurrences("host.yml")
        installs = [item for item in found if APT in item.task]
        self.assertEqual({"docker", "metadata_guard"}, {item.role for item in installs}, "every package install on the host path is checked")
        for item in installs:
            with self.subTest(task=item.task["name"]):
                self.assertIn("timeout", item.task, "a cold install downloads packages, and only the lock wait has a deadline without it")
                self.assertGreater(task_seconds(item), int(item.task[APT]["lock_timeout"]))

    def test_the_rollback_and_the_uninstall_count_once_and_the_install_counts_beside_the_recovery(self) -> None:
        found = playbook_occurrences("host.yml")
        deadline = int(group_variables()["airbyte_release_deadline_seconds"])
        self.assertEqual(deadline + 120, task_seconds(one(found, RELEASE_FILE, INSTALL_TASK)))
        release = sum(task_seconds(item) for item in found if item.file == RELEASE_FILE)
        counted = total_seconds([item for item in found if item.file == RELEASE_FILE])["airbyte"]
        self.assertEqual(release - (deadline + 120), counted, "one of the two recovery tasks is left out")
        self.assertGreaterEqual(counted, 2 * (deadline + 120))

    def test_a_renamed_recovery_task_is_found_missing_and_not_counted_as_an_ordinary_task(self) -> None:
        found = [item for item in playbook_occurrences("host.yml") if item.file == RELEASE_FILE and item.task.get("name") != RECOVERY_TASKS[0]]
        with self.assertRaises(AssertionError):
            total_seconds(found)

    def test_an_include_that_the_walk_cannot_follow_is_refused_and_not_skipped(self) -> None:
        for task in ({"ansible.builtin.import_tasks": "x.yml"}, {"ansible.builtin.include_tasks": "{{ name }}.yml"}, {"include": "x.yml"}):
            with self.subTest(task=task), self.assertRaises(AssertionError):
                occurrences("docker", "main.yml", [{"name": "x", **task}])
        with self.assertRaises(AssertionError):
            number("{{ no_such_variable }}")

    def test_every_task_file_of_the_roles_is_reached_by_one_of_the_three_playbooks(self) -> None:
        reached = {(item.role, item.file) for playbook in ("host.yml", "storage.yml", "connection.yml") for item in playbook_occurrences(playbook) if item.role}
        roles = {role for role, _ in reached}
        present = {(path.parent.parent.name, path.name) for role in roles for path in (ROLES / role / "tasks").glob("*.yml")}
        self.assertEqual(
            NOT_REACHED, present - reached, "a task file that no playbook includes is never run, or is included in a way that the walk does not follow"
        )

    def test_each_playbook_has_its_own_named_budget_and_the_connection_step_keeps_its_own(self) -> None:
        self.assertEqual(
            {"host.yml": host.HOST_SETUP_TIMEOUT_SECONDS, "storage.yml": host.STORAGE_TIMEOUT_SECONDS, "connection.yml": host.CONNECTION_TIMEOUT_SECONDS},
            {name: budget for name, (_, _, budget) in host.PLAYBOOKS.items()},
        )
        self.assertGreater(host.CONNECTION_TIMEOUT_SECONDS, int(group_variables()["airbyte_connection_run_seconds"]))


if __name__ == "__main__":
    unittest.main()
