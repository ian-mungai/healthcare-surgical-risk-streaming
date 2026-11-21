"""The limits file is the one place that gives a service its group, so each Compose service file carries the profile of that group and nothing else.

The first group reads the real files. The second plants one defect at a time in a copy of a service and expects the checker to name that defect.
"""

from __future__ import annotations

import copy
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.groups import ALWAYS_ON_GROUPS, profile_of
from scripts.stack.limits import Limits, load_limits, parse_limits
from scripts.stack.yaml_text import load_yaml

SERVICES = Path(__file__).resolve().parents[2] / "compose" / "services"

GOOD_LIMITS = """\
host_reserve_mib: 4096
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1024
  batch_worker:
    runtime: compose
    group: batch
    memory_limit_mib: 2048
  batch_helper:
    runtime: compose
    group: batch
    memory_limit_mib: 512
  training_job:
    runtime: compose
    group: training
    memory_limit_mib: 512
"""


def load(path: Path) -> dict[str, Any]:
    document = load_yaml(path.read_text())
    if not isinstance(document, dict):
        raise TypeError(f"{path.name} is not a mapping")
    return document


def dependencies(service: dict[str, Any]) -> list[str]:
    declared = service.get("depends_on") or []
    return list(declared) if isinstance(declared, list | dict) else []


def profile_problems(name: str, document: dict[str, Any], limits: Limits) -> list[str]:
    """Each way that a service file departs from the group the limits file gives it: a wrong profile, or a dependency that a group stop would break."""
    service = document["services"][name]
    group = limits.services[name].group
    expected = [] if profile_of(group) is None else [group]
    problems = []
    if list(service.get("profiles") or []) != expected:
        problems.append(f"{name}: profiles must be {expected} for the group {group}, found {list(service.get('profiles') or [])}")
    for target in dependencies(service):
        other = limits.services.get(target)
        if other is not None and other.group != group and other.group not in ALWAYS_ON_GROUPS:
            problems.append(f"{name}: depends on {target} of the group {other.group}, so stopping that group would break this service")
        if other is not None and group in ALWAYS_ON_GROUPS and other.group not in ALWAYS_ON_GROUPS:
            problems.append(f"{name}: an always-on service depends on {target} of the group {other.group}")
    return problems


class RealFilesTest(unittest.TestCase):
    def test_every_service_file_carries_the_profile_of_its_group_and_depends_only_on_its_own_group_or_an_always_on_one(self) -> None:
        limits = load_limits()
        files = sorted(SERVICES.glob("*.yml"))
        self.assertTrue(files)
        for path in files:
            with self.subTest(service=path.stem):
                self.assertEqual([], profile_problems(path.stem, load(path), limits))


class PlantedDefectTest(unittest.TestCase):
    def document(self, name: str, **service: Any) -> dict[str, Any]:
        return {"services": {name: dict(service)}}

    def test_a_service_of_a_profile_group_without_its_profile_is_named(self) -> None:
        problems = profile_problems("batch_worker", self.document("batch_worker"), parse_limits(GOOD_LIMITS))
        self.assertEqual(1, len(problems))
        self.assertIn("profiles must be ['batch']", problems[0])

    def test_a_service_of_a_profile_group_with_another_groups_profile_is_named(self) -> None:
        problems = profile_problems("batch_worker", self.document("batch_worker", profiles=["training"]), parse_limits(GOOD_LIMITS))
        self.assertEqual(1, len(problems))
        self.assertIn("found ['training']", problems[0])

    def test_a_service_of_a_profile_group_with_an_extra_profile_is_named(self) -> None:
        problems = profile_problems("batch_worker", self.document("batch_worker", profiles=["batch", "debug"]), parse_limits(GOOD_LIMITS))
        self.assertEqual(1, len(problems))

    def test_an_always_on_service_with_a_profile_is_named_because_it_would_not_start_with_the_stack(self) -> None:
        problems = profile_problems("postgres", self.document("postgres", profiles=["supporting"]), parse_limits(GOOD_LIMITS))
        self.assertEqual(1, len(problems))
        self.assertIn("profiles must be []", problems[0])

    def test_the_good_cases_have_no_problem(self) -> None:
        limits = parse_limits(GOOD_LIMITS)
        self.assertEqual([], profile_problems("postgres", self.document("postgres"), limits))
        self.assertEqual(
            [], profile_problems("batch_worker", self.document("batch_worker", profiles=["batch"], depends_on={"postgres": {}, "batch_helper": {}}), limits)
        )

    def test_a_dependency_on_another_profile_group_is_named_in_the_list_and_the_mapping_form(self) -> None:
        for depends_on in (["training_job"], {"training_job": {"condition": "service_healthy"}}):
            with self.subTest(form=type(depends_on).__name__):
                problems = profile_problems(
                    "batch_worker", self.document("batch_worker", profiles=["batch"], depends_on=copy.deepcopy(depends_on)), parse_limits(GOOD_LIMITS)
                )
                self.assertEqual(1, len(problems))
                self.assertIn("training_job of the group training", problems[0])

    def test_an_always_on_service_that_depends_on_a_profile_group_is_named(self) -> None:
        problems = profile_problems("postgres", self.document("postgres", depends_on=["batch_worker"]), parse_limits(GOOD_LIMITS))
        self.assertTrue(any("always-on service depends on batch_worker" in problem for problem in problems))


if __name__ == "__main__":
    unittest.main()
