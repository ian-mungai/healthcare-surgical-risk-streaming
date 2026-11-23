"""Each planted fault in a copy of the CI workflow fails the contract with its own diagnostic.

The contract runs the required job's script for every combination of job results, so the planted loop takes about a minute; it is in the slow
group, and the quick group keeps the control that the real workflow keeps the contract.
"""

from __future__ import annotations

import re
import unittest

from tests.fast.test_ci_workflow import PLANTS, WORKFLOW_TEXT, as_tuple, contract_problems


class WorkflowPlantsTest(unittest.TestCase):
    def assert_plants_are_caught(self, base: str) -> None:
        for label, (before, after, diagnostics) in PLANTS.items():
            with self.subTest(label=label):
                planted = base
                for old, new in zip(as_tuple(before), as_tuple(after), strict=True):
                    self.assertIn(old, planted)
                    if label.startswith("the required job"):
                        self.assertGreater(planted.index(old), planted.index("\n  required:\n"), "a plant of the required job must change the required job")
                    planted = planted.replace(old, new, 1)
                problems = contract_problems(planted)
                for diagnostic in as_tuple(diagnostics):
                    self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")

    def test_a_broken_workflow_fails_the_contract_with_its_own_diagnostic(self) -> None:
        self.assert_plants_are_caught(WORKFLOW_TEXT)

    def test_every_plant_lands_in_its_own_job_without_the_comments_of_the_workflow(self) -> None:
        """A plant is located by its job's keys, never by a comment: with any comment after a timeout removed, each plant still changes its own job."""
        bare = re.sub(r"(timeout-minutes: \d+) #.*", r"\1", WORKFLOW_TEXT)
        self.assertEqual(contract_problems(bare), [], "the control: the workflow without those comments keeps the contract")
        self.assert_plants_are_caught(bare)
