"""The cases of a replay plan that stands as the streaming control: development cases only, one of them with the arterial waveform.

Failure modes covered here:

- A control plan holds no case with ``SNUADC/ART``, so no compared window can hold an invalid arterial second and the parity gate can never pass.
- A control plan holds a case of a test patient, so a held-out patient is replayed in a development run.
- A case that is not a candidate at all is accepted, so the run fails late, after the images are built, instead of before anything starts.
- The check reports a test patient's facts (its arterial flag), so a held-out value leaves the check.
"""

from __future__ import annotations

import unittest

from scripts.sample.candidates import ARTERIAL_TRACK, CANDIDATES_FILE, plan_case_problems, read_candidates

CANDIDATES = read_candidates(CANDIDATES_FILE)
NO_ARTERIAL_DEVELOPMENT = (908, 5754)
ARTERIAL_DEVELOPMENT = 1147
ARTERIAL_TEST = 565
NO_ARTERIAL_TEST = 5105


class PlanCases(unittest.TestCase):
    def test_a_plan_of_development_cases_with_one_arterial_case_has_no_problem(self) -> None:
        self.assertEqual([], plan_case_problems((908, ARTERIAL_DEVELOPMENT, 5754), CANDIDATES))
        self.assertEqual([], plan_case_problems((ARTERIAL_DEVELOPMENT,), CANDIDATES))

    def test_a_plan_with_no_case_that_has_the_arterial_waveform_is_refused_by_name(self) -> None:
        problems = plan_case_problems(NO_ARTERIAL_DEVELOPMENT, CANDIDATES)
        self.assertEqual(1, len(problems))
        self.assertIn(ARTERIAL_TRACK, problems[0])

    def test_a_case_of_a_test_patient_is_refused_whether_or_not_it_has_the_arterial_waveform(self) -> None:
        for case in (ARTERIAL_TEST, NO_ARTERIAL_TEST):
            problems = plan_case_problems((ARTERIAL_DEVELOPMENT, case), CANDIDATES)
            self.assertEqual(1, len(problems), case)
            self.assertIn(f"case {case}", problems[0])
            self.assertIn("development", problems[0])

    def test_a_case_that_is_no_candidate_is_refused(self) -> None:
        problems = plan_case_problems((ARTERIAL_DEVELOPMENT, 1), CANDIDATES)
        self.assertEqual(1, len(problems))
        self.assertIn("case 1", problems[0])

    def test_a_plan_that_needs_no_arterial_case_is_checked_for_development_cases_only(self) -> None:
        self.assertEqual([], plan_case_problems(NO_ARTERIAL_DEVELOPMENT, CANDIDATES, arterial=False))
        self.assertEqual(1, len(plan_case_problems((908, NO_ARTERIAL_TEST), CANDIDATES, arterial=False)))

    def test_an_empty_plan_is_refused(self) -> None:
        self.assertNotEqual([], plan_case_problems((), CANDIDATES))

    def test_a_test_patient_s_arterial_case_does_not_stand_in_for_a_development_one(self) -> None:
        problems = plan_case_problems((908, ARTERIAL_TEST), CANDIDATES)
        self.assertTrue(any(ARTERIAL_TRACK in problem for problem in problems))
        self.assertTrue(any("development" in problem for problem in problems))

    def test_the_messages_name_no_arterial_fact_of_a_test_patient(self) -> None:
        for case in (ARTERIAL_TEST, NO_ARTERIAL_TEST):
            text = " ".join(plan_case_problems((ARTERIAL_DEVELOPMENT, case), CANDIDATES))
            self.assertNotIn("arterial", text)
            self.assertNotIn(ARTERIAL_TRACK, text)


if __name__ == "__main__":
    unittest.main()
