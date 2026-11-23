"""The headroom check reads every job of one hosted run and names a job over 80% of its timeout, a job that is missing and a job with no limit.

Failure modes covered here, each with a planted case and a control:

- A job that used more than 80% of its limit passes, or one at exactly 80% fails.
- A job of the workflow that the run does not list (a guard shard that never started) passes.
- A job with no timeout, or a job that the workflow does not define, passes.
- The check reads the first attempt of a rerun, a page that it did not ask for or a run other than its own.
- A command that fails (no read permission, no network) is taken for a pass: tests/slow/test_headroom_program.py runs the program against a stand-in.
"""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

import yaml

from scripts.quality import headroom
from tests.fast.git_fixture import ROOT

START = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
EXPECTED = headroom.expected_limits(WORKFLOW)
HEADROOM_NAME = "No job of this run is near its time limit"
GUARDS_LEG = "Planted faults through the real commit path, shard 3 of 8"


def api_job(name: str, minutes: float | None, *, status: str = "completed") -> dict[str, Any]:
    """A job as the jobs API lists it: its start and, once it is done, its end."""
    finished = None if minutes is None else (START + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
    return {"name": name, "status": status, "conclusion": "success", "started_at": START.isoformat().replace("+00:00", "Z"), "completed_at": finished}


def run_of(minutes: dict[str, float] | None = None) -> list[dict[str, Any]]:
    """Every job that the workflow defines, each at a quarter of its limit unless ``minutes`` names another duration."""
    return [api_job(name, (minutes or {}).get(name, (limit or 1) / 4)) for name, limit in EXPECTED.items()]


class ExpectedLimitsTest(unittest.TestCase):
    def test_every_job_and_every_guard_shard_has_its_own_name_and_limit(self) -> None:
        self.assertEqual(len(EXPECTED), 14, sorted(EXPECTED))
        self.assertEqual(EXPECTED[GUARDS_LEG], 25)
        self.assertEqual(
            [name for name in EXPECTED if name.startswith("Planted faults")],
            [f"Planted faults through the real commit path, shard {n} of 8" for n in range(1, 9)],
        )
        self.assertTrue(all(limit for limit in EXPECTED.values()))

    def test_a_job_without_a_timeout_has_no_limit(self) -> None:
        workflow = {"jobs": {"a": {"name": "A job"}, "b": {"timeout-minutes": 5}}}
        self.assertEqual(headroom.expected_limits(workflow), {"A job": None, "b": 5})

    def test_a_matrix_of_two_keys_gives_every_combination(self) -> None:
        workflow = {
            "jobs": {"a": {"name": "Leg ${{ matrix.x }}-${{ matrix.y }}", "timeout-minutes": 5, "strategy": {"matrix": {"x": [1, 2], "y": ["p", "q"]}}}}
        }
        self.assertEqual(sorted(headroom.expected_limits(workflow)), ["Leg 1-p", "Leg 1-q", "Leg 2-p", "Leg 2-q"])


class ProblemsTest(unittest.TestCase):
    def test_a_run_with_every_job_well_inside_its_limit_has_no_problem(self) -> None:
        self.assertEqual(headroom.problems(EXPECTED, run_of()), [])

    def test_a_job_over_80_percent_of_its_limit_is_named_and_one_at_80_percent_is_not(self) -> None:
        self.assertEqual(headroom.problems(EXPECTED, run_of({GUARDS_LEG: 20.0})), [])
        problems = headroom.problems(EXPECTED, run_of({GUARDS_LEG: 20.1}))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn(GUARDS_LEG, problems[0])
        self.assertIn("20.1 minutes", problems[0])
        self.assertIn("25 minutes", problems[0])

    def test_a_whole_job_time_counts_not_the_time_of_its_steps(self) -> None:
        """The control of the plant above: the time is the job's start to its end, so a long setup or a cache save counts."""
        job = api_job(GUARDS_LEG, 21.0)
        job["steps"] = [{"name": "one short step", "started_at": job["started_at"], "completed_at": job["started_at"]}]
        jobs = [job, *(other for other in run_of() if other["name"] != GUARDS_LEG)]
        self.assertEqual(len(headroom.problems(EXPECTED, jobs)), 1)

    def test_a_guard_shard_that_the_run_does_not_list_is_named(self) -> None:
        problems = headroom.problems(EXPECTED, [job for job in run_of() if job["name"] != GUARDS_LEG])
        self.assertEqual(problems, [f"{GUARDS_LEG}: it is not among the jobs of the run"])

    def test_a_job_the_workflow_does_not_define_has_no_limit(self) -> None:
        problems = headroom.problems(EXPECTED, [*run_of(), api_job("A job added by a plan", 1.0)])
        self.assertEqual(problems, ["A job added by a plan: the workflow gives it no timeout"])

    def test_a_job_with_no_timeout_in_the_workflow_is_named(self) -> None:
        limits = {**EXPECTED, "Hooks on every file and the commit messages": None}
        problems = headroom.problems(limits, run_of())
        self.assertEqual(problems, ["Hooks on every file and the commit messages: the workflow gives it no timeout"])

    def test_the_headroom_job_itself_is_still_running_and_is_not_measured(self) -> None:
        """The headroom job reads the run while it runs itself: its own time is not known yet, and it is not missing."""
        jobs = [api_job(job["name"], None, status="in_progress") if job["name"] == HEADROOM_NAME else job for job in run_of()]
        self.assertEqual(headroom.problems(EXPECTED, jobs, HEADROOM_NAME), [])

    def test_another_job_still_running_is_named_because_its_whole_time_is_not_known(self) -> None:
        """Finding 2: the required job can still run when the headroom job reads the run; it is judged only once it has ended."""
        jobs = [api_job(job["name"], None, status="in_progress") if job["name"] == "Every job of this run passed" else job for job in run_of()]
        problems = headroom.problems(EXPECTED, jobs, HEADROOM_NAME)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("Every job of this run passed: it was still running", problems[0])

    def test_every_problem_is_named_in_one_report(self) -> None:
        jobs = [job for job in run_of({GUARDS_LEG: 24.0}) if not job["name"].endswith("shard 5 of 8")]
        problems = headroom.problems(EXPECTED, [*jobs, api_job("Unknown", 1.0)])
        self.assertEqual(len(problems), 3, problems)


class PagesTest(unittest.TestCase):
    def test_pages_that_the_api_client_prints_one_after_the_other_are_read_as_one_list(self) -> None:
        pages = json.dumps({"total_count": 3, "jobs": [{"name": "a"}, {"name": "b"}]}) + "\n" + json.dumps({"total_count": 3, "jobs": [{"name": "c"}]}) + "\n"
        self.assertEqual([job["name"] for job in headroom.read_jobs(pages)], ["a", "b", "c"])

    def test_text_that_is_not_pages_of_jobs_is_an_error(self) -> None:
        for text in ("", "not json", json.dumps({"message": "Not Found"}), json.dumps([1, 2])):
            with self.subTest(text=text), self.assertRaises(SystemExit):
                headroom.read_jobs(text)

    def test_the_command_asks_for_the_latest_attempt_of_this_run_only(self) -> None:
        arguments = headroom.api_arguments("owner/repository", "4242")
        self.assertEqual(arguments[:2], ["api", "--paginate"])
        self.assertEqual(arguments[2], "repos/owner/repository/actions/runs/4242/jobs?filter=latest&per_page=100")


if __name__ == "__main__":
    unittest.main()
