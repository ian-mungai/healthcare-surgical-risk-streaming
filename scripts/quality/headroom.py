"""Check the headroom of one hosted CI run: no job used more than 80% of its timeout, and no job is missing or without a limit.

Run by the headroom job of ``.github/workflows/ci.yml`` with ``make headroom``. It reads every job of the run, latest attempt, from the jobs API
through the ``gh`` command, so the time of a job is its whole time from start to end, with its setup and its post-steps such as a cache save.
The limits and the expected jobs (every guard shard by its name) come from the workflow file. A job that is still running, which is the headroom
job itself, has no end yet and is not measured. The job writes a table of the durations to the run's summary.
"""

from __future__ import annotations

import itertools
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
SHARE = 0.8
API_TIMEOUT_SECONDS = 120
MATRIX_VALUE = re.compile(r"\$\{\{ matrix\.([a-z_]+) \}\}")


def leg_name(template: str, values: dict[str, Any]) -> str:
    """A job name with each ``${{ matrix.axis }}`` replaced by the leg's value."""
    return MATRIX_VALUE.sub(lambda found: str(values[found.group(1)]), template)


def expected_limits(workflow: dict[str, Any]) -> dict[str, int | None]:
    """Each job of the workflow as the jobs API names it (every matrix leg by its own name) with its timeout in minutes, or None when it has none."""
    limits: dict[str, int | None] = {}
    for key, job in workflow["jobs"].items():
        template = str(job.get("name", key))
        matrix = (job.get("strategy") or {}).get("matrix") or {}
        axes = {axis: values for axis, values in matrix.items() if axis not in ("include", "exclude")}
        timeout = job.get("timeout-minutes")
        limit = timeout if isinstance(timeout, int) and not isinstance(timeout, bool) else None
        for combination in itertools.product(*axes.values()):
            values = dict(zip(axes, combination, strict=True))
            limits[leg_name(template, values)] = limit
    return limits


def minutes_of(job: dict[str, Any]) -> float | None:
    """The whole time of a finished job in minutes, or None while it runs."""
    if job.get("status") != "completed" or not job.get("started_at") or not job.get("completed_at"):
        return None
    return (datetime.fromisoformat(job["completed_at"]) - datetime.fromisoformat(job["started_at"])).total_seconds() / 60


def problems(limits: dict[str, int | None], jobs: list[dict[str, Any]], own: str | None = None) -> list[str]:
    """Name each expected job that the run does not list, each job without a limit, each job over its share of its limit and each job but ``own``
    that is still running, because its whole time is not known yet."""
    found = []
    listed = {str(job["name"]) for job in jobs}
    found += [f"{name}: it is not among the jobs of the run" for name in limits if name not in listed]
    for job in jobs:
        name = str(job["name"])
        limit = limits.get(name)
        if limit is None:
            found.append(f"{name}: the workflow gives it no timeout")
            continue
        used = minutes_of(job)
        if used is None and name != own:
            found.append(f"{name}: it was still running when the headroom job read the run, so its whole time is not known")
        if used is not None and used > SHARE * limit:
            found.append(f"{name}: took {used:.1f} minutes, over {SHARE:.0%} of its {limit} minutes limit")
    return found


def read_jobs(text: str) -> list[dict[str, Any]]:
    """The jobs of the pages that the API client prints one after the other. Text that is not pages of jobs is an error."""
    decoder = json.JSONDecoder()
    jobs: list[dict[str, Any]] = []
    position = 0
    pages = 0
    while text[position:].strip():
        position += len(text[position:]) - len(text[position:].lstrip())
        try:
            page, position = decoder.raw_decode(text, position)
        except json.JSONDecodeError:
            raise SystemExit("the jobs API answer is not JSON") from None
        if not isinstance(page, dict) or not isinstance(page.get("jobs"), list):
            raise SystemExit("the jobs API answer holds no list of jobs")
        jobs += page["jobs"]
        pages += 1
    if pages == 0:
        raise SystemExit("the jobs API answered nothing")
    return jobs


def api_arguments(repository: str, run_id: str) -> list[str]:
    """The ``gh`` arguments that list the jobs of the latest attempt of one run, every page."""
    return ["api", "--paginate", f"repos/{repository}/actions/runs/{run_id}/jobs?filter=latest&per_page=100"]


def summary(limits: dict[str, int | None], jobs: list[dict[str, Any]]) -> str:
    rows = ["| Job | Minutes | Limit | Share |", "| --- | --- | --- | --- |"]
    for job in sorted(jobs, key=lambda each: str(each["name"])):
        limit, used = limits.get(str(job["name"])), minutes_of(job)
        share = "" if used is None or not limit else f"{used / limit:.0%}"
        rows.append(f"| {job['name']} | {'' if used is None else f'{used:.1f}'} | {limit or ''} | {share} |")
    return "\n".join(rows) + "\n"


def main() -> int:
    """Return 1 and name each job that is over its share of its limit, missing or without a limit."""
    missing = [name for name in ("GITHUB_REPOSITORY", "GITHUB_RUN_ID") if not os.environ.get(name)]
    if missing:
        raise SystemExit(f"the environment lacks {', '.join(missing)}: run this in a hosted job")
    answer = run_command("gh", api_arguments(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]), timeout=API_TIMEOUT_SECONDS, check=True)
    jobs = read_jobs(answer.stdout)
    limits = expected_limits(yaml.safe_load(WORKFLOW.read_text()))
    table = summary(limits, jobs)
    sys.stdout.write(table)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as handle:
            handle.write(table)
    workflow = yaml.safe_load(WORKFLOW.read_text())
    own = workflow["jobs"].get(os.environ.get("GITHUB_JOB", ""), {}).get("name")
    found = problems(limits, jobs, own)
    for line in found:
        sys.stderr.write(f"{line}\n")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
