"""The plan job of the CI workflow: which job groups this run starts, or which passed pull request run it reuses, and why.

Run from the repository root of the checkout: ``python -m scripts.quality.ci_plan --event <event> --start <commit> --head <commit>``.
``--start`` is the pull request's base commit and ``--head`` the commit that the run checks: the pull request's head commit, or the pushed
commit. GitHub's job environment names the run (``GITHUB_SHA``, ``GITHUB_REF``, ``GITHUB_REPOSITORY``, ``GITHUB_REPOSITORY_ID``,
``GITHUB_API_URL``) and ``GITHUB_TOKEN`` holds the read-only token.

- A pull request selects the groups whose inputs its base-to-head range changes (scripts/quality/ci_groups.py). A group can skip only when
  the newest push run of the base commit on main passed, because a skipped group trusts the base's result.
- A push to main reuses the newest run of the merged pull request's head commit when its tree is the pushed tree and every required job
  passed (scripts/quality/ci_reuse.py). The record and the summary name the reused run.
- Any other event, a push without a pull request, a different tree, and every doubt run every group; nothing is reused then.

The hooks job never skips. The plan writes one output, ``record``: JSON that the jobs read for their names, their decisions and their
reasons. Every API read shares one deadline (``--api-deadline``, 120 seconds by default); past it, every group runs. The plan never fails
for a doubt: it fails only when the map or the checkout cannot be read.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any

from scripts.process import TimeoutExpired
from scripts.quality import ci_groups, ci_reuse
from scripts.quality.repo_checks import git

DOUBTS = (ci_groups.Unverified, ci_reuse.NoReuse, ci_reuse.ApiError, CalledProcessError, TimeoutExpired, OSError, SyntaxError, KeyError, TypeError, ValueError)
MAIN_REF = "refs/heads/main"


def required_jobs(config: dict[str, Any]) -> ci_reuse.JobSet:
    """The jobs that a reused run must hold: the plan job, each job that always runs, and each selectable group's jobs or its idle job."""
    always = {config["plan"]["job"], *(group["job"] for group in config["groups"].values() if group.get("always"))}
    groups = {name: (frozenset(ci_groups.job_names(config, name).values()), ci_groups.idle_name(config, name)) for name in ci_groups.selectable(config)}
    return ci_reuse.JobSet(frozenset(always), groups)


def pull_request_decisions(
    root: Path, config: dict[str, Any], start: str, head: str, environment: Mapping[str, str], api: ci_reuse.Api | None
) -> tuple[str, dict[str, ci_groups.Decision]]:
    """The basis and each group's decision for a pull request whose range is verified and whose base passed on main."""
    ci_groups.verify_range(root, start, head)
    if api is None:
        raise ci_reuse.NoReuse("no GitHub API token was given, so the base commit's run on main cannot be read")
    ci_reuse.prove_start(api, ci_reuse.context_of(environment), start)
    inputs = ci_groups.group_inputs(root, config)
    changed = ci_groups.changed_paths(root, start, head)
    wide = ci_groups.widening(config, inputs, changed)
    if wide:
        raise ci_groups.Unverified(wide)
    return f"pull request {start[:12]}..{head[:12]}: {len(changed)} changed paths", ci_groups.select(config, inputs, changed, "")


def push_reuse(root: Path, config: dict[str, Any], head: str, environment: Mapping[str, str], api: ci_reuse.Api | None) -> ci_reuse.Reused:
    """The passed pull request run that a push to main after a merge reuses."""
    if environment.get("GITHUB_REF") != MAIN_REF:
        raise ci_reuse.NoReuse(f"the push is to {environment.get('GITHUB_REF') or 'no branch'}, not to {MAIN_REF}")
    if head != environment.get("GITHUB_SHA"):
        raise ci_groups.Unverified(f"the head commit {head} is not the pushed commit {environment.get('GITHUB_SHA') or 'none'}")
    ci_groups.verify_head(root, head)
    if api is None:
        raise ci_reuse.NoReuse("no GitHub API token was given, so no pull request run can be read")
    tree = git("rev-parse", "HEAD^{tree}", cwd=root).strip()
    return ci_reuse.reusable_run(api, ci_reuse.context_of(environment), head, tree, required_jobs(config))


def group_entry(config: dict[str, Any], name: str, decision: ci_groups.Decision) -> dict[str, Any]:
    """The record of one selectable group: whether it runs, why, and the matrix of the jobs it starts."""
    shards = config["groups"][name].get("shards")
    if decision.run:
        include = [
            {"name": job, "run": True, **({"shard": shard, "shards": shards} if shards else {})} for shard, job in ci_groups.job_names(config, name).items()
        ]
    else:
        include = [{"name": ci_groups.idle_name(config, name), "run": False, **({"shard": 0, "shards": shards} if shards else {})}]
    return {"run": decision.run, "reason": decision.reason, "matrix": {"include": include}}


def plan(root: Path, event: str, start: str, head: str, environment: Mapping[str, str], api: ci_reuse.Api | None) -> dict[str, Any]:
    """The record of the run: its basis, the reused run if any and, for each group, whether it runs, why and the names of its jobs."""
    config = ci_groups.load(root)
    reused: ci_reuse.Reused | None = None
    try:
        if event == "pull_request":
            basis, decisions = pull_request_decisions(root, config, start, head, environment, api)
        elif event == "push":
            reused = push_reuse(root, config, head, environment, api)
            basis = f"push of {head[:12]}: reuses pull request {reused.pull_request}, whose head {reused.head[:12]} has the tree {reused.tree[:12]}"
            text = f"reused: every required job passed on this tree in {reused.run_url} (pull request {reused.pull_request})"
            decisions = {name: ci_groups.Decision(False, text) for name in ci_groups.selectable(config)}
        else:
            raise ci_groups.Unverified(f"the event {event} neither selects groups nor reuses a run")
    except DOUBTS as reason:
        reused = None
        basis = f"every group runs: {reason}"
        decisions = {name: ci_groups.Decision(True, basis) for name in ci_groups.selectable(config)}
    groups: dict[str, Any] = {}
    for name, group in config["groups"].items():
        groups[name] = {"run": True, "reason": group["reason"], "name": group["job"]} if group.get("always") else group_entry(config, name, decisions[name])
    record: dict[str, Any] = {"basis": basis, "groups": groups}
    if reused:
        record["reused"] = {
            "pull_request": reused.pull_request,
            "head": reused.head,
            "tree": reused.tree,
            "run_id": reused.run_id,
            "run_url": reused.run_url,
            "jobs": reused.jobs,
        }
    return record


def cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def summary(record: dict[str, Any]) -> str:
    """The run summary: the basis, the reused run with its jobs, then each group with its decision and reason."""
    lines = ["### CI plan", "", cell(record["basis"]), ""]
    reused = record.get("reused")
    if reused:
        lines += [f"Reused run: {reused['run_url']} (id {reused['run_id']}), head {reused['head']}, tree {reused['tree']}.", ""]
        lines += ["| Reused job | Conclusion |", "| --- | --- |", *(f"| {cell(job)} | success |" for job in reused["jobs"]), ""]
    rows = [f"| {group} | {'runs' if entry['run'] else 'does not run'} | {cell(entry['reason'])} |" for group, entry in record["groups"].items()]
    return "\n".join([*lines, "| Group | Decision | Reason |", "| --- | --- | --- |", *rows]) + "\n"


def output_line(record: dict[str, Any]) -> str:
    """The step output that the jobs read, on one line."""
    return "record=" + json.dumps(record, separators=(",", ":")) + "\n"


def append(variable: str, text: str) -> None:
    target = os.environ.get(variable)
    if target:
        with open(target, "a", encoding="utf-8") as stream:
            stream.write(text)


def main(argv: list[str] | None = None) -> int:
    """Write the plan's record to the step output and its summary to the run summary and to standard output."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--event", required=True, help="the name of the event that started the run")
    parser.add_argument("--start", default="", help="the pull request's base commit")
    parser.add_argument("--head", default="", help="the commit that the run checks: the pull request's head commit or the pushed commit")
    parser.add_argument("--api-deadline", type=float, default=ci_reuse.API_DEADLINE_SECONDS, help="seconds that every API read of the plan shares")
    args = parser.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN", "")
    api: ci_reuse.Api | None = None
    if token:
        try:
            api = ci_reuse.github_api(os.environ.get("GITHUB_API_URL", "https://api.github.com"), token, args.api_deadline)
        except ci_reuse.ApiError as error:
            sys.stderr.write(f"no API reader: {error}\n")
    record = plan(Path.cwd(), args.event, args.start, args.head, os.environ, api)
    text = summary(record)
    append("GITHUB_OUTPUT", output_line(record))
    append("GITHUB_STEP_SUMMARY", text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
