"""The plan job of the CI workflow: whether the job groups run, or which passed pull request run this run reuses, and why.

Run from the repository root of the checkout: ``python -m scripts.quality.ci_plan``. GitHub's job environment names the run
(``GITHUB_EVENT_NAME``, ``GITHUB_SHA``, ``GITHUB_REF``, ``GITHUB_REPOSITORY``, ``GITHUB_REPOSITORY_ID``, ``GITHUB_API_URL``) and
``GITHUB_TOKEN`` holds the read-only token.

- A pull request runs every group. Its run is the gate before the merge, and a group that skipped on a pull request would trust a
  selection that cannot see every file a check reads.
- A push to main after a merge reuses the passed run of the merged pull request when ``scripts/quality/ci_reuse.py`` proves that it
  checked the pushed tree. The record and the summary name the reused run.
- Any other event, a push without a pull request and every doubt run every group; nothing is reused then.

The plan job writes its own proof (``scripts/quality/ci_proof.py``), which binds a pull request run to its pull request. The hooks job
never skips. The plan writes two outputs that the group jobs read: ``run`` (``true`` or ``false``) and ``reason``. Every
API read shares one deadline (``--api-deadline``, 120 seconds by default); past it, every group runs. The plan never fails for a doubt:
it fails only when the map or the checkout cannot be read.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any

from scripts.process import TimeoutExpired
from scripts.quality import ci_proof, ci_reuse
from scripts.quality.repo_checks import git

CONFIG = Path("scripts/quality/ci_groups.json")
MAIN_REF = "refs/heads/main"
COMMIT = re.compile(r"[0-9a-f]{40}")
DOUBTS = (ci_reuse.NoReuse, ci_reuse.ApiError, CalledProcessError, TimeoutExpired, OSError, KeyError, TypeError, ValueError)
EVERY_PULL_REQUEST = "runs: a pull request runs every group"


def load(root: Path) -> dict[str, Any]:
    """The map of the checkout at ``root``."""
    config: dict[str, Any] = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    return config


def job_names(config: dict[str, Any], group: str) -> list[str]:
    """The name of each job of ``group``, in shard order for a group with shards."""
    definition = config["groups"][group]
    shards = definition.get("shards")
    if not shards:
        return [definition["job"]]
    return [definition["job"].format(shard=shard, shards=shards) for shard in range(1, shards + 1)]


def selectable(config: dict[str, Any]) -> list[str]:
    return [name for name, group in config["groups"].items() if not group.get("always")]


def required_jobs(config: dict[str, Any]) -> frozenset[str]:
    """Every job of a run: the plan job, the job that waits for all others and each job of each group."""
    names = [config["plan"]["job"], config["required"]["job"], *(name for group in config["groups"] for name in job_names(config, group))]
    return frozenset(names)


def run_jobs(config: dict[str, Any]) -> ci_reuse.Jobs:
    return ci_reuse.Jobs(required_jobs(config), config["plan"]["job"], config["required"]["job"])


def push_reuse(root: Path, config: dict[str, Any], environment: Mapping[str, str], api: ci_reuse.Api | None) -> ci_reuse.Reused:
    """The passed pull request run that a push to main after a merge reuses."""
    head = environment.get("GITHUB_SHA", "")
    if environment.get("GITHUB_REF") != MAIN_REF:
        raise ci_reuse.NoReuse(f"the push is to {environment.get('GITHUB_REF') or 'no branch'}, not to {MAIN_REF}")
    if not COMMIT.fullmatch(head):
        raise ci_reuse.NoReuse("the run names no pushed commit")
    checked_out = git("rev-parse", "HEAD", cwd=root).strip()
    if checked_out != head:
        raise ci_reuse.NoReuse(f"the checkout {checked_out} is not the pushed commit {head}")
    if api is None:
        raise ci_reuse.NoReuse("no GitHub API token was given, so no pull request run can be read")
    runner = ci_proof.runner(environment)
    if not runner["image_os"] or not runner["image_version"]:
        raise ci_reuse.NoReuse("the runner names no image version, so no run can be shown to have run on the same tools")
    tree, blob = git("rev-parse", "HEAD^{tree}", cwd=root).strip(), git("rev-parse", f"HEAD:{ci_reuse.WORKFLOW}", cwd=root).strip()
    checkout = ci_reuse.Checkout(head, tree, blob, runner)
    return ci_reuse.reusable_run(api, ci_reuse.context_of(environment), checkout, run_jobs(config))


def plan(root: Path, environment: Mapping[str, str], api: ci_reuse.Api | None) -> dict[str, Any]:
    """The record of the run: its basis, whether the groups run and why, the jobs of each group and the reused run if any."""
    config = load(root)
    event = environment.get("GITHUB_EVENT_NAME", "")
    reused: ci_reuse.Reused | None = None
    if event == "pull_request":
        basis, run, reason = "pull request: every group runs", True, EVERY_PULL_REQUEST
    else:
        try:
            if event != "push":
                raise ci_reuse.NoReuse(f"the event {event or 'none'} reuses no run")
            reused = push_reuse(root, config, environment, api)
            basis = f"push of {reused.commit[:12]}: reuses pull request {reused.pull_request}, whose head {reused.head[:12]} has the tree {reused.tree[:12]}"
            run, reason = False, f"reused: every job passed on this tree in {reused.run_url} (pull request {reused.pull_request})"
        except DOUBTS as doubt:
            reused = None
            basis = f"every group runs: {doubt}"
            run, reason = True, f"runs: {doubt}"
    groups = {
        name: {"run": True, "reason": group["reason"], "jobs": job_names(config, name)}
        if group.get("always")
        else {"run": run, "reason": reason, "jobs": job_names(config, name)}
        for name, group in config["groups"].items()
    }
    record: dict[str, Any] = {"basis": basis, "run": run, "reason": reason, "groups": groups}
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


def output_lines(record: dict[str, Any]) -> str:
    """The step outputs that the group jobs read, one line each."""
    return f"run={'true' if record['run'] else 'false'}\nreason={cell(record['reason'])}\n"


def append(variable: str, text: str) -> None:
    target = os.environ.get(variable)
    if target:
        with open(target, "a", encoding="utf-8") as stream:
            stream.write(text)


def main(argv: list[str] | None = None) -> int:
    """Write the plan's outputs to the step output and its summary to the run summary and to standard output."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--api-deadline", type=float, default=ci_reuse.API_DEADLINE_SECONDS, help="seconds that every API read of the plan shares")
    args = parser.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN", "")
    api: ci_reuse.Api | None = None
    if token:
        try:
            api = ci_reuse.github_api(os.environ.get("GITHUB_API_URL", "https://api.github.com"), token, args.api_deadline)
        except ci_reuse.ApiError as error:
            sys.stderr.write(f"no API reader: {error}\n")
    record = plan(Path.cwd(), os.environ, api)
    text = summary(record)
    append("GITHUB_OUTPUT", output_lines(record))
    append("GITHUB_STEP_SUMMARY", text)
    sys.stdout.write(text)
    sys.stdout.write(ci_proof.annotation(ci_proof.proof(Path.cwd(), os.environ)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
