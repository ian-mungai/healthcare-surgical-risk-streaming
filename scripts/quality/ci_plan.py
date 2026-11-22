"""The plan job of the CI workflow: which job groups this run starts, which passed jobs it reuses, and why.

Run from the repository root of the checkout: ``python -m scripts.quality.ci_plan --event <event> --start <commit>``. ``--start`` is the
pull request's base commit or the push's previous commit. GitHub's job environment names the run (``GITHUB_SHA``, ``GITHUB_REPOSITORY``,
``GITHUB_REPOSITORY_ID``, ``GITHUB_RUN_ID``, ``GITHUB_RUN_ATTEMPT``, ``GITHUB_API_URL``) and ``GITHUB_TOKEN`` holds the read-only token.

A group can skip only when the range is verified (scripts/quality/ci_groups.py) and the start commit's own push run passed, because a
skipped group trusts the start's result. Reuse needs the reuse key of each group (scripts/quality/ci_reuse.py). Every doubt runs the group.
The plan writes one output, ``record``: JSON that the jobs read for their names, their decisions and their reasons. The run's summary
shows each group's decision and reason. Every API read shares one deadline (``--api-deadline``, 120 seconds by default); past it,
every group runs. The plan never fails for a doubt: it fails only when the map or the checkout cannot be read.
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

DOUBTS = (ci_groups.Unverified, ci_reuse.NoReuse, ci_reuse.ApiError, CalledProcessError, TimeoutExpired, KeyError, TypeError, ValueError)
NOT_RUN = ": not run"


def checked_range(root: Path, event: str, start: str, environment: Mapping[str, str], api: ci_reuse.Api | None) -> tuple[list[str] | None, str]:
    """The changed paths and the text of the range, or None and the reason that every group runs."""
    run_commit = environment.get("GITHUB_SHA", "")
    config = ci_groups.load(root)
    try:
        ci_groups.verify_range(root, event, start, run_commit)
        if api is None:
            raise ci_reuse.NoReuse("no GitHub API token was given, so the start commit's run cannot be read")
        ci_reuse.prove_start(api, ci_reuse.context_of(environment), start)
        changed = ci_groups.changed_paths(root, start, run_commit)
        wide = ci_groups.widening(config, changed)
        if wide:
            raise ci_groups.Unverified(wide)
    except DOUBTS as reason:
        return None, f"every group runs: {reason}"
    return changed, f"{start[:12]}..{run_commit[:12]}: {len(changed)} changed paths"


def reusable(root: Path, environment: Mapping[str, str], api: ci_reuse.Api | None, runner: str) -> tuple[dict[str, str], str]:
    """The passed jobs of earlier runs with this tree, or none and the reason."""
    if api is None:
        return {}, "no GitHub API token was given"
    try:
        tree = git("rev-parse", "HEAD^{tree}", cwd=root).strip()
        return ci_reuse.passed_jobs(api, ci_reuse.context_of(environment), tree, runner), ""
    except DOUBTS as reason:
        return {}, str(reason)


def group_entry(config: dict[str, Any], name: str, decision: ci_groups.Decision, key: str | None, passed: dict[str, str], doubt: str) -> dict[str, Any]:
    """The record of one selectable group: whether it runs, why, and the name of each job it starts."""
    definition = config["groups"][name]
    names = ci_reuse.job_names(config, name, key)
    reused = {shard: passed[job] for shard, job in names.items() if decision.run and key and job in passed}
    remaining = [shard for shard in names if shard not in reused] if decision.run else []
    reason = decision.reason
    if reused:
        sources = ", ".join(sorted(set(reused.values())))
        reuse_text = f"reused {len(reused)} of {len(names)} jobs with key {key[: ci_reuse.KEY_LENGTH] if key else ''} from {sources}"
        reason = f"{reason}; {reuse_text}" if remaining else f"{reuse_text}; {reason}"
    elif decision.run and doubt:
        reason = f"{reason}; no reuse: {doubt}"
    entry: dict[str, Any] = {"run": bool(remaining), "reason": reason}
    shards = definition.get("shards")
    if shards:
        include = [{"shard": shard, "shards": shards, "name": names[shard]} for shard in remaining]
        entry["matrix"] = {"include": include or [{"shard": 0, "shards": shards, "name": definition["job"] + NOT_RUN}]}
    else:
        entry["name"] = names[1] if remaining else definition["job"] + NOT_RUN
    return entry


def plan(root: Path, event: str, start: str, environment: Mapping[str, str], api: ci_reuse.Api | None) -> dict[str, Any]:
    """The record of the run: the range and, for each group, whether it runs, why and the names of its jobs."""
    config = ci_groups.load(root)
    changed, range_text = checked_range(root, event, start, environment, api)
    passed: dict[str, str] | None = None
    passed_doubt = ""
    groups: dict[str, Any] = {}
    for name, decision in ci_groups.select(config, changed, range_text).items():
        definition = config["groups"][name]
        if definition.get("always"):
            groups[name] = {"run": True, "reason": decision.reason, "name": definition["job"]}
            continue
        try:
            key: str | None = ci_reuse.reuse_key(root, config, name)
            key_doubt = ""
        except ci_reuse.NoReuse as reason:
            key, key_doubt = None, str(reason)
        if decision.run and key and passed is None:
            passed, passed_doubt = reusable(root, environment, api, config["runner"])
        groups[name] = group_entry(config, name, decision, key, passed or {}, key_doubt or passed_doubt)
    return {"range": range_text, "groups": groups}


def cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def summary(record: dict[str, Any]) -> str:
    """The run summary: the range, then each group with its decision and reason."""
    rows = [f"| {group} | {'runs' if entry['run'] else 'does not run'} | {cell(entry['reason'])} |" for group, entry in record["groups"].items()]
    return "\n".join(["### CI plan", "", cell(record["range"]), "", "| Group | Decision | Reason |", "| --- | --- | --- |", *rows]) + "\n"


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
    parser.add_argument("--start", default="", help="the pull request's base commit or the push's previous commit")
    parser.add_argument("--api-deadline", type=float, default=ci_reuse.API_DEADLINE_SECONDS, help="seconds that every API read of the plan shares")
    args = parser.parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN", "")
    api: ci_reuse.Api | None = None
    if token:
        try:
            api = ci_reuse.github_api(os.environ.get("GITHUB_API_URL", "https://api.github.com"), token, args.api_deadline)
        except ci_reuse.ApiError as error:
            sys.stderr.write(f"no API reader: {error}\n")
    record = plan(Path.cwd(), args.event, args.start, os.environ, api)
    text = summary(record)
    append("GITHUB_OUTPUT", output_line(record))
    append("GITHUB_STEP_SUMMARY", text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
