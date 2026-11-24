# Accepted limits: a pull request that edits the workflow to skip the required job passes the merge gate; the Makefile's recipes are not checked here;
# the contract does not prove that GitHub accepts the workflow file: a hosted run does, and a file GitHub rejects starts no job, so the merge is blocked.
"""The CI workflow contract: every kind of run (a work-branch push, a pull request, the squash-merge push, a weekly schedule, a manual run) runs every group.

The contract reads the workflow as PyYAML parses it and compares parsed keys and values only, never text, so a spaced key, a quoted key, a
YAML anchor or a merge key cannot hide a change. Failure modes covered here, each with a planted case (run by ``tests/slow/test_ci_workflow_plants.py``)
and a control:

- A group job is skipped: an ``if`` on a job or on one of its steps, or ``continue-on-error`` that lets a failed check pass.
- A step runs under another shell or folder, so its command can become a no-op (``shell: bash -n {0}``).
- A job group has no unconditional step list: a step behind a condition other than the guards' log tail, the two record checks and the two record
  uploads (each check fails unless every run folder holds the record and its retention label as regular files, and each upload keeps them unless the
  run is cancelled), a dropped, added, moved or changed step.
- A guard shard is missing, runs twice, runs another shard's cases or is excluded through ``matrix.exclude``.
- A check command changes in the workflow: a dry-run flag, a dropped or reordered command, another Python or tool version.
- The required job leaves a group out of ``needs``, does not always run, has its step disabled or under another shell, or its script differs
  from the approved one by a character, holds an expression, or passes when a needed job failed, was cancelled or was skipped. The step sets
  ``shell: bash``, so GitHub runs it with ``-eo pipefail``; its script runs here the same way, exactly as parsed, for all 256 combinations of the
  needed jobs' results.
- A trigger is missing or narrowed: a work-branch push, a pull request, the push of the squash-merge commit on main, a weekly schedule or a manual run.
- A superseded work-branch or pull-request run is not cancelled, or a main, scheduled or manual run is cancelled or replaced: the concurrency group and
  the cancel condition are evaluated for each kind of run, as GitHub's expression syntax reads them.
- A guard leg that never ran is counted as a success: each leg sets its own output as its last step, and the required job fails unless all eight are set.
- The headroom job joins the required check, runs with more than read permission, skips a job or drops its limit comparison.
- A job's timeout is less than twice its recorded duration, or has none recorded beside it.
"""

from __future__ import annotations

import copy
import functools
import itertools
import json
import os
import platform
import re
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

from scripts.process import run_command
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

WORKFLOW_TEXT = (ROOT / ".github/workflows/ci.yml").read_text()
RUNNER = "ubuntu-24.04"
PYTHON = "3.14.10"
SHARDS = 8
REQUIRED_NAME = "Every job of this run passed"
GROUPS = ("hooks", "infrastructure", "slow", "guards", "deadline")
HEADROOM = "headroom"
HEADROOM_NAME = "No job of this run is near its time limit"
LEGS = tuple(range(1, SHARDS + 1))
# The headroom job fails any job that took more than this share of its timeout.
HEADROOM_SHARE = 0.8
# The timeout of every job is at least this many times the duration that the workflow records beside it.
TIMEOUT_FACTOR = 2
EXPRESSION = re.compile(r"\$\{\{ ([^}]+?) \}\}")
RESULTS = ("success", "failure", "cancelled", "skipped")
# The command that GitHub runs a `run` script with under the shell `bash`.
GITHUB_BASH = ("--noprofile", "--norc", "-eo", "pipefail")
SHARD_COMMAND = f'make test-guards SHARD="${{SHARD}}/{SHARDS}"'
DEADLINE_COMMAND = "make test-deadline"
COMMIT_RANGE_SCRIPT = "\n".join(
    [
        'if git cat-file -e "${BASE}^{commit}" 2>/dev/null; then',
        '  .venv/bin/python -m scripts.quality.repo_checks commit-range "${BASE}..${HEAD}"',
        "else",
        '  .venv/bin/python -m scripts.quality.repo_checks commit-range "${HEAD}"',
        "fi",
        "",
    ]
)
CHECKOUT = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
SETUP_PYTHON = "actions/setup-python@6d4fd22b20465ccc0f7354476588e8df7c9ca0e0"
UPLOAD_ARTIFACT = "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9"
SETUP_TERRAFORM = "hashicorp/setup-terraform@dfe3c3f87815947d99a8997f908cb6525fc44e9e"
PYTHON_STEP = {"uses": SETUP_PYTHON, "with": {"python-version": PYTHON, "cache": "pip", "cache-dependency-path": "requirements.txt"}}
CHECKOUT_STEP = {"uses": CHECKOUT, "with": {"persist-credentials": False}}
TAIL_STEP = {"name": "Show the end of the run's log after a failure", "if": "failure()", "run": "tail -n 150 runs/*/log.txt"}
# The last step of a guard leg runs only when every step before it succeeded, so a leg that never ran, failed or was cancelled sets no output.
LEG_SCRIPT = 'echo "shard_${SHARD}=success" >> "${GITHUB_OUTPUT}"'
LEG_STEP = {"id": "leg", "name": "Set this shard's own success output", "run": LEG_SCRIPT, "env": {"SHARD": "${{ matrix.shard }}"}}
GUARD_OUTPUTS = {f"shard_{leg}": f"${{{{ steps.leg.outputs.shard_{leg} }}}}" for leg in LEGS}


def record_step(artifact: str) -> dict[str, Any]:
    """The step that keeps a run's record and retention label as a hosted artifact, also after a failure; no file is an error."""
    return {
        "name": "Keep the run record and its retention label",
        "if": "${{ !cancelled() }}",
        "uses": UPLOAD_ARTIFACT,
        "with": {"name": artifact, "path": "runs/*/record.json\nruns/*/retention.json\n", "if-no-files-found": "error", "retention-days": 30},
    }


# The script of the step before each upload. The upload matches runs/*/record.json and runs/*/retention.json as separate patterns, so it succeeds with
# one of them alone: this step fails unless every run folder holds both files as regular files that are not links, and at least one run folder exists.
# GitHub runs it with `bash -e {0}`.
RECORD_CHECK_NAME = "Check that the run record and its retention label exist"
RECORD_CHECK_SCRIPT = r"""shopt -s nullglob
folders=(runs/*/)
if [ "${#folders[@]}" -eq 0 ]; then
  echo "No run folder was written under runs/." >&2
  exit 1
fi
incomplete=0
for folder in "${folders[@]}"; do
  for file in record.json retention.json; do
    if [ ! -f "${folder}${file}" ] || [ -L "${folder}${file}" ]; then
      printf 'The run folder %s has no %s.\n' "${folder}" "${file}" >&2
      incomplete=1
    fi
  done
done
exit "${incomplete}"
"""
RECORD_CHECK_STEP = {"name": RECORD_CHECK_NAME, "if": "${{ !cancelled() }}", "run": RECORD_CHECK_SCRIPT}
# What the check does in a run folder tree: the files that exist (a trailing slash makes a directory, "name -> target" a link) and whether the step must pass.
RECORD_CHECK_CASES: dict[str, tuple[tuple[str, ...], bool]] = {
    "the record and its label in one run folder": (("runs/a/record.json", "runs/a/retention.json"), True),
    "the record alone": (("runs/a/record.json",), False),
    "the label alone": (("runs/a/retention.json",), False),
    "no run folder": ((), False),
    "an empty runs folder": (("runs/",), False),
    "the record and the label in two run folders": (("runs/a/record.json", "runs/b/retention.json"), False),
    "a second run folder without a label": (("runs/a/record.json", "runs/a/retention.json", "runs/b/record.json"), False),
    "a directory where the label belongs": (("runs/a/record.json", "runs/a/retention.json/"), False),
    "the label a link to the label of another run": (
        ("runs/a/record.json", "runs/a/retention.json -> runs/b/retention.json", "runs/b/record.json", "runs/b/retention.json"),
        False,
    ),
    "the record a link to the record of another run": (
        ("runs/a/record.json -> runs/b/record.json", "runs/a/retention.json", "runs/b/record.json", "runs/b/retention.json"),
        False,
    ),
    "two complete run folders": (("runs/a/record.json", "runs/a/retention.json", "runs/b/record.json", "runs/b/retention.json"), True),
}
GUARD_RECORD_STEP = record_step("guard-record-shard-${{ matrix.shard }}")
DEADLINE_RECORD_STEP = record_step("deadline-record")
# The only conditions that a step may have, by job: the log tail after a failure, and the record check and the record upload unless the run is cancelled.
# A step with one of them must still equal its pinned step, which the comparison with PINNED_JOBS checks.
STEP_CONDITIONS = {
    "guards": {TAIL_STEP["if"], RECORD_CHECK_STEP["if"], GUARD_RECORD_STEP["if"]},
    "deadline": {RECORD_CHECK_STEP["if"], DEADLINE_RECORD_STEP["if"]},
}
# Each kind of run, the GitHub context that it has and whether a newer run of the same group may cancel it. A work-branch push and a pull request run are
# superseded by a newer run of the same ref. The push that a squash merge makes on main, a scheduled run and a manual run are never cancelled, and
# each has a group of its own (its run), so none replaces another, not even a pending one.
CONCURRENCY_RUNS: dict[str, tuple[dict[str, str], bool]] = {
    "a push of a work branch": ({"event_name": "push", "ref": "refs/heads/feat/a"}, True),
    "a pull request": ({"event_name": "pull_request", "ref": "refs/pull/7/merge"}, True),
    "the push that a squash merge makes on main": ({"event_name": "push", "ref": "refs/heads/main"}, False),
    "a scheduled run": ({"event_name": "schedule", "ref": "refs/heads/main"}, False),
    "a manual run on main": ({"event_name": "workflow_dispatch", "ref": "refs/heads/main"}, False),
    "a manual run on a work branch": ({"event_name": "workflow_dispatch", "ref": "refs/heads/feat/a"}, False),
}
# Another ref of a superseded kind: its group must differ from the first one's.
OTHER_WORK_REFS = (
    ("a push of another work branch", {"event_name": "push", "ref": "refs/heads/feat/b"}),
    ("another pull request", {"event_name": "pull_request", "ref": "refs/pull/8/merge"}),
)
CRON_WEEKLY = re.compile(r"\d+ \d+ \* \* [0-6]")
ON_TRIGGERS = {"push": {"branches": ["**"]}, "pull_request": None, "workflow_dispatch": None}
# The jobs and steps of the workflow, with only the Python patch release added: a change to any of them is a change to what CI checks.
PINNED_JOBS: dict[str, dict[str, Any]] = {
    "hooks": {
        "name": "Hooks on every file and the commit messages",
        "timeout-minutes": 10,
        "steps": [
            {"uses": CHECKOUT, "with": {"fetch-depth": 0, "persist-credentials": False}},
            PYTHON_STEP,
            {"run": "make setup"},
            {"run": "make check"},
            {
                "name": "Check the commit messages of the pushed range",
                "env": {
                    "BASE": "${{ github.event.pull_request.base.sha || github.event.before }}",
                    "HEAD": "${{ github.event.pull_request.head.sha || github.sha }}",
                },
                "run": COMMIT_RANGE_SCRIPT,
            },
        ],
    },
    "infrastructure": {
        "name": "Static checks of the Terraform and Ansible code",
        "timeout-minutes": 10,
        "steps": [
            CHECKOUT_STEP,
            PYTHON_STEP,
            {"uses": SETUP_TERRAFORM, "with": {"terraform_version": "1.16.4", "terraform_wrapper": False}},
            {"run": "make setup"},
            {"run": "make check-terraform"},
            {"run": "make check-ansible"},
        ],
    },
    "slow": {
        "name": "The slow tests: Ansible roles, local servers and stand-ins, and planted workflow faults",
        "timeout-minutes": 45,
        "steps": [CHECKOUT_STEP, PYTHON_STEP, {"run": "make setup"}, {"run": "make test-slow"}],
    },
    "guards": {
        "name": "Planted faults through the real commit path, shard ${{ matrix.shard }} of 8",
        "timeout-minutes": 25,
        "outputs": GUARD_OUTPUTS,
        "steps": [
            CHECKOUT_STEP,
            PYTHON_STEP,
            {"run": "make setup"},
            {"run": SHARD_COMMAND, "env": {"SHARD": "${{ matrix.shard }}"}},
            TAIL_STEP,
            RECORD_CHECK_STEP,
            GUARD_RECORD_STEP,
            LEG_STEP,
        ],
    },
    "deadline": {
        "name": "A project wait passes its deadline and fails visibly",
        "timeout-minutes": 5,
        "steps": [CHECKOUT_STEP, PYTHON_STEP, {"run": "make setup"}, {"run": DEADLINE_COMMAND}, RECORD_CHECK_STEP, DEADLINE_RECORD_STEP],
    },
}
# The headroom job reads the jobs of the run and is not part of the required check. It asks for read permission only, and it waits for the jobs
# that it measures, whatever their results.
HEADROOM_JOB: dict[str, Any] = {
    "name": HEADROOM_NAME,
    "needs": [*GROUPS, "required"],
    "if": "always()",
    "runs-on": RUNNER,
    "timeout-minutes": 10,
    "permissions": {"actions": "read", "contents": "read"},
    "steps": [CHECKOUT_STEP, PYTHON_STEP, {"run": "make setup"}, {"run": "make headroom", "env": {"GH_TOKEN": "${{ github.token }}"}}],
}
JOB_KEYS = {"name", "runs-on", "timeout-minutes", "steps"}
REQUIRED_KEYS = {"name", "needs", "if", "runs-on", "timeout-minutes", "steps"}
REQUIRED_ENV_VALUES = {
    "HOOKS": "${{ needs.hooks.result }}",
    "INFRASTRUCTURE": "${{ needs.infrastructure.result }}",
    "SLOW": "${{ needs.slow.result }}",
    "GUARDS": "${{ needs.guards.result }}",
    "DEADLINE": "${{ needs.deadline.result }}",
    **{f"GUARD_{leg}": f"${{{{ needs.guards.outputs.shard_{leg} }}}}" for leg in LEGS},
}
# With `shell: bash` GitHub runs the script as `bash --noprofile --norc -eo pipefail {0}`; without a shell it runs `bash -e {0}`, without pipefail.
REQUIRED_STEP = {"name": "Check that every needed job succeeded", "shell": "bash", "env": REQUIRED_ENV_VALUES}
# The approved script of the required job's step, character for character. No `${{` expression: the jobs' results reach it through its env.
REQUIRED_SCRIPT = r"""failed=0
check() {
  if [ "$2" != success ]; then
    printf 'The job %s ended as %s, not as success.\n' "$1" "$2" | tee -a "${GITHUB_STEP_SUMMARY}"
    failed=1
  fi
}
check hooks "${HOOKS}"
check infrastructure "${INFRASTRUCTURE}"
check slow "${SLOW}"
check guards "${GUARDS}"
check deadline "${DEADLINE}"
check_leg() {
  if [ "$2" != success ]; then
    printf 'The guards shard %s did not set its success output.\n' "$1" | tee -a "${GITHUB_STEP_SUMMARY}"
    failed=1
  fi
}
check_leg 1 "${GUARD_1}"
check_leg 2 "${GUARD_2}"
check_leg 3 "${GUARD_3}"
check_leg 4 "${GUARD_4}"
check_leg 5 "${GUARD_5}"
check_leg 6 "${GUARD_6}"
check_leg 7 "${GUARD_7}"
check_leg 8 "${GUARD_8}"
exit "${failed}"
"""


# The fields that GitHub's create-ruleset API requires, for the rule types that the ruleset file uses.
TOP_LEVEL_KEYS = ["name", "on", "permissions", "concurrency", "jobs"]
BOOLEAN_RULE_PARAMETERS = {"required_status_checks": ("strict_required_status_checks_policy",)}
REQUIRED_RULESET_FIELDS = ("name", "enforcement")
REQUIRED_RULE_PARAMETERS = {
    "pull_request": (
        "dismiss_stale_reviews_on_push",
        "require_code_owner_review",
        "require_last_push_approval",
        "required_approving_review_count",
        "required_review_thread_resolution",
    ),
    "required_status_checks": ("required_status_checks", "strict_required_status_checks_policy"),
}


def source_problems(text: str) -> list[str]:
    """What PyYAML reads in the workflow that GitHub's workflow parser does not read the same way, found on the source before any normalising.

    A merge key, an anchor, an alias, a tag or a second YAML document is refused, so the parsed form and the source form are the same. The
    top-level mapping must have a key spelled ``on``, plain or quoted: PyYAML reads a plain ``on``, ``true`` or ``yes`` as a Boolean key, and
    GitHub does not read a Boolean key as the trigger.
    """
    problems = []
    documents = 0
    for event in yaml.parse(text, Loader=yaml.SafeLoader):
        line = event.start_mark.line + 1 if event.start_mark else 0
        if isinstance(event, yaml.DocumentStartEvent):
            documents += 1
        elif isinstance(event, yaml.AliasEvent):
            problems.append(f"line {line}: it uses an alias (*{event.anchor}), which GitHub's workflow parser does not read as PyYAML does")
        elif isinstance(event, yaml.ScalarEvent | yaml.SequenceStartEvent | yaml.MappingStartEvent):
            if event.anchor is not None:
                problems.append(f"line {line}: it uses an anchor (&{event.anchor}), which GitHub's workflow parser does not read as PyYAML does")
            if event.tag is not None:
                problems.append(f"line {line}: it uses the tag {event.tag!r}, which GitHub's workflow parser does not read as PyYAML does")
            if isinstance(event, yaml.ScalarEvent) and event.style is None and event.value == "<<":
                problems.append(f"line {line}: it uses a merge key (<<), which GitHub's workflow parser does not support")
    if documents != 1:
        problems.append(f"the file holds {documents} YAML documents, not one")
    root = next(iter(yaml.compose_all(text, Loader=yaml.SafeLoader)), None)
    keys = [key.value for key, _ in root.value if isinstance(key, yaml.ScalarNode)] if isinstance(root, yaml.MappingNode) else []
    if "on" not in keys:
        problems.append(
            f"the top-level mapping has no key spelled on (its keys are {keys}): GitHub does not read a Boolean key such as true or yes as the trigger"
        )
    if keys != TOP_LEVEL_KEYS:
        problems.append(
            f"the top-level keys are spelled {keys}, not {TOP_LEVEL_KEYS}: PyYAML can merge a key such as true into on before the contract reads it"
        )
    return problems


def ruleset_problems(ruleset: dict[str, Any]) -> list[str]:
    """A field that the create-ruleset API requires and the ruleset file lacks: GitHub would refuse the file when the owner applies it."""
    problems = [f"ruleset: it lacks {field}" for field in REQUIRED_RULESET_FIELDS if field not in ruleset]
    for number, rule in enumerate(ruleset.get("rules", []), 1):
        kind = rule.get("type")
        if kind is None:
            problems.append(f"rule {number}: it lacks type")
            continue
        required = REQUIRED_RULE_PARAMETERS.get(kind)
        if required is None:
            continue
        parameters = rule.get("parameters")
        if parameters is None:
            problems.append(f"rule {kind}: it lacks parameters")
            continue
        problems += [f"rule {kind}: its parameters lack {field}" for field in required if field not in parameters]
        problems += [
            f"rule {kind}: its {field} is not true or false"
            for field in BOOLEAN_RULE_PARAMETERS.get(kind, ())
            if field in parameters and not isinstance(parameters[field], bool)
        ]
        for position, check in enumerate(parameters.get("required_status_checks", []) if kind == "required_status_checks" else [], 1):
            if "context" not in check:
                problems.append(f"rule {kind}: check {position} lacks context")
    return problems


def load_workflow(text: str) -> dict[str, Any]:
    """The workflow as PyYAML parses it. A key set twice, a document that is not a mapping or a YAML 1.1 ``on`` key raises or is normalized."""
    duplicates = duplicate_keys(yaml.compose(text, Loader=yaml.SafeLoader))
    if duplicates:
        raise AssertionError(f"a key is set twice: {duplicates}")
    workflow = yaml.safe_load(text)
    if not isinstance(workflow, dict):
        raise AssertionError("the workflow is not a mapping")
    return {("on" if key is True else key): value for key, value in workflow.items()}


def duplicate_keys(node: yaml.Node | None, seen: set[int] | None = None) -> list[str]:
    """The keys that a mapping of the composed YAML sets twice. PyYAML would silently keep the last one."""
    seen = set() if seen is None else seen
    if node is None or id(node) in seen:
        return []
    seen.add(id(node))
    found: list[str] = []
    if isinstance(node, yaml.MappingNode):
        keys = [key.value for key, _ in node.value if isinstance(key, yaml.ScalarNode) and key.tag != "tag:yaml.org,2002:merge"]
        found += [key for key, count in Counter(keys).items() if count > 1]
        for key, value in node.value:
            found += duplicate_keys(key, seen) + duplicate_keys(value, seen)
    elif isinstance(node, yaml.SequenceNode):
        for item in node.value:
            found += duplicate_keys(item, seen)
    return found


def substituted(text: str, values: dict[str, str]) -> str:
    """``text`` with each ``${{ expression }}`` replaced by its value; an expression without a value raises ``KeyError``."""
    return EXPRESSION.sub(lambda found: values[found.group(1)], text)


def step_problems(name: str, steps: list[Any], *, shell_pinned: bool = False) -> list[str]:
    """A step that skips, lets a failure pass or runs under another shell or folder. Only the guards' log tail and the record checks and uploads may have one.

    The aggregate's step is the one that sets a shell (``shell_pinned``): its value is compared with the approved one on its own.
    """
    problems = []
    for position, step in enumerate(steps, 1):
        if not isinstance(step, dict):
            problems.append(f"{name}: step {position} is not a mapping")
            continue
        if "continue-on-error" in step:
            problems.append(f"{name}: continue-on-error is set on step {position}, so a failed check can pass")
        if "if" in step and step["if"] not in STEP_CONDITIONS.get(name, set()):
            problems.append(f"{name}: step {position} runs behind the condition {step['if']!r}")
        if "shell" in step and not shell_pinned:
            problems.append(f"{name}: step {position} sets the shell {step['shell']!r}, which can turn its command into a no-op")
        if "working-directory" in step:
            problems.append(f"{name}: step {position} sets the working directory {step['working-directory']!r}, so it may run its command elsewhere")
    return problems


def runs_of(steps: list[Any]) -> list[str]:
    """The commands of the steps, without the record check, whose script the probe in ``record_check_problems`` runs."""
    return [str(step["run"]) for step in steps if isinstance(step, dict) and "run" in step and step.get("name") != RECORD_CHECK_NAME]


@functools.cache
def record_check_script_problems(script: str) -> tuple[str, ...]:
    """What is wrong with the record check's ``script``: it must pass in a run folder tree that holds the record and its label, and fail in every other.

    The script runs with the options that GitHub gives a ``run`` step without a ``shell`` (``bash -e {0}``), in a folder with the files of each case.
    """
    problems = []
    for label, (paths, passes) in RECORD_CHECK_CASES.items():
        with tempfile.TemporaryDirectory(prefix="ci_record_check_") as folder:
            for path in paths:
                path, _, link_target = path.partition(" -> ")
                target = Path(folder) / path
                if link_target:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.symlink_to(Path(folder) / link_target)
                elif path.endswith("/"):
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("{}\n")
            (Path(folder) / "step.sh").write_text(script)
            run = run_command("bash", ["--noprofile", "--norc", "-e", "step.sh"], env={"PATH": os.environ["PATH"]}, cwd=Path(folder), timeout=TIMEOUT_SECONDS)
        if (run.returncode == 0) != passes:
            problems.append(f"its record check {'fails' if passes else 'passes'} with {label}")
    return tuple(problems)


def record_check_problems(name: str, job: dict[str, Any]) -> list[str]:
    """The record check is the one step named so, directly before the upload step, and its script does its work."""
    steps = [step for step in job.get("steps") or [] if isinstance(step, dict)]
    checks = [position for position, step in enumerate(steps) if step.get("name") == RECORD_CHECK_NAME]
    uploads = [position for position, step in enumerate(steps) if str(step.get("uses", "")).startswith("actions/upload-artifact@")]
    if len(checks) != 1:
        return [f"{name}: it has {len(checks)} steps named {RECORD_CHECK_NAME!r}, not one"]
    problems = []
    if len(uploads) != 1 or uploads[0] != checks[0] + 1:
        problems.append(f"{name}: the record check is not directly before the one upload step")
    if "run" not in steps[checks[0]]:
        return [*problems, f"{name}: the record check runs no script"]
    return [*problems, *(f"{name}: {problem}" for problem in record_check_script_problems(str(steps[checks[0]]["run"])))]


def job_problems(name: str, job: dict[str, Any]) -> list[str]:
    """A group job runs unconditionally, with no wait on another job, no tolerated failure, no other shell and exactly its pinned steps."""
    problems = []
    pinned = PINNED_JOBS[name]
    if "if" in job:
        problems.append(f"{name}: it runs behind the condition {job['if']!r}")
    if "needs" in job:
        problems.append(f"{name}: it waits for {job['needs']}, and a failed or skipped job before it would skip it")
    if "continue-on-error" in job:
        problems.append(f"{name}: continue-on-error is set on the job, so a failed check can pass")
    for key in sorted(set(job) - JOB_KEYS - {"if", "needs", "continue-on-error"} - ({"strategy", "outputs"} if name == "guards" else set())):
        problems.append(f"{name}: it has the key {key!r}, which the contract does not know")
    if job.get("runs-on") != RUNNER:
        problems.append(f"{name}: it runs on {job.get('runs-on')}, not {RUNNER}")
    if job.get("timeout-minutes") != pinned["timeout-minutes"]:
        problems.append(f"{name}: its timeout is {job.get('timeout-minutes')!r} minutes, not {pinned['timeout-minutes']}")
    if job.get("name") != pinned["name"]:
        problems.append(f"{name}: its name is {job.get('name')!r}, not {pinned['name']!r}")
    steps = job.get("steps") or []
    step_found = step_problems(name, steps)
    problems += step_found
    if runs_of(steps) != runs_of(pinned["steps"]):
        problems.append(f"{name}: it runs {runs_of(steps)}, not {runs_of(pinned['steps'])}")
    elif not step_found and len(steps) != len(pinned["steps"]):
        problems.append(f"{name}: it has {len(steps)} steps, not {len(pinned['steps'])}")
    elif not step_found and steps != pinned["steps"]:
        differing = [position for position, (step, wanted) in enumerate(zip(steps, pinned["steps"], strict=False), 1) if step != wanted]
        problems.append(f"{name}: step {', '.join(map(str, differing))} differs from the pinned one (its action, version or inputs)")
    return problems


def matrix_problems(job: dict[str, Any]) -> list[str]:
    """The effective guard matrix: every shard once, none excluded, each entry running its own shard, and one failed shard cancels no other."""
    strategy = job.get("strategy") or {}
    matrix = strategy.get("matrix") or {}
    problems = []
    if strategy.get("fail-fast") is not False:
        problems.append("guards: one failed shard cancels the others")
    if set(strategy) - {"fail-fast", "matrix"}:
        problems.append(f"guards: the strategy has {sorted(set(strategy) - {'fail-fast', 'matrix'})}, which the contract does not know")
    for key in sorted(set(matrix) - {"shard"}):
        problems.append(f"guards: the matrix uses {key}, which changes the shards that run")
    shards = matrix.get("shard")
    if not isinstance(shards, list):
        return [*problems, f"guards: the matrix's shards are {shards!r}, not a list"]
    excluded = [entry.get("shard") for entry in matrix.get("exclude") or [] if isinstance(entry, dict)]
    suite = [step for step in job.get("steps") or [] if str(step.get("run", "")).startswith("make test-guards")]
    if len(suite) != 1 or suite[0]["run"] != SHARD_COMMAND:
        return [*problems, f"guards: the suite runs {[step.get('run') for step in suite]}"]
    counts = Counter(
        f"{str((suite[0].get('env') or {}).get('SHARD', '')).replace('${{ matrix.shard }}', str(value))}/{SHARDS}" for value in shards if value not in excluded
    )
    expected = [f"{shard}/{SHARDS}" for shard in range(1, SHARDS + 1)]
    if [shard for shard in expected if shard not in counts]:
        problems.append(f"guards: the shards {[shard for shard in expected if shard not in counts]} never run")
    if [shard for shard, count in counts.items() if count > 1]:
        problems.append(f"guards: the shards {[shard for shard, count in counts.items() if count > 1]} run more than once")
    if [shard for shard in counts if shard not in expected]:
        problems.append(f"guards: the matrix runs shards {[shard for shard in counts if shard not in expected]} that do not exist")
    return problems


TOKEN = re.compile(r"\s*(?:(github\.[a-z_]+)|'([^']*)'|(==|!=|&&|\|\||!|\(|\)))")


class Expression:
    """GitHub's expression syntax as far as the concurrency settings use it: ``github.<name>`` contexts, quoted strings, ``==``, ``!=``, ``!``, ``&&``,
    ``||`` and parentheses. ``a && b`` gives ``a`` when ``a`` is falsy and ``b`` otherwise; ``a || b`` gives ``a`` when ``a`` is truthy and ``b`` otherwise,
    which is how GitHub reads them. Anything else raises ``ValueError``, so a setting that this reader does not understand is a finding."""

    def __init__(self, text: str, context: dict[str, str]) -> None:
        self.context = context
        self.tokens: list[tuple[str, str]] = []
        position = 0
        while text[position:].strip():
            found = TOKEN.match(text, position)
            if found is None:
                raise ValueError(f"cannot read {text[position:].strip()!r} in the expression {text!r}")
            context_name, literal, operator = found.groups()
            self.tokens.append(("context", context_name) if context_name else ("operator", operator) if operator else ("string", literal))
            position = found.end()
        self.position = 0

    def take(self, *operators: str) -> str | None:
        if self.position < len(self.tokens) and self.tokens[self.position][0] == "operator" and self.tokens[self.position][1] in operators:
            self.position += 1
            return self.tokens[self.position - 1][1]
        return None

    def value(self) -> Any:
        result = self.disjunction()
        if self.position != len(self.tokens):
            raise ValueError(f"unexpected {self.tokens[self.position][1]!r}")
        return result

    def disjunction(self) -> Any:
        result = self.conjunction()
        while self.take("||"):
            other = self.conjunction()
            result = result if result else other
        return result

    def conjunction(self) -> Any:
        result = self.comparison()
        while self.take("&&"):
            other = self.comparison()
            result = other if result else result
        return result

    def comparison(self) -> Any:
        left = self.negation()
        operator = self.take("==", "!=")
        if operator is None:
            return left
        right = self.negation()
        return (str(left).lower() == str(right).lower()) == (operator == "==")

    def negation(self) -> Any:
        if self.take("!"):
            return not self.negation()
        return self.atom()

    def atom(self) -> Any:
        if self.take("("):
            result = self.disjunction()
            if not self.take(")"):
                raise ValueError("a parenthesis is not closed")
            return result
        if self.position >= len(self.tokens) or self.tokens[self.position][0] == "operator":
            raise ValueError("an operand is missing")
        kind, text = self.tokens[self.position]
        self.position += 1
        if kind == "string":
            return text
        name = text.removeprefix("github.")
        if name not in self.context:
            raise ValueError(f"the context {text} is not one that this contract knows")
        return self.context[name]


def evaluated(setting: Any, context: dict[str, str]) -> Any:
    """A workflow setting with every ``${{ expression }}`` filled in. A setting that is one expression gives the expression's own value."""
    if not isinstance(setting, str):
        return setting
    whole = EXPRESSION.fullmatch(setting)
    if whole:
        return Expression(whole.group(1), context).value()
    return EXPRESSION.sub(lambda found: str(Expression(found.group(1), context).value()), setting)


def concurrency_problems(concurrency: Any) -> list[str]:
    """Evaluate the group and the cancel setting for each kind of run: a work-branch push or pull request run is superseded by a newer run of its ref,
    and a run on main, a scheduled run and a manual run is never cancelled and never replaced, not even while pending."""
    if not isinstance(concurrency, dict) or set(concurrency) != {"group", "cancel-in-progress"}:
        return [f"the workflow's concurrency is {concurrency!r}, not the keys ['cancel-in-progress', 'group']"]
    problems: list[str] = []
    groups: dict[str, str] = {}
    try:
        for label, (context, superseded) in CONCURRENCY_RUNS.items():
            first = {**context, "workflow": "ci", "run_id": "1001"}
            newer = {**context, "workflow": "ci", "run_id": "1002"}
            cancelled = evaluated(concurrency["cancel-in-progress"], first)
            if bool(cancelled) != superseded:
                problems.append(f"concurrency: {label} {'is never superseded' if superseded else 'would be cancelled by a newer run'}")
            same = evaluated(concurrency["group"], first) == evaluated(concurrency["group"], newer)
            if same != superseded:
                problems.append(
                    f"concurrency: a newer run of {label} {'does not share its group, so it supersedes nothing' if superseded else 'replaces its pending run'}"
                )
            groups[label] = str(evaluated(concurrency["group"], first))
        for label, context in OTHER_WORK_REFS:
            other = str(evaluated(concurrency["group"], {**context, "workflow": "ci", "run_id": "1003"}))
            for earlier, group in groups.items():
                if group == other:
                    problems.append(f"concurrency: {label} shares the group of {earlier}, so one cancels the other")
    except ValueError as error:
        problems.append(f"concurrency: it cannot be read: {error}")
    return problems


def on_problems(triggers: Any) -> list[str]:
    """The workflow starts on each push of any branch, on pull requests, on a weekly schedule and on demand."""
    if not isinstance(triggers, dict):
        return [f"the workflow starts on {triggers!r}, not on a mapping of events"]
    problems = []
    for event, wanted in ON_TRIGGERS.items():
        if event not in triggers:
            problems.append(f"the workflow does not start on {event}")
        elif triggers[event] != wanted:
            problems.append(f"the workflow starts on {event} with {triggers[event]!r}, not {wanted!r}")
    schedule = triggers.get("schedule")
    if not isinstance(schedule, list) or len(schedule) != 1 or not isinstance(schedule[0], dict) or set(schedule[0]) != {"cron"}:
        problems.append(f"the workflow's schedule is {schedule!r}, not one cron entry")
    elif not CRON_WEEKLY.fullmatch(str(schedule[0]["cron"])):
        problems.append(f"the workflow's schedule {schedule[0]['cron']!r} is not weekly (minute, hour, any day, any month, one weekday)")
    problems += [f"the workflow starts on {event}, which the contract does not know" for event in sorted(set(triggers) - {*ON_TRIGGERS, "schedule"})]
    return problems


def headroom_problems(job: Any) -> list[str]:
    """The headroom job is outside the required check, reads only, waits for the jobs that it measures and runs its pinned steps with no tolerated failure."""
    if not isinstance(job, dict):
        return ["headroom: it is not a job"]
    problems = []
    for key, wanted in HEADROOM_JOB.items():
        if key != "steps" and job.get(key) != wanted:
            problems.append(f"headroom: its {key} is {job.get(key)!r}, not {wanted!r}")
    problems += [f"headroom: it has the key {key!r}, which the contract does not know" for key in sorted(set(job) - set(HEADROOM_JOB))]
    steps = job.get("steps") or []
    problems += step_problems(HEADROOM, steps)
    if steps != HEADROOM_JOB["steps"]:
        problems.append(f"headroom: its steps are not the pinned ones: {runs_of(steps)}, not {runs_of(HEADROOM_JOB['steps'])}")
    return problems


@functools.cache
def leg_script_problems(script: str) -> tuple[str, ...]:
    """Each guard leg's last step sets the leg's own output name to success: it is run for every shard, as GitHub runs a step without a shell."""
    problems = []
    for leg in LEGS:
        with tempfile.TemporaryDirectory(prefix="ci_leg_step_") as folder:
            (Path(folder) / "step.sh").write_text(script)
            output = Path(folder) / "output"
            output.write_text("")
            env = {"PATH": os.environ["PATH"], "SHARD": str(leg), "GITHUB_OUTPUT": str(output)}
            run_command("bash", ["--noprofile", "--norc", "-e", "step.sh"], env=env, cwd=Path(folder), timeout=TIMEOUT_SECONDS)
            if output.read_text() != f"shard_{leg}=success\n":
                problems.append(f"guards: its last step writes {output.read_text()!r} for shard {leg}, not 'shard_{leg}=success'")
    return tuple(problems)


def leg_problems(job: dict[str, Any]) -> list[str]:
    """The guard job's outputs are the eight leg names and its last step sets the running leg's own name, so no leg can count as passed when it never ran."""
    problems = []
    if job.get("outputs") != GUARD_OUTPUTS:
        problems.append(f"guards: its outputs are {job.get('outputs')!r}, not one steps.leg output for each of the shards {list(LEGS)}")
    steps = [step for step in job.get("steps") or [] if isinstance(step, dict)]
    if not steps or steps[-1].get("id") != "leg" or "run" not in steps[-1]:
        problems.append("guards: its last step is not the one with the id leg")
    else:
        problems += leg_script_problems(str(steps[-1]["run"]))
    return problems


TIMEOUT_LINE = re.compile(r"^    timeout-minutes: (\d+)(?: # (.*))?$")
JOB_LINE = re.compile(r"^  ([a-z_]+):$")
MEASURED = re.compile(r"\bmeasured (\d+(?:\.\d+)?) min\b")


def duration_problems(text: str) -> list[str]:
    """Each job's timeout line carries the duration that it was measured at (``# measured 11 min ...``) and is at least ``TIMEOUT_FACTOR`` times that."""
    problems = []
    seen = set()
    job = ""
    for line in text.splitlines():
        if line == "jobs:":
            job = ""
        elif found := JOB_LINE.match(line):
            job = found.group(1)
        elif (limit := TIMEOUT_LINE.match(line)) and job:
            seen.add(job)
            measured = MEASURED.search(limit.group(2) or "")
            if measured is None:
                problems.append(f"{job}: its timeout of {limit.group(1)} minutes has no 'measured N min' duration beside it")
            elif int(limit.group(1)) < TIMEOUT_FACTOR * float(measured.group(1)):
                problems.append(f"{job}: its timeout of {limit.group(1)} minutes is less than {TIMEOUT_FACTOR} times its measured {measured.group(1)} minutes")
    problems += [f"{missing}: it has no timeout line" for missing in sorted({*GROUPS, HEADROOM, "required"} - seen)]
    return problems


def required_scenarios() -> list[tuple[tuple[str, ...], tuple[bool, ...]]]:
    """Every combination of the five jobs' results with every guard leg's output set, and the legs' outputs on their own with every job a success:
    all legs set, each leg unset, each leg alone set and no leg set."""
    ok_legs = (True,) * len(LEGS)
    scenarios = [(combination, ok_legs) for combination in itertools.product(RESULTS, repeat=len(GROUPS))]
    ok_results = ("success",) * len(GROUPS)
    scenarios += [(ok_results, tuple(leg != unset for leg in LEGS)) for unset in LEGS]
    scenarios += [(ok_results, tuple(leg == only for leg in LEGS)) for only in LEGS]
    scenarios.append((ok_results, (False,) * len(LEGS)))
    return scenarios


@functools.cache
def script_problems(script: str, variables: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """What is wrong with the required job's ``script`` as it passes only when every needed job succeeded and every guard leg set its own output.

    The script is written to a file exactly as PyYAML returned it and run with the shell and options that GitHub uses for ``bash``, for every
    combination of the results of the needed jobs and for the guard legs' outputs set or unset one by one. ``variables`` are the step's
    environment; each value may name an expression of the workflow. Cached: the variants of the contract test share most scripts.
    """
    wrong: list[tuple[tuple[str, ...], tuple[bool, ...]]] = []
    scenarios = required_scenarios()
    with tempfile.TemporaryDirectory(prefix="ci_required_script_") as folder:
        path = Path(folder) / "step.sh"
        path.write_text(script)
        for combination, legs in scenarios:
            values = {f"needs.{name}.result": result for name, result in zip(GROUPS, combination, strict=True)}
            values |= {f"needs.guards.outputs.shard_{leg}": "success" if set_ else "" for leg, set_ in zip(LEGS, legs, strict=True)}
            try:
                environment = {name: substituted(text, values) for name, text in variables}
            except KeyError as error:
                return (f"required: its step reads {error}, which no needed job gives",)
            run = run_command(
                "bash",
                [*GITHUB_BASH, str(path)],
                env={"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": os.devnull, **environment},
                cwd=Path(folder),
                timeout=TIMEOUT_SECONDS,
            )
            if (run.returncode == 0) != (set(combination) == {"success"} and all(legs)):
                wrong.append((combination, legs))
    if not wrong:
        return ()
    wrong.sort(key=lambda case: sum(result != "success" for result in case[0]) + sum(not set_ for set_ in case[1]))
    results, legs = wrong[0]
    example = ", ".join(f"{name}={result}" for name, result in zip(GROUPS, results, strict=True))
    if not all(legs):
        example += f", guards shards without their output: {[leg for leg, set_ in zip(LEGS, legs, strict=True) if not set_]}"
    verdict = (
        "passes when a needed job did not succeed"
        if any(not (set(case[0]) == {"success"} and all(case[1])) for case in wrong)
        else "fails when every job succeeded"
    )
    return (f"required: its script {verdict}, for {len(wrong)} of {len(scenarios)} combinations of results, for example {example}",)


def aggregate_problems(jobs: dict[str, Any]) -> list[str]:
    """The required job has exactly the approved keys and values, waits for every group, always runs, and its one step is enabled and gives the verdict."""
    required = jobs["required"]
    problems = []
    needs = required.get("needs") if isinstance(required.get("needs"), list) else [required.get("needs")]
    for missing in sorted(set(GROUPS) - set(needs)):
        problems.append(f"required: {missing} is missing from its needs, so the check can pass without it")
    if sorted(map(str, needs)) != sorted(GROUPS):
        problems.append(f"required: it waits for {needs}, not for exactly {list(GROUPS)}")
    if required.get("if") != "always()":
        problems.append(
            f"required: its condition is {required.get('if')!r}, not always(): GitHub can accept a skipped required job as passing, "
            "so this job must never be skipped"
        )
    if "continue-on-error" in required:
        problems.append("required: continue-on-error is set on the job")
    if required.get("name") != REQUIRED_NAME:
        problems.append(f"required: its name is {required.get('name')!r}, not {REQUIRED_NAME!r}")
    if required.get("runs-on") != RUNNER:
        problems.append(f"required: it runs on {required.get('runs-on')}, not {RUNNER}")
    if required.get("timeout-minutes") != 5:
        problems.append(f"required: its timeout is {required.get('timeout-minutes')!r} minutes, not 5")
    for key in sorted(set(required) - REQUIRED_KEYS - {"continue-on-error"}):
        problems.append(f"required: it has the key {key!r}, which the contract does not know")
    steps = required.get("steps") or []
    if len(steps) != 1 or not isinstance(steps[0], dict) or "run" not in steps[0]:
        return [*problems, "required: it has not exactly one step that runs a script"]
    step = steps[0]
    problems += step_problems("required", steps, shell_pinned=True)
    for key in sorted(set(step) - {*REQUIRED_STEP, "run", "if", "continue-on-error", "working-directory"}):
        problems.append(f"required: its step has the key {key!r}, which the contract does not know")
    for key in ("name", "shell"):
        if step.get(key) != REQUIRED_STEP[key]:
            problems.append(f"required: its step's {key} is {step.get(key)!r}, not {REQUIRED_STEP[key]!r}")
    environment = step.get("env")
    if not isinstance(environment, dict):
        problems.append(f"required: its step's env is {environment!r}, not the results of the needed jobs")
        environment = {}
    if set(environment) != set(REQUIRED_ENV_VALUES):
        problems.append(f"required: its env has the keys {sorted(environment)}, not {sorted(REQUIRED_ENV_VALUES)}")
    for name in sorted(set(environment) & set(REQUIRED_ENV_VALUES)):
        if environment[name] != REQUIRED_ENV_VALUES[name]:
            problems.append(f"required: its env entry {name} is {environment[name]!r}, not {REQUIRED_ENV_VALUES[name]!r}")
    script = str(step["run"])
    if "${{" in script:
        problems.append("required: its script holds an expression (a context that GitHub would fill in before the shell runs); only its env may")
    if script != REQUIRED_SCRIPT:
        problems.append(f"required: its script differs from the approved one at {first_difference(script, REQUIRED_SCRIPT)}")
    variables = tuple((str(name), str(text)) for name, text in environment.items())
    return [*problems, *script_problems(script, variables)]


def first_difference(actual: str, wanted: str) -> str:
    """The first line at which two texts differ, each shown with its line end."""
    for number, (found, expected) in enumerate(itertools.zip_longest(actual.splitlines(keepends=True), wanted.splitlines(keepends=True)), 1):
        if found != expected:
            return f"line {number}: {found!r}, not {expected!r}"
    return "no line"


def contract_problems(text: str) -> list[str]:
    """What the workflow fails to do: run every group job on every push, pull request, schedule and manual run, each shard once, with the pinned commands,
    and fail the one required job unless every group job succeeded and every guard leg set its own output."""
    problems: list[str] = []
    try:
        problems += source_problems(text)
        workflow = load_workflow(text)
        if set(workflow) != {"name", "on", "permissions", "concurrency", "jobs"}:
            problems.append(f"the workflow has the keys {sorted(map(str, workflow))}")
        problems += on_problems(workflow.get("on"))
        if workflow.get("permissions") != {"contents": "read"}:
            problems.append(f"the workflow asks for the permissions {workflow.get('permissions')}")
        problems += concurrency_problems(workflow.get("concurrency"))
        if set(workflow["jobs"]) != {*GROUPS, HEADROOM, "required"}:
            problems.append(f"the workflow's jobs are {sorted(workflow['jobs'])}, not {sorted({*GROUPS, HEADROOM, 'required'})}")
        for name in GROUPS:
            problems += job_problems(name, workflow["jobs"][name])
        problems += matrix_problems(workflow["jobs"]["guards"])
        problems += leg_problems(workflow["jobs"]["guards"])
        problems += headroom_problems(workflow["jobs"][HEADROOM])
        for name in ("guards", "deadline"):
            problems += record_check_problems(name, workflow["jobs"][name])
        problems += aggregate_problems(workflow["jobs"])
    except (AssertionError, KeyError, StopIteration, ValueError, TypeError, AttributeError, yaml.YAMLError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


def as_tuple(value: str | tuple[str, ...]) -> tuple[str, ...]:
    """A planted case gives one text or several: each is replaced in order, and each diagnostic must appear."""
    return (value,) if isinstance(value, str) else value


TERRAFORM = "      - run: make check-terraform\n"
ANSIBLE = "      - run: make check-ansible\n"
GUARDS = '      - run: make test-guards SHARD="${SHARD}/8"\n'
DEADLINE_RUN = "      - run: make test-deadline\n"
UPLOAD_ACTION = f"      - name: Keep the run record and its retention label\n        if: ${{{{ !cancelled() }}}}\n        uses: {UPLOAD_ARTIFACT} # v7.0.2\n"
UPLOAD_FILES = (
    "          path: |\n            runs/*/record.json\n            runs/*/retention.json\n          if-no-files-found: error\n          retention-days: 30\n"
)
TAIL_YAML = "      - name: Show the end of the run's log after a failure\n        if: failure()\n        run: tail -n 150 runs/*/log.txt\n"
CHECK_YAML = f"      - name: {RECORD_CHECK_NAME}\n        if: ${{{{ !cancelled() }}}}\n        run: |\n" + "".join(
    f"          {line}\n" for line in RECORD_CHECK_SCRIPT.splitlines()
)
# The check that the upload's own patterns would give: one file of each name anywhere under runs/, not both in each run folder.
LOOSE_CHECK_SCRIPT = (
    'shopt -s nullglob\nrecords=(runs/*/record.json)\nlabels=(runs/*/retention.json)\n[ "${#records[@]}" -gt 0 ] && [ "${#labels[@]}" -gt 0 ]\n'
)
LOOSE_CHECK_YAML = f"      - name: {RECORD_CHECK_NAME}\n        if: ${{{{ !cancelled() }}}}\n        run: |\n" + "".join(
    f"          {line}\n" for line in LOOSE_CHECK_SCRIPT.splitlines()
)
# The check and the upload of one job, anchored to the job by the step before them.
GUARD_CHECK = TAIL_YAML + CHECK_YAML
DEADLINE_CHECK = DEADLINE_RUN + CHECK_YAML
REQUIRED_HEAD = "    if: always()\n    runs-on: ubuntu-24.04\n    timeout-minutes: 5"
LEG_YAML = (
    '      - id: leg\n        name: Set this shard\'s own success output\n        run: echo "shard_${SHARD}=success" >> "${GITHUB_OUTPUT}"\n'
    "        env:\n          SHARD: ${{ matrix.shard }}\n"
)
GUARD_NAME = "    name: Planted faults through the real commit path, shard ${{ matrix.shard }} of 8\n"
GUARD_RUNS = "    runs-on: ubuntu-24.04\n    timeout-minutes: 25"
HEADROOM_TOP = "    needs: [hooks, infrastructure, slow, guards, deadline, required]\n    if: always()\n    runs-on: ubuntu-24.04\n    timeout-minutes: 10"
HEADROOM_RUN = "      - run: make headroom\n"
LEG_FUNCTION_END = "              failed=1\n            fi\n          }\n          check_leg 1"
GUARD_UPLOAD = UPLOAD_ACTION + "        with:\n          name: guard-record-shard-${{ matrix.shard }}\n" + UPLOAD_FILES
DEADLINE_UPLOAD = UPLOAD_ACTION + "        with:\n          name: deadline-record\n" + UPLOAD_FILES
REQUIRED_ENV = "        env:\n          HOOKS:"
SUPERSEDED = "github.event_name == 'pull_request' || (github.event_name == 'push' && github.ref != 'refs/heads/main')"
GROUP_LINE = f"  group: ${{{{ github.workflow }}}}-${{{{ ({SUPERSEDED}) && github.ref || github.run_id }}}}\n"
CANCEL_LINE = f"  cancel-in-progress: ${{{{ {SUPERSEDED} }}}}\n"
ON_BLOCK = 'on:\n  push:\n    branches: ["**"]\n  pull_request:\n  schedule:\n    - cron: "23 6 * * 1"\n  workflow_dispatch:\n'
CRON_LINE = '    - cron: "23 6 * * 1"\n'
NEEDS_LINE = "needs: [hooks, infrastructure, slow, guards, deadline]"
ACCEPTS = 'if [ "$2" != success ]; then'
NOT_SUCCESS = "required: its script passes when a needed job did not succeed"
# Each planted change to the workflow text: (the text to replace, its replacement, the diagnostic that the contract must give).
PLANTS: dict[str, tuple[str | tuple[str, ...], str | tuple[str, ...], str | tuple[str, ...]]] = {
    "the guards job never run": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    if: false\n" + GUARD_RUNS,
        "guards: it runs behind the condition False",
    ),
    "the guards job run only on push": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    if: github.event_name == 'push'\n" + GUARD_RUNS,
        "guards: it runs behind the condition \"github.event_name == 'push'\"",
    ),
    "the guards job behind a spaced if key": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    if : ${{ false }}\n" + GUARD_RUNS,
        "guards: it runs behind the condition '${{ false }}'",
    ),
    "the hooks job run only on pull requests": (
        "    name: Hooks on every file and the commit messages\n",
        "    name: Hooks on every file and the commit messages\n    if: github.event_name == 'pull_request'\n",
        "hooks: it runs behind the condition",
    ),
    "the infrastructure job behind a path condition": (
        "    name: Static checks of the Terraform and Ansible code\n",
        "    name: Static checks of the Terraform and Ansible code\n    if: contains(github.event.head_commit.message, 'infra')\n",
        "infrastructure: it runs behind the condition",
    ),
    "the infrastructure job waits for the hooks": (
        "    name: Static checks of the Terraform and Ansible code\n",
        "    name: Static checks of the Terraform and Ansible code\n    needs: hooks\n",
        "infrastructure: it waits for hooks",
    ),
    "a step behind a condition in the infrastructure job": (
        TERRAFORM,
        TERRAFORM + "        if: github.event_name == 'push'\n",
        "infrastructure: step 5 runs behind the condition",
    ),
    "the guard suite never run": (GUARDS, GUARDS + "        if: false\n", "guards: step 4 runs behind the condition False"),
    "a guard job's failure tolerated": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    continue-on-error: true\n" + GUARD_RUNS,
        "guards: continue-on-error is set on the job",
    ),
    "a guard job's failure tolerated through a quoted key": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + '    "continue-on-error": true\n' + GUARD_RUNS,
        "guards: continue-on-error is set on the job",
    ),
    "a hooks step's failure tolerated": (
        "      - run: make check\n",
        "      - run: make check\n        continue-on-error: true\n",
        "hooks: continue-on-error is set on step 4",
    ),
    "a hooks step's failure tolerated through a YAML anchor and merge key": (
        "      - run: make check\n",
        "      - <<: &tolerate {run: make check, continue-on-error: true}\n",
        "hooks: continue-on-error is set on step 4",
    ),
    "the Terraform checks' failure tolerated": (
        TERRAFORM,
        TERRAFORM + "        continue-on-error: true\n",
        "infrastructure: continue-on-error is set on step 5",
    ),
    "the Terraform checks under another shell": (
        TERRAFORM,
        TERRAFORM + "        shell: bash -n {0}\n",
        "infrastructure: step 5 sets the shell 'bash -n {0}'",
    ),
    "the hooks checks under another shell": (
        "      - run: make check\n",
        "      - run: make check\n        shell: bash -n {0}\n",
        "hooks: step 4 sets the shell 'bash -n {0}'",
    ),
    "a guards step in another folder": (
        GUARDS,
        GUARDS + "        working-directory: docs\n",
        "guards: step 4 sets the working directory 'docs'",
    ),
    "every shell of the guards job replaced": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    defaults:\n      run:\n        shell: bash -n {0}\n" + GUARD_RUNS,
        "guards: it has the key 'defaults'",
    ),
    "every shell of the workflow replaced": (
        "permissions:\n  contents: read\n",
        "permissions:\n  contents: read\n\ndefaults:\n  run:\n    shell: bash -n {0}\n",
        "the workflow has the keys",
    ),
    "a key set twice": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    runs-on: ubuntu-24.04\n    timeout-minutes: 99\n    timeout-minutes: 25",
        "a key is set twice",
    ),
    "the guards job's timeout changed": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + GUARD_RUNS.replace("minutes: 25", "minutes: 30"),
        "guards: its timeout is 30 minutes, not 25",
    ),
    "the hooks job renamed": (
        "    name: Hooks on every file and the commit messages\n",
        "    name: Hooks\n",
        "hooks: its name is 'Hooks', not 'Hooks on every file and the commit messages'",
    ),
    "the required job's failure tolerated": (
        "    name: Every job of this run passed\n",
        "    name: Every job of this run passed\n    continue-on-error: true\n",
        "required: continue-on-error is set on the job",
    ),
    "the required job's step tolerated": (
        REQUIRED_ENV,
        "        continue-on-error: true\n" + REQUIRED_ENV,
        "required: continue-on-error is set on step 1",
    ),
    "the required job's step tolerated through a quoted key": (
        REQUIRED_ENV,
        '        "continue-on-error": true\n' + REQUIRED_ENV,
        "required: continue-on-error is set on step 1",
    ),
    "the required job's step under another shell": (
        "        shell: bash\n",
        "        shell: bash -n {0}\n",
        "required: its step's shell is 'bash -n {0}', not 'bash'",
    ),
    "the required job's step under sh, which has no pipefail": (
        "        shell: bash\n",
        "        shell: sh\n",
        "required: its step's shell is 'sh', not 'bash'",
    ),
    "the required job's step with no shell, so GitHub runs bash -e only": (
        "        shell: bash\n",
        "",
        "required: its step's shell is None, not 'bash'",
    ),
    "the required job's exit masked by a pipe under bash -e (the reviewer's first plant)": (
        ("        shell: bash\n", '          exit "${failed}"\n'),
        ("", '          ( exit "${failed}" ) | true\n'),
        ("required: its step's shell is None, not 'bash'", "required: its script differs from the approved one at line 27"),
    ),
    "the required job's exit masked by a pipe under the approved shell": (
        '          exit "${failed}"\n',
        '          ( exit "${failed}" ) | true\n',
        "required: its script differs from the approved one at line 27",
    ),
    "the required job's script holds a context in a comment (the reviewer's second plant)": (
        '          exit "${failed}"\n',
        '          exit "${failed}" # ${{ github.event_name }}\n',
        ("required: its script holds an expression", "required: its script differs from the approved one at line 27"),
    ),
    "the required job's script reads a context": (
        'check hooks "${HOOKS}"',
        'check hooks "${{ github.event_name }}"',
        ("required: its script holds an expression", "required: its script differs from the approved one at line 8"),
    ),
    "the required job's script given a harmless extra line": (
        "          failed=0\n          check() {",
        "          failed=0\n          :\n          check() {",
        "required: its script differs from the approved one at line 2",
    ),
    "the required job's script without its final newline": (
        "        run: |\n          failed=0\n",
        "        run: |-\n          failed=0\n",
        "required: its script differs from the approved one at line 27",
    ),
    "the required job's script with a trailing space": (
        '          exit "${failed}"\n',
        '          exit "${failed}" \n',
        "required: its script differs from the approved one at line 27",
    ),
    "the required job's env given an extra key with no value (the reviewer's low plant)": (
        "          GUARDS: ${{ needs.guards.result }}\n",
        "          GUARDS: ${{ needs.guards.result }}\n          UNUSED:\n",
        f"required: its env has the keys {sorted([*REQUIRED_ENV_VALUES, 'UNUSED'])}, not {sorted(REQUIRED_ENV_VALUES)}",
    ),
    "the required job's env entry reads another job": (
        "INFRASTRUCTURE: ${{ needs.infrastructure.result }}",
        "INFRASTRUCTURE: ${{ needs.hooks.result }}",
        "required: its env entry INFRASTRUCTURE is '${{ needs.hooks.result }}', not '${{ needs.infrastructure.result }}'",
    ),
    "the required job's step behind a spaced if key": (
        REQUIRED_ENV,
        "        if : ${{ false }}\n" + REQUIRED_ENV,
        "required: step 1 runs behind the condition '${{ false }}'",
    ),
    "the required job's step disabled through a YAML anchor and merge key": (
        REQUIRED_ENV,
        "        <<: &off {if: false}\n" + REQUIRED_ENV,
        "required: step 1 runs behind the condition False",
    ),
    "the required job's step disabled": (REQUIRED_ENV, "        if: false\n" + REQUIRED_ENV, "required: step 1 runs behind the condition False"),
    "the required job's step in another folder": (
        REQUIRED_ENV,
        "        working-directory: docs\n" + REQUIRED_ENV,
        "required: step 1 sets the working directory 'docs'",
    ),
    "the required job's step with an extra key": (
        REQUIRED_ENV,
        "        timeout-minutes: 1\n" + REQUIRED_ENV,
        "required: its step has the key 'timeout-minutes'",
    ),
    "the required job's step renamed": (
        "      - name: Check that every needed job succeeded\n",
        "      - name: Check\n",
        "required: its step's name is 'Check'",
    ),
    "the required job's step given another variable": (
        "          GUARDS: ${{ needs.guards.result }}\n",
        "          GUARDS: ${{ needs.guards.result }}\n          BASH_ENV: /dev/null\n",
        f"required: its env has the keys {sorted([*REQUIRED_ENV_VALUES, 'BASH_ENV'])}, not {sorted(REQUIRED_ENV_VALUES)}",
    ),
    "the required job given a shell default": (
        REQUIRED_HEAD,
        "    if: always()\n    runs-on: ubuntu-24.04\n    defaults:\n      run:\n        shell: bash -n {0}\n    timeout-minutes: 5",
        "required: it has the key 'defaults'",
    ),
    "the required job's timeout changed": (REQUIRED_HEAD, REQUIRED_HEAD.replace("minutes: 5", "minutes: 6"), "required: its timeout is 6 minutes, not 5"),
    "a shard dropped": ("shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7]", "guards: the shards ['8/8'] never run"),
    "one shard twice": ("shard: [1, 2, 3", "shard: [1, 1, 3", "guards: the shards ['1/8'] run more than once"),
    "a shard that does not exist": ("shard: [1, 2, 3, 4, 5, 6, 7, 8]", "shard: [1, 2, 3, 4, 5, 6, 7, 9]", "guards: the matrix runs shards ['9/8']"),
    "a shard excluded through the matrix": (
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n",
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n        exclude:\n          - shard: 3\n",
        "guards: the matrix uses exclude",
    ),
    "a shard excluded through the matrix, its effect": (
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n",
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n        exclude:\n          - shard: 3\n",
        "guards: the shards ['3/8'] never run",
    ),
    "a matrix entry added": (
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n",
        "        shard: [1, 2, 3, 4, 5, 6, 7, 8]\n        include:\n          - shard: 9\n",
        "guards: the matrix uses include",
    ),
    "shard 1 in every matrix entry": ('SHARD="${SHARD}/8"', 'SHARD="1/8"', "guards: the suite runs ['make test-guards SHARD=\"1/8\"']"),
    "shard 1 in the step's environment": ("SHARD: ${{ matrix.shard }}", 'SHARD: "1"', "guards: the shards ['2/8', '3/8'"),
    "one failed shard cancels the others": ("fail-fast: false", "fail-fast: true", "guards: one failed shard cancels the others"),
    "a check command made a dry run": (
        TERRAFORM,
        "      - run: make check-terraform -n\n",
        "infrastructure: it runs ['make setup', 'make check-terraform -n'",
    ),
    "the Terraform checks removed": (TERRAFORM, "", "infrastructure: it runs ['make setup', 'make check-ansible']"),
    "the Ansible check before the Terraform checks": (
        TERRAFORM + ANSIBLE,
        ANSIBLE + TERRAFORM,
        "infrastructure: it runs ['make setup', 'make check-ansible', 'make check-terraform']",
    ),
    "the hooks failure ignored": ("      - run: make check\n", "      - run: make check || true\n", "hooks: it runs ['make setup', 'make check || true'"),
    "the commit messages not checked": ("commit-range", "commit-range-skipped", "hooks: it runs"),
    "the guard suite removed": (GUARDS, "", "guards: the suite runs []"),
    "the guard setup removed": (
        "      - run: make setup\n      - run: make test-guards",
        "      - run: make test-guards",
        "guards: it runs ['make test-guards",
    ),
    "another Python in one job": (
        'python-version: "3.14.10"\n          cache',
        'python-version: "3.14"\n          cache',
        "hooks: step 2 differs from the pinned one",
    ),
    "another Terraform": ('terraform_version: "1.16.4"', 'terraform_version: "1.16.5"', "infrastructure: step 3 differs from the pinned one"),
    "a job on another runner": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + GUARD_RUNS.replace("ubuntu-24.04", "ubuntu-latest"),
        "guards: it runs on ubuntu-latest",
    ),
    "a job that asks to write": (
        GUARD_NAME + GUARD_RUNS,
        GUARD_NAME + "    permissions:\n      contents: write\n" + GUARD_RUNS,
        "guards: it has the key 'permissions'",
    ),
    "a plan job added": ("jobs:\n", "jobs:\n  plan:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: make check\n", "the workflow's jobs are"),
    "the required job without the guards in its needs": (
        NEEDS_LINE,
        "needs: [hooks, infrastructure, slow, deadline]",
        "required: guards is missing from its needs",
    ),
    "the required job without the hooks in its needs": (
        NEEDS_LINE,
        "needs: [infrastructure, slow, guards, deadline]",
        "required: hooks is missing from its needs",
    ),
    "the required job without the infrastructure job in its needs": (
        NEEDS_LINE,
        "needs: [hooks, slow, guards, deadline]",
        "required: infrastructure is missing from its needs",
    ),
    "the required job without the slow job in its needs": (
        NEEDS_LINE,
        "needs: [hooks, infrastructure, guards, deadline]",
        "required: slow is missing from its needs",
    ),
    "the required job does not check the slow job": (
        '          check slow "${SLOW}"\n',
        "",
        NOT_SUCCESS
        + ", for 3 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=failure, guards=success, deadline=success",
    ),
    "the required job without the deadline job in its needs": (
        NEEDS_LINE,
        "needs: [hooks, infrastructure, slow, guards]",
        "required: deadline is missing from its needs",
    ),
    "the required job does not check the deadline job": (
        '          check deadline "${DEADLINE}"\n',
        "",
        NOT_SUCCESS
        + ", for 3 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=success, guards=success, deadline=failure",
    ),
    "the required job reads the wrong result for the deadline job": (
        "DEADLINE: ${{ needs.deadline.result }}",
        "DEADLINE: ${{ needs.hooks.result }}",
        "required: its env entry DEADLINE is '${{ needs.hooks.result }}', not '${{ needs.deadline.result }}'",
    ),
    "the deadline job never run": (
        "    name: A project wait passes its deadline and fails visibly\n",
        "    name: A project wait passes its deadline and fails visibly\n    if: false\n",
        "deadline: it runs behind the condition False",
    ),
    "the deadline job run only on push": (
        "    name: A project wait passes its deadline and fails visibly\n",
        "    name: A project wait passes its deadline and fails visibly\n    if: github.event_name == 'push'\n",
        "deadline: it runs behind the condition \"github.event_name == 'push'\"",
    ),
    "the deadline job waits for the hooks": (
        "    name: A project wait passes its deadline and fails visibly\n",
        "    name: A project wait passes its deadline and fails visibly\n    needs: hooks\n",
        "deadline: it waits for hooks",
    ),
    "the deadline case never run": (DEADLINE_RUN, "", "deadline: it runs ['make setup']"),
    "the deadline case run as a dry run": (DEADLINE_RUN, "      - run: make test-deadline -n\n", "deadline: it runs ['make setup', 'make test-deadline -n']"),
    "the deadline case's failure tolerated": (DEADLINE_RUN, DEADLINE_RUN + "        continue-on-error: true\n", "deadline: continue-on-error is set on step 4"),
    "the deadline job's timeout changed": (
        "    name: A project wait passes its deadline and fails visibly\n    runs-on: ubuntu-24.04\n    timeout-minutes: 5",
        "    name: A project wait passes its deadline and fails visibly\n    runs-on: ubuntu-24.04\n    timeout-minutes: 30",
        "deadline: its timeout is 30 minutes, not 5",
    ),
    "the deadline record not kept": (DEADLINE_UPLOAD, "", "deadline: it has 5 steps, not 6"),
    "the deadline record check removed": (DEADLINE_CHECK, DEADLINE_RUN, ("deadline: it has 5 steps, not 6", "deadline: it has 0 steps named")),
    "the guard record check removed": (GUARD_CHECK, TAIL_YAML, ("guards: it has 7 steps, not 8", "guards: it has 0 steps named")),
    "the deadline record check after the upload": (
        DEADLINE_CHECK + DEADLINE_UPLOAD,
        DEADLINE_RUN + DEADLINE_UPLOAD + CHECK_YAML,
        ("deadline: the record check is not directly before the one upload step", "deadline: step 5, 6 differs from the pinned one"),
    ),
    "the guard record check before the log tail": (
        GUARD_CHECK,
        CHECK_YAML + TAIL_YAML,
        ("guards: step 5, 6 differs from the pinned one",),
    ),
    "the deadline record check only after a success": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace("${{ !cancelled() }}", "success()"),
        "deadline: step 5 runs behind the condition 'success()'",
    ),
    "the deadline record check without a condition": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace("        if: ${{ !cancelled() }}\n", ""),
        "deadline: step 5 differs from the pinned one",
    ),
    "the guard record check only after a failure": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace("${{ !cancelled() }}", "failure()"),
        "guards: step 6 differs from the pinned one",
    ),
    "the guard record check's failure tolerated": (
        GUARD_CHECK,
        GUARD_CHECK.replace("        run: |\n", "        continue-on-error: true\n        run: |\n", 1),
        "guards: continue-on-error is set on step 6",
    ),
    "the deadline record check in another folder": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace("        run: |\n", "        working-directory: docs\n        run: |\n", 1),
        "deadline: step 5 sets the working directory 'docs'",
    ),
    "the deadline record check only looks for the record": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace("for file in record.json retention.json; do", "for file in record.json; do"),
        "deadline: its record check passes with the record alone",
    ),
    "the deadline record check only looks for the label": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace("for file in record.json retention.json; do", "for file in retention.json; do"),
        "deadline: its record check passes with the label alone",
    ),
    "the guard record check lets a missing file pass": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace("      incomplete=1\n", "      incomplete=0\n"),
        "guards: its record check passes with the record alone",
    ),
    "the guard record check passes with no run folder": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace('"${#folders[@]}" -eq 0', '"${#folders[@]}" -eq 99'),
        "guards: its record check passes with no run folder",
    ),
    "the guard record check ends with a success": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace('exit "${incomplete}"', "exit 0"),
        "guards: its record check passes with the record alone",
    ),
    "the guard record check gives one folder a free pass": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace('for folder in "${folders[@]}"; do', 'for folder in "${folders[0]}"; do'),
        "guards: its record check passes with a second run folder without a label",
    ),
    "the deadline record check matches the two files as the upload does, each in any folder": (
        DEADLINE_CHECK,
        DEADLINE_RUN + LOOSE_CHECK_YAML,
        ("deadline: its record check passes with the record and the label in two run folders", "deadline: step 5 differs from the pinned one"),
    ),
    "the guard record check accepts a link as the file": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace(' || [ -L "${folder}${file}" ]', ""),
        ("guards: its record check passes with the label a link to the label of another run", "guards: its record check passes with the record a link"),
    ),
    "the guard record check refuses a link only as the label": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace('[ -L "${folder}${file}" ]', '[ -L "${folder}retention.json" ]'),
        "guards: its record check passes with the record a link to the record of another run",
    ),
    "the deadline record check refuses a link only as the record": (
        DEADLINE_CHECK,
        DEADLINE_RUN + CHECK_YAML.replace('[ -L "${folder}${file}" ]', '[ -L "${folder}record.json" ]'),
        "deadline: its record check passes with the label a link to the label of another run",
    ),
    "the guard record check accepts a directory as the label": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace('[ ! -f "${folder}${file}" ]', '[ ! -e "${folder}${file}" ]'),
        "guards: its record check passes with a directory where the label belongs",
    ),
    "the guard record check looks in the runs folder itself": (
        GUARD_CHECK,
        TAIL_YAML + CHECK_YAML.replace("folders=(runs/*/)", "folders=(runs/)"),
        "guards: its record check fails with the record and its label in one run folder",
    ),
    "the deadline record kept only after a success": (
        DEADLINE_UPLOAD,
        DEADLINE_UPLOAD.replace("${{ !cancelled() }}", "success()"),
        "deadline: step 6 runs behind the condition 'success()'",
    ),
    "the deadline record upload without a file is no error": (
        DEADLINE_UPLOAD,
        DEADLINE_UPLOAD.replace("if-no-files-found: error", "if-no-files-found: warn"),
        "deadline: step 6 differs from the pinned one",
    ),
    "the deadline record kept without its retention label": (
        DEADLINE_UPLOAD,
        DEADLINE_UPLOAD.replace("            runs/*/retention.json\n", ""),
        "deadline: step 6 differs from the pinned one",
    ),
    "the deadline record kept by another version of the upload action": (
        DEADLINE_UPLOAD,
        DEADLINE_UPLOAD.replace("cf430e030ddbb5b0abf93d22962f4752f3646cd9", "0000000000000000000000000000000000000000"),
        "deadline: step 6 differs from the pinned one",
    ),
    "the guard record not kept": (GUARD_UPLOAD, "", "guards: it has 7 steps, not 8"),
    "the guard record kept only after a success": (
        GUARD_UPLOAD,
        GUARD_UPLOAD.replace("${{ !cancelled() }}", "success()"),
        "guards: step 7 runs behind the condition 'success()'",
    ),
    "the guard record upload without a file is no error": (
        GUARD_UPLOAD,
        GUARD_UPLOAD.replace("if-no-files-found: error", "if-no-files-found: ignore"),
        "guards: step 7 differs from the pinned one",
    ),
    "the guard record of every shard kept under one name": (
        "name: guard-record-shard-${{ matrix.shard }}",
        "name: guard-record",
        "guards: step 7 differs from the pinned one",
    ),
    "the required job skipped after a failure": ("    if: always()\n", "", "required: its condition is None, not always()"),
    "the required job run only after success": ("    if: always()\n", "    if: success()\n", "required: its condition is 'success()', not always()"),
    "the required job's failure flag cleared by a here-document with an indented end": (
        "        run: |\n          failed=0\n",
        "        run: |\n          cat <<EOF\n          skip\n            EOF\n          failed=0\n",
        NOT_SUCCESS,
    ),
    "the required job's failure flag reset before the exit": ('          exit "${failed}"\n', '          failed=0\n          exit "${failed}"\n', NOT_SUCCESS),
    "the required job's failure flag toggled to start set": (
        "          failed=0\n          check() {",
        "          failed=1\n          check() {",
        "required: its script fails when every job succeeded",
    ),
    "the required job passes a failure": ("          failed=1\n", "", NOT_SUCCESS + ", for 1023 of 1041"),
    "the required job passes a skipped job": (
        ACCEPTS,
        'if [ "$2" != success ] && [ "$2" != skipped ]; then',
        NOT_SUCCESS
        + ", for 31 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=success, guards=success, deadline=skipped",
    ),
    "the required job passes a cancelled job": (
        ACCEPTS,
        'if [ "$2" != success ] && [ "$2" != cancelled ]; then',
        NOT_SUCCESS
        + ", for 31 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=success, guards=success, deadline=cancelled",
    ),
    "the required job does not check the guards": (
        '          check guards "${GUARDS}"\n',
        "",
        NOT_SUCCESS
        + ", for 3 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=success, guards=failure, deadline=success",
    ),
    "the required job reads the wrong result": (
        "INFRASTRUCTURE: ${{ needs.infrastructure.result }}",
        "INFRASTRUCTURE: ${{ needs.hooks.result }}",
        "for example hooks=success, infrastructure=failure, slow=success, guards=success",
    ),
    "pull requests into main only": ("  pull_request:\n", "  pull_request:\n    branches: [main]\n", "the workflow starts on pull_request with"),
    "no pull request run": ("  pull_request:\n", "", "the workflow does not start on pull_request"),
    "pushes of main only": ('    branches: ["**"]\n', "    branches: [main]\n", "the workflow starts on push with"),
    "pushes of every branch but main": ('    branches: ["**"]\n', "    branches-ignore: [main]\n", "the workflow starts on push with"),
    "pushes of one branch pattern": ('    branches: ["**"]\n', '    branches: ["feat/**"]\n', "the workflow starts on push with"),
    "no push run": ('  push:\n    branches: ["**"]\n', "", "the workflow does not start on push"),
    "no scheduled run": ("  schedule:\n" + CRON_LINE, "", "the workflow's schedule is None"),
    "a daily schedule": (CRON_LINE, '    - cron: "23 6 * * *"\n', "is not weekly"),
    "a monthly schedule": (CRON_LINE, '    - cron: "23 6 1 * *"\n', "is not weekly"),
    "two schedules": (CRON_LINE, CRON_LINE + '    - cron: "23 7 * * 2"\n', "not one cron entry"),
    "no manual run": ("  workflow_dispatch:\n", "", "the workflow does not start on workflow_dispatch"),
    "an unknown trigger": ("  workflow_dispatch:\n", "  workflow_dispatch:\n  release:\n", "starts on release, which the contract does not know"),
    "the pending runs on main share a group": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}-${{ github.ref }}\n",
        "concurrency: a newer run of the push that a squash merge makes on main replaces its pending run",
    ),
    "every run in one group": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}\n",
        (
            "concurrency: a newer run of a scheduled run replaces its pending run",
            "concurrency: a newer run of a manual run on main replaces its pending run",
            "concurrency: a push of another work branch shares the group of a push of a work branch",
        ),
    ),
    "a work-branch push and a main push in one group": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}-${{ github.event_name }}\n",
        ("concurrency: a push of another work branch shares the group of", "replaces its pending run"),
    ),
    "a group of its own for every run, so nothing is superseded": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}-${{ github.run_id }}\n",
        "concurrency: a newer run of a push of a work branch does not share its group, so it supersedes nothing",
    ),
    "a group of the commit": (GROUP_LINE, "  group: ${{ github.workflow }}-${{ github.sha }}\n", "the context github.sha is not one that this contract knows"),
    "a group that calls a function": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}-${{ contains(github.ref, 'feat') }}\n",
        "concurrency: it cannot be read",
    ),
    "a push to main cancelled": (
        CANCEL_LINE,
        "  cancel-in-progress: ${{ github.event_name == 'pull_request' || github.event_name == 'push' }}\n",
        "concurrency: the push that a squash merge makes on main would be cancelled by a newer run",
    ),
    "a scheduled run cancelled": (
        CANCEL_LINE,
        "  cancel-in-progress: ${{ github.event_name != 'workflow_dispatch' }}\n",
        "concurrency: a scheduled run would be cancelled by a newer run",
    ),
    "a manual run cancelled": (
        CANCEL_LINE,
        "  cancel-in-progress: ${{ github.event_name != 'schedule' }}\n",
        "concurrency: a manual run on main would be cancelled by a newer run",
    ),
    "every run cancels the run before it": (CANCEL_LINE, "  cancel-in-progress: true\n", "concurrency: a scheduled run would be cancelled by a newer run"),
    "no run cancelled": (
        CANCEL_LINE,
        "  cancel-in-progress: false\n",
        ("concurrency: a push of a work branch is never superseded", "concurrency: a pull request is never superseded"),
    ),
    "only pull requests superseded": (
        CANCEL_LINE,
        "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}\n",
        "concurrency: a push of a work branch is never superseded",
    ),
    "the concurrency emptied": (GROUP_LINE + CANCEL_LINE, "", "the workflow's concurrency is None"),
    "the concurrency removed": ("concurrency:\n" + GROUP_LINE + CANCEL_LINE, "", "the workflow has the keys"),
    "the trigger spelled true (the reviewer's first plant)": ("\non:\n", "\ntrue:\n", "has no key spelled on"),
    "the trigger spelled True": ("\non:\n", "\nTrue:\n", "has no key spelled on"),
    "the trigger spelled yes": ("\non:\n", "\nyes:\n", "has no key spelled on"),
    "a Boolean key before the trigger (the reviewer's plant)": ("\non:\n", "\ntrue: false\non:\n", "top-level keys are spelled"),
    "an unknown top-level key": ("\non:\n", "\nenv_extra: {}\non:\n", "top-level keys are spelled"),
    "the trigger spelled ON": ("\non:\n", "\nON:\n", "has no key spelled on"),
    "the trigger spelled On, quoted": ("\non:\n", '\n"On":\n', "has no key spelled on"),
    "the trigger missing": ("\n" + ON_BLOCK, "", "has no key spelled on"),
    "fail-fast given by a merge key (the reviewer's second plant)": (
        "      fail-fast: false\n",
        "      <<: &guard_defaults {fail-fast: false}\n",
        ("it uses a merge key", "it uses an anchor"),
    ),
    "a merge key alone": ("      fail-fast: false\n", "      <<: {fail-fast: false}\n", "it uses a merge key"),
    "an anchor on a step": ("      - run: make check\n", "      - &anchored\n        run: make check\n", "it uses an anchor"),
    "an alias in needs": (NEEDS_LINE, "needs: [hooks, &second infrastructure, *second]", ("it uses an anchor", "it uses an alias")),
    "an alias of a timeout": (
        ("    timeout-minutes: 10", "    timeout-minutes: 10"),
        ("    timeout-minutes: &limit 10", "    timeout-minutes: *limit"),
        "it uses an alias",
    ),
    "an explicit tag on a value": ("name: ci\n", "name: !!str ci\n", "it uses the tag"),
    "an application tag on a value": ("name: ci\n", "name: !ci ci\n", "it uses the tag"),
    "a second YAML document": (
        "          GH_TOKEN: ${{ github.token }}\n",
        "          GH_TOKEN: ${{ github.token }}\n---\nname: other\n",
        "holds 2 YAML documents",
    ),
    "a guard leg that sets shard 1 for every shard": (
        "shard_${SHARD}=success",
        "shard_1=success",
        ("guards: its last step writes 'shard_1=success\\n' for shard 2", "guards: it runs"),
    ),
    "a guard leg that sets something other than success": ("shard_${SHARD}=success", "shard_${SHARD}=skipped", "guards: its last step writes 'shard_1=skipped"),
    "a guard leg step that runs after a failure": (
        "        name: Set this shard's own success output\n",
        "        name: Set this shard's own success output\n        if: always()\n",
        "guards: step 8 runs behind the condition 'always()'",
    ),
    "a guard leg step without its id": ("      - id: leg\n        name:", "      - name:", "guards: its last step is not the one with the id leg"),
    "the guard leg step removed": (LEG_YAML, "", ("guards: it runs", "guards: its last step is not the one with the id leg")),
    "a guard output missing": ("      shard_3: ${{ steps.leg.outputs.shard_3 }}\n", "", "guards: its outputs are"),
    "a guard output that reads another shard": (
        "shard_3: ${{ steps.leg.outputs.shard_3 }}",
        "shard_3: ${{ steps.leg.outputs.shard_2 }}",
        "guards: its outputs are",
    ),
    "the required job does not check the output of guard leg 3": (
        '          check_leg 3 "${GUARD_3}"\n',
        "",
        NOT_SUCCESS + ", for 1 of 1041 combinations of results, for example hooks=success, infrastructure=success, slow=success, guards=success, "
        "deadline=success, guards shards without their output: [3]",
    ),
    "the required job reads another shard's output for guard leg 3": (
        "GUARD_3: ${{ needs.guards.outputs.shard_3 }}",
        "GUARD_3: ${{ needs.guards.outputs.shard_2 }}",
        "required: its env entry GUARD_3 is '${{ needs.guards.outputs.shard_2 }}', not '${{ needs.guards.outputs.shard_3 }}'",
    ),
    "the required job gives guard leg 8 a fixed success": (
        'check_leg 8 "${GUARD_8}"',
        "check_leg 8 success",
        (NOT_SUCCESS + ", for 1 of 1041", "required: its script differs from the approved one at line 26"),
    ),
    "the required job's leg check never fails": (LEG_FUNCTION_END, LEG_FUNCTION_END.replace("failed=1", ":"), NOT_SUCCESS + ", for 17 of 1041"),
    "the headroom job in the required job's needs": (NEEDS_LINE, "needs: [hooks, infrastructure, slow, guards, deadline, headroom]", "required: it waits for"),
    "the headroom job allowed to write to the actions": ("      actions: read\n", "      actions: write\n", "headroom: its permissions is"),
    "the headroom job allowed to write to the contents": (
        "      actions: read\n      contents: read\n",
        "      actions: read\n      contents: write\n",
        "headroom: its permissions is",
    ),
    "the headroom job without a permissions key": ("    permissions:\n      actions: read\n      contents: read\n", "", "headroom: its permissions is None"),
    "the headroom job skipped after a failure": (HEADROOM_TOP, HEADROOM_TOP.replace("    if: always()\n", ""), "headroom: its if is None, not 'always()'"),
    "the headroom job waiting for fewer jobs": (
        HEADROOM_TOP,
        HEADROOM_TOP.replace("[hooks, infrastructure, slow, guards, deadline, required]", "[hooks, infrastructure, slow, guards, deadline]"),
        "headroom: its needs is ['hooks', 'infrastructure', 'slow', 'guards', 'deadline']",
    ),
    "the headroom job's timeout changed": (HEADROOM_TOP, HEADROOM_TOP.replace("minutes: 10", "minutes: 6"), "headroom: its timeout-minutes is 6, not 10"),
    "the headroom job's failure tolerated": (HEADROOM_TOP, HEADROOM_TOP + "\n    continue-on-error: true", "headroom: it has the key 'continue-on-error'"),
    "the headroom check's failure tolerated": (
        HEADROOM_RUN,
        HEADROOM_RUN + "        continue-on-error: true\n",
        "headroom: continue-on-error is set on step 4",
    ),
    "the headroom check run as a dry run": (HEADROOM_RUN, "      - run: make headroom -n\n", "headroom: its steps are not the pinned ones"),
    "the headroom job without its token": ("        env:\n          GH_TOKEN: ${{ github.token }}\n", "", "headroom: its steps are not the pinned ones"),
    "the headroom job renamed to another key": ("  headroom:\n", "  headroom_job:\n", "the workflow's jobs are"),
}
# Syntax that GitHub reads: the trigger spelled on, in either quote style. The parsed workflow is the same as with the plain key.
QUOTED_TRIGGERS = ("'on':", '"on":')


class WorkflowContractTest(unittest.TestCase):
    """The workflow's parsed keys and values are the contract: every job group runs on every run, and one job requires them all."""

    def test_python_is_one_patch_release_and_the_local_one(self) -> None:
        self.assertEqual(platform.python_version(), PYTHON, "local checks run on the Python that the hosted jobs install")

    def test_the_workflow_keeps_the_contract(self) -> None:
        self.assertEqual(contract_problems(WORKFLOW_TEXT), [])

    def test_every_timeout_is_twice_its_measured_duration_and_the_duration_is_recorded_beside_it(self) -> None:
        self.assertEqual(duration_problems(WORKFLOW_TEXT), [])

    def test_a_timeout_under_twice_its_duration_or_without_one_is_named(self) -> None:
        """The plants of the duration rule: the guards limit cut below twice the hosted 11 minutes, a duration removed and a job without a limit."""
        guards_line = "    timeout-minutes: 25 # measured 11 min"
        self.assertEqual(WORKFLOW_TEXT.count(guards_line), 1, "the guards job's timeout line, with its hosted duration")
        planted = {
            "the guards limit below twice its duration": (
                guards_line,
                guards_line.replace("minutes: 25", "minutes: 21"),
                "guards: its timeout of 21 minutes is less than 2 times",
            ),
            "the guards duration removed": (
                guards_line,
                "    timeout-minutes: 25 # (hosted)\n    # was:",
                "guards: its timeout of 25 minutes has no 'measured N min' duration",
            ),
            "the guards duration raised past half the limit": (
                guards_line,
                guards_line.replace("measured 11", "measured 13"),
                "guards: its timeout of 25 minutes is less than 2 times",
            ),
        }
        for label, (old, new, diagnostic) in planted.items():
            with self.subTest(label=label):
                self.assertTrue(any(diagnostic in problem for problem in duration_problems(WORKFLOW_TEXT.replace(old, new, 1))))
        with self.subTest(label="a job without a timeout line"):
            without = WORKFLOW_TEXT.replace(guards_line, "    # no timeout", 1)
            self.assertIn("guards: it has no timeout line", duration_problems(without))

    def test_the_workflow_has_no_plan_and_no_reuse(self) -> None:
        self.assertNotRegex(WORKFLOW_TEXT.lower(), r"\bplan\b|reuse|ci_proof|ci-proof")

    def test_the_concurrency_expressions_are_read_as_github_reads_them(self) -> None:
        """The controls of the concurrency plants: the reader's results for ``&&``, ``||``, ``!``, ``!=`` and a parenthesis."""
        context = {"event_name": "push", "ref": "refs/heads/main", "run_id": "9"}
        for text, wanted in (
            ("github.event_name == 'push' && github.ref || github.run_id", "refs/heads/main"),
            ("github.event_name == 'pull_request' && github.ref || github.run_id", "9"),
            ("!(github.event_name == 'push')", False),
            ("github.ref != 'refs/heads/main' || github.event_name == 'push'", True),
            ("github.ref != 'refs/heads/main' && github.event_name == 'push'", False),
            ("github.event_name == 'PUSH'", True),
        ):
            with self.subTest(text=text):
                self.assertEqual(Expression(text, context).value(), wanted)
        for text in ("github.sha", "format('{0}', github.ref)", "(github.ref", "github.ref ==", "github.ref github.ref"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                Expression(text, context).value()

    def test_the_headroom_job_is_not_a_gating_job(self) -> None:
        jobs = load_workflow(WORKFLOW_TEXT)["jobs"]
        self.assertNotIn(HEADROOM, jobs["required"]["needs"])
        self.assertEqual(jobs[HEADROOM]["permissions"], {"actions": "read", "contents": "read"})

    def test_the_trigger_in_either_quote_style_is_the_same_workflow(self) -> None:
        """The control of the trigger plants: GitHub reads a quoted ``on`` key, and so does the contract."""
        for quoted in QUOTED_TRIGGERS:
            with self.subTest(quoted=quoted):
                self.assertEqual(contract_problems(WORKFLOW_TEXT.replace("\non:\n", f"\n{quoted}\n", 1)), [])

    def test_a_quoted_double_angle_text_is_not_a_merge_key(self) -> None:
        self.assertEqual([problem for problem in source_problems(WORKFLOW_TEXT.replace("name: ci\n", 'name: "<<"\n', 1))], [])

    def test_the_workflow_uses_no_anchor_alias_merge_key_tag_or_second_document(self) -> None:
        self.assertEqual(source_problems(WORKFLOW_TEXT), [])

    def test_the_script_probe_runs_the_shell_with_the_options_that_github_gives_bash(self) -> None:
        """A script that fails only under ``-e`` or under ``pipefail`` fails here too: with another shell its failure would look like a pass."""
        for script in ("false\nexit 0\n", "false | true\nexit 0\n"):
            with self.subTest(script=script):
                problems = script_problems(script, ())
                self.assertEqual(len(problems), 1)
                self.assertIn("fails when every job succeeded", problems[0])

    def test_the_record_check_runs_in_every_run_folder_tree_as_github_runs_it(self) -> None:
        """The control of the record check's plants: the approved script passes with the record and its label and fails in every other tree."""
        self.assertEqual(record_check_script_problems(RECORD_CHECK_SCRIPT), ())


class RulesetRequiredFieldsTest(unittest.TestCase):
    """The ruleset file has every field that GitHub's create-ruleset API requires for the rule types that it uses; a missing one is refused at
    creation, so the owner would find out only when applying the file."""

    def setUp(self) -> None:
        self.ruleset = json.loads((ROOT / ".github/rulesets/main.json").read_text())

    def test_the_ruleset_has_every_required_field(self) -> None:
        self.assertEqual(ruleset_problems(self.ruleset), [])

    def test_a_ruleset_without_a_required_field_fails_with_its_own_diagnostic(self) -> None:
        def without(path: tuple[str | int, ...]) -> dict[str, Any]:
            planted: dict[str, Any] = copy.deepcopy(self.ruleset)
            holder: Any = planted
            for step in path[:-1]:
                holder = holder[step]
            del holder[path[-1]]
            return planted

        pull_request = ("rules", 0, "parameters")
        checks = ("rules", 1, "parameters")
        planted = {
            "no name": (("name",), "ruleset: it lacks name"),
            "no enforcement": (("enforcement",), "ruleset: it lacks enforcement"),
            "no type on the first rule": (("rules", 0, "type"), "rule 1: it lacks type"),
            "no type on the second rule": (("rules", 1, "type"), "rule 2: it lacks type"),
            "no type on the third rule": (("rules", 2, "type"), "rule 3: it lacks type"),
            "no pull request parameters": (pull_request, "rule pull_request: it lacks parameters"),
            "no dismissal setting": ((*pull_request, "dismiss_stale_reviews_on_push"), "rule pull_request: its parameters lack dismiss_stale_reviews_on_push"),
            "no code owner setting": ((*pull_request, "require_code_owner_review"), "rule pull_request: its parameters lack require_code_owner_review"),
            "no last push setting": ((*pull_request, "require_last_push_approval"), "rule pull_request: its parameters lack require_last_push_approval"),
            "no approval count": ((*pull_request, "required_approving_review_count"), "rule pull_request: its parameters lack required_approving_review_count"),
            "no thread setting": (
                (*pull_request, "required_review_thread_resolution"),
                "rule pull_request: its parameters lack required_review_thread_resolution",
            ),
            "no status check parameters": (checks, "rule required_status_checks: it lacks parameters"),
            "no strict policy": (
                (*checks, "strict_required_status_checks_policy"),
                "rule required_status_checks: its parameters lack strict_required_status_checks_policy",
            ),
            "no check list": ((*checks, "required_status_checks"), "rule required_status_checks: its parameters lack required_status_checks"),
            "no check context": ((*checks, "required_status_checks", 0, "context"), "rule required_status_checks: check 1 lacks context"),
        }
        for label, (path, diagnostic) in planted.items():
            with self.subTest(label=label):
                problems = ruleset_problems(without(path))
                self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")

    def test_a_required_setting_that_is_not_true_or_false_is_named(self) -> None:
        ruleset = json.loads((ROOT / ".github/rulesets/main.json").read_text())
        for field in ("strict_required_status_checks_policy",):
            for value in (None, "true", 1):
                with self.subTest(field=field, value=value):
                    planted = copy.deepcopy(ruleset)
                    planted["rules"][1]["parameters"][field] = value
                    self.assertIn(f"rule required_status_checks: its {field} is not true or false", ruleset_problems(planted))
        self.assertEqual([], ruleset_problems(ruleset))


class RulesetTest(unittest.TestCase):
    """The ruleset file that the owner applies to main requires a pull request, squash merges and the one status check that the required job gives."""

    def setUp(self) -> None:
        self.ruleset = json.loads((ROOT / ".github/rulesets/main.json").read_text())
        self.rules = {rule["type"]: rule.get("parameters", {}) for rule in self.ruleset["rules"]}

    def test_main_requires_a_pull_request_and_the_one_status_check(self) -> None:
        self.assertEqual((self.ruleset["target"], self.ruleset["enforcement"]), ("branch", "active"))
        self.assertEqual(self.ruleset["conditions"]["ref_name"], {"include": ["~DEFAULT_BRANCH"], "exclude": []})
        self.assertEqual(sorted(self.rules), ["non_fast_forward", "pull_request", "required_status_checks"])
        checks = self.rules["required_status_checks"]["required_status_checks"]
        self.assertEqual([check["context"] for check in checks], [REQUIRED_NAME], "the one check is the required job's name")
        self.assertEqual([check.get("integration_id") for check in checks], [15368], "only the GitHub Actions app can give the check")

    def test_a_pull_request_must_be_up_to_date_with_main_before_it_merges(self) -> None:
        """The recorded exception to the check-first design: the branch is up to date, so the squash commit on main is the commit that passed."""
        self.assertIs(self.rules["required_status_checks"]["strict_required_status_checks_policy"], True)

    def test_pull_requests_merge_only_by_squash(self) -> None:
        self.assertEqual(self.rules["pull_request"]["allowed_merge_methods"], ["squash"])

    def test_the_ruleset_names_no_bypass_and_no_review_that_one_owner_cannot_give(self) -> None:
        self.assertEqual(self.ruleset["bypass_actors"], [], "the owner adds a bypass when a force-push to main is needed")
        self.assertEqual(self.rules["pull_request"]["required_approving_review_count"], 0)

    def test_the_required_check_is_the_name_that_the_workflow_gives(self) -> None:
        jobs = load_workflow(WORKFLOW_TEXT)["jobs"]
        self.assertEqual(jobs["required"]["name"], self.rules["required_status_checks"]["required_status_checks"][0]["context"])


if __name__ == "__main__":
    unittest.main()
