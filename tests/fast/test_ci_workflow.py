"""The CI workflow contract: every pull request and every push to main runs every job group, and one job requires them all.

Failure modes covered here, each with a planted case and a control:

- A group job is skipped: an ``if`` on a job or on one of its steps, or ``continue-on-error`` that lets a failed check pass.
- A guard shard is missing, runs twice, runs another shard's cases or is excluded through ``matrix.exclude``.
- A check command changes (a flag that turns the check into a dry run, a dropped or reordered command, another Python or tool version).
- The required job leaves a group out of ``needs``, does not always run, has its step disabled or passes when a needed job failed, was
  cancelled or was skipped. Its script runs here with the result of each needed job.
"""

from __future__ import annotations

import functools
import json
import os
import platform
import re
import unittest
from collections import Counter
from typing import Any

from scripts.process import run_command
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS

WORKFLOW_TEXT = (ROOT / ".github/workflows/ci.yml").read_text()
RUNNER = "ubuntu-24.04"
PYTHON = "3.14.8"
SHARDS = 8
REQUIRED_NAME = "Every job of this run passed"
GROUPS = ("hooks", "infrastructure", "guards")
EXPRESSION = re.compile(r"\$\{\{ ([^}]+?) \}\}")
BAD_RESULTS = ("failure", "cancelled", "skipped")
SHARD_COMMAND = f'make test-guards SHARD="${{SHARD}}/{SHARDS}"'
COMMIT_RANGE_SCRIPT = "\n".join(
    [
        'if git cat-file -e "${BASE}^{commit}" 2>/dev/null; then',
        '.venv/bin/python -m scripts.quality.repo_checks commit-range "${BASE}..${HEAD}"',
        "else",
        '.venv/bin/python -m scripts.quality.repo_checks commit-range "${HEAD}"',
        "fi",
    ]
)
CHECKOUT = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
SETUP_PYTHON = "actions/setup-python@6d4fd22b20465ccc0f7354476588e8df7c9ca0e0"
SETUP_TERRAFORM = "hashicorp/setup-terraform@dfe3c3f87815947d99a8997f908cb6525fc44e9e"
PYTHON_STEP = {"uses": SETUP_PYTHON, "with": {"python-version": PYTHON, "cache": "pip", "cache-dependency-path": "requirements.txt"}}
CHECKOUT_STEP = {"uses": CHECKOUT, "with": {"persist-credentials": "false"}}
TAIL_STEP = {"name": "Show the end of the run's log after a failure", "if": "failure()", "run": "tail -n 150 runs/*/log.txt"}
# The jobs and steps of the workflow on main, with only the Python patch release added: a change to any of them is a change to what CI checks.
PINNED_JOBS: dict[str, dict[str, Any]] = {
    "hooks": {
        "name": "Hooks on every file and the commit messages",
        "timeout-minutes": "10",
        "steps": [
            {"uses": CHECKOUT, "with": {"fetch-depth": "0", "persist-credentials": "false"}},
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
        "timeout-minutes": "10",
        "steps": [
            CHECKOUT_STEP,
            PYTHON_STEP,
            {"uses": SETUP_TERRAFORM, "with": {"terraform_version": "1.16.4", "terraform_wrapper": "false"}},
            {"run": "make setup"},
            {"run": "make check-terraform"},
            {"run": "make check-ansible"},
        ],
    },
    "guards": {
        "name": "Planted faults through the real commit path, shard ${{ matrix.shard }} of 8",
        "timeout-minutes": "15",
        "steps": [CHECKOUT_STEP, PYTHON_STEP, {"run": "make setup"}, {"run": SHARD_COMMAND, "env": {"SHARD": "${{ matrix.shard }}"}}, TAIL_STEP],
    },
}
JOB_KEYS = {"name", "runs-on", "timeout-minutes", "steps"}


def parse_workflow(text: str) -> dict[str, Any]:
    """The workflow as nested mappings and lists, read from the subset of YAML that it uses.

    Block mappings, block lists of mappings, ``|`` block scalars, flow lists of plain words and double-quoted words. A comment after `` # ``
    on a value line is dropped. A line the subset does not know raises ``AssertionError``.
    """
    lines = text.splitlines()
    index = 0

    def skip() -> None:
        nonlocal index
        while index < len(lines) and (not lines[index].strip() or lines[index].lstrip().startswith("#")):
            index += 1

    def indent_of(line: str) -> int:
        return len(line) - len(line.lstrip(" "))

    def scalar(value: str) -> Any:
        value = value.split(" # ")[0].strip()
        if value.startswith("[") and value.endswith("]"):
            return [part.strip() for part in value[1:-1].split(",") if part.strip()]
        if len(value) > 1 and value[0] == value[-1] == '"':
            return value[1:-1]
        return value

    def node(indent: int) -> Any:
        skip()
        if index >= len(lines) or indent_of(lines[index]) != indent:
            raise AssertionError(f"a block that does not start at indent {indent}: line {index + 1}")
        return sequence(indent) if lines[index].lstrip().startswith("- ") else mapping(indent)

    def value_of(value: str, indent: int) -> Any:
        nonlocal index
        if value == "|":
            body = []
            while index < len(lines) and (not lines[index].strip() or indent_of(lines[index]) > indent):
                body.append(lines[index].strip())
                index += 1
            return "\n".join(part for part in body if part)
        if value:
            return scalar(value)
        skip()
        return node(indent_of(lines[index])) if index < len(lines) and indent_of(lines[index]) > indent else None

    def mapping(indent: int) -> dict[str, Any]:
        nonlocal index
        result: dict[str, Any] = {}
        while True:
            skip()
            if index >= len(lines) or indent_of(lines[index]) < indent:
                return result
            line = lines[index]
            if indent_of(line) != indent or line.lstrip().startswith("- "):
                raise AssertionError(f"a line the workflow subset cannot place: {line!r}")
            key, colon, value = line.strip().partition(":")
            if not colon or key in result:
                raise AssertionError(f"not a new key: {line!r}")
            index += 1
            result[key] = value_of(value.strip(), indent)

    def sequence(indent: int) -> list[Any]:
        items: list[Any] = []
        while True:
            skip()
            if index >= len(lines) or indent_of(lines[index]) != indent or not lines[index].lstrip().startswith("- "):
                return items
            lines[index] = " " * (indent + 2) + lines[index].lstrip()[2:]
            items.append(mapping(indent + 2))

    return mapping(0)


def substituted(text: str, values: dict[str, str]) -> str:
    """``text`` with each ``${{ expression }}`` replaced by its value; an expression without a value raises ``KeyError``."""
    return EXPRESSION.sub(lambda found: values[found.group(1)], text)


def step_problems(name: str, steps: list[dict[str, Any]]) -> list[str]:
    """A step that skips or lets a failure pass. Only the guards' log tail may have a condition, and only ``failure()``."""
    problems = []
    for step in steps:
        if "continue-on-error" in step:
            problems.append(f"{name}: continue-on-error is set on a step, so a failed check can pass")
        if "if" in step and not (name == "guards" and step == TAIL_STEP):
            problems.append(f"{name}: a step runs behind the condition {step['if']!r}")
    return problems


def runs_of(steps: list[dict[str, Any]]) -> list[str]:
    return [str(step["run"]) for step in steps if "run" in step]


def job_problems(name: str, job: dict[str, Any]) -> list[str]:
    """A group job runs unconditionally, with no wait on another job, no tolerated failure and exactly its pinned steps."""
    problems = []
    if "if" in job:
        problems.append(f"{name}: it runs behind the condition {job['if']!r}")
    if "needs" in job:
        problems.append(f"{name}: it waits for {job['needs']}, and a failed or skipped job before it would skip it")
    if "continue-on-error" in job:
        problems.append(f"{name}: continue-on-error is set on the job, so a failed check can pass")
    for key in sorted(set(job) - JOB_KEYS - {"if", "needs", "continue-on-error"} - ({"strategy"} if name == "guards" else set())):
        problems.append(f"{name}: it has the key {key!r}, which the contract does not know")
    if job.get("runs-on") != RUNNER:
        problems.append(f"{name}: it runs on {job.get('runs-on')}, not {RUNNER}")
    steps = job.get("steps") or []
    problems += step_problems(name, steps)
    pinned = PINNED_JOBS[name]
    if runs_of(steps) != runs_of(pinned["steps"]):
        problems.append(f"{name}: it runs {runs_of(steps)}, not {runs_of(pinned['steps'])}")
    if not problems and (steps != pinned["steps"] or job.get("name") != pinned["name"] or job.get("timeout-minutes") != pinned["timeout-minutes"]):
        problems.append(f"{name}: its name, timeout or steps differ from the pinned ones")
    return problems


def matrix_problems(job: dict[str, Any]) -> list[str]:
    """The effective guard matrix: every shard once, none excluded, each entry running its own shard, and one failed shard cancels no other."""
    strategy = job.get("strategy") or {}
    matrix = strategy.get("matrix") or {}
    problems = []
    if strategy.get("fail-fast") != "false":
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
        f"{str((suite[0].get('env') or {}).get('SHARD', '')).replace('${{ matrix.shard }}', value)}/{SHARDS}" for value in shards if value not in excluded
    )
    expected = [f"{shard}/{SHARDS}" for shard in range(1, SHARDS + 1)]
    if [shard for shard in expected if shard not in counts]:
        problems.append(f"guards: the shards {[shard for shard in expected if shard not in counts]} never run")
    if [shard for shard, count in counts.items() if count > 1]:
        problems.append(f"guards: the shards {[shard for shard, count in counts.items() if count > 1]} run more than once")
    if [shard for shard in counts if shard not in expected]:
        problems.append(f"guards: the matrix runs shards {[shard for shard in counts if shard not in expected]} that do not exist")
    return problems


def aggregate_scenarios() -> list[tuple[dict[str, str], bool]]:
    """The result of each needed job and whether the required job must pass: only when every one succeeded."""
    every = {name: "success" for name in GROUPS}
    scenarios = [(every, True), (dict.fromkeys(GROUPS, "skipped"), False)]
    scenarios += [(every | {name: bad}, False) for name in GROUPS for bad in BAD_RESULTS]
    return scenarios


@functools.cache
def script_problems(script: str, variables: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """What is wrong with the required job's ``script`` run for each scenario, with the step's environment ``variables`` (each value may name
    an expression of the workflow). Cached: the variants of the contract test share most scripts."""
    problems = []
    for results, passes in aggregate_scenarios():
        values = {f"needs.{name}.result": result for name, result in results.items()}
        try:
            environment = {name: substituted(text, values) for name, text in variables}
        except KeyError as error:
            return (f"required: its step reads {error}, which no needed job gives",)
        run = run_command(
            "bash", ["-e", "-c", script], env={"PATH": os.environ["PATH"], "GITHUB_STEP_SUMMARY": os.devnull, **environment}, timeout=TIMEOUT_SECONDS
        )
        if (run.returncode == 0) != passes:
            wrong = next(iter(name for name, result in results.items() if result != "success"), None)
            what = f"the job {wrong} ended as {results[wrong]}" if wrong else "every job succeeded"
            problems.append(f"required: it {'passes' if run.returncode == 0 else 'fails'} when {what}")
    return tuple(problems)


def aggregate_problems(jobs: dict[str, Any]) -> list[str]:
    """The required job waits for every group, always runs, has one enabled step, and its script passes exactly when every group succeeded."""
    required = jobs["required"]
    problems = []
    needs = required.get("needs") if isinstance(required.get("needs"), list) else [required.get("needs")]
    for missing in sorted(set(GROUPS) - set(needs)):
        problems.append(f"required: {missing} is missing from its needs, so the check can pass without it")
    if sorted(needs) != sorted(GROUPS):
        problems.append(f"required: it waits for {needs}, not for exactly {list(GROUPS)}")
    if required.get("if") != "always()":
        problems.append(f"required: its condition is {required.get('if')!r}, not always(), so a failed job skips it and the check stays pending")
    if "continue-on-error" in required:
        problems.append("required: continue-on-error is set on the job")
    if required.get("name") != REQUIRED_NAME or required.get("runs-on") != RUNNER or required.get("timeout-minutes") != "5":
        problems.append(f"required: its name, runner or timeout is not {REQUIRED_NAME!r}, {RUNNER} and 5")
    if set(required) - {"name", "needs", "if", "runs-on", "timeout-minutes", "steps"}:
        problems.append(f"required: it has the keys {sorted(set(required) - {'name', 'needs', 'if', 'runs-on', 'timeout-minutes', 'steps'})}")
    steps = required.get("steps") or []
    if len(steps) != 1 or "run" not in steps[0]:
        return [*problems, "required: it has not exactly one step that runs a script"]
    if "if" in steps[0]:
        problems.append(f"required: its step is behind the condition {steps[0]['if']!r}")
    if "continue-on-error" in steps[0]:
        problems.append("required: continue-on-error is set on its step")
    variables = tuple((name, str(text)) for name, text in (steps[0].get("env") or {}).items())
    return [*problems, *script_problems(steps[0]["run"], variables)]


def contract_problems(text: str) -> list[str]:
    """What the workflow fails to do: run every group job on every pull request and every push to main, each shard once, with the pinned commands,
    and fail the one required job unless every group job succeeded."""
    problems: list[str] = []
    try:
        workflow = parse_workflow(text)
        if set(workflow) != {"name", "on", "permissions", "concurrency", "jobs"}:
            problems.append(f"the workflow has the keys {sorted(workflow)}")
        if workflow.get("on") != {"pull_request": None, "push": {"branches": ["main"]}}:
            problems.append(f"the workflow starts on {workflow.get('on')}, not on every pull request and every push to main")
        if workflow.get("permissions") != {"contents": "read"}:
            problems.append(f"the workflow asks for the permissions {workflow.get('permissions')}")
        if set(workflow["jobs"]) != {*GROUPS, "required"}:
            problems.append(f"the workflow's jobs are {sorted(workflow['jobs'])}, not {sorted({*GROUPS, 'required'})}")
        for name in GROUPS:
            problems += job_problems(name, workflow["jobs"][name])
        problems += matrix_problems(workflow["jobs"]["guards"])
        problems += aggregate_problems(workflow["jobs"])
    except (AssertionError, KeyError, StopIteration, ValueError, TypeError, AttributeError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


class WorkflowContractTest(unittest.TestCase):
    """The workflow's text is the contract: every job group runs on every run, and one job requires them all."""

    def test_python_is_one_patch_release_and_the_local_one(self) -> None:
        self.assertEqual(platform.python_version(), PYTHON, "local checks run on the Python that the hosted jobs install")

    def test_trivy_uses_the_checks_built_into_its_pinned_binary(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        trivy = [line for line in makefile.splitlines() if ".tools/bin/trivy" in line]
        self.assertEqual(len(trivy), 1)
        self.assertIn("--skip-check-update", trivy[0], "without it Trivy downloads the newest checks bundle of a rolling tag")
        self.assertNotIn(".tools/trivy_cache ", trivy[0], "that cache can hold a bundle that an earlier run downloaded")

    def test_the_workflow_keeps_the_contract(self) -> None:
        self.assertEqual(contract_problems(WORKFLOW_TEXT), [])

    def test_the_workflow_has_no_plan_and_no_reuse(self) -> None:
        self.assertNotRegex(WORKFLOW_TEXT.lower(), r"\bplan\b|reuse|ci_proof|ci-proof")

    def test_a_broken_workflow_fails_the_contract_with_its_own_diagnostic(self) -> None:
        terraform = "      - run: make check-terraform\n"
        ansible = "      - run: make check-ansible\n"
        guards = '      - run: make test-guards SHARD="${SHARD}/8"\n'
        required_check = '          check guards "${GUARDS}"\n'
        accepts = 'if [ "$2" != success ]; then'
        planted = {
            "the guards job never run": (
                "    timeout-minutes: 15\n",
                "    if: false\n    timeout-minutes: 15\n",
                "guards: it runs behind the condition 'false'",
            ),
            "the guards job run only on push": (
                "    timeout-minutes: 15\n",
                "    if: github.event_name == 'push'\n    timeout-minutes: 15\n",
                "guards: it runs behind the condition \"github.event_name == 'push'\"",
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
                terraform,
                terraform + "        if: github.event_name == 'push'\n",
                "infrastructure: a step runs behind the condition",
            ),
            "the guard suite never run": (guards, guards + "        if: false\n", "guards: a step runs behind the condition 'false'"),
            "a guard job's failure tolerated": (
                "    timeout-minutes: 15",
                "    continue-on-error: true\n    timeout-minutes: 15",
                "guards: continue-on-error is set on the job",
            ),
            "a hooks step's failure tolerated": (
                "      - run: make check\n",
                "      - run: make check\n        continue-on-error: true\n",
                "hooks: continue-on-error is set on a step",
            ),
            "the Terraform checks' failure tolerated": (
                terraform,
                terraform + "        continue-on-error: true\n",
                "infrastructure: continue-on-error is set on a step",
            ),
            "the required job's failure tolerated": (
                "    name: Every job of this run passed\n",
                "    name: Every job of this run passed\n    continue-on-error: true\n",
                "required: continue-on-error is set on the job",
            ),
            "the required job's step tolerated": (
                "        env:\n          HOOKS:",
                "        continue-on-error: true\n        env:\n          HOOKS:",
                "required: continue-on-error is set on its step",
            ),
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
                terraform,
                "      - run: make check-terraform -n\n",
                "infrastructure: it runs ['make setup', 'make check-terraform -n'",
            ),
            "the Terraform checks removed": (terraform, "", "infrastructure: it runs ['make setup', 'make check-ansible']"),
            "the Ansible check before the Terraform checks": (
                terraform + ansible,
                ansible + terraform,
                "infrastructure: it runs ['make setup', 'make check-ansible', 'make check-terraform']",
            ),
            "the hooks failure ignored": (
                "      - run: make check\n",
                "      - run: make check || true\n",
                "hooks: it runs ['make setup', 'make check || true'",
            ),
            "the commit messages not checked": ("commit-range", "commit-range-skipped", "hooks: it runs"),
            "the guard suite removed": (guards, "", "guards: the suite runs []"),
            "the guard setup removed": (
                "      - run: make setup\n      - run: make test-guards",
                "      - run: make test-guards",
                "guards: it runs ['make test-guards",
            ),
            "another Python in one job": (
                'python-version: "3.14.8"\n          cache',
                'python-version: "3.14"\n          cache',
                "its name, timeout or steps differ from the pinned ones",
            ),
            "another Terraform": ('terraform_version: "1.16.4"', 'terraform_version: "1.16.5"', "infrastructure: its name, timeout or steps differ"),
            "a job on another runner": (
                "runs-on: ubuntu-24.04\n    timeout-minutes: 15",
                "runs-on: ubuntu-latest\n    timeout-minutes: 15",
                "guards: it runs on ubuntu-latest",
            ),
            "a job that asks to write": (
                "    timeout-minutes: 15\n",
                "    timeout-minutes: 15\n    permissions:\n      contents: write\n",
                "guards: it has the key 'permissions'",
            ),
            "a plan job added": ("jobs:\n", "jobs:\n  plan:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: make check\n", "the workflow's jobs are"),
            "the required job without the guards in its needs": (
                "needs: [hooks, infrastructure, guards]",
                "needs: [hooks, infrastructure]",
                "required: guards is missing from its needs",
            ),
            "the required job without the hooks in its needs": (
                "needs: [hooks, infrastructure, guards]",
                "needs: [infrastructure, guards]",
                "required: hooks is missing from its needs",
            ),
            "the required job without the infrastructure job in its needs": (
                "needs: [hooks, infrastructure, guards]",
                "needs: [hooks, guards]",
                "required: infrastructure is missing from its needs",
            ),
            "the required job skipped after a failure": ("    if: always()\n", "", "required: its condition is None, not always()"),
            "the required job run only after success": ("    if: always()\n", "    if: success()\n", "required: its condition is 'success()', not always()"),
            "the required job's step disabled": (
                "        env:\n          HOOKS:",
                "        if: false\n        env:\n          HOOKS:",
                "required: its step is behind the condition 'false'",
            ),
            "the required job passes a failure": ("          failed=1\n", "", "required: it passes when the job hooks ended as failure"),
            "the required job passes a skipped job": (
                accepts,
                'if [ "$2" != success ] && [ "$2" != skipped ]; then',
                "required: it passes when the job hooks ended as skipped",
            ),
            "the required job passes a cancelled job": (
                accepts,
                'if [ "$2" != success ] && [ "$2" != cancelled ]; then',
                "required: it passes when the job hooks ended as cancelled",
            ),
            "the required job does not check the guards": (required_check, "", "required: it passes when the job guards ended as failure"),
            "the required job reads the wrong result": (
                "INFRASTRUCTURE: ${{ needs.infrastructure.result }}",
                "INFRASTRUCTURE: ${{ needs.hooks.result }}",
                "required: it passes when the job infrastructure ended as failure",
            ),
            "pull requests into main only": ("  pull_request:\n", "  pull_request:\n    branches: [main]\n", "the workflow starts on"),
            "pushes to every branch": ("  push:\n    branches: [main]\n", "  push:\n", "the workflow starts on"),
        }
        for label, (before, after, diagnostic) in planted.items():
            with self.subTest(label=label):
                self.assertIn(before, WORKFLOW_TEXT)
                problems = contract_problems(WORKFLOW_TEXT.replace(before, after, 1))
                self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")


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

    def test_pull_requests_merge_only_by_squash(self) -> None:
        self.assertEqual(self.rules["pull_request"]["allowed_merge_methods"], ["squash"])

    def test_the_ruleset_names_no_bypass_and_no_review_that_one_owner_cannot_give(self) -> None:
        self.assertEqual(self.ruleset["bypass_actors"], [], "the owner adds a bypass when a force-push to main is needed")
        self.assertEqual(self.rules["pull_request"]["required_approving_review_count"], 0)

    def test_the_required_check_is_the_name_that_the_workflow_gives(self) -> None:
        jobs = parse_workflow(WORKFLOW_TEXT)["jobs"]
        self.assertEqual(jobs["required"]["name"], self.rules["required_status_checks"]["required_status_checks"][0]["context"])


if __name__ == "__main__":
    unittest.main()
