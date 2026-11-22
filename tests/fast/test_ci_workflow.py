"""The CI workflow contract: every pull request and every push to main runs every job group, and one job requires them all.

The contract reads the workflow as PyYAML parses it and compares parsed keys and values only, never text, so a spaced key, a quoted key, a
YAML anchor or a merge key cannot hide a change. Failure modes covered here, each with a planted case and a control:

- A group job is skipped: an ``if`` on a job or on one of its steps, or ``continue-on-error`` that lets a failed check pass.
- A step runs under another shell or folder, so its command can become a no-op (``shell: bash -n {0}``).
- A guard shard is missing, runs twice, runs another shard's cases or is excluded through ``matrix.exclude``.
- A check command changes: in the workflow (a dry-run flag, a dropped or reordered command, another Python or tool version) or in the Makefile
  that it runs. The recipes are compared line for line with a baseline that does not depend on the checked-out revision (the commit before a
  push to main, the merge base for a pull request), and line rules over the whole Makefile refuse a second rule, a special target, a shell
  variable, an include or a recipe line that ignores a failure (a Trivy scan that no longer fails).
- The required job leaves a group out of ``needs``, does not always run, has its step disabled or under another shell, or its script differs
  from the approved one by a character, holds an expression, or passes when a needed job failed, was cancelled or was skipped. The step sets
  ``shell: bash``, so GitHub runs it with ``-eo pipefail``; its script runs here the same way, exactly as parsed, for all 64 combinations of the
  needed jobs' results.
- A pending run on main is cancelled before its groups start, or a pull request run is never superseded.
"""

from __future__ import annotations

import fnmatch
import functools
import itertools
import json
import os
import platform
import re
import tempfile
import unittest
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from scripts.process import run_command
from tests.fast.git_fixture import ROOT, TIMEOUT_SECONDS, TemporaryRepository

WORKFLOW_TEXT = (ROOT / ".github/workflows/ci.yml").read_text()
MAKEFILE_TEXT = (ROOT / "Makefile").read_text()
RUNNER = "ubuntu-24.04"
PYTHON = "3.14.8"
SHARDS = 8
REQUIRED_NAME = "Every job of this run passed"
GROUPS = ("hooks", "infrastructure", "guards")
EXPRESSION = re.compile(r"\$\{\{ ([^}]+?) \}\}")
RESULTS = ("success", "failure", "cancelled", "skipped")
# The command that GitHub runs a `run` script with under the shell `bash`.
GITHUB_BASH = ("--noprofile", "--norc", "-eo", "pipefail")
SHARD_COMMAND = f'make test-guards SHARD="${{SHARD}}/{SHARDS}"'
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
SETUP_TERRAFORM = "hashicorp/setup-terraform@dfe3c3f87815947d99a8997f908cb6525fc44e9e"
PYTHON_STEP = {"uses": SETUP_PYTHON, "with": {"python-version": PYTHON, "cache": "pip", "cache-dependency-path": "requirements.txt"}}
CHECKOUT_STEP = {"uses": CHECKOUT, "with": {"persist-credentials": False}}
TAIL_STEP = {"name": "Show the end of the run's log after a failure", "if": "failure()", "run": "tail -n 150 runs/*/log.txt"}
# A pull request run is superseded by a newer commit of the same pull request. A push to main has a group of its own (its run, so even the same
# commit pushed again does not replace a pending run), so no push cancels or replaces another run.
CONCURRENCY = {
    "group": "${{ github.workflow }}-${{ github.event_name == 'push' && github.run_id || github.ref }}",
    "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
}
# The jobs and steps of the workflow on main, with only the Python patch release added: a change to any of them is a change to what CI checks.
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
    "guards": {
        "name": "Planted faults through the real commit path, shard ${{ matrix.shard }} of 8",
        "timeout-minutes": 15,
        "steps": [CHECKOUT_STEP, PYTHON_STEP, {"run": "make setup"}, {"run": SHARD_COMMAND, "env": {"SHARD": "${{ matrix.shard }}"}}, TAIL_STEP],
    },
}
JOB_KEYS = {"name", "runs-on", "timeout-minutes", "steps"}
REQUIRED_KEYS = {"name", "needs", "if", "runs-on", "timeout-minutes", "steps"}
REQUIRED_ENV_VALUES = {"HOOKS": "${{ needs.hooks.result }}", "INFRASTRUCTURE": "${{ needs.infrastructure.result }}", "GUARDS": "${{ needs.guards.result }}"}
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
check guards "${GUARDS}"
exit "${failed}"
"""
# Changes to a Makefile recipe on this branch that the owner approved, as (target, line on main, line here): the reason for each is its value.
APPROVED_MAKEFILE_CHANGES: dict[tuple[str, str], tuple[str, str]] = {
    (
        "check-terraform",
        "\t.tools/bin/trivy config --cache-dir $(CURDIR)/.tools/trivy_cache --severity HIGH,CRITICAL --exit-code 1 --skip-dirs .terraform --quiet infra",
    ): (
        "\t.tools/bin/trivy config --cache-dir $(CURDIR)/.tools/trivy_embedded_cache --skip-check-update --severity HIGH,CRITICAL --exit-code 1"
        " --skip-dirs .terraform --quiet infra",
        "Trivy uses the checks built into its pinned binary: a rolling checks bundle must not change what a run reports (earlier review round).",
    ),
}


SPECIAL_TARGETS = (".IGNORE", ".SILENT", ".ONESHELL", ".NOTPARALLEL", ".DEFAULT", ".POSIX")
# Assignments that change how every recipe runs.
FORBIDDEN_VARIABLES = ("SHELL", ".SHELLFLAGS", "MAKEFLAGS", ".RECIPEPREFIX", "PATH")
INCLUDES = ("include", "-include", "sinclude")


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
    """A step that skips, lets a failure pass or runs under another shell or folder. Only the guards' log tail may have a condition: exactly ``failure()``.

    The aggregate's step is the one that sets a shell (``shell_pinned``): its value is compared with the approved one on its own.
    """
    problems = []
    for position, step in enumerate(steps, 1):
        if not isinstance(step, dict):
            problems.append(f"{name}: step {position} is not a mapping")
            continue
        if "continue-on-error" in step:
            problems.append(f"{name}: continue-on-error is set on step {position}, so a failed check can pass")
        if "if" in step and not (name == "guards" and step == TAIL_STEP):
            problems.append(f"{name}: step {position} runs behind the condition {step['if']!r}")
        if "shell" in step and not shell_pinned:
            problems.append(f"{name}: step {position} sets the shell {step['shell']!r}, which can turn its command into a no-op")
        if "working-directory" in step:
            problems.append(f"{name}: step {position} sets the working directory {step['working-directory']!r}, so it may run its command elsewhere")
    return problems


def runs_of(steps: list[Any]) -> list[str]:
    return [str(step["run"]) for step in steps if isinstance(step, dict) and "run" in step]


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
    for key in sorted(set(job) - JOB_KEYS - {"if", "needs", "continue-on-error"} - ({"strategy"} if name == "guards" else set())):
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


def concurrency_problems(concurrency: Any) -> list[str]:
    """A push to main has its own group, so a pending run on main is never cancelled. Only a pull request run is superseded."""
    if not isinstance(concurrency, dict) or set(concurrency) != set(CONCURRENCY):
        return [f"the workflow's concurrency is {concurrency!r}, not the keys {sorted(CONCURRENCY)}"]
    problems = []
    if concurrency["group"] != CONCURRENCY["group"]:
        problems.append(
            f"the concurrency group is {concurrency['group']!r}, not {CONCURRENCY['group']!r}: runs on main could share a group and replace each other"
        )
    if concurrency["cancel-in-progress"] != CONCURRENCY["cancel-in-progress"]:
        problems.append(
            f"cancel-in-progress is {concurrency['cancel-in-progress']!r}, not {CONCURRENCY['cancel-in-progress']!r}: only a pull request run may be cancelled"
        )
    return problems


@functools.cache
def script_problems(script: str, variables: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    """What is wrong with the required job's ``script`` as it passes only when every needed job succeeded.

    The script is written to a file exactly as PyYAML returned it and run with the shell and options that GitHub uses for ``bash``, for every
    combination of the results of the needed jobs. ``variables`` are the step's environment; each value may name an expression of the workflow.
    Cached: the variants of the contract test share most scripts.
    """
    wrong: list[tuple[str, ...]] = []
    with tempfile.TemporaryDirectory(prefix="ci_required_script_") as folder:
        path = Path(folder) / "step.sh"
        path.write_text(script)
        for combination in itertools.product(RESULTS, repeat=len(GROUPS)):
            values = {f"needs.{name}.result": result for name, result in zip(GROUPS, combination, strict=True)}
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
            if (run.returncode == 0) != (set(combination) == {"success"}):
                wrong.append(combination)
    if not wrong:
        return ()
    wrong.sort(key=lambda combination: sum(result != "success" for result in combination))
    example = ", ".join(f"{name}={result}" for name, result in zip(GROUPS, wrong[0], strict=True))
    verdict = "passes when a needed job did not succeed" if any(set(combination) != {"success"} for combination in wrong) else "fails when every job succeeded"
    return (f"required: its script {verdict}, for {len(wrong)} of {len(RESULTS) ** len(GROUPS)} combinations of results, for example {example}",)


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
        problems.append(f"required: its step's env is {environment!r}, not the three results of the needed jobs")
        environment = {}
    for name in sorted(set(environment) | set(REQUIRED_ENV_VALUES)):
        if environment.get(name) != REQUIRED_ENV_VALUES.get(name):
            problems.append(f"required: its env entry {name} is {environment.get(name)!r}, not {REQUIRED_ENV_VALUES.get(name)!r}")
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


def recipe_of(makefile: str, target: str) -> list[str] | None:
    """The target's own line and each line of its recipe, or ``None`` when the Makefile has no such target."""
    lines = makefile.splitlines()
    start = next((index for index, line in enumerate(lines) if line.startswith(f"{target}:")), None)
    if start is None:
        return None
    recipe = [lines[start]]
    for line in lines[start + 1 :]:
        if line.startswith("\t") or not line.strip():
            recipe.append(line)
        else:
            break
    while recipe and not recipe[-1].strip():
        recipe.pop()
    return recipe


def make_targets_of(workflow: dict[str, Any]) -> list[str]:
    """Every target that a ``make`` command of the workflow runs, once each, in order of first use."""
    targets: list[str] = []
    for job in workflow["jobs"].values():
        for script in runs_of(job.get("steps") or []):
            targets += [word for word in re.findall(r"\bmake\s+([a-z][a-z-]*)", script) if word not in targets]
    return targets


ASSIGNMENT = re.compile(r"^\s*(?:(?:export|override|private)\s+)*([A-Za-z_.][\w.\-]*)\s*(?:::=|:=|\+=|\?=|!=|=)")
RULE = re.compile(r"^([^\s#=:][^=:]*?)\s*(::?)(?!=)(.*)$")
INCLUDE = re.compile(r"^\s*(?:-?include|sinclude)(?=\s|$)")
RULE_NAME = re.compile(r"[A-Za-z0-9_./%-]+")
RECIPE_PREFIX = re.compile(r"\t[@+\-\s]*")
USED_VARIABLE = re.compile(r"\$[({]([A-Za-z_.][\w.\-]*)")


def classify(line: str) -> tuple[Any, ...]:
    """What a Makefile line is, by its text alone: blank, recipe, include, assignment, target_variable, rule or unknown (not understood)."""
    if not line.strip() or line.lstrip().startswith("#"):
        return ("blank",)
    if line.startswith("\t"):
        return ("recipe",)
    if INCLUDE.match(line):
        return ("include",)
    if line.rstrip().endswith("\\"):
        return ("unknown",)
    if assignment := ASSIGNMENT.match(line):
        return ("assignment", assignment.group(1))
    if rule := RULE.match(line):
        names, rest = rule.group(1).split(), rule.group(3).strip()
        if rest and (assigned := ASSIGNMENT.match(rest)):
            return ("target_variable", names, assigned.group(1))
        if ";" not in rest and ":" not in rest and all(RULE_NAME.fullmatch(name) for name in names):
            return ("rule", names, rule.group(2))
    return ("unknown",)


def phony_targets(makefile: str) -> set[str]:
    """Every word that a ``.PHONY`` line lists."""
    return {word for line in makefile.splitlines() if line.startswith(".PHONY") for word in line.partition(":")[2].split()}


def structure_problems(makefile: str, baseline: str, targets: list[str]) -> list[str]:
    """Line rules over the whole Makefile, with a line that the baseline already has left alone: each target that a job runs is in exactly one rule
    line, no special target, shell variable or include changes how it runs, and no recipe line of it ignores a failure. What the rules cannot
    classify is refused as not understood."""
    known = set(baseline.splitlines())
    problems: list[str] = []
    rule_lines: list[tuple[int, list[str], str]] = []
    recipes: dict[str, list[tuple[int, str]]] = {target: [] for target in targets}
    assignments: list[tuple[int, str, str]] = []
    current: list[str] = []
    for number, line in enumerate(makefile.splitlines(), 1):
        kind = classify(line)
        if kind[0] == "blank":
            continue
        if kind[0] == "recipe":
            for target in targets:
                if target in current:
                    recipes[target].append((number, line))
            continue
        current = []
        new = line not in known
        if kind[0] == "unknown" and new:
            problems.append(f"Makefile line {number} is not understood: {line!r}")
        elif kind[0] == "include" and new:
            problems.append(f"Makefile line {number} includes another file, which can change any recipe")
        elif kind[0] == "assignment":
            assignments.append((number, kind[1], line))
            if new and kind[1] in FORBIDDEN_VARIABLES:
                problems.append(f"Makefile line {number} assigns {kind[1]}, which changes how every recipe runs")
        elif kind[0] == "target_variable" and new and (kind[2] in FORBIDDEN_VARIABLES or set(kind[1]) & set(targets)):
            problems.append(f"Makefile line {number} sets the variable {kind[2]} for {kind[1]}, which changes how a recipe runs")
        elif kind[0] == "rule":
            rule_lines.append((number, kind[1], kind[2]))
            current = kind[1]
            problems += [f"Makefile line {number} sets the special target {name}" for name in kind[1] if name in SPECIAL_TARGETS and new]
    for target in targets:
        named = [(number, names, separator) for number, names, separator in rule_lines if target in names]
        if len(named) != 1:
            problems.append(f"make {target}: it is the target of {len(named)} rule lines, not one")
        for number, names, separator in named:
            if len(names) > 1:
                problems.append(f"make {target}: it shares its rule line {number} with other targets")
            if separator == "::":
                problems.append(f"make {target}: its rule line {number} is a double-colon rule")
        for number, names, _ in rule_lines:
            if any("%" in name and fnmatch.fnmatchcase(target, name.replace("%", "*")) for name in names):
                problems.append(f"make {target}: a pattern rule matches it (line {number})")
        for number, line in recipes[target]:
            if line in known:
                continue
            prefix = RECIPE_PREFIX.match(line)
            if prefix and {"-", "+"} & set(prefix.group(0)):
                problems.append(f"make {target}: line {number} starts with a - or + prefix, so a failure of its command passes")
            if re.search(r"\|\|\s*(?:true|:)\s*$", line):
                problems.append(f"make {target}: line {number} ends with || true or || :, so a failure of its command passes")
            if line.rstrip().endswith("\\"):
                problems.append(f"make {target}: line {number} is not understood: its recipe line is continued")
        if target in phony_targets(baseline) and target not in phony_targets(makefile):
            problems.append(f"make {target}: it is no longer in .PHONY, so a file of that name would make it up to date and skip it")
        used = {name for _, line in recipes[target] for name in USED_VARIABLE.findall(line)}
        problems += [
            f"Makefile line {number} assigns {name}, which make {target} uses"
            for number, name, line in assignments
            if name in used and name not in FORBIDDEN_VARIABLES and line not in known
        ]
    return problems


def makefile_problems(makefile: str, baseline: str, targets: list[str]) -> list[str]:
    """A recipe that a workflow command runs differs from the baseline's line for line, apart from the changes that the owner approved, or the
    Makefile breaks a structure rule."""
    problems = []
    for target in targets:
        wanted = recipe_of(baseline, target)
        actual = recipe_of(makefile, target)
        if wanted is None or actual is None:
            problems.append(f"make {target}: {'the baseline' if wanted is None else 'this branch'} has no such target")
            continue
        wanted = [APPROVED_MAKEFILE_CHANGES.get((target, line), (line, ""))[0] for line in wanted]
        if actual == wanted:
            continue
        added = [line.strip() for line in actual if line not in wanted]
        dropped = [line.strip() for line in wanted if line not in actual]
        problems.append(
            f"make {target}: it adds {added} and drops {dropped} against the baseline's recipe"
            if added or dropped
            else f"make {target}: its lines or their order differ from the baseline's"
        )
    return [*problems, *structure_problems(makefile, baseline, targets)]


def contract_problems(text: str) -> list[str]:
    """What the workflow fails to do: run every group job on every pull request and every push to main, each shard once, with the pinned commands,
    and fail the one required job unless every group job succeeded."""
    problems: list[str] = []
    try:
        workflow = load_workflow(text)
        if set(workflow) != {"name", "on", "permissions", "concurrency", "jobs"}:
            problems.append(f"the workflow has the keys {sorted(map(str, workflow))}")
        if workflow.get("on") != {"pull_request": None, "push": {"branches": ["main"]}}:
            problems.append(f"the workflow starts on {workflow.get('on')}, not on every pull request and every push to main")
        if workflow.get("permissions") != {"contents": "read"}:
            problems.append(f"the workflow asks for the permissions {workflow.get('permissions')}")
        problems += concurrency_problems(workflow.get("concurrency"))
        if set(workflow["jobs"]) != {*GROUPS, "required"}:
            problems.append(f"the workflow's jobs are {sorted(workflow['jobs'])}, not {sorted({*GROUPS, 'required'})}")
        for name in GROUPS:
            problems += job_problems(name, workflow["jobs"][name])
        problems += matrix_problems(workflow["jobs"]["guards"])
        problems += aggregate_problems(workflow["jobs"])
    except (AssertionError, KeyError, StopIteration, ValueError, TypeError, AttributeError, yaml.YAMLError) as error:
        problems.append(f"the workflow cannot be read as the contract expects: {type(error).__name__}: {error}")
    return problems


def baseline_makefile_text(cwd: Path = ROOT, env: Mapping[str, str] | None = None) -> str:
    """The Makefile that the recipes are compared with, independent of the revision that is checked out.

    On a push to main (HEAD is the tip of ``origin/main`` or ``main``) it is the first parent of HEAD. Otherwise it is the merge base of HEAD and
    ``origin/main``, or of HEAD and ``main`` when the remote-tracking branch is missing. Nothing that resolves is a failure, not a pass.
    """

    def git(*args: str) -> str | None:
        run = run_command("git", list(args), cwd=cwd, env=env, timeout=TIMEOUT_SECONDS)
        return run.stdout.strip() if run.returncode == 0 else None

    head = git("rev-parse", "--verify", "--quiet", "HEAD^{commit}")
    tips = {ref: git("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}") for ref in ("origin/main", "main")}
    available = {ref: tip for ref, tip in tips.items() if tip}
    if head is None or not available:
        raise AssertionError("neither origin/main nor main resolves, so the recipes cannot be compared: fetch main first (the hooks job fetches all history)")
    if head in available.values():
        base = git("rev-parse", "--verify", "--quiet", "HEAD^1^{commit}")
        if base is None:
            raise AssertionError("HEAD is the tip of main and has no commit before it, so the recipes cannot be compared")
    else:
        ref = next(iter(available))
        base = git("merge-base", "HEAD", ref)
        if base is None:
            raise AssertionError(f"HEAD and {ref} have no merge base, so the recipes cannot be compared: fetch the full history of main")
    shown = run_command("git", ["show", f"{base}:Makefile"], cwd=cwd, env=env, timeout=TIMEOUT_SECONDS)
    if shown.returncode != 0:
        raise AssertionError(f"the Makefile at {base} cannot be read: {shown.stderr.strip()}")
    return shown.stdout


def as_tuple(value: str | tuple[str, ...]) -> tuple[str, ...]:
    """A planted case gives one text or several: each is replaced in order, and each diagnostic must appear."""
    return (value,) if isinstance(value, str) else value


TERRAFORM = "      - run: make check-terraform\n"
ANSIBLE = "      - run: make check-ansible\n"
GUARDS = '      - run: make test-guards SHARD="${SHARD}/8"\n'
REQUIRED_ENV = "        env:\n          HOOKS:"
GROUP_LINE = "  group: ${{ github.workflow }}-${{ github.event_name == 'push' && github.run_id || github.ref }}\n"
CANCEL_LINE = "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}\n"
NEEDS_LINE = "needs: [hooks, infrastructure, guards]"
ACCEPTS = 'if [ "$2" != success ]; then'
NOT_SUCCESS = "required: its script passes when a needed job did not succeed"
# Each planted change to the workflow text: (the text to replace, its replacement, the diagnostic that the contract must give).
PLANTS: dict[str, tuple[str | tuple[str, ...], str | tuple[str, ...], str | tuple[str, ...]]] = {
    "the guards job never run": ("    timeout-minutes: 15\n", "    if: false\n    timeout-minutes: 15\n", "guards: it runs behind the condition False"),
    "the guards job run only on push": (
        "    timeout-minutes: 15\n",
        "    if: github.event_name == 'push'\n    timeout-minutes: 15\n",
        "guards: it runs behind the condition \"github.event_name == 'push'\"",
    ),
    "the guards job behind a spaced if key": (
        "    timeout-minutes: 15\n",
        "    if : ${{ false }}\n    timeout-minutes: 15\n",
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
        "    timeout-minutes: 15",
        "    continue-on-error: true\n    timeout-minutes: 15",
        "guards: continue-on-error is set on the job",
    ),
    "a guard job's failure tolerated through a quoted key": (
        "    timeout-minutes: 15",
        '    "continue-on-error": true\n    timeout-minutes: 15',
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
        "    timeout-minutes: 15\n",
        "    timeout-minutes: 15\n    defaults:\n      run:\n        shell: bash -n {0}\n",
        "guards: it has the key 'defaults'",
    ),
    "every shell of the workflow replaced": (
        "permissions:\n  contents: read\n",
        "permissions:\n  contents: read\n\ndefaults:\n  run:\n    shell: bash -n {0}\n",
        "the workflow has the keys",
    ),
    "a key set twice": (
        "    timeout-minutes: 15\n",
        "    timeout-minutes: 15\n    timeout-minutes: 99\n",
        "a key is set twice",
    ),
    "the guards job's timeout changed": ("    timeout-minutes: 15", "    timeout-minutes: 20", "guards: its timeout is 20 minutes, not 15"),
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
        ("required: its step's shell is None, not 'bash'", "required: its script differs from the approved one at line 11"),
    ),
    "the required job's exit masked by a pipe under the approved shell": (
        '          exit "${failed}"\n',
        '          ( exit "${failed}" ) | true\n',
        "required: its script differs from the approved one at line 11",
    ),
    "the required job's script holds a context in a comment (the reviewer's second plant)": (
        '          exit "${failed}"\n',
        '          exit "${failed}" # ${{ github.event_name }}\n',
        ("required: its script holds an expression", "required: its script differs from the approved one at line 11"),
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
        "required: its script differs from the approved one at line 11",
    ),
    "the required job's script with a trailing space": (
        '          exit "${failed}"\n',
        '          exit "${failed}" \n',
        "required: its script differs from the approved one at line 11",
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
        "required: its env entry BASH_ENV is '/dev/null', not None",
    ),
    "the required job given a shell default": (
        "    timeout-minutes: 5\n",
        "    timeout-minutes: 5\n    defaults:\n      run:\n        shell: bash -n {0}\n",
        "required: it has the key 'defaults'",
    ),
    "the required job's timeout changed": ("    timeout-minutes: 5\n", "    timeout-minutes: 6\n", "required: its timeout is 6 minutes, not 5"),
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
        'python-version: "3.14.8"\n          cache',
        'python-version: "3.14"\n          cache',
        "hooks: step 2 differs from the pinned one",
    ),
    "another Terraform": ('terraform_version: "1.16.4"', 'terraform_version: "1.16.5"', "infrastructure: step 3 differs from the pinned one"),
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
    "the required job without the guards in its needs": (NEEDS_LINE, "needs: [hooks, infrastructure]", "required: guards is missing from its needs"),
    "the required job without the hooks in its needs": (NEEDS_LINE, "needs: [infrastructure, guards]", "required: hooks is missing from its needs"),
    "the required job without the infrastructure job in its needs": (
        NEEDS_LINE,
        "needs: [hooks, guards]",
        "required: infrastructure is missing from its needs",
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
    "the required job passes a failure": ("          failed=1\n", "", NOT_SUCCESS + ", for 63 of 64"),
    "the required job passes a skipped job": (
        ACCEPTS,
        'if [ "$2" != success ] && [ "$2" != skipped ]; then',
        NOT_SUCCESS + ", for 7 of 64 combinations of results, for example hooks=success, infrastructure=success, guards=skipped",
    ),
    "the required job passes a cancelled job": (
        ACCEPTS,
        'if [ "$2" != success ] && [ "$2" != cancelled ]; then',
        NOT_SUCCESS + ", for 7 of 64 combinations of results, for example hooks=success, infrastructure=success, guards=cancelled",
    ),
    "the required job does not check the guards": (
        '          check guards "${GUARDS}"\n',
        "",
        NOT_SUCCESS + ", for 3 of 64 combinations of results, for example hooks=success, infrastructure=success, guards=failure",
    ),
    "the required job reads the wrong result": (
        "INFRASTRUCTURE: ${{ needs.infrastructure.result }}",
        "INFRASTRUCTURE: ${{ needs.hooks.result }}",
        "for example hooks=success, infrastructure=failure, guards=success",
    ),
    "pull requests into main only": ("  pull_request:\n", "  pull_request:\n    branches: [main]\n", "the workflow starts on"),
    "pushes to every branch": ("  push:\n    branches: [main]\n", "  push:\n", "the workflow starts on"),
    "the pending runs on main share a group": (GROUP_LINE, "  group: ci-${{ github.ref }}\n", "the concurrency group is 'ci-${{ github.ref }}'"),
    "a push group of the commit, so the same commit pushed again replaces a pending run": (
        GROUP_LINE,
        "  group: ${{ github.workflow }}-${{ github.event_name == 'push' && github.sha || github.ref }}\n",
        "the concurrency group is",
    ),
    "every run cancels the run before it": (CANCEL_LINE, "  cancel-in-progress: true\n", "cancel-in-progress is True"),
    "no run cancelled": (CANCEL_LINE, "  cancel-in-progress: false\n", "cancel-in-progress is False"),
    "the concurrency emptied": (GROUP_LINE + CANCEL_LINE, "", "the workflow's concurrency is None"),
    "the concurrency removed": ("concurrency:\n" + GROUP_LINE + CANCEL_LINE, "", "the workflow has the keys"),
}
# Each planted change to a Makefile recipe that a job runs: (the text to replace, its replacement, the diagnostic that the contract must give).
LAST_RECIPE = '\t$(PYTHON) -m scripts.infra.teardown destroy --confirm "$(CONFIRM)"\n'
FORMAT = "terraform fmt -check -recursive infra"
# Each planted change to the Makefile that the jobs run: (the text to replace, its replacement, the diagnostic that the contract must give).
MAKEFILE_PLANTS: dict[str, tuple[str, str, str]] = {
    "a Trivy scan that never fails": ("--exit-code 1", "--exit-code 0", "make check-terraform: it adds"),
    "a Trivy scan of low findings only": ("--severity HIGH,CRITICAL", "--severity LOW", "make check-terraform: it adds"),
    "a Trivy scan that skips the checks": ("--skip-dirs .terraform --quiet infra", "--skip-dirs infra --quiet infra", "make check-terraform: it adds"),
    "the Terraform validation removed": (
        "\tTF_DATA_DIR=$(CURDIR)/.tools/terraform/static terraform -chdir=infra validate\n",
        "",
        "make check-terraform: it adds [] and drops",
    ),
    "the Ansible check removed": (
        "\tcd ansible && ANSIBLE_CONFIG=ansible.cfg ../.venv/bin/ansible-playbook --syntax-check host.yml\n",
        "\ttrue\n",
        "make check-ansible: it adds ['true']",
    ),
    "the hooks run on staged files only": ("--all-files --hook-stage manual", "--hook-stage manual", "make check: it adds"),
    "the guard suite given a fixed shard": ("$(if $(SHARD),--shard $(SHARD))", "--shard 1/8", "make test-guards: it adds"),
    "the setup installs without hashes": ("--require-hashes ", "", "make setup: it adds"),
    "a prerequisite added to a target": ("\ncheck:\n", "\ncheck: setup\n", "make check: it adds ['check: setup']"),
    "a second rule for the target with an empty recipe (the reviewer's first plant)": (
        LAST_RECIPE,
        LAST_RECIPE + "\ncheck-terraform:\n\t@true\n",
        "make check-terraform: it is the target of 2 rule lines, not one",
    ),
    "a double-colon rule appended for the target": (
        LAST_RECIPE,
        LAST_RECIPE + "\ncheck-terraform::\n\t@true\n",
        "make check-terraform: it is the target of 2 rule lines, not one",
    ),
    "the target's own rule made a double-colon rule": (
        "\ncheck-ansible:\n",
        "\ncheck-ansible::\n",
        "make check-ansible: its rule line 25 is a double-colon rule",
    ),
    "the target made one of several in a rule line": ("\ncheck-ansible:\n", "\ncheck-ansible check-extra:\n", "make check-ansible: it shares its rule line"),
    "a pattern rule that matches the target": (LAST_RECIPE, LAST_RECIPE + "\ncheck-%:\n\t@true\n", "make check-terraform: a pattern rule matches it"),
    "an inline recipe appended for the target": (LAST_RECIPE, LAST_RECIPE + "\ncheck-ansible: ; @true\n", "is not understood"),
    "a define block": (LAST_RECIPE, LAST_RECIPE + "\ndefine hide\nendef\n", "is not understood"),
    "a conditional block": (LAST_RECIPE, LAST_RECIPE + "\nifeq (a,b)\nendif\n", "is not understood"),
    "a line continuation in a checked recipe": (f"{FORMAT}\n", f"{FORMAT} \\\n\t&& true\n", "is not understood"),
    "the shell replaced": (
        "PYTHON := .venv/bin/python\n",
        "SHELL := /bin/true\nPYTHON := .venv/bin/python\n",
        "assigns SHELL, which changes how every recipe runs",
    ),
    "the shell flags replaced": (
        "PYTHON := .venv/bin/python\n",
        ".SHELLFLAGS := -c\nPYTHON := .venv/bin/python\n",
        "assigns .SHELLFLAGS, which changes how every recipe runs",
    ),
    "make given a dry-run flag": (
        "PYTHON := .venv/bin/python\n",
        "MAKEFLAGS += -n\nPYTHON := .venv/bin/python\n",
        "assigns MAKEFLAGS, which changes how every recipe runs",
    ),
    "the recipe prefix replaced": (
        "PYTHON := .venv/bin/python\n",
        ".RECIPEPREFIX := >\nPYTHON := .venv/bin/python\n",
        "assigns .RECIPEPREFIX, which changes how every recipe runs",
    ),
    "the search path replaced": (
        "PYTHON := .venv/bin/python\n",
        "export PATH := /nowhere\nPYTHON := .venv/bin/python\n",
        "assigns PATH, which changes how every recipe runs",
    ),
    "the Python of the recipes replaced": ("PYTHON := .venv/bin/python\n", "PYTHON := true\n", "assigns PYTHON, which make setup uses"),
    "a variable set for the target only": (
        LAST_RECIPE,
        LAST_RECIPE + "\ncheck-terraform: PATH := /nowhere\n",
        "sets the variable PATH for ['check-terraform']",
    ),
    "an included file": (LAST_RECIPE, LAST_RECIPE + "\ninclude other.mk\n", "includes another file"),
    "an included file that may be missing": (LAST_RECIPE, LAST_RECIPE + "\n-include other.mk\n", "includes another file"),
    "a sinclude": (LAST_RECIPE, LAST_RECIPE + "\nsinclude other.mk\n", "includes another file"),
    "a failure ignored by a dash prefix": (f"\t{FORMAT}\n", f"\t-{FORMAT}\n", "make check-terraform: line 18 starts with a - or + prefix"),
    "a failure ignored by a dash after the at-sign": (f"\t{FORMAT}\n", f"\t@-{FORMAT}\n", "make check-terraform: line 18 starts with a - or + prefix"),
    "a plus prefix": (f"\t{FORMAT}\n", f"\t+{FORMAT}\n", "make check-terraform: line 18 starts with a - or + prefix"),
    "a failure turned into success by || true": (f"{FORMAT}\n", f"{FORMAT} || true\n", "make check-terraform: line 18 ends with || true or || :"),
    "a failure turned into success by || :": (f"{FORMAT}\n", f"{FORMAT} || :\n", "make check-terraform: line 18 ends with || true or || :"),
}
MAKEFILE_PLANTS["the Terraform checks dropped from .PHONY"] = (
    " check-terraform check-ansible",
    " check-ansible",
    "make check-terraform: it is no longer in .PHONY",
)
MAKEFILE_PLANTS |= {
    f"the special target {name} appended (the reviewer's second plant for .IGNORE)": (
        LAST_RECIPE,
        LAST_RECIPE + f"\n{name}: check-terraform\n",
        f"sets the special target {name}",
    )
    for name in SPECIAL_TARGETS
}


class WorkflowContractTest(unittest.TestCase):
    """The workflow's parsed keys and values are the contract: every job group runs on every run, and one job requires them all."""

    def test_python_is_one_patch_release_and_the_local_one(self) -> None:
        self.assertEqual(platform.python_version(), PYTHON, "local checks run on the Python that the hosted jobs install")

    def test_trivy_uses_the_checks_built_into_its_pinned_binary_and_fails_on_high_findings(self) -> None:
        trivy = [line for line in MAKEFILE_TEXT.splitlines() if ".tools/bin/trivy" in line]
        self.assertEqual(len(trivy), 1)
        self.assertIn("--skip-check-update", trivy[0], "without it Trivy downloads the newest checks bundle of a rolling tag")
        self.assertNotIn(".tools/trivy_cache ", trivy[0], "that cache can hold a bundle that an earlier run downloaded")
        self.assertIn(" --exit-code 1 ", trivy[0], "any other exit code lets a finding pass")
        self.assertIn(" --severity HIGH,CRITICAL ", trivy[0])

    def test_the_workflow_keeps_the_contract(self) -> None:
        self.assertEqual(contract_problems(WORKFLOW_TEXT), [])

    def test_the_workflow_has_no_plan_and_no_reuse(self) -> None:
        self.assertNotRegex(WORKFLOW_TEXT.lower(), r"\bplan\b|reuse|ci_proof|ci-proof")

    def test_the_script_probe_runs_the_shell_with_the_options_that_github_gives_bash(self) -> None:
        """A script that fails only under ``-e`` or under ``pipefail`` fails here too: with another shell its failure would look like a pass."""
        for script in ("false\nexit 0\n", "false | true\nexit 0\n"):
            with self.subTest(script=script):
                problems = script_problems(script, ())
                self.assertEqual(len(problems), 1)
                self.assertIn("fails when every job succeeded", problems[0])

    def test_a_broken_workflow_fails_the_contract_with_its_own_diagnostic(self) -> None:
        for label, (before, after, diagnostics) in PLANTS.items():
            with self.subTest(label=label):
                planted = WORKFLOW_TEXT
                for old, new in zip(as_tuple(before), as_tuple(after), strict=True):
                    self.assertIn(old, planted)
                    planted = planted.replace(old, new, 1)
                problems = contract_problems(planted)
                for diagnostic in as_tuple(diagnostics):
                    self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")

    def test_the_commands_that_the_workflow_runs_are_the_ones_that_the_contract_pins(self) -> None:
        self.assertEqual(make_targets_of(load_workflow(WORKFLOW_TEXT)), ["setup", "check", "check-terraform", "check-ansible", "test-guards"])

    def test_every_makefile_recipe_that_a_job_runs_is_the_one_before_it_but_for_the_approved_changes(self) -> None:
        targets = make_targets_of(load_workflow(WORKFLOW_TEXT))
        self.assertEqual(makefile_problems(MAKEFILE_TEXT, baseline_makefile_text(), targets), [])

    def test_a_changed_makefile_fails_with_its_own_diagnostic(self) -> None:
        targets = make_targets_of(load_workflow(WORKFLOW_TEXT))
        baseline = baseline_makefile_text()
        for label, (before, after, diagnostic) in MAKEFILE_PLANTS.items():
            with self.subTest(label=label):
                self.assertEqual(MAKEFILE_TEXT.count(before), 1, "the planted text is in the Makefile once")
                problems = makefile_problems(MAKEFILE_TEXT.replace(before, after, 1), baseline, targets)
                self.assertTrue(any(diagnostic in problem for problem in problems), f"no problem names {diagnostic!r}: {problems}")

    def test_the_makefile_and_its_baseline_break_no_structure_rule_beyond_their_own_variable_lines(self) -> None:
        """Only the variable lines are excused (a recipe uses ``PYTHON``): every other line of both Makefiles is understood and passes every rule."""
        targets = make_targets_of(load_workflow(WORKFLOW_TEXT))
        for text in (MAKEFILE_TEXT, baseline_makefile_text()):
            variables = "\n".join(line for line in text.splitlines() if ASSIGNMENT.match(line))
            self.assertEqual(structure_problems(text, variables, targets), [])

    def test_no_other_makefile_takes_precedence_over_the_one_that_is_compared(self) -> None:
        """GNU make reads GNUmakefile, then makefile, before Makefile: a new file of either name would replace every recipe that is compared here."""
        names = {path.name for path in ROOT.iterdir()}
        for name in ("GNUmakefile", "makefile"):
            self.assertNotIn(name, names, f"{name} would be read before Makefile")

    def test_a_line_that_the_baseline_already_has_is_not_a_new_problem(self) -> None:
        """A rule that main already has (an include, a dash prefix) is main's decision: only the line that this branch adds is refused."""
        targets = ["check-ansible"]
        baseline = MAKEFILE_TEXT + "\ninclude other.mk\n"
        self.assertEqual(makefile_problems(baseline, baseline, targets), [])

    def test_a_recipe_that_main_changes_with_the_owner_approval_is_still_accepted_after_the_merge(self) -> None:
        """After the merge, the baseline has the approved line itself, so the approved change replaces nothing and the recipes still match."""
        targets = ["check-terraform"]
        approved = next(iter(APPROVED_MAKEFILE_CHANGES.values()))[0]
        self.assertEqual(makefile_problems(MAKEFILE_TEXT, MAKEFILE_TEXT, targets), [])
        self.assertIn(approved, MAKEFILE_TEXT.splitlines())


class BaselineMakefileTest(TemporaryRepository):
    """The Makefile that the recipes are compared with does not depend on the revision that is checked out: a push compares with the commit
    before it, and a pull request with the merge base."""

    def commit_makefile(self, text: str) -> None:
        (self.repo / "Makefile").write_text(text)
        self.git("add", "Makefile")
        self.git("commit", "--quiet", "-m", "change the Makefile")

    def baseline(self) -> str:
        return baseline_makefile_text(self.repo, self.env)

    def test_a_push_to_main_is_compared_with_the_commit_before_it(self) -> None:
        self.commit_makefile("one\n")
        self.commit_makefile("two\n")
        self.assertEqual(self.baseline(), "one\n")

    def test_a_push_to_main_that_puts_a_dash_before_a_command_is_refused(self) -> None:
        """The reviewer's third plant: with main itself as the baseline, the dash prefix on main's own tip passed."""
        self.commit_makefile(MAKEFILE_TEXT)
        self.commit_makefile(MAKEFILE_TEXT.replace(f"\t{FORMAT}\n", f"\t-{FORMAT}\n", 1))
        problems = makefile_problems((self.repo / "Makefile").read_text(), self.baseline(), ["check-terraform"])
        self.assertTrue(any("make check-terraform: it adds" in problem for problem in problems), problems)

    def test_a_pull_request_branch_is_compared_with_its_merge_base_not_with_the_main_that_moved_on(self) -> None:
        self.commit_makefile("one\n")
        self.git("checkout", "--quiet", "-b", "feature")
        self.git("checkout", "--quiet", "main")
        self.commit_makefile("two\n")
        self.git("checkout", "--quiet", "feature")
        self.commit_makefile("three\n")
        self.assertEqual(self.baseline(), "one\n")

    def test_the_remote_main_comes_before_a_stale_local_main(self) -> None:
        self.commit_makefile("one\n")
        first = self.git("rev-parse", "HEAD").stdout.strip()
        self.commit_makefile("two\n")
        second = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("checkout", "--quiet", "-b", "feature")
        self.commit_makefile("three\n")
        self.git("update-ref", "refs/remotes/origin/main", second)
        self.git("update-ref", "refs/heads/main", first)
        self.assertEqual(self.baseline(), "two\n")

    def test_the_remote_main_tip_is_a_push_run_too(self) -> None:
        self.commit_makefile("one\n")
        self.commit_makefile("two\n")
        tip = self.git("rev-parse", "HEAD").stdout.strip()
        self.git("checkout", "--quiet", "--detach")
        self.git("branch", "--quiet", "-D", "main")
        self.git("update-ref", "refs/remotes/origin/main", tip)
        self.assertEqual(self.baseline(), "one\n")

    def test_no_main_at_all_fails_with_a_clear_message(self) -> None:
        self.commit_makefile("one\n")
        self.git("checkout", "--quiet", "-b", "work")
        self.git("branch", "--quiet", "-D", "main")
        with self.assertRaisesRegex(AssertionError, "neither origin/main nor main"):
            self.baseline()

    def test_the_first_commit_of_main_has_no_commit_before_it(self) -> None:
        self.commit_makefile("one\n")
        with self.assertRaisesRegex(AssertionError, "no commit before it"):
            self.baseline()


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
        jobs = load_workflow(WORKFLOW_TEXT)["jobs"]
        self.assertEqual(jobs["required"]["name"], self.rules["required_status_checks"]["required_status_checks"][0]["context"])


if __name__ == "__main__":
    unittest.main()
