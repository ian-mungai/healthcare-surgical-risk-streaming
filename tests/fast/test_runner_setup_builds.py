"""The runners that build a table with a sealed job as a setup step call it with ``--result`` and judge its one result file through the accept rules.

The sealed jobs (the patient split, the Silver cases, signals and hypotension jobs, the reconciliation job and the Bronze clinical and laboratory job) exit 2
on a call without ``--result``, and none of them writes ``outcome``. The runners are untracked, so each test that needs one skips when it is absent. No Docker
or Spark runs here: the container is a stand-in that writes the result file.

Failure modes covered here:

- A runner calls a sealed job (``spark.jobs.<job>``) in a function that never passes ``--result`` or a sealed option, so the job exits 2 at the first step.
- A setup build is accepted on a file that is not the closed schema of its job, one that did not write, one with a false pass flag or one that names another
  branch, or it is read from a stream.
- A setup build shares its result folder with another call or leaves it behind.
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from tests.fast.test_outcome_acceptance import split_run
from tests.fast.test_silver_acceptance import written

E2E = Path(__file__).resolve().parents[1] / "e2e"
SEALED_JOBS = ("patient_split", "silver_cases", "silver_signals", "silver_hypotension", "silver_reconcile", "bronze_clinical_lab")
# A function that names one of these (a string ``--result`` or a keyword that starts with one of these words) hands the job its result file.
RESULT_OPTIONS = ("--result",)
RESULT_KEYWORDS = ("sealed", "setup", "result")
BRANCH = "b"


def own_nodes(scope: ast.AST) -> list[ast.AST]:
    """The nodes of ``scope`` that are not inside a function of it: a function is judged as a scope of its own."""
    nodes: list[ast.AST] = []
    pending = list(ast.iter_child_nodes(scope))
    while pending:
        node = pending.pop()
        nodes.append(node)
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            pending.extend(ast.iter_child_nodes(node))
    return nodes


def names_sealed_job(text: str) -> bool:
    return any(text == f"spark.jobs.{job}" or text.startswith(f"spark.jobs.{job} ") for job in SEALED_JOBS)


def sealed_calls_without_result(source: str) -> list[str]:
    """The functions of ``source`` that name ``spark.jobs.<sealed job>`` as a string and neither name ``--result`` nor pass a keyword that starts with
    ``sealed``, ``setup`` or ``result``: ``function:line``. The module level counts as one function."""
    tree = ast.parse(source)
    problems: list[str] = []
    scopes: list[ast.AST] = [tree, *(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef))]
    for scope in scopes:
        own = own_nodes(scope)
        strings = [node for node in own if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        jobs = [node for node in strings if names_sealed_job(str(node.value))]
        if not jobs:
            continue
        keywords = [
            keyword.arg or ""
            for node in own
            if isinstance(node, ast.Call)
            for keyword in node.keywords
            if not (isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, bool))
        ]
        handed = any(str(node.value) in RESULT_OPTIONS for node in strings) or any(name.startswith(RESULT_KEYWORDS) for name in keywords)
        if not handed:
            name = getattr(scope, "name", "<module>")
            problems.extend(f"{name}:{node.lineno}" for node in jobs)
    return sorted(problems)


def sealed_job_calls(source: str) -> list[str]:
    """The sealed job names that ``source`` calls, as ``("-m", "spark.jobs.<job>", ...)`` list items or an f-string of the same form."""
    return sorted({job for job in SEALED_JOBS if f'"spark.jobs.{job}"' in source})


class GuardRule(unittest.TestCase):
    """The rule itself, on small sources: each one breaks it or does not."""

    def test_a_function_that_calls_a_sealed_job_with_no_result_option_is_found(self) -> None:
        source = 'def silver(self):\n    return self.job(["-m", "spark.jobs.silver_signals", "build"], self.mounts())\n'
        self.assertEqual(["silver:2"], sealed_calls_without_result(source))

    def test_a_function_that_passes_the_result_option_or_a_sealed_keyword_is_not_found(self) -> None:
        with_option = 'def silver(self):\n    return self.job(["-m", "spark.jobs.silver_signals", "build", "--result", "/result/result.json"])\n'
        with_keyword = 'def silver(self):\n    return self.job(["-m", "spark.jobs.silver_cases", "build"], sealed=("cases", "build", "b"))\n'
        with_setup = 'def split(self):\n    return self.job(["-m", "spark.jobs.patient_split"], setup="split")\n'
        for source in (with_option, with_keyword, with_setup):
            with self.subTest(source=source[:30]):
                self.assertEqual([], sealed_calls_without_result(source))

    def test_a_flag_that_only_discards_the_streams_is_no_result_option(self) -> None:
        """The labels runner passed ``sealed=True`` to hide the job's streams, and no ``--result``: the job exited 2."""
        source = 'def split(self):\n    return self.job(["-m", "spark.jobs.patient_split", "build"], sealed=True)\n'
        self.assertEqual(["split:2"], sealed_calls_without_result(source))

    def test_a_job_that_is_not_sealed_is_not_judged(self) -> None:
        source = 'def run(self):\n    return self.job(["-m", "spark.jobs.silver_labels", "build"])\n'
        self.assertEqual([], sealed_calls_without_result(source))

    def test_an_f_string_name_of_the_job_under_test_is_judged_through_its_own_function(self) -> None:
        source = 'JOB = "silver_reconcile"\ndef verify(self):\n    return self.job(["-m", "spark.jobs.silver_reconcile", "verify"])\n'
        self.assertEqual(["verify:3"], sealed_calls_without_result(source))


class EveryRunner(unittest.TestCase):
    def test_no_runner_on_this_machine_calls_a_sealed_job_without_the_result_option(self) -> None:
        paths = sorted(path for path in E2E.glob("*.py") if path.name != "__init__.py")
        if not paths:
            self.skipTest("the untracked runners are not on this machine")
        found = {path.name: sealed_calls_without_result(path.read_text()) for path in paths}
        self.assertEqual({}, {name: problems for name, problems in found.items() if problems})
        self.assertTrue(any(sealed_job_calls(path.read_text()) for path in paths))


def runner(name: str) -> ModuleType:
    if not (E2E / f"{name}.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module(f"tests.e2e.{name}")


def place(command: list[str], content: Any) -> str | None:
    """What the container does: write ``content`` into the folder that the command mounts at ``/result``. Returns that folder, or ``None`` with no mount."""
    mounts = [command[index + 1] for index, word in enumerate(command) if word == "--volume" and command[index + 1].endswith(":/result")]
    if not mounts:
        return None
    folder = mounts[0].removesuffix(":/result")
    if content is not None:
        Path(folder, "result.json").write_text(content if isinstance(content, str) else json.dumps(content))
    return folder


class SetupBuilds(unittest.TestCase):
    """The setup build of each runner that has one, on a stand-in container."""

    def stack(self, module: ModuleType, content: Any, code: int, out: str = "") -> tuple[Any, list[list[str]], list[str]]:
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="setup_builds_")))
        log: list[str] = []
        stack = module.Stack(folder, "p-setup-test", folder, SimpleNamespace(commands=[]), log)
        commands: list[list[str]] = []
        folders: list[str] = []

        def run(command: list[str], *_: Any, **__: Any) -> tuple[int, str, str]:
            commands.append(command)
            placed = place(command, content)
            if placed is not None:
                folders.append(placed)
            return code, out, ""

        stack.run = run
        stack.watch = lambda done, stalled: None
        return stack, commands, folders

    def check_command(self, command: list[str], folders: list[str]) -> None:
        self.assertEqual(1, command.count("--result"))
        self.assertEqual("/result/result.json", command[command.index("--result") + 1])
        self.assertEqual(1, sum(1 for word in command if word.endswith(":/result")))
        self.assertEqual(1, len(folders))
        self.assertFalse(os.path.exists(folders[0]))

    def test_the_profile_runner_builds_the_silver_signals_tables_with_the_result_option_and_accepts_the_clean_file(self) -> None:
        module = runner("signal_profile")
        document = {**written(), "branch": BRANCH}
        stack, commands, folders = self.stack(module, document, 0, out='RESULT {"outcome": "written"}\n')
        code, result, err, _ = stack.silver("build", BRANCH, base="trunk")
        self.assertEqual((0, document, ""), (code, result, err))
        self.assertIn("spark.jobs.silver_signals", commands[0])
        self.check_command(commands[0], folders)

    def test_the_profile_runner_refuses_a_file_that_is_not_a_clean_write_of_the_job_and_the_branch(self) -> None:
        module = runner("signal_profile")
        good = {**written(), "branch": BRANCH}
        for label, bad in (
            ("outcome only", {"outcome": "written"}),
            ("extra key", {**good, "outcome": "written"}),
            ("another branch", {**good, "branch": "other"}),
            ("nothing written", {**good, "write_hash": None, "branch_hash": good["checked_hash"]}),
            ("a false flag", {**good, "passed": {"numeric_observations": True, "waveform_seconds": False}}),
        ):
            with self.subTest(label):
                stack, _, _ = self.stack(module, bad, 0)
                self.assertEqual({}, stack.silver("build", BRANCH, base="trunk")[1])
        stack, _, _ = self.stack(module, good, 1)
        self.assertEqual({}, stack.silver("build", BRANCH, base="trunk")[1])
        stack, _, _ = self.stack(module, None, 0)
        self.assertEqual({}, stack.silver("build", BRANCH, base="trunk")[1])

    def test_the_labels_runner_builds_the_patient_split_with_the_result_option_and_accepts_the_clean_file(self) -> None:
        module = runner("silver_labels")
        status, document = split_run("build")
        stack, commands, folders = self.stack(module, {**document, "branch": BRANCH}, status)
        code, _ = stack.split(BRANCH, "trunk")
        self.assertEqual(0, code)
        self.assertIn("spark.jobs.patient_split", " ".join(commands[0]))
        self.check_command(commands[0], folders)

    def test_the_labels_runner_gives_a_nonzero_exit_for_a_split_file_that_is_refused_or_missing(self) -> None:
        module = runner("silver_labels")
        status, document = split_run("build")
        good = {**document, "branch": BRANCH}
        for label, bad, code in (
            ("another branch", {**good, "branch": "other"}, 0),
            ("a false flag", {**good, "passed": {"patient_split": False}}, 0),
            ("an extra key", {**good, "outcome": "written"}, 0),
            ("nothing written", {**good, "write_hash": None, "branch_hash": good["checked_hash"]}, 0),
            ("a write hash that is the checked hash", {**good, "write_hash": good["checked_hash"], "branch_hash": good["checked_hash"]}, 0),
            ("no file", None, 0),
            ("a failed exit", good, 1),
        ):
            with self.subTest(label):
                stack, _, _ = self.stack(module, bad, code)
                self.assertNotEqual(0, stack.split(BRANCH, "trunk")[0])


if __name__ == "__main__":
    unittest.main()
