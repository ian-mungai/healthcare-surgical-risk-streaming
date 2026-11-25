"""The runners that build a table with a sealed job as a setup step call it with ``--result`` and judge its one result file through the accept rules.

The sealed jobs (the patient split, the Silver cases, signals, hypotension and labels jobs, the reconciliation job and the Bronze clinical and laboratory
job) exit 2 on a call without ``--result``, and none of them writes ``outcome``. The runners are untracked, so each test that needs one skips when it is
absent. No Docker or Spark runs here: the container is a stand-in that writes the result file.

Failure modes covered here:

- A runner calls a sealed job (``spark.jobs.<job>``) in a function that never passes ``--result`` or a sealed option, so the job exits 2 at the first step.
- A setup build is accepted on a file that is not the closed schema of its job, one that did not write, one with a false pass flag or one that names another
  branch, or it is read from a stream.
- A setup build shares its result folder with another call or leaves it behind.
- A runner builds the module name of a sealed job with an f-string (``f"spark.jobs.{JOB}"``), so the rule that reads the name as a string never sees it. The
  rule resolves a module constant and takes a name that it cannot resolve for a sealed job, and the two wrappers that build the name (the reconciliation
  runner's ``reconcile`` and the hypotension runner's ``silver``) are run on a stand-in and the command that they would run is read.
- A runner logs or reads ``outcome`` of a setup build of the Silver cases job, a field that the job no longer writes.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from tests.fast.git_fixture import git_in
from tests.fast.label_accept import accepted, empty_exit_ok, refusal_ok
from tests.fast.test_outcome_acceptance import split_run
from tests.fast.test_silver_acceptance import written
from tests.fast.test_silver_labels_acceptance import HEAD
from tests.fast.test_silver_labels_acceptance import written as label_written

E2E = Path(__file__).resolve().parents[1] / "e2e"
SEALED_JOBS = ("patient_split", "silver_cases", "silver_signals", "silver_hypotension", "silver_labels", "silver_reconcile", "bronze_clinical_lab")
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


UNRESOLVED = "\0"


def string_constants(tree: ast.Module) -> dict[str, str]:
    """The module-level names that are assigned one string: ``JOB = "silver_reconcile"``."""
    return {
        node.targets[0].id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }


def formatted_name(node: ast.JoinedStr, constants: dict[str, str]) -> str | None:
    """The text of an f-string that starts ``spark.jobs.``, with a module constant put in and ``UNRESOLVED`` for any other value, or ``None`` for another
    f-string."""
    parts: list[str] = []
    for part in node.values:
        if isinstance(part, ast.Constant):
            parts.append(str(part.value))
        elif isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name) and part.value.id in constants:
            parts.append(constants[part.value.id])
        else:
            parts.append(UNRESOLVED)
    text = "".join(parts)
    return text if text.startswith("spark.jobs.") else None


def names_formatted_sealed_job(text: str) -> bool:
    """Whether a formatted name is a sealed job or holds a part that cannot be resolved (taken for a sealed job, so the function has to pass the option)."""
    return UNRESOLVED in text or names_sealed_job(text)


def sealed_calls_without_result(source: str) -> list[str]:
    """The functions of ``source`` that name ``spark.jobs.<sealed job>`` as a string and neither name ``--result`` nor pass a keyword that starts with
    ``sealed``, ``setup`` or ``result``: ``function:line``. The module level counts as one function."""
    tree = ast.parse(source)
    constants = string_constants(tree)
    problems: list[str] = []
    scopes: list[ast.AST] = [tree, *(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef))]
    for scope in scopes:
        own = own_nodes(scope)
        strings = [node for node in own if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        jobs: list[ast.Constant | ast.JoinedStr] = [node for node in strings if names_sealed_job(str(node.value))]
        for node in own:
            if isinstance(node, ast.JoinedStr):
                text = formatted_name(node, constants)
                if text is not None and names_formatted_sealed_job(text):
                    jobs.append(node)
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
    """The sealed job names that ``source`` calls, as ``("-m", "spark.jobs.<job>", ...)`` list items or as an f-string that a module constant resolves."""
    tree = ast.parse(source)
    constants = string_constants(tree)
    formatted = {formatted_name(node, constants) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr)}
    return sorted({job for job in SEALED_JOBS if f'"spark.jobs.{job}"' in source or f"spark.jobs.{job}" in formatted})


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
        source = 'def run(self):\n    return self.job(["-m", "spark.jobs.table_checks", "run"])\n'
        self.assertEqual([], sealed_calls_without_result(source))

    def test_an_f_string_name_of_the_job_under_test_is_judged_through_its_own_function(self) -> None:
        source = 'JOB = "silver_reconcile"\ndef verify(self):\n    return self.job(["-m", f"spark.jobs.{JOB}", "verify"])\n'
        self.assertEqual(["verify:3"], sealed_calls_without_result(source))

    def test_an_f_string_name_that_a_module_constant_gives_is_judged_and_one_that_passes_a_result_option_is_not(self) -> None:
        with_option = 'JOB = "silver_reconcile"\ndef verify(self):\n    return self.job(["-m", f"spark.jobs.{JOB}", "verify", "--result", "/r"])\n'
        with_keyword = 'JOB = "silver_reconcile"\ndef verify(self):\n    return self.job(["-m", f"spark.jobs.{JOB}"], sealed_branch="b")\n'
        self.assertEqual([], sealed_calls_without_result(with_option))
        self.assertEqual([], sealed_calls_without_result(with_keyword))

    def test_an_f_string_name_of_a_job_that_is_not_sealed_is_not_judged(self) -> None:
        source = 'JOB = "table_checks"\ndef verify(self):\n    return self.job(["-m", f"spark.jobs.{JOB}", "verify"])\n'
        self.assertEqual([], sealed_calls_without_result(source))

    def test_an_f_string_name_from_a_parameter_is_taken_for_a_sealed_job(self) -> None:
        """The hypotension runner's ``silver(job, ...)`` names the job from its argument: the name cannot be resolved, so it has to pass the option."""
        without = 'def silver(self, job, mode):\n    return self.job(["-m", f"spark.jobs.{job}", mode])\n'
        with_option = 'def silver(self, job, mode):\n    return self.job(["-m", f"spark.jobs.{job}", mode, "--result", "/r"])\n'
        self.assertEqual(["silver:2"], sealed_calls_without_result(without))
        self.assertEqual([], sealed_calls_without_result(with_option))

    def test_an_f_string_name_is_counted_as_a_call_of_a_sealed_job(self) -> None:
        self.assertEqual(["silver_reconcile"], sealed_job_calls('JOB = "silver_reconcile"\nx = ["-m", f"spark.jobs.{JOB}"]\n'))
        self.assertEqual([], sealed_job_calls('JOB = "table_checks"\nx = ["-m", f"spark.jobs.{JOB}"]\n'))


def outcome_reads(source: str, call: str) -> list[str]:
    """The functions of ``source`` that call ``<x>.<call>(...)`` and also name ``outcome`` in a string (a ``get("outcome")`` or a log line): ``name:line``."""
    tree = ast.parse(source)
    problems: list[str] = []
    for scope in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)):
        own = own_nodes(scope)
        calls = [node for node in own if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == call]
        strings = [node for node in own if isinstance(node, ast.Constant) and isinstance(node.value, str) and "outcome" in node.value]
        if calls and strings:
            problems.extend(f"{scope.name}:{node.lineno}" for node in strings)
    return sorted(problems)


class OutcomeRule(unittest.TestCase):
    def test_a_function_that_calls_a_setup_build_and_names_outcome_is_found(self) -> None:
        source = 'def setup(self):\n    code, result = self.stack.silver("build")\n    return f"exit {code}, outcome {result.get(\'outcome\')}"\n'
        self.assertEqual(["setup:3", "setup:3"], outcome_reads(source, "silver"))

    def test_a_function_that_names_outcome_without_the_call_is_not_found(self) -> None:
        source = 'def gold(self):\n    return self.gold.gold("build").get("outcome")\n'
        self.assertEqual([], outcome_reads(source, "silver"))

    def test_the_gold_runner_reads_no_outcome_of_the_silver_cases_build(self) -> None:
        path = E2E / "gold_dimensions.py"
        if not path.is_file():
            self.skipTest("the untracked runners are not on this machine")
        self.assertEqual([], outcome_reads(path.read_text(), "silver"))


class SealedList(unittest.TestCase):
    """The guard's list of sealed jobs is the list of the jobs that take ``--result``, read from the command line in each job's own docstring."""

    def test_the_jobs_whose_docstring_shows_the_result_option_are_the_sealed_jobs_of_the_guard(self) -> None:
        jobs = Path(__file__).resolve().parents[2] / "spark" / "jobs"
        taking = {path.stem for path in jobs.glob("*.py") if "--result <path>" in (ast.get_docstring(ast.parse(path.read_text())) or "")}
        self.assertEqual(set(SEALED_JOBS), taking)

    def test_every_command_line_example_of_a_sealed_job_passes_the_result_option(self) -> None:
        jobs = Path(__file__).resolve().parents[2] / "spark" / "jobs"
        for name in SEALED_JOBS:
            with self.subTest(job=name):
                lines = [line for line in (ast.get_docstring(ast.parse((jobs / f"{name}.py").read_text())) or "").splitlines() if f"spark.jobs.{name} " in line]
                self.assertTrue(lines)
                self.assertTrue(all("--result <path>" in line for line in lines), lines)


def git_tracked(folder: Path) -> list[str]:
    """The names of the files Git tracks directly in ``folder``."""
    listed = git_in(folder, "ls-files", "--", ".")
    return [name for name in listed.stdout.splitlines() if "/" not in name]


class EveryRunner(unittest.TestCase):
    def test_no_runner_on_this_machine_calls_a_sealed_job_without_the_result_option(self) -> None:
        tracked = set(git_tracked(E2E))
        paths = sorted(path for path in E2E.glob("*.py") if path.name != "__init__.py")
        if not [path for path in paths if path.name not in tracked]:
            self.skipTest("the untracked runners are not on this machine (a clean checkout holds only the tracked ones)")
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


def stand_in_stack(
    case: unittest.TestCase, module: ModuleType, content: Any, code: int, out: str = "", err: str = ""
) -> tuple[Any, list[list[str]], list[str]]:
    """The ``Stack`` of a runner on a stand-in container: it records each command and writes ``content`` into the result folder that the command mounts.
    Returns the stack, its commands and the result folder of each call."""
    folder = Path(case.enterContext(tempfile.TemporaryDirectory(prefix="setup_builds_")))
    log: list[str] = []
    stack = module.Stack(folder, "p-setup-test", folder, SimpleNamespace(commands=[]), log)
    commands: list[list[str]] = []
    folders: list[str] = []

    def run(command: list[str], *_: Any, **__: Any) -> tuple[int, str, str]:
        commands.append(command)
        placed = place(command, content)
        if placed is not None:
            folders.append(placed)
        return code, out, err

    stack.run = run
    stack.watch = lambda done, stalled: None
    return stack, commands, folders


def check_command(case: unittest.TestCase, command: list[str], folders: list[str]) -> None:
    """The command passes ``--result /result/result.json`` once, mounts one folder at ``/result`` and the folder is gone after the call."""
    case.assertEqual(1, command.count("--result"))
    case.assertEqual("/result/result.json", command[command.index("--result") + 1])
    case.assertEqual(1, sum(1 for word in command if word.endswith(":/result")))
    case.assertEqual(1, len(folders))
    case.assertFalse(os.path.exists(folders[0]))


class SetupBuilds(unittest.TestCase):
    """The setup build of each runner that has one, on a stand-in container."""

    def stack(self, module: ModuleType, content: Any, code: int, out: str = "") -> tuple[Any, list[list[str]], list[str]]:
        return stand_in_stack(self, module, content, code, out)

    def check_command(self, command: list[str], folders: list[str]) -> None:
        check_command(self, command, folders)

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


class WrapperCommands(unittest.TestCase):
    """The command that a wrapper that builds the module name with an f-string would run, read on a stand-in container."""

    def reconcile(self) -> tuple[Any, list[list[str]], list[str]]:
        return stand_in_stack(self, runner("silver_reconcile"), None, 0)

    def test_the_reconciliation_wrapper_runs_the_reconciliation_job_with_one_result_option_and_its_own_folder(self) -> None:
        stack, commands, folders = self.reconcile()
        with contextlib.suppress(RuntimeError):
            stack.reconcile("b")
        self.assertEqual(1, len(commands))
        self.assertIn("spark.jobs.silver_reconcile", commands[0])
        self.assertEqual(["verify", "--branch", "b"], commands[0][commands[0].index("spark.jobs.silver_reconcile") + 1 :][:3])
        check_command(self, commands[0], folders)

    def test_each_call_of_the_reconciliation_wrapper_has_a_fresh_folder(self) -> None:
        stack, commands, folders = self.reconcile()
        for branch in ("b", "c"):
            with contextlib.suppress(RuntimeError):
                stack.reconcile(branch)
        self.assertEqual(2, len(set(folders)))
        self.assertFalse(any(os.path.exists(folder) for folder in folders))

    def test_the_hypotension_wrapper_runs_the_job_that_it_is_given_with_the_result_option(self) -> None:
        module = runner("silver_hypotension")
        folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="wrapper_")))
        stack = module.Stack(folder, "p-wrapper-test", folder, SimpleNamespace(commands=[]), [])
        commands: list[list[str]] = []

        def discard(command: str, arguments: list[str], **_: Any) -> int:
            commands.append([command, *arguments])
            return 0

        with mock.patch.object(module, "run_discarding_output", discard):
            for job in ("silver_hypotension", "silver_signals"):
                stack.silver(job, "build", "b", base="trunk")
        self.assertEqual(2, len(commands))
        for job, command in zip(("silver_hypotension", "silver_signals"), commands, strict=True):
            with self.subTest(job=job):
                self.assertEqual(f"spark.jobs.{job}", command[command.index("-m") + 1])
                self.assertEqual(1, command.count("--result"))
                self.assertEqual("/result/result.json", command[command.index("--result") + 1])
                self.assertEqual(1, sum(1 for word in command if word.endswith(":/result")))
                self.assertEqual("trunk", command[command.index("--base") + 1])


class LabelsCalls(unittest.TestCase):
    """The calls of the labels runner: each one mounts its own fresh folder, passes ``--result`` and reads that file, never a stream."""

    def stack(self, content: Any, code: int, out: str = "", err: str = "") -> tuple[Any, list[list[str]], list[str]]:
        return stand_in_stack(self, runner("silver_labels"), content, code, out, err)

    def test_a_build_passes_the_result_option_and_the_file_is_the_result(self) -> None:
        document = label_written(branch=BRANCH)
        stack, commands, folders = self.stack(document, 0, out='RESULT {"outcome": "written"}\n')
        call = stack.silver("build", BRANCH, base="trunk")
        self.assertEqual((0, document), (call.code, call.document))
        self.assertEqual(document, accepted(call.code, call.document, "build", BRANCH))
        self.assertIn("spark.jobs.silver_labels", commands[0])
        check_command(self, commands[0], folders)

    def test_a_verify_passes_the_result_option_too(self) -> None:
        document = label_written(mode="verify", branch=BRANCH, base_hash=None, write_hash=None, branch_hash=HEAD)
        stack, commands, folders = self.stack(document, 0)
        call = stack.silver("verify", BRANCH)
        self.assertEqual(document, accepted(call.code, call.document, "verify", BRANCH))
        check_command(self, commands[0], folders)

    def test_a_call_keeps_its_base_and_its_environment_beside_the_result_option(self) -> None:
        stack, commands, folders = self.stack(None, 124)
        call = stack.silver("build", BRANCH, base="trunk", env={"JOB_DEADLINE_SECONDS": "1"})
        self.assertIn("JOB_DEADLINE_SECONDS=1", commands[0])
        self.assertEqual("trunk", commands[0][commands[0].index("--base") + 1])
        self.assertTrue(empty_exit_ok(call.code, call.present, 124))
        check_command(self, commands[0], folders)

    def test_a_call_with_extra_arguments_passes_them_and_the_result_option(self) -> None:
        stack, commands, folders = self.stack(None, 2)
        call = stack.silver("build", BRANCH, extra=("--no_such_option",))
        self.assertIn("--no_such_option", commands[0])
        self.assertTrue(empty_exit_ok(call.code, call.present, 2))
        check_command(self, commands[0], folders)

    def test_a_refusal_is_judged_from_the_file_and_never_from_a_stream(self) -> None:
        document = {"job": "silver_labels", "mode": "build", "refusal": ["branch_missing"]}
        stack, _, _ = self.stack(document, 1, out="silver_labels: other: failed\n", err="silver_labels: other: failed\n")
        call = stack.silver("build", BRANCH, base="no_such_base")
        self.assertTrue(refusal_ok(call.code, call.document, "build", ["branch_missing"]))
        self.assertFalse(refusal_ok(call.code, call.document, "build", ["other: failed"]))

    def test_each_call_has_a_fresh_folder_and_no_file_of_an_earlier_call_is_read(self) -> None:
        stack, commands, folders = self.stack(label_written(branch=BRANCH), 0)
        stack.silver("build", BRANCH, base="trunk")

        def run(command: list[str], *_: Any, **__: Any) -> tuple[int, str, str]:
            commands.append(command)
            folders.append(place(command, None) or "")
            return 0, "", ""

        stack.run = run
        call = stack.silver("verify", BRANCH)
        self.assertIsNone(call.document)
        self.assertFalse(call.present)
        self.assertEqual(2, len(set(folders)))
        self.assertFalse(any(os.path.exists(folder) for folder in folders))

    def test_a_stalled_attempt_leaves_no_file_for_the_retry(self) -> None:
        document = label_written(branch=BRANCH)
        stack, commands, folders = self.stack(document, 0)
        spark_calls: list[int] = []

        def run(command: list[str], *_: Any, **__: Any) -> tuple[int, str, str]:
            if "run" not in command:
                return 0, "", ""
            commands.append(command)
            spark_calls.append(1)
            folders.append(place(command, document if len(spark_calls) == 1 else None) or "")
            return 0, "", ""

        watched: list[int] = []

        def watch(done: Any, stalled: Any) -> None:
            watched.append(1)
            if len(watched) == 1:
                stalled.set()

        stack.run, stack.watch = run, watch
        call = stack.silver("build", BRANCH, base="trunk")
        self.assertEqual(2, len(spark_calls))
        self.assertEqual(2, len(set(folders)))
        self.assertIsNone(call.document)
        self.assertFalse(call.present)

    def test_a_file_that_is_not_one_document_is_no_result_and_counts_as_a_file(self) -> None:
        for label, content in (("two documents", '{"a": 1}\n{"a": 1}\n'), ("a list", "[]"), ("not JSON", "x"), ("a repeated key", '{"a": 1, "a": 2}')):
            with self.subTest(label):
                stack, _, _ = self.stack(content, 0)
                call = stack.silver("verify", BRANCH)
                self.assertIsNone(call.document)
                self.assertTrue(call.present)
                self.assertEqual({}, accepted(call.code, call.document, "verify", BRANCH))

    def test_the_streams_of_the_job_are_discarded_inside_the_container(self) -> None:
        stack, commands, _ = self.stack(None, 0)
        stack.silver("verify", BRANCH)
        self.assertEqual("sh", commands[0][commands[0].index("--entrypoint") + 1])
        self.assertIn('exec python "$@" >/dev/null 2>&1', commands[0])

    def test_the_log_holds_no_line_of_a_stream(self) -> None:
        stack, _, _ = self.stack({"job": "silver_labels", "mode": "verify", "refusal": ["kidney_rule: failed"]}, 1, out="SECRET-OUT\n", err="SECRET-ERR\n")
        stack.silver("verify", BRANCH)
        self.assertNotIn("SECRET", "\n".join(stack.log))


if __name__ == "__main__":
    unittest.main()
