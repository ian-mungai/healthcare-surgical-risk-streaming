"""The Silver labels runner expects the codes that the real command line writes to the refusal file, and its accept function takes the real refusal.

The runner stays untracked, so each runner test skips when it is not on this machine. The command line (``spark.silver.cli.run``) is the real one: only the
job's work is a stand-in. Docker, Nessie and Spark do not run here.

Failure modes covered here (fix round 3, owner decision D):

- The runner expects ``["branch_missing: failed"]`` in the refusal file of a build on a base branch that does not exist. The command line writes
  ``["branch_missing"]`` to the file (``branch_missing: failed`` is the spelling of standard error), so the case fails on a correct job. The same holds for the
  other generic codes (``settings``, ``catalog_error``, ``internal_error``).
- A fixture of the accept function holds the suffixed spelling of a generic code, so the test passes on a refusal that the job never writes.
- The codes of a failed check stay ``<code>: failed`` in the file (the one form of every check problem), and the real command line shows it.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import io
import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.silver.checks import SilverSignalsError
from spark.silver.cli import run
from tests.e2e.run_record import Record
from tests.fast.label_accept import empty_exit_ok, refusal_ok

E2E = Path(__file__).resolve().parents[1] / "e2e"
JOB = "silver_labels"
GENERIC = ("settings", "branch_missing", "catalog_error", "internal_error")
MISSING = NessieError("not found", status=404, error_code="REFERENCE_NOT_FOUND")
CAUSES: dict[str, tuple[Exception, int, list[str]]] = {
    "a base branch that is not there": (MISSING, 1, ["branch_missing"]),
    "another error of the catalog": (NessieError("conflict", status=409, error_code="REFERENCE_CONFLICT"), 1, ["catalog_error"]),
    "a setting that cannot be used": (SettingsError("x"), 2, ["settings"]),
    "any other error": (RuntimeError("x"), 70, ["internal_error"]),
    "a failed check": (SilverSignalsError("kidney_rule: failed", "kidney_differs: failed"), 1, ["kidney_rule: failed", "kidney_differs: failed"]),
}


def raising(error: Exception) -> Callable[[Any], dict[str, Any]]:
    def execute(arguments: Any) -> dict[str, Any]:
        raise error

    return execute


def real_refusal(error: Exception, mode: str = "build") -> tuple[int, dict[str, Any]]:
    """The exit code and the document of the result file of the real command line when the job raises ``error``."""
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "result.json"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = run([mode, "--branch", "b", "--result", str(path)], raising(error), JOB)
        return code, json.loads(path.read_text())


def runner() -> ModuleType:
    if not (E2E / "silver_labels.py").is_file():
        raise unittest.SkipTest("the untracked runners are not on this machine")
    return importlib.import_module("tests.e2e.silver_labels")


class RealCommandLine(unittest.TestCase):
    def test_the_accept_function_takes_the_refusal_that_the_real_command_line_writes(self) -> None:
        for name, (error, exit_code, wanted) in CAUSES.items():
            with self.subTest(name):
                code, document = real_refusal(error)
                self.assertEqual(exit_code, code)
                self.assertEqual(wanted, document["refusal"])
                self.assertTrue(refusal_ok(code, document, "build", wanted, expected=exit_code))

    def test_the_accept_function_refuses_the_suffixed_spelling_of_a_generic_code(self) -> None:
        for name, (error, exit_code, wanted) in CAUSES.items():
            if wanted[0] not in GENERIC:
                continue
            with self.subTest(name):
                code, document = real_refusal(error)
                self.assertFalse(refusal_ok(code, document, "build", [f"{wanted[0]}: failed"], expected=exit_code))

    def test_a_failed_check_is_written_as_a_code_and_a_flag_and_a_bare_code_does_not_match_it(self) -> None:
        code, document = real_refusal(SilverSignalsError("kidney_rule: failed"), "verify")
        self.assertTrue(refusal_ok(code, document, "verify", ["kidney_rule: failed"]))
        self.assertFalse(refusal_ok(code, document, "verify", ["kidney_rule"]))


class LabelsRunner(unittest.TestCase):
    def strings(self) -> list[str]:
        tree = ast.parse((E2E / "silver_labels.py").read_text())
        return [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]

    def test_the_runner_never_spells_a_generic_code_with_the_flag(self) -> None:
        runner()
        found = sorted({text for text in self.strings() if text in {f"{code}: failed" for code in GENERIC}})
        self.assertEqual([], found)

    def bounded(self, document: dict[str, Any] | None, code: int) -> Record:
        """The bounded-failure case on a stack whose call of a build on a missing base gives ``code`` and ``document``."""
        module = runner()
        record = Record(feature="test", slug="test", rerun="none")
        run_ = module.Run.__new__(module.Run)
        run_.record, run_.trunk = record, "trunk"
        run_.name = lambda base: f"{base}_named"

        def silver(mode: str, branch: str, *, base: str | None = None, env: dict[str, str] | None = None, extra: tuple[str, ...] = ()) -> Any:
            if extra:
                return module.Call(2, None, False, 0.1)
            if env:
                return module.Call(124, None, False, 1.0)
            return module.Call(code, document, document is not None, 0.1)

        run_.stack = SimpleNamespace(silver=silver, spark_containers=lambda: [])
        run_.bounded_failure()
        return record

    def test_the_bounded_failure_case_passes_on_the_refusal_that_the_real_command_line_writes(self) -> None:
        code, document = real_refusal(MISSING)
        record = self.bounded(document, code)
        self.assertEqual(3, len(record.observations))
        self.assertTrue(all(item.passed for item in record.observations), [item.observed for item in record.observations])

    def test_the_bounded_failure_case_fails_on_another_refusal(self) -> None:
        for error in (RuntimeError("x"), SettingsError("x"), NessieError("c", status=409, error_code="REFERENCE_CONFLICT")):
            with self.subTest(error=type(error).__name__):
                code, document = real_refusal(error)
                first = self.bounded(document, code).observations[0]
                self.assertFalse(first.passed)
        suffixed = {"job": JOB, "mode": "build", "refusal": ["branch_missing: failed"]}
        self.assertFalse(self.bounded(suffixed, 1).observations[0].passed)

    def test_a_bad_call_and_a_stopped_job_are_judged_from_an_empty_folder(self) -> None:
        self.assertTrue(empty_exit_ok(2, False, 2))
        self.assertFalse(empty_exit_ok(2, True, 2))


if __name__ == "__main__":
    unittest.main()
