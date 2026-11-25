"""The Spark jobs that read ``JOB_DEADLINE_SECONDS`` and ``NESSIE_TIMEOUT_SECONDS`` refuse an unusable value with ``SettingsError`` and show none of it.

Failure mode: ``float(os.environ.get(...))`` raises ``ValueError("could not convert string to float: '<value>'")`` for text, and takes ``inf``, ``nan``, a
negative number and a figure of a century without a word. The patient split job reads the same two settings with ``number_setting`` (a finite number above 0
and up to a maximum); every job does now, so a bad value is the settings exit (2, or the ``settings`` code in a sealed job's file) and the text of the error
is never shown.

Three of the four jobs are imported here. ``table_checks`` is a module of the Spark job image, which imports ``pyspark`` inside the functions that build a
session, and mypy does not follow ``importlib.import_module``. Its source is read as well (``Sources``): the deadline comes from ``number_setting`` and from
nowhere else, and a ``SettingsError`` ends ``main`` with exit 2. A source check cannot see what a job writes to its standard error, so ``TableChecksRuntime``
runs ``table_checks.main`` and compares the exit code and every byte of standard output and error. ``number_setting`` itself is tested in
``test_patient_split_cli``. The same source check runs on the other three jobs so a ``float(os.environ...)`` that comes back is caught in all four.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import importlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from spark.jobs import bronze_clinical_lab, catalog_proof
from spark.settings import SettingsError
from tests.fast.fake_vitaldb import installed

MARKER = "marker_value_x9"
URI = "http://nessie.invalid:19120/api/v2"
JOBS = Path(__file__).resolve().parents[2] / "spark" / "jobs"
BAD = (MARKER, "inf", "-inf", "nan", "-5", "0", "1e400", "1e9", "")

with installed():
    bronze_decode = importlib.import_module("spark.jobs.bronze_decode")
table_checks: Any = importlib.import_module("spark.jobs.table_checks")


def other_uses(source: str, name: str) -> list[int]:
    """The lines where the text ``name`` appears as a string anywhere but the first argument of a ``number_setting`` call."""
    tree = ast.parse(source)
    allowed = {id(call.args[0]) for call in ast.walk(tree) if isinstance(call, ast.Call) and ast.unparse(call.func) == "number_setting" and call.args}
    return [node.lineno for node in ast.walk(tree) if isinstance(node, ast.Constant) and node.value == name and id(node) not in allowed]


def number_setting_calls(source: str, name: str) -> list[tuple[str, str]]:
    """The default and the maximum, as source text, of each ``number_setting`` call that reads ``name``."""
    found = []
    for call in ast.walk(ast.parse(source)):
        if isinstance(call, ast.Call) and ast.unparse(call.func) == "number_setting" and len(call.args) == 3:
            first, default, maximum = call.args
            if isinstance(first, ast.Constant) and first.value == name:
                found.append((ast.unparse(default), ast.unparse(maximum)))
    return found


def main_returns_2_for_settings_error(source: str) -> bool:
    """Whether ``main`` has an ``except`` that names ``SettingsError`` and returns 2."""
    for function in ast.walk(ast.parse(source)):
        if isinstance(function, ast.FunctionDef) and function.name == "main":
            for handler in (node for node in ast.walk(function) if isinstance(node, ast.ExceptHandler)):
                names = {item.id for item in ast.walk(handler.type) if isinstance(item, ast.Name)} if handler.type else set()
                returns = [node.value.value for node in ast.walk(handler) if isinstance(node, ast.Return) and isinstance(node.value, ast.Constant)]
                if "SettingsError" in names and returns == [2]:
                    return True
    return False


class Sources(unittest.TestCase):
    """The four jobs read their deadline and timeout with ``number_setting``, checked from their source and not by importing them."""

    EXPECTED = (
        ("catalog_proof", "JOB_DEADLINE_SECONDS", ("DEFAULT_JOB_DEADLINE_SECONDS", "MAX_DEADLINE_SECONDS")),
        ("catalog_proof", "NESSIE_TIMEOUT_SECONDS", ("DEFAULT_NESSIE_TIMEOUT_SECONDS", "MAX_NESSIE_TIMEOUT_SECONDS")),
        ("table_checks", "JOB_DEADLINE_SECONDS", ("DEFAULT_JOB_DEADLINE_SECONDS", "MAX_DEADLINE_SECONDS")),
        ("bronze_decode", "JOB_DEADLINE_SECONDS", ("DEFAULT_JOB_DEADLINE_SECONDS", "MAX_DEADLINE_SECONDS")),
        ("bronze_decode", "NESSIE_TIMEOUT_SECONDS", ("DEFAULT_NESSIE_TIMEOUT_SECONDS", "MAX_NESSIE_TIMEOUT_SECONDS")),
        ("bronze_clinical_lab", "JOB_DEADLINE_SECONDS", ("DEFAULT_JOB_DEADLINE_SECONDS", "MAX_DEADLINE_SECONDS")),
        ("bronze_clinical_lab", "NESSIE_TIMEOUT_SECONDS", ("DEFAULT_NESSIE_TIMEOUT_SECONDS", "MAX_NESSIE_TIMEOUT_SECONDS")),
    )

    def test_each_job_reads_each_of_its_two_numbers_once_with_number_setting_and_in_no_other_way(self) -> None:
        for job, name, limits in self.EXPECTED:
            source = (JOBS / f"{job}.py").read_text()
            with self.subTest(job=job, name=name):
                self.assertEqual([limits], number_setting_calls(source, name))
                self.assertEqual([], other_uses(source, name))

    def test_the_check_covers_the_four_jobs_and_both_settings(self) -> None:
        self.assertEqual({"catalog_proof", "table_checks", "bronze_decode", "bronze_clinical_lab"}, {job for job, _, _ in self.EXPECTED})
        self.assertEqual(2, len({name for _, name, _ in self.EXPECTED}))
        self.assertEqual(7, len(self.EXPECTED))

    def test_the_table_checks_job_ends_a_settings_error_with_exit_2(self) -> None:
        self.assertTrue(main_returns_2_for_settings_error((JOBS / "table_checks.py").read_text()))

    def test_the_source_checks_name_a_read_that_does_not_use_number_setting(self) -> None:
        name = "JOB_DEADLINE_SECONDS"
        good = 'x = number_setting("JOB_DEADLINE_SECONDS", DEFAULT_JOB_DEADLINE_SECONDS, MAX_DEADLINE_SECONDS)\n'
        self.assertEqual([("DEFAULT_JOB_DEADLINE_SECONDS", "MAX_DEADLINE_SECONDS")], number_setting_calls(good, name))
        self.assertEqual([], other_uses(good, name))
        for bad in (
            'x = float(os.environ.get("JOB_DEADLINE_SECONDS", "60"))\n',
            'x = float(os.environ["JOB_DEADLINE_SECONDS"])\n',
            'NAME = "JOB_DEADLINE_SECONDS"\nx = number_setting(NAME, D, M)\n',
            good + 'y = os.environ.get("JOB_DEADLINE_SECONDS")\n',
        ):
            with self.subTest(source=bad):
                self.assertNotEqual([], other_uses(bad, name))
        self.assertEqual([], number_setting_calls('x = float(os.environ.get("JOB_DEADLINE_SECONDS"))\n', name))
        self.assertEqual([], number_setting_calls('x = other_reader("JOB_DEADLINE_SECONDS", DEFAULT, MAXIMUM)\n', name))

    def test_the_settings_exit_check_names_a_main_without_it(self) -> None:
        with_exit = "def main():\n    try:\n        pass\n    except (RulesError, SettingsError):\n        return 2\n"
        self.assertTrue(main_returns_2_for_settings_error(with_exit))
        for bad in (
            "def main():\n    try:\n        pass\n    except RulesError:\n        return 2\n",
            "def main():\n    try:\n        pass\n    except SettingsError:\n        return 1\n",
            "def other():\n    try:\n        pass\n    except SettingsError:\n        return 2\n",
            "def main():\n    return 2\n",
        ):
            with self.subTest(source=bad):
                self.assertFalse(main_returns_2_for_settings_error(bad))


class Refusals(unittest.TestCase):
    def main_with(self, module: Any, argv: list[str], **environment: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, {"NESSIE_URI": URI, **environment}),
            mock.patch.object(module, "start_deadline"),
            mock.patch.object(module, "NessieClient", create=True),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = module.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_a_deadline_that_cannot_be_used_is_the_settings_exit_in_every_job(self) -> None:
        jobs = (
            (catalog_proof, ["--branch", "b", "--rows", "1"]),
            (bronze_decode, ["write", "--branch", "b", "--manifest-reference", "m.json"]),
        )
        for module, argv in jobs:
            for value in BAD:
                if value == "":
                    continue
                with self.subTest(job=module.JOB, value=value):
                    code, out, err = self.main_with(module, argv, JOB_DEADLINE_SECONDS=value)
                    self.assertEqual((2, "", f"{module.JOB}: SettingsError\n"), (code, out, err))

    def test_a_nessie_timeout_that_cannot_be_used_is_the_settings_exit_in_every_job(self) -> None:
        for value in BAD:
            if value == "":
                continue
            with self.subTest(job="catalog_proof", value=value):
                code, out, err = self.main_with(catalog_proof, ["--branch", "b", "--rows", "1"], NESSIE_TIMEOUT_SECONDS=value)
                self.assertEqual((2, "", "catalog_proof: SettingsError\n"), (code, out, err))
            with self.subTest(job="bronze_decode", value=value), mock.patch.dict(os.environ, {"NESSIE_URI": URI, "NESSIE_TIMEOUT_SECONDS": value}):
                with self.assertRaises(SettingsError) as caught:
                    bronze_decode.nessie_client()
                self.assertNotIn(MARKER, str(caught.exception))
            with (
                self.subTest(job="bronze_clinical_lab", value=value),
                mock.patch.dict(os.environ, {"NESSIE_URI": URI, "NESSIE_TIMEOUT_SECONDS": value}),
                self.assertRaises(SettingsError),
                mock.patch.object(bronze_clinical_lab, "start_deadline"),
            ):
                bronze_clinical_lab.execute(argparse.Namespace(command="verify", branch="b", source_hash="h"))

    def test_a_deadline_that_cannot_be_used_is_a_settings_refusal_in_the_sealed_bronze_job(self) -> None:
        for value in BAD[:-1]:
            with (
                self.subTest(value=value),
                mock.patch.dict(os.environ, {"JOB_DEADLINE_SECONDS": value}),
                mock.patch.object(bronze_clinical_lab, "start_deadline"),
                self.assertRaises(SettingsError) as caught,
            ):
                bronze_clinical_lab.execute(argparse.Namespace(command="verify", branch="b", source_hash="h"))
            self.assertNotIn(MARKER, str(caught.exception))

    def test_a_deadline_and_a_timeout_that_can_be_used_are_passed_on(self) -> None:
        with (
            mock.patch.dict(os.environ, {"NESSIE_URI": "http://nessie.invalid:19120/api/v2", "JOB_DEADLINE_SECONDS": "120.5", "NESSIE_TIMEOUT_SECONDS": "7"}),
            mock.patch.object(catalog_proof, "start_deadline") as deadline,
            mock.patch.object(catalog_proof, "NessieClient") as client,
            contextlib.redirect_stderr(io.StringIO()),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            client.return_value.config.side_effect = RuntimeError("stop")
            with contextlib.suppress(RuntimeError):
                catalog_proof.main(["--branch", "b", "--rows", "1"])
        deadline.assert_called_once_with(120.5, catalog_proof.JOB)
        self.assertEqual(7.0, client.call_args.args[1])


class TableChecksRuntime(unittest.TestCase):
    """``table_checks.main`` is run here, because the source checks above pass for a job that writes the text of an error to its standard error.

    ``pyspark`` is not needed: a setting or a plan that cannot be used ends ``main`` before a session is built, and ``build_session`` is replaced so that a
    refusal that did not stop in time would fail the test and not start Spark.
    """

    def run_main(self, staging: Path, **environment: str) -> tuple[int, str, str, mock.MagicMock]:
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(table_checks, "build_session") as session,
            mock.patch.object(table_checks, "start_deadline"),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = table_checks.main(["--staging", str(staging)])
        return code, out.getvalue(), err.getvalue(), session

    def staging_with(self, text: str) -> Path:
        folder = tempfile.TemporaryDirectory(prefix="table_checks_runtime_")
        self.addCleanup(folder.cleanup)
        (Path(folder.name) / table_checks.PLAN_FILE).write_text(text)
        return Path(folder.name)

    def test_a_deadline_that_cannot_be_used_is_the_settings_exit_with_one_fixed_line_and_no_session(self) -> None:
        staging = self.staging_with("{}")
        for value in BAD:
            if value == "":
                continue
            with self.subTest(value=value):
                code, out, err, session = self.run_main(staging, JOB_DEADLINE_SECONDS=value)
                self.assertEqual((2, "", "table_checks: SettingsError\n"), (code, out, err))
                session.assert_not_called()

    def test_a_plan_that_cannot_be_used_ends_with_one_fixed_line_that_holds_no_value_of_the_plan(self) -> None:
        for text in (json.dumps({"format": MARKER}), "not json " + MARKER, ""):
            with self.subTest(plan=text[:12]):
                code, out, err, session = self.run_main(self.staging_with(text))
                self.assertEqual((2, ""), (code, out))
                self.assertTrue(err.startswith("table_checks: ") and err.endswith("\n") and err.count("\n") == 1, err)
                self.assertNotIn(MARKER, err)
                session.assert_not_called()

    def test_a_plan_that_cannot_be_read_is_one_fixed_line_with_the_error_type_and_no_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix=f"{MARKER}_") as folder:
            code, out, err, session = self.run_main(Path(folder))
        self.assertEqual((2, "", "table_checks: the plan cannot be read (FileNotFoundError)\n"), (code, out, err))
        session.assert_not_called()

    def test_a_deadline_that_can_be_used_is_passed_on_to_the_deadline_timer(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, {"JOB_DEADLINE_SECONDS": "120.5"}),
            mock.patch.object(table_checks, "start_deadline") as deadline,
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            table_checks.main(["--staging", str(self.staging_with("{}"))])
        deadline.assert_called_once_with(120.5, table_checks.JOB)


if __name__ == "__main__":
    unittest.main()
