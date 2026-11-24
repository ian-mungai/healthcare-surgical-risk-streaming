"""The Spark jobs that read ``JOB_DEADLINE_SECONDS`` and ``NESSIE_TIMEOUT_SECONDS`` refuse an unusable value with ``SettingsError`` and show none of it.

Failure mode: ``float(os.environ.get(...))`` raises ``ValueError("could not convert string to float: '<value>'")`` for text, and takes ``inf``, ``nan``, a
negative number and a figure of a century without a word. The patient split job reads the same two settings with ``number_setting`` (a finite number above 0
and up to a maximum); every job does now, so a bad value is the settings exit (2, or the ``settings`` code in a sealed job's file) and the text of the error
is never shown.

Three of the four jobs are run here. ``table_checks`` imports ``pyspark``, which the development environment does not hold, so mypy fails when a test imports
it. Its source is read instead (``Sources``): the deadline comes from ``number_setting`` and from nowhere else, and a ``SettingsError`` ends ``main`` with
exit 2. ``number_setting`` itself is tested in ``test_patient_split_cli``. The same source check runs on the other three jobs so a ``float(os.environ...)`` that
comes back is caught in all four.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import importlib
import io
import os
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


if __name__ == "__main__":
    unittest.main()
