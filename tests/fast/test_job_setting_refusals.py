"""The Spark jobs that read ``JOB_DEADLINE_SECONDS`` and ``NESSIE_TIMEOUT_SECONDS`` refuse an unusable value with ``SettingsError`` and show none of it.

Failure mode: ``float(os.environ.get(...))`` raises ``ValueError("could not convert string to float: '<value>'")`` for text, and takes ``inf``, ``nan``, a
negative number and a figure of a century without a word. The patient split job reads the same two settings with ``number_setting`` (a finite number above 0
and up to a maximum); every job does now, so a bad value is the settings exit (2, or the ``settings`` code in a sealed job's file) and the text of the error
is never shown.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import io
import os
import tempfile
import unittest
from typing import Any
from unittest import mock

from spark.jobs import bronze_clinical_lab, catalog_proof, table_checks
from spark.settings import SettingsError
from tests.fast.fake_vitaldb import installed

MARKER = "marker_value_x9"
URI = "http://nessie.invalid:19120/api/v2"
BAD = (MARKER, "inf", "-inf", "nan", "-5", "0", "1e400", "1e9", "")

with installed():
    bronze_decode = importlib.import_module("spark.jobs.bronze_decode")


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
        folder = self.enterContext(tempfile.TemporaryDirectory(prefix="job_settings_"))
        jobs = (
            (catalog_proof, ["--branch", "b", "--rows", "1"]),
            (bronze_decode, ["write", "--branch", "b", "--manifest-reference", "m.json"]),
            (table_checks, ["--staging", folder]),
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
