"""The three numbers that Terraform takes from the limits file to calculate the heavy-job figure.

The limits loader is the one reader of the file, so a malformed file is refused here and never reaches Terraform. The numbers are the host's reserve, the sum
of the limits of the always-on groups and the Spark job's limit, and each is a positive whole number.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.stack import heavy_inputs
from scripts.stack.heavy_inputs import heavy_inputs_document, heavy_inputs_text, main
from scripts.stack.limits import LIMITS_FILE, LimitsError, load_limits, parse_limits
from scripts.stack.yaml_text import load_yaml

GOOD = """\
host_reserve_mib: 5000
services:
  postgres:
    runtime: compose
    group: supporting
    memory_limit_mib: 1000
  nessie:
    runtime: compose
    group: supporting
    memory_limit_mib: 2000
  spark:
    runtime: compose
    group: batch
    memory_limit_mib: 3000
  airbyte_server:
    runtime: k3s
    group: ingestion
    memory_limit_mib: 7000
"""


class DocumentTest(unittest.TestCase):
    def test_the_three_numbers_come_from_the_reserve_the_always_on_group_and_the_spark_service(self) -> None:
        self.assertEqual({"host_reserve_mib": 5000, "always_on_limits_mib": 3000, "spark_limit_mib": 3000}, heavy_inputs_document(parse_limits(GOOD)))

    def test_every_always_on_group_is_summed(self) -> None:
        text = GOOD.replace("group: ingestion", "group: other")
        with mock.patch.object(heavy_inputs, "ALWAYS_ON_GROUPS", frozenset({"supporting", "other"})):
            self.assertEqual(10000, heavy_inputs_document(parse_limits(text))["always_on_limits_mib"])

    def test_the_text_is_json_with_exactly_the_three_keys(self) -> None:
        self.assertEqual(["always_on_limits_mib", "host_reserve_mib", "spark_limit_mib"], sorted(json.loads(heavy_inputs_text(parse_limits(GOOD)))))

    def test_a_limits_file_with_no_always_on_service_is_refused(self) -> None:
        with self.assertRaisesRegex(LimitsError, "always-on"):
            heavy_inputs_document(parse_limits(GOOD.replace("group: supporting", "group: renamed")))

    def test_a_limits_file_with_no_spark_service_is_refused(self) -> None:
        with self.assertRaisesRegex(LimitsError, "spark"):
            heavy_inputs_document(parse_limits(GOOD.replace("  spark:", "  spark_renamed:")))

    def test_the_tracked_file_gives_the_sums_read_again_from_the_yaml(self) -> None:
        document: Any = load_yaml(LIMITS_FILE.read_text())
        services = document["services"]
        expected = {
            "host_reserve_mib": document["host_reserve_mib"],
            "always_on_limits_mib": sum(entry["memory_limit_mib"] for entry in services.values() if entry["group"] == "supporting"),
            "spark_limit_mib": services["spark"]["memory_limit_mib"],
        }
        self.assertEqual(expected, heavy_inputs_document(load_limits(LIMITS_FILE)))
        self.assertTrue(all(isinstance(value, int) and value > 0 for value in expected.values()))


class CommandTest(unittest.TestCase):
    def run_main(self, text: str | None) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "limits.yml"
            if text is not None:
                path.write_text(text)
            out, err = io.StringIO(), io.StringIO()
            with mock.patch.object(heavy_inputs, "LIMITS_FILE", path), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = main([])
        return code, out.getvalue(), err.getvalue()

    def test_the_command_prints_the_json_and_nothing_else(self) -> None:
        code, out, err = self.run_main(GOOD)
        self.assertEqual((0, ""), (code, err))
        self.assertEqual({"host_reserve_mib": 5000, "always_on_limits_mib": 3000, "spark_limit_mib": 3000}, json.loads(out))

    def test_a_file_the_loader_refuses_prints_no_json_and_names_the_reason(self) -> None:
        for text in (
            GOOD.replace("host_reserve_mib: 5000", "host_reserve_mib: 4096.5"),
            GOOD.replace("memory_limit_mib: 3000", "memory_limit_mib: 0"),
            "services: {}\n",
            None,
        ):
            with self.subTest(text=text):
                code, out, err = self.run_main(text)
                self.assertEqual((1, ""), (code, out))
                self.assertIn("limits:", err)


if __name__ == "__main__":
    unittest.main()
