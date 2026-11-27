"""The three numbers that Terraform takes from the limits file to calculate the heavy-job figure.

The limits loader is the one reader of the file, so a malformed file is refused here and never reaches Terraform. The numbers are the host's reserve, the sum
of the limits of the always-on groups and the largest limit of the Spark job services (one job runs at a time, each under its own layer's identity), and each
is a positive whole number.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
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
  spark_bronze:
    runtime: compose
    group: batch_bronze
    memory_limit_mib: 3000
  spark_silver:
    runtime: compose
    group: batch_silver
    memory_limit_mib: 2500
  spark_gold:
    runtime: compose
    group: batch_gold
    memory_limit_mib: 2000
  spark_maintenance:
    runtime: compose
    group: batch_maintenance
    memory_limit_mib: 1000
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

    def test_a_limits_file_without_one_of_the_spark_job_services_is_refused_and_names_it(self) -> None:
        for name in ("spark_bronze", "spark_silver", "spark_gold", "spark_maintenance"):
            with self.subTest(name=name), self.assertRaisesRegex(LimitsError, name):
                heavy_inputs_document(parse_limits(GOOD.replace(f"  {name}:", f"  {name}_renamed:")))

    def test_the_largest_spark_limit_is_the_figure_whichever_service_holds_it(self) -> None:
        for name, large in (("spark_silver", 3600), ("spark_gold", 3700), ("spark_maintenance", 3800)):
            with self.subTest(name=name):
                text = re.sub(rf"(  {name}:\n(?:.*\n){{2}}    memory_limit_mib: )\d+", rf"\g<1>{large}", GOOD)
                self.assertEqual(large, heavy_inputs_document(parse_limits(text))["spark_limit_mib"])

    def test_the_tracked_file_gives_the_sums_read_again_from_the_yaml(self) -> None:
        document: Any = load_yaml(LIMITS_FILE.read_text())
        services = document["services"]
        expected = {
            "host_reserve_mib": document["host_reserve_mib"],
            "always_on_limits_mib": sum(entry["memory_limit_mib"] for entry in services.values() if entry["group"] == "supporting"),
            "spark_limit_mib": max(services[name]["memory_limit_mib"] for name in ("spark_bronze", "spark_silver", "spark_gold", "spark_maintenance")),
        }
        self.assertEqual(expected, heavy_inputs_document(load_limits(LIMITS_FILE)))
        self.assertTrue(all(isinstance(value, int) and value > 0 for value in expected.values()))


class ReviewPromptTest(unittest.TestCase):
    """The heavy figure of the tracked limits on the 32768 MiB host is a figure that the owner chose. A limit that changes it must be reviewed, not drift."""

    def test_the_tracked_limits_leave_9984_mib_on_the_32768_mib_host(self) -> None:
        inputs = heavy_inputs_document(load_limits(LIMITS_FILE))
        figure = 32768 - inputs["host_reserve_mib"] - inputs["always_on_limits_mib"] - inputs["spark_limit_mib"]
        self.assertEqual(
            9984,
            figure,
            f"the heavy figure of compose/limits.yml is now {figure} MiB, not the 9984 MiB. Review the heavy-job rule in the "
            "project standard (Service groups and memory) before you change this number: a job above the figure runs alone.",
        )


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
