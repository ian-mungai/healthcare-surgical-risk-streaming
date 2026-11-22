"""The heavy-job rule: a job is heavy when its group's memory in the limits file is above the one set figure, and heavy jobs share one run slot.

The rule is a pure decision over the limits file and the figure in the environment, which Terraform calculates and the deploy writes: no figure is typed in
the code. The tag it gives a heavy job must be the tag that ``orchestration/dagster.yaml`` limits to one run, so a
rename in one place would silently stop the limiting: the last test holds the two together. Real runs of two heavy jobs are proven by the acceptance test.
"""

from __future__ import annotations

import ast
import os
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration.surgical_risk.concurrency import (
    HEAVY_MEMORY_ENVIRONMENT,
    HEAVY_TAG_KEY,
    HEAVY_TAG_VALUE,
    JOB_GROUPS,
    ConcurrencyError,
    group_memory_mib,
    heavy_job_tags,
    heavy_memory_mib,
    job_tags,
    unknown_groups,
)
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
DAGSTER_YAML = ROOT / "orchestration" / "dagster.yaml"
LIMITS = ROOT / "compose" / "limits.yml"
CODE_LOCATION = ROOT / "compose" / "services" / "dagster_code_location.yml"
DEFINITIONS = ROOT / "orchestration" / "surgical_risk" / "definitions.py"
CODE_FOLDER = DEFINITIONS.parent
# A figure of these tests only. The real one is calculated by Terraform from the host's memory and the limits file.
FIGURE = 10000
ENVIRONMENT = {HEAVY_MEMORY_ENVIRONMENT: str(FIGURE)}


def limits_text(group_mib: dict[str, list[int]]) -> str:
    lines = ["host_reserve_mib: 4096", "services:"]
    for group, figures in group_mib.items():
        for number, figure in enumerate(figures):
            lines += [f"  {group}_{number}:", "    runtime: compose", f"    group: {group}", f"    memory_limit_mib: {figure}"]
    return "\n".join(lines) + "\n"


class DeclaredJobGroupsTest(unittest.TestCase):
    """Every job that runs a service group names it in ``JOB_GROUPS``, and each group named there exists in the limits file, so a job's heavy tag can
    always be judged when the code location starts. The list is empty until the first group job; the planted entries prove the rule holds for one."""

    def test_every_group_that_the_declared_jobs_name_is_in_the_limits_file(self) -> None:
        self.assertEqual([], unknown_groups(JOB_GROUPS, LIMITS))

    def test_a_planted_job_of_a_real_group_passes_and_one_of_an_unknown_group_is_named(self) -> None:
        self.assertEqual([], unknown_groups({"bronze": "batch"}, LIMITS))
        self.assertEqual(["bronze: no_such_group"], unknown_groups({"bronze": "no_such_group", "silver": "batch"}, LIMITS))

    def test_a_declared_job_gets_the_tags_of_its_group_and_an_undeclared_job_is_refused(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concurrency_jobs_") as scratch:
            limits = Path(scratch) / "limits.yml"
            limits.write_text(limits_text({"heavy_group": [FIGURE + 1], "light_group": [1024]}))
            jobs = {"big": "heavy_group", "small": "light_group"}
            self.assertEqual({HEAVY_TAG_KEY: HEAVY_TAG_VALUE}, job_tags("big", limits, jobs, ENVIRONMENT))
            self.assertEqual({}, job_tags("small", limits, jobs, ENVIRONMENT))
            with self.assertRaises(ConcurrencyError):
                job_tags("undeclared", limits, jobs, ENVIRONMENT)


class GroupMemoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="concurrency_test_")))

    def write(self, text: str) -> Path:
        path = self.folder / "limits.yml"
        path.write_text(text)
        return path

    def test_a_group_needs_the_sum_of_its_services_and_no_other_group(self) -> None:
        path = self.write(limits_text({"batch": [2048, 3072], "other": [9999]}))
        self.assertEqual(5120, group_memory_mib("batch", path))

    def test_a_job_at_the_figure_is_light_and_one_mebibyte_above_it_is_heavy(self) -> None:
        at = self.write(limits_text({"edge": [FIGURE - 1024, 1024]}))
        self.assertEqual({}, heavy_job_tags("edge", at, ENVIRONMENT))
        above = self.write(limits_text({"edge": [FIGURE - 1024, 1025]}))
        self.assertEqual({HEAVY_TAG_KEY: HEAVY_TAG_VALUE}, heavy_job_tags("edge", above, ENVIRONMENT))

    def test_the_boundary_follows_the_figure_in_the_environment(self) -> None:
        path = self.write(limits_text({"edge": [FIGURE]}))
        self.assertEqual({}, heavy_job_tags("edge", path, {HEAVY_MEMORY_ENVIRONMENT: str(FIGURE)}))
        self.assertEqual({HEAVY_TAG_KEY: HEAVY_TAG_VALUE}, heavy_job_tags("edge", path, {HEAVY_MEMORY_ENVIRONMENT: str(FIGURE - 1)}))

    def test_an_unknown_group_is_refused_by_name_and_not_taken_as_light(self) -> None:
        path = self.write(limits_text({"batch": [1024]}))
        with self.assertRaisesRegex(ConcurrencyError, "no_such_group"):
            heavy_job_tags("no_such_group", path, ENVIRONMENT)

    def test_a_missing_or_malformed_limits_file_is_refused(self) -> None:
        with self.assertRaisesRegex(ConcurrencyError, "absent.yml"):
            heavy_job_tags("batch", self.folder / "absent.yml", ENVIRONMENT)
        with self.assertRaisesRegex(ConcurrencyError, "services"):
            heavy_job_tags("batch", self.write("host_reserve_mib: 4096\n"), ENVIRONMENT)
        with self.assertRaisesRegex(ConcurrencyError, "memory_limit_mib"):
            heavy_job_tags("batch", self.write("services:\n  a:\n    group: batch\n    memory_limit_mib: many\n"), ENVIRONMENT)

    def test_the_tracked_limits_file_gives_the_supporting_group_as_the_sum_of_its_entries(self) -> None:
        document: Any = load_yaml(LIMITS.read_text())
        expected = sum(entry["memory_limit_mib"] for entry in document["services"].values() if entry["group"] == "supporting")
        self.assertEqual(expected, group_memory_mib("supporting", LIMITS))


class HeavyFigureTest(unittest.TestCase):
    def test_the_figure_is_the_whole_number_in_the_environment(self) -> None:
        self.assertEqual(11008, heavy_memory_mib({HEAVY_MEMORY_ENVIRONMENT: "11008"}))
        self.assertEqual(1, heavy_memory_mib({HEAVY_MEMORY_ENVIRONMENT: "1"}))

    def test_a_missing_figure_is_refused_by_name_and_has_no_default(self) -> None:
        with self.assertRaisesRegex(ConcurrencyError, f"{HEAVY_MEMORY_ENVIRONMENT} is not set"):
            heavy_memory_mib({})

    def test_an_empty_figure_is_refused_as_empty(self) -> None:
        with self.assertRaisesRegex(ConcurrencyError, f"{HEAVY_MEMORY_ENVIRONMENT} is empty"):
            heavy_memory_mib({HEAVY_MEMORY_ENVIRONMENT: ""})

    def test_a_figure_that_is_not_a_positive_whole_number_is_refused_with_the_value_named(self) -> None:
        for value in ("0", "-1", "-11008", "11008.0", "1.5", "abc", "11008m", " 11008", "11008 ", "1_000", "0x2b00", "+5", "١٠٠٠"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ConcurrencyError, f"{HEAVY_MEMORY_ENVIRONMENT}.*positive whole number.*" + re.escape(repr(value))),
            ):
                heavy_memory_mib({HEAVY_MEMORY_ENVIRONMENT: value})

    def test_the_live_environment_is_read_when_none_is_given(self) -> None:
        with mock.patch.dict(os.environ, {HEAVY_MEMORY_ENVIRONMENT: "4321"}):
            self.assertEqual(4321, heavy_memory_mib())
        with mock.patch.dict(os.environ, clear=True), self.assertRaisesRegex(ConcurrencyError, "is not set"):
            heavy_memory_mib()

    def test_a_job_is_not_judged_without_the_figure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concurrency_figure_") as scratch:
            limits = Path(scratch) / "limits.yml"
            limits.write_text(limits_text({"batch": [1024]}))
            with self.assertRaisesRegex(ConcurrencyError, "is not set"):
                heavy_job_tags("batch", limits, {})
            with self.assertRaisesRegex(ConcurrencyError, "positive whole number"):
                job_tags("bronze", limits, {"bronze": "batch"}, {HEAVY_MEMORY_ENVIRONMENT: "0"})

    def test_the_code_location_loads_the_figure_from_the_heavy_file_that_the_deploy_renders_and_fails_without_it(self) -> None:
        document: Any = load_yaml(CODE_LOCATION.read_text())
        service = document["services"]["dagster_code_location"]
        self.assertEqual([{"path": "../host.env", "required": False}, {"path": "../heavy.env", "required": True}], service["env_file"])

    def test_no_shell_variable_reaches_the_figure_because_the_service_file_interpolates_none(self) -> None:
        """Compose reads ``${NAME}`` in a file from the shell first, and a value there wins over a file. The figure arrives through ``env_file`` instead, which
        no shell variable overrides, so neither the service's environment nor any other line of its file names the variable."""
        text = CODE_LOCATION.read_text()
        document: Any = load_yaml(text)
        self.assertNotIn(HEAVY_MEMORY_ENVIRONMENT, document["services"]["dagster_code_location"].get("environment", {}))
        self.assertNotIn("${" + HEAVY_MEMORY_ENVIRONMENT, text)
        self.assertNotIn("$" + HEAVY_MEMORY_ENVIRONMENT, text)

    def test_the_definitions_check_the_figure_when_the_code_location_loads(self) -> None:
        """The figure is read at import, so a missing or invalid one fails the load with its reason and not the first launch. The definitions import Dagster,
        which the fast tests do not have, so the test reads the module's own statements."""
        tree = ast.parse(DEFINITIONS.read_text())
        calls = [
            node.value.func.id
            for node in tree.body
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
        ]
        self.assertIn("heavy_memory_mib", calls)
        imported = [
            alias.name for node in tree.body if isinstance(node, ast.ImportFrom) and node.module == "concurrency" and node.level == 1 for alias in node.names
        ]
        self.assertIn("heavy_memory_mib", imported)


class GroupJobTagsTest(unittest.TestCase):
    """Every job that starts a service group has its heavy tag when it is defined. A job with a fixed group takes its tag from that group through ``job_tags``
    and ``JOB_GROUPS``. The probe job takes its group at launch, so its weight cannot be judged when it is defined: it carries the heavy tag unconditionally and
    is held like a heavy job. The definitions import Dagster, which the fast tests do not have, so these tests read the modules' own statements."""

    PROBE = "group_probe_job"

    def group_jobs(self) -> dict[str, ast.Call]:
        """Each ``@job(...)`` function of the code location that names the group resource in its decorator, by function name, with its decorator call."""
        found: dict[str, ast.Call] = {}
        for path in sorted(CODE_FOLDER.glob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.FunctionDef):
                    continue
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call) and getattr(decorator.func, "id", "") == "job" and "GroupResource" in ast.unparse(decorator):
                        found[node.name] = decorator
        return found

    def tags_of(self, decorator: ast.Call) -> ast.expr | None:
        return next((keyword.value for keyword in decorator.keywords if keyword.arg == "tags"), None)

    def test_the_probe_job_is_a_group_job_and_the_scan_sees_it(self) -> None:
        self.assertIn(self.PROBE, self.group_jobs())

    def test_the_probe_job_carries_the_heavy_tag_unconditionally(self) -> None:
        tags = self.tags_of(self.group_jobs()[self.PROBE])
        self.assertIsNotNone(tags)
        self.assertEqual("{HEAVY_TAG_KEY: HEAVY_TAG_VALUE}", ast.unparse(tags) if tags is not None else "")

    def test_every_other_group_job_takes_its_tag_from_its_fixed_group_through_job_tags(self) -> None:
        for name, decorator in self.group_jobs().items():
            if name == self.PROBE:
                continue
            tags = self.tags_of(decorator)
            with self.subTest(job=name):
                self.assertIsInstance(tags, ast.Call)
                call = tags if isinstance(tags, ast.Call) else ast.Call(ast.Name("missing"), [ast.Constant("")], [])
                self.assertEqual("job_tags", getattr(call.func, "id", ""))
                self.assertIn(ast.unparse(call.args[0]).strip("'\""), JOB_GROUPS)

    def test_the_scan_finds_a_job_that_starts_a_group_with_no_tag(self) -> None:
        decorator = ast.parse('@job(resource_defs={"group": GroupResource.configure_at_launch()})\ndef untagged(): ...').body[0].decorator_list[0]  # type: ignore[attr-defined]
        self.assertIsInstance(decorator, ast.Call)
        self.assertIsNone(self.tags_of(decorator))


class InstanceSettingTest(unittest.TestCase):
    """Dagster lets the tags of a launch override the tags of a job, so a limit written for the value ``true`` is escaped by a launch that sets the value to
    ``false``. The limit names the key alone: "if no value is set, the limit is applied across all values of that key" (the ``tag_concurrency_limits``
    description in Dagster 1.13.31, ``_core/instance/config.py`` and ``_core/run_coordinator/queued_run_coordinator.py``, read Nov 22 2026)."""

    def heavy_limits(self) -> list[dict[str, Any]]:
        document: Any = load_yaml(DAGSTER_YAML.read_text())
        return [entry for entry in document["concurrency"]["runs"]["tag_concurrency_limits"] if entry["key"] == HEAVY_TAG_KEY]

    def test_the_instance_limits_every_run_that_carries_the_heavy_key_to_one_at_a_time_whatever_its_value(self) -> None:
        self.assertEqual([{"key": HEAVY_TAG_KEY, "limit": 1}], self.heavy_limits())

    def test_no_value_is_added_to_the_limit_of_the_heavy_key(self) -> None:
        for entry in self.heavy_limits():
            self.assertNotIn("value", entry)

    def test_a_light_group_gets_no_heavy_key_at_all(self) -> None:
        with tempfile.TemporaryDirectory(prefix="concurrency_light_") as scratch:
            limits = Path(scratch) / "limits.yml"
            limits.write_text(limits_text({"light_group": [1024]}))
            self.assertNotIn(HEAVY_TAG_KEY, heavy_job_tags("light_group", limits, ENVIRONMENT))


if __name__ == "__main__":
    unittest.main()
