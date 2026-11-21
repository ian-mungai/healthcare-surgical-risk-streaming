"""The heavy-job rule: a job is heavy when its group's memory in the limits file is above the one set figure, and heavy jobs share one run slot.

The rule is a pure decision over the limits file. The tag it gives a heavy job must be the tag that ``orchestration/dagster.yaml`` limits to one run, so a
rename in one place would silently stop the limiting: the last test holds the two together. Real runs of two heavy jobs are proven by the acceptance test.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from orchestration.surgical_risk.concurrency import (
    HEAVY_MEMORY_MIB,
    HEAVY_TAG_KEY,
    HEAVY_TAG_VALUE,
    ConcurrencyError,
    group_memory_mib,
    heavy_job_tags,
)
from scripts.stack.yaml_text import load_yaml

ROOT = Path(__file__).resolve().parents[2]
DAGSTER_YAML = ROOT / "orchestration" / "dagster.yaml"
LIMITS = ROOT / "compose" / "limits.yml"


def limits_text(group_mib: dict[str, list[int]]) -> str:
    lines = ["host_reserve_mib: 4096", "services:"]
    for group, figures in group_mib.items():
        for number, figure in enumerate(figures):
            lines += [f"  {group}_{number}:", "    runtime: compose", f"    group: {group}", f"    memory_limit_mib: {figure}"]
    return "\n".join(lines) + "\n"


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
        at = self.write(limits_text({"edge": [HEAVY_MEMORY_MIB - 1024, 1024]}))
        self.assertEqual({}, heavy_job_tags("edge", at))
        above = self.write(limits_text({"edge": [HEAVY_MEMORY_MIB - 1024, 1025]}))
        self.assertEqual({HEAVY_TAG_KEY: HEAVY_TAG_VALUE}, heavy_job_tags("edge", above))

    def test_the_owners_figure_is_12544_mib_and_the_boundary_is_at_that_figure(self) -> None:
        self.assertEqual(12544, HEAVY_MEMORY_MIB)
        self.assertEqual({}, heavy_job_tags("edge", self.write(limits_text({"edge": [12544]}))))
        self.assertEqual({HEAVY_TAG_KEY: HEAVY_TAG_VALUE}, heavy_job_tags("edge", self.write(limits_text({"edge": [12545]}))))

    def test_an_unknown_group_is_refused_by_name_and_not_taken_as_light(self) -> None:
        path = self.write(limits_text({"batch": [1024]}))
        with self.assertRaisesRegex(ConcurrencyError, "no_such_group"):
            heavy_job_tags("no_such_group", path)

    def test_a_missing_or_malformed_limits_file_is_refused(self) -> None:
        with self.assertRaisesRegex(ConcurrencyError, "absent.yml"):
            heavy_job_tags("batch", self.folder / "absent.yml")
        with self.assertRaisesRegex(ConcurrencyError, "services"):
            heavy_job_tags("batch", self.write("host_reserve_mib: 4096\n"))
        with self.assertRaisesRegex(ConcurrencyError, "memory_limit_mib"):
            heavy_job_tags("batch", self.write("services:\n  a:\n    group: batch\n    memory_limit_mib: many\n"))

    def test_the_tracked_limits_file_gives_the_supporting_group_as_the_sum_of_its_entries(self) -> None:
        document: Any = load_yaml(LIMITS.read_text())
        expected = sum(entry["memory_limit_mib"] for entry in document["services"].values() if entry["group"] == "supporting")
        self.assertEqual(expected, group_memory_mib("supporting", LIMITS))


class InstanceSettingTest(unittest.TestCase):
    def test_the_instance_limits_the_heavy_tag_to_one_run_at_a_time(self) -> None:
        document: Any = load_yaml(DAGSTER_YAML.read_text())
        limits = document["concurrency"]["runs"]["tag_concurrency_limits"]
        heavy = [entry for entry in limits if entry["key"] == HEAVY_TAG_KEY]
        self.assertEqual([{"key": HEAVY_TAG_KEY, "value": HEAVY_TAG_VALUE, "limit": 1}], heavy)


if __name__ == "__main__":
    unittest.main()
