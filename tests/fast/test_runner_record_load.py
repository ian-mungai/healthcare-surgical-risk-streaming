"""The record load runner removes only the container that it started, and its record assertions compare an observed value with the exact value.

Failure modes covered here:

- The container name is taken by another container: ``docker run`` fails and the cleanup force-removes that other container by its name.
- The run removes its container by name and not by the ID that ``docker run`` gave it.
- A start that fails after the container was made (the port is taken) leaves the container, because no ID was kept.
- A removal that fails is not a failed record.
- The load branch holds a raw table, or main does, and the record still passes (the branch was not compared with its exact value).
- A branch of another owner is moved, and the record passes because the assertion only checks that a branch of that name exists.

The runner is untracked, so the tests run only where it is on disk. A stand-in ``docker`` program on the path answers the commands; nothing starts.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from tests.e2e import run_record

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "tests" / "e2e" / "record_load.py"
IDENTITY = "c0ffee" * 10 + "abcd"
STAND_IN = """#!/bin/sh
printf '%s\\n' "$*" >> "$DOCKER_LOG"
case "$1" in
run)
  if [ -n "$COLLIDE" ]; then
    echo "docker: Error response from daemon: Conflict. The container name is already in use" >&2
    exit 125
  fi
  while [ $# -gt 0 ]; do
    if [ "$1" = "--cidfile" ] && [ -z "$NO_CIDFILE" ]; then printf '%s' "$IDENTITY" > "$2"; fi
    shift
  done
  echo "$IDENTITY"
  exit "${RUN_EXIT:-0}"
  ;;
rm)
  exit "${RM_EXIT:-0}"
  ;;
esac
exit 0
"""


def passing_cases(record: Any, log: list[str], folder: Path) -> None:
    record.expect("stand-in", "the cases ran", "yes", "yes", True)


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class ContainerCleanup(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.record_load")
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name).resolve()
        bin_folder = self.scratch / "bin"
        bin_folder.mkdir()
        program = bin_folder / "docker"
        program.write_text(STAND_IN)
        program.chmod(program.stat().st_mode | stat.S_IXUSR)
        self.log = self.scratch / "docker.log"
        self.log.write_text("")
        self.runs = self.scratch / "root"
        self.runs.mkdir()
        self.environment = {"PATH": f"{bin_folder}{os.pathsep}{os.environ['PATH']}", "DOCKER_LOG": str(self.log), "IDENTITY": IDENTITY}
        for patcher in (
            mock.patch.object(run_record, "ROOT", self.runs),
            mock.patch.object(self.module, "recorded_state", return_value=("test", "0" * 64)),
            mock.patch.object(self.module, "wait_until_ready", return_value=None),
            mock.patch.object(self.module, "run_cases", side_effect=passing_cases),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_main(self, **extra: str) -> tuple[int, dict[str, Any]]:
        with mock.patch.dict(os.environ, self.environment | extra), contextlib.redirect_stdout(io.StringIO()):
            code = self.module.main()
        records = sorted((self.runs / "runs").glob("*/record.json"))
        self.assertEqual(1, len(records))
        return code, json.loads(records[0].read_text())

    def commands(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if line]

    def removals(self) -> list[str]:
        return [line for line in self.commands() if line.startswith("rm")]

    def test_a_name_that_is_taken_removes_nothing(self) -> None:
        code, record = self.run_main(COLLIDE="1")
        self.assertEqual([], self.removals(), "the container of another owner was removed")
        self.assertEqual(1, code)
        self.assertTrue(record["outcome"].startswith("blocked: the Nessie container did not start"))
        self.assertIn("nothing started", record["cleanup"])

    def test_the_container_is_removed_by_the_id_that_docker_gave(self) -> None:
        code, record = self.run_main()
        self.assertEqual([f"rm -f {IDENTITY}"], self.removals())
        self.assertEqual(0, code)
        self.assertEqual("pass", record["outcome"])
        self.assertNotIn(self.module.PROJECT, " ".join(self.removals()))
        self.assertIn(IDENTITY[:12], record["cleanup"])

    def test_a_start_that_fails_after_the_container_was_made_removes_that_container(self) -> None:
        code, record = self.run_main(RUN_EXIT="1")
        self.assertEqual([f"rm -f {IDENTITY}"], self.removals())
        self.assertEqual(1, code)
        self.assertTrue(record["outcome"].startswith("blocked"))

    def test_a_start_that_gave_no_id_removes_nothing(self) -> None:
        code, record = self.run_main(NO_CIDFILE="1")
        self.assertEqual([], self.removals())
        self.assertEqual(1, code)
        self.assertTrue(record["outcome"].startswith("blocked"))

    def test_a_failed_removal_fails_the_record(self) -> None:
        code, record = self.run_main(RM_EXIT="1")
        self.assertEqual(1, code)
        self.assertEqual("fail", record["outcome"])
        self.assertTrue(any(item["case"] == "cleanup" and not item["passed"] for item in record["observations"]))
        self.assertNotEqual("pending", record["cleanup"])

    def test_the_record_is_closed_after_the_cleanup(self) -> None:
        _, record = self.run_main()
        self.assertNotEqual("pending", record["cleanup"])
        self.assertIn(IDENTITY[:12], record["cleanup"])


class FakeCatalog:
    def __init__(self, branches: dict[str, dict[str, int] | None], heads: dict[str, str] | None = None) -> None:
        self.branches = branches
        self.heads = heads or {}

    def reference(self, name: str) -> SimpleNamespace | None:
        return SimpleNamespace(hash=self.heads.get(name, "h" * 64)) if self.branches.get(name) is not None else None

    def snapshot_ids(self, name: str) -> dict[str, int]:
        found = self.branches.get(name)
        if found is None:
            raise KeyError(name)
        return found


@unittest.skipUnless(RUNNER.is_file(), "the temporary acceptance runner is not in this checkout")
class RecordAssertions(unittest.TestCase):
    def setUp(self) -> None:
        self.module: ModuleType = importlib.import_module("tests.e2e.record_load")

    def record(self) -> run_record.Record:
        return run_record.Record(feature="stand-in", slug="stand_in", rerun="none")

    def test_a_failed_load_that_left_no_table_passes(self) -> None:
        record = self.record()
        self.module.expect_no_raw_table(record, "case", FakeCatalog({"load_x": {}, "main": {}}), "load_x")
        self.assertEqual([True, True], [item.passed for item in record.observations])
        self.assertEqual(["{}", "{}"], [item.observed for item in record.observations])

    def test_an_absent_branch_holds_no_table(self) -> None:
        record = self.record()
        self.module.expect_no_raw_table(record, "case", FakeCatalog({"main": {}}), "load_x")
        self.assertTrue(all(item.passed for item in record.observations))

    def test_a_table_on_the_load_branch_fails(self) -> None:
        record = self.record()
        self.module.expect_no_raw_table(record, "case", FakeCatalog({"load_x": {"clinical_data": 7001}, "main": {}}), "load_x")
        self.assertEqual([False, True], [item.passed for item in record.observations])
        self.assertIn("7001", record.observations[0].observed)

    def test_a_table_on_main_fails(self) -> None:
        record = self.record()
        self.module.expect_no_raw_table(record, "case", FakeCatalog({"load_x": {}, "main": {"clinical_data": 7001}}), "load_x")
        self.assertEqual([True, False], [item.passed for item in record.observations])

    def test_a_branch_at_its_recorded_head_passes(self) -> None:
        record = self.record()
        self.module.expect_branch_head(record, "case", FakeCatalog({"other": {}}, {"other": "a" * 64}), "other", "a" * 64)
        self.assertTrue(record.observations[0].passed)

    def test_a_branch_at_another_head_fails(self) -> None:
        record = self.record()
        self.module.expect_branch_head(record, "case", FakeCatalog({"other": {}}, {"other": "b" * 64}), "other", "a" * 64)
        self.assertFalse(record.observations[0].passed)
        self.assertEqual(("a" * 64, "b" * 64), (record.observations[0].expected, record.observations[0].observed))

    def test_a_missing_branch_fails(self) -> None:
        record = self.record()
        self.module.expect_branch_head(record, "case", FakeCatalog({}), "other", "a" * 64)
        self.assertFalse(record.observations[0].passed)

    def test_exact_compares_the_two_values_it_shows(self) -> None:
        record = self.record()
        self.assertTrue(self.module.expect_exact(record, "case", "step", {"a": 1}, {"a": 1}))
        self.assertFalse(self.module.expect_exact(record, "case", "step", {"a": 1}, {"a": 2}))
        self.assertEqual([True, False], [item.passed for item in record.observations])


if __name__ == "__main__":
    unittest.main()
