"""The end-to-end runners that start a Spark job name its service, profile, credential folder and memory limit through ``scripts/stack/job_workloads.py``.

The runners are not tracked (``tests/e2e/*`` is ignored but for four files), so each check skips when the runners are absent. The one ``spark`` service, the
``batch`` profile, ``SPARK_ACCESS_DIR`` and ``MEM_LIMIT_SPARK`` are gone: a runner that still names one would start no job, or start it under no identity. The
scan reads every runner for those names, and the launch helper of the runners (``tests/e2e/spark_launch.py``) is held to what the services and the launcher
do. The planted cases put each stale name into a clean text and expect the scan to name it.
"""

from __future__ import annotations

import importlib
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = sorted((ROOT / "tests" / "e2e").glob("*.py")) + sorted((ROOT / "tests" / "e2e").glob("*/*.py"))
# A runner whose own Compose file (a stream stack, a replay stack) has its own ``batch`` service is not about the Spark services.
OWN_COMPOSE = frozenset({"stream_features.py", "runner_checks.py", "batch_features.py", "batch_oracle.py"})
STALE = {
    "the old access setting": re.compile(r"SPARK_ACCESS_DIR"),
    "the old memory limit": re.compile(r"MEM_LIMIT_SPARK(?!_)"),
    "the old service": re.compile(r"""["']spark["']"""),
    "the old profile": re.compile(r"""["']batch["']"""),
    "the old service file": re.compile(r"spark\.yml"),
    "the old workload role": re.compile(r"workload_spark"),
    "the old container filter": re.compile(r"com\.docker\.compose\.service=spark(?!_)"),
}
# The only line that names the ``spark`` folder as a string: a path, not a service.
PATHS = re.compile(r"""ROOT / ["']spark["']|ROOT / 'spark'|\{ROOT / 'spark'\}""")


def stale_names(text: str) -> list[str]:
    """Each stale name in ``text``, as ``what: line``, ignoring the ``spark`` folder as a path."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if PATHS.search(line):
            continue
        found += [f"{what}: line {number}" for what, pattern in STALE.items() if pattern.search(line)]
    return found


@unittest.skipUnless(RUNNERS, "the untracked runners are not here")
class RunnerScanTest(unittest.TestCase):
    def test_no_runner_names_a_removed_service_profile_or_setting(self) -> None:
        found = {path.relative_to(ROOT).as_posix(): stale_names(path.read_text()) for path in RUNNERS if path.name not in OWN_COMPOSE}
        self.assertEqual({}, {name: hits for name, hits in found.items() if hits})

    def test_the_scan_names_each_stale_name_when_one_is_planted(self) -> None:
        planted = {
            "the old access setting": 'f"SPARK_ACCESS_DIR={access}"',
            "the old memory limit": 're.sub(r"MEM_LIMIT_SPARK=\\d+m", "x", text)',
            "the old service": 'words = ["docker", "compose", "run", "spark", "x"]',
            "the old profile": 'command = ["--profile", "batch", "run"]',
            "the old service file": 'path = compose / "services" / "spark.yml"',
            "the old workload role": '"workload_spark" in identity',
            "the old container filter": '"label=com.docker.compose.service=spark"',
        }
        for what, text in planted.items():
            with self.subTest(what=what):
                self.assertEqual([f"{what}: line 1"], stale_names(text))
        self.assertEqual([], stale_names('build = ["--file", str(ROOT / "spark" / "Dockerfile")]\nservice = "spark_silver"\nprofile = "batch_silver"'))


@unittest.skipUnless((ROOT / "tests" / "e2e" / "spark_launch.py").is_file(), "the untracked runner helper is not here")
class LaunchHelperTest(unittest.TestCase):
    def setUp(self) -> None:
        self.launch: Any = importlib.import_module("tests.e2e.spark_launch")

    def test_a_launch_keeps_the_python_entry_point_and_names_the_service_of_the_job_that_its_arguments_run(self) -> None:
        job = ["-m", "spark.jobs.silver_cases", "build", "--branch", "b"]
        self.assertEqual(["--entrypoint", "python"], self.launch.launch_options(job, "gold_dimensions"))
        self.assertEqual(["spark_silver", *job], self.launch.launch_words(job, "gold_dimensions"))
        script = ["/data/stand_in.py", "build", "b"]
        self.assertEqual(["spark_gold", *script], self.launch.launch_words(script, "gold_dimensions"))

    def test_every_job_resolves_to_its_workloads_service_profile_and_credential_folder(self) -> None:
        for job, workload in JOB_WORKLOADS.items():
            if workload not in SERVICE_WORKLOADS:
                continue
            arguments = ["-m", f"spark.jobs.{job}", "x"]
            with self.subTest(job=job):
                self.assertEqual(f"spark_{workload}", self.launch.service_for(arguments, "catalog_proof"))
                self.assertEqual(f"batch_{workload}", self.launch.profile_for(arguments, "catalog_proof"))
                self.assertEqual([ROOT / "a" / workload], self.launch.access_folders(ROOT / "a", [job]))
                self.assertEqual([f"{workload.upper()}_ACCESS_DIR={ROOT / 'a' / workload}"], self.launch.access_settings(ROOT / "a", [job]))

    def test_the_filters_list_every_spark_service(self) -> None:
        self.assertEqual(
            ["spark_bronze", "spark_silver", "spark_gold", "spark_maintenance"],
            [item.rsplit("=", 1)[1] for item in self.launch.SPARK_FILTERS if item.startswith("label=")],
        )


if __name__ == "__main__":
    unittest.main()
