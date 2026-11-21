"""The Spark job service holds what only a one-off batch job can get wrong, and the shared service rules hold it to what applies to a batch job.

The generic checks of ``test_compose_services`` hold every service to the shared rules. These checks hold the one-off Spark job: it must not start with the
stack, must get its memory from the limits file, must reach no host credential and must run as the numeric user of its image.
"""

from __future__ import annotations

import copy
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.limits import load_limits
from tests.fast.test_compose_services import load, service_problems

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "compose"
CREDENTIAL_PROPERTY = re.compile(r"password|secret|token|access-key|credentials", re.IGNORECASE)


def service(name: str) -> dict[str, Any]:
    return dict(load(COMPOSE / "services" / f"{name}.yml")["services"][name])


class SparkServiceTest(unittest.TestCase):
    def test_the_job_is_a_batch_service_that_the_stack_start_does_not_run_and_that_is_never_restarted(self) -> None:
        job = service("spark")
        self.assertEqual(["batch"], job["profiles"])
        self.assertEqual("no", job["restart"])

    def test_the_job_has_a_limit_in_the_batch_group_and_reads_it_from_the_limits_file(self) -> None:
        entry = load_limits().services["spark"]
        self.assertEqual(("compose", "batch"), (entry.runtime, entry.group))
        self.assertEqual("${MEM_LIMIT_SPARK:?render the limits first}", service("spark")["mem_limit"])

    def test_no_memory_figure_is_typed_in_the_service_because_the_job_derives_it_from_the_limit(self) -> None:
        text = (COMPOSE / "services" / "spark.yml").read_text()
        self.assertNotRegex(text, r"(?i)spark\.(driver|executor)\.memory|-Xmx|-Xms")

    def test_the_job_publishes_no_port_and_holds_no_capability_and_no_credential_value(self) -> None:
        job = service("spark")
        self.assertNotIn("ports", job)
        self.assertEqual(["ALL"], job["cap_drop"])
        self.assertEqual(["no-new-privileges:true"], job["security_opt"])
        self.assertFalse([key for key in job["environment"] if CREDENTIAL_PROPERTY.search(key)])

    def test_the_job_reaches_nessie_by_its_service_name_on_the_version_2_path_and_waits_for_it_to_be_healthy(self) -> None:
        job = service("spark")
        self.assertEqual("${NESSIE_URI:-http://nessie:19120/api/v2}", job["environment"]["NESSIE_URI"])
        self.assertEqual({"nessie": {"condition": "service_healthy"}}, job["depends_on"])

    def test_the_job_never_asks_instance_metadata_for_the_host_role(self) -> None:
        self.assertEqual("true", job_environment()["AWS_EC2_METADATA_DISABLED"])

    def test_the_job_reads_its_own_short_lived_access_read_only_and_no_other_host_path(self) -> None:
        job = service("spark")
        self.assertEqual("/etc/workload_access/client_config", job["environment"]["AWS_CONFIG_FILE"])
        self.assertIn("workload_access_spark:/run/workload_access:ro", job["volumes"])
        self.assertTrue(all(re.fullmatch(r"[a-z][a-z0-9_]*:/[a-z/_]+(:ro)?", volume) for volume in job["volumes"]), job["volumes"])

    def test_the_job_runs_as_the_numeric_user_that_the_image_names(self) -> None:
        users = [line.split()[1] for line in (ROOT / "spark" / "Dockerfile").read_text().splitlines() if line.startswith("USER ")]
        self.assertEqual([service("spark")["user"]], users)


def job_environment() -> dict[str, str]:
    return dict(service("spark")["environment"])


class BatchExceptionTest(unittest.TestCase):
    """The shared service rules hold a one-off batch job to what applies to it: no digest, no health check and no restart, and nothing else is relaxed."""

    def good(self) -> dict[str, Any]:
        return copy.deepcopy(load(COMPOSE / "services" / "spark.yml"))

    def defect(self, mutate: Any) -> list[str]:
        document = self.good()
        mutate(document, document["services"]["spark"])
        return service_problems("spark", document)

    def test_the_real_spark_file_has_no_problem(self) -> None:
        self.assertEqual([], service_problems("spark", self.good()))

    def test_a_batch_job_that_restarts_is_named(self) -> None:
        for restart in ("unless-stopped", "always", "on-failure"):
            with self.subTest(restart=restart):
                problems = self.defect(lambda _d, s, restart=restart: s.update(restart=restart))
                self.assertEqual(1, len(problems))
                self.assertIn("restart", problems[0])

    def test_a_batch_job_with_no_dockerfile_or_a_moving_tag_is_named(self) -> None:
        for mutate in (
            lambda _d, s: s.pop("build"),
            lambda _d, s: s.update(image="surgical_risk_spark:latest"),
            lambda _d, s: s.update(image="surgical_risk_spark"),
        ):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("image", problems[0])

    def test_only_the_batch_profile_earns_the_exception(self) -> None:
        for profiles in (["debug"], ["batch", "debug"], []):
            with self.subTest(profiles=profiles):
                problems = self.defect(lambda _d, s, profiles=profiles: s.update(profiles=profiles))
                self.assertTrue([problem for problem in problems if "image" in problem], problems)

    def test_a_batch_job_still_needs_its_limit_platform_and_named_volumes(self) -> None:
        self.assertIn("mem_limit", self.defect(lambda _d, s: s.pop("mem_limit"))[0])
        self.assertIn("platform", self.defect(lambda _d, s: s.update(platform="linux/arm64"))[0])
        self.assertIn("named volumes", self.defect(lambda _d, s: s.update(volumes=["/srv/data:/scratch"]))[0])


if __name__ == "__main__":
    unittest.main()
