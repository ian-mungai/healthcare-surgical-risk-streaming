"""Each Spark job service holds what only a one-off batch job can get wrong, and the shared service rules hold it to what applies to a batch job.

There is one service for each Spark workload (Bronze, Silver, Gold and maintenance), and each reads the credentials of its own workload only. The generic
checks of ``test_compose_services`` hold every service to the shared rules. These checks hold each one-off Spark job service: it must not start with the
stack, must get its memory from the limits file, must reach no host credential, must run as the numeric user of its image and must differ from its
siblings in nothing but its workload.
"""

from __future__ import annotations

import copy
import re
import unittest
from pathlib import Path
from typing import Any

from scripts.infra.host import PROJECT_WORKLOAD_GIDS
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS
from scripts.stack.limits import load_limits
from tests.fast.test_compose_services import load, service_problems

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "compose"
CREDENTIAL_PROPERTY = re.compile(r"password|secret|token|access-key|credentials", re.IGNORECASE)


def service(name: str) -> dict[str, Any]:
    return dict(load(COMPOSE / "services" / f"{name}.yml")["services"][name])


def job_environment(name: str) -> dict[str, str]:
    return dict(service(name)["environment"])


class SparkServiceTest(unittest.TestCase):
    def each(self) -> list[str]:
        return [f"spark_{workload}" for workload in SERVICE_WORKLOADS]

    def test_there_is_one_service_for_each_workload_that_runs_a_job_and_no_other_spark_service(self) -> None:
        found = sorted(path.stem for path in (COMPOSE / "services").glob("spark*.yml"))
        self.assertEqual(sorted(self.each()), found)
        self.assertEqual(set(SERVICE_WORKLOADS), set(JOB_WORKLOADS.values()) - {"stream"})
        root = (COMPOSE / "compose.yml").read_text()
        self.assertEqual([f"services/{name}.yml" for name in self.each()], [line.split()[-1] for line in root.splitlines() if "services/spark" in line])

    def test_each_service_is_a_batch_service_in_its_own_group_that_the_stack_start_does_not_run_and_that_is_never_restarted(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                job = service(name)
                self.assertEqual([f"batch_{name.removeprefix('spark_')}"], job["profiles"])
                self.assertEqual("no", job["restart"])

    def test_each_service_has_a_limit_in_its_own_batch_group_and_reads_it_from_the_limits_file(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                entry = load_limits().services[name]
                self.assertEqual(("compose", f"batch_{name.removeprefix('spark_')}"), (entry.runtime, entry.group))
                self.assertEqual(f"${{MEM_LIMIT_{name.upper()}:?render the limits first}}", service(name)["mem_limit"])

    def test_no_memory_figure_is_typed_in_a_service_because_the_job_derives_it_from_the_limit(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                self.assertNotRegex((COMPOSE / "services" / f"{name}.yml").read_text(), r"(?i)spark\.(driver|executor)\.memory|-Xmx|-Xms")

    def test_no_service_publishes_a_port_or_holds_a_capability_or_a_credential_value(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                job = service(name)
                self.assertNotIn("ports", job)
                self.assertEqual(["ALL"], job["cap_drop"])
                self.assertEqual(["no-new-privileges:true"], job["security_opt"])
                self.assertFalse([key for key in job["environment"] if CREDENTIAL_PROPERTY.search(key)])

    def test_each_service_reaches_nessie_by_its_service_name_on_the_version_2_path_and_waits_for_it_to_be_healthy(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                job = service(name)
                self.assertEqual("${NESSIE_URI:-http://nessie:19120/api/v2}", job["environment"]["NESSIE_URI"])
                self.assertEqual({"nessie": {"condition": "service_healthy"}}, job["depends_on"])

    def test_no_service_asks_instance_metadata_for_the_host_role(self) -> None:
        for name in self.each():
            with self.subTest(name=name):
                self.assertEqual("true", job_environment(name)["AWS_EC2_METADATA_DISABLED"])

    def test_each_service_reads_the_access_of_its_own_workload_read_only_in_its_workload_group_and_no_other_host_path(self) -> None:
        for name in self.each():
            workload = name.removeprefix("spark_")
            with self.subTest(name=name):
                job = service(name)
                self.assertEqual("/etc/workload_access/client_config", job["environment"]["AWS_CONFIG_FILE"])
                self.assertIn(f"workload_access_{workload}:/run/workload_access:ro", job["volumes"])
                self.assertEqual([str(PROJECT_WORKLOAD_GIDS[workload])], job["group_add"])
                device = load(COMPOSE / "services" / f"{name}.yml")["volumes"][f"workload_access_{workload}"]["driver_opts"]["device"]
                self.assertEqual(f"${{{workload.upper()}_ACCESS_DIR:-/run/workload_access/{workload}}}", device)
                self.assertTrue(all(re.fullmatch(r"[a-z][a-z0-9_]*:/[a-z/_]+(:ro)?", volume) for volume in job["volumes"]), job["volumes"])

    def test_each_service_runs_as_the_numeric_user_that_the_image_names(self) -> None:
        users = [line.split()[1] for line in (ROOT / "spark" / "Dockerfile").read_text().splitlines() if line.startswith("USER ")]
        for name in self.each():
            with self.subTest(name=name):
                self.assertEqual([service(name)["user"]], users)

    def test_the_services_differ_only_in_what_names_their_workload(self) -> None:
        """A change to one service that its siblings lack is a change in what a layer may do: each file must equal the Bronze file with its names put in."""

        def shape(name: str) -> str:
            workload = name.removeprefix("spark_")
            text = (COMPOSE / "services" / f"{name}.yml").read_text()
            for old, new in (
                (workload.upper(), "W_UPPER"),
                (workload, "w"),
                (str(PROJECT_WORKLOAD_GIDS[workload]), "GID"),
            ):
                text = text.replace(old, new)
            return text

        shapes = {name: shape(name) for name in self.each()}
        for name, text in shapes.items():
            with self.subTest(name=name):
                self.assertEqual(shapes["spark_bronze"], text)


class BatchExceptionTest(unittest.TestCase):
    """The shared service rules hold a one-off batch job to what applies to it: no digest, no health check and no restart, and nothing else is relaxed."""

    def good(self) -> dict[str, Any]:
        return copy.deepcopy(load(COMPOSE / "services" / "spark_bronze.yml"))

    def defect(self, mutate: Any) -> list[str]:
        document = self.good()
        mutate(document, document["services"]["spark_bronze"])
        return service_problems("spark_bronze", document)

    def test_the_real_spark_files_have_no_problem(self) -> None:
        for workload in SERVICE_WORKLOADS:
            name = f"spark_{workload}"
            self.assertEqual([], service_problems(name, copy.deepcopy(load(COMPOSE / "services" / f"{name}.yml"))))

    def test_a_batch_job_that_restarts_is_named(self) -> None:
        for restart in ("unless-stopped", "always", "on-failure"):
            with self.subTest(restart=restart):
                problems = self.defect(lambda _d, s, restart=restart: s.update(restart=restart))
                self.assertEqual(1, len(problems))
                self.assertIn("restart", problems[0])

    def test_a_batch_job_with_no_dockerfile_or_a_moving_tag_is_named(self) -> None:
        for mutate in (
            lambda _d, s: s.pop("build"),
            lambda _d, s: s.update(image="surgical-risk-spark:latest"),
            lambda _d, s: s.update(image="surgical-risk-spark"),
        ):
            problems = self.defect(mutate)
            self.assertEqual(1, len(problems))
            self.assertIn("image", problems[0])

    def test_only_the_batch_profile_earns_the_exception(self) -> None:
        for profiles in (["debug"], ["batch_bronze", "debug"], ["batch"], []):
            with self.subTest(profiles=profiles):
                problems = self.defect(lambda _d, s, profiles=profiles: s.update(profiles=profiles))
                self.assertTrue([problem for problem in problems if "healthcheck" in problem], problems)
                self.assertTrue([problem for problem in problems if "restart" in problem], problems)

    def test_a_batch_job_still_needs_its_limit_platform_and_named_volumes(self) -> None:
        self.assertIn("mem_limit", self.defect(lambda _d, s: s.pop("mem_limit"))[0])
        self.assertIn("platform", self.defect(lambda _d, s: s.update(platform="linux/arm64"))[0])
        self.assertIn("named volumes", self.defect(lambda _d, s: s.update(volumes=["/srv/data:/scratch"]))[0])


if __name__ == "__main__":
    unittest.main()
