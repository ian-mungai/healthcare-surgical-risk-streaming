"""The Trino service holds what only it can get wrong: a read-only server on the loopback address whose catalogs are tracked files.

The generic checks of ``test_compose_services`` hold every service to the shared rules. These checks hold Trino: its every environment value is defined,
because a missing value stops Trino at start, its catalogs are the native Nessie catalog with no credential and its access control refuses every write.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any

from scripts.stack.limits import load_limits
from tests.fast.test_compose_services import load

COMPOSE = Path(__file__).resolve().parents[2] / "compose"
TRINO_FILES = COMPOSE / "trino"
CREDENTIAL_PROPERTY = re.compile(r"password|secret|token|access-key|credentials", re.IGNORECASE)


def service(name: str) -> dict[str, Any]:
    return dict(load(COMPOSE / "services" / f"{name}.yml")["services"][name])


def properties(path: Path) -> dict[str, str]:
    pairs = (line.partition("=") for line in path.read_text().splitlines() if line.strip() and not line.startswith("#"))
    return {key.strip(): value.strip() for key, _, value in pairs}


class TrinoServiceTest(unittest.TestCase):
    def test_trino_is_a_supporting_service_with_its_limit_in_the_limits_file(self) -> None:
        entry = load_limits().services["trino"]
        self.assertEqual(("compose", "supporting"), (entry.runtime, entry.group))

    def test_only_the_web_interface_is_published_and_only_on_the_loopback_address_where_caddy_routes_it(self) -> None:
        self.assertEqual(["127.0.0.1:${TRINO_HOST_PORT:-8080}:8080"], service("trino")["ports"])

    def test_trino_runs_as_the_numeric_user_of_its_image_with_every_capability_dropped(self) -> None:
        trino = service("trino")
        self.assertEqual("1000:1000", trino["user"])
        self.assertEqual(["ALL"], trino["cap_drop"])
        self.assertEqual(["no-new-privileges:true"], trino["security_opt"])

    def test_the_heap_is_a_share_of_the_container_limit_and_no_size_is_typed(self) -> None:
        options = (TRINO_FILES / "jvm.config").read_text().splitlines()
        self.assertEqual([], [option for option in options if re.match(r"-X(mx|ms)", option)])
        share = [int(option.split("=")[1]) for option in options if option.startswith("-XX:MaxRAMPercentage=")]
        self.assertEqual(1, len(share))
        self.assertLessEqual(share[0], 70, "the rest of the container's limit is for the JVM's non-heap memory")
        self.assertNotIn("-XX:InitialRAMPercentage=80", options)

    def test_trino_is_read_only_so_no_query_writes_and_it_hides_the_sample_catalogs(self) -> None:
        self.assertEqual({"access-control.name": "read-only"}, properties(TRINO_FILES / "access-control.properties"))
        server = properties(TRINO_FILES / "config.properties")
        self.assertEqual("static", server["catalog.management"])
        self.assertEqual({"jmx", "memory", "tpcds", "tpch"}, set(server["catalog.disabled-catalogs"].split(",")))
        self.assertFalse([key for key in server if CREDENTIAL_PROPERTY.search(key)])

    def test_every_catalog_is_the_native_nessie_catalog_and_holds_no_credential(self) -> None:
        catalogs = sorted(TRINO_FILES.glob("lake*.properties"))
        self.assertEqual(["lake.properties", "lake_pinned.properties"], [path.name for path in catalogs])
        for path in catalogs:
            with self.subTest(catalog=path.name):
                catalog = properties(path)
                self.assertEqual("iceberg", catalog["connector.name"])
                self.assertEqual("nessie", catalog["iceberg.catalog.type"])
                self.assertEqual("${ENV:NESSIE_URI}", catalog["iceberg.nessie-catalog.uri"])
                self.assertEqual("${ENV:WAREHOUSE_LOCATION}", catalog["iceberg.nessie-catalog.default-warehouse-dir"])
                self.assertEqual("${ENV:TRINO_FS_S3_ENABLED}", catalog["fs.s3.enabled"])
                self.assertEqual("${ENV:TRINO_FS_HADOOP_ENABLED}", catalog["fs.hadoop.enabled"])
                self.assertFalse([key for key in catalog if CREDENTIAL_PROPERTY.search(key)])

    def test_the_main_catalog_follows_main_and_the_pinned_catalog_follows_the_setting(self) -> None:
        self.assertEqual("main", properties(TRINO_FILES / "lake.properties")["iceberg.nessie-catalog.ref"])
        self.assertEqual("${ENV:TRINO_PINNED_REF}", properties(TRINO_FILES / "lake_pinned.properties")["iceberg.nessie-catalog.ref"])

    def test_every_environment_value_that_a_catalog_reads_is_set_by_the_service_so_trino_starts(self) -> None:
        wanted = {name for path in TRINO_FILES.glob("*.properties") for name in re.findall(r"\$\{ENV:(\w+)\}", path.read_text())}
        defined = set(service("trino")["environment"])
        self.assertEqual([], sorted(wanted - defined))

    def test_the_file_system_defaults_are_the_host_s3_and_the_pinned_reference_defaults_to_main(self) -> None:
        environment = service("trino")["environment"]
        self.assertEqual("${TRINO_FS_S3_ENABLED:-true}", environment["TRINO_FS_S3_ENABLED"])
        self.assertEqual("${TRINO_FS_HADOOP_ENABLED:-false}", environment["TRINO_FS_HADOOP_ENABLED"])
        self.assertEqual("${TRINO_PINNED_REF:-main}", environment["TRINO_PINNED_REF"])

    def test_an_unset_warehouse_location_is_an_s3_placeholder_that_fails_when_read_and_never_a_local_folder(self) -> None:
        self.assertEqual("${WAREHOUSE_LOCATION:-s3://warehouse-location-not-set/warehouse/}", service("trino")["environment"]["WAREHOUSE_LOCATION"])

    def test_trino_reads_its_own_short_lived_access_read_only_and_never_the_host_role(self) -> None:
        environment = service("trino")["environment"]
        self.assertEqual("true", environment["AWS_EC2_METADATA_DISABLED"])
        self.assertEqual("/etc/workload_access/client_config", environment["AWS_CONFIG_FILE"])
        self.assertIn("workload_access_trino:/run/workload_access:ro", service("trino")["volumes"])

    def test_the_healthcheck_asks_trino_itself_and_waits_for_a_healthy_nessie(self) -> None:
        trino = service("trino")
        self.assertIn("/usr/lib/trino/bin/health-check", " ".join(trino["healthcheck"]["test"]))
        self.assertEqual({"nessie": {"condition": "service_healthy"}}, trino["depends_on"])


if __name__ == "__main__":
    unittest.main()
