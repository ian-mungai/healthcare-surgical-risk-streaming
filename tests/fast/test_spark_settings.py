"""The Spark jobs take their memory from the container's limit and their catalog from the warehouse location, and refuse what they cannot run safely.

A heap that adds up to more than the container's limit is killed by the kernel in the middle of a write, and a catalog opened with the wrong file
system or the wrong API version fails late. These checks hold the arithmetic and the refusals. The real runs are in ``tests/e2e/catalog_proof.py``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from spark.settings import OVERHEAD_FLOOR_MIB, SettingsError, catalog_options, driver_memory_mib, read_memory_limit_mib

GOOD = {"nessie_uri": "http://nessie:19120/api/v2", "warehouse": "s3://example-bucket/warehouse/", "ref": "proof_ab12cd34", "region": "us-west-2"}


def refused(**changes: str) -> str:
    try:
        catalog_options(**{**GOOD, **changes})
    except SettingsError as error:
        return str(error)
    raise AssertionError("the settings accepted a value that they must refuse")


class MemoryTest(unittest.TestCase):
    def test_the_heap_and_its_overhead_always_fit_the_container_limit(self) -> None:
        for limit in range(900, 33000, 37):
            with self.subTest(limit=limit):
                heap = driver_memory_mib(limit)
                overhead = max(OVERHEAD_FLOOR_MIB, -(-heap * 4 // 10))
                self.assertLessEqual(heap + overhead, limit)

    def test_a_bigger_limit_never_gives_a_smaller_heap(self) -> None:
        heaps = [driver_memory_mib(limit) for limit in range(900, 33000, 10)]
        self.assertEqual(sorted(heaps), heaps)

    def test_known_limits_give_the_stated_heap(self) -> None:
        self.assertEqual(2925, driver_memory_mib(4096))
        self.assertEqual(640, driver_memory_mib(1024))
        self.assertEqual(512, driver_memory_mib(896))

    def test_a_limit_too_small_for_any_job_is_refused_with_its_figure(self) -> None:
        with self.assertRaisesRegex(SettingsError, "895 MiB"):
            driver_memory_mib(895)

    def test_the_limit_is_read_from_the_cgroup_file_in_whole_mebibytes(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "memory.max"
            path.write_text(f"{4096 * 1024 * 1024}\n")
            self.assertEqual(4096, read_memory_limit_mib(path))

    def test_a_container_with_no_limit_is_refused_so_a_job_never_runs_unbounded(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            for text in ("max\n", f"{2**63 - 4096}\n", "", "lots\n"):
                path = Path(folder) / "memory.max"
                path.write_text(text)
                with self.subTest(text=text), self.assertRaisesRegex(SettingsError, "memory limit"):
                    read_memory_limit_mib(path)

    def test_a_missing_cgroup_file_is_refused_by_name(self) -> None:
        with self.assertRaisesRegex(SettingsError, "does_not_exist"):
            read_memory_limit_mib(Path("/nonexistent/does_not_exist"))


class CatalogTest(unittest.TestCase):
    def test_an_s3_warehouse_opens_the_native_nessie_catalog_on_the_s3_file_io_with_no_credential(self) -> None:
        options = catalog_options(**GOOD)
        prefix = "spark.sql.catalog.lake"
        self.assertEqual("org.apache.iceberg.spark.SparkCatalog", options[prefix])
        self.assertEqual("org.apache.iceberg.nessie.NessieCatalog", options[f"{prefix}.catalog-impl"])
        self.assertEqual("http://nessie:19120/api/v2", options[f"{prefix}.uri"])
        self.assertEqual("2", options[f"{prefix}.client-api-version"])
        self.assertEqual("proof_ab12cd34", options[f"{prefix}.ref"])
        self.assertEqual("s3://example-bucket/warehouse/", options[f"{prefix}.warehouse"])
        self.assertEqual("org.apache.iceberg.aws.s3.S3FileIO", options[f"{prefix}.io-impl"])
        self.assertEqual("us-west-2", options[f"{prefix}.client.region"])
        self.assertFalse([key for key in options if "access-key" in key or "secret" in key or "token" in key])

    def test_a_file_warehouse_differs_only_in_its_location_and_its_file_io(self) -> None:
        s3 = catalog_options(**GOOD)
        local = catalog_options(**{**GOOD, "warehouse": "file:///warehouse/"})
        differing = sorted(key for key in set(s3) | set(local) if s3.get(key) != local.get(key))
        self.assertEqual(["spark.sql.catalog.lake.client.region", "spark.sql.catalog.lake.io-impl", "spark.sql.catalog.lake.warehouse"], differing)
        self.assertEqual("org.apache.iceberg.hadoop.HadoopFileIO", local["spark.sql.catalog.lake.io-impl"])

    def test_a_second_catalog_of_another_name_on_a_tag_has_every_option_under_its_own_name_and_none_under_lake(self) -> None:
        options = catalog_options(**{**GOOD, "ref": "run-20261122t0925-4f2a9c", "catalog": "pin0"})
        self.assertEqual(catalog_options(**GOOD).keys(), {key.replace("catalog.pin0", "catalog.lake") for key in options})
        self.assertFalse([key for key in options if key.startswith("spark.sql.catalog.lake")])
        self.assertEqual("run-20261122t0925-4f2a9c", options["spark.sql.catalog.pin0.ref"])

    def test_an_address_without_the_version_2_path_is_refused_because_the_client_would_speak_version_1(self) -> None:
        for uri in ("http://nessie:19120", "http://nessie:19120/api/v1", "http://nessie:19120/api/v2/extra", "https://nessie:19120/api/v2", ""):
            with self.subTest(uri=uri):
                self.assertIn("api/v2", refused(nessie_uri=uri))

    def test_a_warehouse_outside_the_two_known_file_systems_is_refused(self) -> None:
        for warehouse in ("gs://bucket/warehouse/", "http://host/warehouse/", "/warehouse/", "s3://", ""):
            with self.subTest(warehouse=warehouse):
                self.assertIn("warehouse", refused(warehouse=warehouse))

    def test_an_empty_reference_is_refused_so_a_job_never_writes_to_the_default_branch_by_accident(self) -> None:
        self.assertIn("reference", refused(ref=""))

    def test_an_s3_warehouse_without_a_region_is_refused_and_a_file_warehouse_needs_none(self) -> None:
        self.assertIn("region", refused(region=""))
        self.assertNotIn("spark.sql.catalog.lake.client.region", catalog_options(**{**GOOD, "warehouse": "file:///warehouse/", "region": ""}))


if __name__ == "__main__":
    unittest.main()
