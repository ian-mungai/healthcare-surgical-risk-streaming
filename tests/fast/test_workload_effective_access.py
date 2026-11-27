"""What each workload role can do to a given object key, judged the way AWS judges it: an action is allowed only when the role's own policy allows it and
the permissions boundary allows it too.

The role policy is built from ``workloads`` in ``infra/environments/dev.tfvars`` by the rules of ``infra/modules/workload_roles/main.tf``: read gives
``GetObject`` and ``GetObjectVersion``, write gives ``PutObject`` and ``AbortMultipartUpload``, delete gives ``DeleteObject``, each on
``<bucket>/<prefix>/*``. The boundary is ``infra/iam/boundary/*.json`` with the bucket filled in. The tests probe object keys, not policy strings, so a
grant written as a wider pattern is caught as well as an exact one: ``warehouse/*`` covers ``warehouse/raw_records/...``, which the Airbyte load owns and
no workload may write or delete.
"""

from __future__ import annotations

import fnmatch
import json
import unittest
from pathlib import Path

from spark.maintenance import COMPACTED_NAMESPACES
from tests.fast.test_workload_grants import TFVARS, read_workloads

ROOT = Path(__file__).resolve().parents[2]
BOUNDARY = ROOT / "infra" / "iam" / "boundary" / "healthcare_surgical_risk_streaming_role_boundary.json"
BUCKET = "example-project"
ACTIONS = {
    "read": ("s3:GetObject", "s3:GetObjectVersion"),
    "write": ("s3:PutObject", "s3:AbortMultipartUpload"),
    "delete": ("s3:DeleteObject",),
}
WRITE_ACTIONS = ("s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject", "s3:DeleteObjectVersion")
RAW_KEY = "warehouse/raw_records/airbyte_load/data/00000-0-file.parquet"
# Nessie keeps a table at warehouse/<namespace>/<table>/..., so the key of a file of a table of a namespace is this.


def key_of(namespace: str) -> str:
    return f"warehouse/{namespace}/some_table_1234/data/00000-0-file.parquet"


def role_statements(access: dict[str, list[str]]) -> list[tuple[str, str]]:
    """The allowed (action, resource pattern) pairs of a role from its prefixes and modes."""
    return [(action, f"arn:aws:s3:::{BUCKET}/{prefix}/*") for prefix, modes in access.items() for mode in modes for action in ACTIONS[mode]]


def boundary_statements(text: str) -> list[tuple[str, str]]:
    document = json.loads(text.replace("${S3_BUCKET}", BUCKET))
    found = []
    for statement in document["Statement"]:
        if statement["Effect"] != "Allow":
            continue
        actions = statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]
        resources = statement["Resource"] if isinstance(statement["Resource"], list) else [statement["Resource"]]
        found += [(action, resource) for action in actions for resource in resources]
    return found


def allowed(statements: list[tuple[str, str]], action: str, key: str) -> bool:
    arn = f"arn:aws:s3:::{BUCKET}/{key}"
    return any(granted == action and fnmatch.fnmatchcase(arn, pattern) for granted, pattern in statements)


def effective(access: dict[str, list[str]], boundary: list[tuple[str, str]], action: str, key: str) -> bool:
    """The role's policy AND the permissions boundary both allow ``action`` on ``key``."""
    return allowed(role_statements(access), action, key) and allowed(boundary, action, key)


def access_of(text: str) -> dict[str, dict[str, list[str]]]:
    return {name: entry["access"] for name, entry in read_workloads(text).items()}


def raw_records_writers(accesses: dict[str, dict[str, list[str]]], boundary: list[tuple[str, str]]) -> list[str]:
    """Each workload and write or delete action that is effectively allowed on a ``raw_records`` key."""
    return [f"{name}: {action}" for name, access in sorted(accesses.items()) for action in WRITE_ACTIONS if effective(access, boundary, action, RAW_KEY)]


class RealFileTest(unittest.TestCase):
    def setUp(self) -> None:
        self.text = TFVARS.read_text()
        self.boundary = boundary_statements(BOUNDARY.read_text())
        self.access = access_of(self.text)

    def test_the_boundary_alone_allows_a_raw_records_write_so_the_role_policy_is_what_stops_it(self) -> None:
        self.assertTrue(allowed(self.boundary, "s3:PutObject", RAW_KEY))

    def test_no_workload_can_write_or_delete_a_raw_records_key(self) -> None:
        self.assertEqual([], raw_records_writers(self.access, self.boundary))

    def test_the_workloads_that_check_tags_and_read_raw_records_still_read_it(self) -> None:
        for name in ("bronze", "silver", "maintenance"):
            with self.subTest(name):
                self.assertTrue(effective(self.access[name], self.boundary, "s3:GetObject", RAW_KEY))

    def test_maintenance_writes_every_namespace_it_compacts_and_no_other(self) -> None:
        maintenance = self.access["maintenance"]
        for namespace in COMPACTED_NAMESPACES:
            with self.subTest(namespace):
                self.assertTrue(effective(maintenance, self.boundary, "s3:PutObject", key_of(namespace)))
        self.assertFalse(effective(maintenance, self.boundary, "s3:PutObject", key_of("raw_records")))
        self.assertFalse(effective(maintenance, self.boundary, "s3:PutObject", key_of("another_namespace")))
        self.assertFalse(effective(maintenance, self.boundary, "s3:PutObject", "warehouse/loose_file.parquet"))

    def test_maintenance_deletes_nothing_anywhere(self) -> None:
        maintenance = self.access["maintenance"]
        for action in ("s3:DeleteObject", "s3:DeleteObjectVersion"):
            for namespace in (*COMPACTED_NAMESPACES, "raw_records"):
                with self.subTest(action=action, namespace=namespace):
                    self.assertFalse(effective(maintenance, self.boundary, action, key_of(namespace)))

    def test_the_compacted_namespaces_are_exactly_the_ones_maintenance_may_write(self) -> None:
        written = sorted(
            prefix.removeprefix("warehouse/") for prefix, modes in self.access["maintenance"].items() if "write" in modes and prefix != "warehouse"
        )
        self.assertEqual(sorted(COMPACTED_NAMESPACES), written)
        self.assertNotIn("warehouse", [prefix for prefix, modes in self.access["maintenance"].items() if "write" in modes])


class PlantedGrantTest(unittest.TestCase):
    """The effective check names a grant that reaches ``raw_records`` however it is written."""

    def setUp(self) -> None:
        self.boundary = boundary_statements(BOUNDARY.read_text())
        self.access = access_of(TFVARS.read_text())

    def test_maintenance_with_write_on_the_whole_warehouse_is_named(self) -> None:
        self.access["maintenance"]["warehouse"] = ["read", "write"]
        self.assertEqual(["maintenance: s3:PutObject", "maintenance: s3:AbortMultipartUpload"], raw_records_writers(self.access, self.boundary))

    def test_a_layer_workload_with_write_and_delete_on_raw_records_is_named(self) -> None:
        self.access["bronze"]["warehouse/raw_records"] = ["read", "write", "delete"]
        self.assertEqual(
            ["bronze: s3:PutObject", "bronze: s3:AbortMultipartUpload", "bronze: s3:DeleteObject"], raw_records_writers(self.access, self.boundary)
        )

    def test_a_workload_with_write_on_the_whole_warehouse_is_named_by_the_wide_pattern_not_by_an_exact_arn(self) -> None:
        self.access["trino"]["warehouse"] = ["read", "write"]
        self.assertEqual(["trino: s3:PutObject", "trino: s3:AbortMultipartUpload"], raw_records_writers(self.access, self.boundary))


if __name__ == "__main__":
    unittest.main()
