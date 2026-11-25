"""Each Spark job runs under the identity of one lake layer, and that identity reaches what the job reads and writes and nothing else.

``scripts/stack/job_workloads.py`` names the workload of each job module. The grants are in ``infra/environments/dev.tfvars`` and the Compose services in
``compose/services``. ``JOB_TABLES`` is the design's table of what each job reads and writes (from the job's own description in ``spark/jobs``): the namespaces
of the lake, which Nessie's catalog keeps in the warehouse folder of the same name, and the object prefixes outside the warehouse. A job that runs under a
workload that cannot reach what it reads or writes fails here, and so does a grant that no job of the workload needs. The planted cases move one job to
another workload or widen one grant, and each is named.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from scripts.infra.host import PROJECT_WORKLOAD_GIDS
from scripts.stack.job_workloads import JOB_WORKLOADS, SERVICE_WORKLOADS, JobWorkloadError, profile_of_job, service_of, workload_of
from scripts.stack.limits import load_limits
from tests.fast.test_workload_grants import TFVARS, read_workloads

ROOT = Path(__file__).resolve().parents[2]
# job: (namespaces it reads, namespaces it writes, object prefixes outside the warehouse that it reads)
JOB_TABLES: dict[str, tuple[set[str], set[str], set[str]]] = {
    "bronze_clinical_lab": ({"raw_records"}, {"bronze"}, set()),
    "bronze_decode": (set(), {"bronze"}, {"landing", "manifests"}),
    "table_checks": ({"raw_records"}, set(), set()),
    "patient_split": ({"bronze"}, {"silver"}, set()),
    "signal_profile": ({"silver", "bronze", "raw_records"}, {"silver"}, set()),
    "silver_cases": ({"bronze", "silver"}, {"silver"}, set()),
    "silver_hypotension": ({"silver", "bronze"}, {"silver"}, set()),
    "silver_labels": ({"silver"}, {"silver"}, set()),
    "silver_reconcile": ({"bronze", "silver"}, set(), set()),
    "silver_signals": ({"bronze"}, {"silver"}, set()),
    "gold_dimensions": ({"silver", "bronze"}, {"gold"}, set()),
    "stream_features": (set(), {"feature", "stream"}, set()),
    "catalog_proof": ({"catalog_proof"}, {"catalog_proof"}, set()),
    "table_maintenance": ({"*"}, {"*"}, set()),
}
WIDE = "*"


def needed(workload: str, jobs: dict[str, str], tables: dict[str, tuple[set[str], set[str], set[str]]]) -> dict[str, set[str]]:
    """The modes that the jobs of ``workload`` need on each prefix, from the table. A layer writer needs read, write and delete there, maintenance no delete."""
    found: dict[str, set[str]] = {}
    for job, owner in jobs.items():
        if owner != workload:
            continue
        reads, writes, objects = tables[job]
        for prefix in objects:
            found.setdefault(prefix, set()).add("read")
        for namespace in reads:
            found.setdefault("warehouse" if namespace == WIDE else f"warehouse/{namespace}", set()).add("read")
        for namespace in writes:
            if namespace in (WIDE, "catalog_proof"):
                found.setdefault("warehouse", set()).update({"read", "write"})
            else:
                found.setdefault(f"warehouse/{namespace}", set()).update({"read", "write", "delete"})
    if "warehouse" in found:
        found = {prefix: modes for prefix, modes in found.items() if not prefix.startswith("warehouse/")} | {"warehouse": found["warehouse"]}
    return found


def problems(jobs: dict[str, str], granted: dict[str, dict[str, set[str]]]) -> list[str]:
    """Each way that a job's workload reaches less than the job needs or more than any of its jobs need, as ``workload: what``."""
    found = []
    for workload in sorted(set(jobs.values())):
        have = granted.get(workload)
        if have is None:
            found.append(f"{workload}: the variable file holds no such workload")
            continue
        want = needed(workload, jobs, JOB_TABLES)
        for prefix, modes in sorted(want.items()):
            if not modes <= have.get(prefix, set()):
                found.append(f"{workload}: lacks {sorted(modes - have.get(prefix, set()))} on {prefix}")
        for prefix, modes in sorted(have.items()):
            if not modes <= want.get(prefix, set()):
                found.append(f"{workload}: has {sorted(modes - want.get(prefix, set()))} on {prefix}, which no job of it needs")
    return found


def granted_modes(text: str) -> dict[str, dict[str, set[str]]]:
    return {name: {prefix: set(modes) for prefix, modes in entry["access"].items()} for name, entry in read_workloads(text).items()}


class JobTableTest(unittest.TestCase):
    def test_every_job_module_has_one_workload_and_one_row_of_the_table_and_no_other_does(self) -> None:
        modules = sorted(path.stem for path in (ROOT / "spark" / "jobs").glob("*.py") if path.stem != "__init__")
        self.assertEqual(modules, sorted(JOB_WORKLOADS))
        self.assertEqual(modules, sorted(JOB_TABLES))

    def test_every_workload_of_a_job_has_a_grant_a_group_and_a_service_unless_it_is_the_streaming_job(self) -> None:
        workloads = set(JOB_WORKLOADS.values())
        self.assertLessEqual(workloads, set(read_workloads(TFVARS.read_text())))
        self.assertLessEqual(workloads, set(PROJECT_WORKLOAD_GIDS))
        self.assertEqual(set(SERVICE_WORKLOADS), workloads - {"stream"})
        limits = load_limits().services
        for workload in SERVICE_WORKLOADS:
            with self.subTest(workload=workload):
                self.assertTrue((ROOT / "compose" / "services" / f"spark_{workload}.yml").is_file())
                self.assertEqual(f"batch_{workload}", limits[f"spark_{workload}"].group)

    def test_a_job_resolves_to_its_workloads_service_and_profile_and_the_streaming_job_to_none(self) -> None:
        self.assertEqual(("spark_bronze", "batch_bronze"), (service_of("table_checks"), profile_of_job("table_checks")))
        self.assertEqual(("spark_silver", "batch_silver"), (service_of("silver_reconcile"), profile_of_job("silver_reconcile")))
        self.assertEqual(("spark_gold", "batch_gold"), (service_of("gold_dimensions"), profile_of_job("gold_dimensions")))
        self.assertEqual("spark_maintenance", service_of("catalog_proof"))
        with self.assertRaisesRegex(JobWorkloadError, "no Compose service"):
            service_of("stream_features")
        with self.assertRaisesRegex(JobWorkloadError, "no workload"):
            workload_of("another_job")

    def test_no_launcher_in_the_repository_names_a_spark_service_by_hand(self) -> None:
        """A launcher that starts a job module takes its service and profile from ``job_workloads``, so no job starts under a layer's identity by chance."""
        launchers = []
        for folder in ("scripts", "orchestration"):
            for path in sorted((ROOT / folder).rglob("*.py")):
                text = path.read_text()
                if re.search(r"spark\.jobs\.", text) and path.name != "job_workloads.py":
                    launchers.append(path.relative_to(ROOT).as_posix())
                    self.assertIn("job_workloads", text, path.name)
                    self.assertNotRegex(text, r'"python",\s*"spark[a-z_]*"', path.name)
        self.assertEqual(["scripts/ingestion/table_checks.py"], launchers)


class GrantTest(unittest.TestCase):
    def test_each_workload_reaches_what_its_jobs_read_and_write_and_nothing_else(self) -> None:
        self.assertEqual([], problems(JOB_WORKLOADS, granted_modes(TFVARS.read_text())))

    def planted_job(self, job: str, workload: str) -> list[str]:
        return problems({**JOB_WORKLOADS, job: workload}, granted_modes(TFVARS.read_text()))

    def test_a_job_moved_to_the_wrong_workload_is_named(self) -> None:
        cases = {
            "the Bronze decoder under Silver": ("bronze_decode", "silver", "silver: lacks ['read'] on landing"),
            "the Silver cases job under Bronze": ("silver_cases", "bronze", "bronze: lacks ['delete', 'read', 'write'] on warehouse/silver"),
            "the Gold dimensions under Silver": ("gold_dimensions", "silver", "silver: lacks ['delete', 'read', 'write'] on warehouse/gold"),
            "the table checks under Gold": ("table_checks", "gold", "gold: lacks ['read'] on warehouse/raw_records"),
            "the maintenance under Bronze": ("table_maintenance", "bronze", "bronze: lacks ['read', 'write'] on warehouse"),
            "the streaming job under Gold": ("stream_features", "gold", "gold: lacks ['delete', 'read', 'write'] on warehouse/feature"),
        }
        for label, (job, workload, named) in cases.items():
            with self.subTest(label):
                found = self.planted_job(job, workload)
                self.assertTrue([line for line in found if line.startswith(named)], found)

    def test_a_widened_grant_is_named(self) -> None:
        text = TFVARS.read_text()
        for label, old, new, named in (
            (
                "silver writes bronze",
                '"warehouse/bronze"      = ["read"]\n      "warehouse/silver"',
                '"warehouse/bronze"      = ["read", "write"]\n      "warehouse/silver"',
                "silver: has ['write'] on warehouse/bronze",
            ),
            (
                "gold reads landing",
                '"warehouse/gold"   = ["read", "write", "delete"]',
                '"warehouse/gold"   = ["read", "write", "delete"]\n      landing = ["read"]',
                "gold: has ['read'] on landing",
            ),
            (
                "maintenance deletes",
                'access = { warehouse = ["read", "write"] }',
                'access = { warehouse = ["read", "write", "delete"] }',
                "maintenance: has ['delete'] on warehouse",
            ),
        ):
            with self.subTest(label):
                self.assertEqual(1, text.count(old))
                self.assertTrue([line for line in problems(JOB_WORKLOADS, granted_modes(text.replace(old, new))) if line.startswith(named)])


if __name__ == "__main__":
    unittest.main()
