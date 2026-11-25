"""The workload, and so the AWS identity, of each Spark job: one role for each lake layer.

A Spark job runs as a one-off of the Compose service that holds its workload's credentials (``spark_<workload>``, in the group and profile
``batch_<workload>``), so a job reaches only the warehouse folders of its own layer. ``JOB_WORKLOADS`` is the one place that names the workload of each job
module in ``spark/jobs``. A fast test holds it to the modules, to ``infra/environments/dev.tfvars`` and to the Compose services. A launcher takes the service
and the profile of a job from here and never types them.

- ``bronze``: reads the landed files, the manifests and the loaded ``raw_records`` tables and writes the Bronze tables. The table checks job also runs
  here: it only reads ``raw_records`` and writes nothing, and no workload may write ``raw_records``.
- ``silver``: reads Bronze and ``raw_records`` and writes the Silver tables. The reconciliation job also runs here: it reads Bronze and Silver and
  writes nothing.
- ``gold``: reads Silver and Bronze (``bronze.tracks`` feeds ``dim_track``) and writes the Gold tables.
- ``stream``: writes the ``feature`` and ``stream`` tables of the streaming job, which no batch job writes. It has no Compose service until the job has one.
- ``maintenance``: reads every table to prove the tags, rewrites the files of the layer folders (``spark.maintenance.COMPACTED_NAMESPACES``, never
  ``raw_records``) and deletes nothing. The catalog proof job writes its own namespace and runs here.
"""

from __future__ import annotations

JOB_WORKLOADS: dict[str, str] = {
    "bronze_clinical_lab": "bronze",
    "bronze_decode": "bronze",
    "table_checks": "bronze",
    "patient_split": "silver",
    "signal_profile": "silver",
    "silver_cases": "silver",
    "silver_hypotension": "silver",
    "silver_labels": "silver",
    "silver_reconcile": "silver",
    "silver_signals": "silver",
    "gold_dimensions": "gold",
    "stream_features": "stream",
    "catalog_proof": "maintenance",
    "table_maintenance": "maintenance",
}
# The workloads that have a Compose service. The streaming job has none yet.
SERVICE_WORKLOADS: tuple[str, ...] = ("bronze", "silver", "gold", "maintenance")


class JobWorkloadError(ValueError):
    """A job has no workload, or its workload has no Compose service."""


def workload_of(job: str) -> str:
    try:
        return JOB_WORKLOADS[job]
    except KeyError:
        raise JobWorkloadError(f"the job {job} has no workload: add it to JOB_WORKLOADS") from None


def service_of(job: str) -> str:
    """The Compose service that runs ``job`` with its layer's credentials."""
    workload = workload_of(job)
    if workload not in SERVICE_WORKLOADS:
        raise JobWorkloadError(f"the workload {workload} of the job {job} has no Compose service")
    return f"spark_{workload}"


def profile_of_job(job: str) -> str:
    """The Compose profile, and the service group, of the service that runs ``job``."""
    return f"batch_{service_of(job).removeprefix('spark_')}"
