"""The workload, and so the AWS identity, of each Spark job: one role for each lake layer.

A Spark job runs as a one-off of the Compose service that holds its workload's credentials (``spark_<workload>``, in the group and profile
``batch_<workload>``), so a job reaches only the warehouse folders of its own layer. ``JOB_WORKLOADS`` is the one place that names the workload of each job
module in ``spark/jobs``. A fast test holds it to the modules, to ``infra/environments/dev.tfvars`` and to the Compose services. A launcher takes the service
and the profile of a job from here and never types them, and the service's entry point (``spark/launcher.py``) refuses a job that is not mapped to its workload.

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
# The Compose services that run Spark jobs and their service groups. One Spark job runs at a time across all of them: the limits assume one job's memory.
SPARK_SERVICES: tuple[str, ...] = tuple(f"spark_{workload}" for workload in SERVICE_WORKLOADS)
SPARK_GROUPS: tuple[str, ...] = tuple(f"batch_{workload}" for workload in SERVICE_WORKLOADS)


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


def refusal_of(job: str, workload: str | None) -> str | None:
    """The fixed code that refuses ``job`` in a service of ``workload``, or None when ``JOB_WORKLOADS`` maps the job to that workload. The Spark image's
    launcher asks this before Spark starts. The codes hold no text of the arguments: ``no_workload``, ``unknown_job`` and ``wrong_workload``."""
    if not workload:
        return "no_workload"
    if job not in JOB_WORKLOADS:
        return "unknown_job"
    return None if JOB_WORKLOADS[job] == workload else "wrong_workload"


def job_command(job: str, *arguments: str) -> list[str]:
    """The words after the options of ``docker compose run`` that start ``job`` in its workload's service, through the service's own entry point (the Spark
    launcher, which refuses a job that is not mapped to the service). A launcher never overrides the entry point."""
    return [service_of(job), job, *arguments]
