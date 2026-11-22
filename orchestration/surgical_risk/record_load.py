"""The Dagster step of the record load: it runs ``scripts.ingestion.record_load`` for one run and records the load branch, its head and each table's snapshot.

The step takes the reference of the passed manifest as run configuration and runs the gate, the exact-version copy, the load branch and the three syncs in the
order that ``record_load`` fixes. It never merges: the table checks run on the load branch after this step, and the merge follows only when they pass. A job
that composes the steps of ingestion makes this step depend on the manifest step and the table checks depend on this step, so the order copy, check, manifest,
load, table checks holds by the job's own edges.

What the step records, as run tags that the run folder keeps: ``surgical_risk/records/load_branch``, ``.../base_hash``, ``.../head_hash``,
``.../manifest_sha256`` and ``.../snapshot_<table>``. The step's output is the same facts as a mapping for the next step.

The job runs inside the ``ingestion`` service group and must use the in-process executor, like every job with a group resource.
"""

import os
from collections.abc import Mapping
from typing import Any

from dagster import Config, Definitions, OpExecutionContext, in_process_executor, job, op

from scripts.ingestion.manifest import ManifestRef
from scripts.ingestion.record_load import LoadResult, result_tags
from scripts.ingestion.record_run import run_load, settings_from_environment

from .groups import GroupResource

TAG_PREFIX = "surgical_risk/records"
GROUP = "ingestion"


class RecordLoadConfig(Config):
    run_id: str
    manifest_reference: str


def load_facts(result: LoadResult) -> dict[str, Any]:
    return {"run_id": result.run_id, "branch": result.branch, "head_hash": result.head_hash, "snapshots": dict(result.snapshots)}


@op
def load_record_tables(context: OpExecutionContext, config: RecordLoadConfig, group: GroupResource) -> Mapping[str, Any]:
    context.log.info(f"group {group.group} while the step runs: {group.services_text()}")
    result = run_load(settings_from_environment(os.environ), config.run_id, ManifestRef.from_json(config.manifest_reference))
    if context.dagster_run is not None and context.instance is not None:
        context.instance.add_run_tags(context.dagster_run.run_id, result_tags(result, TAG_PREFIX))
    context.log.info(f"loaded {', '.join(sorted(result.snapshots))} on {result.branch} at {result.head_hash}")
    return load_facts(result)


@job(resource_defs={"group": GroupResource(group=GROUP)}, executor_def=in_process_executor)
def record_load_job() -> None:
    load_record_tables()


defs = Definitions(jobs=[record_load_job])
