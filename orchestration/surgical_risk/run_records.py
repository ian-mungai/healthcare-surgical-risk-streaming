"""A sensor that writes the run folder of every Dagster run that ends, and the call that a job makes to report what it left running.

Dagster runs a sensor in the code location, so the folder is written under ``RUN_RECORDS_DIR``, a volume of that container. The sensor keeps a cursor of the
last update time it handled. A tick that cannot write a folder fails and leaves the cursor where it was, so the next tick tries the same runs again and no run
is skipped. Dagster's own run status sensors move their cursor on after a failed handler, which would skip the run. A run that has a folder keeps it unchanged,
so a repeated tick, an overlap of the query window or a restart adds nothing.
"""

import os
import sys
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path

from dagster import (
    DagsterEventType,
    DagsterInstance,
    DagsterRun,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from scripts.ingestion.folder import list_folder

from .retention import PRUNE_JOB, parse_removal
from .run_folder import LEFT_RUNNING_TAG, CheckFacts, RunFacts, StepFacts, build_record, left_running_tag_value, write_record

RUNS_DIR = Path(os.environ.get("RUN_RECORDS_DIR", "/opt/dagster/runs"))
FAILURE_EVENTS = {DagsterEventType.STEP_FAILURE, DagsterEventType.RUN_FAILURE}
SENSOR_INTERVAL_SECONDS = 5
OVERLAP_SECONDS = 2.0
BATCH_LIMIT = 500
ENDED = [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED]


def report_left_running(instance: DagsterInstance, run_id: str, names: list[str]) -> None:
    """Record on the run which services its job left running. An empty list says that nothing was left."""
    instance.add_run_tags(run_id, {LEFT_RUNNING_TAG: left_running_tag_value(names)})


def error_text(instance: DagsterInstance, run_id: str) -> str:
    """The error of the first failure event of the run, from its root cause, or an empty text."""
    for entry in instance.all_logs(run_id, of_type=FAILURE_EVENTS):
        event = entry.dagster_event
        if event is None:
            continue
        info = event.step_failure_data.error if event.event_type == DagsterEventType.STEP_FAILURE else event.job_failure_data.error
        while info is not None and info.cause is not None:
            info = info.cause
        if info is not None:
            return str(info.message).strip()
    return ""


def removals(instance: DagsterInstance, run_id: str) -> tuple[tuple[str, float], ...]:
    """The run IDs and end times that the pruning run logged as removed."""
    found = (parse_removal(entry.user_message) for entry in instance.all_logs(run_id) if entry.dagster_event is None)
    return tuple(item for item in found if item is not None)


def run_facts(instance: DagsterInstance, run: DagsterRun) -> RunFacts:
    stats = instance.get_run_stats(run.run_id)
    steps = tuple(
        StepFacts(step.step_key, step.status.value if step.status else "UNKNOWN", step.start_time, step.end_time)
        for step in instance.get_run_step_stats(run.run_id)
    )
    checks = tuple(
        CheckFacts(data.check_name, data.asset_key.to_user_string(), data.passed)
        for entry in instance.all_logs(run.run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION)
        if entry.dagster_event is not None
        for data in [entry.dagster_event.asset_check_evaluation_data]
    )
    return RunFacts(
        run_id=run.run_id,
        job_name=run.job_name,
        status=run.status.value,
        run_config=dict(run.run_config),
        tags=dict(run.tags),
        enqueued=stats.enqueued_time,
        launched=stats.launch_time,
        started=stats.start_time,
        ended=stats.end_time,
        steps=steps,
        checks=checks,
        error=error_text(instance, run.run_id),
        parent_run_id=run.parent_run_id,
        asset_selection=tuple(sorted(key.to_user_string() for key in run.asset_selection or ())),
        removed_runs=removals(instance, run.run_id) if run.job_name == PRUNE_JOB else (),
    )


def write_run_folder(instance: DagsterInstance, run: DagsterRun, runs: Path = RUNS_DIR) -> Path:
    environment = {"dagster": metadata.version("dagster"), "python": ".".join(map(str, sys.version_info[:3]))}
    record = build_record(run_facts(instance, run), commit=os.environ.get("PLATFORM_COMMIT", ""), environment=environment, recorded=datetime.now(UTC))
    return write_record(runs, record)


@sensor(name="run_folders", minimum_interval_seconds=SENSOR_INTERVAL_SECONDS, default_status=DefaultSensorStatus.RUNNING)
def run_folders(context: SensorEvaluationContext) -> SkipReason:
    """Write the folder of every run that ended since the cursor, oldest first. The cursor moves only after every folder of the batch is written."""
    after = float(context.cursor) if context.cursor else 0.0
    since = datetime.fromtimestamp(max(after - OVERLAP_SECONDS, 0.0), UTC)
    records = context.instance.get_run_records(RunsFilter(statuses=ENDED, updated_after=since), limit=BATCH_LIMIT, order_by="update_timestamp", ascending=True)
    existing = {path.name.rsplit("_", 1)[-1] for path in list_folder(RUNS_DIR, "*_*")}
    written = 0
    for entry in records:
        if entry.dagster_run.run_id not in existing:
            write_run_folder(context.instance, entry.dagster_run)
            written += 1
    if records:
        context.update_cursor(str(records[-1].update_timestamp.timestamp()))
    return SkipReason(f"wrote {written} run folders of {len(records)} runs that ended")


run_folder_sensors = [run_folders]
