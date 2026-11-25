"""The daily job that removes Dagster's old runs, with their event records and compute logs, through Dagster's own API.

The rule is in ``retention.py``. This file gives it the runs of the instance and Dagster's deletion calls, and logs each removal in the run of the job, from
which ``run_records.py`` puts the run ID and end time into that run's folder. The job starts with the platform: removing old history is its own housekeeping.
"""

import time
from datetime import UTC, datetime

from dagster import (
    Config,
    DagsterInstance,
    DefaultScheduleStatus,
    Definitions,
    OpExecutionContext,
    RunsFilter,
    ScheduleDefinition,
    job,
    op,
)

from scripts.ingestion.folder import list_folder

from .retention import PRUNE_JOB, RUN_RETENTION_DAYS, SECONDS_PER_DAY, EndedRun, prune, removal_message
from .run_records import ENDED, RUNS_DIR, write_run_folder

BATCH_LIMIT = 500
MAX_BATCHES = 100
SCHEDULE_NAME = "prune_old_runs_daily"
SCHEDULE_CRON = "17 3 * * *"


class PruneConfig(Config):
    now: float | None = None


class DagsterRunStore:
    def __init__(self, instance: DagsterInstance, context: OpExecutionContext, runs: list[EndedRun]) -> None:
        self.instance = instance
        self.context = context
        self.runs = {run.run_id: run for run in runs}

    def ensure_folder(self, run_id: str) -> None:
        run = self.instance.get_run_by_id(run_id)
        if run is not None and not list_folder(RUNS_DIR, f"*_{run_id}"):
            write_run_folder(self.instance, run)

    def delete_compute_logs(self, run_id: str) -> None:
        self.instance.compute_log_manager.delete_logs(prefix=[run_id])

    def delete_run(self, run_id: str) -> None:
        self.context.log.info(removal_message(self.runs[run_id]))
        self.instance.delete_run(run_id)


@op
def prune_old_runs(context: OpExecutionContext, config: PruneConfig) -> None:
    now = config.now if config.now is not None else time.time()
    cutoff = datetime.fromtimestamp(now - RUN_RETENTION_DAYS * SECONDS_PER_DAY, UTC)
    removed = 0
    for _ in range(MAX_BATCHES):
        records = context.instance.get_run_records(
            RunsFilter(statuses=ENDED, updated_before=cutoff), limit=BATCH_LIMIT, order_by="update_timestamp", ascending=True
        )
        runs = [
            EndedRun(
                record.dagster_run.run_id, record.dagster_run.status.value, record.dagster_run.job_name, record.end_time or record.update_timestamp.timestamp()
            )
            for record in records
        ]
        done = prune(runs, now, DagsterRunStore(context.instance, context, runs))
        removed += len(done)
        if not done:
            break
    context.log.info(f"pruned {removed} runs that ended more than {RUN_RETENTION_DAYS} days before {datetime.fromtimestamp(now, UTC):%Y-%m-%dT%H:%M:%SZ}")


@job(name=PRUNE_JOB)
def prune_old_runs_job() -> None:
    prune_old_runs()


prune_old_runs_daily = ScheduleDefinition(name=SCHEDULE_NAME, job=prune_old_runs_job, cron_schedule=SCHEDULE_CRON, default_status=DefaultScheduleStatus.RUNNING)

defs = Definitions(jobs=[prune_old_runs_job], schedules=[prune_old_runs_daily])
