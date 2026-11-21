import os
from pathlib import Path

from dagster import (
    Config,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    OpExecutionContext,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    job,
    op,
    sensor,
)

INBOX = Path(os.environ.get("DAGSTER_INBOX", "/opt/dagster/data/inbox"))
MARKER_SUFFIX = ".marker"


class PingConfig(Config):
    fail: bool = False


@op
def ping(context: OpExecutionContext, config: PingConfig) -> None:
    if config.fail:
        raise RuntimeError("ping was asked to fail")
    context.log.info("ping")


@job
def ping_job() -> None:
    ping()


ping_every_minute = ScheduleDefinition(name="ping_every_minute", job=ping_job, cron_schedule="* * * * *", default_status=DefaultScheduleStatus.RUNNING)


@sensor(job=ping_job, minimum_interval_seconds=5, default_status=DefaultSensorStatus.RUNNING)
def marker_file_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
    markers = sorted(INBOX.glob(f"*{MARKER_SUFFIX}")) if INBOX.is_dir() else []
    if not markers:
        return SkipReason(f"no {MARKER_SUFFIX} file in {INBOX}")
    marker = markers[0]
    return RunRequest(run_key=f"{marker.name}-{marker.stat().st_mtime_ns}", tags={"marker": marker.name})


defs = Definitions(jobs=[ping_job], schedules=[ping_every_minute], sensors=[marker_file_sensor])
