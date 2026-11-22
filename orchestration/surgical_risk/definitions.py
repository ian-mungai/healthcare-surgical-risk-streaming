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
    in_process_executor,
    job,
    op,
    sensor,
)

from . import housekeeping, lineage_sample
from .concurrency import heavy_memory_mib
from .groups import GroupResource
from .run_records import run_folder_sensors

# The figure above which a job is heavy is read when the code location loads: a missing or invalid one fails the load with its reason, not a later launch.
heavy_memory_mib()

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


ping_every_minute = ScheduleDefinition(name="ping_every_minute", job=ping_job, cron_schedule="* * * * *", default_status=DefaultScheduleStatus.STOPPED)


class GroupProbeConfig(Config):
    fail: bool = False


@op
def group_probe(context: OpExecutionContext, config: GroupProbeConfig, group: GroupResource) -> None:
    context.log.info(f"group {group.group} while the step runs: {group.services_text()}")
    if config.fail:
        raise RuntimeError("group_probe was asked to fail")


@job(resource_defs={"group": GroupResource.configure_at_launch()}, executor_def=in_process_executor)
def group_probe_job() -> None:
    group_probe()


@sensor(job=ping_job, minimum_interval_seconds=5, default_status=DefaultSensorStatus.RUNNING)
def marker_file_sensor(context: SensorEvaluationContext) -> RunRequest | SkipReason:
    markers = sorted(INBOX.glob(f"*{MARKER_SUFFIX}")) if INBOX.is_dir() else []
    if not markers:
        return SkipReason(f"no {MARKER_SUFFIX} file in {INBOX}")
    marker = markers[0]
    return RunRequest(run_key=f"{marker.name}-{marker.stat().st_mtime_ns}", tags={"marker": marker.name})


# Dagster loads a code location from the one Definitions object at module scope, so the parts are merged here and none of them is a module-scope name.
defs = Definitions.merge(
    Definitions(jobs=[ping_job, group_probe_job], schedules=[ping_every_minute], sensors=[marker_file_sensor]),
    lineage_sample.defs,
    housekeeping.defs,
    Definitions(sensors=run_folder_sensors),
)
