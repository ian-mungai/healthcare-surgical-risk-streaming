"""The Dagster job of the table checks: it runs after the load step and merges the load branch only when every loaded table matches its verified source.

The job's one op reads the ``<run ID>.load_record.json`` file that the load step wrote and calls ``scripts.ingestion.table_checks.run_table_checks``, which
holds the order (the gate, the Nessie branch at its recorded hash, the staged verified bytes, the Spark job, the merge) and saves ``table_checks.json`` against
the manifest's SHA-256 under ``results_root``. A failed check fails the run and merges nothing. The track list's pin comes from the tracked release file only.
The environment names what differs between hosts: ``S3_BUCKET``, ``AWS_PROFILE`` and ``AWS_REGION`` for the landing prefix and ``NESSIE_URI``. The staged files
are in the Docker volume ``table_check_staging`` of the Compose project, mounted here at ``STAGING_DIRECTORY`` and read-only for the Spark job. The job uses
the in-process executor.
"""

import os
from pathlib import Path

from dagster import Config, Definitions, Failure, OpExecutionContext, in_process_executor, job, op

from scripts.aws_cli import Aws
from scripts.ingestion import table_checks as checks
from scripts.ingestion.store import S3Store
from scripts.stack.groups import settings_from_environment
from scripts.stack.nessie import NessieClient
from spark.table_checks import EMPTY_FORM

JOB = "table_checks_job"
DEFAULT_DEADLINE_SECONDS = 2400.0
NESSIE_TIMEOUT_SECONDS = 60.0


class TableChecksConfig(Config):
    load_record: str
    results_root: str
    empty_form: str = EMPTY_FORM
    deadline_seconds: float = DEFAULT_DEADLINE_SECONDS


def required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise Failure(description=f"the environment variable {name} is not set")
    return value


@op
def table_checks_step(context: OpExecutionContext, config: TableChecksConfig) -> None:
    try:
        record = checks.parse_load_record(Path(config.load_record).read_text())
        pin = checks.read_track_pin()
        store = S3Store(Aws(required("AWS_PROFILE"), required("AWS_REGION")), required("S3_BUCKET"))
        settings = settings_from_environment()
        runner = checks.compose_runner(
            settings,
            volume=f"{settings.project}_{checks.STAGING_VOLUME}",
            directory=checks.STAGING_DIRECTORY,
            deadline_seconds=config.deadline_seconds,
        )
        outcome = checks.run_table_checks(
            store=store,
            nessie=NessieClient(required("NESSIE_URI"), NESSIE_TIMEOUT_SECONDS),
            record=record,
            track_pin=pin,
            staging=Path(checks.STAGING_DIRECTORY) / context.run_id,
            run_folder=Path(config.results_root) / record.manifest.sha256,
            runner=runner,
            empty_form=config.empty_form,
        )
    except OSError as error:
        raise Failure(description=f"the table checks could not run: a file could not be read or written ({type(error).__name__})") from None
    except checks.TableChecksError as error:
        raise Failure(description=f"the table checks could not run: {error}") from None
    tables = (outcome.document.get("checked") or {}).get("tables", [])
    failed = [f"{table['table']}: {check['name']}" for table in tables for check in table["checks"] if not check["passed"]]
    if not outcome.passed:
        raise Failure(description=f"a table check failed and nothing was merged: {', '.join(failed)}", metadata={"manifest_sha256": record.manifest.sha256})
    context.log.info(f"every check passed; {record.branch} merged into {outcome.document['merged_into']} at {outcome.document['merged_hash']}")


@job(executor_def=in_process_executor)
def table_checks_job() -> None:
    table_checks_step()


defs = Definitions(jobs=[table_checks_job])
