"""Run one Spark job by hand through the one Spark slot: ``python -m scripts.stack.spark_run [--wait-seconds N] [--deadline-seconds N] <job> [arguments]``.

The job starts as ``docker compose run`` in the service and profile of its workload (``scripts/stack/job_workloads.py``), through the service's own entry point,
which refuses a job that is not mapped to its workload. The command holds the one Spark slot (``scripts/stack/spark_slot.py``) until the container exits, so it
does not start while any Spark service runs, and the table checks one-off, which takes the same lock file, does not start beside it. Run it in the code
location container, where that lock file is:

    docker compose exec dagster_code_location python -m scripts.stack.spark_run table_maintenance --dry-run

Exit status: the container's own exit status, 124 for a job that passed its deadline, 1 for a refusal (the slot is taken or cannot be claimed, a setting is
wrong or Docker failed) and 2 for wrong usage (no job, an unknown job or a job whose workload has no service). The job's output is written when it ends.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Collection, Sequence

from scripts.ingestion.closed import QuietParser
from scripts.ingestion.notice import describe_error
from scripts.process import TimeoutExpired, run_command
from scripts.stack.admission import AdmissionError
from scripts.stack.groups import GroupError, compose_arguments, rendered_environment, running_services, settings_from_environment
from scripts.stack.job_workloads import JOB_WORKLOADS, JobWorkloadError, job_command, profile_of_job
from scripts.stack.limits import LimitsError, load_limits
from scripts.stack.spark_slot import SlotError, spark_slot

DEADLINE_SECONDS = 3600.0
EXIT_TIMEOUT = 124


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog="python -m scripts.stack.spark_run", description=__doc__.split("\n\n")[0] if __doc__ else None)
    command.add_argument("--wait-seconds", type=float, default=0, help="how long to wait for a Spark job that runs to end (default 0: refuse)")
    command.add_argument("--deadline-seconds", type=float, default=DEADLINE_SECONDS, help="how long the job has to end before it is stopped")
    command.add_argument("job", help="the job module in spark/jobs")
    command.add_argument("arguments", nargs=argparse.REMAINDER, help="the job's own arguments")
    return command


def main(argv: Sequence[str] | None = None, *, running: Callable[[], Collection[str]] | None = None) -> int:
    """Run the job and return the exit status. ``running`` reads the running services and defaults to the stack's own."""
    arguments = parser().parse_args(argv)
    job: str = arguments.job
    try:
        if job not in JOB_WORKLOADS:
            sys.stderr.write("spark_run: unknown_job\n")
            return 2
        words = job_command(job, *arguments.arguments)
        profile = profile_of_job(job)
    except JobWorkloadError:
        sys.stderr.write("spark_run: no_service\n")
        return 2
    try:
        settings = settings_from_environment()
        limits = load_limits(settings.limits_file)
        command = compose_arguments(settings, profile, "run", "--rm", "--no-TTY", "--no-deps", *words)
        environment = rendered_environment(limits)
        with spark_slot(running or (lambda: running_services(settings, limits)), wait_seconds=arguments.wait_seconds):
            result = run_command("docker", command, timeout=arguments.deadline_seconds, env=environment)
    except TimeoutExpired:
        sys.stderr.write("spark_run: the job passed its deadline and was stopped\n")
        return EXIT_TIMEOUT
    except (SlotError, GroupError, AdmissionError, LimitsError) as error:
        refusal = str(error) if isinstance(error, (SlotError, GroupError)) else describe_error(error)  # these two hold fixed text and a service or folder name
        sys.stderr.write(f"spark_run: refused: {refusal}\n")
        return 1
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
