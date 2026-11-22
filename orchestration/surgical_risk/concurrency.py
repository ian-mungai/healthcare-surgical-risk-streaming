"""Which jobs are heavy, and the run tag that holds heavy jobs to one at a time.

A job runs one service group. It is heavy when the memory limits of that group's services in the limits file add up to more than ``HEAVY_MEMORY_MIB``.
A heavy job carries the tag ``HEAVY_TAG_KEY``. The instance setting ``concurrency.runs.tag_concurrency_limits`` in ``dagster.yaml`` lets one run with that tag
be in progress, so a second heavy job stays queued until the first ends. A job below the figure has no tag and is never held.

Each job that runs a service group is declared in ``JOB_GROUPS`` with its group, and takes its tags from ``job_tags``. A fast test holds every group named
there to the limits file. The record load is the first.

The limits file is read when the code location starts. After the file changes, restart the code location so that the tags follow it.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

import yaml

# The one setting, the owner's figure: a job above this many MiB is heavy. It is 32768 less the 4096 reserve, 12032 always-on and one 4096 Spark job.
HEAVY_MEMORY_MIB = 12544
HEAVY_TAG_KEY = "surgical_risk/heavy"
HEAVY_TAG_VALUE = "true"
LIMITS_FILE = Path(os.environ.get("LIMITS_FILE", "/opt/dagster/limits.yml"))
# Each job that runs a service group, by job name, with the group of the limits file that it runs.
JOB_GROUPS: Mapping[str, str] = MappingProxyType({"record_load_job": "ingestion"})


class ConcurrencyError(ValueError):
    """The limits file or the group cannot be judged, so the job's weight is unknown."""


def group_memory_mib(group: str, limits_file: Path = LIMITS_FILE) -> int:
    """The sum of the memory limits, in MiB, of the services of ``group`` in the limits file."""
    try:
        document = yaml.safe_load(limits_file.read_text())
    except (OSError, yaml.YAMLError) as error:
        raise ConcurrencyError(f"cannot read the limits file {limits_file.name}: {error}") from error
    services = document.get("services") if isinstance(document, dict) else None
    if not isinstance(services, dict):
        raise ConcurrencyError(f"the limits file {limits_file.name} holds no services mapping")
    total = 0
    found = False
    for name, entry in services.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("memory_limit_mib"), int) or isinstance(entry.get("memory_limit_mib"), bool):
            raise ConcurrencyError(f"service {name} in {limits_file.name} has no whole-number memory_limit_mib")
        if entry.get("group") == group:
            found = True
            total += entry["memory_limit_mib"]
    if not found:
        raise ConcurrencyError(f"the limits file {limits_file.name} holds no service of the group {group}")
    return total


def heavy_job_tags(group: str, limits_file: Path = LIMITS_FILE) -> dict[str, str]:
    """The run tags for a job that runs ``group``: the heavy tag above the figure, none at or below it."""
    return {HEAVY_TAG_KEY: HEAVY_TAG_VALUE} if group_memory_mib(group, limits_file) > HEAVY_MEMORY_MIB else {}


def unknown_groups(job_groups: Mapping[str, str], limits_file: Path = LIMITS_FILE) -> list[str]:
    """Each declared job, as ``job: group``, whose group the limits file does not hold, in job order."""
    unknown = []
    for job, group in sorted(job_groups.items()):
        try:
            group_memory_mib(group, limits_file)
        except ConcurrencyError:
            unknown.append(f"{job}: {group}")
    return unknown


def job_tags(job: str, limits_file: Path = LIMITS_FILE, job_groups: Mapping[str, str] = JOB_GROUPS) -> dict[str, str]:
    """The run tags of a declared group job. Raise ``ConcurrencyError`` for a job that ``job_groups`` does not declare."""
    if job not in job_groups:
        raise ConcurrencyError(f"the job {job} is not declared in JOB_GROUPS, so its group and weight are unknown")
    return heavy_job_tags(job_groups[job], limits_file)
