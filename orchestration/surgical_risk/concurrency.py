"""Which jobs are heavy, and the run tag that holds heavy jobs to one at a time.

A job runs one service group. It is heavy when the memory limits of that group's services in the limits file add up to more than ``HEAVY_MEMORY_MIB``.
A heavy job carries the tag ``HEAVY_TAG_KEY``. The instance setting ``concurrency.runs.tag_concurrency_limits`` in ``dagster.yaml`` lets one run with that tag
be in progress, so a second heavy job stays queued until the first ends. A job below the figure has no tag and is never held.

The limits file is read when the code location starts. After the file changes, restart the code location so that the tags follow it.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

# The one setting: a job above this many MiB is heavy. A proposal for the owner to confirm: 4096 MiB, an eighth of the host's 32768 MiB.
HEAVY_MEMORY_MIB = 4096
HEAVY_TAG_KEY = "surgical_risk/heavy"
HEAVY_TAG_VALUE = "true"
LIMITS_FILE = Path(os.environ.get("LIMITS_FILE", "/opt/dagster/limits.yml"))


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
