"""The three numbers that Terraform needs from the limits file to calculate the heavy-job figure.

The limits loader is the one reader of ``compose/limits.yml``, so the file is validated once and Terraform reads no YAML of its own. ``main`` prints one JSON
object with exactly these positive whole numbers, in MiB: ``host_reserve_mib``, ``always_on_limits_mib`` (the sum of the limits of the services of the
always-on groups, as ``scripts/stack/groups.py`` defines them) and ``spark_limit_mib`` (the largest limit of the Spark job services, because one job runs at
a time). The Terraform wrapper stages the object as ``heavy_inputs.tfvars.json`` in each run's copy of the configuration.

``.venv/bin/python -m scripts.stack.heavy_inputs`` prints the JSON.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence

from scripts.ingestion.notice import describe_error
from scripts.stack.groups import ALWAYS_ON_GROUPS
from scripts.stack.job_workloads import SERVICE_WORKLOADS
from scripts.stack.limits import LIMITS_FILE, Limits, LimitsError, load_limits

SPARK_SERVICES = tuple(f"spark_{workload}" for workload in SERVICE_WORKLOADS)


def heavy_inputs_document(limits: Limits) -> dict[str, int]:
    """The three numbers from checked limits, or ``LimitsError`` naming what the file lacks."""
    always_on = [entry.memory_limit_mib for entry in limits.services.values() if entry.group in ALWAYS_ON_GROUPS]
    if not always_on:
        raise LimitsError(f"the limits file holds no service of the always-on groups ({', '.join(sorted(ALWAYS_ON_GROUPS))})")
    missing = [name for name in SPARK_SERVICES if name not in limits.services]
    if missing:
        raise LimitsError(f"the limits file holds no service {', '.join(missing)}, whose limit is a Spark job's")
    return {
        "host_reserve_mib": limits.host_reserve_mib,
        "always_on_limits_mib": sum(always_on),
        "spark_limit_mib": max(limits.services[name].memory_limit_mib for name in SPARK_SERVICES),
    }


def heavy_inputs_text(limits: Limits) -> str:
    return json.dumps(heavy_inputs_document(limits), indent=2, sort_keys=True) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    if list(sys.argv[1:] if argv is None else argv):
        sys.stderr.write("usage: python -m scripts.stack.heavy_inputs\n")
        return 2
    try:
        text = heavy_inputs_text(load_limits(LIMITS_FILE))
    except LimitsError as error:
        sys.stderr.write(f"limits: {describe_error(error)}\n")
        return 1
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
