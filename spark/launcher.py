"""The entry point of every Spark job service: run one job module, and only when the job is mapped to the service's workload.

``python -m spark.launcher <job> [job arguments]`` is the entry point of the services ``spark_bronze``, ``spark_silver``, ``spark_gold`` and
``spark_maintenance``. Each service sets ``SPARK_WORKLOAD`` to its workload, which is the AWS identity that its credentials hold. The launcher asks
``scripts.stack.job_workloads.refusal_of`` whether ``spark.jobs.<job>`` belongs to that workload. It refuses before the job's module is imported, so Spark has
not started and no job runs under the identity of another layer. With no job, as when a container of an older version of the service starts again, it
prints its usage and exits 2: nothing runs, so a start of the service never rewrites a table. A refusal is one fixed code on standard error, with no argument.
"""

from __future__ import annotations

import os
import runpy
import sys
from collections.abc import Mapping, Sequence

from scripts.stack.job_workloads import refusal_of

WORKLOAD_VARIABLE = "SPARK_WORKLOAD"
REFUSED_EXIT_CODE = 2


def refuse(code: str) -> int:
    sys.stderr.write(f"spark_launcher: {code}\n")
    return REFUSED_EXIT_CODE


def main(argv: Sequence[str] | None = None, environment: Mapping[str, str] | None = None) -> int:
    """Run the job named first in ``argv`` as the main program with the other arguments, or return the exit status 2 and print one fixed code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        return refuse("usage")
    job, *rest = arguments
    code = refusal_of(job, (os.environ if environment is None else environment).get(WORKLOAD_VARIABLE))
    if code is not None:
        return refuse(code)
    module = f"spark.jobs.{job}"
    sys.argv = [module, *rest]
    runpy.run_module(module, run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
