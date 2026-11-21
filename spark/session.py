"""The Spark session of a job: one local JVM whose memory comes from the container's limit and whose catalog is Nessie's, on one branch.

Everything that differs between hosts comes from the environment, which the Compose service sets: ``NESSIE_URI``, ``WAREHOUSE_LOCATION`` and,
for an s3 warehouse, ``AWS_REGION``. A job never types an address, a bucket or a memory figure.
"""

from __future__ import annotations

import os
import sys
import threading
from typing import Any

from spark.settings import SettingsError, catalog_options, driver_memory_mib, read_memory_limit_mib

SCRATCH_FOLDER = "/scratch"
DEADLINE_EXIT_CODE = 124


def required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SettingsError(f"the environment variable {name} is not set")
    return value


def start_deadline(seconds: float, job: str) -> threading.Timer:
    """Kill the whole process when the job runs past ``seconds``. The JVM child ends with it, and so does the container."""

    def expire() -> None:
        sys.stderr.write(f"{job}: did not finish within {seconds:g} seconds and was stopped\n")
        sys.stderr.flush()
        os._exit(DEADLINE_EXIT_CODE)

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


def build_session(name: str, ref: str) -> Any:
    """A local Spark session on the Nessie reference ``ref`` with the memory that the container's limit allows."""
    from pyspark.sql import SparkSession

    heap = f"{driver_memory_mib(read_memory_limit_mib())}m"
    options = catalog_options(
        nessie_uri=required_environment("NESSIE_URI"),
        warehouse=required_environment("WAREHOUSE_LOCATION"),
        ref=ref,
        region=os.environ.get("AWS_REGION", ""),
    )
    builder = SparkSession.builder.appName(name).master("local[*]")
    for key, value in {
        **options,
        "spark.driver.memory": heap,
        "spark.executor.memory": heap,
        "spark.local.dir": SCRATCH_FOLDER,
        "spark.ui.enabled": "false",
    }.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
