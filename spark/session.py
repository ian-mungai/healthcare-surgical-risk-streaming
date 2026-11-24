"""The Spark session of a job: one local JVM whose memory comes from the container's limit and whose catalog is Nessie's, on one branch.

Everything that differs between hosts comes from the environment, which the Compose service sets: ``NESSIE_URI``, ``WAREHOUSE_LOCATION`` and,
for an s3 warehouse, ``AWS_REGION``. A job never types an address, a bucket or a memory figure.
"""

from __future__ import annotations

import importlib
import os
import sys
import threading
from typing import Any

from spark.catalog import environment_catalog_options
from spark.result_file import release_reserved
from spark.settings import driver_memory_mib, read_memory_limit_mib

SCRATCH_FOLDER = "/scratch"
DEADLINE_EXIT_CODE = 124


def start_deadline(seconds: float, job: str) -> threading.Timer:
    """Kill the whole process when the job runs past ``seconds``. The JVM child ends with it, and so does the container. The message is the fixed code
    ``timeout`` and holds no duration. A part file that the job reserved for its result (``spark/result_file.py``) is removed first."""

    def expire() -> None:
        sys.stderr.write(f"{job}: timeout\n")
        sys.stderr.flush()
        release_reserved()
        os._exit(DEADLINE_EXIT_CODE)

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


def build_session(name: str, ref: str, ref_hash: str | None = None) -> Any:
    """A local Spark session on the Nessie reference ``ref`` (as of ``ref_hash`` when given) with the memory that the container's limit allows.

    PySpark is installed in the job image only, so it is imported by name here and the rest of the sources are checked without it.
    """
    SparkSession = importlib.import_module("pyspark.sql").SparkSession

    heap = f"{driver_memory_mib(read_memory_limit_mib())}m"
    options = environment_catalog_options(ref, ref_hash=ref_hash)
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
