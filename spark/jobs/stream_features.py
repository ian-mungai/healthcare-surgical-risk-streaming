"""The streaming job: replayed case events from Kafka to feature rows in the lake and feature-ready events in Kafka.

``python -m spark.jobs.stream_features``. Settings come from the environment (``spark/streaming/settings.py``). The job writes the lake tables on
its own Nessie branch, which it creates from ``main`` when it is missing, and keeps its offsets and state in ``STREAM_CHECKPOINT_DIR``. With
``STREAM_MAX_SECONDS`` set it stops cleanly when that time has passed (a deadline for a test or a batch run) and prints one line that starts with
``RESULT ``. With it unset the job is the always-on service and runs until it is stopped. A stop signal stops the query between batches. A failed
query ends the process with exit status 1.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Any

from scripts.stack.nessie import NessieClient, NessieConflict, NessieError
from spark.catalog import required_environment
from spark.session import build_session
from spark.settings import SettingsError
from spark.streaming.clinical import ClinicalFileError, load_clinical
from spark.streaming.lake_sinks import LakeSinks
from spark.streaming.query import SESSION_OPTIONS, start_query, wait_for_query
from spark.streaming.settings import StreamSettingsError, from_environment

JOB = "stream_features"
RESULT_PREFIX = "RESULT "
WAIT_SLICE_SECONDS = 5.0
NESSIE_TIMEOUT_SECONDS = 30.0


def ensure_branch(client: NessieClient, branch: str) -> None:
    """Create the branch from ``main`` when it does not exist. A branch that exists is used as it is: a restart finds its tables there."""
    try:
        client.create_branch(branch)
    except NessieConflict:
        if client.reference(branch).kind != "BRANCH":
            raise NessieError(f"{branch} exists and is not a branch") from None


def main() -> int:
    try:
        settings = from_environment(os.environ)
        load_clinical(settings.clinical_file)
        client = NessieClient(required_environment("NESSIE_URI"), NESSIE_TIMEOUT_SECONDS)
        client.config()
        ensure_branch(client, settings.branch)
        from replay.service.registry import RegistryClient
        from replay.service.wire_codec import Writer

        writer = Writer(RegistryClient(settings.registry_url))
    except (StreamSettingsError, SettingsError, ClinicalFileError) as error:
        sys.stderr.write(f"{JOB}: {error}\n")
        return 2
    except NessieError as error:
        sys.stderr.write(f"{JOB}: cannot work with Nessie: {error}\n")
        return 1
    session = build_session(JOB, settings.branch, extra=SESSION_OPTIONS)
    sinks = LakeSinks(session, writer, settings.bootstrap, settings.output_topic)
    sinks.create_tables()
    query = start_query(session, sinks, settings)
    stopped = {"signal": False}

    def request_stop(signum: int, frame: Any) -> None:
        stopped["signal"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    deadline = time.monotonic() + settings.max_seconds if settings.max_seconds else None
    try:
        failure = wait_for_query(query, deadline, lambda: stopped["signal"], WAIT_SLICE_SECONDS)
    finally:
        progress = query.lastProgress
        query.stop()
        session.stop()
    if failure is not None:
        sys.stderr.write(f"{JOB}: the query failed: {type(failure).__name__}: {str(failure)[:500]}\n")
        return 1
    sys.stdout.write(RESULT_PREFIX + json.dumps({"branch": settings.branch, "last_progress": progress}, sort_keys=True, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
