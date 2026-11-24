"""The command line of the patient split job: it prints fixed codes and a result, and no error holds an argument, a name or upstream text.

The test set is sealed, and an argument or an upstream message can carry a name that the caller chose or a server chose. One rule for names: a successful
result names the branch, because the operator passed it, and no error names the branch, an argument, a setting, a duration or an upstream message. The
parser prints ``usage`` for any bad call, a setting that cannot be used is ``settings``, a deadline is ``timeout``, and an error from Nessie or from anything
else that is not mapped is one of ``branch_missing``, ``catalog_error`` and ``internal_error``.

Every local input (the arguments, every setting that the job reads, the candidate file) is read and validated before the first call to Nessie, so a bad
input never leaves a branch behind that blocks the retry.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ingestion.closed import QuietParser
from scripts.stack.nessie import NessieClient, NessieError
from spark.settings import SettingsError, catalog_options
from spark.split_table import PatientSplitError

JOB = "patient_split"
RESULT_PREFIX = "RESULT "
NOT_FOUND = 404
REFERENCE_NOT_FOUND = "REFERENCE_NOT_FOUND"
INTERNAL_EXIT_CODE = 70
DEADLINE_EXIT_CODE = 124
DEFAULT_DEADLINE_SECONDS = 900.0
MAX_DEADLINE_SECONDS = 86_400.0
DEFAULT_NESSIE_TIMEOUT_SECONDS = 30.0
MAX_NESSIE_TIMEOUT_SECONDS = 600.0
# Every environment variable that the job reads, here and in spark/session.py. A test holds this list to the source.
SETTING_NAMES = ("JOB_DEADLINE_SECONDS", "NESSIE_TIMEOUT_SECONDS", "NESSIE_URI", "WAREHOUSE_LOCATION", "AWS_REGION")
REFERENCE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


def reference_name(text: str) -> str:
    """A branch or base name that Nessie takes. Anything else is a usage error, before anything is called."""
    if REFERENCE_NAME.fullmatch(text) is None:
        raise argparse.ArgumentTypeError("not a branch name")
    return text


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the patient split table on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the table on it"), ("verify", "check the table that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="the full cohort: require the shares and the exact counts and refuse a new patient")
        mode.add_argument("--fixed-sample", type=Path, help="the complete tracked candidate file of the fixed sample, to check each case's set")
        if name == "build":
            mode.add_argument("--base", default="main", type=reference_name, help="the branch to create the private branch from")
    return command


def number_setting(name: str, default: float, maximum: float) -> float:
    """The number above 0 and up to ``maximum`` in the variable ``name``, or ``default`` when it is not set. Any other value is a ``SettingsError``."""
    text = os.environ.get(name)
    if text is None:
        return default
    try:
        value = float(text)
    except ValueError:
        raise SettingsError(f"{name} is not a number") from None
    if not math.isfinite(value) or not 0 < value <= maximum:
        raise SettingsError(f"{name} is not a number from 0 to {maximum:g}")
    return value


def required_setting(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SettingsError(f"the environment variable {name} is not set")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    nessie_uri: str
    warehouse: str
    region: str
    deadline_seconds: float
    nessie_timeout_seconds: float


def read_settings() -> Settings:
    """Every setting that the job reads, validated together, or a ``SettingsError``. The catalog settings are checked as Spark will check them."""
    deadline = number_setting("JOB_DEADLINE_SECONDS", DEFAULT_DEADLINE_SECONDS, MAX_DEADLINE_SECONDS)
    timeout = number_setting("NESSIE_TIMEOUT_SECONDS", DEFAULT_NESSIE_TIMEOUT_SECONDS, MAX_NESSIE_TIMEOUT_SECONDS)
    uri = required_setting("NESSIE_URI")
    warehouse = required_setting("WAREHOUSE_LOCATION")
    region = os.environ.get("AWS_REGION", "")
    catalog_options(nessie_uri=uri, warehouse=warehouse, ref="validate", region=region)
    return Settings(uri, warehouse, region, deadline, timeout)


def start_deadline(seconds: float, exit_with: Callable[[int], object] = os._exit, job: str = JOB) -> threading.Timer:
    """Stop the whole process, with the fixed code ``timeout`` and exit 124, when the job runs past ``seconds``. The message names ``job``, not a duration."""

    def expire() -> None:
        sys.stderr.write(f"{job}: timeout\n")
        sys.stderr.flush()
        exit_with(DEADLINE_EXIT_CODE)

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


def open_client(uri: str, timeout_seconds: float) -> NessieClient:
    """The Nessie client for ``uri``. An address that cannot be parsed, such as a port that is not a number, is a ``SettingsError`` that names no value."""
    try:
        return NessieClient(uri, timeout_seconds)
    except ValueError, NessieError:
        raise SettingsError("the Nessie address cannot be parsed") from None


def report(message: str) -> None:
    sys.stderr.write(f"{JOB}: {message}\n")


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, run ``execute`` and print its result line.

    Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else.
    """
    arguments = parser().parse_args(argv)
    try:
        result = execute(arguments)
    except PatientSplitError as error:
        for problem in error.problems:
            report(problem)
        return 1
    except NessieError as error:
        report("branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error")
        return 1
    except SettingsError:
        report("settings")
        return 2
    except Exception:
        report("internal_error")
        return INTERNAL_EXIT_CODE
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0
