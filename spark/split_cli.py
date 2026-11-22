"""The command line of the patient split job: it prints fixed codes and a result, never an argument, a branch name or the text of an upstream error.

The test set is sealed, and an argument or an upstream message can carry a name that the caller chose or a server chose. So an error never names the branch,
an argument, a setting or an upstream message: the parser prints ``usage`` for any bad call, and an error from Nessie, from the settings or from anything
else that is not mapped is one of ``branch_missing``, ``catalog_error``, ``settings`` and ``internal_error``. A successful result does name the branch.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieClient, NessieError
from spark.settings import SettingsError
from spark.split_table import PatientSplitError

JOB = "patient_split"
RESULT_PREFIX = "RESULT "
NOT_FOUND = 404
REFERENCE_NOT_FOUND = "REFERENCE_NOT_FOUND"
INTERNAL_EXIT_CODE = 70


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the patient split table on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the table on it"), ("verify", "check the table that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="the full cohort: require the shares and the exact counts and refuse a new patient")
        mode.add_argument("--fixed-sample", type=Path, help="the complete tracked candidate file of the fixed sample, to check each case's set")
        if name == "build":
            mode.add_argument("--base", default="main", help="the branch to create the private branch from")
    return command


def number_setting(name: str, default: float) -> float:
    """The positive number in the environment variable ``name``, or ``default`` when it is not set. Any other value is a ``SettingsError`` with no value."""
    text = os.environ.get(name)
    if text is None:
        return default
    try:
        value = float(text)
    except ValueError:
        raise SettingsError(f"{name} is not a number") from None
    if not math.isfinite(value) or value <= 0:
        raise SettingsError(f"{name} is not a positive number")
    return value


def open_client(uri: str, timeout_seconds: float) -> NessieClient:
    """The Nessie client for ``uri``. An address that cannot be parsed, such as a port that is not a number, is a ``SettingsError`` that names no value."""
    try:
        return NessieClient(uri, timeout_seconds)
    except ValueError:
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
