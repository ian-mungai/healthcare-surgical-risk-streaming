"""The command line of the patient split job: it prints fixed codes and a result, never an argument, a branch name or the text of an upstream error.

The test set is sealed, and an argument or an upstream message can carry a name that the caller chose or a server chose. So the parser prints ``usage`` and
nothing else for any bad call, and an error from Nessie or from the settings is one of ``branch_missing``, ``catalog_error`` and ``settings``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.split_table import PatientSplitError

JOB = "patient_split"
RESULT_PREFIX = "RESULT "
NOT_FOUND = 404


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


def report(message: str) -> None:
    sys.stderr.write(f"{JOB}: {message}\n")


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, run ``execute`` and print its result line. Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting."""
    arguments = parser().parse_args(argv)
    try:
        result = execute(arguments)
    except PatientSplitError as error:
        for problem in error.problems:
            report(problem)
        return 1
    except NessieError as error:
        report("branch_missing" if error.status == NOT_FOUND else "catalog_error")
        return 1
    except SettingsError:
        report("settings")
        return 2
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0
