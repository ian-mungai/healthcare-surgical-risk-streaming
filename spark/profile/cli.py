"""The command line of the signal profile job: it prints fixed codes and a result, and no error holds an argument, a name or upstream text.

The rules are those of the Silver signals job (``spark/silver/cli.py``, whose settings, deadline and Nessie client this job also uses): the parser prints
``usage`` for any bad call, a setting that cannot be used is ``settings``, an error from Nessie or from anything else that is not mapped is one of
``branch_missing``, ``catalog_error`` and ``internal_error``, and a failed check prints one line for each problem, each a code and a count.
Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieError
from spark.profile.checks import SignalProfileError
from spark.settings import SettingsError
from spark.split_cli import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, reference_name

JOB = "signal_profile"
RESULT_PREFIX = "RESULT "


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the development signal profile tables on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the tables on it"), ("verify", "check the tables that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="the full cohort: judge every prespecified source count")
        if name == "build":
            mode.add_argument("--base", default="main", type=reference_name, help="the branch to create the private branch from")
    return command


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, run ``execute`` and print its result line."""
    arguments = parser().parse_args(argv)
    try:
        result = execute(arguments)
    except SignalProfileError as error:
        for problem in error.problems:
            sys.stderr.write(f"{JOB}: {problem}\n")
        return 1
    except NessieError as error:
        sys.stderr.write(f"{JOB}: {'branch_missing' if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else 'catalog_error'}\n")
        return 1
    except SettingsError:
        sys.stderr.write(f"{JOB}: settings\n")
        return 2
    except Exception:
        sys.stderr.write(f"{JOB}: internal_error\n")
        return INTERNAL_EXIT_CODE
    sys.stdout.write(RESULT_PREFIX + json.dumps(result, sort_keys=True) + "\n")
    return 0
