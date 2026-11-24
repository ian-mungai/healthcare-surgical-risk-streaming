"""The command line of the Silver jobs (signals, pressure series): it writes its result as one file and prints fixed codes, and no error holds an argument, a
name or upstream text.

Every call takes ``--result <path>``, an absolute path that does not exist. The job reserves ``<path>.part`` (a new file) before ``execute`` runs, so a
path that cannot hold its result (a folder that is not there, a part file that is there, a name of 255 bytes) is a bad call, ``usage``, and nothing runs. The
result is the one JSON document that the job writes to a private file in the folder and then links to the path, so a reader sees no file or the whole file, and
a file that is there is never written over (``spark/result_file.py``, which the watchdog uses to remove the part file of a job that it stops). A failed call
writes a refusal document there instead (``{"job", "mode", "refusal"}``, the refusal being the codes of its problems).
Standard output stays empty, and standard error holds the codes for an operator: a caller reads the exit code and the file, never the streams.

The rules are those of the patient split job (``spark/split_cli.py``, whose settings, deadline and Nessie client this job also uses): the parser prints
``usage`` for any bad call (and writes no file, as it has no call to answer), a setting that cannot be used is ``settings``, an error from Nessie or from
anything else that is not mapped is one of ``branch_missing``, ``catalog_error`` and ``internal_error``, and every refusal prints one line for each code, each
the job's name, the code and a flag (``<job>: <code>: failed``), never a count and never text after the code. A result that cannot be written is an
``internal_error``.
Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieError
from spark.result_file import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, add_result_option, line_of, problem_code, run_sealed
from spark.settings import SettingsError
from spark.silver.checks import SilverSignalsError
from spark.split_cli import reference_name

JOB = "silver_signals"


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``, after the name of its job."""

    def __init__(self, *args: Any, job: str = JOB, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.job = job

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{self.job}: usage\n")
        raise SystemExit(2)


def parser(job: str = JOB) -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{job}", description="Build or verify Silver tables on a private Nessie branch.", job=job)
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (("build", "create the branch from the base and write the tables on it"), ("verify", "check the tables that the branch holds")):
        mode = modes.add_parser(name, help=text)
        mode.job = job
        add_result_option(mode)
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
        if name == "build":
            mode.add_argument("--base", default="main", type=reference_name, help="the branch to create the private branch from")
    return command


def ending(arguments: argparse.Namespace, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str) -> tuple[int, dict[str, Any], list[str]]:
    """Run ``execute`` and return the exit code, the document of the result file and the lines for standard error. The document is the result, or the refusal
    with its codes. Every line is a code and the flag, and no text of an error is used. A result that is not JSON is an ``internal_error`` refusal."""

    def refused(status: int, codes: list[str]) -> tuple[int, dict[str, Any], list[str]]:
        return status, {"job": job, "mode": arguments.mode, "refusal": codes}, [line_of(problem_code(code)) for code in codes]

    try:
        result = execute(arguments)
        json.dumps(result, sort_keys=True, allow_nan=False)
    except SilverSignalsError as error:
        return refused(1, [line_of(problem_code(problem)) for problem in error.problems])
    except NessieError as error:
        return refused(1, ["branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error"])
    except SettingsError:
        return refused(2, ["settings"])
    except Exception:
        return refused(INTERNAL_EXIT_CODE, ["internal_error"])
    return 0, result, []


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str = JOB) -> int:
    """Parse the call, reserve the result file, run ``execute`` and write its result to the file that the call names (``run_sealed``). Every code is printed
    after the name of ``job``."""
    return run_sealed(parser(job), argv, execute, job, ending)
