"""The command line of the reconciliation job: it writes its result to one file and prints fixed codes, and no error holds an argument, a name or upstream
text.

``python -m spark.jobs.silver_reconcile verify --branch <name> --result <path>``. The result reaches the caller through the file and through nothing else: the
caller reads no line of the job's standard output or standard error, which stay for the operator (the Spark launcher writes its own lines to them). The path
is absolute, its folder is there and the path does not exist, so that a file is never replaced. The job writes the whole document with ``write_new_file``
(``scripts/ingestion/output_file.py``), so that a reader sees no file or the whole file. Every ending that the job maps to a code writes one: a clean
run (outcome ``verified``, with the facts of the run), a failed check (outcome ``failed``, one diagnostic for each cause, each a code with a count or with no
count, and the flag of each table when every flag was judged), and a failed upstream, setting or other error (outcome ``failed`` and its one code). A bad call,
and a job that the watchdog stops, write none.

The rules are those of the patient split job (``spark/split_cli.py``, whose settings, deadline and Nessie client this job also uses): the parser
prints ``usage`` for any bad call, a setting that cannot be used is ``settings``, an error from Nessie or from anything else that is not mapped is one
of ``branch_missing``, ``catalog_error`` and ``internal_error``, and a failed check prints one line for each problem, each a code and a count. The job
only reads, so it has one mode. Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.ingestion.output_file import write_new_file
from scripts.stack.nessie import NessieError
from spark.internal_diagnostic import internal_error_line
from spark.reconcile.checks import ReconcileError
from spark.settings import SettingsError
from spark.split_cli import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, reference_name

JOB = "silver_reconcile"


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


RESULT_PATH_LIMIT = 4096
FILE_MODE = 0o644


def result_path(text: str) -> str:
    """An absolute path of a sane length with no NUL. Its folder and the absence of the file are checked by ``usable``."""
    if not text.startswith("/") or "\x00" in text or len(text) > RESULT_PATH_LIMIT or text.endswith("/"):
        raise argparse.ArgumentTypeError("usage")
    return text


class Once(argparse.Action):
    """An option that may be given once."""

    def __call__(self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None) -> None:
        if getattr(namespace, self.dest, None) is not None:
            parser.error("usage")
        setattr(namespace, self.dest, values)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Reconcile the Silver tables with Bronze by key, on the hash of a Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    mode = modes.add_parser("verify", help="compare every Bronze key with the Silver tables that the branch holds and write nothing but the result file")
    mode.add_argument("--branch", required=True, type=reference_name, help="the branch to check")
    mode.add_argument("--result", required=True, type=result_path, action=Once, help="the absolute path of the result file, which must not exist")
    return command


def usable(path: str) -> bool:
    """Whether the result can be written there without replacing anything: the folder is there and the path is not."""
    return os.path.isdir(os.path.dirname(path)) and not os.path.lexists(path)


def write_result(path: str, document: dict[str, Any]) -> None:
    """Write the whole document to a new file at ``path`` (``write_new_file``: a path that exists in any form is refused, and a failed write leaves no result
    file and no part file when the part name can be removed; a part name that cannot be removed stays, after a failed link as after a link that worked
    (beside the whole file), and the call raises)."""
    write_new_file(Path(path), json.dumps(document, sort_keys=True) + "\n", mode=FILE_MODE)


def diagnostic(problem: str) -> dict[str, Any]:
    """One problem of a check as its code and its count, or ``None`` for a count when the message has none: ``cases_key_unaccounted: 3 keys`` is
    ``{"code": "cases_key_unaccounted", "count": 3}`` and ``lab_results_held_out: a held-out key is not accounted for`` is ``{"code": "lab_results_held_out",
    "count": None}``."""
    code, _, tail = problem.partition(": ")
    first = tail.partition(" ")[0]
    return {"code": code, "count": int(first) if first.isascii() and first.isdigit() else None}


def failure(arguments: argparse.Namespace, problems: Sequence[str], held_out_clean: dict[str, bool] | None = None) -> dict[str, Any]:
    """The document of an ending that is not a clean run."""
    return {
        "mode": arguments.mode,
        "branch": arguments.branch,
        "job": JOB,
        "outcome": "failed",
        "diagnostics": [diagnostic(problem) for problem in problems],
        "held_out_clean": held_out_clean,
    }


def ending(arguments: argparse.Namespace, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> tuple[int, dict[str, Any], list[str]]:
    """Run ``execute`` and return the exit code, the document of the result file and the lines for standard error."""
    try:
        result = execute(arguments)
    except ReconcileError as error:
        return 1, failure(arguments, error.problems, error.held_out_clean), error.problems
    except NessieError as error:
        code = "branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error"
        return 1, failure(arguments, [code]), [code]
    except SettingsError:
        return 2, failure(arguments, ["settings"]), ["settings"]
    except Exception as error:
        return INTERNAL_EXIT_CODE, failure(arguments, ["internal_error"]), ["internal_error", internal_error_line(error)]
    return 0, {**result, "diagnostics": []}, []


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, run ``execute``, write the result file and then print its operator lines. A path that cannot hold a result is a bad call and nothing
    runs. A result that cannot be written is ``internal_error``."""
    arguments = parser().parse_args(argv)
    if not usable(arguments.result):
        sys.stderr.write(f"{JOB}: usage\n")
        return 2
    code, document, problems = ending(arguments, execute)
    try:
        write_result(arguments.result, document)
    except Exception as error:
        diagnostic = internal_error_line(error)
        sys.stderr.write(f"{JOB}: internal_error\n{JOB}: {diagnostic}\n")
        return INTERNAL_EXIT_CODE
    for problem in problems:
        sys.stderr.write(f"{JOB}: {problem}\n")
    return code
