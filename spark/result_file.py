"""The one file that a sealed job writes its result to: ``--result <absolute path>``.

The caller reads that file with a closed schema and no line of the job's standard output or standard error, which stay for the operator (the Spark launcher
writes its own lines to them). The path is absolute, its folder is there, and neither the path nor ``<path>.part`` exists, so a file is never replaced. The job
writes the whole document to ``<path>.part``, flushes it and renames it to ``<path>``, so that a reader sees no file or the whole file. The pattern is that of
``spark/reconcile/cli.py``. ``run_sealed`` is the whole ending of a job whose failures are problems of the form ``code: failed`` (the patient split job and the
Silver cases job). This module imports no Spark.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError

RESULT_PATH_LIMIT = 4096
FILE_MODE = 0o644
RESULT_PREFIX = "RESULT "
NOT_FOUND = 404
REFERENCE_NOT_FOUND = "REFERENCE_NOT_FOUND"
INTERNAL_EXIT_CODE = 70


class SealedError(ValueError):
    """A rule was broken or a check failed. ``problems`` holds one message for each cause, each a code and a flag (``code: failed``) or a count of tables."""

    def __init__(self, *problems: str) -> None:
        super().__init__("; ".join(problems))
        self.problems = list(problems)


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


def add_result_option(command: argparse.ArgumentParser) -> None:
    command.add_argument("--result", required=True, type=result_path, action=Once, help="the absolute path of the result file, which must not exist")


def usable(path: str) -> bool:
    """Whether the result can be written there without replacing anything: the folder is there and neither the path nor its part file is."""
    return os.path.isdir(os.path.dirname(path)) and not os.path.lexists(path) and not os.path.lexists(path + ".part")


def write_result(path: str, document: dict[str, Any]) -> None:
    """Write the whole document to ``<path>.part`` and rename it to ``<path>``. A part file that this call made and a failure left is removed."""
    part = path + ".part"
    text = json.dumps(document, sort_keys=True) + "\n"
    descriptor = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, FILE_MODE)
    try:
        with os.fdopen(descriptor, "w") as handle:
            os.fchmod(handle.fileno(), FILE_MODE)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(part, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(part)
        raise


def problem_code(problem: str) -> str:
    """The code of a problem: the text before the colon. A flag or a count of tables after it is never written to the result file."""
    return problem.partition(":")[0]


def failure(job: str, arguments: argparse.Namespace, codes: Sequence[str]) -> dict[str, Any]:
    """The document of an ending that is not a clean run: the mode, the branch that the caller asked for, the job and one diagnostic for each code."""
    return {"mode": arguments.mode, "branch": arguments.branch, "job": job, "diagnostics": [{"code": code} for code in codes]}


def ending(arguments: argparse.Namespace, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str) -> tuple[int, dict[str, Any], list[str]]:
    """Run ``execute`` and return the exit code, the document of the result file and the lines for standard error. No text of an error is used."""
    try:
        result = execute(arguments)
    except SealedError as error:
        return 1, failure(job, arguments, [problem_code(problem) for problem in error.problems]), error.problems
    except NessieError as error:
        code = "branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error"
        return 1, failure(job, arguments, [code]), [code]
    except SettingsError:
        return 2, failure(job, arguments, ["settings"]), ["settings"]
    except Exception:
        return INTERNAL_EXIT_CODE, failure(job, arguments, ["internal_error"]), ["internal_error"]
    return 0, {**result, "diagnostics": []}, []


def run_sealed(command: argparse.ArgumentParser, argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str) -> int:
    """Parse the call, run ``execute``, write the result file and then print the operator lines. A path that cannot hold a result is a bad call and nothing
    runs. A result that cannot be written is ``internal_error``. A bad call and the watchdog write no file."""
    arguments = command.parse_args(argv)
    if not usable(arguments.result):
        sys.stderr.write(f"{job}: usage\n")
        return 2
    status, document, lines = ending(arguments, execute, job)
    try:
        write_result(arguments.result, document)
    except Exception:
        sys.stderr.write(f"{job}: internal_error\n")
        return INTERNAL_EXIT_CODE
    for line in lines:
        sys.stderr.write(f"{job}: {line}\n")
    if status == 0:
        sys.stdout.write(RESULT_PREFIX + json.dumps(document, sort_keys=True) + "\n")
    return status
