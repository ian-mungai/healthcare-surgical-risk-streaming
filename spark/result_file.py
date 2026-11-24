"""The one file that a sealed job writes its result to: ``--result <absolute path>``.

The caller reads that file with a closed schema and no line of the job's standard output or standard error, which stay for the operator (the Spark launcher
writes its own lines to them). The path is absolute, its folder is there, and neither the path nor ``<path>.part`` exists, so a file is never replaced. The job
reserves ``<path>.part`` (an exclusive create) before any work, so a path that cannot hold it (a name of 255 bytes) is a bad call, ``usage``, and nothing runs.
It writes the whole document to the reserved file, flushes it and renames it to ``<path>``, so that a reader sees no file or the whole file. A watchdog calls
``release_reserved`` so that the job it stops leaves no part file. The pattern is that of ``spark/reconcile/cli.py``. ``run_sealed`` is the whole ending of a
job whose failures are problems of the form ``code: failed`` (the patient split job and the Silver cases job): every line that it prints for a refusal is a
fixed code and the flag, never a count. This module imports no Spark.
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
RESULT_PREFIX = "RESULT "  # the line of the Silver signal and hypotension jobs (``spark/silver/cli.py``); a sealed job prints no result line
FLAG = "failed"
NOT_FOUND = 404
REFERENCE_NOT_FOUND = "REFERENCE_NOT_FOUND"
INTERNAL_EXIT_CODE = 70


class SealedError(ValueError):
    """A rule was broken or a check failed. ``problems`` holds one message for each cause, each a code and a flag (``code: failed``)."""

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


RESERVED: list[str] = []


def reserve(path: str) -> int | None:
    """Reserve the result: create ``<path>.part`` exclusively and return its descriptor, or ``None`` when the result cannot be written there.

    ``None`` is every case that makes the call a bad one: the folder is missing, the path or its part file exists (also a dangling link or a folder), the part
    file's name is too long (a name of 255 bytes passes ``result_path``, but ``<name>.part`` does not) or the folder cannot be written. Nothing is replaced."""
    part = path + ".part"
    if not os.path.isdir(os.path.dirname(path)) or os.path.lexists(path):
        return None
    try:
        descriptor = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, FILE_MODE)
    except OSError:
        return None
    RESERVED.append(part)
    return descriptor


def release_reserved() -> None:
    """Remove every part file that this process reserved and did not rename. The watchdog and the end of a call use it, so that no part file is left."""
    while RESERVED:
        with contextlib.suppress(OSError):
            os.unlink(RESERVED.pop())


def write_reserved(descriptor: int, path: str, document: dict[str, Any]) -> None:
    """Write the whole document to the reserved part file and rename it to ``path``. A failure removes the part file."""
    part = path + ".part"
    try:
        text = json.dumps(document, sort_keys=True) + "\n"
        with os.fdopen(descriptor, "w") as handle:
            os.fchmod(handle.fileno(), FILE_MODE)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(part, path)
    except BaseException:
        release_reserved()
        raise
    RESERVED.remove(part)


def problem_code(problem: str) -> str:
    """The code of a problem: the text before the colon. Whatever follows it is never written to the result file or a stream."""
    return problem.partition(":")[0]


def line_of(code: str) -> str:
    """The one form of every refusal that a stream shows: the fixed code and the flag."""
    return f"{code}: {FLAG}"


def failure(job: str, arguments: argparse.Namespace, codes: Sequence[str]) -> dict[str, Any]:
    """The document of an ending that is not a clean run: the mode, the branch that the caller asked for, the job and one diagnostic for each code."""
    return {"mode": arguments.mode, "branch": arguments.branch, "job": job, "diagnostics": [{"code": code} for code in codes]}


def ending(arguments: argparse.Namespace, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str) -> tuple[int, dict[str, Any], list[str]]:
    """Run ``execute`` and return the exit code, the document of the result file and the lines for standard error. No text of an error is used."""
    try:
        result = execute(arguments)
    except SealedError as error:
        codes = [problem_code(problem) for problem in error.problems]
        return 1, failure(job, arguments, codes), [line_of(code) for code in codes]
    except NessieError as error:
        code = "branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error"
        return 1, failure(job, arguments, [code]), [line_of(code)]
    except SettingsError:
        return 2, failure(job, arguments, ["settings"]), [line_of("settings")]
    except Exception:
        return INTERNAL_EXIT_CODE, failure(job, arguments, ["internal_error"]), [line_of("internal_error")]
    return 0, {**result, "diagnostics": []}, []


def run_sealed(
    command: argparse.ArgumentParser,
    argv: Sequence[str] | None,
    execute: Callable[[argparse.Namespace], dict[str, Any]],
    job: str,
    close: Callable[[argparse.Namespace, Callable[[argparse.Namespace], dict[str, Any]], str], tuple[int, dict[str, Any], list[str]]] = ending,
) -> int:
    """Parse the call, reserve the result file, run ``execute``, write the result file and then print the operator lines. A path that cannot hold a result is
    a bad call and nothing runs. A result that cannot be written is ``internal_error``. A bad call and the watchdog write no file and leave no part file. The
    job prints no result line: the caller reads the file. ``close`` maps the end of ``execute`` to the exit code, the document and the lines (a job with its
    own list of codes passes its own)."""
    arguments = command.parse_args(argv)
    descriptor = reserve(arguments.result)
    if descriptor is None:
        sys.stderr.write(f"{job}: usage\n")
        return 2
    try:
        status, document, lines = close(arguments, execute, job)
        try:
            write_reserved(descriptor, arguments.result, document)
        except Exception:
            sys.stderr.write(f"{job}: {line_of('internal_error')}\n")
            return INTERNAL_EXIT_CODE
    finally:
        release_reserved()
    for line in lines:
        sys.stderr.write(f"{job}: {line}\n")
    return status
