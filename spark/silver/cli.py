"""The command line of the Silver jobs (signals, pressure series): it writes its result as one file and prints fixed codes, and no error holds an argument, a
name or upstream text.

Every call takes ``--result <path>``, an absolute path that does not exist. The result is the one JSON document that the job writes there: it is written whole
to a file of another name in the same folder and then renamed, so a reader sees no file or the whole file, and a file that is already there is never written
over. A failed call writes a refusal document there instead (``{"job", "mode", "refusal"}``, the refusal being the codes that are printed on standard error).
Standard output stays empty, and standard error holds the codes for an operator: a caller reads the exit code and the file, never the streams.

The rules are those of the patient split job (``spark/split_cli.py``, whose settings, deadline and Nessie client this job also uses): the parser prints
``usage`` for any bad call (and writes no file, as it has no call to answer), a setting that cannot be used is ``settings``, an error from Nessie or from
anything else that is not mapped is one of ``branch_missing``, ``catalog_error`` and ``internal_error``, and a failed check prints one line for each problem,
each a code and a flag (``code: failed``), except a count of tables. A result that cannot be written is an ``internal_error``.
Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.silver.checks import SilverSignalsError
from spark.split_cli import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, reference_name

JOB = "silver_signals"


def result_path(text: str) -> Path:
    """The path of the result file: absolute and not there yet. Anything else is a bad call, and the text is never echoed."""
    path = Path(text)
    if not text or not path.is_absolute() or os.path.lexists(path):
        raise argparse.ArgumentTypeError("result")
    return path


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
        mode.add_argument("--result", required=True, type=result_path, help="the file to write the result to: an absolute path that does not exist")
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
        if name == "build":
            mode.add_argument("--base", default="main", type=reference_name, help="the branch to create the private branch from")
    return command


def write_result(path: Path, document: Any) -> None:
    """Write ``document`` as the whole file at ``path``: to a file of another name in the same folder, then renamed. A file that is there is never replaced."""
    text = json.dumps(document, sort_keys=True, allow_nan=False) + "\n"
    descriptor, name = tempfile.mkstemp(prefix=".result-", suffix=".part", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(name, 0o644)
        if os.path.lexists(path):
            raise FileExistsError
        os.rename(name, path)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str = JOB) -> int:
    """Parse the call, run ``execute`` and write its result to the file that the call names. Every code is printed after the name of ``job``."""
    arguments = parser(job).parse_args(argv)

    def written(document: Any) -> bool:
        try:
            write_result(arguments.result, document)
        except OSError, TypeError, ValueError:
            return False
        return True

    def refuse(code: int, codes: list[str]) -> int:
        """Print the codes and write them as the refusal document. A refusal that cannot be written is an ``internal_error``."""
        for problem in codes:
            sys.stderr.write(f"{job}: {problem}\n")
        if written({"job": job, "mode": arguments.mode, "refusal": codes}):
            return code
        if codes != ["internal_error"]:
            sys.stderr.write(f"{job}: internal_error\n")
        return INTERNAL_EXIT_CODE

    try:
        result = execute(arguments)
    except SilverSignalsError as error:
        return refuse(1, list(error.problems))
    except NessieError as error:
        return refuse(1, ["branch_missing" if error.status == NOT_FOUND and error.error_code == REFERENCE_NOT_FOUND else "catalog_error"])
    except SettingsError:
        return refuse(2, ["settings"])
    except Exception:
        return refuse(INTERNAL_EXIT_CODE, ["internal_error"])
    return 0 if written(result) else refuse(INTERNAL_EXIT_CODE, ["internal_error"])
