"""The ending of the Bronze clinical and laboratory job: its result file, its operator lines and its fixed codes. No Spark runs here.

``python -m spark.jobs.bronze_clinical_lab <write|verify|publish> --branch <name> --source-hash <hash> --result <path>``

The result reaches the caller through one file (``spark/result_file.py``) and through nothing else; the job's standard output and standard error are for the
operator. Every ending that the job maps to a code writes the file: a run (the result and a list of diagnostics, which is empty when every check passed and
holds ``checks_failed`` when one did) and a refusal (the job, the command, the branch, ``passed`` false and one diagnostic that is a fixed code). A bad call and
the watchdog write nothing. Both Bronze tables are sealed, so the result holds snapshot IDs, the names of the checks with a flag each and one pass flag for each
table. No error holds an argument, a name, a column, a count or the text of an upstream message.

Exit 0 when every check passed, 1 when one failed or Nessie or the publication refused, 2 for a bad call, a setting or a table that Bronze does not accept,
70 for anything else.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from typing import Any

from scripts.stack.nessie import NessieConflict, NessieError
from spark.bronze.publication import PublicationError
from spark.bronze_clinical_lab import BronzeRulesError
from spark.bronze_clinical_lab_run import JOB, NotABranch, SourceNotInHistory, parser
from spark.internal_diagnostic import internal_error_line
from spark.result_file import INTERNAL_EXIT_CODE, line_of, run_sealed
from spark.settings import SettingsError

CHECKS_FAILED = "checks_failed"
# Every code that a refusal can give, with its exit status. The runner of the job reads its result file against this list.
EXIT_CODES = {
    "publication_refused": 1,
    "source_hash_not_in_history": 1,
    "not_a_branch": 1,
    "branch_conflict": 1,
    "catalog_error": 1,
    "settings": 2,
    "columns_refused": 2,
    "source_table_missing": 2,
    "repeated_rows_over_limit": 2,
    "rules_broken": 2,
    "internal_error": INTERNAL_EXIT_CODE,
}


def code_of(error: Exception) -> tuple[int, str]:
    """The exit status and the one fixed code of an error that ended the job. The text of the error is never used."""
    if isinstance(error, PublicationError):
        return 1, "publication_refused"
    if isinstance(error, SourceNotInHistory):
        return 1, "source_hash_not_in_history"
    if isinstance(error, NotABranch):
        return 1, "not_a_branch"
    if isinstance(error, NessieConflict):
        return 1, "branch_conflict"
    if isinstance(error, NessieError):
        return 1, "catalog_error"
    if isinstance(error, SettingsError):
        return 2, "settings"
    if isinstance(error, BronzeRulesError):
        return 2, error.code
    return INTERNAL_EXIT_CODE, "internal_error"


def check_lines(result: dict[str, Any]) -> list[str]:
    """The lines for standard error of a run in which a check failed: the code, then each failed check by its table and its name, each with a flag."""
    failed = [
        f"{table['bronze_table'].rpartition('.')[2]}: {check['name']}"
        for table in result["tables"].values()
        for check in table["checks"]
        if not check["passed"]
    ]
    return [line_of(CHECKS_FAILED), *[line_of(name) for name in failed]]


def ending(arguments: argparse.Namespace, execute: Callable[[argparse.Namespace], dict[str, Any]], job: str = JOB) -> tuple[int, dict[str, Any], list[str]]:
    """Run ``execute`` and return the exit code, the document of the result file and the lines for standard error, each a fixed code and the flag (an
    ``internal_error`` adds the line of its classes and frames)."""
    try:
        result = execute(arguments)
    except Exception as error:
        status, code = code_of(error)
        document = {"job": JOB, "command": arguments.command, "branch": arguments.branch, "passed": False, "diagnostics": [{"code": code}]}
        return status, document, [line_of(code), *([internal_error_line(error)] if code == "internal_error" else [])]
    if result["passed"]:
        return 0, {**result, "diagnostics": []}, []
    return 1, {**result, "diagnostics": [{"code": CHECKS_FAILED}]}, check_lines(result)


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, reserve the result file, run ``execute``, write the result file and then print the operator lines (``run_sealed``). A path that cannot
    hold a result, its part file included, is a bad call and nothing runs. A result that cannot be written is ``internal_error``. No result line is printed."""
    return run_sealed(parser(), argv, execute, JOB, ending)
