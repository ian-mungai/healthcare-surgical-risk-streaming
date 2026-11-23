"""The sequence of the Silver cases job and its command line, with the Spark steps as parameters. This module imports no Spark, so the whole sequence runs on
stand-in steps in the fast tests.

``build`` runs every input check and the freeze before it writes anything, writes the four tables, and then checks the written tables against Bronze. A rerun
that changes nothing writes nothing. ``verify`` runs the same input checks and the output checks on what a branch holds and writes nothing. The merge of the
branch is the caller's step.

The test set is sealed. A message or the result holds codes, counts, table names and the cutoffs (measured on development patients), and never a case, a
patient or an argument. The command line prints fixed codes only: ``usage``, ``settings``, ``branch_missing``, ``catalog_error``, ``internal_error`` and the
codes of a failed check.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from scripts.stack.nessie import NessieError
from spark.settings import SettingsError
from spark.silver_cases import (
    CutoffRow,
    MeasureRow,
    SilverCasesError,
    cutoff_rows,
    frozen_problems,
    input_problems,
    measure_rows,
    output_problems,
    unit,
)
from spark.split_cli import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, RESULT_PREFIX, reference_name

JOB = "silver_cases"
TABLES = ("cases", "lab", "rules")
CHECKS_SAMPLE = [
    "key_not_null",
    "key_unique",
    "types_enforced",
    "domains",
    "operation_order",
    "split_join",
    "lab_case_join",
    "development_cases",
    "cutoffs_frozen",
    "row_counts",
    "no_value_changed",
    "occurrence_numbers",
    "never_feature_marking",
    "cutoffs_read_back",
]
CHECKS_BUILD = [*CHECKS_SAMPLE[:7], "pinned_counts", *CHECKS_SAMPLE[7:]]
RULES_READ_BACK = "readback_differs: the saved measure or cutoffs are not the ones that were written"


class SilverOps(Protocol):
    """The Spark steps of the job. ``input_facts`` holds the counts over Bronze and the split, ``output_facts`` the counts over the written tables, and
    ``development_lengths`` the operation lengths of the development patients only. A write gives ``True`` when it changed the table and ``False`` when the
    table already held the rows."""

    def input_facts(self) -> dict[str, object]: ...

    def development_lengths(self) -> list[int]: ...

    def existing_rules(self) -> tuple[list[MeasureRow] | None, list[CutoffRow] | None]: ...

    def write_cases(self) -> bool: ...

    def write_lab(self) -> bool: ...

    def write_rules(self, measure: list[MeasureRow], cutoffs: list[CutoffRow]) -> bool: ...

    def output_facts(self) -> dict[str, object]: ...

    def snapshot_ids(self) -> dict[str, int | None]: ...


@dataclass(frozen=True, slots=True)
class Outcome:
    outcome: str
    written: dict[str, bool]
    pinned: bool
    measure: list[MeasureRow]
    cutoffs: list[CutoffRow]
    output: dict[str, object]
    snapshot_ids: dict[str, int | None]

    def facts(self) -> dict[str, Any]:
        """What the result line holds: the outcome, which tables were written, the checks that ran, counts, the snapshot IDs, the measure and the cutoffs."""
        return {
            "outcome": self.outcome,
            "written": self.written,
            "checks_run": CHECKS_BUILD if self.pinned else CHECKS_SAMPLE,
            "cases": self.output["case_rows"],
            "lab_rows": self.output["lab_rows"],
            "lab_copies": self.output["lab_copies"],
            "development_cases": self.cutoffs[0].development_cases,
            "snapshot_ids": self.snapshot_ids,
            "measure": [asdict(row) for row in self.measure],
            "cutoffs": [asdict(row) for row in self.cutoffs],
        }


def checked_inputs(ops: SilverOps, *, pinned: bool) -> tuple[list[MeasureRow], list[CutoffRow]]:
    """Run every input check, then measure the development operation lengths and set the cutoffs, or raise every cause."""
    facts = ops.input_facts()
    lengths = ops.development_lengths()
    problems = input_problems(**facts, development_cases=len(lengths), pinned=pinned)  # type: ignore[arg-type]
    if problems:
        raise SilverCasesError(*problems)
    return measure_rows(lengths), cutoff_rows(lengths)


def run_build(ops: SilverOps, *, pinned: bool) -> Outcome:
    """Check the inputs and the freeze, write the tables that change, then check what was written and read the cutoffs back."""
    measure, cutoffs = checked_inputs(ops, pinned=pinned)
    held_measure, held_cutoffs = ops.existing_rules()
    problems = frozen_problems(held_measure, held_cutoffs, measure, cutoffs)
    if problems:
        raise SilverCasesError(*problems)
    written = {"cases": ops.write_cases(), "lab": ops.write_lab(), "rules": ops.write_rules(measure, cutoffs)}
    saved = ops.existing_rules()
    output = ops.output_facts()
    problems = output_problems(**output)  # type: ignore[arg-type]
    if saved != (measure, cutoffs):
        problems.append(RULES_READ_BACK)
    if problems:
        raise SilverCasesError(*problems)
    return Outcome("written" if any(written.values()) else "unchanged", written, pinned, measure, cutoffs, output, ops.snapshot_ids())


def run_verify(ops: SilverOps, *, pinned: bool) -> Outcome:
    """Run the input checks and the output checks on what the branch holds, and compare the saved cutoffs with the ones the development data gives."""
    measure, cutoffs = checked_inputs(ops, pinned=pinned)
    held_measure, held_cutoffs = ops.existing_rules()
    missing = (held_measure is None) + (held_cutoffs is None)
    problems = [f"table_missing: {unit(missing, 'table')}"] if missing else frozen_problems(held_measure, held_cutoffs, measure, cutoffs)
    output = ops.output_facts()
    problems += output_problems(**output)  # type: ignore[arg-type]
    if problems:
        raise SilverCasesError(*problems)
    return Outcome("verified", {name: False for name in TABLES}, pinned, measure, cutoffs, output, ops.snapshot_ids())


DEADLINE_EXIT_CODE = 124


def start_deadline(seconds: float, exit_with: Callable[[int], object] = os._exit) -> threading.Timer:
    """Stop the whole process, with the fixed code ``timeout`` and exit 124, when the job runs past ``seconds``. The message holds no duration."""

    def expire() -> None:
        sys.stderr.write(f"{JOB}: timeout\n")
        sys.stderr.flush()
        exit_with(DEADLINE_EXIT_CODE)

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify silver.cases and silver.lab_results on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (
        ("build", "create the branch from the base and write the Silver tables on it"),
        ("verify", "check the Silver tables that the branch holds"),
    ):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
        mode.add_argument("--pinned-release", action="store_true", help="the full release: require its exact row counts")
        if name == "build":
            mode.add_argument("--base", default="main", type=reference_name, help="the branch to create the private branch from")
    return command


def report(message: str) -> None:
    sys.stderr.write(f"{JOB}: {message}\n")


def run(argv: Sequence[str] | None, execute: Callable[[argparse.Namespace], dict[str, Any]]) -> int:
    """Parse the call, run ``execute`` and print its result line.

    Exit 0 on success, 1 on a failed check or a failed upstream, 2 on a bad call or setting, 70 on anything else.
    """
    arguments = parser().parse_args(argv)
    try:
        result = execute(arguments)
    except SilverCasesError as error:
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
