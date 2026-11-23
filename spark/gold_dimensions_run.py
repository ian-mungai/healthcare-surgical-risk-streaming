"""The sequence of the Gold dimensions job and its command line, with the Spark steps as parameters. This module imports no Spark, so the whole sequence runs on
stand-in steps in the fast tests.

``build`` runs every input check and the check of the members that the tables already hold before it writes anything, writes the six dimensions, and then checks
the written tables against their sources. A rerun that changes nothing writes nothing. ``verify`` runs the input checks and the output checks on what a branch
holds and writes nothing. The merge or the publication of the branch is the caller's step.

The test set is sealed. A message or the result holds codes, dimension names and counts, and never a case, a patient, a key or an argument. The command line
prints fixed codes only: ``usage``, ``settings``, ``branch_missing``, ``catalog_error``, ``internal_error`` and the codes of a failed check.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from scripts.stack.nessie import NessieError
from spark.gold_dimensions import DIMENSIONS, Dimension, GoldDimensionsError, frozen_problems, input_problems, output_problems
from spark.settings import SettingsError
from spark.silver_cases import unit
from spark.split_cli import INTERNAL_EXIT_CODE, NOT_FOUND, REFERENCE_NOT_FOUND, RESULT_PREFIX, reference_name

JOB = "gold_dimensions"
CHECKS = [
    "input_sources",
    "frozen_members",
    "output_schema",
    "unknown_member",
    "key_unique",
    "key_in_range",
    "key_rule",
    "natural_key",
    "coverage",
    "member_values",
]


class DimensionOps(Protocol):
    """The Spark steps of the job. A write gives ``True`` when it changed the table and ``False`` when the table already held the rows."""

    def source_facts(self) -> dict[str, dict[str, int]]: ...

    def held_changes(self) -> dict[str, tuple[int, int]]: ...

    def write(self, item: Dimension) -> bool: ...

    def tables_missing(self) -> int: ...

    def output_schema_tables_wrong(self) -> int: ...

    def table_facts(self) -> dict[str, dict[str, int]]: ...

    def snapshot_ids(self) -> dict[str, int | None]: ...


@dataclass(frozen=True, slots=True)
class Outcome:
    outcome: str
    written: dict[str, bool]
    output: dict[str, dict[str, int]]
    snapshot_ids: dict[str, int | None]

    def facts(self) -> dict[str, Any]:
        """What the result line holds: the outcome, the tables written, the checks that ran, the members of each dimension (a count) and the snapshot IDs."""
        return {
            "outcome": self.outcome,
            "written": self.written,
            "checks_run": CHECKS,
            "members": {name: facts["members"] for name, facts in self.output.items()},
            "snapshot_ids": self.snapshot_ids,
        }


def checked_inputs(ops: DimensionOps) -> None:
    """Run every input check, or raise every cause."""
    problems = input_problems(ops.source_facts())
    if problems:
        raise GoldDimensionsError(*problems)


def check_stored_schema(ops: DimensionOps) -> None:
    """Refuse a written table with a dropped, extra or retyped column before any query reads a value of it."""
    wrong = ops.output_schema_tables_wrong()
    if wrong:
        raise GoldDimensionsError(f"output_schema: {unit(wrong, 'table')}")


def checked_output(ops: DimensionOps) -> dict[str, dict[str, int]]:
    check_stored_schema(ops)
    output = ops.table_facts()
    problems = output_problems(output)
    if problems:
        raise GoldDimensionsError(*problems)
    return output


def run_build(ops: DimensionOps) -> Outcome:
    """Check the inputs and the held members, write the dimensions that change, then check what was written."""
    checked_inputs(ops)
    problems = frozen_problems(ops.held_changes())
    if problems:
        raise GoldDimensionsError(*problems)
    written = {item.name: ops.write(item) for item in DIMENSIONS}
    output = checked_output(ops)
    return Outcome("written" if any(written.values()) else "unchanged", written, output, ops.snapshot_ids())


def run_verify(ops: DimensionOps) -> Outcome:
    """Run the input checks and the output checks on what the branch holds."""
    checked_inputs(ops)
    missing = ops.tables_missing()
    if missing:
        raise GoldDimensionsError(f"table_missing: {unit(missing, 'table')}")
    output = checked_output(ops)
    return Outcome("verified", {item.name: False for item in DIMENSIONS}, output, ops.snapshot_ids())


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog=f"python -m spark.jobs.{JOB}", description="Build or verify the Gold dimensions on a private Nessie branch.")
    modes = command.add_subparsers(dest="mode", required=True)
    for name, text in (
        ("build", "create the branch from the base and write the six Gold dimensions on it"),
        ("verify", "check the Gold dimensions that the branch holds"),
    ):
        mode = modes.add_parser(name, help=text)
        mode.add_argument("--branch", required=True, type=reference_name, help="the private branch" + (" to create" if name == "build" else " to check"))
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
    except GoldDimensionsError as error:
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
