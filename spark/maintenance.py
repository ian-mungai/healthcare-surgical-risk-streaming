"""The arguments and decisions of table maintenance, which only compacts: it deletes nothing before teardown.

The job rewrites data files and manifests on the trunk. The rewrites add files and a commit, and the files they replace stay on disk, because older snapshots
and the tags that point to them still need them. This module holds what the job decides from plain facts: its arguments, the branches that make it wait, the
table names it accepts and the comparison of what every tag reads before and after. It imports nothing from Spark, so these decisions are proved without a Spark
install.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections.abc import Collection, Mapping, Sequence
from typing import Any

from scripts.stack.tags import MAX_DEADLINE_SECONDS

TABLE_NAME = re.compile(r"\w+(\.\w+)+", re.ASCII)
# The tables whose row count and checksum may leave the job: the Bronze signal tables hold no outcome or label, and the catalog proof table holds a few
# numbers that are not about patients. A table that is not listed (a table with an outcome or a label, or any new one) is sealed.
OPEN_TABLES = frozenset({"bronze.numeric_observations", "bronze.waveform_seconds", "bronze.tracks", "bronze.string_events", "catalog_proof.numbers"})
SEALED_FIELDS = ("rows", "checksum")
# The checks that every run makes, in order, named in the result: the trunk is Nessie's default branch, no other branch exists, the tables asked for are on it,
# no table could lose a file that a tag needs, every tag is as it was and every tagged read is as it was.
CHECKS_RUN = ("trunk_is_default", "no_branch_in_flight", "tables_on_trunk", "table_properties_safe", "tags_unchanged", "tagged_reads_unchanged")
JOB = "table_maintenance"
DEFAULT_DEADLINE_SECONDS = 900.0


class RetentionError(RuntimeError):
    """The job refuses to start or to finish: a tag does not read as it did, a table could lose files, or a run may be writing.

    The message is for the code that raises it and for its tests. The job's output holds ``code``, a fixed word, and nothing of the message, which can name a
    table, a tag, a branch or the text of an upstream error.
    """

    def __init__(self, message: str, *, code: str = "refused") -> None:
        super().__init__(message)
        self.code = code


def finite_seconds(text: str) -> float:
    """A finite number of seconds above zero and at most one day, for an argument."""
    try:
        seconds = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("the deadline must be a number of seconds") from None
    if not math.isfinite(seconds) or not 0 < seconds <= MAX_DEADLINE_SECONDS:
        raise argparse.ArgumentTypeError(f"the deadline must be a finite number of seconds above 0 and at most {MAX_DEADLINE_SECONDS:g}")
    return seconds


def table_identifier(table: str) -> str:
    """``table`` when it is plain names joined by dots, which a statement can hold safely, else ``RetentionError``."""
    if TABLE_NAME.fullmatch(table) is None:
        raise RetentionError("a table name must be plain letters, digits and underscores joined by dots", code="table_name")
    return table


def table_argument(text: str) -> str:
    try:
        return table_identifier(text)
    except RetentionError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose only error output is the code ``usage``: it never echoes an argument."""

    def error(self, message: str) -> Any:
        sys.stderr.write(f"{JOB}: usage\n")
        raise SystemExit(2)


def parser() -> argparse.ArgumentParser:
    command = QuietParser(prog="python -m spark.jobs.table_maintenance", description="Compact the tables of the trunk. Nothing is deleted.")
    command.add_argument("--trunk", default=None, help="an assertion: Nessie's default branch, whose tables are compacted, must be this one")
    command.add_argument(
        "--table", dest="tables", action="append", default=[], type=table_argument, help="a table to compact, such as schema.table; the default is every table"
    )
    command.add_argument("--rewrite-all", action="store_true", help="rewrite every data file, not only the ones that the table's size rules select")
    command.add_argument("--dry-run", action="store_true", help="list what would be rewritten and change nothing")
    command.add_argument("--deadline-seconds", type=finite_seconds, default=DEFAULT_DEADLINE_SECONDS, help="one finite deadline for the whole job")
    return command


def parse(argv: Sequence[str] | None) -> argparse.Namespace:
    """The job's arguments. A bad argument ends the program with exit status 2 before anything runs."""
    return parser().parse_args(argv)


def in_flight_branches(branches: Collection[str], trunk: str) -> list[str]:
    """The branches other than the trunk. A private branch means a run may be writing files that no metadata names yet, so maintenance waits for it."""
    return sorted(name for name in branches if name != trunk)


def compare_tags(before: Mapping[str, str], after: Mapping[str, str]) -> tuple[list[str], list[str]]:
    """The tags that vanished or moved during the run, each as a message, and the names of the tags that were made during it.

    A compaction adds a commit and removes nothing, so a tag made during the run is safe: it is reported and not a change.
    """
    changed = []
    for name in sorted(before):
        if name not in after:
            changed.append(f"the tag {name} was at {before[name]} and is gone")
        elif after[name] != before[name]:
            changed.append(f"the tag {name} was at {before[name]} and is now at {after[name]}")
    return changed, sorted(set(after) - set(before))


def compare_reads(before: Sequence[Mapping[str, Any]], after: Sequence[Mapping[str, Any]]) -> list[str]:
    """The differences between what every tag read before the run and what it reads after: a missing read, another snapshot, row count or checksum.

    A read that has no counterpart before it (a tag made during the run) is not compared with anything.
    """
    later = {(item["tag"], item["table"]): item for item in after}
    found = []
    for item in before:
        key = (item["tag"], item["table"])
        if key not in later:
            found.append(f"the tag {key[0]} read {key[1]} before the run and has no read of it after")
            continue
        for field in ("snapshot_id", "rows", "checksum"):
            if later[key].get(field) == item.get(field):
                continue
            if key[1] in OPEN_TABLES or field == "snapshot_id":
                found.append(f"the tag {key[0]} read {key[1]} with {field} {item.get(field)} before the run and {later[key].get(field)} after")
            else:
                found.append(f"the tag {key[0]} read {key[1]} with a {field} that differs after the run")
    return found


def public_reads(reads: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The reads for the result: a table that is not open holds its tag, its name and its snapshot ID only, with no row count and no checksum."""
    return [dict(item) if item["table"] in OPEN_TABLES else {key: value for key, value in item.items() if key not in SEALED_FIELDS} for item in reads]


def sealed_report(table: str, report: Mapping[str, Any], *, dry_run: bool) -> dict[str, Any]:
    """What a compaction reports for the result: the whole report for an open table, and for any other table one flag, because the counts of files and bytes
    that Iceberg reports follow the rows of the table. The flag says what happened: ``compacted`` after a rewrite and ``dry_run`` when nothing was rewritten."""
    if table in OPEN_TABLES:
        return dict(report)
    return {"dry_run": True} if dry_run else {"compacted": True}
