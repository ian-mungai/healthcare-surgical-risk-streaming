"""The order of the reconciliation, in plain data with no Spark. The job ``spark/jobs/silver_reconcile.py`` passes its Spark steps to ``run_verify``,
so that the order runs in the fast tests on stand-in steps.

The job only reads. It resolves the branch to one hash and reads every table at that hash. It checks the columns of every table before it runs a query, compares
each table's keys (development patients as counts and dispositions, every other patient as one flag), checks the cardinality of each join, reads the snapshot
IDs and ends only when the branch still has the hash. Every table, join and flag is judged before the job refuses, so that one run names every cause.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spark.reconcile.checks import (
    CHECK_NAMES,
    JOINS,
    RECONCILIATIONS,
    JoinFacts,
    KeyFacts,
    ReconcileError,
    held_out_problems,
    join_problems,
    key_problems,
)

BRANCH_MOVED = "branch_moved: the branch head is not the one that was checked"


@dataclass(frozen=True, slots=True)
class Operations:
    """The Spark steps, all reads. ``key_facts`` and ``join_facts`` are given a table or join name. ``held_out_problem`` gives one flag and never a count."""

    head: Callable[[], str]
    source_problems: Callable[[], list[str]]
    key_facts: Callable[[str], KeyFacts]
    held_out_problem: Callable[[str], bool]
    join_facts: Callable[[str], JoinFacts]
    snapshot_ids: Callable[[], dict[str, int | None]]


@dataclass(frozen=True, slots=True)
class Outcome:
    outcome: str
    branch_hash: str
    keys: dict[str, KeyFacts]
    joins: dict[str, JoinFacts]
    snapshots: dict[str, int | None]

    def facts(self) -> dict[str, Any]:
        """What the result line may hold: the hash, snapshot IDs, counts of development keys with their dispositions, the flag of the other patients (a
        set flag fails the job, so a result always says clean) and the checks that ran. Nothing in it names a case, a track, a time or a value."""
        return {
            "outcome": self.outcome,
            "branch_hash": self.branch_hash,
            "snapshots": dict(self.snapshots),
            "reconciled": {
                name: {
                    "bronze_keys": facts.bronze_keys,
                    "matched_keys": facts.matched,
                    "dispositions": dict(facts.dispositions),
                    "held_out_clean": True,
                }
                for name, facts in self.keys.items()
            },
            "joins": {name: {"rows": facts.rows} for name, facts in self.joins.items()},
            "checks_run": list(CHECK_NAMES),
        }


def run_verify(ops: Operations, head: str) -> Outcome:
    """Check every table and join of the branch whose head is ``head`` and confirm that the branch still has ``head``. Nothing is written."""
    problems = list(ops.source_problems())
    if problems:
        raise ReconcileError(*problems)
    keys: dict[str, KeyFacts] = {}
    flags: dict[str, bool] = {}
    for reconciliation in RECONCILIATIONS:
        keys[reconciliation.name] = ops.key_facts(reconciliation.name)
        flags[reconciliation.name] = ops.held_out_problem(reconciliation.name)
    joins = {join.name: ops.join_facts(join.name) for join in JOINS}
    for name, facts in keys.items():
        problems += key_problems(name, facts)
    problems += held_out_problems(flags)
    for name, join in joins.items():
        problems += join_problems(name, join)
    snapshots = ops.snapshot_ids()
    if ops.head() != head:
        problems.append(BRANCH_MOVED)
    if problems:
        raise ReconcileError(*problems)
    return Outcome("verified", head, keys, joins, snapshots)
