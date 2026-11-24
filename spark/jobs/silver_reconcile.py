"""The reconciliation of the Silver tables with Bronze, by stable key, on the hash of a Nessie branch.

``python -m spark.jobs.silver_reconcile verify --branch <name> --result <path>``

The job resolves the branch to one hash, opens its Spark session pinned to that hash and reads Bronze and Silver there, so a branch that moves changes
nothing that it reads. It writes nothing and creates no table. For each of ``silver.numeric_observations``, ``silver.waveform_seconds``,
``silver.cases`` and ``silver.lab_results`` it proves that every Bronze key is in Silver or has a recorded disposition (a duplicate in the ledger of a
reading, a cause of invalidity, a collapsed copy of a laboratory row), that no Silver key is without a Bronze key and that no key is claimed twice,
and it checks that each join that the Silver build makes gives every row exactly one partner of its kind (``spark/reconcile/checks.py``,
``spark/reconcile/sql.py``). The order is in ``spark/reconcile/run.py`` and the command line in ``spark/reconcile/cli.py``. The settings come from the
environment, as the patient split's do: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``.

Before any count it checks the patient split table by the split job's own rules and the owner of each Bronze case by the Silver cases job's own query, and a
failure of either refuses the whole run with one fixed code (``split_rule_broken``, ``ambiguous_case_owner``) and no count.

The result is one JSON file at ``<path>`` (it must not exist, and it is written whole and renamed into place), with outcome ``verified`` or ``failed``; the
job's standard output and standard error are for the operator and no caller reads them (``spark/reconcile/cli.py``).

The test set is sealed. The counts and dispositions are those of development patients; for every other patient the result holds one flag for each table. The
result holds hashes, snapshot IDs, counts of keys and the checks that ran, and no case, track, time or value.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

from spark.jobs.silver_cases import Lake
from spark.reconcile.checks import JOINS, OWNER_REFUSAL, SOURCE_TABLES, SPLIT_REFUSAL, JoinFacts, KeyFacts, schema_problems
from spark.reconcile.cli import JOB, run
from spark.reconcile.run import Operations, run_verify
from spark.reconcile.sql import decode_join_facts, decode_key_facts, held_out_query, join_held_out_query, join_query, key_facts_query
from spark.session import build_session
from spark.settings import CATALOG
from spark.split_cli import open_client, read_settings, start_deadline


class ReconcileLake(Lake):
    """The Spark steps of the reconciliation: reads only, through the catalog of the branch pinned to the hash that the job resolved."""

    def head(self) -> str:
        return self.client.reference(self.branch).hash

    def source_problems(self) -> list[str]:
        return schema_problems({table: self.columns(f"{CATALOG}.{table}") for table in SOURCE_TABLES})

    def refusals(self) -> list[str]:
        """The split table checked by the split job's own rules and the owner of each Bronze case by the Silver cases job's own query, each as one fixed
        code with no count. Both are named in one run, and the run makes no count before they are clear."""
        return [*([SPLIT_REFUSAL] if self.split_problems() else []), *([OWNER_REFUSAL] if self.ownership_problems() else [])]

    def key_facts(self, name: str) -> KeyFacts:
        return decode_key_facts(self.session.sql(key_facts_query(name)).collect()[0].asDict())

    def held_out_problem(self, name: str) -> bool:
        queries = [held_out_query(name), *(join_held_out_query(join.name) for join in JOINS if join.table == name)]
        return any(self.count(query) for query in queries)

    def join_facts(self, name: str) -> JoinFacts:
        return decode_join_facts(self.session.sql(join_query(name)).collect()[0].asDict())

    def snapshot_ids(self) -> dict[str, int | None]:
        return {table: self.snapshot_id(table) for table in SOURCE_TABLES}

    def operations(self) -> Operations:
        return Operations(self.head, self.source_problems, self.refusals, self.key_facts, self.held_out_problem, self.join_facts, self.snapshot_ids)


def execute(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run one call. Every local step comes first: the settings, the Nessie client and the deadline. Then Nessie resolves the branch hash once, and the Spark
    session and every read are pinned to it."""
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        start_deadline(settings.deadline_seconds, job=JOB)
        client.config()
        pinned = client.reference(arguments.branch).hash
        session = build_session(JOB, arguments.branch, pinned)
        lake = ReconcileLake(session, client, arguments.branch, pinned)
        outcome = run_verify(lake.operations(), pinned)
        return {"mode": arguments.mode, "branch": arguments.branch, "job": JOB, **outcome.facts()}
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
