"""The Gold job for the six dimensions: ``gold.dim_case``, ``gold.dim_patient``, ``gold.dim_track``, ``gold.dim_department``, ``gold.dim_surgery_type`` and
``gold.dim_lab_test``, on a private Nessie branch.

``python -m spark.jobs.gold_dimensions build --branch <name> [--base <name>] --valid-from <YYYY-MM-DD>``
``python -m spark.jobs.gold_dimensions verify --branch <name>``

``build`` creates the branch from ``main`` (or ``--base``) and refuses a branch that exists. It reads ``silver.cases``, ``silver.lab_results`` and
``bronze.tracks`` on that branch and writes each dimension as type 2: one row for each version of a member, with a surrogate key, the natural key, a few
descriptive columns, ``valid_from``, ``valid_to`` and ``is_current``, and one unknown member (``spark/gold_dimensions.py`` has the rules). ``--valid-from`` is
the version date of the run (the nightly batch passes its data date): a new member's first version starts on it, and a member whose descriptive columns
changed has its open version closed on it and a new version opened. The surrogate key is a function of the version alone, so a rebuild, and a later build that
finds new or changed cases, keep every key they already gave and never change a version that is closed. A table that already holds the rows is not written
again. A rebuild that would remove a held member, or change a member whose open version starts on or after the version date, fails before it writes. Every
input check and that check come before the first write; the checks of the written tables come after the last. The merge of the branch is the caller's step.
``verify`` runs the checks on a branch and writes nothing.

No dimension holds an outcome, a laboratory result or the patient split, and no query reads one, so the job reads no held-out value: the test set is sealed.
The result and the messages hold codes, dimension names, counts and snapshot IDs, except for ``dim_lab_test``: the laboratory table holds the results of
held-out patients, so a held-out change adds or removes a test name. The member count and the write indicator of ``dim_lab_test`` stay inside the job (its
write does not change the outcome), the result holds one pass flag for each dimension, and a failure of ``dim_lab_test`` prints as ``<code>: failed``.
Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from spark.gold_dimensions import (
    DIMENSIONS,
    NAMESPACE,
    SOURCE_SCHEMAS,
    Dimension,
    GoldDimensionsError,
    count_of,
    source_schema_problems,
    surrogate_key,
    unit,
    version_date,
)
from spark.gold_dimensions_run import JOB, run, run_build, run_verify
from spark.gold_dimensions_sql import (
    ambiguous_query,
    chain_query,
    coverage_queries,
    desired_query,
    held_query,
    member_differs_query,
    natural_duplicates_query,
    natural_keys_query,
    next_query,
    output_facts_query,
    source_facts_query,
    table_name,
)
from spark.jobs.silver_cases import Lake
from spark.session import build_session
from spark.settings import CATALOG
from spark.split_cli import open_client, read_settings, start_deadline
from spark.split_table import BRANCH_MOVED

OUTPUT_ROW_FACTS = ("members", "unknown_rows", "unknown_not_empty", "key_duplicates", "key_out_of_range", "natural_null")


def number(row: Mapping[str, Any], name: str) -> int:
    """The count in column ``name`` of a result row, or a ``GoldDimensionsError`` when Spark gave anything but a count."""
    return count_of({name: row[name]}, name)


class GoldLake(Lake):
    """The Spark steps of a build or a verify, on one branch, through the catalog of the branch (``lake``). The writes, the property steps and the snapshot
    lookup are the Silver job's own."""

    def __init__(self, session: Any, client: Any, branch: str, at: str | None = None, valid_from: str | None = None) -> None:
        super().__init__(session, client, branch, at)
        self.valid_from = valid_from

    def version_start(self) -> str:
        """The version date of this build: it is a build's own argument, and a verify has none."""
        if self.valid_from is None:
            raise GoldDimensionsError("bad_date: a build has no version date")
        return version_date(self.valid_from)

    def count(self, query: str) -> int:
        return number(self.session.sql(query).collect()[0], "n")

    def check_sources(self) -> None:
        """Refuse a missing source table and a source table whose columns or types are not the ones Silver and Bronze write, before any other read."""
        problems = source_schema_problems({table: self.columns(f"{CATALOG}.{table}") for table in SOURCE_SCHEMAS})
        if problems:
            raise GoldDimensionsError(*problems)

    def source_facts(self) -> dict[str, dict[str, int]]:
        facts: dict[str, dict[str, int]] = {}
        for item in DIMENSIONS:
            row = self.session.sql(source_facts_query(item)).collect()[0]
            found = {"source_members": number(row, "source_members"), "key_null_rows": number(row, "key_null_rows")}
            facts[item.name] = {**found, "ambiguous_members": self.count(ambiguous_query(item))}
        return facts

    def held_changes(self) -> dict[str, tuple[int, int]]:
        """For each dimension, the held members that the source no longer has and the changed members whose open version starts on or after the version date.
        A table that is not there holds none. A table with other columns is never read."""
        held: dict[str, tuple[int, int]] = {}
        for item in DIMENSIONS:
            fields = self.columns(table_name(item))
            if fields is None:
                held[item.name] = (0, 0)
                continue
            if fields != self.expected_columns(item):
                raise GoldDimensionsError(f"inherited_schema: {unit(1, 'table')}")
            removed, not_after = held_query(item, self.version_start())
            held[item.name] = (self.count(removed), self.count(not_after))
        return held

    def expected_columns(self, item: Dimension) -> list[tuple[str, str]]:
        return [(column.name, column.type) for column in item.columns]

    def write(self, item: Dimension) -> bool:
        """Write the versions of ``item``: the first build writes every member and the unknown member, a later build closes and opens versions. True when the
        table changed."""
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        start = self.version_start()
        query = next_query(item, start) if self.exists(table_name(item)) else desired_query(item, start)
        return self.write_frame(self.session.sql(query), table_name(item), {})

    def tables_missing(self) -> int:
        return sum(1 for item in DIMENSIONS if not self.exists(table_name(item)))

    def output_schema_tables_wrong(self) -> int:
        """How many of the six written tables have other columns or types than the rules give, or are not there."""
        return sum(1 for item in DIMENSIONS if self.columns(table_name(item)) != self.expected_columns(item))

    def key_rule_wrong(self, item: Dimension) -> int:
        """The members whose key is not the key of the rule, worked out here in Python from the natural key that Spark wrote. A member with no natural key is
        wrong. Only keys and natural keys reach the driver, and nothing is printed."""
        wrong = 0
        for row in self.session.sql(natural_keys_query(item)).collect():
            try:
                wrong += int(row[item.key] != surrogate_key(item.name, row[item.natural.name], row["valid_from"]))
            except GoldDimensionsError:
                wrong += 1
        return wrong

    def table_facts(self) -> dict[str, dict[str, int]]:
        facts: dict[str, dict[str, int]] = {}
        for item in DIMENSIONS:
            row = self.session.sql(output_facts_query(item)).collect()[0]
            found = {name: number(row, name) for name in OUTPUT_ROW_FACTS}
            missing, extra = coverage_queries(item)
            found["natural_duplicates"] = self.count(natural_duplicates_query(item))
            found["key_rule_wrong"] = self.key_rule_wrong(item)
            found["source_without_member"] = self.count(missing)
            found["member_without_source"] = self.count(extra)
            found["member_differs"] = self.count(member_differs_query(item))
            found["interval_wrong"] = number(row, "interval_wrong")
            found["chain_broken"] = self.count(chain_query(item))
            facts[item.name] = found
        return facts

    def snapshot_ids(self) -> dict[str, int | None]:
        return {item.table: self.snapshot_id(item.table) for item in DIMENSIONS}


def execute(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run one call: the settings, the Nessie client and the deadline come first.

    A verify resolves the branch hash, opens its Spark session pinned to it, reads and looks up snapshots at that hash only, carries the hash in its result
    and fails with ``branch_moved`` when the branch head is another one at the end. A build creates the branch from the base and writes on it, so it is not
    pinned: only this job moves a branch that it just made.
    """
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        start_deadline(settings.deadline_seconds, job=JOB)
        building = arguments.mode == "build"
        client.config()
        pinned = None if building else client.reference(arguments.branch).hash
        session = build_session(JOB, arguments.branch, pinned)
        lake = GoldLake(session, client, arguments.branch, pinned, arguments.valid_from if building else None)
        base = client.create_branch(arguments.branch, arguments.base).hash if building else None
        lake.check_sources()
        outcome = (run_build if building else run_verify)(lake)
        head = client.reference(arguments.branch).hash
        if pinned is not None and head != pinned:
            raise GoldDimensionsError(BRANCH_MOVED)
        return {"mode": arguments.mode, "branch": arguments.branch, "base_hash": base, "branch_hash": head, **outcome.facts()}
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
