"""The Silver job for the clinical and laboratory tables: ``silver.cases`` and ``silver.lab_results``, with the measured operation lengths and the score
cutoffs, on a private Nessie branch.

``python -m spark.jobs.silver_cases build --branch <name> [--base <name>] [--pinned-release]``
``python -m spark.jobs.silver_cases verify --branch <name> [--pinned-release]``

``build`` creates the branch from ``main`` (or ``--base``) and refuses a branch that exists. It reads ``bronze.clinical_cases``, ``bronze.lab_results`` and
``silver.patient_split`` on that branch. Silver does the standard steps only: it enforces types, keeps a missing value missing, collapses laboratory rows
that are equal in every published value into one row with their count, and checks. It fills nothing and converts no unit. ``subjectid`` stays on every case
row, and the fields that are never features stay in the table, named in its property ``never_feature_columns``.

It also measures the operation lengths (``opend`` minus ``opstart``) of the development patients (training and validation) and sets the score cutoffs from
the median (``spark/silver_cases.py``). Both are saved as tables. Once a branch holds them, a rebuild that would change either fails before it writes.
Every input check and that freeze come before the first write; the checks of the written tables come after the last. A table that already holds the
rows is not written again. The merge of the branch is the caller's step. ``verify`` runs the checks on a branch and writes nothing.

The test set is sealed: the result and the messages hold codes, counts, table names and the cutoffs, and never a case or a patient.
Settings come from the environment: ``NESSIE_URI``, ``WAREHOUSE_LOCATION``, ``AWS_REGION`` for an s3 warehouse, ``NESSIE_TIMEOUT_SECONDS`` and
``JOB_DEADLINE_SECONDS``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from scripts.stack.nessie import NessieClient, NessieError
from spark.session import build_session
from spark.settings import CATALOG
from spark.silver_cases import (
    CASE_COLUMNS,
    CASES_TABLE,
    CUTOFFS_TABLE,
    LAB_COLUMNS,
    LAB_TABLE,
    MEASURE_TABLE,
    NEVER_FEATURE_COLUMNS,
    NEVER_FEATURE_PROPERTY,
    SOURCE_SCHEMAS,
    CutoffRow,
    MeasureRow,
    SilverCasesError,
    same_rows,
    source_schema_problems,
    split_rule_problems,
    unit,
)
from spark.silver_cases_run import JOB, run, run_build, run_verify, start_deadline
from spark.silver_cases_sql import (
    BRONZE_CASES_SOURCE,
    BRONZE_LAB_SOURCE,
    CASES_SOURCE,
    CUTOFFS_SOURCE,
    LAB_SOURCE,
    MEASURE_SOURCE,
    SPLIT_SOURCE,
    case_duplicates_query,
    case_facts_query,
    case_key_query,
    case_output_query,
    cases_query,
    count_query,
    decode_case_facts,
    decode_lab_facts,
    development_lengths_query,
    lab_facts_query,
    lab_output_queries,
    lab_pinned_query,
    lab_query,
    lab_scope,
    lab_unmatched_query,
    outcome_facts_query,
    outcome_output_query,
    sealed_lab_form_query,
    sealed_lab_output_query,
    sealed_outcome_output_query,
    sealed_outcome_query,
    silver_lab_orphans_query,
    split_facts_queries,
    values_query,
)
from spark.split_cli import open_client, read_settings
from spark.split_table import SplitRow

MEASURE_COLUMNS = (("statistic", "string"), ("unit", "string"), ("value", "bigint"))
CUTOFF_COLUMNS = (
    ("cutoff", "string"),
    ("anchor", "string"),
    ("elapsed_seconds", "bigint"),
    ("basis", "string"),
    ("rule_version", "string"),
    ("development_cases", "bigint"),
    ("median_length_seconds", "bigint"),
)
NEVER_FEATURE_VALUE = ",".join(NEVER_FEATURE_COLUMNS)
READ_TABLES = (*SOURCE_SCHEMAS, CASES_TABLE, LAB_TABLE, MEASURE_TABLE, CUTOFFS_TABLE)
NOT_FOUND = 404


class Lake:
    """The Spark steps of a build or a verify, on one branch, through the catalog of the branch (``lake``)."""

    def __init__(self, session: Any, client: NessieClient, branch: str) -> None:
        self.session = session
        self.client = client
        self.branch = branch

    def count(self, query: str) -> int:
        return int(self.session.sql(query).collect()[0]["n"])

    def exists(self, source: str) -> bool:
        return bool(self.session.catalog.tableExists(source))

    def columns(self, source: str) -> list[tuple[str, str]] | None:
        if not self.exists(source):
            return None
        return [(field.name, field.dataType.simpleString()) for field in self.session.table(source).schema.fields]

    def check_sources(self) -> None:
        """Refuse a missing source table and a source table whose columns or types are not the ones Bronze and the split write, before any other read."""
        found = {table: self.columns(f"{CATALOG}.{table}") for table in SOURCE_SCHEMAS}
        problems = source_schema_problems(found)
        if problems:
            raise SilverCasesError(*problems)

    def bronze_lab_scope(self, *, development: bool, strict: bool = False) -> str:
        return lab_scope(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=development, bronze=True, strict=strict)

    def silver_lab_scope(self, *, development: bool) -> str:
        return lab_scope(LAB_SOURCE, CASES_SOURCE, SPLIT_SOURCE, development=development, bronze=False)

    def split_problems(self) -> list[str]:
        """The patient split table checked by the split job's own rules: a development patient's set is only trusted when this is empty."""
        rows = [
            SplitRow(row["subjectid"], row["set"], row["rule_version"], int(row["source_snapshot_id"])) for row in self.session.table(SPLIT_SOURCE).collect()
        ]
        return split_rule_problems(rows, pinned=False)

    def input_facts(self) -> dict[str, object]:
        """The counts over the inputs. The outcome columns are counted for development patients only; for the others one flag (0 or 1) says whether any value
        is not in its published form."""
        case_row = self.session.sql(case_facts_query(BRONZE_CASES_SOURCE)).collect()[0].asDict()
        case_row |= self.session.sql(outcome_facts_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)).collect()[0].asDict()
        lab_row = self.session.sql(lab_facts_query(self.bronze_lab_scope(development=True))).collect()[0].asDict()
        lab_row["lab_rows_without_case"] = self.count(lab_unmatched_query(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE))
        without_one, patients = split_facts_queries(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        split = {"cases_without_one_split_row": self.count(without_one), "split_patients_without_case": self.count(patients)}
        sealed = {
            "split_problems": self.split_problems(),
            "sealed_outcome_form_bad": self.count(sealed_outcome_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)),
            "sealed_lab_form_bad": self.count(sealed_lab_form_query(self.bronze_lab_scope(development=False))),
            "lab_pinned_mismatch": self.count(lab_pinned_query(BRONZE_LAB_SOURCE)),
        }
        return {**decode_case_facts(case_row), **decode_lab_facts(lab_row), **split, **sealed}

    def development_lengths(self) -> list[int]:
        rows = self.session.sql(development_lengths_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)).collect()
        return [int(row["length_seconds"]) for row in rows if row["length_seconds"] is not None]

    def properties(self, source: str) -> dict[str, str]:
        return {row["key"]: row["value"] for row in self.session.sql(f"SHOW TBLPROPERTIES {source}").collect()}

    def write_frame(self, frame: Any, source: str, properties: dict[str, str]) -> bool:
        """Write the rows of ``frame`` to the table ``source``. True when the table changed: it was made, its rows differ or its properties differ.

        A table that holds exactly the rows (same columns and types, no row of one missing from the other) is left as it is. A table with the same columns
        is overwritten in place, because a replace of an existing table stalls in Spark with the Nessie catalog. One partition gives one data file.
        """
        held = self.session.table(source) if self.exists(source) else None
        same_columns = held is not None and held.dtypes == frame.dtypes
        if held is not None and same_columns:
            if held.exceptAll(frame).count() == 0 and frame.exceptAll(held).count() == 0:
                return self.set_properties(source, properties)
            frame.coalesce(1).writeTo(source).overwritePartitions()
            self.set_properties(source, properties)
            return True
        writer = frame.coalesce(1).writeTo(source).tableProperty("format-version", "2")
        for key, value in properties.items():
            writer = writer.tableProperty(key, value)
        writer.createOrReplace()
        return True

    def set_properties(self, source: str, wanted: dict[str, str]) -> bool:
        """Set a property that the table does not hold yet, as one commit. True when it changed."""
        held = self.properties(source)
        changes = {key: value for key, value in wanted.items() if held.get(key) != value}
        if changes:
            pairs = ", ".join(f"'{key}' = '{value}'" for key, value in changes.items())
            self.session.sql(f"ALTER TABLE {source} SET TBLPROPERTIES ({pairs})")
        return bool(changes)

    def write_cases(self) -> bool:
        self.session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
        return self.write_frame(self.session.sql(cases_query(BRONZE_CASES_SOURCE)), CASES_SOURCE, {NEVER_FEATURE_PROPERTY: NEVER_FEATURE_VALUE})

    def write_lab(self) -> bool:
        return self.write_frame(self.session.sql(lab_query(BRONZE_LAB_SOURCE)), LAB_SOURCE, {})

    def read_rows(self, source: str, expected: Sequence[tuple[str, str]]) -> list[Any] | None:
        fields = self.columns(source)
        if fields is None:
            return None
        if fields != list(expected):
            raise SilverCasesError(f"inherited_schema: {unit(1, 'table')}")
        return list(self.session.table(source).collect())

    def existing_rules(self) -> tuple[list[MeasureRow] | None, list[CutoffRow] | None]:
        measure = self.read_rows(MEASURE_SOURCE, MEASURE_COLUMNS)
        cutoffs = self.read_rows(CUTOFFS_SOURCE, CUTOFF_COLUMNS)
        measure_rows = None if measure is None else [MeasureRow(row["statistic"], row["unit"], int(row["value"])) for row in measure]
        cutoff_rows = None
        if cutoffs is not None:
            cutoff_rows = [
                CutoffRow(
                    row["cutoff"],
                    row["anchor"],
                    None if row["elapsed_seconds"] is None else int(row["elapsed_seconds"]),
                    row["basis"],
                    row["rule_version"],
                    int(row["development_cases"]),
                    int(row["median_length_seconds"]),
                )
                for row in cutoffs
            ]
        return measure_rows, cutoff_rows

    def write_values(self, source: str, columns: Sequence[tuple[str, str]], rows: Sequence[Sequence[object]]) -> bool:
        """Write literal rows as a table. The values reach Spark as one ``VALUES`` text, built in the JVM, after each value was checked."""
        frame = self.session.sql(values_query(rows)).toDF(*(name for name, _ in columns))
        return self.write_frame(frame, source, {})

    def write_rules(self, measure: list[MeasureRow], cutoffs: list[CutoffRow]) -> bool:
        held_measure, held_cutoffs = self.existing_rules()
        changed = False
        if held_measure is None or not same_rows(held_measure, measure):
            changed |= self.write_values(MEASURE_SOURCE, MEASURE_COLUMNS, [(row.statistic, row.unit, row.value) for row in measure])
        if held_cutoffs is None or not same_rows(held_cutoffs, cutoffs):
            rows = [
                (row.cutoff, row.anchor, row.elapsed_seconds, row.basis, row.rule_version, row.development_cases, row.median_length_seconds) for row in cutoffs
            ]
            changed |= self.write_values(CUTOFFS_SOURCE, CUTOFF_COLUMNS, rows)
        return changed

    def output_schema_tables_wrong(self) -> int:
        """How many of the two written tables have other columns or types than the rules give, or are not there."""
        wanted = {CASES_SOURCE: [(column.name, column.type) for column in CASE_COLUMNS], LAB_SOURCE: [(column.name, column.type) for column in LAB_COLUMNS]}
        return sum(1 for table, fields in wanted.items() if self.columns(table) != fields)

    def output_facts(self) -> dict[str, object]:
        bronze_lab, silver_lab = self.bronze_lab_scope(development=True, strict=True), self.silver_lab_scope(development=True)
        differences, duplicates, gaps, copies = lab_output_queries(bronze_lab, silver_lab)
        held_out = sealed_lab_output_query(self.bronze_lab_scope(development=False, strict=True), self.silver_lab_scope(development=False))
        marked = self.properties(CASES_SOURCE).get(NEVER_FEATURE_PROPERTY) == NEVER_FEATURE_VALUE
        return {
            "case_rows": self.count(count_query(CASES_SOURCE)),
            "bronze_case_rows": self.count(count_query(BRONZE_CASES_SOURCE)),
            "case_value_differences": self.count(case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE))
            + self.count(outcome_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE, SPLIT_SOURCE)),
            "case_key_differences": self.count(case_key_query(BRONZE_CASES_SOURCE, CASES_SOURCE)),
            "sealed_outcome_differs": self.count(sealed_outcome_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE, SPLIT_SOURCE)),
            "output_schema_tables_wrong": self.output_schema_tables_wrong(),
            "case_key_duplicates": self.count(case_duplicates_query(CASES_SOURCE)),
            "lab_rows": self.count(count_query(silver_lab)),
            "lab_copies": self.count(copies),
            "bronze_lab_rows": self.count(count_query(bronze_lab)),
            "lab_value_differences": self.count(differences),
            "lab_key_duplicates": self.count(duplicates),
            "lab_occurrence_gaps": self.count(gaps),
            "silver_lab_orphans": self.count(silver_lab_orphans_query(LAB_SOURCE, CASES_SOURCE)),
            "sealed_lab_differs": self.count(held_out),
            "never_feature_marking_wrong": 0 if marked else 1,
        }

    def snapshot_id(self, table: str) -> int | None:
        """The snapshot that Nessie holds for ``table`` on the branch, or ``None`` when the branch has no such table."""
        try:
            document = self.client.request("GET", f"/trees/{quote(self.branch, safe='')}/contents/{table}") or {}
        except NessieError as error:
            if error.status == NOT_FOUND:
                return None
            raise
        snapshot = (document.get("content") or {}).get("snapshotId")
        return None if snapshot is None else int(snapshot)

    def snapshot_ids(self) -> dict[str, int | None]:
        return {table: self.snapshot_id(table) for table in READ_TABLES}


def execute(arguments: argparse.Namespace) -> dict[str, Any]:
    """Run one call. Every local step comes first: the settings, the Nessie client, the deadline and the Spark session. Then the first call to Nessie, which
    creates the branch (build) or finds it (verify), and the job on that branch."""
    session = None
    try:
        settings = read_settings()
        client = open_client(settings.nessie_uri, settings.nessie_timeout_seconds)
        start_deadline(settings.deadline_seconds)
        session = build_session(JOB, arguments.branch)
        lake = Lake(session, client, arguments.branch)
        client.config()
        building = arguments.mode == "build"
        if building:
            base = client.create_branch(arguments.branch, arguments.base).hash
        else:
            base = None
            client.reference(arguments.branch)
        lake.check_sources()
        outcome = (run_build if building else run_verify)(lake, pinned=arguments.pinned_release)
        return {
            "mode": arguments.mode,
            "branch": arguments.branch,
            "base_hash": base,
            "branch_hash": client.reference(arguments.branch).hash,
            "pinned_release": arguments.pinned_release,
            **outcome.facts(),
        }
    finally:
        if session is not None:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv, execute)


if __name__ == "__main__":
    sys.exit(main())
