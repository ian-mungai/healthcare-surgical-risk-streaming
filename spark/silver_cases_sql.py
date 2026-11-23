"""The Spark SQL text of the Silver cases job, built from the column rules of ``spark/silver_cases.py`` so that no column, type or pattern is typed twice.

Every query holds column names from the rules and no data value. A column name is checked to be plain before it enters a text. The build casts with
``TRY_CAST`` after a pattern test, so a value that is not in its published form becomes missing and is counted as a failure by the facts queries; the checks
of a written table use the plain ``CAST`` against Bronze's own text, so they do not use the build's expression.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from spark.settings import CATALOG
from spark.silver_cases import (
    AGE_COLUMN,
    AGE_OVER_COLUMN,
    ASA_VALUES,
    BIGINT,
    CASE_COLUMNS,
    COPIES_COLUMN,
    DEVELOPMENT_SETS,
    DOUBLE,
    FLAG_COLUMNS,
    INT_PATTERN,
    LAB_GROUP,
    LAB_PUBLISHED_COLUMNS,
    NUMBER_PATTERN,
    OCCURRENCE_COLUMN,
    OUTCOME_COLUMNS,
    PINNED_LAB_ROWS,
    SEX_VALUES,
    STRING,
    TOP_CODED_AGE,
    SilverCasesError,
)

BRONZE_CASES_SOURCE = f"{CATALOG}.bronze.clinical_cases"
BRONZE_LAB_SOURCE = f"{CATALOG}.bronze.lab_results"
SPLIT_SOURCE = f"{CATALOG}.silver.patient_split"
CASES_SOURCE = f"{CATALOG}.silver.cases"
LAB_SOURCE = f"{CATALOG}.silver.lab_results"
MEASURE_SOURCE = f"{CATALOG}.silver.operation_length_measure"
CUTOFFS_SOURCE = f"{CATALOG}.silver.score_cutoffs"
PLAIN_NAME = re.compile(r"[a-z][a-z0-9_]*")
PLAIN_TEXT = re.compile(r"[A-Za-z0-9_:=<>.,% -]+")
SPARK_TYPES = {BIGINT: "BIGINT", DOUBLE: "DOUBLE"}
# The patterns are the rules' own, anchored at both ends. In a Spark string literal ``\\z`` is the regular expression ``\z``, the end of the text.
WHOLE_PATTERN = f"^{INT_PATTERN}\\\\z"
DECIMAL_PATTERN = f"^{NUMBER_PATTERN.replace('\\.', '[.]')}\\\\z"
PATIENT_PATTERN = "^[1-9][0-9]*\\\\z"
PATTERNS = {BIGINT: WHOLE_PATTERN, DOUBLE: DECIMAL_PATTERN}
DOMAIN_COLUMNS = (*(name for name in FLAG_COLUMNS if name not in OUTCOME_COLUMNS), "sex", "asa")
LAB_TYPES = {"caseid": BIGINT, "dt": BIGINT, "name": STRING, "result": DOUBLE}
DEVELOPMENT_LIST = ", ".join(f"'{name}'" for name in DEVELOPMENT_SETS)


def sql(*parts: str) -> str:
    """A statement from its parts, joined by one space. Every dynamic part is a column name that ``plain`` checked, a source name that is a constant of this
    module or an expression that this module built from them: no value of the data enters a statement."""
    return " ".join(parts)


def plain(name: str) -> str:
    if PLAIN_NAME.fullmatch(name) is None:
        raise SilverCasesError("bad_name: a column name is not plain")
    return name


def reference(name: str, alias: str | None = None) -> str:
    return f"`{plain(name)}`" if alias is None else f"`{plain(alias)}`.`{plain(name)}`"


def typed_expression(name: str, kind: str, alias: str | None = None) -> str:
    """The typed value of a published text column. A missing value (no value or an empty text) is missing; a text keeps every character."""
    column = reference(name, alias)
    if kind == STRING:
        return f"CASE WHEN {column} IS NULL OR {column} = '' THEN NULL ELSE {column} END"
    if kind not in SPARK_TYPES:
        raise SilverCasesError("bad_type: a column type is not known")
    return f"CASE WHEN {column} RLIKE '{PATTERNS[kind]}' THEN TRY_CAST({column} AS {SPARK_TYPES[kind]}) END"


def missing_text(column: str) -> str:
    return f"{column} IS NULL OR {column} = ''"


def case_expression(name: str, kind: str, alias: str | None = None) -> str:
    if name == AGE_OVER_COLUMN:
        column = reference(AGE_COLUMN, alias)
        return f"CASE WHEN {missing_text(column)} THEN NULL ELSE {column} = '{TOP_CODED_AGE}' END"
    return typed_expression(name, kind, alias)


def cases_query(source: str) -> str:
    """The typed rows of ``silver.cases`` from the Bronze table ``source``: every column in order, ``age_over_89`` beside ``age``."""
    items = [f"{case_expression(column.name, column.type)} AS {column.name}" for column in CASE_COLUMNS]
    return sql("SELECT", ", ".join(items), "FROM", source)


def lab_query(source: str) -> str:
    """The typed rows of ``silver.lab_results``: rows equal in all four published values are one row with their count in ``source_copies``, and the rows of one
    case, time and test name are numbered from 1 by their result (a missing result first) in ``source_occurrence``."""
    typed = ", ".join(f"{typed_expression(name, LAB_TYPES[name])} AS {name}" for name in LAB_PUBLISHED_COLUMNS)
    group = ", ".join(LAB_PUBLISHED_COLUMNS)
    key = ", ".join(LAB_GROUP)
    counted = f"CAST(COUNT(*) AS INT) AS {COPIES_COLUMN}"
    ranked = f"CAST(ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY result ASC NULLS FIRST) AS INT) AS {OCCURRENCE_COLUMN}"
    typed_part = sql("WITH typed AS (SELECT", typed, "FROM", source + "),")
    grouped_part = sql("grouped AS (SELECT", group + ",", counted, "FROM typed GROUP BY", group + ")")
    return sql(typed_part, grouped_part, "SELECT", group + ",", COPIES_COLUMN + ",", ranked, "FROM grouped")


def development_lengths_query(cases: str, split: str) -> str:
    """The length of each development operation in seconds (``opend`` minus ``opstart``). The test set is not named: only the development sets are listed."""
    end, start = typed_expression("opend", BIGINT, "c"), typed_expression("opstart", BIGINT, "c")
    joined = sql(cases, "c JOIN", split, "s ON c.`subjectid` = s.`subjectid`")
    return sql("SELECT (" + end + ") - (" + start + ") AS length_seconds FROM", joined, "WHERE s.`set` IN (" + DEVELOPMENT_LIST + ")")


def sum_of(condition: str, alias: str) -> str:
    return f"COALESCE(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END), 0) AS {alias}"


def case_facts_query(source: str) -> str:
    """One row of counts over the Bronze cases: the rows, null and repeated keys, patient numbers not in their published form, a missing or reversed operation
    time, each column's values that are not in their published form, and the values outside the published domain of a flag, the sex and the ASA class."""
    caseid = typed_expression("caseid", BIGINT)
    start, end = typed_expression("opstart", BIGINT), typed_expression("opend", BIGINT)
    items = [
        "COUNT(*) AS row_count",
        sum_of(f"({caseid}) IS NULL", "key_null"),
        f"COALESCE(COUNT(*) - COUNT(DISTINCT {caseid}), 0) AS key_duplicates",
        sum_of(f"`subjectid` IS NULL OR `subjectid` NOT RLIKE '{PATIENT_PATTERN}'", "subjectid_form"),
        sum_of(f"({start}) IS NULL OR ({end}) IS NULL OR ({end}) < ({start})", "operation_order"),
    ]
    for column in CASE_COLUMNS:
        if column.type not in SPARK_TYPES or column.name in OUTCOME_COLUMNS:
            continue
        top = f" AND {reference(column.name)} <> '{TOP_CODED_AGE}'" if column.name == AGE_COLUMN else ""
        unparsed = f"NOT ({missing_text(reference(column.name))}) AND ({typed_expression(column.name, column.type)}) IS NULL{top}"
        items.append(sum_of(unparsed, f"unparsed__{column.name}"))
    kinds = {column.name: column.type for column in CASE_COLUMNS}
    for name in DOMAIN_COLUMNS:
        typed = f"({typed_expression(name, kinds[name])})"
        if name == "sex":
            condition = f"{typed} IS NOT NULL AND {typed} NOT IN ({', '.join(repr(value) for value in SEX_VALUES)})"
        elif name == "asa":
            condition = f"{typed} IS NOT NULL AND {typed} NOT IN ({', '.join(str(value) for value in ASA_VALUES)})"
        else:
            condition = f"{typed} IS NOT NULL AND {typed} NOT IN (0, 1)"
        items.append(sum_of(condition, f"domain__{name}"))
    return sql("SELECT", ", ".join(items), "FROM", source)


def split_facts_queries(cases: str, split: str) -> tuple[str, str]:
    """Two counts: the cases that do not have exactly one split row, and the split patients that have no case."""
    joined = sql(
        "SELECT b.`caseid` FROM", cases, "b LEFT JOIN", split, "s ON b.`subjectid` = s.`subjectid` GROUP BY b.`caseid` HAVING COUNT(s.`subjectid`) <> 1"
    )
    patients = sql("SELECT COUNT(*) AS n FROM", split, "s LEFT ANTI JOIN", cases, "b ON s.`subjectid` = b.`subjectid`")
    return sql("SELECT COUNT(*) AS n FROM (", joined, ")"), patients


def outcome_unparsed(name: str, kinds: dict[str, str]) -> str:
    return f"NOT ({missing_text(reference(name, 'b'))}) AND ({typed_expression(name, kinds[name], 'b')}) IS NULL"


def outcome_bad(kinds: dict[str, str]) -> list[str]:
    """The conditions that make an outcome value not what the release publishes: a value that is not in its form, or a death flag outside 0 and 1."""
    conditions = [outcome_unparsed(name, kinds) for name in OUTCOME_COLUMNS]
    typed = f"({typed_expression('death_inhosp', kinds['death_inhosp'], 'b')})"
    return [*conditions, f"{typed} IS NOT NULL AND {typed} NOT IN (0, 1)"]


def outcome_facts_query(cases: str, split: str) -> str:
    """One row of counts of the outcome columns over the development patients only: the values not in their form and the death flags outside 0 and 1."""
    kinds = {column.name: column.type for column in CASE_COLUMNS}
    items = [sum_of(outcome_unparsed(name, kinds), f"unparsed__{name}") for name in OUTCOME_COLUMNS]
    typed = f"({typed_expression('death_inhosp', kinds['death_inhosp'], 'b')})"
    items.append(sum_of(f"{typed} IS NOT NULL AND {typed} NOT IN (0, 1)", "domain__death_inhosp"))
    joined = sql(cases, "b JOIN", split, "s ON b.`subjectid` = s.`subjectid`")
    return sql("SELECT", ", ".join(items), "FROM", joined, "WHERE s.`set` IN (" + DEVELOPMENT_LIST + ")")


def sealed_outcome_query(cases: str, split: str) -> str:
    """One flag, 0 or 1, never a count: whether any outcome value of a patient outside the development sets is not in its published form."""
    kinds = {column.name: column.type for column in CASE_COLUMNS}
    anything = " OR ".join(f"({condition})" for condition in outcome_bad(kinds))
    joined = sql(cases, "b LEFT JOIN", split, "s ON b.`subjectid` = s.`subjectid`")
    held_out = "COALESCE(s.`set`, '') NOT IN (" + DEVELOPMENT_LIST + ")"
    return sql("SELECT CASE WHEN COALESCE(SUM(CASE WHEN", anything, "THEN 1 ELSE 0 END), 0) > 0 THEN 1 ELSE 0 END AS n FROM", joined, "WHERE", held_out)


def lab_scope(lab: str, cases: str, split: str, *, development: bool, bronze: bool, strict: bool = False) -> str:
    """The laboratory rows whose case is a case of a development patient (``development``) or of any other patient, as a subquery. A row with no case is in
    neither. ``bronze`` is for the Bronze tables, where the case number is a text that is typed first: with the guarded typing of the build, or with the plain
    ``CAST`` when ``strict`` (the checks of a written table, which run after the input checks found no case number that is not a number)."""

    def number(alias: str) -> str:
        return f"CAST({reference('caseid', alias)} AS BIGINT)" if strict else typed_expression("caseid", BIGINT, alias)

    lab_case = number("l") if bronze else reference("caseid", "l")
    case_case = number("c") if bronze else reference("caseid", "c")
    condition = "s.`set` IN (" + DEVELOPMENT_LIST + ")" if development else "COALESCE(s.`set`, '') NOT IN (" + DEVELOPMENT_LIST + ")"
    joined = sql(lab, "l JOIN", cases, "c ON", lab_case, "=", case_case, "LEFT JOIN", split, "s ON c.`subjectid` = s.`subjectid`")
    return sql("( SELECT l.* FROM", joined, "WHERE", condition, ")")


def lab_bad_conditions() -> list[str]:
    """The conditions that make a laboratory row not what the release publishes: a missing time or test name, or a time or result not in its form."""
    dt, name = typed_expression("dt", BIGINT, "l"), typed_expression("name", STRING, "l")
    conditions = [f"({dt}) IS NULL", f"({name}) IS NULL"]
    for column in ("dt", "result"):
        conditions.append(f"NOT ({missing_text(reference(column, 'l'))}) AND ({typed_expression(column, LAB_TYPES[column], 'l')}) IS NULL")
    return conditions


def lab_facts_query(scope: str) -> str:
    """One row of counts over the development laboratory rows (``scope`` from ``lab_scope``): the rows, rows with a null key part and values not in their
    published form."""
    dt, name = typed_expression("dt", BIGINT, "l"), typed_expression("name", STRING, "l")
    items = ["COUNT(*) AS row_count", sum_of(f"({dt}) IS NULL OR ({name}) IS NULL", "lab_key_null")]
    for column in ("dt", "result"):
        unparsed = f"NOT ({missing_text(reference(column, 'l'))}) AND ({typed_expression(column, LAB_TYPES[column], 'l')}) IS NULL"
        items.append(sum_of(unparsed, f"unparsed__{column}"))
    return sql("SELECT", ", ".join(items), "FROM", scope, "l")


def sealed_lab_form_query(scope: str) -> str:
    """One flag, 0 or 1, never a count: whether any laboratory value of a patient outside the development sets (``scope`` from ``lab_scope``) is not in its
    published form."""
    anything = " OR ".join(f"({condition})" for condition in lab_bad_conditions())
    return sql("SELECT CASE WHEN COALESCE(SUM(CASE WHEN", anything, "THEN 1 ELSE 0 END), 0) > 0 THEN 1 ELSE 0 END AS n FROM", scope, "l")


def lab_pinned_query(lab: str) -> str:
    """One flag, 0 or 1: whether the table does not hold the release's number of laboratory rows. The total never leaves the query."""
    return sql("SELECT CASE WHEN COUNT(*) =", str(PINNED_LAB_ROWS), "THEN 0 ELSE 1 END AS n FROM", lab)


def silver_lab_orphans_query(lab: str, cases: str) -> str:
    """The count of written laboratory rows whose case number is in no case row."""
    return sql("SELECT COUNT(*) AS n FROM", lab, "l LEFT ANTI JOIN", cases, "c ON", reference("caseid", "l"), "=", reference("caseid", "c"))


def sealed_lab_output_query(bronze_scope: str, silver_scope: str) -> str:
    """One flag, 0 or 1, never a count: whether the written laboratory rows of the patients outside the development sets differ from Bronze, repeat a key
    or leave a gap in the occurrence numbers. The scopes come from ``lab_scope``."""
    differences, duplicates, gaps, _ = lab_output_queries(bronze_scope, silver_scope)
    return sql("SELECT CASE WHEN (", differences, ") + (", duplicates, ") + (", gaps, ") > 0 THEN 1 ELSE 0 END AS n")


def lab_unmatched_query(lab: str, cases: str) -> str:
    """The count of laboratory rows whose case has no clinical row."""
    joined = typed_expression("caseid", BIGINT, "x") + " = " + typed_expression("caseid", BIGINT, "y")
    return sql("SELECT COUNT(*) AS n FROM", lab, "x LEFT ANTI JOIN", cases, "y ON", joined)


def count_of(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if type(value) is not int or value < 0:
        raise SilverCasesError("bad_fact: a count is missing or is not a count")
    return value


def prefixed(row: Mapping[str, object], prefix: str) -> dict[str, int]:
    return {name.removeprefix(prefix): count_of(row, name) for name in row if name.startswith(prefix)}


def decode_case_facts(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "bronze_case_rows": count_of(row, "row_count"),
        "case_key_null": count_of(row, "key_null"),
        "case_key_duplicates": count_of(row, "key_duplicates"),
        "subjectid_form": count_of(row, "subjectid_form"),
        "operation_ends_before_it_starts": count_of(row, "operation_order"),
        "unparsed": prefixed(row, "unparsed__"),
        "domain": prefixed(row, "domain__"),
    }


def decode_lab_facts(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "bronze_lab_rows": count_of(row, "row_count"),
        "lab_key_null": count_of(row, "lab_key_null"),
        "lab_rows_without_case": count_of(row, "lab_rows_without_case"),
        "lab_unparsed": prefixed(row, "unparsed__"),
    }


def count_query(source: str) -> str:
    return sql("SELECT COUNT(*) AS n FROM", source)


def case_duplicates_query(silver: str) -> str:
    return sql("SELECT COUNT(*) - COUNT(DISTINCT `caseid`) AS n FROM", silver)


def case_difference(name: str, kind: str) -> str:
    """One condition: the Silver value of a column is not what Bronze's own text gives, by the plain ``CAST`` and a null-safe equality."""
    bronze, silver = reference(name, "b"), reference(name, "s")
    if name == AGE_OVER_COLUMN:
        age = reference(AGE_COLUMN, "b")
        return f"NOT ({silver} <=> CASE WHEN NULLIF({age}, '') IS NULL THEN NULL ELSE {age} = '{TOP_CODED_AGE}' END)"
    if name == AGE_COLUMN:
        return f"NOT ({silver} <=> CASE WHEN {bronze} = '{TOP_CODED_AGE}' THEN NULL ELSE CAST(NULLIF({bronze}, '') AS DOUBLE) END)"
    wanted = f"NULLIF({bronze}, '')" if kind == STRING else f"CAST(NULLIF({bronze}, '') AS {SPARK_TYPES[kind]})"
    return f"NOT ({silver} <=> {wanted})"


def case_output_query(bronze: str, silver: str) -> str:
    """The count of Silver case rows whose value in any column but the outcome columns differs from its Bronze row, joined on the case number."""
    conditions = " OR ".join(case_difference(column.name, column.type) for column in CASE_COLUMNS if column.name not in OUTCOME_COLUMNS)
    return sql("SELECT COUNT(*) AS n FROM", bronze, "b JOIN", silver, "s ON CAST(b.`caseid` AS BIGINT) = s.`caseid` WHERE", conditions)


def outcome_differences() -> str:
    kinds = {column.name: column.type for column in CASE_COLUMNS}
    return " OR ".join(case_difference(name, kinds[name]) for name in OUTCOME_COLUMNS)


def outcome_output_query(bronze: str, silver: str, split: str) -> str:
    """The count of development case rows whose outcome value in Silver differs from Bronze."""
    joined = sql(bronze, "b JOIN", silver, "s ON CAST(b.`caseid` AS BIGINT) = s.`caseid` JOIN", split, "p ON b.`subjectid` = p.`subjectid`")
    return sql("SELECT COUNT(*) AS n FROM", joined, "WHERE p.`set` IN (" + DEVELOPMENT_LIST + ") AND (" + outcome_differences() + ")")


def sealed_outcome_output_query(bronze: str, silver: str, split: str) -> str:
    """One flag, 0 or 1, never a count: whether any outcome value of a patient outside the development sets differs between Silver and Bronze."""
    joined = sql(bronze, "b JOIN", silver, "s ON CAST(b.`caseid` AS BIGINT) = s.`caseid` LEFT JOIN", split, "p ON b.`subjectid` = p.`subjectid`")
    held_out = "COALESCE(p.`set`, '') NOT IN (" + DEVELOPMENT_LIST + ")"
    return sql("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS n FROM", joined, "WHERE", held_out, "AND (" + outcome_differences() + ")")


def case_key_query(bronze: str, silver: str) -> str:
    """The count of case numbers that one table has and the other has not: Bronze against Silver and Silver against Bronze, by anti-joins."""
    typed = "CAST(b.`caseid` AS BIGINT)"
    missing_in_silver = sql("SELECT", typed, "AS caseid FROM", bronze, "b LEFT ANTI JOIN", silver, "s ON", typed, "= s.`caseid`")
    missing_in_bronze = sql("SELECT s.`caseid` AS caseid FROM", silver, "s LEFT ANTI JOIN", bronze, "b ON", typed, "= s.`caseid`")
    return sql("SELECT COUNT(*) AS n FROM (", missing_in_silver, "UNION ALL", missing_in_bronze, ")")


def lab_output_queries(bronze: str, silver: str) -> tuple[str, str, str, str]:
    """The checks of the written laboratory table: the groups of Bronze against Silver in both directions, a repeated key, a gap in the occurrence numbers and
    the sum of the copies."""
    typed = "CAST(`caseid` AS BIGINT) AS `caseid`, CAST(`dt` AS BIGINT) AS `dt`, NULLIF(`name`, '') AS `name`, CAST(NULLIF(`result`, '') AS DOUBLE) AS `result`"
    counted = f"CAST(COUNT(*) AS INT) AS `{COPIES_COLUMN}`"
    inner = sql("SELECT", typed, "FROM", bronze)
    counts = sql("SELECT `caseid`, `dt`, `name`, `result`,", counted, "FROM (", inner, ") GROUP BY `caseid`, `dt`, `name`, `result`")
    ranked = f"CAST(ROW_NUMBER() OVER (PARTITION BY `caseid`, `dt`, `name` ORDER BY `result` ASC NULLS FIRST) AS INT) AS `{OCCURRENCE_COLUMN}`"
    grouped = sql("SELECT `caseid`, `dt`, `name`, `result`,", f"`{COPIES_COLUMN}`,", ranked, "FROM (", counts, ")")
    held = sql("SELECT `caseid`, `dt`, `name`, `result`,", f"`{COPIES_COLUMN}`, `{OCCURRENCE_COLUMN}` FROM", silver)
    missing_in_silver = sql("( (", grouped, ") EXCEPT ALL (", held, ") )")
    missing_in_bronze = sql("( (", held, ") EXCEPT ALL (", grouped, ") )")
    differences = sql("SELECT COUNT(*) AS n FROM (", missing_in_silver, "UNION ALL", missing_in_bronze, ")")
    key = f"`caseid`, `dt`, `name`, `{OCCURRENCE_COLUMN}`"
    duplicates = sql("SELECT COUNT(*) AS n FROM (SELECT 1 FROM", silver, "GROUP BY", key, "HAVING COUNT(*) > 1)")
    out_of_order = f"MIN(`{OCCURRENCE_COLUMN}`) <> 1 OR MAX(`{OCCURRENCE_COLUMN}`) <> COUNT(*)"
    gaps = sql("SELECT COUNT(*) AS n FROM (SELECT 1 FROM", silver, "GROUP BY `caseid`, `dt`, `name` HAVING", out_of_order + ")")
    copies = sql(f"SELECT COALESCE(SUM(`{COPIES_COLUMN}`), 0) AS n FROM", silver)
    return differences, duplicates, gaps, copies


def values_query(rows: Sequence[Sequence[object]]) -> str:
    """One ``VALUES`` query that holds the rows as literals, so that Spark builds the frame in the JVM and no Python worker starts.

    A text is plain characters only, a whole number is written as a ``BIGINT`` and ``None`` is a null ``BIGINT``. Any other value is refused.
    """
    if not rows:
        raise SilverCasesError("unwritable_row: no row to write")
    tuples = []
    for row in rows:
        literals = []
        for value in row:
            if isinstance(value, str) and PLAIN_TEXT.fullmatch(value):
                literals.append(f"'{value}'")
            elif type(value) is int:
                literals.append(f"{value}L")
            elif value is None:
                literals.append("CAST(NULL AS BIGINT)")
            else:
                raise SilverCasesError("unwritable_row: a value is not plain text or a whole number")
        tuples.append(f"({', '.join(literals)})")
    return f"VALUES {', '.join(tuples)}"
