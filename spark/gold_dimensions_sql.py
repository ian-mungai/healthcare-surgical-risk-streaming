"""The Spark SQL text of the Gold dimensions job, built from the dimensions of ``spark/gold_dimensions.py`` so that no column, type or key rule is typed twice.

Every query holds column names from the rules and no data value. A column name is checked to be plain before it enters a text. No query reads an outcome, the
patient split or a laboratory result: a dimension reads its source columns only, and every count is over keys and descriptive columns.
"""

from __future__ import annotations

from spark.gold_dimensions import KEY_HEX_DIGITS, KEY_LIMIT, NAMESPACE, UNKNOWN_KEY, Dimension, GoldDimensionsError
from spark.settings import CATALOG
from spark.silver_cases_sql import PLAIN_NAME, sql

SPARK_TYPES = {"bigint": "BIGINT", "string": "STRING"}
SUM_ZERO = "COALESCE(SUM(CASE WHEN {condition} THEN 1 ELSE 0 END), 0) AS {alias}"


def plain(name: str) -> str:
    if PLAIN_NAME.fullmatch(name) is None:
        raise GoldDimensionsError("bad_name: a column name is not plain")
    return name


def quoted(name: str, alias: str | None = None) -> str:
    return f"`{plain(name)}`" if alias is None else f"{alias}.`{plain(name)}`"


def spark_type(kind: str) -> str:
    found = SPARK_TYPES.get(kind)
    if found is None:
        raise GoldDimensionsError("bad_type: a column type is not known")
    return found


def source_name(item: Dimension) -> str:
    return f"{CATALOG}.{item.source}"


def table_name(item: Dimension) -> str:
    return f"{CATALOG}.{NAMESPACE}.{plain(item.name)}"


def key_expression(item: Dimension, column: str) -> str:
    """The surrogate key of the natural key in ``column``: the first ``KEY_HEX_DIGITS`` hex digits of the SHA-256 of ``<dimension>:<natural key as text>``, as a
    number. It is the rule of ``surrogate_key``, and the job checks every written key against that function."""
    text = f"CONCAT('{plain(item.name)}:', CAST({quoted(column)} AS STRING))"
    return f"CAST(CONV(SUBSTRING(SHA2({text}, 256), 1, {KEY_HEX_DIGITS}), 16, 10) AS BIGINT)"


def members_query(item: Dimension) -> str:
    """The distinct members of the source (natural key and descriptive columns, under the dimension's names) for the rows that have a natural key."""
    items = ", ".join(f"{quoted(source)} AS {quoted(column.name)}" for column, source in item.source_columns)
    groups = ", ".join(quoted(source) for _, source in item.source_columns)
    return sql("SELECT", items, "FROM", source_name(item), "WHERE", quoted(item.source_natural), "IS NOT NULL GROUP BY", groups)


def desired_members(item: Dimension) -> str:
    """The members with their keys, without the unknown member."""
    names = ", ".join(quoted(column.name) for column in item.columns[1:])
    return sql("SELECT", key_expression(item, item.natural.name), "AS", quoted(item.key) + ",", names, "FROM (", members_query(item), ")")


def desired_query(item: Dimension) -> str:
    """The rows of the dimension: the members and then the unknown member, whose key is ``UNKNOWN_KEY`` and whose other columns are all missing."""
    unknown = [f"CAST({UNKNOWN_KEY} AS BIGINT) AS {quoted(item.key)}"]
    unknown += [f"CAST(NULL AS {spark_type(column.type)}) AS {quoted(column.name)}" for column in item.columns[1:]]
    return sql(desired_members(item), "UNION ALL SELECT", ", ".join(unknown))


def source_facts_query(item: Dimension) -> str:
    """One row of counts over the source: its distinct natural keys and its rows with no natural key."""
    natural = quoted(item.source_natural)
    members = f"COUNT(DISTINCT {natural}) AS source_members"
    return sql("SELECT", members + ",", SUM_ZERO.format(condition=f"{natural} IS NULL", alias="key_null_rows"), "FROM", source_name(item))


def ambiguous_query(item: Dimension) -> str:
    """The count of natural keys that the source gives with more than one set of descriptive columns."""
    natural = quoted(item.natural.name)
    return sql("SELECT COUNT(*) AS n FROM (SELECT", natural, "FROM (", members_query(item), ") GROUP BY", natural, "HAVING COUNT(*) > 1)")


def output_facts_query(item: Dimension) -> str:
    """One row of counts over the written table, with the unknown member kept apart from the members."""
    key = quoted(item.key)
    member, unknown = f"NOT ({key} <=> {UNKNOWN_KEY})", f"{key} <=> {UNKNOWN_KEY}"
    holds_a_value = " OR ".join(f"{quoted(column.name)} IS NOT NULL" for column in item.columns[1:])
    items = [
        SUM_ZERO.format(condition=member, alias="members"),
        SUM_ZERO.format(condition=unknown, alias="unknown_rows"),
        SUM_ZERO.format(condition=f"{unknown} AND ({holds_a_value})", alias="unknown_not_empty"),
        f"COUNT(*) - COUNT(DISTINCT {key}) AS key_duplicates",
        SUM_ZERO.format(condition=f"{member} AND ({key} IS NULL OR {key} <= 0 OR {key} >= {KEY_LIMIT})", alias="key_out_of_range"),
        SUM_ZERO.format(condition=f"{member} AND {quoted(item.natural.name)} IS NULL", alias="natural_null"),
    ]
    return sql("SELECT", ", ".join(items), "FROM", table_name(item))


def natural_duplicates_query(item: Dimension) -> str:
    """The count of natural keys that more than one member of the written table holds."""
    natural = quoted(item.natural.name)
    members = f"WHERE NOT ({quoted(item.key)} <=> {UNKNOWN_KEY}) AND {natural} IS NOT NULL"
    return sql("SELECT COUNT(*) AS n FROM (SELECT", natural, "FROM", table_name(item), members, "GROUP BY", natural, "HAVING COUNT(*) > 1)")


def natural_keys_query(item: Dimension) -> str:
    """The key and the natural key of every member, for the check of the key rule against ``surrogate_key``."""
    key = quoted(item.key)
    return sql("SELECT", key + ",", quoted(item.natural.name), "FROM", table_name(item), "WHERE NOT (" + key, "<=>", f"{UNKNOWN_KEY})")


def held_members(item: Dimension) -> str:
    return sql("SELECT", quoted(item.natural.name), "FROM", table_name(item), "WHERE NOT (" + quoted(item.key), "<=>", f"{UNKNOWN_KEY})")


def source_distinct(item: Dimension) -> str:
    natural = quoted(item.source_natural)
    return sql("SELECT DISTINCT", natural, "AS", quoted(item.natural.name), "FROM", source_name(item), "WHERE", natural, "IS NOT NULL")


def coverage_queries(item: Dimension) -> tuple[str, str]:
    """The count of natural keys of the source that the written table has no member for, and of members that the source has no natural key for."""
    natural = quoted(item.natural.name)
    on = f"ON s.{natural} = t.{natural}"
    missing = sql("SELECT COUNT(*) AS n FROM (", source_distinct(item), ") s LEFT ANTI JOIN (", held_members(item), ") t", on)
    extra = sql("SELECT COUNT(*) AS n FROM (", held_members(item), ") t LEFT ANTI JOIN (", source_distinct(item), ") s", on)
    return missing, extra


def held_query(item: Dimension) -> tuple[str, str]:
    """The counts of the members that the written table holds and a rebuild would remove, and would change: by key, and in any descriptive column."""
    key = quoted(item.key)
    held = sql("SELECT", key, "FROM", table_name(item), "WHERE NOT (" + key, "<=>", f"{UNKNOWN_KEY})")
    removed = sql("SELECT COUNT(*) AS n FROM (", held, ") h LEFT ANTI JOIN (SELECT", key, "FROM (", desired_members(item), ")) d ON", f"h.{key} = d.{key}")
    differences = " OR ".join(f"NOT (h.{quoted(column.name)} <=> d.{quoted(column.name)})" for column in item.columns[1:])
    changed = sql("SELECT COUNT(*) AS n FROM", table_name(item), "h JOIN (", desired_members(item), ") d ON", f"h.{key} = d.{key}", "WHERE", differences)
    return removed, changed
