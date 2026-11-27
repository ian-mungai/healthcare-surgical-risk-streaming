"""The Spark SQL text of the Gold dimensions job, built from the dimensions of ``spark/gold_dimensions.py`` so that no column, type or key rule is typed twice.

Every query holds column names from the rules and no data value. A column name is checked to be plain before it enters a text. No query reads an outcome, the
patient split or a laboratory result: a dimension reads its source columns only, and every count is over keys and descriptive columns.
"""

from __future__ import annotations

from spark.gold_dimensions import KEY_HEX_DIGITS, KEY_LIMIT, NAMESPACE, UNKNOWN_KEY, Dimension, GoldDimensionsError, version_date
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


def key_expression(item: Dimension, column: str, valid_from: str) -> str:
    """The surrogate key of the version of the member whose natural key is in ``column`` and whose version starts at the expression ``valid_from``: the first
    ``KEY_HEX_DIGITS`` hex digits of the SHA-256 of ``<dimension>:<natural key as text>:<valid_from>``, as a number. It is the rule of ``surrogate_key``:
    the job checks every written key against it."""
    text = f"CONCAT('{plain(item.name)}:', CAST({quoted(column)} AS STRING), ':', CAST({valid_from} AS STRING))"
    return f"CAST(CONV(SUBSTRING(SHA2({text}, 256), 1, {KEY_HEX_DIGITS}), 16, 10) AS BIGINT)"


def date_literal(as_of: str) -> str:
    return f"DATE'{version_date(as_of)}'"


def members_query(item: Dimension) -> str:
    """The distinct members of the source (natural key and descriptive columns, under the dimension's names) for the rows that have a natural key."""
    items = ", ".join(f"{quoted(source)} AS {quoted(column.name)}" for column, source in item.source_columns)
    groups = ", ".join(quoted(source) for _, source in item.source_columns)
    return sql("SELECT", items, "FROM", source_name(item), "WHERE", quoted(item.source_natural), "IS NOT NULL GROUP BY", groups)


def described(item: Dimension) -> str:
    return ", ".join(quoted(column.name) for column in item.described)


def new_versions(item: Dimension, as_of: str, members: str) -> str:
    """The open versions that start on ``as_of`` for the members in the query ``members``, with their keys."""
    start = date_literal(as_of)
    key = key_expression(item, item.natural.name, start)
    return sql(
        "SELECT",
        key,
        "AS",
        quoted(item.key) + ",",
        described(item) + ",",
        start,
        "AS `valid_from`, CAST(NULL AS DATE) AS `valid_to`, TRUE AS `is_current` FROM (",
        members,
        ")",
    )


def unknown_row(item: Dimension) -> str:
    """The unknown member: the key ``UNKNOWN_KEY``, every other column missing, and ``is_current`` true."""
    unknown = [f"CAST({UNKNOWN_KEY} AS BIGINT) AS {quoted(item.key)}"]
    unknown += [f"CAST(NULL AS {spark_type(column.type)}) AS {quoted(column.name)}" for column in item.described]
    return ", ".join([*unknown, "CAST(NULL AS DATE) AS `valid_from`", "CAST(NULL AS DATE) AS `valid_to`", "TRUE AS `is_current`"])


def desired_query(item: Dimension, as_of: str) -> str:
    """The rows of a first build: the first version of every member, and then the unknown member."""
    return sql(new_versions(item, as_of, members_query(item)), "UNION ALL SELECT", unknown_row(item))


def held_current(item: Dimension) -> str:
    key = quoted(item.key)
    return sql("SELECT * FROM", table_name(item), "WHERE `is_current` AND NOT (" + key, "<=>", f"{UNKNOWN_KEY})")


def differs(item: Dimension, left: str, right: str) -> str:
    """The condition that a descriptive column differs between ``left`` and ``right`` (aliases); false for a dimension that has none."""
    conditions = [f"NOT ({left}.{quoted(column.name)} <=> {right}.{quoted(column.name)})" for column, _ in item.attributes]
    return " OR ".join(conditions) if conditions else "FALSE"


def changed_members(item: Dimension) -> str:
    """The members whose descriptive columns differ from their open version: their new description."""
    natural = quoted(item.natural.name)
    return sql(
        "SELECT s.* FROM (", members_query(item), ") s JOIN (", held_current(item), ") c ON", f"s.{natural} = c.{natural}", "WHERE", differs(item, "s", "c")
    )


def fresh_members(item: Dimension) -> str:
    """The members that the table does not hold."""
    natural = quoted(item.natural.name)
    return sql("SELECT s.* FROM (", members_query(item), ") s LEFT ANTI JOIN (", held_current(item), ") c ON", f"s.{natural} = c.{natural}")


def next_query(item: Dimension, as_of: str) -> str:
    """The rows of a later build: every held version as it is, except that the open version of a changed member is closed at ``as_of``, and then the new
    open versions of the changed members and of the new members. Held versions are never renumbered."""
    natural, start = quoted(item.natural.name), date_literal(as_of)
    columns = ", ".join(f"h.{quoted(column.name)}" for column in item.columns[:-2])
    closing = f"ch.{natural} IS NOT NULL AND h.`is_current`"
    held = sql(
        "SELECT",
        columns + ",",
        f"CASE WHEN {closing} THEN {start} ELSE h.`valid_to` END AS `valid_to`,",
        f"CASE WHEN {closing} THEN FALSE ELSE h.`is_current` END AS `is_current` FROM",
        table_name(item),
        "h LEFT JOIN (",
        changed_members(item),
        ") ch ON",
        f"h.{natural} = ch.{natural}",
    )
    fresh = new_versions(item, as_of, sql("SELECT * FROM (", changed_members(item), ") UNION ALL SELECT * FROM (", fresh_members(item), ")"))
    return sql(held, "UNION ALL", fresh)


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
    """One row of counts over the written table, with the unknown member kept apart from the members (the members are the open versions). The unknown
    member must hold no value and be current."""
    key = quoted(item.key)
    member, unknown = f"NOT ({key} <=> {UNKNOWN_KEY})", f"{key} <=> {UNKNOWN_KEY}"
    holds_a_value = " OR ".join(f"{quoted(column.name)} IS NOT NULL" for column in (*item.described, *item.columns[-3:-1]))
    not_open = "NOT (`is_current` <=> TRUE)"
    open_version = (
        "`valid_from` IS NULL OR `is_current` IS NULL OR `is_current` <> (`valid_to` IS NULL) OR (`valid_to` IS NOT NULL AND `valid_to` <= `valid_from`)"
    )
    items = [
        SUM_ZERO.format(condition=f"{member} AND `is_current`", alias="members"),
        SUM_ZERO.format(condition=unknown, alias="unknown_rows"),
        SUM_ZERO.format(condition=f"{unknown} AND ({holds_a_value} OR {not_open})", alias="unknown_not_empty"),
        f"COUNT(*) - COUNT(DISTINCT {key}) AS key_duplicates",
        SUM_ZERO.format(condition=f"{member} AND ({key} IS NULL OR {key} <= 0 OR {key} >= {KEY_LIMIT})", alias="key_out_of_range"),
        SUM_ZERO.format(condition=f"{member} AND {quoted(item.natural.name)} IS NULL", alias="natural_null"),
        SUM_ZERO.format(condition=f"{member} AND ({open_version})", alias="interval_wrong"),
    ]
    return sql("SELECT", ", ".join(items), "FROM", table_name(item))


def natural_duplicates_query(item: Dimension) -> str:
    """The count of natural keys that more than one open version of the written table holds."""
    natural = quoted(item.natural.name)
    members = f"WHERE `is_current` AND NOT ({quoted(item.key)} <=> {UNKNOWN_KEY}) AND {natural} IS NOT NULL"
    return sql("SELECT COUNT(*) AS n FROM (SELECT", natural, "FROM", table_name(item), members, "GROUP BY", natural, "HAVING COUNT(*) > 1)")


def chain_query(item: Dimension) -> str:
    """The count of versions that break the chain of their member. The versions of one natural key, ordered by ``valid_from``, form a chain: every version but
    the last closes on the start of the next one and is not current; the last is open and current; no two start on the same day; every version has a start."""
    natural, key = quoted(item.natural.name), quoted(item.key)
    window = f"OVER (PARTITION BY {natural} ORDER BY `valid_from`)"
    ordered = sql(
        "SELECT `valid_from`, `valid_to`, `is_current`,",
        f"LEAD(`valid_from`) {window} AS next_start,",
        f"COUNT(*) OVER (PARTITION BY {natural}, `valid_from`) AS same_start FROM",
        table_name(item),
        f"WHERE NOT ({key} <=> {UNKNOWN_KEY}) AND {natural} IS NOT NULL",
    )
    not_last = "(next_start IS NOT NULL AND NOT (`valid_to` <=> next_start AND `is_current` <=> FALSE))"
    last = "(next_start IS NULL AND NOT (`valid_to` IS NULL AND `is_current` <=> TRUE))"
    return sql("SELECT COUNT(*) AS n FROM (", ordered, ") WHERE same_start > 1 OR `valid_from` IS NULL OR", not_last, "OR", last)


def natural_keys_query(item: Dimension) -> str:
    """The key, the natural key and the start of every version, for the check of the key rule against ``surrogate_key``."""
    key = quoted(item.key)
    return sql(
        "SELECT",
        key + ",",
        quoted(item.natural.name) + ", CAST(`valid_from` AS STRING) AS `valid_from` FROM",
        table_name(item),
        "WHERE NOT (" + key,
        "<=>",
        f"{UNKNOWN_KEY})",
    )


def source_distinct(item: Dimension) -> str:
    natural = quoted(item.source_natural)
    return sql("SELECT DISTINCT", natural, "AS", quoted(item.natural.name), "FROM", source_name(item), "WHERE", natural, "IS NOT NULL")


def held_members(item: Dimension) -> str:
    return sql("SELECT", quoted(item.natural.name), "FROM (", held_current(item), ")")


def coverage_queries(item: Dimension) -> tuple[str, str]:
    """The count of natural keys of the source that the written table has no open version for, and of open versions that the source has no natural key for."""
    natural = quoted(item.natural.name)
    on = f"ON s.{natural} = t.{natural}"
    missing = sql("SELECT COUNT(*) AS n FROM (", source_distinct(item), ") s LEFT ANTI JOIN (", held_members(item), ") t", on)
    extra = sql("SELECT COUNT(*) AS n FROM (", held_members(item), ") t LEFT ANTI JOIN (", source_distinct(item), ") s", on)
    return missing, extra


def member_differs_query(item: Dimension) -> str:
    """The count of open versions whose descriptive columns are not the source's."""
    natural = quoted(item.natural.name)
    return sql(
        "SELECT COUNT(*) AS n FROM (",
        held_current(item),
        ") c JOIN (",
        members_query(item),
        ") s ON",
        f"c.{natural} = s.{natural}",
        "WHERE",
        differs(item, "c", "s"),
    )


def held_query(item: Dimension, as_of: str) -> tuple[str, str]:
    """The counts, before a rebuild writes, of the held members that the source no longer has, and of the changed members whose open version starts on or after
    ``as_of`` (their new version could not follow it)."""
    natural = quoted(item.natural.name)
    removed = sql("SELECT COUNT(*) AS n FROM (", held_current(item), ") h LEFT ANTI JOIN (", source_distinct(item), ") s ON", f"h.{natural} = s.{natural}")
    not_after = sql(
        "SELECT COUNT(*) AS n FROM (", held_current(item), ") c JOIN (", members_query(item), ") s ON", f"c.{natural} = s.{natural}",
        "WHERE (", differs(item, "c", "s"), ") AND c.`valid_from` >=", date_literal(as_of),
    )  # fmt: skip
    return removed, not_after
