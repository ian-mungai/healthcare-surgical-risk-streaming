"""The Spark SQL text of the Gold dimensions job: each key, the source columns read, the unknown member and what each check counts.

No Spark runs here, so these tests hold the text to the rules: a key made from another text, a query that reads an outcome, the patient split or a laboratory
result, an unknown member with a value in it, or a check that counts the wrong thing would each change Gold or the checks without a failure. The queries run
in the acceptance run.

Failure modes covered here:

- A key expression that differs from the rule (another digest, another length, no dimension name in the text), so Spark and the job's own key check disagree.
- A query that reads a column that is not a source column of its dimension, such as an outcome, a split set or a laboratory result.
- An unknown member that carries a value, has another key or sits first so that a reader takes it for a member.
- A check over the held table that counts the unknown member as a member or the other way round.
"""

from __future__ import annotations

import re
import unittest

from spark.gold_dimensions import DIMENSIONS, KEY_HEX_DIGITS, KEY_LIMIT, UNKNOWN_KEY, GoldDimensionsError, dimension
from spark.gold_dimensions_sql import (
    ambiguous_query,
    coverage_queries,
    desired_query,
    held_query,
    key_expression,
    members_query,
    natural_duplicates_query,
    natural_keys_query,
    output_facts_query,
    source_facts_query,
    source_name,
    table_name,
)

NAMES = re.compile(r"`([A-Za-z0-9_]+)`")
BANNED = ("patient_split", "icu_days", "death_inhosp", "intraop_", "preop_", "`set`", "`result`", "`value`", "`samples`")


def queries_of(item: object) -> list[str]:
    found = [
        members_query(item),  # type: ignore[arg-type]
        desired_query(item),  # type: ignore[arg-type]
        source_facts_query(item),  # type: ignore[arg-type]
        ambiguous_query(item),  # type: ignore[arg-type]
        output_facts_query(item),  # type: ignore[arg-type]
        natural_duplicates_query(item),  # type: ignore[arg-type]
        natural_keys_query(item),  # type: ignore[arg-type]
        *coverage_queries(item),  # type: ignore[arg-type]
        *held_query(item),  # type: ignore[arg-type]
    ]
    return found


class KeyExpressionTests(unittest.TestCase):
    def test_the_key_is_the_rule_of_the_module_written_in_spark_sql(self) -> None:
        case = dimension("dim_case")
        text = key_expression(case, "case_id")
        self.assertEqual(
            f"CAST(CONV(SUBSTRING(SHA2(CONCAT('dim_case:', CAST(`case_id` AS STRING)), 256), 1, {KEY_HEX_DIGITS}), 16, 10) AS BIGINT)",
            text,
        )

    def test_each_dimension_puts_its_own_name_in_the_text_that_is_hashed(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                self.assertIn(f"CONCAT('{item.name}:', CAST(`{item.natural.name}` AS STRING))", key_expression(item, item.natural.name))

    def test_the_key_cannot_reach_the_unknown_key_or_the_limit(self) -> None:
        self.assertEqual(-1, UNKNOWN_KEY)
        self.assertEqual(16**KEY_HEX_DIGITS, KEY_LIMIT)


class SourceTests(unittest.TestCase):
    def test_a_query_reads_only_its_dimensions_source_table_of_the_lake(self) -> None:
        for item in DIMENSIONS:
            for text in (members_query(item), source_facts_query(item), ambiguous_query(item), *coverage_queries(item), held_query(item)[0]):
                with self.subTest(name=item.name):
                    self.assertIn(f"FROM lake.{item.source}", text)

    def test_a_query_reads_no_column_but_the_source_columns_of_its_dimension_and_the_columns_it_writes(self) -> None:
        for item in DIMENSIONS:
            allowed = {name for _, name in item.source_columns} | {column.name for column in item.columns}
            for text in queries_of(item):
                with self.subTest(name=item.name):
                    self.assertLessEqual(set(NAMES.findall(text)), allowed, text)

    def test_no_query_names_an_outcome_the_patient_split_or_a_laboratory_result(self) -> None:
        for item in DIMENSIONS:
            for text in queries_of(item):
                for banned in BANNED:
                    with self.subTest(name=item.name, banned=banned):
                        self.assertNotIn(banned, text)

    def test_the_members_are_the_distinct_rows_of_the_source_with_a_natural_key(self) -> None:
        text = members_query(dimension("dim_track"))
        self.assertIn("FROM lake.bronze.tracks", text)
        self.assertIn("WHERE `track` IS NOT NULL", text)
        self.assertIn("GROUP BY `track`, `device`, `track_name`, `kind`, `unit`", text)

    def test_the_source_name_and_the_table_name_are_the_lake_names(self) -> None:
        self.assertEqual("lake.silver.cases", source_name(dimension("dim_case")))
        self.assertEqual("lake.gold.dim_case", table_name(dimension("dim_case")))


class DesiredTests(unittest.TestCase):
    def test_the_rows_are_the_members_with_their_keys_and_then_the_unknown_member(self) -> None:
        item = dimension("dim_case")
        text = desired_query(item)
        self.assertEqual(1, text.count("UNION ALL"))
        members, unknown = text.split(" UNION ALL ")
        self.assertIn("AS `case_key`, `case_id`, `emop`, `approach`, `position`, `ane_type` FROM (", members)
        self.assertEqual(
            "SELECT CAST(-1 AS BIGINT) AS `case_key`, CAST(NULL AS BIGINT) AS `case_id`, CAST(NULL AS BIGINT) AS `emop`, CAST(NULL AS STRING) AS `approach`, "
            "CAST(NULL AS STRING) AS `position`, CAST(NULL AS STRING) AS `ane_type`",
            unknown,
        )

    def test_the_unknown_member_of_every_dimension_holds_nothing_but_its_key(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                unknown = desired_query(item).split(" UNION ALL ")[1]
                self.assertEqual(len(item.columns), unknown.count(" AS `"))
                self.assertEqual(len(item.columns) - 1, unknown.count("CAST(NULL AS "))
                self.assertEqual(1, unknown.count("CAST(-1 AS BIGINT)"))


class FactTests(unittest.TestCase):
    def test_the_source_facts_are_one_row_of_counts_with_the_names_of_the_rules(self) -> None:
        for item in DIMENSIONS:
            text = source_facts_query(item)
            self.assertEqual(["source_members", "key_null_rows"], re.findall(r"AS (\w+)", text))
            self.assertEqual(["n"], re.findall(r"AS (\w+)", ambiguous_query(item)))

    def test_the_output_facts_are_one_row_of_counts_and_treat_the_unknown_key_apart(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                text = output_facts_query(item)
                self.assertEqual(
                    ["members", "unknown_rows", "unknown_not_empty", "key_duplicates", "key_out_of_range", "natural_null"],
                    re.findall(r"AS (\w+)", text),
                )
                self.assertIn(f"NOT (`{item.key}` <=> -1)", text)
                self.assertIn(f"`{item.key}` <=> -1", text)
                self.assertIn(f"`{item.key}` >= {KEY_LIMIT}", text)

    def test_the_unknown_member_must_hold_no_natural_key_and_no_descriptive_column(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                text = output_facts_query(item)
                for column in item.columns[1:]:
                    self.assertIn(f"`{column.name}` IS NOT NULL", text)

    def test_the_natural_duplicates_are_counted_among_the_members_only(self) -> None:
        item = dimension("dim_department")
        text = natural_duplicates_query(item)
        self.assertIn("WHERE NOT (`department_key` <=> -1)", text)
        self.assertIn("GROUP BY `department_name` HAVING COUNT(*) > 1", text)

    def test_the_natural_keys_that_python_checks_are_the_members_without_the_unknown_one(self) -> None:
        text = natural_keys_query(dimension("dim_lab_test"))
        self.assertEqual("SELECT `lab_test_key`, `lab_test_name` FROM lake.gold.dim_lab_test WHERE NOT (`lab_test_key` <=> -1)", text)

    def test_the_coverage_is_counted_in_both_directions_by_an_anti_join_that_leaves_the_unknown_member_out(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                missing, extra = coverage_queries(item)
                self.assertIn("LEFT ANTI JOIN", missing)
                self.assertIn("LEFT ANTI JOIN", extra)
                self.assertIn(f"FROM lake.{item.source}", missing)
                self.assertIn(f"FROM lake.{item.source}", extra)
                self.assertNotEqual(missing, extra)
                self.assertIn(f"NOT (`{item.key}` <=> -1)", extra)
                source, table = f"FROM lake.{item.source}", f"FROM lake.gold.{item.name}"
                self.assertLess(missing.index(source), missing.index(table), "the missing members are the source keys that the table lacks")
                self.assertLess(extra.index(table), extra.index(source), "the extra members are the table keys that the source lacks")

    def test_the_held_table_is_compared_with_the_desired_members_by_key_and_by_every_column(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                removed, changed = held_query(item)
                self.assertIn("LEFT ANTI JOIN", removed)
                self.assertIn(f"lake.gold.{item.name}", removed)
                self.assertIn(f"NOT (`{item.key}` <=> -1)", removed)
                for column in item.columns[1:]:
                    self.assertIn(f"NOT (h.`{column.name}` <=> d.`{column.name}`)", changed)

    def test_every_count_query_names_its_result_n(self) -> None:
        item = dimension("dim_track")
        for text in (ambiguous_query(item), natural_duplicates_query(item), *coverage_queries(item), *held_query(item)):
            self.assertTrue(text.startswith("SELECT COUNT(*) AS n FROM"), text)

    def test_a_dimension_with_a_name_that_is_not_plain_is_refused_before_a_query_is_made(self) -> None:
        from spark.gold_dimensions import Dimension
        from spark.silver_cases import STRING, Column

        bad = Dimension("dim_bad", Column("name; DROP", STRING), "silver.cases", "caseid")
        with self.assertRaises(GoldDimensionsError):
            members_query(bad)


if __name__ == "__main__":
    unittest.main()
