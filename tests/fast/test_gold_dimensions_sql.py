"""The Spark SQL text of the Gold dimensions job: each key, the source columns read, the versions and what each check counts.

No Spark runs here, so these tests hold the text to the rules: a key made from another text, a query that reads an outcome, the patient split or a laboratory
result, an unknown member with a value in it, a closed version that is not closed at the run's date, or a check that counts the wrong thing would each change
Gold or the checks without a failure. The queries run in the acceptance run.

Failure modes covered here:

- A key expression that differs from the rule (another digest, another length, no dimension name or no version date in the text).
- A query that reads a column that is not a source column of its dimension, such as an outcome, a split set or a laboratory result.
- An unknown member that carries a value, has another key or is missing.
- A rebuild that renumbers a held version, that does not close the open version of a changed member, or that opens a version for an unchanged one.
- A count of distinct keys that counts rows, or a check that lets two descriptions of one member through.
"""

from __future__ import annotations

import re
import unittest

from spark.gold_dimensions import DIMENSIONS, KEY_HEX_DIGITS, KEY_LIMIT, UNKNOWN_KEY, Dimension, GoldDimensionsError, dimension
from spark.gold_dimensions_sql import (
    ambiguous_query,
    chain_query,
    coverage_queries,
    desired_query,
    held_query,
    key_expression,
    member_differs_query,
    members_query,
    natural_duplicates_query,
    natural_keys_query,
    next_query,
    output_facts_query,
    source_facts_query,
    source_name,
    table_name,
)
from spark.silver_cases import STRING, Column

NAMES = re.compile(r"`([A-Za-z0-9_]+)`")
BANNED = ("patient_split", "icu_days", "death_inhosp", "intraop_", "preop_", "`set`", "`result`", "`value`", "`samples`")
DATE = "2026-11-23"
CASE = dimension("dim_case")


def queries_of(item: Dimension) -> list[str]:
    return [
        members_query(item),
        desired_query(item, DATE),
        next_query(item, DATE),
        source_facts_query(item),
        ambiguous_query(item),
        output_facts_query(item),
        natural_duplicates_query(item),
        natural_keys_query(item),
        chain_query(item),
        member_differs_query(item),
        *coverage_queries(item),
        *held_query(item, DATE),
    ]


class KeyExpressionTests(unittest.TestCase):
    def test_the_key_is_the_rule_of_the_module_written_in_spark_sql(self) -> None:
        text = key_expression(CASE, "case_id", f"DATE'{DATE}'")
        self.assertEqual(
            "CAST(CONV(SUBSTRING(SHA2(CONCAT('dim_case:', CAST(`case_id` AS STRING), ':', "
            f"CAST(DATE'{DATE}' AS STRING)), 256), 1, {KEY_HEX_DIGITS}), 16, 10) AS BIGINT)",
            text,
        )

    def test_each_dimension_puts_its_own_name_in_the_text_that_is_hashed(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                self.assertIn(f"CONCAT('{item.name}:', CAST(`{item.natural.name}` AS STRING)", key_expression(item, item.natural.name, "`valid_from`"))

    def test_the_key_cannot_reach_the_unknown_key_or_the_limit(self) -> None:
        self.assertEqual(-1, UNKNOWN_KEY)
        self.assertEqual(16**KEY_HEX_DIGITS, KEY_LIMIT)

    def test_the_version_date_of_a_query_is_a_checked_date(self) -> None:
        for bad in ("2026-11-23'; DROP", "tomorrow", ""):
            with self.subTest(bad=bad), self.assertRaises(GoldDimensionsError):
                desired_query(CASE, bad)


class SourceTests(unittest.TestCase):
    def test_a_query_reads_only_its_dimensions_source_table_of_the_lake(self) -> None:
        for item in DIMENSIONS:
            for text in (members_query(item), source_facts_query(item), ambiguous_query(item), *coverage_queries(item), held_query(item, DATE)[0]):
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
        self.assertEqual("lake.silver.cases", source_name(CASE))
        self.assertEqual("lake.gold.dim_case", table_name(CASE))

    def test_a_dimension_with_a_name_that_is_not_plain_is_refused_before_a_query_is_made(self) -> None:
        bad = Dimension("dim_bad", Column("name; DROP", STRING), "silver.cases", "caseid")
        with self.assertRaises(GoldDimensionsError):
            members_query(bad)


class FirstBuildTests(unittest.TestCase):
    def test_the_rows_are_the_first_versions_of_the_members_and_then_the_unknown_member(self) -> None:
        text = desired_query(CASE, DATE)
        self.assertEqual(1, text.count("UNION ALL"))
        members, unknown = text.split(" UNION ALL ")
        self.assertIn(f"DATE'{DATE}' AS `valid_from`, CAST(NULL AS DATE) AS `valid_to`, TRUE AS `is_current` FROM (", members)
        self.assertEqual(
            "SELECT CAST(-1 AS BIGINT) AS `case_key`, CAST(NULL AS BIGINT) AS `case_id`, CAST(NULL AS BIGINT) AS `emop`, CAST(NULL AS STRING) AS `approach`, "
            "CAST(NULL AS STRING) AS `position`, CAST(NULL AS STRING) AS `ane_type`, CAST(NULL AS DATE) AS `valid_from`, CAST(NULL AS DATE) AS `valid_to`, "
            "TRUE AS `is_current`",
            unknown,
        )

    def test_the_unknown_member_of_every_dimension_holds_nothing_but_its_key_and_the_open_flag(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                unknown = desired_query(item, DATE).split(" UNION ALL ")[1]
                self.assertEqual(len(item.columns), unknown.count(" AS `"))
                self.assertEqual(len(item.columns) - 2, unknown.count("CAST(NULL AS "))
                self.assertEqual(1, unknown.count("CAST(-1 AS BIGINT)"))


class LaterBuildTests(unittest.TestCase):
    def test_every_held_version_is_kept_with_its_own_key_and_only_the_open_version_of_a_changed_member_is_closed_at_the_run_date(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                held, fresh = next_query(item, DATE).split(" UNION ALL SELECT ", 1)
                self.assertIn(f"h.`{item.key}`", held)
                self.assertIn(f"CASE WHEN ch.`{item.natural.name}` IS NOT NULL AND h.`is_current` THEN DATE'{DATE}' ELSE h.`valid_to` END AS `valid_to`", held)
                self.assertIn("THEN FALSE ELSE h.`is_current` END AS `is_current`", held)
                self.assertIn("LEFT JOIN", held)
                self.assertNotIn("SHA2", held)
                self.assertIn("SHA2", fresh)
                self.assertIn(f"DATE'{DATE}' AS `valid_from`, CAST(NULL AS DATE) AS `valid_to`, TRUE AS `is_current`", fresh)

    def test_a_new_version_is_opened_for_a_changed_member_and_for_a_new_member_only(self) -> None:
        text = next_query(CASE, DATE)
        self.assertIn("NOT (s.`emop` <=> c.`emop`) OR NOT (s.`approach` <=> c.`approach`)", text)
        self.assertIn("LEFT ANTI JOIN", text)
        self.assertIn("WHERE `is_current` AND NOT (`case_key` <=> -1)", text)

    def test_a_dimension_with_no_descriptive_column_never_has_a_changed_member(self) -> None:
        self.assertIn("WHERE FALSE", next_query(dimension("dim_patient"), DATE))


class FactTests(unittest.TestCase):
    def test_the_source_facts_count_distinct_natural_keys_and_name_their_results_as_the_rules_do(self) -> None:
        for item in DIMENSIONS:
            text = source_facts_query(item)
            self.assertEqual(["source_members", "key_null_rows"], re.findall(r"AS (\w+)", text))
            self.assertIn(f"COUNT(DISTINCT `{item.source_natural}`) AS source_members", text)
            self.assertEqual(["n"], re.findall(r"AS (\w+)", ambiguous_query(item)))

    def test_two_descriptions_of_one_member_are_counted_by_grouping_the_members_on_the_natural_key(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                text = ambiguous_query(item)
                self.assertTrue(text.endswith(f"GROUP BY `{item.natural.name}` HAVING COUNT(*) > 1)"), text)
                self.assertIn(members_query(item), text)

    def test_the_output_facts_are_one_row_of_counts_that_count_distinct_keys_and_treat_the_unknown_key_apart(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                text = output_facts_query(item)
                self.assertEqual(
                    ["members", "unknown_rows", "unknown_not_empty", "key_duplicates", "key_out_of_range", "natural_null", "interval_wrong"],
                    re.findall(r"AS (\w+)", text),
                )
                self.assertIn(f"COUNT(*) - COUNT(DISTINCT `{item.key}`) AS key_duplicates", text)
                self.assertIn(f"NOT (`{item.key}` <=> -1) AND `is_current`", text)
                self.assertIn(f"`{item.key}` >= {KEY_LIMIT}", text)

    def test_the_unknown_member_must_hold_no_natural_key_no_descriptive_column_and_no_date(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                text = output_facts_query(item)
                for column in (*item.described, *item.columns[-3:-1]):
                    self.assertIn(f"`{column.name}` IS NOT NULL", text)

    def test_a_version_must_start_before_it_closes_and_close_exactly_when_it_is_not_current(self) -> None:
        text = output_facts_query(CASE)
        self.assertIn("`is_current` <> (`valid_to` IS NULL)", text)
        self.assertIn("`valid_to` <= `valid_from`", text)

    def test_a_closed_version_must_be_followed_by_a_version_that_starts_on_its_closing_date(self) -> None:
        text = chain_query(CASE)
        self.assertTrue(text.startswith("SELECT COUNT(*) AS n FROM"))
        self.assertIn("LEFT ANTI JOIN", text)
        self.assertIn("v.`valid_to` = w.`valid_from`", text)
        self.assertIn("`valid_to` IS NOT NULL", text)

    def test_the_natural_duplicates_are_counted_among_the_open_members_only(self) -> None:
        text = natural_duplicates_query(dimension("dim_department"))
        self.assertIn("WHERE `is_current` AND NOT (`department_key` <=> -1)", text)
        self.assertIn("GROUP BY `department_name` HAVING COUNT(*) > 1", text)

    def test_the_versions_that_python_checks_are_all_versions_without_the_unknown_member_with_their_start_as_text(self) -> None:
        text = natural_keys_query(dimension("dim_lab_test"))
        self.assertEqual(
            "SELECT `lab_test_key`, `lab_test_name`, CAST(`valid_from` AS STRING) AS `valid_from` "
            "FROM lake.gold.dim_lab_test WHERE NOT (`lab_test_key` <=> -1)",
            text,
        )

    def test_the_coverage_is_counted_in_both_directions_over_the_open_versions(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                missing, extra = coverage_queries(item)
                for text in (missing, extra):
                    self.assertIn("LEFT ANTI JOIN", text)
                    self.assertIn(f"FROM lake.{item.source}", text)
                    self.assertIn("WHERE `is_current`", text)
                source, table = f"FROM lake.{item.source}", f"FROM lake.gold.{item.name}"
                self.assertLess(missing.index(source), missing.index(table), "the missing members are the source keys that the table lacks")
                self.assertLess(extra.index(table), extra.index(source), "the extra members are the table keys that the source lacks")

    def test_the_open_versions_are_compared_with_the_source_by_every_descriptive_column(self) -> None:
        text = member_differs_query(CASE)
        for column in ("emop", "approach", "position", "ane_type"):
            self.assertIn(f"NOT (c.`{column}` <=> s.`{column}`)", text)
        self.assertIn("WHERE `is_current`", text)

    def test_the_held_checks_count_removed_members_and_changed_members_whose_version_could_not_follow(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                removed, not_after = held_query(item, DATE)
                self.assertIn("LEFT ANTI JOIN", removed)
                self.assertIn(f"lake.gold.{item.name}", removed)
                self.assertIn(f"c.`valid_from` >= DATE'{DATE}'", not_after)
        for column in ("emop", "approach"):
            self.assertIn(f"NOT (c.`{column}` <=> s.`{column}`)", held_query(CASE, DATE)[1])

    def test_every_count_query_names_its_result_n(self) -> None:
        item = dimension("dim_track")
        for text in (
            ambiguous_query(item),
            natural_duplicates_query(item),
            chain_query(item),
            member_differs_query(item),
            *coverage_queries(item),
            *held_query(item, DATE),
        ):
            self.assertTrue(text.startswith("SELECT COUNT(*) AS n FROM"), text)


if __name__ == "__main__":
    unittest.main()
