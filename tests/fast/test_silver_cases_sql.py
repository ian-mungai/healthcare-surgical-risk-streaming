"""The Spark SQL text of the Silver cases job: what each typed column is built from, which rows set the cutoffs and what each check counts.

No Spark runs here, so these tests hold the text to the rules: a column cast to another type, a development filter that lets the test set in, a check that
counts the wrong thing, or a value written into a query would each change Silver or the checks without a failure. The queries run in the acceptance run.
"""

from __future__ import annotations

import re
import unittest

from spark.silver_cases import (
    CASE_COLUMNS,
    DEVELOPMENT_SETS,
    FLAG_COLUMNS,
    LAB_COLUMNS,
    OUTCOME_COLUMNS,
    PINNED_LAB_ROWS,
    SilverCasesError,
    age_over_89_value,
)
from spark.silver_cases_sql import (
    BRONZE_CASES_SOURCE,
    BRONZE_LAB_SOURCE,
    CASES_SOURCE,
    LAB_SOURCE,
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
    lab_source_empty_query,
    lab_unmatched_query,
    outcome_facts_query,
    outcome_output_query,
    ownership_query,
    sealed_lab_form_query,
    sealed_lab_output_query,
    sealed_outcome_output_query,
    sealed_outcome_query,
    silver_lab_orphans_query,
    split_facts_queries,
    typed_expression,
    values_query,
)

DEV_BRONZE = lab_scope(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=True, lab_text=True)
HELD_BRONZE = lab_scope(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=False, lab_text=True)
CHECK_DEV_BRONZE = lab_scope(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=True, lab_text=True, strict=True)
CHECK_HELD_BRONZE = lab_scope(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=False, lab_text=True, strict=True)
DEV_SILVER = lab_scope(LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=True, lab_text=False, strict=True)
HELD_SILVER = lab_scope(LAB_SOURCE, BRONZE_CASES_SOURCE, SPLIT_SOURCE, development=False, lab_text=False, strict=True)


def select_list(query: str) -> list[str]:
    """The expressions of the first SELECT of a query, split at the commas that are not inside parentheses."""
    body = query.split("SELECT", 1)[1].rsplit("FROM", 1)[0]
    items, depth, current = [], 0, ""
    for character in body:
        depth += character == "("
        depth -= character == ")"
        if character == "," and depth == 0:
            items.append(current.strip())
            current = ""
        else:
            current += character
    return [*items, current.strip()]


class TypedExpressionTest(unittest.TestCase):
    def test_a_whole_number_column_is_a_checked_try_cast_to_bigint(self) -> None:
        expression = typed_expression("opstart", "bigint")
        self.assertIn("TRY_CAST(`opstart` AS BIGINT)", expression)
        self.assertIn("RLIKE '^-?[0-9]+\\\\z'", expression)
        self.assertTrue(expression.startswith("CASE WHEN"))

    def test_a_decimal_column_is_a_checked_try_cast_to_double_with_no_exponent_and_no_plus(self) -> None:
        expression = typed_expression("height", "double")
        self.assertIn("TRY_CAST(`height` AS DOUBLE)", expression)
        self.assertIn("RLIKE '^-?[0-9]+([.][0-9]+)?\\\\z'", expression)

    def test_a_text_column_keeps_its_value_and_only_an_empty_text_becomes_missing(self) -> None:
        self.assertEqual("CASE WHEN `dx` IS NULL OR `dx` = '' THEN NULL ELSE `dx` END", typed_expression("dx", "string"))

    def test_a_table_alias_prefixes_the_column_and_nothing_else(self) -> None:
        self.assertIn("`c`.`opend`", typed_expression("opend", "bigint", "c"))
        self.assertNotIn("`opend` AS", typed_expression("opend", "bigint", "c").replace("`c`.`opend`", ""))

    def test_a_column_name_that_is_not_plain_is_refused_so_no_value_can_enter_the_text(self) -> None:
        for name in ("a`b", "a b", "a;b", "", "A", "a'b"):
            with self.assertRaises(SilverCasesError, msg=name):
                typed_expression(name, "string")

    def test_a_type_that_is_not_known_is_refused(self) -> None:
        with self.assertRaises(SilverCasesError):
            typed_expression("age", "decimal")


class AgeTest(unittest.TestCase):
    def test_the_top_coded_age_is_missing_as_a_number_and_flagged_beside_it(self) -> None:
        self.assertIsNone(age_over_89_value(None))
        self.assertIsNone(age_over_89_value(""))
        self.assertTrue(age_over_89_value(">89"))
        for raw in ("89", "0.7", "90", ">90", " >89"):
            self.assertFalse(age_over_89_value(raw), raw)

    def test_the_age_column_is_a_number_and_the_flag_is_computed_from_the_published_text(self) -> None:
        query = cases_query(BRONZE_CASES_SOURCE)
        items = {line.rsplit(" AS ", 1)[1].strip(): line for line in select_list(query)}
        self.assertIn("TRY_CAST(`age` AS DOUBLE)", items["age"])
        self.assertIn("`age` = '>89'", items["age_over_89"])
        self.assertTrue(items["age_over_89"].startswith("CASE WHEN `age` IS NULL OR `age` = '' THEN NULL"))


class CasesQueryTest(unittest.TestCase):
    def test_every_silver_column_is_selected_once_in_order_from_the_bronze_table(self) -> None:
        query = cases_query(BRONZE_CASES_SOURCE)
        names = [line.rsplit(" AS ", 1)[1].strip() for line in select_list(query)]
        self.assertEqual([column.name for column in CASE_COLUMNS], names)
        self.assertTrue(query.rstrip().endswith(f"FROM {BRONZE_CASES_SOURCE}"))

    def test_no_published_value_is_written_into_the_text(self) -> None:
        literals = set(re.findall(r"'[^']*'", cases_query(BRONZE_CASES_SOURCE)))
        self.assertEqual({"''", "'>89'", "'^-?[0-9]+\\\\z'", "'^-?[0-9]+([.][0-9]+)?\\\\z'"}, literals)

    def test_the_build_uses_try_cast_and_the_check_uses_the_plain_cast(self) -> None:
        self.assertNotIn("TRY_CAST", case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE))
        self.assertIn(" CAST(", case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE))
        numbers = sum(1 for column in CASE_COLUMNS if column.type in {"bigint", "double"})
        self.assertEqual(numbers, cases_query(BRONZE_CASES_SOURCE).count("TRY_CAST("))


class LabQueryTest(unittest.TestCase):
    def test_the_laboratory_rows_are_grouped_on_all_four_published_values_and_numbered_by_result(self) -> None:
        query = lab_query(BRONZE_LAB_SOURCE)
        self.assertIn("GROUP BY caseid, dt, name, result", query)
        self.assertIn("COUNT(*) AS INT) AS source_copies", query)
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY caseid, dt, name ORDER BY result ASC NULLS FIRST)", query)
        self.assertEqual("FROM " + BRONZE_LAB_SOURCE, re.search(r"FROM " + re.escape(BRONZE_LAB_SOURCE), query).group(0))  # type: ignore[union-attr]

    def test_the_laboratory_result_is_a_double_and_the_time_and_case_are_whole_numbers(self) -> None:
        query = lab_query(BRONZE_LAB_SOURCE)
        for expression in ("TRY_CAST(`caseid` AS BIGINT)", "TRY_CAST(`dt` AS BIGINT)", "TRY_CAST(`result` AS DOUBLE)"):
            self.assertIn(expression, query)
        self.assertEqual(["caseid", "dt", "name", "result", "source_copies", "source_occurrence"], [column.name for column in LAB_COLUMNS])

    def test_the_final_columns_are_in_the_order_of_the_table(self) -> None:
        names = [item.rsplit(" AS ", 1)[-1] for item in select_list(query_tail(lab_query(BRONZE_LAB_SOURCE)))]
        self.assertEqual([column.name for column in LAB_COLUMNS], names)


def query_tail(query: str) -> str:
    return query[query.rindex("SELECT") :]


class DevelopmentTest(unittest.TestCase):
    def test_the_lengths_come_from_the_development_sets_only_and_never_name_the_test_set(self) -> None:
        query = development_lengths_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        self.assertEqual(("training", "validation"), DEVELOPMENT_SETS)
        self.assertIn("IN ('training', 'validation')", query)
        self.assertNotIn("test", query.replace("TRY_CAST", ""))
        self.assertIn("`opend`", query)
        self.assertIn("`opstart`", query)

    def test_the_length_is_the_end_minus_the_start_of_the_operation_in_seconds(self) -> None:
        query = development_lengths_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        self.assertIn(" AS length_seconds", query)
        self.assertLess(query.index("`c`.`opend`"), query.index(") - ("))
        self.assertLess(query.index(") - ("), query.rindex("`c`.`opstart`"))


class FactsTest(unittest.TestCase):
    def test_the_case_facts_name_one_unparsed_count_for_each_published_column_except_the_text_ones(self) -> None:
        query = case_facts_query(BRONZE_CASES_SOURCE)
        aliases = re.findall(r" AS (unparsed__\w+)", query)
        typed = [column.name for column in CASE_COLUMNS if column.type in {"bigint", "double"} and column.name not in OUTCOME_COLUMNS]
        self.assertEqual([f"unparsed__{name}" for name in typed], aliases)

    def test_no_outcome_column_is_read_in_the_full_cohort_facts(self) -> None:
        query = case_facts_query(BRONZE_CASES_SOURCE)
        for name in OUTCOME_COLUMNS:
            self.assertNotIn(name, query)

    def test_the_age_count_does_not_count_the_top_coded_value_as_unparsed(self) -> None:
        items = {item.rsplit(" AS ", 1)[1]: item for item in select_list(case_facts_query(BRONZE_CASES_SOURCE))}
        self.assertIn("`age` <> '>89'", items["unparsed__age"])
        self.assertNotIn("<> '>89'", items["unparsed__height"])

    def test_the_flags_the_sex_and_the_asa_class_are_held_to_their_published_values(self) -> None:
        query = case_facts_query(BRONZE_CASES_SOURCE)
        for name in FLAG_COLUMNS:
            if name not in OUTCOME_COLUMNS:
                self.assertRegex(query, rf"NOT IN \(0, 1\) THEN 1 ELSE 0 END\), 0\) AS domain__{name}\b")
        self.assertIn("NOT IN ('F', 'M')", query)
        self.assertIn("NOT IN (1, 2, 3, 4, 5, 6)", query)

    def test_a_missing_start_or_end_or_an_end_before_the_start_is_one_failure(self) -> None:
        query = case_facts_query(BRONZE_CASES_SOURCE)
        self.assertIn("AS operation_order", query)
        self.assertRegex(query, r"IS NULL OR [^;]*IS NULL OR [^;]*<")

    def test_the_patient_number_must_be_in_its_published_form(self) -> None:
        self.assertIn("`subjectid` NOT RLIKE '^[1-9][0-9]*\\\\z'", case_facts_query(BRONZE_CASES_SOURCE))

    def test_the_split_join_counts_a_case_that_has_not_exactly_one_split_row_and_a_patient_with_no_case(self) -> None:
        cases, patients = split_facts_queries(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        self.assertIn("HAVING COUNT(s.`subjectid`) <> 1", cases)
        self.assertIn("LEFT JOIN", cases)
        self.assertIn("LEFT ANTI JOIN", patients)

    def test_the_outcome_columns_are_counted_for_development_patients_only_and_never_name_the_held_out_set(self) -> None:
        query = outcome_facts_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        self.assertIn("WHERE s.`set` IN ('training', 'validation')", query)
        for alias in ("unparsed__icu_days", "unparsed__death_inhosp", "domain__death_inhosp"):
            self.assertIn(f" AS {alias}", query)
        self.assertNotIn("test", query.replace("TRY_CAST", ""))

    def test_the_held_out_outcome_check_is_one_flag_that_is_zero_or_one_and_never_a_count(self) -> None:
        query = sealed_outcome_query(BRONZE_CASES_SOURCE, SPLIT_SOURCE)
        self.assertTrue(query.startswith("SELECT CASE WHEN COALESCE(SUM("))
        self.assertIn("> 0 THEN 1 ELSE 0 END AS n", query)
        self.assertIn("COALESCE(s.`set`, '') NOT IN ('training', 'validation')", query)
        self.assertIn("LEFT JOIN", query)
        for name in OUTCOME_COLUMNS:
            self.assertIn(name, query)
        self.assertNotIn("test", query.replace("TRY_CAST", ""))

    def test_the_laboratory_facts_count_the_development_rows_only_and_name_no_held_out_set(self) -> None:
        query = lab_facts_query(DEV_BRONZE)
        for name in ("dt", "result"):
            self.assertIn(f"AS unparsed__{name}", query)
        self.assertIn("AS lab_key_null", query)
        self.assertNotIn("COUNT(*) AS row_count", query)
        self.assertIn("s.`set` IN ('training', 'validation')", query)
        self.assertNotIn("NOT IN ('training'", query)
        self.assertNotIn("test", query.replace("TRY_CAST", ""))

    def test_the_held_out_laboratory_check_is_one_flag_that_is_zero_or_one_and_never_a_count(self) -> None:
        query = sealed_lab_form_query(HELD_BRONZE)
        self.assertTrue(query.startswith("SELECT CASE WHEN COALESCE(SUM("))
        self.assertIn("> 0 THEN 1 ELSE 0 END AS n", query)
        self.assertIn("COALESCE(s.`set`, '') NOT IN ('training', 'validation')", query)
        for name in ("dt", "result", "name"):
            self.assertIn(f"`l`.`{name}`", query)
        self.assertNotIn("test", query.replace("TRY_CAST", ""))

    def test_the_release_total_of_laboratory_rows_is_a_flag_made_inside_the_query(self) -> None:
        query = lab_pinned_query(BRONZE_LAB_SOURCE)
        self.assertEqual("SELECT CASE WHEN COUNT(*) = 928448 THEN 0 ELSE 1 END AS n FROM lake.bronze.lab_results", query)
        self.assertEqual(928448, PINNED_LAB_ROWS)

    def test_the_scope_of_the_laboratory_rows_joins_the_case_and_the_split_and_keeps_only_the_wanted_side(self) -> None:
        for scope in (DEV_BRONZE, HELD_BRONZE, DEV_SILVER, HELD_SILVER):
            self.assertIn("JOIN", scope)
            self.assertIn("LEFT JOIN", scope)
            self.assertIn(SPLIT_SOURCE, scope)
        self.assertIn("WHERE s.`set` IN ('training', 'validation')", DEV_BRONZE)
        self.assertIn("WHERE COALESCE(s.`set`, '') NOT IN ('training', 'validation')", HELD_BRONZE)
        self.assertIn("`l`.`caseid` = CAST(`c`.`caseid` AS BIGINT)", DEV_SILVER)
        self.assertIn("TRY_CAST(`l`.`caseid` AS BIGINT)", DEV_BRONZE)
        self.assertIn("CAST(`l`.`caseid` AS BIGINT) = CAST(`c`.`caseid` AS BIGINT)", CHECK_DEV_BRONZE)
        self.assertNotIn("TRY_CAST", CHECK_DEV_BRONZE)

    def test_both_laboratory_tables_are_scoped_by_the_bronze_case_owner_and_never_by_the_silver_case_row(self) -> None:
        for scope in (DEV_SILVER, HELD_SILVER, DEV_BRONZE, HELD_BRONZE):
            self.assertIn(BRONZE_CASES_SOURCE, scope)
            self.assertNotIn(CASES_SOURCE, scope)
        for scope in (DEV_SILVER, HELD_SILVER):
            self.assertIn(LAB_SOURCE, scope)

    def test_a_laboratory_row_whose_case_has_no_clinical_row_is_one_flag_found_with_an_anti_join_on_the_typed_case_number(self) -> None:
        query = lab_unmatched_query(BRONZE_LAB_SOURCE, BRONZE_CASES_SOURCE)
        self.assertTrue(query.startswith("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS n FROM"))
        self.assertIn(f"FROM {BRONZE_LAB_SOURCE} x LEFT ANTI JOIN {BRONZE_CASES_SOURCE} y ON", query)
        self.assertEqual(2, query.count("TRY_CAST("))

    def test_whether_the_laboratory_table_has_any_row_is_one_bit_of_the_whole_source(self) -> None:
        self.assertEqual("SELECT CASE WHEN EXISTS ( SELECT 1 FROM lake.bronze.lab_results ) THEN 0 ELSE 1 END AS n", lab_source_empty_query(BRONZE_LAB_SOURCE))

    def test_a_fact_row_is_read_back_into_the_names_the_rules_take(self) -> None:
        row = {
            "row_count": 5,
            "key_null": 1,
            "key_duplicates": 2,
            "subjectid_form": 3,
            "operation_order": 4,
            "unparsed__height": 2,
            "unparsed__age": 0,
            "domain__sex": 1,
        }
        found = decode_case_facts(row)
        self.assertEqual(
            {
                "bronze_case_rows": 5,
                "case_key_null": 1,
                "case_key_duplicates": 2,
                "subjectid_form": 3,
                "operation_ends_before_it_starts": 4,
                "unparsed": {"height": 2, "age": 0},
                "domain": {"sex": 1},
            },
            found,
        )
        lab = decode_lab_facts({"lab_source_empty": 0, "lab_key_null": 1, "lab_rows_without_case": 1, "unparsed__result": 3})
        self.assertEqual({"lab_source_empty": 0, "lab_key_null": 1, "lab_rows_without_case": 1, "lab_unparsed": {"result": 3}}, lab)

    def test_a_fact_that_is_not_a_count_is_refused(self) -> None:
        with self.assertRaises(SilverCasesError):
            decode_case_facts({"row_count": None, "key_null": 0, "key_duplicates": 0, "subjectid_form": 0, "operation_order": 0})
        with self.assertRaises(SilverCasesError):
            decode_case_facts({"row_count": -1, "key_null": 0, "key_duplicates": 0, "subjectid_form": 0, "operation_order": 0})


class OutputQueryTest(unittest.TestCase):
    def test_every_silver_column_is_compared_with_its_bronze_value_through_a_null_safe_equality(self) -> None:
        query = case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE)
        outside = [column for column in CASE_COLUMNS if column.name not in OUTCOME_COLUMNS]
        self.assertEqual(len(outside), query.count("<=>"))
        for column in outside:
            if column.name != "age_over_89":
                self.assertIn(f"`s`.`{column.name}`", query)

    def test_the_outcome_columns_are_compared_for_development_patients_only_and_the_held_out_ones_as_one_flag(self) -> None:
        full = case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE)
        for name in OUTCOME_COLUMNS:
            self.assertNotIn(name, full)
        development = outcome_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE, SPLIT_SOURCE)
        self.assertEqual(len(OUTCOME_COLUMNS), development.count("<=>"))
        self.assertIn("p.`set` IN ('training', 'validation')", development)
        flag = sealed_outcome_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE, SPLIT_SOURCE)
        self.assertTrue(flag.startswith("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS n"))
        self.assertIn("COALESCE(p.`set`, '') NOT IN ('training', 'validation')", flag)
        self.assertEqual(len(OUTCOME_COLUMNS), flag.count("<=>"))

    def test_the_case_keys_are_compared_both_ways_with_anti_joins_so_a_replaced_key_fails(self) -> None:
        query = case_key_query(BRONZE_CASES_SOURCE, CASES_SOURCE)
        self.assertEqual(2, query.count("LEFT ANTI JOIN"))
        self.assertEqual(1, query.count("UNION ALL"))
        left, right = query.split("UNION ALL")
        self.assertLess(left.index(BRONZE_CASES_SOURCE), left.index(CASES_SOURCE))
        self.assertLess(right.index(CASES_SOURCE), right.index(BRONZE_CASES_SOURCE))

    def test_a_missing_bronze_value_is_compared_as_missing(self) -> None:
        self.assertIn("NULLIF(`b`.`height`, '')", case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE))

    def test_the_top_coded_age_is_compared_as_missing_and_the_flag_as_true(self) -> None:
        query = case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE)
        self.assertIn("`b`.`age` = '>89'", query)

    def test_the_laboratory_check_compares_the_groups_of_bronze_with_silver_in_both_directions(self) -> None:
        differences, duplicates, gaps, copies = lab_output_queries(CHECK_DEV_BRONZE, DEV_SILVER)
        self.assertEqual(2, differences.count("EXCEPT ALL"))
        self.assertEqual(2, differences.count("( ( SELECT"))
        self.assertEqual(1, differences.count("UNION ALL"))
        self.assertIn("CAST(COUNT(*) AS INT)", differences)
        self.assertNotIn("TRY_CAST", differences)
        self.assertIn("GROUP BY `caseid`, `dt`, `name`, `source_occurrence`", duplicates)
        self.assertIn("HAVING MIN(`source_occurrence`) <> 1 OR MAX(`source_occurrence`) <> COUNT(*)", gaps)
        self.assertIn("SUM(`source_copies`)", copies)

    def test_each_laboratory_row_is_compared_with_its_expected_occurrence_beside_its_result_and_copy_count(self) -> None:
        differences = lab_output_queries(CHECK_DEV_BRONZE, DEV_SILVER)[0]
        self.assertEqual(2, differences.count("ROW_NUMBER() OVER (PARTITION BY `caseid`, `dt`, `name` ORDER BY `result` ASC NULLS FIRST)"))
        for side in differences.split(" UNION ALL "):
            self.assertEqual(1, side.count("ROW_NUMBER()"))
            self.assertEqual(1, side.count("`source_copies`, `source_occurrence` FROM"))


class LabCheckQueryTest(unittest.TestCase):
    def test_both_sides_are_grouped_on_all_four_values_and_each_direction_is_bronze_against_silver_then_silver_against_bronze(self) -> None:
        differences = lab_output_queries(CHECK_DEV_BRONZE, DEV_SILVER)[0]
        self.assertEqual(2, differences.count("GROUP BY `caseid`, `dt`, `name`, `result`"))
        first, second = differences.split(" UNION ALL ")
        self.assertLess(first.index(BRONZE_LAB_SOURCE), first.index(LAB_SOURCE))
        self.assertLess(second.index(LAB_SOURCE), second.index(BRONZE_LAB_SOURCE))
        self.assertEqual(1, first.count(BRONZE_LAB_SOURCE))
        self.assertEqual(1, second.count(BRONZE_LAB_SOURCE))

    def test_the_held_out_laboratory_comparison_is_one_flag_made_of_the_three_checks_and_never_a_count(self) -> None:
        query = sealed_lab_output_query(CHECK_HELD_BRONZE, HELD_SILVER)
        self.assertTrue(query.startswith("SELECT CASE WHEN ("))
        self.assertTrue(query.endswith("> 0 THEN 1 ELSE 0 END AS n"))
        differences, duplicates, gaps, _ = lab_output_queries(CHECK_HELD_BRONZE, HELD_SILVER)
        for part in (differences, duplicates, gaps):
            self.assertIn(part, query)
        self.assertEqual(3, query.count("SELECT COUNT(*) AS n FROM"))
        self.assertNotIn("test", query.replace("TRY_CAST", ""))

    def test_a_written_laboratory_row_with_no_bronze_case_is_one_flag_found_with_an_anti_join(self) -> None:
        query = silver_lab_orphans_query(LAB_SOURCE, BRONZE_CASES_SOURCE)
        self.assertEqual(
            "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS n FROM lake.silver.lab_results l "
            "LEFT ANTI JOIN lake.bronze.clinical_cases c ON `l`.`caseid` = CAST(`c`.`caseid` AS BIGINT)",
            query,
        )

    def test_an_empty_bronze_text_is_missing_in_every_check_of_a_column(self) -> None:
        query = case_output_query(BRONZE_CASES_SOURCE, CASES_SOURCE)
        outside = [column for column in CASE_COLUMNS if column.name not in OUTCOME_COLUMNS]
        for column in outside:
            if column.name != "age_over_89":
                self.assertIn(f"NULLIF(`b`.`{column.name}`, '')" if column.name != "age" else "NULLIF(`b`.`age`, '')", query, column.name)
        self.assertEqual(len(outside), query.count("NULLIF("))


class CountQueryTest(unittest.TestCase):
    def test_a_row_count_and_a_repeated_case_number_are_counted_on_the_table_that_is_named(self) -> None:
        self.assertEqual("SELECT COUNT(*) AS n FROM lake.silver.cases", count_query(CASES_SOURCE))
        self.assertEqual("SELECT COUNT(*) - COUNT(DISTINCT `caseid`) AS n FROM lake.silver.cases", case_duplicates_query(CASES_SOURCE))


class OwnershipTest(unittest.TestCase):
    def test_a_case_that_is_there_twice_or_under_two_patients_is_one_flag_from_the_case_number_alone(self) -> None:
        query = ownership_query(BRONZE_CASES_SOURCE)
        self.assertTrue(query.startswith("SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS n FROM ( SELECT"))
        self.assertIn("GROUP BY CASE WHEN `caseid` RLIKE", query)
        self.assertIn("HAVING COUNT(*) > 1", query)
        self.assertNotIn("subjectid", query)
        self.assertNotIn("`set`", query)
        self.assertIn(f"FROM {BRONZE_CASES_SOURCE}", query)

    def test_case_numbers_that_cannot_be_typed_are_left_out_so_two_of_them_do_not_make_one_ambiguous_owner(self) -> None:
        query = ownership_query(BRONZE_CASES_SOURCE)
        guarded = "CASE WHEN `caseid` RLIKE '^-?[0-9]+\\\\z' THEN TRY_CAST(`caseid` AS BIGINT) END"
        self.assertIn(f"WHERE ({guarded}) IS NOT NULL GROUP BY {guarded}", query)


class ValuesTest(unittest.TestCase):
    def test_a_row_is_a_literal_in_the_column_order_and_a_missing_value_is_null(self) -> None:
        query = values_query([("start", "opstart", 0, "fixed", "v", 3, 5400), ("final", "opend", None, "fixed", "v", 3, 5400)])
        self.assertEqual("VALUES ('start', 'opstart', 0L, 'fixed', 'v', 3L, 5400L), ('final', 'opend', CAST(NULL AS BIGINT), 'fixed', 'v', 3L, 5400L)", query)

    def test_a_text_with_a_quote_or_other_unsafe_characters_is_refused(self) -> None:
        for text in ("a'b", "a;b", "a b\n", "a\\b", ""):
            with self.assertRaises(SilverCasesError, msg=text):
                values_query([(text, 1)])

    def test_no_rows_and_a_value_of_another_type_are_refused(self) -> None:
        with self.assertRaises(SilverCasesError):
            values_query([])
        with self.assertRaises(SilverCasesError):
            values_query([(1.5,)])


if __name__ == "__main__":
    unittest.main()
