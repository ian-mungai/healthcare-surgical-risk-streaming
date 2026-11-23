"""The rules of ``silver.cases`` and ``silver.lab_results``: the column list and its types, how a published value becomes a typed one, which columns are never
features, the measured length of an operation and the score cutoffs that come from it, and what each check refuses.

The Spark job applies these rules. A wrong rule changes a value in Silver without a failure, so each test holds one way that could happen.
"""

from __future__ import annotations

import unittest
from dataclasses import replace

from features.leakage import is_forbidden
from scripts.sample.split import patient_part
from spark.silver_cases import (
    CASE_COLUMNS,
    CUTOFF_FRACTIONS,
    LAB_COLUMNS,
    NEVER_FEATURE_COLUMNS,
    OUTCOME_COLUMNS,
    PUBLISHED_CLINICAL_COLUMNS,
    SOURCE_SCHEMAS,
    SilverCasesError,
    cutoff_rows,
    frozen_problems,
    input_problems,
    measure_rows,
    nearest_rank,
    output_problems,
    source_schema_problems,
    split_rule_problems,
    typed_value,
)
from spark.split_table import RULE_VERSION, SET_NAMES, SplitRow

CLINICAL_HEADER = (
    "caseid,subjectid,casestart,caseend,anestart,aneend,opstart,opend,adm,dis,icu_days,death_inhosp,age,sex,height,weight,bmi,asa,emop,department,optype,dx,"
    "opname,approach,position,ane_type,preop_htn,preop_dm,preop_ecg,preop_pft,preop_hb,preop_plt,preop_pt,preop_aptt,preop_na,preop_k,preop_gluc,preop_alb,"
    "preop_ast,preop_alt,preop_bun,preop_cr,preop_ph,preop_hco3,preop_be,preop_pao2,preop_paco2,preop_sao2,cormack,airway,tubesize,dltubesize,lmasize,iv1,"
    "iv2,aline1,aline2,cline1,cline2,intraop_ebl,intraop_uo,intraop_rbc,intraop_ffp,intraop_crystalloid,intraop_colloid,intraop_ppf,intraop_mdz,intraop_ftn,"
    "intraop_rocu,intraop_vecu,intraop_eph,intraop_phe,intraop_epi,intraop_ca"
)


def refusal(call: object) -> str:
    try:
        call()  # type: ignore[operator]
    except SilverCasesError as error:
        return str(error)
    raise AssertionError("the rules accepted what they must refuse")


def clean_input(**changes: object) -> dict[str, object]:
    base: dict[str, object] = {
        "bronze_case_rows": 6388,
        "case_key_null": 0,
        "case_key_duplicates": 0,
        "subjectid_form": 0,
        "unparsed": {},
        "domain": {},
        "operation_ends_before_it_starts": 0,
        "cases_without_one_split_row": 0,
        "split_patients_without_case": 0,
        "lab_source_empty": 0,
        "lab_key_null": 0,
        "lab_unparsed": {},
        "lab_rows_without_case": 0,
        "sealed_outcome_form_bad": 0,
        "lab_pinned_mismatch": 0,
        "sealed_lab_form_bad": 0,
        "development_cases": 4900,
    }
    return base | changes


def clean_output(**changes: object) -> dict[str, object]:
    base: dict[str, object] = {
        "case_rows": 6388,
        "bronze_case_rows": 6388,
        "case_value_differences": 0,
        "case_key_duplicates": 0,
        "lab_rows": 690000,
        "lab_copies": 700000,
        "bronze_lab_rows": 700000,
        "lab_value_differences": 0,
        "lab_key_duplicates": 0,
        "lab_occurrence_gaps": 0,
        "never_feature_marking_wrong": 0,
        "case_key_differences": 0,
        "sealed_outcome_differs": 0,
        "silver_lab_orphans": 0,
        "sealed_lab_differs": 0,
    }
    return base | changes


class ColumnListTest(unittest.TestCase):
    def test_the_published_columns_are_the_74_of_the_pinned_release_in_order(self) -> None:
        self.assertEqual(tuple(CLINICAL_HEADER.split(",")), PUBLISHED_CLINICAL_COLUMNS)
        self.assertEqual(74, len(PUBLISHED_CLINICAL_COLUMNS))

    def test_silver_has_every_published_column_in_order_and_one_added_column_beside_age(self) -> None:
        names = [column.name for column in CASE_COLUMNS]
        self.assertEqual(list(PUBLISHED_CLINICAL_COLUMNS[:13]) + ["age_over_89"] + list(PUBLISHED_CLINICAL_COLUMNS[13:]), names)
        self.assertEqual(len(names), len(set(names)))

    def test_no_owner_renamed_field_exists_and_the_stay_keeps_the_releases_name(self) -> None:
        names = {column.name for column in CASE_COLUMNS}
        self.assertIn("icu_days", names)
        self.assertTrue({"opdur", "anedur", "los_postop", "los_icu"}.isdisjoint(names))

    def test_identifiers_are_a_whole_number_for_the_case_and_the_published_text_for_the_patient(self) -> None:
        types = {column.name: column.type for column in CASE_COLUMNS}
        self.assertEqual(("bigint", "string"), (types["caseid"], types["subjectid"]))

    def test_the_laboratory_table_keeps_the_published_columns_and_adds_the_copy_count_and_the_occurrence(self) -> None:
        self.assertEqual(
            [("caseid", "bigint"), ("dt", "bigint"), ("name", "string"), ("result", "double"), ("source_copies", "int"), ("source_occurrence", "int")],
            [(column.name, column.type) for column in LAB_COLUMNS],
        )

    def test_the_stated_units_stay_as_published_so_no_column_is_scaled(self) -> None:
        for column in CASE_COLUMNS:
            self.assertIn(column.type, {"bigint", "double", "string", "boolean"})
        self.assertEqual("double", {column.name: column.type for column in CASE_COLUMNS}["height"])


class NeverFeatureTest(unittest.TestCase):
    def test_the_marked_columns_are_exactly_the_ones_the_feature_code_refuses(self) -> None:
        refused = {column.name for column in CASE_COLUMNS if is_forbidden(column.name)}
        self.assertEqual(refused, set(NEVER_FEATURE_COLUMNS))
        self.assertEqual(len(NEVER_FEATURE_COLUMNS), len(set(NEVER_FEATURE_COLUMNS)))

    def test_the_end_times_the_stay_the_death_and_every_intraoperative_total_are_marked(self) -> None:
        for name in ("caseend", "opend", "aneend", "dis", "icu_days", "death_inhosp", "intraop_ebl", "intraop_ca"):
            self.assertIn(name, NEVER_FEATURE_COLUMNS)
        for name in ("opstart", "adm", "age", "preop_cr", "emop"):
            self.assertNotIn(name, NEVER_FEATURE_COLUMNS)


class TypedValueTest(unittest.TestCase):
    def test_a_missing_value_stays_missing_for_every_type_and_is_never_filled(self) -> None:
        for kind in ("bigint", "double", "string"):
            self.assertIsNone(typed_value(kind, None))
            self.assertIsNone(typed_value(kind, ""))

    def test_a_whitespace_value_is_not_missing_and_not_a_number(self) -> None:
        self.assertEqual(" ", typed_value("string", " "))
        self.assertEqual("refused", refusal_or_value("bigint", " 12"))

    def test_a_number_keeps_its_published_value(self) -> None:
        self.assertEqual(-236220, typed_value("bigint", "-236220"))
        self.assertEqual(5117.999999, typed_value("double", "5117.999999"))
        self.assertEqual(26.3, typed_value("double", "26.3"))
        self.assertEqual(3.0, typed_value("double", "3"))

    def test_a_text_keeps_every_published_character_including_trailing_blanks(self) -> None:
        self.assertEqual("Rectal cancer ", typed_value("string", "Rectal cancer "))

    def test_a_value_that_is_not_in_the_published_form_is_refused_by_the_type(self) -> None:
        for raw in ("1e3", "NaN", "Infinity", "1,5", "+4", "1_0", "٣", "4.", ".5", "--1", "0x10"):
            self.assertEqual("refused", refusal_or_value("double", raw), raw)
        for raw in ("1.5", "1e3", " 1", "1_0", "٣", "+4"):
            self.assertEqual("refused", refusal_or_value("bigint", raw), raw)

    def test_a_whole_number_beyond_the_range_of_a_64_bit_integer_is_refused(self) -> None:
        self.assertEqual(2**63 - 1, typed_value("bigint", str(2**63 - 1)))
        self.assertEqual("refused", refusal_or_value("bigint", str(2**63)))

    def test_the_signed_64_bit_minimum_is_accepted_as_spark_accepts_it_and_one_below_is_refused(self) -> None:
        self.assertEqual(-(2**63), typed_value("bigint", str(-(2**63))))
        self.assertEqual("refused", refusal_or_value("bigint", str(-(2**63) - 1)))


def refusal_or_value(kind: str, raw: str) -> object:
    try:
        return typed_value(kind, raw)
    except SilverCasesError:
        return "refused"


class OperationLengthTest(unittest.TestCase):
    def test_nearest_rank_takes_the_value_at_the_rank_ceil_of_p_times_n_over_100(self) -> None:
        values = list(range(1, 11))
        self.assertEqual([1, 3, 5, 8, 10], [nearest_rank(values, percent) for percent in (5, 25, 50, 75, 100)])
        self.assertEqual(7, nearest_rank([7], 50))

    def test_the_measure_holds_the_count_the_extremes_and_the_percentiles_of_the_lengths_in_seconds(self) -> None:
        rows = {row.statistic: (row.unit, row.value) for row in measure_rows([int(value) for value in range(100, 1100, 10)])}
        self.assertEqual(("cases", 100), rows["cases"])
        self.assertEqual(("seconds", 100), rows["min"])
        self.assertEqual(("seconds", 1090), rows["max"])
        self.assertEqual(("seconds", 590), rows["p50"])
        self.assertEqual(("seconds", 1040), rows["p95"])
        self.assertEqual(["cases", "min", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "max"], [row.statistic for row in measure_rows([60, 61])])

    def test_the_measure_does_not_depend_on_the_order_of_the_lengths(self) -> None:
        lengths = [300, 7200, 900, 5400, 1800, 120, 3600]
        self.assertEqual(measure_rows(lengths), measure_rows(sorted(lengths)))
        self.assertEqual(measure_rows(lengths), measure_rows(sorted(lengths, reverse=True)))

    def test_no_development_case_is_a_refusal_because_no_cutoff_can_be_set(self) -> None:
        self.assertIn("no_development_cases", refusal(lambda: measure_rows([])))

    def test_a_negative_length_is_refused_and_a_zero_length_is_kept(self) -> None:
        self.assertIn("bad_length", refusal(lambda: measure_rows([100, -1])))
        self.assertEqual(0, {row.statistic: row.value for row in measure_rows([0, 5])}["min"])


class CutoffTest(unittest.TestCase):
    def test_the_cutoffs_are_the_start_a_quarter_a_half_and_three_quarters_of_the_median_in_whole_minutes_and_the_final(self) -> None:
        rows = cutoff_rows([60 * minute for minute in range(100, 201)] + [60 * 150])
        self.assertEqual(["start", "p25_of_median", "p50_of_median", "p75_of_median", "final"], [row.cutoff for row in rows])
        self.assertEqual(
            [("opstart", 0), ("opstart", 2220), ("opstart", 4500), ("opstart", 6720), ("opend", None)], [(row.anchor, row.elapsed_seconds) for row in rows]
        )
        self.assertEqual((25, 50, 75), CUTOFF_FRACTIONS)

    def test_a_fraction_rounds_down_to_a_whole_minute(self) -> None:
        rows = cutoff_rows([6099] * 3)
        self.assertEqual([0, 1500, 3000, 4560, None], [row.elapsed_seconds for row in rows])

    def test_the_median_of_an_even_number_of_cases_is_the_mean_of_the_two_middle_lengths_and_the_minute_rounding_comes_after_it(self) -> None:
        rows = cutoff_rows([4799, 4800])
        self.assertEqual([0, 1140, 2340, 3540, None], [row.elapsed_seconds for row in rows])
        self.assertEqual({4799}, {row.median_length_seconds for row in rows})
        self.assertEqual([0, 1080, 2220, 3360, None], [row.elapsed_seconds for row in cutoff_rows([3000, 6000])])

    def test_an_odd_number_of_cases_gives_the_same_cutoffs_as_the_nearest_rank_median_did(self) -> None:
        for size in (3, 5, 101, 5125):
            lengths = [85 + (index * 7919 + size * 104729) % 57215 for index in range(size)]
            median = nearest_rank(lengths, 50)
            old = [median * percent // 100 // 60 * 60 for percent in CUTOFF_FRACTIONS]
            rows = cutoff_rows(lengths)
            self.assertEqual([0, *old, None], [row.elapsed_seconds for row in rows], size)
            self.assertEqual({median}, {row.median_length_seconds for row in rows}, size)

    def test_every_row_names_the_development_case_count_the_median_and_one_rule_version(self) -> None:
        rows = cutoff_rows([3600, 7200, 5400])
        self.assertEqual({3}, {row.development_cases for row in rows})
        self.assertEqual({5400}, {row.median_length_seconds for row in rows})
        self.assertEqual(1, len({row.rule_version for row in rows}))

    def test_a_median_too_short_for_four_different_times_is_refused(self) -> None:
        self.assertIn("cutoffs_not_increasing", refusal(lambda: cutoff_rows([200, 200, 200])))

    def test_the_cutoffs_are_the_same_whatever_the_order_of_the_lengths(self) -> None:
        lengths = [3000, 9000, 4500, 1200, 6000]
        self.assertEqual(cutoff_rows(lengths), cutoff_rows(lengths[::-1]))


class FreezeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lengths = [3600, 7200, 5400, 4800]
        self.measure = measure_rows(self.lengths)
        self.cutoffs = cutoff_rows(self.lengths)

    def test_a_first_build_has_nothing_to_freeze(self) -> None:
        self.assertEqual([], frozen_problems(None, None, self.measure, self.cutoffs))

    def test_the_same_measure_and_cutoffs_pass_in_any_order(self) -> None:
        self.assertEqual([], frozen_problems(self.measure, self.cutoffs, self.measure, self.cutoffs))
        self.assertEqual([], frozen_problems(self.measure[::-1], self.cutoffs[::-1], self.measure, self.cutoffs))

    def test_a_changed_cutoff_is_refused_with_its_count_and_every_row_carries_the_development_count_and_the_median(self) -> None:
        other = [*self.lengths, 20000, 20000, 20000]
        problems = frozen_problems(self.measure, self.cutoffs, measure_rows(other), cutoff_rows(other))
        self.assertIn("changed_cutoff: 5 cutoffs", problems)
        self.assertTrue(any(problem.startswith("changed_measure: ") for problem in problems))

    def test_a_wrong_development_count_or_median_with_the_same_times_is_refused(self) -> None:
        held = [replace(row, development_cases=row.development_cases + 1) if row.cutoff == "p50_of_median" else row for row in self.cutoffs]
        self.assertEqual(["changed_cutoff: 1 cutoff"], frozen_problems(self.measure, held, self.measure, self.cutoffs))
        held = [replace(row, median_length_seconds=row.median_length_seconds + 1) if row.cutoff == "final" else row for row in self.cutoffs]
        self.assertEqual(["changed_cutoff: 1 cutoff"], frozen_problems(self.measure, held, self.measure, self.cutoffs))

    def test_a_measure_that_changed_with_the_same_cutoffs_is_still_refused(self) -> None:
        other = [3600, 7200, 5400, 4800, 7300]
        problems = frozen_problems(self.measure, self.cutoffs, measure_rows(other), self.cutoffs)
        self.assertEqual(1, len(problems))
        self.assertTrue(problems[0].startswith("changed_measure: "))

    def test_a_held_cutoff_that_is_there_twice_with_another_time_is_refused_as_a_repeated_key(self) -> None:
        twice = [*self.cutoffs, replace(self.cutoffs[2], elapsed_seconds=999)]
        self.assertEqual(["inherited_duplicates: 1 key"], frozen_problems(self.measure, twice, self.measure, self.cutoffs))

    def test_a_held_measure_row_that_is_there_twice_is_refused_as_a_repeated_key_even_when_the_copy_is_equal(self) -> None:
        twice = [*self.measure, self.measure[3]]
        self.assertEqual(["inherited_duplicates: 1 key"], frozen_problems(twice, self.cutoffs, self.measure, self.cutoffs))
        self.assertEqual(["inherited_duplicates: 1 key"], frozen_problems(self.measure, [*self.cutoffs, self.cutoffs[0]], self.measure, self.cutoffs))

    def test_a_repeated_key_in_both_tables_counts_each_key_once_per_table(self) -> None:
        problems = frozen_problems([*self.measure, self.measure[0]], [*self.cutoffs, self.cutoffs[0]], self.measure, self.cutoffs)
        self.assertEqual(["inherited_duplicates: 2 keys"], problems)

    def test_an_existing_table_with_no_row_is_refused(self) -> None:
        problems = frozen_problems([], self.cutoffs, self.measure, self.cutoffs)
        self.assertEqual(["inherited_empty: 0 rows"], problems)

    def test_an_existing_measure_with_no_cutoffs_or_the_reverse_is_refused(self) -> None:
        self.assertEqual(["inherited_incomplete: the measure and the cutoffs are not both there"], frozen_problems(self.measure, None, [], []))


class InputProblemTest(unittest.TestCase):
    def test_a_clean_input_has_no_problem(self) -> None:
        self.assertEqual([], input_problems(**clean_input(), pinned=True))  # type: ignore[arg-type]

    def test_each_failure_is_one_code_and_a_count_and_never_a_value(self) -> None:
        cases = {
            "case_key_null": ("key_null", "3 rows"),
            "case_key_duplicates": ("key_duplicate", "2 keys"),
            "subjectid_form": ("subjectid_form", "4 rows"),
            "operation_ends_before_it_starts": ("operation_order", "1 row"),
            "cases_without_one_split_row": ("split_join", "5 cases"),
            "split_patients_without_case": ("split_patient_without_case", "2 patients"),
            "lab_key_null": ("lab_key_null", "7 rows"),
        }
        for field, (code, count) in cases.items():
            number = int(count.split()[0])
            problems = input_problems(**clean_input(**{field: number}), pinned=True)  # type: ignore[arg-type]
            self.assertEqual([f"{code}: {count}"], problems, field)

    def test_an_unparsed_value_names_its_columns_and_the_count_of_values(self) -> None:
        problems = input_problems(**clean_input(unparsed={"height": 2, "age": 1}), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["unparsed: 3 values in 2 columns (age, height)"], problems)
        problems = input_problems(**clean_input(lab_unparsed={"result": 1}), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["lab_unparsed: 1 value in 1 column (result)"], problems)

    def test_a_value_outside_the_domain_names_its_column(self) -> None:
        problems = input_problems(**clean_input(domain={"emop": 1, "sex": 2}), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["domain: 3 values in 2 columns (emop, sex)"], problems)

    def test_a_laboratory_row_with_no_case_is_one_flag_with_no_count_because_its_owner_cannot_be_classified(self) -> None:
        problems = input_problems(**clean_input(lab_rows_without_case=1), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["lab_case_missing: a laboratory row has no case"], problems)

    def test_a_held_out_outcome_value_not_in_its_published_form_is_a_code_with_no_count_and_no_column(self) -> None:
        problems = input_problems(**clean_input(sealed_outcome_form_bad=1), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["sealed_outcome_form: a held-out outcome value is not in its published form"], problems)

    def test_no_measurement_yet_skips_the_development_count_check(self) -> None:
        self.assertEqual([], input_problems(**clean_input(development_cases=None), pinned=True))  # type: ignore[arg-type]

    def test_no_development_case_and_empty_tables_are_refused(self) -> None:
        self.assertEqual(["no_development_cases: 0 cases"], input_problems(**clean_input(development_cases=0), pinned=True))  # type: ignore[arg-type]
        problems = input_problems(**clean_input(bronze_case_rows=0, lab_source_empty=1), pinned=False)  # type: ignore[arg-type]
        self.assertIn("empty_cases: 0 rows", problems)
        self.assertIn("empty_lab: the laboratory table has no row", problems)

    def test_a_laboratory_table_that_has_rows_only_for_held_out_patients_is_not_empty(self) -> None:
        self.assertEqual([], input_problems(**clean_input(lab_source_empty=0), pinned=True))  # type: ignore[arg-type]

    def test_the_pinned_release_demands_its_case_count_and_the_laboratory_total_as_a_flag_and_a_sample_does_not(self) -> None:
        problems = input_problems(**clean_input(bronze_case_rows=13, lab_pinned_mismatch=1), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["pinned_counts: 13 cases, not 6388", "pinned_counts: the laboratory row count is not the release's"], problems)
        self.assertEqual([], input_problems(**clean_input(bronze_case_rows=13, lab_pinned_mismatch=1), pinned=False))  # type: ignore[arg-type]

    def test_a_held_out_laboratory_value_not_in_its_published_form_is_a_code_with_no_count_and_no_column(self) -> None:
        problems = input_problems(**clean_input(sealed_lab_form_bad=1), pinned=True)  # type: ignore[arg-type]
        self.assertEqual(["sealed_lab_form: a held-out laboratory value is not in its published form"], problems)

    def test_no_laboratory_total_over_all_patients_is_in_any_message(self) -> None:
        problems = input_problems(**clean_input(bronze_case_rows=13, lab_pinned_mismatch=1, sealed_lab_form_bad=1), pinned=True)  # type: ignore[arg-type]
        self.assertNotIn("928448", " ".join(problems))


class OutcomeColumnTest(unittest.TestCase):
    def test_the_outcome_columns_are_the_stay_and_the_death_and_are_columns_of_the_table(self) -> None:
        self.assertEqual(("icu_days", "death_inhosp"), OUTCOME_COLUMNS)
        self.assertTrue(set(OUTCOME_COLUMNS) <= {column.name for column in CASE_COLUMNS})


def split_rows(count: int) -> list[SplitRow]:
    return [SplitRow(str(patient), SET_NAMES[patient_part(patient)], RULE_VERSION, 7) for patient in range(1, count + 1)]


class SplitRuleTest(unittest.TestCase):
    def test_a_split_made_by_the_rule_has_no_problem(self) -> None:
        self.assertEqual([], split_rule_problems(split_rows(300), pinned=False))

    def test_a_held_out_patient_relabelled_training_is_a_rule_mismatch_and_counted_once(self) -> None:
        rows = split_rows(300)
        held_out = next(index for index, row in enumerate(rows) if row.set == "test")
        rows[held_out] = replace(rows[held_out], set="training")
        self.assertEqual(["rule_mismatch: 1 patient"], split_rule_problems(rows, pinned=False))

    def test_a_patient_in_two_sets_an_unknown_set_and_another_rule_version_are_each_refused(self) -> None:
        rows = split_rows(300)
        problems = split_rule_problems([*rows, replace(rows[0], set="validation")], pinned=False)
        self.assertTrue(any(problem.startswith("two_sets: ") for problem in problems))
        self.assertIn("unknown_set: 1 row", split_rule_problems([*rows[1:], replace(rows[0], set="holdout")], pinned=False))
        self.assertIn("rule_version: 1 row", split_rule_problems([*rows[1:], replace(rows[0], rule_version="other")], pinned=False))


class SourceSchemaTest(unittest.TestCase):
    def found(self) -> dict[str, list[tuple[str, str]] | None]:
        return {table: list(fields) for table, fields in SOURCE_SCHEMAS.items()}

    def test_the_three_tables_have_the_columns_and_types_that_bronze_and_the_split_write(self) -> None:
        self.assertEqual({"bronze.clinical_cases", "bronze.lab_results", "silver.patient_split"}, set(SOURCE_SCHEMAS))
        self.assertEqual([(name, "string") for name in PUBLISHED_CLINICAL_COLUMNS], SOURCE_SCHEMAS["bronze.clinical_cases"])
        self.assertEqual(("source_occurrence", "int"), SOURCE_SCHEMAS["bronze.lab_results"][-1])
        self.assertEqual([], source_schema_problems(self.found()))

    def test_a_missing_table_a_changed_type_an_extra_column_and_a_missing_column_are_each_counted(self) -> None:
        found = self.found()
        found["bronze.lab_results"] = None
        self.assertEqual(["source_missing: 1 table"], source_schema_problems(found))
        found = self.found()
        found["bronze.clinical_cases"] = [(name, "bigint" if name == "caseid" else kind) for name, kind in SOURCE_SCHEMAS["bronze.clinical_cases"]]
        found["silver.patient_split"] = [*SOURCE_SCHEMAS["silver.patient_split"], ("extra", "string")]
        self.assertEqual(["source_schema: 2 tables"], source_schema_problems(found))
        found = self.found()
        found["bronze.lab_results"] = SOURCE_SCHEMAS["bronze.lab_results"][:-1]
        self.assertEqual(["source_schema: 1 table"], source_schema_problems(found))

    def test_a_table_that_is_not_asked_about_counts_as_missing(self) -> None:
        self.assertEqual(["source_missing: 3 tables"], source_schema_problems({}))


class OutputProblemTest(unittest.TestCase):
    def test_more_laboratory_rows_than_copies_is_impossible_and_refused(self) -> None:
        self.assertEqual(["lab_rows: 700001 rows from 700000 copies"], output_problems(**clean_output(lab_rows=700001)))  # type: ignore[arg-type]

    def test_a_held_out_outcome_value_that_differs_from_bronze_is_a_code_with_no_count(self) -> None:
        problems = output_problems(**clean_output(sealed_outcome_differs=1))  # type: ignore[arg-type]
        self.assertEqual(["sealed_outcome_differs: a held-out outcome value differs from Bronze"], problems)

    def test_a_held_out_laboratory_difference_is_a_code_with_no_count_and_silver_rows_with_no_case_are_counted(self) -> None:
        self.assertEqual(
            ["sealed_lab_differs: a held-out laboratory row differs from Bronze or breaks its key"],
            output_problems(**clean_output(sealed_lab_differs=1)),  # type: ignore[arg-type]
        )
        self.assertEqual(["lab_case_missing_in_silver: a written laboratory row has no case"], output_problems(**clean_output(silver_lab_orphans=1)))  # type: ignore[arg-type]

    def test_a_clean_output_has_no_problem(self) -> None:
        self.assertEqual([], output_problems(**clean_output()))  # type: ignore[arg-type]

    def test_a_row_count_that_differs_from_bronze_is_refused_for_both_tables(self) -> None:
        problems = output_problems(**clean_output(case_rows=6387, lab_copies=699999))  # type: ignore[arg-type]
        self.assertEqual(["case_rows: silver 6387, bronze 6388", "lab_rows: the copies add up to 699999, bronze 700000"], problems)

    def test_a_changed_value_a_repeated_key_and_a_gap_in_the_occurrence_are_each_refused(self) -> None:
        cases = {
            "case_value_differences": "case_value_differences: 4 rows",
            "case_key_duplicates": "case_key_duplicate: 4 keys",
            "lab_value_differences": "lab_value_differences: 4 rows",
            "lab_key_duplicates": "lab_key_duplicate: 4 keys",
            "lab_occurrence_gaps": "lab_occurrence_gap: 4 groups",
            "never_feature_marking_wrong": "never_feature_marking: 4 tables",
            "case_key_differences": "case_key_differences: 4 keys",
        }
        for field, text in cases.items():
            self.assertEqual([text], output_problems(**clean_output(**{field: 4})), field)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
