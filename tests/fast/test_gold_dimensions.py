"""The rules of the Gold dimensions: the surrogate key, the unknown member, the columns of each dimension and what each check refuses.

The surrogate key is the one decision that Gold facts and the nightly batch depend on: a member keeps its key for ever, so a rerun and a night that adds
cases give the keys that a first build gave. These tests hold that rule to values that were computed outside this code (``shasum``), and hold each dimension
away from every outcome and laboratory value.

Failure modes covered here:

- A key that depends on the order or the number of the other members (a counter), so that a night of new cases renumbers an old member.
- A key that two dimensions share for the same text, or that collides with the unknown member or is not a positive number below the limit.
- A dimension that carries an outcome, a never-feature column, an intraoperative total, a laboratory value or any column of a held-out patient's result.
- A check that does not name its dimension, or that names a value, a case or a patient.
"""

from __future__ import annotations

import re
import unittest

from spark.gold_dimensions import (
    DIMENSIONS,
    KEY_LIMIT,
    SOURCE_SCHEMAS,
    UNKNOWN_KEY,
    GoldDimensionsError,
    dimension,
    frozen_problems,
    input_problems,
    output_problems,
    source_schema_problems,
    surrogate_key,
    version_date,
)
from spark.silver_cases import NEVER_FEATURE_COLUMNS, OUTCOME_COLUMNS

NAMES = ("dim_case", "dim_patient", "dim_track", "dim_department", "dim_surgery_type", "dim_lab_test")
CLEAN_INPUT = {"source_members": 12, "ambiguous_members": 0, "key_null_rows": 0}
CLEAN_OUTPUT = {
    "members": 12,
    "unknown_rows": 1,
    "unknown_not_empty": 0,
    "key_duplicates": 0,
    "key_out_of_range": 0,
    "natural_null": 0,
    "natural_duplicates": 0,
    "key_rule_wrong": 0,
    "source_without_member": 0,
    "member_without_source": 0,
    "member_differs": 0,
    "interval_wrong": 0,
    "chain_broken": 0,
}


class SurrogateKeyTests(unittest.TestCase):
    def test_the_key_is_the_first_fifteen_hex_digits_of_the_sha256_of_the_dimension_and_the_natural_key(self) -> None:
        # The expected values were computed with `printf '%s' '<dimension>:<natural key>:2026-11-23' | shasum -a 256`, not with this code.
        wanted: dict[tuple[str, int | str], int] = {
            ("dim_case", 1147): 422893559182838084,
            ("dim_case", 1): 197494221555917002,
            ("dim_patient", "3333"): 986166312712892028,
            ("dim_department", "General surgery"): 668291668996376548,
            ("dim_surgery_type", "Colorectal"): 185962443777614359,
            ("dim_lab_test", "cr"): 434781063390605624,
            ("dim_track", "Solar8000/ART_MBP"): 1010606676282803311,
            ("dim_track", "Café/Ünï"): 550888254487257824,
        }
        for (name, natural), key in wanted.items():
            with self.subTest(name=name, natural=natural):
                self.assertEqual(key, surrogate_key(name, natural, "2026-11-23"))

    def test_a_whole_number_and_its_text_give_the_same_key(self) -> None:
        self.assertEqual(surrogate_key("dim_case", 1147, "2026-11-23"), surrogate_key("dim_case", "1147", "2026-11-23"))

    def test_the_same_text_in_two_dimensions_gives_two_keys(self) -> None:
        self.assertNotEqual(surrogate_key("dim_department", "Other", "2026-11-23"), surrogate_key("dim_surgery_type", "Other", "2026-11-23"))

    def test_a_member_keeps_its_key_when_other_members_come_and_go(self) -> None:
        first = {natural: surrogate_key("dim_case", natural, "2026-11-23") for natural in (3, 1, 2)}
        later = {natural: surrogate_key("dim_case", natural, "2026-11-23") for natural in (9, 2, 8, 3, 1, 7)}
        for natural, key in first.items():
            self.assertEqual(key, later[natural])

    def test_a_key_is_positive_below_the_limit_and_never_the_unknown_key(self) -> None:
        keys = [surrogate_key("dim_case", number, "2026-11-23") for number in range(1, 2001)]
        self.assertTrue(all(0 < key < KEY_LIMIT for key in keys))
        self.assertNotIn(UNKNOWN_KEY, keys)
        self.assertEqual(len(keys), len(set(keys)))

    def test_a_natural_key_that_is_not_a_whole_number_or_a_text_is_refused(self) -> None:
        for bad in (None, 1.5, True, b"1", ""):
            with self.subTest(bad=bad), self.assertRaises(GoldDimensionsError):
                surrogate_key("dim_case", bad, "2026-11-23")  # type: ignore[arg-type]

    def test_a_new_version_of_a_member_has_a_new_key_and_the_old_version_keeps_its_own(self) -> None:
        self.assertEqual(247720517620395626, surrogate_key("dim_case", 1, "2026-11-24"))
        self.assertNotEqual(surrogate_key("dim_case", 1, "2026-11-23"), surrogate_key("dim_case", 1, "2026-11-24"))

    def test_a_version_date_that_is_not_a_date_written_year_month_day_is_refused(self) -> None:
        for bad in ("2026-13-01", "2026-1-1", "20261123", "", "2026-11-23T00:00", None, 20261123):
            with self.subTest(bad=bad), self.assertRaises(GoldDimensionsError):
                version_date(bad)  # type: ignore[arg-type]
            with self.subTest(key=bad), self.assertRaises(GoldDimensionsError):
                surrogate_key("dim_case", 1, bad)  # type: ignore[arg-type]
        self.assertEqual("2026-02-28", version_date("2026-02-28"))

    def test_a_dimension_that_is_not_one_of_the_six_is_refused(self) -> None:
        with self.assertRaises(GoldDimensionsError):
            surrogate_key("dim_other", 1, "2026-11-23")


class DimensionDefinitionTests(unittest.TestCase):
    def test_the_six_dimensions_of_the_card_exist_with_their_kimball_names(self) -> None:
        self.assertEqual(NAMES, tuple(item.name for item in DIMENSIONS))
        self.assertEqual(len(NAMES), len({item.key for item in DIMENSIONS}))

    def test_each_dimension_starts_with_its_surrogate_key_then_its_natural_key_and_holds_no_other_key(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                columns = item.columns
                self.assertEqual(item.key, columns[0].name)
                self.assertEqual("bigint", columns[0].type)
                self.assertEqual(item.key, item.name.removeprefix("dim_") + "_key")
                self.assertEqual(item.natural, columns[1])
                self.assertEqual(len(columns), len({column.name for column in columns}))

    def test_each_dimension_is_type_2_and_ends_with_the_three_version_columns(self) -> None:
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                self.assertEqual([("valid_from", "date"), ("valid_to", "date"), ("is_current", "boolean")], [(c.name, c.type) for c in item.columns[-3:]])
                self.assertEqual(("valid_from", "valid_to", "is_current"), tuple(c.name for c in item.columns[-3:]))

    def test_no_dimension_holds_an_outcome_a_never_feature_column_an_intraoperative_or_preoperative_value_or_a_laboratory_result(self) -> None:
        banned = {*OUTCOME_COLUMNS, *NEVER_FEATURE_COLUMNS, "result", "value", "samples", "set"}
        for item in DIMENSIONS:
            for column in item.columns:
                with self.subTest(name=item.name, column=column.name):
                    self.assertNotIn(column.name, banned)
                    self.assertFalse(column.name.startswith(("intraop_", "preop_")))
        self.assertTrue({"icu_days", "death_inhosp", "intraop_ebl"} <= banned)

    def test_no_dimension_reads_an_outcome_or_a_never_feature_column_from_its_source(self) -> None:
        """The counts of this job's messages (members, rows and keys of a source) stay counts only because no dimension reads a field that a held-out patient's
        outcome can change: a dimension that read one would have to refuse as ``code: failed`` and never as a count."""
        banned = {*OUTCOME_COLUMNS, *NEVER_FEATURE_COLUMNS}
        for item in DIMENSIONS:
            for _, source_column in item.source_columns:
                with self.subTest(name=item.name, source_column=source_column):
                    self.assertNotIn(source_column, banned)
        self.assertEqual({"icu_days", "death_inhosp"}, set(OUTCOME_COLUMNS))

    def test_a_dimension_is_sealed_exactly_when_it_reads_the_laboratory_table_that_holds_held_out_patients_results(self) -> None:
        self.assertEqual({"dim_lab_test"}, {item.name for item in DIMENSIONS if item.sealed})
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                self.assertEqual(item.source == "silver.lab_results", item.sealed)

    def test_the_patient_dimension_holds_the_patient_number_and_nothing_else(self) -> None:
        self.assertEqual(["patient_key", "subject_id", "valid_from", "valid_to", "is_current"], [column.name for column in dimension("dim_patient").columns])

    def test_the_lab_test_dimension_is_the_catalog_of_test_names_and_holds_no_result(self) -> None:
        self.assertEqual(
            ["lab_test_key", "lab_test_name", "valid_from", "valid_to", "is_current"], [column.name for column in dimension("dim_lab_test").columns]
        )

    def test_a_source_column_exists_in_the_source_table_of_its_dimension(self) -> None:
        from spark.bronze.tables import TRACKS
        from spark.silver_cases import CASE_COLUMNS, LAB_COLUMNS

        held = {
            "silver.cases": {column.name: column.type for column in CASE_COLUMNS},
            "silver.lab_results": {column.name: column.type for column in LAB_COLUMNS},
            "bronze.tracks": dict(TRACKS.columns),
        }
        for item in DIMENSIONS:
            with self.subTest(name=item.name):
                source = held[item.source]
                for column, source_name in item.source_columns:
                    self.assertEqual(source[source_name], column.type)

    def test_an_unknown_name_is_refused(self) -> None:
        with self.assertRaises(GoldDimensionsError):
            dimension("dim_missing")


class SourceSchemaTests(unittest.TestCase):
    def found(self) -> dict[str, list[tuple[str, str]] | None]:
        return {table: list(fields) for table, fields in SOURCE_SCHEMAS.items()}

    def test_the_three_sources_are_the_silver_cases_the_silver_lab_table_and_the_bronze_tracks(self) -> None:
        self.assertEqual({"silver.cases", "silver.lab_results", "bronze.tracks"}, set(SOURCE_SCHEMAS))

    def test_sources_that_have_the_expected_columns_give_no_problem(self) -> None:
        self.assertEqual([], source_schema_problems(self.found()))

    def test_a_missing_table_and_a_table_with_other_columns_are_two_codes_with_a_count(self) -> None:
        found = self.found()
        found["bronze.tracks"] = None
        found["silver.cases"] = list(SOURCE_SCHEMAS["silver.cases"])[:-1]
        found["silver.lab_results"] = [(name, "string") for name, _ in SOURCE_SCHEMAS["silver.lab_results"]]
        self.assertEqual(["source_missing: 1 table", "source_schema: 2 tables"], source_schema_problems(found))

    def test_a_table_that_was_not_looked_up_counts_as_missing(self) -> None:
        self.assertEqual(["source_missing: 3 tables"], source_schema_problems({}))


class InputProblemTests(unittest.TestCase):
    def facts(self, **changes: int) -> dict[str, dict[str, int]]:
        return {name: CLEAN_INPUT | changes for name in NAMES}

    def test_clean_inputs_give_no_problem(self) -> None:
        self.assertEqual([], input_problems(self.facts()))

    def test_an_empty_source_is_refused_and_names_its_dimension(self) -> None:
        facts = self.facts()
        facts["dim_lab_test"] = CLEAN_INPUT | {"source_members": 0}
        self.assertEqual(["empty_source: failed"], input_problems(facts))

    def test_a_member_with_two_sets_of_attributes_is_refused(self) -> None:
        facts = self.facts()
        facts["dim_track"] = CLEAN_INPUT | {"ambiguous_members": 3}
        self.assertEqual(["ambiguous_members: dim_track, 3 members"], input_problems(facts))

    def test_a_case_without_a_case_number_or_a_patient_number_is_refused_for_the_two_keys_that_have_no_unknown_source(self) -> None:
        facts = self.facts()
        facts["dim_case"] = CLEAN_INPUT | {"key_null_rows": 1}
        facts["dim_patient"] = CLEAN_INPUT | {"key_null_rows": 2}
        self.assertEqual(["key_null: dim_case, 1 row", "key_null: dim_patient, 2 rows"], input_problems(facts))

    def test_a_null_department_or_type_or_track_is_not_refused_because_it_maps_to_the_unknown_member(self) -> None:
        facts = self.facts()
        for name in ("dim_department", "dim_surgery_type", "dim_track", "dim_lab_test"):
            facts[name] = CLEAN_INPUT | {"key_null_rows": 5}
        self.assertEqual([], input_problems(facts))

    def test_a_dimension_that_is_missing_from_the_facts_is_refused(self) -> None:
        facts = self.facts()
        del facts["dim_case"]
        with self.assertRaises(GoldDimensionsError):
            input_problems(facts)

    def test_a_fact_that_is_not_a_count_is_refused(self) -> None:
        for bad in (-1, True, "3", None):
            with self.subTest(bad=bad), self.assertRaises(GoldDimensionsError):
                input_problems(self.facts(source_members=bad))  # type: ignore[arg-type]


class OutputProblemTests(unittest.TestCase):
    def facts(self) -> dict[str, dict[str, int]]:
        return {name: dict(CLEAN_OUTPUT) for name in NAMES}

    def test_clean_tables_give_no_problem(self) -> None:
        self.assertEqual([], output_problems(self.facts()))

    def test_each_fault_has_its_own_code_and_names_its_dimension(self) -> None:
        wanted = {
            "unknown_rows": ("unknown_member", 2),
            "unknown_not_empty": ("unknown_member_not_empty", 1),
            "key_duplicates": ("key_duplicate", 4),
            "key_out_of_range": ("key_out_of_range", 1),
            "natural_null": ("natural_key_null", 1),
            "natural_duplicates": ("natural_key_duplicate", 2),
            "key_rule_wrong": ("key_rule", 3),
            "source_without_member": ("source_without_member", 5),
            "member_without_source": ("member_without_source", 6),
            "member_differs": ("member_value_differs", 7),
            "interval_wrong": ("interval_wrong", 8),
            "chain_broken": ("version_chain", 9),
        }
        for fact, (code, count) in wanted.items():
            with self.subTest(fact=fact):
                facts = self.facts()
                facts["dim_track"] = CLEAN_OUTPUT | {fact: count}
                problems = output_problems(facts)
                self.assertEqual(1, len(problems), problems)
                self.assertTrue(problems[0].startswith(f"{code}: dim_track, {count} "), problems)

    def test_no_unknown_member_and_two_unknown_members_are_both_refused(self) -> None:
        for count in (0, 2):
            facts = self.facts()
            facts["dim_case"] = CLEAN_OUTPUT | {"unknown_rows": count}
            self.assertEqual(1, len(output_problems(facts)))

    def test_a_table_with_no_member_is_refused(self) -> None:
        facts = self.facts()
        facts["dim_department"] = CLEAN_OUTPUT | {"members": 0}
        self.assertEqual(["no_members: dim_department, 0 members"], output_problems(facts))

    def test_every_dimension_is_judged_and_the_problems_come_in_the_order_of_the_dimensions(self) -> None:
        facts = self.facts()
        facts["dim_lab_test"] = CLEAN_OUTPUT | {"key_duplicates": 1}
        facts["dim_case"] = CLEAN_OUTPUT | {"key_duplicates": 1}
        self.assertEqual(["key_duplicate: dim_case, 1 key", "key_duplicate: failed"], output_problems(facts))

    def test_a_missing_fact_is_refused_and_never_taken_for_zero(self) -> None:
        facts = self.facts()
        del facts["dim_case"]["key_duplicates"]
        with self.assertRaises(GoldDimensionsError):
            output_problems(facts)

    def test_a_sealed_dimension_fails_as_its_code_and_failed_whatever_the_count(self) -> None:
        for fact, code, bad in (("unknown_rows", "unknown_member", (0, 2, 9)), ("key_rule_wrong", "key_rule", (1, 9)), ("members", "no_members", (0,))):
            for count in bad:
                with self.subTest(fact=fact, count=count):
                    facts = self.facts()
                    facts["dim_lab_test"] = CLEAN_OUTPUT | {fact: count}
                    self.assertEqual([f"{code}: failed"], output_problems(facts))

    def test_a_held_member_that_a_sealed_dimension_lost_fails_as_its_code_and_failed(self) -> None:
        held = {name: (0, 0) for name in NAMES}
        held["dim_lab_test"] = (4, 5)
        self.assertEqual(["member_removed: failed", "version_date_not_after: failed"], frozen_problems(held))

    def test_a_problem_names_no_value(self) -> None:
        facts = self.facts()
        facts["dim_case"] = {name: 7 for name in CLEAN_OUTPUT}
        for problem in output_problems(facts):
            self.assertRegex(problem, re.compile(r"^[a-z_]+: dim_[a-z_]+, [0-9]+ [a-z]+$"))


class FrozenProblemTests(unittest.TestCase):
    def test_a_first_build_or_an_unchanged_rebuild_has_nothing_to_refuse(self) -> None:
        self.assertEqual([], frozen_problems({name: (0, 0) for name in NAMES}))

    def test_a_removed_member_and_a_changed_member_are_two_codes_that_name_their_dimension(self) -> None:
        held = {name: (0, 0) for name in NAMES}
        held["dim_case"] = (2, 0)
        held["dim_track"] = (0, 1)
        self.assertEqual(["member_removed: dim_case, 2 members", "version_date_not_after: dim_track, 1 member"], frozen_problems(held))

    def test_a_dimension_that_is_missing_from_the_held_state_is_refused(self) -> None:
        held = {name: (0, 0) for name in NAMES[1:]}
        with self.assertRaises(GoldDimensionsError):
            frozen_problems(held)


if __name__ == "__main__":
    unittest.main()
